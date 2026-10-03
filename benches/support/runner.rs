//! Shared live S3/R2 sweep runner.

use super::support::{
    Case, Matrix, Metrics, Options, REPORT_HEADER, Report, Result, Sweep, report_row,
};
use aws_sdk_s3::config::{RequestChecksumCalculation, ResponseChecksumValidation};
use futures::{FutureExt, StreamExt, stream};
use object_wal::{
    core::{
        codec,
        objectstore::*,
        util::{manifest_key, stream_to_struct},
        wal::*,
    },
    objectstore::s3::S3Store,
    wal::{AppenderConfig, AppenderImpl},
};
use serde::Serialize;
use std::{
    collections::HashSet,
    fs::{File, OpenOptions},
    future::Future,
    io::Write,
    panic::AssertUnwindSafe,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{
    task::JoinSet,
    time::{MissedTickBehavior, interval, timeout},
};

fn required(name: &str) -> Result<String> {
    let value = std::env::var(name).map_err(|_| format!("set {name} using .envrc"))?;
    if value.trim().is_empty() || value.contains('<') {
        return Err(format!("configure {name}").into());
    }
    Ok(value)
}

struct Sample {
    seq: u64,
    latency: Duration,
}

async fn workload(
    appender: &AppenderImpl<S3Store>,
    options: &Options,
    count: usize,
    completed: &AtomicUsize,
    phase: &str,
) -> Result<Vec<Sample>> {
    let requests = stream::iter(0..count)
        .map(|index| async move {
            let request = options.request(index);
            let start = Instant::now();
            let response = appender.append(request).await?;
            Ok::<_, WALError>(Sample {
                seq: response.chunk_seq,
                latency: start.elapsed(),
            })
        })
        .buffer_unordered(options.concurrency);
    futures::pin_mut!(requests);
    let mut samples = Vec::new();
    let start = Instant::now();
    let mut progress = interval(Duration::from_secs(5));
    progress.set_missed_tick_behavior(MissedTickBehavior::Skip);
    progress.tick().await;
    loop {
        tokio::select! {
            sample = requests.next() => match sample {
                Some(sample) => {
                    samples.push(sample?);
                    completed.fetch_add(1, Ordering::Relaxed);
                }
                None => return Ok(samples),
            },
            _ = progress.tick() => eprintln!("  {phase}: {}/{} acknowledged in {:.1}s", samples.len(), count, start.elapsed().as_secs_f64()),
        }
    }
}

/// Watch run() even while waiting for warmup visibility: otherwise a publication
/// failure after successful append acknowledgements can leave warmup stuck.
async fn supervised<T>(
    tasks: &mut JoinSet<std::result::Result<(), WALError>>,
    work: impl Future<Output = Result<T>>,
) -> Result<T> {
    tokio::select! {
        biased;
        stopped = tasks.join_next() => match stopped {
            Some(Ok(Err(error))) => Err(error.into()),
            Some(Err(error)) => Err(error.into()),
            _ => Err("appender stopped before shutdown".into()),
        },
        result = work => result,
    }
}

async fn manifest(store: &S3Store, bucket: &str, prefix: &str) -> Result<Manifest> {
    let GetObjectResult::Found(response) = store
        .get_object(GetObjectRequest {
            bucket: bucket.into(),
            key: manifest_key(prefix),
            if_none_match: None,
        })
        .await?
    else {
        return Err("unexpected unchanged manifest".into());
    };
    // Bound body reads too, with a phase-specific error.
    Ok(
        timeout(Duration::from_secs(30), stream_to_struct(response.body))
            .await
            .map_err(|_| "manifest body read exceeded 30 seconds")??,
    )
}

async fn cleanup(client: &aws_sdk_s3::Client, bucket: &str, prefix: &str) -> Result {
    let prefix = format!("{prefix}/");
    let mut pages = client
        .list_objects_v2()
        .bucket(bucket)
        .prefix(&prefix)
        .into_paginator()
        .send();
    let mut keys = Vec::new();
    while let Some(page) = pages.next().await {
        for object in page?.contents() {
            let key = object.key().ok_or("listed object has no key")?;
            if !key.starts_with(&prefix) {
                return Err("out-of-prefix cleanup key".into());
            }
            keys.push(key.to_owned());
        }
    }
    let failures: Vec<_> = stream::iter(keys)
        .map(|key| async move {
            client
                .delete_object()
                .bucket(bucket)
                .key(key)
                .send()
                .await
                .map(|_| ())
        })
        .buffer_unordered(8)
        .filter_map(|r| async { r.err() })
        .collect()
        .await;
    if !failures.is_empty() {
        return Err(format!("failed to delete {} benchmark objects", failures.len()).into());
    }
    Ok(())
}

async fn benchmark(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    case: &Case,
    prefix: String,
) -> Report {
    let options = &case.options;
    let store = Arc::new(S3Store::new(client.clone()));
    let appender = Arc::new(AppenderImpl::with_config(
        store.clone(),
        bucket,
        &prefix,
        AppenderConfig {
            flush_timeout: Duration::from_millis(options.flush_ms),
        },
    ));
    let runner = appender.clone();
    let mut tasks = JoinSet::new();
    tasks.spawn(async move { runner.run().await });
    let completed = AtomicUsize::new(0);
    let mut phase = "warmup";
    let outcome = AssertUnwindSafe(async {
        if options.warmup > 0 {
            let warmup_completed = AtomicUsize::new(0);
            let samples = supervised(
                &mut tasks,
                workload(
                    &appender,
                    options,
                    options.warmup,
                    &warmup_completed,
                    "warmup",
                ),
            )
            .await?;
            let last = samples.iter().map(|s| s.seq).max().unwrap();
            phase = "warmup publication";
            supervised(&mut tasks, async {
                while manifest(&store, bucket, &prefix).await?.next_seq <= last {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Ok(())
            })
            .await?;
        }
        phase = "measurement";
        let start = Instant::now();
        let samples = supervised(
            &mut tasks,
            workload(
                &appender,
                options,
                options.appends,
                &completed,
                "measurement",
            ),
        )
        .await?;
        let elapsed = start.elapsed();
        phase = "publication drain";
        appender.close().await;
        tasks.join_next().await.ok_or("missing appender task")???;
        let published_elapsed = start.elapsed();
        phase = "manifest verification";
        let last = samples.iter().map(|s| s.seq).max().unwrap();
        if manifest(&store, bucket, &prefix).await?.next_seq != last + 1 {
            return Err("manifest did not publish all acknowledged chunks".into());
        }
        let chunks = samples.iter().map(|s| s.seq).collect::<HashSet<_>>().len();
        let latencies = samples.into_iter().map(|s| s.latency).collect();
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(Metrics::calculate(
            options,
            latencies,
            chunks,
            elapsed,
            published_elapsed,
        ))
    })
    .catch_unwind()
    .await;
    // On failure, abort and join the writer before deleting its objects.
    appender.close().await;
    tasks.shutdown().await;
    let (metrics, error) = match outcome {
        Ok(Ok(metrics)) => (Some(metrics), None),
        Ok(Err(error)) => (None, Some(format!("{phase}: {error}"))),
        Err(_) => (None, Some(format!("{phase}: benchmark panicked"))),
    };
    let cleanup_error = cleanup(client, bucket, &prefix)
        .await
        .err()
        .map(|e| e.to_string());
    Report {
        schema_version: 2,
        stage: "sweep".into(),
        case: case.clone(),
        prefix,
        status: if error.is_none() && cleanup_error.is_none() {
            "ok"
        } else {
            "failed"
        }
        .into(),
        completed_appends: completed.load(Ordering::Relaxed),
        codec_max_chunk_bytes: codec::MAX_CHUNK_LEN,
        codec_max_key_value_bytes: codec::MAX_KEY_VALUE_LEN,
        codec_max_records: codec::MAX_NUM_RECORDS,
        metrics,
        error,
        cleanup_error,
    }
}

fn plan(matrix: &Matrix) -> Result {
    println!(
        "{} runs; payload including warmup {:.3} GiB; one independent appender per run",
        matrix.cases.len(),
        matrix.total_payload_bytes as f64 / 1073741824.0
    );
    println!("case rep appends concurrency records/append value_bytes flush_ms");
    for c in &matrix.cases {
        let o = &c.options;
        println!(
            "{} {} {} {} {} {} {}",
            c.case_id,
            c.repetition,
            o.appends,
            o.concurrency,
            o.records_per_append,
            o.value_bytes,
            o.flush_ms
        );
    }
    if matrix.cases.iter().any(|c| c.options.appends < 100) {
        eprintln!(
            "Note: fewer than 100 append samples gives a very sparse p99 estimate; sample counts are saved."
        );
    }
    Ok(())
}

async fn run(matrix: Matrix, sweep: Sweep) -> Result {
    let bucket = required("S3_TEST_BUCKET")?;
    let endpoint = required("AWS_ENDPOINT_URL")?;
    required("AWS_REGION")?;
    let run_id = format!(
        "{}-{}",
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos(),
        std::process::id()
    );
    let output = std::env::var("WAL_BENCH_OUTPUT")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            PathBuf::from(format!(
                "target/bench-results/{}-{run_id}.jsonl",
                sweep.name()
            ))
        });
    if output.extension().and_then(|s| s.to_str()) != Some("jsonl") {
        return Err(
            "WAL_BENCH_OUTPUT must end in .jsonl; the Markdown report uses the same stem".into(),
        );
    }
    let output = if output.is_absolute() {
        output
    } else {
        std::env::current_dir()?.join(output)
    };
    let markdown = output.with_extension("md");
    if let Some(parent) = output.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&output)?;
    let mut report_file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&markdown)?;
    println!(
        "JSONL: {}\nReport: {}",
        output.display(),
        markdown.display()
    );
    writeln!(
        report_file,
        "# {}\n\nRun: {run_id}. All configurations are measured; no adaptive selection.\n",
        sweep.name()
    )?;
    let o = &matrix.cases[0].options;
    writeln!(
        report_file,
        "{} measured appends and {} excluded warmup appends per run. Fixed concurrency: {}.\n",
        o.appends, o.warmup, o.concurrency
    )?;
    writeln!(
        report_file,
        "Append p99 uses nearest-rank latency from calling append to its durable acknowledgement, excluding request allocation. Append throughput includes allocation and upload; published throughput also includes the final manifest drain. Payload includes 8-byte keys and values. The 64 MiB key/value limit excludes per-record length fields and the {}-byte chunk header and checksum. Repeats are shown individually; p99 values are not averaged.\n",
        codec::CHUNK_OVERHEAD_LEN
    )?;
    if o.appends < 100 {
        writeln!(
            report_file,
            "**Small sample run: p99 estimates are sparse.**\n"
        )?;
    }
    writeln!(report_file, "{REPORT_HEADER}")?;
    report_file.sync_data()?;
    let shared = timeout(
        Duration::from_secs(30),
        aws_config::load_defaults(aws_config::BehaviorVersion::latest()),
    )
    .await
    .map_err(|_| "AWS configuration loading exceeded 30 seconds")?;
    let client = aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::config::Builder::from(&shared)
            .endpoint_url(endpoint)
            .force_path_style(true)
            .request_checksum_calculation(RequestChecksumCalculation::WhenRequired)
            .response_checksum_validation(ResponseChecksumValidation::WhenRequired)
            .timeout_config(
                aws_sdk_s3::config::timeout::TimeoutConfig::builder()
                    .operation_timeout(Duration::from_secs(15))
                    .build(),
            )
            .build(),
    );
    let mut rows = Vec::new();
    let mut failed = 0;
    for (index, case) in matrix.cases.iter().enumerate() {
        let prefix = format!(
            "object-wal-benchmark/{run_id}/case-{}-rep-{}",
            case.case_id, case.repetition
        );
        println!(
            "Run {}/{}: value_bytes={} records/append={} flush_ms={} repetition={}; prefix {prefix}",
            index + 1,
            matrix.cases.len(),
            case.options.value_bytes,
            case.options.records_per_append,
            case.options.flush_ms,
            case.repetition
        );
        let mut report = benchmark(&client, &bucket, case, prefix).await;
        report.stage = sweep.name().into();
        save(&mut file, &report)?;
        let row = report_row(&report);
        writeln!(report_file, "{row}")?;
        report_file.flush()?;
        report_file.sync_data()?;
        println!("{row}");
        rows.push(row);
        if report.status != "ok" {
            failed += 1;
        }
    }
    println!("\n{REPORT_HEADER}\n{}", rows.join("\n"));
    writeln!(
        report_file,
        "\nCompleted {} runs; {failed} failed. Failed runs are excluded from performance comparisons; details remain in JSONL.\n",
        matrix.cases.len()
    )?;
    report_file.sync_data()?;
    println!("Report: {}", markdown.display());
    if failed > 0 {
        return Err(format!(
            "{failed} runs failed; report saved to {}",
            markdown.display()
        )
        .into());
    }
    Ok(())
}

fn save(file: &mut File, report: &impl Serialize) -> Result {
    serde_json::to_writer(&mut *file, report)?;
    writeln!(file)?;
    file.flush()?;
    file.sync_data()?;
    Ok(())
}

pub fn main(sweep: Sweep) -> Result {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args
        .iter()
        .any(|a| !matches!(a.as_str(), "--run" | "--plan" | "--bench" | "--help" | "-h"))
    {
        return Err("unknown argument; use --help".into());
    }
    if args.iter().any(|a| a == "--help" || a == "-h")
        || !args.iter().any(|a| a == "--run" || a == "--plan")
    {
        println!(
            concat!(
                "S3/R2 fixed sweep: {}. --plan lists all valid runs without network I/O; --run measures them.\n",
                "Run: direnv exec . cargo bench --bench {} -- --run\n",
                "Axes: WAL_BENCH_VALUE_BYTES, WAL_BENCH_RECORDS_PER_APPEND, WAL_BENCH_FLUSH_MS.\n",
                "Size sweep: 1024,16384,65536,262144,1048576,2097152,4194304 bytes; 1,16,64 records, plus 4 MiB x 15 records; 100 ms flush.\n",
                "Flush sweep: 16384 bytes; 32 records; 0,10,50,100,500 ms.\n",
                "Common: WAL_BENCH_APPENDS=10 WAL_BENCH_WARMUP_APPENDS=32 WAL_BENCH_CONCURRENCY=16 WAL_BENCH_REPEATS=1\n",
                "Output: WAL_BENCH_OUTPUT=<new.jsonl>, plus a sibling Markdown report.\n",
                "No overall deadline. Individual S3 request timeout: 15s.\n",
                "Required: AWS_ENDPOINT_URL, AWS_REGION, S3_TEST_BUCKET, SDK credentials."
            ),
            sweep.name(),
            sweep.name()
        );
        return Ok(());
    }
    let matrix = Matrix::load(sweep, |name| std::env::var(name).ok())?;
    plan(&matrix)?;
    if args.iter().any(|a| a == "--plan") {
        return Ok(());
    }
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(run(matrix, sweep))
}
