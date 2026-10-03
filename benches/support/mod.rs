use object_wal::core::{
    codec,
    wal::{AppendRequest, Record},
};
use serde::Serialize;
use std::{error::Error, time::Duration};

pub type Result<T = ()> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct Options {
    pub appends: usize,
    pub concurrency: usize,
    pub records_per_append: usize,
    pub value_bytes: usize,
    pub warmup: usize,
    pub flush_ms: u64,
}

impl Options {
    pub fn payload_bytes(&self, count: usize) -> Result<usize> {
        count
            .checked_mul(self.records_per_append)
            .and_then(|n| n.checked_mul(8usize.checked_add(self.value_bytes)?))
            .ok_or_else(|| "payload size overflow".into())
    }

    pub fn validate(&self) -> Result {
        if self.appends == 0 || self.concurrency == 0 || self.records_per_append == 0 {
            return Err("appends, concurrency, and records per append must be positive".into());
        }
        if self.records_per_append > codec::MAX_NUM_RECORDS || self.value_bytes > codec::MAX_VAL_LEN
        {
            return Err("request exceeds codec record/value limits".into());
        }
        let payload = self
            .records_per_append
            .checked_mul(
                self.value_bytes
                    .checked_add(8)
                    .ok_or("payload size overflow")?,
            )
            .ok_or("payload size overflow")?;
        if payload > codec::MAX_KEY_VALUE_LEN {
            return Err("append exceeds maximum key/value payload size".into());
        }
        self.payload_bytes(self.appends)?;
        self.payload_bytes(self.warmup)?;
        if std::time::Instant::now()
            .checked_add(Duration::from_millis(self.flush_ms))
            .is_none()
        {
            return Err("flush timeout is too large".into());
        }
        Ok(())
    }

    pub fn request(&self, index: usize) -> AppendRequest {
        AppendRequest {
            records: (0..self.records_per_append)
                .map(|_| Record {
                    key: (index as u64).to_le_bytes().to_vec(),
                    value: vec![b'x'; self.value_bytes],
                })
                .collect(),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct Case {
    pub case_id: usize,
    pub repetition: usize,
    #[serde(flatten)]
    pub options: Options,
}

#[allow(dead_code)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Sweep {
    Sizes,
    Flush,
}

impl Sweep {
    pub fn name(self) -> &'static str {
        match self {
            Self::Sizes => "s3_throughput",
            Self::Flush => "s3_flush_timeout",
        }
    }
}

pub struct Matrix {
    pub cases: Vec<Case>,
    pub total_payload_bytes: u128,
}

impl Matrix {
    pub fn load(sweep: Sweep, mut env: impl FnMut(&str) -> Option<String>) -> Result<Self> {
        for key in [
            "WAL_TUNE_VALUES",
            "WAL_TUNE_RECORDS",
            "WAL_TUNE_FLUSH_MS",
            "WAL_TUNE_APPENDS",
            "WAL_TUNE_WARMUP",
            "WAL_TUNE_TOP",
            "WAL_TUNE_CONFIRM_APPENDS",
            "WAL_TUNE_REPEATS",
            "WAL_TUNE_CANDIDATES",
            "WAL_TUNE_MIN_GAIN_PERCENT",
            "WAL_TUNE_MAX_FLUSH_MS",
        ] {
            if env(key).is_some() {
                return Err(format!(
                    "{key} is obsolete; use WAL_BENCH_* sweep settings (see --help)"
                )
                .into());
            }
        }
        let values = list::<usize>(
            "WAL_BENCH_VALUE_BYTES",
            &env("WAL_BENCH_VALUE_BYTES").unwrap_or(
                match sweep {
                    Sweep::Sizes => "1024,16384,65536,262144,1048576,2097152,4194304",
                    Sweep::Flush => "16384",
                }
                .into(),
            ),
        )?;
        let records = list::<usize>(
            "WAL_BENCH_RECORDS_PER_APPEND",
            &env("WAL_BENCH_RECORDS_PER_APPEND").unwrap_or(
                match sweep {
                    Sweep::Sizes => "1,16,64",
                    Sweep::Flush => "32",
                }
                .into(),
            ),
        )?;
        let flush = list::<u64>(
            "WAL_BENCH_FLUSH_MS",
            &env("WAL_BENCH_FLUSH_MS").unwrap_or(
                match sweep {
                    Sweep::Sizes => "100",
                    Sweep::Flush => "0,10,50,100,500",
                }
                .into(),
            ),
        )?;
        match sweep {
            Sweep::Sizes if flush.len() != 1 => {
                return Err("size sweep requires exactly one flush timeout".into());
            }
            Sweep::Flush if values.len() != 1 || records.len() != 1 => {
                return Err("flush sweep requires exactly one value size and batch size".into());
            }
            _ => {}
        }
        let mut scalar = |key: &str, default: usize| -> Result<usize> {
            env(key).map_or(Ok(default), |v| {
                v.parse()
                    .map_err(|_| format!("{key} must be a single nonnegative integer").into())
            })
        };
        let appends = scalar("WAL_BENCH_APPENDS", 10)?;
        let warmup = scalar("WAL_BENCH_WARMUP_APPENDS", 32)?;
        let concurrency = scalar("WAL_BENCH_CONCURRENCY", 16)?;
        let repeats = scalar("WAL_BENCH_REPEATS", 1)?;
        let runs = values
            .len()
            .checked_mul(records.len())
            .and_then(|n| n.checked_mul(flush.len()))
            .and_then(|n| n.checked_mul(repeats))
            .ok_or("too many runs")?;
        if repeats == 0 || appends == 0 || concurrency == 0 || records.contains(&0) || runs > 10000
        {
            return Err("appends, concurrency, records and repeats must be positive; sweep must not exceed 10000 runs".into());
        }
        let mut base = Vec::new();
        for &value_bytes in &values {
            for &records_per_append in &records {
                for &flush_ms in &flush {
                    let options = Options {
                        appends,
                        warmup,
                        concurrency,
                        value_bytes,
                        records_per_append,
                        flush_ms,
                    };
                    if let Err(error) = options.validate() {
                        if value_bytes <= codec::MAX_VAL_LEN
                            && records_per_append <= codec::MAX_NUM_RECORDS
                            && records_per_append
                                .checked_mul(value_bytes.saturating_add(8))
                                .is_some_and(|n| n <= codec::MAX_KEY_VALUE_LEN)
                        {
                            return Err(error);
                        }
                    } else {
                        base.push(Case {
                            case_id: base.len() + 1,
                            repetition: 1,
                            options,
                        });
                    }
                }
            }
        }
        if sweep == Sweep::Sizes && values.contains(&codec::MAX_VAL_LEN) && !records.contains(&15) {
            let options = Options {
                appends,
                warmup,
                concurrency,
                value_bytes: codec::MAX_VAL_LEN,
                records_per_append: 15,
                flush_ms: flush[0],
            };
            if options.validate().is_ok() {
                base.push(Case {
                    case_id: base.len() + 1,
                    repetition: 1,
                    options,
                });
            }
        }
        if base.is_empty() {
            return Err("no codec-valid configurations".into());
        }
        let mut cases = Vec::with_capacity(runs);
        let mut total_payload_bytes = 0;
        for repetition in 1..=repeats {
            // Rotate each repeat so the same configuration is not always first.
            for offset in 0..base.len() {
                let mut case = base[(offset + repetition - 1) % base.len()].clone();
                case.repetition = repetition;
                total_payload_bytes += case.options.payload_bytes(appends)? as u128
                    + case.options.payload_bytes(warmup)? as u128;
                cases.push(case);
            }
        }
        Ok(Self {
            cases,
            total_payload_bytes,
        })
    }
}

#[derive(Serialize)]
pub struct Report {
    pub schema_version: u32,
    pub stage: String,
    #[serde(flatten)]
    pub case: Case,
    pub prefix: String,
    pub status: String,
    pub completed_appends: usize,
    pub codec_max_chunk_bytes: usize,
    pub codec_max_key_value_bytes: usize,
    pub codec_max_records: usize,
    pub metrics: Option<Metrics>,
    pub error: Option<String>,
    pub cleanup_error: Option<String>,
}

pub const REPORT_HEADER: &str = "| Case | Repeat | Value bytes | Records/append | Flush ms | Concurrency | Samples | Status | Append p99 ms | Appends/s | Records/s | Append MiB/s | Published MiB/s | Error |\n|---:|---:|---:|---:|---:|---:|---:|:---|---:|---:|---:|---:|---:|:---|";

pub fn report_row(r: &Report) -> String {
    let o = &r.case.options;
    let metrics = if r.status == "ok" {
        r.metrics.as_ref().map(|m| {
            format!(
                "{:.3} | {:.1} | {:.1} | {:.3} | {:.3}",
                m.p99_ms,
                m.appends_per_second,
                m.records_per_second,
                m.payload_mib_per_second,
                m.published_payload_mib_per_second
            )
        })
    } else {
        None
    }
    .unwrap_or_else(|| "— | — | — | — | —".into());
    let error = r
        .error
        .iter()
        .chain(r.cleanup_error.iter())
        .cloned()
        .collect::<Vec<_>>()
        .join("; ")
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('|', "&#124;")
        .replace(['\r', '\n'], " ");
    format!(
        "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |",
        r.case.case_id,
        r.case.repetition,
        o.value_bytes,
        o.records_per_append,
        o.flush_ms,
        o.concurrency,
        r.completed_appends,
        r.status,
        metrics,
        error
    )
}

pub fn list<T: std::str::FromStr + PartialEq>(name: &str, value: &str) -> Result<Vec<T>> {
    let mut result = Vec::new();
    for part in value.split(',') {
        let item = part
            .trim()
            .parse()
            .map_err(|_| format!("invalid {name}: expected comma-separated numbers"))?;
        if !result.contains(&item) {
            result.push(item);
        }
    }
    Ok(result)
}

/// Nearest-rank percentile of sorted, per-append latency samples.
pub fn percentile(sorted: &[Duration], percent: usize) -> f64 {
    assert!(!sorted.is_empty() && (1..=100).contains(&percent));
    let rank = ((sorted.len() as u128 * percent as u128).div_ceil(100)) as usize;
    sorted[rank - 1].as_secs_f64() * 1000.0
}

#[derive(Debug, Serialize)]
pub struct Metrics {
    pub samples: usize,
    pub records: usize,
    pub payload_bytes: usize,
    pub chunks: usize,
    pub records_per_chunk: f64,
    pub append_seconds: f64,
    pub publication_seconds: f64,
    pub publication_drain_ms: f64,
    pub appends_per_second: f64,
    pub records_per_second: f64,
    pub payload_mib_per_second: f64,
    pub published_records_per_second: f64,
    pub published_payload_mib_per_second: f64,
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
    pub max_ms: f64,
}

impl Metrics {
    pub fn calculate(
        options: &Options,
        mut latencies: Vec<Duration>,
        chunks: usize,
        append: Duration,
        published: Duration,
    ) -> Self {
        assert_eq!(latencies.len(), options.appends);
        assert!(chunks > 0 && !append.is_zero() && published >= append);
        latencies.sort_unstable();
        let records = options.appends * options.records_per_append;
        let payload_bytes = options
            .payload_bytes(options.appends)
            .expect("validated payload");
        let mib = payload_bytes as f64 / (1024.0 * 1024.0);
        let seconds = append.as_secs_f64();
        let total = published.as_secs_f64();
        Self {
            samples: latencies.len(),
            records,
            payload_bytes,
            chunks,
            records_per_chunk: records as f64 / chunks as f64,
            append_seconds: seconds,
            publication_seconds: total,
            publication_drain_ms: (total - seconds) * 1000.0,
            appends_per_second: options.appends as f64 / seconds,
            records_per_second: records as f64 / seconds,
            payload_mib_per_second: mib / seconds,
            published_records_per_second: records as f64 / total,
            published_payload_mib_per_second: mib / total,
            p50_ms: percentile(&latencies, 50),
            p95_ms: percentile(&latencies, 95),
            p99_ms: percentile(&latencies, 99),
            max_ms: percentile(&latencies, 100),
        }
    }
}
