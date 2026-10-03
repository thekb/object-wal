#[allow(dead_code)]
#[path = "../benches/support/mod.rs"]
mod support;
use object_wal::core::codec;
use std::time::Duration;
use support::{Case, Matrix, Metrics, Options, Report, Sweep, percentile, report_row};
fn sweep(mode: Sweep, settings: &[(&str, &str)]) -> support::Result<Matrix> {
    Matrix::load(mode, |name| {
        settings
            .iter()
            .find(|(key, _)| *key == name)
            .map(|(_, value)| value.to_string())
    })
}
#[test]
fn sizes_are_a_cartesian_product_with_constant_flush_and_rotating_repeats() {
    let m = sweep(
        Sweep::Sizes,
        &[
            ("WAL_BENCH_VALUE_BYTES", "1024,16384,1024"),
            ("WAL_BENCH_RECORDS_PER_APPEND", "1,16"),
            ("WAL_BENCH_FLUSH_MS", "100"),
            ("WAL_BENCH_REPEATS", "2"),
        ],
    )
    .unwrap();
    assert_eq!(m.cases.len(), 8);
    assert!(
        m.cases
            .iter()
            .all(|c| c.options.flush_ms == 100 && c.options.concurrency == 16)
    );
    let pairs: Vec<_> = m.cases[..4]
        .iter()
        .map(|c| (c.options.value_bytes, c.options.records_per_append))
        .collect();
    assert_eq!(pairs, vec![(1024, 1), (1024, 16), (16384, 1), (16384, 16)]);
    assert_eq!(
        m.cases[4..].iter().map(|c| c.case_id).collect::<Vec<_>>(),
        vec![2, 3, 4, 1]
    );
    assert!(m.cases[4..].iter().all(|c| c.repetition == 2));
    assert_eq!(
        m.total_payload_bytes,
        m.cases
            .iter()
            .map(
                |c| c.options.payload_bytes(c.options.appends).unwrap() as u128
                    + c.options.payload_bytes(c.options.warmup).unwrap() as u128
            )
            .sum()
    );
}
#[test]
fn flush_sweep_changes_only_timeout() {
    let m = sweep(Sweep::Flush, &[("WAL_BENCH_FLUSH_MS", "0,10,100,10")]).unwrap();
    assert_eq!(m.cases.len(), 3);
    assert_eq!(
        m.cases
            .iter()
            .map(|c| c.options.flush_ms)
            .collect::<Vec<_>>(),
        vec![0, 10, 100]
    );
    assert!(
        m.cases
            .iter()
            .all(|c| c.options.value_bytes == 16384 && c.options.records_per_append == 32)
    );
    assert!(sweep(Sweep::Sizes, &[("WAL_BENCH_FLUSH_MS", "10,100")]).is_err());
    assert!(sweep(Sweep::Flush, &[("WAL_BENCH_VALUE_BYTES", "1024,16384")]).is_err());
    assert!(sweep(Sweep::Flush, &[("WAL_BENCH_RECORDS_PER_APPEND", "1,16")]).is_err());
}
#[test]
fn invalid_budgets_and_obsolete_settings_fail_before_network_access() {
    for settings in [
        vec![("WAL_BENCH_CONCURRENCY", "1,16")],
        vec![("WAL_BENCH_CONCURRENCY", "0")],
        vec![("WAL_BENCH_APPENDS", "0")],
        vec![("WAL_BENCH_RECORDS_PER_APPEND", "0")],
        vec![("WAL_BENCH_REPEATS", "0")],
        vec![("WAL_BENCH_REPEATS", "10001")],
        vec![("WAL_BENCH_VALUE_BYTES", "1024,")],
        vec![("WAL_TUNE_APPENDS", "1")],
    ] {
        assert!(sweep(Sweep::Sizes, &settings).is_err(), "{settings:?}");
    }
}
#[test]
fn requested_large_values_keep_only_codec_valid_cases() {
    let m = sweep(Sweep::Sizes, &[]).unwrap();
    assert_eq!(m.cases.len(), 18);
    assert!(m.cases.iter().all(|c| c.options.validate().is_ok()));
    assert_eq!(
        m.cases.iter().map(|c| c.case_id).collect::<Vec<_>>(),
        (1..=18).collect::<Vec<_>>()
    );
    for value in [1024, 16384, 65536, 262144, 1048576, 2097152, 4194304] {
        assert!(m.cases.iter().any(|c| c.options.value_bytes == value));
    }
    let case = m
        .cases
        .iter()
        .find(|c| c.options.value_bytes == 4194304)
        .unwrap();
    let records = case.options.request(0).records;
    assert_eq!(
        codec::decode_chunk(&codec::encode_records(&records).unwrap()).unwrap(),
        records
    );
    assert!(
        sweep(
            Sweep::Sizes,
            &[(
                "WAL_BENCH_VALUE_BYTES",
                &(codec::MAX_VAL_LEN + 1).to_string()
            )]
        )
        .is_err()
    );
    assert!(
        !m.cases
            .iter()
            .any(|c| c.options.value_bytes == 4194304 && c.options.records_per_append == 16)
    );
    let near_max = m
        .cases
        .iter()
        .find(|c| c.options.value_bytes == codec::MAX_VAL_LEN && c.options.records_per_append == 15)
        .unwrap();
    let payload = near_max.options.payload_bytes(1).unwrap();
    assert!(payload > 60 * 1024 * 1024);
    assert!(payload < codec::MAX_CHUNK_LEN);
}
#[test]
fn report_rows_include_configuration_metrics_and_do_not_mask_failures() {
    let options = Options {
        appends: 100,
        concurrency: 16,
        records_per_append: 2,
        value_bytes: 1016,
        warmup: 0,
        flush_ms: 10,
    };
    let metrics = Metrics::calculate(
        &options,
        (1..=100).map(Duration::from_millis).collect(),
        5,
        Duration::from_secs(2),
        Duration::from_secs(4),
    );
    let mut r = Report {
        schema_version: 2,
        stage: "s3_throughput".into(),
        case: Case {
            case_id: 1,
            repetition: 1,
            options,
        },
        prefix: "test".into(),
        status: "ok".into(),
        completed_appends: 100,
        codec_max_chunk_bytes: codec::MAX_CHUNK_LEN,
        codec_max_key_value_bytes: codec::MAX_KEY_VALUE_LEN,
        codec_max_records: codec::MAX_NUM_RECORDS,
        metrics: Some(metrics),
        error: None,
        cleanup_error: None,
    };
    let row = report_row(&r);
    assert!(row.contains("1016 | 2 | 10 | 16 | 100 | ok | 99.000 | 50.0 | 100.0 | 0.098 | 0.049"));
    r.status = "failed".into();
    r.cleanup_error = Some("cleanup | failed\nretry".into());
    let row = report_row(&r);
    assert!(!row.contains("99.000"));
    assert!(row.contains("cleanup &#124; failed retry"));
    assert_eq!(row.lines().count(), 1);
    assert_eq!(row.matches('|').count(), 15);
}
#[test]
fn percentiles_use_nearest_rank_per_append() {
    let samples: Vec<_> = (1..=100).map(Duration::from_millis).collect();
    assert_eq!(percentile(&samples, 50), 50.0);
    assert_eq!(percentile(&samples, 95), 95.0);
    assert_eq!(percentile(&samples, 99), 99.0);
    assert_eq!(percentile(&samples[..1], 99), 1.0);
    assert_eq!(percentile(&samples[..2], 99), 2.0);
}

#[test]
fn metrics_keep_append_and_publication_throughput_separate() {
    let options = Options {
        appends: 100,
        concurrency: 16,
        records_per_append: 2,
        value_bytes: 1016,
        warmup: 8,
        flush_ms: 10,
    };
    let samples: Vec<_> = (1..=100).rev().map(Duration::from_millis).collect();
    let metrics = Metrics::calculate(
        &options,
        samples,
        5,
        Duration::from_secs(2),
        Duration::from_secs(4),
    );
    assert_eq!(metrics.samples, 100);
    assert_eq!(metrics.records, 200);
    assert_eq!(metrics.payload_bytes, 204800);
    assert_eq!(metrics.appends_per_second, 50.0);
    assert_eq!(metrics.records_per_second, 100.0);
    assert_eq!(metrics.published_records_per_second, 50.0);
    assert_eq!(metrics.records_per_chunk, 40.0);
    assert_eq!(metrics.p99_ms, 99.0);
    assert_eq!(metrics.publication_drain_ms, 2000.0);
    let row = serde_json::to_value(metrics).unwrap();
    assert_eq!(row["p99_ms"], 99.0);
}
