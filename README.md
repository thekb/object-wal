# object-wal
Experiment to create a WAL out of S3 like object stores

The repository is a single Rust package, `object-wal` (import as `object_wal`).
`src/core` defines the WAL and storage contracts and chunk codec; `src/wal`
implements buffering, manifest coordination, and tailing; `src/objectstore`
contains the S3 adapter. Integration tests live in `tests/`, and the adaptive
throughput benchmark lives in `benches/`.

```bash
cargo test
cargo bench --bench s3_throughput -- --plan
```

## Live S3 / Cloudflare R2 integration tests

The live tests are ignored by default. They exercise conditional object writes,
buffered append/tail round trips, independent appender instances, and recovery
of unpublished chunks against an existing bucket.

Copy `.envrc.example` to `.envrc` if you have not configured it already. Set the
R2 S3 access key pair, `AWS_REGION=auto`, the account's S3 API endpoint in
`AWS_ENDPOINT_URL`, and an existing bucket in `S3_TEST_BUCKET`. Use the endpoint
for the bucket's jurisdiction when applicable. `.envrc` is ignored by Git.

```bash
direnv allow
direnv exec . cargo test -p object-wal --test s3_integration -- --ignored --test-threads=1
```

Credentials are loaded through the AWS SDK credential chain; Rust does not parse
`.envrc`. The tests need object read, write, list, and delete access, but do not
create or delete buckets. Each test uses a unique prefix beneath
`object-wal-integration/` and deletes only that prefix's objects afterward,
including after assertions fail or a test times out. Forced process termination
can leave objects behind; cleanup failures report the affected prefix.

Tests explicitly start `Appender::run`, close each appender, and wait for manifest
publication before cleanup. SDK calls, test cases, shutdown, and cleanup have
timeouts. No credentials are printed by the test harness.

## R2 / S3 fixed benchmark reports

Use the same `.envrc` as the integration tests. The two benchmarks produce
fixed-sweep JSONL and Markdown reports. Preview either sweep:

```bash
direnv exec . cargo bench --bench s3_throughput -- --plan
```

Replace `--plan` with `--run` to execute. Without `--run`, no network I/O occurs.

The throughput report measures every value-size and batch-size combination at a
constant flush timeout. A separate report measures every flush timeout at one
fixed value size and batch size. There is no adaptive selection or winner.

The size sweep uses 1 KiB, 16 KiB, 64 KiB, 256 KiB, 1 MiB, 2 MiB, and 4 MiB
values, crossed with batches of 1, 16, and 64 records at a constant 100 ms
flush timeout. The timeout sweep uses 16 KiB values, batches of 32 records, and
0, 10, 50, 100, and 500 ms timeouts.

| Setting | Default |
| --- | --- |
| `WAL_BENCH_VALUE_BYTES` | sweep-specific value sizes |
| `WAL_BENCH_RECORDS_PER_APPEND` | sweep-specific batch sizes |
| `WAL_BENCH_FLUSH_MS` | one constant for size sweep; list for timeout sweep |
| `WAL_BENCH_CONCURRENCY` | `16` (single value) |
| `WAL_BENCH_APPENDS` | `10` measured appends per configuration |
| `WAL_BENCH_WARMUP_APPENDS` | `32` unmeasured appends per run |
| `WAL_BENCH_REPEATS` | `1` |
| `WAL_BENCH_OUTPUT` | new `target/bench-results/s3-throughput-<run-id>.jsonl` |

Only combinations that fit the codec's 64 MiB chunk limit appear in the plan
and report. The size sweep also includes a 4 MiB × 15 record case, about 60 MiB
per append. If no requested combination fits, planning fails before network
access. All appends remain atomic.

Run both reports:

```bash
direnv exec . cargo bench --bench s3_throughput -- --run

direnv exec . cargo bench --bench s3_flush_timeout -- --run
```

Each configuration uses 10 measured appends by default, so p99 is a sparse
diagnostic estimate. Increase `WAL_BENCH_APPENDS` for a more stable tail
estimate. Increasing value size maximizes bytes/s, which can differ from
maximizing records/s. This is a closed-loop workload; it does not measure
latency under a fixed independent arrival rate.

Chunks allow up to 64 MiB of encoded key and value bytes and 65,535 records (the
existing u16 count format). Each record's 8 bytes of length fields, the 12-byte
header (`!OBJWAL!`, a 2-byte version, and a 2-byte record count), and the
4-byte CRC32 trailer are additional. Version 1 uses CRC32 over the header and
records. Readers verify it before decoding. Segments written with either
previous magic value are not supported by this format.
The byte limit, record limit, timeout, or close can trigger a flush. At fixed
concurrency, callers wait for append completion before supplying more data:
approximately `concurrency × records_per_append × (value_bytes + 16)` encoded
bytes can be supplied before waiting. A longer timeout cannot fill a chunk if
that amount is too small; increase the batch size or concurrency. Requests are
not split across chunks, so chunks can also flush just before the size limit.

Every run creates an independent appender and unique object prefix. Warmup waits
for publication before measurement. Latency starts when `append` is called and
includes queueing, buffering, and upload, using nearest rank per-append samples.
Payload counts 8-byte keys plus values, excluding encoding and HTTP overhead.
Both append and publication throughput are recorded.

JSONL results are flushed after every run with stage, configuration, sample count,
p50/p95/p99/max latency, throughputs, codec limits, and any errors. A sibling
Markdown report contains one table row per configuration. Results are not
overwritten. Relative paths resolve from Cargo's crate directory, and the full
output path is printed. Failed cases are recorded and later cases continue;
any failed case results in a nonzero exit, even if a recommendation is available.

There is no overall workload, publication-drain, or cleanup deadline. Individual
S3 operations use a 15-second timeout; setup and manifest body reads have
30-second limits. Progress prints every five seconds during uploads.
The harness supervises the appender during warmup and measurement and cleans up
only its own `object-wal-benchmark/` prefixes after success, errors, or caught
measurement panics. Process termination can leave the printed prefix behind.
