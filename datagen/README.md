# datagen — three-way log generator

Generates synthetic Kubernetes-style JSON logs and sends every generated batch
to ClickHouse, O2-Parquet, and O2-Vortex. Records are serialized once into one
NDJSON body. Both O2 instances ingest it through `/_multi`, while ClickHouse
ingests the exact same bytes as `JSONEachRow`.

This single-pass fan-out guarantees byte-identical logical records, including
the `_timestamp`, across all three benchmark datasets.

## Build and run

```bash
cargo build --release

# Preview one record without sending it
./target/release/benchmark-data --dry-run

# All three backends (default). Use a recently closed hour as the event-time
# anchor. Each later batch adds actual wall-clock elapsed time to this anchor.
START_TIMESTAMP_US="$(python3 -c 'import time; print((int(time.time()) // 3600 - 1) * 3600 * 1_000_000)')"
./target/release/benchmark-data --target all --total 100000000 \
  --start-timestamp-us "$START_TIMESTAMP_US"

# One backend only
./target/release/benchmark-data --target clickhouse --total 100000000
./target/release/benchmark-data --target o2-parquet --total 100000000
./target/release/benchmark-data --target o2-vortex --total 100000000
```

Create the ClickHouse table first with
`clickhouse client --queries-file ../schemas/clickhouse.sql`. Start the two O2
instances with `../scripts/start-openobserve.sh parquet` and `vortex`.

## Key options

| Flag | Default | Description |
| --- | --- | --- |
| `--target` | `all` | `all`, `clickhouse`, `o2-parquet`, or `o2-vortex` |
| `--o2-parquet-url` | `http://localhost:5080` | O2-Parquet base URL |
| `--o2-vortex-url` | `http://localhost:5090` | O2-Vortex base URL |
| `--org` | `default` | OpenObserve organization id |
| `--stream` | `k8s_logs` | O2 stream and ClickHouse table |
| `--username` / `--password` | `root@example.com` / `Complexpass#123` | O2 credentials |
| `--ch-url` | `http://localhost:8123` | ClickHouse HTTP URL |
| `--ch-database` | `default` | ClickHouse database |
| `--ch-user` / `--ch-password` | `default` / empty | ClickHouse credentials |
| `--total` | `100000000` | Records to generate |
| `--start-timestamp-us` | current time | Event-time anchor; later batches advance it by actual elapsed wall-clock time |
| `--batch-size` | `8000` | Records per HTTP batch |
| `--concurrency` | `6` | Concurrent batch fan-outs |
| `--compress` | `true` | Gzip request bodies |
| `--seed` | `0xC0FFEE` | Deterministic content seed |
| `--stats-out` | none | Write ingest counts, throughput, bytes, per-target failures, and the actual event-time range |

Example completion output:

```text
done: sent=100000000 failed=0 (clickhouse_failed=0 o2_parquet_failed=0 o2_vortex_failed=0) elapsed=42.13s rate=237000 rec/s (500.0 MB/s)
```

A record contributes to `sent` only when every enabled target accepted its
batch. `raw_bytes` counts the logical dataset once, not once per destination.
The completion output and stats file report `timestamp_min_us`,
`timestamp_max_us`, and `timestamp_span_s` so long runs can verify that event
time advanced with the ingest.

## Reproducibility

For a fixed `--seed` and `--total`, all non-time record content is independent of
request concurrency and batch completion order. `_timestamp` intentionally
tracks the benchmark's real elapsed time: a six-hour ingest produces roughly a
six-hour event-time range instead of compressing one billion rows into 16m40s.
`--start-timestamp-us` is the event-time anchor, not a fixed timestamp for the
whole run. Using the beginning of the previous hour keeps every batch at a
constant one-to-two-hour lag, inside O2's normal five-hour ingest window, and
lets completed hours become eligible for final compaction and external bloom
generation during the run.

Each batch still has one byte-identical NDJSON body shared by all enabled
destinations. Rows inside a batch are separated by one microsecond; the next
batch advances from the anchor by the generator's actual monotonic elapsed time.
The generator primes batch 0 synchronously so both O2 streams establish their
time range from the true earliest batch before later batches are sent
concurrently.

O2 can report record-level rejection inside an HTTP 200 response. The generator
validates the `/_multi` response body and counts a batch as successful only when
all expected rows were accepted. The benchmark does not override O2's ingest-age
policy, so the timestamp anchor must remain inside its normal window.

The schema and cardinalities live in `src/main.rs` and mirror
[`../schemas/clickhouse.sql`](../schemas/clickhouse.sql). Query anchors are fixed
in [`../queries/queries.json`](../queries/queries.json).
