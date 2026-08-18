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

# All three backends (default). Use a recently closed hour so O2 can finish
# bloom generation without relaxing its normal ingest-age policy.
START_TIMESTAMP_US="$(python3 -c 'import time; print((int(time.time()) // 3600 - 2) * 3600 * 1_000_000)')"
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
| `--start-timestamp-us` | current time | First `_timestamp`; if set, keep it within the backend's normal ingest window |
| `--batch-size` | `8000` | Records per HTTP batch |
| `--concurrency` | `6` | Concurrent batch fan-outs |
| `--compress` | `true` | Gzip request bodies |
| `--seed` | `0xC0FFEE` | Deterministic content seed |
| `--stats-out` | none | Write ingest counts, throughput, bytes, and per-target failures |

Example completion output:

```text
done: sent=100000000 failed=0 (clickhouse_failed=0 o2_parquet_failed=0 o2_vortex_failed=0) elapsed=42.13s rate=237000 rec/s (500.0 MB/s)
```

A record contributes to `sent` only when every enabled target accepted its
batch. `raw_bytes` counts the logical dataset once, not once per destination.

## Reproducibility

For fixed `--seed`, `--total`, and `--start-timestamp-us`, record content is
independent of request concurrency and batch completion order. Use a timestamp
inside the benchmark query window and at least one completed hour in the past;
O2 intentionally postpones the current hour's final compaction and external
bloom build until that hour closes. The generator primes batch 0 synchronously
so both O2 streams establish their time range from the true earliest record
before later batches are sent concurrently.

O2 can report record-level rejection inside an HTTP 200 response. The generator
validates the `/_multi` response body and counts a batch as successful only when
all expected rows were accepted. The benchmark does not override O2's ingest-age
policy, so an explicitly supplied timestamp must remain inside its normal window.

The schema and cardinalities live in `src/main.rs` and mirror
[`../schemas/clickhouse.sql`](../schemas/clickhouse.sql). Query anchors are fixed
in [`../queries/queries.json`](../queries/queries.json).
