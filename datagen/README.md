# datagen — dual-write log generator

Generates synthetic Kubernetes-style JSON logs and ships them to **OpenObserve**,
**ClickHouse**, or **both at once** from the same generated batch. Each batch is
generated a single time, then serialized for whichever targets are enabled:

- **OpenObserve** — bulk JSON array → `POST /api/{org}/{stream}/_json`
- **ClickHouse** — newline-delimited rows → `POST /?query=INSERT INTO db.table FORMAT JSONEachRow`

Writing both from one batch guarantees the two systems receive **byte-identical
records**, which is what makes the cross-system query comparison fair.

## Build

```bash
cargo build --release
```

## Quick start

```bash
# Preview one record, send nothing
./target/release/benchmark-data --dry-run

# Write the SAME data to both backends (default --target both)
./target/release/benchmark-data --total 100000000

# OpenObserve only
./target/release/benchmark-data --target openobserve --total 100000000

# ClickHouse only
./target/release/benchmark-data --target clickhouse --total 100000000
```

> Create the ClickHouse table first: `clickhouse client --queries-file ../schemas/clickhouse.sql`.
> The `--stream` value (default `k8s_logs`) is used as both the OpenObserve stream
> and the ClickHouse table name, so keep it consistent with the schema.

## Key options

| Flag | Default | Description |
|------|---------|-------------|
| `--target` | `both` | `openobserve` \| `clickhouse` \| `both` |
| `--o2-url` | `http://localhost:5080` | OpenObserve base URL |
| `--org` | `default` | OpenObserve organization id |
| `--stream` | `k8s_logs` | Stream name **and** ClickHouse table name |
| `--username` / `--password` | `root@example.com` / `Complexpass#123` | OpenObserve login |
| `--ch-url` | `http://localhost:8123` | ClickHouse HTTP URL |
| `--ch-database` | `default` | ClickHouse database |
| `--ch-user` / `--ch-password` | `default` / *(empty)* | ClickHouse credentials |
| `--total` | `100_000_000` | Total records to generate |
| `--batch-size` | `8_000` | Records per HTTP request |
| `--concurrency` | `6` | Parallel in-flight requests |
| `--compress` | `true` | Gzip request bodies (`Content-Encoding: gzip`). Big win across nodes; pass `--compress false` to disable |
| `--seed` | `0xC0FFEE` | PRNG seed for reproducible payloads |
| `--stats-out` | *(none)* | Write a JSON ingest summary (records, raw bytes, rec/s, MB/s) for the report |
| `--dry-run` | `false` | Print one sample record and exit |

## Output

On completion it reports per-target delivery, e.g.:

```
done: sent=100000000 failed=0 (openobserve_failed=0 clickhouse_failed=0) elapsed=42.13s rate=237000 rec/s
```

A record counts as `sent` only when **all** enabled targets accepted it; otherwise
it counts as `failed`, with the per-target breakdown shown alongside.

## Schema / cardinality

The generated schema (k8s metadata, HTTP fields, tracing ids, security fields)
and the field cardinalities are documented in the canonical fields of
`src/main.rs` and mirrored by [`../schemas/clickhouse.sql`](../schemas/clickhouse.sql).
The `message` line is built from real-world log templates (`src/templates.rs`) and
ends with a `trace_id=<trace_id> span_id=<span_id> x_request_id=<request_id>`
request-context suffix, so text needle queries have guaranteed matches: the full
ids are whole tokens (FTS-index lookups), and the request id is the needle for the
rare whole-token search (q2).

## Reproducibility

For the same `--seed` and `--total`, generated record content is deterministic
across machines and independent of request concurrency or batch size. `_timestamp`
defaults to the current time at ingest startup, then increments by 1 us per row.
The benchmark query ids and tokens are embedded directly in
[`../queries/queries.json`](../queries/queries.json); keep `--seed` and `--total`
consistent with those values when generating benchmark datasets.
