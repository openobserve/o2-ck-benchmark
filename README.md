# OpenObserve vs ClickHouse — Observability Benchmark

A reproducible, three-way benchmark of the same observability-log workload on:

1. **ClickHouse**
2. **OpenObserve with `ZO_FILE_FORMAT=parquet`** (`o2-parquet`)
3. **OpenObserve with `ZO_FILE_FORMAT=vortex`** (`o2-vortex`)

The two OpenObserve instances use the same binary, auth, indexes, query SQL, and
runtime settings. Their only storage-engine A/B variable is `ZO_FILE_FORMAT`;
separate ports and data directories merely allow both datasets to coexist.

## Methodology

The Rust generator serializes each record once into one NDJSON body and sends
the exact same bytes to all three backends concurrently (`/_multi` on O2,
`JSONEachRow` on ClickHouse). Query latency is then measured with exactly one
backend process running at a time, on the same machine and frozen dataset.

Each query is run five times by default:

- sample 1 is **cold** only when the driver's passwordless `sudo` cache-drop
  succeeds; the result records the outcome instead of assuming success;
- samples 2–5 are **hot** after a successful cold first sample;
- backend result caches remain disabled on every request.

The static report provides Cold Run, Hot Run, storage, and combined views. The
Markdown summary reports medians and p50/p95/p99; use the interactive report to
inspect cold and hot states separately.

## Repository layout

| Path | Purpose |
| --- | --- |
| [`datagen/`](datagen/) | Rust generator and three-way fan-out ingest |
| [`schemas/clickhouse.sql`](schemas/clickhouse.sql) | ClickHouse table and indexes |
| [`queries/queries.json`](queries/queries.json) | Equivalent ClickHouse and O2 query templates |
| [`scripts/start-clickhouse.sh`](scripts/start-clickhouse.sh) | Local ClickHouse process |
| [`scripts/start-openobserve.sh`](scripts/start-openobserve.sh) | Starts either the Parquet or Vortex O2 instance |
| [`scripts/run-benchmark.py`](scripts/run-benchmark.py) | Isolated query runner and Markdown summary builder |
| [`scripts/build-report.py`](scripts/build-report.py) | Builds `data.generated.js` for the interactive report |
| [`scripts/measure-storage.sh`](scripts/measure-storage.sh) | Standalone three-way storage inspection |
| [`index.html`](index.html) | Interactive three-way result page |
| `results/` | Generated results; git-ignored |

ClickHouse uses bloom skip indexes on `trace_id`, `span_id`, and
`kubernetes_pod_name`, plus a `text` inverted index on `message`. Both O2
formats use Tantivy full-text/secondary indexes on the corresponding fields and
the same external bloom-pruning layer on `trace_id`, with the same `0.01` target
false-positive probability as ClickHouse. Embedded Parquet row-group bloom is
disabled so it does not become a Parquet-only advantage. ClickHouse is ordered
only by `_timestamp`, so no structured column gets a sort-key advantage.

## Install

Both native binaries live under the git-ignored `bin/` directory. The install
script pins ClickHouse `26.7.3.19-stable` and OpenObserve `v0.92.2`; an existing
binary is reused only when its version matches the pin.

```bash
scripts/install.sh
# or install one binary:
scripts/install.sh clickhouse
scripts/install.sh openobserve
```

Pinned defaults:

| Backend | Version / format | Endpoint | Data directory |
| --- | --- | --- | --- |
| ClickHouse | `26.7.3.19-stable` | HTTP `127.0.0.1:8123`, native `:9000` | `clickhouse-data/` |
| o2-parquet | `v0.92.2`, Parquet | HTTP `127.0.0.1:5080`, gRPC `:5081` | `openobserve-parquet-data/` |
| o2-vortex | `v0.92.2`, Vortex | HTTP `127.0.0.1:5090`, gRPC `:5091` | `openobserve-vortex-data/` |

Copy the shared configuration before the first run:

```bash
cp .env.example .env
```

## Phase 1: ingest one identical dataset into all three backends

Start the three processes in separate terminals:

```bash
# Terminal A
scripts/start-clickhouse.sh

# Terminal B: ZO_FILE_FORMAT=parquet
scripts/start-openobserve.sh parquet

# Terminal C: ZO_FILE_FORMAT=vortex
scripts/start-openobserve.sh vortex
```

Check health and create the ClickHouse table:

```bash
curl http://127.0.0.1:8123/ping
curl http://127.0.0.1:5080/healthz
curl http://127.0.0.1:5090/healthz
./bin/clickhouse client --host 127.0.0.1 \
  --queries-file schemas/clickhouse.sql
```

Build the generator and fan out the same batches to all three targets. `all` is
the default target and is written explicitly here for clarity:

```bash
cd datagen
cargo build --release
START_TIMESTAMP_US="$(python3 -c 'import time; print((int(time.time()) // 3600 - 2) * 3600 * 1_000_000)')"
cd ..
./datagen/target/release/benchmark-data \
  --target all \
  --total 1000000000 \
  --start-timestamp-us "$START_TIMESTAMP_US" \
  --o2-parquet-url http://127.0.0.1:5080 \
  --o2-vortex-url http://127.0.0.1:5090 \
  --ch-url http://127.0.0.1:8123 \
  --stats-out ./results/ingest.json
```

`records_sent` advances only when every enabled backend accepted a batch. The
stats file also has separate `clickhouse_failed`, `o2_parquet_failed`, and
`o2_vortex_failed` counters.

Confirm the row count is identical before measuring:

```bash
./bin/clickhouse client --host 127.0.0.1 \
  -q 'SELECT count(*) FROM k8s_logs'

sqlite3 openobserve-parquet-data/db/metadata.sqlite \
  "SELECT sum(records) FROM file_list WHERE stream='default/logs/k8s_logs';"

sqlite3 openobserve-vortex-data/db/metadata.sqlite \
  "SELECT sum(records) FROM file_list WHERE stream='default/logs/k8s_logs';"
```

The generated start time is the beginning of the hour two hours ago. It remains
inside O2's normal ingest-age window while belonging to an already closed hour,
so O2 can complete full compaction and build external `trace_id` bloom files
without changing its normal ingest-age policy.
It also sets `ZO_COMPACT_DELETE_FILES_DELAY_MINUTES=10`, reducing O2's v0.92.2
default delayed-deletion window from 120 minutes to 10 minutes. Compacted input
files become eligible for physical deletion after that delay, limiting peak
disk usage during the billion-row run.
Wait for both O2 instances to compact the WAL tail into data files and produce
their `.ttv` and `.bf` files, then stop all three processes. Do not begin query
measurements while ingest or compaction is still active.

For the closed-hour benchmark data, trigger a short-lived finalize pass only
after ingest completes. Restart each O2 process with these overrides, wait
until every active `file_list` row has `bloom_ver > 0`, then stop it again:

```bash
ZO_COMPACT_OLD_DATA_INTERVAL=10 \
ZO_COMPACT_OLD_DATA_MIN_FILES=1 \
scripts/start-openobserve.sh parquet
```

Repeat for `vortex`. Do not enable this aggressive historical scan during the
timed ingest; it would add merge/write amplification while late data arrives.

## Phase 2: benchmark each backend in isolation

Run one server at a time. Stop each process before starting the next one; this
prevents CPU, memory, disk, and page-cache contention from contaminating the
comparison.

```bash
# 1. ClickHouse only
scripts/start-clickhouse.sh
# in another terminal:
python3 scripts/run-benchmark.py \
  --target clickhouse --runs 5 \
  --ch-url http://127.0.0.1:8123
# stop ClickHouse

# 2. O2-Parquet only
scripts/start-openobserve.sh parquet
# in another terminal:
python3 scripts/run-benchmark.py \
  --target o2-parquet --runs 5 \
  --o2-parquet-url http://127.0.0.1:5080 \
  --o2-parquet-data-dir ./openobserve-parquet-data
# stop O2-Parquet

# 3. O2-Vortex only
scripts/start-openobserve.sh vortex
# in another terminal:
python3 scripts/run-benchmark.py \
  --target o2-vortex --runs 5 \
  --o2-vortex-url http://127.0.0.1:5090 \
  --o2-vortex-data-dir ./openobserve-vortex-data
# stop O2-Vortex
```

The cache drop needs elevated OS permission: macOS uses `sudo purge`; Linux
uses `sync` followed by `/proc/sys/vm/drop_caches`.

Each isolated run writes its own durable result and rebuilds the merged summary:

```text
results/clickhouse.json
results/o2-parquet.json
results/o2-vortex.json
results/machine.json
results/ingest.json
results/summary.md
```

## Build and view the interactive report

```bash
python3 scripts/build-report.py
python3 -m http.server
```

Open `http://127.0.0.1:8000/index.html`. The report discovers the three result
files in the stable order `clickhouse`, `o2-parquet`, `o2-vortex`.

The checked-in `data.generated.js` retains the historical ClickHouse and
Parquet measurements. It deliberately does not invent a Vortex series; after a
real three-way run, `build-report.py` replaces it with all three measured
systems.

For a standalone storage check:

```bash
scripts/measure-storage.sh \
  --o2-parquet-data-dir ./openobserve-parquet-data \
  --o2-vortex-data-dir ./openobserve-vortex-data
```

## Fairness checklist

- Use the same OpenObserve binary for Parquet and Vortex.
- Keep all shared O2 settings identical; only `ZO_FILE_FORMAT` should differ.
- Ingest once with `--target all`; do not generate three independent datasets.
- Verify all three row counts before benchmarking.
- Wait for O2 WAL compaction to finish.
- Freeze ingestion and run only one query backend at a time.
- Record the same machine, query templates, run count, and cache state.
- Compare matched-run cold samples and hot medians; do not select the fastest
  individual sample as the headline result.

To start a completely fresh comparison, remove the generated `results/*.json`
and use new empty data directories for all three backends.
