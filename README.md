# OpenObserve vs ClickHouse — Observability Benchmark

A reproducible, three-way benchmark of the same observability-log workload on:

1. **ClickHouse**
2. **OpenObserve with `ZO_FILE_FORMAT=parquet`** (`o2-parquet`)
3. **OpenObserve with `ZO_FILE_FORMAT=vortex`** (`o2-vortex`)

The two OpenObserve instances use the same binary, auth, indexes, query SQL, and
runtime settings. Their only storage-engine A/B variable is `ZO_FILE_FORMAT`;
separate ports and data directories merely allow both datasets to coexist.

**Results from the one-billion-record run: [RESULTS.md](RESULTS.md) /
[RESULTS.html](RESULTS.html).** The interactive per-query pages are
[`index.html`](index.html) (stock `ZO_COMPACT_MAX_FILE_SIZE`) and
[`index-v2.html`](index-v2.html) (raised to 10 GB).

## Architecture

The published results were produced on **four independent nodes**:

| Node | Role | Under test |
| --- | --- | --- |
| `clickhouse` | ClickHouse, data on local NVMe | yes |
| `o2-parquet` | OpenObserve, `ZO_FILE_FORMAT=parquet`, data on local NVMe | yes |
| `o2-vortex` | OpenObserve, `ZO_FILE_FORMAT=vortex`, data on local NVMe | yes |
| `driver` | runs `datagen` and `run-benchmark.py` | **no** |

One dedicated `i8g.2xlarge` per backend, and a `c7g.2xlarge` for the driver.
All four run concurrently; the three
backends never share CPU, memory, disk bandwidth or page cache with each other,
so storage is not a variable in the comparison. Only the **measurement** is
serialized — one backend is queried at a time, so no engine is answering
queries while another is being timed.

The driver is deliberately not a machine under test. A driver co-resident with
a backend would spend CPU on JSON serialization and result parsing on the very
host it is timing, and would do so unequally across three engines that return
different payload sizes. Because both engines report their own server-side time
(ClickHouse `statistics.elapsed`, OpenObserve `took`), the network hop between
driver and backend is not charged to either engine — the two choices only work
together.

> A single-machine run also works and is useful for smoke tests at small record
> counts. It is **not** how the published numbers were produced: on one box the
> three backends contend for the same resources, so each must be stopped before
> the next is measured. See [Single-machine variant](#single-machine-variant).

## Methodology

The Rust generator serializes each record once into one NDJSON body and sends
the exact same bytes to all three backends concurrently (`/_multi` on O2,
`JSONEachRow` on ClickHouse). A batch counts as sent only when every backend
accepted it. Query latency is then measured against a frozen dataset, one
backend at a time.

Each query is run five times by default:

- sample 1 is **cold** — the OS page cache is dropped on the backend node
  itself before its measurement pass begins;
- samples 2–5 are **hot**;
- backend result caches remain disabled on every request.

> **Required manual step on a multi-node run.** `drop_os_page_cache()` in
> `run-benchmark.py` drops the page cache of *the host it runs on*; its
> docstring assumes driver and server share a host, which is true only in the
> single-machine variant. On the multi-node layout it clears the **driver's**
> cache, does nothing to the backend under test, and still reports success — so
> the summary reads `os_page_cache_dropped: true` either way. Drop the cache on
> each backend node yourself, immediately before measuring that backend:
>
> ```bash
> # on the backend node, right before its measurement pass
> sync
> sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
> ```
>
> Then pass `--cache-dropped-on-backend` so the run is recorded as cold. The
> runner resolves each backend URL and refuses to claim a cold run 1 for a
> backend that is not on its own host — without the flag it prints a warning
> and records `cold_state: remote-backend`, which the report reads as warm.
>
> This is what the published run did, on all three backend nodes.

The static report provides Cold Run, Hot Run, storage, and combined views. The
Markdown summary reports medians and p50/p95/p99; use the interactive report to
inspect cold and hot states separately.

## Repository layout

| Path | Purpose |
| --- | --- |
| [`datagen/`](datagen/) | Rust generator and three-way fan-out ingest |
| [`schemas/clickhouse.sql`](schemas/clickhouse.sql) | ClickHouse table and indexes |
| [`queries/queries.json`](queries/queries.json) | Equivalent ClickHouse and O2 query templates |
| [`scripts/start-clickhouse.sh`](scripts/start-clickhouse.sh) | Starts ClickHouse on its node |
| [`scripts/start-openobserve.sh`](scripts/start-openobserve.sh) | Starts either the Parquet or Vortex O2 instance on its node |
| [`scripts/run-benchmark.py`](scripts/run-benchmark.py) | Query runner and Markdown summary builder; runs on the driver |
| [`scripts/build-report.py`](scripts/build-report.py) | Builds `data.generated.js` for the interactive report |
| [`scripts/measure-storage.sh`](scripts/measure-storage.sh) | Standalone storage inspection; reads local data dirs, so run it on the backend nodes |
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

Install on each of the three backend nodes; the driver node needs only Python
and the generator.

Pinned defaults:

| Backend | Version / format | Listens on | Data directory |
| --- | --- | --- | --- |
| ClickHouse | `26.7.3.19-stable` | HTTP `:8123`, native `:9000` | `clickhouse-data/` |
| o2-parquet | `v0.92.2`, Parquet | HTTP `:5080`, gRPC `:5081` | `openobserve-parquet-data/` |
| o2-vortex | `v0.92.2`, Vortex | HTTP `:5090`, gRPC `:5091` | `openobserve-vortex-data/` |

The two OpenObserve port sets differ so both instances can coexist on one host
in the single-machine variant. On dedicated nodes the ports are free to be
identical; keep them distinct anyway so the same `.env` works in both layouts.

Copy the shared configuration before the first run, on every backend node:

```bash
cp .env.example .env
```

The rest of this guide uses these placeholders for the backend node addresses,
as seen from the driver:

```bash
CH_URL=http://clickhouse-node:8123
O2P_URL=http://o2-parquet-node:5080
O2V_URL=http://o2-vortex-node:5090
```

## Phase 1: ingest one identical dataset into all three backends

Start one process on each backend node:

```bash
# on the clickhouse node
scripts/start-clickhouse.sh

# on the o2-parquet node
scripts/start-openobserve.sh parquet

# on the o2-vortex node
scripts/start-openobserve.sh vortex
```

All three stay up for the whole benchmark — ingest and both measurement rounds.
Nothing is stopped between backends; they are on separate hosts.

From the driver, check health and create the ClickHouse table:

```bash
curl "$CH_URL/ping"
curl "$O2P_URL/healthz"
curl "$O2V_URL/healthz"
./bin/clickhouse client --host clickhouse-node \
  --queries-file schemas/clickhouse.sql
```

Build the generator and fan out the same batches to all three targets. `all` is
the default target and is written explicitly here for clarity:

```bash
cd datagen
cargo build --release
START_TIMESTAMP_US="$(python3 -c 'import time; print((int(time.time()) // 3600 - 1) * 3600 * 1_000_000)')"
cd ..
mkdir -p ./results/
./datagen/target/release/benchmark-data \
  --target all \
  --total 1000000000 \
  --start-timestamp-us "$START_TIMESTAMP_US" \
  --o2-parquet-url "$O2P_URL" \
  --o2-vortex-url "$O2V_URL" \
  --ch-url "$CH_URL" \
  --stats-out ./results/ingest.json
```

`records_sent` advances only when every enabled backend accepted a batch. The
stats file also has separate `clickhouse_failed`, `o2_parquet_failed`, and
`o2_vortex_failed` counters.

Confirm the row count is identical before measuring. The first command runs
from the driver; the two `sqlite3` commands read each OpenObserve node's local
metastore, so run them **on those nodes**:

```bash
# from the driver
./bin/clickhouse client --host clickhouse-node \
  -q 'SELECT count(*) FROM k8s_logs'

# on the o2-parquet node
sqlite3 openobserve-parquet-data/db/metadata.sqlite \
  "SELECT sum(records) FROM file_list WHERE stream='default/logs/k8s_logs';"

# on the o2-vortex node
sqlite3 openobserve-vortex-data/db/metadata.sqlite \
  "SELECT sum(records) FROM file_list WHERE stream='default/logs/k8s_logs';"
```

All three must report the same number before any latency is worth recording.
`run-benchmark.py` repeats this check across backends and marks the summary
`MISMATCH — results are not comparable` if they disagree.

The timestamp anchor is the beginning of the previous hour. Every later
batch adds the generator's actual monotonic elapsed time to that anchor. A
six-hour ingest therefore produces roughly six hours of event time while
remaining at a constant one-to-two-hour lag behind the server, inside O2's
normal five-hour ingest window. Completed hours can be fully compacted and get
their external `trace_id` bloom files without changing the ingest-age policy.
Rows inside one batch remain one microsecond apart, and the same byte-identical
NDJSON body is sent to all three backends.
It also sets `ZO_COMPACT_DELETE_FILES_DELAY_MINUTES=10`, reducing O2's v0.92.2
default delayed-deletion window from 120 minutes to 10 minutes. Compacted input
files become eligible for physical deletion after that delay, limiting peak
disk usage during the billion-row run.
Wait for both O2 instances to compact the WAL tail into data files and produce
their `.ttv` and `.bf` files. Do not begin query measurements while ingest or
compaction is still active — the dataset must be frozen, or a run repeated later
measures different bytes. The backend processes themselves stay up; on separate
nodes there is no reason to stop them.

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

## Phase 2: measure one backend at a time

All three backends stay running on their own nodes. Run these three commands
**sequentially from the driver** — never in parallel. Serializing the
measurement is what keeps one engine from answering queries while another is
being timed:

Before each command, drop the page cache **on that backend's node**
(`sync; sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'`), then:

```bash
# 1. ClickHouse
python3 scripts/run-benchmark.py \
  --target clickhouse --runs 5 \
  --ch-url "$CH_URL" --cache-dropped-on-backend

# 2. O2-Parquet
python3 scripts/run-benchmark.py \
  --target o2-parquet --runs 5 \
  --o2-parquet-url "$O2P_URL" --cache-dropped-on-backend

# 3. O2-Vortex
python3 scripts/run-benchmark.py \
  --target o2-vortex --runs 5 \
  --o2-vortex-url "$O2V_URL" --cache-dropped-on-backend
```

Each result file records `backend_is_local`, `cold_state` and `backend_url`, so
the layout and the basis for the cold sample are auditable after the fact rather
than assumed.

`--o2-*-data-dir` is omitted on purpose. It is a local-filesystem fallback for
storage measurement, used only when OpenObserve's stream-stats API reports zero
rows — and the data directory does not exist on the driver. Pass it only in the
single-machine variant.

The cache drop needs elevated OS permission: macOS uses `sudo purge`; Linux

```bash
sync
sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
```

`run-benchmark.py` issues this on **its own host** — the driver on a multi-node
run, where it does nothing for the backend under test. Run it on the backend
node yourself instead; see [Methodology](#methodology).

Each run writes its own durable result and rebuilds the merged summary. The
result files land on the driver, which is where all three runs happen:

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

For a standalone storage check. This one reads data directories off the local
filesystem, so run it on the OpenObserve nodes rather than the driver:

```bash
scripts/measure-storage.sh \
  --o2-parquet-data-dir ./openobserve-parquet-data \
  --o2-vortex-data-dir ./openobserve-vortex-data
```

## Fairness checklist

- Use the same OpenObserve binary for Parquet and Vortex.
- Keep all shared O2 settings identical; only `ZO_FILE_FORMAT` should differ.
- Give every backend the same instance type and the same storage class; a
  faster disk under one engine invalidates the whole comparison.
- Keep the driver off the backend nodes, and rely on server-reported time
  (`statistics.elapsed`, `took`) so the network hop is charged to neither side.
- Ingest once with `--target all`; do not generate three independent datasets.
- Verify all three row counts before benchmarking.
- Wait for O2 WAL compaction to finish.
- Freeze ingestion, then measure one backend at a time — never in parallel.
- Record the node type, query templates, run count, and cache state, and say
  whether the layout was multi-node or single-machine.
- Compare matched-run cold samples and hot medians; do not select the
  fastest individual sample as the headline result.

To start a completely fresh comparison, remove the generated `results/*.json`
on the driver and use new empty data directories on all three backend nodes.

## Single-machine variant

Everything above also runs on one box, which is useful for smoke tests at small
record counts. **This is not how the published numbers were produced**, and the
difference is not cosmetic: on one host the three backends share CPU, memory,
disk bandwidth and page cache, so each must be stopped before the next is
measured, and the run takes three sequential passes instead of three sequential
query batches.

The changes to the flow above:

- point `CH_URL` / `O2P_URL` / `O2V_URL` at `127.0.0.1`; the distinct
  OpenObserve port pairs (`5080`/`5081` and `5090`/`5091`) exist for exactly
  this case;
- start all three for the ingest, then stop all three;
- for each backend in turn: start it, run `run-benchmark.py` against it, stop it
  before starting the next;
- pass `--o2-parquet-data-dir` / `--o2-vortex-data-dir` so the local-filesystem
  storage fallback can find the data directories;
- the page-cache drop now works unattended, because the driver and the server
  really do share a host — omit `--cache-dropped-on-backend` and let the runner
  do it (it detects the local endpoint automatically).

Anything published from a single-machine run should say so — the isolation
argument is different, and so is what "cold" means.
