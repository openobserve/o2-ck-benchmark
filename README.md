# OpenObserve vs ClickHouse — Observability Benchmark

A reproducible benchmark comparing [OpenObserve](https://openobserve.ai/) and
[ClickHouse](https://clickhouse.com/) as backends for **observability data**
(logs, metrics, and traces).

## Goal

Evaluate how each system performs under realistic observability workloads and
help decide which backend fits a given use case. We measure both systems on the
same hardware, the same datasets, and equivalent queries.

## Methodology — cold vs hot, measured side by side

We report **both** a cold and a hot number per query, ClickBench-style. The driver
([`scripts/run-benchmark.py`](scripts/run-benchmark.py)) drops the OS page cache
**once**, then runs the query five times — so sample 1 is **cold** (nothing in RAM)
and 2–5 are **hot** (data now page-cached). [`index.html`](index.html) has
**Cold Run** / **Hot Run** toggles; the default blends them (`0.25·cold + 0.75·hot`).

- **Cold** = a new search over data not in RAM — what incident investigation hits.
- **Hot** = steady state once page-cached. ClickHouse gains a lot here (**~1 s →
  ~50 ms**, reading the prior run's data from RAM); OpenObserve moves much less. That
  spread is a useful signal, so we surface it rather than hide it.

Both states stay apples-to-apples: each engine's own *result* cache is off per
request, so we always measure search + IO, never a cached result. ClickHouse also
drops its mark / uncompressed / query-condition caches and disables the query and
filesystem caches; OpenObserve sets `use_cache: false`.

> `results/summary.md` reports median/p50 across all five runs, so it tracks the
> **hot** number for most queries — use the toggles on [`index.html`](index.html)
> to separate the two cleanly.

## Repository

| Path | What |
|------|------|
| [`datagen/`](datagen/) | Rust log generator — writes byte-identical batches to OpenObserve and/or ClickHouse ([details](datagen/README.md)) |
| [`schemas/clickhouse.sql`](schemas/clickhouse.sql) | ClickHouse table + indexes — `bloom_filter` on `trace_id`/`span_id`/`kubernetes_pod_name` and a full-text `text()` index on `message`, mirroring OpenObserve one-for-one. `ORDER BY (_timestamp)` only (time-ordered like OO; no structured column in the sort-key prefix, so neither engine gets a layout edge). |
| [`queries/queries.json`](queries/queries.json) | Two workloads. **Needle queries** (q0–q7) with fixed ids/tokens embedded directly in each SQL template, in each system's SQL dialect — each in two shapes: `count()` (pure index+scan) and `SELECT * ... ORDER BY _timestamp DESC LIMIT 100` (row-fetch UX). q0/q1/q7 are exact-match id/pod lookups through each engine's skip index; q2/q3/q5/q6 are text-token searches through each engine's FTS index (OpenObserve `match_all`, ClickHouse `hasAnyTokens`); q4 combines a structured filter with a trace_id lookup. **Aggregation queries** (q8–q10, `category: "aggregation"`): histogram, top-N, and filtered histogram — full-range `GROUP BY` work that exercises each engine's aggregation/scan path rather than its skip indexes, using each side's idiomatic histogram (CH `toStartOfHour`, OO `histogram()`). The report page groups these into their own section. |
| [`scripts/run-benchmark.py`](scripts/run-benchmark.py) | Benchmark driver: drops the page cache once, runs each query N× per backend (1 cold + N−1 hot), reports p50/p95/p99 |
| [`index.html`](index.html) + [`scripts/build-report.py`](scripts/build-report.py) | Interactive results page (ClickBench-style UI). `build-report.py` regenerates `data.generated.js` (and the query tooltips) from `results/*.json` — open `index.html` via any static server, e.g. `python3 -m http.server` |
| [`scripts/measure-storage.sh`](scripts/measure-storage.sh) | On-disk storage & index sizes for both backends |
| `results/` | Output reports (git-ignored) |

## Binaries & servers

Both are single-node, native binaries living in `bin/` (git-ignored). Install with
[`scripts/install.sh`](scripts/install.sh) (`scripts/install.sh clickhouse` /
`openobserve` for just one):

| | Binary | Version (tested) | Endpoint |
|---|---|---|---|
| ClickHouse  | `bin/clickhouse`  | 26.6.1       | HTTP `http://127.0.0.1:8123`, native `127.0.0.1:9000` |
| OpenObserve | `bin/openobserve` | OSS v0.91.0-rc2  | `http://localhost:5080` (login `root@example.com` / `Complexpass#123`) |

> `bin/clickhouse` is both the server and the client. OpenObserve OSS arm64 comes
> from `https://downloads.openobserve.ai/releases/openobserve/<ver>/openobserve-<ver>-darwin-arm64.tar.gz`.

### Start the servers

From the repo root, in **two separate terminals**. Data persists under the dirs
shown, so restarting does not require re-loading.

```bash
# Terminal A — ClickHouse  (state under ./clickhouse-data/)
scripts/start-clickhouse.sh

# Terminal B — OpenObserve (state under ./openobserve-data/)
#   start-openobserve.sh also sets ZO_FEATURE_FULLTEXT_EXTRA_FIELDS and
#   ZO_FEATURE_INDEX_EXTRA_FIELDS so ingest builds indexes matching ClickHouse.
scripts/start-openobserve.sh
```

Check they are up:

```bash
curl http://127.0.0.1:8123/ping     # -> Ok.
curl http://127.0.0.1:5080/healthz  # -> {"status":"ok"}
./bin/clickhouse client --host 127.0.0.1   # interactive SQL
```

Equivalent manual commands (no scripts):

```bash
./bin/clickhouse server -- --path=./clickhouse-data/ \
  --listen_host=127.0.0.1 --http_port=8123 --tcp_port=9000

ZO_ROOT_USER_EMAIL=root@example.com ZO_ROOT_USER_PASSWORD=Complexpass#123 \
ZO_DATA_DIR=./openobserve-data \
ZO_FEATURE_FULLTEXT_EXTRA_FIELDS=message \
ZO_FEATURE_INDEX_EXTRA_FIELDS=trace_id,span_id,kubernetes_pod_name \
  ./bin/openobserve
```

Stop: `Ctrl-C` in each terminal, or `pkill -f "clickhouse server"` /
`pkill -f bin/openobserve`.

> ⚠️ When **measuring query latency**, keep both servers running but avoid heavy
> concurrent load on the idle one — they share CPU and OS page cache.

## Workflow

The benchmark runs in **two phases**. Ingest with both servers up (one batch fed
to both, so the data is identical). Then **shut both down and benchmark each engine
alone** — never measure query latency with the other server running, or they
contend for CPU and OS page cache and the numbers are meaningless.

### Phase 1 — Ingest (both servers up)

```bash
cp .env.example .env

# Start both (separate terminals)
scripts/start-clickhouse.sh
scripts/start-openobserve.sh

# Create the ClickHouse table
./bin/clickhouse client --host 127.0.0.1 --queries-file schemas/clickhouse.sql

# Generate once, write the same batches to BOTH backends
# --stats-out records ingest throughput (rec/s, MB/s) for the final report.
cd datagen && cargo build --release
./target/release/benchmark-data --total 100000000 \
  --o2-url http://127.0.0.1:5080 \
  --ch-url http://127.0.0.1:8123 \
  --stats-out ../results/ingest.json                  # --target both (default)
cd ..

# Let OpenObserve finish compacting WAL -> parquet+index, then STOP BOTH servers
pkill -f "clickhouse server"; pkill -f bin/openobserve
```

With the default `--seed`, generated row content is deterministic across
machines for the same row count. `_timestamp` defaults to the ingest start time.

### Phase 2 — Benchmark each engine in isolation

Run one engine at a time. Fixed table names, `trace_id`, `span_id`, request id,
pod, token values, and the ClickHouse `_timestamp` window
(`2026-06-01T00:00:00Z` to `2027-06-01T00:00:00Z`) are embedded directly in
[`queries/queries.json`](queries/queries.json), so every backend and machine runs
the same searches. Each run
writes `results/<backend>.json` and rebuilds the merged `results/summary.md`.

```bash
# --- Clean system cache ---
sync && echo 3 | tee /proc/sys/vm/drop_caches

# --- OpenObserve alone ---
scripts/start-openobserve.sh
python3 scripts/run-benchmark.py --target openobserve --runs 5 --o2-url http://127.0.0.1:5080
pkill -f bin/openobserve

# --- ClickHouse alone ---
scripts/start-clickhouse.sh
python3 scripts/run-benchmark.py --target clickhouse --runs 5 --ch-url http://127.0.0.1:8123
pkill -f "clickhouse server"

# --- Generate report ---
python3 scripts/build-report.py
```

The runner clears the local OS page cache once at the start of each query variant,
then runs that query five times by default — so the first sample is cold and the
rest are hot, and the report shows both (see
[Methodology — cold vs hot](#methodology--cold-vs-hot-measured-side-by-side)).
The cache drop is platform-aware and needs sudo: **macOS** uses `sudo purge`,
**Linux** uses `sync && echo 3 > /proc/sys/vm/drop_caches`.

Each `run-benchmark.py` run also collects that backend's storage/index sizes
(ClickHouse via `system.parts` while it is up; OpenObserve via its data dir), so
the combined report includes everything. `measure-storage.sh` remains as a
standalone inspector:

```bash
scripts/measure-storage.sh --oo-data-dir ./openobserve-data
```

### Output

[`results/summary.md`](results/) is the unified report — machine specs, ingest
throughput, storage & index size (index as % of raw JSON), and the latency
table + percentiles. Supporting files: `results/{openobserve,clickhouse}.json`
(per-backend raw), `machine.json`, and `ingest.json`.

To start a fresh comparison, delete `results/*.json`.
