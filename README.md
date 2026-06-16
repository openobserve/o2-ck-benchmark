# OpenObserve vs ClickHouse — Observability Benchmark

A reproducible benchmark comparing [OpenObserve](https://openobserve.ai/) and
[ClickHouse](https://clickhouse.com/) as backends for **observability data**
(logs, metrics, and traces).

## Goal

Evaluate how each system performs under realistic observability workloads and
help decide which backend fits a given use case. We measure both systems on the
same hardware, the same datasets, and equivalent queries.

## Methodology — why we measure cold cache

Every query is measured **cold**: before each run the runner drops the OS page
cache and bypasses each engine's internal caches, then runs the query five times.

This is the only fair way to compare the two. ClickHouse leans heavily on
caching: a query that takes **~1 s cold** drops to **~50 ms** when you repeat it
within a minute — and then climbs back to ~1 s once the cache ages out. That warm
50 ms isn't ClickHouse re-running the search; it's reading the previous result's
data straight out of RAM. It behaves almost like a result cache. OpenObserve, by
contrast, returns a **stable** number whether you run it once or ten times,
because what we report is its actual search runtime.

So if we let ClickHouse run warm, we'd be comparing *ClickHouse's cache* against
*OpenObserve's search engine* — which flatters ClickHouse and tells you nothing
about real query work. An analyst investigating an incident is almost always
running a **new** search over data that isn't already hot in RAM; cold is what
that feels like. We therefore force both engines cold so the numbers reflect
search work, not cache residency.

Concretely, per run the driver ([`scripts/run-benchmark.py`](scripts/run-benchmark.py)):

- drops the OS page cache (`sudo purge` on macOS; `sync` + `drop_caches` on Linux);
- **ClickHouse** — drops the mark / uncompressed / query-condition caches and
  disables the query cache and filesystem cache per request;
- **OpenObserve** — sets `use_cache: false` per request (its result cache).

If you want warm steady-state numbers too, run the same query repeatedly without
the cache drop and report it **separately** — never mix a warm engine against a
cold one.

## Repository

| Path | What |
|------|------|
| [`datagen/`](datagen/) | Rust log generator — writes byte-identical batches to OpenObserve and/or ClickHouse ([details](datagen/README.md)) |
| [`schemas/clickhouse.sql`](schemas/clickhouse.sql) | ClickHouse table + indexes — `bloom_filter` on `trace_id`/`span_id`/`kubernetes_pod_name` and a full-text `text()` index on `message`, mirroring OpenObserve one-for-one. `ORDER BY (_timestamp)` only (time-ordered like OO; no structured column in the sort-key prefix, so neither engine gets a layout edge). |
| [`queries/queries.json`](queries/queries.json) | Two workloads. **Needle queries** (q0–q7) with fixed ids/tokens embedded directly in each SQL template, in each system's SQL dialect — each in two shapes: `count()` (pure index+scan) and `SELECT * ... ORDER BY _timestamp DESC LIMIT 100` (row-fetch UX). q0/q1/q7 are exact-match id/pod lookups through each engine's skip index; q2/q3/q5/q6 are text-token searches through each engine's FTS index (OpenObserve `match_all`, ClickHouse `hasAnyTokens`); q4 combines a structured filter with a trace_id lookup. **Aggregation queries** (q8–q10, `category: "aggregation"`): histogram, top-N, and filtered histogram — full-range `GROUP BY` work that exercises each engine's aggregation/scan path rather than its skip indexes, using each side's idiomatic histogram (CH `toStartOfHour`, OO `histogram()`). The report page groups these into their own section. |
| [`scripts/run-benchmark.py`](scripts/run-benchmark.py) | Cold-cache driver: runs each query N× per backend, reports p50/p95/p99 |
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

The runner clears the local OS page cache at the start of each query variant,
then runs that query five times by default — a cold measurement (see
[Methodology — why we measure cold cache](#methodology--why-we-measure-cold-cache)).
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
