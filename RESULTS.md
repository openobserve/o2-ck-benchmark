# Results

Measured with the scripts in this repo: **one billion log records**, 2 TiB of
raw NDJSON, written byte-identically into ClickHouse and two OpenObserve
instances — each on its own dedicated node — then queried one backend at a time
against a frozen dataset.

**All latencies are milliseconds.** Bold is the fastest system in that row.

The benchmark was run twice against the same frozen dataset, changing exactly
one thing: OpenObserve's compaction target file size, `ZO_COMPACT_MAX_FILE_SIZE`,
from its **2 GB** default to **10 GB**. ClickHouse's configuration is identical
in both rounds and serves as the control.

## Conditions

| | |
| --- | --- |
| Hardware | one dedicated `i8g.2xlarge` per backend, local NVMe, all running concurrently |
| Driver | a fourth node not under test, `c7g.2xlarge`, runs the generator and the benchmark client |
| ClickHouse | `26.7.3.19-stable`, `MergeTree ORDER BY (_timestamp)` |
| OpenObserve | `v0.92.2`, two instances differing only in `ZO_FILE_FORMAT` (parquet / vortex) |
| Dataset | 1,000,000,000 records, 2,199 GB raw NDJSON, spanning ~4h13m of event time |
| Ingestion | one fan-out pass, 16,666 s (4h37m) wall clock, 60,002 rec/s, 128.1 MB/s, then **stopped** |
| Query window | 2026-06-01 → 2027-06-01 UTC, pinned absolutely, wider than the data |
| Queries | 19: 8 indexed `count()`, 3 full-scan aggregations, 8 `SELECT * … LIMIT 100` |
| Runs | 5 per query; OS page cache dropped on each backend node before its pass — run 1 cold, runs 2–5 warm |
| Caches | ClickHouse filesystem cache off; OpenObserve result cache off (`use_cache=false`) |
| Isolation | separate nodes, so no backend shares hardware with another; measured one at a time |
| Timing | server-reported on both sides — ClickHouse `statistics.elapsed`, OpenObserve `took` |

**Cold** below means run 1, taken after the OS page cache was dropped on the
backend node itself — `sync; echo 3 > /proc/sys/vm/drop_caches`, run on each of
the three nodes immediately before its measurement pass. The dataset is 674 GB
to 1 TB against 64 GiB of RAM, so the cache can never hold more than a small
fraction of it in any case. **Hot** means `min(run 2, run 3)`, the convention
the interactive report ([`index.html`](index.html)) uses.

### How the measurement is taken

Five choices move these numbers more than any setting in the table above.

**One dataset, not three.** The Rust generator serializes each record once and
sends the same bytes to all three backends concurrently — `/_multi` on
OpenObserve, `JSONEachRow` on ClickHouse. A batch counts as sent only when every
backend accepted it. Three independently generated datasets would not be
comparable.

**One backend per node, one measurement at a time.** Each backend has a
dedicated `i8g.2xlarge` with its data on that node's local NVMe, so CPU, memory,
disk bandwidth and page cache are never shared between them — storage is not a
variable in this comparison. The three nodes run concurrently; only the
measurement is serialized, one backend at a time, so no engine is answering
queries while another is being timed.

**Nothing under test also drives the test.** The generator and the benchmark
client run on a fourth node, a `c7g.2xlarge`. A driver co-resident with a backend would spend its
CPU on JSON serialization and result parsing on the same box it is timing, and
would do so unequally across three engines that return different payload sizes.
Keeping it off the measured nodes costs one instance and removes the whole
class of problem. Because timing is server-reported on both sides, the network
hop between driver and backend is not charged to either engine — the two choices
only work together.

**Both sides report their own time.** ClickHouse's `statistics.elapsed` and
OpenObserve's `took` are both server-side, so neither engine is charged for HTTP
or client overhead the other avoids. OpenObserve reports `took` in whole
milliseconds; below ~30 ms that quantization is a real part of the spread, and
differences of two or three milliseconds in the tables below should be read as
a tie.

**Index parity is deliberate, and it is the hard part of this comparison.**
Both engines get a full-text index on `message` and skip/secondary indexes on
the same three high-cardinality columns:

| Column | ClickHouse | OpenObserve (both formats) |
| --- | --- | --- |
| `message` | `text(tokenizer = splitByNonAlpha)` inverted index | Tantivy FTS (`ZO_FEATURE_FULLTEXT_EXTRA_FIELDS`) |
| `trace_id` | `bloom_filter(0.01)` | Tantivy secondary index + external bloom, `ZO_BLOOM_FILTER_FPP=0.01` |
| `span_id` | `bloom_filter(0.01)` | Tantivy secondary index |
| `kubernetes_pod_name` | `bloom_filter(0.01)` | Tantivy secondary index |
| `kubernetes_container_name` | **none** | **none** |

`kubernetes_container_name` is deliberately left unindexed on both sides — it has
8 distinct values, so an index prunes nothing, and indexing it on one side only
would be a gift. Parquet's embedded row-group bloom filter is disabled
(`ZO_BLOOM_FILTER_PARQUET_ENABLED=false`) so it cannot become a Parquet-only
advantage over Vortex. ClickHouse is ordered by `_timestamp` alone, so no
structured column gets a sort-key advantage either.

### What this benchmark does not measure

- **Ingestion throughput per engine.** The 16,666 s figure is one fan-out pass
  feeding all three backends at once, gated by the slowest of them plus the
  generator. It is not three ingest benchmarks.
- **Time-range pruning.** The query window is a full year around a 4h13m
  dataset, so every query touches all of it. That is the worst case for both
  engines and identical for both, but it means nothing here rewards partition
  pruning.
- **Concurrency.** Every query is issued serially against an otherwise idle box.
- **Per-query row counts.** The harness reads `rows_read` (ClickHouse) and
  `scan_records` (OpenObserve) on every request but does not persist them, so
  the recorded results carry latency without the work behind it. The row check
  that *is* recorded is at storage level — `system.parts` against `file_list` —
  and confirms all three backends hold the same 1,000,000,000 rows. Persisting
  the per-query counts is the first thing to add before the next run.

## Storage

Measured from each engine's own metastore — `system.parts` plus
`system.data_skipping_indices` for ClickHouse, the stream-stats API (backed by
`file_list`) for OpenObserve. Both figures are compressed data **plus** indexes.

| System | On disk | vs raw | vs ClickHouse |
| --- | ---: | ---: | ---: |
| ClickHouse | 1,026.5 GB | 2.14× | — |
| O2 · Parquet | **673.5 GB** | **3.26×** | **0.66×** |
| O2 · Vortex | 710.7 GB | 3.09× | 0.69× |

Raw input is 2,199 GB of NDJSON. OpenObserve stores the same billion records in
**a third less space than ClickHouse** while carrying a Tantivy full-text index
that ClickHouse's `text()` index is the counterpart to. Raising the compaction
target to 10 GB grew both OpenObserve footprints by 0.49% — larger merge units
produce marginally larger output, and it is noise next to the query effect.

## Round 1 · stock configuration

`ZO_COMPACT_MAX_FILE_SIZE` at its 2 GB default. Hot medians, ms.

### Indexed `count()`

| Query | ClickHouse | O2 · Parquet | O2 · Vortex |
| --- | ---: | ---: | ---: |
| Single `trace_id` lookup | **86** | 97 | 99 |
| Single `span_id` lookup | **79** | 96 | 94 |
| Rare token in `message` | **23** | 96 | 95 |
| Common token in `message` | 1,858 | 96 | **95** |
| Container + `trace_id` | 91 | **31** | 36 |
| Container + rare token | **28** | 97 | 104 |
| Two tokens (common AND rare) | **25** | 132 | 133 |
| High-cardinality `pod_name` | 2,916 | **100** | 103 |

### Full-scan aggregation

| Query | ClickHouse | O2 · Parquet | O2 · Vortex |
| --- | ---: | ---: | ---: |
| Histogram, 1h buckets | 1,584 | **169** | 172 |
| Top-N namespaces | 1,624 | 1,607 | **1,371** |
| Filtered histogram (token) | 2,233 | 977 | **974** |

### `SELECT * … ORDER BY _timestamp DESC LIMIT 100`

| Query | ClickHouse | O2 · Parquet | O2 · Vortex |
| --- | ---: | ---: | ---: |
| `trace_id` → 100 rows | 186 | 102 | **63** |
| `span_id` → 100 rows | 162 | 141 | **135** |
| Rare token → 100 rows | **85** | 142 | 135 |
| Common token → 100 rows | **22** | 68 | 50 |
| Container + `trace_id` → 100 rows | 186 | 77 | **51** |
| Container + rare token → 100 rows | **105** | 128 | 109 |
| Two tokens → 100 rows | **91** | 186 | 173 |
| `pod_name` → 100 rows | 192 | 321 | **114** |

Totalled across all 19 queries: ClickHouse 11,578 ms, Parquet 4,663 ms, Vortex
4,106 ms. OpenObserve is 2.5–2.8× faster in aggregate, but it wins only
**9 of 19** queries (Parquet) and 10 of 19 (Vortex) — the aggregate is carried
by two queries where ClickHouse takes seconds.

### The 96 ms floor

The most informative number in round 1 is one that barely varies:

| Round 1, Parquet | `trace_id` | `span_id` | rare token | common token | container + rare | `pod_name` |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| hot, ms | 97 | 96 | 96 | 96 | 97 | 100 |

Six queries with completely different selectivity, hitting three different index
types — a bloom-backed ID lookup, a full-text term that matches almost nothing,
a full-text term that matches a large fraction of the dataset, a secondary index
on a high-cardinality column — and they all answer in 96–100 ms. That is not six
query costs. That is **one fixed cost, paid before any of them start**, and the
actual query work disappearing underneath it.

The exception is the compound `container + trace_id` query at 31 ms, which was
already below the floor.

## Round 2 · `ZO_COMPACT_MAX_FILE_SIZE=10240`

Same dataset, same queries, same machine, ClickHouse untouched. OpenObserve's
compaction target file size raised from 2 GB to 10 GB — roughly a fifth as many
data files, each five times larger.

### The control

ClickHouse was re-measured in round 2 with an unchanged configuration, so its
drift is the run-to-run noise floor of the whole harness:

| | |
| --- | --- |
| Total across 19 queries | 11,578 ms → 11,694 ms (**+1.0%**) |
| Median per-query drift | 1.2% |
| Largest per-query drift | 17.5%, on a 22 ms query |

Every OpenObserve change below is far outside that band.

### Effect on OpenObserve

Hot medians, ms. **Δ** is round 1 ÷ round 2 — above 1.00× is faster after the
change.

| Query | Parquet 2 GB | Parquet 10 GB | Δ | Vortex 2 GB | Vortex 10 GB | Δ |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Single `trace_id` lookup | 97 | 26 | **3.73×** | 99 | 27 | **3.67×** |
| Single `span_id` lookup | 96 | 25 | **3.84×** | 94 | 26 | **3.62×** |
| Rare token in `message` | 96 | 24 | **4.00×** | 95 | 24 | **3.96×** |
| Common token in `message` | 96 | 25 | **3.84×** | 95 | 27 | **3.52×** |
| Container + `trace_id` | 31 | 19 | **1.63×** | 36 | 24 | **1.50×** |
| Container + rare token | 97 | 28 | **3.46×** | 104 | 32 | **3.25×** |
| Two tokens (common AND rare) | 132 | 41 | **3.22×** | 133 | 45 | **2.96×** |
| High-cardinality `pod_name` | 100 | 27 | **3.70×** | 103 | 27 | **3.81×** |
| Histogram, 1h buckets | 169 | 63 | **2.68×** | 172 | 63 | **2.73×** |
| Top-N namespaces | 1,607 | 1,500 | 1.07× | 1,371 | 1,204 | 1.14× |
| Filtered histogram (token) | 977 | 595 | **1.64×** | 974 | 598 | **1.63×** |
| `trace_id` → 100 rows | 102 | 77 | **1.32×** | 63 | 44 | **1.43×** |
| `span_id` → 100 rows | 141 | 73 | **1.93×** | 135 | 53 | **2.55×** |
| Rare token → 100 rows | 142 | 66 | **2.15×** | 135 | 52 | **2.60×** |
| Common token → 100 rows | 68 | 52 | **1.31×** | 50 | 47 | 1.06× |
| Container + `trace_id` → 100 rows | 77 | 56 | **1.38×** | 51 | 32 | **1.59×** |
| Container + rare token → 100 rows | 128 | 55 | **2.33×** | 109 | 38 | **2.87×** |
| Two tokens → 100 rows | 186 | 81 | **2.30×** | 173 | 67 | **2.58×** |
| `pod_name` → 100 rows | 321 | **654** | **0.49×** | 114 | 103 | 1.11× |

| | Parquet | Vortex |
| --- | ---: | ---: |
| Total, 19 queries | 4,663 → 3,487 ms | 4,106 → 2,533 ms |
| Geometric mean per query | 141.6 → 66.3 ms | 124.3 → 54.3 ms |
| Geometric-mean speedup | **2.14×** | **2.29×** |
| Queries improved | 18 of 19 | 19 of 19 |

**The floor collapsed.** The six queries pinned at 96–100 ms in round 1 land at
24–28 ms in round 2 — a ~3.8× drop, close to the ~5× reduction in file count.
That is the signature of a per-file fixed cost: opening files, reading their
metadata, loading their index segments and merging their results. It scales with
how many files a query touches, not with how much data it reads, which is why it
was identical across six queries doing entirely different work. Fewer, larger
files pay it fewer times.

It also explains the shape of the rest of the table. Queries that were already
dominated by real work move least: **top-N namespaces** — a full scan and group
by over a billion rows, where per-file overhead is a rounding error — improves
only 1.07× on Parquet and 1.14× on Vortex.

### The one regression

`SELECT * FROM k8s_logs WHERE kubernetes_pod_name = '…' ORDER BY _timestamp DESC LIMIT 100`
got **2.0× slower on Parquet** — 321 ms → 654 ms hot, and 395 ms → 857 ms cold.

It is the only regression in 38 measurements, and it is worth being precise
about what the data supports:

- It is **Parquet-specific**. The same query, on the same data, with the same
  10 GB cap, got *faster* on Vortex (114 → 103 ms). So the cause is in the read
  path, not in compaction itself.
- It is **row fetch specific**. The `pod_name` `count()` query, which uses the
  same secondary index over the same rows, improved 3.70×. Only materializing
  the rows regressed.
- It shows up **cold and hot alike**, at a similar ratio, so it is not a cache
  artifact.

The likely mechanism — and this is a hypothesis, not something this run
profiled — is that a top-K row fetch has to materialize candidate rows out of
whichever files the index points at, and a 10 GB Parquet file makes the unit of
that work five times larger: a footer five times bigger to deserialize, five
times the row groups to seek through, and larger column chunks to decompress,
all to return 100 rows. Vortex's layout is lazily addressable, so the same
lookup does not pay in proportion to file size.

The practical reading: **if your workload is dominated by
`SELECT * … LIMIT n` on a secondary-indexed high-cardinality column and you run
Parquet, measure before raising this knob.** On Vortex the change is a straight
win.

## Round 2 · head to head

Hot medians, ms, all three systems at their round-2 settings.

### Indexed `count()`

| Query | ClickHouse | O2 · Parquet | O2 · Vortex |
| --- | ---: | ---: | ---: |
| Single `trace_id` lookup | 87 | **26** | 27 |
| Single `span_id` lookup | 80 | **25** | 26 |
| Rare token in `message` | **24** | 24 | 24 |
| Common token in `message` | 1,891 | **25** | 27 |
| Container + `trace_id` | 93 | **19** | 24 |
| Container + rare token | 29 | **28** | 32 |
| Two tokens (common AND rare) | **25** | 41 | 45 |
| High-cardinality `pod_name` | 2,949 | **27** | **27** |

### Full-scan aggregation

| Query | ClickHouse | O2 · Parquet | O2 · Vortex |
| --- | ---: | ---: | ---: |
| Histogram, 1h buckets | 1,591 | **63** | **63** |
| Top-N namespaces | 1,639 | 1,500 | **1,204** |
| Filtered histogram (token) | 2,248 | **595** | 598 |

### `SELECT * … ORDER BY _timestamp DESC LIMIT 100`

| Query | ClickHouse | O2 · Parquet | O2 · Vortex |
| --- | ---: | ---: | ---: |
| `trace_id` → 100 rows | 190 | 77 | **44** |
| `span_id` → 100 rows | 158 | 73 | **53** |
| Rare token → 100 rows | 86 | 66 | **52** |
| Common token → 100 rows | **26** | 52 | 47 |
| Container + `trace_id` → 100 rows | 187 | 56 | **32** |
| Container + rare token → 100 rows | 104 | 55 | **38** |
| Two tokens → 100 rows | 92 | 81 | **67** |
| `pod_name` → 100 rows | 195 | 654 | **103** |

### Aggregate

| | ClickHouse | O2 · Parquet | O2 · Vortex |
| --- | ---: | ---: | ---: |
| Total, 19 queries (hot) | 11,694 ms | 3,487 ms | **2,533 ms** |
| vs ClickHouse | — | 3.35× | **4.62×** |
| Geometric mean per query | 181.9 ms | 66.3 ms | **54.3 ms** |
| Per-query geometric-mean speedup | — | 2.74× | **3.35×** |
| Queries faster than ClickHouse | — | 15 of 19 | 15 of 19 |

ClickHouse is the fastest of the three on **3 of the 19**, one of which is a tie
at the millisecond resolution floor. The round-1 → round-2 change moves
OpenObserve from *winning on aggregate while losing most individual queries* to
**beating ClickHouse on 15 of 19**, with the geometric-mean advantage going from
1.26× to 2.74× (Parquet) and 1.44× to 3.35× (Vortex).

### The four rows OpenObserve does not take outright

| Query | ClickHouse | Parquet | Vortex | Why |
| --- | ---: | ---: | ---: | --- |
| Rare token in `message` | 24 | 24 | 24 | a tie at the measurement floor |
| Two tokens (common AND rare) | 25 | 41 | 45 | ClickHouse intersects two `hasAnyTokens` posting lists cheaply |
| Common token → 100 rows | 26 | 52 | 47 | `ORDER BY _timestamp DESC LIMIT 100` on the sort key, on a term that matches everywhere: ClickHouse reads the newest granules and stops |
| `pod_name` → 100 rows | 195 | 654 | **103** | Parquet only — see [the regression](#the-one-regression); Vortex wins this row |

Only two of those are outright ClickHouse wins over both formats; the rare-token
count is a tie, and Vortex takes the `pod_name` row fetch. There is a pattern in
them: ClickHouse wins where its `ORDER BY (_timestamp)` sort key lets it answer
a top-K query by reading the tail of the table, and where a selective term makes
the text index do almost all the work. It loses badly — 25×, 70×, 109× —
wherever a query has to scan or aggregate.

### The queries that decide the aggregate

| Query | ClickHouse | O2 · Vortex | Ratio |
| --- | ---: | ---: | ---: |
| High-cardinality `pod_name` `count()` | 2,949 | 27 | **109×** |
| Common token `count()` | 1,891 | 27 | **70×** |
| Histogram, 1h buckets | 1,591 | 63 | **25×** |
| Filtered histogram (token) | 2,248 | 598 | **3.8×** |

`kubernetes_pod_name` has a `bloom_filter(0.01)` skip index on the ClickHouse
side, and it still takes 2.9 seconds: a bloom filter can only reject granules,
and for a pod that appears throughout a billion rows very few granules can be
rejected. OpenObserve's Tantivy secondary index returns the matching row set
directly. The same asymmetry drives the common-token count: ClickHouse's
`text()` index finds the term everywhere and then counts, OpenObserve reads the
count out of the index.

## Parquet vs Vortex

The two OpenObserve instances share a binary, a dataset, an index
configuration and every environment variable except `ZO_FILE_FORMAT`.

| | Parquet | Vortex |
| --- | ---: | ---: |
| Total, 19 queries (round 2, hot) | 3,487 ms | **2,533 ms** |
| Storage | **673.5 GB** | 710.7 GB |
| `count()` group (8 queries) | **215 ms** | 232 ms |
| Aggregation group (3 queries) | 2,158 ms | **1,865 ms** |
| Row-fetch group (8 queries) | 1,114 ms | **436 ms** |

They are level on indexed `count()` — both are answering out of the same Tantivy
index, and the format barely participates. The gap is entirely in the two groups
that read data:

- **Row fetch: Vortex is 2.6× faster in aggregate**, and wins all eight
  individually. Materializing 100 wide rows out of a large file is exactly what
  its lazily addressable layout is for, and it is also why Vortex was immune to
  the regression that cost Parquet 2× on `pod_name` row fetch.
- **Aggregation: Vortex leads by 1.16×**, almost all of it from top-N
  namespaces (1,204 ms vs 1,500 ms).
- **Storage: Parquet is 5.5% smaller.**

For a log-search workload — find the matching lines, show me the lines — Vortex
is the better default, and the storage difference is not close to paying for it.

## The bottom line

- **On aggregate, OpenObserve answers this 19-query suite 3.4× (Parquet) to
  4.6× (Vortex) faster than ClickHouse**, on a third less disk, with a full-text
  index on both sides.
- **The gap is not uniform.** ClickHouse is competitive or better on small,
  highly selective queries that ride its `_timestamp` sort key, and 25–109×
  slower on anything that scans or aggregates.
- **`ZO_COMPACT_MAX_FILE_SIZE` is worth more than the format choice on most of
  this suite.** Raising it from 2 GB to 10 GB was a 2.1–2.3× geometric-mean
  improvement, and it is what turns a 9-of-19 result into a 15-of-19 result.
- **It has one sharp edge**: Parquet row fetch on a secondary-indexed
  high-cardinality column got 2× slower. Vortex did not.

### Check it yourself

**Nothing here is a screenshot of a number we ask you to trust.** The entire
benchmark — data generator, ClickHouse schema and index definitions, the
matched query templates for both engines, the isolated runner, the report
builder, and the raw per-run samples behind every figure in this document — is
in one public repository:

**<https://github.com/openobserve/openobserve-clickhouse-benchmark>**

That includes the parts that make a benchmark arguable rather than merely
quotable: which columns are indexed on each side and why
([`.env.example`](.env.example), [`schemas/clickhouse.sql`](schemas/clickhouse.sql)),
the exact SQL each engine was asked to run
([`queries/queries.json`](queries/queries.json)), and all five raw samples for
every query, engine and round ([`data.generated.js`](data.generated.js),
[`data.v2.js`](data.v2.js)) — so any table here can be recomputed, and any
choice we made can be changed and re-run.

If you reproduce it and get a different answer, that is a useful result; open an
issue with your numbers.

## Reproducing

Full instructions are in [README.md](README.md). In short:

```bash
# on each of the three backend nodes
scripts/install.sh && cp .env.example .env
scripts/start-clickhouse.sh          # clickhouse node
scripts/start-openobserve.sh parquet # o2-parquet node
scripts/start-openobserve.sh vortex  # o2-vortex node

# from the driver node
./bin/clickhouse client --host clickhouse-node --queries-file schemas/clickhouse.sql

# one fan-out ingest into all three, then stop it
./datagen/target/release/benchmark-data --target all --total 1000000000 \
  --start-timestamp-us "$START_TIMESTAMP_US" \
  --ch-url "$CH_URL" --o2-parquet-url "$O2P_URL" --o2-vortex-url "$O2V_URL" \
  --stats-out ./results/ingest.json

# on the backend node, immediately before its pass:
#   sync; sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
# then, sequentially from the driver — never in parallel:
python3 scripts/run-benchmark.py --target clickhouse --runs 5 --ch-url "$CH_URL"
python3 scripts/run-benchmark.py --target o2-parquet --runs 5 --o2-parquet-url "$O2P_URL"
python3 scripts/run-benchmark.py --target o2-vortex  --runs 5 --o2-vortex-url "$O2V_URL"

python3 scripts/build-report.py && python3 -m http.server
```

The page-cache drop has to happen **on the backend node**. `run-benchmark.py`
also attempts one, but it runs on the driver, where it does nothing for the
engine under test — and it still reports success.

`ZO_COMPACT_MAX_FILE_SIZE` is a compaction-time setting: it changes the files
compaction *produces*, so it does nothing to files that already exist. Either
set it in `.env` before ingesting, or force a recompaction pass over the
existing data (`ZO_COMPACT_OLD_DATA_INTERVAL` / `ZO_COMPACT_OLD_DATA_MIN_FILES`,
as described in [README.md](README.md)) and wait for it to finish. Restarting
with the new value alone measures nothing.

Verify the row counts match on all three backends before measuring anything, and
wait for compaction to finish producing `.ttv` and `.bf` files. Interactive
versions of both rounds are checked in as [`index.html`](index.html) (2 GB) and
[`index-v2.html`](index-v2.html) (10 GB).
