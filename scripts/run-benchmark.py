#!/usr/bin/env python3
"""Run the 'find the needle' query benchmark against OpenObserve and ClickHouse.

For each query in queries/queries.json, the driver:
  1. disables backend result/query caches and drops supported engine caches,
  2. runs the query N times on each enabled backend — in both its count()
     shape (pure index+scan) and its SELECT * LIMIT 100 shape (`*_rows`
     templates; adds row materialization/transfer, may terminate early),
  3. records server-side latency,
  4. reports p50 / p95 / p99 across all runs per backend (the OS page cache is
     dropped once before run 1, so run 1 is cold and the rest are hot; the
     result page splits these into Cold/Hot, but these percentiles span both).

Needle values (a real trace_id / span_id / request id / pod) are embedded in
queries/queries.json so every backend and every machine runs the same query
anchors without runtime sampling.

Stdlib only; no pip installs required.
"""

import argparse
import base64
import json
import os
import statistics
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

# Fixed query window in epoch microseconds: 2026-06-01T00:00:00Z through
# 2027-06-01T00:00:00Z. OpenObserve also needs bounded request times.
TS_START = 1_780_272_000_000_000
TS_END = 1_811_808_000_000_000
BENCH_TABLE = "k8s_logs"

# OpenObserve reports stream stats (storage/compressed/index sizes) in MiB —
# it divides raw byte counts by this before serializing (config SIZE_IN_MB).
# Multiplying back is exact (2^20 is a power of two) for any realistic size.
SIZE_IN_MB = 1024 * 1024


# --------------------------------------------------------------------------- #
# HTTP helpers
# --------------------------------------------------------------------------- #
def _http(method, url, *, headers=None, body=None, timeout=300):
    req = urllib.request.Request(url, data=body, method=method)
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.status, resp.read(), dict(resp.headers)


# --------------------------------------------------------------------------- #
# ClickHouse backend
# --------------------------------------------------------------------------- #
class ClickHouse:
    name = "clickhouse"

    def __init__(self, url, database, table, user, password):
        self.url = url.rstrip("/")
        self.database = database
        self.table = table
        self.user = user
        self.password = password

    def _post(self, sql, params=None):
        query = {"query": sql, "user": self.user, "password": self.password}
        if params:
            query.update(params)
        url = f"{self.url}/?{urllib.parse.urlencode(query)}"
        status, data, _ = _http("POST", url, body=b"")
        if status != 200:
            raise RuntimeError(f"clickhouse {status}: {data[:300]!r}")
        return data

    def drop_cache(self):
        for stmt in (
            "SYSTEM DROP MARK CACHE",
            "SYSTEM DROP UNCOMPRESSED CACHE",
            "SYSTEM DROP COMPILED EXPRESSION CACHE",
            # The query-condition cache memoizes WHICH granules matched a WHERE, so
            # a repeated rare-match query reads 1 granule instead of scanning — that
            # is a warm-cache optimization and must be cleared for a cold measurement.
            "SYSTEM DROP QUERY CONDITION CACHE",
        ):
            try:
                self._post(stmt)
            except Exception:
                pass

    def timed(self, sql, size=0):
        """Run sql, return (server_ms, rows_read). Uses FORMAT JSON statistics.

        `size` is unused — row-returning variants carry LIMIT inline in the SQL;
        it exists for signature parity with OpenObserve.timed.

        Disables the query-cache and query-condition-cache per query so every run
        is a genuine cold scan, comparable to OpenObserve's use_cache:false.
        """
        data = self._post(sql + " FORMAT JSON",
                          params={
                              "use_query_cache": "0",
                              "use_query_condition_cache": "0",
                              "use_uncompressed_cache": "0",
                              "enable_filesystem_cache": "0",
                              "read_from_filesystem_cache_if_exists_otherwise_bypass_cache": "0",
                          })
        doc = json.loads(data)
        stats = doc.get("statistics", {})
        return stats.get("elapsed", 0.0) * 1000.0, stats.get("rows_read", 0)

    def collect_storage(self):
        parts = json.loads(self._post(
            f"SELECT sum(data_compressed_bytes) c, sum(data_uncompressed_bytes) u, "
            f"sum(rows) r FROM system.parts WHERE database='{self.database}' "
            f"AND table='{self.table}' AND active FORMAT JSON"))["data"][0]
        idx = json.loads(self._post(
            f"SELECT name, type_full t, sum(data_compressed_bytes) b "
            f"FROM system.data_skipping_indices WHERE database='{self.database}' "
            f"AND table='{self.table}' GROUP BY name, type_full ORDER BY name FORMAT JSON"))["data"]
        indexes = [{"name": x["name"], "type": x["t"], "bytes": int(x["b"])} for x in idx]
        return {
            "data_on_disk": int(parts["c"] or 0),
            "data_uncompressed": int(parts["u"] or 0),
            "rows": int(parts["r"] or 0),
            "index_bytes": sum(i["bytes"] for i in indexes),
            "indexes": indexes,
        }


# --------------------------------------------------------------------------- #
# OpenObserve backend
# --------------------------------------------------------------------------- #
class OpenObserve:
    name = "openobserve"

    def __init__(self, url, org, stream, username, password):
        self.url = url.rstrip("/")
        self.org = org
        self.stream = stream
        self.username = username
        self.password = password
        raw = f"{username}:{password}".encode()
        self._auth = "Basic " + base64.b64encode(raw).decode()

    def _search(self, sql, size=0):
        # use_cache MUST be a URL query param: the HTTP handler overwrites the
        # body field with the URL value (src/handler/http/request/search/mod.rs:
        # `req.use_cache = get_use_cache_from_request(&url_query) && ...`), and
        # the URL value defaults to TRUE when absent. The body field below is
        # kept only for older/newer versions that may honor it.
        endpoint = f"{self.url}/api/{self.org}/_search?use_cache=false"
        payload = {
            "query": {
                "sql": sql,
                "start_time": TS_START,
                "end_time": TS_END,
                "from": 0,
                "size": size,
                "use_cache": False,
            }
        }
        body = json.dumps(payload).encode()
        status, data, _ = _http(
            "POST",
            endpoint,
            headers={"Authorization": self._auth, "Content-Type": "application/json"},
            body=body,
        )
        if status != 200:
            raise RuntimeError(f"openobserve {status}: {data[:300]!r}")
        return json.loads(data)

    def drop_cache(self):
        # OpenObserve has no API to drop its file/page cache; result-cache is
        # bypassed per request via "use_cache": false.
        pass

    def timed(self, sql, size=0):
        # size>0 for SELECT-rows variants so _search actually returns hits
        # (the SQL also carries LIMIT, which takes precedence when present).
        doc = self._search(sql, size=size)
        # OpenObserve reports server time in `took` (milliseconds).
        return float(doc.get("took", 0)), doc.get("scan_records", 0)

    def collect_storage(self, data_dir=None):
        """Authoritative sizes from OpenObserve's stream-stats API.

        GET /api/{org}/streams/{stream}/schema returns stats.storage_size (raw
        ingested bytes), stats.compressed_size (parquet on disk) and
        stats.index_size (tantivy .ttv) — the proper analog to ClickHouse's
        system.parts, sourced from the same metastore (`file_list`) as the local
        read but reachable over HTTP, so it works when the driver and server are
        on different hosts. Sizes are reported in MiB; we scale back to bytes.
        Note: only compacted files are counted; data still in WAL is excluded.

        Falls back to a local metastore/filesystem read when `data_dir` is given
        and accessible (e.g. driver co-located with the server) and the API call
        fails.
        """
        try:
            return self._collect_storage_api()
        except Exception as e:
            import os
            if data_dir and os.path.isdir(data_dir):
                print(f"   openobserve stream-stats API failed ({e}); "
                      "falling back to local data dir")
                return self._collect_storage_local(data_dir)
            raise

    def _collect_storage_api(self):
        endpoint = f"{self.url}/api/{self.org}/streams/{self.stream}/schema?type=logs"
        status, data, _ = _http(
            "GET", endpoint, headers={"Authorization": self._auth})
        if status != 200:
            raise RuntimeError(f"openobserve {status}: {data[:300]!r}")
        stats = json.loads(data).get("stats", {})
        index_bytes = int(stats.get("index_size", 0) * SIZE_IN_MB)
        return {
            "source": "stream_stats_api",
            "files": int(stats.get("file_num", 0)),
            "rows": int(stats.get("doc_num", 0)),
            "data_on_disk": int(stats.get("compressed_size", 0) * SIZE_IN_MB),
            "data_uncompressed": int(stats.get("storage_size", 0) * SIZE_IN_MB),
            "index_bytes": index_bytes,
            "indexes": [{"name": "inverted_index (.ttv)",
                         "type": "tantivy", "bytes": index_bytes}],
        }

    def _collect_storage_local(self, data_dir):
        """Read the same numbers from a co-located OpenObserve data dir.

        The `file_list` table in db/metadata.sqlite records, per compacted parquet
        file, original_size (raw ingested bytes), compressed_size (parquet on disk)
        and index_size (tantivy .ttv). Falls back to a filesystem scan if the DB
        is unavailable.
        """
        import os
        import sqlite3

        db = os.path.join(data_dir, "db", "metadata.sqlite")
        stream_key = f"{self.org}/logs/{self.stream}"
        if os.path.exists(db):
            try:
                con = sqlite3.connect(f"file:{db}?mode=ro", uri=True, timeout=10)
                files, rows, original, compressed, index = con.execute(
                    "SELECT count(*), COALESCE(sum(records),0), "
                    "COALESCE(sum(original_size),0), COALESCE(sum(compressed_size),0), "
                    "COALESCE(sum(index_size),0) FROM file_list "
                    "WHERE stream=? AND deleted=false", (stream_key,)).fetchone()
                con.close()
                if files:
                    return {
                        "source": "file_list",
                        "files": files,
                        "rows": rows,
                        "data_on_disk": compressed,
                        "data_uncompressed": original,
                        "index_bytes": index,
                        "indexes": [{"name": "inverted_index (.ttv)",
                                     "type": "tantivy", "bytes": index}],
                    }
                print("   openobserve file_list empty (data still in WAL?); "
                      "falling back to filesystem scan")
            except Exception as e:
                print(f"   openobserve file_list read failed ({e}); "
                      "falling back to filesystem scan")

        # Fallback: sum file sizes by extension (no original_size available).
        data_bytes = index_bytes = wal_bytes = 0
        for root, _dirs, files in os.walk(data_dir):
            in_wal = (os.sep + "wal") in (root + os.sep)
            for f in files:
                try:
                    sz = os.path.getsize(os.path.join(root, f))
                except OSError:
                    continue
                if in_wal:
                    wal_bytes += sz
                elif f.endswith(".parquet"):
                    data_bytes += sz
                elif f.endswith((".ttv", ".idx", ".fst")):
                    index_bytes += sz
        return {
            "source": "filesystem",
            "data_on_disk": data_bytes,
            "index_bytes": index_bytes,
            "wal_bytes": wal_bytes,
            "indexes": [{"name": "inverted_index (.ttv)", "type": "tantivy",
                         "bytes": index_bytes}],
        }


# --------------------------------------------------------------------------- #
# Stats
# --------------------------------------------------------------------------- #
def percentile(values, p):
    if not values:
        return 0.0
    s = sorted(values)
    if len(s) == 1:
        return s[0]
    k = (len(s) - 1) * p
    lo = int(k)
    hi = min(lo + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (k - lo)


def fmt_ms(ms):
    return f"{ms/1000:.3f} s" if ms >= 1000 else f"{ms:.1f} ms"


def fmt_bytes(n):
    n = float(n)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if n < 1024 or unit == "TiB":
            return f"{n:.1f} {unit}"
        n /= 1024


def machine_info():
    """Best-effort host description (CPU / cores / memory). Works on macOS
    (sysctl) and Linux (/proc), falling back to `platform` elsewhere."""
    import platform

    system = platform.system()
    cpu = platform.processor() or "unknown"
    cores = os.cpu_count()
    mem_gib = None

    if system == "Darwin":
        def sysctl(key):
            try:
                return subprocess.check_output(["sysctl", "-n", key]).decode().strip()
            except Exception:
                return None

        mem = sysctl("hw.memsize")
        ncpu = sysctl("hw.ncpu")
        cpu = sysctl("machdep.cpu.brand_string") or cpu
        cores = int(ncpu) if ncpu else cores
        mem_gib = round(int(mem) / 1024**3, 1) if mem else None
    elif system == "Linux":
        try:
            for line in open("/proc/cpuinfo"):
                if line.startswith("model name"):
                    cpu = line.split(":", 1)[1].strip()
                    break
        except Exception:
            pass
        try:
            for line in open("/proc/meminfo"):
                if line.startswith("MemTotal"):
                    mem_gib = round(int(line.split()[1]) / 1024**2, 1)
                    break
        except Exception:
            pass

    return {
        "cpu": cpu,
        "cores": cores,
        "memory_gib": mem_gib,
        "platform": platform.platform(),
        "python": platform.python_version(),
    }


def drop_os_page_cache():
    """Drop the local OS page cache so the next run is a genuine cold read
    (driver and server share the host). Called once per query variant, so only
    the first of the following runs is cold; the rest read from a warm page
    cache. Needs sudo. macOS: `sudo purge`.
    Linux: `sync` then write 3 to /proc/sys/vm/drop_caches."""
    import platform

    system = platform.system()
    if system == "Darwin":
        subprocess.run(["sudo", "purge"], check=False)
    elif system == "Linux":
        subprocess.run(["sync"], check=False)
        subprocess.run(
            ["sudo", "sh", "-c", "echo 3 > /proc/sys/vm/drop_caches"],
            check=False,
        )
    else:
        print(f"   (no page-cache drop implemented for {system}; "
              "results may be warm)")


def run_query(backend, sql, runs, size=0):
    samples = []
    drop_os_page_cache()
    for _ in range(runs):
        backend.drop_cache()
        ms, _rows = backend.timed(sql, size)
        samples.append(ms)
    return samples


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--target", choices=["both", "openobserve", "clickhouse"], default="both")
    ap.add_argument("--queries", default="queries/queries.json")
    ap.add_argument("--runs", type=int, default=5,
                    help="runs per query (1 cold + N-1 hot; page cache dropped once before run 1)")
    ap.add_argument("--out", default="results/summary.md")
    # OpenObserve
    ap.add_argument("--o2-url", default="http://localhost:5080")
    ap.add_argument("--org", default="default")
    ap.add_argument("--oo-user", default="root@example.com")
    ap.add_argument("--oo-password", default="Complexpass#123")
    # ClickHouse
    ap.add_argument("--ch-url", default="http://localhost:8123")
    ap.add_argument("--ch-database", default="default")
    ap.add_argument("--ch-user", default="default")
    ap.add_argument("--ch-password", default="")
    ap.add_argument("--oo-data-dir", default="openobserve-data",
                    help="OpenObserve data dir, for storage measurement")
    ap.add_argument("--ingest-stats", default=None,
                    help="path to datagen --stats-out JSON (default <results>/ingest.json)")
    args = ap.parse_args()

    backends = []
    if args.target in ("both", "openobserve"):
        backends.append(OpenObserve(args.o2_url, args.org, BENCH_TABLE,
                                    args.oo_user, args.oo_password))
    if args.target in ("both", "clickhouse"):
        backends.append(ClickHouse(args.ch_url, args.ch_database, BENCH_TABLE,
                                   args.ch_user, args.ch_password))
    if not backends:
        sys.exit("no backends enabled")
    for b in backends:
        b.run_entries = []

    spec = json.loads(Path(args.queries).read_text())
    queries = spec["queries"]

    results_dir = Path(args.out).parent
    results_dir.mkdir(parents=True, exist_ok=True)
    (results_dir / "machine.json").write_text(json.dumps(machine_info(), indent=2))
    print(f"using query templates from {args.queries}")
    print()

    # Each query runs in two shapes: count() (`ch`/`oo` templates — pure
    # index+scan work) and, when `*_rows` templates exist, SELECT * LIMIT 100
    # (row materialization + transfer included; engines may stop early once the
    # LIMIT is satisfied, so this is the interactive-UX metric, not a scan metric).
    for q in queries:
        for variant in ("count", "rows"):
            suffix = "" if variant == "count" else "_rows"
            if variant == "rows" and not (q.get("oo_rows") or q.get("ch_rows")):
                continue
            qid = q["id"] + ("__rows" if variant == "rows" else "")
            qname = q["name"] + (" — SELECT * LIMIT 100" if variant == "rows" else "")
            size = 100 if variant == "rows" else 0
            print(f"== {qid}: {qname}")
            for b in backends:
                key = ("oo" if b.name == "openobserve" else "ch") + suffix
                tmpl = q.get(key)
                if not tmpl:
                    continue
                sql = tmpl
                entry = {"id": qid, "name": qname, "note": q.get("note", ""),
                         "category": q.get("category", ""),
                         "sql_template": tmpl, "sql": sql}
                try:
                    samples = run_query(b, sql, args.runs, size)
                except (urllib.error.URLError, RuntimeError) as e:
                    print(f"   {b.name:12s} ERROR: {e}")
                    entry["error"] = str(e)
                else:
                    entry.update({
                        "median": statistics.median(samples),
                        "p50": percentile(samples, 0.50),
                        "p95": percentile(samples, 0.95),
                        "p99": percentile(samples, 0.99),
                        "min": min(samples),
                        "max": max(samples),
                        "samples": samples,
                    })
                    print(f"   {b.name:12s} median={fmt_ms(entry['median'])}  "
                          f"p95={fmt_ms(entry['p95'])}  p99={fmt_ms(entry['p99'])}")
                b.run_entries.append(entry)
            print()

    # Persist this run's per-backend results (with storage, collected while this
    # backend's server is up), then (re)build the combined report from whichever
    # backend files exist — so separate single-target runs accumulate into one
    # OO-vs-CH comparison.
    for b in backends:
        try:
            storage = (b.collect_storage(args.oo_data_dir)
                       if b.name == "openobserve" else b.collect_storage())
        except Exception as e:
            print(f"   {b.name:12s} storage collection failed: {e}")
            storage = None
        payload = {"backend": b.name, "runs": args.runs, "table": BENCH_TABLE,
                   "os_page_cache_dropped_at_query_start": True,
                   "storage": storage, "queries": b.run_entries}
        (results_dir / f"{b.name}.json").write_text(json.dumps(payload, indent=2))

    ingest_stats = args.ingest_stats or str(results_dir / "ingest.json")
    build_combined_report(results_dir, Path(args.out), ingest_stats)
    print(f"wrote {args.out}")


def build_combined_report(results_dir, out_path, ingest_stats_path=None):
    """Render results/summary.md from whichever per-backend JSON files exist.

    Works whether both engines were benchmarked together (--target both) or in
    separate isolated runs (--target openobserve, then --target clickhouse): each
    run drops results/<backend>.json, and this merges all present backends by
    query id into one OO-vs-CH table. Also folds in machine specs (machine.json),
    ingest throughput (ingest.json), and per-backend storage / index sizes.
    """
    order = ["openobserve", "clickhouse"]
    data, names = {}, []
    for name in order:
        f = results_dir / f"{name}.json"
        if f.exists():
            data[name] = json.loads(f.read_text())
            names.append(name)
    if not names:
        return

    def _load(path):
        try:
            return json.loads(Path(path).read_text())
        except Exception:
            return None

    machine = _load(results_dir / "machine.json")
    ingest = _load(ingest_stats_path) if ingest_stats_path else None

    # Common raw-JSON denominator for index % and compression. OpenObserve's
    # `original_size` (from file_list) IS the raw ingested JSON volume and is the
    # preferred source; fall back to datagen's --stats-out raw_bytes if OO wasn't
    # measured. Both engines are compared against this single number.
    oo_storage = (data.get("openobserve") or {}).get("storage") or {}
    raw_bytes = oo_storage.get("data_uncompressed")
    raw_source = "OpenObserve original_size"
    if not raw_bytes and ingest and ingest.get("raw_bytes"):
        raw_bytes = int(ingest["raw_bytes"])
        raw_source = "datagen --stats-out"
    raw_bytes = int(raw_bytes) if raw_bytes else None

    meta = data[names[0]]
    # Query order/metadata from the first available backend; merge stats by id.
    by_id = {}
    q_order = []
    for name in names:
        for q in data[name]["queries"]:
            if q["id"] not in by_id:
                by_id[q["id"]] = {"name": q["name"], "note": q.get("note", ""), "b": {}}
                q_order.append(q["id"])
            by_id[q["id"]]["b"][name] = q

    L = []
    L.append("# Benchmark results — OpenObserve vs ClickHouse")
    L.append("")
    L.append(f"- backends measured: **{', '.join(names)}**"
             + ("  _(isolated runs — only one server up at a time)_"
                if len(names) == 2 else ""))
    L.append(f"- runs per query (1 cold + rest hot; page cache dropped once): **{meta['runs']}**")
    L.append(f"- table / stream: `{meta['table']}`")
    L.append("- OS page-cache dropped at each query start: **True**")
    if machine:
        cores = machine.get("cores")
        mem = machine.get("memory_gib")
        L.append(f"- machine: **{machine.get('cpu','?')}**, "
                 f"{cores if cores else '?'} cores, "
                 f"{mem if mem else '?'} GiB RAM ({machine.get('platform','')})")
    if ingest:
        tgts = ", ".join(ingest.get("targets", [])) or "?"
        L.append(f"- ingest: **{int(ingest.get('records_sent',0)):,} records** "
                 f"({fmt_bytes(raw_bytes) if raw_bytes else '?'} raw JSON) "
                 f"in {ingest.get('elapsed_s','?')}s → "
                 f"**{int(ingest.get('rate_rec_s',0)):,} rec/s, "
                 f"{ingest.get('rate_mb_s','?')} MB/s** (→ {tgts})")
    L.append("")

    # ---- Storage & index ----
    if any(data[n].get("storage") for n in names):
        L.append("## Storage & index size")
        L.append("")
        if raw_bytes:
            L.append(f"Raw ingested JSON: **{fmt_bytes(raw_bytes)}** "
                     f"(source: {raw_source}) — common denominator for index % and "
                     f"compression on both engines.")
        else:
            L.append("_No raw-JSON size available (OpenObserve not measured and no "
                     "datagen --stats-out); index % / compression use each engine's "
                     "own reported uncompressed size instead._")
        L.append("")
        L.append("| backend | data on disk | index size | index % of raw | compression (raw/disk) |")
        L.append("|---|---|---|---|---|")
        for n in names:
            s = data[n].get("storage")
            if not s:
                L.append(f"| {n} | - | - | - | - |")
                continue
            disk = s.get("data_on_disk", 0)
            idxb = s.get("index_bytes", 0)
            denom = raw_bytes or s.get("data_uncompressed")  # common raw, else engine's own
            idx_pct = f"{idxb / denom * 100:.2f}%" if denom else "-"
            comp = f"{denom / disk:.2f}×" if denom and disk else "-"
            L.append(f"| {n} | {fmt_bytes(disk)} | {fmt_bytes(idxb)} | {idx_pct} | {comp} |")
        L.append("")
        # Per-index breakdown
        for n in names:
            s = data[n].get("storage") or {}
            idxs = s.get("indexes") or []
            if not idxs:
                continue
            L.append(f"<details><summary>{n} index breakdown</summary>")
            L.append("")
            L.append("| index | type | size |")
            L.append("|---|---|---|")
            for i in idxs:
                L.append(f"| {i['name']} | {i.get('type','')} | {fmt_bytes(i['bytes'])} |")
            L.append("")
            L.append("</details>")
            L.append("")

    L.append("## Median latency (lower is better)")
    L.append("")
    head = "| Query | " + " | ".join(n + " median" for n in names) + " |"
    if len(names) == 2:
        head += " speedup |"
    L.append(head)
    L.append("|---|" + "---|" * len(names) + ("---|" if len(names) == 2 else ""))
    for qid in q_order:
        q = by_id[qid]
        # Query cell: name + the actual SQL template run on each backend.
        qcell = f"**{q['name']}**"
        for n in names:
            e = q["b"].get(n)
            if e and e.get("sql_template"):
                tag = "OO" if n == "openobserve" else "CH"
                sqlt = e["sql_template"]
                qcell += f"<br>`{tag}:` `{sqlt}`"
        cells = [qcell]
        meds = {}
        for n in names:
            b = q["b"].get(n)
            if not b or "error" in b:
                cells.append("ERROR" if b else "-")
            else:
                meds[n] = b["median"]
                cells.append(fmt_ms(b["median"]))
        if len(names) == 2:
            if len(meds) == 2 and meds["openobserve"] > 0 and meds["clickhouse"] > 0:
                oo, ch = meds["openobserve"], meds["clickhouse"]
                cells.append(f"OO {ch/oo:.1f}× faster" if oo < ch
                             else f"CH {oo/ch:.1f}× faster")
            else:
                cells.append("-")
        L.append("| " + " | ".join(cells) + " |")
    L.append("")
    L.append("## Full percentiles (ms)")
    L.append("")
    for qid in q_order:
        q = by_id[qid]
        L.append(f"### {q['name']} (`{qid}`)")
        if q["note"]:
            L.append(f"_{q['note']}_")
        L.append("")
        L.append("| backend | median | p50 | p95 | p99 | min | max |")
        L.append("|---|---|---|---|---|---|---|")
        for n in names:
            b = q["b"].get(n)
            if not b or "error" in b:
                L.append(f"| {n} | ERROR | | | | | |")
                continue
            L.append(f"| {n} | {b['median']:.1f} | {b['p50']:.1f} | {b['p95']:.1f} | "
                     f"{b['p99']:.1f} | {b['min']:.1f} | {b['max']:.1f} |")
        L.append("")
    out_path.write_text("\n".join(L))


if __name__ == "__main__":
    main()
