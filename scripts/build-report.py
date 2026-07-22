#!/usr/bin/env python3
"""Generate data.generated.js for the ClickBench-style results page (index.html).

Reads results/{openobserve,clickhouse}.json (+ machine.json, ingest.json) and
writes data.generated.js next to index.html. All report data lives in that file
(data, queries, query_sections, dataset_meta) so index.html does not need to be
rewritten on each build.

Usage:
  python3 scripts/build-report.py [--results results] [--root .]
  python3 scripts/build-report.py --records 500000000   # if ingest.json is missing
"""
import argparse
import datetime
import json
from pathlib import Path

TAGS = {
    "openobserve": ["Rust", "log search engine", "parquet", "tantivy"],
    "clickhouse": ["C++", "column-oriented", "ClickHouse derivative"],
}


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--results", default="results")
    ap.add_argument("--root", default=".", help="dir containing index.html")
    ap.add_argument("--records", type=int, default=None,
                    help="override dataset record count (default: ingest.json records_sent)")
    args = ap.parse_args()
    rd, root = Path(args.results), Path(args.root)

    systems, per_system = [], {}
    for name in ("openobserve", "clickhouse"):
        f = rd / f"{name}.json"
        if f.exists():
            per_system[name] = json.loads(f.read_text())
            systems.append(name)
    if not systems:
        raise SystemExit(f"no per-backend json found under {rd}/")

    def load(p):
        try:
            return json.loads(Path(p).read_text())
        except Exception:
            return None

    machine = load(rd / "machine.json") or {}
    ingest = load(rd / "ingest.json") or {}
    records = args.records
    if records is None and ingest.get("records_sent") is not None:
        records = int(ingest["records_sent"])

    # Merged query order. The runner interleaves count/rows shapes; regroup so
    # the page shows all scan count() queries first, then aggregation queries
    # (histogram / topN), then all SELECT * queries, with a section divider
    # before each group (query_sections, rendered by index.html).
    order, meta_by_id = [], {}
    for s in systems:
        for q in per_system[s]["queries"]:
            if q["id"] not in meta_by_id:
                meta_by_id[q["id"]] = q
                order.append(q["id"])

    def is_agg(qid):
        return meta_by_id[qid].get("category") == "aggregation"

    count_ids = [i for i in order if not i.endswith("__rows") and not is_agg(i)]
    agg_ids = [i for i in order if not i.endswith("__rows") and is_agg(i)]
    rows_ids = [i for i in order if i.endswith("__rows")]
    order = count_ids + agg_ids + rows_ids
    sections = {0: "count() — index + scan only"}
    if agg_ids:
        sections[len(count_ids)] = "aggregation — histogram / topN (full scan)"
    if rows_ids:
        sections[len(count_ids) + len(agg_ids)] = (
            "SELECT * ... ORDER BY _timestamp DESC LIMIT 100 — row fetch")

    # Per-system per-query samples (seconds); null when errored/missing.
    by_sys = {s: {q["id"]: q for q in per_system[s]["queries"]} for s in systems}
    machine_str = (f"{machine.get('cpu', 'unknown')}, {machine.get('cores', '?')} cores, "
                   f"{machine.get('memory_gib', '?')} GiB"
                   + (f", {machine['disk']}" if machine.get("disk") else ""))
    today = datetime.date.today().isoformat()

    entries = []
    for s in systems:
        result = []
        for qid in order:
            q = by_sys[s].get(qid)
            if q and "samples" in q:
                result.append([round(ms / 1000.0, 6) for ms in q["samples"]])
            else:
                result.append(None)
        st = per_system[s].get("storage") or {}
        entry = {
            "system": s,
            "date": today,
            "machine": machine_str,
            "cluster_size": 1,
            "proprietary": "no",
            "tuned": "no",
            "hardware": "cpu",
            "tags": TAGS.get(s, []),
            "load_time": ingest.get("elapsed_s", 0),
            "data_size": int(st.get("data_on_disk", 0)) + int(st.get("index_bytes", 0)),
            "result": result,
            "source": f"results/{s}.json",
        }
        if records is not None:
            entry["records"] = records
        entries.append(entry)

    dataset_meta = {}
    if records is not None:
        dataset_meta["records"] = records
    if ingest.get("raw_bytes"):
        dataset_meta["raw_bytes"] = int(ingest["raw_bytes"])
    if ingest.get("elapsed_s") is not None:
        dataset_meta["load_time"] = ingest["elapsed_s"]
    if ingest.get("rate_rec_s") is not None:
        dataset_meta["rate_rec_s"] = ingest["rate_rec_s"]
    if ingest.get("rate_mb_s") is not None:
        dataset_meta["rate_mb_s"] = ingest["rate_mb_s"]

    # Query SQL tooltips (same order as result columns).
    qstrings = []
    for qid in order:
        sqls = " | ".join(
            f"{'OO' if s == 'openobserve' else 'CH'}: {by_sys[s][qid]['sql']}"
            for s in systems if qid in by_sys[s] and by_sys[s][qid].get("sql"))
        qstrings.append(sqls)

    out_js = root / "data.generated.js"
    parts = [
        "const data = [\n" + ",\n".join(json.dumps(e) for e in entries) + "\n];",
        "const queries = [\n" + ",\n".join(json.dumps(s) for s in qstrings) + ",\n];",
        "const query_sections = "
        + json.dumps({str(k): v for k, v in sections.items()}) + ";",
        "const dataset_meta = " + json.dumps(dataset_meta) + ";",
    ]
    out_js.write_text("\n".join(parts) + "\n")
    rec_note = f", {records:,} records" if records is not None else ""
    print(f"wrote {out_js} ({len(entries)} systems x {len(order)} queries{rec_note})")


if __name__ == "__main__":
    main()
