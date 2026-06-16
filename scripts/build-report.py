#!/usr/bin/env python3
"""Generate data.generated.js for the ClickBench-style results page (index.html).

Reads results/{openobserve,clickhouse}.json (+ machine.json, ingest.json) and
writes data.generated.js next to index.html. Also rewrites the `const queries`
array inside index.html so the Q0..Qn tooltips always match the data order.

Usage:  python3 scripts/build-report.py [--results results] [--root .]
"""
import argparse
import datetime
import json
import re
from pathlib import Path

TAGS = {
    "openobserve": ["Rust", "log search engine", "parquet", "tantivy"],
    "clickhouse": ["C++", "column-oriented", "ClickHouse derivative"],
}


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--results", default="results")
    ap.add_argument("--root", default=".", help="dir containing index.html")
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
                   f"{machine.get('memory_gib', '?')} GiB")
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
        entries.append({
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
        })

    out_js = root / "data.generated.js"
    out_js.write_text("const data = [\n"
                      + ",\n".join(json.dumps(e) for e in entries) + "\n];\n"
                      + "const query_sections = "
                      + json.dumps({str(k): v for k, v in sections.items()})
                      + ";\n")
    print(f"wrote {out_js} ({len(entries)} systems x {len(order)} queries)")

    # Rewrite the `const queries = [...]` block in index.html so tooltips match.
    qstrings = []
    for qid in order:
        q = meta_by_id[qid]
        sqls = " | ".join(
            f"{'OO' if s == 'openobserve' else 'CH'}: {by_sys[s][qid]['sql']}"
            for s in systems if qid in by_sys[s] and by_sys[s][qid].get("sql"))
        qstrings.append(sqls)
    block = ("const queries = [\n"
             + ",\n".join(json.dumps(s) for s in qstrings) + ",\n];")
    html_path = root / "index.html"
    html = html_path.read_text()
    html, n = re.subn(r"const queries = \[.*?\n\];", lambda _: block, html,
                      count=1, flags=re.S)
    if n != 1:
        raise SystemExit("could not locate `const queries` block in index.html")
    # Raw-data size reference points (replaces ClickBench's hits.* points).
    points = []
    if ingest.get("raw_bytes"):
        points.append({"fake": True, "system": "raw JSON",
                       "data_size": int(ingest["raw_bytes"])})
    pblock = ("const additional_data_size_points = [\n"
              + ",\n".join(json.dumps(p) for p in points) + ("\n" if points else "")
              + "];")
    html, n = re.subn(r"const additional_data_size_points = \[.*?\n\];",
                      lambda _: pblock, html, count=1, flags=re.S)
    if n != 1:
        raise SystemExit("could not locate additional_data_size_points block")
    # Branding: title / h1 / header links.
    html = html.replace(
        "<title>ClickBench — a Benchmark For Analytical DBMS</title>",
        "<title>OpenObserve vs ClickHouse — Observability Benchmark</title>")
    html = html.replace(
        "<h1>ClickBench — a Benchmark For Analytical DBMS</h1>",
        "<h1>OpenObserve vs ClickHouse — Observability Benchmark</h1>")
    html = re.sub(
        r'<a href="https://github\.com/ClickHouse/ClickBench/">Methodology</a>.*?</a>\n',
        '<a href="README.md">Methodology</a> | <a href="PLAN.md">Plan</a> | '
        '<a href="results/summary.md">Markdown report</a>\n',
        html, count=1, flags=re.S)
    html_path.write_text(html)
    print(f"patched {html_path} (queries block: {len(qstrings)} entries)")


if __name__ == "__main__":
    main()
