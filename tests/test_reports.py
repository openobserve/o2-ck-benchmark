import importlib.util
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RUNNER_PATH = ROOT / "scripts" / "run-benchmark.py"
BUILD_REPORT_PATH = ROOT / "scripts" / "build-report.py"


def load_runner():
    spec = importlib.util.spec_from_file_location("benchmark_runner", RUNNER_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def backend_payload(name, median_ms, rows=100):
    return {
        "backend": name,
        "runs": 3,
        "table": "k8s_logs",
        "isolated_run": True,
        "storage": {
            "rows": rows,
            "data_on_disk": 1_000,
            "data_uncompressed": 4_000,
            "index_bytes": 100,
            "indexes": [],
        },
        "queries": [{
            "id": "q0",
            "name": "needle",
            "note": "",
            "category": "",
            "sql_template": "SELECT count(*) FROM k8s_logs",
            "sql": "SELECT count(*) FROM k8s_logs",
            "median": median_ms,
            "p50": median_ms,
            "p95": median_ms,
            "p99": median_ms,
            "min": median_ms,
            "max": median_ms,
            "samples": [median_ms, median_ms, median_ms],
        }],
    }


class ThreeWayReportTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.results = self.root / "results"
        self.results.mkdir()
        for name, median in (
            ("clickhouse", 30.0),
            ("o2-parquet", 20.0),
            ("o2-vortex", 10.0),
        ):
            (self.results / f"{name}.json").write_text(
                json.dumps(backend_payload(name, median)))
        (self.results / "machine.json").write_text(json.dumps({
            "cpu": "test-cpu", "cores": 8, "memory_gib": 16,
            "platform": "test-os",
        }))
        (self.results / "ingest.json").write_text(json.dumps({
            "targets": ["clickhouse", "o2-parquet", "o2-vortex"],
            "records_sent": 100,
            "raw_bytes": 4_000,
            "elapsed_s": 10,
            "rate_rec_s": 10,
            "rate_mb_s": 0.01,
        }))

    def tearDown(self):
        self.tmp.cleanup()

    def test_markdown_summary_is_three_way(self):
        runner = load_runner()
        output = self.results / "summary.md"
        runner.build_combined_report(
            self.results, output, self.results / "ingest.json")
        report = output.read_text()

        self.assertIn("ClickHouse vs O2-Parquet vs O2-Vortex", report)
        self.assertIn("clickhouse, o2-parquet, o2-vortex", report)
        self.assertIn("dataset row-count check: **matched**", report)
        self.assertIn("**O2-Vortex fastest**", report)
        self.assertIn("3.0× vs ClickHouse", report)
        self.assertIn("2.0× vs O2-Parquet", report)

    def test_interactive_data_uses_stable_system_order_and_labels(self):
        subprocess.run(
            [sys.executable, str(BUILD_REPORT_PATH),
             "--results", str(self.results), "--root", str(self.root)],
            check=True,
            capture_output=True,
            text=True,
        )
        generated = (self.root / "data.generated.js").read_text()

        positions = [generated.index(f'"system": "{name}"') for name in (
            "clickhouse", "o2-parquet", "o2-vortex")]
        self.assertEqual(positions, sorted(positions))
        self.assertIn("ClickHouse: SELECT count(*)", generated)
        self.assertIn("O2-Parquet: SELECT count(*)", generated)
        self.assertIn("O2-Vortex: SELECT count(*)", generated)
        self.assertIn('"os_page_cache_dropped": false', generated)


if __name__ == "__main__":
    unittest.main()
