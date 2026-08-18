import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class BenchmarkConfigurationTest(unittest.TestCase):
    def test_backend_versions_are_pinned(self):
        installer = (ROOT / "scripts" / "install.sh").read_text()
        self.assertIn('CLICKHOUSE_VERSION="26.7.3.19"', installer)
        self.assertIn('CLICKHOUSE_RELEASE_TAG="v${CLICKHOUSE_VERSION}-stable"', installer)
        self.assertIn('OPENOBSERVE_VERSION="${OPENOBSERVE_VERSION:-v0.92.2}"', installer)
        self.assertNotIn("https://clickhouse.com/ | sh", installer)

    def test_o2_trace_bloom_is_cross_format_and_explicit(self):
        env = (ROOT / ".env.example").read_text()
        start = (ROOT / "scripts" / "start-openobserve.sh").read_text()

        for text in (env, start):
            self.assertIn("ZO_BLOOM_FILTER_ENABLED", text)
            self.assertIn("ZO_FEATURE_BLOOM_FILTER_EXTRA_FIELDS", text)
            self.assertIn("trace_id", text)
            self.assertIn("ZO_BLOOM_FILTER_FPP", text)
            self.assertIn("0.01", text)
            self.assertIn("ZO_BLOOM_FILTER_PARQUET_ENABLED", text)
            self.assertIn("false", text)

    def test_o2_ingest_age_policy_is_not_overridden(self):
        env = (ROOT / ".env.example").read_text()
        start = (ROOT / "scripts" / "start-openobserve.sh").read_text()

        self.assertNotIn("ZO_INGEST_ALLOWED_UPTO", env)
        self.assertNotIn("ZO_INGEST_ALLOWED_UPTO", start)

    def test_compacted_source_files_are_deleted_after_ten_minutes(self):
        env = (ROOT / ".env.example").read_text()
        start = (ROOT / "scripts" / "start-openobserve.sh").read_text()

        self.assertIn("ZO_COMPACT_DELETE_FILES_DELAY_MINUTES=10", env)
        self.assertIn(
            'ZO_COMPACT_DELETE_FILES_DELAY_MINUTES="${ZO_COMPACT_DELETE_FILES_DELAY_MINUTES:-10}"',
            start,
        )


if __name__ == "__main__":
    unittest.main()
