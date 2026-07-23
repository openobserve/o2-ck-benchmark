#!/usr/bin/env bash
# Start single-node OpenObserve with the benchmark's index configuration baked in.
#
# The index env vars below must be set BEFORE any data is ingested — OpenObserve
# applies them when the stream schema is first created. They mirror the ClickHouse
# skip indexes in schemas/clickhouse.sql so neither side is under-tuned:
#
#   ClickHouse bloom_filter (trace_id / span_id / kubernetes_pod_name)
#       <->  OpenObserve secondary index   (ZO_FEATURE_INDEX_EXTRA_FIELDS)
#   ClickHouse text() inverted index (message)
#       <->  OpenObserve full-text index   (ZO_FEATURE_FULLTEXT_EXTRA_FIELDS)
# ClickHouse is ORDER BY (_timestamp) only — time-ordered like OpenObserve, with
# no structured column in the sort-key prefix — so neither engine gets a layout
# advantage on the benchmark queries.
#
# Usage:
#   scripts/start-openobserve.sh                  # uses ./bin/openobserve
#   OPENOBSERVE_BIN=openobserve scripts/start-openobserve.sh
set -euo pipefail

# Load overrides from .env if present (does not override already-exported vars).
if [[ -f .env ]]; then
  set -a; source .env; set +a
fi

BIN="${OPENOBSERVE_BIN:-./bin/openobserve}"

# --- Auth (matches datagen + run-benchmark.py defaults) ---
export ZO_ROOT_USER_EMAIL="${ZO_ROOT_USER_EMAIL:-root@example.com}"
export ZO_ROOT_USER_PASSWORD="${ZO_ROOT_USER_PASSWORD:-Complexpass#123}"

# --- Single-node local storage (parquet + sqlite meta on local disk) ---
export ZO_DATA_DIR="${ZO_DATA_DIR:-./openobserve-data}"
mkdir -p "$ZO_DATA_DIR"
export ZO_DATA_DIR="$(cd "$ZO_DATA_DIR" && pwd -P)"

# Rotate open WAL files after 60s (default 600s) so the tail of an ingest moves
# from wal/ to parquet + file_list quickly — without this, the last partially
# filled WAL file sits unindexed for up to 10 minutes after ingest finishes.
export ZO_MAX_FILE_RETENTION_TIME="${ZO_MAX_FILE_RETENTION_TIME:-60}"

# --- Indexing config (the important part) ---
export ZO_ENABLE_INVERTED_INDEX="${ZO_ENABLE_INVERTED_INDEX:-true}"
# Full-text index on `message`. The text-search queries (q2/q3/q5/q6 match_all()
# plus the q10 filtered histogram) are served from the tantivy FTS index, mirroring
# the text() index in schemas/clickhouse.sql — so token search compares FTS vs FTS.
export ZO_FEATURE_FULLTEXT_EXTRA_FIELDS="${ZO_FEATURE_FULLTEXT_EXTRA_FIELDS:-message}"
# Secondary indexes on exactly the high-cardinality columns the queries filter
# on: trace_id (q0, q4), span_id (q1), kubernetes_pod_name (q7). Mirrors the
# three ClickHouse bloom filters one-for-one. kubernetes_container_name is left
# out on both engines (low-cardinality — indexing it prunes nothing, and it is
# no longer in the ClickHouse sort key either).
export ZO_FEATURE_INDEX_EXTRA_FIELDS="${ZO_FEATURE_INDEX_EXTRA_FIELDS:-trace_id,span_id,kubernetes_pod_name}"

# disable result cache
export ZO_RESULT_CACHE_ENABLED=false

if [[ ! -x "$BIN" && -z "$(command -v "$BIN" 2>/dev/null || true)" ]]; then
  echo "OpenObserve binary not found at '$BIN'." >&2
  echo "Download the arm64 release into ./bin/ or set OPENOBSERVE_BIN." >&2
  exit 1
fi

echo "starting OpenObserve (single-node)"
echo "  data dir            : $ZO_DATA_DIR"
echo "  inverted index      : $ZO_ENABLE_INVERTED_INDEX"
echo "  full-text fields +  : $ZO_FEATURE_FULLTEXT_EXTRA_FIELDS"
echo "  secondary idx fields: $ZO_FEATURE_INDEX_EXTRA_FIELDS"
echo "  wal retention (s)   : $ZO_MAX_FILE_RETENTION_TIME"
echo "  listening on        : http://localhost:5080"
exec "$BIN"
