#!/usr/bin/env bash
# Start one OpenObserve benchmark instance with an explicit file format.
#
# Parquet and Vortex use separate data directories and ports. Apart from
# ZO_FILE_FORMAT and those process-isolation settings, both receive the exact
# same benchmark configuration.
#
# Usage:
#   scripts/start-openobserve.sh parquet
#   scripts/start-openobserve.sh vortex
#   OPENOBSERVE_BIN=openobserve scripts/start-openobserve.sh parquet
set -euo pipefail

FORMAT="${1:-}"
case "$FORMAT" in
  parquet)
    BACKEND="o2-parquet"
    DATA_DIR="${O2_PARQUET_DATA_DIR:-./openobserve-parquet-data}"
    HTTP_PORT="${O2_PARQUET_HTTP_PORT:-5080}"
    GRPC_PORT="${O2_PARQUET_GRPC_PORT:-5081}"
    ;;
  vortex)
    BACKEND="o2-vortex"
    DATA_DIR="${O2_VORTEX_DATA_DIR:-./openobserve-vortex-data}"
    HTTP_PORT="${O2_VORTEX_HTTP_PORT:-5090}"
    GRPC_PORT="${O2_VORTEX_GRPC_PORT:-5091}"
    ;;
  *)
    echo "usage: $0 parquet|vortex" >&2
    exit 2
    ;;
esac

# Load shared auth/index settings and optional O2_* overrides.
if [[ -f .env ]]; then
  set -a
  source .env
  set +a

  # Re-evaluate format-specific values after loading .env.
  if [[ "$FORMAT" == "parquet" ]]; then
    DATA_DIR="${O2_PARQUET_DATA_DIR:-./openobserve-parquet-data}"
    HTTP_PORT="${O2_PARQUET_HTTP_PORT:-5080}"
    GRPC_PORT="${O2_PARQUET_GRPC_PORT:-5081}"
  else
    DATA_DIR="${O2_VORTEX_DATA_DIR:-./openobserve-vortex-data}"
    HTTP_PORT="${O2_VORTEX_HTTP_PORT:-5090}"
    GRPC_PORT="${O2_VORTEX_GRPC_PORT:-5091}"
  fi
fi

BIN="${OPENOBSERVE_BIN:-./bin/openobserve}"

export ZO_ROOT_USER_EMAIL="${ZO_ROOT_USER_EMAIL:-root@example.com}"
export ZO_ROOT_USER_PASSWORD="${ZO_ROOT_USER_PASSWORD:-Complexpass#123}"

# Keep the two formats physically isolated. ZO_FILE_FORMAT is intentionally
# assigned here rather than accepted from .env so the command name is the
# authoritative A/B setting.
export ZO_DATA_DIR="$DATA_DIR"
mkdir -p "$ZO_DATA_DIR"
export ZO_DATA_DIR="$(cd "$ZO_DATA_DIR" && pwd -P)"
export ZO_FILE_FORMAT="$FORMAT"
export ZO_HTTP_PORT="$HTTP_PORT"
export ZO_GRPC_PORT="$GRPC_PORT"

# Rotate the ingest tail into immutable data files promptly.
export ZO_MAX_FILE_RETENTION_TIME="${ZO_MAX_FILE_RETENTION_TIME:-60}"
# Reclaim compacted source files quickly so the three-way billion-row run does
# not retain hours of obsolete Parquet/Vortex data. O2's delayed-deletion job
# makes merged inputs eligible for physical deletion after this interval.
export ZO_COMPACT_DELETE_FILES_DELAY_MINUTES="${ZO_COMPACT_DELETE_FILES_DELAY_MINUTES:-10}"

# Identical index configuration for both O2 formats and equivalent coverage to
# schemas/clickhouse.sql.
export ZO_ENABLE_INVERTED_INDEX="${ZO_ENABLE_INVERTED_INDEX:-true}"
export ZO_FEATURE_FULLTEXT_EXTRA_FIELDS="${ZO_FEATURE_FULLTEXT_EXTRA_FIELDS:-message}"
export ZO_FEATURE_INDEX_EXTRA_FIELDS="${ZO_FEATURE_INDEX_EXTRA_FIELDS:-trace_id,span_id,kubernetes_pod_name}"
export ZO_BLOOM_FILTER_ENABLED="${ZO_BLOOM_FILTER_ENABLED:-true}"
export ZO_FEATURE_BLOOM_FILTER_EXTRA_FIELDS="${ZO_FEATURE_BLOOM_FILTER_EXTRA_FIELDS:-trace_id}"
export ZO_BLOOM_FILTER_FPP="${ZO_BLOOM_FILTER_FPP:-0.01}"
# Parquet-only embedded bloom would make the two O2 index stacks different.
# Use O2's external .bf pruning layer for both formats instead.
export ZO_BLOOM_FILTER_PARQUET_ENABLED="${ZO_BLOOM_FILTER_PARQUET_ENABLED:-false}"
export ZO_RESULT_CACHE_ENABLED=false

if [[ ! -x "$BIN" && -z "$(command -v "$BIN" 2>/dev/null || true)" ]]; then
  echo "OpenObserve binary not found at '$BIN'." >&2
  echo "Install it with scripts/install.sh openobserve or set OPENOBSERVE_BIN." >&2
  exit 1
fi

echo "starting $BACKEND (single-node)"
echo "  file format         : $ZO_FILE_FORMAT"
echo "  data dir            : $ZO_DATA_DIR"
echo "  inverted index      : $ZO_ENABLE_INVERTED_INDEX"
echo "  full-text fields +  : $ZO_FEATURE_FULLTEXT_EXTRA_FIELDS"
echo "  secondary idx fields: $ZO_FEATURE_INDEX_EXTRA_FIELDS"
echo "  bloom fields        : $ZO_FEATURE_BLOOM_FILTER_EXTRA_FIELDS"
echo "  bloom FPP           : $ZO_BLOOM_FILTER_FPP"
echo "  parquet embedded bf : $ZO_BLOOM_FILTER_PARQUET_ENABLED"
echo "  wal retention (s)   : $ZO_MAX_FILE_RETENTION_TIME"
echo "  merged delete delay : ${ZO_COMPACT_DELETE_FILES_DELAY_MINUTES} min"
echo "  listening on        : http://localhost:$ZO_HTTP_PORT (gRPC $ZO_GRPC_PORT)"
exec "$BIN"
