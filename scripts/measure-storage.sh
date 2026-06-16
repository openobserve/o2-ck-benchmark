#!/usr/bin/env bash
# Measure on-disk storage and index sizes for both backends.
#
#   ClickHouse: queried from system.parts / system.data_skipping_indices.
#   OpenObserve: queried from its metastore db/metadata.sqlite `file_list` table
#                (original_size / compressed_size / index_size per parquet file) —
#                the engine's own accounting; falls back to filesystem du.
#
# Usage:
#   scripts/measure-storage.sh \
#     [--ch-url http://localhost:8123] [--ch-database default] [--table k8s_logs] \
#     [--ch-user default] [--ch-password ''] \
#     [--oo-data-dir ./openobserve-data] [--org default]
set -euo pipefail

CH_URL="http://localhost:8123"
CH_DB="default"
CH_USER="default"
CH_PASSWORD=""
TABLE="k8s_logs"
OO_DATA_DIR="./openobserve-data"
ORG="default"
OUT="results/storage.md"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --ch-url) CH_URL="$2"; shift 2;;
    --ch-database) CH_DB="$2"; shift 2;;
    --ch-user) CH_USER="$2"; shift 2;;
    --ch-password) CH_PASSWORD="$2"; shift 2;;
    --table) TABLE="$2"; shift 2;;
    --oo-data-dir) OO_DATA_DIR="$2"; shift 2;;
    --org) ORG="$2"; shift 2;;
    --out) OUT="$2"; shift 2;;
    *) echo "unknown arg: $1" >&2; exit 1;;
  esac
done

mkdir -p "$(dirname "$OUT")"

ch() {
  curl -sS "${CH_URL%/}/?user=${CH_USER}&password=${CH_PASSWORD}" --data-binary "$1"
}

{
  echo "# Storage & index size"
  echo
  echo "Generated: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo
  echo "## ClickHouse (\`${CH_DB}.${TABLE}\`)"
  echo
  echo '```'
  ch "SELECT
        formatReadableSize(sum(data_compressed_bytes))   AS compressed,
        formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed,
        round(sum(data_uncompressed_bytes) / sum(data_compressed_bytes), 2) AS ratio,
        sum(rows) AS rows
      FROM system.parts
      WHERE database = '${CH_DB}' AND table = '${TABLE}' AND active
      FORMAT Vertical" || echo "(clickhouse query failed)"
  echo '```'
  echo
  echo "Skip-index sizes:"
  echo
  echo '```'
  ch "SELECT
        name,
        type_full,
        formatReadableSize(sum(data_compressed_bytes)) AS compressed
      FROM system.data_skipping_indices
      WHERE database = '${CH_DB}' AND table = '${TABLE}'
      GROUP BY name, type_full
      ORDER BY name
      FORMAT PrettyCompactMonoBlock" || echo "(clickhouse query failed)"
  echo '```'
  echo
  echo "## OpenObserve (stream \`${TABLE}\`, org \`${ORG}\`)"
  echo
  if [[ -d "$OO_DATA_DIR" ]]; then
    OO_DB="${OO_DATA_DIR}/db/metadata.sqlite"
    STREAM_KEY="${ORG}/logs/${TABLE}"
    echo "Authoritative sizes from the metastore (db/metadata.sqlite, \`file_list\` table)."
    echo "original_size = raw ingested bytes, compressed_size = parquet on disk,"
    echo "index_size = tantivy (.ttv). Only compacted files are listed (WAL excluded)."
    echo
    echo '```'
    if command -v sqlite3 >/dev/null && [[ -f "$OO_DB" ]]; then
      sqlite3 -header -column "file:${OO_DB}?mode=ro" "
        SELECT
          count(*)                                              AS files,
          sum(records)                                          AS records,
          printf('%.2f GiB', sum(original_size)/1073741824.0)   AS uncompressed,
          printf('%.2f GiB', sum(compressed_size)/1073741824.0) AS parquet_on_disk,
          printf('%.2f GiB', sum(index_size)/1073741824.0)      AS tantivy_index,
          printf('%.2fx',  1.0*sum(original_size)/sum(compressed_size))  AS compression,
          printf('%.2f%%', 100.0*sum(index_size)/sum(original_size))     AS index_pct_of_raw
        FROM file_list WHERE stream='${STREAM_KEY}' AND deleted=false;" \
        || echo "(file_list query failed)"
    else
      echo "(sqlite3 or ${OO_DB} not available — falling back to filesystem du)"
      find "$OO_DATA_DIR" -name '*.parquet' -type f -exec du -ch {} + 2>/dev/null | tail -1
      find "$OO_DATA_DIR" \( -name '*.ttv' -o -name '*.idx' -o -name '*.fst' \) \
        -type f -exec du -ch {} + 2>/dev/null | tail -1
    fi
    echo '```'
    echo
    echo "Physical dir breakdown (incl. WAL not yet compacted):"
    echo
    echo '```'
    du -sh "$OO_DATA_DIR"/* 2>/dev/null | sort -rh || true
    echo '```'
  else
    echo "_OpenObserve data dir not found: \`$OO_DATA_DIR\` — pass --oo-data-dir._"
  fi
} | tee "$OUT"

echo
echo "wrote $OUT"
