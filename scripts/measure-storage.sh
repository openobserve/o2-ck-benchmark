#!/usr/bin/env bash
# Measure storage and index sizes for ClickHouse, O2-Parquet, and O2-Vortex.
#
# Usage:
#   scripts/measure-storage.sh \
#     [--ch-url http://localhost:8123] [--ch-database default] \
#     [--table k8s_logs] [--ch-user default] [--ch-password ''] \
#     [--o2-parquet-data-dir ./openobserve-parquet-data] \
#     [--o2-vortex-data-dir ./openobserve-vortex-data] [--org default]
set -euo pipefail

CH_URL="http://localhost:8123"
CH_DB="default"
CH_USER="default"
CH_PASSWORD=""
TABLE="k8s_logs"
O2_PARQUET_DATA_DIR="./openobserve-parquet-data"
O2_VORTEX_DATA_DIR="./openobserve-vortex-data"
ORG="default"
OUT="results/storage.md"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --ch-url) CH_URL="$2"; shift 2;;
    --ch-database) CH_DB="$2"; shift 2;;
    --ch-user) CH_USER="$2"; shift 2;;
    --ch-password) CH_PASSWORD="$2"; shift 2;;
    --table) TABLE="$2"; shift 2;;
    --o2-parquet-data-dir) O2_PARQUET_DATA_DIR="$2"; shift 2;;
    --o2-vortex-data-dir) O2_VORTEX_DATA_DIR="$2"; shift 2;;
    --org) ORG="$2"; shift 2;;
    --out) OUT="$2"; shift 2;;
    *) echo "unknown arg: $1" >&2; exit 1;;
  esac
done

mkdir -p "$(dirname "$OUT")"

ch() {
  curl -sS "${CH_URL%/}/?user=${CH_USER}&password=${CH_PASSWORD}" --data-binary "$1"
}

emit_o2() {
  local backend="$1"
  local format="$2"
  local data_dir="$3"
  local db="${data_dir}/db/metadata.sqlite"
  local stream_key="${ORG}/logs/${TABLE}"

  echo "## ${backend} (ZO_FILE_FORMAT=${format})"
  echo
  if [[ ! -d "$data_dir" ]]; then
    echo "_Data dir not found: \`$data_dir\`._"
    echo
    return
  fi

  echo "Authoritative sizes from \`file_list\`; only compacted files are counted."
  echo
  echo '```'
  if command -v sqlite3 >/dev/null && [[ -f "$db" ]]; then
    sqlite3 -header -column "file:${db}?mode=ro" "
      SELECT
        count(*)                                              AS files,
        sum(records)                                          AS records,
        printf('%.2f GiB', sum(original_size)/1073741824.0)   AS uncompressed,
        printf('%.2f GiB', sum(compressed_size)/1073741824.0) AS data_on_disk,
        printf('%.2f GiB', sum(index_size)/1073741824.0)      AS tantivy_index,
        printf('%.2fx', 1.0*sum(original_size)/sum(compressed_size)) AS compression,
        printf('%.2f%%', 100.0*sum(index_size)/sum(original_size)) AS index_pct_of_raw
      FROM file_list WHERE stream='${stream_key}' AND deleted=false;" \
      || echo "(file_list query failed)"
  else
    echo "(sqlite3 or ${db} unavailable; filesystem fallback)"
    find "$data_dir" \( -name '*.parquet' -o -name '*.vortex' \) \
      -type f -exec du -ch {} + 2>/dev/null | tail -1 || true
    find "$data_dir" \( -name '*.ttv' -o -name '*.idx' -o -name '*.fst' \) \
      -type f -exec du -ch {} + 2>/dev/null | tail -1 || true
  fi
  echo '```'
  echo
  echo "Physical directory breakdown (including WAL):"
  echo
  echo '```'
  du -sh "$data_dir"/* 2>/dev/null | sort -rh || true
  echo '```'
  echo
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
        formatReadableSize(sum(data_compressed_bytes)) AS compressed,
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
  ch "SELECT name, type_full,
        formatReadableSize(sum(data_compressed_bytes)) AS compressed
      FROM system.data_skipping_indices
      WHERE database = '${CH_DB}' AND table = '${TABLE}'
      GROUP BY name, type_full ORDER BY name
      FORMAT PrettyCompactMonoBlock" || echo "(clickhouse query failed)"
  echo '```'
  echo

  emit_o2 "O2-Parquet" "parquet" "$O2_PARQUET_DATA_DIR"
  emit_o2 "O2-Vortex" "vortex" "$O2_VORTEX_DATA_DIR"
} | tee "$OUT"

echo
echo "wrote $OUT"
