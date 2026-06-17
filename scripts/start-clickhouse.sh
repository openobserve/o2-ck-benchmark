#!/usr/bin/env bash
# Start single-node ClickHouse from the standalone binary, keeping all state
# (data, logs, access control, preprocessed config) under one repo-local dir so
# it is easy to inspect, measure (scripts/measure-storage.sh), and wipe.
#
# Usage:
#   scripts/start-clickhouse.sh                 # foreground, uses ./bin/clickhouse
#   CLICKHOUSE_BIN=clickhouse scripts/start-clickhouse.sh
#
# HTTP:   http://127.0.0.1:8123
# Native: 127.0.0.1:9000   (./bin/clickhouse client)
set -euo pipefail

BIN="${CLICKHOUSE_BIN:-./bin/clickhouse}"
DATA_DIR="${CH_DATA_DIR:-./clickhouse-data}"
HTTP_PORT="${CH_HTTP_PORT:-8123}"
TCP_PORT="${CH_TCP_PORT:-9000}"

if [[ ! -x "$BIN" && -z "$(command -v "$BIN" 2>/dev/null || true)" ]]; then
  echo "ClickHouse binary not found at '$BIN'." >&2
  echo "Install it with:  (cd bin && curl -fsSL https://clickhouse.com/ | sh)" >&2
  exit 1
fi

mkdir -p "$DATA_DIR"
DATA_DIR="$(cd "$DATA_DIR" && pwd -P)"

# Listen addresses. Configure explicitly with CH_LISTEN_HOSTS (comma- or
# space-separated list), e.g. for a remote datagen over the private network:
#   CH_LISTEN_HOSTS="127.0.0.1,172.31.4.214" scripts/start-clickhouse.sh
# Default: localhost + this machine's auto-detected LAN/internal IP. Avoid
# 0.0.0.0 — the server is unauthenticated; never expose a public interface.
# Repeated --listen_host CLI flags are last-wins in ClickHouse, so multiple
# addresses must go through a config file.
if [[ -n "${CH_LISTEN_HOSTS:-}" ]]; then
  LISTEN_HOSTS="${CH_LISTEN_HOSTS//,/ }"
else
  # Linux: `hostname -I`. macOS: query the interface backing the default route
  # (often en1/Wi-Fi, not en0). The trailing `|| true` on each lookup keeps a
  # failure from tripping `set -e` — `route get` is macOS-only and errors on
  # Linux; an empty LAN_IP just means we listen on localhost only.
  MAC_IF="$(route -n get default 2>/dev/null | awk '/interface:/{print $2}' || true)"
  LAN_IP="$( (hostname -I 2>/dev/null || ipconfig getifaddr "${MAC_IF:-en0}" 2>/dev/null || true) | awk '{print $1}' || true)"
  LISTEN_HOSTS="127.0.0.1${LAN_IP:+ $LAN_IP}"
fi
LISTEN_CONF="${DATA_DIR}/listen.xml"
{
  echo "<clickhouse>"
  for h in $LISTEN_HOSTS; do
    echo "  <listen_host>${h}</listen_host>"
  done
  # --config-file REPLACES the embedded config, so the minimal users/profiles
  # sections the embedded config would have provided must be restated here.
  cat <<'XML'
  <profiles><default/></profiles>
  <users>
    <default>
      <password></password>
      <networks><ip>::/0</ip></networks>
      <profile>default</profile>
      <quota>default</quota>
    </default>
  </users>
  <quotas><default/></quotas>
  <background_pool_size>32</background_pool_size>
  <logger>
    <level>information</level>
  </logger>
</clickhouse>
XML
} > "$LISTEN_CONF"

echo "starting ClickHouse (single-node)"
echo "  data dir : $DATA_DIR"
echo "  listen   : ${LISTEN_HOSTS// /, }"
echo "  http     : http://127.0.0.1:${HTTP_PORT}"
echo "  native   : 127.0.0.1:${TCP_PORT}"

# Everything after `--` overrides the config. Pinning the access and
# user-directory paths keeps ClickHouse from scattering dirs into the cwd.
exec "$BIN" server --config-file="$LISTEN_CONF" -- \
  --path="${DATA_DIR}/" \
  --tmp_path="${DATA_DIR}/tmp/" \
  --user_files_path="${DATA_DIR}/user_files/" \
  --format_schema_path="${DATA_DIR}/format_schemas/" \
  --http_port="${HTTP_PORT}" \
  --tcp_port="${TCP_PORT}" \
  --mysql_port= \
  --postgresql_port= \
  --logger.log="${DATA_DIR}/clickhouse-server.log" \
  --logger.errorlog="${DATA_DIR}/clickhouse-server.err.log" \
  --user_directories.local_directory.path="${DATA_DIR}/access/"
