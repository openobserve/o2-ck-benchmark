#!/usr/bin/env bash
# Download the ClickHouse and OpenObserve single-node binaries into ./bin/
# Detects the current OS/CPU for OpenObserve. Binaries are git-ignored.
#
# Usage:
#   scripts/install.sh            # both
#   scripts/install.sh clickhouse # just one
#   scripts/install.sh openobserve
set -euo pipefail

mkdir -p bin
WHAT="${1:-all}"

# OpenObserve open-source release (https://downloads.openobserve.ai). Override with
# OPENOBSERVE_VERSION=vX.Y.Z to pin a different version.
OPENOBSERVE_VERSION="${OPENOBSERVE_VERSION:-v0.92.0-rc2}"

detect_openobserve_platform() {
  local os arch
  os="$(uname -s)"
  arch="$(uname -m)"

  case "$os" in
    Linux)
      case "$arch" in
        aarch64|arm64) echo "linux-arm64" ;;
        x86_64|amd64)  echo "linux-amd64" ;;
        *) echo "unsupported Linux architecture: $arch" >&2; return 1 ;;
      esac
      ;;
    Darwin)
      case "$arch" in
        arm64|aarch64) echo "darwin-arm64" ;;
        x86_64|amd64)  echo "darwin-amd64" ;;
        *) echo "unsupported macOS architecture: $arch" >&2; return 1 ;;
      esac
      ;;
    *)
      echo "unsupported OS for OpenObserve binary download: $os" >&2
      return 1
      ;;
  esac
}

install_clickhouse() {
  if [[ -x bin/clickhouse ]]; then
    echo "clickhouse already present: $(./bin/clickhouse --version | head -1)"
    return
  fi
  echo "downloading ClickHouse..."
  ( cd bin && curl -fsSL https://clickhouse.com/ | sh )
  echo "clickhouse: $(./bin/clickhouse --version | head -1)"
}

install_openobserve() {
  if [[ -x bin/openobserve ]]; then
    if version_out="$(./bin/openobserve --version 2>/dev/null)"; then
      echo "openobserve already present: $version_out"
      return
    fi
    echo "existing bin/openobserve is not runnable on this host; replacing it"
  fi

  local platform tarball url
  platform="$(detect_openobserve_platform)"
  tarball="openobserve-${OPENOBSERVE_VERSION}-${platform}.tar.gz"
  url="https://downloads.openobserve.ai/releases/openobserve/${OPENOBSERVE_VERSION}/${tarball}"
  echo "downloading OpenObserve ${OPENOBSERVE_VERSION} for ${platform} (open source)..."
  curl -fsSL "$url" -o "/tmp/${tarball}"
  tar -xzf "/tmp/${tarball}" -C bin
  rm -f "/tmp/${tarball}"
  chmod +x bin/openobserve
  echo "openobserve: $(./bin/openobserve --version)"
}

case "$WHAT" in
  all)          install_clickhouse; install_openobserve;;
  clickhouse)   install_clickhouse;;
  openobserve)  install_openobserve;;
  *) echo "unknown target: $WHAT (use: all | clickhouse | openobserve)" >&2; exit 1;;
esac
