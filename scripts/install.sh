#!/usr/bin/env bash
# Download the ClickHouse and OpenObserve single-node binaries into ./bin/
# Detects the current OS/CPU for both binaries. Binaries are git-ignored.
#
# Usage:
#   scripts/install.sh            # both
#   scripts/install.sh clickhouse # just one
#   scripts/install.sh openobserve
set -euo pipefail

mkdir -p bin
WHAT="${1:-all}"

# Benchmark version pins. ClickHouse 26.7 is a stable release with the GA text
# index used by schemas/clickhouse.sql. OpenObserve can still be overridden for
# an explicit compatibility run, but the reproducible default is v0.92.2.
CLICKHOUSE_VERSION="26.7.3.19"
CLICKHOUSE_RELEASE_TAG="v${CLICKHOUSE_VERSION}-stable"
OPENOBSERVE_VERSION="${OPENOBSERVE_VERSION:-v0.92.2}"

verify_sha256() {
  local file="$1" expected="$2" actual
  if command -v sha256sum >/dev/null 2>&1; then
    actual="$(sha256sum "$file" | awk '{print $1}')"
  elif command -v shasum >/dev/null 2>&1; then
    actual="$(shasum -a 256 "$file" | awk '{print $1}')"
  else
    echo "sha256sum or shasum is required to verify ClickHouse" >&2
    return 1
  fi
  if [[ "$actual" != "$expected" ]]; then
    echo "ClickHouse checksum mismatch: expected $expected, got $actual" >&2
    return 1
  fi
}

detect_clickhouse_asset() {
  local os arch
  os="$(uname -s)"
  arch="$(uname -m)"

  case "$os:$arch" in
    Darwin:arm64|Darwin:aarch64)
      echo "clickhouse-macos-aarch64|53644a8269e372bfde0a78c8ffd9baad256e9c17bddbc2db43459dfa7256efd3|binary"
      ;;
    Darwin:x86_64|Darwin:amd64)
      echo "clickhouse-macos|4680a21fa259542c8b04943b84e3106d444e28c754ce1c7e978d96a96e93434f|binary"
      ;;
    Linux:aarch64|Linux:arm64)
      echo "clickhouse-common-static-${CLICKHOUSE_VERSION}-arm64.tgz|a6e165c4dc5fa42fc26cf28f99c1845736cb4dcf866c0c236d67a45cf0aa77e8|archive"
      ;;
    Linux:x86_64|Linux:amd64)
      echo "clickhouse-common-static-${CLICKHOUSE_VERSION}-amd64.tgz|3338b507932c1616fda42d5fdd8ee6541bf2c3ecf8a6767ebf5814014f120c9f|archive"
      ;;
    *)
      echo "unsupported ClickHouse platform: $os/$arch" >&2
      return 1
      ;;
  esac
}

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

install_clickhouse() (
  if [[ -x bin/clickhouse ]]; then
    version_out="$(./bin/clickhouse --version 2>/dev/null || true)"
    if [[ "$version_out" == *"$CLICKHOUSE_VERSION"* ]]; then
      echo "clickhouse already present: $version_out"
      return
    fi
    echo "replacing ClickHouse binary: ${version_out:-unreadable} -> $CLICKHOUSE_VERSION"
  fi

  local asset sha256 kind url tmp_dir source version_out
  IFS='|' read -r asset sha256 kind <<< "$(detect_clickhouse_asset)"
  url="https://github.com/ClickHouse/ClickHouse/releases/download/${CLICKHOUSE_RELEASE_TAG}/${asset}"
  tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/o2-benchmark-clickhouse.XXXXXX")"
  trap 'rm -rf "$tmp_dir"' EXIT

  echo "downloading ClickHouse ${CLICKHOUSE_RELEASE_TAG} ($asset)..."
  curl -fsSL "$url" -o "$tmp_dir/$asset"
  verify_sha256 "$tmp_dir/$asset" "$sha256"

  if [[ "$kind" == "archive" ]]; then
    tar -xzf "$tmp_dir/$asset" -C "$tmp_dir"
    source="$(find "$tmp_dir" -type f -path '*/usr/bin/clickhouse' -print -quit)"
    if [[ -z "$source" ]]; then
      echo "clickhouse binary not found in $asset" >&2
      return 1
    fi
  else
    source="$tmp_dir/$asset"
  fi

  install -m 0755 "$source" bin/clickhouse
  version_out="$(./bin/clickhouse --version | head -1)"
  if [[ "$version_out" != *"$CLICKHOUSE_VERSION"* ]]; then
    echo "unexpected ClickHouse version after install: $version_out" >&2
    return 1
  fi
  echo "clickhouse: $version_out"
)

install_openobserve() (
  if [[ -x bin/openobserve ]]; then
    if version_out="$(./bin/openobserve --version 2>/dev/null)"; then
      if [[ "$version_out" == *"${OPENOBSERVE_VERSION#v}"* ]]; then
        echo "openobserve already present: $version_out"
        return
      fi
      echo "replacing OpenObserve binary: $version_out -> $OPENOBSERVE_VERSION"
    else
      echo "existing bin/openobserve is not runnable on this host; replacing it"
    fi
  fi

  local platform tarball url tmp_dir source version_out
  platform="$(detect_openobserve_platform)"
  tarball="openobserve-${OPENOBSERVE_VERSION}-${platform}.tar.gz"
  url="https://downloads.openobserve.ai/releases/openobserve/${OPENOBSERVE_VERSION}/${tarball}"
  tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/o2-benchmark-openobserve.XXXXXX")"
  trap 'rm -rf "$tmp_dir"' EXIT

  echo "downloading OpenObserve ${OPENOBSERVE_VERSION} for ${platform} (open source)..."
  curl -fsSL "$url" -o "$tmp_dir/$tarball"
  tar -xzf "$tmp_dir/$tarball" -C "$tmp_dir"
  source="$(find "$tmp_dir" -type f -name openobserve -print -quit)"
  if [[ -z "$source" ]]; then
    echo "openobserve binary not found in $tarball" >&2
    return 1
  fi
  install -m 0755 "$source" bin/openobserve
  version_out="$(./bin/openobserve --version)"
  if [[ "$version_out" != *"${OPENOBSERVE_VERSION#v}"* ]]; then
    echo "unexpected OpenObserve version after install: $version_out" >&2
    return 1
  fi
  echo "openobserve: $version_out"
)

case "$WHAT" in
  all)          install_clickhouse; install_openobserve;;
  clickhouse)   install_clickhouse;;
  openobserve)  install_openobserve;;
  *) echo "unknown target: $WHAT (use: all | clickhouse | openobserve)" >&2; exit 1;;
esac
