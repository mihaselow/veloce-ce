#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
PKG="${1:?package name}"
DEST="${2:?dest root}"
BIN_DIR="${BIN_DIR:-$ROOT/target/release}"

rm -rf "$DEST"
mkdir -p "$DEST/usr/bin" "$DEST/etc/veloce"

stage_bin() {
  local name="$1"
  install -m 755 "$BIN_DIR/$name" "$DEST/usr/bin/$name"
}

case "$PKG" in
  veloce-controller)
    stage_bin veloce-controller
    install -m 644 "$ROOT/examples/veloce.toml" "$DEST/etc/veloce/veloce.toml.example"
    mkdir -p "$DEST/lib/systemd/system"
    install -m 644 "$ROOT/packaging/systemd/veloce-controller.service" \
      "$DEST/lib/systemd/system/veloce-controller.service"
    ;;
  veloce-worker)
    stage_bin veloce-worker
    install -m 644 "$ROOT/examples/veloce-worker.toml" "$DEST/etc/veloce/veloce-worker.toml.example"
    mkdir -p "$DEST/lib/systemd/system"
    install -m 644 "$ROOT/packaging/systemd/veloce-worker.service" \
      "$DEST/lib/systemd/system/veloce-worker.service"
    ;;
  veloce-fileserver)
    stage_bin veloce-fileserver
    install -m 644 "$ROOT/examples/veloce-fileserver.toml" \
      "$DEST/etc/veloce/veloce-fileserver.toml.example"
    mkdir -p "$DEST/lib/systemd/system"
    install -m 644 "$ROOT/packaging/systemd/veloce-fileserver.service" \
      "$DEST/lib/systemd/system/veloce-fileserver.service"
    ;;
  veloce-cli)
    stage_bin veloce
    stage_bin veloce-exec
    install -m 644 "$ROOT/examples/cli-config.toml" "$DEST/etc/veloce/cli-config.toml.example"
    ;;
  *)
    echo "unknown package: $PKG" >&2
    exit 2
    ;;
esac
