#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
PKG="${1:?package}"
VERSION="${VERSION:?VERSION required}"
ARCH="${ARCH:-amd64}"
STAGE="${STAGE_DIR:-$ROOT/build/stage/$PKG}"
OUT_DIR="${OUT_DIR:-$ROOT/dist}"
mkdir -p "$OUT_DIR"
"$ROOT/packaging/scripts/stage-root.sh" "$PKG" "$STAGE"
install -m 644 "$ROOT/LICENSE" "$STAGE/LICENSE"
TAR_NAME="${PKG}-${VERSION}-linux-${ARCH}.tar.gz"
tar -C "$STAGE" -czf "$OUT_DIR/$TAR_NAME" .
echo "$OUT_DIR/$TAR_NAME"
