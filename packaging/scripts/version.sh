#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
crate_version() {
  grep -m1 '^version = ' "$ROOT/controller/Cargo.toml" | cut -d'"' -f2
}
case "${1:-}" in
  print) crate_version ;;
  check-tag)
    tag="${2:?tag required}"
    want="${tag#v}"
    got="$(crate_version)"
    if [[ "$got" != "$want" ]]; then
      echo "version mismatch: tag=$want crate=$got" >&2
      exit 1
    fi
    ;;
  *)
    echo "usage: $0 print|check-tag <tag>" >&2
    exit 2
    ;;
esac
