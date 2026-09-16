#!/usr/bin/env bash
set -euo pipefail
DIST="${1:-dist}"

assert_deb_has() {
  local deb="$1"
  shift
  local listing path
  listing="$(dpkg-deb -c "$deb")"
  dpkg-deb -I "$deb" >/dev/null
  for path in "$@"; do
    if ! grep -F "./$path" <<<"$listing" >/dev/null; then
      echo "missing in $deb: ./$path" >&2
      exit 1
    fi
  done
}

shopt -s nullglob
debs=("$DIST"/*.deb)
if [[ ${#debs[@]} -eq 0 ]]; then
  echo "no debs in $DIST" >&2
  exit 1
fi

for deb in "${debs[@]}"; do
  base="$(basename "$deb")"
  case "$base" in
    veloce-controller_*)
      assert_deb_has "$deb" usr/bin/veloce-controller \
        etc/veloce/veloce.toml.example \
        lib/systemd/system/veloce-controller.service
      ;;
    veloce-worker_*)
      assert_deb_has "$deb" usr/bin/veloce-worker \
        etc/veloce/veloce-worker.toml.example \
        lib/systemd/system/veloce-worker.service
      ;;
    veloce-fileserver_*)
      assert_deb_has "$deb" usr/bin/veloce-fileserver \
        etc/veloce/veloce-fileserver.toml.example \
        lib/systemd/system/veloce-fileserver.service
      ;;
    veloce-cli_*)
      assert_deb_has "$deb" usr/bin/veloce usr/bin/veloce-exec \
        etc/veloce/cli-config.toml.example
      ;;
    *)
      echo "unexpected deb name: $base" >&2
      exit 1
      ;;
  esac
done
echo "smoke-debs: ok (${#debs[@]} packages)"
