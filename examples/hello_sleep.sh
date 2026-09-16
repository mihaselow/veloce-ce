#!/usr/bin/env bash
# Submit a tiny named job. Requires a running cluster and a configured `veloce` CLI
# (see docs/quickstart.md).
set -euo pipefail

out="$(veloce submit --json --name hello-sleep --nodes 1 --cores 1 --mem 512 /bin/sleep 5)"
echo "$out"
id="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["job_id"])' <<<"$out")"
veloce jobs show "$id"
veloce jobs logs "$id"
