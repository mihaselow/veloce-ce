# Sample configuration

Copy these into the directory you run the daemons from (see [docs/quickstart.md](../docs/quickstart.md)). Replace every `CHANGE_ME_*` placeholder. Generate `cert.pem` / `key.pem` in that same directory.

| File | Loaded by |
|------|-----------|
| [`veloce.toml`](veloce.toml) | `veloce-controller` (cwd) |
| [`veloce-web.toml`](veloce-web.toml) | `veloce-controller` (cwd; public dashboard + API key wiring) |
| [`veloce-worker.toml`](veloce-worker.toml) | `veloce-worker` (cwd) |
| [`veloce-fileserver.toml`](veloce-fileserver.toml) | `veloce-fileserver` (cwd) |
| [`cli-config.toml`](cli-config.toml) | `veloce` when copied to `~/.veloce/config.toml` |
| [`hello_sleep.sh`](hello_sleep.sh) | Submit `/bin/sleep 5` once the CLI can reach a controller |

Environment variables named in the comments override the TOML fields of the same name when set.
