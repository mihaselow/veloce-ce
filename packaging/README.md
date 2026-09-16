# Packaging (Community Edition)

Role packages for Linux **amd64**. Built by [`.github/workflows/release.yml`](../.github/workflows/release.yml) on every `v*` tag (attached to the GitHub Release) and on manual **workflow_dispatch** (workflow artifacts only).

## Packages

| Package | Binaries | Sample config | systemd |
|---------|----------|---------------|---------|
| `veloce-controller` | `veloce-controller` | `/etc/veloce/veloce.toml.example` | `veloce-controller.service` |
| `veloce-worker` | `veloce-worker` | `/etc/veloce/veloce-worker.toml.example` | `veloce-worker.service` |
| `veloce-fileserver` | `veloce-fileserver` | `/etc/veloce/veloce-fileserver.toml.example` | `veloce-fileserver.service` |
| `veloce-cli` | `veloce`, `veloce-exec` | `/etc/veloce/cli-config.toml.example` | none |

Formats per package: `.deb`, `.rpm`, and `.tar.gz`, plus a release-wide `SHA256SUMS`.

## Install notes

1. Install only what the node needs (workers do not need the controller package).
2. Copy `*.example` configs to the live path your process expects (often cwd or `/etc/veloce/…` without `.example`), then edit secrets.
3. Create `/var/lib/veloce` for the systemd working directory.
4. Units are **not** enabled by package scripts. After install:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now veloce-controller   # or worker / fileserver
```

5. [Apptainer](https://apptainer.org/) remains **optional** and must be installed on each **worker host** that runs `--image` / `.sif` jobs. Packages never bundle Apptainer.

## Dry-run

Actions → **Release packages** → **Run workflow**. Download the `veloce-ce-linux-amd64-*` artifact. Dispatch runs do not modify a GitHub Release.

## Limits (v1)

- **amd64 only.** An `arm64` matrix can be added later without changing package names.
- No apt/yum repository hosting — GitHub Release assets only.
- No code signing (Cosign/GPG) yet.
- `veloce-slurm` and the WASM dashboard are not packaged here.

## Local packaging (optional)

With release binaries in `target/release` (or `BIN_DIR`):

```bash
export VERSION="$(./packaging/scripts/version.sh print)" ARCH=amd64
for p in veloce-controller veloce-worker veloce-fileserver veloce-cli; do
  ./packaging/scripts/stage-root.sh "$p" "build/stage/$p"
  ./packaging/scripts/make-tarball.sh "$p"
  nfpm package -f "packaging/nfpm/${p}.yaml" -p deb -t dist/
  nfpm package -f "packaging/nfpm/${p}.yaml" -p rpm -t dist/
done
```
