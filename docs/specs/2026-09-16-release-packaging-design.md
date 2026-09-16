# Design: CE release packaging pipeline

**Date:** 2026-09-16  
**Repo:** [veloce-ce](https://github.com/mihaselow/veloce-ce)  
**Status:** Approved for implementation planning

## Goal

On every `v*` GitHub tag, build linux/amd64 release binaries and attach installable artifacts (`.tar.gz`, `.deb`, `.rpm`) to that GitHub Release. Support `workflow_dispatch` dry-runs that produce the same artifacts without requiring a tag upload.

## Decisions

| Topic | Choice |
|-------|--------|
| Triggers | `v*` tags (auto-attach to Release) **and** `workflow_dispatch` (artifacts only unless a tag context is present) |
| Package slice | Role packages: `veloce-controller`, `veloce-worker`, `veloce-fileserver`, `veloce-cli` |
| Arch | `linux/amd64` first; document an arm64 matrix hook for later |
| Contents | Binaries + sample configs + systemd unit templates (no auto-enable) |
| Tooling | `nfpm` for `.deb`/`.rpm`; small script for matching `.tar.gz` layouts |
| Out of scope | `veloce-slurm`, WASM dashboard zip, Docker images, signing, apt/yum hosting, Windows/macOS |

## Pipeline

Single GitHub Actions job on `ubuntu-latest` (`.github/workflows/release.yml`):

1. Checkout; install system deps used by CI (`protobuf-compiler`, `libssl-dev`, `pkg-config`).
2. Install Rust stable (aligned with `rust-toolchain.toml`).
3. Resolve version from the git tag (`v1.0.0-beta.2` → `1.0.0-beta.2`). On `workflow_dispatch` without a tag, use the crate version from `controller/Cargo.toml` and label artifacts as a dry-run build id (run number / short SHA); do not upload to a Release unless `github.ref` is a `v*` tag.
4. **Version guard (tag builds only):** fail if tag version ≠ `version` in `controller/Cargo.toml` (workspace packages share this version).
5. `cargo build --locked --release -p veloce-controller -p veloce-worker -p veloce-fileserver -p veloce-cli`.
6. Install `nfpm`; package each of the four packages to `.deb` and `.rpm`.
7. Build one `.tar.gz` per package with the same file layout as the packages.
8. Emit `SHA256SUMS` for all artifacts.
9. Smoke: `sha256sum -c SHA256SUMS`; for each `.deb`, `dpkg-deb -I` / `dpkg-deb -c` and assert binary + unit paths (CLI has no unit; assert both `veloce` and `veloce-exec`).
10. Upload: tag → GitHub Release assets; dispatch dry-run → workflow artifacts only.

## Package layout

Install prefix uses FHS paths suitable for distro packages:

| Package | Binary path | Sample config | systemd |
|---------|-------------|---------------|---------|
| `veloce-controller` | `/usr/bin/veloce-controller` | `/etc/veloce/veloce.toml.example` | `/lib/systemd/system/veloce-controller.service` |
| `veloce-worker` | `/usr/bin/veloce-worker` | `/etc/veloce/veloce-worker.toml.example` | `/lib/systemd/system/veloce-worker.service` |
| `veloce-fileserver` | `/usr/bin/veloce-fileserver` | `/etc/veloce/veloce-fileserver.toml.example` | `/lib/systemd/system/veloce-fileserver.service` |
| `veloce-cli` | `/usr/bin/veloce` and `/usr/bin/veloce-exec` | `/etc/veloce/cli-config.toml.example` | none |

- Sample TOMLs come from `examples/` at pack time (renamed to `*.example`).
- systemd units are CE-owned templates under `packaging/systemd/`; packages must **not** enable or start services in postinst. Docs show `systemctl enable --now …`.
- Maintainer metadata: `Michael Haselow <hello@veloce-hpc.io>`. License metadata: Business Source License 1.1. Each tarball includes `LICENSE` at archive root.
- Artifact naming (illustrative): `veloce-controller_1.0.0~beta.2_amd64.deb`, `veloce-controller-1.0.0_beta.2-1.x86_64.rpm`, `veloce-controller-1.0.0-beta.2-linux-amd64.tar.gz` (exact nfpm version munging follows nfpm defaults for semver prereleases).

## Repo layout

```
packaging/
  README.md                 # install/enable notes; Apptainer optional on workers
  systemd/*.service
  nfpm/*.yaml               # one YAML per package
  scripts/make-tarball.sh   # shared staging → .tar.gz
.github/workflows/release.yml
docs/specs/2026-09-16-release-packaging-design.md
```

## Docs & release process

- Link packaging from root `README.md` and `CHANGELOG.md` (“download packages from GitHub Releases”).
- Existing tag `v1.0.0-beta.2` is **not** automatically rebuilt; next `v*` tag (or a deliberate re-run / retag if requested) is the first artifact-bearing release.
- `packaging/README.md` notes future `linux/arm64` as a workflow matrix extension.

## Non-goals (v1 of this pipeline)

- Cosign / GPG signing
- Hosted apt or yum repositories
- Multi-arch builds in CI
- Bundling the WASM dashboard or Apptainer itself

## Success criteria

- Pushing `vX.Y.Z` (when crate version matches) produces four packages × (tar.gz + deb + rpm) plus `SHA256SUMS` on the Release.
- `workflow_dispatch` on `main` produces the same artifact set as workflow artifacts without mutating a Release.
- A worker node can install only `veloce-worker` (+ optional Apptainer on the host); a head node can install controller + fileserver + cli without worker bits.
