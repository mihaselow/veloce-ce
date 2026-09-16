# CE Release Packaging Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a GitHub Actions release pipeline that builds linux/amd64 binaries and attaches `.tar.gz`, `.deb`, and `.rpm` packages (plus `SHA256SUMS`) to `v*` GitHub Releases, with `workflow_dispatch` dry-runs.

**Architecture:** One `ubuntu-latest` job builds four crates with `cargo build --locked --release`, stages FHS layouts under `packaging/`, emits packages via `nfpm`, builds matching tarballs with a shell script, smokes checksums and `dpkg-deb` contents, then uploads to the Release (tags) or workflow artifacts (dispatch).

**Tech Stack:** Rust 1.94 workspace, GitHub Actions, nfpm, `dpkg-deb`, bash.

**Spec:** [docs/specs/2026-09-16-release-packaging-design.md](../specs/2026-09-16-release-packaging-design.md)

## Global Constraints

- Work only in the nested CE repo at `veloce-opensource/` (remote `mihaselow/veloce-ce`); do not modify parent `veloce` product tree for this work.
- Packages: `veloce-controller`, `veloce-worker`, `veloce-fileserver`, `veloce-cli` only (no `veloce-slurm`, no WASM dashboard).
- `veloce-cli` must install **both** `/usr/bin/veloce` and `/usr/bin/veloce-exec`.
- Arch: `linux/amd64` only in v1; document arm64 as a future matrix note.
- Install paths: binaries `/usr/bin/`, configs `/etc/veloce/*.example`, units `/lib/systemd/system/` (no postinst enable/start).
- Maintainer: `Michael Haselow <hello@veloce-hpc.io>`; license metadata Business Source License 1.1.
- Commits in CE: use `git commit-tree` (or equivalent) so Cursor does not inject `Co-authored-by`.
- Do not rebuild/reattach artifacts to existing tag `v1.0.0-beta.2` unless the user explicitly asks.

## File map

| Path | Responsibility |
|------|----------------|
| `packaging/systemd/*.service` | CE systemd unit templates (`ExecStart=/usr/bin/...`) |
| `packaging/nfpm/*.yaml` | One nfpm config per package |
| `packaging/scripts/stage-root.sh` | Stage FHS tree from built binaries + examples + units |
| `packaging/scripts/make-tarball.sh` | Pack staged root → `.tar.gz` (+ LICENSE) |
| `packaging/scripts/version.sh` | Resolve/compare crate vs tag version |
| `packaging/scripts/smoke-debs.sh` | Assert deb contents |
| `packaging/README.md` | Install / enable notes |
| `.github/workflows/release.yml` | Tag + dispatch pipeline |
| `docs/specs/...` | Already written (update only if plan drifts) |
| `README.md`, `CHANGELOG.md`, `docs/README.md` | Discoverability links |

---

### Task 1: systemd unit templates

**Files:**
- Create: `packaging/systemd/veloce-controller.service`
- Create: `packaging/systemd/veloce-worker.service`
- Create: `packaging/systemd/veloce-fileserver.service`

**Interfaces:**
- Consumes: none
- Produces: units with `ExecStart=/usr/bin/<binary>` and `WorkingDirectory=/var/lib/veloce` (worker also documents controller flag)

- [ ] **Step 1: Write the three unit files**

`packaging/systemd/veloce-controller.service`:

```ini
[Unit]
Description=Veloce Controller
After=network.target
Documentation=https://github.com/mihaselow/veloce-ce

[Service]
Type=simple
User=root
ExecStart=/usr/bin/veloce-controller
WorkingDirectory=/var/lib/veloce
Restart=on-failure
Environment=RUST_LOG=info
# Environment=VELOCE_SECRET=

[Install]
WantedBy=multi-user.target
```

`packaging/systemd/veloce-worker.service`:

```ini
[Unit]
Description=Veloce Worker
After=network.target
Documentation=https://github.com/mihaselow/veloce-ce

[Service]
Type=simple
User=root
ExecStart=/usr/bin/veloce-worker --controller 127.0.0.1:9000
WorkingDirectory=/var/lib/veloce
Restart=on-failure
Environment=RUST_LOG=info
# Environment=VELOCE_SECRET=

[Install]
WantedBy=multi-user.target
```

`packaging/systemd/veloce-fileserver.service`:

```ini
[Unit]
Description=Veloce Fileserver
After=network.target
Documentation=https://github.com/mihaselow/veloce-ce

[Service]
Type=simple
User=root
ExecStart=/usr/bin/veloce-fileserver --config /etc/veloce/veloce-fileserver.toml
WorkingDirectory=/var/lib/veloce
Restart=on-failure
Environment=RUST_LOG=info

[Install]
WantedBy=multi-user.target
```

- [ ] **Step 2: Sanity-check paths**

Run: `rg -n 'ExecStart=' packaging/systemd/`
Expected: three lines, all under `/usr/bin/`, none under `/usr/local/bin/`.

- [ ] **Step 3: Commit**

```bash
cd /Users/michaelhaselow/_devel/veloce/veloce-opensource
git add packaging/systemd/
# commit-tree without Co-authored-by; subject:
# Add CE systemd unit templates for packaged /usr/bin paths.
```

---

### Task 2: Version helper + staging script

**Files:**
- Create: `packaging/scripts/version.sh`
- Create: `packaging/scripts/stage-root.sh`
- Test: invoke scripts locally with fake binaries

**Interfaces:**
- Consumes: `controller/Cargo.toml` `version = "…"`, `examples/*.toml`, `packaging/systemd/*.service`, release binaries under `$BIN_DIR` (default `target/release`)
- Produces:
  - `version.sh print` → stdout crate version (e.g. `1.0.0-beta.2`)
  - `version.sh check-tag v1.0.0-beta.2` → exit 0 if matches; else exit 1
  - `stage-root.sh <package> <dest_root>` → populates `$dest_root` with FHS layout for that package

- [ ] **Step 1: Write `version.sh`**

```bash
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
  *) echo "usage: $0 print|check-tag <tag>" >&2; exit 2 ;;
esac
```

- [ ] **Step 2: Write `stage-root.sh`**

```bash
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
  *) echo "unknown package: $PKG" >&2; exit 2 ;;
esac
```

- [ ] **Step 3: Local smoke with fake binaries**

```bash
cd /Users/michaelhaselow/_devel/veloce/veloce-opensource
chmod +x packaging/scripts/*.sh
mkdir -p /tmp/veloce-fake-bin
for b in veloce-controller veloce-worker veloce-fileserver veloce veloce-exec; do
  echo '#!/bin/sh' > "/tmp/veloce-fake-bin/$b"
  chmod +x "/tmp/veloce-fake-bin/$b"
done
BIN_DIR=/tmp/veloce-fake-bin packaging/scripts/stage-root.sh veloce-cli /tmp/veloce-stage-cli
test -x /tmp/veloce-stage-cli/usr/bin/veloce
test -x /tmp/veloce-stage-cli/usr/bin/veloce-exec
test -f /tmp/veloce-stage-cli/etc/veloce/cli-config.toml.example
./packaging/scripts/version.sh print
# expect: 1.0.0-beta.2 (or current crate version)
./packaging/scripts/version.sh check-tag "v$(./packaging/scripts/version.sh print)"
```

Expected: all commands exit 0; both CLI binaries present.

- [ ] **Step 4: Commit**

```bash
git add packaging/scripts/version.sh packaging/scripts/stage-root.sh
# Subject: Add packaging version guard and FHS staging script (incl. veloce-exec).
```

---

### Task 3: Tarball script + nfpm configs

**Files:**
- Create: `packaging/scripts/make-tarball.sh`
- Create: `packaging/nfpm/veloce-controller.yaml`
- Create: `packaging/nfpm/veloce-worker.yaml`
- Create: `packaging/nfpm/veloce-fileserver.yaml`
- Create: `packaging/nfpm/veloce-cli.yaml`

**Interfaces:**
- Consumes: staged root from `stage-root.sh`; env `VERSION` (semver with optional prerelease, e.g. `1.0.0-beta.2`); `ARCH=amd64`
- Produces: `dist/<pkg>-<version>-linux-amd64.tar.gz` and nfpm-built `.deb`/`.rpm` under `dist/`

- [ ] **Step 1: Write `make-tarball.sh`**

```bash
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
```

- [ ] **Step 2: Write nfpm YAMLs**

Use the same contents pattern for each package. Example `packaging/nfpm/veloce-cli.yaml` (both binaries):

```yaml
name: veloce-cli
arch: ${ARCH}
platform: linux
version: ${VERSION}
section: utils
priority: optional
maintainer: Michael Haselow <hello@veloce-hpc.io>
description: Veloce CLI (veloce + veloce-exec)
vendor: Veloce
license: Business Source License 1.1
contents:
  - src: ./build/stage/veloce-cli/usr/bin/veloce
    dst: /usr/bin/veloce
    file_info: { mode: 0755 }
  - src: ./build/stage/veloce-cli/usr/bin/veloce-exec
    dst: /usr/bin/veloce-exec
    file_info: { mode: 0755 }
  - src: ./build/stage/veloce-cli/etc/veloce/cli-config.toml.example
    dst: /etc/veloce/cli-config.toml.example
    file_info: { mode: 0644 }
    type: config|noreplace
```

`veloce-controller.yaml` (mirror for worker/fileserver with their binary, example config, and unit):

```yaml
name: veloce-controller
arch: ${ARCH}
platform: linux
version: ${VERSION}
section: utils
priority: optional
maintainer: Michael Haselow <hello@veloce-hpc.io>
description: Veloce controller
vendor: Veloce
license: Business Source License 1.1
contents:
  - src: ./build/stage/veloce-controller/usr/bin/veloce-controller
    dst: /usr/bin/veloce-controller
    file_info: { mode: 0755 }
  - src: ./build/stage/veloce-controller/etc/veloce/veloce.toml.example
    dst: /etc/veloce/veloce.toml.example
    file_info: { mode: 0644 }
    type: config|noreplace
  - src: ./build/stage/veloce-controller/lib/systemd/system/veloce-controller.service
    dst: /lib/systemd/system/veloce-controller.service
    file_info: { mode: 0644 }
```

Repeat for `veloce-worker` and `veloce-fileserver` with their paths (worker unit + `veloce-worker.toml.example`; fileserver unit + `veloce-fileserver.toml.example`).

- [ ] **Step 3: Local package dry-run (fake bins + nfpm if available)**

```bash
chmod +x packaging/scripts/make-tarball.sh
export VERSION="$(./packaging/scripts/version.sh print)" ARCH=amd64 BIN_DIR=/tmp/veloce-fake-bin
for p in veloce-controller veloce-worker veloce-fileserver veloce-cli; do
  ./packaging/scripts/stage-root.sh "$p" "build/stage/$p"
done
./packaging/scripts/make-tarball.sh veloce-cli
tar -tzf "dist/veloce-cli-${VERSION}-linux-amd64.tar.gz" | rg 'usr/bin/(veloce|veloce-exec)$'
# If nfpm is installed:
#   for p in veloce-controller veloce-worker veloce-fileserver veloce-cli; do
#     nfpm package -f "packaging/nfpm/${p}.yaml" -p deb -t dist/
#     nfpm package -f "packaging/nfpm/${p}.yaml" -p rpm -t dist/
#   done
```

Expected: tarball lists both CLI binaries. Skip nfpm locally if not installed; CI will run it.

- [ ] **Step 4: Commit**

```bash
git add packaging/scripts/make-tarball.sh packaging/nfpm/
# Subject: Add nfpm configs and tarball builder for CE role packages.
```

---

### Task 4: Smoke script for deb contents

**Files:**
- Create: `packaging/scripts/smoke-debs.sh`

**Interfaces:**
- Consumes: `dist/*.deb`
- Produces: exit 0 if each expected path is present; else non-zero with message

- [ ] **Step 1: Write smoke script**

```bash
#!/usr/bin/env bash
set -euo pipefail
DIST="${1:-dist}"

assert_deb_has() {
  local deb="$1"; shift
  local listing
  listing="$(dpkg-deb -c "$deb")"
  local path
  for path in "$@"; do
    if ! grep -qE "[.]${path}$|[.]${path} ->" <<<"$listing" && ! grep -q " ${path}$" <<<"$listing"; then
      # dpkg-deb -c prints paths like ./usr/bin/veloce
      if ! grep -q "\.${path}\$" <<<"$listing" && ! grep -q "\./${path#/}" <<<"$listing"; then
        echo "missing in $deb: $path" >&2
        echo "$listing" >&2
        exit 1
      fi
    fi
  done
  dpkg-deb -I "$deb" >/dev/null
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
```

Simplify `assert_deb_has` during implementation if the nested grep is awkward — requirement is: fail when any listed relative path is absent from `dpkg-deb -c` output (paths appear as `./usr/bin/...`).

Preferred minimal assert body:

```bash
assert_deb_has() {
  local deb="$1"; shift
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
```

- [ ] **Step 2: Commit**

```bash
git add packaging/scripts/smoke-debs.sh
chmod +x packaging/scripts/smoke-debs.sh
# Subject: Add deb content smoke checks including veloce-exec.
```

---

### Task 5: `release.yml` workflow

**Files:**
- Create: `.github/workflows/release.yml`

**Interfaces:**
- Consumes: packaging scripts + nfpm YAMLs from prior tasks
- Produces: Release assets on `v*` tags; workflow artifacts on `workflow_dispatch`

- [ ] **Step 1: Write the workflow**

```yaml
name: Release packages

on:
  push:
    tags: ["v*"]
  workflow_dispatch:

permissions:
  contents: write

env:
  CARGO_TERM_COLOR: always
  ARCH: amd64

jobs:
  packages:
    name: linux-amd64 packages
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v7

      - name: Install system dependencies
        run: |
          sudo apt-get update
          sudo apt-get install -y protobuf-compiler libssl-dev pkg-config rpm

      - uses: dtolnay/rust-toolchain@stable

      - uses: Swatinem/rust-cache@v2

      - name: Resolve version
        id: ver
        run: |
          CRATE="$(./packaging/scripts/version.sh print)"
          if [[ "${GITHUB_REF_TYPE}" == "tag" ]]; then
            ./packaging/scripts/version.sh check-tag "${GITHUB_REF_NAME}"
            echo "version=${GITHUB_REF_NAME#v}" >> "$GITHUB_OUTPUT"
            echo "upload_release=true" >> "$GITHUB_OUTPUT"
          else
            echo "version=${CRATE}" >> "$GITHUB_OUTPUT"
            echo "upload_release=false" >> "$GITHUB_OUTPUT"
          fi

      - name: Build release binaries
        run: |
          cargo build --locked --release \
            -p veloce-controller \
            -p veloce-worker \
            -p veloce-fileserver \
            -p veloce-cli

      - name: Install nfpm
        run: |
          curl -sL "https://github.com/goreleaser/nfpm/releases/download/v2.41.3/nfpm_2.41.3_Linux_x86_64.tar.gz" \
            | sudo tar -xz -C /usr/local/bin nfpm
          nfpm --version

      - name: Stage, tarball, and package
        env:
          VERSION: ${{ steps.ver.outputs.version }}
        run: |
          set -euo pipefail
          mkdir -p dist
          for p in veloce-controller veloce-worker veloce-fileserver veloce-cli; do
            ./packaging/scripts/stage-root.sh "$p" "build/stage/$p"
            ./packaging/scripts/make-tarball.sh "$p"
            nfpm package -f "packaging/nfpm/${p}.yaml" -p deb -t dist/
            nfpm package -f "packaging/nfpm/${p}.yaml" -p rpm -t dist/
          done
          (cd dist && sha256sum * > SHA256SUMS)
          ./packaging/scripts/smoke-debs.sh dist
          sha256sum -c dist/SHA256SUMS

      - name: Upload workflow artifacts
        uses: actions/upload-artifact@v4
        with:
          name: veloce-ce-linux-amd64-${{ steps.ver.outputs.version }}
          path: dist/*

      - name: Attach to GitHub Release
        if: steps.ver.outputs.upload_release == 'true'
        uses: softprops/action-gh-release@v2
        with:
          files: dist/*
          fail_on_unmatched_files: true
```

Pin nfpm to a concrete release in the plan; bump only if that tag 404s at implement time (pick latest stable 2.x from goreleaser/nfpm releases and keep the pin in-repo).

- [ ] **Step 2: YAML sanity**

Run: `python3 -c "import yaml; yaml.safe_load(open('.github/workflows/release.yml'))"`  
(or `actionlint` if available). Expected: parse OK.

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/release.yml
# Subject: Add tag-triggered amd64 deb/rpm/tar.gz release workflow.
```

---

### Task 6: Packaging README + root docs links

**Files:**
- Create: `packaging/README.md`
- Modify: `README.md` (short Releases / packages blurb)
- Modify: `CHANGELOG.md` (Unreleased bullet)
- Modify: `docs/README.md` (link to packaging README)
- Modify: `docs/specs/2026-09-16-release-packaging-design.md` if CLI row still incomplete (already includes `veloce-exec`)

- [ ] **Step 1: Write `packaging/README.md`**

Cover: what each package installs; both CLI binaries; copy `*.example` configs; `systemctl enable --now` is manual; Apptainer optional on workers; dry-run via Actions → workflow_dispatch; arm64 is a future matrix; no apt/yum repo yet.

- [ ] **Step 2: Wire discoverability**

Root README: one sentence under Quick start or Documentation pointing at GitHub Releases for `.deb`/`.rpm`/`.tar.gz`.  
CHANGELOG Unreleased: “GitHub Actions release packages (amd64 tar.gz/deb/rpm) via nfpm”.  
`docs/README.md`: row for `../packaging/README.md`.

- [ ] **Step 3: Commit**

```bash
git add packaging/README.md README.md CHANGELOG.md docs/README.md docs/specs/2026-09-16-release-packaging-design.md
# Subject: Document CE package install paths and release artifacts.
```

---

### Task 7: End-to-end verification on GitHub

**Files:** none (CI only)

- [ ] **Step 1: Push `main` if any commits remain unpushed**

- [ ] **Step 2: Run `workflow_dispatch` on `Release packages`**

Via UI or: `gh workflow run "Release packages" --repo mihaselow/veloce-ce`  
(Requires `gh auth`.) Download the artifact and confirm `veloce-cli_*.deb` contains `./usr/bin/veloce-exec`.

- [ ] **Step 3: Report**

Paste artifact list + smoke log excerpt to the user. Do **not** cut a new version tag unless asked.

---

## Spec coverage checklist

| Spec requirement | Task |
|------------------|------|
| Tag `v*` + `workflow_dispatch` | 5 |
| Four role packages | 2–5 |
| `veloce` + `veloce-exec` in cli package | 2, 3, 4 |
| amd64 first / arm64 note | 5, 6 |
| Binaries + samples + systemd (no auto-enable) | 1–3, 6 |
| nfpm + tar.gz + SHA256SUMS | 3, 5 |
| Version guard on tags | 2, 5 |
| Deb smoke | 4, 5 |
| Docs / CHANGELOG / no silent beta.2 rebuild | 6, 7 |
| No signing / apt repo / WASM / slurm | Global constraints |

## Self-review notes

- No TBD placeholders in tasks; nfpm pin may be bumped at implement time if the URL 404s.
- CLI package explicitly stages and smokes `veloce-exec`.
- `make-tarball.sh` calls `stage-root.sh` again — workflow may stage twice; acceptable. Implementers may set `STAGE_DIR` and skip re-stage later; not required for v1.
