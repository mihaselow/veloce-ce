# Quickstart (single-node lab)

This walkthrough starts a fileserver, controller, and one worker on the same Linux host. It uses self-signed TLS and file-backed accounting. Use it to confirm a build; do not copy the self-signed certs or example keys into a real cluster.

**Needs:** Rust (stable), OpenSSL CLI, Linux with cgroup v2 for real job isolation. The dashboard also needs [Trunk](https://trunkrs.dev).

## 1. Build

From the repository root:

```bash
cargo build --locked --release \
  -p veloce-common -p veloce-controller -p veloce-worker \
  -p veloce-cli -p veloce-fileserver -p veloce-slurm
```

Binaries land in `target/release/` (`veloce`, `veloce-controller`, `veloce-worker`, `veloce-fileserver`, `veloce-slurm`). Put that directory on `PATH`, or invoke them with a full path.

Optional dashboard:

```bash
cd web && trunk build --release && cd ..
```

## 2. Lab directory and secrets

Processes load `veloce.toml`, `veloce-worker.toml`, `veloce-fileserver.toml`, and `veloce-web.toml` from the **current working directory**.

```bash
REPO="$(pwd)"
mkdir -p "$HOME/veloce-lab" && cd "$HOME/veloce-lab"

export VELOCE_SECRET="$(openssl rand -base64 32)"
export VELOCE_API_KEY="$(openssl rand -base64 32)"
export VELOCE_FILESERVER_KEY="$(openssl rand -base64 32)"

# REST API key must not equal the Noise cluster secret
cp "$REPO/examples/veloce.toml" veloce.toml
cp "$REPO/examples/veloce-worker.toml" veloce-worker.toml
cp "$REPO/examples/veloce-fileserver.toml" veloce-fileserver.toml
cp "$REPO/examples/veloce-web.toml" veloce-web.toml
```

Fill the `CHANGE_ME_*` placeholders in those files with the same three values you exported (or rely on the env vars listed in each sample). `VELOCE_SECRET` is the Noise mesh PSK only; it is not a REST API key.

## 3. TLS certificates

Controller and fileserver expect `cert.pem` and `key.pem` in the lab directory (paths in the sample TOML).

```bash
openssl req -x509 -newkey rsa:2048 -sha256 -days 365 -nodes \
  -subj "/CN=localhost" \
  -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" \
  -keyout key.pem -out cert.pem
chmod 600 key.pem
```

For a first bring-up against this self-signed pair, set `VELOCE_ALLOW_INSECURE=true` in the shells that start the daemons and the CLI. Turn that off once you have a real CA.

## 4. Start the daemons

Three terminals, all in `$HOME/veloce-lab`, with the same `VELOCE_*` exports:

```bash
# terminal 1
veloce-fileserver

# terminal 2
# optional: web_dist_path = "$REPO/web/dist" in veloce.toml after trunk build
veloce-controller

# terminal 3
veloce-worker
```

## 5. CLI

Configure the client (env is enough for a lab):

```bash
export VELOCE_CONTROLLER=127.0.0.1:9000
# VELOCE_SECRET / VELOCE_API_KEY / VELOCE_FILESERVER_KEY already set

veloce doctor
veloce nodes list
veloce submit --name baseline-run --nodes 1 --cores 1 --mem 512 /bin/sleep 5
veloce jobs list --mine
veloce jobs logs <JOB_ID> --follow
```

Persistent CLI defaults (optional): copy [`examples/cli-config.toml`](../examples/cli-config.toml) to `~/.veloce/config.toml`. Flags override environment variables, which override that file.

## 6. Dashboard

`trunk serve` from `web/` proxies `/api/v1/` to `https://127.0.0.1:8080`. Or set `web_dist_path` on the controller to the Trunk `dist/` directory and open the controller HTTPS API port in a browser.

## Next

- Container jobs: [apptainer.md](apptainer.md)
- Scriptable CLI: [cli-json.md](cli-json.md)
- Slurm-shaped commands: [veloce-slurm/README.md](../veloce-slurm/README.md)
