# Veloce Controller

Central management server for the cluster: resource tracking, scheduling, HTTPS API, accounting, and high availability.

## Scheduling

### Multi-dimensional matching

The scheduler evaluates jobs against a resource vector:

- **Cores** — specific CPU core IDs and affinity
- **Memory** — virtual allocation tracking to avoid over-subscription
- **GRES** — device counts and concrete IDs (for example GPU 0, 2)

### Gang scheduling and homogeneous bundling

For multi-node jobs, Veloce uses **homogeneous bundling**: it groups nodes with matching CPU models and architectures so parallel ranks stay aligned, and it waits until all required nodes are free before dispatch.

### Fair share and starvation prevention

Queue order uses dynamic scoring:

- **Fair share** — penalizes heavy users from decayed cumulative usage
- **Aging** — raises priority for every second a job waits
- **Starvation boost** — after a configurable wait (default 30 minutes), priority escalates non-linearly so low-priority work still runs

### Backfilling

A projected occupancy map finds the earliest start for the top-priority job, then a first-fit pass packs shorter jobs into gaps without delaying that primary allocation.

### QoS and preemption

- **QoS classes:** `Interactive` (+10,000), `Production` (0), `Preemptible` (−5,000), `Background` (−10,000)
- **Preemption:** `Preemptible` jobs yield to `Interactive`. The controller sends `SIGTERM`, waits five seconds for checkpointing, then `SIGKILL` if needed

## High availability

- **Active-passive** — hot-standby nodes mirror state over Noise
- **Leader election** — Noise-based election among controllers
- **Leader proxying** — standbys forward launch, VNC, and TTY traffic to the leader so clients keep one entry point
- **Atomic persistence** — global state writes to `veloce_state.bin` via write-then-rename
- **Accounting backends** — binary log, SQLite, or PostgreSQL for history and telemetry

## Configuration

The controller reads `veloce.toml` (and optional `veloce-web.toml`) from the current working directory. Lab sample: [`examples/veloce.toml`](../examples/veloce.toml). Bring-up: [docs/quickstart.md](../docs/quickstart.md).

```toml
bind_address = "127.0.0.1:9000"
cluster_secret = "..." # same value as VELOCE_SECRET on workers and the CLI
api_port = 8080
api_key = "..." # VELOCE_API_KEY — must not equal cluster_secret
fileserver_url = "https://127.0.0.1:9001"
fileserver_api_key = "..."
cert_path = "cert.pem"
key_path = "key.pem"

[accounting]
backend = "file" # or "sqlite", "postgres"
database_url = ""
```

## Modules

- **`api.rs`** — Axum HTTPS API and dashboard hosting
- **`scheduler.rs`** — matching, backfill, and queue priority
- **`accounting.rs`** — historical job store backends
- **`containers.rs`** — Apptainer registry and solver manifests
- **`main.rs`** — Noise listener, persistence, and component wiring

## Dependencies

- **`veloce-common`** — protocols and models
- **`axum`** — REST API and static dashboard
- **`sqlx`** — async database pools
- **`dashmap`** — concurrent state maps
- **`rustls`** — TLS
