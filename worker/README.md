# Veloce Worker

Execution agent on each Linux node: process lifecycle, cgroup isolation, telemetry, and Noise mesh I/O.

## Execution and isolation

### Cgroup v2 pivot

The worker enforces resource limits with a cgroup v2 **pivot**:

- Moves the worker process into a `/worker` sub-cgroup so the root hierarchy can host job trees
- Enables `cpu`, `memory`, `pids`, and device controllers for the subtree
- Places each job under a dedicated path (for example `/veloce/job-<id>`)

### Apptainer

When the **worker host** has Apptainer installed (`apptainer` on `PATH`), the worker can run `.sif` images. Apptainer is optional for host binaries. The controller and fileserver only store and register images.

- No container daemon
- Jobs inherit cgroup v2 and GPU device filters
- Images are fetched and cached from the fileserver

See [docs/apptainer.md](../docs/apptainer.md).

### PMI-1 / PMI-2

An in-tree Process Management Interface implementation lets MPI apps rendezvous without an external launcher. Barriers and key-value exchange use async TCP, coordinated by the controller.

### GRES and GPUs

- Discovers NVIDIA GPUs via **NVML**
- Pins assigned device IDs into the job and enforces them with cgroup device filters

## Monitoring and interactive access

### Telemetry

Streams host and job metrics to the controller on a configurable interval:

- **Host** — CPU load, network RX/TX, disk I/O, thermal data
- **GPU** — utilization, VRAM, temperature
- **Job** — per-process CPU and RSS

### TTY and VNC

- **Web TTY** — PTY attached into the job namespaces via `nsenter`
- **VNC** — bridges loopback VNC inside the container/job to the dashboard noVNC client over Noise (no public VNC ports)

## Modules

- **`main.rs`** — Noise connection and job lifecycle
- **`cgroups.rs`** — cgroup v2 for CPU, memory, and devices
- **`pmi/`** — PMI-1/2 wire protocols
- **`apptainer.rs`** — fetch and run `.sif` images
- **`terminal.rs`** — PTY for Web TTY
- **`vnc.rs`** — loopback bridge and WebSocket proxy

## Reliability and security

- **Pivot recovery** — persists running jobs to `veloce_worker_jobs.bin` so the worker can reattach after restart
- **User impersonation** — runs jobs as the requesting OS user (`setuid` / `setgid`)
- **Noise** — commands, telemetry, and interactive streams are authenticated and encrypted
- **Draining** — `WorkerDraining` lets in-flight jobs finish before the node leaves the pool

## Configuration

Configured via `veloce-worker.toml` in the current working directory. Sample: [`examples/veloce-worker.toml`](../examples/veloce-worker.toml).

```toml
controller = "127.0.0.1:9000"
cluster_secret = "..." # VELOCE_SECRET
fileserver_url = "https://127.0.0.1:9001"
fileserver_api_key = "..."
idle_threshold_cpu = 0.5
idle_timeout_seconds = 600
# prolog = "/path/to/setup.sh"
# epilog = "/path/to/cleanup.sh"

[gres]
# Manual overrides for auto-discovery
# gpu = 2
```

## Dependencies

- **`veloce-common`** — protocols and models
- **`sysinfo`** — host and process telemetry
- **`nvml-wrapper`** — NVIDIA discovery and metrics
- **`nix`** — Linux system calls
- **`portable-pty`** — PTY support
- **`tokio`** — async runtime
