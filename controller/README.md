# Veloce Controller

The **Veloce Controller** is the "Brain" of the cluster. It is a central management server responsible for resource tracking, multi-dimensional scheduling, high-availability coordination, and overall cluster orchestration.

## 🧠 Sophisticated Scheduling Engine

The Veloce scheduler is designed to maximize throughput while ensuring fairness and resource efficiency.

### **1. Multi-Dimensional Candidate Selection**
The scheduler evaluates jobs based on a complex resource vector:
- **Cores**: Specific CPU core IDs and affinity.
- **Memory**: Virtual allocation tracking to prevent over-subscription.
- **Generic Resources (GRES)**: Tracking and specific device ID assignment (e.g., GPU 0, 2) for specialized hardware.

### **2. Gang Scheduling & Homogeneous Bundling**
For multi-node jobs, Veloce implements strict **Homogeneous Bundling**. It groups nodes with identical CPU models and architectures to prevent rank desynchronization in parallel simulations. It ensures all required nodes are available before a job is dispatched.

### **3. Fair Share Priority & Starvation Prevention**
Jobs are sorted using a dynamic scoring system:
- **Fair Share**: Penalizes heavy users based on decayed cumulative resource usage.
- **Aging**: Accumulates priority for every second a job waits in the queue.
- **Starvation Boost**: A non-linear boost that accelerates priority escalation after a configurable threshold wait time (default 30m), ensuring even low-priority background tasks eventually execute.

### **4. Aggressive Backfilling**
Utilizes a **Projected Future Occupancy Map**. It identifies the earliest start time for the top-priority job and then performs a First-Fit pass to launch short-running jobs in the intervening resource gaps without delaying the primary allocation.

### **5. Quality of Service (QoS) & Preemption**
- **QoS Classes**: `Interactive` (+10,000 priority), `Production` (0), `Preemptible` (-5,000), and `Background` (-10,000).
- **Graceful Preemption**: Automatically evicts `Preemptible` jobs to free resources for `Interactive` tasks. Initiates a 5-second `SIGTERM` window for checkpointing before a hard `SIGKILL`.

## 🛡️ High Availability & Reliability

- **Active-Passive Clustering**: Supports hot-standby nodes with real-time state mirroring over the Noise protocol.
- **Leader Election**: Autonomous, Noise-based election ensures zero-downtime cluster management.
- **Leader-Proxying**: Standby nodes automatically proxy internal requests (job launch, VNC/TTY streams) to the active leader, providing a unified entry point for all clients.
- **Atomic Persistence**: Global state is persisted to `veloce_state.bin` using an **Atomic Write-Rename** cycle to ensure zero corruption on crash.
- **Pluggable Accounting**: Supports multiple backends (Binary log, SQLite, PostgreSQL) for historical job data and telemetry.

## ⚙️ Configuration

The controller reads `veloce.toml` (and optional `veloce-web.toml`) from the current working directory. A lab-ready sample is [`examples/veloce.toml`](../examples/veloce.toml). Lab bring-up: [docs/quickstart.md](../docs/quickstart.md).

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

## 🏗 Architecture & Internal Modules

The controller's codebase is modularized to handle complex orchestration tasks asynchronously:
- **`api.rs`**: High-performance REST API (Port 8080) built with Axum, serving the Web Dashboard.
- **`scheduler.rs`**: The core logic engine for resource matching, backfilling, and queue prioritization.
- **`accounting.rs`**: Pluggable historical data layer with support for multiple database backends.
- **`containers.rs`**: Orchestration of the Apptainer container registry and solver manifest lifecycle.
- **`main.rs`**: Noise protocol listener, state persistence management, and component coordination.

## 🏗 Dependencies

- **`veloce-common`**: Core protocols and data models.
- **`axum`**: Web framework for the REST API and dashboard hosting.
- **`sqlx`**: Async database pool management.
- **`dashmap`**: High-concurrency state containers.
- **`rustls`**: Modern, secure TLS implementation.
