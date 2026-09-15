# Veloce Worker

The **Veloce Worker** is the "Muscle" of the cluster. It is a high-performance execution agent that manages process lifecycles, resource isolation, real-time telemetry, and secure internal cluster communication.

## 🚀 Advanced Execution & Isolation

The worker utilizes modern Linux kernel features to provide robust and secure workload execution.

### **1. Cgroup v2 Isolation & The "Pivot Strategy"**
To enforce strict resource boundaries, the worker implements a sophisticated **Pivot Strategy** for Cgroup v2 management:
- **Pivoting**: Moves the main worker process into a dedicated `/worker` sub-cgroup, "clearing" the root hierarchy.
- **Strict Enforcement**: Enables `cpu`, `memory`, `pids`, and device controllers for the entire subtree.
- **Job Containers**: Every job is executed in a unique, strictly governed cgroup path (e.g., `/veloce/job-<id>`), preventing resource leakage and interference between tasks.

### **2. First-Class Apptainer Support**
Natively orchestrates **Apptainer** `.sif` container images with:
- **Daemonless Execution**: No persistent container daemon required.
- **Seamless Integration**: Inherits Cgroup v2 constraints and GPU isolation natively.
- **Automatic Staging**: Securely fetches and caches container images from the S3-compatible Veloce Fileserver.

### **3. Native PMI-1/PMI-2 Implementation**
Includes a zero-dependency implementation of the **Process Management Interface** wire protocols.
- **`mpirun`-less Launch**: Enables MPI applications to run directly without external launchers.
- **Distributed Coordination**: Manages rank barriers and key-value exchanges over custom async TCP listeners, coordinated by the Controller.

### **4. Generic Resource (GRES) & GPU Pining**
- **Discovery**: Automatically detects NVIDIA GPUs via **NVML**.
- **Hardware Pinning**: Maps specific device IDs (e.g., GPU 0, 2) to jobs and enforces access via Cgroup v2 device filters.

## 📡 Real-time Monitoring & Interactive Access

### **1. High-Resolution Telemetry**
Streams host and job-specific metrics to the Controller at a configurable interval:
- **Host**: CPU load, Net RX/TX, Disk IO (bytes/ops), and thermal data.
- **GPU**: Real-time utilization, VRAM usage, and thermal readouts.
- **Job**: Per-process CPU and RSS memory usage.

### **2. Interactive Sessions (TTY & VNC)**
Provides secure, real-time access to running jobs through the cluster control plane:
- **Web TTY**: Spawns a PTY and attaches to the job's Linux namespace (`mnt`, `uts`, `net`, etc.) via `nsenter`.
- **Graphical VNC**: Bridges loopback VNC servers from inside container namespaces to the browser-based **noVNC** client via a secure Noise-encrypted tunnel. No public ports required.

## 🏗 Architecture & Internal Modules

- **`main.rs`**: Core event loop, Noise connection management, and high-level job lifecycle orchestration.
- **`cgroups.rs`**: Linux-native Cgroup v2 management for CPU, Memory, and Device (GRES) isolation.
- **`pmi/`**: Implementation of the PMI-1 and PMI-2 wire protocols for zero-dependency MPI execution.
- **`apptainer.rs`**: Secure orchestration logic for fetching and running `.sif` container images.
- **`terminal.rs`**: PTY allocation and management for the integrated Web TTY.
- **`vnc.rs`**: Loopback bridge and WebSocket proxying for graphical sessions.

## 🛡️ Reliability & Security

- **Pivot Recovery**: Persists running job state to `veloce_worker_jobs.bin`, allowing the worker to re-attach to active processes after a crash or restart.
- **User Impersonation**: Securely executes jobs as the requesting user (`setuid`/`setgid`).
- **Noise Protocol**: All commands, telemetry, and interactive data are encrypted and authenticated via the **Noise Protocol Framework**.
- **Graceful Draining**: Implements the `WorkerDraining` protocol for Kubernetes scale-down events, ensuring jobs complete before a node is reclaimed.

## ⚙️ Configuration

Configured via `veloce-worker.toml`.

```toml
controller = "127.0.0.1:9000"
cluster_secret = "..." # Derived from VELOCE_SECRET
idle_threshold_cpu = 0.5
idle_timeout_seconds = 600
# prolog = "/path/to/setup.sh"
# epilog = "/path/to/cleanup.sh"

[gres]
# Manual overrides for auto-discovery
# gpu = 2
```

## 🏗 Dependencies

- **`veloce-common`**: Core protocols and data models.
- **`sysinfo`**: Cross-platform system and process telemetry.
- **`nvml-wrapper`**: NVIDIA GPU discovery and monitoring.
- **`nix`**: Low-level Linux/Unix system calls.
- **`portable-pty`**: Cross-platform PTY support.
- **`tokio`**: Asynchronous runtime.
