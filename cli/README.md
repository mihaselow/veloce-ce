# Veloce CLI

The **Veloce CLI** provides a powerful, secure, and scriptable command-line interface for the Veloce cluster. It includes two specialized binaries: the `veloce` management tool and the `veloce-exec` high-performance launcher.

## 🛠 `veloce`: Cluster Management Tool

The primary interface for cluster interaction, supporting a wide range of administrative and user tasks.

### **1. Advanced Job Submission**
- **Resources**: Request specific core counts, memory (MB), walltime, and Generic Resources (GRES/GPUs).
- **Arrays**: Submit thousands of tasks with a single command using the `--array` flag. Each task is injected with `VELOCE_ARRAY_TASK_ID`.
- **DAGs (CWL)**: Submit complex multi-stage pipelines defined in YAML. The CLI validates the DAG for circular dependencies before submission.
- **Input Decks**: Automatically handles file staging via the Fileserver using the `--input-deck` flag.
- **Wait Mode**: The `--wait` flag allows for synchronous job execution in shell scripts, returning the job's exit code directly.

### **2. Observability & Control**
- **Live Logs**: Stream `stdout` and `stderr` from active jobs in real-time with the `jobs logs <ID> --follow` command.
- **Resource Monitoring**: `nodes list` provides deep visibility into worker health, resource capacity, and host telemetry (CPU Load, Net RX/TX).
- **History & Accounting**: Query historical job data and aggregated cluster metrics with the `history` and `metrics` commands.
- **Diagnostics**: `doctor` (alias `ping`) checks controller connectivity, Noise authentication, RBAC, leader redirects, and fileserver reachability with sanitized output.
- **Job Control**: Manage the lifecycle of any job with `submit`, `jobs list`, `jobs show`, `jobs resubmit`, `jobs logs`, and `jobs kill`.

### **3. Cluster Administration**
- **Reservations**: Create and manage time-windowed node allocations for specific users (`reservations create` / `reservations list` / `reservations delete`).
- **Registry**: List and manage registered Apptainer container images and solver manifests (`veloce containers …`). Register/delete need `VELOCE_API_KEY` (admin) and `VELOCE_FILESERVER_KEY` for S3 upload/delete — not `VELOCE_SECRET`. Details: [docs/apptainer.md](../docs/apptainer.md) §3.

## 🚀 `veloce-exec`: HPC Launcher

A specialized shim designed for seamless integration with HPC runtimes like **OpenMPI**.

- **Drop-in Replacement**: Acts as a high-performance replacement for `ssh` or `rsh` (e.g., using OpenMPI's `plm_rsh_agent`).
- **Secure Remote Launch**: Routes execution requests through the Veloce Controller, ensuring they are executed within the correct cgroup-isolated job allocation.
- **Stream Proxying**: Transparently proxies `stdout`, `stderr`, and exit codes from remote ranks back to the primary launcher process.

## 🚀 Quick Start & Usage

### **1. Submit a Standard Job**
```bash
veloce submit --name baseline-run --nodes 2 --cores 8 --mem 16000 ./my_simulation
```

When `--user` is omitted, `veloce submit` uses the current OS user from `USER`, `LOGNAME`, or `USERNAME`.
Use `--name` and `--comment` to attach human-readable context that appears in job lists, job details, and history.

### **2. Submit a Containerized Job**
```bash
veloce submit --image s3://registry/solver.sif /usr/bin/solver
```

Put Veloce options before the executable. After the executable starts, remaining tokens are passed to the job; `--` still works as an optional escape hatch for unusual ambiguity.

### **3. Monitor Cluster Status**
```bash
veloce nodes list
veloce jobs list --mine
veloce jobs list --user alice --state running
```

### **4. Follow Live Logs**
```bash
veloce jobs logs <JOB_ID> --follow
veloce jobs logs <JOB_ID> stdout --rank 1
```

### **5. Show Job Details**
```bash
veloce jobs show <JOB_ID>
```

### **6. Inspect Job Events**
```bash
veloce jobs events <JOB_ID>
veloce jobs events <JOB_ID> --json
```

Job events expose controller-generated domain signals (for example convergence or failure analysis) when available.

### **7. Resubmit a Previous Job**
```bash
veloce jobs resubmit <JOB_ID>
veloce jobs clone <JOB_ID> --name rerun-baseline
```

Active jobs are copied with their reusable submission spec. Completed jobs can be cloned from history, but transient fields such as per-job environment variables, uploaded input handles, and working directory may not be available from accounting records.

### **8. Diagnose Connectivity**
```bash
veloce doctor
veloce ping --json
```

`doctor` checks the controller TCP path, Noise handshake, client token registration, read-only RBAC, leader redirects, and the configured fileserver without printing secrets.

### **9. Persist CLI Defaults**
```toml
# ~/.veloce/config.toml or $XDG_CONFIG_HOME/veloce/config.toml
controllers = "controller-1:9000,controller-2:9000"
secret = "<noise-psk>"
fileserver = "https://fileserver.example.com:9001"
fileserver_key = "<VELOCE_FILESERVER_KEY>"
api_key = "<VELOCE_API_KEY>"
client_id = "cli"
client_token = "<client-registration-token>"
```

CLI flags override environment variables, environment variables override TOML config, and TOML config overrides built-in defaults. Set `VELOCE_CONFIG` to point at a different config file. Sample: [`examples/cli-config.toml`](../examples/cli-config.toml).

### **10. Machine-Readable Output**
```bash
veloce submit --json ./solver
veloce jobs logs --json <JOB_ID>
veloce jobs kill --json <JOB_ID>
```

`--json` is supported for submit/resubmit, job and node listing, job details, job events, logs, kill, history, metrics, reservations, containers, and doctor. Log JSON emits one object per chunk with UTF-8 text plus base64 content for exact byte preservation. The canonical output contract lives in [docs/cli-json.md](../docs/cli-json.md).

## 🛡️ Security & Performance

- **Noise Protocol**: All communication is secured via the `Noise_XXpsk3_25519_ChaChaPoly_BLAKE2s` handshake.
- **High Availability**: Supports automatic leader discovery. Provide a comma-separated list of controllers via `VELOCE_CONTROLLERS`, and the CLI will automatically follow redirects to the current active leader.
- **Efficiency**: Built on a zero-allocation binary protocol (`Bincode`), ensuring minimal overhead for even the largest job submissions.

## 🏗 Dependencies

- **`veloce-common`**: Core protocols and data models.
- **`clap`**: Robust argument parsing with environment variable support.
- **`tokio`**: Asynchronous runtime.
- **`snow`**: Noise Protocol Framework implementation.
- **`reqwest`**: HTTPS client for Fileserver and API interaction.
