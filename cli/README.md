# Veloce CLI

Command-line tools for the cluster: `veloce` (management) and `veloce-exec` (MPI remote agent).

## `veloce`

### Job submission

Control-plane submit (`veloce submit`, arrays, CWL) uses **Noise** to the controller. HTTPS is for REST-backed ops and fileserver staging (`--input-deck`, containers).

- **Resources** — cores, memory (MB), walltime, GRES/GPUs
- **Arrays** — `--array` injects `VELOCE_ARRAY_TASK_ID` per task
- **CWL DAGs** — YAML pipelines; the CLI rejects cycles before submit
- **Input decks** — `--input-deck` stages files through the fileserver
- **Wait mode** — `--wait` blocks until completion and returns the job exit code

### Observability and control

- **Logs** — `jobs logs <ID> --follow`; use `--rank` for multi-node history
- **Nodes** — `nodes list` for capacity and host telemetry
- **History / metrics** — `history` and `metrics`
- **Diagnostics** — `doctor` (alias `ping`) checks TCP, Noise, RBAC, leader redirects, and fileserver reachability without printing secrets
- **Lifecycle** — `submit`, `jobs list`, `jobs show`, `jobs resubmit`, `jobs logs`, `jobs kill`

### Administration

- **Reservations** — `reservations create` / `list` / `delete`
- **Containers** — `veloce containers …` for Apptainer images and solver manifests. Register/delete need `VELOCE_API_KEY` (admin) and `VELOCE_FILESERVER_KEY` for upload/delete—not `VELOCE_SECRET`. See [docs/apptainer.md](../docs/apptainer.md) §3.

## `veloce-exec`

Shim for OpenMPI (and similar) launchers:

- Drop-in for `ssh` / `rsh` via `OMPI_MCA_plm_rsh_agent=veloce-exec`
- Starts remote ranks inside the existing Veloce allocation (cgroup, GPU, container)
- Proxies stdout, stderr, and exit codes back to the launcher

## Usage

### Submit a job

```bash
veloce submit --name baseline-run --nodes 2 --cores 8 --mem 16000 ./my_simulation
```

When `--user` is omitted, submit uses the current OS user from `USER`, `LOGNAME`, or `USERNAME`. Use `--name` and `--comment` for labels in lists and history.

### Container job

```bash
veloce submit --image s3://registry/solver.sif /usr/bin/solver
```

Put Veloce options before the executable. Tokens after the executable go to the job; `--` is an optional escape hatch when parsing is ambiguous.

### Cluster status

```bash
veloce nodes list
veloce jobs list --mine
veloce jobs list --user alice --state running
```

### Logs

```bash
veloce jobs logs <JOB_ID> --follow
veloce jobs logs <JOB_ID> stdout --rank 1
```

### Job details and events

```bash
veloce jobs show <JOB_ID>
veloce jobs events <JOB_ID>
veloce jobs events <JOB_ID> --json
```

### Resubmit

```bash
veloce jobs resubmit <JOB_ID>
veloce jobs clone <JOB_ID> --name rerun-baseline
```

Active jobs copy their reusable submission spec. History clones may omit transient fields (per-job env, uploaded inputs, working directory).

### Connectivity

```bash
veloce doctor
veloce ping --json
```

### Persist CLI defaults

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

Precedence: flags > environment > TOML > built-in defaults. Set `VELOCE_CONFIG` for an alternate config path. Sample: [`examples/cli-config.toml`](../examples/cli-config.toml).

### Machine-readable output

```bash
veloce submit --json ./solver
veloce jobs logs --json <JOB_ID>
veloce jobs kill --json <JOB_ID>
```

`--json` covers submit/resubmit, job and node listing, details, events, logs, kill, history, metrics, reservations, containers, and doctor. Log JSON emits one object per chunk (UTF-8 text plus base64 for exact bytes). Contract: [docs/cli-json.md](../docs/cli-json.md).

## Security and HA

- **Noise** — `Noise_XXpsk3_25519_ChaChaPoly_BLAKE2s`
- **Leader discovery** — comma-separated controllers via `VELOCE_CONTROLLERS`; the CLI follows redirects to the active leader
- **Transport** — bincode over Noise for control-plane RPCs

## Dependencies

- **`veloce-common`** — protocols and models
- **`clap`** — argument and env parsing
- **`tokio`** — async runtime
- **`snow`** — Noise
- **`reqwest`** — HTTPS to fileserver and API
