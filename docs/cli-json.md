Version: 1.0.0-beta.3

# Veloce CLI JSON output reference

Canonical contract for JSON written by `veloce --json`. Update this page in the same change whenever a field is added, removed, renamed, or changes type.

Two output families:

- **Command envelopes** — small, command-specific objects (submit, kill, reservation create, and similar acknowledgements)
- **Shared DTO output** — pretty-printed `veloce-common` structs such as `JobInfo`, `JobUsage`, `WorkerInfo`, `NodeMetrics`, `Reservation`, `JobEvent`, and `ContainerAsset`

## Contract rules

- JSON goes to `stdout`.
- Progress for JSON-capable commands goes to `stderr`.
- Errors are human-readable on `stderr` and usually exit non-zero; they are not guaranteed to be JSON.
- Timestamps are Unix epoch seconds unless a field says otherwise.
- Memory units follow the struct: requested memory is MB; most live telemetry and peak memory fields are bytes.
- `JobStatus` uses Serde external tagging: `"Pending"`, `"Running"`, `"Killed"`, `{"Completed": 0}`, or `{"Failed": "message"}`.
- `QosLevel` serializes as `"Background"`, `"Preemptible"`, `"Production"`, or `"Interactive"`.
- `JobInfo.secret` and `JobUsage.secret` are cleared before print and appear as `""` when present on the DTO.

Legacy top-level aliases emit the same JSON as their grouped command equivalents.

## Command summary

| Command | JSON shape |
|---------|------------|
| `veloce doctor --json` / `veloce ping --json` | `DoctorReport` object |
| `veloce submit --json ...` | Job submit envelope |
| `veloce submit --json --array ... <command>` | Array submit envelope |
| `veloce submit --json --cwl workflow.cwl` | DAG submit envelope |
| `veloce submit --json --wait ...` | Terminal job or array status envelope |
| `veloce step --json ...` | Step submit envelope |
| `veloce nodes list --json` | `WorkerInfo[]` |
| `veloce jobs list --json` | Sanitized `JobInfo[]` |
| `veloce jobs show <id> --json` | Sanitized `JobInfo` for active jobs or sanitized `JobUsage` for historical jobs |
| `veloce jobs events <id> --json` | `JobEvent[]` |
| `veloce jobs resubmit <id> --json` / `clone` | Resubmit envelope |
| `veloce jobs kill <id> --json` | Kill acknowledgement envelope |
| `veloce jobs logs <id> --json` | NDJSON log chunk objects, one object per line |
| `veloce history --json` | Sanitized `JobUsage[]` |
| `veloce metrics --json` | `NodeMetrics[]` |
| `veloce reservations create --json` | Reservation create envelope |
| `veloce reservations list --json` | `Reservation[]` |
| `veloce reservations delete --json` | Reservation delete acknowledgement envelope |
| `veloce containers list --json` | `ContainerAsset[]` |
| `veloce containers register --json` | Container register acknowledgement envelope |
| `veloce containers delete --json` | Container delete acknowledgement envelope |

## Doctor

`veloce doctor --json` and `veloce ping --json` print:

```json
{
  "ok": true,
  "connected_controller": "127.0.0.1:9000",
  "worker_count": 2,
  "checks": [
    {
      "name": "127.0.0.1:9000:tcp",
      "ok": true,
      "detail": "TCP connection established"
    }
  ],
  "hints": []
}
```

`connected_controller` and `worker_count` are `null` when no controller was reached or no worker list could be read.

## Submit and step envelopes

Single job submit:

```json
{
  "type": "job",
  "job_id": 42
}
```

Array submit:

```json
{
  "type": "array",
  "base_job_id": 100,
  "task_count": 16
}
```

CWL DAG submit:

```json
{
  "type": "dag",
  "base_job_id": 200,
  "task_count": 3
}
```

Step submit:

```json
{
  "type": "step",
  "parent_job_id": 42,
  "step_id": 1
}
```

`veloce submit --json --wait ...` suppresses the initial submit envelope and prints one terminal status envelope:

```json
{
  "type": "job",
  "job_id": 42,
  "status": "completed",
  "exit_code": 0
}
```

Other terminal job states:

```json
{
  "type": "job",
  "job_id": 42,
  "status": "failed",
  "error": "solver exited before writing results"
}
```

```json
{
  "type": "job",
  "job_id": 42,
  "status": "killed"
}
```

Array wait terminal states:

```json
{
  "type": "array",
  "base_job_id": 100,
  "task_count": 16,
  "status": "completed"
}
```

`status` is `"completed"` or `"failed"`.

## Jobs

`veloce jobs list --json` prints sanitized `JobInfo[]`.
`veloce jobs show <id> --json` prints one sanitized `JobInfo` for an active job, or one sanitized `JobUsage` when the job exists only in history.

Representative `JobInfo`:

```json
{
  "id": 42,
  "job_name": "baseline-run",
  "job_comment": "mesh v3",
  "binary": "./run_solver.sh",
  "args": [],
  "status": "Running",
  "req_nodes": 2,
  "req_cores": 8,
  "req_memory": 16000,
  "assigned_workers": ["worker-1:9002", "worker-2:9002"],
  "walltime": 0,
  "start_time": 1781770000,
  "priority": 0,
  "user_id": "alice",
  "working_directory": "/scratch",
  "queued_time": 1781769900,
  "current_cpu_usage": 150.0,
  "current_memory_usage": 1048576000,
  "is_idle": false,
  "idle_duration": 0,
  "end_time": null,
  "env_vars": [],
  "reason": null,
  "array_id": null,
  "array_task_id": null,
  "inputs": [],
  "cgroup_active": true,
  "gres_req": {"gpu": 1},
  "allocated_cores": {"worker-1": [0, 1, 2, 3]},
  "allocated_gres": {"worker-1": {"gpu": [0]}},
  "mpi_stats": null,
  "secret": "",
  "stdout_file_id": null,
  "stderr_file_id": null,
  "workdir_file_id": null,
  "wait_for_licenses": false,
  "estimated_walltime": null,
  "priority_offset": null,
  "dependencies": null,
  "dependency_specs": null,
  "qos": "Production",
  "container_asset": null,
  "vnc_enabled": false,
  "inherit_host_env": false,
  "env_allowlist": null,
  "job_profile": null
}
```

Representative historical `JobUsage`:

```json
{
  "job_id": 42,
  "job_name": "baseline-run",
  "job_comment": "mesh v3",
  "command_line": "./run_solver.sh",
  "user_id": "alice",
  "submission_time": 1781769900,
  "start_time": 1781770000,
  "end_time": 1781773600,
  "exit_code": 0,
  "status": {"Completed": 0},
  "cpu_time_ms": 123456,
  "max_memory_bytes": 2147483648,
  "req_nodes": 2,
  "req_cores": 8,
  "req_memory": 16000,
  "array_id": null,
  "array_task_id": null,
  "assigned_workers": ["worker-1:9002", "worker-2:9002"],
  "gres_req": {"gpu": 1},
  "cgroup_active": true,
  "secret": "",
  "stdout_file_id": "s3://veloce-staging/path/stdout.log",
  "stderr_file_id": "s3://veloce-staging/path/stderr.log",
  "workdir_file_id": null,
  "wait_for_licenses": false,
  "estimated_walltime": null,
  "priority_offset": null,
  "dependencies": null,
  "dependency_specs": null,
  "qos": "Production",
  "container_asset": null
}
```

Job events:

```json
[
  {
    "timestamp": 1781770000,
    "event_type": "Converged",
    "severity": "Info",
    "message": "Residuals reached target threshold",
    "metadata": {
      "residual": "1e-5"
    }
  }
]
```

Resubmit or clone:

```json
{
  "job_id": 43,
  "resubmitted_from": 42
}
```

Array resubmit:

```json
{
  "base_job_id": 101,
  "task_count": 16,
  "resubmitted_from": 42
}
```

Kill acknowledgement:

```json
{
  "ok": true,
  "action": "kill_job",
  "job_id": 42
}
```

## Logs

`veloce jobs logs <id> --json` prints newline-delimited JSON, one object per chunk. With `--follow`, objects continue until interrupt or the controller closes the stream.

```json
{
  "job_id": 42,
  "log_type": "stdout",
  "rank": 1,
  "offset": 0,
  "bytes": 13,
  "content": "hello world\n",
  "content_base64": "aGVsbG8gd29ybGQK"
}
```

Without `--follow`, if no content is available, the CLI emits one empty chunk (`bytes: 0`, empty `content` and `content_base64`).

## Nodes and metrics

`veloce nodes list --json` prints `WorkerInfo[]`:

```json
[
  {
    "id": "worker-1",
    "hostname": "veloce-worker-lite-1",
    "ip_address": "172.20.0.10",
    "total_cores": 8,
    "available_cores": 6,
    "total_memory": 17179869184,
    "allocated_memory": 2048,
    "cpu_model": "Generic",
    "arch": "x86_64",
    "os_name": "Linux",
    "os_version": "6.x",
    "kernel_version": "6.x",
    "cpu_usage": 12.5,
    "used_memory": 2147483648,
    "load_avg": [0.1, 0.2, 0.3],
    "disk_total": 107374182400,
    "disk_free": 53687091200,
    "uptime": 3600,
    "boot_time": 1781766400,
    "process_count": 120,
    "swap_total": 0,
    "swap_free": 0,
    "version": "1.0.0-beta.3",
    "cgroup_enabled": true,
    "gres": {"gpu": 1},
    "allocated_gres": {"gpu": [0]},
    "net_rx_rate": 1024,
    "net_tx_rate": 2048,
    "disk_read_rate": 0,
    "disk_write_rate": 0,
    "online": true,
    "controller_id": null
  }
]
```

`veloce metrics --json` prints `NodeMetrics[]`:

```json
[
  {
    "node_id": "worker-1",
    "timestamp": 1781770000,
    "running_jobs": 1,
    "cpu_load": 25.0,
    "memory_usage": 2147483648,
    "memory_total": 17179869184,
    "disk_usage": 1048576,
    "disk_total": 107374182400,
    "load_avg": [0.1, 0.2, 0.3],
    "net_rx_rate": 1024,
    "net_tx_rate": 2048,
    "net_packets_rx_rate": 10,
    "net_packets_tx_rate": 12,
    "disk_read_rate": 0,
    "disk_write_rate": 0
  }
]
```

## Reservations

Create:

```json
{
  "ok": true,
  "reservation_id": "reservation-uuid"
}
```

List:

```json
[
  {
    "id": "reservation-uuid",
    "nodes": ["worker-1", "worker-2"],
    "start_time": 1781770000,
    "end_time": 1781773600,
    "owner": "alice"
  }
]
```

Delete:

```json
{
  "ok": true,
  "action": "delete_reservation",
  "reservation_id": "reservation-uuid"
}
```

## Containers

`veloce containers list --json` prints `ContainerAsset[]`:

```json
[
  {
    "id": "container-uuid",
    "name": "openfoam-11",
    "image_uri": "s3://veloce-system-containers/openfoam-11.sif",
    "manifest": {
      "manifest_version": "1.0",
      "solver_identity": {
        "vendor": "OpenFOAM",
        "product": "OpenFOAM",
        "version": "11",
        "capabilities": ["CFD", "MPI"]
      },
      "execution": {
        "entrypoint": "/workspace/run_all.sh",
        "launch_wrapper": null,
        "command_template": "{{entrypoint}}",
        "environment_vars": {}
      },
      "file_handling": {
        "working_dir": "/workspace",
        "input_mapping": [],
        "output_collection": [
          { "path": "*.log", "type": "logs" },
          { "path": "postProcessing/residuals.dat", "type": "telemetry" }
        ]
      },
      "parameter_mapping": {},
      "vnc_enabled": false
    }
  }
]
```

Register:

```json
{
  "ok": true,
  "action": "register_container",
  "name": "openfoam-11",
  "image_uri": "s3://veloce-system-containers/openfoam-11.sif"
}
```

Delete:

```json
{
  "ok": true,
  "action": "delete_container",
  "name": "openfoam-11"
}
```
