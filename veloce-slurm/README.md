Version: 1.0.0-beta.3

# veloce-slurm — Slurm front door, not a Slurm clone

`veloce-slurm` is a **sidecar CLI** that accepts familiar Slurm commands (`sbatch`, `srun`, `squeue`, `scancel`, `sinfo`) and execs [`veloce --json`](../docs/cli-json.md). It does **not** implement slurmd, partitions, slurmdbd, SPANK, or the Slurm wire protocol.

Install (after `cargo build -p veloce-slurm`):

```bash
ln -s "$(pwd)/target/debug/veloce-slurm" /usr/local/bin/veloce-slurm
for cmd in sbatch srun squeue scancel sinfo; do
  ln -sf /usr/local/bin/veloce-slurm /usr/local/bin/$cmd
done
```

`VELOCE_BIN` overrides the `veloce` binary (default: `veloce` on `PATH`). Connection secrets stay with the Veloce CLI (`~/.veloce/config.toml`, `VELOCE_*`). JSON shapes: [docs/cli-json.md](../docs/cli-json.md).

This is a translation layer. Native control remains the HTTPS API and the `veloce` CLI.

---

## Command map

| Facade | Veloce |
|--------|--------|
| `sbatch` | `veloce --json submit` (honor flags only) |
| `squeue` | `veloce --json jobs list` (`-u` / `--user` → `--user`; `--me` → `--mine`) |
| `scancel <id>` | `veloce --json jobs kill <id>` |
| `sinfo` | `veloce --json nodes list` |
| `srun` | `veloce --json submit --wait`, or `veloce --json step --wait` when `VELOCE_JOB_ID` is set |

`squeue` prints a `PARTITION` column for familiarity; the value is the Veloce **QoS**, not a Slurm partition.

Not in v1: `salloc`, `scontrol`, `sacct`, `libslurm`, `slurmrestd`.

---

## Honor / translate / skip / reject

Unsupported `#SBATCH` and CLI options are **skipped** (stderr warning) and the job is still submitted. That is intentional: brownfield scripts often carry site options Veloce will not emulate.

Stderr (grep-stable):

```
veloce-slurm: skipping unsupported option --partition=gpu (Veloce has no Slurm partitions; use --qos)
```

### Honor (mapped to `veloce submit`)

| Slurm | Veloce |
|-------|--------|
| `--job-name` / `-J` | `--name` |
| `--nodes` / `-N` | `--nodes` |
| `--cpus-per-task` / `-c` | `--cores` (cores per node; not a full ntasks model) |
| `--mem` | `--mem` (MB; `K`/`M`/`G`/`T` suffixes accepted) |
| `--time` / `-t` | `--walltime` (seconds) |
| `--gres=gpu:N` / `--gres=name:count` | `--gres` |
| `--array` / `-a` | `--array` |
| `--dependency` / `-d` | `--dependency` — **only** `afterok`, `afterany`, `afternotok` |
| `--comment` | `--comment` |
| `--wrap` | `/bin/sh -lc '…'` |
| `--export=NAME=value` | `-e NAME=value` |
| `--export=NONE` | ignored (Veloce already does not inherit the host env) |

Command-line flags override `#SBATCH` in the script.

### Translate if exact

| Slurm | Behavior |
|-------|----------|
| `--qos=<veloce qos>` | Passed through when the value is `interactive`, `production`, `preemptible`, or `background` |
| `--partition` / `-p` | If the name is a Veloce QoS, treat as `--qos`; **otherwise skip-warn** (Veloce has no partition object) |

### Skip-warn (by design)

Do not pretend these exist in Veloce:

- `--partition` names that are not Veloce QoS
- `--account`
- `--licenses` — Slurm license tokens are slurmdbd counters; this facade does not map them onto Veloce jobs
- `--constraint` / `--nodelist` / `--exclude`
- `--exclusive` / `--oversubscribe` / `--contiguous` / `--switches`
- `--cpu-bind` / `--mem-bind` / `--hint` / `--ntasks-per-socket` / `--ntasks-per-node` / `--ntasks`
- `--mem-per-cpu`
- `--mail-user` / `--mail-type`
- `--begin` / `--requeue` / `--signal`
- `--open-mode` / `--output` / `--error` / `--input`
- `--get-user-env` / `--export=ALL`
- `--network` / `--mpi` / `--spank`
- `--chdir`
- typed GRES such as `--gres=gpu:mi300x:8` (use `--gres=gpu:N`)
- any **unknown** `#SBATCH` or flag

### Reject (bad mapped values)

The job is **not** submitted when a mapped flag cannot be parsed: invalid `--time`, malformed `--array`, unknown `--dependency` kind (for example `aftercorr`), or missing script/`--wrap`.

---

## `SLURM_*` inside the job

Workers inject `VELOCE_*` only. The facade wraps the user command so aliases appear at start:

`SLURM_JOB_ID` ← `VELOCE_JOB_ID`, `SLURM_ARRAY_JOB_ID` ← `VELOCE_ARRAY_JOB_ID`, `SLURM_ARRAY_TASK_ID` ← `VELOCE_ARRAY_TASK_ID`, `SLURM_NODELIST` ← `VELOCE_NODES`, `SLURM_NNODES` ← `VELOCE_NODE_COUNT`, `SLURM_PROCID` ← `VELOCE_RANK`.

No worker code changes.

---

## Tests

`cargo test -p veloce-slurm` is **facade-only**: parse/map, argv0 dispatch, and a fake `veloce` that prints JSON fixtures. It does not start the controller or scheduler.
