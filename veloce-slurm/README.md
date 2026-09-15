Version: 1.0.0-beta.2

# veloce-slurm — Slurm front door, not a Slurm clone

`veloce-slurm` is a **sidecar CLI** that speaks a familiar Slurm mouth (`sbatch`, `srun`, `squeue`, `scancel`, `sinfo`) and execs the existing [`veloce --json`](cli-json.md) client. It does **not** implement slurmd, partitions, slurmdbd, SPANK, or the Slurm wire protocol.

Install (after `cargo build -p veloce-slurm`):

```bash
ln -s "$(pwd)/target/debug/veloce-slurm" /usr/local/bin/veloce-slurm
for cmd in sbatch srun squeue scancel sinfo; do
  ln -sf /usr/local/bin/veloce-slurm /usr/local/bin/$cmd
done
```

`VELOCE_BIN` overrides the `veloce` binary (default: `veloce` on `PATH`). Connection secrets stay in the Veloce CLI (`~/.veloce/config.toml`, `VELOCE_*`).

This is a translation layer. Native Veloce remains API-first (REST, MCP, `veloce` CLI). See [comparison.md](comparison.md) for how this sits next to SchedMD Slurm and AMD Spur.

---

## Command map

| Facade | Veloce |
|--------|--------|
| `sbatch` | `veloce --json submit` (honor flags only) |
| `squeue` | `veloce --json jobs list` (`-u` / `--user` → `--user`; `--me` → `--mine`) |
| `scancel <id>` | `veloce --json jobs kill <id>` |
| `sinfo` | `veloce --json nodes list` |
| `srun` | `veloce --json submit --wait`, or `veloce --json step --wait` when `VELOCE_JOB_ID` is set |

`squeue` prints a `PARTITION` column for muscle memory; the value is the Veloce **QoS**, not a Slurm partition.

Not in v1: `salloc`, `scontrol`, `sacct`, `libslurm`, `slurmrestd`.

---

## Honor / translate / skip / reject

Unsupported `#SBATCH` and CLI options are **skipped** (stderr warning) and the job is still submitted. That is intentional: brownfield scripts are full of site folklore Veloce will not grow into.

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
- `--licenses` — **not** Veloce licensing. Slurm license tokens are slurmdbd counters. Community Edition keeps the `wait_for_licenses` job field for protocol compatibility but does not query FlexLM/LM-X. The facade will **not** map `--licenses` onto that path.
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

The job is **not** submitted when a flag we *would* honor cannot be parsed: invalid `--time`, malformed `--array`, unknown `--dependency` kind (e.g. `aftercorr`), missing script/`--wrap`.

---

## `SLURM_*` inside the job

Workers only inject `VELOCE_*`. The facade wraps the user command so aliases appear at start:

`SLURM_JOB_ID` ← `VELOCE_JOB_ID`, `SLURM_ARRAY_JOB_ID` ← `VELOCE_ARRAY_JOB_ID`, `SLURM_ARRAY_TASK_ID` ← `VELOCE_ARRAY_TASK_ID`, `SLURM_NODELIST` ← `VELOCE_NODES`, `SLURM_NNODES` ← `VELOCE_NODE_COUNT`, `SLURM_PROCID` ← `VELOCE_RANK`.

No worker code changes.

---

## Tests

`cargo test -p veloce-slurm` is **facade-only**: parse/map, argv0 dispatch, a fake `veloce` that prints JSON fixtures. It does not start the controller or scheduler.
