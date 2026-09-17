# MPI and PMI

Workers implement **PMI-1 / PMI-2** so MPI ranks can rendezvous without a site `mpirun` overlay. The controller coordinates rank barriers and key-value exchange; each rank still runs inside the job cgroup (and optional Apptainer `.sif`).

## Job launch

Submit a multi-node (or multi-rank) job as usual:

```bash
veloce submit --name mpi-run --nodes 2 --cores 8 --mem 16000 ./my_mpi_app
```

Ranks receive `VELOCE_*` in the environment (`VELOCE_JOB_ID`, `VELOCE_RANK`, `VELOCE_NODES`, …). `veloce-slurm` can alias some of those to `SLURM_*` for brownfield scripts—see [veloce-slurm/README.md](../veloce-slurm/README.md).

## `veloce-exec` (OpenMPI remote agent)

[`veloce-exec`](../cli/README.md) is a drop-in for `ssh`/`rsh` on the MPI launcher side. Point OpenMPI at it:

```bash
export OMPI_MCA_plm_rsh_agent=veloce-exec
# launch inside an allocation the way your site already does
```

The shim asks the controller to start the remote rank in the **existing job allocation** (cgroup, GPU pin, container). stdout/stderr and the exit code return through the control plane.

`VELOCE_JOB_ID` must identify the allocation when you spawn extra steps (`veloce step` / `srun` via the facade).

## What this is not

- Not Hydra, and not full PMIx (the worker has an optional `pmix` Cargo feature; default builds use the in-tree PMI path).
- Not a replacement for a vendor MPI library—link your app against OpenMPI/MPICH as usual. Veloce supplies process management and isolation.

GPU + container MPI: register the image first ([apptainer.md](apptainer.md)), then `--gres gpu:N --image s3://…`.
