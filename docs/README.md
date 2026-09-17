# Documentation

Guides for this repository. Per-crate flags and internals live next to the source. Contributing: [CONTRIBUTING.md](../CONTRIBUTING.md). Security: [SECURITY.md](../SECURITY.md).

| Document | Contents |
|----------|----------|
| [quickstart.md](quickstart.md) | Build, TLS, sample configs, start fileserver + controller + worker, submit a job |
| [apptainer.md](apptainer.md) | Optional Apptainer on **workers**; `.sif` registry, manifests, GPU |
| [cwl.md](cwl.md) | CWL DAG submit (`veloce submit --cwl`) |
| [mpi.md](mpi.md) | PMI-1/2 and `veloce-exec` for MPI |
| [cli-json.md](cli-json.md) | `veloce --json` stdout contract |
| [rest.md](rest.md) | Controller HTTPS routes in this tree |
| [packaging/README.md](../packaging/README.md) | Installable packages: contents, systemd, dry-run workflow |

| Crate README | Binary / library |
|--------------|------------------|
| [controller/README.md](../controller/README.md) | `veloce-controller` |
| [worker/README.md](../worker/README.md) | `veloce-worker` |
| [cli/README.md](../cli/README.md) | `veloce`, `veloce-exec` |
| [fileserver/README.md](../fileserver/README.md) | `veloce-fileserver` |
| [web/README.md](../web/README.md) | WASM dashboard |
| [common/README.md](../common/README.md) | `veloce-common` |
| [veloce-slurm/README.md](../veloce-slurm/README.md) | Slurm-shaped facade |

Sample configuration files: [`examples/`](../examples/).
