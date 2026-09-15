# Documentation

Guides that apply to this repository. Per-crate flags and internals live next to the source.

| Document | What it covers |
|----------|----------------|
| [quickstart.md](quickstart.md) | Build, TLS, sample configs, start fileserver + controller + worker, submit a job |
| [cli-json.md](cli-json.md) | `veloce --json` stdout contract |
| [apptainer.md](apptainer.md) | `.sif` images, solver manifests, `veloce containers` |

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
