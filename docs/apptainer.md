# Apptainer

Veloce runs Apptainer (formerly Singularity) `.sif` images on workers without a container daemon. Images are stored on the fileserver; the controller holds manifests; workers pull and `apptainer exec` inside the job cgroup.

## 1. Build or pull an image

```bash
apptainer pull ubuntu_latest.sif docker://ubuntu:latest
```

## 2. Solver manifest

Register a JSON manifest with the image so the dashboard and CLI know entrypoints, file mapping, and optional UI parameters.

```json
{
  "manifest_version": "1.0",
  "solver_identity": {
    "vendor": "Ubuntu",
    "product": "Base",
    "version": "22.04",
    "capabilities": ["General"]
  },
  "execution": {
    "entrypoint": "",
    "launch_wrapper": "apptainer exec",
    "command_template": "{{input_file}} {{custom_args}}",
    "environment_vars": {}
  },
  "file_handling": {
    "working_dir": "/workspace",
    "input_mapping": [
      {
        "role": "primary_input",
        "match": "*.sh",
        "required": false
      }
    ],
    "output_collection": []
  },
  "parameter_mapping": {}
}
```

`command_template` substitutions include `{{entrypoint}}`, `{{custom_args}}`, `{{input_file}}`, `{{cpus}}`, and `{{memory}}`. Keys in `parameter_mapping` become extra mustache variables (and form controls on the submit page).

Set `"vnc_enabled": true` at the root of the manifest when the job should expose a loopback VNC desktop through the dashboard.

## 3. Register

Registration uploads the `.sif` to the fileserver, then records metadata on the controller.

| Step | Credential | Env / flag |
|------|------------|------------|
| Upload `.sif` | Fileserver key | `VELOCE_FILESERVER_KEY` / `--fileserver-key` |
| Register / delete | Controller REST key with **admin** | `VELOCE_API_KEY` / `--api-key` |
| List | Controller REST key (any job role) | `VELOCE_API_KEY` |

`VELOCE_SECRET` is the Noise mesh PSK only. It is not accepted as `X-API-KEY`.

```bash
veloce containers register ubuntu-base ./ubuntu_latest.sif ./manifest.json
veloce containers list
veloce containers delete ubuntu-base
```

## 4. Submit

```bash
veloce submit --image s3://veloce-system-containers/ubuntu-base.sif echo "hello from apptainer"
veloce submit --gres gpu:1 --image s3://veloce-system-containers/pytorch.sif python3 train.py
```

The worker caches the image, downloads from the fileserver if needed, and runs `apptainer exec` under cgroup v2. GPU jobs map assigned device IDs into the container.
