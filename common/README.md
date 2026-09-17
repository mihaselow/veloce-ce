# Veloce Common

Shared library for the Veloce control plane: wire protocol, scheduling models, and types used by controller, worker, CLI, web, and `veloce-slurm`.

## Noise mesh

Internal cluster traffic uses the **Noise Protocol Framework** (not TLS between mesh peers):

- **Pattern:** `Noise_XXpsk3_25519_ChaChaPoly_BLAKE2s`
  - **XX** — mutual authentication (static keys exchanged)
  - **psk3** — 32-byte PSK from `VELOCE_SECRET` for cluster membership
- **Serialization:** `bincode` of the central `Message` enum
- **Framing:** 2-byte big-endian length prefix (frames up to 64 KiB)

`MessageCodec` implements this stack for Tokio I/O.

## Core types

### `Message`

Wire protocol enum (handshake, job lifecycle, telemetry, TTY/VNC, HA sync, and more). Variants include `HelloClient`, `Submit`, `SubmitDag`, `CancelJob`, `WorkerHeartbeat`, `LeaderRedirect`, and interactive session frames.

### Scheduling models

- **`Resources`** — cores, memory (MB), disk, GRES (for example GPUs)
- **`UsageTracker`** — fair-share with usage decay
- **`StarvationBoost`** — non-linear priority for long-waiting jobs
- **`QosLevel`** — `Interactive`, `Production`, `Preemptible`, `Background`

### Orchestration

- Apptainer / solver manifests (parameters, file mapping, identity)
- CWL DAG parsing with cycle detection
- PMI-1/2 coordination types for MPI

### Telemetry

Host and job metrics structures, plus queue analytics such as `QueueResourceGap`.

## Consumers

- **`veloce-controller`** — scheduler models and protocol
- **`veloce-worker`** — resources, PMI, terminal/VNC relay
- **`veloce-cli`** — submit models and Noise handshake
- **`veloce-web`** — shared types in Wasm
- **`veloce-slurm`** — JSON DTOs via the CLI

## Dependencies

- **`snow`** — Noise
- **`bincode` / `serde`** — binary serialization
- **`tokio-util`** — codecs
- **`sysinfo`** — telemetry helpers
- **`reqwest`** — HTTP client helpers
