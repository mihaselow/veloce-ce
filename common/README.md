# Veloce Common

`veloce-common` is the foundational library and "Source of Truth" for the entire Veloce ecosystem. It defines the shared data structures, high-performance network protocols, and core logic used for cluster orchestration, scheduling, and secure communication.

## 🛡️ Secure Communication: The Noise Protocol

Veloce eschews traditional TLS for internal cluster communication to simplify deployment and improve performance. This crate implements a custom **`MessageCodec`** built on the **Noise Protocol Framework**.

- **Handshake Pattern**: `Noise_XXpsk3_25519_ChaChaPoly_BLAKE2s`
  - **XX**: Full mutual authentication (both parties exchange static keys).
  - **psk3**: A 32-byte Pre-Shared Key (PSK) derived from the `VELOCE_SECRET` is incorporated into the handshake for cluster-wide authorization.
- **Serialization**: Uses **`Bincode`** for high-efficiency binary transport of the central `Message` enum, providing minimal overhead compared to JSON.
- **Framing**: Messages are wrapped in a 2-byte Big-Endian length prefix, supporting frames up to 64KB.

## 🧩 Core Components

### **The `Message` Enum**
The primary definition of the cluster's wire protocol. It includes over 50 variants covering:
- **Handshake & Heartbeats**: `HelloClient`, `Heartbeat`, `Ack`.
- **Job Lifecycle**: `Submit`, `SubmitDag`, `CancelJob`, `JobId`.
- **Telemetry**: `WorkerHeartbeat` (containing `WorkerInfo` and `JobStats`), `MetricsData`.
- **Interactive Sessions**: `TerminalInput`, `TerminalOutput`, `VncData`.
- **Distributed Diagnostics**: `GetSystemLogs`, `LogStream`, `GetComponentLogs`.
- **HA Coordination**: `LeaderRedirect`, `StateSync`, `StateUpdate`.

### **Resource Management & Scheduling Models**
- **`Resources`**: Tracking for CPU cores (available vs. total), Memory (MB), Disk, and **Generic Resources (GRES)** like GPUs.
- **`UsageTracker`**: Implementation of the **Fair Share** algorithm with resource-usage decay.
- **`StarvationBoost`**: Logic for non-linear priority escalation for long-pending jobs to prevent resource starvation.
- **`QosLevel`**: Definitions for `Interactive`, `Production`, `Preemptible`, and `Background` classes.

### **Orchestration Layer**
- **Apptainer & Solver Manifests**: Complex models for defining virtual containers, including parameter mapping, file handling, and solver identity.
- **CWL DAG Engine**: A tailored implementation of the Common Workflow Language (CWL) for declaring multi-stage simulation pipelines with dependency validation and circularity detection.
- **Native PMI-1/2**: Definitions and coordination logic for the zero-dependency Process Management Interface used by MPI applications.

### **Telemetry & Observability**
- **Real-time Metrics**: High-resolution structures for host performance (CPU load, Net RX/TX, Disk IO) and individual job telemetry.
- **Queue Analytics**: The `QueueResourceGap` model used by the KEDA scaler for demand-driven autoscaling.

## 🛠 Usage

This crate is the central dependency for:
- **`veloce-controller`**: Uses the scheduler models and protocol definitions.
- **`veloce-worker`**: Uses the resource tracking, PMI implementation, and terminal relay.
- **`veloce-cli`**: Uses the submission models and Noise handshake.
- **`veloce-mcp`**: Uses the schema-guaranteed tools and resource models.

## 🏗 Dependencies

- **`snow`**: Implementation of the Noise Protocol Framework.
- **`bincode` & `serde`**: Zero-cost binary serialization.
- **`tokio-util`**: Codecs and asynchronous I/O primitives.
- **`sysinfo`**: Cross-platform system telemetry.
- **`reqwest`**: Async HTTP client for Fileserver and API interaction.
