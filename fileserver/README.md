# Veloce Fileserver

The **Veloce Fileserver** is the central data depot and container registry for the cluster. It provides a high-performance, secure, and S3-compatible storage service tailored for engineering simulations and large-scale data staging.

## 📦 Multi-Protocol Storage

The fileserver implements two primary interfaces for maximum flexibility:

### **1. Native REST API (`/api/v1/files`)**
A lightweight, UUID-based API designed for internal cluster operations:
- **Fast Upload/Download**: Optimized for low-latency job input staging and result retrieval.
- **UUID Mapping**: Automatically generates unique IDs for every file, eliminating filename collisions.
- **Multipart Support**: Native support for splitting large uploads into chunks for reliable transfer.

### **2. S3-Compatible API (`/s3`) — dev/legacy**
A subset of the Amazon S3 API for local tooling and worker staging paths:
- **Bucket Operations**: Support for `ListObjectsV2`, `PutObject`, `GetObject`, and `DeleteObject`.
- **Multipart Uploads**: Full S3-compliant multipart lifecycle (`Initiate`, `UploadPart`, `Complete`, `Abort`).
- **Internal Bridge**: A special `/s3/legacy` bucket allows S3-compatible tools to access files originally uploaded via the Native REST API.

**Production:** Prefer `/api/v1/files` (via the controller proxy). When `/s3/*` is used, present `X-API-KEY` with the fileserver API key. Legacy substring `Authorization` matching is available only when `VELOCE_ALLOW_INSECURE=true` (dev/lab).

## 🚀 Key Features

- **Apptainer Registry**: Acts as the central distribution point for `.sif` container images. The Fileserver manages the storage and lifecycle of these images, while the Controller tracks their manifests.
- **Secure Transport**: Mandatory **HTTPS/TLS** (via `rustls`) for all data transfers.
- **Unified Authentication**: Native REST and hardened `/s3/*` use `X-API-KEY` with the fileserver API key.
- **Automated Lifecycle Management**: Background tasks automatically purge expired files from the staging area based on a configurable retention policy, preventing disk exhaustion.
- **Staging-to-Archive**: Seamlessly integrates with the Veloce Web Dashboard for automated "Input Deck" staging and "Result Archive" retrieval.

## ⚙️ Configuration

Configured via `veloce-fileserver.toml`.

```toml
bind_address = "0.0.0.0:9001"
staging_dir = "/path/to/storage"
api_key = "your_cluster_secret"
cert_path = "certs/cert.pem"
key_path = "certs/key.pem"
```

## 🏗 Dependencies

- **`axum`**: Modern web framework for both REST and S3 layers.
- **`axum-server`**: High-performance Rustls integration.
- **`quick-xml`**: Efficient XML serialization for S3 compatibility.
- **`tokio`**: Asynchronous runtime for high-concurrency I/O.
- **`md5`**: For S3 ETag calculation and integrity verification.
