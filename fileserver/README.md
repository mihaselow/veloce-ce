# Veloce Fileserver

HTTPS staging store and Apptainer image registry for the cluster. S3-compatible endpoints exist for tooling; prefer the native REST API in production.

## Interfaces

### Native REST (`/api/v1/files`)

UUID-based API for internal staging:

- Upload and download for job inputs and results
- Unique IDs per object (no filename collisions)
- Multipart uploads for large transfers

### S3-compatible (`/s3`) — dev / legacy

Subset of Amazon S3 for local tools and worker staging paths:

- `ListObjectsV2`, `PutObject`, `GetObject`, `DeleteObject`
- Multipart lifecycle (`Initiate`, `UploadPart`, `Complete`, `Abort`)
- `/s3/legacy` bridges to objects originally uploaded via native REST

**Production:** prefer `/api/v1/files` (often via the controller proxy). For `/s3/*`, send `X-API-KEY` with the fileserver key. Legacy `Authorization` substring matching is available only when `VELOCE_ALLOW_INSECURE=true` (lab/dev).

## Features

- **Apptainer registry** — stores `.sif` blobs; the controller tracks manifests
- **TLS** — HTTPS via `rustls` for all traffic
- **Auth** — native REST and hardened `/s3/*` use `X-API-KEY` (`VELOCE_FILESERVER_KEY`)
- **Retention** — background purge of expired staging objects
- **Dashboard staging** — supports input-deck upload and result download flows

## Configuration

Configured via `veloce-fileserver.toml` in the current working directory. Sample: [`examples/veloce-fileserver.toml`](../examples/veloce-fileserver.toml).

```toml
bind_address = "127.0.0.1:9001"
staging_dir = "staging"
api_key = "..." # VELOCE_FILESERVER_KEY — not the Noise cluster secret
cert_path = "cert.pem"
key_path = "key.pem"
```

## Dependencies

- **`axum`** — REST and S3 HTTP layers
- **`axum-server`** — rustls listener
- **`quick-xml`** — S3 XML
- **`tokio`** — async I/O
- **`md5`** — S3 ETags
