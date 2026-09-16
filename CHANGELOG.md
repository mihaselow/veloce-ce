# Changelog

## Unreleased

- MSRV `1.94`; SQLx 0.9 without MySQL/rsa so Dependabot no longer flags Marvin
- OpenTelemetry 0.32 + tracing-opentelemetry 0.33 (controller and worker)
- `quick-xml` 0.42 and current AWS SigV4/smithy pins on the fileserver
- Dependabot: grouped OpenTelemetry/AWS updates; ignore Cargo semver majors
- Mint throwaway `certs/` TLS files in controller integration tests so CI can start the API
- jsonwebtoken `aws_lc_rs` backend (no `rsa` crate) so OIDC tests have a CryptoProvider
- Lab REST route map, `SECURITY.md`, `rust-toolchain.toml`, `examples/hello_sleep.sh`
- Dashboard admin/telemetry no longer labels MCP or federation nodes
- `jsonwebtoken` 10.x for controller OIDC

## 1.0.0-beta.2

First source-available tree of this control plane (BSL 1.1).

- Linux controller, worker, fileserver, CLI, WASM dashboard, and `veloce-slurm` facade
- Lab quickstart, sample TOML, and crate READMEs
- HTTPS API, Noise mesh, Apptainer, CWL, cgroup v2, PMI, and HA as implemented in this tree
