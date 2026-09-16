# Changelog

## Unreleased

- MSRV `1.90`; OpenTelemetry 0.32 + tracing-opentelemetry 0.33 (controller and worker)
- `quick-xml` 0.42 and current AWS SigV4/smithy pins on the fileserver
- Dependabot: grouped OpenTelemetry/AWS updates; ignore Cargo semver majors
- cargo-audit ignore for `RUSTSEC-2023-0071` (rsa 0.9 Marvin; no 0.9 patch)
- GitHub Actions CI (fmt, clippy, test, locked build, WASM check)
- Lab REST route map, `SECURITY.md`, `rust-toolchain.toml`, `examples/hello_sleep.sh`
- Dashboard admin/telemetry no longer labels MCP or federation nodes
- `jsonwebtoken` 10.x for controller OIDC

## 1.0.0-beta.2

First source-available tree of this control plane (BSL 1.1).

- Linux controller, worker, fileserver, CLI, WASM dashboard, and `veloce-slurm` facade
- Lab quickstart, sample TOML, and crate READMEs
- HTTPS API, Noise mesh, Apptainer, CWL, cgroup v2, PMI, and HA as implemented in this tree
