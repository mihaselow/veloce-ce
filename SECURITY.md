# Security

Report vulnerabilities privately. Do not open a public GitHub issue for a security defect.

## How to report

Use [GitHub private vulnerability reporting](https://github.com/mihaselow/veloce-ce/security/advisories/new) on this repository.

If that form is unavailable, email [hello@veloce-hpc.io](mailto:hello@veloce-hpc.io).

Include:

- Affected crate and version (`1.0.0-beta.3` or git SHA)
- A short description of the issue and impact
- Steps or a proof of concept, if available
- Whether you plan to disclose on a timeline

You should hear back within a few days. Wait for a fix or coordinated disclosure before posting details publicly.

## Scope

In scope: the Veloce control plane in this repository (controller, worker, CLI, fileserver, web dashboard, `veloce-slurm`), including TLS/API authentication and the Noise mesh.

Out of scope: denial of service against a lab you do not operate, social engineering, and physical access.
