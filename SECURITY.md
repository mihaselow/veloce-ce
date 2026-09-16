# Security

Report vulnerabilities privately. Do not open a public GitHub issue for a security defect.

## How to report

Use [GitHub private vulnerability reporting](https://github.com/mihaselow/veloce-ce/security/advisories/new) on this repository.

If that form is unavailable, email the Licensor listed in [LICENSE](LICENSE).

Include:

- Affected crate and version (`1.0.0-beta.2` or git SHA)
- A short description of the issue and impact
- Steps or a proof of concept if you have one
- Whether you plan to disclose on a timeline

You should hear back within a few days. Please wait for a fix or coordinated disclosure before posting details publicly.

## Scope

This repository is the Veloce control plane you can clone and build (controller, worker, CLI, fileserver, web dashboard, `veloce-slurm`). Issues in those crates, their TLS/API auth, and the Noise mesh are in scope.

Out of scope: denial of service against a lab you do not operate, social engineering, and physical access.
