# Contributing

Thanks for considering a contribution to Veloce CE.

## Before you start

- This repository is licensed under [BSL 1.1](LICENSE). By submitting a patch, you agree that your contribution is licensed under the same terms.
- Open an issue first for larger design changes so we can agree on direction.
- Security defects: follow [SECURITY.md](SECURITY.md) — do not file a public issue.

## Development

Requirements: Rust **1.94+** (see `rust-toolchain.toml`), Linux for worker/cgroup paths, OpenSSL.

```bash
cargo fmt
cargo clippy -p veloce-common -p veloce-controller -p veloce-worker \
  -p veloce-cli -p veloce-fileserver -p veloce-slurm -- -D warnings
cargo test --locked -p veloce-common -p veloce-controller -p veloce-worker \
  -p veloce-cli -p veloce-fileserver -p veloce-slurm
```

Dashboard (optional): `cd web && trunk build`.

Do not commit `target/`, `web/dist/`, local certs, or lab `.env` files.

## Pull requests

- Keep changes focused; update docs in the same PR when behavior or the `veloce --json` contract changes.
- Prefer clear commit messages that explain **why**.
- Expect CI (`fmt`, `clippy`, `test`, locked build) to pass before merge.
