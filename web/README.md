# Veloce Web Dashboard

Reactive cluster UI built with **Leptos** and compiled to **WebAssembly**.

## Monitoring

- **Core grid** — workers and per-core CPU utilization (2–5s polling)
- **WebSocket refresh** — job and node lists subscribe to `/api/v1/ws/events`, then refetch
- **Per-job metrics (SSE)** — job detail streams CPU via `/api/v1/jobs/:id/metrics/stream` ([`metrics_chart.rs`](src/metrics_chart.rs))
- **Statistics** — bucketed node metrics from the controller REST API

## Interactive access

- **Web TTY** — xterm.js over an authenticated WebSocket through the controller
- **VNC** — noVNC canvas bridged through the control plane
- **Logs** — stdout/stderr on the job detail page with periodic refresh

## Orchestration

- **Submit** — solver templates, CWL upload, file staging, command preview
- **Containers** — lists Apptainer assets from `GET /api/v1/containers` (register via CLI/REST; admin UI registration is planned)
- **HA status** — controller leader/standby and component health

## Runtime configuration

The dashboard loads **`/web-config.json`** from the controller. In hardened mode this is public config only (no API keys):

```json
{
  "fileserver_url": "https://your-controller/api/v1",
  "oidc_enabled": true
}
```

OIDC mode uses HttpOnly session cookies. Legacy API-key mode stores the controller key in `sessionStorage` after login validation.

## Architecture

- **Frontend** — Leptos (Rust → Wasm), `gloo-net` for HTTP/WebSocket
- **Models** — `veloce-common`
- **Styling** — [`style.css`](style.css) (CSS variables)
- **Build** — [Trunk](Trunk.toml)

### Local development

```bash
trunk serve
```

Trunk proxies `/api/v1/` to the local controller (`127.0.0.1:8080`). Lab wiring: [docs/quickstart.md](../docs/quickstart.md). Sample `veloce-web.toml`: [`examples/veloce-web.toml`](../examples/veloce-web.toml).

### Production build

```bash
trunk build --release
```

Serve `dist/` from the controller static path or a reverse proxy.

## Dependencies

- `leptos` / `leptos_router` — UI and routing
- `gloo-net` — HTTP and WebSocket from Wasm
- `veloce-common` — shared types
- `wasm-bindgen` / `web-sys` — browser APIs
- `chrono` — timestamps
