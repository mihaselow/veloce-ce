# Veloce Web Dashboard

The **Veloce Web Dashboard** is a high-performance, reactive web interface for cluster management. Built with **Leptos** and compiled to **WebAssembly (Wasm)**, it provides a single-page application experience directly in the browser.

## Reactive monitoring and visualization

The dashboard is designed for observability into cluster health and individual job execution.

### Cluster and job views
- **Cluster core grid**: Live-updating map of worker nodes and per-core CPU utilization (2–5s polling).
- **WebSocket refresh**: Job and node list pages subscribe to `/api/v1/ws/events` for push invalidation, then refetch data.
- **Per-job metrics (SSE)**: The job detail page streams CPU metrics via Server-Sent Events (`/api/v1/jobs/:id/metrics/stream`) in [`metrics_chart.rs`](src/metrics_chart.rs).
- **Historical statistics**: The statistics page fetches bucketed node metrics from the controller REST API.

### Interactive job access
- **Web-based TTY**: xterm.js terminal with authenticated WebSocket access proxied through the controller.
- **Graphical VNC**: noVNC canvas bridged through the control plane.
- **Log tailing**: stdout/stderr for active jobs with periodic refresh on the job detail page.

### Orchestration
- **Job submission**: Solver templates, CWL upload, file staging, and command preview.
- **Container registry**: Lists Apptainer assets from `GET /api/v1/containers` (registration via CLI/REST; admin UI registration is planned).
- **HA status**: Controller leader/standby and component health on the dashboard.

### Runtime configuration

The dashboard loads **`/web-config.json`** from the controller. In hardened deployments this returns a public config only (no API keys):

```json
{
  "fileserver_url": "https://your-controller/api/v1",
  "oidc_enabled": true
}
```

OIDC mode uses HttpOnly session cookies for API calls. Legacy API-key mode stores the controller key in `sessionStorage` after login validation.

## Architecture and tech stack

- **Frontend**: Leptos (Rust → Wasm), `gloo-net` for HTTP/WebSocket
- **Shared models**: `veloce-common`
- **Styling**: [`style.css`](style.css) — dark theme, CSS variables
- **Build**: [Trunk](Trunk.toml)

### Local development

```bash
trunk serve
```

Trunk proxies `/api/v1/` to the local controller (`127.0.0.1:8080`). Lab wiring: [docs/quickstart.md](../docs/quickstart.md). Sample `veloce-web.toml`: [`examples/veloce-web.toml`](../examples/veloce-web.toml).

### Production release

```bash
trunk build --release
```

Serve `dist/` from the controller static path or nginx.

## Dependencies

- `leptos` / `leptos_router` — reactive UI and routing
- `gloo-net` — HTTP and WebSocket from Wasm
- `veloce-common` — shared cluster types
- `wasm-bindgen` / `web-sys` — browser APIs
- `chrono` — timestamps in the browser
