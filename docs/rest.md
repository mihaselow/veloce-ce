# HTTPS API (controller)

The controller listens on `api_port` (sample: `8080`) with TLS. Authenticate with `X-API-KEY` or an OIDC session cookie. This is a map of routes in this tree, not a full schema.

## Cluster

| Method | Path | Notes |
|--------|------|-------|
| GET | `/health` | Process up |
| GET | `/health/leader` | Leader probe |
| GET | `/metrics` | Prometheus text |
| GET | `/web-config.json` | Public dashboard config (no secrets in hardened mode) |
| GET | `/api/v1/nodes` | Workers |
| GET | `/api/v1/controllers` | Controller HA list |
| GET | `/api/v1/metrics/nodes` | Bucketed node metrics |
| GET | `/api/v1/queue/gap` | Queue resource gap |
| GET | `/api/v1/ws/events` | Cluster event WebSocket |
| POST | `/api/v1/ws/ticket` | Short-lived WS ticket |

## Jobs

| Method | Path | Notes |
|--------|------|-------|
| POST | `/api/v1/jobs` | Submit |
| GET | `/api/v1/jobs` | List |
| POST | `/api/v1/jobs/cwl` | CWL DAG |
| GET / DELETE | `/api/v1/jobs/:id` | Show / cancel |
| GET / POST | `/api/v1/jobs/:id/steps` | List / submit step |
| GET | `/api/v1/jobs/:id/logs` | stdout/stderr |
| GET | `/api/v1/jobs/:id/outputs` | Artifacts |
| GET | `/api/v1/jobs/:id/outputs/content` | Artifact bytes |
| GET | `/api/v1/jobs/:id/metrics/stream` | SSE CPU metrics |
| GET | `/api/v1/jobs/:id/terminal` | TTY WebSocket |
| GET | `/api/v1/jobs/:id/vnc` | VNC WebSocket |
| ANY | `/api/v1/jobs/:id/proxy/*path` | Interactive HTTP proxy |

## Staging, containers, admin

| Method | Path | Notes |
|--------|------|-------|
| POST | `/api/v1/files` | Upload via controller |
| GET / DELETE | `/api/v1/files/:id` | Download / delete |
| GET | `/api/v1/containers` | Apptainer registry |
| POST | `/api/v1/containers/register` | Admin |
| DELETE | `/api/v1/containers/:name` | Admin |
| GET | `/api/v1/solvers` | Solver manifests |
| POST | `/api/v1/solvers/register` | Admin |
| GET / POST | `/api/v1/reservations` | List / create |
| DELETE | `/api/v1/reservations/:id` | |
| GET | `/api/v1/audit/events` | Audit log |
| POST | `/api/v1/admin/components` | Issue component tokens |
| POST | `/api/v1/admin/components/:id/rotate` | |
| DELETE | `/api/v1/admin/components/:id` | |
| POST | `/api/v1/system/restart` | Operator |
| POST | `/api/v1/fileserver/restart` | Operator |
| POST | `/api/v1/internal/launch` | Internal worker launch |

Fileserver HTTPS (sample port `9001`) exposes native `/api/v1/files` and a subset of `/s3`. See [fileserver/README.md](../fileserver/README.md).

Machine-readable CLI: [cli-json.md](cli-json.md).
