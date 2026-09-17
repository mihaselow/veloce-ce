use crate::{Config, SharedContext};
use anyhow::{Context, Result};
use axum::{
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use axum_server::tls_rustls::RustlsConfig;
use log::info;
use std::net::SocketAddr;
use tower_http::cors::CorsLayer;
use tower_http::services::{ServeDir, ServeFile};

mod admin;
mod api_middleware;
mod containers;
mod files;
mod interactive;
mod jobs;
mod nodes;
mod reservations;
mod types;
mod ws;

pub use api_middleware::{auth_middleware, leader_only_middleware, validate_and_redeem_ticket};
pub use reservations::check_overlap;
pub use types::*;

use admin::*;
use containers::*;
use files::*;
use interactive::*;
use jobs::*;
use nodes::{
    api_health, api_health_leader, api_list_controllers, api_list_nodes, api_metrics,
    api_node_metrics_history,
};
pub use nodes::{
    build_node_metrics_history_response, controller_health_status, parse_metric_node_filter,
    ClusterMetricsBucket, HealthResponse, NodeMetricsBucket, NodeMetricsHistoryQuery,
    NodeMetricsHistoryResponse, NodeMetricsNodeSeries,
};
use reservations::*;
use ws::*;

pub async fn run_api_server(ctx: SharedContext, config: Config) -> Result<()> {
    let port = config.api_port.unwrap_or(8080);
    let addr = SocketAddr::from(([0, 0, 0, 0], port));

    // Configure TLS
    let tls_config = RustlsConfig::from_pem_file(&config.cert_path, &config.key_path)
        .await
        .context("Failed to load TLS config for API server")?;

    let cors = CorsLayer::permissive();

    let api_routes = Router::new()
        .route("/api/v1/jobs", post(api_submit_job).get(api_list_jobs))
        .route("/api/v1/jobs/cwl", post(api_submit_cwl))
        .route("/api/v1/jobs/:id", get(api_get_job).delete(api_cancel_job))
        .route(
            "/api/v1/jobs/:id/steps",
            get(api_get_job_steps).post(api_submit_step),
        )
        .route("/api/v1/jobs/:id/logs", get(api_get_job_logs))
        .route("/api/v1/jobs/:id/outputs", get(api_list_job_outputs))
        .route(
            "/api/v1/jobs/:id/outputs/content",
            get(api_get_job_output_content),
        )
        .route(
            "/api/v1/jobs/:id/metrics/stream",
            get(api_job_metrics_stream),
        )
        .route("/api/v1/jobs/:id/terminal", get(api_job_terminal))
        .route("/api/v1/jobs/:id/vnc", get(api_job_vnc))
        .route(
            "/api/v1/jobs/:id/proxy/*path",
            axum::routing::any(api_job_interactive_proxy),
        )
        .route("/api/v1/ws/events", get(api_ws_events))
        .route("/api/v1/ws/ticket", post(api_create_ws_ticket))
        .route(
            "/api/v1/reservations",
            post(api_create_reservation).get(api_list_reservations),
        )
        .route(
            "/api/v1/reservations/:id",
            axum::routing::delete(api_delete_reservation),
        )
        .route("/api/v1/nodes", get(api_list_nodes))
        .route("/api/v1/metrics/nodes", get(api_node_metrics_history))
        .route("/api/v1/controllers", get(api_list_controllers))
        .route("/api/v1/queue/gap", get(api_get_queue_gap))
        .route("/api/v1/containers", get(api_list_containers))
        .route("/api/v1/containers/register", post(api_register_container))
        .route(
            "/api/v1/containers/:name",
            axum::routing::delete(api_delete_container),
        )
        .route(
            "/api/v1/admin/components",
            post(api_issue_component_token).get(api_list_components),
        )
        .route(
            "/api/v1/admin/components/:id/rotate",
            post(api_rotate_component_token),
        )
        .route(
            "/api/v1/admin/components/:id",
            axum::routing::delete(api_revoke_component),
        )
        .route("/api/v1/audit/events", get(api_list_audit_events))
        .route("/api/v1/internal/launch", post(api_internal_launch))
        .route("/api/v1/system/restart", post(api_system_restart))
        .route("/api/v1/fileserver/restart", post(api_fileserver_restart))
        .route("/api/v1/files", post(api_upload_file))
        .route(
            "/api/v1/files/:id",
            get(api_download_file).delete(api_delete_file),
        )
        .route("/api/v1/solvers", get(api_list_solvers))
        .route("/api/v1/solvers/register", post(api_register_solver))
        .layer(axum::middleware::from_fn_with_state(
            ctx.clone(),
            crate::rate_limit::rate_limit_middleware,
        ))
        .layer(axum::middleware::from_fn_with_state(
            ctx.clone(),
            leader_only_middleware,
        ))
        .layer(axum::middleware::from_fn_with_state(
            ctx.clone(),
            auth_middleware,
        ))
        .layer(cors);

    let mut app = Router::new()
        .route("/metrics", get(api_metrics))
        .route("/web-config.json", get(api_get_web_config))
        .route("/health", get(api_health))
        .route("/health/leader", get(api_health_leader))
        .merge(crate::auth::routes(ctx.clone()))
        .merge(api_routes)
        .with_state(ctx);

    if let Some(ref dist_path) = config.web_dist_path {
        info!("Serving web frontend from: {}", dist_path);
        let index_path = std::path::Path::new(dist_path).join("index.html");
        let serve_dir = ServeDir::new(dist_path).fallback(ServeFile::new(index_path));

        app = app.fallback_service(serve_dir);
    }

    // Pass config to handler via extension or closure, simpler to use extension
    app = app.layer(axum::Extension(config));

    info!("API Server listening on https://{}", addr);

    axum_server::bind_rustls(addr, tls_config)
        .serve(app.into_make_service())
        .await
        .context("API server failed")
}
pub(super) async fn api_get_web_config(
    axum::Extension(config): axum::Extension<Config>,
) -> impl IntoResponse {
    if let Some(web_conf) = config.web_config {
        let oidc_enabled = std::env::var("FUSIONAUTH_CLIENT_ID").is_ok();
        let public_conf = PublicWebConfig {
            fileserver_url: web_conf.fileserver_url,
            oidc_enabled,
        };
        Json(public_conf).into_response()
    } else {
        (StatusCode::NOT_FOUND, "Web config not found").into_response()
    }
}

#[cfg(test)]
mod health_tests {
    use super::controller_health_status;
    use crate::{ControllerRole, MultinodeJobTracking};

    fn sample_state(role: ControllerRole, leader_addr: Option<&str>) -> crate::GlobalState {
        crate::GlobalState {
            workers: Default::default(),
            jobs: Default::default(),
            queue: Default::default(),
            next_job_id: 1,
            usage_tracker: crate::UsageTracker::new(),
            log_requests: Default::default(),
            scheduler_notify: Default::default(),
            steps: Default::default(),
            next_step_id: 1,
            role,
            leader_addr: leader_addr.map(|s| s.to_string()),
            leader_id: None,
            peers: Default::default(),
            job_output_buffers: Default::default(),
            multinode_job_tracking: MultinodeJobTracking::default(),
            api_port: 8080,
            step_waiters: Default::default(),
            step_output_waiters: Default::default(),
            component_log_requests: Default::default(),
            next_request_id: Default::default(),
            solver_configs: Default::default(),
            reservations: Default::default(),
        }
    }

    #[test]
    fn leader_health_is_ready() {
        let status = controller_health_status(&sample_state(ControllerRole::Leader, None));
        assert_eq!(status.role, "leader");
        assert!(status.ready);
    }

    #[test]
    fn follower_without_leader_addr_not_ready() {
        let status = controller_health_status(&sample_state(ControllerRole::Follower, None));
        assert_eq!(status.role, "follower");
        assert!(!status.ready);
    }

    #[test]
    fn follower_with_leader_addr_ready_for_reads() {
        let status =
            controller_health_status(&sample_state(ControllerRole::Follower, Some("ctrl-1:9000")));
        assert_eq!(status.role, "follower");
        assert!(status.ready);
        assert_eq!(status.leader_addr.as_deref(), Some("ctrl-1:9000"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Config;
    use crate::ControllerContext;
    use crate::MultinodeJobTracking;
    use axum::extract::{Path, State};
    use axum::http::StatusCode;
    use axum::response::IntoResponse;
    use axum::Json;
    use dashmap::DashMap;
    use sqlx::any::AnyPoolOptions;
    use std::collections::HashMap;
    use std::sync::atomic::AtomicU64;
    use std::sync::Arc;
    use tokio::sync::mpsc;
    use tokio::sync::Mutex;
    use veloce_common::auth::ComponentType;
    use veloce_common::NodeMetrics;

    fn sample_node_metric(
        node_id: &str,
        timestamp: u64,
        cpu_load: f32,
        memory_usage: u64,
    ) -> NodeMetrics {
        NodeMetrics {
            node_id: node_id.to_string(),
            timestamp,
            running_jobs: 1,
            cpu_load,
            memory_usage,
            memory_total: 1_000,
            disk_usage: 100,
            disk_total: 1_000,
            load_avg: [cpu_load / 10.0, 0.0, 0.0],
            net_rx_rate: 100,
            net_tx_rate: 200,
            net_packets_rx_rate: 10,
            net_packets_tx_rate: 20,
            net_errors: 0,
            net_drops: 0,
            disk_read_rate: 300,
            disk_write_rate: 400,
            disk_read_ops_rate: 0,
            disk_write_ops_rate: 0,
            procs_running: 4,
            procs_blocked: 0,
            swap_usage: 0,
            process_count: 40,
            uptime: 60,
            cgroup_enabled: true,
            gpu_usage: None,
            gpu_mem_usage: None,
            gpu_temp: None,
        }
    }

    #[test]
    fn node_metric_history_buckets_samples_by_node() {
        let mut node_meta = HashMap::new();
        node_meta.insert("worker-a".to_string(), ("compute-a".to_string(), true));
        node_meta.insert("worker-b".to_string(), ("compute-b".to_string(), false));

        let response = build_node_metrics_history_response(
            vec![
                sample_node_metric("worker-a", 1_000, 10.0, 100),
                sample_node_metric("worker-a", 1_020, 30.0, 300),
                sample_node_metric("worker-b", 1_030, 90.0, 900),
                sample_node_metric("worker-a", 1_080, 50.0, 500),
            ],
            1_000,
            1_120,
            60,
            &node_meta,
        );

        assert_eq!(response.node_count, 2);
        assert_eq!(response.sample_count, 4);
        assert!(!response.management_node_history);

        let worker_a = response
            .nodes
            .iter()
            .find(|node| node.node_id == "worker-a")
            .unwrap();
        assert_eq!(worker_a.hostname.as_deref(), Some("compute-a"));
        assert_eq!(worker_a.online, Some(true));
        assert_eq!(worker_a.samples.len(), 2);
        assert_eq!(worker_a.samples[0].sample_count, 2);
        assert_eq!(worker_a.samples[0].cpu_avg, 20.0);
        assert_eq!(worker_a.samples[0].cpu_max, 30.0);
        assert_eq!(worker_a.samples[0].memory_pct_avg, 20.0);

        assert_eq!(response.cluster.len(), 2);
        assert_eq!(response.cluster[0].node_count, 2);
        assert_eq!(response.cluster[0].cpu_peak, 90.0);
    }

    #[test]
    fn parse_metric_node_filter_accepts_comma_separated_aliases() {
        let query = NodeMetricsHistoryQuery {
            start: None,
            end: None,
            node: Some("worker-b, worker-a".to_string()),
            nodes: Some("worker-a".to_string()),
            bucket_seconds: None,
            resolution: None,
        };

        assert_eq!(
            parse_metric_node_filter(&query),
            Some(vec!["worker-a".to_string(), "worker-b".to_string()])
        );
    }

    async fn create_test_context() -> SharedContext {
        let state = crate::GlobalState {
            workers: HashMap::new(),
            jobs: HashMap::new(),
            queue: std::collections::VecDeque::new(),
            next_job_id: 1,
            usage_tracker: crate::UsageTracker::new(),
            log_requests: HashMap::new(),
            scheduler_notify: Arc::new(tokio::sync::Notify::new()),
            steps: HashMap::new(),
            next_step_id: 1,
            role: crate::ControllerRole::Follower,
            leader_addr: None,
            leader_id: None,
            peers: HashMap::new(),
            job_output_buffers: HashMap::new(),
            multinode_job_tracking: MultinodeJobTracking::default(),
            api_port: 8080,
            step_waiters: HashMap::new(),
            step_output_waiters: HashMap::new(),
            component_log_requests: HashMap::new(),
            next_request_id: Arc::new(std::sync::atomic::AtomicU64::new(1)),
            solver_configs: HashMap::new(),
            reservations: HashMap::new(),
        };

        sqlx::any::install_default_drivers();
        let comp_pool = AnyPoolOptions::new()
            .max_connections(1)
            .connect("sqlite://")
            .await
            .unwrap();
        let component_registry = Arc::new(
            crate::component_registry::ComponentRegistry::new(comp_pool.clone())
                .await
                .unwrap(),
        );
        let audit = Arc::new(
            crate::audit::AuditLog::new(comp_pool.clone())
                .await
                .unwrap(),
        );

        Arc::new(ControllerContext {
            config: Config::default(),
            ws_tickets: DashMap::new(),
            state: Mutex::new(state),
            metrics_store: DashMap::new(),
            job_metrics_history: DashMap::new(),
            next_job_id: AtomicU64::new(1),
            submission_tx: mpsc::channel(1).0,
            file_client: Arc::new(veloce_common::file_client::FileClient::new(
                "".into(),
                "".into(),
            )),
            loki_tx: mpsc::channel(1).0,
            accounting_store: Arc::new(crate::accounting::FileAccountingStore),
            container_store: Arc::new(
                crate::containers::ContainerStore::new("sqlite://")
                    .await
                    .unwrap(),
            ),
            component_registry,
            proxy_client: reqwest::Client::new(),
            terminal_sessions: DashMap::new(),
            vnc_sessions: DashMap::new(),
            missing_jobs: DashMap::new(),
            last_heartbeat: std::sync::atomic::AtomicU64::new(0),
            pmi_kvs: DashMap::new(),
            pmi_barriers: DashMap::new(),
            active_jobs: DashMap::new(),
            worker_resources: DashMap::new(),
            job_worker_stats: DashMap::new(),
            global_idle_start: DashMap::new(),
            completed_jobs: std::sync::atomic::AtomicU64::new(0),
            failed_jobs: std::sync::atomic::AtomicU64::new(0),
            jwks_cache: Arc::new(crate::auth::JwksCache::new()),
            event_tx: tokio::sync::broadcast::channel(1024).0,
            audit,
            rate_limiter: None,
        })
    }

    #[tokio::test]
    async fn test_component_registry_api_handlers() {
        let ctx = create_test_context().await;
        let principal = crate::auth::AuthenticatedPrincipal {
            user_id: "admin".to_string(),
            roles: vec!["admin".to_string()],
            auth_method: crate::auth::AuthMethod::ApiKey,
        };
        let ext = axum::Extension(principal);

        // 1. Issue token
        let payload = IssueComponentTokenRequest {
            component_id: "test-cli-client".to_string(),
            component_type: "client".to_string(),
            roles: vec!["cli".to_string()],
        };

        let response = api_issue_component_token(State(ctx.clone()), ext.clone(), Json(payload))
            .await
            .into_response();
        assert_eq!(response.status(), StatusCode::CREATED);

        // Parse response body
        let body_bytes = axum::body::to_bytes(response.into_body(), 1000)
            .await
            .unwrap();
        let issue_resp: IssueComponentTokenResponse = serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(issue_resp.component_id, "test-cli-client");
        assert!(issue_resp.token.starts_with("veloce_tok_"));

        // 2. List components
        let response = api_list_components(State(ctx.clone()), ext.clone())
            .await
            .into_response();
        assert_eq!(response.status(), StatusCode::OK);
        let body_bytes = axum::body::to_bytes(response.into_body(), 1000)
            .await
            .unwrap();
        let list: Vec<veloce_common::auth::RegisteredComponent> =
            serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].component_id, "test-cli-client");
        assert_eq!(list[0].component_type, ComponentType::Client);
        assert!(!list[0].revoked);

        // 3. Rotate token
        let response = api_rotate_component_token(
            State(ctx.clone()),
            ext.clone(),
            Path("test-cli-client".to_string()),
        )
        .await
        .into_response();
        assert_eq!(response.status(), StatusCode::OK);
        let body_bytes = axum::body::to_bytes(response.into_body(), 1000)
            .await
            .unwrap();
        let rotate_resp: IssueComponentTokenResponse = serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(rotate_resp.component_id, "test-cli-client");
        assert_ne!(rotate_resp.token, issue_resp.token);

        // 4. Revoke component
        let response = api_revoke_component(
            State(ctx.clone()),
            ext.clone(),
            Path("test-cli-client".to_string()),
        )
        .await
        .into_response();
        assert_eq!(response.status(), StatusCode::OK);

        // Verify revoked in list
        let response = api_list_components(State(ctx.clone()), ext.clone())
            .await
            .into_response();
        assert_eq!(response.status(), StatusCode::OK);
        let body_bytes = axum::body::to_bytes(response.into_body(), 1000)
            .await
            .unwrap();
        let list: Vec<veloce_common::auth::RegisteredComponent> =
            serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(list.len(), 1);
        assert!(list[0].revoked);
    }
}
