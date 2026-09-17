#![allow(clippy::all)]
use accounting::AccountingStore;
use anyhow::{Context, Result};
use clap::Parser;
use dashmap::DashMap;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::WithExportConfig;
use reqwest;
use serde_json;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::File;
use std::io::BufReader;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, Mutex, Notify};
use tokio::time::sleep;
use tracing::{error, info, warn};
use tracing_subscriber::prelude::*;
use veloce_common::{noise, JobStatus, PersistedState, Resources, UsageTracker};

pub mod accounting;
pub mod api;
pub mod audit;
pub mod auth;
pub mod component_registry;
mod containers;
mod job_logs;
mod job_secrets;
pub(crate) mod rate_limit;

mod config;
mod metrics_store;
mod state;

pub use config::{
    cluster_env_allowlist_enabled, load_config, sanitize_database_url, validate_job_submission,
    validate_production_config, AccountingConfig, Config, JobProfileSettings, WebConfig,
};
pub use metrics_store::generate_prometheus_metrics;
pub(crate) use metrics_store::read_metrics;
pub use state::{
    get_leader_peer_tx, get_worker_sender, is_controller_node, normalize_component_id,
    sync_active_state_to_dashmaps, ControllerContext, ControllerRole, GlobalState,
    MultinodeJobTracking, SharedContext, SharedState, WorkerHandle, WorkerRouting, WorkerSender,
    WorkerSyncState, WsTicket,
};

mod scheduler;
pub use scheduler::{
    check_timeouts, finalize_job, preempt_job, prune_jobs, run_scheduler, run_scheduling_pass,
    schedule_jobs, schedule_steps, submit_step,
};

mod ha;
mod handlers;
mod persistence;
mod submission;
pub use ha::{handle_peer, start_p2p_manager};
pub use handlers::{
    handle_client, handle_connection, handle_worker, handle_worker_message, is_authorized,
    send_to_worker,
};
pub use persistence::{job_to_usage, save_state, STATE_FILE};
pub use submission::submission_processor;

struct LokiProxy {
    client: reqwest::Client,
    loki_url: String,
}

impl LokiProxy {
    fn new(loki_url: String) -> Self {
        Self {
            client: reqwest::Client::new(),
            loki_url: format!("{}/loki/api/v1/push", loki_url),
        }
    }

    async fn push_logs(&self, labels: HashMap<String, String>, line: String, timestamp: u64) {
        let timestamp_ns = timestamp * 1_000_000_000;
        let payload = serde_json::json!({
            "streams": [{
                "stream": labels,
                "values": [[timestamp_ns.to_string(), line]]
            }]
        });

        let res = self.client.post(&self.loki_url).json(&payload).send().await;

        if let Err(e) = res {
            eprintln!("Failed to push logs to Loki: {}", e);
        }
    }
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Bind address for the controller
    #[arg(short, long)]
    bind: Option<String>,
    /// API port
    #[arg(long)]
    api_port: Option<u16>,
    /// Cluster secret
    #[arg(long, env = "VELOCE_SECRET")]
    secret: Option<String>,
    /// Fileserver URL
    #[arg(long, env = "VELOCE_FILESERVER")]
    fileserver_url: Option<String>,
    /// Peer controllers (comma-separated IPs)
    #[arg(long, env = "VELOCE_PEERS")]
    peers: Option<String>,
    /// TLS certificate path
    #[arg(long, env = "VELOCE_CERT_PATH")]
    cert_path: Option<String>,
    /// TLS key path
    #[arg(long, env = "VELOCE_KEY_PATH")]
    key_path: Option<String>,
    /// API key
    #[arg(long, env = "VELOCE_API_KEY")]
    api_key: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    // 1. Initialize tracing as early as possible
    let otlp_exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint("http://jaeger:4317")
        .build()
        .expect("Failed to build OTLP span exporter");

    let tracer_provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_batch_exporter(otlp_exporter)
        .build();
    let tracer = tracer_provider.tracer("veloce-controller");
    opentelemetry::global::set_tracer_provider(tracer_provider);

    let file_appender = tracing_appender::rolling::never(".", "veloce-controller.log");
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .with(tracing_subscriber::fmt::layer().with_writer(non_blocking))
        .with(tracing_opentelemetry::layer().with_tracer(tracer))
        .init();

    // Install default crypto provider for rustls
    let _ = rustls::crypto::ring::default_provider().install_default();

    if let Err(e) = std::fs::create_dir_all("data") {
        eprintln!("Failed to create data directory: {}", e);
    }
    let args = Args::parse();
    let mut config = load_config();

    // Override config with CLI args
    if let Some(b) = args.bind {
        config.bind_address = b;
    }
    if let Some(p) = args.api_port {
        config.api_port = Some(p);
    }
    if let Some(s) = args.secret {
        config.cluster_secret = Some(s);
    }
    if let Some(f) = args.fileserver_url {
        config.fileserver_url = Some(f);
    }
    if let Some(c) = args.cert_path {
        config.cert_path = c;
    }
    if let Some(k) = args.key_path {
        config.key_path = k;
    }
    if let Some(k) = args.api_key {
        config.api_key = Some(k);
    }

    validate_production_config(&config);

    let addr = config.bind_address.clone();

    // Internal Protocol Listener (Noise)
    let listener = TcpListener::bind(&addr).await.context("Failed to bind")?;
    info!("Controller listening on {} (Noise Protocol enabled)", addr);

    let mut solver_configs = veloce_common::SolverConfig::load_all("conf/solvers")
        .await
        .unwrap_or_default();
    if solver_configs.is_empty() {
        solver_configs = veloce_common::SolverConfig::load_all("controller/conf/solvers")
            .await
            .unwrap_or_default();
        if !solver_configs.is_empty() {
            log::info!(
                "Loaded {} solver configurations from controller/conf/solvers",
                solver_configs.len()
            );
        }
    } else {
        log::info!(
            "Loaded {} solver configurations from conf/solvers",
            solver_configs.len()
        );
    }

    let mut initial_state = GlobalState {
        workers: HashMap::new(),
        jobs: HashMap::new(),
        queue: VecDeque::new(),
        next_job_id: 1,
        usage_tracker: UsageTracker::new(),
        log_requests: HashMap::new(),
        scheduler_notify: Arc::new(Notify::new()),
        steps: HashMap::new(),
        next_step_id: 1,
        role: ControllerRole::Follower,
        leader_addr: None,
        leader_id: None,
        peers: HashMap::new(),
        job_output_buffers: HashMap::new(),
        multinode_job_tracking: MultinodeJobTracking::default(),
        api_port: config.api_port.unwrap_or(8080),
        step_waiters: HashMap::new(),
        step_output_waiters: HashMap::new(),
        solver_configs,
        component_log_requests: HashMap::new(),
        next_request_id: Arc::new(AtomicU64::new(1)),
        reservations: HashMap::new(),
    };

    // Load State
    if let Ok(file) = File::open(STATE_FILE) {
        let reader = BufReader::new(file);
        match bincode::deserialize_from::<_, PersistedState>(reader) {
            Ok(mut loaded) => {
                job_secrets::restore_persisted_state(&mut loaded);
                info!(
                    "Loaded state from {}. {} jobs.",
                    STATE_FILE,
                    loaded.jobs.len()
                );
                initial_state.jobs = loaded.jobs;
                initial_state.queue = loaded.queue;
                initial_state.next_job_id = loaded.next_job_id;
                initial_state.usage_tracker = loaded.usage_tracker;
                initial_state.steps = loaded.steps;
                initial_state.next_step_id = loaded.next_step_id;
                initial_state.reservations = loaded.reservations;

                // RECONSTITUTION: Instead of re-queuing, we reconstruct worker handles
                // in a disconnected state so that they can be re-adopted when workers connect.
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs();
                for (id, job) in &initial_state.jobs {
                    if matches!(job.status, JobStatus::Running) {
                        for worker_name in &job.assigned_workers {
                            let worker = initial_state
                                .workers
                                .entry(worker_name.clone())
                                .or_insert_with(|| {
                                    WorkerHandle {
                                        addr: "0.0.0.0:0".parse().unwrap(), // To be updated on connection
                                        hostname: "unknown".to_string(),
                                        resources: Resources {
                                            cpu_cores: 0,
                                            total_memory: 0,
                                            free_memory: 0,
                                            cpu_usage: 0.0,
                                            cpu_model: "unknown".into(),
                                            arch: "unknown".into(),
                                            os_name: "unknown".into(),
                                            os_version: "unknown".into(),
                                            kernel_version: "unknown".into(),
                                            host_name: "unknown".into(),
                                            load_avg: [0.0, 0.0, 0.0],
                                            disk_total: 0,
                                            disk_free: 0,
                                            uptime: 0,
                                            boot_time: 0,
                                            process_count: 0,
                                            swap_total: 0,
                                            swap_free: 0,
                                            version: "unknown".into(),
                                            gres: std::collections::BTreeMap::new(),
                                            cgroup_enabled: false,
                                        },
                                        routing: WorkerRouting::Direct(mpsc::channel(1).0), // Dummy until connection
                                        assigned_jobs: HashSet::new(),
                                        allocated_memory: 0,
                                        connection_id: 0,
                                        available_core_ids: Vec::new(),
                                        job_core_assignments: HashMap::new(),
                                        available_gres_ids: HashMap::new(),
                                        job_gres_assignments: HashMap::new(),
                                        connected: false,
                                        last_seen: now,
                                        cgroup_enabled: false,
                                        draining: false,
                                        current_pulse_interval: 2.0,
                                        features: 0,
                                    }
                                });
                            worker.assigned_jobs.insert(*id);
                            worker.allocated_memory =
                                worker.allocated_memory.saturating_add(job.req_memory);
                            // Core assignments are lost unless we persist them specifically,
                            // but we'll recover them from the worker's heartbeat if possible
                            // or just wait for worker to tell us.
                        }
                    }
                }
                info!(
                    "Reconstituted {} disconnected worker handles from job state.",
                    initial_state.workers.len()
                );
            }
            Err(e) => error!("Failed to parse state file: {}", e),
        }
    }

    let (sub_tx, sub_rx) = mpsc::channel(1024); // High capacity queue

    let (loki_tx, mut loki_rx) = mpsc::channel(10000);
    let loki_url = std::env::var("LOKI_URL").unwrap_or_else(|_| "http://loki:3100".to_string());
    let loki_proxy = Arc::new(LokiProxy::new(loki_url));

    tokio::spawn(async move {
        while let Some((labels, line, timestamp)) = loki_rx.recv().await {
            let proxy = loki_proxy.clone();
            // Simple concurrency limit or batching could be added here
            tokio::spawn(async move {
                proxy.push_logs(labels, line, timestamp).await;
            });
        }
    });

    let fileserver_url = config
        .fileserver_url
        .clone()
        .unwrap_or_else(|| "https://veloce-fileserver-ha:9001".to_string());
    let fileserver_api_key = config
        .fileserver_api_key
        .clone()
        .unwrap_or_else(|| "secret-fileserver-key".to_string());
    let file_client = Arc::new(veloce_common::file_client::FileClient::new(
        fileserver_url,
        fileserver_api_key,
    ));

    // Initialize pluggable accounting store
    let accounting_config = config.accounting.clone().unwrap_or_default();
    let raw_store: Arc<dyn accounting::AccountingStore> = match accounting_config.backend.as_str() {
        "sqlite" | "postgres" => {
            let db_url = if accounting_config.database_url.is_empty() {
                if accounting_config.backend == "sqlite" {
                    "sqlite://data/veloce_accounting.db".to_string()
                } else {
                    "postgresql://veloce:secure_pass@postgres:5432/veloce_accounting".to_string()
                }
            } else {
                accounting_config.database_url.clone()
            };
            let sanitized_db_url = sanitize_database_url(&db_url);
            match accounting::SqlxAccountingStore::new(
                &sanitized_db_url,
                accounting_config.max_connections,
                accounting_config.connection_timeout_seconds,
            )
            .await
            {
                Ok(s) => Arc::new(s),
                Err(e) => {
                    error!("Failed to initialize RDBMS accounting store ({}): {}. Falling back to file store.", sanitized_db_url, e);
                    Arc::new(accounting::FileAccountingStore)
                }
            }
        }
        _ => Arc::new(accounting::FileAccountingStore),
    };

    let accounting_store = accounting::ResilientAccountingStore::new(raw_store);
    let container_db_url = std::env::var("VELOCE_CONTAINER_DB_URL")
        .unwrap_or_else(|_| "sqlite://data/veloce_containers.db".to_string());
    let sanitized_container_db_url = sanitize_database_url(&container_db_url);
    let container_store = Arc::new(
        containers::ContainerStore::new(&sanitized_container_db_url)
            .await
            .unwrap(),
    );

    let component_db_url = std::env::var("VELOCE_COMPONENT_DB_URL")
        .or_else(|_| std::env::var("VELOCE_CONTAINER_DB_URL"))
        .unwrap_or_else(|_| "sqlite://data/veloce_components.db".to_string());
    let sanitized_component_db_url = sanitize_database_url(&component_db_url);
    let comp_conn_url = if sanitized_component_db_url.starts_with("sqlite:")
        && !sanitized_component_db_url.starts_with("sqlite://")
    {
        format!(
            "sqlite://{}",
            sanitized_component_db_url.trim_start_matches("sqlite:")
        )
    } else {
        sanitized_component_db_url.to_string()
    };
    sqlx::any::install_default_drivers();
    let comp_pool = sqlx::any::AnyPoolOptions::new()
        .max_connections(config::sqlite_pool_max_connections(&comp_conn_url))
        .connect(&comp_conn_url)
        .await
        .unwrap();
    let component_registry = Arc::new(
        component_registry::ComponentRegistry::new(comp_pool.clone())
            .await
            .unwrap(),
    );
    // Audit log (P1-6.1) - uses the same security/identity DB as the component registry for now
    let audit = Arc::new(audit::AuditLog::new(comp_pool.clone()).await.unwrap());

    // Sync next_job_id with database maximum to prevent ID collisions, and count historical finished jobs
    let mut init_completed = 0;
    let mut init_failed = 0;
    if let Ok(history) = accounting_store
        .query_history(&veloce_common::HistoryFilter::All)
        .await
    {
        let max_db_id = history.iter().map(|j| j.job_id).max().unwrap_or(0);
        if max_db_id >= initial_state.next_job_id {
            info!(
                "Synchronizing next_job_id from database: {} -> {}",
                initial_state.next_job_id,
                max_db_id + 1
            );
            initial_state.next_job_id = max_db_id + 1;
        }
        for job in &history {
            match job.status {
                JobStatus::Completed(_) => init_completed += 1,
                JobStatus::Failed(_) | JobStatus::Killed => init_failed += 1,
                _ => {}
            }
        }
    }
    let next_id = initial_state.next_job_id;

    let proxy_client = reqwest::Client::builder()
        .danger_accept_invalid_certs(!cfg!(feature = "production"))
        .build()
        .unwrap_or_default();

    let (event_tx, _) = tokio::sync::broadcast::channel(1024);

    let context = Arc::new(ControllerContext {
        config: config.clone(),
        ws_tickets: DashMap::new(),
        state: Mutex::new(initial_state),
        metrics_store: DashMap::new(),
        job_metrics_history: DashMap::new(),
        next_job_id: AtomicU64::new(next_id),
        submission_tx: sub_tx,
        file_client,
        loki_tx,
        accounting_store: accounting_store.clone(),
        container_store: container_store.clone(),
        component_registry: component_registry.clone(),
        audit: audit.clone(),
        proxy_client,
        terminal_sessions: DashMap::new(),
        vnc_sessions: DashMap::new(),
        missing_jobs: DashMap::new(),
        last_heartbeat: std::sync::atomic::AtomicU64::new(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        ),
        pmi_kvs: DashMap::new(),
        pmi_barriers: DashMap::new(),
        active_jobs: DashMap::new(),
        worker_resources: DashMap::new(),
        job_worker_stats: DashMap::new(),
        global_idle_start: DashMap::new(),
        completed_jobs: std::sync::atomic::AtomicU64::new(init_completed),
        failed_jobs: std::sync::atomic::AtomicU64::new(init_failed),
        jwks_cache: Arc::new(auth::JwksCache::new()),
        event_tx,
        rate_limiter: if rate_limit::rate_limit_enabled() {
            Some(rate_limit::new_default_rate_limiter())
        } else {
            None
        },
    });

    // Run initial sync
    {
        let state_lock = context.state.lock().await;
        sync_active_state_to_dashmaps(&context, &state_lock);
    }

    // Spawn Submission Processor
    let proc_ctx = context.clone();
    tokio::spawn(async move {
        submission_processor(proc_ctx, sub_rx).await;
    });

    // Spawn Scheduler
    let sched_ctx = context.clone();
    tokio::spawn(async move {
        run_scheduler(sched_ctx).await;
    });

    // Spawn Active State Syncer
    let sync_ctx = context.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(2));
        loop {
            interval.tick().await;
            let state_lock = sync_ctx.state.lock().await;
            if state_lock.role == ControllerRole::Leader {
                sync_active_state_to_dashmaps(&sync_ctx, &state_lock);
            }
        }
    });

    // Spawn Walltime Monitor
    let monitor_ctx = context.clone();
    tokio::spawn(async move {
        loop {
            sleep(Duration::from_secs(1)).await;
            check_timeouts(monitor_ctx.clone()).await;
        }
    });

    // Spawn Job Pruner
    let pruner_ctx = context.clone();
    let retention_period = config.job_retention_seconds.unwrap_or(86400);
    tokio::spawn(async move {
        loop {
            sleep(Duration::from_secs(3600)).await; // Run every hour
            prune_jobs(pruner_ctx.clone(), retention_period).await;
        }
    });

    // Spawn API Server (HTTPS)
    let api_ctx = context.clone();
    let api_config = config.clone();
    tokio::spawn(async move {
        if let Err(e) = api::run_api_server(api_ctx, api_config).await {
            error!("API server failed: {}", e);
        }
    });

    // Spawn P2P Manager
    if let Some(peers) = args.peers.clone() {
        let p2p_ctx = context.clone();
        let p2p_secret = config
            .cluster_secret
            .clone()
            .unwrap_or_else(|| "veloce_default_secret_change_me".into());
        tokio::spawn(async move {
            start_p2p_manager(p2p_ctx, peers, p2p_secret).await;
        });
    } else {
        // No peers, become Leader immediately
        let mut state = context.state.lock().await;
        state.role = ControllerRole::Leader;
        info!("No peers configured. Starting as standalone Leader.");
    }

    loop {
        let (stream, addr) = listener.accept().await?;
        let ctx = context.clone();
        let secret = match config.cluster_secret.clone() {
            Some(s) if s == "veloce_default_secret_change_me" => {
                warn!("Using the DEFAULT cluster secret is extremely insecure! Change VELOCE_SECRET or cluster_secret in config.");
                s
            }
            Some(s) => s,
            None => {
                error!("CRITICAL: No cluster_secret configured. Set VELOCE_SECRET environment variable or cluster_secret in veloce.toml");
                std::process::exit(1);
            }
        };

        tokio::spawn(async move {
            match noise::upgrade_responder(stream, &secret).await {
                Ok(noise_stream) => {
                    if let Err(e) = handle_connection(noise_stream, addr, ctx).await {
                        error!("Connection error with {}: {}", addr, e);
                    }
                }
                Err(e) => error!("Noise Handshake error with {}: {}", addr, e),
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        persistence::merge_usage_report_with_job, scheduler::calculate_effective_priority,
    };
    use std::collections::BTreeMap;
    use veloce_common::JobInfo;
    use veloce_common::QosLevel;

    #[test]
    fn test_validate_job_submission_binary_prefix() {
        let exec = veloce_common::job_policy::JobExecutionOptions::default();
        let prefixes = vec!["/usr/bin/".to_string(), "/opt/veloce/".to_string()];
        assert!(validate_job_submission("/usr/bin/python3", &[], &exec, &prefixes).is_ok());
        assert!(validate_job_submission("/bin/bash", &[], &exec, &prefixes).is_err());
    }

    #[test]
    fn test_usage_tracker_decay() {
        let mut tracker = UsageTracker::new();
        tracker.accrue("user1", 100.0);

        // Force decay time
        tracker.last_decay -= 120; // 2 minutes ago

        tracker.apply_decay();

        // 100 * 0.9 * 0.9 = 81.0
        let usage = tracker.get_usage("user1");
        assert!((usage - 81.0).abs() < 0.001);
    }

    #[test]
    fn test_effective_priority() {
        let mut tracker = UsageTracker::new();
        tracker.accrue("hog_user", 10000.0); // High usage

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let job_normal = JobInfo {
            container_asset: None,
            id: 1,
            job_name: None,
            job_comment: None,
            binary: "test".into(),
            args: vec![],
            status: JobStatus::Pending,
            req_nodes: 1,
            req_cores: 1,
            req_memory: 100,
            assigned_workers: vec![],
            walltime: 0,
            start_time: None,
            priority: 0,
            user_id: "normal_user".into(),
            working_directory: "/".into(),
            queued_time: now - 100, // Waiting 100s
            current_cpu_usage: 0.0,
            current_memory_usage: 0,
            end_time: None,
            reason: None,
            array_id: None,
            array_task_id: None,
            inputs: Vec::new(),
            cgroup_active: false,
            is_idle: false,
            idle_duration: 0,
            gres_req: BTreeMap::new(),
            allocated_cores: HashMap::new(),
            allocated_gres: HashMap::new(),
            mpi_stats: None,
            env_vars: Vec::new(),
            secret: "test_secret".into(),
            stdout_file_id: None,
            stderr_file_id: None,
            workdir_file_id: None,
            worker_log_files: Default::default(),
            output_artifacts: Vec::new(),
            wait_for_licenses: false,
            estimated_walltime: None,
            priority_offset: None,
            dependencies: None,
            dependency_specs: None,
            qos: QosLevel::Production,
            vnc_enabled: false,
            inherit_host_env: false,
            env_allowlist: None,
            job_profile: None,
            interactive_port: None,
        };

        let job_hog = JobInfo {
            id: 2,
            job_name: None,
            job_comment: None,
            user_id: "hog_user".into(),
            queued_time: now - 10, // Waiting 10s
            ..job_normal.clone()
        };

        // Normal: 0 + (100 * 0.5) - (0 * 0.01) = 50.0
        let score_normal = calculate_effective_priority(&job_normal, &tracker);

        // Hog: 0 + (10 * 0.5) - (10000 * 0.01) = 5 - 100 = -95.0
        let score_hog = calculate_effective_priority(&job_hog, &tracker);

        assert!(score_normal > score_hog);
    }

    #[test]
    fn test_priority_boosting() {
        let tracker = UsageTracker::new();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Set env vars to short values for testing
        std::env::set_var("VELOCE_STARVATION_THRESHOLD", "10"); // 10 seconds threshold
        std::env::set_var("VELOCE_BOOST_RATE", "5.0");

        let job_short_wait = JobInfo {
            container_asset: None,
            id: 1,
            job_name: None,
            job_comment: None,
            binary: "sleep".into(),
            args: vec!["10".into()],
            status: JobStatus::Pending,
            req_nodes: 1,
            req_cores: 1,
            req_memory: 1024,
            assigned_workers: Vec::new(),
            walltime: 0,
            start_time: None,
            priority: 0,
            user_id: "user1".into(),
            working_directory: "/tmp".into(),
            queued_time: now - 5, // Waiting 5s (no boost yet)
            current_cpu_usage: 0.0,
            current_memory_usage: 0,
            is_idle: false,
            idle_duration: 0,
            end_time: None,
            env_vars: Vec::new(),
            reason: None,
            array_id: None,
            array_task_id: None,
            inputs: Vec::new(),
            cgroup_active: false,
            gres_req: BTreeMap::new(),
            allocated_cores: HashMap::new(),
            allocated_gres: HashMap::new(),
            mpi_stats: None,
            secret: "test_secret".into(),
            stdout_file_id: None,
            stderr_file_id: None,
            workdir_file_id: None,
            worker_log_files: Default::default(),
            output_artifacts: Vec::new(),
            wait_for_licenses: false,
            estimated_walltime: None,
            priority_offset: None,
            dependencies: None,
            dependency_specs: None,
            qos: QosLevel::Production,
            vnc_enabled: false,
            inherit_host_env: false,
            env_allowlist: None,
            job_profile: None,
            interactive_port: None,
        };

        let job_long_wait = JobInfo {
            id: 2,
            job_name: None,
            job_comment: None,
            queued_time: now - 70, // Waiting 70s (exceeds threshold by 60s / 1 min)
            ..job_short_wait.clone()
        };

        let score_short = calculate_effective_priority(&job_short_wait, &tracker);
        let score_long = calculate_effective_priority(&job_long_wait, &tracker);

        // Short wait: base(0) + wait(5 * 0.5) - penalty(0) = 2.5
        // Long wait: base(0) + wait(70 * 0.5) + boost((60/60)^1.5 * 5.0) - penalty(0) = 35 + 5 = 40.0
        assert_eq!(score_short, 2.5);
        assert_eq!(score_long, 40.0);

        // Clean up env vars
        std::env::remove_var("VELOCE_STARVATION_THRESHOLD");
        std::env::remove_var("VELOCE_BOOST_RATE");
    }

    #[test]
    fn merge_usage_report_preserves_controller_workers() {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let mut job = JobInfo {
            container_asset: None,
            id: 42,
            job_name: Some("mpi-smoke".into()),
            job_comment: Some("six ranks".into()),
            binary: "sleep".into(),
            args: vec!["30".into()],
            status: JobStatus::Running,
            req_nodes: 6,
            req_cores: 1,
            req_memory: 1024,
            assigned_workers: vec!["worker-a".into(), "worker-b".into()],
            walltime: 0,
            start_time: Some(now),
            priority: 0,
            user_id: "user1".into(),
            working_directory: "/tmp".into(),
            queued_time: now,
            current_cpu_usage: 0.0,
            current_memory_usage: 0,
            is_idle: false,
            idle_duration: 0,
            end_time: None,
            env_vars: Vec::new(),
            reason: None,
            array_id: None,
            array_task_id: None,
            inputs: Vec::new(),
            cgroup_active: false,
            gres_req: BTreeMap::new(),
            allocated_cores: HashMap::new(),
            allocated_gres: HashMap::new(),
            mpi_stats: None,
            secret: "controller-secret".into(),
            stdout_file_id: None,
            stderr_file_id: None,
            workdir_file_id: None,
            worker_log_files: Default::default(),
            output_artifacts: Vec::new(),
            wait_for_licenses: false,
            estimated_walltime: None,
            priority_offset: None,
            dependencies: None,
            dependency_specs: None,
            qos: QosLevel::Production,
            vnc_enabled: false,
            inherit_host_env: false,
            env_allowlist: None,
            job_profile: None,
            interactive_port: None,
        };
        job.stdout_file_id = Some("controller-stdout".into());

        let usage = veloce_common::JobUsage {
            job_id: 42,
            job_name: None,
            job_comment: None,
            command_line: "sleep 30".into(),
            user_id: "user1".into(),
            submission_time: now,
            start_time: Some(now),
            end_time: Some(now + 30),
            exit_code: Some(0),
            status: JobStatus::Completed(0),
            cpu_time_ms: 1234,
            max_memory_bytes: 4096,
            req_nodes: 6,
            req_cores: 1,
            req_memory: 1024,
            array_id: None,
            array_task_id: None,
            assigned_workers: Vec::new(),
            gres_req: BTreeMap::new(),
            cgroup_active: true,
            secret: "worker-secret".into(),
            stdout_file_id: Some("worker-stdout".into()),
            stderr_file_id: Some("worker-stderr".into()),
            workdir_file_id: Some("worker-workdir".into()),
            worker_log_files: Default::default(),
            output_artifacts: Vec::new(),
            wait_for_licenses: false,
            estimated_walltime: None,
            priority_offset: None,
            dependencies: None,
            dependency_specs: None,
            qos: QosLevel::Production,
            container_asset: None,
        };

        let merged = merge_usage_report_with_job(usage, &job);

        assert_eq!(merged.assigned_workers, vec!["worker-a", "worker-b"]);
        assert_eq!(merged.job_name.as_deref(), Some("mpi-smoke"));
        assert_eq!(merged.job_comment.as_deref(), Some("six ranks"));
        assert_eq!(merged.status, JobStatus::Completed(0));
        assert_eq!(merged.exit_code, Some(0));
        assert_eq!(merged.cpu_time_ms, 1234);
        assert_eq!(merged.max_memory_bytes, 4096);
        assert!(merged.cgroup_active);
        assert_eq!(merged.stdout_file_id.as_deref(), Some("worker-stdout"));
        assert_eq!(merged.stderr_file_id.as_deref(), Some("worker-stderr"));
        assert_eq!(merged.workdir_file_id.as_deref(), Some("worker-workdir"));
        assert_eq!(merged.secret, "controller-secret");
    }
}

#[cfg(test)]
mod scheduler_tests {
    use super::*;
    use crate::{
        scheduler::are_dependencies_satisfied,
        state::{rebuild_worker_allocations, reconcile_missing_worker_jobs},
    };
    use futures::{SinkExt, StreamExt};
    use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use tokio::sync::mpsc;
    use tokio_util::codec::Framed;
    use veloce_common::{JobInfo, Message, MessageCodec, NodeMetrics, QosLevel};

    fn create_mock_worker(
        _id: &str,
        cores: usize,
        mem: u64,
    ) -> (WorkerHandle, mpsc::Receiver<Message>) {
        let (tx, rx) = mpsc::channel(10);
        let resources = Resources {
            cpu_cores: cores,
            total_memory: mem * 1024 * 1024,
            free_memory: mem * 1024 * 1024,
            cpu_usage: 0.0,
            cpu_model: "MockCPU".into(),
            arch: "x86_64".into(),
            os_name: "MockOS".into(),
            os_version: "1.0".into(),
            kernel_version: "5.0".into(),
            host_name: "mock-host".into(),
            load_avg: [0.0, 0.0, 0.0],
            disk_total: 0,
            disk_free: 0,
            uptime: 0,
            boot_time: 0,
            process_count: 0,
            swap_total: 0,
            swap_free: 0,
            version: "v1".into(),
            gres: BTreeMap::new(),
            cgroup_enabled: false,
        };

        let handle = WorkerHandle {
            addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080),
            hostname: "mock-host".into(),
            resources,
            routing: WorkerRouting::Direct(tx),
            assigned_jobs: HashSet::new(),
            allocated_memory: 0,
            connection_id: 0,
            available_core_ids: (0..cores).collect(),
            job_core_assignments: HashMap::new(),
            available_gres_ids: HashMap::new(),
            job_gres_assignments: HashMap::new(),
            connected: true,
            last_seen: 0,
            cgroup_enabled: false,
            draining: false,
            current_pulse_interval: 2.0,
            features: 0,
        };

        (handle, rx)
    }

    fn create_mock_job(id: u64, nodes: usize, cores: u32, mem: u64, prio: u32) -> JobInfo {
        JobInfo {
            container_asset: None,
            id,
            job_name: None,
            job_comment: None,
            binary: "/bin/sleep".to_string(),
            args: vec!["1".to_string()],
            status: JobStatus::Pending,
            req_nodes: nodes,
            req_cores: cores,
            req_memory: mem,
            assigned_workers: Vec::new(),
            walltime: 3600,
            start_time: None,
            priority: prio,
            user_id: "test".to_string(),
            working_directory: "/tmp".to_string(),
            queued_time: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            current_cpu_usage: 0.0,
            current_memory_usage: 0,
            is_idle: false,
            idle_duration: 0,
            end_time: None,
            reason: None,
            array_id: None,
            array_task_id: None,
            inputs: Vec::new(),
            cgroup_active: false,
            gres_req: BTreeMap::new(),
            allocated_cores: HashMap::new(),
            allocated_gres: HashMap::new(),
            mpi_stats: None,
            env_vars: Vec::new(),
            secret: "test_secret".into(),
            stdout_file_id: None,
            stderr_file_id: None,
            workdir_file_id: None,
            worker_log_files: Default::default(),
            output_artifacts: Vec::new(),
            wait_for_licenses: false,
            estimated_walltime: None,
            priority_offset: None,
            dependencies: None,
            dependency_specs: None,
            qos: QosLevel::Production,
            vnc_enabled: false,
            inherit_host_env: false,
            env_allowlist: None,
            job_profile: None,
            interactive_port: None,
        }
    }

    fn create_mock_state() -> GlobalState {
        GlobalState {
            workers: HashMap::new(),
            jobs: HashMap::new(),
            queue: VecDeque::new(),
            next_job_id: 1,
            usage_tracker: UsageTracker::new(),
            log_requests: HashMap::new(),
            scheduler_notify: Arc::new(Notify::new()),
            steps: HashMap::new(),
            next_step_id: 1,
            role: ControllerRole::Leader,
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
        }
    }

    #[test]
    fn test_rebuild_worker_allocations_recovers_persisted_running_job() {
        let (worker, _) = create_mock_worker("worker1", 4, 1000);
        let mut job = create_mock_job(1, 1, 2, 500, 0);
        job.status = JobStatus::Running;
        job.assigned_workers = vec!["worker1".into()];
        job.allocated_cores.insert("worker1".into(), vec![0, 1]);

        let mut jobs = HashMap::new();
        jobs.insert(job.id, job);

        let rebuilt = rebuild_worker_allocations("worker1", &worker.resources, &jobs);

        assert_eq!(rebuilt.assigned_jobs, HashSet::from([1]));
        assert_eq!(rebuilt.allocated_memory, 500);
        assert_eq!(rebuilt.available_core_ids, vec![2, 3]);
        assert_eq!(rebuilt.job_core_assignments.get(&1), Some(&vec![0, 1]));
    }

    #[test]
    fn test_gateway_worker_registration_rebuilds_allocations() {
        let (mut worker, _) = create_mock_worker("worker1", 4, 1000);
        worker.routing = WorkerRouting::Gateway("controller-2".into());
        worker.assigned_jobs.insert(1);
        worker.available_core_ids.clear();
        worker.job_core_assignments.clear();

        let mut job = create_mock_job(1, 1, 2, 500, 0);
        job.status = JobStatus::Running;
        job.assigned_workers = vec!["worker1".into()];
        job.allocated_cores.insert("worker1".into(), vec![0, 1]);

        let mut jobs = HashMap::new();
        jobs.insert(job.id, job);

        let rebuilt = rebuild_worker_allocations("worker1", &worker.resources, &jobs);
        worker.assigned_jobs = rebuilt.assigned_jobs;
        worker.allocated_memory = rebuilt.allocated_memory;
        worker.available_core_ids = rebuilt.available_core_ids;
        worker.job_core_assignments = rebuilt.job_core_assignments;

        assert!(matches!(worker.routing, WorkerRouting::Gateway(_)));
        assert_eq!(worker.assigned_jobs, HashSet::from([1]));
        assert_eq!(worker.available_core_ids, vec![2, 3]);
        assert_eq!(worker.job_core_assignments.get(&1), Some(&vec![0, 1]));
    }

    #[test]
    fn test_reconcile_missing_worker_jobs_finalizes_stale_running_job() {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let mut state = create_mock_state();
        let (mut worker, _) = create_mock_worker("worker1", 4, 1000);
        worker.assigned_jobs.insert(1);
        worker.available_core_ids = vec![2, 3];
        worker.job_core_assignments.insert(1, vec![0, 1]);
        worker.allocated_memory = 500;
        state.workers.insert("worker1".into(), worker);

        let mut job = create_mock_job(1, 1, 2, 500, 0);
        job.status = JobStatus::Running;
        job.assigned_workers = vec!["worker1".into()];
        job.start_time = Some(now - 30);
        job.allocated_cores.insert("worker1".into(), vec![0, 1]);
        state.jobs.insert(1, job);

        let missing_jobs = DashMap::new();
        missing_jobs.insert(1, now - 11);
        reconcile_missing_worker_jobs(&missing_jobs, &mut state, "worker1", &HashSet::new(), now);

        assert!(matches!(
            state.jobs.get(&1).unwrap().status,
            JobStatus::Failed(_)
        ));
        assert!(missing_jobs.get(&1).is_none());
        let worker = state.workers.get("worker1").unwrap();
        assert!(worker.assigned_jobs.is_empty());
        assert_eq!(worker.allocated_memory, 0);
        assert_eq!(worker.available_core_ids, vec![0, 1, 2, 3]);
    }

    #[tokio::test]
    async fn test_finalize_job_recovers_persisted_cores_without_runtime_assignment() {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let mut state = create_mock_state();
        let (mut worker, _) = create_mock_worker("worker1", 4, 1000);
        worker.assigned_jobs.insert(1);
        worker.available_core_ids.clear();
        worker.job_core_assignments.clear();
        worker.allocated_memory = 500;
        state.workers.insert("worker1".into(), worker);

        let mut job = create_mock_job(1, 1, 2, 500, 0);
        job.status = JobStatus::Running;
        job.assigned_workers = vec!["worker1".into()];
        job.start_time = Some(now - 100);
        job.allocated_cores.insert("worker1".into(), vec![0, 1]);
        state.jobs.insert(1, job);

        finalize_job(&mut state, 1, JobStatus::Completed(0));

        let worker = state.workers.get("worker1").unwrap();
        assert!(worker.assigned_jobs.is_empty());
        assert_eq!(worker.allocated_memory, 0);
        assert_eq!(worker.available_core_ids, vec![0, 1, 2, 3]);
    }

    #[tokio::test]
    async fn test_job_dependency_evaluator() {
        let mut jobs = HashMap::new();

        // Target job (Job 2) which depends on Job 1
        let mut job2 = create_mock_job(2, 1, 1, 64, 10);
        job2.dependency_specs = Some(vec!["afterok:1".to_string()]);

        // Scenario 1: Parent Job 1 does not exist in history or active jobs (returns Ok(false) to keep waiting)
        assert_eq!(
            are_dependencies_satisfied(&job2, &jobs, &HashMap::new()),
            Ok(false)
        );

        // Scenario 2: Parent Job 1 is Pending
        let job1 = create_mock_job(1, 1, 1, 64, 10);
        jobs.insert(1, job1.clone());
        assert_eq!(
            are_dependencies_satisfied(&job2, &jobs, &HashMap::new()),
            Ok(false)
        );

        // Scenario 3: Parent Job 1 is Running
        jobs.get_mut(&1).unwrap().status = JobStatus::Running;
        assert_eq!(
            are_dependencies_satisfied(&job2, &jobs, &HashMap::new()),
            Ok(false)
        );

        // Scenario 4: Parent Job 1 is Completed (Success)
        jobs.get_mut(&1).unwrap().status = JobStatus::Completed(0);
        assert_eq!(
            are_dependencies_satisfied(&job2, &jobs, &HashMap::new()),
            Ok(true)
        );

        // Scenario 5: Parent Job 1 Failed (with afterok:1 dependency)
        jobs.get_mut(&1).unwrap().status = JobStatus::Failed("Error".to_string());
        assert!(are_dependencies_satisfied(&job2, &jobs, &HashMap::new()).is_err());

        // Scenario 6: Parent Job 1 Failed, but dependency is afterany:1
        job2.dependency_specs = Some(vec!["afterany:1".to_string()]);
        assert_eq!(
            are_dependencies_satisfied(&job2, &jobs, &HashMap::new()),
            Ok(true)
        );

        // Scenario 7: Parent Job 1 Completed, dependency is afterany:1
        jobs.get_mut(&1).unwrap().status = JobStatus::Completed(0);
        assert_eq!(
            are_dependencies_satisfied(&job2, &jobs, &HashMap::new()),
            Ok(true)
        );

        // Scenario 8: Parent Job 1 Completed, dependency is afternotok:1
        job2.dependency_specs = Some(vec!["afternotok:1".to_string()]);
        assert!(are_dependencies_satisfied(&job2, &jobs, &HashMap::new()).is_err());

        // Scenario 9: Parent Job 1 Failed, dependency is afternotok:1
        jobs.get_mut(&1).unwrap().status = JobStatus::Failed("Error".to_string());
        assert_eq!(
            are_dependencies_satisfied(&job2, &jobs, &HashMap::new()),
            Ok(true)
        );
    }

    #[tokio::test]
    async fn test_schedule_basic_success() {
        let mut state = GlobalState {
            workers: HashMap::new(),
            jobs: HashMap::new(),
            queue: VecDeque::new(),
            next_job_id: 1,
            usage_tracker: UsageTracker::new(),
            log_requests: HashMap::new(),
            scheduler_notify: Arc::new(Notify::new()),
            steps: HashMap::new(),
            next_step_id: 1,

            role: ControllerRole::Leader,
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

        let (worker, mut rx) = create_mock_worker("worker1", 4, 1000);
        state.workers.insert("worker1".into(), worker);

        let job = create_mock_job(1, 1, 2, 500, 0);
        state.jobs.insert(1, job);
        state.queue.push_back(1);

        schedule_jobs(&mut state, &HashMap::new());

        // Verify Job Status
        let updated_job = state.jobs.get(&1).unwrap();
        assert_eq!(updated_job.status, JobStatus::Running);
        assert_eq!(updated_job.assigned_workers, vec!["worker1"]);

        // Verify Message
        match rx.recv().await {
            Some(Message::RunJob { job_id, .. }) => assert_eq!(job_id, 1),
            _ => panic!("Expected RunJob message"),
        }
    }

    #[tokio::test]
    async fn test_schedule_insufficient_resources() {
        let mut state = GlobalState {
            workers: HashMap::new(),
            jobs: HashMap::new(),
            queue: VecDeque::new(),
            next_job_id: 1,
            usage_tracker: UsageTracker::new(),
            log_requests: HashMap::new(),
            scheduler_notify: Arc::new(Notify::new()),
            steps: HashMap::new(),
            next_step_id: 1,

            role: ControllerRole::Leader,
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

        let (worker, mut rx) = create_mock_worker("worker1", 2, 1000); // Only 2 cores
        state.workers.insert("worker1".into(), worker);

        let job = create_mock_job(1, 1, 4, 500, 0); // Requests 4 cores
        state.jobs.insert(1, job);
        state.queue.push_back(1);

        schedule_jobs(&mut state, &HashMap::new());

        let updated_job = state.jobs.get(&1).unwrap();
        assert_eq!(updated_job.status, JobStatus::Pending);

        assert!(rx.try_recv().is_err()); // No message
    }

    #[tokio::test]
    async fn test_schedule_priority() {
        let mut state = GlobalState {
            workers: HashMap::new(),
            jobs: HashMap::new(),
            queue: VecDeque::new(),
            next_job_id: 1,
            usage_tracker: UsageTracker::new(),
            log_requests: HashMap::new(),
            scheduler_notify: Arc::new(Notify::new()),
            steps: HashMap::new(),
            next_step_id: 1,

            role: ControllerRole::Leader,
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

        // Worker has 4 cores.
        // Job 1 needs 4 cores (Low Prio).
        // Job 2 needs 4 cores (High Prio).
        // Only one can run.
        let (worker, mut _rx) = create_mock_worker("worker1", 4, 1000);
        state.workers.insert("worker1".into(), worker);

        let job1 = create_mock_job(1, 1, 4, 100, 0);
        let job2 = create_mock_job(2, 1, 4, 100, 100);

        state.jobs.insert(1, job1);
        state.jobs.insert(2, job2);
        state.queue.push_back(1);
        state.queue.push_back(2);

        schedule_jobs(&mut state, &HashMap::new());

        // Job 2 should run
        assert_eq!(state.jobs.get(&2).unwrap().status, JobStatus::Running);
        assert_eq!(state.jobs.get(&1).unwrap().status, JobStatus::Pending);
    }

    #[tokio::test]
    async fn test_finalize_job() {
        let mut state = GlobalState {
            workers: HashMap::new(),
            jobs: HashMap::new(),
            queue: VecDeque::new(),
            next_job_id: 1,
            usage_tracker: UsageTracker::new(),
            log_requests: HashMap::new(),
            scheduler_notify: Arc::new(Notify::new()),
            steps: HashMap::new(),
            next_step_id: 1,

            role: ControllerRole::Leader,
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

        let (mut worker, _) = create_mock_worker("worker1", 4, 1000);
        // Manually assign resources to simulate running state
        worker.assigned_jobs.insert(1);
        worker.job_core_assignments.insert(1, vec![0, 1]); // 2 cores used
        worker.available_core_ids = vec![2, 3]; // 2 cores left
        worker.allocated_memory = 500;

        state.workers.insert("worker1".into(), worker);

        let mut job = create_mock_job(1, 1, 2, 500, 0);
        job.status = JobStatus::Running;
        job.assigned_workers = vec!["worker1".into()];
        job.start_time = Some(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs()
                - 100,
        ); // Started 100s ago

        state.jobs.insert(1, job);

        // Run finalize
        finalize_job(&mut state, 1, JobStatus::Completed(0));

        // Check Job Status
        let updated_job = state.jobs.get(&1).unwrap();
        assert_eq!(updated_job.status, JobStatus::Completed(0));
        assert!(updated_job.end_time.is_some());

        // Check Worker Resources Released
        let updated_worker = state.workers.get("worker1").unwrap();
        assert!(updated_worker.assigned_jobs.is_empty());
        assert_eq!(updated_worker.allocated_memory, 0);
        assert_eq!(updated_worker.available_core_ids.len(), 4); // All cores returned

        // Check Usage Accrual
        // 100s * 1 node * 2 cores = 200 cost
        let usage = state.usage_tracker.get_usage("test");
        assert!((usage - 200.0).abs() < 0.001);
    }

    #[tokio::test]
    async fn test_schedule_reason() {
        let mut state = GlobalState {
            workers: HashMap::new(),
            jobs: HashMap::new(),
            queue: VecDeque::new(),
            next_job_id: 1,
            usage_tracker: UsageTracker::new(),
            log_requests: HashMap::new(),
            scheduler_notify: Arc::new(Notify::new()),
            steps: HashMap::new(),
            next_step_id: 1,

            role: ControllerRole::Leader,
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

        let (worker, _) = create_mock_worker("worker1", 2, 1000); // 2 cores
        state.workers.insert("worker1".into(), worker);

        let job = create_mock_job(1, 1, 4, 500, 0); // Need 4 cores
        state.jobs.insert(1, job);
        state.queue.push_back(1);

        schedule_jobs(&mut state, &HashMap::new());

        let updated_job = state.jobs.get(&1).unwrap();
        assert_eq!(updated_job.status, JobStatus::Pending);
        assert!(updated_job.reason.is_some());
        let reason = updated_job.reason.as_ref().unwrap();
        assert!(reason.contains("Waiting for resources"));
        assert!(reason.contains("worker1: [CPU]"));
    }

    #[tokio::test]
    async fn test_schedule_most_available() {
        let mut state = GlobalState {
            workers: HashMap::new(),
            jobs: HashMap::new(),
            queue: VecDeque::new(),
            next_job_id: 1,
            usage_tracker: UsageTracker::new(),
            log_requests: HashMap::new(),
            scheduler_notify: Arc::new(Notify::new()),
            steps: HashMap::new(),
            next_step_id: 1,

            role: ControllerRole::Leader,
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

        // Worker Small: 2 cores
        let (worker_small, _) = create_mock_worker("worker_small", 2, 1000);
        state.workers.insert("worker_small".into(), worker_small);

        // Worker Big: 10 cores
        let (worker_big, _) = create_mock_worker("worker_big", 10, 1000);
        state.workers.insert("worker_big".into(), worker_big);

        // Job needs 1 core. Should go to Big worker (10 > 2)
        let job = create_mock_job(1, 1, 1, 100, 0);
        state.jobs.insert(1, job);
        state.queue.push_back(1);

        schedule_jobs(&mut state, &HashMap::new());

        let updated_job = state.jobs.get(&1).unwrap();
        assert_eq!(updated_job.status, JobStatus::Running);
        assert_eq!(updated_job.assigned_workers, vec!["worker_big"]);
    }

    #[tokio::test]
    async fn test_prometheus_metrics() {
        let mut state = GlobalState {
            workers: HashMap::new(),
            jobs: HashMap::new(),
            queue: VecDeque::new(),
            next_job_id: 1,
            usage_tracker: UsageTracker::new(),
            log_requests: HashMap::new(),
            scheduler_notify: Arc::new(Notify::new()),
            steps: HashMap::new(),
            next_step_id: 1,

            role: ControllerRole::Leader,
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

        // Add a mock worker
        let (worker, _) = create_mock_worker("worker1", 4, 1000);
        state.workers.insert("worker1".into(), worker);

        // Add a completed mock job
        let mut job = create_mock_job(123, 1, 1, 100, 0);
        job.status = JobStatus::Completed(0);
        job.queued_time = 1000;
        job.start_time = Some(1010);
        job.end_time = Some(1020);
        job.binary = "my_app".into();
        job.user_id = "testuser".into();
        state.jobs.insert(123, job);

        let ctx = Arc::new(ControllerContext {
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
            accounting_store: Arc::new(accounting::FileAccountingStore),
            container_store: Arc::new(
                crate::containers::ContainerStore::new(
                    "sqlite://test_containers_main?mode=memory&cache=shared",
                )
                .await
                .unwrap(),
            ),
            component_registry: {
                sqlx::any::install_default_drivers();
                let comp_pool = sqlx::any::AnyPoolOptions::new()
                    .max_connections(1)
                    .connect("sqlite://test_components_main?mode=memory&cache=shared")
                    .await
                    .unwrap();
                Arc::new(
                    component_registry::ComponentRegistry::new(comp_pool.clone())
                        .await
                        .unwrap(),
                )
            },
            audit: {
                // In-memory audit for tests
                let audit_pool = sqlx::any::AnyPoolOptions::new()
                    .max_connections(1)
                    .connect("sqlite://test_audit_main?mode=memory&cache=shared")
                    .await
                    .unwrap();
                Arc::new(audit::AuditLog::new(audit_pool).await.unwrap())
            },
            proxy_client: reqwest::Client::new(),
            terminal_sessions: DashMap::new(),
            vnc_sessions: DashMap::new(),
            missing_jobs: DashMap::new(),
            last_heartbeat: std::sync::atomic::AtomicU64::new(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
            ),
            pmi_kvs: DashMap::new(),
            pmi_barriers: DashMap::new(),
            active_jobs: DashMap::new(),
            worker_resources: DashMap::new(),
            job_worker_stats: DashMap::new(),
            global_idle_start: DashMap::new(),
            completed_jobs: std::sync::atomic::AtomicU64::new(0),
            failed_jobs: std::sync::atomic::AtomicU64::new(0),
            jwks_cache: Arc::new(auth::JwksCache::new()),
            event_tx: tokio::sync::broadcast::channel(1024).0,
            rate_limiter: None,
        });

        // Sync active state
        {
            let state_lock = ctx.state.lock().await;
            sync_active_state_to_dashmaps(&ctx, &state_lock);
        }

        // Add mock metrics
        let metrics = NodeMetrics {
            node_id: "worker1".into(),
            timestamp: 1234567890,
            running_jobs: 1,
            cpu_load: 10.5,
            memory_usage: 2048,
            net_rx_rate: 100,
            net_tx_rate: 200,
            net_packets_rx_rate: 0,
            net_packets_tx_rate: 0,
            net_errors: 0,
            net_drops: 0,
            disk_read_rate: 0,
            disk_write_rate: 0,
            disk_read_ops_rate: 0,
            disk_write_ops_rate: 0,
            disk_total: 0,
            disk_usage: 0,
            load_avg: [0.0, 0.0, 0.0],
            memory_total: 0,
            procs_running: 0,
            procs_blocked: 0,
            swap_usage: 0,
            process_count: 0,
            uptime: 0,
            cgroup_enabled: false,
            gpu_usage: None,
            gpu_mem_usage: None,
            gpu_temp: None,
        };
        ctx.metrics_store.insert("worker1".into(), metrics);

        let output = generate_prometheus_metrics(ctx).await;

        assert!(output.contains("veloce_node_cpu_load{node=\"worker1\", name=\"mock-host\"} 10.5"));
        assert!(output
            .contains("veloce_node_memory_usage_bytes{node=\"worker1\", name=\"mock-host\"} 2048"));

        // Verify Job Metrics
        assert!(output.contains("veloce_job_info{id=\"123\", user=\"testuser\", status=\"completed\", binary=\"my_app\", array_id=\"\", array_task_id=\"\", priority=\"0\", nodes=\"\", req_cores=\"1\", req_memory_mb=\"100\", used_cpu_ms=\"0\", used_mem_mb=\"0\"} 1"));
    }

    #[tokio::test]
    async fn test_schedule_reservations() {
        use std::collections::HashSet;

        let mut state = GlobalState {
            workers: HashMap::new(),
            jobs: HashMap::new(),
            queue: VecDeque::new(),
            next_job_id: 1,
            usage_tracker: UsageTracker::new(),
            log_requests: HashMap::new(),
            scheduler_notify: Arc::new(Notify::new()),
            steps: HashMap::new(),
            next_step_id: 1,

            role: ControllerRole::Leader,
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

        // Add 2 mock workers
        let (worker1, _) = create_mock_worker("worker1", 4, 1000);
        let (worker2, _) = create_mock_worker("worker2", 4, 1000);
        state.workers.insert("worker1".into(), worker1);
        state.workers.insert("worker2".into(), worker2);

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // 1. Add active reservation on worker1 for user "owner"
        let mut reserved_nodes = HashSet::new();
        reserved_nodes.insert("worker1".to_string());
        state.reservations.insert(
            "res1".to_string(),
            veloce_common::Reservation {
                id: "res1".to_string(),
                nodes: reserved_nodes,
                start_time: now - 100,
                end_time: now + 3600,
                owner: "owner".to_string(),
            },
        );

        // Job 1 submitted by "non-owner" requesting 1 core, unlimited walltime
        let mut job1 = create_mock_job(1, 1, 1, 100, 0);
        job1.user_id = "non-owner".to_string();
        state.jobs.insert(1, job1);
        state.queue.push_back(1);

        schedule_jobs(&mut state, &HashMap::new());

        // Since worker1 is reserved for "owner" and worker2 is free, job1 should be scheduled on worker2
        let scheduled_job1 = state.jobs.get(&1).unwrap();
        println!("Job 1 reason: {:?}", scheduled_job1.reason);
        assert_eq!(scheduled_job1.status, JobStatus::Running);
        assert_eq!(scheduled_job1.assigned_workers, vec!["worker2"]);

        // 2. Add an upcoming reservation on worker2 starting in 1000 seconds for "owner"
        let mut reserved_nodes2 = HashSet::new();
        reserved_nodes2.insert("worker2".to_string());
        state.reservations.insert(
            "res2".to_string(),
            veloce_common::Reservation {
                id: "res2".to_string(),
                nodes: reserved_nodes2,
                start_time: now + 1000,
                end_time: now + 5000,
                owner: "owner".to_string(),
            },
        );

        // Job 2 submitted by "non-owner" requesting 1 core, walltime = 500s (fits before reservation start time)
        let mut job2 = create_mock_job(2, 1, 1, 100, 0);
        job2.walltime = 500;
        job2.user_id = "non-owner".to_string();
        state.jobs.insert(2, job2);
        state.queue.push_back(2);

        // Job 3 submitted by "non-owner" requesting 1 core, walltime = 2000s (overlaps with reservation start time)
        let mut job3 = create_mock_job(3, 1, 1, 100, 0);
        job3.walltime = 2000;
        job3.user_id = "non-owner".to_string();
        state.jobs.insert(3, job3);
        state.queue.push_back(3);

        schedule_jobs(&mut state, &HashMap::new());

        // Job 2 should be scheduled on worker2 because 500s fits before res2 start (1000s)
        let scheduled_job2 = state.jobs.get(&2).unwrap();
        assert_eq!(scheduled_job2.status, JobStatus::Running);
        assert_eq!(scheduled_job2.assigned_workers, vec!["worker2"]);

        // Job 3 should remain pending because its walltime (2000s) overflows res2 start (1000s)
        let scheduled_job3 = state.jobs.get(&3).unwrap();
        assert_eq!(scheduled_job3.status, JobStatus::Pending);
    }

    #[tokio::test]
    async fn test_schedule_backfilling() {
        let mut state = GlobalState {
            workers: HashMap::new(),
            jobs: HashMap::new(),
            queue: VecDeque::new(),
            next_job_id: 1,
            usage_tracker: UsageTracker::new(),
            log_requests: HashMap::new(),
            scheduler_notify: Arc::new(Notify::new()),
            steps: HashMap::new(),
            next_step_id: 1,

            role: ControllerRole::Leader,
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

        // Create worker with 8 cores and 8192 MB memory
        let (mut worker, _rx) = create_mock_worker("worker1", 8, 8192);
        worker.hostname = "worker1".to_string();
        worker.addr = "127.0.0.1:9001".parse().unwrap();
        state.workers.insert("worker1".to_string(), worker);

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // 1. Submit Job 1: needs 6 cores, is currently running (walltime = 1000s)
        let mut job1 = create_mock_job(1, 1, 6, 100, 0);
        job1.status = JobStatus::Running;
        job1.walltime = 1000;
        job1.estimated_walltime = Some(1000);
        job1.start_time = Some(now);
        job1.assigned_workers = vec!["worker1".to_string()];

        // Manually reflect the resource utilization of job 1 on the worker
        if let Some(w) = state.workers.get_mut("worker1") {
            w.assigned_jobs.insert(1);
            w.allocated_memory = 100;
            w.available_core_ids = (6..8).collect(); // 2 cores remaining
            w.job_core_assignments.insert(1, (0..6).collect());
        }
        state.jobs.insert(1, job1);

        // 2. Submit Job 2 (High Priority): needs 8 cores, pending
        let mut job2 = create_mock_job(2, 1, 8, 100, 0);
        job2.walltime = 5000;
        job2.estimated_walltime = Some(5000);
        job2.queued_time = now;
        state.jobs.insert(2, job2);
        state.queue.push_back(2);

        // 3. Submit Job 3 (Low Priority, short): needs 2 cores, walltime = 500s. Should backfill!
        let mut job3 = create_mock_job(3, 1, 2, 10, 0);
        job3.walltime = 500;
        job3.estimated_walltime = Some(500);
        job3.queued_time = now;
        state.jobs.insert(3, job3);
        state.queue.push_back(3);

        // 4. Submit Job 4 (Low Priority, long): needs 2 cores, walltime = 2000s. Should NOT backfill (would delay Job 2)!
        let mut job4 = create_mock_job(4, 1, 2, 10, 0);
        job4.walltime = 2000;
        job4.estimated_walltime = Some(2000);
        job4.queued_time = now;
        state.jobs.insert(4, job4);
        state.queue.push_back(4);

        schedule_jobs(&mut state, &HashMap::new());

        // Job 2 must remain pending, but now have its earliest start time calculated as now + 1000
        let scheduled_job2 = state.jobs.get(&2).unwrap();
        assert_eq!(scheduled_job2.status, JobStatus::Pending);
        assert!(scheduled_job2
            .reason
            .as_ref()
            .unwrap()
            .contains("Earliest start"));

        // Job 3 must be successfully backfilled and running
        let scheduled_job3 = state.jobs.get(&3).unwrap();
        assert_eq!(scheduled_job3.status, JobStatus::Running);

        // Job 4 must remain pending (backfill denied)
        let scheduled_job4 = state.jobs.get(&4).unwrap();
        assert_eq!(scheduled_job4.status, JobStatus::Pending);
        assert!(scheduled_job4
            .reason
            .as_ref()
            .unwrap()
            .contains("Backfill denied"));
    }

    #[tokio::test]
    async fn test_preemption_logic() {
        let mut state = GlobalState {
            workers: HashMap::new(),
            jobs: HashMap::new(),
            queue: VecDeque::new(),
            next_job_id: 1,
            usage_tracker: UsageTracker::new(),
            log_requests: HashMap::new(),
            scheduler_notify: Arc::new(Notify::new()),
            steps: HashMap::new(),
            next_step_id: 1,

            role: ControllerRole::Leader,
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

        // Create a worker with 4 cores
        let (mut worker, mut rx) = create_mock_worker("worker1", 4, 4000);

        // Let's run a preemptible job 1 on this worker, using 4 cores
        let mut job1 = create_mock_job(1, 1, 4, 1000, 0);
        job1.qos = QosLevel::Preemptible;
        job1.status = JobStatus::Running;
        job1.assigned_workers = vec!["worker1".to_string()];

        worker.assigned_jobs.insert(1);
        worker.allocated_memory = 1000;
        worker.available_core_ids = vec![];
        worker.job_core_assignments.insert(1, vec![0, 1, 2, 3]);

        state.workers.insert("worker1".to_string(), worker);
        state.jobs.insert(1, job1);

        // Now, we submit job 2: QoS Interactive, needs 4 cores
        let mut job2 = create_mock_job(2, 1, 4, 1000, 0);
        job2.qos = QosLevel::Interactive;

        state.jobs.insert(2, job2);
        state.queue.push_back(2);

        // Run scheduler
        schedule_jobs(&mut state, &HashMap::new());

        // Job 1 should be preempted (status Pending, reason contains Preempted)
        let preempted_job = state.jobs.get(&1).unwrap();
        assert_eq!(preempted_job.status, JobStatus::Pending);
        assert!(preempted_job.reason.as_ref().unwrap().contains("Preempted"));

        // Job 2 should be running
        let running_job = state.jobs.get(&2).unwrap();
        assert_eq!(running_job.status, JobStatus::Running);
        assert_eq!(running_job.assigned_workers, vec!["worker1"]);

        // Verify PreemptJob message was sent to worker
        let msg = rx.try_recv().unwrap();
        assert!(matches!(msg, Message::PreemptJob { job_id: 1 }));
    }

    #[tokio::test]
    async fn test_hello_worker_token_validation() {
        sqlx::any::install_default_drivers();
        let comp_pool = sqlx::any::AnyPoolOptions::new()
            .max_connections(1)
            .connect("sqlite://")
            .await
            .unwrap();
        let component_registry = Arc::new(
            component_registry::ComponentRegistry::new(comp_pool.clone())
                .await
                .unwrap(),
        );
        let audit = Arc::new(audit::AuditLog::new(comp_pool.clone()).await.unwrap());

        let token = component_registry
            .issue_token(
                "worker1",
                veloce_common::auth::ComponentType::Worker,
                &["worker".to_string()],
            )
            .await
            .unwrap();

        let state = GlobalState {
            workers: HashMap::new(),
            jobs: HashMap::new(),
            queue: VecDeque::new(),
            next_job_id: 1,
            usage_tracker: UsageTracker::new(),
            log_requests: HashMap::new(),
            scheduler_notify: Arc::new(Notify::new()),
            steps: HashMap::new(),
            next_step_id: 1,
            role: ControllerRole::Leader,
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

        let ctx = Arc::new(ControllerContext {
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
            accounting_store: Arc::new(accounting::FileAccountingStore),
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
            jwks_cache: Arc::new(auth::JwksCache::new()),
            event_tx: tokio::sync::broadcast::channel(1024).0,
            audit,
            rate_limiter: None,
        });

        std::env::set_var("VELOCE_REQUIRE_WORKER_TOKENS", "true");

        let resources = Resources {
            cpu_cores: 4,
            total_memory: 1024,
            free_memory: 512,
            cpu_usage: 0.0,
            cpu_model: "TestCPU".to_string(),
            arch: "x86_64".to_string(),
            host_name: "test-host".to_string(),
            kernel_version: "0.0.0".to_string(),
            os_name: "Linux".to_string(),
            os_version: "1.0".to_string(),
            load_avg: [0.0, 0.0, 0.0],
            disk_total: 0,
            disk_free: 0,
            uptime: 0,
            boot_time: 0,
            process_count: 0,
            swap_total: 0,
            swap_free: 0,
            version: "v1".to_string(),
            gres: BTreeMap::new(),
            cgroup_enabled: false,
        };

        // Case 1: Token required but none provided -> Rejected
        let (client, server) = tokio::io::duplex(1024);
        let client_framed = Framed::new(client, MessageCodec::new());
        let server_framed = Framed::new(server, MessageCodec::new());

        let hello = Message::HelloWorker {
            worker_id: "worker1".to_string(),
            hostname: "localhost".to_string(),
            resources: resources.clone(),
            features: Some(0),
            registration_token: None,
        };

        tokio::spawn(async move {
            let mut f = client_framed;
            let _ = f.send(hello).await;
            if let Some(Ok(Message::Error(e))) = f.next().await {
                assert!(e.contains("token is required"));
            } else {
                panic!("Expected Error message");
            }
        });

        let addr = "127.0.0.1:8080".parse().unwrap();
        let res = handle_connection(server_framed.into_inner(), addr, ctx.clone()).await;
        assert!(res.is_err());

        // Case 2: Token required, invalid token provided -> Rejected
        let (client, server) = tokio::io::duplex(1024);
        let client_framed = Framed::new(client, MessageCodec::new());
        let server_framed = Framed::new(server, MessageCodec::new());

        let hello = Message::HelloWorker {
            worker_id: "worker1".to_string(),
            hostname: "localhost".to_string(),
            resources: resources.clone(),
            features: Some(0),
            registration_token: Some("invalid_token".to_string()),
        };

        tokio::spawn(async move {
            let mut f = client_framed;
            let _ = f.send(hello).await;
            if let Some(Ok(Message::Error(e))) = f.next().await {
                assert!(e.contains("invalid or revoked token"));
            } else {
                panic!("Expected Error message");
            }
        });

        let res = handle_connection(server_framed.into_inner(), addr, ctx.clone()).await;
        assert!(res.is_err());

        // Case 3: Token required, valid token provided -> Accepted
        let (client, server) = tokio::io::duplex(1024);
        let client_framed = Framed::new(client, MessageCodec::new());
        let server_framed = Framed::new(server, MessageCodec::new());

        let hello = Message::HelloWorker {
            worker_id: "worker1".to_string(),
            hostname: "localhost".to_string(),
            resources: resources.clone(),
            features: Some(0),
            registration_token: Some(token.clone()),
        };

        tokio::spawn(async move {
            let mut f = client_framed;
            let _ = f.send(hello).await;
        });

        let ctx_clone = ctx.clone();
        tokio::spawn(async move {
            let _ = handle_connection(server_framed.into_inner(), addr, ctx_clone).await;
        });

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        let state = ctx.state.lock().await;
        assert!(state.workers.contains_key("worker1"));

        std::env::remove_var("VELOCE_REQUIRE_WORKER_TOKENS");
    }

    #[tokio::test]
    async fn test_hello_client_token_validation() {
        sqlx::any::install_default_drivers();
        let comp_pool = sqlx::any::AnyPoolOptions::new()
            .max_connections(1)
            .connect("sqlite://")
            .await
            .unwrap();
        let component_registry = Arc::new(
            component_registry::ComponentRegistry::new(comp_pool.clone())
                .await
                .unwrap(),
        );
        let audit = Arc::new(audit::AuditLog::new(comp_pool.clone()).await.unwrap());

        let token = component_registry
            .issue_token(
                "client1",
                veloce_common::auth::ComponentType::Client,
                &["cli".to_string()],
            )
            .await
            .unwrap();

        let state = GlobalState {
            workers: HashMap::new(),
            jobs: HashMap::new(),
            queue: VecDeque::new(),
            next_job_id: 1,
            usage_tracker: UsageTracker::new(),
            log_requests: HashMap::new(),
            scheduler_notify: Arc::new(Notify::new()),
            steps: HashMap::new(),
            next_step_id: 1,
            role: ControllerRole::Leader,
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

        let ctx = Arc::new(ControllerContext {
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
            accounting_store: Arc::new(accounting::FileAccountingStore),
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
            jwks_cache: Arc::new(auth::JwksCache::new()),
            event_tx: tokio::sync::broadcast::channel(1024).0,
            audit,
            rate_limiter: None,
        });

        std::env::set_var("VELOCE_REQUIRE_CLIENT_TOKENS", "true");

        // Case 1: Token required but none provided -> Rejected
        let (client, server) = tokio::io::duplex(1024);
        let client_framed = Framed::new(client, MessageCodec::new());
        let server_framed = Framed::new(server, MessageCodec::new());

        let hello = Message::HelloClient {
            client_id: "client1".to_string(),
            registration_token: None,
        };

        tokio::spawn(async move {
            let mut f = client_framed;
            let _ = f.send(hello).await;
            if let Some(Ok(Message::Error(e))) = f.next().await {
                assert!(e.contains("token is required"));
            } else {
                panic!("Expected Error message");
            }
        });

        let addr = "127.0.0.1:8080".parse().unwrap();
        let res = handle_connection(server_framed.into_inner(), addr, ctx.clone()).await;
        assert!(res.is_err());

        // Case 2: Token required, invalid token provided -> Rejected
        let (client, server) = tokio::io::duplex(1024);
        let client_framed = Framed::new(client, MessageCodec::new());
        let server_framed = Framed::new(server, MessageCodec::new());

        let hello = Message::HelloClient {
            client_id: "client1".to_string(),
            registration_token: Some("invalid_token".to_string()),
        };

        tokio::spawn(async move {
            let mut f = client_framed;
            let _ = f.send(hello).await;
            if let Some(Ok(Message::Error(e))) = f.next().await {
                assert!(e.contains("invalid or revoked token"));
            } else {
                panic!("Expected Error message");
            }
        });

        let res = handle_connection(server_framed.into_inner(), addr, ctx.clone()).await;
        assert!(res.is_err());

        // Case 3: Token required, valid token provided -> Accepted
        let (client, server) = tokio::io::duplex(1024);
        let client_framed = Framed::new(client, MessageCodec::new());
        let server_framed = Framed::new(server, MessageCodec::new());

        let hello = Message::HelloClient {
            client_id: "client1".to_string(),
            registration_token: Some(token.clone()),
        };

        tokio::spawn(async move {
            let mut f = client_framed;
            let _ = f.send(hello).await;
            if let Some(Ok(Message::Ack)) = f.next().await {
                // Success!
            } else {
                panic!("Expected Ack message");
            }
        });

        let res = handle_connection(server_framed.into_inner(), addr, ctx.clone()).await;
        assert!(res.is_ok());

        std::env::remove_var("VELOCE_REQUIRE_CLIENT_TOKENS");
    }

    #[tokio::test]
    async fn test_hello_peer_token_validation() {
        sqlx::any::install_default_drivers();
        let comp_pool = sqlx::any::AnyPoolOptions::new()
            .max_connections(1)
            .connect("sqlite://")
            .await
            .unwrap();
        let component_registry = Arc::new(
            component_registry::ComponentRegistry::new(comp_pool.clone())
                .await
                .unwrap(),
        );
        let audit = Arc::new(audit::AuditLog::new(comp_pool.clone()).await.unwrap());

        let token = component_registry
            .issue_token(
                "peer1",
                veloce_common::auth::ComponentType::Peer,
                &["peer".to_string()],
            )
            .await
            .unwrap();

        let state = GlobalState {
            workers: HashMap::new(),
            jobs: HashMap::new(),
            queue: VecDeque::new(),
            next_job_id: 1,
            usage_tracker: UsageTracker::new(),
            log_requests: HashMap::new(),
            scheduler_notify: Arc::new(Notify::new()),
            steps: HashMap::new(),
            next_step_id: 1,
            role: ControllerRole::Leader,
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

        let ctx = Arc::new(ControllerContext {
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
            accounting_store: Arc::new(accounting::FileAccountingStore),
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
            jwks_cache: Arc::new(auth::JwksCache::new()),
            event_tx: tokio::sync::broadcast::channel(1024).0,
            audit,
            rate_limiter: None,
        });

        std::env::set_var("VELOCE_REQUIRE_PEER_TOKENS", "true");

        let addr = "127.0.0.1:8080".parse().unwrap();

        // Case 1: Token required but none provided -> Rejected
        let (client, server) = tokio::io::duplex(1024);
        let client_framed = Framed::new(client, MessageCodec::new());
        let server_framed = Framed::new(server, MessageCodec::new());

        let hello = Message::HelloPeer {
            controller_id: "peer1".to_string(),
            features: Some(0),
            registration_token: None,
        };

        tokio::spawn(async move {
            let mut f = client_framed;
            let _ = f.send(hello).await;
            if let Some(Ok(Message::Error(e))) = f.next().await {
                assert!(e.contains("token is required"));
            } else {
                panic!("Expected Error message");
            }
        });

        let res = handle_connection(server_framed.into_inner(), addr, ctx.clone()).await;
        assert!(res.is_err());

        // Case 2: Token required, invalid token provided -> Rejected
        let (client, server) = tokio::io::duplex(1024);
        let client_framed = Framed::new(client, MessageCodec::new());
        let server_framed = Framed::new(server, MessageCodec::new());

        let hello = Message::HelloPeer {
            controller_id: "peer1".to_string(),
            features: Some(0),
            registration_token: Some("invalid_token".to_string()),
        };

        tokio::spawn(async move {
            let mut f = client_framed;
            let _ = f.send(hello).await;
            if let Some(Ok(Message::Error(e))) = f.next().await {
                assert!(e.contains("invalid or revoked token"));
            } else {
                panic!("Expected Error message");
            }
        });

        let res = handle_connection(server_framed.into_inner(), addr, ctx.clone()).await;
        assert!(res.is_err());

        // Case 3: Token required, valid token provided -> Accepted
        let (client, server) = tokio::io::duplex(1024);
        let client_framed = Framed::new(client, MessageCodec::new());
        let server_framed = Framed::new(server, MessageCodec::new());

        let hello = Message::HelloPeer {
            controller_id: "peer1".to_string(),
            features: Some(0),
            registration_token: Some(token.clone()),
        };

        let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();

        tokio::spawn(async move {
            let mut f = client_framed;
            let _ = f.send(hello).await;
            if let Some(Ok(Message::HelloPeer { controller_id, .. })) = f.next().await {
                assert_eq!(controller_id, "unknown");
            } else {
                panic!("Expected HelloPeer handshake response");
            }
            let _ = done_rx.await;
        });

        let ctx_clone = ctx.clone();
        tokio::spawn(async move {
            let _ = handle_connection(server_framed.into_inner(), addr, ctx_clone).await;
        });

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        let state = ctx.state.lock().await;
        assert!(state.peers.contains_key("peer1"));
        drop(state);

        let _ = done_tx.send(());
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        std::env::remove_var("VELOCE_REQUIRE_PEER_TOKENS");
    }

    #[test]
    fn test_noise_rpc_rbac() {
        use veloce_common::auth::{ROLE_ADMIN, ROLE_OPERATOR, ROLE_SUBMITTER, ROLE_VIEWER};

        // 1. Admin role can access anything
        let admin_roles = vec![ROLE_ADMIN.to_string()];

        let msg_submit = Message::Submit {
            job_name: None,
            job_comment: None,
            binary: "solve".to_string(),
            args: vec![],
            req_nodes: 1,
            req_cores: 1,
            req_memory: 1024,
            walltime: 3600,
            priority: 10,
            user_id: "user1".to_string(),
            working_directory: "/tmp".to_string(),
            array_indices: None,
            inputs: vec![],
            gres_req: std::collections::BTreeMap::new(),
            env_vars: vec![],
            wait_for_licenses: false,
            estimated_walltime: None,
            priority_offset: None,
            dependencies: None,
            dependency_specs: None,
            qos: veloce_common::QosLevel::Production,
            image_uri: None,
            vnc_enabled: false,
            inherit_host_env: false,
            env_allowlist: None,
            job_profile: None,
        };

        assert!(is_authorized(&msg_submit, &admin_roles));

        // 2. Submitter role
        let submitter_roles = vec![ROLE_SUBMITTER.to_string()];
        // Submitter can submit
        assert!(is_authorized(&msg_submit, &submitter_roles));
        // Submitter can cancel
        assert!(is_authorized(
            &Message::CancelJob { job_id: 123 },
            &submitter_roles
        ));
        // Submitter cannot create a reservation
        let msg_res = Message::CreateReservation {
            nodes: std::collections::HashSet::new(),
            start_time: 0,
            end_time: 100,
            owner: "operator".to_string(),
        };
        assert!(!is_authorized(&msg_res, &submitter_roles));
        // Submitter cannot get system logs
        let msg_syslogs = Message::GetSystemLogs {
            request_id: 1,
            component_id: "worker1".to_string(),
            log_source: "syslog".to_string(),
            lines: 100,
        };
        assert!(!is_authorized(&msg_syslogs, &submitter_roles));

        // 3. Operator role
        let operator_roles = vec![ROLE_OPERATOR.to_string()];
        // Operator cannot submit
        assert!(!is_authorized(&msg_submit, &operator_roles));
        // Operator can cancel
        assert!(is_authorized(
            &Message::CancelJob { job_id: 123 },
            &operator_roles
        ));
        // Operator can create reservation
        assert!(is_authorized(&msg_res, &operator_roles));
        // Operator can get system logs
        assert!(is_authorized(&msg_syslogs, &operator_roles));

        // 4. Viewer role
        let viewer_roles = vec![ROLE_VIEWER.to_string()];
        // Viewer cannot submit
        assert!(!is_authorized(&msg_submit, &viewer_roles));
        // Viewer cannot cancel
        assert!(!is_authorized(
            &Message::CancelJob { job_id: 123 },
            &viewer_roles
        ));
        // Viewer cannot create reservation
        assert!(!is_authorized(&msg_res, &viewer_roles));
        // Viewer can list jobs
        assert!(is_authorized(
            &Message::ListJobs { state_filter: None },
            &viewer_roles
        ));
    }
}
