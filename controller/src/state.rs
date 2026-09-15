//! Controller in-memory state, worker routing, and context types.

use crate::{accounting, audit, auth, component_registry, config::Config, containers, rate_limit};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex, Notify};
use tracing::warn;
use veloce_common::{
    JobInfo, JobStatus, Message, NodeMetrics, PeerMessage, Resources, StepInfo, UsageTracker,
};

#[derive(Clone, Debug)]
pub enum WorkerRouting {
    Direct(mpsc::Sender<Message>),
    Gateway(String), // Routes via this peer controller ID
}

#[derive(Clone, Debug)]
pub enum WorkerSender {
    Direct(mpsc::Sender<Message>),
    Gateway {
        worker_id: String,
        peer_tx: mpsc::Sender<PeerMessage>,
    },
}

impl WorkerSender {
    pub async fn send(&self, msg: Message) -> anyhow::Result<()> {
        match self {
            WorkerSender::Direct(tx) => tx.send(msg).await.map_err(|e| anyhow::anyhow!(e)),
            WorkerSender::Gateway { worker_id, peer_tx } => peer_tx
                .send(PeerMessage::ForwardToWorker {
                    worker_id: worker_id.clone(),
                    msg: Box::new(msg),
                })
                .await
                .map_err(|e| anyhow::anyhow!(e)),
        }
    }
}

pub fn normalize_component_id(component_id: &str) -> String {
    let local_hostname = gethostname::gethostname()
        .into_string()
        .unwrap_or_else(|_| "controller".to_string());
    if component_id == "controller" {
        return "controller".to_string();
    }
    if local_hostname.starts_with("veloce-") && !component_id.starts_with("veloce-") {
        format!("veloce-{}", component_id)
    } else if !local_hostname.starts_with("veloce-") && component_id.starts_with("veloce-") {
        component_id.strip_prefix("veloce-").unwrap().to_string()
    } else {
        component_id.to_string()
    }
}

pub fn get_worker_sender(state: &GlobalState, worker_id: &str) -> Option<WorkerSender> {
    if let Some(worker) = state.workers.get(worker_id) {
        match &worker.routing {
            WorkerRouting::Direct(tx) => Some(WorkerSender::Direct(tx.clone())),
            WorkerRouting::Gateway(peer_id) => {
                state
                    .peers
                    .get(peer_id)
                    .map(|peer_tx| WorkerSender::Gateway {
                        worker_id: worker_id.to_string(),
                        peer_tx: peer_tx.clone(),
                    })
            }
        }
    } else {
        let normalized = normalize_component_id(worker_id);
        let found = state.workers.iter().find(|(id, w)| {
            id.as_str() == worker_id
                || w.hostname == worker_id
                || id.as_str() == normalized
                || w.hostname == normalized
        });
        if let Some((id, worker)) = found {
            match &worker.routing {
                WorkerRouting::Direct(tx) => Some(WorkerSender::Direct(tx.clone())),
                WorkerRouting::Gateway(peer_id) => {
                    state
                        .peers
                        .get(peer_id)
                        .map(|peer_tx| WorkerSender::Gateway {
                            worker_id: id.clone(),
                            peer_tx: peer_tx.clone(),
                        })
                }
            }
        } else {
            None
        }
    }
}

pub fn get_leader_peer_tx(state: &GlobalState) -> Option<mpsc::Sender<PeerMessage>> {
    if let Some(leader_id) = &state.leader_id {
        state.peers.get(leader_id).cloned()
    } else {
        if state.peers.len() == 1 {
            state.peers.values().next().cloned()
        } else {
            None
        }
    }
}

pub async fn is_controller_node(ctx: &ControllerContext, component_id: &str) -> bool {
    if component_id == "controller" {
        return true;
    }
    let local_hostname = gethostname::gethostname()
        .into_string()
        .unwrap_or_else(|_| "controller".to_string());
    if component_id == local_hostname {
        return true;
    }
    let normalized = normalize_component_id(component_id);
    if normalized == local_hostname {
        return true;
    }
    if let Ok(peers_env) = std::env::var("VELOCE_PEERS") {
        for peer in peers_env.split(',') {
            if let Some(host) = peer.split(':').next() {
                let host_trimmed = host.trim();
                if host_trimmed == component_id || host_trimmed == normalized {
                    return true;
                }
            }
        }
    }
    {
        let state_lock = ctx.state.lock().await;
        if state_lock.peers.contains_key(component_id) || state_lock.peers.contains_key(&normalized)
        {
            return true;
        }
    }
    false
}

pub struct WorkerHandle {
    pub addr: SocketAddr,
    pub hostname: String,
    pub resources: Resources,
    pub routing: WorkerRouting,
    pub assigned_jobs: HashSet<u64>, // Job IDs currently running
    pub allocated_memory: u64,
    pub connection_id: u64,
    // Phase 1: Core Affinity Tracking
    pub available_core_ids: Vec<usize>,
    pub job_core_assignments: HashMap<u64, Vec<usize>>,
    pub available_gres_ids: HashMap<String, Vec<u32>>,
    pub job_gres_assignments: HashMap<u64, HashMap<String, Vec<u32>>>,
    // Reconnection support
    pub connected: bool,
    pub last_seen: u64,
    pub cgroup_enabled: bool,
    pub draining: bool,
    pub current_pulse_interval: f32,
    pub features: u64,
}

pub(crate) struct RebuiltWorkerAllocations {
    pub assigned_jobs: HashSet<u64>,
    pub allocated_memory: u64,
    pub available_core_ids: Vec<usize>,
    pub job_core_assignments: HashMap<u64, Vec<usize>>,
    pub available_gres_ids: HashMap<String, Vec<u32>>,
    pub job_gres_assignments: HashMap<u64, HashMap<String, Vec<u32>>>,
}

pub(crate) fn rebuild_worker_allocations(
    worker_id: &str,
    resources: &Resources,
    jobs: &HashMap<u64, JobInfo>,
) -> RebuiltWorkerAllocations {
    let mut assigned_jobs = HashSet::new();
    let mut allocated_memory = 0;
    let mut job_core_assignments = HashMap::new();
    let mut job_gres_assignments = HashMap::new();
    let mut available_core_ids: Vec<usize> = (0..resources.cpu_cores).collect();
    let mut available_gres_ids = resources
        .gres
        .iter()
        .map(|(name, &count)| (name.clone(), (0..count as u32).collect::<Vec<_>>()))
        .collect::<HashMap<_, _>>();

    for job in jobs.values() {
        if job.status == JobStatus::Running && job.assigned_workers.iter().any(|id| id == worker_id)
        {
            assigned_jobs.insert(job.id);
            allocated_memory += job.req_memory;

            if let Some(cores) = job.allocated_cores.get(worker_id) {
                job_core_assignments.insert(job.id, cores.clone());
                available_core_ids.retain(|core| !cores.contains(core));
            }

            if let Some(gres) = job.allocated_gres.get(worker_id) {
                job_gres_assignments.insert(job.id, gres.clone());
                for (name, ids) in gres {
                    if let Some(available) = available_gres_ids.get_mut(name) {
                        available.retain(|id| !ids.contains(id));
                    }
                }
            }
        }
    }

    RebuiltWorkerAllocations {
        assigned_jobs,
        allocated_memory,
        available_core_ids,
        job_core_assignments,
        available_gres_ids,
        job_gres_assignments,
    }
}

pub(crate) fn reconcile_missing_worker_jobs(
    missing_jobs: &DashMap<u64, u64>,
    state: &mut GlobalState,
    worker_id: &str,
    active_job_ids: &HashSet<u64>,
    now: u64,
) {
    let mut jobs_to_reap = Vec::new();

    for (job_id, job) in &state.jobs {
        if job.status == JobStatus::Running && job.assigned_workers.iter().any(|id| id == worker_id)
        {
            if !active_job_ids.contains(job_id) {
                // Give newly spawned jobs a short grace period before treating
                // absent heartbeat stats as proof the process disappeared.
                let is_after_spawn = job
                    .start_time
                    .map(|start| now.saturating_sub(start) > 5)
                    .unwrap_or(true);

                if is_after_spawn {
                    let first_seen_missing = *missing_jobs.entry(*job_id).or_insert(now);
                    if now.saturating_sub(first_seen_missing) > 10 {
                        jobs_to_reap.push(*job_id);
                    }
                }
            } else {
                missing_jobs.remove(job_id);
            }
        }
    }

    for job_id in jobs_to_reap {
        warn!(
            "Job {} is marked Running on worker {}, but is missing from worker heartbeat. Reaping job...",
            job_id, worker_id
        );
        missing_jobs.remove(&job_id);
        crate::finalize_job(
            state,
            job_id,
            JobStatus::Failed("Process exited or failed to start".to_string()),
        );
    }
}

#[derive(Clone, PartialEq, Debug)]
pub struct WorkerSyncState {
    pub draining: bool,
    pub connected: bool,
    pub available_cores: usize,
    pub allocated_memory: u64,
    pub assigned_jobs: HashSet<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ControllerRole {
    Leader,
    Follower,
}

#[derive(Clone, Default)]
pub struct MultinodeJobTracking {
    pub dispatched_workers: HashMap<u64, Vec<String>>,
    pub worker_log_files: HashMap<u64, HashMap<String, (Option<String>, Option<String>)>>,
    pub done_workers: HashMap<u64, HashSet<String>>,
}

pub struct GlobalState {
    pub workers: HashMap<String, WorkerHandle>, // Key: Worker ID
    pub jobs: HashMap<u64, JobInfo>,
    pub queue: VecDeque<u64>,
    pub next_job_id: u64,
    pub usage_tracker: UsageTracker,
    pub log_requests: HashMap<u64, tokio::sync::oneshot::Sender<Message>>, // RequestID -> Sender
    pub component_log_requests: HashMap<u64, tokio::sync::oneshot::Sender<Message>>, // RequestID -> Sender
    pub scheduler_notify: Arc<Notify>,
    pub steps: HashMap<(u64, u32), StepInfo>,
    pub next_step_id: u32,
    pub role: ControllerRole,
    pub leader_addr: Option<String>,
    pub leader_id: Option<String>,
    pub peers: HashMap<String, mpsc::Sender<PeerMessage>>, // Peer controller channels (Key: controller ID)
    pub job_output_buffers: HashMap<u64, String>,          // Job ID -> Buffered output for parsing
    pub multinode_job_tracking: MultinodeJobTracking,
    pub api_port: u16,
    pub step_waiters: HashMap<(u64, u32), Vec<tokio::sync::oneshot::Sender<i32>>>, // (job_id, step_id) -> Waiters
    pub step_output_waiters:
        HashMap<(u64, u32), Vec<tokio::sync::mpsc::UnboundedSender<(bool, Vec<u8>)>>>, // (job_id, step_id) -> Output Waiters
    pub solver_configs: HashMap<String, veloce_common::SolverConfig>,
    pub next_request_id: Arc<AtomicU64>,
    pub reservations: HashMap<String, veloce_common::Reservation>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct WsTicket {
    pub scope: String,
    pub job_id: Option<u64>,
    pub expires_at: u64,
    pub user_id: String,
    pub roles: Vec<String>,
}

pub struct ControllerContext {
    pub config: Config,
    pub ws_tickets: DashMap<String, WsTicket>,
    pub state: Mutex<GlobalState>,
    pub metrics_store: DashMap<String, NodeMetrics>,
    pub job_metrics_history: DashMap<u64, std::collections::VecDeque<veloce_common::JobStats>>,
    pub next_job_id: AtomicU64,
    pub submission_tx: mpsc::Sender<JobInfo>,
    pub file_client: Arc<veloce_common::file_client::FileClient>,
    pub loki_tx: mpsc::Sender<(HashMap<String, String>, String, u64)>,
    pub accounting_store: Arc<dyn accounting::AccountingStore>,
    pub container_store: Arc<containers::ContainerStore>,
    pub component_registry: Arc<component_registry::ComponentRegistry>,
    pub audit: Arc<audit::AuditLog>,
    pub proxy_client: reqwest::Client,
    pub terminal_sessions: DashMap<u64, tokio::sync::mpsc::Sender<Message>>,
    pub vnc_sessions: DashMap<u64, tokio::sync::mpsc::Sender<Message>>,
    pub missing_jobs: DashMap<u64, u64>, // Job ID -> Timestamp (secs) when first noticed missing
    pub last_heartbeat: std::sync::atomic::AtomicU64,
    pub pmi_kvs: DashMap<(u64, u32), DashMap<String, String>>, // (parent_job_id, step_id) -> KVS
    pub pmi_barriers: DashMap<(u64, u32), HashSet<u32>>, // (parent_job_id, step_id) -> Ranks checked in
    pub active_jobs: DashMap<u64, JobInfo>,
    pub worker_resources: DashMap<String, (Resources, WorkerSyncState)>, // worker_id -> (Resources, sync_state)
    pub job_worker_stats: DashMap<u64, HashMap<String, (f32, u64, bool)>>, // Job ID -> (Worker ID -> (CPU, Memory, is_idle))
    pub global_idle_start: DashMap<u64, u64>, // Job ID -> UNIX timestamp when it became globally idle
    pub completed_jobs: std::sync::atomic::AtomicU64,
    pub failed_jobs: std::sync::atomic::AtomicU64,
    pub jwks_cache: Arc<auth::JwksCache>,
    pub event_tx: tokio::sync::broadcast::Sender<String>,
    pub rate_limiter: Option<Arc<rate_limit::ApiRateLimiter>>,
}

pub type SharedContext = Arc<ControllerContext>;
pub type SharedState = SharedContext;

fn resources_metadata_eq(a: &veloce_common::Resources, b: &veloce_common::Resources) -> bool {
    a.cpu_cores == b.cpu_cores
        && a.total_memory == b.total_memory
        && a.cpu_model == b.cpu_model
        && a.arch == b.arch
        && a.os_name == b.os_name
        && a.os_version == b.os_version
        && a.kernel_version == b.kernel_version
        && a.host_name == b.host_name
        && a.disk_total == b.disk_total
        && a.boot_time == b.boot_time
        && a.swap_total == b.swap_total
        && a.version == b.version
        && a.gres == b.gres
        && a.cgroup_enabled == b.cgroup_enabled
}

pub fn sync_active_state_to_dashmaps(ctx: &ControllerContext, state: &GlobalState) {
    let mut workers_changed = false;

    // 1. Sync workers
    let prev_workers_len = ctx.worker_resources.len();
    ctx.worker_resources
        .retain(|id, _| state.workers.contains_key(id));
    if ctx.worker_resources.len() != prev_workers_len {
        workers_changed = true;
    }

    for (id, worker) in &state.workers {
        let fresh_resources = worker.resources.clone();
        let fresh_sync_state = WorkerSyncState {
            draining: worker.draining,
            connected: worker.connected,
            available_cores: worker.available_core_ids.len(),
            allocated_memory: worker.allocated_memory,
            assigned_jobs: worker.assigned_jobs.clone(),
        };
        let mut insert_needed = true;

        if let Some(existing) = ctx.worker_resources.get(id) {
            let (ref old_res, ref old_sync) = *existing.value();
            if resources_metadata_eq(old_res, &fresh_resources) && old_sync == &fresh_sync_state {
                insert_needed = false;
            }
        }

        if insert_needed {
            ctx.worker_resources
                .insert(id.clone(), (fresh_resources, fresh_sync_state));
            workers_changed = true;
        }
    }

    let mut jobs_changed = false;

    // 2. Sync jobs
    let prev_jobs_len = ctx.active_jobs.len();
    ctx.active_jobs.retain(|id, _| state.jobs.contains_key(id));
    ctx.job_worker_stats
        .retain(|id, _| state.jobs.contains_key(id));
    ctx.global_idle_start
        .retain(|id, _| state.jobs.contains_key(id));
    if ctx.active_jobs.len() != prev_jobs_len {
        jobs_changed = true;
    }

    for (id, job) in &state.jobs {
        let mut insert_needed = true;

        if let Some(existing) = ctx.active_jobs.get(id) {
            if existing.value() == job {
                insert_needed = false;
            }
        }

        if insert_needed {
            ctx.active_jobs.insert(*id, job.clone());
            jobs_changed = true;
        }
    }

    if workers_changed {
        let _ = ctx.event_tx.send("nodes_updated".to_string());
    }
    if jobs_changed {
        let _ = ctx.event_tx.send("jobs_updated".to_string());
    }
}
