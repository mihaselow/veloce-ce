//! API module: nodes.rs

use crate::{generate_prometheus_metrics, SharedContext};
use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Extension, Json,
};
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};
use veloce_common::{ControllerInfo, NodeMetrics, WorkerInfo};

#[derive(Deserialize)]
pub struct NodeMetricsHistoryQuery {
    pub start: Option<u64>,
    pub end: Option<u64>,
    pub node: Option<String>,
    pub nodes: Option<String>,
    pub bucket_seconds: Option<u64>,
    pub resolution: Option<u64>,
}

#[derive(Serialize, Debug, Clone)]
pub struct NodeMetricsHistoryResponse {
    pub start_time: u64,
    pub end_time: u64,
    pub bucket_seconds: u64,
    pub node_count: usize,
    pub sample_count: usize,
    pub management_node_history: bool,
    pub coverage_note: String,
    pub nodes: Vec<NodeMetricsNodeSeries>,
    pub cluster: Vec<ClusterMetricsBucket>,
}

#[derive(Serialize, Debug, Clone)]
pub struct NodeMetricsNodeSeries {
    pub node_id: String,
    pub hostname: Option<String>,
    pub online: Option<bool>,
    pub sample_count: usize,
    pub samples: Vec<NodeMetricsBucket>,
}

#[derive(Serialize, Debug, Clone)]
pub struct NodeMetricsBucket {
    pub timestamp: u64,
    pub sample_count: u32,
    pub cpu_avg: f32,
    pub cpu_max: f32,
    pub memory_used_avg: u64,
    pub memory_total_max: u64,
    pub memory_pct_avg: f32,
    pub memory_pct_max: f32,
    pub running_jobs_avg: f32,
    pub running_jobs_max: u32,
    pub net_rx_avg: u64,
    pub net_tx_avg: u64,
    pub disk_read_avg: u64,
    pub disk_write_avg: u64,
    pub swap_usage_avg: u64,
    pub process_count_avg: u32,
    pub load1_avg: f32,
    pub gpu_avg: Option<f32>,
}

#[derive(Serialize, Debug, Clone)]
pub struct ClusterMetricsBucket {
    pub timestamp: u64,
    pub node_count: u32,
    pub cpu_avg: f32,
    pub cpu_peak: f32,
    pub memory_pct_avg: f32,
    pub memory_pct_peak: f32,
    pub running_jobs_avg: f32,
    pub net_rx_avg: u64,
    pub net_tx_avg: u64,
    pub disk_read_avg: u64,
    pub disk_write_avg: u64,
}

#[derive(Default)]
struct NodeBucketAccumulator {
    pub count: u32,
    pub cpu_sum: f64,
    pub cpu_max: f32,
    pub memory_sum: u128,
    pub memory_total_max: u64,
    pub memory_pct_sum: f64,
    pub memory_pct_max: f32,
    pub running_jobs_sum: u64,
    pub running_jobs_max: u32,
    pub net_rx_sum: u128,
    pub net_tx_sum: u128,
    pub disk_read_sum: u128,
    pub disk_write_sum: u128,
    pub swap_sum: u128,
    pub process_count_sum: u64,
    pub load1_sum: f64,
    pub gpu_sum: f64,
    pub gpu_count: u32,
}

impl NodeBucketAccumulator {
    fn add(&mut self, metric: &NodeMetrics) {
        self.count += 1;
        self.cpu_sum += metric.cpu_load as f64;
        self.cpu_max = self.cpu_max.max(metric.cpu_load);
        self.memory_sum += metric.memory_usage as u128;
        self.memory_total_max = self.memory_total_max.max(metric.memory_total);
        let memory_pct = if metric.memory_total > 0 {
            (metric.memory_usage as f32 / metric.memory_total as f32 * 100.0).clamp(0.0, 100.0)
        } else {
            0.0
        };
        self.memory_pct_sum += memory_pct as f64;
        self.memory_pct_max = self.memory_pct_max.max(memory_pct);
        self.running_jobs_sum += metric.running_jobs as u64;
        self.running_jobs_max = self.running_jobs_max.max(metric.running_jobs);
        self.net_rx_sum += metric.net_rx_rate as u128;
        self.net_tx_sum += metric.net_tx_rate as u128;
        self.disk_read_sum += metric.disk_read_rate as u128;
        self.disk_write_sum += metric.disk_write_rate as u128;
        self.swap_sum += metric.swap_usage as u128;
        self.process_count_sum += metric.process_count as u64;
        self.load1_sum += metric.load_avg[0] as f64;
        if let Some(gpu) = metric.gpu_usage {
            self.gpu_sum += gpu as f64;
            self.gpu_count += 1;
        }
    }

    fn finish(&self, timestamp: u64) -> NodeMetricsBucket {
        let count = self.count.max(1) as f64;
        NodeMetricsBucket {
            timestamp,
            sample_count: self.count,
            cpu_avg: (self.cpu_sum / count) as f32,
            cpu_max: self.cpu_max,
            memory_used_avg: (self.memory_sum / self.count.max(1) as u128) as u64,
            memory_total_max: self.memory_total_max,
            memory_pct_avg: (self.memory_pct_sum / count) as f32,
            memory_pct_max: self.memory_pct_max,
            running_jobs_avg: (self.running_jobs_sum as f64 / count) as f32,
            running_jobs_max: self.running_jobs_max,
            net_rx_avg: (self.net_rx_sum / self.count.max(1) as u128) as u64,
            net_tx_avg: (self.net_tx_sum / self.count.max(1) as u128) as u64,
            disk_read_avg: (self.disk_read_sum / self.count.max(1) as u128) as u64,
            disk_write_avg: (self.disk_write_sum / self.count.max(1) as u128) as u64,
            swap_usage_avg: (self.swap_sum / self.count.max(1) as u128) as u64,
            process_count_avg: (self.process_count_sum / self.count.max(1) as u64) as u32,
            load1_avg: (self.load1_sum / count) as f32,
            gpu_avg: if self.gpu_count > 0 {
                Some((self.gpu_sum / self.gpu_count as f64) as f32)
            } else {
                None
            },
        }
    }
}

#[derive(Default)]
struct ClusterBucketAccumulator {
    pub node_count: u32,
    pub cpu_sum: f64,
    pub cpu_peak: f32,
    pub memory_pct_sum: f64,
    pub memory_pct_peak: f32,
    pub running_jobs_sum: f64,
    pub net_rx_sum: u128,
    pub net_tx_sum: u128,
    pub disk_read_sum: u128,
    pub disk_write_sum: u128,
}

impl ClusterBucketAccumulator {
    fn add(&mut self, bucket: &NodeMetricsBucket) {
        self.node_count += 1;
        self.cpu_sum += bucket.cpu_avg as f64;
        self.cpu_peak = self.cpu_peak.max(bucket.cpu_max);
        self.memory_pct_sum += bucket.memory_pct_avg as f64;
        self.memory_pct_peak = self.memory_pct_peak.max(bucket.memory_pct_max);
        self.running_jobs_sum += bucket.running_jobs_avg as f64;
        self.net_rx_sum += bucket.net_rx_avg as u128;
        self.net_tx_sum += bucket.net_tx_avg as u128;
        self.disk_read_sum += bucket.disk_read_avg as u128;
        self.disk_write_sum += bucket.disk_write_avg as u128;
    }

    fn finish(&self, timestamp: u64) -> ClusterMetricsBucket {
        let nodes = self.node_count.max(1) as f64;
        ClusterMetricsBucket {
            timestamp,
            node_count: self.node_count,
            cpu_avg: (self.cpu_sum / nodes) as f32,
            cpu_peak: self.cpu_peak,
            memory_pct_avg: (self.memory_pct_sum / nodes) as f32,
            memory_pct_peak: self.memory_pct_peak,
            running_jobs_avg: (self.running_jobs_sum / nodes) as f32,
            net_rx_avg: (self.net_rx_sum / self.node_count.max(1) as u128) as u64,
            net_tx_avg: (self.net_tx_sum / self.node_count.max(1) as u128) as u64,
            disk_read_avg: (self.disk_read_sum / self.node_count.max(1) as u128) as u64,
            disk_write_avg: (self.disk_write_sum / self.node_count.max(1) as u128) as u64,
        }
    }
}

pub(super) async fn api_metrics(State(ctx): State<SharedContext>) -> impl IntoResponse {
    generate_prometheus_metrics(ctx).await
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct HealthResponse {
    pub role: String,
    pub ready: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub leader_addr: Option<String>,
}

pub fn controller_health_status(state: &crate::GlobalState) -> HealthResponse {
    let role = match state.role {
        crate::ControllerRole::Leader => "leader",
        crate::ControllerRole::Follower => "follower",
    };
    let ready = match state.role {
        crate::ControllerRole::Leader => true,
        crate::ControllerRole::Follower => state.leader_addr.is_some(),
    };
    HealthResponse {
        role: role.to_string(),
        ready,
        leader_addr: state.leader_addr.clone(),
    }
}

pub(super) async fn api_health(State(ctx): State<SharedContext>) -> impl IntoResponse {
    let status = {
        let state = ctx.state.lock().await;
        controller_health_status(&state)
    };
    (StatusCode::OK, Json(status))
}

/// Returns 200 only when this instance is the elected leader and ready for writes.
pub(super) async fn api_health_leader(State(ctx): State<SharedContext>) -> impl IntoResponse {
    let status = {
        let state = ctx.state.lock().await;
        controller_health_status(&state)
    };
    if status.role == "leader" && status.ready {
        (StatusCode::OK, Json(status))
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, Json(status))
    }
}

pub(super) async fn api_list_nodes(
    State(ctx): State<SharedContext>,
    Extension(principal): Extension<crate::auth::AuthenticatedPrincipal>,
) -> Response {
    let has_valid_role = principal
        .roles
        .iter()
        .any(|r| r == "admin" || r == "operator" || r == "submitter" || r == "viewer");
    if !has_valid_role {
        return (StatusCode::FORBIDDEN, "Forbidden: insufficient role").into_response();
    }
    let state_lock = ctx.state.lock().await;
    let nodes: Vec<WorkerInfo> = state_lock
        .workers
        .iter()
        .map(|(id, w)| {
            let metrics = ctx.metrics_store.get(id);
            WorkerInfo {
                id: id.clone(),
                hostname: w.hostname.clone(),
                ip_address: w.addr.ip().to_string(),
                total_cores: w.resources.cpu_cores,
                available_cores: w.available_core_ids.len(),
                total_memory: w.resources.total_memory,
                allocated_memory: w.allocated_memory,
                cpu_model: w.resources.cpu_model.clone(),
                arch: w.resources.arch.clone(),
                os_name: w.resources.os_name.clone(),
                os_version: w.resources.os_version.clone(),
                kernel_version: w.resources.kernel_version.clone(),
                cpu_usage: metrics
                    .as_ref()
                    .map(|m| m.cpu_load)
                    .unwrap_or(w.resources.cpu_usage),
                used_memory: metrics.as_ref().map(|m| m.memory_usage).unwrap_or(
                    w.resources
                        .total_memory
                        .saturating_sub(w.resources.free_memory),
                ),
                load_avg: metrics
                    .as_ref()
                    .map(|m| {
                        [
                            m.load_avg[0] as f64,
                            m.load_avg[1] as f64,
                            m.load_avg[2] as f64,
                        ]
                    })
                    .unwrap_or(w.resources.load_avg),
                disk_total: metrics
                    .as_ref()
                    .map(|m| m.disk_total)
                    .unwrap_or(w.resources.disk_total),
                disk_free: metrics
                    .as_ref()
                    .map(|m| m.disk_total.saturating_sub(m.disk_usage))
                    .unwrap_or(w.resources.disk_free),
                uptime: metrics
                    .as_ref()
                    .map(|m| m.uptime)
                    .unwrap_or(w.resources.uptime),
                boot_time: w.resources.boot_time,
                process_count: metrics
                    .as_ref()
                    .map(|m| m.process_count)
                    .unwrap_or(w.resources.process_count),
                swap_total: w.resources.swap_total,
                swap_free: metrics
                    .as_ref()
                    .map(|m| w.resources.swap_total.saturating_sub(m.swap_usage))
                    .unwrap_or(w.resources.swap_free),
                version: w.resources.version.clone(),
                cgroup_enabled: metrics
                    .as_ref()
                    .map(|m| m.cgroup_enabled)
                    .unwrap_or(w.cgroup_enabled),
                gres: w.resources.gres.clone(),
                allocated_gres: w.job_gres_assignments.values().fold(
                    std::collections::HashMap::new(),
                    |mut acc, map| {
                        for (k, v) in map {
                            acc.entry(k.clone()).or_insert_with(Vec::new).extend(v);
                        }
                        acc
                    },
                ),
                net_rx_rate: metrics.as_ref().map(|m| m.net_rx_rate).unwrap_or(0),
                net_tx_rate: metrics.as_ref().map(|m| m.net_tx_rate).unwrap_or(0),
                disk_read_rate: metrics.as_ref().map(|m| m.disk_read_rate).unwrap_or(0),
                disk_write_rate: metrics.as_ref().map(|m| m.disk_write_rate).unwrap_or(0),
                online: w.connected,
                controller_id: match &w.routing {
                    crate::WorkerRouting::Direct(_) => {
                        let local_hostname = gethostname::gethostname()
                            .into_string()
                            .unwrap_or_else(|_| "controller".to_string());
                        Some(local_hostname)
                    }
                    crate::WorkerRouting::Gateway(peer_id) => Some(peer_id.clone()),
                },
            }
        })
        .collect();
    Json(nodes).into_response()
}

pub(super) async fn api_node_metrics_history(
    State(ctx): State<SharedContext>,
    Extension(principal): Extension<crate::auth::AuthenticatedPrincipal>,
    Query(query): Query<NodeMetricsHistoryQuery>,
) -> Response {
    let has_elevated_role = principal
        .roles
        .iter()
        .any(|r| r == "admin" || r == "operator" || r == "submitter");
    if !has_elevated_role {
        return (StatusCode::FORBIDDEN, "Forbidden: insufficient role").into_response();
    }

    const DEFAULT_RANGE_SECONDS: u64 = 6 * 60 * 60;
    const MAX_RANGE_SECONDS: u64 = 7 * 24 * 60 * 60;
    const DEFAULT_BUCKET_COUNT: u64 = 180;

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let end_time = query.end.unwrap_or(now);
    let start_time = query
        .start
        .unwrap_or_else(|| end_time.saturating_sub(DEFAULT_RANGE_SECONDS));

    if start_time > end_time {
        return (StatusCode::BAD_REQUEST, "start must be before end").into_response();
    }
    if end_time.saturating_sub(start_time) > MAX_RANGE_SECONDS {
        return (
            StatusCode::BAD_REQUEST,
            "time range must be seven days or less",
        )
            .into_response();
    }

    let range = end_time.saturating_sub(start_time).max(1);
    let default_bucket_seconds = (range / DEFAULT_BUCKET_COUNT).max(30);
    let bucket_seconds = query
        .bucket_seconds
        .or(query.resolution)
        .unwrap_or(default_bucket_seconds)
        .clamp(10, 6 * 60 * 60);

    let node_filter = parse_metric_node_filter(&query);
    let metrics = crate::read_metrics(Some(start_time), Some(end_time), node_filter, false);

    let node_meta = {
        let state_lock = ctx.state.lock().await;
        state_lock
            .workers
            .iter()
            .map(|(id, worker)| (id.clone(), (worker.hostname.clone(), worker.connected)))
            .collect::<std::collections::HashMap<_, _>>()
    };

    let response = build_node_metrics_history_response(
        metrics,
        start_time,
        end_time,
        bucket_seconds,
        &node_meta,
    );
    Json(response).into_response()
}

pub fn parse_metric_node_filter(query: &NodeMetricsHistoryQuery) -> Option<Vec<String>> {
    let mut nodes = Vec::new();
    for value in [query.node.as_deref(), query.nodes.as_deref()]
        .into_iter()
        .flatten()
    {
        nodes.extend(
            value
                .split(',')
                .map(str::trim)
                .filter(|node| !node.is_empty())
                .map(ToString::to_string),
        );
    }
    if nodes.is_empty() {
        None
    } else {
        nodes.sort();
        nodes.dedup();
        Some(nodes)
    }
}

pub fn build_node_metrics_history_response(
    metrics: Vec<NodeMetrics>,
    start_time: u64,
    end_time: u64,
    bucket_seconds: u64,
    node_meta: &std::collections::HashMap<String, (String, bool)>,
) -> NodeMetricsHistoryResponse {
    let sample_count = metrics.len();
    let mut buckets = std::collections::BTreeMap::<(String, u64), NodeBucketAccumulator>::new();

    for metric in metrics {
        if metric.timestamp < start_time || metric.timestamp > end_time {
            continue;
        }
        let bucket_start = start_time
            + ((metric.timestamp.saturating_sub(start_time)) / bucket_seconds) * bucket_seconds;
        buckets
            .entry((metric.node_id.clone(), bucket_start))
            .or_default()
            .add(&metric);
    }

    let mut by_node = std::collections::BTreeMap::<String, Vec<NodeMetricsBucket>>::new();
    let mut cluster_accumulators =
        std::collections::BTreeMap::<u64, ClusterBucketAccumulator>::new();

    for ((node_id, timestamp), accumulator) in buckets {
        let bucket = accumulator.finish(timestamp);
        cluster_accumulators
            .entry(timestamp)
            .or_default()
            .add(&bucket);
        by_node.entry(node_id).or_default().push(bucket);
    }

    let nodes = by_node
        .into_iter()
        .map(|(node_id, mut samples)| {
            samples.sort_by_key(|sample| sample.timestamp);
            let sample_count = samples
                .iter()
                .map(|sample| sample.sample_count as usize)
                .sum();
            let (hostname, online) = node_meta
                .get(&node_id)
                .map(|(hostname, online)| (Some(hostname.clone()), Some(*online)))
                .unwrap_or((None, None));

            NodeMetricsNodeSeries {
                node_id,
                hostname,
                online,
                sample_count,
                samples,
            }
        })
        .collect::<Vec<_>>();

    let cluster = cluster_accumulators
        .into_iter()
        .map(|(timestamp, accumulator)| accumulator.finish(timestamp))
        .collect::<Vec<_>>();

    NodeMetricsHistoryResponse {
        start_time,
        end_time,
        bucket_seconds,
        node_count: nodes.len(),
        sample_count,
        management_node_history: false,
        coverage_note: "Historical host metrics are recorded for worker-emitted NodeMetrics only. Controllers and other management hosts need a telemetry-only worker or host exporter for full load, memory, disk, and network history.".to_string(),
        nodes,
        cluster,
    }
}

pub(super) async fn api_list_controllers(
    State(ctx): State<SharedContext>,
    Extension(principal): Extension<crate::auth::AuthenticatedPrincipal>,
) -> Response {
    let has_valid_role = principal
        .roles
        .iter()
        .any(|r| r == "admin" || r == "operator" || r == "submitter" || r == "viewer");
    if !has_valid_role {
        return (StatusCode::FORBIDDEN, "Forbidden: insufficient role").into_response();
    }
    let local_hostname = gethostname::gethostname()
        .into_string()
        .unwrap_or_else(|_| "controller".to_string());

    let mut controllers = std::collections::HashMap::new();

    // 1. Determine local controller info
    let (local_role, connected_peers) = {
        let state_lock = ctx.state.lock().await;
        let role_str = match state_lock.role {
            crate::ControllerRole::Leader => "Leader",
            crate::ControllerRole::Follower => "Standby",
        };
        let peers: Vec<String> = state_lock.peers.keys().cloned().collect();
        (role_str, peers)
    };

    controllers.insert(
        local_hostname.clone(),
        ControllerInfo {
            hostname: local_hostname.clone(),
            role: local_role.to_string(),
            online: true,
        },
    );

    // 2. Add configured peers from environment
    if let Ok(peers_env) = std::env::var("VELOCE_PEERS") {
        for peer in peers_env.split(',') {
            if let Some(host) = peer.split(':').next() {
                let trimmed = host.trim().to_string();
                if !trimmed.is_empty() && trimmed != local_hostname {
                    controllers
                        .entry(trimmed.clone())
                        .or_insert(ControllerInfo {
                            hostname: trimmed,
                            role: "Standby".to_string(),
                            online: false,
                        });
                }
            }
        }
    }

    // 3. Mark connected peers as online
    for peer_id in connected_peers {
        controllers.insert(
            peer_id.clone(),
            ControllerInfo {
                hostname: peer_id,
                role: "Standby".to_string(),
                online: true,
            },
        );
    }

    let mut result: Vec<ControllerInfo> = controllers.into_values().collect();
    result.sort_by(|a, b| a.hostname.cmp(&b.hostname));
    Json(result).into_response()
}
