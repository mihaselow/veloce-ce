use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, VecDeque};
#[cfg(not(target_arch = "wasm32"))]
use std::io;

use crate::apptainer;
use crate::message::Message;
use crate::usage::UsageTracker;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
pub struct MpiStats {
    pub send_calls: u32,
    pub send_bytes: u64,
    pub send_time_secs: f64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LicenseFeatureStatus {
    pub feature: String,
    pub total: u32,
    pub used: u32,
    pub available: u32,
    pub expiration: Option<u64>, // Unix timestamp
    pub vendor: String,          // e.g., "ansyslmd", "cdlmd"
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LicenseServerStatus {
    pub server: String,
    pub status: String, // "Up", "Down"
    pub features: Vec<LicenseFeatureStatus>,
    pub last_update: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LicenseCheckResult {
    pub feature: String,
    pub requested: u32,
    pub available: u32,
    pub can_fulfill: bool,
    pub message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SolverConfig {
    pub name: String,
    pub description: Option<String>,
    pub icon: Option<String>,
    pub is_commercial: bool,
    pub executable: String,
    pub command_template: Option<String>,
    pub default_batch_flags: Vec<String>,
    pub licensing: Option<LicensingConfig>,
    pub input_files: Vec<InputFileConfig>,
    pub output_files: Vec<OutputFileConfig>,
    pub log_format: Option<LogFormatConfig>,
    #[serde(default)]
    pub parameter_mapping: std::collections::HashMap<String, crate::apptainer::ParameterMapping>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LicensingConfig {
    pub server_env_var: String,
    pub base_features: Vec<String>,
    pub hpc: Option<HpcLicensingConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HpcLicensingConfig {
    pub feature_name: String,
    pub scaling_rule: String,
    pub tokens_per_core: usize,
    pub free_cores: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InputFileConfig {
    pub extension: String,
    pub required: bool,
    pub description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputFileConfig {
    pub extension: String,
    #[serde(default)]
    pub is_log: bool,
    #[serde(default)]
    pub is_result: bool,
    pub description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogFormatConfig {
    pub target_output: String,
    pub residual_keywords: Vec<String>,
}

#[cfg(not(target_arch = "wasm32"))]
impl SolverConfig {
    pub async fn load_all<P: AsRef<std::path::Path>>(
        conf_dir: P,
    ) -> io::Result<HashMap<String, SolverConfig>> {
        let mut map = HashMap::new();
        if !conf_dir.as_ref().exists() {
            return Ok(map);
        }
        let mut entries = tokio::fs::read_dir(conf_dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.extension().is_some_and(|ext| ext == "json") {
                let content = tokio::fs::read_to_string(&path).await?;
                match serde_json::from_str::<SolverConfig>(&content) {
                    Ok(config) => {
                        map.insert(config.name.clone(), config);
                    }
                    Err(e) => {
                        log::error!(
                            "Failed to parse SolverConfig from {}: {}",
                            path.display(),
                            e
                        );
                    }
                }
            }
        }
        Ok(map)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct QueueResourceGap {
    pub pending_jobs: usize,
    pub pending_cores: u32,
    pub pending_memory_mb: u64,
    pub pending_nodes: usize,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Resources {
    pub cpu_cores: usize,
    pub total_memory: u64,
    pub free_memory: u64,
    pub cpu_usage: f32,
    pub cpu_model: String,
    pub arch: String,
    // Extended info
    pub os_name: String,
    pub os_version: String,
    pub kernel_version: String,
    pub host_name: String,
    pub load_avg: [f64; 3], // 1, 5, 15 min
    pub disk_total: u64,    // Total disk space in bytes (for working directory partition)
    pub disk_free: u64,     // Free disk space in bytes
    pub uptime: u64,        // Worker uptime in seconds
    pub boot_time: u64,     // System boot time (Unix timestamp)
    pub process_count: u32,
    pub swap_total: u64,
    pub swap_free: u64,
    pub version: String,
    pub gres: BTreeMap<String, u64>,
    pub cgroup_enabled: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct FileHandle {
    pub file_id: String,
    pub original_name: String,
    pub is_executable: bool,
    pub is_archive: bool, // If true, it will be decompressed
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum StepStatus {
    Pending,
    Running,
    Completed(i32),
    Failed(String),
    Killed,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct StepInfo {
    pub step_id: u32,
    pub parent_job_id: u64,
    pub binary: String,
    pub args: Vec<String>,
    pub status: StepStatus,
    pub req_nodes: usize,
    pub req_cores: u32,
    pub ntasks: u32,
    pub assigned_workers: Vec<String>,
    pub is_idle: bool,
    pub idle_duration: u64,
    pub inputs: Vec<FileHandle>,
    pub env_vars: Vec<(String, String)>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Reservation {
    pub id: String,
    pub nodes: std::collections::HashSet<String>,
    pub start_time: u64,
    pub end_time: u64,
    pub owner: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum JobStatus {
    Pending,
    Running,
    Completed(i32),
    Failed(String),
    Killed,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub enum QosLevel {
    Background,
    Preemptible,
    #[default]
    Production,
    Interactive,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum DependencyCondition {
    AfterOk,
    AfterNotOk,
    AfterAny,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct DependencySpec {
    pub parent_id: u64,
    pub condition: DependencyCondition,
}

impl DependencySpec {
    pub fn parse(s: &str) -> Result<Self, String> {
        let parts: Vec<&str> = s.split(':').collect();
        if parts.len() != 2 {
            return Err(format!(
                "Invalid dependency spec format '{}'. Expected condition:parent_id",
                s
            ));
        }
        let cond = match parts[0].to_lowercase().as_str() {
            "afterok" => DependencyCondition::AfterOk,
            "afternotok" => DependencyCondition::AfterNotOk,
            "afterany" => DependencyCondition::AfterAny,
            _ => return Err(format!("Unknown dependency condition '{}'", parts[0])),
        };
        let parent_id = parts[1]
            .parse::<u64>()
            .map_err(|_| format!("Invalid parent job ID '{}'", parts[1]))?;

        Ok(Self {
            parent_id,
            condition: cond,
        })
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
pub struct JobOutputArtifact {
    pub path: String,
    pub output_type: String,
    #[serde(default)]
    pub file_id: Option<String>,
    #[serde(default)]
    pub size: Option<u64>,
    #[serde(default)]
    pub updated_at: Option<u64>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct JobInfo {
    pub id: u64,
    #[serde(default)]
    pub job_name: Option<String>,
    #[serde(default)]
    pub job_comment: Option<String>,
    pub binary: String,
    pub args: Vec<String>,
    pub status: JobStatus,
    pub req_nodes: usize,
    pub req_cores: u32,                // Cores per node
    pub req_memory: u64,               // Memory per node (MB)
    pub assigned_workers: Vec<String>, // List of worker addresses
    pub walltime: u64,                 // Seconds, 0 = infinite
    pub start_time: Option<u64>,       // Unix timestamp
    pub priority: u32,                 // 0 = default, higher = higher priority
    pub user_id: String,               // User who submitted the job
    pub working_directory: String,     // Directory where the job was submitted
    pub queued_time: u64,              // Unix timestamp when queued
    pub current_cpu_usage: f32,        // Live CPU usage percentage
    pub current_memory_usage: u64,     // Live memory usage in bytes
    pub is_idle: bool,                 // True if the job is currently idle
    pub idle_duration: u64,            // Duration in seconds the job has been idle
    pub end_time: Option<u64>,         // Unix timestamp when job finished
    pub env_vars: Vec<(String, String)>,
    pub reason: Option<String>, // Reason for current status (e.g., pending reason)
    pub array_id: Option<u64>,  // Null unless it's a job array
    pub array_task_id: Option<u32>, // The task id inside a job array
    pub inputs: Vec<FileHandle>,
    pub cgroup_active: bool,
    pub gres_req: BTreeMap<String, u64>,
    #[serde(default)]
    pub allocated_cores: HashMap<String, Vec<usize>>, // worker_id -> core IDs
    #[serde(default)]
    pub allocated_gres: HashMap<String, HashMap<String, Vec<u32>>>, // worker_id -> (gres_name -> IDs)
    #[serde(default)]
    pub mpi_stats: Option<MpiStats>,
    #[serde(default)]
    pub secret: String,
    pub stdout_file_id: Option<String>,
    pub stderr_file_id: Option<String>,
    pub workdir_file_id: Option<String>,
    /// Per-worker log object ids for multi-node rank resolution after completion.
    #[serde(default)]
    pub worker_log_files: HashMap<String, WorkerLogFiles>,
    #[serde(default)]
    pub output_artifacts: Vec<JobOutputArtifact>,
    #[serde(default)]
    pub wait_for_licenses: bool,
    #[serde(default)]
    pub estimated_walltime: Option<u64>,
    #[serde(default)]
    pub priority_offset: Option<i32>,
    #[serde(default)]
    pub dependencies: Option<Vec<u64>>,
    #[serde(default)]
    pub dependency_specs: Option<Vec<String>>,
    #[serde(default)]
    pub qos: QosLevel,
    #[serde(default)]
    pub container_asset: Option<apptainer::ContainerAsset>,
    #[serde(default)]
    pub vnc_enabled: bool,
    #[serde(default)]
    pub inherit_host_env: bool,
    #[serde(default)]
    pub env_allowlist: Option<Vec<String>>,
    #[serde(default)]
    pub job_profile: Option<String>,
    #[serde(default)]
    pub interactive_port: Option<u16>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PersistedJob {
    pub job_id: u64,
    pub pid: u32,  // Process ID
    pub pgid: u32, // Process Group ID
    pub binary: String,
    pub args: Vec<String>,
    pub env_vars: Vec<(String, String)>,
    pub working_directory: String,
    pub req_cores: u32,
    pub req_memory: u64,
    pub user_id: String,
    pub walltime: u64,
    pub submission_time: u64,
    pub array_id: Option<u64>,
    pub array_task_id: Option<u32>,
    #[serde(default)]
    pub gres_req: BTreeMap<String, u64>,
    pub secret: String,
    #[serde(default)]
    pub wait_for_licenses: bool,
    #[serde(default)]
    pub estimated_walltime: Option<u64>,
    #[serde(default)]
    pub priority_offset: Option<i32>,
    #[serde(default)]
    pub dependencies: Option<Vec<u64>>,
    #[serde(default)]
    pub dependency_specs: Option<Vec<String>>,
    #[serde(default)]
    pub qos: QosLevel,
    #[serde(default)]
    pub container_asset: Option<apptainer::ContainerAsset>,
    #[serde(default)]
    pub inherit_host_env: bool,
    #[serde(default)]
    pub env_allowlist: Option<Vec<String>>,
    #[serde(default)]
    pub job_profile: Option<String>,
    #[serde(default)]
    pub output_artifacts: Vec<JobOutputArtifact>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct JobEvent {
    pub timestamp: u64,
    pub event_type: String, // e.g. "ResidualsRising", "LicenseLost", "Converged"
    pub severity: String,   // "Info", "Warning", "Critical"
    pub message: String,
    pub metadata: HashMap<String, String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct EfficiencyStat {
    pub solver_name: String,
    pub avg_cpu_percent: f32,
    pub peak_cpu_percent: f32,
    pub avg_memory_mb: u64,
    pub peak_memory_mb: u64,
    pub avg_io_mbps: f32,
    pub sample_count: usize,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct JobStats {
    pub job_id: u64,
    pub cpu_usage_percent: f32, // Percentage of one core
    pub memory_usage_bytes: u64,
    pub is_idle: bool,
    pub idle_duration: u64,
    pub cgroup_active: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LaunchRequest {
    pub job_id: u64,
    pub secret: String,
    pub hostname: String,
    pub binary: String,
    pub args: Vec<String>,
    pub working_directory: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct ControllerInfo {
    pub hostname: String,
    pub role: String,
    pub online: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct WorkerInfo {
    pub id: String,
    pub hostname: String,
    pub ip_address: String,
    pub total_cores: usize,
    pub available_cores: usize,
    pub total_memory: u64,
    pub allocated_memory: u64,
    pub cpu_model: String,
    pub arch: String,
    // Extended Info
    pub os_name: String,
    pub os_version: String,
    pub kernel_version: String,
    pub cpu_usage: f32,
    pub used_memory: u64,
    pub load_avg: [f64; 3],
    pub disk_total: u64,
    pub disk_free: u64,
    pub uptime: u64,
    pub boot_time: u64,
    pub process_count: u32,
    pub swap_total: u64,
    pub swap_free: u64,
    pub version: String,
    pub cgroup_enabled: bool,
    pub gres: BTreeMap<String, u64>,
    pub allocated_gres: HashMap<String, Vec<u32>>,
    pub net_rx_rate: u64,
    pub net_tx_rate: u64,
    pub disk_read_rate: u64,
    pub disk_write_rate: u64,
    pub online: bool,
    #[serde(default)]
    pub controller_id: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum JobStateFilter {
    Pending,
    Running,
    Completed,
    Failed,
    Killed,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
pub struct WorkerLogFiles {
    #[serde(default)]
    pub stdout_file_id: Option<String>,
    #[serde(default)]
    pub stderr_file_id: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct JobUsage {
    pub job_id: u64,
    #[serde(default)]
    pub job_name: Option<String>,
    #[serde(default)]
    pub job_comment: Option<String>,
    pub command_line: String,
    pub user_id: String,
    pub submission_time: u64,
    pub start_time: Option<u64>,
    pub end_time: Option<u64>,
    pub exit_code: Option<i32>,
    pub status: JobStatus,
    pub cpu_time_ms: u64,      // CPU time in milliseconds
    pub max_memory_bytes: u64, // Max resident set size in bytes
    pub req_nodes: usize,
    pub req_cores: u32,
    pub req_memory: u64,
    pub array_id: Option<u64>,
    pub array_task_id: Option<u32>,
    pub assigned_workers: Vec<String>,
    #[serde(default)]
    pub gres_req: BTreeMap<String, u64>,
    #[serde(default)]
    pub cgroup_active: bool,
    pub secret: String,
    #[serde(default)]
    pub stdout_file_id: Option<String>,
    #[serde(default)]
    pub stderr_file_id: Option<String>,
    #[serde(default)]
    pub workdir_file_id: Option<String>,
    /// Per-worker log object ids (worker_id → stdout/stderr). Required so completed
    /// multi-node jobs can still resolve `--rank` / `?rank=` after in-memory tracking
    /// is cleared. See veloce-ce#4.
    #[serde(default)]
    pub worker_log_files: HashMap<String, WorkerLogFiles>,
    #[serde(default)]
    pub output_artifacts: Vec<JobOutputArtifact>,
    #[serde(default)]
    pub wait_for_licenses: bool,
    #[serde(default)]
    pub estimated_walltime: Option<u64>,
    #[serde(default)]
    pub priority_offset: Option<i32>,
    #[serde(default)]
    pub dependencies: Option<Vec<u64>>,
    #[serde(default)]
    pub dependency_specs: Option<Vec<String>>,
    #[serde(default)]
    pub qos: QosLevel,
    #[serde(default)]
    pub container_asset: Option<apptainer::ContainerAsset>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct NodeMetrics {
    pub node_id: String,
    pub timestamp: u64,
    pub running_jobs: u32,
    pub cpu_load: f32,     // Percentage (0.0 - 100.0)
    pub memory_usage: u64, // Bytes
    pub memory_total: u64,
    pub disk_usage: u64, // Bytes
    pub disk_total: u64,
    pub load_avg: [f32; 3], // 1, 5, 15 min
    pub net_rx_rate: u64,   // bytes/sec
    pub net_tx_rate: u64,   // bytes/sec
    pub net_packets_rx_rate: u64,
    pub net_packets_tx_rate: u64,
    pub net_errors: u64,
    pub net_drops: u64,
    pub disk_read_rate: u64,
    pub disk_write_rate: u64,
    pub disk_read_ops_rate: u64,
    pub disk_write_ops_rate: u64,
    pub procs_running: u32,
    pub procs_blocked: u32,
    pub swap_usage: u64,
    pub process_count: u32,
    pub uptime: u64,
    pub cgroup_enabled: bool,
    // Optional GPU (from PulsePlane/NVML)
    pub gpu_usage: Option<f32>,
    pub gpu_mem_usage: Option<u64>,
    pub gpu_temp: Option<f32>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum HistoryFilter {
    Single(u64),
    Range(u64, u64), // Start (inclusive), End (inclusive)
    All,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum LogType {
    Stdout,
    Stderr,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PersistedState {
    pub jobs: HashMap<u64, JobInfo>,
    pub queue: VecDeque<u64>,
    pub next_job_id: u64,
    pub usage_tracker: UsageTracker,
    pub steps: HashMap<(u64, u32), StepInfo>,
    pub next_step_id: u32,
    #[serde(default)]
    pub reservations: HashMap<String, Reservation>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SubmitDag {
    pub name: String,
    pub nodes: Vec<DagNode>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DagNode {
    pub node_id: String,
    pub task: Box<Message>, // Expecting Message::Submit
    #[serde(default)]
    pub depends_on: Vec<String>,
}

// Feature Capability flags
pub const FEATURE_RESERVATIONS: u64 = 1 << 0;
pub const FEATURE_DEPENDENCIES: u64 = 1 << 1;
pub const FEATURE_BACKFILLING: u64 = 1 << 2;
pub const FEATURE_ACCOUNTING: u64 = 1 << 3;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FederationClusterStatus {
    pub id: String,
    pub display_name: String,
    pub controller_api: String,
    pub fileserver_url: Option<String>,
    pub fileserver_configured: bool,
    pub healthy: bool,
    pub controller_status: Option<u16>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FederationClusterCapacity {
    pub cluster_id: String,
    pub display_name: String,
    pub healthy: bool,
    pub pending: Option<QueueResourceGap>,
    pub total_nodes: usize,
    pub total_cores: usize,
    pub available_cores: usize,
    pub total_memory_mb: u64,
    pub available_memory_mb: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FederationJobInfo {
    pub cluster_id: String,
    pub federated_id: String,
    pub job: JobInfo,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FederationTemplateInfo {
    pub cluster_id: String,
    pub template: apptainer::ContainerAsset,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FederationSubmitJobRequest {
    #[serde(default)]
    pub job_name: Option<String>,
    #[serde(default)]
    pub job_comment: Option<String>,
    pub binary: String,
    #[serde(default)]
    pub args: Vec<String>,
    pub req_nodes: usize,
    pub req_cores: u32,
    pub req_memory: u64,
    pub walltime: u64,
    pub priority: u32,
    pub user_id: String,
    pub working_directory: String,
    #[serde(default)]
    pub array_indices: Option<Vec<u32>>,
    #[serde(default)]
    pub inputs: Vec<FileHandle>,
    #[serde(default)]
    pub gres_req: BTreeMap<String, u64>,
    #[serde(default)]
    pub env_vars: Vec<(String, String)>,
    #[serde(default)]
    pub wait_for_licenses: bool,
    #[serde(default)]
    pub dependencies: Option<Vec<u64>>,
    #[serde(default)]
    pub dependency_specs: Option<Vec<String>>,
    #[serde(default)]
    pub qos: QosLevel,
    #[serde(default)]
    pub image_uri: Option<String>,
    #[serde(default)]
    pub vnc_enabled: Option<bool>,
    #[serde(default)]
    pub inherit_host_env: bool,
    #[serde(default)]
    pub env_allowlist: Option<Vec<String>>,
    #[serde(default)]
    pub job_profile: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FederationSubmitStepRequest {
    pub binary: String,
    pub args: Vec<String>,
    pub req_nodes: usize,
    pub req_cores: u32,
    pub ntasks: u32,
    #[serde(default)]
    pub inputs: Vec<FileHandle>,
    #[serde(default)]
    pub gres_req: BTreeMap<String, u64>,
    #[serde(default)]
    pub env_vars: Vec<(String, String)>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FederationLogRequest {
    pub job_id: u64,
    pub log_type: String,
    pub offset: Option<u64>,
    pub length: Option<u64>,
    pub rank: Option<usize>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FederationOutputContentRequest {
    pub job_id: u64,
    pub path: String,
    pub offset: Option<u64>,
    pub length: Option<u64>,
}
