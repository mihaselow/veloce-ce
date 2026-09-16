//! API module: types.rs

use serde::{Deserialize, Serialize};
use veloce_common::{JobInfo, JobStatus};

#[derive(Serialize, Debug, Clone)]
pub struct PublicWebConfig {
    pub fileserver_url: String,
    pub oidc_enabled: bool,
}

#[derive(Serialize, Debug, Clone)]
pub struct JobInfoResponse {
    pub id: u64,
    pub job_name: Option<String>,
    pub job_comment: Option<String>,
    pub binary: String,
    pub args: Vec<String>,
    pub status: JobStatus,
    pub req_nodes: usize,
    pub req_cores: u32,
    pub req_memory: u64,
    pub assigned_workers: Vec<String>,
    pub walltime: u64,
    pub start_time: Option<u64>,
    pub priority: u32,
    pub user_id: String,
    pub working_directory: String,
    pub queued_time: u64,
    pub current_cpu_usage: f32,
    pub current_memory_usage: u64,
    pub is_idle: bool,
    pub idle_duration: u64,
    pub end_time: Option<u64>,
    pub env_vars: Vec<(String, String)>,
    pub reason: Option<String>,
    pub array_id: Option<u64>,
    pub array_task_id: Option<u32>,
    pub inputs: Vec<veloce_common::FileHandle>,
    pub cgroup_active: bool,
    pub gres_req: std::collections::BTreeMap<String, u64>,
    pub allocated_cores: std::collections::HashMap<String, Vec<usize>>,
    pub allocated_gres:
        std::collections::HashMap<String, std::collections::HashMap<String, Vec<u32>>>,
    pub mpi_stats: Option<veloce_common::MpiStats>,
    pub stdout_file_id: Option<String>,
    pub stderr_file_id: Option<String>,
    pub workdir_file_id: Option<String>,
    #[serde(default)]
    pub worker_log_files: std::collections::HashMap<String, veloce_common::WorkerLogFiles>,
    pub output_artifacts: Vec<veloce_common::JobOutputArtifact>,
    pub wait_for_licenses: bool,
    pub estimated_walltime: Option<u64>,
    pub priority_offset: Option<i32>,
    pub dependencies: Option<Vec<u64>>,
    pub dependency_specs: Option<Vec<String>>,
    pub qos: veloce_common::QosLevel,
    pub container_asset: Option<veloce_common::apptainer::ContainerAsset>,
    pub vnc_enabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub interactive_port: Option<u16>,
}

impl From<JobInfo> for JobInfoResponse {
    fn from(info: JobInfo) -> Self {
        Self {
            id: info.id,
            job_name: info.job_name,
            job_comment: info.job_comment,
            binary: info.binary,
            args: info.args,
            status: info.status,
            req_nodes: info.req_nodes,
            req_cores: info.req_cores,
            req_memory: info.req_memory,
            assigned_workers: info.assigned_workers,
            walltime: info.walltime,
            start_time: info.start_time,
            priority: info.priority,
            user_id: info.user_id,
            working_directory: info.working_directory,
            queued_time: info.queued_time,
            current_cpu_usage: info.current_cpu_usage,
            current_memory_usage: info.current_memory_usage,
            is_idle: info.is_idle,
            idle_duration: info.idle_duration,
            end_time: info.end_time,
            env_vars: info.env_vars,
            reason: info.reason,
            array_id: info.array_id,
            array_task_id: info.array_task_id,
            inputs: info.inputs,
            cgroup_active: info.cgroup_active,
            gres_req: info.gres_req,
            allocated_cores: info.allocated_cores,
            allocated_gres: info.allocated_gres,
            mpi_stats: info.mpi_stats,
            stdout_file_id: info.stdout_file_id,
            stderr_file_id: info.stderr_file_id,
            workdir_file_id: info.workdir_file_id,
            worker_log_files: info.worker_log_files,
            output_artifacts: info.output_artifacts,
            wait_for_licenses: info.wait_for_licenses,
            estimated_walltime: info.estimated_walltime,
            priority_offset: info.priority_offset,
            dependencies: info.dependencies,
            dependency_specs: info.dependency_specs,
            qos: info.qos,
            container_asset: info.container_asset,
            vnc_enabled: info.vnc_enabled,
            interactive_port: info.interactive_port,
        }
    }
}

#[derive(Deserialize)]
pub(super) struct CreateWsTicketRequest {
    pub scope: String,
    pub job_id: Option<u64>,
}
