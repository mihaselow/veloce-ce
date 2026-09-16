//! State persistence and job usage accounting helpers.

use crate::job_secrets;
use crate::state::{ControllerRole, GlobalState};
use std::fs::File;
use tracing::error;
use veloce_common::{JobInfo, JobStatus, PeerMessage, PersistedState};

pub const STATE_FILE: &str = "data/veloce_state.bin";

pub fn job_to_usage(job: &JobInfo) -> veloce_common::JobUsage {
    veloce_common::JobUsage {
        container_asset: job.container_asset.clone(),
        job_id: job.id,
        job_name: job.job_name.clone(),
        job_comment: job.job_comment.clone(),
        user_id: job.user_id.clone(),
        command_line: format!("{} {}", job.binary, job.args.join(" ")),
        submission_time: job.queued_time,
        start_time: job.start_time,
        end_time: job.end_time,
        exit_code: match job.status {
            JobStatus::Completed(code) => Some(code),
            _ => None,
        },
        status: job.status.clone(),
        cpu_time_ms: 0,
        max_memory_bytes: 0,
        req_nodes: job.req_nodes,
        req_cores: job.req_cores,
        req_memory: job.req_memory,
        array_id: job.array_id,
        array_task_id: job.array_task_id,
        assigned_workers: job.assigned_workers.clone(),
        gres_req: job.gres_req.clone(),
        cgroup_active: job.cgroup_active,
        secret: job.secret.clone(),
        stdout_file_id: job.stdout_file_id.clone(),
        stderr_file_id: job.stderr_file_id.clone(),
        workdir_file_id: job.workdir_file_id.clone(),
        worker_log_files: job.worker_log_files.clone(),
        output_artifacts: job.output_artifacts.clone(),
        wait_for_licenses: job.wait_for_licenses,
        estimated_walltime: job.estimated_walltime,
        priority_offset: job.priority_offset,
        dependencies: job.dependencies.clone(),
        dependency_specs: job.dependency_specs.clone(),
        qos: job.qos.clone(),
    }
}

pub(crate) fn merge_usage_report_with_job(
    mut usage: veloce_common::JobUsage,
    job: &JobInfo,
) -> veloce_common::JobUsage {
    let mut merged = job_to_usage(job);

    merged.status = usage.status;
    merged.exit_code = usage.exit_code;
    merged.end_time = usage.end_time.or(job.end_time);
    merged.cpu_time_ms = usage.cpu_time_ms;
    merged.max_memory_bytes = usage.max_memory_bytes;
    merged.cgroup_active = usage.cgroup_active;
    merged.stdout_file_id = usage
        .stdout_file_id
        .take()
        .or_else(|| job.stdout_file_id.clone());
    merged.stderr_file_id = usage
        .stderr_file_id
        .take()
        .or_else(|| job.stderr_file_id.clone());
    merged.workdir_file_id = usage
        .workdir_file_id
        .take()
        .or_else(|| job.workdir_file_id.clone());
    if !usage.worker_log_files.is_empty() {
        merged.worker_log_files = usage.worker_log_files;
    } else if !job.worker_log_files.is_empty() {
        merged.worker_log_files = job.worker_log_files.clone();
    }
    if !usage.output_artifacts.is_empty() {
        merged.output_artifacts = usage.output_artifacts;
    }

    if merged.assigned_workers.is_empty() {
        merged.assigned_workers = usage.assigned_workers;
    }

    merged
}

pub fn save_state(state: &GlobalState) {
    if state.role != ControllerRole::Leader {
        // In HA mode with shared storage, followers should not overwrite the leader's state file.
        // They will load the leader's state on startup or promotion.
        return;
    }

    let persisted = PersistedState {
        jobs: state.jobs.clone(),
        queue: state.queue.clone(),
        next_job_id: state.next_job_id,
        usage_tracker: state.usage_tracker.clone(),
        steps: state.steps.clone(),
        next_step_id: state.next_step_id,
        reservations: state.reservations.clone(),
    };

    for tx in state.peers.values() {
        let _ = tx.try_send(PeerMessage::StateSync(persisted.clone()));
    }

    let mut disk_state = persisted;
    job_secrets::prepare_persisted_state_for_disk(&mut disk_state);

    let temp_file = format!("{}.tmp", STATE_FILE);
    match File::create(&temp_file) {
        Ok(file) => {
            if let Err(e) = bincode::serialize_into(&file, &disk_state) {
                error!("Failed to serialize state: {}", e);
                let _ = std::fs::remove_file(&temp_file);
            } else {
                // Ensure data is synced to disk before rename
                if let Err(e) = file.sync_all() {
                    error!("Failed to sync state file: {}", e);
                    let _ = std::fs::remove_file(&temp_file);
                } else {
                    if let Err(e) = std::fs::rename(&temp_file, STATE_FILE) {
                        error!("Failed to rename state file to {}: {}", STATE_FILE, e);
                    }
                }
            }
        }
        Err(e) => {
            error!("Failed to create temporary state file {}: {}", temp_file, e);
        }
    }
}
