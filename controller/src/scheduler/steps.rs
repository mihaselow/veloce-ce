//! MPI/CWL step scheduling and dispatch.

use crate::{send_to_worker, state::GlobalState};
use std::collections::HashMap;
use tracing::{error, info};
use veloce_common::{JobStatus, Message};

pub fn submit_step(
    state: &mut GlobalState,
    parent_job_id: u64,
    binary: String,
    args: Vec<String>,
    req_nodes: usize,
    req_cores: u32,
    ntasks: u32,
    inputs: Vec<veloce_common::FileHandle>,
    _gres_req: std::collections::BTreeMap<String, u64>,
    env_vars: Vec<(String, String)>,
) -> std::result::Result<u32, String> {
    let parent_job = state
        .jobs
        .get(&parent_job_id)
        .ok_or("Parent job not found")?;

    if parent_job.status != JobStatus::Running {
        return Err("Parent job is not running".to_string());
    }

    if parent_job.assigned_workers.is_empty() {
        return Err("Parent job has no assigned workers".to_string());
    }

    let step_id = state.next_step_id;
    state.next_step_id += 1;

    let step_info = veloce_common::StepInfo {
        step_id,
        parent_job_id,
        binary: binary.clone(),
        args: args.clone(),
        status: veloce_common::StepStatus::Pending,
        req_nodes,
        req_cores,
        ntasks,
        assigned_workers: Vec::new(),
        is_idle: false,
        idle_duration: 0,
        inputs: inputs.clone(),
        env_vars,
    };

    state.steps.insert((parent_job_id, step_id), step_info);
    state.scheduler_notify.notify_one();

    Ok(step_id)
}

pub fn schedule_steps(state: &mut GlobalState) {
    let mut to_dispatch = Vec::new();

    // Group pending steps by job
    let mut job_pending_steps: HashMap<u64, Vec<u32>> = HashMap::new();
    for ((jid, sid), step) in state.steps.iter() {
        if matches!(step.status, veloce_common::StepStatus::Pending) {
            job_pending_steps.entry(*jid).or_default().push(*sid);
        }
    }

    for (job_id, mut step_ids) in job_pending_steps {
        step_ids.sort(); // Submission order

        if let Some(parent_job) = state.jobs.get(&job_id) {
            if parent_job.status != JobStatus::Running {
                continue;
            }

            // Calculate currently running cores
            let mut current_cores_used: u32 = state
                .steps
                .iter()
                .filter(|((jid, _), s)| {
                    *jid == job_id && matches!(s.status, veloce_common::StepStatus::Running)
                })
                .map(|(_, s)| s.req_cores)
                .sum();

            for step_id in step_ids {
                if let Some(step) = state.steps.get(&(job_id, step_id)) {
                    if current_cores_used + step.req_cores <= parent_job.req_cores {
                        to_dispatch.push((job_id, step_id));
                        current_cores_used += step.req_cores;
                    }
                }
            }
        }
    }

    for (job_id, step_id) in to_dispatch {
        if let Err(e) = perform_step_dispatch(state, job_id, step_id) {
            error!(
                "Failed to dispatch scheduled step {}:{}: {}",
                job_id, step_id, e
            );
        }
    }
}

fn perform_step_dispatch(
    state: &mut GlobalState,
    parent_job_id: u64,
    step_id: u32,
) -> std::result::Result<(), String> {
    let parent_job = state
        .jobs
        .get(&parent_job_id)
        .ok_or("Parent job not found")?
        .clone();
    let step_info = state
        .steps
        .get_mut(&(parent_job_id, step_id))
        .ok_or("Step not found")?;

    let binary = step_info.binary.clone();
    let args = step_info.args.clone();
    let req_nodes = step_info.req_nodes;
    let ntasks = step_info.ntasks;
    let inputs = step_info.inputs.clone();
    let env_vars = step_info.env_vars.clone();

    info!(
        "Dispatching step {}:{} (binary={}, nodes={}, tasks={})",
        parent_job_id, step_id, binary, req_nodes, ntasks
    );

    let workers_to_use = parent_job
        .assigned_workers
        .iter()
        .take(req_nodes)
        .cloned()
        .collect::<Vec<_>>();
    let mut node_ips = Vec::new();
    for w_id in &workers_to_use {
        if let Some(w) = state.workers.get(w_id) {
            node_ips.push(w.addr.ip().to_string());
        }
    }

    step_info.assigned_workers = workers_to_use.clone();
    // StepStatus::Running will be set when the worker reports back, but we can set it here too
    // to prevent schedule_steps from picking it up again in the next micro-tick before worker responds.
    step_info.status = veloce_common::StepStatus::Running;

    let num_workers = workers_to_use.len() as u32;
    let ranks_per_worker = if num_workers > 0 {
        ntasks / num_workers
    } else {
        ntasks
    };
    let remainder = if num_workers > 0 {
        ntasks % num_workers
    } else {
        0
    };

    let mut current_rank_id = 0;
    for (i, worker_id) in workers_to_use.iter().enumerate() {
        let ranks_to_assign = ranks_per_worker + if (i as u32) < remainder { 1 } else { 0 };
        let mut assigned_ranks = Vec::new();
        for _ in 0..ranks_to_assign {
            assigned_ranks.push(current_rank_id);
            current_rank_id += 1;
        }

        if let Some(worker) = state.workers.get(worker_id) {
            let msg = Message::RunStep {
                parent_job_id,
                step_id,
                binary: binary.clone(),
                args: args.clone(),
                node_list: node_ips.clone(),
                assigned_cores: None,
                working_directory: parent_job.working_directory.clone(),
                user_id: parent_job.user_id.clone(),
                assigned_ranks,
                total_ranks: ntasks,
                inputs: inputs.clone(),
                gres_req: std::collections::BTreeMap::new(),
                allocated_gres: HashMap::new(),
                secret: parent_job.secret.clone(),
                controller_url: format!("https://localhost:{}", state.api_port),
                env_vars: env_vars.clone(),
            };

            let parent_job_id_log = parent_job_id;
            let step_id_log = step_id;
            let worker_id_log = worker_id.clone();
            let worker_addr_log = worker.addr;

            if let Err(e) = send_to_worker(state, worker_id, msg) {
                error!(
                    "Failed to send RunStep {}:{} to worker {}: {}",
                    parent_job_id_log, step_id_log, worker_id_log, e
                );
            } else {
                info!(
                    "Sending RunStep {}:{} to worker {} at {}",
                    parent_job_id_log, step_id_log, worker_id_log, worker_addr_log
                );
            }
        }
    }

    Ok(())
}
