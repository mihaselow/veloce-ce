//! Job finalization, timeouts, and resource release.

use crate::{
    job_to_usage, send_to_worker,
    state::{sync_active_state_to_dashmaps, ControllerRole, GlobalState, SharedState},
};
use std::sync::atomic::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{info, warn};
use veloce_common::{JobStatus, Message};

use super::calculate_effective_priority;

pub async fn check_timeouts(state: SharedState) {
    let mut state_lock = state.state.lock().await;
    if state_lock.role != ControllerRole::Leader {
        return;
    }
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    // 1. Check Job Walltimes
    let mut timed_out_jobs = Vec::new();

    for (id, job) in &state_lock.jobs {
        if job.status == JobStatus::Running && job.walltime > 0 {
            if let Some(start) = job.start_time {
                if now.saturating_sub(start) > job.walltime {
                    timed_out_jobs.push(*id);
                }
            }
        }
    }

    for job_id in &timed_out_jobs {
        let job_id = *job_id;
        info!("Job {} timed out. Terminating...", job_id);

        // Notify head node
        let head_worker_id = if let Some(job) = state_lock.jobs.get(&job_id) {
            job.assigned_workers.first().cloned()
        } else {
            None
        };

        if let Some(worker_id) = head_worker_id {
            let _ = send_to_worker(&state_lock, &worker_id, Message::TerminateJob { job_id });
        }

        state.missing_jobs.remove(&job_id);
        finalize_job(
            &mut state_lock,
            job_id,
            JobStatus::Failed("Timeout".to_string()),
        );
        let job_clone = state_lock.jobs.get(&job_id).cloned();
        state_lock.jobs.remove(&job_id);
        state.failed_jobs.fetch_add(1, Ordering::Relaxed);

        if let Some(job) = job_clone {
            let usage = job_to_usage(&job);
            let store = state.accounting_store.clone();
            tokio::spawn(async move {
                let _ = store.record_job(&usage).await;
            });
        }
    }

    // 2. Check Worker Timeouts (Grace Period)
    let mut dead_workers: Vec<String> = Vec::new();
    const WORKER_TIMEOUT: u64 = 60; // 60 seconds grace period

    for (id, worker) in &state_lock.workers {
        if !worker.connected && (now.saturating_sub(worker.last_seen) > WORKER_TIMEOUT) {
            dead_workers.push(id.clone());
        }
    }

    for worker_id in &dead_workers {
        info!(
            "Worker {} timed out after disconnect. Removing...",
            worker_id
        );

        // Logic to fail assigned jobs
        let jobs_to_fail: Vec<u64> = if let Some(worker) = state_lock.workers.get(worker_id) {
            worker.assigned_jobs.iter().cloned().collect()
        } else {
            Vec::new()
        };

        state_lock.workers.remove(worker_id);
        state.metrics_store.remove(worker_id);

        for job_id in jobs_to_fail {
            // Also reset resource usage for these failed jobs
            if let Some(job) = state_lock.jobs.get_mut(&job_id) {
                job.current_cpu_usage = 0.0;
                job.current_memory_usage = 0;
            }
            state.missing_jobs.remove(&job_id);
            finalize_job(
                &mut state_lock,
                job_id,
                JobStatus::Failed("Worker timeout".to_string()),
            );
            let job_clone = state_lock.jobs.get(&job_id).cloned();
            state_lock.jobs.remove(&job_id);
            state.failed_jobs.fetch_add(1, Ordering::Relaxed);

            if let Some(job) = job_clone {
                let usage = job_to_usage(&job);
                let store = state.accounting_store.clone();
                tokio::spawn(async move {
                    let _ = store.record_job(&usage).await;
                });
            }
        }
    }

    if !timed_out_jobs.is_empty() || !dead_workers.is_empty() {
        sync_active_state_to_dashmaps(&state, &state_lock);
        state_lock.scheduler_notify.notify_one();
    }
}

pub async fn prune_jobs(state: SharedState, retention_seconds: u64) {
    let mut state_lock = state.state.lock().await;
    if state_lock.role != ControllerRole::Leader {
        return;
    }
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let mut to_remove = Vec::new();

    for (id, job) in &state_lock.jobs {
        // Only prune completed, failed, or killed jobs
        match job.status {
            JobStatus::Completed(_) | JobStatus::Failed(_) | JobStatus::Killed => {
                if let Some(end_time) = job.end_time {
                    if now.saturating_sub(end_time) > retention_seconds {
                        to_remove.push(*id);
                    }
                } else {
                    // Fallback: If end_time is missing but it's done, use submission time + retention + 1 day
                    if now.saturating_sub(job.queued_time) > retention_seconds + 86400 {
                        to_remove.push(*id);
                    }
                }
            }
            _ => {}
        }
    }

    if !to_remove.is_empty() {
        info!(
            "Pruning {} old jobs (retention: {}s)",
            to_remove.len(),
            retention_seconds
        );
        for id in to_remove {
            state_lock.jobs.remove(&id);
        }
        // No need to notify scheduler or save state immediately, next periodic save will catch it
    }
}

pub fn preempt_job(state: &mut GlobalState, job_id: u64) {
    if let Some(job) = state.jobs.get(&job_id) {
        if matches!(job.status, JobStatus::Running) {
            info!("Preempting job {}", job_id);
            if let Some(worker_id) = job.assigned_workers.first() {
                let _ = send_to_worker(state, worker_id, Message::PreemptJob { job_id });
            }
            // 1. Release resources
            release_job_resources_internal(state, job_id);

            // 2. Put back to pending state
            if let Some(job_mut) = state.jobs.get_mut(&job_id) {
                job_mut.status = JobStatus::Pending;
                job_mut.start_time = None;
                job_mut.assigned_workers.clear();
                job_mut.allocated_cores.clear();
                job_mut.allocated_gres.clear();
                job_mut.current_cpu_usage = 0.0;
                job_mut.current_memory_usage = 0;
                job_mut.reason = Some("Preempted by higher priority job".to_string());
            }

            // 3. Put back to queue
            if !state.queue.contains(&job_id) {
                state.queue.push_back(job_id);
            }
        }
    }
}

pub fn finalize_job(state: &mut GlobalState, job_id: u64, new_status: JobStatus) -> Vec<String> {
    let mut files_to_delete = Vec::new();
    if let Some(job) = state.jobs.get(&job_id) {
        for input in &job.inputs {
            files_to_delete.push(input.file_id.clone());
        }
    }
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    // 1. Calculate Usage and Accrue
    if let Some(job) = state.jobs.get(&job_id) {
        if matches!(job.status, JobStatus::Running) {
            if let Some(start_time) = job.start_time {
                let elapsed = now.saturating_sub(start_time);
                // Usage based on allocated cores * time
                let cores_used = job.req_nodes as f64 * job.req_cores as f64;
                let cost = (elapsed as f64) * cores_used;
                state.usage_tracker.accrue(&job.user_id, cost);
                let current_total_cost = state.usage_tracker.get_usage(&job.user_id);
                let effective_priority = calculate_effective_priority(job, &state.usage_tracker);
                info!("Job {} finished. User {} (Eff. Prio: {:.2}) accrued {} cost. Current total cost: {:.2}. ({}s * {} nodes * {} cores)",
                     job_id, job.user_id, effective_priority, cost, current_total_cost, elapsed, job.req_nodes, job.req_cores);
            }
        }
    }

    // 2. Release Resources
    release_job_resources_internal(state, job_id);

    // 3. Update Status, reset usage metrics, and set end_time
    if let Some(job) = state.jobs.get_mut(&job_id) {
        job.status = new_status;
        job.current_cpu_usage = 0.0;
        job.current_memory_usage = 0;
        job.end_time = Some(now); // Set end_time here
        job.reason = None;
    }

    // save_state(state); // Deferred to scheduler loop
    files_to_delete
}

fn release_job_resources_internal(state: &mut GlobalState, job_id: u64) {
    if let Some(job) = state.jobs.get(&job_id) {
        for worker_id in &job.assigned_workers {
            if let Some(worker) = state.workers.get_mut(worker_id) {
                if worker.assigned_jobs.remove(&job_id) {
                    // Release resources

                    // Recover specific cores
                    let released_cores =
                        if let Some(assigned_cores) = worker.job_core_assignments.remove(&job_id) {
                            assigned_cores
                        } else if let Some(assigned_cores) = job.allocated_cores.get(worker_id) {
                            assigned_cores.clone()
                        } else {
                            warn!(
                                "Job {} had no recorded core assignments on worker {}",
                                job_id, worker_id
                            );
                            Vec::new()
                        };
                    for core in released_cores {
                        if core < worker.resources.cpu_cores
                            && !worker.available_core_ids.contains(&core)
                        {
                            worker.available_core_ids.push(core);
                        }
                    }
                    worker.available_core_ids.sort_unstable();

                    worker.allocated_memory =
                        worker.allocated_memory.saturating_sub(job.req_memory);

                    // Recover GRES IDs
                    let released_gres = worker
                        .job_gres_assignments
                        .remove(&job_id)
                        .or_else(|| job.allocated_gres.get(worker_id).cloned());
                    if let Some(job_gres) = released_gres {
                        for (name, ids) in job_gres {
                            let resource_count =
                                worker.resources.gres.get(&name).cloned().unwrap_or(0) as u32;
                            let available = worker.available_gres_ids.entry(name).or_default();
                            for id in ids {
                                if id < resource_count && !available.contains(&id) {
                                    available.push(id);
                                }
                            }
                            available.sort_unstable(); // Keep them tidy
                        }
                    }

                    if worker.assigned_jobs.is_empty() {
                        worker.available_core_ids = (0..worker.resources.cpu_cores).collect();
                        worker.available_gres_ids = worker
                            .resources
                            .gres
                            .iter()
                            .map(|(name, &count)| {
                                (name.clone(), (0..count as u32).collect::<Vec<_>>())
                            })
                            .collect();
                    }
                }
            }
        }
    }
}
