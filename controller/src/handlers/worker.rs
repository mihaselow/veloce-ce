//! Worker TCP handler and outbound message routing.

use crate::{
    metrics_store,
    persistence::{job_to_usage, merge_usage_report_with_job},
    scheduler::finalize_job,
    state::{
        get_leader_peer_tx, rebuild_worker_allocations, reconcile_missing_worker_jobs,
        sync_active_state_to_dashmaps, ControllerRole, GlobalState, SharedContext, WorkerHandle,
        WorkerRouting,
    },
};
use anyhow::Result;
use dashmap::DashMap;
use futures::{SinkExt, StreamExt};
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tokio_util::codec::Framed;
use tracing::{error, info, warn};
use veloce_common::{JobStatus, Message, MessageCodec, PeerMessage, QosLevel, Resources};

pub async fn handle_worker<S>(
    framed: Framed<S, MessageCodec>,
    addr: SocketAddr,
    ctx: SharedContext,
    worker_id: String,
    hostname: String,
    initial_resources: Resources,
    features: u64,
) -> Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let (tx, mut rx) = mpsc::channel(1024);
    let connection_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    let tx_clone = tx.clone();

    // Register worker
    {
        let mut state_lock = ctx.state.lock().await;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let rebuilt_allocations =
            rebuild_worker_allocations(&worker_id, &initial_resources, &state_lock.jobs);

        if let Some(w) = state_lock.workers.get_mut(&worker_id) {
            info!(
                "Worker {} reconnected. Resuming session from {}",
                worker_id, w.addr
            );
            let current_pulse_interval = w.current_pulse_interval;
            *w = WorkerHandle {
                addr: addr,
                hostname: hostname.clone(),
                resources: initial_resources.clone(),
                routing: WorkerRouting::Direct(tx_clone.clone()),
                assigned_jobs: rebuilt_allocations.assigned_jobs,
                allocated_memory: rebuilt_allocations.allocated_memory,
                connection_id: connection_id,
                available_core_ids: rebuilt_allocations.available_core_ids,
                job_core_assignments: rebuilt_allocations.job_core_assignments,
                available_gres_ids: rebuilt_allocations.available_gres_ids,
                job_gres_assignments: rebuilt_allocations.job_gres_assignments,
                connected: true,
                last_seen: now,
                cgroup_enabled: initial_resources.cgroup_enabled,
                draining: false,
                current_pulse_interval,
                features,
            };
        } else {
            // New worker OR orphaned worker (e.g. after failover)
            if !rebuilt_allocations.assigned_jobs.is_empty() {
                info!(
                    "Worker {} adopted {} running jobs and recovered resource assignments.",
                    worker_id,
                    rebuilt_allocations.assigned_jobs.len()
                );
            }

            state_lock.workers.insert(
                worker_id.clone(),
                WorkerHandle {
                    addr,
                    hostname: hostname.clone(),
                    resources: initial_resources.clone(),
                    routing: WorkerRouting::Direct(tx),
                    assigned_jobs: rebuilt_allocations.assigned_jobs,
                    allocated_memory: rebuilt_allocations.allocated_memory,
                    connection_id,
                    available_core_ids: rebuilt_allocations.available_core_ids,
                    job_core_assignments: rebuilt_allocations.job_core_assignments,
                    available_gres_ids: rebuilt_allocations.available_gres_ids,
                    job_gres_assignments: rebuilt_allocations.job_gres_assignments,
                    connected: true,
                    last_seen: now,
                    cgroup_enabled: initial_resources.cgroup_enabled,
                    draining: false,
                    current_pulse_interval: 2.0,
                    features,
                },
            );
            info!("Worker {} connected from {}", worker_id, addr);
        }

        if state_lock.role == ControllerRole::Follower {
            if let Some(peer_tx) = get_leader_peer_tx(&state_lock) {
                let _ = peer_tx.try_send(PeerMessage::RegisterWorker {
                    worker_id: worker_id.clone(),
                    hostname: hostname.clone(),
                    resources: initial_resources.clone(),
                    features: Some(features),
                    ip_address: Some(addr.ip().to_string()),
                });
            } else {
                // If leader is not set yet, broadcast registration to all peers.
                // Followers will ignore it, but the leader (if connected) will process it.
                for peer_tx in state_lock.peers.values() {
                    let _ = peer_tx.try_send(PeerMessage::RegisterWorker {
                        worker_id: worker_id.clone(),
                        hostname: hostname.clone(),
                        resources: initial_resources.clone(),
                        features: Some(features),
                        ip_address: Some(addr.ip().to_string()),
                    });
                }
            }
        }

        sync_active_state_to_dashmaps(&ctx, &state_lock);
        state_lock.scheduler_notify.notify_one();
    }

    // Split framed into write and read
    let (mut sink, mut stream) = framed.split();

    // Writer task
    let writer = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if sink.send(msg).await.is_err() {
                break;
            }
        }
    });

    // Reader loop
    let mut last_telemetry_sent = std::time::Instant::now();
    while let Some(msg_res) = stream.next().await {
        // Continuous validation: Ensure this worker is still in our map.
        {
            let state = ctx.state.lock().await;
            if !state.workers.contains_key(&worker_id) {
                info!(
                    "Closing connection to worker {} because worker was cleared.",
                    worker_id
                );
                break;
            }
        }

        let msg = match msg_res {
            Ok(m) => m,
            Err(e) => {
                error!("Error reading from worker {} ({}): {}", worker_id, addr, e);
                break;
            }
        };

        // Follower forwarding check
        let role = { ctx.state.lock().await.role };
        if role == ControllerRole::Follower {
            match msg {
                Message::Heartbeat {
                    resources,
                    job_stats,
                } => {
                    {
                        let mut state_lock = ctx.state.lock().await;
                        if let Some(worker) = state_lock.workers.get_mut(&worker_id) {
                            worker.resources = resources.clone();
                            worker.last_seen = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap()
                                .as_secs();
                        }
                    }
                    if last_telemetry_sent.elapsed() >= Duration::from_secs(5) {
                        if let Some(leader_tx) = get_leader_peer_tx(&*ctx.state.lock().await) {
                            let _ = leader_tx.try_send(PeerMessage::BatchWorkerHeartbeats {
                                updates: vec![(worker_id.clone(), resources, job_stats)],
                            });
                            last_telemetry_sent = std::time::Instant::now();
                        }
                    }
                }
                Message::WorkerDraining { worker_id: ref id } => {
                    {
                        let mut state_lock = ctx.state.lock().await;
                        if let Some(worker) = state_lock.workers.get_mut(id) {
                            worker.draining = true;
                            info!("Worker {} is now in DRAIN mode", id);
                        }
                    }
                    if let Some(leader_tx) = get_leader_peer_tx(&*ctx.state.lock().await) {
                        let _ = leader_tx.try_send(PeerMessage::ForwardFromWorker {
                            worker_id: worker_id.clone(),
                            msg: Box::new(msg),
                        });
                    }
                }
                other => {
                    if let Some(leader_tx) = get_leader_peer_tx(&*ctx.state.lock().await) {
                        let _ = leader_tx.try_send(PeerMessage::ForwardFromWorker {
                            worker_id: worker_id.clone(),
                            msg: Box::new(other),
                        });
                    }
                }
            }
            continue; // Skip normal leader-only processing
        }

        match msg {
            Message::Heartbeat {
                resources,
                job_stats,
            } => {
                let mut state_lock = ctx.state.lock().await;
                if let Some(worker) = state_lock.workers.get_mut(&worker_id) {
                    if worker.connection_id == connection_id {
                        worker.resources = resources;
                        let now = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_secs();
                        worker.last_seen = now;
                        let active_job_ids: std::collections::HashSet<u64> =
                            job_stats.iter().map(|s| s.job_id).collect();
                        reconcile_missing_worker_jobs(
                            &ctx.missing_jobs,
                            &mut state_lock,
                            &worker_id,
                            &active_job_ids,
                            now,
                        );

                        // Update individual job stats and real-time telemetry ring buffer (Milestone 2)
                        for stats in job_stats {
                            {
                                let mut q = ctx
                                    .job_metrics_history
                                    .entry(stats.job_id)
                                    .or_insert_with(std::collections::VecDeque::new);
                                if q.len() >= 300 {
                                    q.pop_front();
                                }
                                q.push_back(stats.clone());
                            }

                            // Update per-worker stats for multinode aggregation
                            {
                                let mut stats_map = ctx
                                    .job_worker_stats
                                    .entry(stats.job_id)
                                    .or_insert_with(HashMap::new);
                                stats_map.insert(
                                    worker_id.clone(),
                                    (
                                        stats.cpu_usage_percent,
                                        stats.memory_usage_bytes,
                                        stats.is_idle,
                                    ),
                                );
                            }

                            if let Some(job) = state_lock.jobs.get_mut(&stats.job_id) {
                                let mut total_cpu = 0.0f32;
                                let mut total_mem = 0u64;
                                let mut all_idle = true;
                                let mut reports_count = 0;

                                if let Some(stats_map) = ctx.job_worker_stats.get(&stats.job_id) {
                                    for worker_addr in &job.assigned_workers {
                                        if let Some(&(w_cpu, w_mem, w_idle)) =
                                            stats_map.get(worker_addr)
                                        {
                                            total_cpu += w_cpu;
                                            total_mem += w_mem;
                                            all_idle &= w_idle;
                                            reports_count += 1;
                                        } else {
                                            all_idle = false;
                                        }
                                    }
                                } else {
                                    all_idle = false;
                                }

                                let globally_idle =
                                    all_idle && reports_count == job.assigned_workers.len();

                                job.current_cpu_usage = total_cpu;
                                job.current_memory_usage = total_mem;
                                job.cgroup_active = stats.cgroup_active;

                                if globally_idle {
                                    if !job.is_idle {
                                        job.is_idle = true;
                                        ctx.global_idle_start.insert(job.id, now);
                                        job.idle_duration = 0;
                                    } else if let Some(start_time) =
                                        ctx.global_idle_start.get(&job.id)
                                    {
                                        job.idle_duration = now.saturating_sub(*start_time);
                                    }
                                } else {
                                    job.is_idle = false;
                                    ctx.global_idle_start.remove(&job.id);
                                    job.idle_duration = 0;
                                }

                                let timeout_secs = std::env::var("VELOCE_IDLE_TIMEOUT")
                                    .ok()
                                    .and_then(|v| v.parse::<u64>().ok())
                                    .unwrap_or(600);

                                if job.is_idle
                                    && job.idle_duration >= timeout_secs
                                    && job.qos > QosLevel::Preemptible
                                {
                                    info!("Job {} has been idle for {} seconds (>= {}s limit). Degrading QoS from {:?} to Preemptible.", job.id, job.idle_duration, timeout_secs, job.qos);
                                    job.qos = QosLevel::Preemptible;
                                }
                            }
                        }
                        state_lock.scheduler_notify.notify_one();
                    }
                }
            }
            other => {
                if let Err(e) = handle_worker_message(&ctx, &worker_id, other).await {
                    error!("Error handling message from worker {}: {}", worker_id, e);
                }
            }
        }
    }

    writer.abort();

    // Cleanup
    {
        let mut state_lock = ctx.state.lock().await;

        let should_mark_disconnected = if let Some(handle) = state_lock.workers.get(&worker_id) {
            handle.connection_id == connection_id
        } else {
            false
        };

        if should_mark_disconnected {
            if let Some(worker) = state_lock.workers.get_mut(&worker_id) {
                worker.connected = false;
                worker.last_seen = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs();
                info!(
                    "Worker {} disconnected: {} (marked for recovery)",
                    worker_id, addr
                );
            }

            if state_lock.role == ControllerRole::Follower {
                if let Some(peer_tx) = get_leader_peer_tx(&state_lock) {
                    let _ = peer_tx.try_send(PeerMessage::DeregisterWorker {
                        worker_id: worker_id.clone(),
                    });
                } else {
                    for peer_tx in state_lock.peers.values() {
                        let _ = peer_tx.try_send(PeerMessage::DeregisterWorker {
                            worker_id: worker_id.clone(),
                        });
                    }
                }
            }
        } else {
            info!(
                "Old connection for worker {} disconnected (replaced)",
                worker_id
            );
        }
        sync_active_state_to_dashmaps(&ctx, &state_lock);
    }

    Ok(())
}

pub async fn handle_worker_message(
    ctx: &SharedContext,
    worker_id: &str,
    msg: Message,
) -> Result<()> {
    match msg {
        Message::WorkerDraining { worker_id: id } => {
            let mut state_lock = ctx.state.lock().await;
            if let Some(worker) = state_lock.workers.get_mut(&id) {
                worker.draining = true;
                info!("Worker {} is now in DRAIN mode", id);
            }
        }
        Message::JobStarted { job_id } => {
            let mut state_lock = ctx.state.lock().await;

            let cgroup_active = if let Some(job) = state_lock.jobs.get(&job_id) {
                if let Some(worker_id) = job.assigned_workers.first() {
                    state_lock
                        .workers
                        .get(worker_id)
                        .map(|w| w.cgroup_enabled)
                        .unwrap_or(false)
                } else {
                    false
                }
            } else {
                false
            };

            if let Some(job) = state_lock.jobs.get_mut(&job_id) {
                job.status = JobStatus::Running;
                job.cgroup_active = cgroup_active;
            }
            state_lock.scheduler_notify.notify_one();
        }
        Message::JobInteractivePort { job_id, port } => {
            let mut state_lock = ctx.state.lock().await;
            if let Some(job) = state_lock.jobs.get_mut(&job_id) {
                job.interactive_port = Some(port);
                info!("Job {} interactive port registered: {}", job_id, port);
            }
        }
        Message::JobOutput { job_id, data } => {
            if let Ok(s) = String::from_utf8(data) {
                print!("[Job {}]: {}", job_id, s);

                let mut state_lock = ctx.state.lock().await;
                let buffer = state_lock
                    .job_output_buffers
                    .entry(job_id)
                    .or_insert_with(String::new);
                buffer.push_str(&s);

                if buffer.len() > 10240 {
                    let drain_point = buffer.len() - 10240;
                    buffer.drain(..drain_point);
                }
            }
        }
        Message::JobDone { job_id, exit_code } => {
            ctx.missing_jobs.remove(&job_id);
            let should_finalize = {
                let mut state_lock = ctx.state.lock().await;
                let done_count = crate::job_logs::mark_worker_done(
                    &mut state_lock.multinode_job_tracking,
                    job_id,
                    worker_id,
                );
                let Some(job) = state_lock.jobs.get(&job_id) else {
                    return Ok(());
                };
                if job.status != JobStatus::Running {
                    return Ok(());
                }
                done_count
                    >= crate::job_logs::expected_done_workers(
                        &state_lock.multinode_job_tracking,
                        job_id,
                        job,
                    )
            };
            if !should_finalize {
                return Ok(());
            }

            let (job_for_merge, tracking_snapshot) = {
                let state_lock = ctx.state.lock().await;
                (
                    state_lock.jobs.get(&job_id).cloned(),
                    state_lock.multinode_job_tracking.clone(),
                )
            };
            if let Some(job) = job_for_merge {
                let worker_order = crate::job_logs::worker_order_for_logs(&job, &tracking_snapshot);
                let log_files = tracking_snapshot
                    .worker_log_files
                    .get(&job_id)
                    .cloned()
                    .unwrap_or_default();
                let (stdout_id, stderr_id) = crate::job_logs::merge_and_upload_job_logs(
                    &ctx,
                    job_id,
                    &worker_order,
                    &log_files,
                )
                .await;

                let (files, job_clone) = {
                    let mut state_lock = ctx.state.lock().await;

                    if let Some(buffer) = state_lock.job_output_buffers.remove(&job_id) {
                        let mut total_send_calls = 0;
                        let mut total_send_bytes = 0;
                        let mut max_send_time = 0.0;
                        let mut found = false;

                        let re = regex::Regex::new(r"\[VELOCE_PROFILER\] Rank \d+: MPI_Send calls: (\d+), Total Data: (\d+) bytes, Total Time: ([\d.]+) s").unwrap();
                        for cap in re.captures_iter(&buffer) {
                            found = true;
                            if let Ok(calls) = cap[1].parse::<u32>() {
                                total_send_calls += calls;
                            }
                            if let Ok(bytes) = cap[2].parse::<u64>() {
                                total_send_bytes += bytes;
                            }
                            if let Ok(time) = cap[3].parse::<f64>() {
                                if time > max_send_time {
                                    max_send_time = time;
                                }
                            }
                        }

                        if found {
                            if let Some(job) = state_lock.jobs.get_mut(&job_id) {
                                job.mpi_stats = Some(veloce_common::MpiStats {
                                    send_calls: total_send_calls,
                                    send_bytes: total_send_bytes,
                                    send_time_secs: max_send_time,
                                });
                            }
                        }
                    }

                    let fresh_logs = state_lock
                        .multinode_job_tracking
                        .worker_log_files
                        .get(&job_id)
                        .cloned()
                        .filter(|m| !m.is_empty())
                        .or_else(|| {
                            if log_files.is_empty() {
                                None
                            } else {
                                Some(log_files.clone())
                            }
                        });
                    if let Some(job) = state_lock.jobs.get_mut(&job_id) {
                        if let Some(id) = stdout_id {
                            job.stdout_file_id = Some(id);
                        }
                        if let Some(id) = stderr_id {
                            job.stderr_file_id = Some(id);
                        }
                        // Prefer live tracking — peers may have reported during merge await.
                        if let Some(fresh) = fresh_logs {
                            job.worker_log_files =
                                crate::job_logs::tracking_to_worker_log_files(&fresh);
                        }
                    }

                    let files =
                        finalize_job(&mut state_lock, job_id, JobStatus::Completed(exit_code));
                    let job_clone = state_lock.jobs.get(&job_id).cloned();
                    state_lock.jobs.remove(&job_id);
                    crate::job_logs::cleanup_job_tracking(
                        &mut state_lock.multinode_job_tracking,
                        job_id,
                    );
                    ctx.completed_jobs.fetch_add(1, Ordering::Relaxed);
                    sync_active_state_to_dashmaps(ctx, &state_lock);
                    state_lock.scheduler_notify.notify_one();
                    (files, job_clone)
                };
                let fc = ctx.file_client.clone();
                let ctx_clone = ctx.clone();
                tokio::spawn(async move {
                    for f_id in files {
                        let is_referenced = {
                            let state = ctx_clone.state.lock().await;
                            state.jobs.values().any(|j| {
                                (j.status == JobStatus::Pending || j.status == JobStatus::Running)
                                    && j.inputs.iter().any(|input| input.file_id == f_id)
                            })
                        };
                        if !is_referenced {
                            let _ = fc.delete_file(&f_id).await;
                        } else {
                            info!("Bypassing deletion of file {} because it is still referenced by other active jobs", f_id);
                        }
                    }
                });
                if let Some(job) = job_clone {
                    let usage = job_to_usage(&job);
                    let store = ctx.accounting_store.clone();
                    tokio::spawn(async move {
                        let _ = store.record_job(&usage).await;
                    });
                }
            }
        }
        Message::JobError { job_id, error } => {
            let audit_user_missing = error.contains("job.launch.user_not_found");
            let audit_user_id = if audit_user_missing {
                let state_lock = ctx.state.lock().await;
                state_lock.jobs.get(&job_id).map(|j| j.user_id.clone())
            } else {
                None
            };
            if let Some(ref user_id) = audit_user_id {
                let _ = ctx
                    .audit
                    .log(&crate::audit::AuditEvent::job_launch_user_not_found(
                        job_id, user_id,
                    ))
                    .await;
            }
            ctx.missing_jobs.remove(&job_id);
            let (files, job_clone) = {
                let mut state_lock = ctx.state.lock().await;
                if let Some(log_files) = state_lock
                    .multinode_job_tracking
                    .worker_log_files
                    .get(&job_id)
                    .cloned()
                {
                    if let Some(job) = state_lock.jobs.get_mut(&job_id) {
                        if !log_files.is_empty() {
                            job.worker_log_files =
                                crate::job_logs::tracking_to_worker_log_files(&log_files);
                        }
                    }
                }
                let files = finalize_job(&mut state_lock, job_id, JobStatus::Failed(error));
                let job_clone = state_lock.jobs.get(&job_id).cloned();
                state_lock.jobs.remove(&job_id);
                crate::job_logs::cleanup_job_tracking(
                    &mut state_lock.multinode_job_tracking,
                    job_id,
                );
                ctx.failed_jobs.fetch_add(1, Ordering::Relaxed);
                sync_active_state_to_dashmaps(ctx, &state_lock);
                state_lock.scheduler_notify.notify_one();
                (files, job_clone)
            };
            let fc = ctx.file_client.clone();
            let ctx_clone = ctx.clone();
            tokio::spawn(async move {
                for f_id in files {
                    let is_referenced = {
                        let state = ctx_clone.state.lock().await;
                        state.jobs.values().any(|j| {
                            (j.status == JobStatus::Pending || j.status == JobStatus::Running)
                                && j.inputs.iter().any(|input| input.file_id == f_id)
                        })
                    };
                    if !is_referenced {
                        let _ = fc.delete_file(&f_id).await;
                    } else {
                        info!("Bypassing deletion of file {} because it is still referenced by other active jobs", f_id);
                    }
                }
            });
            if let Some(job) = job_clone {
                let usage = job_to_usage(&job);
                let store = ctx.accounting_store.clone();
                tokio::spawn(async move {
                    let _ = store.record_job(&usage).await;
                });
            }
        }
        Message::JobKilled { job_id } => {
            ctx.missing_jobs.remove(&job_id);
            let (files, job_clone) = {
                let mut state_lock = ctx.state.lock().await;
                let files = finalize_job(&mut state_lock, job_id, JobStatus::Killed);
                let job_clone = state_lock.jobs.get(&job_id).cloned();
                state_lock.jobs.remove(&job_id);
                ctx.failed_jobs.fetch_add(1, Ordering::Relaxed);
                sync_active_state_to_dashmaps(ctx, &state_lock);
                state_lock.scheduler_notify.notify_one();
                (files, job_clone)
            };
            let fc = ctx.file_client.clone();
            let ctx_clone = ctx.clone();
            tokio::spawn(async move {
                for f_id in files {
                    let is_referenced = {
                        let state = ctx_clone.state.lock().await;
                        state.jobs.values().any(|j| {
                            (j.status == JobStatus::Pending || j.status == JobStatus::Running)
                                && j.inputs.iter().any(|input| input.file_id == f_id)
                        })
                    };
                    if !is_referenced {
                        let _ = fc.delete_file(&f_id).await;
                    } else {
                        info!("Bypassing deletion of file {} because it is still referenced by other active jobs", f_id);
                    }
                }
            });
            if let Some(job) = job_clone {
                let usage = job_to_usage(&job);
                let store = ctx.accounting_store.clone();
                tokio::spawn(async move {
                    let _ = store.record_job(&usage).await;
                });
            }
        }
        Message::StepStarted {
            parent_job_id,
            step_id,
        } => {
            info!(
                "Step {}:{} started on worker {}",
                parent_job_id, step_id, worker_id
            );
            let mut state_lock = ctx.state.lock().await;
            if let Some(step) = state_lock.steps.get_mut(&(parent_job_id, step_id)) {
                step.status = veloce_common::StepStatus::Running;
            }
        }
        Message::StepDone {
            parent_job_id,
            step_id,
            exit_code,
        } => {
            info!(
                "Step {}:{} completed with exit code {} on worker {}",
                parent_job_id, step_id, exit_code, worker_id
            );
            let mut state_lock = ctx.state.lock().await;
            if let Some(step) = state_lock.steps.get_mut(&(parent_job_id, step_id)) {
                step.status = veloce_common::StepStatus::Completed(exit_code);
            }

            if let Some(waiters) = state_lock.step_waiters.remove(&(parent_job_id, step_id)) {
                for waiter in waiters {
                    let _ = waiter.send(exit_code);
                }
            }
            let _ = state_lock
                .step_output_waiters
                .remove(&(parent_job_id, step_id));
        }
        Message::StepError {
            parent_job_id,
            step_id,
            error,
        } => {
            error!(
                "Step {}:{} failed on worker {}: {}",
                parent_job_id, step_id, worker_id, error
            );
            let mut state_lock = ctx.state.lock().await;
            if let Some(step) = state_lock.steps.get_mut(&(parent_job_id, step_id)) {
                step.status = veloce_common::StepStatus::Failed(error.clone());
            }
            if let Some(waiters) = state_lock.step_waiters.remove(&(parent_job_id, step_id)) {
                for waiter in waiters {
                    let _ = waiter.send(-1);
                }
            }
            let _ = state_lock
                .step_output_waiters
                .remove(&(parent_job_id, step_id));
        }
        Message::StepOutput {
            parent_job_id,
            step_id,
            is_stderr,
            data,
        } => {
            let mut state_lock = ctx.state.lock().await;
            if let Some(waiters) = state_lock
                .step_output_waiters
                .get_mut(&(parent_job_id, step_id))
            {
                waiters.retain(|waiter| waiter.send((is_stderr, data.clone())).is_ok());
            }
        }
        Message::StepPMIPut {
            parent_job_id,
            step_id,
            rank,
            key,
            value,
        } => {
            info!(
                "PMI PUT {}:{} rank {} - key: {}, value: {}",
                parent_job_id, step_id, rank, key, value
            );
            let entry = ctx
                .pmi_kvs
                .entry((parent_job_id, step_id))
                .or_insert_with(DashMap::new);
            entry.insert(key, value);
        }
        Message::StepPMIGet {
            parent_job_id,
            step_id,
            rank,
            key,
            request_id,
        } => {
            info!(
                "PMI GET {}:{} rank {} - key: {}",
                parent_job_id, step_id, rank, key
            );
            let value = ctx
                .pmi_kvs
                .get(&(parent_job_id, step_id))
                .and_then(|m| m.get(&key).map(|v| v.value().clone()));

            let state_lock = ctx.state.lock().await;
            if state_lock.workers.contains_key(worker_id) {
                let msg = Message::StepPMIGetResponse {
                    parent_job_id,
                    step_id,
                    rank,
                    request_id,
                    key: key.clone(),
                    value,
                };
                let _ = send_to_worker(&state_lock, worker_id, msg);
            }
        }
        Message::StepPMIBarrierEnter {
            parent_job_id,
            step_id,
            rank,
        } => {
            info!(
                "PMI BARRIER ENTER {}:{} rank {}",
                parent_job_id, step_id, rank
            );

            let step_ntasks = {
                let state_lock = ctx.state.lock().await;
                if let Some(step) = state_lock.steps.get(&(parent_job_id, step_id)) {
                    step.ntasks
                } else {
                    return Ok(());
                }
            };

            let mut is_complete = false;
            {
                let mut barriers = ctx
                    .pmi_barriers
                    .entry((parent_job_id, step_id))
                    .or_insert_with(HashSet::new);
                barriers.insert(rank);
                if barriers.len() as u32 == step_ntasks {
                    is_complete = true;
                    info!(
                        "PMI BARRIER COMPLETE {}:{} ({} ranks)",
                        parent_job_id, step_id, step_ntasks
                    );
                }
            }

            if is_complete {
                ctx.pmi_barriers.remove(&(parent_job_id, step_id));
                let state_lock = ctx.state.lock().await;
                if let Some(step) = state_lock.steps.get(&(parent_job_id, step_id)) {
                    let workers = step.assigned_workers.clone();
                    for w_id in &workers {
                        let _ = send_to_worker(
                            &state_lock,
                            w_id,
                            Message::StepPMIBarrierRelease {
                                parent_job_id,
                                step_id,
                            },
                        );
                    }
                }
            }
        }
        Message::LogData {
            request_id,
            job_id,
            content,
        } => {
            let mut state_lock = ctx.state.lock().await;
            if let Some(tx) = state_lock.log_requests.remove(&request_id) {
                let _ = tx.send(Message::LogData {
                    request_id,
                    job_id,
                    content,
                });
            }
        }
        Message::JobOutputFiles {
            request_id,
            job_id,
            artifacts,
        } => {
            let mut state_lock = ctx.state.lock().await;
            if let Some(tx) = state_lock.log_requests.remove(&request_id) {
                let _ = tx.send(Message::JobOutputFiles {
                    request_id,
                    job_id,
                    artifacts,
                });
            }
        }
        Message::JobOutputFileChunk {
            request_id,
            job_id,
            path,
            content,
            size,
        } => {
            let mut state_lock = ctx.state.lock().await;
            if let Some(tx) = state_lock.log_requests.remove(&request_id) {
                let _ = tx.send(Message::JobOutputFileChunk {
                    request_id,
                    job_id,
                    path,
                    content,
                    size,
                });
            }
        }
        Message::TerminalOutput { session_id, data } => {
            if let Some(ws_tx) = ctx.terminal_sessions.get(&session_id) {
                let _ = ws_tx
                    .send(Message::TerminalOutput { session_id, data })
                    .await;
            }
        }
        Message::TerminalClosed { session_id, reason } => {
            if let Some((_, ws_tx)) = ctx.terminal_sessions.remove(&session_id) {
                let _ = ws_tx
                    .send(Message::TerminalClosed { session_id, reason })
                    .await;
            }
        }
        Message::VncOutput { session_id, data } => {
            if let Some(ws_tx) = ctx.vnc_sessions.get(&session_id) {
                let _ = ws_tx.send(Message::VncOutput { session_id, data }).await;
            }
        }
        Message::VncClosed { session_id, reason } => {
            if let Some((_, ws_tx)) = ctx.vnc_sessions.remove(&session_id) {
                let _ = ws_tx.send(Message::VncClosed { session_id, reason }).await;
            }
        }
        Message::ComponentLogs {
            request_id,
            component_id,
            content,
            ..
        } => {
            let mut state_lock = ctx.state.lock().await;
            if let Some(tx) = state_lock.component_log_requests.remove(&request_id) {
                let hostname = gethostname::gethostname().into_string().unwrap_or_default();
                let _ = tx.send(Message::ComponentLogs {
                    request_id,
                    component_id,
                    hostname,
                    content,
                });
            }
        }
        Message::SystemLogs {
            request_id,
            component_id,
            content,
            ..
        } => {
            let mut state_lock = ctx.state.lock().await;
            if let Some(tx) = state_lock.component_log_requests.remove(&request_id) {
                let hostname = gethostname::gethostname().into_string().unwrap_or_default();
                let _ = tx.send(Message::SystemLogs {
                    request_id,
                    component_id,
                    hostname,
                    content,
                });
            }
        }
        Message::ReportJobUsage(usage) => {
            info!("Received usage report for job {}", usage.job_id);
            let usage_to_record = {
                let mut state_lock = ctx.state.lock().await;
                crate::job_logs::record_worker_log_files(
                    &mut state_lock.multinode_job_tracking,
                    usage.job_id,
                    worker_id,
                    usage.stdout_file_id.clone(),
                    usage.stderr_file_id.clone(),
                );
                let tracked_logs = state_lock
                    .multinode_job_tracking
                    .worker_log_files
                    .get(&usage.job_id)
                    .cloned()
                    .unwrap_or_default();
                let multinode = state_lock
                    .jobs
                    .get(&usage.job_id)
                    .map(|job| {
                        crate::job_logs::expected_done_workers(
                            &state_lock.multinode_job_tracking,
                            usage.job_id,
                            job,
                        ) > 1
                    })
                    .unwrap_or(false);
                if let Some(job) = state_lock.jobs.get_mut(&usage.job_id) {
                    job.cgroup_active = usage.cgroup_active;
                    if !multinode {
                        job.stdout_file_id = usage.stdout_file_id.clone();
                        job.stderr_file_id = usage.stderr_file_id.clone();
                    }
                    job.workdir_file_id = usage.workdir_file_id.clone();
                    job.output_artifacts = usage.output_artifacts.clone();
                    if !tracked_logs.is_empty() {
                        job.worker_log_files =
                            crate::job_logs::tracking_to_worker_log_files(&tracked_logs);
                    }
                    let mut merged = merge_usage_report_with_job(usage, job);
                    if !tracked_logs.is_empty() {
                        merged.worker_log_files =
                            crate::job_logs::tracking_to_worker_log_files(&tracked_logs);
                    }
                    merged
                } else {
                    let mut usage = usage;
                    if !tracked_logs.is_empty() {
                        usage.worker_log_files =
                            crate::job_logs::tracking_to_worker_log_files(&tracked_logs);
                    }
                    usage
                }
            };
            let store = ctx.accounting_store.clone();
            tokio::spawn(async move {
                if let Err(e) = store.record_job(&usage_to_record).await {
                    error!("Failed to record job usage: {}", e);
                }
            });
        }
        Message::ReportMetrics(metrics) => {
            let mut state_lock = ctx.state.lock().await;
            if let Some(worker) = state_lock.workers.get_mut(worker_id) {
                worker.cgroup_enabled = metrics.cgroup_enabled;
            }
            ctx.metrics_store
                .insert(worker_id.to_string(), metrics.clone());
            metrics_store::append_metrics(&metrics);
        }
        Message::Pulse(metrics) => {
            let mut state_lock = ctx.state.lock().await;
            if let Some(worker) = state_lock.workers.get_mut(worker_id) {
                worker.cgroup_enabled = metrics.cgroup_enabled;

                let current_cpu = metrics.cpu_load;
                let target_interval = if current_cpu > 50.0 { 0.5f32 } else { 2.0f32 };

                if target_interval != worker.current_pulse_interval {
                    worker.current_pulse_interval = target_interval;
                    let worker_id_str = worker_id.to_string();
                    let ctx_clone = ctx.clone();
                    tokio::spawn(async move {
                        let state = ctx_clone.state.lock().await;
                        let _ = send_to_worker(
                            &state,
                            &worker_id_str,
                            Message::SetPulseInterval {
                                seconds: target_interval,
                            },
                        );
                    });
                }
            }
            ctx.metrics_store
                .insert(worker_id.to_string(), metrics.clone());
            metrics_store::append_metrics(&metrics);
        }
        Message::LogStream {
            labels,
            line,
            timestamp,
        } => {
            let _ = ctx.loki_tx.send((labels, line, timestamp)).await;
        }
        _ => warn!("Unexpected message from worker {}: {:?}", worker_id, msg),
    }
    Ok(())
}

pub fn send_to_worker(
    state: &GlobalState,
    worker_id: &str,
    msg: Message,
) -> Result<(), anyhow::Error> {
    if let Some(worker) = state.workers.get(worker_id) {
        match &worker.routing {
            WorkerRouting::Direct(tx) => {
                if let Err(err) = tx.try_send(msg) {
                    match err {
                        tokio::sync::mpsc::error::TrySendError::Full(msg) => {
                            let tx = tx.clone();
                            tokio::spawn(async move {
                                let _ = tx.send(msg).await;
                            });
                        }
                        tokio::sync::mpsc::error::TrySendError::Closed(_) => {
                            anyhow::bail!("Worker channel closed");
                        }
                    }
                }
                return Ok(());
            }
            WorkerRouting::Gateway(peer_id) => {
                if let Some(tx) = state.peers.get(peer_id) {
                    let wrapped_msg = PeerMessage::ForwardToWorker {
                        worker_id: worker_id.to_string(),
                        msg: Box::new(msg),
                    };
                    if let Err(err) = tx.try_send(wrapped_msg) {
                        match err {
                            tokio::sync::mpsc::error::TrySendError::Full(wrapped_msg) => {
                                let tx = tx.clone();
                                tokio::spawn(async move {
                                    let _ = tx.send(wrapped_msg).await;
                                });
                            }
                            tokio::sync::mpsc::error::TrySendError::Closed(_) => {
                                anyhow::bail!("Peer channel closed");
                            }
                        }
                    }
                    return Ok(());
                }
            }
        }
    }
    anyhow::bail!("Worker {} not found or routing path unavailable", worker_id)
}
