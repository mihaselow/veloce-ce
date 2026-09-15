//! HA peer controller message handling.

use crate::{
    handlers::worker::handle_worker_message,
    persistence::save_state,
    state::{
        rebuild_worker_allocations, reconcile_missing_worker_jobs, sync_active_state_to_dashmaps,
        ControllerRole, SharedContext, WorkerHandle, WorkerRouting,
    },
};
use anyhow::Result;
use futures::{SinkExt, StreamExt};
use std::collections::HashSet;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tokio_util::codec::Framed;
use tracing::{debug, error, info};
use veloce_common::{Message, MessageCodec, PeerMessage, PersistedState};

pub async fn handle_peer<S>(
    mut framed: Framed<S, MessageCodec>,
    peer_addr_str: String,
    ctx: SharedContext,
    controller_id: String,
) -> Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    info!(
        "Peer controller connected: {} at {}",
        controller_id, peer_addr_str
    );

    // Send our HelloPeer back to complete the handshake
    let my_id = std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown".into());
    let peer_token = std::env::var("VELOCE_PEER_TOKEN").ok();
    framed
        .send(Message::HelloPeer {
            controller_id: my_id,
            features: None,
            registration_token: peer_token,
        })
        .await?;

    // For now, we only support 1 peer.
    // If we are follower, we listen for updates.
    // If we are leader, we might have initiated this or they connected to us.

    let (tx, mut rx) = mpsc::channel(100);
    {
        let mut state = ctx.state.lock().await;
        state.peers.insert(controller_id.clone(), tx.clone());

        if state.role == ControllerRole::Follower {
            info!("Sending worker registrations to peer {}", controller_id);
            for (id, worker) in &state.workers {
                if let WorkerRouting::Direct(_) = &worker.routing {
                    let _ = tx.try_send(PeerMessage::RegisterWorker {
                        worker_id: id.clone(),
                        hostname: worker.hostname.clone(),
                        resources: worker.resources.clone(),
                        features: Some(worker.features),
                        ip_address: Some(worker.addr.ip().to_string()),
                    });
                }
            }
        }

        if state.role == ControllerRole::Leader {
            info!(
                "Sending initial state sync to newly connected peer: {}",
                controller_id
            );
            let current_state = PersistedState {
                jobs: state.jobs.clone(),
                queue: state.queue.clone(),
                next_job_id: state.next_job_id,
                usage_tracker: state.usage_tracker.clone(),
                steps: state.steps.clone(),
                next_step_id: state.next_step_id,
                reservations: state.reservations.clone(),
            };
            let _ = tx.try_send(PeerMessage::StateSync(current_state));
        }
    }

    struct PeerCleanup {
        ctx: SharedContext,
        controller_id: String,
        tx: mpsc::Sender<PeerMessage>,
    }
    impl Drop for PeerCleanup {
        fn drop(&mut self) {
            let ctx = self.ctx.clone();
            let controller_id = self.controller_id.clone();
            let tx = self.tx.clone();
            tokio::spawn(async move {
                let mut state = ctx.state.lock().await;
                if let Some(existing_tx) = state.peers.get(&controller_id) {
                    if existing_tx.same_channel(&tx) {
                        state.peers.remove(&controller_id);
                    }
                }
            });
        }
    }
    let _cleanup = PeerCleanup {
        ctx: ctx.clone(),
        controller_id: controller_id.clone(),
        tx: tx.clone(),
    };

    let mut heartbeat_interval = tokio::time::interval(Duration::from_secs(2));
    let start_time = std::time::Instant::now();
    let startup_grace = Duration::from_secs(15);

    loop {
        tokio::select! {
            msg_opt = framed.next() => {
                let msg = match msg_opt {
                    Some(res) => res?,
                    None => {
                        info!("Peer connection to {} closed (EOF)", peer_addr_str);
                        return Ok(());
                    }
                };
                match msg {
                    Message::PeerAction(PeerMessage::Heartbeat { term: _, leader_id }) => {
                        ctx.last_heartbeat.store(
                            std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_secs(),
                            std::sync::atomic::Ordering::Relaxed
                        );
                        let mut state = ctx.state.lock().await;

                        let my_id = std::env::var("HOSTNAME").unwrap_or_default();

                        if leader_id != my_id {
                            let host = peer_addr_str.split(':').next().unwrap_or("localhost");
                            state.leader_addr = Some(format!("{}:9000", host));
                            let leader_changed = state.leader_id.as_ref() != Some(&leader_id);
                            state.leader_id = Some(leader_id.clone());
                            if state.role == ControllerRole::Leader {
                                if leader_id < my_id {
                                    info!("Yielding leadership to lower ID peer: {}", leader_id);
                                    state.role = ControllerRole::Follower;

                                    let mut direct_workers = Vec::new();
                                    state.workers.retain(|id, worker| {
                                        if let WorkerRouting::Direct(_) = &worker.routing {
                                            direct_workers.push((id.clone(), worker.hostname.clone(), worker.resources.clone(), worker.features, worker.addr.ip().to_string()));
                                            true
                                        } else {
                                            false
                                        }
                                    });

                                    for (worker_id, hostname, resources, features, ip) in direct_workers {
                                        let _ = tx.try_send(PeerMessage::RegisterWorker {
                                            worker_id,
                                            hostname,
                                            resources,
                                            features: Some(features),
                                            ip_address: Some(ip),
                                        });
                                    }
                                } else {
                                    debug!("Ignoring heartbeat from higher ID peer: {}", leader_id);
                                }
                            } else {
                                state.role = ControllerRole::Follower;
                                state.workers.retain(|_, worker| matches!(worker.routing, WorkerRouting::Direct(_)));
                                if leader_changed {
                                    if let Some(peer_tx) = state.peers.get(&leader_id).cloned() {
                                        info!("Leader changed or initialized to {}. Registering direct workers.", leader_id);
                                        for (id, worker) in &state.workers {
                                            if let WorkerRouting::Direct(_) = &worker.routing {
                                                let _ = peer_tx.try_send(PeerMessage::RegisterWorker {
                                                    worker_id: id.clone(),
                                                    hostname: worker.hostname.clone(),
                                                    resources: worker.resources.clone(),
                                                    features: Some(worker.features),
                                                    ip_address: Some(worker.addr.ip().to_string()),
                                                });
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Message::PeerAction(PeerMessage::StateSync(new_state)) => {
                        ctx.last_heartbeat.store(
                            std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_secs(),
                            std::sync::atomic::Ordering::Relaxed
                        );
                        info!("Received full state sync from leader");
                        let mut state = ctx.state.lock().await;
                        state.jobs = new_state.jobs;
                        state.queue = new_state.queue;
                        state.next_job_id = new_state.next_job_id;
                        state.usage_tracker = new_state.usage_tracker;
                        state.steps = new_state.steps;
                        state.next_step_id = new_state.next_step_id;
                        state.reservations = new_state.reservations;
                        sync_active_state_to_dashmaps(&ctx, &state);
                        save_state(&state);
                        state.scheduler_notify.notify_one();
                    }
                    Message::PeerAction(PeerMessage::RegisterWorker { worker_id, hostname, resources, features, ip_address }) => {
                        let mut state = ctx.state.lock().await;
                        if state.role == ControllerRole::Leader {
                            info!("Registering worker {} via gateway peer {}", worker_id, controller_id);
                            let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();

                            let worker_ip = ip_address.clone().unwrap_or_else(|| peer_addr_str.split(':').next().unwrap_or("127.0.0.1").to_string());
                            let worker_port = peer_addr_str.split(':').nth(1).unwrap_or("9000");
                            let parsed_addr = format!("{}:{}", worker_ip, worker_port).parse().unwrap_or_else(|_| "127.0.0.1:9000".parse().unwrap());
                            let rebuilt_allocations = rebuild_worker_allocations(&worker_id, &resources, &state.jobs);

                            if let Some(w) = state.workers.get_mut(&worker_id) {
                                let current_pulse_interval = w.current_pulse_interval;
                                let connection_id = w.connection_id;
                                w.addr = parsed_addr;
                                w.hostname = hostname;
                                w.resources = resources.clone();
                                w.routing = WorkerRouting::Gateway(controller_id.clone());
                                w.assigned_jobs = rebuilt_allocations.assigned_jobs;
                                w.allocated_memory = rebuilt_allocations.allocated_memory;
                                w.connection_id = connection_id;
                                w.available_core_ids = rebuilt_allocations.available_core_ids;
                                w.job_core_assignments = rebuilt_allocations.job_core_assignments;
                                w.available_gres_ids = rebuilt_allocations.available_gres_ids;
                                w.job_gres_assignments = rebuilt_allocations.job_gres_assignments;
                                w.last_seen = now;
                                w.connected = true;
                                w.cgroup_enabled = resources.cgroup_enabled;
                                w.draining = false;
                                w.current_pulse_interval = current_pulse_interval;
                                w.features = features.unwrap_or(0);
                            } else {
                                state.workers.insert(worker_id.clone(), WorkerHandle {
                                    addr: parsed_addr,
                                    hostname,
                                    resources: resources.clone(),
                                    routing: WorkerRouting::Gateway(controller_id.clone()),
                                    assigned_jobs: rebuilt_allocations.assigned_jobs,
                                    allocated_memory: rebuilt_allocations.allocated_memory,
                                    connection_id: 0,
                                    available_core_ids: rebuilt_allocations.available_core_ids,
                                    job_core_assignments: rebuilt_allocations.job_core_assignments,
                                    available_gres_ids: rebuilt_allocations.available_gres_ids,
                                    job_gres_assignments: rebuilt_allocations.job_gres_assignments,
                                    connected: true,
                                    last_seen: now,
                                    cgroup_enabled: resources.cgroup_enabled,
                                    draining: false,
                                    current_pulse_interval: 2.0,
                                    features: features.unwrap_or(0),
                                });
                            }
                            state.scheduler_notify.notify_one();
                        }
                    }
                    Message::PeerAction(PeerMessage::DeregisterWorker { worker_id }) => {
                        let mut state = ctx.state.lock().await;
                        if state.role == ControllerRole::Leader {
                            info!("Deregistering worker {} via gateway peer {}", worker_id, controller_id);
                            state.workers.remove(&worker_id);
                            state.scheduler_notify.notify_one();
                        }
                    }
                    Message::PeerAction(PeerMessage::ForwardFromWorker { worker_id, msg }) => {
                        let is_leader = { ctx.state.lock().await.role == ControllerRole::Leader };
                        if is_leader {
                            if let Err(e) = handle_worker_message(&ctx, &worker_id, *msg).await {
                                error!("Error handling forwarded worker message: {}", e);
                            }
                        }
                    }
                    Message::PeerAction(PeerMessage::ForwardToWorker { worker_id, msg }) => {
                        let state = ctx.state.lock().await;
                        if let Some(worker) = state.workers.get(&worker_id) {
                            if let WorkerRouting::Direct(worker_tx) = &worker.routing {
                                let _ = worker_tx.try_send(*msg);
                            }
                        }
                    }
                    Message::PeerAction(PeerMessage::BatchWorkerHeartbeats { updates }) => {
                        let mut state = ctx.state.lock().await;
                        if state.role == ControllerRole::Leader {
                            let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
                            for (worker_id, resources, job_stats) in updates {
                                if state.workers.contains_key(&worker_id) {
                                    if let Some(worker) = state.workers.get_mut(&worker_id) {
                                        worker.resources = resources;
                                        worker.last_seen = now;
                                    }

                                    let active_job_ids: HashSet<u64> = job_stats.iter().map(|s| s.job_id).collect();
                                    reconcile_missing_worker_jobs(&ctx.missing_jobs, &mut state, &worker_id, &active_job_ids, now);

                                    for stats in job_stats {
                                        {
                                            let mut q = ctx.job_metrics_history.entry(stats.job_id).or_insert_with(std::collections::VecDeque::new);
                                            if q.len() >= 300 {
                                                q.pop_front();
                                            }
                                            q.push_back(stats.clone());
                                        }

                                        if let Some(job) = state.jobs.get_mut(&stats.job_id) {
                                            job.current_cpu_usage = stats.cpu_usage_percent;
                                            job.current_memory_usage = stats.memory_usage_bytes;
                                            job.is_idle = stats.is_idle;
                                            job.idle_duration = stats.idle_duration;
                                            job.cgroup_active = stats.cgroup_active;
                                        }
                                    }
                                }
                            }
                            state.scheduler_notify.notify_one();
                        }
                    }
                    Message::PeerAction(PeerMessage::Restart { delay_ms, reason }) => {
                        info!("Received peer restart request from leader. Reason: {}. Delay: {}ms", reason, delay_ms);
                        tokio::spawn(async move {
                            tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                            veloce_common::utils::self_restart();
                        });
                    }
                    Message::PeerAction(PeerMessage::GetLogs { request_id, msg }) => {
                        let tx_clone = tx.clone();
                        tokio::spawn(async move {
                            let response = match *msg {
                                Message::GetComponentLogs { request_id, component_id: _, lines } => {
                                    let content = match std::fs::read_to_string("veloce-controller.log") {
                                        Ok(c) => c.lines().rev().take(lines).collect::<Vec<_>>().into_iter().rev().collect::<Vec<_>>().join("\n"),
                                        Err(_) => "Log file not found".to_string(),
                                    };
                                    let hostname = gethostname::gethostname().into_string().unwrap_or_default();
                                    Message::ComponentLogs { request_id, component_id: hostname.clone(), hostname, content }
                                }
                                Message::GetSystemLogs { request_id, component_id: _, log_source, lines } => {
                                    let content = match log_source.as_str() {
                                        "dmesg" => match std::process::Command::new("dmesg").arg("-T").output() {
                                            Ok(o) if o.status.success() => String::from_utf8_lossy(&o.stdout).lines().rev().take(lines).collect::<Vec<_>>().into_iter().rev().collect::<Vec<_>>().join("\n"),
                                            Ok(o) => format!("dmesg failed ({}): {}", o.status, String::from_utf8_lossy(&o.stderr)),
                                            Err(e) => format!("Failed to run dmesg: {}", e),
                                        },
                                        "syslog" => match std::process::Command::new("tail").args(&["-n", &lines.to_string(), "/var/log/syslog"]).output() {
                                            Ok(o) if o.status.success() => String::from_utf8_lossy(&o.stdout).to_string(),
                                            Ok(o) => format!("Failed to read syslog ({}): {}", o.status, String::from_utf8_lossy(&o.stderr)),
                                            Err(e) => format!("Failed to read syslog: {}", e),
                                        },
                                        "journal" => match std::process::Command::new("journalctl").args(&["-n", &lines.to_string(), "--no-pager"]).output() {
                                            Ok(o) if o.status.success() => String::from_utf8_lossy(&o.stdout).to_string(),
                                            Ok(o) => format!("Failed to read journal ({}): {}", o.status, String::from_utf8_lossy(&o.stderr)),
                                            Err(e) => format!("Failed to read journal: {}", e),
                                        },
                                        _ => format!("Unsupported log source: {}", log_source),
                                    };
                                    let hostname = gethostname::gethostname().into_string().unwrap_or_default();
                                    Message::SystemLogs { request_id, component_id: hostname.clone(), hostname, content }
                                }
                                _ => Message::Error("Unsupported message type forwarded via GetLogs".into()),
                            };
                            let _ = tx_clone.try_send(PeerMessage::LogsResponse { request_id, msg: Box::new(response) });
                        });
                    }
                    Message::PeerAction(PeerMessage::LogsResponse { request_id, msg }) => {
                        let mut state = ctx.state.lock().await;
                        if let Some(tx) = state.component_log_requests.remove(&request_id) {
                            let _ = tx.send(*msg);
                        }
                    }
                    _ => {}
                }
            }
            Some(out_msg) = rx.recv() => {
                framed.send(Message::PeerAction(out_msg)).await?;
            }
            _ = heartbeat_interval.tick() => {
                let role = {
                    let state = ctx.state.lock().await;
                    state.role
                };

                let last_hb_sec = ctx.last_heartbeat.load(std::sync::atomic::Ordering::Relaxed);
                let now_sec = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                if start_time.elapsed() > startup_grace && now_sec.saturating_sub(last_hb_sec) > 5 && role == ControllerRole::Follower {
                    info!("No heartbeat for 5s (and grace period passed). Promoting to Leader.");
                    let mut state = ctx.state.lock().await;
                    state.role = ControllerRole::Leader;

                    let current_state = PersistedState {
                        jobs: state.jobs.clone(),
                        queue: state.queue.clone(),
                        next_job_id: state.next_job_id,
                        usage_tracker: state.usage_tracker.clone(),
                        steps: state.steps.clone(),
                        next_step_id: state.next_step_id,
                        reservations: state.reservations.clone(),
                    };

                    for (peer_id, peer_tx) in &state.peers {
                        info!("Sending state sync to peer {}", peer_id);
                        let _ = peer_tx.try_send(PeerMessage::StateSync(current_state.clone()));
                    }
                }

                if role == ControllerRole::Leader {
                    let my_id = std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown".into());
                    let msg = PeerMessage::Heartbeat {
                        term: 1,
                        leader_id: my_id
                    };
                    let _ = framed.send(Message::PeerAction(msg)).await;
                }
            }
        }
    }
}
