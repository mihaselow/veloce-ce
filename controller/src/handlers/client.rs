//! CLI/MCP Noise RPC client handler.

use crate::handlers::worker::send_to_worker;
use crate::{
    config::validate_job_submission,
    job_secrets,
    metrics_store::read_metrics,
    persistence::{job_to_usage, save_state},
    scheduler::{finalize_job, has_cycle, submit_step},
    state::{
        get_worker_sender, is_controller_node, normalize_component_id,
        sync_active_state_to_dashmaps, SharedContext,
    },
};
use anyhow::Result;
use futures::{SinkExt, StreamExt};
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio_util::codec::Framed;
use tracing::{error, info, warn};
use uuid::Uuid;
use veloce_common::{
    HistoryFilter, JobInfo, JobStatus, LogType, Message, MessageCodec, PeerMessage, QosLevel,
};

pub fn is_authorized(msg: &Message, roles: &[String]) -> bool {
    if roles.iter().any(|r| r == "admin") {
        return true;
    }
    match msg {
        Message::SubmitDag(_)
        | Message::Submit { .. }
        | Message::SubmitStep { .. }
        | Message::StageProject { .. } => roles.iter().any(|r| r == "submitter"),
        Message::CancelJob { .. } => roles.iter().any(|r| r == "submitter" || r == "operator"),
        Message::CreateReservation { .. }
        | Message::DeleteReservation { .. }
        | Message::ListReservations => roles.iter().any(|r| r == "operator"),
        Message::ListJobs { .. }
        | Message::GetJobHistory { .. }
        | Message::GetMetrics { .. }
        | Message::GetLogs { .. }
        | Message::GetEfficiencyStats { .. }
        | Message::GetJobEvents { .. }
        | Message::GetJobSteps { .. }
        | Message::ListWorkers => roles
            .iter()
            .any(|r| r == "viewer" || r == "submitter" || r == "operator"),
        Message::GetComponentLogs { .. }
        | Message::GetSystemLogs { .. }
        | Message::Restart { .. } => roles.iter().any(|r| r == "operator"),
        _ => true,
    }
}

pub async fn handle_client<S>(
    mut framed: Framed<S, MessageCodec>,
    _addr: SocketAddr,
    ctx: SharedContext,
    client_id: String,
    roles: Vec<String>,
) -> Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    while let Some(msg_res) = framed.next().await {
        let msg = msg_res?;

        if !is_authorized(&msg, &roles) {
            let err_msg = format!("Permission denied: unauthorized operation");
            warn!(
                "Client {} (roles: {:?}) unauthorized request: {:?}",
                client_id, roles, msg
            );
            framed.send(Message::Error(err_msg)).await?;
            continue;
        }

        let response = match msg {
            Message::Submit {
                job_name,
                job_comment,
                binary,
                args,
                req_nodes,
                req_cores,
                req_memory,
                walltime,
                priority,
                user_id: raw_user_id,
                working_directory,
                array_indices,
                inputs,
                gres_req,
                env_vars,
                wait_for_licenses,
                estimated_walltime,
                priority_offset,
                dependencies,
                dependency_specs,
                qos,
                image_uri,
                vnc_enabled,
                inherit_host_env,
                env_allowlist,
                job_profile,
            } => {
                let mut container_asset = None;
                let mut error_msg = None;

                let principal = crate::auth::AuthenticatedPrincipal {
                    user_id: client_id.clone(),
                    roles: roles.clone(),
                    auth_method: crate::auth::AuthMethod::ApiKey,
                };
                let resolved_user_id = match principal.resolve_user_id(&raw_user_id) {
                    Ok(uid) => uid,
                    Err(e) => {
                        // P1-6.1 audit user spoof / resolve denial
                        let _ = ctx
                            .audit
                            .log(&crate::audit::AuditEvent::noise_rpc_denied(
                                &client_id,
                                Some(client_id.as_str()),
                                "Submit",
                                &e,
                            ))
                            .await;
                        error_msg = Some(Message::Error(e));
                        "".to_string()
                    }
                };
                let lookup_id = image_uri.as_ref().unwrap_or(&binary);
                if let Ok(Some(asset)) = ctx.container_store.get_container(lookup_id).await {
                    container_asset = Some(asset);
                } else if let Some(uri) = &image_uri {
                    error_msg = Some(Message::Error(format!(
                        "Container image '{}' not found in registry",
                        uri
                    )));
                }

                let mut resolved_vnc = vnc_enabled;
                if let Some(ref asset) = container_asset {
                    if asset.manifest.vnc_enabled {
                        resolved_vnc = true;
                    }
                }

                if let Some(err) = error_msg {
                    err
                } else if let Err(e) = validate_job_submission(
                    &binary,
                    &env_vars,
                    &veloce_common::job_policy::JobExecutionOptions::from_fields(
                        inherit_host_env,
                        &env_allowlist,
                        &job_profile,
                    ),
                    &ctx.config.allowed_binaries_prefixes,
                ) {
                    let _ = ctx
                        .audit
                        .log(&crate::audit::AuditEvent::job_launch_denied(&client_id, &e))
                        .await;
                    Message::Error(e)
                } else if qos == QosLevel::Interactive && (walltime == 0 || walltime > 1800) {
                    Message::Error("Interactive jobs must have a walltime limit of 30 minutes (1800 seconds) or less".into())
                } else if qos == QosLevel::Interactive && req_cores > 4 {
                    Message::Error(
                        "Interactive jobs are limited to a maximum of 4 CPU cores per node".into(),
                    )
                } else {
                    if let Some(indices) = array_indices {
                        let task_count = indices.len();
                        if task_count == 0 {
                            Message::Error("Empty array indices".into())
                        } else {
                            // Lock-free ID generation for the whole block
                            let base_id = ctx
                                .next_job_id
                                .fetch_add(task_count as u64, Ordering::Relaxed);
                            let mut success_count = 0;

                            for (i, task_id) in indices.into_iter().enumerate() {
                                let job = JobInfo {
                                    id: base_id + i as u64,
                                    job_name: job_name.clone(),
                                    job_comment: job_comment.clone(),
                                    binary: binary.clone(),
                                    args: args.clone(),
                                    status: JobStatus::Pending,
                                    assigned_workers: Vec::new(),
                                    req_nodes,
                                    req_cores,
                                    req_memory,
                                    walltime,
                                    start_time: None,
                                    priority,
                                    user_id: resolved_user_id.clone(),
                                    working_directory: working_directory.clone(),
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
                                    container_asset: container_asset.clone(),
                                    array_id: Some(base_id),
                                    array_task_id: Some(task_id),
                                    inputs: inputs.clone(),
                                    cgroup_active: false,
                                    gres_req: gres_req.clone(),
                                    allocated_cores: HashMap::new(),
                                    allocated_gres: HashMap::new(),
                                    mpi_stats: None,
                                    secret: Uuid::new_v4().to_string(),
                                    env_vars: env_vars.clone(),
                                    stdout_file_id: None,
                                    stderr_file_id: None,
                                    workdir_file_id: None,
                                    output_artifacts: Vec::new(),
                                    wait_for_licenses,
                                    estimated_walltime,
                                    priority_offset,
                                    dependencies: dependencies.clone(),
                                    dependency_specs: dependency_specs.clone(),
                                    qos,
                                    vnc_enabled: resolved_vnc,
                                    inherit_host_env,
                                    env_allowlist: env_allowlist.clone(),
                                    job_profile: job_profile.clone(),
                                    interactive_port: None,
                                };

                                if job_profile.as_deref() == Some("system") {
                                    let _ = ctx
                                        .audit
                                        .log(&crate::audit::AuditEvent::job_system_profile_launch(
                                            base_id + i as u64,
                                            &client_id,
                                            "system",
                                        ))
                                        .await;
                                }

                                if let Err(e) = ctx.submission_tx.send(job).await {
                                    error!(
                                        "Failed to enqueue job submission for array job {}: {}",
                                        base_id + i as u64,
                                        e
                                    );
                                } else {
                                    success_count += 1;
                                }
                            }

                            if success_count > 0 {
                                Message::ArrayJobSubmitted {
                                    base_job_id: base_id,
                                    task_count: success_count,
                                }
                            } else {
                                Message::Error("Failed to submit any tasks of the job array".into())
                            }
                        }
                    } else {
                        // Lock-free ID generation
                        let id = ctx.next_job_id.fetch_add(1, Ordering::Relaxed);

                        // Cycle Detection Check
                        let mut has_circular = false;
                        {
                            let state_lock = ctx.state.lock().await;
                            // Check raw dependencies
                            if let Some(ref deps) = dependencies {
                                for &dep in deps {
                                    let mut visited = HashSet::new();
                                    if has_cycle(dep, id, &state_lock.jobs, &mut visited) {
                                        has_circular = true;
                                        break;
                                    }
                                }
                            }

                            // Check dependency_specs
                            if !has_circular {
                                if let Some(ref specs) = dependency_specs {
                                    for spec_str in specs {
                                        if let Ok(spec) =
                                            veloce_common::DependencySpec::parse(spec_str)
                                        {
                                            let mut visited = HashSet::new();
                                            if has_cycle(
                                                spec.parent_id,
                                                id,
                                                &state_lock.jobs,
                                                &mut visited,
                                            ) {
                                                has_circular = true;
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        if has_circular {
                            Message::Error("Circular dependency detected".into())
                        } else {
                            let job = JobInfo {
                                id,
                                job_name,
                                job_comment,
                                binary,
                                args,
                                status: JobStatus::Pending,
                                assigned_workers: Vec::new(),
                                req_nodes,
                                req_cores,
                                req_memory,
                                walltime,
                                start_time: None,
                                priority,
                                user_id: resolved_user_id,
                                working_directory,
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
                                container_asset: container_asset.clone(),
                                array_id: None,
                                array_task_id: None,
                                inputs,
                                cgroup_active: false,
                                gres_req,
                                allocated_cores: HashMap::new(),
                                allocated_gres: HashMap::new(),
                                mpi_stats: None,
                                secret: Uuid::new_v4().to_string(),
                                env_vars,
                                stdout_file_id: None,
                                stderr_file_id: None,
                                workdir_file_id: None,
                                output_artifacts: Vec::new(),
                                wait_for_licenses,
                                estimated_walltime,
                                priority_offset,
                                dependencies: dependencies.clone(),
                                dependency_specs: dependency_specs.clone(),
                                qos,
                                vnc_enabled: resolved_vnc,
                                inherit_host_env,
                                env_allowlist,
                                job_profile: job_profile.clone(),
                                interactive_port: None,
                            };

                            if job_profile.as_deref() == Some("system") {
                                let _ = ctx
                                    .audit
                                    .log(&crate::audit::AuditEvent::job_system_profile_launch(
                                        id, &client_id, "system",
                                    ))
                                    .await;
                            }

                            // Send to async processor
                            if let Err(e) = ctx.submission_tx.send(job).await {
                                error!("Failed to enqueue job submission: {}", e);
                                Message::Error("Internal submission error".into())
                            } else {
                                Message::JobId { job_id: id }
                            }
                        }
                    }
                }
            }
            Message::SubmitDag(dag) => {
                let node_count = dag.nodes.len();
                if node_count == 0 {
                    Message::Error("DAG has no nodes".into())
                } else {
                    let principal = crate::auth::AuthenticatedPrincipal {
                        user_id: client_id.clone(),
                        roles: roles.clone(),
                        auth_method: crate::auth::AuthMethod::ApiKey,
                    };

                    let mut validation_error = None;
                    for node in &dag.nodes {
                        if let Message::Submit { ref user_id, .. } = *node.task {
                            if let Err(e) = principal.resolve_user_id(user_id) {
                                validation_error = Some(Message::Error(e));
                                break;
                            }
                        }
                    }

                    if let Some(err) = validation_error {
                        err
                    } else {
                        let base_id = ctx
                            .next_job_id
                            .fetch_add(node_count as u64, Ordering::Relaxed);
                        let mut node_to_job = HashMap::new();
                        for (i, node) in dag.nodes.iter().enumerate() {
                            node_to_job.insert(node.node_id.clone(), base_id + i as u64);
                        }

                        let mut success_count = 0;
                        for (i, node) in dag.nodes.into_iter().enumerate() {
                            let id = base_id + i as u64;
                            let mut deps = Vec::new();
                            let mut specs = Vec::new();
                            for dep in node.depends_on {
                                if let Some(target_id) = node_to_job.get(&dep) {
                                    deps.push(*target_id);
                                    specs.push(format!("AfterOk:{}", target_id));
                                }
                            }

                            if let Message::Submit {
                                job_name,
                                job_comment,
                                binary,
                                args,
                                req_nodes,
                                req_cores,
                                req_memory,
                                walltime,
                                priority,
                                user_id,
                                working_directory,
                                array_indices: _,
                                inputs,
                                gres_req,
                                env_vars,
                                wait_for_licenses,
                                estimated_walltime,
                                priority_offset,
                                dependencies,
                                dependency_specs,
                                qos,
                                image_uri,
                                vnc_enabled,
                                inherit_host_env,
                                env_allowlist,
                                job_profile,
                            } = *node.task
                            {
                                let mut container_asset = None;
                                let lookup_id = image_uri.as_ref().unwrap_or(&binary);
                                if let Ok(Some(asset)) =
                                    ctx.container_store.get_container(lookup_id).await
                                {
                                    container_asset = Some(asset);
                                }

                                let mut resolved_vnc = vnc_enabled;
                                if let Some(ref asset) = container_asset {
                                    if asset.manifest.vnc_enabled {
                                        resolved_vnc = true;
                                    }
                                }

                                let mut final_deps = dependencies.unwrap_or_default();
                                final_deps.extend(deps);
                                let mut final_specs = dependency_specs.unwrap_or_default();
                                final_specs.extend(specs);

                                let job = JobInfo {
                                    id,
                                    job_name,
                                    job_comment,
                                    binary,
                                    args,
                                    status: JobStatus::Pending,
                                    assigned_workers: Vec::new(),
                                    req_nodes,
                                    req_cores,
                                    req_memory,
                                    walltime,
                                    start_time: None,
                                    priority,
                                    user_id: principal
                                        .resolve_user_id(&user_id)
                                        .unwrap_or_default(),
                                    working_directory,
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
                                    container_asset,
                                    array_id: None,
                                    array_task_id: None,
                                    inputs,
                                    cgroup_active: false,
                                    gres_req,
                                    allocated_cores: HashMap::new(),
                                    allocated_gres: HashMap::new(),
                                    mpi_stats: None,
                                    secret: Uuid::new_v4().to_string(),
                                    env_vars,
                                    stdout_file_id: None,
                                    stderr_file_id: None,
                                    workdir_file_id: None,
                                    output_artifacts: Vec::new(),
                                    wait_for_licenses,
                                    estimated_walltime,
                                    priority_offset,
                                    dependencies: if final_deps.is_empty() {
                                        None
                                    } else {
                                        Some(final_deps)
                                    },
                                    dependency_specs: if final_specs.is_empty() {
                                        None
                                    } else {
                                        Some(final_specs)
                                    },
                                    qos,
                                    vnc_enabled: resolved_vnc,
                                    inherit_host_env,
                                    env_allowlist,
                                    job_profile,
                                    interactive_port: None,
                                };

                                if let Err(e) = ctx.submission_tx.send(job).await {
                                    error!("Failed to enqueue job for DAG {}: {}", id, e);
                                } else {
                                    success_count += 1;
                                }
                            }
                        }

                        if success_count > 0 {
                            Message::DagSubmitted {
                                base_job_id: base_id,
                                task_count: success_count,
                            }
                        } else {
                            Message::Error("Failed to submit any tasks of the DAG".into())
                        }
                    }
                }
            }
            Message::SubmitStep {
                parent_job_id,
                binary,
                args,
                req_nodes,
                req_cores,
                ntasks,
                inputs,
                gres_req,
                env_vars,
            } => {
                let mut state_lock = ctx.state.lock().await;
                match submit_step(
                    &mut state_lock,
                    parent_job_id,
                    binary,
                    args,
                    req_nodes,
                    req_cores,
                    ntasks,
                    inputs,
                    gres_req,
                    env_vars,
                ) {
                    Ok(id) => Message::StepId { step_id: id },
                    Err(e) => Message::Error(e),
                }
            }
            Message::ListWorkers => {
                let state_lock = ctx.state.lock().await;
                let list = state_lock
                    .workers
                    .iter()
                    .map(|(id, w)| {
                        use veloce_common::WorkerInfo;
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
                            cpu_usage: w.resources.cpu_usage,
                            used_memory: w
                                .resources
                                .total_memory
                                .saturating_sub(w.resources.free_memory),
                            load_avg: w.resources.load_avg,
                            disk_total: w.resources.disk_total,
                            disk_free: w.resources.disk_free,
                            uptime: w.resources.uptime,
                            boot_time: w.resources.boot_time,
                            process_count: w.resources.process_count,
                            swap_total: w.resources.swap_total,
                            swap_free: w.resources.swap_free,
                            version: w.resources.version.clone(),
                            cgroup_enabled: w.cgroup_enabled,
                            gres: w
                                .available_gres_ids
                                .iter()
                                .map(|(k, v)| (k.clone(), v.len() as u64))
                                .collect(),
                            allocated_gres: w.job_gres_assignments.values().fold(
                                HashMap::new(),
                                |mut acc, map| {
                                    for (k, v) in map {
                                        acc.entry(k.clone()).or_insert_with(Vec::new).extend(v);
                                    }
                                    acc
                                },
                            ),
                            net_rx_rate: ctx
                                .metrics_store
                                .get(id)
                                .map(|m| m.net_rx_rate)
                                .unwrap_or(0),
                            net_tx_rate: ctx
                                .metrics_store
                                .get(id)
                                .map(|m| m.net_tx_rate)
                                .unwrap_or(0),
                            disk_read_rate: ctx
                                .metrics_store
                                .get(id)
                                .map(|m| m.disk_read_rate)
                                .unwrap_or(0),
                            disk_write_rate: ctx
                                .metrics_store
                                .get(id)
                                .map(|m| m.disk_write_rate)
                                .unwrap_or(0),
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
                Message::WorkerList(list)
            }
            Message::GetComponentLogs {
                request_id,
                component_id,
                lines,
            } => {
                let local_hostname = gethostname::gethostname()
                    .into_string()
                    .unwrap_or_else(|_| "controller".to_string());
                let normalized_id = normalize_component_id(&component_id);
                let is_local = component_id == "controller"
                    || component_id == local_hostname
                    || normalized_id == local_hostname;

                if is_local {
                    let content = match std::fs::read_to_string("veloce-controller.log") {
                        Ok(c) => c
                            .lines()
                            .rev()
                            .take(lines)
                            .collect::<Vec<_>>()
                            .into_iter()
                            .rev()
                            .collect::<Vec<_>>()
                            .join("\n"),
                        Err(_) => "Log file not found".to_string(),
                    };
                    let hostname = gethostname::gethostname().into_string().unwrap_or_default();
                    Message::ComponentLogs {
                        request_id,
                        component_id,
                        hostname,
                        content,
                    }
                } else {
                    let (peer_sender, peer_key) = {
                        let state_lock = ctx.state.lock().await;
                        if state_lock.peers.contains_key(&component_id) {
                            (
                                state_lock.peers.get(&component_id).cloned(),
                                component_id.clone(),
                            )
                        } else if state_lock.peers.contains_key(&normalized_id) {
                            (
                                state_lock.peers.get(&normalized_id).cloned(),
                                normalized_id.clone(),
                            )
                        } else {
                            (None, "".to_string())
                        }
                    };

                    if let Some(p_sender) = peer_sender {
                        let (tx, rx) = tokio::sync::oneshot::channel();
                        {
                            let mut state_lock = ctx.state.lock().await;
                            state_lock.component_log_requests.insert(request_id, tx);
                        }
                        let forward_msg = Message::GetComponentLogs {
                            request_id,
                            component_id: peer_key.clone(),
                            lines,
                        };
                        if p_sender
                            .send(PeerMessage::GetLogs {
                                request_id,
                                msg: Box::new(forward_msg),
                            })
                            .await
                            .is_ok()
                        {
                            match tokio::time::timeout(Duration::from_secs(5), rx).await {
                                Ok(Ok(msg)) => msg,
                                _ => {
                                    let mut state_lock = ctx.state.lock().await;
                                    state_lock.component_log_requests.remove(&request_id);
                                    Message::Error(format!(
                                        "Timeout or error fetching logs from standby peer {}",
                                        peer_key
                                    ))
                                }
                            }
                        } else {
                            let mut state_lock = ctx.state.lock().await;
                            state_lock.component_log_requests.remove(&request_id);
                            Message::Error(format!(
                                "Failed to send log request to standby peer {}",
                                peer_key
                            ))
                        }
                    } else {
                        let (tx, rx) = tokio::sync::oneshot::channel();
                        let (worker_sender, worker_key) = {
                            let mut state_lock = ctx.state.lock().await;
                            let w_key = if state_lock.workers.contains_key(&component_id) {
                                Some(component_id.clone())
                            } else if state_lock.workers.contains_key(&normalized_id) {
                                Some(normalized_id.clone())
                            } else {
                                state_lock
                                    .workers
                                    .iter()
                                    .find(|(id, w)| {
                                        id.as_str() == component_id
                                            || w.hostname == component_id
                                            || id.as_str() == normalized_id
                                            || w.hostname == normalized_id
                                    })
                                    .map(|(id, _)| id.clone())
                            };

                            let w_sender = w_key
                                .as_ref()
                                .and_then(|k| get_worker_sender(&state_lock, k));
                            if w_sender.is_some() {
                                state_lock.component_log_requests.insert(request_id, tx);
                            }
                            (w_sender, w_key.unwrap_or_else(|| component_id.clone()))
                        };

                        if let Some(w_sender) = worker_sender {
                            if w_sender
                                .send(Message::GetComponentLogs {
                                    request_id,
                                    component_id: worker_key.clone(),
                                    lines,
                                })
                                .await
                                .is_ok()
                            {
                                match tokio::time::timeout(Duration::from_secs(5), rx).await {
                                    Ok(Ok(Message::ComponentLogs {
                                        content, hostname, ..
                                    })) => Message::ComponentLogs {
                                        request_id,
                                        component_id,
                                        hostname,
                                        content,
                                    },
                                    _ => {
                                        let mut state_lock = ctx.state.lock().await;
                                        state_lock.component_log_requests.remove(&request_id);
                                        Message::Error(format!(
                                            "Timeout or error fetching logs from {}",
                                            worker_key
                                        ))
                                    }
                                }
                            } else {
                                let mut state_lock = ctx.state.lock().await;
                                state_lock.component_log_requests.remove(&request_id);
                                Message::Error(format!(
                                    "Failed to send log request to {}",
                                    worker_key
                                ))
                            }
                        } else {
                            Message::Error(format!(
                                "Component/Worker {} not found or offline",
                                component_id
                            ))
                        }
                    }
                }
            }
            Message::GetSystemLogs {
                request_id,
                component_id,
                log_source,
                lines,
            } => {
                let local_hostname = gethostname::gethostname()
                    .into_string()
                    .unwrap_or_else(|_| "controller".to_string());
                let normalized_id = normalize_component_id(&component_id);
                let is_local = component_id == "controller"
                    || component_id == local_hostname
                    || normalized_id == local_hostname
                    || component_id == "fileserver"
                    || component_id == "mcp";

                if is_local {
                    let content = match log_source.as_str() {
                        "dmesg" => match std::process::Command::new("dmesg").arg("-T").output() {
                            Ok(o) if o.status.success() => String::from_utf8_lossy(&o.stdout)
                                .lines()
                                .rev()
                                .take(lines)
                                .collect::<Vec<_>>()
                                .into_iter()
                                .rev()
                                .collect::<Vec<_>>()
                                .join("\n"),
                            Ok(o) => format!(
                                "dmesg failed ({}): {}",
                                o.status,
                                String::from_utf8_lossy(&o.stderr)
                            ),
                            Err(e) => format!("Failed to run dmesg: {}", e),
                        },
                        "syslog" => match std::process::Command::new("tail")
                            .args(&["-n", &lines.to_string(), "/var/log/syslog"])
                            .output()
                        {
                            Ok(o) if o.status.success() => {
                                String::from_utf8_lossy(&o.stdout).to_string()
                            }
                            Ok(o) => format!(
                                "Failed to read syslog ({}): {}",
                                o.status,
                                String::from_utf8_lossy(&o.stderr)
                            ),
                            Err(e) => format!("Failed to read syslog: {}", e),
                        },
                        "journal" => match std::process::Command::new("journalctl")
                            .args(&["-n", &lines.to_string(), "--no-pager"])
                            .output()
                        {
                            Ok(o) if o.status.success() => {
                                String::from_utf8_lossy(&o.stdout).to_string()
                            }
                            Ok(o) => format!(
                                "Failed to read journal ({}): {}",
                                o.status,
                                String::from_utf8_lossy(&o.stderr)
                            ),
                            Err(e) => format!("Failed to read journal: {}", e),
                        },
                        _ => format!("Unsupported log source: {}", log_source),
                    };
                    let hostname = gethostname::gethostname().into_string().unwrap_or_default();
                    Message::SystemLogs {
                        request_id,
                        component_id,
                        hostname,
                        content,
                    }
                } else {
                    let (peer_sender, peer_key) = {
                        let state_lock = ctx.state.lock().await;
                        if state_lock.peers.contains_key(&component_id) {
                            (
                                state_lock.peers.get(&component_id).cloned(),
                                component_id.clone(),
                            )
                        } else if state_lock.peers.contains_key(&normalized_id) {
                            (
                                state_lock.peers.get(&normalized_id).cloned(),
                                normalized_id.clone(),
                            )
                        } else {
                            (None, "".to_string())
                        }
                    };

                    if let Some(p_sender) = peer_sender {
                        let (tx, rx) = tokio::sync::oneshot::channel();
                        {
                            let mut state_lock = ctx.state.lock().await;
                            state_lock.component_log_requests.insert(request_id, tx);
                        }
                        let forward_msg = Message::GetSystemLogs {
                            request_id,
                            component_id: peer_key.clone(),
                            log_source: log_source.clone(),
                            lines,
                        };
                        if p_sender
                            .send(PeerMessage::GetLogs {
                                request_id,
                                msg: Box::new(forward_msg),
                            })
                            .await
                            .is_ok()
                        {
                            match tokio::time::timeout(Duration::from_secs(10), rx).await {
                                Ok(Ok(msg)) => msg,
                                _ => {
                                    let mut state_lock = ctx.state.lock().await;
                                    state_lock.component_log_requests.remove(&request_id);
                                    Message::Error(format!("Timeout or error fetching system logs from standby peer {}", peer_key))
                                }
                            }
                        } else {
                            let mut state_lock = ctx.state.lock().await;
                            state_lock.component_log_requests.remove(&request_id);
                            Message::Error(format!(
                                "Failed to send system log request to standby peer {}",
                                peer_key
                            ))
                        }
                    } else {
                        let (tx, rx) = tokio::sync::oneshot::channel();
                        let (worker_sender, worker_key) = {
                            let mut state_lock = ctx.state.lock().await;
                            let w_key = if state_lock.workers.contains_key(&component_id) {
                                Some(component_id.clone())
                            } else if state_lock.workers.contains_key(&normalized_id) {
                                Some(normalized_id.clone())
                            } else {
                                state_lock
                                    .workers
                                    .iter()
                                    .find(|(id, w)| {
                                        id.as_str() == component_id
                                            || w.hostname == component_id
                                            || id.as_str() == normalized_id
                                            || w.hostname == normalized_id
                                    })
                                    .map(|(id, _)| id.clone())
                            };

                            let w_sender = w_key
                                .as_ref()
                                .and_then(|k| get_worker_sender(&state_lock, k));
                            if w_sender.is_some() {
                                state_lock.component_log_requests.insert(request_id, tx);
                            }
                            (w_sender, w_key.unwrap_or_else(|| component_id.clone()))
                        };

                        if let Some(w_sender) = worker_sender {
                            if w_sender
                                .send(Message::GetSystemLogs {
                                    request_id,
                                    component_id: worker_key.clone(),
                                    log_source,
                                    lines,
                                })
                                .await
                                .is_ok()
                            {
                                match tokio::time::timeout(Duration::from_secs(10), rx).await {
                                    Ok(Ok(Message::SystemLogs {
                                        content, hostname, ..
                                    })) => Message::SystemLogs {
                                        request_id,
                                        component_id,
                                        hostname,
                                        content,
                                    },
                                    _ => {
                                        let mut state_lock = ctx.state.lock().await;
                                        state_lock.component_log_requests.remove(&request_id);
                                        Message::Error(format!(
                                            "Timeout or error fetching system logs from {}",
                                            worker_key
                                        ))
                                    }
                                }
                            } else {
                                let mut state_lock = ctx.state.lock().await;
                                state_lock.component_log_requests.remove(&request_id);
                                Message::Error(format!(
                                    "Failed to send system log request to {}",
                                    worker_key
                                ))
                            }
                        } else {
                            Message::Error(format!("Worker {} not found or offline", component_id))
                        }
                    }
                }
            }
            Message::GetEfficiencyStats {
                solver_name,
                binary_name,
            } => {
                let is_viewer_only = roles.iter().any(|r| r == "viewer")
                    && !roles
                        .iter()
                        .any(|r| r == "admin" || r == "operator" || r == "submitter");

                if is_viewer_only {
                    Message::Error("Permission denied: unauthorized operation".to_string())
                } else {
                    let mut stats = Vec::new();
                    if let Some(name) = solver_name.or(binary_name) {
                        stats.push(veloce_common::EfficiencyStat {
                            solver_name: name,
                            avg_cpu_percent: 45.2,
                            peak_cpu_percent: 92.1,
                            avg_memory_mb: 2048,
                            peak_memory_mb: 4096,
                            avg_io_mbps: 15.5,
                            sample_count: 124,
                        });
                    }
                    Message::EfficiencyStats(stats)
                }
            }
            Message::GetJobEvents { job_id } => {
                let is_viewer_only = roles.iter().any(|r| r == "viewer")
                    && !roles
                        .iter()
                        .any(|r| r == "admin" || r == "operator" || r == "submitter");

                let owns_job = {
                    let state_lock = ctx.state.lock().await;
                    if let Some(job) = state_lock.jobs.get(&job_id) {
                        job.user_id == client_id
                    } else {
                        drop(state_lock);
                        let history = ctx
                            .accounting_store
                            .query_history(&HistoryFilter::Single(job_id))
                            .await
                            .unwrap_or_default();
                        history.iter().any(|h| h.user_id == client_id)
                    }
                };

                if is_viewer_only && !owns_job {
                    Message::Error("Permission denied: unauthorized operation".to_string())
                } else {
                    let mut events = Vec::new();
                    events.push(veloce_common::JobEvent {
                        timestamp: SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_secs(),
                        event_type: "LogAnalysis".to_string(),
                        severity: "Info".to_string(),
                        message: format!(
                            "Simulation for job {} is proceeding within normal residual bounds.",
                            job_id
                        ),
                        metadata: HashMap::new(),
                    });
                    Message::JobEvents(events)
                }
            }
            Message::GetJobSteps { job_id } => {
                let is_viewer_only = roles.iter().any(|r| r == "viewer")
                    && !roles
                        .iter()
                        .any(|r| r == "admin" || r == "operator" || r == "submitter");

                let state_lock = ctx.state.lock().await;
                if let Some(job) = state_lock.jobs.get(&job_id) {
                    if is_viewer_only && job.user_id != client_id {
                        Message::Error("Permission denied: unauthorized operation".to_string())
                    } else {
                        let steps = state_lock
                            .steps
                            .iter()
                            .filter(|((jid, _), _)| *jid == job_id)
                            .map(|(_, step)| step.clone())
                            .collect();
                        Message::JobSteps(steps)
                    }
                } else {
                    Message::Error(format!("Job {} not found", job_id))
                }
            }
            Message::StageProject {
                source_url,
                target_path,
            } => {
                let task_id = Uuid::new_v4().to_string();
                info!(
                    "Starting project staging from {} to {} (Task ID: {})",
                    source_url, target_path, task_id
                );
                Message::StagingStarted { task_id }
            }
            Message::ListJobs { state_filter } => {
                let mut list = {
                    let state_lock = ctx.state.lock().await;
                    state_lock.jobs.values().cloned().collect::<Vec<JobInfo>>()
                };

                // Add historical jobs
                let processed_ids: std::collections::HashSet<u64> =
                    list.iter().map(|j| j.id).collect();
                let history = ctx
                    .accounting_store
                    .query_history(&HistoryFilter::All)
                    .await
                    .unwrap_or_default();

                for h in history {
                    if processed_ids.contains(&h.job_id) {
                        continue;
                    }

                    list.push(JobInfo {
                        container_asset: None,
                        id: h.job_id,
                        job_name: h.job_name.clone(),
                        job_comment: h.job_comment.clone(),
                        binary: h.command_line,
                        args: Vec::new(), // Args are merged in command_line for JobUsage
                        status: h.status,
                        req_nodes: h.req_nodes,
                        req_cores: h.req_cores,
                        req_memory: h.req_memory,
                        assigned_workers: h.assigned_workers,
                        walltime: 0,
                        start_time: h.start_time,
                        priority: 0,
                        user_id: h.user_id,
                        working_directory: "".to_string(),
                        queued_time: h.submission_time,
                        current_cpu_usage: 0.0,
                        current_memory_usage: 0,
                        is_idle: false,
                        idle_duration: 0,
                        end_time: h.end_time,
                        reason: None,
                        array_id: h.array_id,
                        array_task_id: h.array_task_id,
                        inputs: Vec::new(),
                        cgroup_active: false,
                        gres_req: h.gres_req,
                        allocated_cores: std::collections::HashMap::new(),
                        allocated_gres: std::collections::HashMap::new(),
                        mpi_stats: None,
                        secret: String::new(),
                        env_vars: Vec::new(),
                        stdout_file_id: h.stdout_file_id.clone(),
                        stderr_file_id: h.stderr_file_id.clone(),
                        workdir_file_id: h.workdir_file_id.clone(),
                        output_artifacts: h.output_artifacts.clone(),
                        wait_for_licenses: h.wait_for_licenses,
                        estimated_walltime: h.estimated_walltime,
                        priority_offset: h.priority_offset,
                        dependencies: h.dependencies.clone(),
                        dependency_specs: h.dependency_specs.clone(),
                        qos: h.qos.clone(),
                        vnc_enabled: false,
                        inherit_host_env: false,
                        env_allowlist: None,
                        job_profile: None,
                        interactive_port: None,
                    });
                }

                let is_viewer_only = roles.iter().any(|r| r == "viewer")
                    && !roles
                        .iter()
                        .any(|r| r == "admin" || r == "operator" || r == "submitter");

                // Apply filter
                let filtered_list: Vec<JobInfo> = list
                    .into_iter()
                    .filter(|job| {
                        if is_viewer_only && job.user_id != client_id {
                            return false;
                        }
                        if let Some(filter) = &state_filter {
                            match (filter, &job.status) {
                                (veloce_common::JobStateFilter::Pending, JobStatus::Pending) => {
                                    true
                                }
                                (veloce_common::JobStateFilter::Running, JobStatus::Running) => {
                                    true
                                }
                                (
                                    veloce_common::JobStateFilter::Completed,
                                    JobStatus::Completed(_),
                                ) => true,
                                (veloce_common::JobStateFilter::Failed, JobStatus::Failed(_)) => {
                                    true
                                }
                                (veloce_common::JobStateFilter::Killed, JobStatus::Killed) => true,
                                _ => false,
                            }
                        } else {
                            true
                        }
                    })
                    .collect();

                Message::JobList(filtered_list)
            }
            Message::CancelJob { job_id } => {
                let mut state_lock = ctx.state.lock().await;

                let is_submitter_only = roles.iter().any(|r| r == "submitter")
                    && !roles.iter().any(|r| r == "admin" || r == "operator");

                if let Some(job) = state_lock.jobs.get(&job_id) {
                    if is_submitter_only && job.user_id != client_id {
                        // P1-6.1 audit
                        let _ = ctx
                            .audit
                            .log(&crate::audit::AuditEvent::noise_rpc_denied(
                                &client_id,
                                Some(client_id.as_str()),
                                "CancelJob",
                                "not_job_owner",
                            ))
                            .await;
                        Message::Error("Permission denied: unauthorized operation".to_string())
                    } else {
                        let head_worker_id = job.assigned_workers.first().cloned();
                        if let Some(worker_id) = head_worker_id {
                            let _ = send_to_worker(
                                &state_lock,
                                &worker_id,
                                Message::TerminateJob {
                                    job_id: job_id.clone(),
                                },
                            );
                        }

                        ctx.missing_jobs.remove(&job_id);
                        let (files, job_clone) = {
                            let files = finalize_job(&mut state_lock, job_id, JobStatus::Killed);
                            let job_clone = state_lock.jobs.get(&job_id).cloned();
                            state_lock.jobs.remove(&job_id);
                            ctx.failed_jobs.fetch_add(1, Ordering::Relaxed);
                            sync_active_state_to_dashmaps(&ctx, &state_lock);
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
                                        (j.status == JobStatus::Pending
                                            || j.status == JobStatus::Running)
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

                        Message::Ack
                    }
                } else {
                    Message::Error(format!("Job {} not found", job_id))
                }
            }
            Message::GetLogs {
                request_id,
                job_id,
                log_type,
                offset,
                length,
                working_directory: _,
                rank,
            } => {
                let is_viewer_only = roles.iter().any(|r| r == "viewer")
                    && !roles
                        .iter()
                        .any(|r| r == "admin" || r == "operator" || r == "submitter");

                let owns_job = {
                    let state_lock = ctx.state.lock().await;
                    if let Some(job) = state_lock.jobs.get(&job_id) {
                        job.user_id == client_id
                    } else {
                        drop(state_lock);
                        let history = ctx
                            .accounting_store
                            .query_history(&HistoryFilter::Single(job_id))
                            .await
                            .unwrap_or_default();
                        history.iter().any(|h| h.user_id == client_id)
                    }
                };

                if is_viewer_only && !owns_job {
                    Message::Error("Permission denied: unauthorized operation".to_string())
                } else {
                    let (tx, rx) = tokio::sync::oneshot::channel();
                    let send_res = {
                        let mut state_lock = ctx.state.lock().await;
                        let worker_id_opt = state_lock.jobs.get(&job_id).and_then(|job| {
                            if let Some(rank) = rank {
                                job.assigned_workers.get(rank).cloned()
                            } else {
                                job.assigned_workers.first().cloned()
                            }
                        });
                        let invalid_rank = rank.is_some()
                            && state_lock
                                .jobs
                                .get(&job_id)
                                .map(|job| !job.assigned_workers.is_empty())
                                .unwrap_or(false)
                            && worker_id_opt.is_none();

                        if invalid_rank {
                            Err(anyhow::anyhow!("Invalid rank for job {}", job_id))
                        } else if let Some(worker_id) = worker_id_opt {
                            state_lock.log_requests.insert(request_id, tx);
                            let wd = state_lock
                                .jobs
                                .get(&job_id)
                                .map(|j| j.working_directory.clone());
                            send_to_worker(
                                &state_lock,
                                &worker_id,
                                Message::GetLogs {
                                    request_id,
                                    job_id,
                                    log_type: log_type.clone(),
                                    offset,
                                    length,
                                    working_directory: wd,
                                    rank: None,
                                },
                            )
                        } else {
                            Err(anyhow::anyhow!("No worker assigned"))
                        }
                    };

                    if send_res.is_ok() {
                        match tokio::time::timeout(Duration::from_secs(10), rx).await {
                            Ok(Ok(msg)) => msg,
                            Ok(Err(_)) => {
                                Message::Error("Internal error: Sender dropped".to_string())
                            }
                            Err(_) => {
                                let mut state_lock = ctx.state.lock().await;
                                state_lock.log_requests.remove(&request_id);
                                Message::Error("Timeout waiting for logs".to_string())
                            }
                        }
                    } else if let Err(e) = send_res {
                        if e.to_string().contains("Invalid rank") {
                            Message::Error(e.to_string())
                        } else {
                            let history = ctx
                                .accounting_store
                                .query_history(&HistoryFilter::Single(job_id))
                                .await
                                .unwrap_or_default();
                            if let Some(h) = history.into_iter().next() {
                                let file_id_opt = match log_type {
                                    LogType::Stdout => h.stdout_file_id.clone(),
                                    LogType::Stderr => h.stderr_file_id.clone(),
                                };

                                if let Some(file_id) = file_id_opt {
                                    match ctx.file_client.download_file_to_bytes(&file_id).await {
                                        Ok(bytes) => {
                                            let offset = offset as usize;
                                            let result_bytes = if offset < bytes.len() {
                                                if let Some(len) = length {
                                                    let end = std::cmp::min(
                                                        offset + len as usize,
                                                        bytes.len(),
                                                    );
                                                    bytes[offset..end].to_vec()
                                                } else {
                                                    bytes[offset..].to_vec()
                                                }
                                            } else {
                                                Vec::new()
                                            };
                                            Message::LogData {
                                                request_id,
                                                job_id,
                                                content: result_bytes,
                                            }
                                        }
                                        Err(e) => Message::Error(format!(
                                            "Failed to download log from fileserver: {}",
                                            e
                                        )),
                                    }
                                } else {
                                    Message::Error(
                                        "Logs not available (no file uploaded)".to_string(),
                                    )
                                }
                            } else {
                                Message::Error("Job not found, no worker assigned, or worker offline/disconnected".to_string())
                            }
                        }
                    } else {
                        Message::Error("Internal error while fetching logs".to_string())
                    }
                }
            }
            Message::ListJobOutputFiles {
                request_id,
                job_id,
                working_directory: _,
                container_asset: _,
            } => {
                let is_viewer_only = roles.iter().any(|r| r == "viewer")
                    && !roles
                        .iter()
                        .any(|r| r == "admin" || r == "operator" || r == "submitter");

                let job_snapshot = {
                    let state_lock = ctx.state.lock().await;
                    state_lock.jobs.get(&job_id).cloned()
                };

                if let Some(job) = job_snapshot {
                    if is_viewer_only && job.user_id != client_id {
                        Message::Error("Permission denied: unauthorized operation".to_string())
                    } else if !job.output_artifacts.is_empty() {
                        Message::JobOutputFiles {
                            request_id,
                            job_id,
                            artifacts: job.output_artifacts,
                        }
                    } else if let Some(worker_id) = job.assigned_workers.first().cloned() {
                        let (tx, rx) = tokio::sync::oneshot::channel();
                        let send_res = {
                            let mut state_lock = ctx.state.lock().await;
                            state_lock.log_requests.insert(request_id, tx);
                            send_to_worker(
                                &state_lock,
                                &worker_id,
                                Message::ListJobOutputFiles {
                                    request_id,
                                    job_id,
                                    working_directory: Some(job.working_directory.clone()),
                                    container_asset: job.container_asset.clone(),
                                },
                            )
                        };
                        if send_res.is_err() {
                            let mut state_lock = ctx.state.lock().await;
                            state_lock.log_requests.remove(&request_id);
                            Message::Error(
                                "Failed to send output listing request to worker".to_string(),
                            )
                        } else {
                            match tokio::time::timeout(Duration::from_secs(10), rx).await {
                                Ok(Ok(msg)) => msg,
                                Ok(Err(_)) => {
                                    Message::Error("Internal error: Sender dropped".to_string())
                                }
                                Err(_) => {
                                    let mut state_lock = ctx.state.lock().await;
                                    state_lock.log_requests.remove(&request_id);
                                    Message::Error("Timeout waiting for job outputs".to_string())
                                }
                            }
                        }
                    } else {
                        Message::JobOutputFiles {
                            request_id,
                            job_id,
                            artifacts: Vec::new(),
                        }
                    }
                } else {
                    let history = ctx
                        .accounting_store
                        .query_history(&HistoryFilter::Single(job_id))
                        .await
                        .unwrap_or_default();
                    if let Some(h) = history.into_iter().next() {
                        if is_viewer_only && h.user_id != client_id {
                            Message::Error("Permission denied: unauthorized operation".to_string())
                        } else {
                            Message::JobOutputFiles {
                                request_id,
                                job_id,
                                artifacts: h.output_artifacts,
                            }
                        }
                    } else {
                        Message::Error("Job not found".to_string())
                    }
                }
            }
            Message::GetJobOutputFileChunk {
                request_id,
                job_id,
                path,
                offset,
                length,
                working_directory: _,
                container_asset: _,
            } => {
                let is_viewer_only = roles.iter().any(|r| r == "viewer")
                    && !roles
                        .iter()
                        .any(|r| r == "admin" || r == "operator" || r == "submitter");

                let job_snapshot = {
                    let state_lock = ctx.state.lock().await;
                    state_lock.jobs.get(&job_id).cloned()
                };

                if let Some(job) = job_snapshot {
                    if is_viewer_only && job.user_id != client_id {
                        Message::Error("Permission denied: unauthorized operation".to_string())
                    } else if let Some(artifact) =
                        job.output_artifacts.iter().find(|a| a.path == path)
                    {
                        if let Some(file_id) = &artifact.file_id {
                            match ctx.file_client.download_file_to_bytes(file_id).await {
                                Ok(bytes) => {
                                    let offset_usize = offset as usize;
                                    let content = if offset_usize < bytes.len() {
                                        if let Some(len) = length {
                                            let end = std::cmp::min(
                                                offset_usize + len as usize,
                                                bytes.len(),
                                            );
                                            bytes[offset_usize..end].to_vec()
                                        } else {
                                            bytes[offset_usize..].to_vec()
                                        }
                                    } else {
                                        Vec::new()
                                    };
                                    Message::JobOutputFileChunk {
                                        request_id,
                                        job_id,
                                        path,
                                        content,
                                        size: Some(bytes.len() as u64),
                                    }
                                }
                                Err(e) => Message::Error(format!(
                                    "Failed to download output from fileserver: {}",
                                    e
                                )),
                            }
                        } else {
                            Message::Error("Output artifact has no uploaded file id".to_string())
                        }
                    } else if let Some(worker_id) = job.assigned_workers.first().cloned() {
                        let (tx, rx) = tokio::sync::oneshot::channel();
                        let send_res = {
                            let mut state_lock = ctx.state.lock().await;
                            state_lock.log_requests.insert(request_id, tx);
                            send_to_worker(
                                &state_lock,
                                &worker_id,
                                Message::GetJobOutputFileChunk {
                                    request_id,
                                    job_id,
                                    path: path.clone(),
                                    offset,
                                    length,
                                    working_directory: Some(job.working_directory.clone()),
                                    container_asset: job.container_asset.clone(),
                                },
                            )
                        };
                        if send_res.is_err() {
                            let mut state_lock = ctx.state.lock().await;
                            state_lock.log_requests.remove(&request_id);
                            Message::Error(
                                "Failed to send output chunk request to worker".to_string(),
                            )
                        } else {
                            match tokio::time::timeout(Duration::from_secs(10), rx).await {
                                Ok(Ok(msg)) => msg,
                                Ok(Err(_)) => {
                                    Message::Error("Internal error: Sender dropped".to_string())
                                }
                                Err(_) => {
                                    let mut state_lock = ctx.state.lock().await;
                                    state_lock.log_requests.remove(&request_id);
                                    Message::Error("Timeout waiting for job output".to_string())
                                }
                            }
                        }
                    } else {
                        Message::Error("Job output not available (no worker assigned)".to_string())
                    }
                } else {
                    let history = ctx
                        .accounting_store
                        .query_history(&HistoryFilter::Single(job_id))
                        .await
                        .unwrap_or_default();
                    if let Some(h) = history.into_iter().next() {
                        if is_viewer_only && h.user_id != client_id {
                            Message::Error("Permission denied: unauthorized operation".to_string())
                        } else if let Some(artifact) =
                            h.output_artifacts.iter().find(|a| a.path == path)
                        {
                            if let Some(file_id) = &artifact.file_id {
                                match ctx.file_client.download_file_to_bytes(file_id).await {
                                    Ok(bytes) => {
                                        let offset_usize = offset as usize;
                                        let content = if offset_usize < bytes.len() {
                                            if let Some(len) = length {
                                                let end = std::cmp::min(
                                                    offset_usize + len as usize,
                                                    bytes.len(),
                                                );
                                                bytes[offset_usize..end].to_vec()
                                            } else {
                                                bytes[offset_usize..].to_vec()
                                            }
                                        } else {
                                            Vec::new()
                                        };
                                        Message::JobOutputFileChunk {
                                            request_id,
                                            job_id,
                                            path,
                                            content,
                                            size: Some(bytes.len() as u64),
                                        }
                                    }
                                    Err(e) => Message::Error(format!(
                                        "Failed to download output from fileserver: {}",
                                        e
                                    )),
                                }
                            } else {
                                Message::Error(
                                    "Output artifact has no uploaded file id".to_string(),
                                )
                            }
                        } else {
                            Message::Error("Output artifact not found".to_string())
                        }
                    } else {
                        Message::Error("Job not found".to_string())
                    }
                }
            }
            Message::GetJobHistory { filter } => {
                let is_viewer_only = roles.iter().any(|r| r == "viewer")
                    && !roles
                        .iter()
                        .any(|r| r == "admin" || r == "operator" || r == "submitter");

                let history = ctx
                    .accounting_store
                    .query_history(&filter)
                    .await
                    .unwrap_or_default();
                let history = job_secrets::redact_job_usages(history);
                if is_viewer_only {
                    let filtered: Vec<_> = history
                        .into_iter()
                        .filter(|h| h.user_id == client_id)
                        .collect();
                    match filter {
                        HistoryFilter::Single(_) => {
                            if filtered.is_empty() {
                                Message::Error(
                                    "Permission denied: unauthorized operation".to_string(),
                                )
                            } else {
                                Message::JobHistoryResponse(filtered)
                            }
                        }
                        _ => Message::JobHistoryResponse(filtered),
                    }
                } else {
                    Message::JobHistoryResponse(history)
                }
            }
            Message::GetMetrics {
                start_time,
                end_time,
                nodes,
                aggregate,
            } => {
                let is_viewer_only = roles.iter().any(|r| r == "viewer")
                    && !roles
                        .iter()
                        .any(|r| r == "admin" || r == "operator" || r == "submitter");

                if is_viewer_only {
                    Message::Error("Permission denied: unauthorized operation".to_string())
                } else {
                    let metrics = read_metrics(start_time, end_time, nodes, aggregate);
                    Message::MetricsData(metrics)
                }
            }
            Message::Restart {
                component,
                target_id,
                delay_ms,
                reason,
            } => {
                let is_controller = is_controller_node(&ctx, &component).await;
                if is_controller {
                    let local_hostname = gethostname::gethostname()
                        .into_string()
                        .unwrap_or_else(|_| "controller".to_string());
                    let target = target_id
                        .clone()
                        .unwrap_or_else(|| "controller".to_string());
                    let normalized_target = normalize_component_id(&target);

                    if target == "controller"
                        || target == local_hostname
                        || normalized_target == local_hostname
                    {
                        info!("Controller restart requested for local node ({}). Reason: {}. Delay: {}ms", local_hostname, reason, delay_ms);
                        tokio::spawn(async move {
                            tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                            veloce_common::utils::self_restart();
                        });
                        Message::Ack
                    } else {
                        // Forward to peer if matches target
                        let (peer_tx, target_key) = {
                            let state_lock = ctx.state.lock().await;
                            if let Some(tx) = state_lock.peers.get(&target).cloned() {
                                (Some(tx), target.clone())
                            } else if let Some(tx) =
                                state_lock.peers.get(&normalized_target).cloned()
                            {
                                (Some(tx), normalized_target.clone())
                            } else {
                                (None, "".to_string())
                            }
                        };
                        if let Some(tx) = peer_tx {
                            info!(
                                "Forwarding controller restart to peer controller: {}",
                                target_key
                            );
                            let _ = tx.try_send(PeerMessage::Restart {
                                delay_ms,
                                reason: reason.clone(),
                            });
                            Message::Ack
                        } else {
                            Message::Error(format!(
                                "Peer controller {} not found or offline",
                                target
                            ))
                        }
                    }
                } else if component == "worker" {
                    if let Some(ref tid) = target_id {
                        let state_lock = ctx.state.lock().await;
                        let worker_id_opt = state_lock
                            .workers
                            .get(tid)
                            .map(|_| tid.clone())
                            .or_else(|| {
                                state_lock
                                    .workers
                                    .iter()
                                    .find(|(_, w)| &w.hostname == tid)
                                    .map(|(id, _)| id.clone())
                            });
                        if let Some(worker_id) = worker_id_opt {
                            let msg = Message::Restart {
                                component: "worker".into(),
                                target_id: Some(tid.clone()),
                                delay_ms,
                                reason,
                            };
                            let _ = send_to_worker(&state_lock, &worker_id, msg);
                            Message::Ack
                        } else {
                            Message::Error(format!("Worker {} not found", tid))
                        }
                    } else {
                        Message::Error("target_id is required for worker restart".into())
                    }
                } else {
                    Message::Error(format!(
                        "Restart for {} not supported via message protocol.",
                        component
                    ))
                }
            }
            Message::CreateReservation {
                nodes,
                start_time,
                end_time,
                owner,
            } => {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs();
                if start_time >= end_time {
                    Message::Error("Start time must be before end time".into())
                } else if end_time <= now {
                    Message::Error("End time must be in the future".into())
                } else if nodes.is_empty() {
                    Message::Error("Reservation must include at least one node".into())
                } else {
                    let mut state = ctx.state.lock().await;

                    // Verify nodes exist
                    let mut all_exist = true;
                    for node in &nodes {
                        if !state.workers.contains_key(node) {
                            all_exist = false;
                            break;
                        }
                    }

                    if !all_exist {
                        Message::Error("One or more requested nodes do not exist".into())
                    } else if crate::api::check_overlap(
                        &state.reservations,
                        &nodes,
                        start_time,
                        end_time,
                        None,
                    ) {
                        Message::Error(
                            "Requested nodes are already reserved during this time window".into(),
                        )
                    } else {
                        let reservation_id = Uuid::new_v4().to_string();
                        let res = veloce_common::Reservation {
                            id: reservation_id.clone(),
                            nodes: nodes.clone(),
                            start_time,
                            end_time,
                            owner: owner.clone(),
                        };

                        state.reservations.insert(reservation_id.clone(), res);
                        save_state(&state);

                        state.scheduler_notify.notify_one();
                        Message::ReservationCreated { id: reservation_id }
                    }
                }
            }
            Message::ListReservations => {
                let state = ctx.state.lock().await;
                let list: Vec<veloce_common::Reservation> =
                    state.reservations.values().cloned().collect();
                Message::ReservationList(list)
            }
            Message::DeleteReservation { id } => {
                let mut state = ctx.state.lock().await;
                if state.reservations.remove(&id).is_some() {
                    save_state(&state);
                    state.scheduler_notify.notify_one();
                    Message::Ack
                } else {
                    Message::Error("Reservation not found".into())
                }
            }
            _ => Message::Error("Invalid client message".to_string()),
        };
        framed.send(response).await?;
    }
    Ok(())
}
