//! API module: jobs.rs

use crate::{finalize_job, submit_step, Config, SharedContext};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Extension, Json,
};
use log::{error, info};
use serde::Deserialize;
use std::sync::atomic::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time::{timeout, Duration};
use uuid::Uuid;
use veloce_common::{HistoryFilter, JobInfo, JobStatus, LaunchRequest, LogType, Message};

use super::types::*;

#[derive(Deserialize)]
pub(super) struct SubmitJobRequest {
    #[serde(default)]
    pub job_name: Option<String>,
    #[serde(default)]
    pub job_comment: Option<String>,
    pub binary: String,
    pub args: Vec<String>,
    pub req_nodes: usize,
    pub req_cores: u32,
    pub req_memory: u64,
    pub walltime: u64,
    pub priority: u32,
    pub user_id: String,
    pub working_directory: String,
    pub array_indices: Option<Vec<u32>>,
    #[serde(default)]
    pub inputs: Vec<veloce_common::FileHandle>,
    #[serde(default)]
    pub gres_req: std::collections::BTreeMap<String, u64>,
    #[serde(default)]
    pub env_vars: Vec<(String, String)>,
    #[serde(default)]
    pub wait_for_licenses: bool,
    #[serde(default)]
    pub dependencies: Option<Vec<u64>>,
    #[serde(default)]
    pub dependency_specs: Option<Vec<String>>,
    #[serde(default)]
    pub qos: veloce_common::QosLevel,
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

pub(super) async fn api_submit_job(
    State(ctx): State<SharedContext>,
    Extension(config): Extension<Config>,
    Extension(principal): Extension<crate::auth::AuthenticatedPrincipal>,
    Json(mut payload): Json<SubmitJobRequest>,
) -> impl IntoResponse {
    if !principal
        .roles
        .iter()
        .any(|r| r == "admin" || r == "operator" || r == "submitter")
    {
        return (StatusCode::FORBIDDEN, "Forbidden: insufficient role").into_response();
    }
    let resolved_user_id = match principal.resolve_user_id(&payload.user_id) {
        Ok(uid) => uid,
        Err(e) => return (StatusCode::FORBIDDEN, e).into_response(),
    };
    payload.user_id = resolved_user_id;

    let exec_opts = veloce_common::job_policy::JobExecutionOptions::from_fields(
        payload.inherit_host_env,
        &payload.env_allowlist,
        &payload.job_profile,
    );
    if let Err(e) = crate::validate_job_submission(
        &payload.binary,
        &payload.env_vars,
        &exec_opts,
        &config.allowed_binaries_prefixes,
    ) {
        let _ = ctx
            .audit
            .log(&crate::audit::AuditEvent::job_launch_denied(
                &principal.user_id,
                &e,
            ))
            .await;
        return (StatusCode::BAD_REQUEST, e).into_response();
    }

    if payload.qos == veloce_common::QosLevel::Interactive {
        if payload.walltime == 0 || payload.walltime > 1800 {
            return (
                StatusCode::BAD_REQUEST,
                "Interactive jobs must have a walltime limit of 30 minutes (1800 seconds) or less",
            )
                .into_response();
        }
        if payload.req_cores > 4 {
            return (
                StatusCode::BAD_REQUEST,
                "Interactive jobs are limited to a maximum of 4 CPU cores per node",
            )
                .into_response();
        }
    }

    let mut container_asset = None;

    // Check for virtual container first
    if let Some(uri) = &payload.image_uri {
        if uri.starts_with("veloce://virtual://") {
            let name = uri.trim_start_matches("veloce://virtual://");
            let state = ctx.state.lock().await;
            if let Some(config) = state.solver_configs.get(name) {
                payload.binary = config.executable.clone();
                payload.image_uri = None; // Reset so it runs natively
            } else {
                return (
                    StatusCode::BAD_REQUEST,
                    format!("Virtual container '{}' not found", name),
                )
                    .into_response();
            }
        }
    }

    if let Some(uri) = &payload.image_uri {
        let lookup_id = if uri.starts_with("veloce://") && !uri.starts_with("veloce://virtual://") {
            uri.trim_start_matches("veloce://")
        } else {
            uri
        };
        if let Ok(Some(asset)) = ctx.container_store.get_container(lookup_id).await {
            container_asset = Some(asset);
        } else {
            return (
                StatusCode::BAD_REQUEST,
                format!("Container image '{}' not found in registry", uri),
            )
                .into_response();
        }
    } else if container_asset.is_none() && !payload.binary.is_empty() {
        // Fallback: try lookup by binary name if image_uri is not provided
        if let Ok(Some(asset)) = ctx.container_store.get_container(&payload.binary).await {
            container_asset = Some(asset);
        }
    }

    let mut vnc_enabled = payload.vnc_enabled.unwrap_or(false);
    if let Some(ref asset) = container_asset {
        if asset.manifest.vnc_enabled {
            vnc_enabled = true;
        }
    }

    if let Some(indices) = payload.array_indices {
        let task_count = indices.len();
        if task_count == 0 {
            return (StatusCode::BAD_REQUEST, "Empty array indices").into_response();
        }

        let base_id = ctx
            .next_job_id
            .fetch_add(task_count as u64, Ordering::Relaxed);
        let mut success_count = 0;

        for (i, task_id) in indices.into_iter().enumerate() {
            let job = JobInfo {
                id: base_id + i as u64,
                job_name: payload.job_name.clone(),
                job_comment: payload.job_comment.clone(),
                binary: payload.binary.clone(),
                args: payload.args.clone(),
                status: JobStatus::Pending,
                assigned_workers: Vec::new(),
                req_nodes: payload.req_nodes,
                req_cores: payload.req_cores,
                req_memory: payload.req_memory,
                walltime: payload.walltime,
                start_time: None,
                priority: payload.priority,
                user_id: payload.user_id.clone(),
                working_directory: if payload.working_directory.trim().is_empty() {
                    "/scratch".to_string()
                } else {
                    payload.working_directory.clone()
                },
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
                inputs: payload.inputs.clone(),
                cgroup_active: false,
                gres_req: payload.gres_req.clone(),
                allocated_cores: std::collections::HashMap::new(),
                allocated_gres: std::collections::HashMap::new(),
                mpi_stats: None,
                secret: Uuid::new_v4().to_string(),
                env_vars: payload.env_vars.clone(),
                stdout_file_id: None,
                stderr_file_id: None,
                workdir_file_id: None,
                output_artifacts: Vec::new(),
                wait_for_licenses: payload.wait_for_licenses,
                estimated_walltime: None,
                priority_offset: None,
                dependencies: payload.dependencies.clone(),
                dependency_specs: payload.dependency_specs.clone(),
                qos: payload.qos,
                vnc_enabled,
                inherit_host_env: payload.inherit_host_env,
                env_allowlist: payload.env_allowlist.clone(),
                job_profile: payload.job_profile.clone(),
                interactive_port: None,
            };

            if payload.job_profile.as_deref() == Some("system") {
                let _ = ctx
                    .audit
                    .log(&crate::audit::AuditEvent::job_system_profile_launch(
                        job.id,
                        &principal.user_id,
                        "system",
                    ))
                    .await;
            }

            if let Err(e) = ctx.submission_tx.send(job).await {
                error!("Failed to enqueue job submission via API: {}", e);
            } else {
                success_count += 1;
            }
        }

        if success_count > 0 {
            (
                StatusCode::CREATED,
                Json(serde_json::json!({ "base_job_id": base_id, "task_count": success_count })),
            )
                .into_response()
        } else {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Submission failed for array jobs",
            )
                .into_response()
        }
    } else {
        let id = ctx.next_job_id.fetch_add(1, Ordering::Relaxed);

        let job = JobInfo {
            id,
            job_name: payload.job_name,
            job_comment: payload.job_comment,
            binary: payload.binary,
            args: payload.args,
            status: JobStatus::Pending,
            assigned_workers: Vec::new(),
            req_nodes: payload.req_nodes,
            req_cores: payload.req_cores,
            req_memory: payload.req_memory,
            walltime: payload.walltime,
            start_time: None,
            priority: payload.priority,
            user_id: payload.user_id,
            working_directory: if payload.working_directory.trim().is_empty() {
                "/scratch".to_string()
            } else {
                payload.working_directory
            },
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
            inputs: payload.inputs,
            cgroup_active: false,
            gres_req: payload.gres_req,
            allocated_cores: std::collections::HashMap::new(),
            allocated_gres: std::collections::HashMap::new(),
            mpi_stats: None,
            secret: Uuid::new_v4().to_string(),
            env_vars: payload.env_vars,
            stdout_file_id: None,
            stderr_file_id: None,
            workdir_file_id: None,
            output_artifacts: Vec::new(),
            wait_for_licenses: payload.wait_for_licenses,
            estimated_walltime: None,
            priority_offset: None,
            dependencies: payload.dependencies,
            dependency_specs: payload.dependency_specs,
            qos: payload.qos,
            vnc_enabled,
            inherit_host_env: payload.inherit_host_env,
            env_allowlist: payload.env_allowlist,
            job_profile: payload.job_profile,
            interactive_port: None,
        };

        if job.job_profile.as_deref() == Some("system") {
            let _ = ctx
                .audit
                .log(&crate::audit::AuditEvent::job_system_profile_launch(
                    id,
                    &principal.user_id,
                    "system",
                ))
                .await;
        }

        if let Err(e) = ctx.submission_tx.send(job).await {
            error!("Failed to enqueue job submission via API: {}", e);
            return (StatusCode::INTERNAL_SERVER_ERROR, "Submission failed").into_response();
        }

        (
            StatusCode::CREATED,
            Json(serde_json::json!({ "job_id": id })),
        )
            .into_response()
    }
}

#[derive(Deserialize)]
pub(super) struct SubmitCwlRequest {
    pub cwl_content: String,
    pub user_id: String,
    pub working_directory: String,
}

pub(super) async fn api_submit_cwl(
    State(ctx): State<SharedContext>,
    Extension(principal): Extension<crate::auth::AuthenticatedPrincipal>,
    Json(mut payload): Json<SubmitCwlRequest>,
) -> impl IntoResponse {
    if !principal
        .roles
        .iter()
        .any(|r| r == "admin" || r == "operator" || r == "submitter")
    {
        return (StatusCode::FORBIDDEN, "Forbidden: insufficient role").into_response();
    }
    let resolved_user_id = match principal.resolve_user_id(&payload.user_id) {
        Ok(uid) => uid,
        Err(e) => return (StatusCode::FORBIDDEN, e).into_response(),
    };
    payload.user_id = resolved_user_id;

    match veloce_common::cwl::parse_cwl_to_dag(
        &payload.cwl_content,
        &payload.user_id,
        &payload.working_directory,
    ) {
        Ok(dag) => {
            let node_count = dag.nodes.len();
            if node_count == 0 {
                return (StatusCode::BAD_REQUEST, "DAG has no nodes").into_response();
            }

            let base_id = ctx
                .next_job_id
                .fetch_add(node_count as u64, Ordering::Relaxed);
            let mut node_to_job = std::collections::HashMap::new();
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
                    if let Ok(Some(asset)) = ctx.container_store.get_container(lookup_id).await {
                        container_asset = Some(asset);
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
                        user_id,
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
                        allocated_cores: std::collections::HashMap::new(),
                        allocated_gres: std::collections::HashMap::new(),
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
                        dependencies: Some(final_deps),
                        dependency_specs: Some(final_specs),
                        qos,
                        vnc_enabled,
                        inherit_host_env,
                        env_allowlist,
                        job_profile,
                        interactive_port: None,
                    };

                    if let Err(e) = ctx.submission_tx.send(job).await {
                        error!("Failed to enqueue CWL job submission via API: {}", e);
                    } else {
                        success_count += 1;
                    }
                }
            }

            if success_count > 0 {
                (
                    StatusCode::CREATED,
                    Json(
                        serde_json::json!({ "base_job_id": base_id, "task_count": success_count }),
                    ),
                )
                    .into_response()
            } else {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Submission failed for CWL workflow",
                )
                    .into_response()
            }
        }
        Err(e) => (
            StatusCode::BAD_REQUEST,
            format!("Failed to parse CWL: {}", e),
        )
            .into_response(),
    }
}

pub(super) async fn api_list_jobs(
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
    let is_viewer_only = principal.roles.iter().any(|r| r == "viewer")
        && !principal
            .roles
            .iter()
            .any(|r| r == "admin" || r == "operator" || r == "submitter");

    let mut jobs = {
        let state_lock = ctx.state.lock().await;
        state_lock.jobs.values().cloned().collect::<Vec<JobInfo>>()
    };

    // Add historical jobs
    let processed_ids: std::collections::HashSet<u64> = jobs.iter().map(|j| j.id).collect();
    let history = ctx
        .accounting_store
        .query_history(&HistoryFilter::All)
        .await
        .unwrap_or_default();

    for h in history {
        if processed_ids.contains(&h.job_id) {
            continue;
        }

        jobs.push(JobInfo {
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

    if is_viewer_only {
        jobs.retain(|j| j.user_id == principal.user_id);
    }

    info!(
        "api_list_jobs: Returning {} jobs (including history)",
        jobs.len()
    );
    let mapped: Vec<JobInfoResponse> = jobs.into_iter().map(JobInfoResponse::from).collect();
    Json(mapped).into_response()
}

pub(super) async fn api_get_job(
    State(ctx): State<SharedContext>,
    Extension(principal): Extension<crate::auth::AuthenticatedPrincipal>,
    Path(id): Path<u64>,
) -> Response {
    let has_valid_role = principal
        .roles
        .iter()
        .any(|r| r == "admin" || r == "operator" || r == "submitter" || r == "viewer");
    if !has_valid_role {
        return (StatusCode::FORBIDDEN, "Forbidden: insufficient role").into_response();
    }
    let is_viewer_only = principal.roles.iter().any(|r| r == "viewer")
        && !principal
            .roles
            .iter()
            .any(|r| r == "admin" || r == "operator" || r == "submitter");

    let state_lock = ctx.state.lock().await;
    if let Some(job) = state_lock.jobs.get(&id) {
        if is_viewer_only && job.user_id != principal.user_id {
            return (StatusCode::FORBIDDEN, "Forbidden").into_response();
        }
        let resp = JobInfoResponse::from(job.clone());
        Json(resp).into_response()
    } else {
        drop(state_lock);
        let history = ctx
            .accounting_store
            .query_history(&veloce_common::HistoryFilter::All)
            .await
            .unwrap_or_default();
        if let Some(h) = history.iter().find(|h| h.job_id == id) {
            if is_viewer_only && h.user_id != principal.user_id {
                return (StatusCode::FORBIDDEN, "Forbidden").into_response();
            }
            let job = JobInfo {
                container_asset: None,
                id: h.job_id,
                job_name: h.job_name.clone(),
                job_comment: h.job_comment.clone(),
                binary: h.command_line.clone(),
                args: Vec::new(),
                status: h.status.clone(),
                req_nodes: h.req_nodes,
                req_cores: h.req_cores,
                req_memory: h.req_memory,
                assigned_workers: h.assigned_workers.clone(),
                walltime: 0,
                start_time: h.start_time,
                priority: 0,
                user_id: h.user_id.clone(),
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
                gres_req: h.gres_req.clone(),
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
            };
            let resp = JobInfoResponse::from(job);
            Json(resp).into_response()
        } else {
            (StatusCode::NOT_FOUND, "Job not found").into_response()
        }
    }
}

pub(super) async fn api_cancel_job(
    State(ctx): State<SharedContext>,
    Extension(principal): Extension<crate::auth::AuthenticatedPrincipal>,
    Path(id): Path<u64>,
) -> Response {
    let has_valid_role = principal
        .roles
        .iter()
        .any(|r| r == "admin" || r == "operator" || r == "submitter");
    if !has_valid_role {
        let _ = ctx
            .audit
            .log(&crate::audit::AuditEvent::auth_denied(
                &principal.user_id,
                "oidc",
                "insufficient_role_for_cancel",
            ))
            .await;
        return (StatusCode::FORBIDDEN, "Forbidden: insufficient role").into_response();
    }
    let is_submitter_only = principal.roles.iter().any(|r| r == "submitter")
        && !principal
            .roles
            .iter()
            .any(|r| r == "admin" || r == "operator");

    let mut state_lock = ctx.state.lock().await;

    if let Some(job) = state_lock.jobs.get(&id) {
        if is_submitter_only && job.user_id != principal.user_id {
            let _ = ctx
                .audit
                .log(&crate::audit::AuditEvent::auth_denied(
                    &principal.user_id,
                    "oidc",
                    "not_job_owner",
                ))
                .await;
            return (StatusCode::FORBIDDEN, "Forbidden: not the job owner").into_response();
        }
    } else {
        return (StatusCode::NOT_FOUND, "Job not found").into_response();
    }

    // Kill logic (same as handle_client)
    let head_worker_id = if let Some(job) = state_lock.jobs.get(&id) {
        job.assigned_workers.first().cloned()
    } else {
        None
    };

    if let Some(worker_id) = head_worker_id {
        let _ = crate::send_to_worker(
            &state_lock,
            &worker_id,
            Message::TerminateJob { job_id: id },
        );
    }

    finalize_job(&mut state_lock, id, JobStatus::Killed);
    let job_clone = state_lock.jobs.get(&id).cloned();
    state_lock.jobs.remove(&id);
    ctx.failed_jobs.fetch_add(1, Ordering::Relaxed);
    crate::sync_active_state_to_dashmaps(&ctx, &state_lock);

    drop(state_lock);

    if let Some(job) = job_clone {
        let usage = crate::job_to_usage(&job);
        let store = ctx.accounting_store.clone();
        tokio::spawn(async move {
            let _ = store.record_job(&usage).await;
        });
    }

    // P1-6.1 audit
    let _ = ctx
        .audit
        .log(&crate::audit::AuditEvent::job_mutation(
            "cancel",
            id,
            &principal.user_id,
        ))
        .await;

    (StatusCode::OK, "Job canceled").into_response()
}

pub(super) async fn api_get_job_steps(
    State(ctx): State<SharedContext>,
    Extension(principal): Extension<crate::auth::AuthenticatedPrincipal>,
    Path(id): Path<u64>,
) -> Response {
    let has_valid_role = principal
        .roles
        .iter()
        .any(|r| r == "admin" || r == "operator" || r == "submitter" || r == "viewer");
    if !has_valid_role {
        return (StatusCode::FORBIDDEN, "Forbidden: insufficient role").into_response();
    }
    let is_viewer_only = principal.roles.iter().any(|r| r == "viewer")
        && !principal
            .roles
            .iter()
            .any(|r| r == "admin" || r == "operator" || r == "submitter");

    let owns_job = {
        let state_lock = ctx.state.lock().await;
        if let Some(job) = state_lock.jobs.get(&id) {
            job.user_id == principal.user_id
        } else {
            drop(state_lock);
            let history = ctx
                .accounting_store
                .query_history(&veloce_common::HistoryFilter::All)
                .await
                .unwrap_or_default();
            if let Some(h) = history.iter().find(|h| h.job_id == id) {
                h.user_id == principal.user_id
            } else {
                false
            }
        }
    };

    if is_viewer_only && !owns_job {
        return (StatusCode::FORBIDDEN, "Forbidden").into_response();
    }

    let state_lock = ctx.state.lock().await;

    let steps: Vec<veloce_common::StepInfo> = state_lock
        .steps
        .iter()
        .filter(|((jid, _), _)| *jid == id)
        .map(|(_, s)| s.clone())
        .collect();
    Json(steps).into_response()
}

#[derive(Deserialize)]
pub(super) struct SubmitStepRequest {
    binary: String,
    args: Vec<String>,
    req_nodes: usize,
    req_cores: u32,
    ntasks: u32,
    #[serde(default)]
    inputs: Vec<veloce_common::FileHandle>,
    #[serde(default)]
    gres_req: std::collections::BTreeMap<String, u64>,
    #[serde(default)]
    env_vars: Vec<(String, String)>,
}

pub(super) async fn api_submit_step(
    State(ctx): State<SharedContext>,
    Extension(principal): Extension<crate::auth::AuthenticatedPrincipal>,
    Path(id): Path<u64>,
    Json(payload): Json<SubmitStepRequest>,
) -> Response {
    if !principal
        .roles
        .iter()
        .any(|r| r == "admin" || r == "operator" || r == "submitter")
    {
        return (StatusCode::FORBIDDEN, "Forbidden: insufficient role").into_response();
    }
    let is_submitter_only = principal.roles.iter().any(|r| r == "submitter")
        && !principal
            .roles
            .iter()
            .any(|r| r == "admin" || r == "operator");

    let mut state_lock = ctx.state.lock().await;

    if is_submitter_only {
        if let Some(job) = state_lock.jobs.get(&id) {
            if job.user_id != principal.user_id {
                return (StatusCode::FORBIDDEN, "Forbidden: not the job owner").into_response();
            }
        } else {
            return (StatusCode::NOT_FOUND, "Job not found").into_response();
        }
    }

    match submit_step(
        &mut state_lock,
        id,
        payload.binary,
        payload.args,
        payload.req_nodes,
        payload.req_cores,
        payload.ntasks,
        payload.inputs,
        payload.gres_req,
        payload.env_vars,
    ) {
        Ok(step_id) => (StatusCode::CREATED, step_id.to_string()).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, e).into_response(),
    }
}

#[derive(Deserialize)]
pub(super) struct LogParams {
    #[serde(default = "default_log_type")]
    pub r#type: String, // "stdout" or "stderr"
    pub offset: Option<u64>,
    pub length: Option<u64>,
    pub rank: Option<usize>,
}

pub(super) fn default_log_type() -> String {
    "stdout".to_string()
}

#[derive(Deserialize)]
pub(super) struct OutputContentParams {
    pub path: String,
    pub offset: Option<u64>,
    pub length: Option<u64>,
}

pub(super) async fn api_list_job_outputs(
    State(ctx): State<SharedContext>,
    Extension(principal): Extension<crate::auth::AuthenticatedPrincipal>,
    Path(id): Path<u64>,
) -> Response {
    let has_valid_role = principal
        .roles
        .iter()
        .any(|r| r == "admin" || r == "operator" || r == "submitter" || r == "viewer");
    if !has_valid_role {
        return (StatusCode::FORBIDDEN, "Forbidden: insufficient role").into_response();
    }
    let is_viewer_only = principal.roles.iter().any(|r| r == "viewer")
        && !principal
            .roles
            .iter()
            .any(|r| r == "admin" || r == "operator" || r == "submitter");

    let job_snapshot = {
        let state_lock = ctx.state.lock().await;
        state_lock.jobs.get(&id).cloned()
    };

    if let Some(job) = job_snapshot {
        if is_viewer_only && job.user_id != principal.user_id {
            return (StatusCode::FORBIDDEN, "Forbidden").into_response();
        }
        if !job.output_artifacts.is_empty() {
            return Json(job.output_artifacts).into_response();
        }
        let Some(worker_id) = job.assigned_workers.first().cloned() else {
            return Json(Vec::<veloce_common::JobOutputArtifact>::new()).into_response();
        };
        let request_id = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        let send_res = {
            let mut state_lock = ctx.state.lock().await;
            state_lock.log_requests.insert(request_id, response_tx);
            let msg = Message::ListJobOutputFiles {
                request_id,
                job_id: id,
                working_directory: Some(job.working_directory.clone()),
                container_asset: job.container_asset.clone(),
            };
            crate::send_to_worker(&state_lock, &worker_id, msg)
        };
        if send_res.is_err() {
            let mut state_lock = ctx.state.lock().await;
            state_lock.log_requests.remove(&request_id);
            return (StatusCode::BAD_GATEWAY, "Failed to send request to worker").into_response();
        }
        match timeout(Duration::from_secs(5), response_rx).await {
            Ok(Ok(Message::JobOutputFiles { artifacts, .. })) => Json(artifacts).into_response(),
            Ok(Ok(Message::Error(e))) => (StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
            Ok(Ok(_)) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Unexpected response type",
            )
                .into_response(),
            Ok(Err(_)) => (StatusCode::INTERNAL_SERVER_ERROR, "Channel closed").into_response(),
            Err(_) => {
                let mut state_lock = ctx.state.lock().await;
                state_lock.log_requests.remove(&request_id);
                (StatusCode::GATEWAY_TIMEOUT, "Timeout waiting for outputs").into_response()
            }
        }
    } else {
        let history = ctx
            .accounting_store
            .query_history(&HistoryFilter::Single(id))
            .await
            .unwrap_or_default();
        if let Some(h) = history.into_iter().next() {
            if is_viewer_only && h.user_id != principal.user_id {
                return (StatusCode::FORBIDDEN, "Forbidden").into_response();
            }
            Json(h.output_artifacts).into_response()
        } else {
            (StatusCode::NOT_FOUND, "Job not found").into_response()
        }
    }
}

pub(super) async fn api_get_job_output_content(
    State(ctx): State<SharedContext>,
    Extension(principal): Extension<crate::auth::AuthenticatedPrincipal>,
    Path(id): Path<u64>,
    Query(params): Query<OutputContentParams>,
) -> Response {
    let has_valid_role = principal
        .roles
        .iter()
        .any(|r| r == "admin" || r == "operator" || r == "submitter" || r == "viewer");
    if !has_valid_role {
        return (StatusCode::FORBIDDEN, "Forbidden: insufficient role").into_response();
    }
    let is_viewer_only = principal.roles.iter().any(|r| r == "viewer")
        && !principal
            .roles
            .iter()
            .any(|r| r == "admin" || r == "operator" || r == "submitter");

    let read_uploaded = |bytes: Vec<u8>, offset: u64, length: Option<u64>| {
        let offset = offset as usize;
        if offset < bytes.len() {
            if let Some(len) = length {
                let end = std::cmp::min(offset + len as usize, bytes.len());
                bytes[offset..end].to_vec()
            } else {
                bytes[offset..].to_vec()
            }
        } else {
            Vec::new()
        }
    };

    let job_snapshot = {
        let state_lock = ctx.state.lock().await;
        state_lock.jobs.get(&id).cloned()
    };

    if let Some(job) = job_snapshot {
        if is_viewer_only && job.user_id != principal.user_id {
            return (StatusCode::FORBIDDEN, "Forbidden").into_response();
        }
        if let Some(artifact) = job.output_artifacts.iter().find(|a| a.path == params.path) {
            if let Some(file_id) = &artifact.file_id {
                return match ctx.file_client.download_file_to_bytes(file_id).await {
                    Ok(bytes) => (
                        StatusCode::OK,
                        read_uploaded(bytes, params.offset.unwrap_or(0), params.length),
                    )
                        .into_response(),
                    Err(e) => (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("Failed to download output from fileserver: {}", e),
                    )
                        .into_response(),
                };
            }
        }

        let Some(worker_id) = job.assigned_workers.first().cloned() else {
            return (StatusCode::NOT_FOUND, "Job output not available").into_response();
        };
        let request_id = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        let send_res = {
            let mut state_lock = ctx.state.lock().await;
            state_lock.log_requests.insert(request_id, response_tx);
            let msg = Message::GetJobOutputFileChunk {
                request_id,
                job_id: id,
                path: params.path.clone(),
                offset: params.offset.unwrap_or(0),
                length: params.length,
                working_directory: Some(job.working_directory.clone()),
                container_asset: job.container_asset.clone(),
            };
            crate::send_to_worker(&state_lock, &worker_id, msg)
        };
        if send_res.is_err() {
            let mut state_lock = ctx.state.lock().await;
            state_lock.log_requests.remove(&request_id);
            return (StatusCode::BAD_GATEWAY, "Failed to send request to worker").into_response();
        }
        match timeout(Duration::from_secs(5), response_rx).await {
            Ok(Ok(Message::JobOutputFileChunk { content, .. })) => {
                (StatusCode::OK, content).into_response()
            }
            Ok(Ok(Message::Error(e))) => (StatusCode::BAD_REQUEST, e).into_response(),
            Ok(Ok(_)) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Unexpected response type",
            )
                .into_response(),
            Ok(Err(_)) => (StatusCode::INTERNAL_SERVER_ERROR, "Channel closed").into_response(),
            Err(_) => {
                let mut state_lock = ctx.state.lock().await;
                state_lock.log_requests.remove(&request_id);
                (StatusCode::GATEWAY_TIMEOUT, "Timeout waiting for output").into_response()
            }
        }
    } else {
        let history = ctx
            .accounting_store
            .query_history(&HistoryFilter::Single(id))
            .await
            .unwrap_or_default();
        if let Some(h) = history.into_iter().next() {
            if is_viewer_only && h.user_id != principal.user_id {
                return (StatusCode::FORBIDDEN, "Forbidden").into_response();
            }
            let Some(artifact) = h.output_artifacts.iter().find(|a| a.path == params.path) else {
                return (StatusCode::NOT_FOUND, "Output artifact not found").into_response();
            };
            let Some(file_id) = &artifact.file_id else {
                return (
                    StatusCode::NOT_FOUND,
                    "Output artifact has no uploaded file id",
                )
                    .into_response();
            };
            match ctx.file_client.download_file_to_bytes(file_id).await {
                Ok(bytes) => (
                    StatusCode::OK,
                    read_uploaded(bytes, params.offset.unwrap_or(0), params.length),
                )
                    .into_response(),
                Err(e) => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Failed to download output from fileserver: {}", e),
                )
                    .into_response(),
            }
        } else {
            (StatusCode::NOT_FOUND, "Job not found").into_response()
        }
    }
}

pub(super) async fn api_get_job_logs(
    State(ctx): State<SharedContext>,
    Extension(principal): Extension<crate::auth::AuthenticatedPrincipal>,
    Path(id): Path<u64>,
    Query(params): Query<LogParams>,
) -> Response {
    let has_valid_role = principal
        .roles
        .iter()
        .any(|r| r == "admin" || r == "operator" || r == "submitter" || r == "viewer");
    if !has_valid_role {
        return (StatusCode::FORBIDDEN, "Forbidden: insufficient role").into_response();
    }
    let is_viewer_only = principal.roles.iter().any(|r| r == "viewer")
        && !principal
            .roles
            .iter()
            .any(|r| r == "admin" || r == "operator" || r == "submitter");

    let mut state_lock = ctx.state.lock().await;

    // 1. First check if it's in active jobs
    if let Some(j) = state_lock.jobs.get(&id) {
        let job = j.clone();
        if is_viewer_only && job.user_id != principal.user_id {
            return (StatusCode::FORBIDDEN, "Forbidden").into_response();
        }
        let log_type = match params.r#type.to_lowercase().as_str() {
            "stderr" => LogType::Stderr,
            _ => LogType::Stdout,
        };

        if params.rank.is_none() && job.assigned_workers.len() > 1 {
            let tracking = state_lock.multinode_job_tracking.clone();
            let offset = params.offset.unwrap_or(0) as usize;
            let length = params.length.map(|l| l as usize);
            drop(state_lock);
            if let Some(text) = crate::job_logs::fetch_multinode_log_text(
                &ctx, &job, &tracking, &log_type, offset, length,
            )
            .await
            {
                return (StatusCode::OK, text).into_response();
            }
            state_lock = ctx.state.lock().await;
            if state_lock.jobs.get(&id).is_none() {
                return (StatusCode::NOT_FOUND, "Job not found").into_response();
            }
        }

        let file_id_opt = match log_type {
            LogType::Stdout => job.stdout_file_id.clone(),
            LogType::Stderr => job.stderr_file_id.clone(),
        };

        if let Some(file_id) = file_id_opt {
            drop(state_lock);
            match ctx.file_client.download_file_to_bytes(&file_id).await {
                Ok(bytes) => {
                    let text = String::from_utf8_lossy(&bytes).to_string();
                    let offset = params.offset.unwrap_or(0) as usize;
                    let length = params.length.map(|l| l as usize);
                    let result_str = if offset < text.len() {
                        if let Some(len) = length {
                            let end = std::cmp::min(offset + len, text.len());
                            text[offset..end].to_string()
                        } else {
                            text[offset..].to_string()
                        }
                    } else {
                        String::new()
                    };
                    return (StatusCode::OK, result_str).into_response();
                }
                Err(e) => {
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("Failed to download log from fileserver: {}", e),
                    )
                        .into_response();
                }
            }
        }

        let working_directory = job.working_directory.clone();
        let assigned_workers = job.assigned_workers.clone();
        let status = job.status.clone();

        if params.rank.is_none() && assigned_workers.len() > 1 {
            let worker_order =
                crate::job_logs::worker_order_for_logs(&job, &state_lock.multinode_job_tracking);
            let mut merged = String::new();
            for (rank, worker_id) in worker_order.iter().enumerate() {
                let request_id = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as u64;
                let (response_tx, response_rx) = tokio::sync::oneshot::channel();
                state_lock.log_requests.insert(request_id, response_tx);
                let _guard = LogRequestGuard {
                    request_id,
                    ctx: ctx.clone(),
                };
                let msg = Message::GetLogs {
                    request_id,
                    job_id: id,
                    log_type: log_type.clone(),
                    offset: 0,
                    length: None,
                    working_directory: Some(working_directory.clone()),
                    rank: None,
                };
                if crate::send_to_worker(&state_lock, worker_id, msg).is_err() {
                    continue;
                }
                drop(state_lock);
                if let Ok(Ok(Message::LogData { content, .. })) =
                    timeout(Duration::from_secs(5), response_rx).await
                {
                    if !content.is_empty() {
                        merged.push_str(&format!("=== rank {rank} ({worker_id}) ===\n"));
                        merged.push_str(&String::from_utf8_lossy(&content));
                        if !merged.ends_with('\n') {
                            merged.push('\n');
                        }
                    }
                }
                state_lock = ctx.state.lock().await;
            }
            if !merged.is_empty() {
                let offset = params.offset.unwrap_or(0) as usize;
                let length = params.length.map(|l| l as usize);
                let result_str = if offset < merged.len() {
                    if let Some(len) = length {
                        let end = (offset + len).min(merged.len());
                        merged[offset..end].to_string()
                    } else {
                        merged[offset..].to_string()
                    }
                } else {
                    String::new()
                };
                return (StatusCode::OK, result_str).into_response();
            }
        }

        // Active job log retrieval from worker
        // Determine worker
        let worker_id = if let Some(rank) = params.rank {
            if let Some(w_id) = assigned_workers.get(rank) {
                w_id.clone()
            } else {
                return (StatusCode::BAD_REQUEST, "Invalid rank for job").into_response();
            }
        } else if let Some(w_id) = assigned_workers.first() {
            w_id.clone()
        } else {
            if matches!(status, JobStatus::Pending) {
                return (StatusCode::OK, "").into_response();
            }
            return (
                StatusCode::NOT_FOUND,
                "Job logs not available (no worker assigned)",
            )
                .into_response();
        };

        // Get worker handle
        // Prepare request
        let request_id = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();

        state_lock.log_requests.insert(request_id, response_tx);

        // Use RAII guard to ensure cleanup on drop/cancel/timeout
        let _guard = LogRequestGuard {
            request_id,
            ctx: ctx.clone(),
        };

        let msg = Message::GetLogs {
            request_id,
            job_id: id,
            log_type,
            offset: params.offset.unwrap_or(0),
            length: params.length,
            working_directory: Some(working_directory),
            rank: params.rank,
        };

        let send_res = crate::send_to_worker(&state_lock, &worker_id, msg);

        // Drop lock before await
        drop(state_lock);

        if send_res.is_err() {
            let mut state_lock = ctx.state.lock().await;
            state_lock.log_requests.remove(&request_id);
            return (StatusCode::BAD_GATEWAY, "Failed to send request to worker").into_response();
        }

        // Wait for response with timeout
        match timeout(Duration::from_secs(5), response_rx).await {
            Ok(Ok(Message::LogData { content, .. })) => {
                let text = String::from_utf8_lossy(&content).to_string();
                (StatusCode::OK, text).into_response()
            }
            Ok(Ok(Message::Error(e))) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Worker error: {}", e),
            )
                .into_response(),
            Ok(Ok(_)) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Unexpected response type",
            )
                .into_response(),
            Ok(Err(_)) => (StatusCode::INTERNAL_SERVER_ERROR, "Channel closed").into_response(),
            Err(_) => (StatusCode::GATEWAY_TIMEOUT, "Timeout waiting for logs").into_response(),
        }
    } else {
        // Drop lock to query history asynchronously
        drop(state_lock);

        let history = ctx
            .accounting_store
            .query_history(&veloce_common::HistoryFilter::All)
            .await
            .unwrap_or_default();
        if let Some(h) = history.iter().find(|h| h.job_id == id) {
            if is_viewer_only && h.user_id != principal.user_id {
                return (StatusCode::FORBIDDEN, "Forbidden").into_response();
            }
            let log_type = match params.r#type.to_lowercase().as_str() {
                "stderr" => LogType::Stderr,
                _ => LogType::Stdout,
            };

            let file_id_opt = match log_type {
                LogType::Stdout => h.stdout_file_id.clone(),
                LogType::Stderr => h.stderr_file_id.clone(),
            };

            if let Some(file_id) = file_id_opt {
                match ctx.file_client.download_file_to_bytes(&file_id).await {
                    Ok(bytes) => {
                        let text = String::from_utf8_lossy(&bytes).to_string();
                        let offset = params.offset.unwrap_or(0) as usize;
                        let length = params.length.map(|l| l as usize);
                        let result_str = if offset < text.len() {
                            if let Some(len) = length {
                                let end = std::cmp::min(offset + len, text.len());
                                text[offset..end].to_string()
                            } else {
                                text[offset..].to_string()
                            }
                        } else {
                            String::new()
                        };
                        (StatusCode::OK, result_str).into_response()
                    }
                    Err(e) => (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("Failed to download log from fileserver: {}", e),
                    )
                        .into_response(),
                }
            } else {
                (
                    StatusCode::NOT_FOUND,
                    "Job log file ID not found in history",
                )
                    .into_response()
            }
        } else {
            (StatusCode::NOT_FOUND, "Job not found").into_response()
        }
    }
}

pub(super) struct LogRequestGuard {
    request_id: u64,
    ctx: SharedContext,
}

impl Drop for LogRequestGuard {
    fn drop(&mut self) {
        let ctx = self.ctx.clone();
        let request_id = self.request_id;
        // Since drop is synchronous, we spawn a task to re-acquire the lock and clean up.
        tokio::spawn(async move {
            let mut state_lock = ctx.state.lock().await;
            if state_lock.log_requests.remove(&request_id).is_some() {
                info!("Pruned stale/finished log request {}", request_id);
            }
        });
    }
}

pub(super) async fn api_get_queue_gap(
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
    let mut gap = veloce_common::QueueResourceGap::default();

    for job_id in &state_lock.queue {
        if let Some(job) = state_lock.jobs.get(job_id) {
            if matches!(job.status, JobStatus::Pending) {
                gap.pending_jobs += 1;
                gap.pending_cores += job.req_cores * job.req_nodes as u32;
                gap.pending_memory_mb += job.req_memory * job.req_nodes as u64;
                gap.pending_nodes += job.req_nodes;
            }
        }
    }

    Json(gap).into_response()
}

pub(super) async fn api_internal_launch(
    State(ctx): State<SharedContext>,
    axum::Extension(config): axum::Extension<Config>,
    Json(payload): Json<LaunchRequest>,
) -> impl IntoResponse {
    let mut state_lock = ctx.state.lock().await;

    // 1. Authenticate Request
    let (job_id, secret, user_id, env_vars) = match state_lock.jobs.get(&payload.job_id) {
        Some(j) if j.secret == payload.secret => (
            j.id,
            j.secret.clone(),
            j.user_id.clone(),
            j.env_vars.clone(),
        ),
        _ => return (StatusCode::UNAUTHORIZED, "Invalid Job ID or Secret").into_response(),
    };

    // 2. Identify Target Worker ID by Hostname/IP/ID
    let target_worker_id = state_lock
        .workers
        .iter()
        .find(|(id, w)| {
            **id == payload.hostname
                || w.hostname == payload.hostname
                || w.addr.ip().to_string() == payload.hostname
        })
        .map(|(id, _)| id.clone());

    let worker_id = match target_worker_id {
        Some(id) => {
            let is_connected = state_lock
                .workers
                .get(&id)
                .map(|w| w.connected)
                .unwrap_or(false);
            if is_connected {
                id
            } else {
                return (StatusCode::NOT_FOUND, "Target worker disconnected").into_response();
            }
        }
        _ => return (StatusCode::NOT_FOUND, "Target worker not found").into_response(),
    };

    // 3. Assign unique Step ID and register waiter
    let step_id = state_lock.next_step_id;
    state_lock.next_step_id += 1;

    let (wait_tx, wait_rx) = tokio::sync::oneshot::channel();
    state_lock
        .step_waiters
        .entry((job_id, step_id))
        .or_default()
        .push(wait_tx);

    let (output_tx, output_rx) = tokio::sync::mpsc::unbounded_channel();
    state_lock
        .step_output_waiters
        .entry((job_id, step_id))
        .or_default()
        .push(output_tx);

    let controller_url = format!("https://localhost:{}", config.api_port.unwrap_or(8080));

    let msg = Message::RunStep {
        parent_job_id: job_id,
        step_id,
        binary: payload.binary.trim().to_string(),
        args: payload
            .args
            .into_iter()
            .map(|s| s.trim().to_string())
            .collect(),
        node_list: vec![payload.hostname.clone()],
        assigned_cores: None,
        working_directory: payload.working_directory,
        user_id,
        assigned_ranks: vec![0], // Internal launch usually assumes single rank on target
        total_ranks: 1,
        inputs: Vec::new(),
        allocated_gres: std::collections::HashMap::new(),
        gres_req: std::collections::BTreeMap::new(),
        secret,
        controller_url,
        env_vars,
    };

    let send_res = crate::send_to_worker(&state_lock, &worker_id, msg);

    // Drop lock before await
    drop(state_lock);

    if let Err(e) = send_res {
        error!("Failed to send launch message to worker: {}", e);
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Worker communication failure",
        )
            .into_response();
    }

    // 4. Stream output and wait for completion
    let stream = futures::stream::unfold(
        (output_rx, wait_rx, false),
        move |(mut output_rx, mut wait_rx, finished)| async move {
            if finished {
                return None;
            }
            tokio::select! {
                // 1. Check for stdout/stderr data from worker
                msg_opt = output_rx.recv() => {
                    if let Some((is_stderr, data)) = msg_opt {
                        let stream_type = if is_stderr { 2u8 } else { 1u8 };
                        let len = data.len() as u32;
                        let mut chunk = Vec::with_capacity(1 + 4 + data.len());
                        chunk.push(stream_type);
                        chunk.extend_from_slice(&len.to_be_bytes());
                        chunk.extend_from_slice(&data);
                        Some((Ok::<_, std::io::Error>(axum::body::Bytes::from(chunk)), (output_rx, wait_rx, false)))
                    } else {
                        // Channel closed (worker disconnected or done)
                        // Just wait for wait_rx to finish
                        match wait_rx.await {
                            Ok(exit_code) => {
                                let mut chunk = Vec::with_capacity(1 + 4 + 4);
                                chunk.push(3u8); // Stream type 3 = ExitCode
                                chunk.extend_from_slice(&4u32.to_be_bytes());
                                chunk.extend_from_slice(&exit_code.to_be_bytes());
                                let (_, dummy_rx) = tokio::sync::oneshot::channel();
                                Some((Ok(axum::body::Bytes::from(chunk)), (output_rx, dummy_rx, true)))
                            }
                            Err(_) => {
                                let mut chunk = Vec::with_capacity(1 + 4 + 4);
                                chunk.push(3u8); // Stream type 3 = ExitCode
                                chunk.extend_from_slice(&4u32.to_be_bytes());
                                chunk.extend_from_slice(&(-1i32).to_be_bytes()); // Error code -1
                                let (_, dummy_rx) = tokio::sync::oneshot::channel();
                                Some((Ok(axum::body::Bytes::from(chunk)), (output_rx, dummy_rx, true)))
                            }
                        }
                    }
                }
                // 2. Check for process termination
                exit_res = &mut wait_rx => {
                    let exit_code = exit_res.unwrap_or(-1);

                    // Drain any remaining output in output_rx first
                    let mut chunk = Vec::new();
                    while let Ok((is_stderr, data)) = output_rx.try_recv() {
                        let stream_type = if is_stderr { 2u8 } else { 1u8 };
                        let len = data.len() as u32;
                        chunk.push(stream_type);
                        chunk.extend_from_slice(&len.to_be_bytes());
                        chunk.extend_from_slice(&data);
                    }

                    chunk.push(3u8); // Stream type 3 = ExitCode
                    chunk.extend_from_slice(&4u32.to_be_bytes());
                    chunk.extend_from_slice(&exit_code.to_be_bytes());
                    let (_, dummy_rx) = tokio::sync::oneshot::channel();
                    Some((Ok(axum::body::Bytes::from(chunk)), (output_rx, dummy_rx, true)))
                }
            }
        },
    );

    let body = axum::body::Body::from_stream(stream);
    Response::builder()
        .header("content-type", "application/octet-stream")
        .body(body)
        .unwrap()
        .into_response()
}
