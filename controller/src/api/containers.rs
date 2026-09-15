//! API module: containers.rs

use crate::SharedContext;
use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    Extension, Json,
};
use log::error;
use serde::Deserialize;
use uuid::Uuid;

pub(super) async fn api_list_solvers(
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
    let state = ctx.state.lock().await;
    Json(state.solver_configs.values().cloned().collect::<Vec<_>>()).into_response()
}

pub(super) async fn api_register_solver(
    State(ctx): State<SharedContext>,
    Extension(principal): Extension<crate::auth::AuthenticatedPrincipal>,
    Json(config): Json<veloce_common::SolverConfig>,
) -> impl IntoResponse {
    if !principal
        .roles
        .iter()
        .any(|r| r == "admin" || r == "operator")
    {
        return (StatusCode::FORBIDDEN, "Forbidden: insufficient role").into_response();
    }
    let mut state = ctx.state.lock().await;
    state
        .solver_configs
        .insert(config.name.clone(), config.clone());

    let conf_dir = std::path::Path::new("controller/conf/solvers");
    if !conf_dir.exists() {
        let _ = std::fs::create_dir_all(conf_dir);
    }

    let path = conf_dir.join(format!("{}.json", config.name));
    if let Ok(json) = serde_json::to_string_pretty(&config) {
        if let Err(e) = std::fs::write(&path, json) {
            error!("Failed to save solver profile: {}", e);
        }
    }

    (StatusCode::OK, "Solver profile registered").into_response()
}

#[derive(Deserialize)]
pub(super) struct RegisterContainerRequest {
    pub name: String,
    pub image_uri: String,
    pub manifest: veloce_common::apptainer::SolverManifest,
}

pub(super) async fn api_register_container(
    State(ctx): State<SharedContext>,
    Extension(principal): Extension<crate::auth::AuthenticatedPrincipal>,
    Json(req): Json<RegisterContainerRequest>,
) -> impl IntoResponse {
    if !principal.roles.iter().any(|r| r == "admin") {
        return (StatusCode::FORBIDDEN, "Forbidden: admin role required").into_response();
    }
    let id = match ctx.container_store.get_container(&req.name).await {
        Ok(Some(existing)) => existing.id,
        _ => Uuid::new_v4().to_string(),
    };

    let asset = veloce_common::apptainer::ContainerAsset {
        id: id.clone(),
        name: req.name,
        image_uri: req.image_uri,
        manifest: req.manifest,
    };

    match ctx.container_store.add_container(&asset).await {
        Ok(_) => (StatusCode::CREATED, Json(serde_json::json!({ "id": id }))).into_response(),
        Err(e) => {
            error!("Failed to register container: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to register container",
            )
                .into_response()
        }
    }
}

// Since get_container isn't all containers yet, wait, we need list_containers.
// Let's modify ContainerStore to add list_containers.

pub(super) async fn api_list_containers(
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
    match ctx.container_store.list_containers().await {
        Ok(mut assets) => {
            let state = ctx.state.lock().await;
            for (name, config) in &state.solver_configs {
                assets.push(veloce_common::apptainer::ContainerAsset {
                    id: format!("virtual://{}", name),
                    name: config.name.clone(),
                    image_uri: format!("virtual://{}", name),
                    manifest: veloce_common::apptainer::SolverManifest {
                        manifest_version: "1.0".to_string(),
                        solver_identity: veloce_common::apptainer::SolverIdentity {
                            vendor: config.name.clone(),
                            product: config
                                .description
                                .clone()
                                .unwrap_or_else(|| "Legacy".to_string()),
                            version: "Virtual".to_string(),
                            capabilities: config.default_batch_flags.clone(),
                        },
                        execution: veloce_common::apptainer::Execution {
                            entrypoint: config.executable.clone(),
                            launch_wrapper: None,
                            command_template: config
                                .command_template
                                .clone()
                                .unwrap_or_else(|| "{{entrypoint}} {{custom_args}}".to_string()),
                            environment_vars: std::collections::HashMap::new(),
                        },
                        file_handling: veloce_common::apptainer::FileHandling {
                            working_dir: "/scratch".to_string(),
                            input_mapping: Vec::new(),
                            output_collection: Vec::new(),
                        },
                        parameter_mapping: config.parameter_mapping.clone(),
                        vnc_enabled: false,
                    },
                });
            }
            (StatusCode::OK, Json(assets)).into_response()
        }
        Err(e) => {
            error!("Failed to list containers: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to list containers",
            )
                .into_response()
        }
    }
}

pub(super) async fn api_delete_container(
    State(ctx): State<SharedContext>,
    Extension(principal): Extension<crate::auth::AuthenticatedPrincipal>,
    axum::extract::Path(name): axum::extract::Path<String>,
) -> impl IntoResponse {
    if !principal.roles.iter().any(|r| r == "admin") {
        return (StatusCode::FORBIDDEN, "Forbidden: admin role required").into_response();
    }
    if let Ok(Some(container)) = ctx.container_store.get_container(&name).await {
        if container.image_uri.starts_with("s3://") {
            let _ = ctx.file_client.delete_s3_file(&container.image_uri).await;
        }
    }

    match ctx.container_store.delete_container(&name).await {
        Ok(_) => (
            StatusCode::OK,
            Json(serde_json::json!({ "status": "deleted" })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}
