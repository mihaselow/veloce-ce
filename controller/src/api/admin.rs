//! API module: admin.rs

use crate::{Config, SharedContext};
use anyhow::Result;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Extension, Json,
};
use log::info;
use serde::{Deserialize, Serialize};
use tokio::time::Duration;

#[derive(Deserialize)]
pub(super) struct RestartQuery {
    target: Option<String>,
}

pub(super) async fn api_system_restart(
    State(ctx): State<SharedContext>,
    Extension(principal): Extension<crate::auth::AuthenticatedPrincipal>,
    Query(query): Query<RestartQuery>,
) -> Result<StatusCode, (StatusCode, String)> {
    if !principal.roles.iter().any(|r| r == "admin") {
        let _ = ctx
            .audit
            .log(&crate::audit::AuditEvent::auth_denied(
                &principal.user_id,
                "oidc",
                "admin_required_for_restart",
            ))
            .await;
        return Err((
            StatusCode::FORBIDDEN,
            "Forbidden: admin role required".to_string(),
        ));
    }
    let local_hostname = gethostname::gethostname()
        .into_string()
        .unwrap_or_else(|_| "controller".to_string());
    let target = query.target.unwrap_or_else(|| "controller".to_string());
    let normalized_target = crate::normalize_component_id(&target);

    if target == "controller" || target == local_hostname || normalized_target == local_hostname {
        info!(
            "Controller restart requested for local node ({}).",
            local_hostname
        );
        // P1-6.1 audit
        let _ = ctx
            .audit
            .log(&crate::audit::AuditEvent::system_restart(
                &principal.user_id,
            ))
            .await;
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(1000)).await;
            veloce_common::utils::self_restart();
        });
        Ok(StatusCode::OK)
    } else {
        // Forward restart request to peer controller over Noise P2P channel
        let (peer_tx, target_key) = {
            let state_lock = ctx.state.lock().await;
            if let Some(tx) = state_lock.peers.get(&target).cloned() {
                (Some(tx), target.clone())
            } else if let Some(tx) = state_lock.peers.get(&normalized_target).cloned() {
                (Some(tx), normalized_target.clone())
            } else {
                (None, "".to_string())
            }
        };

        if let Some(tx) = peer_tx {
            info!(
                "Forwarding controller restart request to peer: {}",
                target_key
            );
            if tx
                .try_send(veloce_common::PeerMessage::Restart {
                    delay_ms: 1000,
                    reason: "Restarted via leader proxy".to_string(),
                })
                .is_ok()
            {
                // P1-6.1 audit (proxied restart)
                let _ = ctx
                    .audit
                    .log(&crate::audit::AuditEvent::system_restart(
                        &principal.user_id,
                    ))
                    .await;
                Ok(StatusCode::OK)
            } else {
                Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Failed to send restart message to peer {}", target_key),
                ))
            }
        } else {
            Err((
                StatusCode::NOT_FOUND,
                format!("Peer controller {} not found or offline", target),
            ))
        }
    }
}

pub(super) async fn api_fileserver_restart(
    axum::Extension(config): axum::Extension<Config>,
    Extension(principal): Extension<crate::auth::AuthenticatedPrincipal>,
) -> impl IntoResponse {
    if !principal.roles.iter().any(|r| r == "admin") {
        return (StatusCode::FORBIDDEN, "Forbidden: admin role required").into_response();
    }
    if let (Some(url), Some(key)) = (config.fileserver_url, config.fileserver_api_key) {
        let client = reqwest::Client::builder()
            .danger_accept_invalid_certs(!cfg!(feature = "production"))
            .build()
            .unwrap();

        let restart_url = format!("{}/api/v1/system/restart", url);
        match client
            .post(&restart_url)
            .header("X-API-KEY", &key)
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => {
                (StatusCode::OK, "Restart initiated").into_response()
            }
            Ok(resp) => (
                StatusCode::BAD_GATEWAY,
                format!("Fileserver returned error: {}", resp.status()),
            )
                .into_response(),
            Err(e) => (
                StatusCode::BAD_GATEWAY,
                format!("Failed to reach fileserver: {}", e),
            )
                .into_response(),
        }
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "Fileserver not configured").into_response()
    }
}

#[derive(Deserialize)]
pub(super) struct IssueComponentTokenRequest {
    pub component_id: String,
    pub component_type: String,
    pub roles: Vec<String>,
}

#[derive(Serialize, Deserialize)]
pub(super) struct IssueComponentTokenResponse {
    pub component_id: String,
    pub token: String,
}

pub(super) async fn api_issue_component_token(
    State(ctx): State<SharedContext>,
    Extension(principal): Extension<crate::auth::AuthenticatedPrincipal>,
    Json(payload): Json<IssueComponentTokenRequest>,
) -> impl IntoResponse {
    if !principal.roles.iter().any(|r| r == "admin") {
        return (StatusCode::FORBIDDEN, "Forbidden: admin role required").into_response();
    }
    let component_type = match payload
        .component_type
        .parse::<veloce_common::auth::ComponentType>()
    {
        Ok(t) => t,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                "Invalid component type. Must be 'worker', 'client', or 'peer'",
            )
                .into_response()
        }
    };

    match ctx
        .component_registry
        .issue_token(&payload.component_id, component_type, &payload.roles)
        .await
    {
        Ok(token) => {
            // P1-6.1 audit
            let _ = ctx
                .audit
                .log(&crate::audit::AuditEvent::component_token_action(
                    "issued",
                    &payload.component_id,
                    &principal.user_id,
                ))
                .await;
            (
                StatusCode::CREATED,
                Json(IssueComponentTokenResponse {
                    component_id: payload.component_id,
                    token,
                }),
            )
                .into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to issue token: {:#}", e),
        )
            .into_response(),
    }
}

pub(super) async fn api_list_components(
    State(ctx): State<SharedContext>,
    Extension(principal): Extension<crate::auth::AuthenticatedPrincipal>,
) -> impl IntoResponse {
    if !principal.roles.iter().any(|r| r == "admin") {
        return (StatusCode::FORBIDDEN, "Forbidden: admin role required").into_response();
    }
    match ctx.component_registry.list_components().await {
        Ok(list) => (StatusCode::OK, Json(list)).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to list components: {:?}", e),
        )
            .into_response(),
    }
}

pub(super) async fn api_rotate_component_token(
    State(ctx): State<SharedContext>,
    Extension(principal): Extension<crate::auth::AuthenticatedPrincipal>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    if !principal.roles.iter().any(|r| r == "admin") {
        return (StatusCode::FORBIDDEN, "Forbidden: admin role required").into_response();
    }
    match ctx.component_registry.rotate(&id).await {
        Ok(token) => {
            // P1-6.1 audit
            let _ = ctx
                .audit
                .log(&crate::audit::AuditEvent::component_token_action(
                    "rotated",
                    &id,
                    &principal.user_id,
                ))
                .await;
            (
                StatusCode::OK,
                Json(IssueComponentTokenResponse {
                    component_id: id,
                    token,
                }),
            )
                .into_response()
        }
        Err(e) => {
            if e.to_string().contains("not found") {
                (StatusCode::NOT_FOUND, e.to_string()).into_response()
            } else {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Failed to rotate token: {:#}", e),
                )
                    .into_response()
            }
        }
    }
}

pub(super) async fn api_revoke_component(
    State(ctx): State<SharedContext>,
    Extension(principal): Extension<crate::auth::AuthenticatedPrincipal>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    if !principal.roles.iter().any(|r| r == "admin") {
        return (StatusCode::FORBIDDEN, "Forbidden: admin role required").into_response();
    }
    match ctx.component_registry.revoke(&id).await {
        Ok(_) => {
            // P1-6.1 audit
            let _ = ctx
                .audit
                .log(&crate::audit::AuditEvent::component_token_action(
                    "revoked",
                    &id,
                    &principal.user_id,
                ))
                .await;
            (
                StatusCode::OK,
                Json(serde_json::json!({ "status": "revoked" })),
            )
                .into_response()
        }
        Err(e) => {
            if e.to_string().contains("not found") {
                (StatusCode::NOT_FOUND, e.to_string()).into_response()
            } else {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Failed to revoke component: {:#}", e),
                )
                    .into_response()
            }
        }
    }
}

#[derive(Deserialize)]
pub(super) struct AuditQuery {
    event_type: Option<String>,
    principal: Option<String>,
    limit: Option<i64>,
}

pub(super) async fn api_list_audit_events(
    State(ctx): State<SharedContext>,
    Extension(principal): Extension<crate::auth::AuthenticatedPrincipal>,
    Query(q): Query<AuditQuery>,
) -> impl IntoResponse {
    if !principal.roles.iter().any(|r| r == "admin") {
        return (StatusCode::FORBIDDEN, "Forbidden: admin role required").into_response();
    }

    match ctx
        .audit
        .list(q.event_type.as_deref(), q.principal.as_deref(), q.limit)
        .await
    {
        Ok(events) => (StatusCode::OK, Json(events)).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to query audit: {:#}", e),
        )
            .into_response(),
    }
}
