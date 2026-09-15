//! API module: api_middleware.rs

use crate::SharedContext;
use anyhow::Result;
use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use log::{error, warn};
use std::time::{SystemTime, UNIX_EPOCH};
use subtle::ConstantTimeEq;

pub async fn leader_only_middleware(
    State(ctx): State<SharedContext>,
    request: axum::extract::Request,
    next: Next,
) -> Response {
    // Leadership is now enforced for ALL API requests to ensure data consistency
    // and prevent split-view issues where the Follower doesn't see ephemeral
    // state (like active worker connections).

    let role = {
        let state = ctx.state.lock().await;
        state.role
    };

    if role != crate::ControllerRole::Leader {
        // Followers return 503 so nginx (proxy_next_upstream http_503) can retry another
        // controller that may be the elected leader. Do not HTTP-proxy to the leader here:
        // that path returned 502 on transient leader reachability failures, and nginx does
        // not retry 502. Ephemeral state (ws tickets, worker channels) lives on the leader.
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "Not a leader, retry against leader",
        )
            .into_response();
    }
    next.run(request).await
}

pub async fn auth_middleware(
    State(ctx): State<SharedContext>,
    headers: HeaderMap,
    mut request: axum::extract::Request,
    next: Next,
) -> Response {
    if request.uri().path() == "/api/v1/internal/launch" {
        return next.run(request).await;
    }

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let is_websocket = headers
        .get("Upgrade")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_lowercase() == "websocket")
        .unwrap_or(false);

    let is_follower = {
        let state = ctx.state.lock().await;
        state.role == crate::ControllerRole::Follower
    };

    // Follower forwards WebSocket connections to Leader, which will authenticate them.
    if is_follower && is_websocket {
        return next.run(request).await;
    }

    let mut principal: Option<crate::auth::AuthenticatedPrincipal> = None;

    // 1. Check WebSocket ticket in query params
    if let Some(q) = request.uri().query() {
        let ticket_opt = q
            .split('&')
            .find(|p| p.starts_with("ticket="))
            .and_then(|p| p.split('=').nth(1))
            .map(|s| {
                urlencoding::decode(s)
                    .map(|cow| cow.into_owned())
                    .unwrap_or_else(|_| s.to_string())
            });

        if let Some(ticket_str) = ticket_opt {
            if let Some(ticket) = ctx.ws_tickets.get(&ticket_str) {
                if ticket.expires_at >= now {
                    principal = Some(crate::auth::AuthenticatedPrincipal {
                        user_id: ticket.user_id.clone(),
                        roles: ticket.roles.clone(),
                        auth_method: crate::auth::AuthMethod::WsTicket,
                    });
                }
            }
        }
    }

    // 2. Check Authorization Bearer header or Cookie
    if principal.is_none() {
        let bearer_opt = headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| {
                if s.starts_with("Bearer ") {
                    Some(s["Bearer ".len()..].trim())
                } else {
                    None
                }
            });

        let token_opt = bearer_opt
            .map(|s| s.to_string())
            .or_else(|| crate::auth::get_cookie(&headers, "veloce_session_token"));

        if let Some(token) = token_opt {
            let app_url = std::env::var("FUSIONAUTH_APP_URL")
                .unwrap_or_else(|_| "http://localhost:9011".to_string());
            if let Ok(p) = crate::auth::verify_jwt(&ctx, &token, &app_url).await {
                principal = Some(p);
            }
        }
    }

    // 3. Check X-API-KEY header
    if principal.is_none() {
        let api_key = headers.get("X-API-KEY").and_then(|val| val.to_str().ok());

        if let Some(key) = api_key {
            // Check cluster secret separation
            let mut is_cluster_secret = false;
            if let Some(ref secret) = ctx.config.cluster_secret {
                if key.len() == secret.len()
                    && key.as_bytes().ct_eq(secret.as_bytes()).unwrap_u8() == 1
                {
                    is_cluster_secret = true;
                }
            }

            if is_cluster_secret {
                let allow_insecure = std::env::var("VELOCE_ALLOW_INSECURE")
                    .map(|v| v == "true")
                    .unwrap_or(false);
                if allow_insecure {
                    warn!("Using cluster secret (VELOCE_SECRET) as REST X-API-KEY is deprecated.");
                } else {
                    error!("Use of cluster secret (VELOCE_SECRET) as REST X-API-KEY is denied in hardened mode.");
                    return (StatusCode::UNAUTHORIZED, "Unauthorized").into_response();
                }
            }

            // Check global api_key
            let mut global_match = false;
            if let Some(ref secret) = ctx.config.api_key {
                if key.len() == secret.len()
                    && key.as_bytes().ct_eq(secret.as_bytes()).unwrap_u8() == 1
                {
                    global_match = true;
                }
            }

            if global_match {
                principal = Some(crate::auth::AuthenticatedPrincipal {
                    user_id: "admin".to_string(),
                    roles: vec!["admin".to_string()],
                    auth_method: crate::auth::AuthMethod::ApiKey,
                });
            } else {
                let mut static_match = None;
                if let Some(ref static_keys) = ctx.config.api_keys {
                    let presented_hash = crate::component_registry::hash_token(key);
                    for static_key in static_keys {
                        if static_key
                            .key_hash
                            .as_bytes()
                            .ct_eq(presented_hash.as_bytes())
                            .unwrap_u8()
                            == 1
                        {
                            static_match = Some(crate::auth::AuthenticatedPrincipal {
                                user_id: static_key
                                    .linked_component_id
                                    .clone()
                                    .unwrap_or_else(|| static_key.id.clone()),
                                roles: static_key.roles.clone(),
                                auth_method: crate::auth::AuthMethod::ApiKey,
                            });
                            break;
                        }
                    }
                }

                if let Some(p) = static_match {
                    principal = Some(p);
                } else {
                    // Query component registry
                    match ctx.component_registry.validate_token(key).await {
                        Ok(Some(comp)) => {
                            principal = Some(crate::auth::AuthenticatedPrincipal {
                                user_id: comp.component_id,
                                roles: comp.roles,
                                auth_method: crate::auth::AuthMethod::ApiKey,
                            });
                        }
                        Ok(None) => {}
                        Err(e) => {
                            log::error!(
                                "Component registry query failed in auth middleware: {}",
                                e
                            );
                        }
                    }
                }
            }
        }
    }

    // 4. Support legacy query parameters only if insecure-query-auth feature is active
    #[cfg(feature = "insecure-query-auth")]
    if principal.is_none() {
        if let Some(q) = request.uri().query() {
            let query_key = q
                .split('&')
                .find(|p| {
                    p.starts_with("token=")
                        || p.starts_with("api_key=")
                        || p.starts_with("X-API-KEY=")
                })
                .and_then(|p| p.split('=').nth(1))
                .map(|s| {
                    urlencoding::decode(s)
                        .map(|cow| cow.into_owned())
                        .unwrap_or_else(|_| s.to_string())
                });

            if let Some(key) = query_key {
                // Check cluster secret separation
                let mut is_cluster_secret = false;
                if let Some(ref secret) = ctx.config.cluster_secret {
                    if key.len() == secret.len()
                        && key.as_bytes().ct_eq(secret.as_bytes()).unwrap_u8() == 1
                    {
                        is_cluster_secret = true;
                    }
                }

                if is_cluster_secret {
                    let allow_insecure = std::env::var("VELOCE_ALLOW_INSECURE")
                        .map(|v| v == "true")
                        .unwrap_or(false);
                    if allow_insecure {
                        warn!("Using cluster secret (VELOCE_SECRET) as legacy query parameter API key is deprecated.");
                    } else {
                        error!("Use of cluster secret (VELOCE_SECRET) as legacy query parameter API key is denied in hardened mode.");
                        return (StatusCode::UNAUTHORIZED, "Unauthorized").into_response();
                    }
                }

                let mut global_match = false;
                if let Some(ref secret) = ctx.config.api_key {
                    if key.len() == secret.len()
                        && key.as_bytes().ct_eq(secret.as_bytes()).unwrap_u8() == 1
                    {
                        global_match = true;
                    }
                }

                if global_match {
                    principal = Some(crate::auth::AuthenticatedPrincipal {
                        user_id: "admin".to_string(),
                        roles: vec!["admin".to_string()],
                        auth_method: crate::auth::AuthMethod::ApiKey,
                    });
                } else {
                    let mut static_match = None;
                    if let Some(ref static_keys) = ctx.config.api_keys {
                        let presented_hash = crate::component_registry::hash_token(&key);
                        for static_key in static_keys {
                            if static_key
                                .key_hash
                                .as_bytes()
                                .ct_eq(presented_hash.as_bytes())
                                .unwrap_u8()
                                == 1
                            {
                                static_match = Some(crate::auth::AuthenticatedPrincipal {
                                    user_id: static_key
                                        .linked_component_id
                                        .clone()
                                        .unwrap_or_else(|| static_key.id.clone()),
                                    roles: static_key.roles.clone(),
                                    auth_method: crate::auth::AuthMethod::ApiKey,
                                });
                                break;
                            }
                        }
                    }

                    if let Some(p) = static_match {
                        principal = Some(p);
                    } else {
                        match ctx.component_registry.validate_token(&key).await {
                            Ok(Some(comp)) => {
                                principal = Some(crate::auth::AuthenticatedPrincipal {
                                    user_id: comp.component_id,
                                    roles: comp.roles,
                                    auth_method: crate::auth::AuthMethod::ApiKey,
                                });
                            }
                            Ok(None) => {}
                            Err(e) => {
                                log::error!(
                                    "Component registry query failed in auth middleware: {}",
                                    e
                                );
                            }
                        }
                    }
                }
            }
        }
    }

    if principal.is_none() && ctx.config.api_key.is_none() {
        principal = Some(crate::auth::AuthenticatedPrincipal {
            user_id: "anonymous".to_string(),
            roles: vec!["admin".to_string()],
            auth_method: crate::auth::AuthMethod::ApiKey,
        });
    }

    if let Some(p) = principal {
        request.extensions_mut().insert(p);
        next.run(request).await
    } else {
        (StatusCode::UNAUTHORIZED, "Unauthorized").into_response()
    }
}

pub fn validate_and_redeem_ticket(
    ctx: &SharedContext,
    params: &std::collections::HashMap<String, String>,
    expected_scope: &str,
    expected_job_id: Option<u64>,
) -> Result<(), (StatusCode, String)> {
    if let Some(ticket_str) = params.get("ticket") {
        let ticket = match ctx.ws_tickets.remove(ticket_str) {
            Some((_, t)) => t,
            None => {
                return Err((
                    StatusCode::UNAUTHORIZED,
                    "Invalid or already redeemed ticket".to_string(),
                ))
            }
        };

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        if ticket.expires_at < now {
            return Err((StatusCode::UNAUTHORIZED, "Ticket expired".to_string()));
        }

        if ticket.scope != expected_scope {
            return Err((
                StatusCode::UNAUTHORIZED,
                "Ticket scope mismatch".to_string(),
            ));
        }

        if ticket.job_id != expected_job_id {
            return Err((
                StatusCode::UNAUTHORIZED,
                "Ticket job ID mismatch".to_string(),
            ));
        }

        Ok(())
    } else {
        // If no ticket is provided, let it pass if it's already authenticated by the middleware.
        Ok(())
    }
}
