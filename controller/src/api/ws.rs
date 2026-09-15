//! API module: ws.rs

use crate::{SharedContext, WsTicket};
use axum::{
    extract::{
        ws::{Message as WsMessage, WebSocket, WebSocketUpgrade},
        State,
    },
    http::StatusCode,
    response::{IntoResponse, Response},
    Extension, Json,
};
use futures::StreamExt;
use log::{error, info};
use std::time::{SystemTime, UNIX_EPOCH};

use super::api_middleware::validate_and_redeem_ticket;
use super::types::*;

pub(super) async fn api_create_ws_ticket(
    State(ctx): State<SharedContext>,
    Extension(principal): Extension<crate::auth::AuthenticatedPrincipal>,
    Json(payload): Json<CreateWsTicketRequest>,
) -> impl IntoResponse {
    match payload.scope.as_str() {
        "events" => {
            let allowed = principal
                .roles
                .iter()
                .any(|r| r == "admin" || r == "operator" || r == "submitter" || r == "viewer");
            if !allowed {
                return (StatusCode::FORBIDDEN, "Forbidden: insufficient role").into_response();
            }
        }
        "terminal" | "vnc" => {
            let allowed = principal
                .roles
                .iter()
                .any(|r| r == "admin" || r == "operator" || r == "submitter" || r == "viewer");
            if !allowed {
                return (StatusCode::FORBIDDEN, "Forbidden: insufficient role").into_response();
            }
            let is_restricted = {
                let has_elevated = principal
                    .roles
                    .iter()
                    .any(|r| r == "admin" || r == "operator");
                (principal.roles.iter().any(|r| r == "submitter") && !has_elevated)
                    || (principal.roles.iter().any(|r| r == "viewer")
                        && !principal
                            .roles
                            .iter()
                            .any(|r| r == "admin" || r == "operator" || r == "submitter"))
            };
            if is_restricted {
                if let Some(jid) = payload.job_id {
                    let state = ctx.state.lock().await;
                    if let Some(job) = state.jobs.get(&jid) {
                        if job.user_id != principal.user_id {
                            return (StatusCode::FORBIDDEN, "Forbidden: not the job owner")
                                .into_response();
                        }
                    } else {
                        return (StatusCode::NOT_FOUND, "Job not found").into_response();
                    }
                } else {
                    return (
                        StatusCode::BAD_REQUEST,
                        "Job ID required for terminal/vnc tickets",
                    )
                        .into_response();
                }
            }
        }
        _ => {
            return (StatusCode::BAD_REQUEST, "Invalid ticket scope").into_response();
        }
    }

    let ticket_str = uuid::Uuid::new_v4().to_string();
    // Long enough for slow VPN + first-paint asset load before WS redeem.
    let expires_in_secs = 90;
    let expires_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        + expires_in_secs;

    let ticket = WsTicket {
        scope: payload.scope,
        job_id: payload.job_id,
        expires_at,
        user_id: principal.user_id.clone(),
        roles: principal.roles.clone(),
    };

    ctx.ws_tickets.insert(ticket_str.clone(), ticket);

    (
        StatusCode::CREATED,
        Json(serde_json::json!({
            "ticket": ticket_str,
            "expires_at": expires_at,
        })),
    )
        .into_response()
}

pub(super) async fn api_ws_events(
    State(ctx): State<SharedContext>,
    ws: WebSocketUpgrade,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Response {
    let (role, leader_addr) = {
        let state = ctx.state.lock().await;
        (state.role, state.leader_addr.clone())
    };

    if role != crate::ControllerRole::Leader {
        if let Some(leader) = leader_addr {
            let token = params.get("token").cloned().unwrap_or_default();
            let ticket = params.get("ticket").cloned().unwrap_or_default();
            return ws
                .on_upgrade(move |socket| async move {
                    if let Err(e) =
                        handle_follower_events_ws_proxy(socket, leader, token, ticket).await
                    {
                        error!("Follower Events WebSocket proxy failed: {}", e);
                    }
                })
                .into_response();
        } else {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                "No leader controller available",
            )
                .into_response();
        }
    }

    if let Err((status, msg)) = validate_and_redeem_ticket(&ctx, &params, "events", None) {
        return (status, msg).into_response();
    }

    ws.on_upgrade(move |socket| handle_ws_events(socket, ctx))
}

pub(super) async fn handle_ws_events(mut socket: WebSocket, ctx: SharedContext) {
    let mut rx = ctx.event_tx.subscribe();

    // Send initial connected ping
    if socket
        .send(WsMessage::Text("connected".to_string()))
        .await
        .is_err()
    {
        return;
    }

    loop {
        tokio::select! {
            Ok(msg) = rx.recv() => {
                if socket.send(WsMessage::Text(msg)).await.is_err() {
                    break;
                }
            }
            msg = socket.recv() => {
                match msg {
                    Some(Ok(WsMessage::Close(_))) | None => break,
                    _ => {}
                }
            }
        }
    }
}

pub(super) async fn handle_follower_events_ws_proxy(
    client_socket: axum::extract::ws::WebSocket,
    leader_addr: String,
    token: String,
    ticket: String,
) -> anyhow::Result<()> {
    let leader_host = leader_addr.split(':').next().unwrap_or("localhost");
    let query = if !ticket.is_empty() {
        format!("ticket={}", urlencoding::encode(&ticket))
    } else {
        format!("token={}", urlencoding::encode(&token))
    };
    let leader_ws_url = format!("wss://{}:8080/api/v1/ws/events?{}", leader_host, query);

    info!(
        "Follower WebSocket proxy: connecting to leader events at {}",
        leader_ws_url
    );

    let tls_connector = native_tls::TlsConnector::builder()
        .danger_accept_invalid_certs(!cfg!(feature = "production"))
        .danger_accept_invalid_hostnames(!cfg!(feature = "production"))
        .build()?;
    let connector = tokio_tungstenite::Connector::NativeTls(tls_connector);

    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    let req = leader_ws_url.into_client_request()?;

    let (leader_socket, _) =
        tokio_tungstenite::connect_async_tls_with_config(req, None, false, Some(connector)).await?;

    info!("Follower WebSocket proxy: connected to leader events");

    let (mut leader_write, mut leader_read) = futures::StreamExt::split(leader_socket);
    let (mut client_write, mut client_read) = futures::StreamExt::split(client_socket);

    let client_to_leader = async {
        while let Some(Ok(msg)) = client_read.next().await {
            let tungsten_msg = match msg {
                axum::extract::ws::Message::Text(t) => {
                    tokio_tungstenite::tungstenite::Message::Text(t)
                }
                axum::extract::ws::Message::Binary(b) => {
                    tokio_tungstenite::tungstenite::Message::Binary(b)
                }
                axum::extract::ws::Message::Ping(p) => {
                    tokio_tungstenite::tungstenite::Message::Ping(p)
                }
                axum::extract::ws::Message::Pong(p) => {
                    tokio_tungstenite::tungstenite::Message::Pong(p)
                }
                axum::extract::ws::Message::Close(c) => {
                    let frame =
                        c.map(
                            |frame| tokio_tungstenite::tungstenite::protocol::CloseFrame {
                                code: frame.code.into(),
                                reason: frame.reason.clone(),
                            },
                        );
                    tokio_tungstenite::tungstenite::Message::Close(frame)
                }
            };
            if futures::SinkExt::send(&mut leader_write, tungsten_msg)
                .await
                .is_err()
            {
                break;
            }
        }
        anyhow::Ok(())
    };

    let leader_to_client = async {
        while let Some(Ok(msg)) = leader_read.next().await {
            let axum_msg = match msg {
                tokio_tungstenite::tungstenite::Message::Text(t) => {
                    axum::extract::ws::Message::Text(t)
                }
                tokio_tungstenite::tungstenite::Message::Binary(b) => {
                    axum::extract::ws::Message::Binary(b)
                }
                tokio_tungstenite::tungstenite::Message::Ping(p) => {
                    axum::extract::ws::Message::Ping(p)
                }
                tokio_tungstenite::tungstenite::Message::Pong(p) => {
                    axum::extract::ws::Message::Pong(p)
                }
                tokio_tungstenite::tungstenite::Message::Close(c) => {
                    let frame = c.map(|frame| axum::extract::ws::CloseFrame {
                        code: frame.code.into(),
                        reason: frame.reason.clone(),
                    });
                    axum::extract::ws::Message::Close(frame)
                }
                tokio_tungstenite::tungstenite::Message::Frame(_) => continue,
            };
            if futures::SinkExt::send(&mut client_write, axum_msg)
                .await
                .is_err()
            {
                break;
            }
        }
        anyhow::Ok(())
    };

    tokio::select! {
        _ = client_to_leader => {}
        _ = leader_to_client => {}
    }

    info!("Follower WebSocket proxy finished for events");
    Ok(())
}
