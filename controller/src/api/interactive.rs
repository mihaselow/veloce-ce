//! API module: interactive.rs

use crate::SharedContext;
use axum::{
    body::Body,
    extract::{
        ws::{Message as WsMessage, WebSocket, WebSocketUpgrade},
        Path, State,
    },
    http::StatusCode,
    response::{IntoResponse, Response},
    Extension,
};
use futures::StreamExt;
use log::{error, info};
use uuid::Uuid;
use veloce_common::{JobStatus, Message};

use super::api_middleware::validate_and_redeem_ticket;
use axum::response::sse::{Event, Sse};
use std::convert::Infallible;

pub(super) async fn api_job_metrics_stream(
    Path(id): Path<u64>,
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

    if is_viewer_only {
        let state_lock = ctx.state.lock().await;
        let owns_job = if let Some(job) = state_lock.jobs.get(&id) {
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
        };
        if !owns_job {
            return (StatusCode::FORBIDDEN, "Forbidden").into_response();
        }
    }

    let stream = futures::stream::unfold((ctx, id, true), |(ctx, id, is_first)| async move {
        if is_first {
            // Send full history on first connect
            let history: Vec<_> = if let Some(q) = ctx.job_metrics_history.get(&id) {
                q.iter().cloned().collect()
            } else {
                Vec::new()
            };
            if let Ok(json) = serde_json::to_string(&history) {
                let event = Event::default().event("init").data(json);
                return Some((Ok::<Event, Infallible>(event), (ctx, id, false)));
            }
        }

        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

            let latest = if let Some(q) = ctx.job_metrics_history.get(&id) {
                q.back().cloned()
            } else {
                None
            };

            if let Some(p) = latest {
                if let Ok(json) = serde_json::to_string(&p) {
                    let event = Event::default().event("update").data(json);
                    return Some((Ok::<Event, Infallible>(event), (ctx, id, false)));
                }
            }
        }
    });

    Sse::new(stream)
        .keep_alive(axum::response::sse::KeepAlive::new())
        .into_response()
}

pub(super) async fn api_job_terminal(
    State(ctx): State<SharedContext>,
    Extension(principal): Extension<crate::auth::AuthenticatedPrincipal>,
    Path(id): Path<u64>,
    ws: WebSocketUpgrade,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Response {
    let has_valid_role = principal
        .roles
        .iter()
        .any(|r| r == "admin" || r == "operator" || r == "submitter" || r == "viewer");
    if !has_valid_role {
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
        let state = ctx.state.lock().await;
        if let Some(job) = state.jobs.get(&id) {
            if job.user_id != principal.user_id {
                return (StatusCode::FORBIDDEN, "Forbidden: not the job owner").into_response();
            }
        } else {
            return (StatusCode::NOT_FOUND, "Job not found").into_response();
        }
    }

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
                        handle_follower_ws_proxy(socket, id, leader, token, ticket, "terminal")
                            .await
                    {
                        error!("Follower Terminal proxy failed: {}", e);
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

    if let Err((status, msg)) = validate_and_redeem_ticket(&ctx, &params, "terminal", Some(id)) {
        return (status, msg).into_response();
    }

    let tx = {
        let state = ctx.state.lock().await;
        let job = match state.jobs.get(&id) {
            Some(j) => j,
            None => return (StatusCode::NOT_FOUND, "Job not found").into_response(),
        };

        if job.status != JobStatus::Running {
            return (StatusCode::BAD_REQUEST, "Job is not running").into_response();
        }

        let head_worker_id = match job.assigned_workers.first() {
            Some(w_id) => w_id.clone(),
            None => return (StatusCode::BAD_REQUEST, "No assigned worker for job").into_response(),
        };

        match crate::get_worker_sender(&state, &head_worker_id) {
            Some(sender) => sender,
            None => {
                return (
                    StatusCode::BAD_GATEWAY,
                    "Head worker connection path not found",
                )
                    .into_response()
            }
        }
    };

    let session_id = Uuid::new_v4().as_u128() as u64;

    ws.on_upgrade(move |socket| handle_terminal_socket_loop(ctx, id, session_id, tx, socket))
        .into_response()
}

pub(super) async fn handle_terminal_socket_loop(
    ctx: SharedContext,
    job_id: u64,
    session_id: u64,
    worker_tx: crate::WorkerSender,
    mut socket: WebSocket,
) {
    let (ws_tx, mut ws_rx) = tokio::sync::mpsc::channel::<Message>(1000);
    ctx.terminal_sessions.insert(session_id, ws_tx);

    let start_msg = Message::StartTerminalSession { job_id, session_id };
    if let Err(e) = worker_tx.send(start_msg).await {
        error!("Failed to send StartTerminalSession to worker: {}", e);
        ctx.terminal_sessions.remove(&session_id);
        let _ = socket.send(WsMessage::Close(None)).await;
        return;
    }

    let mut closed = false;

    loop {
        tokio::select! {
            ws_msg_opt = socket.recv() => {
                let ws_msg = match ws_msg_opt {
                    Some(Ok(m)) => m,
                    _ => break,
                };

                match ws_msg {
                    WsMessage::Text(text) => {
                        if let Ok(client_msg) = serde_json::from_str::<ClientTerminalMessage>(&text) {
                            match client_msg {
                                ClientTerminalMessage::Input { data } => {
                                    let _ = worker_tx.send(Message::TerminalInput {
                                        session_id,
                                        data: data.into_bytes(),
                                    }).await;
                                }
                                ClientTerminalMessage::Resize { cols, rows } => {
                                    let _ = worker_tx.send(Message::TerminalResize {
                                        session_id,
                                        rows,
                                        cols,
                                    }).await;
                                }
                            }
                        } else {
                            let _ = worker_tx.send(Message::TerminalInput {
                                session_id,
                                data: text.into_bytes(),
                            }).await;
                        }
                    }
                    WsMessage::Binary(bin) => {
                        let _ = worker_tx.send(Message::TerminalInput {
                            session_id,
                            data: bin,
                        }).await;
                    }
                    WsMessage::Close(_) => break,
                    _ => {}
                }
            }

            worker_msg_opt = ws_rx.recv() => {
                let worker_msg = match worker_msg_opt {
                    Some(m) => m,
                    None => break,
                };

                match worker_msg {
                    Message::TerminalOutput { data, .. } => {
                        if socket.send(WsMessage::Binary(data)).await.is_err() {
                            break;
                        }
                    }
                    Message::TerminalClosed { reason, .. } => {
                        let payload = serde_json::json!({
                            "type": "closed",
                            "reason": reason,
                        });
                        let _ = socket.send(WsMessage::Text(payload.to_string())).await;
                        closed = true;
                        break;
                    }
                    _ => {}
                }
            }
        }
    }

    ctx.terminal_sessions.remove(&session_id);
    let _ = worker_tx
        .send(Message::StopTerminalSession { session_id })
        .await;

    if !closed {
        let _ = socket.send(WsMessage::Close(None)).await;
    }
}

#[derive(serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ClientTerminalMessage {
    Input { data: String },
    Resize { cols: u16, rows: u16 },
}

pub(super) async fn api_job_interactive_proxy(
    State(ctx): State<SharedContext>,
    Extension(principal): Extension<crate::auth::AuthenticatedPrincipal>,
    Path((job_id, path)): Path<(u64, String)>,
    req: axum::http::Request<Body>,
) -> impl IntoResponse {
    let has_valid_role = principal
        .roles
        .iter()
        .any(|r| r == "admin" || r == "operator" || r == "submitter");
    if !has_valid_role {
        return (StatusCode::FORBIDDEN, "Forbidden: insufficient role").into_response();
    }

    let (worker_ip, port) = {
        let state = ctx.state.lock().await;
        let job = match state.jobs.get(&job_id) {
            Some(j) => j,
            None => return (StatusCode::NOT_FOUND, "Job not found").into_response(),
        };
        if job.status != JobStatus::Running {
            return (StatusCode::BAD_REQUEST, "Job is not running").into_response();
        }
        let port = match job.interactive_port {
            Some(p) => p,
            None => {
                return (
                    StatusCode::NOT_FOUND,
                    "No interactive port registered for this job",
                )
                    .into_response();
            }
        };
        let worker_id = match job.assigned_workers.first() {
            Some(id) => id,
            None => {
                return (StatusCode::BAD_REQUEST, "No assigned worker for job").into_response();
            }
        };
        let worker = match state.workers.get(worker_id) {
            Some(w) => w,
            None => {
                return (StatusCode::BAD_GATEWAY, "Assigned worker not connected").into_response();
            }
        };
        (worker.addr.ip().to_string(), port)
    };

    let query = req
        .uri()
        .query()
        .map(|q| format!("?{q}"))
        .unwrap_or_default();
    let upstream = format!(
        "http://{}:{}/{}{}",
        worker_ip,
        port,
        path.trim_start_matches('/'),
        query
    );

    let method = req.method().clone();
    let headers = req.headers().clone();
    let body = match axum::body::to_bytes(req.into_body(), usize::MAX).await {
        Ok(bytes) => bytes,
        Err(err) => {
            return (
                StatusCode::BAD_REQUEST,
                format!("Failed to read proxy request body: {err}"),
            )
                .into_response();
        }
    };

    let mut builder = ctx.proxy_client.request(method, &upstream);
    for (name, value) in headers.iter() {
        if name == axum::http::header::HOST {
            continue;
        }
        builder = builder.header(name, value);
    }

    match builder.body(body).send().await {
        Ok(resp) => {
            let status = resp.status();
            let headers = resp.headers().clone();
            let bytes = resp.bytes().await.unwrap_or_default();
            let mut response = Response::builder().status(status);
            if let Some(h) = response.headers_mut() {
                *h = headers;
            }
            response.body(Body::from(bytes)).unwrap_or_else(|_| {
                (StatusCode::BAD_GATEWAY, "Invalid upstream response").into_response()
            })
        }
        Err(err) => (
            StatusCode::BAD_GATEWAY,
            format!("Interactive proxy failed: {err}"),
        )
            .into_response(),
    }
}

pub(super) async fn api_job_vnc(
    State(ctx): State<SharedContext>,
    Extension(principal): Extension<crate::auth::AuthenticatedPrincipal>,
    Path(id): Path<u64>,
    ws: WebSocketUpgrade,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Response {
    let has_valid_role = principal
        .roles
        .iter()
        .any(|r| r == "admin" || r == "operator" || r == "submitter" || r == "viewer");
    if !has_valid_role {
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
        let state = ctx.state.lock().await;
        if let Some(job) = state.jobs.get(&id) {
            if job.user_id != principal.user_id {
                return (StatusCode::FORBIDDEN, "Forbidden: not the job owner").into_response();
            }
        } else {
            return (StatusCode::NOT_FOUND, "Job not found").into_response();
        }
    }

    let (role, leader_addr) = {
        let state = ctx.state.lock().await;
        (state.role, state.leader_addr.clone())
    };

    if role != crate::ControllerRole::Leader {
        if let Some(leader) = leader_addr {
            let token = params.get("token").cloned().unwrap_or_default();
            let ticket = params.get("ticket").cloned().unwrap_or_default();
            return ws
                .protocols(["binary"])
                .on_upgrade(move |socket| async move {
                    if let Err(e) =
                        handle_follower_ws_proxy(socket, id, leader, token, ticket, "vnc").await
                    {
                        error!("Follower VNC proxy failed: {}", e);
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

    if let Err((status, msg)) = validate_and_redeem_ticket(&ctx, &params, "vnc", Some(id)) {
        return (status, msg).into_response();
    }

    let tx = {
        let state = ctx.state.lock().await;
        let job = match state.jobs.get(&id) {
            Some(j) => j,
            None => return (StatusCode::NOT_FOUND, "Job not found").into_response(),
        };

        if job.status != JobStatus::Running {
            return (StatusCode::BAD_REQUEST, "Job is not running").into_response();
        }

        if !job.vnc_enabled {
            return (StatusCode::BAD_REQUEST, "VNC is not enabled for this job").into_response();
        }

        let head_worker_id = match job.assigned_workers.first() {
            Some(w_id) => w_id.clone(),
            None => return (StatusCode::BAD_REQUEST, "No assigned worker for job").into_response(),
        };

        match crate::get_worker_sender(&state, &head_worker_id) {
            Some(sender) => sender,
            None => {
                return (
                    StatusCode::BAD_GATEWAY,
                    "Head worker connection path not found",
                )
                    .into_response()
            }
        }
    };

    let session_id = Uuid::new_v4().as_u128() as u64;

    ws.protocols(["binary"])
        .on_upgrade(move |socket| handle_vnc_socket_loop(ctx, id, session_id, tx, socket))
        .into_response()
}

pub(super) async fn handle_vnc_socket_loop(
    ctx: SharedContext,
    job_id: u64,
    session_id: u64,
    worker_tx: crate::WorkerSender,
    mut socket: WebSocket,
) {
    let (ws_tx, mut ws_rx) = tokio::sync::mpsc::channel::<Message>(1000);
    ctx.vnc_sessions.insert(session_id, ws_tx);

    let start_msg = Message::StartVncSession { job_id, session_id };
    if let Err(e) = worker_tx.send(start_msg).await {
        error!("Failed to send StartVncSession to worker: {}", e);
        ctx.vnc_sessions.remove(&session_id);
        let _ = socket.send(WsMessage::Close(None)).await;
        return;
    }

    let mut closed = false;

    loop {
        tokio::select! {
            ws_msg_opt = socket.recv() => {
                let ws_msg = match ws_msg_opt {
                    Some(Ok(m)) => m,
                    _ => break,
                };

                match ws_msg {
                    WsMessage::Binary(bin) => {
                        let _ = worker_tx.send(Message::VncInput {
                            session_id,
                            data: bin,
                        }).await;
                    }
                    WsMessage::Close(_) => break,
                    _ => {}
                }
            }

            worker_msg_opt = ws_rx.recv() => {
                let worker_msg = match worker_msg_opt {
                    Some(m) => m,
                    None => break,
                };

                match worker_msg {
                    Message::VncOutput { data, .. } => {
                        if socket.send(WsMessage::Binary(data)).await.is_err() {
                            break;
                        }
                    }
                    Message::VncClosed { reason, .. } => {
                        // Binary RFB protocol cannot carry text; close with reason in close frame.
                        info!("VNC session {} closed: {}", session_id, reason);
                        let _ = socket
                            .send(WsMessage::Close(Some(axum::extract::ws::CloseFrame {
                                code: axum::extract::ws::close_code::NORMAL,
                                reason: reason.chars().take(120).collect::<String>().into(),
                            })))
                            .await;
                        closed = true;
                        break;
                    }
                    _ => {}
                }
            }
        }
    }

    ctx.vnc_sessions.remove(&session_id);
    let _ = worker_tx.send(Message::StopVncSession { session_id }).await;

    if !closed {
        let _ = socket.send(WsMessage::Close(None)).await;
    }
}

pub(super) async fn handle_follower_ws_proxy(
    client_socket: axum::extract::ws::WebSocket,
    job_id: u64,
    leader_addr: String,
    token: String,
    ticket: String,
    endpoint: &'static str, // "vnc" or "terminal"
) -> anyhow::Result<()> {
    let leader_host = leader_addr.split(':').next().unwrap_or("localhost");
    let query = if !ticket.is_empty() {
        format!("ticket={}", urlencoding::encode(&ticket))
    } else {
        format!("token={}", urlencoding::encode(&token))
    };
    let leader_ws_url = format!(
        "wss://{}:8080/api/v1/jobs/{}/{}?{}",
        leader_host, job_id, endpoint, query
    );

    info!(
        "Follower WebSocket proxy: connecting to leader at {}",
        leader_ws_url
    );

    let tls_connector = native_tls::TlsConnector::builder()
        .danger_accept_invalid_certs(!cfg!(feature = "production"))
        .danger_accept_invalid_hostnames(!cfg!(feature = "production"))
        .build()?;
    let connector = tokio_tungstenite::Connector::NativeTls(tls_connector);

    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    let mut req = leader_ws_url.into_client_request()?;
    if endpoint == "vnc" {
        req.headers_mut().insert(
            axum::http::header::SEC_WEBSOCKET_PROTOCOL,
            axum::http::HeaderValue::from_static("binary"),
        );
    }

    let (leader_socket, _) =
        tokio_tungstenite::connect_async_tls_with_config(req, None, false, Some(connector)).await?;

    info!(
        "Follower WebSocket proxy: connected to leader for job {} at endpoint {}",
        job_id, endpoint
    );

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

    info!(
        "Follower WebSocket proxy finished for job {} at endpoint {}",
        job_id, endpoint
    );
    Ok(())
}
