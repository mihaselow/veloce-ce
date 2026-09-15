//! Noise connection upgrade and role dispatch.

use crate::ha::handle_peer;
use crate::handlers::client::handle_client;
use crate::handlers::worker::handle_worker;
use crate::state::{ControllerRole, SharedContext};
use anyhow::Result;
use futures::{SinkExt, StreamExt};
use std::net::SocketAddr;
use tokio_util::codec::Framed;
use tracing::{error, info, warn};
use veloce_common::{Message, MessageCodec};

pub async fn handle_connection<S>(stream: S, addr: SocketAddr, ctx: SharedContext) -> Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let mut framed = Framed::new(stream, MessageCodec::new());

    // Handshake
    let msg = framed
        .next()
        .await
        .ok_or_else(|| anyhow::anyhow!("Connection closed during handshake"))??;

    match msg {
        Message::HelloWorker {
            worker_id,
            hostname,
            resources,
            features,
            registration_token,
        } => {
            let require_tokens = std::env::var("VELOCE_REQUIRE_WORKER_TOKENS")
                .map(|v| v == "true")
                .unwrap_or(false);

            if require_tokens {
                let token = match registration_token {
                    Some(ref t) => t,
                    None => {
                        let err_msg = format!("Worker {} registration rejected: token is required but none was provided", worker_id);
                        warn!("{}", err_msg);
                        let _ = framed.send(Message::Error(err_msg.clone())).await;
                        anyhow::bail!("{}", err_msg);
                    }
                };

                match ctx
                    .component_registry
                    .validate(
                        &worker_id,
                        veloce_common::auth::ComponentType::Worker,
                        token,
                    )
                    .await
                {
                    Ok(Some(comp)) => {
                        // P1-6.1 audit success
                        let _ = ctx
                            .audit
                            .log(&crate::audit::AuditEvent::component_register_success(
                                &worker_id, "worker", &worker_id,
                            ))
                            .await;
                        info!(
                            "Worker {} identity validated successfully (roles: {:?})",
                            worker_id, comp.roles
                        );
                    }
                    Ok(None) => {
                        let err_msg = format!(
                            "Worker {} registration rejected: invalid or revoked token",
                            worker_id
                        );
                        warn!("{}", err_msg);
                        // P1-6.1 audit denial with reason
                        let _ = ctx
                            .audit
                            .log(&crate::audit::AuditEvent::component_register_denied(
                                &worker_id,
                                "worker",
                                "invalid_or_revoked_token",
                                &worker_id,
                            ))
                            .await;
                        let _ = framed.send(Message::Error(err_msg.clone())).await;
                        anyhow::bail!("{}", err_msg);
                    }
                    Err(e) => {
                        let err_msg = format!(
                            "Worker {} registration failed due to database error: {:#}",
                            worker_id, e
                        );
                        error!("{}", err_msg);
                        let _ = framed.send(Message::Error(err_msg.clone())).await;
                        anyhow::bail!("{}", err_msg);
                    }
                }
            }

            info!(
                "Worker connected: {} ({}) at {}. Model: {}, Arch: {}, Cores: {}, Mem: {}MB",
                worker_id,
                hostname,
                addr,
                resources.cpu_model,
                resources.arch,
                resources.cpu_cores,
                resources.total_memory / 1024 / 1024
            );
            handle_worker(
                framed,
                addr,
                ctx,
                worker_id,
                hostname,
                resources,
                features.unwrap_or(0),
            )
            .await
        }
        Message::HelloClient {
            client_id,
            registration_token,
        } => {
            let require_tokens = std::env::var("VELOCE_REQUIRE_CLIENT_TOKENS")
                .map(|v| v == "true")
                .unwrap_or(false);

            let client_roles = if require_tokens {
                let token = match registration_token {
                    Some(ref t) => t,
                    None => {
                        let err_msg = format!("Client {} registration rejected: token is required but none was provided", client_id);
                        warn!("{}", err_msg);
                        let _ = framed.send(Message::Error(err_msg.clone())).await;
                        anyhow::bail!("{}", err_msg);
                    }
                };

                match ctx
                    .component_registry
                    .validate(
                        &client_id,
                        veloce_common::auth::ComponentType::Client,
                        token,
                    )
                    .await
                {
                    Ok(Some(comp)) => {
                        let _ = ctx
                            .audit
                            .log(&crate::audit::AuditEvent::component_register_success(
                                &client_id, "client", &client_id,
                            ))
                            .await;
                        info!(
                            "Client {} identity validated successfully (roles: {:?})",
                            client_id, comp.roles
                        );
                        comp.roles
                    }
                    Ok(None) => {
                        let err_msg = format!(
                            "Client {} registration rejected: invalid or revoked token",
                            client_id
                        );
                        warn!("{}", err_msg);
                        let _ = framed.send(Message::Error(err_msg.clone())).await;
                        anyhow::bail!("{}", err_msg);
                    }
                    Err(e) => {
                        let err_msg = format!(
                            "Client {} registration failed due to database error: {:#}",
                            client_id, e
                        );
                        error!("{}", err_msg);
                        let _ = framed.send(Message::Error(err_msg.clone())).await;
                        anyhow::bail!("{}", err_msg);
                    }
                }
            } else {
                vec![
                    "admin".to_string(),
                    "operator".to_string(),
                    "submitter".to_string(),
                    "viewer".to_string(),
                ]
            };

            // Leadership Check
            {
                let state = ctx.state.lock().await;
                if state.role != ControllerRole::Leader {
                    let leader_addr = state.leader_addr.clone().unwrap_or_default();
                    warn!(
                        "Client attempted to connect to follower at {}. Redirecting to {}.",
                        addr, leader_addr
                    );
                    let _ = framed.send(Message::LeaderRedirect { leader_addr }).await;
                    return Ok(());
                }
            }
            let _ = framed.send(Message::Ack).await;
            handle_client(framed, addr, ctx, client_id, client_roles).await
        }
        Message::HelloPeer {
            controller_id,
            features: _,
            registration_token,
        } => {
            let require_tokens = std::env::var("VELOCE_REQUIRE_PEER_TOKENS")
                .map(|v| v == "true")
                .unwrap_or(false);

            if require_tokens {
                let token = match registration_token {
                    Some(ref t) => t,
                    None => {
                        let err_msg = format!("Peer {} registration rejected: token is required but none was provided", controller_id);
                        warn!("{}", err_msg);
                        let _ = framed.send(Message::Error(err_msg.clone())).await;
                        anyhow::bail!("{}", err_msg);
                    }
                };

                match ctx
                    .component_registry
                    .validate(
                        &controller_id,
                        veloce_common::auth::ComponentType::Peer,
                        token,
                    )
                    .await
                {
                    Ok(Some(comp)) => {
                        info!(
                            "Peer {} identity validated successfully (roles: {:?})",
                            controller_id, comp.roles
                        );
                    }
                    Ok(None) => {
                        let err_msg = format!(
                            "Peer {} registration rejected: invalid or revoked token",
                            controller_id
                        );
                        warn!("{}", err_msg);
                        let _ = framed.send(Message::Error(err_msg.clone())).await;
                        anyhow::bail!("{}", err_msg);
                    }
                    Err(e) => {
                        let err_msg = format!(
                            "Peer {} registration failed due to database error: {:#}",
                            controller_id, e
                        );
                        error!("{}", err_msg);
                        let _ = framed.send(Message::Error(err_msg.clone())).await;
                        anyhow::bail!("{}", err_msg);
                    }
                }
            }

            handle_peer(framed, addr.to_string(), ctx, controller_id).await
        }
        _ => anyhow::bail!("Invalid handshake message"),
    }
}
