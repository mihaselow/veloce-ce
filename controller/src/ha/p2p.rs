//! P2P manager for controller HA clustering.

use crate::ha::peer::handle_peer;
use crate::state::SharedContext;
use futures::{SinkExt, StreamExt};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;
use tracing::{debug, error, info};
use veloce_common::{noise, Message, MessageCodec};

pub async fn start_p2p_manager(ctx: SharedContext, peers: String, secret: String) {
    let peer_list: Vec<String> = peers.split(',').map(|s| s.to_string()).collect();
    let controller_id =
        std::env::var("HOSTNAME").unwrap_or_else(|_| format!("controller-{}", std::process::id()));
    info!("Starting P2P Manager with ID: {}", controller_id);

    for peer_addr in peer_list {
        let ctx_clone = ctx.clone();
        let secret_clone = secret.clone();
        let peer_addr_clone = peer_addr.clone();
        let id_clone = controller_id.clone();

        tokio::spawn(async move {
            loop {
                info!("Attempting to connect to peer {}...", peer_addr_clone);
                match TcpStream::connect(&peer_addr_clone).await {
                    Ok(stream) => {
                        match noise::upgrade_initiator(stream, &secret_clone).await {
                            Ok(noise_stream) => {
                                let mut framed = Framed::new(noise_stream, MessageCodec::new());
                                let peer_token = std::env::var("VELOCE_PEER_TOKEN").ok();
                                // Send HelloPeer
                                if let Err(e) = framed
                                    .send(Message::HelloPeer {
                                        controller_id: id_clone.clone(),
                                        features: None,
                                        registration_token: peer_token,
                                    })
                                    .await
                                {
                                    error!(
                                        "Failed to send HelloPeer to {}: {}",
                                        peer_addr_clone, e
                                    );
                                } else {
                                    // Wait for HelloPeer back
                                    match framed.next().await {
                                        Some(Ok(Message::HelloPeer {
                                            controller_id: remote_id,
                                            features: _,
                                            registration_token: _,
                                        })) => {
                                            if let Err(e) = handle_peer(
                                                framed,
                                                peer_addr_clone.clone(),
                                                ctx_clone.clone(),
                                                remote_id,
                                            )
                                            .await
                                            {
                                                error!(
                                                    "Peer connection to {} lost: {}",
                                                    peer_addr_clone, e
                                                );
                                            }
                                        }
                                        _ => error!(
                                            "Failed to receive HelloPeer from {}",
                                            peer_addr_clone
                                        ),
                                    }
                                }
                            }
                            Err(e) => {
                                error!("Noise handshake failed for peer {}: {}", peer_addr_clone, e)
                            }
                        }
                    }
                    Err(e) => debug!("Failed to connect to peer {}: {}", peer_addr_clone, e),
                }
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        });
    }
}
