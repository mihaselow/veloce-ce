use anyhow::Result;
use log::{debug, error};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{broadcast, mpsc};
use veloce_common::Message;

pub async fn start_pmi_server(
    parent_job_id: u64,
    step_id: u32,
    tx: mpsc::Sender<Message>,
    mut rx: mpsc::Receiver<Message>,
) -> Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();

    let (barrier_tx, _) = broadcast::channel::<()>(16);
    let (get_tx, _) = broadcast::channel::<Message>(100);

    let b_tx_clone = barrier_tx.clone();
    let g_tx_clone = get_tx.clone();

    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            match msg {
                Message::StepPMIBarrierRelease { .. } => {
                    let _ = b_tx_clone.send(());
                }
                m @ Message::StepPMIGetResponse { .. } => {
                    let _ = g_tx_clone.send(m);
                }
                _ => {}
            }
        }
    });

    let request_counter = Arc::new(AtomicU64::new(1));

    tokio::spawn(async move {
        loop {
            if let Ok((mut socket, _addr)) = listener.accept().await {
                let tx_clone = tx.clone();
                let mut barrier_rx = barrier_tx.subscribe();
                let mut get_rx = get_tx.subscribe();
                let counter_clone = request_counter.clone();

                tokio::spawn(async move {
                    let mut buf = [0; 4096];
                    loop {
                        match socket.read(&mut buf).await {
                            Ok(0) => break,
                            Ok(n) => {
                                let req_str = String::from_utf8_lossy(&buf[..n]);
                                for req in req_str.split('\n') {
                                    let req = req.trim();
                                    if req.is_empty() {
                                        continue;
                                    }
                                    debug!("PMI IN: {}", req);

                                    let res = process_pmi_request(
                                        req,
                                        parent_job_id,
                                        step_id,
                                        &tx_clone,
                                        &mut barrier_rx,
                                        &mut get_rx,
                                        &counter_clone,
                                    )
                                    .await;

                                    debug!("PMI OUT: {}", res.trim());
                                    if let Err(e) = socket.write_all(res.as_bytes()).await {
                                        error!("PMI write error: {}", e);
                                        break;
                                    }
                                }
                            }
                            Err(e) => {
                                error!("PMI read error: {}", e);
                                break;
                            }
                        }
                    }
                });
            }
        }
    });

    Ok(port)
}

async fn process_pmi_request(
    req: &str,
    parent_job_id: u64,
    step_id: u32,
    tx: &mpsc::Sender<Message>,
    barrier_rx: &mut broadcast::Receiver<()>,
    get_rx: &mut broadcast::Receiver<Message>,
    counter: &Arc<AtomicU64>,
) -> String {
    let mut parts: HashMap<&str, &str> = HashMap::new();

    // PMI-1 format uses space, PMI-2 uses semicolon
    let delimiter = if req.contains(';') { ';' } else { ' ' };
    for token in req.split(delimiter) {
        if let Some((k, v)) = token.split_once('=') {
            parts.insert(k.trim(), v.trim());
        }
    }

    if let Some(&cmd) = parts.get("cmd") {
        match cmd {
            "init" => {
                format!("cmd=response_to_init pmi_version=1 pmi_subversion=1 rc=0\n")
            }
            "init-v1-req" => {
                format!("cmd=init-v1-req-response;rc=0;pmi_version=2;pmi_subversion=0;\n")
            }
            "get_maxes" | "get-maxes" => {
                format!(
                    "cmd=get_maxes_response kvsname_max=255 keylen_max=255 vallen_max=1024 rc=0\n"
                )
            }
            "get_my_kvsname" | "kvs-get-my-name" => {
                format!("cmd=get_my_kvsname_response kvsname=veloce_step rc=0\n")
            }
            "get_appnum" | "appnum-get" => {
                format!("cmd=get_appnum_response appnum=0 rc=0\n")
            }
            "get_universe_size" | "universe-size-get" => {
                format!("cmd=get_universe_size_response size=1 rc=0\n")
            }
            "commit" | "kvs-commit" => {
                format!("cmd=commit_response rc=0\n")
            }
            "barrier_in" | "kvs-fence" => {
                let rank = parts.get("rank").and_then(|v| v.parse().ok()).unwrap_or(0);
                let _ = tx
                    .send(Message::StepPMIBarrierEnter {
                        parent_job_id,
                        step_id,
                        rank,
                    })
                    .await;
                let _ = barrier_rx.recv().await; // Block until controller fires release!
                if cmd.contains("fence") {
                    format!("cmd=kvs-fence-response;rc=0;\n")
                } else {
                    format!("cmd=barrier_out\n")
                }
            }
            "put" | "kvs-put" => {
                let rank = parts.get("rank").and_then(|v| v.parse().ok()).unwrap_or(0);
                let key = parts.get("key").unwrap_or(&"unknown").to_string();
                let value = parts.get("value").unwrap_or(&"").to_string();
                let _ = tx
                    .send(Message::StepPMIPut {
                        parent_job_id,
                        step_id,
                        rank,
                        key,
                        value,
                    })
                    .await;
                if cmd.contains("put") {
                    format!("cmd=kvs-put-response;rc=0;\n")
                } else {
                    format!("cmd=put_response rc=0\n")
                }
            }
            "get" | "kvs-get" => {
                let rank = parts.get("rank").and_then(|v| v.parse().ok()).unwrap_or(0);
                let key = parts.get("key").unwrap_or(&"unknown").to_string();
                let request_id = counter.fetch_add(1, Ordering::SeqCst);
                let _ = tx
                    .send(Message::StepPMIGet {
                        parent_job_id,
                        step_id,
                        rank,
                        key,
                        request_id,
                    })
                    .await;

                // Wait for the specific response
                loop {
                    if let Ok(Message::StepPMIGetResponse {
                        request_id: rx_id,
                        value,
                        ..
                    }) = get_rx.recv().await
                    {
                        if rx_id == request_id {
                            let val_str = value.unwrap_or_else(|| "unknown".to_string());
                            if cmd.contains("get") {
                                return format!("cmd=kvs-get-response;rc=0;value={};\n", val_str);
                            } else {
                                return format!("cmd=get_response rc=0 value={}\n", val_str);
                            }
                        }
                    }
                }
            }
            "finalize" | "job-disconnect" => {
                if cmd.contains("disconnect") {
                    format!("cmd=job-disconnect-response;rc=0;\n")
                } else {
                    format!("cmd=finalize_response rc=0\n")
                }
            }
            "info" => {
                format!("cmd=info-response;rc=0;\n")
            }
            _ => format!("cmd=error rc=1 msg=unknown_cmd\n"),
        }
    } else {
        format!("cmd=error rc=1 msg=no_cmd\n")
    }
}
