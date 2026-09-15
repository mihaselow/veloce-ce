#![allow(clippy::all)]
use anyhow::{Context, Result};
use reqwest;
use std::env;
use veloce_common::LaunchRequest;

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    // Debug logging to stderr (captured by worker logs)
    eprintln!("veloce-exec: args: {:?}", args);

    if args.len() < 3 {
        eprintln!("Usage: veloce-exec <hostname> <command...>");
        std::process::exit(1);
    }

    // Skip options starting with '-' (like -p) until we find the hostname
    let mut hostname_idx = 1;
    while hostname_idx < args.len() && args[hostname_idx].starts_with('-') {
        // Simple skip for now, maybe skip next arg if it's -p or -l
        if args[hostname_idx] == "-p" || args[hostname_idx] == "-l" {
            hostname_idx += 2;
        } else {
            hostname_idx += 1;
        }
    }

    if hostname_idx >= args.len() {
        eprintln!("Error: hostname not found");
        std::process::exit(1);
    }

    let hostname = &args[hostname_idx];
    let binary = args[hostname_idx + 1].trim();
    let cmd_args: Vec<String> = args[hostname_idx + 2..]
        .iter()
        .map(|s| s.trim().to_string())
        .collect();

    let job_id: u64 = env::var("VELOCE_JOB_ID")
        .context("VELOCE_JOB_ID not set")?
        .parse()?;
    let secret = env::var("VELOCE_SECRET").context("VELOCE_SECRET not set")?;
    let controller_url =
        env::var("VELOCE_CONTROLLER_URL").context("VELOCE_CONTROLLER_URL not set")?;
    let working_directory = env::current_dir()?.to_string_lossy().to_string();

    let mut client_builder = reqwest::Client::builder()
        .danger_accept_invalid_hostnames(true)
        .timeout(std::time::Duration::from_secs(3600)); // 1 hour timeout for long MPI jobs

    // Load CA cert if provided
    if let Ok(ca_path) = env::var("VELOCE_CA_CERT") {
        let cert_data = std::fs::read(ca_path).context("Failed to read VELOCE_CA_CERT")?;
        let cert =
            reqwest::Certificate::from_pem(&cert_data).context("Failed to parse CA certificate")?;
        client_builder = client_builder.add_root_certificate(cert);
    }

    let client = client_builder.build()?;

    let payload = LaunchRequest {
        job_id,
        secret,
        hostname: hostname.to_string(),
        binary: binary.to_string(),
        args: cmd_args,
        working_directory,
    };

    let res = client
        .post(format!("{}/api/v1/internal/launch", controller_url))
        .json(&payload)
        .send()
        .await
        .context("Failed to send launch request to controller")?;

    if !res.status().is_success() {
        let status = res.status();
        let err_text = res.text().await.unwrap_or_default();
        eprintln!("Launch failed: {} - {}", status, err_text);
        std::process::exit(1);
    }

    use futures::StreamExt;
    use std::io::Write;

    let mut stream = res.bytes_stream();
    let mut buffer = Vec::new();
    let mut exit_code = 0i32;

    while let Some(chunk_res) = stream.next().await {
        let chunk = chunk_res.context("Error reading response stream")?;
        buffer.extend_from_slice(&chunk);

        let mut read_pos = 0;
        loop {
            if buffer.len() - read_pos < 5 {
                break;
            }

            let stream_type = buffer[read_pos];
            let len = u32::from_be_bytes([
                buffer[read_pos + 1],
                buffer[read_pos + 2],
                buffer[read_pos + 3],
                buffer[read_pos + 4],
            ]) as usize;

            if buffer.len() - read_pos < 5 + len {
                break;
            }

            let payload_start = read_pos + 5;
            let payload_end = payload_start + len;
            let payload = &buffer[payload_start..payload_end];

            match stream_type {
                1 => {
                    std::io::stdout().write_all(payload)?;
                    let _ = std::io::stdout().flush();
                }
                2 => {
                    std::io::stderr().write_all(payload)?;
                    let _ = std::io::stderr().flush();
                }
                3 => {
                    if payload.len() == 4 {
                        exit_code =
                            i32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
                    }
                }
                _ => {}
            }

            read_pos = payload_end;
        }

        if read_pos > 0 {
            buffer.drain(..read_pos);
        }
    }

    if exit_code != 0 {
        std::process::exit(exit_code);
    }

    Ok(())
}
