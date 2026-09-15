#![allow(clippy::all)]
pub mod apptainer;
mod bpf_filter;
mod cgroups;
mod config;
mod connection;
mod inputs;
mod job_runner;
mod log_manager;
mod outputs;
mod pmi;
#[cfg(feature = "pmix")]
mod pmix;
mod recovery;
mod resources;
mod telemetry;
pub mod terminal;

use anyhow::{Context, Result};
use clap::Parser;
use config::{load_config, Args};
use connection::connect_and_run;
use log_manager::{LogManager, GLOBAL_LOG_TX};
use nvml_wrapper::Nvml;
use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};
use tracing::{info, warn, Level};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, Layer};
use uuid::Uuid;

#[cfg(unix)]
use tokio::signal::unix::{signal, SignalKind};

use resources::START_TIME;
use veloce_common::Message;

const WORKER_ID_FILE: &str = "data/.veloce_worker_id";

pub struct NoiseLogLayer {
    tx: mpsc::Sender<Message>,
    worker_id: String,
}

impl<S: tracing::Subscriber> Layer<S> for NoiseLogLayer {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let mut labels = std::collections::HashMap::new();
        labels.insert("worker_id".to_string(), self.worker_id.clone());
        labels.insert("level".to_string(), event.metadata().level().to_string());
        labels.insert("target".to_string(), event.metadata().target().to_string());

        let mut line = String::new();
        struct Visitor<'a>(&'a mut String);
        impl<'a> tracing::field::Visit for Visitor<'a> {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                if field.name() == "message" {
                    use std::fmt::Write;
                    write!(self.0, "{:?}", value).ok();
                }
            }
        }
        event.record(&mut Visitor(&mut line));

        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let _ = self.tx.try_send(Message::LogStream {
            labels,
            line,
            timestamp,
        });
    }
}

fn ensure_host_user() {
    #[cfg(unix)]
    {
        if unsafe { libc::getuid() } == 0 {
            if let (Ok(user), Ok(uid_str), Ok(gid_str)) = (
                std::env::var("HOST_USER"),
                std::env::var("HOST_UID"),
                std::env::var("HOST_GID"),
            ) {
                if !user.is_empty() && !uid_str.is_empty() && !gid_str.is_empty() {
                    info!(
                        "Ensuring host user '{}' (UID: {}, GID: {}) exists inside container...",
                        user, uid_str, gid_str
                    );

                    let group_status = std::process::Command::new("groupadd")
                        .args(&["-g", &gid_str, &user])
                        .status();
                    match group_status {
                        Ok(status) if status.success() => {
                            info!("Created group '{}' with GID {}", user, gid_str)
                        }
                        Ok(_) => {
                            tracing::debug!("groupadd did not succeed (group may already exist)")
                        }
                        Err(e) => warn!("Failed to run groupadd: {}", e),
                    }

                    let user_status = std::process::Command::new("useradd")
                        .args(&[
                            "-u",
                            &uid_str,
                            "-g",
                            &gid_str,
                            "-m",
                            "-s",
                            "/bin/bash",
                            &user,
                        ])
                        .status();
                    match user_status {
                        Ok(status) if status.success() => {
                            info!("Created user '{}' with UID {}", user, uid_str)
                        }
                        Ok(_) => {
                            tracing::debug!("useradd did not succeed (user may already exist)")
                        }
                        Err(e) => warn!("Failed to run useradd: {}", e),
                    }
                }
            }
        }
    }
}

fn get_worker_id() -> Result<String> {
    let path = Path::new(WORKER_ID_FILE);
    if path.exists() {
        let mut file = File::open(path).context("Failed to open worker ID file")?;
        let mut id = String::new();
        file.read_to_string(&mut id)
            .context("Failed to read worker ID file")?;
        Ok(id.trim().to_string())
    } else {
        let id = Uuid::new_v4().to_string();
        let mut file = File::create(path).context("Failed to create worker ID file")?;
        file.write_all(id.as_bytes())
            .context("Failed to write worker ID file")?;
        Ok(id)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let _ = rustls::crypto::ring::default_provider().install_default();

    START_TIME.set(SystemTime::now()).unwrap();

    let (drain_tx, drain_rx) = tokio::sync::watch::channel(false);

    #[cfg(unix)]
    {
        tokio::spawn(async move {
            let mut sigterm = signal(SignalKind::terminate()).unwrap();
            let mut sigint = signal(SignalKind::interrupt()).unwrap();
            tokio::select! {
                _ = sigterm.recv() => info!("SIGTERM received"),
                _ = sigint.recv() => info!("SIGINT received"),
            };
            info!("Starting graceful drain...");
            let _ = drain_tx.send(true);
        });
    }

    let args = Args::parse();
    let config = load_config();

    if let Err(e) = std::fs::create_dir_all("data") {
        warn!("Failed to create data directory: {}", e);
    }

    let worker_id = if let Some(sid) = &args.sim_id {
        sid.clone()
    } else {
        get_worker_id()?
    };

    let (log_tx, log_rx) = mpsc::channel(10000);
    GLOBAL_LOG_TX.set(log_tx).unwrap();

    let (log_mgr_tx, log_mgr_rx) = mpsc::channel(32);
    let log_manager = LogManager::new(log_rx);
    tokio::spawn(log_manager.run(log_mgr_rx));

    let file_appender = tracing_appender::rolling::never(".", "veloce-worker.log");
    let (non_blocking, _log_guard) = tracing_appender::non_blocking(file_appender);

    let noise_layer = NoiseLogLayer {
        tx: GLOBAL_LOG_TX.get().unwrap().clone(),
        worker_id: worker_id.clone(),
    };

    let registry = tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::from_default_env().add_directive(Level::INFO.into()))
        .with(tracing_subscriber::fmt::layer())
        .with(tracing_subscriber::fmt::layer().with_writer(non_blocking))
        .with(noise_layer);

    if let Some(tracer) = telemetry::try_otlp_tracer(&worker_id) {
        registry
            .with(tracing_opentelemetry::layer().with_tracer(tracer))
            .init();
        if let Some(endpoint) = telemetry::otlp_endpoint() {
            info!("OTLP trace export enabled (endpoint={endpoint})");
        }
    } else {
        registry.init();
    }

    info!("Worker ID: {}", worker_id);

    ensure_host_user();

    #[cfg(feature = "pmix")]
    {
        if let Err(e) = pmix::pmix_server::init_global_server() {
            tracing::error!("Failed to initialize global PMIx server: {}", e);
        }
    }

    let mut config = config;
    let nvml = Nvml::init().ok().map(Arc::new);
    if nvml.is_some() {
        info!("NVML initialized, GPU telemetry enabled.");
    } else {
        warn!("NVML initialization failed, GPU telemetry disabled.");
    }

    if let Some(c) = &args.controllers {
        config.controllers = c.split(',').map(|s| s.trim().to_string()).collect();
    }
    if let Some(s) = &args.cluster_secret {
        config.cluster_secret = Some(s.clone());
    }
    if let Some(f) = &args.fileserver_url {
        config.fileserver_url = Some(f.clone());
    }
    if let Some(key) = &args.fileserver_api_key {
        config.fileserver_api_key = Some(key.clone());
    }
    if let Some(c) = &args.controller {
        config.controller = Some(c.clone());
    }
    if let Some(t) = args.idle_timeout_seconds {
        config.idle_timeout_seconds = Some(t);
    }
    if let Some(cpu) = args.idle_threshold_cpu {
        config.idle_threshold_cpu = Some(cpu);
    }
    if let Some(t) = &args.worker_token {
        config.worker_token = Some(t.clone());
    }

    let mut controller_list = config.controllers.clone();
    if let Some(c) = config.controller.clone() {
        if !controller_list.contains(&c) {
            controller_list.insert(0, c);
        }
    }
    if controller_list.is_empty() {
        controller_list.push("127.0.0.1:9000".into());
    }

    let mut rng = {
        let mut h = 0u64;
        for b in worker_id.bytes() {
            h = h.wrapping_mul(31).wrapping_add(b as u64);
        }
        h
    };
    let next_u32 = |rng: &mut u64| {
        *rng = rng
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (*rng >> 32) as u32
    };

    if controller_list.len() > 1 {
        for i in (1..controller_list.len()).rev() {
            let j = (next_u32(&mut rng) as usize) % (i + 1);
            controller_list.swap(i, j);
        }
    }

    info!("Controller connection order: {:?}", controller_list);

    let mut current_idx = 0;

    loop {
        if *drain_rx.borrow() {
            info!("Worker is draining. Checking if jobs are finished...");
        }

        let addr = &controller_list[current_idx];
        info!(
            "Connecting to controller at {} (Target {}/{})",
            addr,
            current_idx + 1,
            controller_list.len()
        );
        match connect_and_run(
            addr,
            &worker_id,
            &config,
            args.clone(),
            drain_rx.clone(),
            log_mgr_tx.clone(),
            nvml.clone(),
        )
        .await
        {
            Ok(_) => {
                if *drain_rx.borrow() {
                    info!("Drain complete. Exiting.");
                    break;
                }
                warn!("Connection closed by controller, trying next controller in 5s...");
                current_idx = (current_idx + 1) % controller_list.len();
            }
            Err(e) => {
                tracing::error!(
                    "Connection error to {}: {}. Trying next controller in 5s...",
                    addr,
                    e
                );
                current_idx = (current_idx + 1) % controller_list.len();
            }
        }

        if *drain_rx.borrow() {
            break;
        }
        sleep(Duration::from_secs(5)).await;
    }

    #[cfg(feature = "pmix")]
    {
        if let Err(e) = pmix::pmix_server::finalize_global_server() {
            tracing::error!("Failed to finalize global PMIx server: {}", e);
        }
    }

    Ok(())
}
