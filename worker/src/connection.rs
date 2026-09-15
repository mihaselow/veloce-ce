#![allow(clippy::all)]
use crate::cgroups;
use crate::config::{Args, Config};
use crate::job_runner::{run_job, run_step, JobStatsEntry, KillSignal};
use crate::log_manager::LogManagerCmd;
use crate::outputs::{collect_declared_output_artifacts, read_declared_output_chunk};
use crate::recovery::{
    load_persisted_job_states, load_persisted_job_wds, persist_job_wd, recover_and_monitor_job,
    JOB_WDS_FILE, PERSISTENCE_FILE,
};
use crate::resources::{
    clock_ticks_per_second, get_cgroup_process_resources, get_disk_io,
    get_process_descendants_resources, get_process_group_resources, get_resources,
    read_cgroup_cpu_usec, read_cgroup_memory_bytes, read_namespace_process_totals, START_TIME,
};
use crate::terminal;
use anyhow::{Context, Result};
use futures::{SinkExt, StreamExt};
use nvml_wrapper::Nvml;
use portable_pty::PtySize;
use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use sysinfo::{CpuExt, DiskExt, NetworkExt, Pid, PidExt, System, SystemExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::{sleep, Duration};
use tokio_util::codec::Framed;
use tracing::{debug, error, info, warn};
use veloce_common::{noise, JobStats, Message, MessageCodec, NodeMetrics};

pub(crate) async fn connect_and_run(
    addr: &str,
    worker_id: &str,
    config: &Config,
    cli_args: Args,
    mut drain_rx: tokio::sync::watch::Receiver<bool>,
    log_mgr_tx: mpsc::Sender<LogManagerCmd>,
    nvml: Option<Arc<Nvml>>,
) -> Result<()> {
    let stream = TcpStream::connect(addr)
        .await
        .context("Failed to connect to controller")?;

    // Noise Upgrade
    let secret = match config.cluster_secret.as_deref() {
        Some(s) if s == "veloce_default_secret_change_me" => {
            warn!("Using the DEFAULT cluster secret is extremely insecure!");
            s
        }
        Some(s) => s,
        None => {
            error!("CRITICAL: No cluster_secret configured. Set VELOCE_SECRET or cluster_secret in config.");
            std::process::exit(1);
        }
    };
    let stream = noise::upgrade_initiator(stream, secret)
        .await
        .context("Noise Handshake failed")?;

    let fileserver_url = config
        .fileserver_url
        .clone()
        .unwrap_or_else(|| "https://veloce-fileserver-ha:9001".to_string());
    let fileserver_api_key = config
        .fileserver_api_key
        .clone()
        .unwrap_or_else(|| secret.to_string());
    let file_client = Arc::new(veloce_common::file_client::FileClient::new(
        fileserver_url,
        fileserver_api_key,
    ));

    let mut framed = Framed::new(stream, MessageCodec::new());
    let worker_id_for_job = worker_id.to_string();

    let mut cgroup_manager = cgroups::CgroupManager::new();
    if let Err(e) = cgroup_manager.init() {
        warn!(
            "Failed to initialize cgroups: {}. Falling back to setrlimit.",
            e
        );
    }
    let cgroup_manager = Arc::new(cgroup_manager);

    let mut sys = System::new_all();
    sys.refresh_all();
    let mut initial_resources =
        get_resources(&sys, &config, cgroup_manager.is_enabled(), nvml.as_ref());
    if cli_args.simulate {
        initial_resources.cpu_cores = cli_args.sim_cores;
        initial_resources.total_memory = cli_args.sim_memory * 1024 * 1024;
        initial_resources.free_memory = cli_args.sim_memory * 1024 * 1024;
        initial_resources.cpu_model = "Simulated Phantom CPU".to_string();
    }
    let sys_hostname = sys.host_name().unwrap_or_else(|| "unknown".to_string());
    let sys_shared = Arc::new(Mutex::new(sys));

    let features = veloce_common::FEATURE_RESERVATIONS
        | veloce_common::FEATURE_DEPENDENCIES
        | veloce_common::FEATURE_BACKFILLING
        | veloce_common::FEATURE_ACCOUNTING;
    framed
        .send(Message::HelloWorker {
            worker_id: worker_id.to_string(),
            hostname: sys_hostname,
            resources: initial_resources,
            features: Some(features),
            registration_token: config.worker_token.clone(),
        })
        .await?;

    // Split
    let (mut sink, mut stream) = framed.split();
    let (tx, mut rx) = mpsc::channel(32);

    let _ = log_mgr_tx.send(LogManagerCmd::SetTx(tx.clone())).await;

    let sink_handle = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if sink.send(msg).await.is_err() {
                break;
            }
        }
    });

    let tx_heartbeat = tx.clone();
    let sys_heartbeat = sys_shared.clone();
    let nvml_heartbeat = nvml.clone();
    let nvml_metrics = nvml.clone();
    let running_pids_for_heartbeat = Arc::new(Mutex::new(HashMap::<u64, Pid>::new())); // JobId -> Pid
    let running_pids_for_spawn = running_pids_for_heartbeat.clone();
    let running_jobs: Arc<Mutex<HashMap<u64, oneshot::Sender<KillSignal>>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let running_jobs_cleanup = running_jobs.clone();
    let running_steps: Arc<Mutex<HashMap<u64, Vec<mpsc::Sender<()>>>>> =
        Arc::new(Mutex::new(HashMap::new()));

    let running_steps_spawn = running_steps.clone();

    // Use a map to store job details for log retrieval
    let wds_map = load_persisted_job_wds(Path::new(JOB_WDS_FILE)).unwrap_or_else(|e| {
        error!("Failed to load persisted job WDs: {}", e);
        HashMap::new()
    });
    info!(
        "Loaded {} job working directories from persistence.",
        wds_map.len()
    );

    let job_wds: Arc<Mutex<HashMap<u64, String>>> = Arc::new(Mutex::new(wds_map));
    let job_stats_history: Arc<Mutex<HashMap<u64, JobStatsEntry>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let job_stats_history_spawn = job_stats_history.clone();
    let job_stats_history_heartbeat = job_stats_history.clone();
    let pmi_routers: Arc<Mutex<HashMap<(u64, u32), mpsc::Sender<Message>>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let pmi_routers_spawn = pmi_routers.clone();
    let active_terminal_sessions: Arc<Mutex<HashMap<u64, terminal::ActiveTerminalSession>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let active_terminal_sessions_clone = active_terminal_sessions.clone();
    struct ActiveVncProxy {
        input_tx: mpsc::Sender<Vec<u8>>,
        abort_read: tokio::task::AbortHandle,
        abort_write: tokio::task::AbortHandle,
    }
    let active_vnc_sessions: Arc<Mutex<HashMap<u64, ActiveVncProxy>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let active_vnc_sessions_clone = active_vnc_sessions.clone();
    let vnc_ports: Arc<Mutex<HashMap<u64, u16>>> = Arc::new(Mutex::new(HashMap::new()));
    let vnc_ports_clone = vnc_ports.clone();

    match load_persisted_job_states(Path::new(PERSISTENCE_FILE)) {
        Ok(persisted_jobs) => {
            info!(
                "Found {} persisted jobs. Attempting to recover...",
                persisted_jobs.len()
            );
            let mut wds = job_wds.lock().unwrap();
            for p_job in persisted_jobs {
                wds.insert(p_job.job_id, p_job.working_directory.clone());

                let job_id = p_job.job_id;
                let (tx_kill, rx_kill) = oneshot::channel::<KillSignal>();
                {
                    let mut jobs = running_jobs.lock().unwrap();
                    jobs.insert(job_id, tx_kill);
                    // Add pid to running_pids map for heartbeat
                    let mut pids = running_pids_for_spawn.lock().unwrap();
                    pids.insert(job_id, Pid::from(p_job.pid as usize));
                }
                let tx_clone = tx.clone();
                let running_pids_for_this_spawn = running_pids_for_heartbeat.clone(); // Clone for this spawn
                let running_jobs_cleanup_clone = running_jobs.clone();

                let stats_history_recovery = job_stats_history.clone();

                tokio::spawn(async move {
                    let tx_for_error = tx_clone.clone(); // Clone for error reporting
                    if let Err(e) = recover_and_monitor_job(
                        p_job,
                        tx_clone,
                        rx_kill,
                        running_pids_for_this_spawn.clone(),
                        stats_history_recovery,
                    )
                    .await
                    {
                        // Pass clone
                        error!("Failed to recover and monitor job {}: {}", job_id, e);
                        // Notify controller of startup failure
                        let _ = tx_for_error
                            .send(Message::JobError {
                                job_id,
                                error: format!("Failed to recover job: {}", e),
                            })
                            .await;
                    }
                    // Cleanup kill channel
                    let mut jobs = running_jobs_cleanup_clone.lock().unwrap();
                    jobs.remove(&job_id);
                    // Cleanup PID from tracking
                    let mut pids_lock = running_pids_for_this_spawn.lock().unwrap(); // Use this clone
                    pids_lock.remove(&job_id);
                });
            }
        }
        Err(e) => {
            error!("Failed to load persisted job states on startup: {}", e);
        }
    }

    let job_wds_spawn = job_wds.clone();

    // Pulse Interval Watch
    let (pulse_interval_tx, pulse_interval_rx) = watch::channel(2.0f32);

    // Metrics Task
    let tx_metrics = tx.clone();
    let worker_id_metrics = worker_id.to_string();
    let sys_metrics = sys_shared.clone();
    let running_pids_metrics = running_pids_for_heartbeat.clone();
    let cgroup_manager_metrics = cgroup_manager.clone();

    tokio::spawn(async move {
        let mut last_sample = tokio::time::Instant::now();
        let mut last_rx = 0u64;
        let mut last_tx = 0u64;
        let mut last_rx_pkts = 0u64;
        let mut last_tx_pkts = 0u64;
        let (mut last_d_read, mut last_d_write) = get_disk_io();

        // Initial sample
        {
            let mut s = sys_metrics.lock().unwrap();
            s.refresh_all();
            for (_name, network) in s.networks() {
                last_rx += network.total_received();
                last_tx += network.total_transmitted();
                last_rx_pkts += network.total_packets_received();
                last_tx_pkts += network.total_packets_transmitted();
            }
        }

        loop {
            let interval_secs = *pulse_interval_rx.borrow();
            tokio::time::sleep(Duration::from_secs_f32(interval_secs)).await;

            let now = tokio::time::Instant::now();
            let delta = now.duration_since(last_sample).as_secs_f64();
            if delta < 0.1 {
                continue;
            } // Avoid division by zero
            last_sample = now;

            let (metrics, rx_total, tx_total, rx_p_total, tx_p_total, d_read_total, d_write_total) = {
                let mut s = sys_metrics.lock().unwrap();
                s.refresh_all();

                let running_jobs_count = running_pids_metrics.lock().unwrap().len() as u32;
                let cpu_load = s.global_cpu_info().cpu_usage();
                let memory_usage = s.used_memory();

                let mut rx = 0u64;
                let mut tx = 0u64;
                let mut rx_p = 0u64;
                let mut tx_p = 0u64;
                for (_name, network) in s.networks() {
                    rx += network.total_received();
                    tx += network.total_transmitted();
                    rx_p += network.total_packets_received();
                    tx_p += network.total_packets_transmitted();
                }

                let rx_delta = rx.saturating_sub(last_rx);
                let tx_delta = tx.saturating_sub(last_tx);
                let rx_p_delta = rx_p.saturating_sub(last_rx_pkts);
                let tx_p_delta = tx_p.saturating_sub(last_tx_pkts);

                let (d_read_total, d_write_total) = get_disk_io();
                let d_read_delta = d_read_total.saturating_sub(last_d_read);
                let d_write_delta = d_write_total.saturating_sub(last_d_write);

                // GPU utilization from NVML if available
                let mut gpu_util = 0.0;
                let mut gpu_mem_util = 0.0;
                let mut gpu_temp = 0.0;
                if let Some(n) = &nvml_metrics {
                    if let Ok(device) = n.device_by_index(0) {
                        if let Ok(util) = device.utilization_rates() {
                            gpu_util = util.gpu as f32;
                            gpu_mem_util = util.memory as f32;
                        }
                        if let Ok(temp) = device.temperature(
                            nvml_wrapper::enum_wrappers::device::TemperatureSensor::Gpu,
                        ) {
                            gpu_temp = temp as f32;
                        }
                    }
                }

                let mut total_mem = s.total_memory();
                let mut cpu_load_val = cpu_load;
                let mut mem_usage = memory_usage;
                if cli_args.simulate {
                    total_mem = cli_args.sim_memory * 1024 * 1024;
                    cpu_load_val = 0.5; // Simulate low CPU footprint
                    mem_usage = 100 * 1024 * 1024; // Simulate 100MB usage
                }
                (
                    NodeMetrics {
                        node_id: worker_id_metrics.clone(),
                        timestamp: SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_secs(),
                        running_jobs: running_jobs_count,
                        cpu_load: cpu_load_val,
                        memory_usage: mem_usage,
                        memory_total: total_mem,
                        disk_usage: s
                            .disks()
                            .iter()
                            .map(|d| d.total_space() - d.available_space())
                            .sum(),
                        disk_total: s.disks().iter().map(|d| d.total_space()).sum(),
                        load_avg: [
                            s.load_average().one as f32,
                            s.load_average().five as f32,
                            s.load_average().fifteen as f32,
                        ],
                        net_rx_rate: (rx_delta as f64 / delta) as u64,
                        net_tx_rate: (tx_delta as f64 / delta) as u64,
                        net_packets_rx_rate: (rx_p_delta as f64 / delta) as u64,
                        net_packets_tx_rate: (tx_p_delta as f64 / delta) as u64,
                        net_errors: 0,
                        net_drops: 0,
                        disk_read_rate: (d_read_delta as f64 / delta) as u64,
                        disk_write_rate: (d_write_delta as f64 / delta) as u64,
                        disk_read_ops_rate: 0,
                        disk_write_ops_rate: 0,
                        procs_running: s.processes().len() as u32, // Simplified
                        procs_blocked: 0,
                        swap_usage: s.used_swap(),
                        process_count: s.processes().len() as u32,
                        uptime: START_TIME
                            .get()
                            .map(|t| t.elapsed().unwrap_or_default().as_secs())
                            .unwrap_or(0),
                        cgroup_enabled: cgroup_manager_metrics.is_enabled(),
                        gpu_usage: if nvml_metrics.is_some() {
                            Some(gpu_util)
                        } else {
                            None
                        },
                        gpu_mem_usage: if nvml_metrics.is_some() {
                            Some(gpu_mem_util as u64)
                        } else {
                            None
                        },
                        gpu_temp: if nvml_metrics.is_some() {
                            Some(gpu_temp)
                        } else {
                            None
                        },
                    },
                    rx,
                    tx,
                    rx_p,
                    tx_p,
                    d_read_total,
                    d_write_total,
                )
            };

            last_rx = rx_total;
            last_tx = tx_total;
            last_rx_pkts = rx_p_total;
            last_tx_pkts = tx_p_total;
            last_d_read = d_read_total;
            last_d_write = d_write_total;

            if tx_metrics.send(Message::Pulse(metrics)).await.is_err() {
                break;
            }
        }
    });

    let threshold_cpu = config.idle_threshold_cpu.unwrap_or(0.5);
    let timeout_secs = config.idle_timeout_seconds.unwrap_or(600);

    // Heartbeat task
    let cgroup_manager_heartbeat = cgroup_manager.clone();
    let config_heartbeat = config.clone();
    tokio::spawn(async move {
        loop {
            sleep(Duration::from_secs(1)).await; // Faster heartbeat for monitoring
            let (resources, job_stats) = {
                let mut s = sys_heartbeat.lock().unwrap();
                s.refresh_all(); // Refresh system and process info

                let mut current_job_stats = Vec::new();
                let pids_lock = running_pids_for_heartbeat.lock().unwrap();

                // Update stats history
                let mut stats_history = job_stats_history_heartbeat.lock().unwrap();

                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs();

                for (&job_id, &pid) in pids_lock.iter() {
                    if let Some(entry) = stats_history.get_mut(&job_id) {
                        let has_process = s.process(pid).is_some();
                        if !has_process {
                            #[cfg(target_family = "unix")]
                            {
                                let is_alive = unsafe { libc::kill(pid.as_u32() as i32, 0) == 0 };
                                if is_alive {
                                    warn!("Process for job {} (PID {}) exists but not found in sysinfo.", job_id, pid);
                                } else {
                                    debug!(
                                        "Process for job {} (PID {}) not found (likely finished).",
                                        job_id, pid
                                    );
                                }
                            }
                            #[cfg(not(target_family = "unix"))]
                            warn!(
                                "Process for job {} (PID {}) not found during heartbeat.",
                                job_id, pid
                            );
                        }

                        let (mut cpu, mut mem) = (0.0f32, 0u64);
                        let mut cgroup_collected = false;

                        if cgroup_manager_heartbeat.is_enabled() {
                            if let Some(cgroup_mem) = read_cgroup_memory_bytes(job_id) {
                                mem = cgroup_mem;
                                let current_usec = read_cgroup_cpu_usec(job_id);
                                let current_time = std::time::Instant::now();
                                if let Some(curr_usec) = current_usec {
                                    if let (Some(last_usec), Some(last_time)) =
                                        (entry.cgroup_last_cpu_usec, entry.cgroup_last_measured_at)
                                    {
                                        let delta_usec = curr_usec.saturating_sub(last_usec);
                                        let delta_time_usec =
                                            current_time.duration_since(last_time).as_micros()
                                                as u64;
                                        if delta_time_usec > 0 {
                                            cpu = (delta_usec as f32 / delta_time_usec as f32)
                                                * 100.0;
                                        }
                                    } else {
                                        let (fallback_cpu, _) =
                                            get_process_descendants_resources(pid, &s);
                                        cpu = fallback_cpu;
                                    }
                                    entry.cgroup_last_cpu_usec = Some(curr_usec);
                                    entry.cgroup_last_measured_at = Some(current_time);
                                    let (cgroup_tree_cpu, cgroup_tree_mem) =
                                        get_cgroup_process_resources(job_id, &s);
                                    if cpu == 0.0 && cgroup_tree_cpu > 0.0 {
                                        cpu = cgroup_tree_cpu;
                                    }
                                    if mem == 0 && cgroup_tree_mem > 0 {
                                        mem = cgroup_tree_mem;
                                    }
                                    let (process_group_cpu, process_group_mem) =
                                        get_process_group_resources(pid, &s);
                                    if cpu == 0.0 && process_group_cpu > 0.0 {
                                        cpu = process_group_cpu;
                                    }
                                    if process_group_mem > mem {
                                        mem = process_group_mem;
                                    }
                                    cgroup_collected = true;
                                }
                            }
                        }

                        if !cgroup_collected {
                            let (tree_cpu, tree_mem) = get_process_descendants_resources(pid, &s);
                            let (process_group_cpu, process_group_mem) =
                                get_process_group_resources(pid, &s);
                            cpu = tree_cpu.max(process_group_cpu);
                            mem = tree_mem.max(process_group_mem);
                        }

                        if let Some((namespace_cpu_ticks, namespace_mem)) =
                            read_namespace_process_totals(pid)
                        {
                            let current_time = std::time::Instant::now();
                            if let (Some(last_ticks), Some(last_time)) = (
                                entry.namespace_last_cpu_ticks,
                                entry.namespace_last_measured_at,
                            ) {
                                let delta_ticks = namespace_cpu_ticks.saturating_sub(last_ticks);
                                let delta_time =
                                    current_time.duration_since(last_time).as_secs_f32();
                                let ticks_per_second = clock_ticks_per_second() as f32;
                                if delta_time > 0.0 && ticks_per_second > 0.0 {
                                    let namespace_cpu =
                                        (delta_ticks as f32 / ticks_per_second / delta_time)
                                            * 100.0;
                                    if namespace_cpu > cpu {
                                        cpu = namespace_cpu;
                                    }
                                }
                            }
                            entry.namespace_last_cpu_ticks = Some(namespace_cpu_ticks);
                            entry.namespace_last_measured_at = Some(current_time);
                            if namespace_mem > mem {
                                mem = namespace_mem;
                            }
                        }

                        if mem > entry.max_memory_bytes {
                            entry.max_memory_bytes = mem;
                        }
                        entry.cpu_time_ms += (cpu * 10.0) as u64;

                        // Dynamic multicore threshold: scaled by req_cores (cores per node assigned to job)
                        let scaled_threshold = threshold_cpu * (entry.req_cores.max(1) as f32);

                        // Idle Detection Logic
                        if cpu < scaled_threshold {
                            if !entry.is_idle {
                                if now.saturating_sub(entry.last_active_at) >= timeout_secs {
                                    entry.is_idle = true;
                                    entry.idle_duration = now.saturating_sub(entry.last_active_at);
                                }
                            } else {
                                entry.idle_duration = now.saturating_sub(entry.last_active_at);
                            }
                        } else {
                            entry.is_idle = false;
                            entry.idle_duration = 0;
                            entry.last_active_at = now;
                        }

                        current_job_stats.push(JobStats {
                            job_id,
                            cpu_usage_percent: cpu,
                            memory_usage_bytes: mem,
                            is_idle: entry.is_idle,
                            idle_duration: entry.idle_duration,
                            cgroup_active: cgroup_manager_heartbeat.is_enabled(),
                        });
                    }
                }
                let mut res = get_resources(
                    &s,
                    &config_heartbeat,
                    cgroup_manager_heartbeat.is_enabled(),
                    nvml_heartbeat.as_ref(),
                );
                if cli_args.simulate {
                    res.cpu_cores = cli_args.sim_cores;
                    res.total_memory = cli_args.sim_memory * 1024 * 1024;
                    res.free_memory = cli_args.sim_memory * 1024 * 1024;
                    res.cpu_model = "Simulated Phantom CPU".to_string();
                }
                (res, current_job_stats)
            };
            if tx_heartbeat
                .send(Message::Heartbeat {
                    resources,
                    job_stats,
                })
                .await
                .is_err()
            {
                break;
            }
        }
    });

    // Command Loop
    let mut sent_draining = false;
    let mut current_pulse_interval = 2.0f32;
    loop {
        tokio::select! {
                msg_res = stream.next() => {
                    let msg = match msg_res {
                        Some(Ok(m)) => m,
                        Some(Err(e)) => anyhow::bail!("Stream error: {}", e),
                        None => break,
                    };

                    match msg {
                Message::RunJob { job_id, binary, args, node_list, assigned_cores, req_memory, working_directory, user_id, walltime, submission_time, array_id, array_task_id, inputs, allocated_gres, gres_req, secret, controller_url, env_vars, wait_for_licenses, estimated_walltime, priority_offset, dependencies, dependency_specs, qos, container_asset, vnc_enabled, inherit_host_env, env_allowlist, job_profile } => {
                    let controller_ip = addr.split(':').next().unwrap_or("localhost");
                    let controller_url = controller_url.replace("localhost", controller_ip);

                    let cores_str = assigned_cores.as_ref()
                        .map(|c| format!("{} (IDs: {:?})", c.len(), c))
                        .unwrap_or_else(|| "None".to_string());
                    info!("Starting job {}: {} {:?} on nodes {:?} with cores: {} and memory {}MB in {} as user {} (walltime: {}s) [Array: {:?}:{:?}]",
                        job_id, binary, args, node_list, cores_str, req_memory, working_directory, user_id, walltime, array_id, array_task_id);

                    // Track WD
                    {
                        let mut wds = job_wds_spawn.lock().unwrap();
                        let effective_wd = if config.run_as_worker_user {
                            std::env::temp_dir().to_string_lossy().to_string()
                        } else {
                            working_directory.clone()
                        };
                        wds.insert(job_id, effective_wd.clone());

                        if let Err(e) = persist_job_wd(Path::new(JOB_WDS_FILE), job_id, &effective_wd) {
                            error!("Failed to persist working directory for job {}: {}", job_id, e);
                        }
                    }

                    // Create kill channel
                    let (tx_kill, rx_kill) = oneshot::channel::<KillSignal>();
                    {
                        let mut jobs = running_jobs.lock().unwrap();
                        jobs.insert(job_id, tx_kill);
                    }

                    let running_jobs_for_this_job_closure = running_jobs_cleanup.clone();
                    let running_pids_for_this_job_closure = running_pids_for_spawn.clone();
                    let _job_stats_history = job_stats_history_spawn.clone();
                    let run_as_worker_user = config.run_as_worker_user;
                    let prolog = config.prolog.clone();
                    let epilog = config.epilog.clone();
                    let tx_clone = tx.clone();
                    let file_client = file_client.clone();

                    let cgroup_manager_clone = cgroup_manager.clone();
                    let worker_id_spawn = worker_id_for_job.clone();
                    let running_pids_for_job = running_pids_for_spawn.clone();
                    let job_stats_history_for_job = job_stats_history_spawn.clone();
                    let vnc_ports_spawn = vnc_ports_clone.clone();

                    let is_simulate = cli_args.simulate;
                    let sim_duration = cli_args.sim_job_duration;

                    let worker_id_sim = worker_id_spawn.clone();
                    let node_list_sim = node_list.clone();

                    tokio::spawn(async move {
                        if is_simulate {
                            let rank = node_list_sim.iter().position(|id| id == &worker_id_sim).unwrap_or(0);
                            let _ = tx_clone.send(Message::JobStarted { job_id }).await;
                            info!("Simulating job {} on node {} (Rank {}/{}) for {} seconds...",
                                job_id, worker_id_sim, rank, node_list_sim.len(), sim_duration);
                            tokio::time::sleep(tokio::time::Duration::from_secs(sim_duration)).await;
                            let _ = tx_clone.send(Message::JobDone { job_id, exit_code: 0 }).await;

                            // Cleanup channels just like a real job
                            let mut jobs = running_jobs_for_this_job_closure.lock().unwrap();
                            jobs.remove(&job_id);
                            return;
                        }

                        if let Err(e) = run_job(
                            job_id,
                            binary,
                            args,
                            node_list,
                            assigned_cores,
                            req_memory,
                            working_directory.clone(),
                            user_id,
                            walltime,
                            submission_time,
                            array_id,
                            array_task_id,
                            prolog,
                            epilog,
                            tx_clone.clone(),
                            rx_kill,
                            running_pids_for_job.clone(),
                            job_stats_history_for_job.clone(),
                            run_as_worker_user,
                            file_client.clone(),
                            inputs,
                            cgroup_manager_clone,
                            gres_req.clone(),
                            allocated_gres,
                            worker_id_spawn,
                            secret,
                            wait_for_licenses,
                            controller_url,
                            env_vars,
                            estimated_walltime,
                            priority_offset,
                            dependencies,
                            dependency_specs.clone(),
                            qos,
                            container_asset,
                            vnc_enabled,
                            inherit_host_env,
                            env_allowlist,
                            job_profile,
                            vnc_ports_spawn,
                        ).await {
                            error!("Job {} failed to start: {}", job_id, e);
                             // Notify controller of startup failure
                             let _ = tx_clone.send(Message::JobError { job_id, error: e.to_string() }).await;
                        }
                        // Cleanup kill channel
                        let mut jobs = running_jobs_for_this_job_closure.lock().unwrap();
                        jobs.remove(&job_id);
                        // Cleanup PID from tracking
                        let mut pids_lock = running_pids_for_this_job_closure.lock().unwrap();
                        pids_lock.remove(&job_id);
                    });
                }
                Message::RunStep { parent_job_id, step_id, binary, args, node_list, assigned_cores, working_directory, user_id, assigned_ranks, total_ranks, inputs, allocated_gres: _, gres_req: _, secret, controller_url, env_vars } => {
                    let controller_ip = addr.split(':').next().unwrap_or("localhost");
                    let controller_url = controller_url.replace("localhost", controller_ip);

                    info!("RECEIVED RunStep {}:{} from controller", parent_job_id, step_id);
                    info!("Starting step {}:{} {} {:?} on nodes {:?} with {} ranks", parent_job_id, step_id, binary, args, node_list, assigned_ranks.len());
                    let (pmi_tx, pmi_rx) = mpsc::channel(32);
                    let (kill_tx, kill_rx) = mpsc::channel(1);

                    {
                        let mut routers = pmi_routers_spawn.lock().unwrap();
                        routers.insert((parent_job_id, step_id), pmi_tx);
                    }

                    {
                        let mut steps_lock = running_steps_spawn.lock().unwrap();
                        steps_lock.entry(parent_job_id).or_insert_with(Vec::new).push(kill_tx);
                    }

                    let routers_cleanup = pmi_routers_spawn.clone();
                    let tx_step = tx.clone();
                    let tx_step_err = tx.clone();
                    let run_as_worker_user = config.run_as_worker_user;
                    let fc_step = file_client.clone();

                    let cgroup_manager_clone_step = cgroup_manager.clone();
                    tokio::spawn(async move {
                        if let Err(e) = run_step(parent_job_id, step_id, binary, args, node_list, assigned_cores, working_directory, user_id, assigned_ranks, total_ranks, tx_step, run_as_worker_user, pmi_rx, fc_step, inputs, kill_rx, cgroup_manager_clone_step, secret, controller_url, env_vars).await {
                            error!("Step {}:{} failed to start: {}", parent_job_id, step_id, e);
                            let _ = tx_step_err.send(veloce_common::Message::StepError { parent_job_id, step_id, error: e.to_string() }).await;
                        }

                        let mut routers = routers_cleanup.lock().unwrap();
                        routers.remove(&(parent_job_id, step_id));
                    });
                }
                m @ Message::StepPMIGetResponse { .. } => {
                    if let Message::StepPMIGetResponse { parent_job_id, step_id, .. } = &m {
                        let routers = pmi_routers_spawn.lock().unwrap();
                        if let Some(r_tx) = routers.get(&(*parent_job_id, *step_id)) {
                            let _ = r_tx.send(m).await;
                        }
                    }
                }
                m @ Message::StepPMIBarrierRelease { .. } => {
                    if let Message::StepPMIBarrierRelease { parent_job_id, step_id } = &m {
                        let routers = pmi_routers_spawn.lock().unwrap();
                        if let Some(r_tx) = routers.get(&(*parent_job_id, *step_id)) {
                            let _ = r_tx.send(m).await;
                        }
                    }
                }
                Message::TerminateJob { job_id } => {
                    info!("Request to terminate job {} and all its steps", job_id);

                    // 1. Kill parent job
                    let mut jobs = running_jobs.lock().unwrap();
                    if let Some(tx_kill) = jobs.remove(&job_id) {
                        let _ = tx_kill.send(KillSignal::Cancel);
                    } else {
                        warn!("Job {} not found or already finished", job_id);
                    }

                    // 2. Kill all associated steps
                    let mut steps_lock = running_steps_spawn.lock().unwrap();
                    if let Some(step_kills) = steps_lock.remove(&job_id) {
                        for tx_kill in step_kills {
                            let _ = tx_kill.try_send(());
                        }
                    }
                }
                Message::PreemptJob { job_id } => {
                    info!("Request to preempt job {} and all its steps", job_id);

                    // 1. Preempt parent job
                    let mut jobs = running_jobs.lock().unwrap();
                    if let Some(tx_kill) = jobs.remove(&job_id) {
                        let _ = tx_kill.send(KillSignal::Preempt);
                    } else {
                        warn!("Job {} not found or already finished during preemption", job_id);
                    }

                    // 2. Kill all associated steps
                    let mut steps_lock = running_steps_spawn.lock().unwrap();
                    if let Some(step_kills) = steps_lock.remove(&job_id) {
                        for tx_kill in step_kills {
                            let _ = tx_kill.try_send(());
                        }
                    }
                }
                Message::GetComponentLogs { request_id, component_id: _, lines } => {
                    let content = match std::fs::read_to_string("veloce-worker.log") {
                        Ok(c) => c.lines().rev().take(lines).collect::<Vec<_>>().into_iter().rev().collect::<Vec<_>>().join("\n"),
                        Err(_) => "Log file not found".to_string(),
                    };
                    let hostname = gethostname::gethostname().into_string().unwrap_or_default();
                    let _ = tx.send(Message::ComponentLogs { request_id, component_id: worker_id.to_string(), hostname, content }).await;
                }
                Message::GetSystemLogs { request_id, component_id: _, log_source, lines } => {
                    let content = match log_source.as_str() {
                        "dmesg" => match std::process::Command::new("dmesg").arg("-T").output() {
                            Ok(o) if o.status.success() => String::from_utf8_lossy(&o.stdout).lines().rev().take(lines).collect::<Vec<_>>().into_iter().rev().collect::<Vec<_>>().join("\n"),
                            Ok(o) => format!("dmesg failed ({}): {}", o.status, String::from_utf8_lossy(&o.stderr)),
                            Err(e) => format!("Failed to run dmesg: {}", e),
                        },
                        "syslog" => match std::process::Command::new("tail").args(&["-n", &lines.to_string(), "/var/log/syslog"]).output() {
                            Ok(o) if o.status.success() => String::from_utf8_lossy(&o.stdout).to_string(),
                            Ok(o) => format!("Failed to read syslog ({}): {}", o.status, String::from_utf8_lossy(&o.stderr)),
                            Err(e) => format!("Failed to read syslog: {}", e),
                        },
                        "journal" => match std::process::Command::new("journalctl").args(&["-n", &lines.to_string(), "--no-pager"]).output() {
                            Ok(o) if o.status.success() => String::from_utf8_lossy(&o.stdout).to_string(),
                            Ok(o) => format!("Failed to read journal ({}): {}", o.status, String::from_utf8_lossy(&o.stderr)),
                            Err(e) => format!("Failed to read journal: {}", e),
                        },
                        _ => format!("Unsupported log source: {}", log_source),
                    };
                    let hostname = gethostname::gethostname().into_string().unwrap_or_default();
                    let _ = tx.send(Message::SystemLogs { request_id, component_id: worker_id.to_string(), hostname, content }).await;
                }
                Message::GetLogs { request_id, job_id, log_type, offset, length, working_directory: provided_wd, rank: _ } => {
                     let wd = {
                         let wds = job_wds_spawn.lock().unwrap();
                         wds.get(&job_id).cloned().or(provided_wd)
                     };

                     if let Some(working_dir) = wd {
                         use veloce_common::LogType;
                         let filename = match log_type {
                             LogType::Stdout => format!("{}-{}-stdout.log", job_id, worker_id),
                             LogType::Stderr => format!("{}-{}-stderr.log", job_id, worker_id),
                         };
                         let path = Path::new(&working_dir).join(filename);

                         let content = tokio::task::spawn_blocking(move || -> Result<Vec<u8>> {
                             if !path.exists() {
                                 return Ok(Vec::new());
                             }
                             let mut file = File::open(path)?;
                             file.seek(SeekFrom::Start(offset))?;
                             let mut buffer = Vec::new();
                             if let Some(len) = length {
                                 let mut limited_reader = file.take(len);
                                 limited_reader.read_to_end(&mut buffer)?;
                             } else {
                                 file.read_to_end(&mut buffer)?;
                             }
                             Ok(buffer)
                         }).await.unwrap_or_else(|e| Err(anyhow::anyhow!("Join error: {}", e)));

                         match content {
                             Ok(data) => {
                                 let _ = tx.send(Message::LogData { request_id, job_id, content: data }).await;
                             },
                             Err(e) => {
                                 error!("Failed to read log for job {}: {}", job_id, e);
                                 let _ = tx.send(Message::LogData { request_id, job_id, content: format!("Error reading log: {}", e).into_bytes() }).await;
                             }
                         }
                     } else {
                         warn!("Working directory for job {} not found.", job_id);
                         let _ = tx.send(Message::LogData { request_id, job_id, content: b"Job working directory not found.".to_vec() }).await;
                     }
                }
                Message::ListJobOutputFiles { request_id, job_id, working_directory: provided_wd, container_asset } => {
                    let wd = {
                        let wds = job_wds_spawn.lock().unwrap();
                        wds.get(&job_id).cloned().or(provided_wd)
                    };
                    let artifacts = if let Some(working_dir) = wd {
                        let asset = container_asset;
                        tokio::task::spawn_blocking(move || {
                            collect_declared_output_artifacts(Path::new(&working_dir), asset.as_ref())
                        })
                        .await
                        .unwrap_or_else(|e| Err(anyhow::anyhow!("Join error: {}", e)))
                        .unwrap_or_else(|e| {
                            error!("Failed to list declared outputs for job {}: {}", job_id, e);
                            Vec::new()
                        })
                    } else {
                        Vec::new()
                    };
                    let _ = tx.send(Message::JobOutputFiles { request_id, job_id, artifacts }).await;
                }
                Message::GetJobOutputFileChunk { request_id, job_id, path, offset, length, working_directory: provided_wd, container_asset } => {
                    let wd = {
                        let wds = job_wds_spawn.lock().unwrap();
                        wds.get(&job_id).cloned().or(provided_wd)
                    };
                    if let Some(working_dir) = wd {
                        let result = tokio::task::spawn_blocking(move || {
                            read_declared_output_chunk(
                                Path::new(&working_dir),
                                container_asset.as_ref(),
                                &path,
                                offset,
                                length,
                            )
                        })
                        .await
                        .unwrap_or_else(|e| Err(anyhow::anyhow!("Join error: {}", e)));

                        match result {
                            Ok((normalized_path, content, size)) => {
                                let _ = tx.send(Message::JobOutputFileChunk {
                                    request_id,
                                    job_id,
                                    path: normalized_path,
                                    content,
                                    size,
                                }).await;
                            }
                            Err(e) => {
                                error!("Failed to read declared output for job {}: {}", job_id, e);
                                let _ = tx.send(Message::Error(e.to_string())).await;
                            }
                        }
                    } else {
                        let _ = tx.send(Message::Error(format!("Working directory for job {} not found", job_id))).await;
                    }
                }
                Message::SetPulseInterval { seconds } => {
                    if seconds != current_pulse_interval {
                        debug!("Updating pulse interval to {}s (was {}s)", seconds, current_pulse_interval);
                        current_pulse_interval = seconds;
                    }
                    let _ = pulse_interval_tx.send(seconds);
                }
                Message::Restart { component, target_id: _, delay_ms, reason } => {
                    if component == "worker" {
                        info!("Worker restart requested. Reason: {}. Delay: {}ms", reason, delay_ms);
                        let worker_id_str = worker_id.to_string();
                        let tx_drain = tx.clone();
                        let running_jobs_restart = running_jobs.clone();

                        tokio::spawn(async move {
                            // Signal draining to controller so no new jobs are scheduled
                            let _ = tx_drain.send(Message::WorkerDraining { worker_id: worker_id_str }).await;

                            // Small grace period to allow draining signal to be processed
                            tokio::time::sleep(std::time::Duration::from_millis(500)).await;

                            // Option: We could terminate jobs here, but usually a "Remediate"
                            // might want to be graceful or hard. For now, let's just wait the requested delay.
                            tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;

                            // Force kill remaining jobs if any (safety)
                            let mut jobs = running_jobs_restart.lock().unwrap();
                            for (job_id, tx_kill) in jobs.drain() {
                                info!("Terminating job {} for restart", job_id);
                                let _ = tx_kill.send(KillSignal::Cancel);
                            }
                            drop(jobs);

                            veloce_common::utils::self_restart();
                        });
                    }
                }
                Message::StartTerminalSession { job_id, session_id } => {
                    let target_pid = {
                        let pids = running_pids_for_spawn.lock().unwrap();
                        pids.get(&job_id).map(|pid| pid.as_u32())
                    };
                    let working_directory = {
                        let wds = job_wds.lock().unwrap();
                        wds.get(&job_id).cloned().unwrap_or_else(|| "/tmp".to_string())
                    };

                    let tx_term = tx.clone();
                    let active_sessions = active_terminal_sessions_clone.clone();

                    tokio::task::spawn_blocking(move || {
                        info!("Spawning terminal for job {} (Session: {}, PID: {:?}, WD: {})", job_id, session_id, target_pid, working_directory);
                        match terminal::spawn_terminal(&working_directory, target_pid) {
                            Ok(session) => {
                                let mut reader = match session.pair.master.try_clone_reader() {
                                    Ok(r) => r,
                                    Err(e) => {
                                        error!("Failed to clone PTY reader: {}", e);
                                        let rt = tokio::runtime::Handle::current();
                                        let _ = rt.block_on(tx_term.send(Message::TerminalClosed { session_id, reason: format!("Failed to clone PTY reader: {}", e) }));
                                        return;
                                    }
                                };

                                let tx_read = tx_term.clone();
                                tokio::task::spawn_blocking(move || {
                                    let mut buf = [0u8; 1024];
                                    let rt = tokio::runtime::Handle::current();
                                    while let Ok(n) = reader.read(&mut buf) {
                                        if n == 0 { break; }
                                        let data = buf[..n].to_vec();
                                        let send_res = rt.block_on(tx_read.send(Message::TerminalOutput { session_id, data }));
                                        if send_res.is_err() {
                                            break;
                                        }
                                    }
                                    let _ = rt.block_on(tx_read.send(Message::TerminalClosed { session_id, reason: "EOF".into() }));
                                });

                                active_sessions.lock().unwrap().insert(session_id, session);
                                info!("Terminal session {} started for job {}", session_id, job_id);
                            }
                            Err(e) => {
                                error!("Failed to spawn terminal for job {}: {}", job_id, e);
                                let rt = tokio::runtime::Handle::current();
                                let _ = rt.block_on(tx_term.send(Message::TerminalClosed { session_id, reason: e.to_string() }));
                            }
                        }
                    });
                }
                Message::TerminalInput { session_id, data } => {
                    let mut sessions = active_terminal_sessions_clone.lock().unwrap();
                    if let Some(session) = sessions.get_mut(&session_id) {
                        if let Err(e) = session.writer.write_all(&data) {
                            error!("Failed to write to terminal session {}: {}", session_id, e);
                        } else {
                            let _ = session.writer.flush();
                        }
                    }
                }
                Message::TerminalResize { session_id, rows, cols } => {
                    let sessions = active_terminal_sessions_clone.lock().unwrap();
                    if let Some(session) = sessions.get(&session_id) {
                        let _ = session.pair.master.resize(PtySize {
                            rows,
                            cols,
                            pixel_width: 0,
                            pixel_height: 0,
                        });
                    }
                }
                Message::StopTerminalSession { session_id } => {
                    let mut sessions = active_terminal_sessions_clone.lock().unwrap();
                    if sessions.remove(&session_id).is_some() {
                        info!("Terminal session {} stopped", session_id);
                    }
                }
                Message::StartVncSession { job_id, session_id } => {
                    let vnc_port = {
                        let ports = vnc_ports_clone.lock().unwrap();
                        ports.get(&job_id).copied()
                    };

                    let tx_vnc = tx.clone();
                    let active_vnc = active_vnc_sessions_clone.clone();

                    if let Some(port) = vnc_port {
                        tokio::spawn(async move {
                            info!(
                                "Connecting VNC proxy for job {} to 127.0.0.1:{} (retrying if necessary)",
                                job_id, port
                            );

                            let mut stream = None;
                            for attempt in 1..=10 {
                                match tokio::net::TcpStream::connect(format!("127.0.0.1:{}", port))
                                    .await
                                {
                                    Ok(s) => {
                                        stream = Some(s);
                                        break;
                                    }
                                    Err(e) => {
                                        if attempt == 10 {
                                            error!(
                                                "Failed to connect to VNC server on port {} after 10 attempts: {}",
                                                port, e
                                            );
                                            let _ = tx_vnc
                                                .send(Message::VncClosed {
                                                    session_id,
                                                    reason: format!("TCP connect refused: {e}"),
                                                })
                                                .await;
                                            return;
                                        }
                                        debug!(
                                            "VNC connection attempt {} failed, retrying in 500ms...",
                                            attempt
                                        );
                                        tokio::time::sleep(std::time::Duration::from_millis(500))
                                            .await;
                                    }
                                }
                            }

                            if let Some(stream) = stream {
                                let (mut read_half, mut write_half) = tokio::io::split(stream);
                                // Bounded but large enough; backpressure via awaited send (never drop RFB bytes).
                                let (vnc_tx, mut vnc_rx) =
                                    tokio::sync::mpsc::channel::<Vec<u8>>(4096);

                                let tx_read = tx_vnc.clone();
                                let read_handle = tokio::spawn(async move {
                                    use tokio::io::AsyncReadExt;
                                    let mut buf = [0u8; 65536];
                                    let mut close_reason = "EOF".to_string();
                                    loop {
                                        match read_half.read(&mut buf).await {
                                            Ok(0) => break,
                                            Ok(n) => {
                                                let data = buf[..n].to_vec();
                                                if tx_read
                                                    .send(Message::VncOutput { session_id, data })
                                                    .await
                                                    .is_err()
                                                {
                                                    close_reason = "controller disconnected".into();
                                                    break;
                                                }
                                            }
                                            Err(e) => {
                                                close_reason = format!("TCP read error: {e}");
                                                break;
                                            }
                                        }
                                    }
                                    let _ = tx_read
                                        .send(Message::VncClosed {
                                            session_id,
                                            reason: close_reason,
                                        })
                                        .await;
                                });

                                let write_handle = tokio::spawn(async move {
                                    use tokio::io::AsyncWriteExt;
                                    while let Some(bytes) = vnc_rx.recv().await {
                                        if write_half.write_all(&bytes).await.is_err() {
                                            break;
                                        }
                                        let _ = write_half.flush().await;
                                    }
                                    // Dropping write_half closes the TCP write side.
                                });

                                active_vnc.lock().unwrap().insert(
                                    session_id,
                                    ActiveVncProxy {
                                        input_tx: vnc_tx,
                                        abort_read: read_handle.abort_handle(),
                                        abort_write: write_handle.abort_handle(),
                                    },
                                );

                                info!(
                                    "VNC proxy session {} started for job {}",
                                    session_id, job_id
                                );
                            }
                        });
                    } else {
                        error!("VNC port not found for job {}", job_id);
                        let _ = tx_vnc
                            .send(Message::VncClosed {
                                session_id,
                                reason: "VNC port not found".into(),
                            })
                            .await;
                    }
                }
                Message::VncInput { session_id, data } => {
                    let input_tx = {
                        let sessions = active_vnc_sessions_clone.lock().unwrap();
                        sessions.get(&session_id).map(|s| s.input_tx.clone())
                    };
                    if let Some(input_tx) = input_tx {
                        // Awaited send preserves RFB framing; never silently drop.
                        if input_tx.send(data).await.is_err() {
                            warn!(
                                "VNC input dropped: session {} already closed",
                                session_id
                            );
                        }
                    }
                }
                Message::StopVncSession { session_id } => {
                    let mut sessions = active_vnc_sessions_clone.lock().unwrap();
                    if let Some(proxy) = sessions.remove(&session_id) {
                        proxy.abort_read.abort();
                        proxy.abort_write.abort();
                        info!("VNC session {} stopped (TCP proxy aborted)", session_id);
                    }
                }
                _ => {}
            }
        }
                _ = drain_rx.changed() => {
                    if *drain_rx.borrow() && !sent_draining {
                        info!("Sending Draining message to controller...");
                        let _ = tx.send(Message::WorkerDraining { worker_id: worker_id.to_string() }).await;
                        sent_draining = true;
                    }
                }
                _ = sleep(Duration::from_secs(2)) => {
                    if *drain_rx.borrow() {
                        let jobs = running_jobs.lock().unwrap();
                        if jobs.is_empty() {
                             info!("Draining: No jobs running. Closing connection.");
                             return Ok(());
                        }
                    }
                }
            }
    }

    let _ = sink_handle.await;
    Ok(())
}
