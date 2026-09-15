#![allow(clippy::all)]
use crate::cgroups;
use crate::config::{filtered_host_env_for_launch, worker_env_policy};
use crate::inputs::process_inputs;
use crate::log_manager::GLOBAL_LOG_TX;
use crate::outputs::{collect_declared_output_artifacts, compress_working_directory};
use crate::pmi;
#[cfg(feature = "pmix")]
use crate::pmix;
use crate::recovery::{persist_job_state, remove_persisted_job_state, PERSISTENCE_FILE};
use crate::resources::find_free_port;
use anyhow::{Context, Result};
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use sysinfo::{Pid, PidExt};
use tokio::process::Command;
use tokio::sync::{mpsc, oneshot};
use tokio::time::sleep;
use tracing::{debug, error, info, warn};
use veloce_common::{JobStatus, JobUsage, Message, PersistedJob, QosLevel};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum KillSignal {
    Cancel,
    Preempt,
}

pub(crate) struct JobStatsEntry {
    pub(crate) start_time: u64,
    pub(crate) submission_time: u64,
    pub(crate) max_memory_bytes: u64,
    pub(crate) cpu_time_ms: u64,
    pub(crate) command_line: String,
    pub(crate) user_id: String,
    pub(crate) req_nodes: usize,
    pub(crate) req_cores: u32,
    pub(crate) req_memory: u64,
    pub(crate) array_id: Option<u64>,
    pub(crate) array_task_id: Option<u32>,
    pub(crate) assigned_workers: Vec<String>,
    pub(crate) last_active_at: u64,
    pub(crate) is_idle: bool,
    pub(crate) idle_duration: u64,
    pub(crate) gres_req: BTreeMap<String, u64>,
    pub(crate) secret: String,
    pub(crate) wait_for_licenses: bool,
    pub(crate) estimated_walltime: Option<u64>,
    pub(crate) priority_offset: Option<i32>,
    pub(crate) dependencies: Option<Vec<u64>>,
    pub(crate) dependency_specs: Option<Vec<String>>,
    pub(crate) qos: QosLevel,
    pub(crate) cgroup_last_cpu_usec: Option<u64>,
    pub(crate) cgroup_last_measured_at: Option<std::time::Instant>,
    pub(crate) namespace_last_cpu_ticks: Option<u64>,
    pub(crate) namespace_last_measured_at: Option<std::time::Instant>,
}

fn prepare_job_env(
    job_id: u64,
    node_list: &[String],
    working_directory: &str,
    hostfile_path: &str,
    array_id: Option<u64>,
    array_task_id: Option<u32>,
    user_id: &str,
    allocated_gres: &HashMap<String, Vec<u32>>,
) -> HashMap<String, String> {
    let mut env_vars = HashMap::new();
    let node_str = node_list.join(",");
    let node_count = node_list.len().to_string();
    let master_addr = node_list.first().cloned().unwrap_or_default();

    env_vars.insert("VELOCE_NODES".to_string(), node_str);
    env_vars.insert("VELOCE_JOB_ID".to_string(), job_id.to_string());
    env_vars.insert("VELOCE_NODE_COUNT".to_string(), node_count);
    env_vars.insert("VELOCE_MASTER_ADDR".to_string(), master_addr);
    env_vars.insert(
        "VELOCE_WORKING_DIR".to_string(),
        working_directory.to_string(),
    );
    env_vars.insert("VELOCE_HOSTFILE".to_string(), hostfile_path.to_string());
    env_vars.insert(
        "OMPI_MCA_orte_default_hostfile".to_string(),
        hostfile_path.to_string(),
    );
    env_vars.insert("HYDRA_HOST_FILE".to_string(), hostfile_path.to_string());
    env_vars.insert("VELOCE_SUBMITTING_USER".to_string(), user_id.to_string());

    if !allocated_gres.is_empty() {
        if let Ok(json) = serde_json::to_string(allocated_gres) {
            env_vars.insert("VELOCE_ALLOCATED_GRES".to_string(), json);
        }
    }

    if let Some(id) = array_id {
        env_vars.insert("VELOCE_ARRAY_JOB_ID".to_string(), id.to_string());
    }
    if let Some(task_id) = array_task_id {
        env_vars.insert("VELOCE_ARRAY_TASK_ID".to_string(), task_id.to_string());
    }

    env_vars
}

pub fn canonicalize_command(binary: String, args: Vec<String>) -> (String, Vec<String>) {
    // 1. Split binary by whitespace/shell-words to get the executable and any extra arguments.
    let bin_parts = shell_words::split(&binary)
        .unwrap_or_else(|_| binary.split_whitespace().map(String::from).collect());

    if bin_parts.is_empty() {
        return (binary, args);
    }

    let bin_path = bin_parts[0].clone();
    let bin_extra_args = bin_parts[1..].to_vec();

    // Get the base name (filename) of the binary path, e.g. "/bin/sleep" -> "sleep"
    let bin_filename = std::path::Path::new(&bin_path)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or(&bin_path)
        .to_string();

    // 2. Now let's process `args` and look for duplication.
    let mut new_args = Vec::new();

    // We iterate through args and check if they duplicate the binary path or binary filename.
    for arg in args {
        // Parse this argument into words to see if it starts with the command.
        let arg_parts = shell_words::split(&arg)
            .unwrap_or_else(|_| arg.split_whitespace().map(String::from).collect());

        if arg_parts.is_empty() {
            continue;
        }

        let first_word = &arg_parts[0];
        let first_word_filename = std::path::Path::new(first_word)
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or(first_word)
            .to_string();

        // Check if the first word of this argument is a duplicate of the binary path or filename.
        if first_word == &bin_path
            || first_word == &bin_filename
            || first_word_filename == bin_filename
        {
            // Yes, it duplicates the command!
            // We strip the command token and add the rest of the tokens from this argument to new_args.
            new_args.extend(arg_parts[1..].to_vec());
        } else {
            // Not a duplicate, keep the argument as is.
            new_args.push(arg);
        }
    }

    // 3. Combine bin_extra_args and new_args.
    // Check if new_args starts with the exact same elements as bin_extra_args.
    let mut final_args = bin_extra_args.clone();

    let mut matches_start = true;
    for (i, extra_arg) in bin_extra_args.iter().enumerate() {
        if i >= new_args.len() || &new_args[i] != extra_arg {
            matches_start = false;
            break;
        }
    }

    if matches_start {
        // If they match, we only keep the extra elements from new_args to avoid duplicating args!
        final_args.extend(new_args[bin_extra_args.len()..].to_vec());
    } else {
        // If they don't match, we append new_args to final_args.
        final_args.extend(new_args);
    }

    (bin_path, final_args)
}

#[tracing::instrument(
    skip(
        tx,
        rx_kill,
        running_pids,
        job_stats_history,
        file_client,
        cgroup_manager,
        vnc_ports
    ),
    fields(job_id)
)]
pub(crate) async fn run_job(
    job_id: u64,
    binary: String,
    args: Vec<String>,
    node_list: Vec<String>,
    assigned_cores: Option<Vec<usize>>,
    req_memory: u64, // in MB
    working_directory: String,
    user_id: String,
    walltime: u64,
    submission_time: u64,
    array_id: Option<u64>,
    array_task_id: Option<u32>,
    prolog: Option<String>,
    epilog: Option<String>,
    tx: mpsc::Sender<Message>,
    rx_kill: oneshot::Receiver<KillSignal>,
    running_pids: Arc<Mutex<HashMap<u64, Pid>>>,
    job_stats_history: Arc<Mutex<HashMap<u64, JobStatsEntry>>>,
    run_as_worker_user: bool,
    file_client: Arc<veloce_common::file_client::FileClient>,
    inputs: Vec<veloce_common::FileHandle>,
    cgroup_manager: Arc<cgroups::CgroupManager>,
    gres_req: BTreeMap<String, u64>,
    allocated_gres: HashMap<String, Vec<u32>>,
    worker_id: String,
    secret: String,
    wait_for_licenses: bool,
    controller_url: String,
    env_vars: Vec<(String, String)>,
    estimated_walltime: Option<u64>,
    priority_offset: Option<i32>,
    dependencies: Option<Vec<u64>>,
    dependency_specs: Option<Vec<String>>,
    qos: QosLevel,
    container_asset: Option<veloce_common::apptainer::ContainerAsset>,
    vnc_enabled: bool,
    inherit_host_env: bool,
    env_allowlist: Option<Vec<String>>,
    job_profile: Option<String>,
    vnc_ports: Arc<Mutex<HashMap<u64, u16>>>,
) -> Result<()> {
    let (binary, args) = canonicalize_command(binary, args);
    if wait_for_licenses {
        warn!(
            "Job {} requested wait_for_licenses; Community Edition does not check FlexLM/LM-X",
            job_id
        );
    }

    let working_directory_path = Path::new(&working_directory);
    if !working_directory_path.exists() {
        return Err(anyhow::anyhow!(
            "Working directory {} does not exist",
            working_directory
        ));
    }

    // 0. Prepare Cgroup if enabled
    if cgroup_manager.is_enabled() {
        if let Err(e) = cgroup_manager.create_job_cgroup(job_id) {
            warn!(
                "Failed to create cgroup for job {}: {}. Continuing without cgroup isolation.",
                job_id, e
            );
        } else {
            let cores_count = assigned_cores.as_ref().map_or(0, |c| c.len() as u32);
            if let Err(e) =
                cgroup_manager.set_limits(job_id, req_memory, cores_count, &allocated_gres)
            {
                warn!("Failed to set cgroup limits for job {}: {}.", job_id, e);
            }
        }
    }

    // Process inputs
    if !inputs.is_empty() {
        process_inputs(&file_client, &inputs, working_directory_path).await?;
    }

    if let Some((_, port_str)) = env_vars
        .iter()
        .find(|(key, _)| key == "VELOCE_INTERACTIVE_PORT")
    {
        if let Ok(port) = port_str.parse::<u16>() {
            let _ = tx.send(Message::JobInteractivePort { job_id, port }).await;
        }
    }

    let working_directory = if run_as_worker_user {
        std::env::temp_dir().to_string_lossy().to_string()
    } else {
        working_directory
    };

    {
        let mut stats = job_stats_history.lock().unwrap();
        stats.insert(
            job_id,
            JobStatsEntry {
                start_time: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
                submission_time,
                max_memory_bytes: 0,
                cpu_time_ms: 0,
                command_line: format!("{} {}", binary, args.join(" ")),
                user_id: user_id.clone(),
                req_nodes: node_list.len(),
                req_cores: assigned_cores.as_ref().map(|c| c.len() as u32).unwrap_or(0),
                req_memory,
                array_id,
                array_task_id,
                assigned_workers: node_list.clone(),
                last_active_at: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
                is_idle: false,
                idle_duration: 0,
                gres_req: gres_req.clone(),
                secret: secret.clone(),
                wait_for_licenses,
                estimated_walltime,
                priority_offset,
                dependencies: dependencies.clone(),
                dependency_specs: dependency_specs.clone(),
                qos: qos.clone(),
                cgroup_last_cpu_usec: None,
                cgroup_last_measured_at: None,
                namespace_last_cpu_ticks: None,
                namespace_last_measured_at: None,
            },
        );
    }

    // Generate Hostfile
    let mut hostfile_path = std::env::temp_dir();
    hostfile_path.push(format!("veloce_hostfile_{}.txt", job_id));
    let hostfile_path_str = hostfile_path.to_string_lossy().to_string();

    {
        let mut file = File::create(&hostfile_path).context("Failed to create hostfile")?;
        let slots = assigned_cores.as_ref().map(|c| c.len()).unwrap_or(1);
        for node in &node_list {
            // Strip port if present to get just the IP/hostname
            let host = if let Some(idx) = node.rfind(':') {
                &node[..idx]
            } else {
                node
            };
            writeln!(file, "{} slots={}", host, slots).context("Failed to write to hostfile")?;
        }
    }

    // Output Redirection
    let work_dir = Path::new(&working_directory);
    let stdout_path = work_dir.join(format!("{}-{}-stdout.log", job_id, worker_id));
    let stderr_path = work_dir.join(format!("{}-{}-stderr.log", job_id, worker_id));

    // Prepare Environment Variables
    let mut job_env = prepare_job_env(
        job_id,
        &node_list,
        &working_directory,
        &hostfile_path_str,
        array_id,
        array_task_id,
        &user_id,
        &allocated_gres,
    );
    // Merge provided env_vars
    for (k, v) in env_vars {
        job_env.insert(k, v);
    }
    let env_vars = job_env;

    // Prolog
    if let Some(path) = &prolog {
        if let Err(e) = run_script(path, job_id, &env_vars).await {
            tx.send(Message::JobError {
                job_id,
                error: format!("Prolog failed: {}", e),
            })
            .await?;
            // Cleanup hostfile
            let _ = std::fs::remove_file(&hostfile_path);
            return Ok(());
        }
    }

    // Prepare User Impersonation Data (Resolving UID/GID)
    // We do this BEFORE spawning to fail early if user doesn't exist.
    #[cfg(target_family = "unix")]
    let user_impersonation_info = if !run_as_worker_user {
        use std::ffi::CString;
        let user_c =
            CString::new(user_id.clone()).map_err(|_| anyhow::anyhow!("Invalid username"))?;
        let passwd_ptr: *mut libc::passwd;

        unsafe {
            passwd_ptr = libc::getpwnam(user_c.as_ptr());
        }

        if passwd_ptr.is_null() {
            let (_, _, require_impersonation) = worker_env_policy();
            if veloce_common::job_policy::must_fail_missing_user(
                require_impersonation,
                job_profile.as_deref(),
                false,
            ) {
                return Err(anyhow::anyhow!(
                    "job.launch.user_not_found: User '{}' not found on worker node",
                    user_id
                ));
            }
            warn!(
                "User '{}' not found on worker node. Falling back to worker user (root).",
                user_id
            );
            None
        } else {
            unsafe { Some(((*passwd_ptr).pw_uid, (*passwd_ptr).pw_gid, user_c)) }
        }
    } else {
        None
    };

    let exec_opts = veloce_common::job_policy::JobExecutionOptions::from_fields(
        inherit_host_env,
        &env_allowlist,
        &job_profile,
    );
    let host_env = filtered_host_env_for_launch(&exec_opts, !run_as_worker_user);

    #[allow(unused_mut)]
    let mut env_vars = env_vars;
    // Replace localhost in controller_url with the actual controller IP if necessary
    let controller_url = if controller_url.contains("localhost") {
        // Fallback to controller_url as is if we can't determine IP,
        // but we should have passed it.
        controller_url
    } else {
        controller_url
    };

    let rank = env_vars
        .iter()
        .find(|(k, _)| k.as_str() == "VELOCE_RANK")
        .and_then(|(_, v)| v.parse::<usize>().ok())
        .unwrap_or_else(|| {
            node_list
                .iter()
                .position(|id| id == &worker_id)
                .unwrap_or(0)
        });
    let size = env_vars
        .iter()
        .find(|(k, _)| k.as_str() == "VELOCE_SIZE")
        .and_then(|(_, v)| v.parse::<usize>().ok())
        .unwrap_or(node_list.len());

    #[allow(unused_mut)]
    let mut pmix_socket_dir: Option<String> = None;
    #[allow(unused_mut)]
    let mut pmix_uri: Option<String> = None;
    #[allow(unused_mut, unused_variables)]
    let mut pmix_started = false;

    #[cfg(feature = "pmix")]
    {
        let is_pmix_requested = env_vars
            .iter()
            .any(|(k, v)| k == "VELOCE_USE_PMIX" && v == "1");

        if is_pmix_requested {
            if node_list.len() <= 1 {
                pmix_socket_dir = Some("/tmp/pmix.veloce".to_string());
                if let Some(uri) = pmix::pmix_server::get_server_uri(job_id) {
                    pmix_uri = Some(uri);
                    pmix_started = true;
                    if let Err(e) = pmix::pmix_server::register_nspace(job_id) {
                        error!("Failed to register PMIx namespace: {}", e);
                    }
                } else {
                    error!("Failed to get global PMIx server URI");
                }
            } else {
                warn!("PMIx requested (VELOCE_USE_PMIX=1) but job has multiple nodes. Falling back to PMI-2 for inter-node communication.");
            }
        }
    }

    let (mut command, execution_env_vars) = if let Some(asset) = &container_asset {
        let cache_dir = Path::new("/scratch/veloce_images");
        std::fs::create_dir_all(cache_dir).unwrap_or_default();
        let safe_name = asset.image_uri.replace("/", "_").replace(":", "_");
        let image_path = cache_dir.join(safe_name);

        // Disabling SIF image caching for now: always download the latest image
        info!(
            "Downloading Apptainer image from {} (caching disabled)",
            asset.image_uri
        );
        if let Err(e) = file_client
            .download_s3_file(&asset.image_uri, &image_path)
            .await
        {
            return Err(anyhow::anyhow!(
                "Failed to download image {}: {}",
                asset.image_uri,
                e
            ));
        }

        let runner = crate::apptainer::ApptainerRunner::new(
            asset,
            &working_directory,
            &image_path.to_string_lossy(),
        );

        let custom_args = shell_words::join(args.clone());
        let (bin, apptainer_args) =
            runner.build_command(&custom_args, &binary, job_id, pmix_socket_dir.as_deref())?;

        let mut cmd = Command::new(bin);
        cmd.args(apptainer_args);

        (cmd, runner.build_environment_vars())
    } else {
        let mut cmd = Command::new(&binary);
        cmd.args(&args);
        (cmd, vec![])
    };

    if vnc_enabled {
        let vnc_port = find_free_port().unwrap_or(5901);
        info!("Allocated dynamic VNC port {} for Job {}", vnc_port, job_id);
        vnc_ports.lock().unwrap().insert(job_id, vnc_port);
        command.env("APPTAINERENV_VELOCE_VNC_PORT", vnc_port.to_string());
    }

    if let Some(uri) = pmix_uri {
        if container_asset.is_some() {
            command.env("APPTAINERENV_PMIX_SERVER_URI", &uri);
            command.env(
                "APPTAINERENV_PMIX_NAMESPACE",
                format!("pmix-job-{}", job_id),
            );
            command.env("APPTAINERENV_PMIX_RANK", rank.to_string());
        } else {
            command.env("PMIX_SERVER_URI", &uri);
            command.env("PMIX_NAMESPACE", format!("pmix-job-{}", job_id));
            command.env("PMIX_RANK", rank.to_string());
        }
    }

    command
        .envs(host_env)
        .envs(env_vars.clone())
        .envs(execution_env_vars)
        .env("VELOCE_JOB_ID", job_id.to_string())
        .env("VELOCE_RANK", rank.to_string())
        .env("VELOCE_SIZE", size.to_string())
        .env("VELOCE_SECRET", secret.clone())
        .env("VELOCE_CONTROLLER_URL", controller_url)
        .env("VELOCE_CA_CERT", "/app/certs/ca.pem") // Standard path in Docker
        .env("VELOCE_FILESERVER_URL", file_client.base_url())
        .env("VELOCE_FILESERVER_KEY", file_client.api_key())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);

    command.current_dir(&working_directory);

    #[cfg(target_family = "unix")]
    {
        command.process_group(0); // Create a new process group for the job

        let assigned_cores_clone = assigned_cores.clone(); // Clone for pre_exec
        let user_id_clone_for_pre_exec_msg = user_id.clone(); // Clone for pre_exec error message
        let user_info_clone = user_impersonation_info.clone();
        let use_rlimit = !cgroup_manager.is_enabled();

        // Apply CPU affinity, memory limits, and User Impersonation using pre_exec
        unsafe {
            command.pre_exec(move || {
                // 1. User Impersonation
                // Only attempt if we are running as root (uid 0), otherwise these calls will likely fail (or are unnecessary).
                if let Some((uid, gid, user_cstr_for_pre_exec)) = &user_info_clone {
                    if libc::getuid() == 0 {
                        // Set Group ID
                        if libc::setgid(*gid) != 0 {
                            let errno = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
                            warn!("Failed to setgid to {}: {}", gid, errno);
                            // Don't panic/fail hard yet, allow soft fail? No, security implies we should fail.
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::PermissionDenied,
                                "setgid failed",
                            ));
                        }

                        // Initialize Supplementary Groups
                        // initgroups requires the username string
                        //                    if libc::initgroups(user_cstr_for_pre_exec.as_ptr(), gid as libc::c_int) != 0 {
                        if libc::initgroups(user_cstr_for_pre_exec.as_ptr(), *gid as _) != 0 {
                            let errno = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
                            warn!(
                                "Failed to initgroups for {}: {}",
                                user_id_clone_for_pre_exec_msg, errno
                            );
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::PermissionDenied,
                                "initgroups failed",
                            ));
                        }

                        // Set User ID
                        if libc::setuid(*uid) != 0 {
                            let errno = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
                            warn!("Failed to setuid to {}: {}", uid, errno);
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::PermissionDenied,
                                "setuid failed",
                            ));
                        }
                        // info!("Dropped privileges to user {} ({}:{})", user_id, uid, gid); // Can't log easily in pre_exec context safely?
                    }
                }

                if let Some(cores) = &assigned_cores_clone {
                    // Use cloned variable
                    if !cores.is_empty() {
                        // info!("Job {} pinning to cores: {:?}", job_id, cores); // unsafe to alloc in pre_exec
                        for &core_idx in cores.iter() {
                            core_affinity::set_for_current(core_affinity::CoreId { id: core_idx });
                        }
                    }
                }

                if use_rlimit && req_memory > 0 {
                    // Convert MB to bytes, check for overflow
                    let mem_bytes: u64 = req_memory.saturating_mul(1024 * 1024);
                    // NOTE: RLIMIT_AS (Address Space) is a coarse memory limit.
                    // For robust resource isolation on Linux, Cgroups are highly recommended.
                    let _ = custom_setrlimit(libc::RLIMIT_AS as _, mem_bytes, mem_bytes);
                }
                Ok(())
            });
        }
    }

    let mut child = command.spawn().context("Failed to spawn process")?;

    let pid = child
        .id()
        .map(|id| Pid::from(id as usize))
        .context("Failed to get child PID")?;

    #[cfg(feature = "pmix")]
    if pmix_started {
        #[cfg(target_family = "unix")]
        {
            let (uid, gid) = if let Some((u, g, _)) = &user_impersonation_info {
                (*u, *g)
            } else {
                unsafe { (libc::getuid(), libc::getgid()) }
            };
            if let Err(e) = pmix::pmix_server::register_client(job_id, rank as u32, uid, gid) {
                error!("Failed to register PMIx client: {}", e);
            }
        }
    }

    // 0.1 Move to cgroup
    if cgroup_manager.is_enabled() {
        if let Err(e) = cgroup_manager.add_process(job_id, pid.as_u32()) {
            error!("CRITICAL: Failed to move job {} process to cgroup: {}. Job may exceed resource limits.", job_id, e);
        } else {
            info!(
                "Successfully attached job {} (PID {}) to cgroup",
                job_id, pid
            );
        }
    }

    let pgid_option = unsafe { libc::getpgid(pid.as_u32() as libc::pid_t) };
    let pgid = if pgid_option > 0 {
        pgid_option as u32
    } else {
        0
    };

    {
        let mut pids_lock = running_pids.lock().unwrap();
        pids_lock.insert(job_id, pid);
    }

    // Capture Output and Stream
    let stdout = child.stdout.take().unwrap();
    let stderr = child.stderr.take().unwrap();

    let stdout_file = File::create(&stdout_path).context("Failed to create stdout log file")?;
    let stderr_file = File::create(&stderr_path).context("Failed to create stderr log file")?;

    let worker_id_label = worker_id.to_string();

    let handle1 = tokio::spawn(async move {
        stream_job_output(job_id, stdout, stdout_file, "stdout", worker_id_label).await;
    });

    let worker_id_label = worker_id.to_string();
    let handle2 = tokio::spawn(async move {
        stream_job_output(job_id, stderr, stderr_file, "stderr", worker_id_label).await;
    });

    // Prepare env_vars for PersistedJob
    let persisted_env_vars: Vec<(String, String)> = env_vars
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();

    // Persist job state
    let persisted_job = PersistedJob {
        container_asset: container_asset.clone(),
        job_id,
        pid: pid.as_u32(),
        pgid,
        binary: binary.clone(),
        args: args.clone(),
        env_vars: persisted_env_vars,
        working_directory: working_directory.clone(),
        req_cores: assigned_cores.as_ref().map_or(0, |c| c.len() as u32),
        req_memory,
        user_id: user_id.clone(),
        walltime,
        submission_time,
        array_id,
        array_task_id,
        secret: secret.clone(),
        gres_req: gres_req.clone(),
        wait_for_licenses,
        estimated_walltime,
        priority_offset,
        dependencies: dependencies.clone(),
        dependency_specs: dependency_specs.clone(),
        qos: qos.clone(),
        inherit_host_env,
        env_allowlist,
        job_profile,
        output_artifacts: Vec::new(),
    };

    if let Err(e) = persist_job_state(Path::new(PERSISTENCE_FILE), &persisted_job) {
        error!("Failed to persist job {} state: {}", job_id, e);
        // Depending on policy, might want to fail the job here.
        // For now, we'll let it continue but log the error.
    }

    // No need to spawn threads for stdout/stderr reading as they are redirected to files.

    let walltime_duration = if walltime > 0 {
        Duration::from_secs(walltime)
    } else {
        Duration::from_secs(365 * 24 * 3600 * 100) // Effectively infinite
    };

    let completion_msg;

    let (exit_code, status_enum) = tokio::select! {
        res = child.wait() => {
            match res {
                Ok(status) => {
                    let code = status.code().unwrap_or(-1);
                    if let Err(e) = remove_persisted_job_state(Path::new(PERSISTENCE_FILE), job_id) {
                        error!("Failed to remove persisted job {} on JobDone: {}", job_id, e);
                    }
                    completion_msg = Some(Message::JobDone { job_id, exit_code: code });
                    (Some(code), JobStatus::Completed(code))
                }
                Err(e) => {
                    let error_msg = e.to_string();
                    if let Err(err) = remove_persisted_job_state(Path::new(PERSISTENCE_FILE), job_id) {
                        error!("Failed to remove persisted job {} on JobError: {}", job_id, err);
                    }
                    completion_msg = Some(Message::JobError { job_id, error: error_msg.clone() });
                    (None, JobStatus::Failed(error_msg))
                }
            }
        }
        res = rx_kill => {
            let signal = res.unwrap_or(KillSignal::Cancel);
            match signal {
                KillSignal::Cancel => {
                    info!("Killing job {}", job_id);
                    #[cfg(target_family = "unix")]
                    {
                         unsafe {
                            let pgid = pid.as_u32() as i32;
                            libc::kill(-pgid, libc::SIGKILL);
                         }
                    }
                    let _ = child.start_kill();
                    let _ = child.wait().await;
                }
                KillSignal::Preempt => {
                    info!("Preempting job {}", job_id);
                    #[cfg(target_family = "unix")]
                    {
                         unsafe {
                            let pgid = pid.as_u32() as i32;
                            libc::kill(-pgid, libc::SIGTERM);
                         }
                    }
                    tokio::select! {
                        _ = child.wait() => {
                            info!("Preempted job {} exited gracefully", job_id);
                        }
                        _ = tokio::time::sleep(tokio::time::Duration::from_secs(5)) => {
                            info!("Preempted job {} did not exit gracefully, killing", job_id);
                            #[cfg(target_family = "unix")]
                            {
                                 unsafe {
                                    let pgid = pid.as_u32() as i32;
                                    libc::kill(-pgid, libc::SIGKILL);
                                 }
                            }
                            let _ = child.start_kill();
                            let _ = child.wait().await;
                        }
                    }
                }
            }
            if let Err(e) = remove_persisted_job_state(Path::new(PERSISTENCE_FILE), job_id) {
                error!("Failed to remove persisted job {} on rx_kill: {}", job_id, e);
            }
            completion_msg = Some(Message::JobKilled { job_id });

            // 0.2 Cleanup cgroup
            if cgroup_manager.is_enabled() {
                let _ = cgroup_manager.cleanup_job_cgroup(job_id);
            }

            (Some(-9), JobStatus::Killed)
        }
        _ = sleep(walltime_duration) => {
             info!("Job {} exceeded walltime ({}s). Killing...", job_id, walltime);
            // Kill the entire process group
            #[cfg(target_family = "unix")]
            {
                 unsafe {
                    let pgid = pid.as_u32() as i32;
                    // Send SIGKILL (9) to the process group (-pgid)
                    libc::kill(-pgid, libc::SIGKILL);
                 }
            }

            // Fallback/Ensure leader is dead
            let _ = child.start_kill();
            let _ = child.wait().await;
            if let Err(e) = remove_persisted_job_state(Path::new(PERSISTENCE_FILE), job_id) {
                error!("Failed to remove persisted job {} on walltime exceeded: {}", job_id, e);
            }
            completion_msg = Some(Message::JobError { job_id, error: "Walltime exceeded".to_string() });
            (None, JobStatus::Failed("Walltime exceeded".to_string()))
        }
    };

    let _ = tokio::join!(handle1, handle2);

    // 0.3 Final Cgroup Cleanup (Catch-all)
    if cgroup_manager.is_enabled() {
        let _ = cgroup_manager.cleanup_job_cgroup(job_id);
    }

    // Epilog
    if let Some(path) = &epilog {
        if let Err(e) = run_script(path, job_id, &env_vars).await {
            error!("Epilog failed for job {}: {}", job_id, e);
        }
    }

    let mut stdout_file_id = None;
    let mut stderr_file_id = None;
    let mut workdir_file_id = None;

    if stdout_path.exists() {
        match file_client.upload_file(&stdout_path, false, false).await {
            Ok(handle) => {
                info!(
                    "Successfully uploaded stdout for job {}: file_id={}",
                    job_id, handle.file_id
                );
                stdout_file_id = Some(handle.file_id);
            }
            Err(e) => error!("Failed to upload stdout for job {}: {}", job_id, e),
        }
    } else {
        warn!(
            "Stdout path does not exist for job {}: {:?}",
            job_id, stdout_path
        );
    }

    if stderr_path.exists() {
        match file_client.upload_file(&stderr_path, false, false).await {
            Ok(handle) => {
                info!(
                    "Successfully uploaded stderr for job {}: file_id={}",
                    job_id, handle.file_id
                );
                stderr_file_id = Some(handle.file_id);
            }
            Err(e) => error!("Failed to upload stderr for job {}: {}", job_id, e),
        }
    } else {
        warn!(
            "Stderr path does not exist for job {}: {:?}",
            job_id, stderr_path
        );
    }

    let wd_trimmed = working_directory.trim_end_matches('/');
    if wd_trimmed != "/tmp" && !wd_trimmed.is_empty() && working_directory != "/" {
        let archive_path =
            std::env::temp_dir().join(format!("veloce_job_{}_workdir.tar.gz", job_id));
        if let Ok(()) = compress_working_directory(Path::new(&working_directory), &archive_path) {
            match file_client.upload_file(&archive_path, false, true).await {
                Ok(handle) => {
                    info!(
                        "Successfully uploaded working dir for job {}: file_id={}",
                        job_id, handle.file_id
                    );
                    workdir_file_id = Some(handle.file_id);
                }
                Err(e) => error!("Failed to upload working dir for job {}: {}", job_id, e),
            }
            let _ = std::fs::remove_file(&archive_path);
        }
    }

    let mut output_artifacts = match collect_declared_output_artifacts(
        Path::new(&working_directory),
        container_asset.as_ref(),
    ) {
        Ok(artifacts) => artifacts,
        Err(e) => {
            error!(
                "Failed to collect declared output artifacts for job {}: {}",
                job_id, e
            );
            Vec::new()
        }
    };
    for artifact in output_artifacts.iter_mut() {
        let artifact_path = Path::new(&working_directory).join(&artifact.path);
        match file_client.upload_file(&artifact_path, false, false).await {
            Ok(handle) => {
                info!(
                    "Successfully uploaded declared output for job {}: path={}, file_id={}",
                    job_id, artifact.path, handle.file_id
                );
                artifact.file_id = Some(handle.file_id);
            }
            Err(e) => error!(
                "Failed to upload declared output for job {} ({}): {}",
                job_id, artifact.path, e
            ),
        }
    }

    // Report Job Usage
    let usage_report = {
        let mut stats = job_stats_history.lock().unwrap();
        if let Some(entry) = stats.remove(&job_id) {
            let end_time = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();

            Some(JobUsage {
                container_asset: container_asset.clone(),
                job_id,
                job_name: None,
                job_comment: None,
                command_line: entry.command_line,
                user_id: entry.user_id,
                submission_time: entry.submission_time,
                start_time: Some(entry.start_time),
                end_time: Some(end_time),
                exit_code: exit_code,
                status: status_enum,
                cpu_time_ms: entry.cpu_time_ms,
                max_memory_bytes: entry.max_memory_bytes,
                req_nodes: entry.req_nodes,
                req_cores: entry.req_cores,
                req_memory: entry.req_memory,
                array_id: entry.array_id,
                array_task_id: entry.array_task_id,
                assigned_workers: entry.assigned_workers,
                gres_req: entry.gres_req,
                cgroup_active: cgroup_manager.is_enabled(),
                secret: entry.secret,
                stdout_file_id,
                stderr_file_id,
                workdir_file_id,
                output_artifacts,
                wait_for_licenses: entry.wait_for_licenses,
                estimated_walltime: entry.estimated_walltime,
                priority_offset: entry.priority_offset,
                dependencies: entry.dependencies.clone(),
                dependency_specs: entry.dependency_specs.clone(),
                qos: entry.qos.clone(),
            })
        } else {
            None
        }
    };

    if let Some(report) = usage_report {
        info!(
            "Reporting usage for job {}: CPU {}ms, Mem {}MB",
            job_id,
            report.cpu_time_ms,
            report.max_memory_bytes / 1024 / 1024
        );
        let _ = tx.send(Message::ReportJobUsage(report)).await;
    }

    if let Some(msg) = completion_msg {
        let _ = tx.send(msg).await;
    }

    // Cleanup hostfile
    if let Err(e) = std::fs::remove_file(&hostfile_path) {
        warn!("Failed to remove hostfile: {}", e);
    }

    // Cleanup VNC port allocation if any
    vnc_ports.lock().unwrap().remove(&job_id);

    // Cleanup PMIx namespace/client if enabled (no-op for global server, let it keep running)

    Ok(())
}

async fn run_script(
    script_path: &str,
    _job_id: u64,
    env_vars: &HashMap<String, String>,
) -> Result<()> {
    info!("Running script: {}", script_path);
    let status = Command::new(script_path)
        .envs(env_vars)
        .status()
        .await
        .context(format!("Failed to execute script {}", script_path))?;

    if !status.success() {
        anyhow::bail!(
            "Script {} failed with exit code {:?}",
            script_path,
            status.code()
        );
    }
    Ok(())
}

fn parse_shell_prefix_command(
    binary: &str,
    args: &[String],
) -> (
    String,
    Vec<String>,
    std::collections::HashMap<String, String>,
) {
    let mut real_binary = binary.to_string();
    let mut real_args = args.to_vec();
    let mut extra_env = std::collections::HashMap::new();

    if binary.contains(';') {
        let parts: Vec<&str> = binary.split(';').map(|s| s.trim()).collect();
        for part in parts {
            if part.is_empty() {
                continue;
            }
            if part.starts_with("export ") {
                continue;
            } else if let Some(eq_idx) = part.find('=') {
                let key = part[..eq_idx].trim().to_string();
                let mut val = part[eq_idx + 1..].trim().to_string();

                if key == "PATH" {
                    let sys_path = std::env::var("PATH").unwrap_or_default();
                    val = val
                        .replace("$PATH", &sys_path)
                        .replace("${PATH:-}", &sys_path);
                } else if key == "LD_LIBRARY_PATH" {
                    let sys_ld = std::env::var("LD_LIBRARY_PATH").unwrap_or_default();
                    val = val
                        .replace("$LD_LIBRARY_PATH", &sys_ld)
                        .replace("${LD_LIBRARY_PATH:-}", &sys_ld);
                } else if key == "DYLD_LIBRARY_PATH" {
                    let sys_dyld = std::env::var("DYLD_LIBRARY_PATH").unwrap_or_default();
                    val = val
                        .replace("$DYLD_LIBRARY_PATH", &sys_dyld)
                        .replace("${DYLD_LIBRARY_PATH:-}", &sys_dyld);
                }
                extra_env.insert(key, val);
            } else {
                let cmd_parts: Vec<String> =
                    part.split_whitespace().map(|s| s.to_string()).collect();
                if !cmd_parts.is_empty() {
                    real_binary = cmd_parts[0].clone();
                    let mut new_args = cmd_parts[1..].to_vec();
                    new_args.extend(real_args);
                    real_args = new_args;
                }
            }
        }
    }

    (real_binary, real_args, extra_env)
}

pub(crate) async fn run_step(
    parent_job_id: u64,
    step_id: u32,
    binary: String,
    args: Vec<String>,
    node_list: Vec<String>,
    _assigned_cores: Option<Vec<usize>>,
    working_directory: String,
    _user_id: String,
    assigned_ranks: Vec<u32>,
    total_ranks: u32,
    tx: mpsc::Sender<Message>,
    run_as_worker_user: bool,
    pmi_rx: mpsc::Receiver<veloce_common::Message>,
    file_client: Arc<veloce_common::file_client::FileClient>,
    inputs: Vec<veloce_common::FileHandle>,
    mut kill_rx: mpsc::Receiver<()>,
    cgroup_manager: Arc<cgroups::CgroupManager>,
    secret: String,
    controller_url: String,
    env_vars: Vec<(String, String)>,
) -> Result<()> {
    let effective_wd = if run_as_worker_user {
        std::env::temp_dir().to_string_lossy().to_string()
    } else {
        working_directory.clone()
    };

    let work_dir_path = Path::new(&effective_wd);
    if !work_dir_path.exists() {
        std::fs::create_dir_all(work_dir_path)?;
    }

    // Cgroup for step
    let cgroup_name = format!("job-{}/step-{}", parent_job_id, step_id);
    if cgroup_manager.is_enabled() {
        // We need to create the job cgroup first if it doesn't exist on this worker
        // (e.g. if this worker is ONLY running a step, not the main job process)
        let _ = cgroup_manager.create_job_cgroup(parent_job_id);

        // Create step cgroup manually for now as create_job_cgroup is job-specific
        // I should probably generalize create_job_cgroup.
        let step_path = PathBuf::from("/sys/fs/cgroup/veloce").join(&cgroup_name);
        if !step_path.exists() {
            let _ = fs::create_dir_all(&step_path);
        }
    }

    // Process inputs
    if !inputs.is_empty() {
        process_inputs(&file_client, &inputs, work_dir_path).await?;
    }

    let work_dir = Path::new(&effective_wd);

    let hostfile_path =
        std::env::temp_dir().join(format!("veloce_hostfile_{}_{}.txt", parent_job_id, step_id));
    {
        let mut file = File::create(&hostfile_path).context("Failed to create hostfile")?;
        for node in &node_list {
            let host = if let Some(idx) = node.rfind(':') {
                &node[..idx]
            } else {
                node
            };
            writeln!(file, "{}", host).context("Failed to write to hostfile")?;
        }
    }

    let pmi_tx = tx.clone();
    let pmi_port = pmi::start_pmi_server(parent_job_id, step_id, pmi_tx, pmi_rx).await?;

    let mut job_env = prepare_job_env(
        parent_job_id,
        &node_list,
        &effective_wd,
        &hostfile_path.to_string_lossy(),
        None,
        None,
        &_user_id,
        &HashMap::new(),
    );
    // Merge provided env_vars
    for (k, v) in env_vars {
        job_env.insert(k, v);
    }
    let mut env_vars = job_env;
    env_vars.insert("VELOCE_STEP_ID".to_string(), step_id.to_string());
    env_vars.insert("PMI_PORT".to_string(), format!("127.0.0.1:{}", pmi_port));
    env_vars.insert("PMI_SIZE".to_string(), total_ranks.to_string());

    // PMI-2 Compatibility variables
    env_vars.insert("PMI2_PORT".to_string(), format!("127.0.0.1:{}", pmi_port));
    env_vars.insert("PMI_VERSION".to_string(), "2".to_string());
    env_vars.insert("PMI_SUBVERSION".to_string(), "0".to_string());

    let _ = tx
        .send(veloce_common::Message::StepStarted {
            parent_job_id,
            step_id,
        })
        .await;

    let mut child_handles = Vec::new();

    for rank in &assigned_ranks {
        let stdout_path = work_dir.join(format!(
            "{}-step-{}-rank-{}-stdout.log",
            parent_job_id, step_id, rank
        ));
        let stderr_path = work_dir.join(format!(
            "{}-step-{}-rank-{}-stderr.log",
            parent_job_id, step_id, rank
        ));

        let mut rank_env = env_vars.clone();
        rank_env.insert("PMI_ID".to_string(), rank.to_string());
        rank_env.insert("PMI_RANK".to_string(), rank.to_string());
        rank_env.insert("VELOCE_RANK".to_string(), rank.to_string());
        rank_env.insert("VELOCE_SIZE".to_string(), total_ranks.to_string());

        let (real_binary, real_args, extra_env) = parse_shell_prefix_command(&binary, &args);
        let host_env = filtered_host_env_for_launch(
            &veloce_common::job_policy::JobExecutionOptions::default(),
            !run_as_worker_user,
        );
        let mut command = Command::new(&real_binary);
        command
            .args(&real_args)
            .envs(host_env)
            .envs(rank_env) // Overlay rank/step vars
            .envs(extra_env) // Apply parsed env settings from openmpi wrapper
            .env("VELOCE_JOB_ID", parent_job_id.to_string())
            .env("VELOCE_STEP_ID", step_id.to_string())
            .env("VELOCE_SECRET", secret.clone())
            .env("VELOCE_CONTROLLER_URL", controller_url.clone())
            .env("VELOCE_CA_CERT", "/app/certs/ca.pem")
            .env("VELOCE_FILESERVER_URL", file_client.base_url())
            .env("VELOCE_FILESERVER_KEY", file_client.api_key())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        command.current_dir(&effective_wd);

        info!(
            "Worker spawning step rank {}: \"{}\" {:?} in {}",
            rank, real_binary, real_args, effective_wd
        );
        match command.spawn() {
            Ok(mut c) => {
                let stdout = c.stdout.take();
                let stderr = c.stderr.take();

                if cgroup_manager.is_enabled() {
                    if let Some(pid) = c.id() {
                        let step_path = PathBuf::from("/sys/fs/cgroup/veloce").join(format!(
                            "job-{}/step-{}/cgroup.procs",
                            parent_job_id, step_id
                        ));
                        debug!(
                            "Moving step rank process {} to cgroup {}",
                            pid,
                            step_path.display()
                        );
                        let _ = fs::write(&step_path, pid.to_string());
                    }
                }

                use std::io::Write;
                use tokio::io::AsyncReadExt;

                if let Some(mut rdr) = stdout {
                    let mut stdout_file = std::fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(&stdout_path)
                        .context("Failed to open stdout for rank")?;
                    let tx = tx.clone();
                    tokio::spawn(async move {
                        let mut buf = [0u8; 1024];
                        while let Ok(n) = rdr.read(&mut buf).await {
                            if n == 0 {
                                break;
                            }
                            let _ = stdout_file.write_all(&buf[..n]);
                            let _ = stdout_file.sync_all();
                            let _ = tx
                                .send(veloce_common::Message::StepOutput {
                                    parent_job_id,
                                    step_id,
                                    is_stderr: false,
                                    data: buf[..n].to_vec(),
                                })
                                .await;
                        }
                    });
                }

                if let Some(mut rdr) = stderr {
                    let mut stderr_file = std::fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(&stderr_path)
                        .context("Failed to open stderr for rank")?;
                    let tx = tx.clone();
                    tokio::spawn(async move {
                        let mut buf = [0u8; 1024];
                        while let Ok(n) = rdr.read(&mut buf).await {
                            if n == 0 {
                                break;
                            }
                            let _ = stderr_file.write_all(&buf[..n]);
                            let _ = stderr_file.sync_all();
                            let _ = tx
                                .send(veloce_common::Message::StepOutput {
                                    parent_job_id,
                                    step_id,
                                    is_stderr: true,
                                    data: buf[..n].to_vec(),
                                })
                                .await;
                        }
                    });
                }

                child_handles.push(c);
            }
            Err(e) => {
                error!("Failed to spawn step rank {}: {}", rank, e);
            }
        }
    }

    if !child_handles.is_empty() {
        let _ = tx
            .send(veloce_common::Message::StepStarted {
                parent_job_id,
                step_id,
            })
            .await;
    }

    let mut step_success = true;
    let mut last_code = 0;
    let mut kill_triggered = false;

    for mut child in child_handles {
        tokio::select! {
            _ = kill_rx.recv() => {
                info!("Step {}:{} received kill signal, terminating child processes", parent_job_id, step_id);
                let _ = child.start_kill();
                step_success = false;
                last_code = -1;
                kill_triggered = true;
                break;
            }
            res = child.wait() => {
                match res {
                    Ok(status) => {
                        let code = status.code().unwrap_or(-1);
                        info!("Step {}:{} rank finished with code {}", parent_job_id, step_id, code);
                        if code != 0 {
                            step_success = false;
                            last_code = code;
                        }
                    }
                    Err(e) => {
                        error!("Wait failed: {}", e);
                        step_success = false;
                        last_code = -1;
                    }
                }
            }
        }
    }

    if kill_triggered {
        let _ = tx
            .send(veloce_common::Message::StepDone {
                parent_job_id,
                step_id,
                exit_code: -1,
            })
            .await;
    } else if step_success {
        let _ = tx
            .send(veloce_common::Message::StepDone {
                parent_job_id,
                step_id,
                exit_code: 0,
            })
            .await;
    } else {
        let _ = tx
            .send(veloce_common::Message::StepDone {
                parent_job_id,
                step_id,
                exit_code: last_code,
            })
            .await;
    }

    let _ = std::fs::remove_file(&hostfile_path);

    Ok(())
}

async fn stream_job_output<R: tokio::io::AsyncRead + Unpin>(
    job_id: u64,
    mut reader: R,
    mut file: std::fs::File,
    stream_type: &'static str,
    worker_id: String,
) {
    use tokio::io::AsyncReadExt;
    let mut buffer = [0u8; 4096];
    loop {
        match reader.read(&mut buffer).await {
            Ok(0) => break,
            Ok(n) => {
                // Write to file
                let _ = file.write_all(&buffer[..n]);
                let _ = file.sync_all();

                // Stream to Noise channel via GLOBAL_LOG_TX
                if let Some(tx) = GLOBAL_LOG_TX.get() {
                    if let Ok(line) = String::from_utf8(buffer[..n].to_vec()) {
                        let mut labels = HashMap::new();
                        labels.insert("worker_id".to_string(), worker_id.clone());
                        labels.insert("job_id".to_string(), job_id.to_string());
                        labels.insert("stream".to_string(), stream_type.to_string());

                        let timestamp = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_secs();
                        let _ = tx.try_send(Message::LogStream {
                            labels,
                            line,
                            timestamp,
                        });
                    }
                }
            }
            Err(e) => {
                error!("Error reading {} for job {}: {}", stream_type, job_id, e);
                break;
            }
        }
    }
}

#[cfg(target_family = "unix")]
unsafe fn custom_setrlimit(resource: libc::c_int, soft: u64, hard: u64) -> std::io::Result<()> {
    let rlim = libc::rlimit {
        rlim_cur: soft as libc::rlim_t,
        rlim_max: hard as libc::rlim_t,
    };
    //    if libc::setrlimit(resource, &rlim) != 0 {
    if libc::setrlimit(resource as _, &rlim) != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}
