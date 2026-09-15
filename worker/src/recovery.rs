#![allow(clippy::all)]
use crate::job_runner::{JobStatsEntry, KillSignal};
use anyhow::{Context, Result};
use std::collections::HashMap;
use std::io::{Read, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};
use sysinfo::{Pid, PidExt, ProcessExt, System, SystemExt};
use tokio::sync::{mpsc, oneshot};
use tokio::time::{sleep, Duration};
use tracing::{error, info, warn};
use veloce_common::{Message, PersistedJob};

pub const PERSISTENCE_FILE: &str = "data/veloce_worker_jobs.bin";
pub const JOB_WDS_FILE: &str = "data/veloce_worker_job_wds.bin";

pub(crate) fn load_persisted_job_wds(path: &Path) -> Result<HashMap<u64, String>> {
    if !path.exists() {
        return Ok(HashMap::new());
    }

    let mut file = std::fs::File::open(path).context("Failed to open job WDs file for reading")?;

    let mut wds = HashMap::new();
    let mut buffer = Vec::new();

    loop {
        let mut len_bytes = [0u8; 8];
        if file.read_exact(&mut len_bytes).is_err() {
            break;
        }
        let len = u64::from_le_bytes(len_bytes) as usize;

        if len == 0 {
            break;
        }

        buffer.resize(len, 0);
        file.read_exact(&mut buffer)
            .context("Failed to read job WD data")?;

        match bincode::deserialize::<(u64, String)>(&buffer) {
            Ok((job_id, wd)) => {
                wds.insert(job_id, wd);
            }
            Err(e) => {
                error!("Failed to deserialize job WD: {}", e);
                break;
            }
        }
    }

    Ok(wds)
}

pub(crate) fn persist_job_wd(path: &Path, job_id: u64, wd: &str) -> Result<()> {
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .context("Failed to open job WDs file")?;

    let encoded: Vec<u8> =
        bincode::serialize(&(job_id, wd.to_string())).context("Failed to serialize job WD")?;

    file.write_all(&(encoded.len() as u64).to_le_bytes())
        .context("Failed to write length prefix to job WDs file")?;
    file.write_all(&encoded)
        .context("Failed to write job WD to file")?;

    file.sync_all().context("Failed to sync job WDs file")?;

    Ok(())
}

static PERSISTENCE_LOCK: Mutex<()> = Mutex::new(());

pub(crate) fn load_persisted_job_states(path: &Path) -> Result<Vec<PersistedJob>> {
    let _guard = PERSISTENCE_LOCK.lock().unwrap();
    load_persisted_job_states_internal(path)
}

fn load_persisted_job_states_internal(path: &Path) -> Result<Vec<PersistedJob>> {
    if !path.exists() {
        return Ok(Vec::new());
    }

    let mut file =
        std::fs::File::open(path).context("Failed to open persistence file for reading")?;

    let mut jobs = Vec::new();
    let mut buffer = Vec::new(); // Use a buffer to read chunks

    loop {
        let mut len_bytes = [0u8; 8]; // u64 is 8 bytes
        if file.read_exact(&mut len_bytes).is_err() {
            // EOF or read error
            break;
        }
        let len = u64::from_le_bytes(len_bytes) as usize;

        if len == 0 {
            // Should not happen with valid serialized data, but as a safeguard
            break;
        }

        buffer.resize(len, 0); // Resize buffer to exact length
        file.read_exact(&mut buffer)
            .context("Failed to read job data from persistence file")?;

        match bincode::deserialize::<PersistedJob>(&buffer) {
            Ok(job) => jobs.push(job),
            Err(e) => {
                error!(
                    "Failed to deserialize job state from persistence file: {}",
                    e
                );
                // Depending on policy, decide whether to continue or break
                break;
            }
        }
    }

    Ok(jobs)
}

pub(crate) fn persist_job_state(path: &Path, job_state: &PersistedJob) -> Result<()> {
    let _guard = PERSISTENCE_LOCK.lock().unwrap();
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .context("Failed to open persistence file")?;

    let encoded: Vec<u8> =
        bincode::serialize(job_state).context("Failed to serialize job state")?;

    // Write length prefix before the serialized data (little-endian)
    file.write_all(&(encoded.len() as u64).to_le_bytes())
        .context("Failed to write length prefix to persistence file")?;
    file.write_all(&encoded)
        .context("Failed to write job state to persistence file")?;

    file.sync_all().context("Failed to sync persistence file")?;

    Ok(())
}

pub(crate) fn remove_persisted_job_state(path: &Path, job_id_to_remove: u64) -> Result<()> {
    let _guard = PERSISTENCE_LOCK.lock().unwrap();
    let mut jobs = load_persisted_job_states_internal(path)?;
    let initial_len = jobs.len();
    jobs.retain(|job| job.job_id != job_id_to_remove);

    if jobs.len() == initial_len {
        warn!(
            "Attempted to remove job {} from persistence, but it was not found.",
            job_id_to_remove
        );
        return Ok(());
    }

    // Overwrite the file with remaining jobs using temporary file + rename
    let temp_path = path.with_extension("tmp");
    {
        let mut file = std::fs::File::create(&temp_path)
            .context("Failed to create temporary persistence file for rewriting")?;

        for job in jobs {
            let encoded: Vec<u8> =
                bincode::serialize(&job).context("Failed to serialize job state for rewriting")?;

            file.write_all(&(encoded.len() as u64).to_le_bytes())
                .context("Failed to write length prefix during rewrite")?;
            file.write_all(&encoded)
                .context("Failed to write job state during rewrite")?;
        }

        file.sync_all()
            .context("Failed to sync temporary persistence file during rewrite")?;
    }

    std::fs::rename(&temp_path, path).context("Failed to rename temporary persistence file")?;

    Ok(())
}

pub(crate) async fn recover_and_monitor_job(
    persisted_job: PersistedJob,
    tx: mpsc::Sender<Message>,
    rx_kill: oneshot::Receiver<KillSignal>,
    running_pids: Arc<Mutex<HashMap<u64, Pid>>>,
    job_stats_history: Arc<Mutex<HashMap<u64, JobStatsEntry>>>,
) -> Result<()> {
    let job_id = persisted_job.job_id;
    let pid = Pid::from(persisted_job.pid as usize);
    let pgid = persisted_job.pgid;

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    if persisted_job.walltime > 0 {
        if let Some(elapsed) = now.checked_sub(persisted_job.submission_time) {
            if elapsed > persisted_job.walltime {
                warn!(
                    "Recovered job {} already exceeded walltime during downtime ({}s). Clearing.",
                    job_id, persisted_job.walltime
                );
                if let Err(e) = remove_persisted_job_state(Path::new(PERSISTENCE_FILE), job_id) {
                    error!("Failed to remove expired persisted job {}: {}", job_id, e);
                }
                return Ok(());
            }
        }
    }

    info!(
        "Attempting to recover and monitor job {} (PID: {}, PGID: {})",
        job_id, pid, pgid
    );

    // Check if the process or process group is still alive and VERIFY identity
    let mut sys = System::new();
    sys.refresh_process(pid);

    let process_id_match = if let Some(p) = sys.process(pid) {
        // Verify command line or binary name to avoid PID reuse issues
        let cmd = p.cmd();
        let persisted_binary = &persisted_job.binary;

        // Simple heuristic: Does the command line contain the binary name?
        // Or if we have args, do they match?
        let cmd_joined = cmd.join(" ");
        let expected_cmd = format!("{} {}", persisted_job.binary, persisted_job.args.join(" "));

        let matches = if !cmd.is_empty() {
            // Check if binary name is the first element or present
            cmd[0].contains(persisted_binary) || cmd_joined.contains(&expected_cmd)
        } else {
            false
        };

        if !matches {
            warn!("PID {} found, but command line {:?} does not match expected binary {}. PID reuse detected.", pid, cmd, persisted_binary);
            false
        } else {
            true
        }
    } else {
        false
    };

    if !process_id_match {
        let error_msg = "Job process not found or PID reused after restart.";
        warn!(
            "Recovered job {} (PID: {}, PGID: {}) invalid. reporting failure.",
            job_id, pid, pgid
        );
        let _ = tx
            .send(Message::JobError {
                job_id,
                error: error_msg.to_string(),
            })
            .await;
        if let Err(e) = remove_persisted_job_state(Path::new(PERSISTENCE_FILE), job_id) {
            error!(
                "Failed to remove persisted job {} after recovery failure: {}",
                job_id, e
            );
        }
        return Ok(());
    }

    // Add to running_pids map so heartbeat can track it
    {
        let mut pids_lock = running_pids.lock().unwrap();
        pids_lock.insert(job_id, pid);
    }

    {
        let mut stats = job_stats_history.lock().unwrap();
        stats.insert(
            job_id,
            JobStatsEntry {
                start_time: now,
                submission_time: persisted_job.submission_time,
                max_memory_bytes: 0,
                cpu_time_ms: 0,
                command_line: format!("{} {}", persisted_job.binary, persisted_job.args.join(" ")),
                user_id: persisted_job.user_id.clone(),
                req_nodes: 1, // Simplified for recovered jobs
                req_cores: persisted_job.req_cores,
                req_memory: persisted_job.req_memory,
                array_id: persisted_job.array_id,
                array_task_id: persisted_job.array_task_id,
                assigned_workers: Vec::new(), // Unknown on recovery
                last_active_at: now,
                is_idle: false,
                idle_duration: 0,
                gres_req: persisted_job.gres_req.clone(),
                secret: persisted_job.secret.clone(),
                wait_for_licenses: persisted_job.wait_for_licenses,
                estimated_walltime: persisted_job.estimated_walltime,
                priority_offset: persisted_job.priority_offset,
                dependencies: persisted_job.dependencies.clone(),
                dependency_specs: persisted_job.dependency_specs.clone(),
                qos: persisted_job.qos.clone(),
                cgroup_last_cpu_usec: None,
                cgroup_last_measured_at: None,
                namespace_last_cpu_ticks: None,
                namespace_last_measured_at: None,
            },
        );
    }

    // Simulate waiting for a child process. Since we can't `tokio::process::Command::from_pid`
    // easily, we'll monitor using sysinfo or `waitid` for Unix.
    // For now, let's just create a loop that checks if the process is alive.
    // This is a simplified monitoring. A proper re-attachment to process events is complex.

    let walltime_duration = if persisted_job.walltime > 0 {
        Duration::from_secs(persisted_job.walltime)
    } else {
        Duration::from_secs(365 * 24 * 3600 * 100) // Effectively infinite
    };
    tokio::select! {
        res = rx_kill => {
            let signal = res.unwrap_or(KillSignal::Cancel);
            match signal {
                KillSignal::Cancel => {
                    info!("Killing recovered job {}", job_id);
                    #[cfg(target_family = "unix")]
                    {
                         unsafe {
                            let target_pgid = if pgid != 0 { pgid as i32 } else { pid.as_u32() as i32 };
                            libc::kill(-target_pgid, libc::SIGKILL);
                         }
                    }
                }
                KillSignal::Preempt => {
                    info!("Preempting recovered job {}", job_id);
                    #[cfg(target_family = "unix")]
                    {
                         unsafe {
                            let target_pgid = if pgid != 0 { pgid as i32 } else { pid.as_u32() as i32 };
                            libc::kill(-target_pgid, libc::SIGTERM);
                         }
                    }
                }
            }
            tx.send(Message::JobDone { job_id, exit_code: -9 }).await?;
            if let Err(e) = remove_persisted_job_state(Path::new(PERSISTENCE_FILE), job_id) {
                error!("Failed to remove persisted job {} after kill on recovery: {}", job_id, e);
            }
        }
        _ = sleep(walltime_duration) => {
             info!("Recovered job {} exceeded walltime ({}s). Killing...", job_id, persisted_job.walltime);
            #[cfg(target_family = "unix")]
            {
                 unsafe {
                    let target_pgid = if pgid != 0 { pgid as i32 } else { pid.as_u32() as i32 };
                    libc::kill(-target_pgid, libc::SIGKILL);
                 }
            }
            if let Err(e) = remove_persisted_job_state(Path::new(PERSISTENCE_FILE), job_id) {
                error!("Failed to remove persisted job {} after walltime kill: {}", job_id, e);
            }
            let _ = tx.send(Message::JobError { job_id, error: "Walltime exceeded".to_string() }).await;
        }
        _ = tokio::task::spawn_blocking(move || {
            // This blocking task monitors the process group.
            loop {
                // Check if the process group still exists
                #[cfg(target_family = "unix")]
                let alive = unsafe {
                    let target = if pgid != 0 { -(pgid as i32) } else { pid.as_u32() as i32 };
                    if libc::kill(target, 0) == 0 {
                        true
                    } else {
                        let errno = std::io::Error::last_os_error().raw_os_error();
                        errno == Some(libc::EPERM)
                    }
                };

                #[cfg(not(target_family = "unix"))]
                let alive = {
                    let mut current_sys = System::new();
                    current_sys.refresh_processes();
                    if pgid != 0 {
                         let mut found = false;
                         for (_p_id, p) in current_sys.processes() {
                            if let Some(process_pgid) = p.group_id() {
                                if process_pgid.as_u32() == pgid {
                                    found = true;
                                    break;
                                }
                            }
                         }
                         found
                    } else {
                        current_sys.process(pid).is_some()
                    }
                };

                if !alive {
                    info!("Recovered job {} process group (PGID: {}) has terminated.", job_id, pgid);
                    return;
                }
                std::thread::sleep(Duration::from_secs(5)); // Check every 5 seconds
            }
        }) => {
            // We can't get the actual exit code this way, so we report a special value (-100)
            // indicating that the actual status is unknown because it was recovered after a restart.
            let _ = tx.send(Message::JobDone { job_id, exit_code: -100 }).await;
            if let Err(e) = remove_persisted_job_state(Path::new(PERSISTENCE_FILE), job_id) {
                error!("Failed to remove persisted job {} after termination on recovery: {}", job_id, e);
            }
        }
    }

    Ok(())
}
