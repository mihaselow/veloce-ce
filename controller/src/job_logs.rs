use crate::state::{GlobalState, MultinodeJobTracking, SharedContext};
use anyhow::Result;
use std::collections::HashMap;
use std::path::Path;
use veloce_common::{JobInfo, JobUsage, LogType, WorkerLogFiles};

pub fn record_dispatched_workers(state: &mut GlobalState, job_id: u64, workers: Vec<String>) {
    state
        .multinode_job_tracking
        .dispatched_workers
        .insert(job_id, workers);
}

pub fn record_worker_log_files(
    tracking: &mut MultinodeJobTracking,
    job_id: u64,
    worker_id: &str,
    stdout_file_id: Option<String>,
    stderr_file_id: Option<String>,
) {
    tracking
        .worker_log_files
        .entry(job_id)
        .or_default()
        .insert(worker_id.to_string(), (stdout_file_id, stderr_file_id));
}

pub fn tracking_to_worker_log_files(
    log_files: &HashMap<String, (Option<String>, Option<String>)>,
) -> HashMap<String, WorkerLogFiles> {
    log_files
        .iter()
        .map(|(worker_id, (stdout, stderr))| {
            (
                worker_id.clone(),
                WorkerLogFiles {
                    stdout_file_id: stdout.clone(),
                    stderr_file_id: stderr.clone(),
                },
            )
        })
        .collect()
}

/// Union per-worker log ids so a partial accounting UPSERT cannot drop peers.
pub fn merge_worker_log_file_maps(
    mut base: HashMap<String, WorkerLogFiles>,
    extra: HashMap<String, WorkerLogFiles>,
) -> HashMap<String, WorkerLogFiles> {
    for (worker_id, files) in extra {
        base.entry(worker_id)
            .and_modify(|existing| {
                if files.stdout_file_id.is_some() {
                    existing.stdout_file_id = files.stdout_file_id.clone();
                }
                if files.stderr_file_id.is_some() {
                    existing.stderr_file_id = files.stderr_file_id.clone();
                }
            })
            .or_insert(files);
    }
    base
}

pub fn expected_done_workers(tracking: &MultinodeJobTracking, job_id: u64, job: &JobInfo) -> usize {
    tracking
        .dispatched_workers
        .get(&job_id)
        .map(|workers| workers.len())
        .unwrap_or_else(|| job.assigned_workers.len().max(1))
}

pub fn mark_worker_done(
    tracking: &mut MultinodeJobTracking,
    job_id: u64,
    worker_id: &str,
) -> usize {
    tracking
        .done_workers
        .entry(job_id)
        .or_default()
        .insert(worker_id.to_string());
    tracking
        .done_workers
        .get(&job_id)
        .map(|done| done.len())
        .unwrap_or(0)
}

pub fn cleanup_job_tracking(tracking: &mut MultinodeJobTracking, job_id: u64) {
    tracking.dispatched_workers.remove(&job_id);
    tracking.worker_log_files.remove(&job_id);
    tracking.done_workers.remove(&job_id);
}

pub fn worker_order_for_logs(job: &JobInfo, tracking: &MultinodeJobTracking) -> Vec<String> {
    if let Some(dispatched) = tracking.dispatched_workers.get(&job.id) {
        if !dispatched.is_empty() {
            return dispatched.clone();
        }
    }
    job.assigned_workers.clone()
}

/// Resolve which fileserver object to download for a completed (history) job.
/// Prefer per-worker ids when `rank` is set; otherwise fall back to the job-level id
/// (often a merged multinode artifact).
pub fn resolve_history_log_file_id(
    usage: &JobUsage,
    log_type: &LogType,
    rank: Option<usize>,
) -> Result<Option<String>, String> {
    if let Some(rank) = rank {
        let worker_id = usage
            .assigned_workers
            .get(rank)
            .ok_or_else(|| format!("Invalid rank {rank} for job {}", usage.job_id))?;
        let files = usage.worker_log_files.get(worker_id);
        let file_id = match log_type {
            LogType::Stdout => files.and_then(|f| f.stdout_file_id.clone()),
            LogType::Stderr => files.and_then(|f| f.stderr_file_id.clone()),
        };
        if file_id.is_some() {
            return Ok(file_id);
        }
        // Legacy rows without worker_log_files: only rank 0 can use the single id.
        if rank == 0 {
            return Ok(match log_type {
                LogType::Stdout => usage.stdout_file_id.clone(),
                LogType::Stderr => usage.stderr_file_id.clone(),
            });
        }
        return Err(format!(
            "No per-worker log file for rank {rank} on job {} (worker {worker_id})",
            usage.job_id
        ));
    }

    Ok(match log_type {
        LogType::Stdout => usage.stdout_file_id.clone(),
        LogType::Stderr => usage.stderr_file_id.clone(),
    })
}

pub async fn merge_worker_log_bytes(
    ctx: &SharedContext,
    worker_order: &[String],
    log_files: &HashMap<String, (Option<String>, Option<String>)>,
    log_type: &LogType,
) -> Vec<u8> {
    let mut merged = Vec::new();
    let multinode = worker_order.len() > 1;
    for (rank, worker_id) in worker_order.iter().enumerate() {
        let Some((stdout, stderr)) = log_files.get(worker_id) else {
            continue;
        };
        let file_id = match log_type {
            LogType::Stdout => stdout,
            LogType::Stderr => stderr,
        };
        let Some(id) = file_id else { continue };
        if let Ok(bytes) = ctx.file_client.download_file_to_bytes(id).await {
            if multinode {
                let header = format!("=== rank {rank} ({worker_id}) ===\n");
                merged.extend_from_slice(header.as_bytes());
            }
            merged.extend_from_slice(&bytes);
            if !bytes.ends_with(b"\n") {
                merged.push(b'\n');
            }
        }
    }
    merged
}

async fn upload_merged_log(
    ctx: &SharedContext,
    job_id: u64,
    suffix: &str,
    bytes: &[u8],
) -> Result<Option<String>> {
    if bytes.is_empty() {
        return Ok(None);
    }
    let path = std::env::temp_dir().join(format!("veloce-{job_id}-merged-{suffix}.log"));
    std::fs::write(&path, bytes)?;
    let uploaded = ctx
        .file_client
        .upload_file(Path::new(&path), false, false)
        .await
        .ok()
        .map(|handle| handle.file_id);
    let _ = std::fs::remove_file(&path);
    Ok(uploaded)
}

pub async fn merge_and_upload_job_logs(
    ctx: &SharedContext,
    job_id: u64,
    worker_order: &[String],
    log_files: &HashMap<String, (Option<String>, Option<String>)>,
) -> (Option<String>, Option<String>) {
    if worker_order.len() <= 1 {
        let sole = worker_order
            .first()
            .and_then(|worker_id| log_files.get(worker_id))
            .cloned()
            .unwrap_or((None, None));
        return sole;
    }

    let stdout_bytes = merge_worker_log_bytes(ctx, worker_order, log_files, &LogType::Stdout).await;
    let stderr_bytes = merge_worker_log_bytes(ctx, worker_order, log_files, &LogType::Stderr).await;

    let stdout_file_id = upload_merged_log(ctx, job_id, "stdout", &stdout_bytes)
        .await
        .unwrap_or(None);
    let stderr_file_id = upload_merged_log(ctx, job_id, "stderr", &stderr_bytes)
        .await
        .unwrap_or(None);
    (stdout_file_id, stderr_file_id)
}

pub async fn fetch_multinode_log_text(
    ctx: &SharedContext,
    job: &JobInfo,
    tracking: &MultinodeJobTracking,
    log_type: &LogType,
    offset: usize,
    length: Option<usize>,
) -> Option<String> {
    let worker_order = worker_order_for_logs(job, tracking);
    if worker_order.len() <= 1 {
        return None;
    }

    let log_files = tracking.worker_log_files.get(&job.id)?;
    if log_files.is_empty() {
        return None;
    }

    let bytes = merge_worker_log_bytes(ctx, &worker_order, log_files, log_type).await;
    if bytes.is_empty() {
        return None;
    }
    let text = String::from_utf8_lossy(&bytes).to_string();
    let sliced = if offset < text.len() {
        if let Some(len) = length {
            let end = (offset + len).min(text.len());
            text[offset..end].to_string()
        } else {
            text[offset..].to_string()
        }
    } else {
        String::new()
    };
    Some(sliced)
}

#[cfg(test)]
mod tests {
    use super::*;
    use veloce_common::{JobStatus, QosLevel};

    fn sample_usage() -> JobUsage {
        let mut worker_log_files = HashMap::new();
        worker_log_files.insert(
            "w0".into(),
            WorkerLogFiles {
                stdout_file_id: Some("s3://out-w0".into()),
                stderr_file_id: Some("s3://err-w0".into()),
            },
        );
        worker_log_files.insert(
            "w1".into(),
            WorkerLogFiles {
                stdout_file_id: Some("s3://out-w1".into()),
                stderr_file_id: Some("s3://err-w1".into()),
            },
        );
        JobUsage {
            job_id: 42,
            job_name: None,
            job_comment: None,
            command_line: "hostname".into(),
            user_id: "lab".into(),
            submission_time: 0,
            start_time: None,
            end_time: None,
            exit_code: Some(0),
            status: JobStatus::Completed(0),
            cpu_time_ms: 0,
            max_memory_bytes: 0,
            req_nodes: 2,
            req_cores: 1,
            req_memory: 256,
            array_id: None,
            array_task_id: None,
            assigned_workers: vec!["w0".into(), "w1".into()],
            gres_req: Default::default(),
            cgroup_active: false,
            secret: "s".into(),
            stdout_file_id: Some("s3://merged-out".into()),
            stderr_file_id: Some("s3://merged-err".into()),
            workdir_file_id: None,
            worker_log_files,
            output_artifacts: vec![],
            wait_for_licenses: false,
            estimated_walltime: None,
            priority_offset: None,
            dependencies: None,
            dependency_specs: None,
            qos: QosLevel::Production,
            container_asset: None,
        }
    }

    #[test]
    fn history_rank_resolves_per_worker_files() {
        let usage = sample_usage();
        assert_eq!(
            resolve_history_log_file_id(&usage, &LogType::Stdout, Some(0)).unwrap(),
            Some("s3://out-w0".into())
        );
        assert_eq!(
            resolve_history_log_file_id(&usage, &LogType::Stdout, Some(1)).unwrap(),
            Some("s3://out-w1".into())
        );
        assert_eq!(
            resolve_history_log_file_id(&usage, &LogType::Stderr, Some(1)).unwrap(),
            Some("s3://err-w1".into())
        );
    }

    #[test]
    fn history_without_rank_uses_job_level_file() {
        let usage = sample_usage();
        assert_eq!(
            resolve_history_log_file_id(&usage, &LogType::Stdout, None).unwrap(),
            Some("s3://merged-out".into())
        );
    }

    #[test]
    fn history_invalid_rank_errors() {
        let usage = sample_usage();
        assert!(resolve_history_log_file_id(&usage, &LogType::Stdout, Some(9)).is_err());
    }

    #[test]
    fn merge_worker_log_file_maps_unions_workers() {
        let mut a = HashMap::new();
        a.insert(
            "w0".into(),
            WorkerLogFiles {
                stdout_file_id: Some("out0".into()),
                stderr_file_id: Some("err0".into()),
            },
        );
        let mut b = HashMap::new();
        b.insert(
            "w1".into(),
            WorkerLogFiles {
                stdout_file_id: Some("out1".into()),
                stderr_file_id: None,
            },
        );
        let merged = merge_worker_log_file_maps(a, b);
        assert_eq!(merged.len(), 2);
        assert_eq!(merged["w0"].stdout_file_id.as_deref(), Some("out0"));
        assert_eq!(merged["w1"].stdout_file_id.as_deref(), Some("out1"));
    }
}
