use crate::state::{GlobalState, MultinodeJobTracking, SharedContext};
use anyhow::Result;
use std::collections::HashMap;
use std::path::Path;
use veloce_common::{JobInfo, LogType};

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
