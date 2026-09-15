//! On-disk node metrics ring buffer and Prometheus export.

use crate::state::SharedContext;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::sync::atomic::Ordering;
use tracing::error;
use veloce_common::{JobStatus, NodeMetrics};

pub const METRICS_FILE: &str = "data/veloce_metrics.bin";

static METRICS_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

pub(crate) fn append_metrics(metrics: &NodeMetrics) {
    let _lock = METRICS_LOCK.lock().unwrap();
    if let Ok(encoded) = bincode::serialize(metrics) {
        if let Ok(mut file) = OpenOptions::new()
            .create(true)
            .append(true)
            .open(METRICS_FILE)
        {
            let len = encoded.len() as u64;
            let mut combined = Vec::with_capacity(8 + encoded.len());
            combined.extend_from_slice(&len.to_le_bytes());
            combined.extend_from_slice(&encoded);

            if let Err(e) = file.write_all(&combined) {
                error!("Failed to write to metrics file: {}", e);
            } else {
                let _ = file.sync_all();
            }
        } else {
            error!("Failed to open metrics file for appending");
        }
    }
}

pub(crate) fn read_metrics(
    start_time: Option<u64>,
    end_time: Option<u64>,
    nodes: Option<Vec<String>>,
    _aggregate: bool,
) -> Vec<NodeMetrics> {
    let _lock = METRICS_LOCK.lock().unwrap();
    let mut results = Vec::new();
    if let Ok(mut file) = File::open(METRICS_FILE) {
        loop {
            let mut len_bytes = [0u8; 8];
            if file.read_exact(&mut len_bytes).is_err() {
                break;
            }
            let len = u64::from_le_bytes(len_bytes) as usize;
            let mut buffer = vec![0u8; len];
            if file.read_exact(&mut buffer).is_err() {
                break;
            }

            if let Ok(metric) = bincode::deserialize::<NodeMetrics>(&buffer) {
                let mut include = true;
                if let Some(start) = start_time {
                    if metric.timestamp < start {
                        include = false;
                    }
                }
                if let Some(end) = end_time {
                    if metric.timestamp > end {
                        include = false;
                    }
                }
                if let Some(ref ns) = nodes {
                    if !ns.contains(&metric.node_id) {
                        include = false;
                    }
                }

                if include {
                    results.push(metric);
                }
            }
        }
    }
    results
}

pub async fn generate_prometheus_metrics(ctx: SharedContext) -> String {
    let mut out = String::new();

    // 1. Map node IDs to Resources from worker_resources DashMap
    let mut worker_refs = HashMap::new();
    let mut draining = 0;
    for entry in ctx.worker_resources.iter() {
        let (id, (res, sync_state)) = entry.pair();
        worker_refs.insert(id.clone(), res.clone());
        if sync_state.draining {
            draining += 1;
        }
    }

    // 2. Count Job States, calculate Resource Gap, and MPI stats from active_jobs DashMap
    let mut running = 0;
    let mut pending = 0;
    let mut gap_cores = 0;
    let mut gap_nodes = 0;
    let mut gap_mem = 0;
    let mut total_mpi_send_calls = 0;
    let mut total_mpi_send_bytes = 0;
    let mut total_mpi_time = 0.0;
    let mut all_jobs = Vec::new();

    for entry in ctx.active_jobs.iter() {
        let job = entry.value();
        all_jobs.push(job.clone());
        match job.status {
            JobStatus::Running => running += 1,
            JobStatus::Pending => {
                pending += 1;
                gap_cores += job.req_cores * job.req_nodes as u32;
                gap_nodes += job.req_nodes;
                gap_mem += job.req_memory * job.req_nodes as u64;
            }
            _ => {}
        }
        if let Some(stats) = &job.mpi_stats {
            total_mpi_send_calls += stats.send_calls as u64;
            total_mpi_send_bytes += stats.send_bytes;
            total_mpi_time += stats.send_time_secs;
        }
    }

    let completed = ctx.completed_jobs.load(Ordering::Relaxed);
    let failed = ctx.failed_jobs.load(Ordering::Relaxed);

    // KEDA / Resource Gap Metrics
    out.push_str(&format!("veloce_queue_gap_cores {}\n", gap_cores));
    out.push_str(&format!("veloce_queue_gap_nodes {}\n", gap_nodes));
    out.push_str(&format!(
        "veloce_queue_gap_memory_bytes {}\n",
        gap_mem * 1024 * 1024
    ));

    // Node Metrics (Using DashMap store - NO LOCK HELD)
    for entry in ctx.metrics_store.iter() {
        let (node_id, metrics): (&String, &NodeMetrics) = entry.pair();
        let safe_id = node_id.replace(|c: char| !c.is_alphanumeric(), "_");

        let res = worker_refs.get(node_id);
        let name = res
            .map(|r| r.host_name.clone())
            .unwrap_or_else(|| "unknown".into());

        let labels = format!("node=\"{}\", name=\"{}\"", safe_id, name);

        out.push_str(&format!(
            "veloce_node_cpu_load{{{}}} {}\n",
            labels, metrics.cpu_load
        ));
        out.push_str(&format!(
            "veloce_node_memory_usage_bytes{{{}}} {}\n",
            labels, metrics.memory_usage
        ));
        out.push_str(&format!(
            "veloce_node_memory_total_bytes{{{}}} {}\n",
            labels, metrics.memory_total
        ));
        out.push_str(&format!(
            "veloce_node_swap_usage_bytes{{{}}} {}\n",
            labels, metrics.swap_usage
        ));
        out.push_str(&format!(
            "veloce_node_process_count{{{}}} {}\n",
            labels, metrics.process_count
        ));
        out.push_str(&format!(
            "veloce_node_uptime_seconds{{{}}} {}\n",
            labels, metrics.uptime
        ));
        out.push_str(&format!(
            "veloce_node_running_jobs{{{}}} {}\n",
            labels, metrics.running_jobs
        ));
        out.push_str(&format!(
            "veloce_node_network_rx_bytes{{{}}} {}\n",
            labels, metrics.net_rx_rate
        ));
        out.push_str(&format!(
            "veloce_node_network_tx_bytes{{{}}} {}\n",
            labels, metrics.net_tx_rate
        ));
        out.push_str(&format!(
            "veloce_node_disk_read_bytes{{{}}} {}\n",
            labels, metrics.disk_read_rate
        ));
        out.push_str(&format!(
            "veloce_node_disk_write_bytes{{{}}} {}\n",
            labels, metrics.disk_write_rate
        ));

        if let Some(r) = res {
            out.push_str(&format!(
                "veloce_node_disk_total_bytes{{{}}} {}\n",
                labels, r.disk_total
            ));
            out.push_str(&format!(
                "veloce_node_disk_free_bytes{{{}}} {}\n",
                labels, r.disk_free
            ));
            out.push_str(&format!(
                "veloce_node_boot_timestamp_seconds{{{}}} {}\n",
                labels, r.boot_time
            ));
            out.push_str(&format!(
                "veloce_node_info{{{}, version=\"{}\", arch=\"{}\", os=\"{}\"}} 1\n",
                labels, r.version, r.arch, r.os_name
            ));
        }
    }

    // Cluster Job Totals
    out.push_str(&format!(
        "veloce_jobs_total{{state=\"running\"}} {}\n",
        running
    ));
    out.push_str(&format!(
        "veloce_jobs_total{{state=\"pending\"}} {}\n",
        pending
    ));
    out.push_str(&format!(
        "veloce_jobs_total{{state=\"completed\"}} {}\n",
        completed
    ));
    out.push_str(&format!("veloce_cluster_jobs_failed {}\n", failed));
    out.push_str(&format!("veloce_cluster_workers_draining {}\n", draining));

    // MPI Aggregates
    out.push_str(&format!(
        "veloce_cluster_mpi_send_calls_total {}\n",
        total_mpi_send_calls
    ));
    out.push_str(&format!(
        "veloce_cluster_mpi_send_bytes_total {}\n",
        total_mpi_send_bytes
    ));
    out.push_str(&format!(
        "veloce_cluster_mpi_time_seconds_total {}\n",
        total_mpi_time
    ));

    // Detailed Job Metrics (Current Jobs Only)
    for job in all_jobs {
        let status_str = match job.status {
            JobStatus::Pending => "pending",
            JobStatus::Running => "running",
            JobStatus::Completed(_) => "completed",
            JobStatus::Failed(_) => "failed",
            JobStatus::Killed => "killed",
        };

        let binary_safe = job.binary.replace('"', "\\\"");
        let user_safe = job.user_id.replace('"', "\\\"");

        let labels = format!(
            "id=\"{}\", user=\"{}\", status=\"{}\", binary=\"{}\", array_id=\"{}\", array_task_id=\"{}\", priority=\"{}\", nodes=\"{}\", req_cores=\"{}\", req_memory_mb=\"{}\", used_cpu_ms=\"{}\", used_mem_mb=\"{}\"",
            job.id,
            user_safe,
            status_str,
            binary_safe,
            job.array_id.map(|v| v.to_string()).unwrap_or_else(|| "".into()),
            job.array_task_id.map(|v| v.to_string()).unwrap_or_else(|| "".into()),
            job.priority,
            job.assigned_workers.join(","),
            job.req_cores,
            job.req_memory,
            0, // Active jobs haven't reported total CPU time yet
            job.current_memory_usage / 1024 / 1024
        );

        out.push_str(&format!("veloce_job_info{{{}}} 1\n", labels));
    }

    out
}
