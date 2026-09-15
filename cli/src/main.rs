#![allow(clippy::all)]
use anyhow::{Context, Result};
use base64::{engine::general_purpose, Engine as _};
use chrono::{Local, TimeZone};
use clap::{CommandFactory, Parser, Subcommand};
use clap_complete::{generate, Shell};
use futures::{SinkExt, StreamExt};
use log::debug;
use serde::{Deserialize, Serialize};
use std::io::Write;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;
use veloce_common::{noise, HistoryFilter, JobStateFilter, JobStatus, Message, MessageCodec};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Controller address to connect to
    #[arg(
        short = 'C',
        long,
        env = "VELOCE_CONTROLLER",
        default_value = "127.0.0.1:9000",
        global = true
    )]
    controller: String,

    /// Cluster secret for authentication
    #[arg(
        long,
        env = "VELOCE_SECRET",
        default_value = "veloce_default_secret_change_me",
        global = true
    )]
    secret: String,

    /// Fileserver URL
    #[arg(
        long,
        env = "VELOCE_FILESERVER",
        default_value = "https://127.0.0.1:9001",
        global = true
    )]
    fileserver: String,

    /// Fileserver API key / authentication token
    #[arg(long, env = "VELOCE_FILESERVER_KEY", global = true)]
    fileserver_key: Option<String>,

    /// Output as JSON
    #[arg(long, global = true)]
    json: bool,

    /// Comma-separated list of controller addresses for HA
    #[arg(long, env = "VELOCE_CONTROLLERS", global = true)]
    controllers: Option<String>,

    /// Controller REST API key (X-API-KEY). Required for containers register/list/delete
    /// (admin role for register/delete). Not VELOCE_SECRET.
    #[arg(long, env = "VELOCE_API_KEY", global = true)]
    api_key: Option<String>,

    /// Controller HTTPS API base URL for REST-backed operations
    #[arg(
        long,
        env = "VELOCE_CONTROLLER_API",
        default_value = "https://127.0.0.1:8080",
        global = true
    )]
    controller_api: String,

    /// Client ID for Noise component registration
    #[arg(long, env = "VELOCE_CLIENT_ID", default_value = "cli", global = true)]
    client_id: String,

    /// Client registration token for Noise component registration
    #[arg(long, env = "VELOCE_CLIENT_TOKEN", global = true)]
    client_token: Option<String>,
}

#[derive(Subcommand, Clone)]
enum Commands {
    /// Submit a job
    Submit {
        /// Human-readable job name
        #[arg(long = "name", value_name = "NAME")]
        job_name: Option<String>,
        /// Free-form job comment
        #[arg(long = "comment", value_name = "COMMENT")]
        job_comment: Option<String>,
        /// Submit a CWL workflow DAG
        #[arg(long)]
        cwl: Option<String>,
        /// Number of nodes to request
        #[arg(short, long, default_value_t = 1)]
        nodes: usize,
        /// Cores per node
        #[arg(short = 'c', long, default_value_t = 1)]
        cores: u32,
        /// Memory per node in MB (0 = no limit)
        #[arg(short = 'm', long, default_value_t = 0)]
        mem: u64,
        /// Walltime limit in seconds (0 = no limit)
        #[arg(short, long, default_value_t = 0)]
        walltime: u64,
        /// Estimated walltime in seconds (used for backfilling projection)
        #[arg(long)]
        estimated_walltime: Option<u64>,
        /// Job priority (higher = more urgent)
        #[arg(short, long, default_value_t = 0)]
        priority: u32,
        /// User ID (defaults to current user)
        #[arg(long = "user", alias = "as-user", value_name = "USER")]
        as_user: Option<String>,
        /// Array indexing (e.g. 1-10, 1-10:2, 1,2,5)
        #[arg(long)]
        array: Option<String>,
        /// Wait for job completion
        #[arg(long)]
        wait: bool,
        /// Directory containing input files (will be compressed and uploaded)
        #[arg(long = "input-deck", alias = "input-dir", alias = "input_dir")]
        input_dir: Option<String>,
        /// Generic resources requested (e.g. gpu:1)
        #[arg(long, value_parser = parse_gres_req)]
        gres: Vec<(String, u64)>,
        /// Environment variables (e.g. KEY=VALUE)
        #[arg(short = 'e', long, value_parser = parse_env_var)]
        env: Vec<(String, String)>,
        /// Job dependencies (e.g. afterok:100,afterany:101)
        #[arg(short = 'd', long)]
        dependency: Option<String>,
        /// QoS level (Interactive, Production, Preemptible, Background)
        #[arg(short = 'q', long, default_value = "production", value_parser = parse_qos)]
        qos: veloce_common::QosLevel,
        /// Container image URI (e.g. s3://bucket/image.sif)
        #[arg(long)]
        image: Option<String>,
        /// Enable VNC interactive graphical session
        #[arg(long)]
        vnc: bool,
        /// Executable plus arguments. Veloce options must come before the executable.
        #[arg(value_name = "COMMAND", num_args = 0.., trailing_var_arg = true, allow_hyphen_values = true)]
        command: Vec<String>,
    },
    /// Submit a job step within an existing allocation
    Step {
        /// Number of nodes to request
        #[arg(short, long, default_value_t = 1)]
        nodes: usize,
        /// Cores per node
        #[arg(short = 'c', long, default_value_t = 1)]
        cores: u32,
        /// Number of total tasks to launch (MPI ranks)
        #[arg(long, default_value_t = 1)]
        ntasks: u32,
        /// Specific Parent Job ID. If not provided, reads VELOCE_JOB_ID from environment.
        #[arg(long)]
        job_id: Option<u64>,
        /// Directory containing input files (will be compressed and uploaded)
        #[arg(long = "input-deck", alias = "input-dir", alias = "input_dir")]
        input_dir: Option<String>,
        /// Generic resources requested (e.g. gpu:1)
        #[arg(long, value_parser = parse_gres_req)]
        gres: Vec<(String, u64)>,
        /// Environment variables (e.g. KEY=VALUE)
        #[arg(short = 'e', long, value_parser = parse_env_var)]
        env: Vec<(String, String)>,
        /// Wait for the step to finish
        #[arg(long)]
        wait: bool,
        /// Follow the parent job log while waiting for the step
        #[arg(short, long)]
        follow: bool,
        /// Parent job log type to follow when --follow is set: stdout or stderr
        #[arg(long, default_value = "stdout")]
        log_type: String,
        /// Executable plus arguments. Veloce options must come before the executable.
        #[arg(value_name = "COMMAND", num_args = 1.., trailing_var_arg = true, allow_hyphen_values = true)]
        command: Vec<String>,
    },
    /// Manage jobs
    Jobs {
        #[command(subcommand)]
        command: JobCommands,
    },
    /// Manage worker nodes
    Nodes {
        #[command(subcommand)]
        command: NodeCommands,
    },
    /// Manage advance reservations
    Reservations {
        #[command(subcommand)]
        command: ReservationCommands,
    },
    /// Diagnose controller, authentication, RBAC, and fileserver reachability
    #[command(alias = "ping")]
    Doctor,
    /// Generate shell completion scripts to stdout
    Completions {
        /// Target shell
        #[arg(value_enum)]
        shell: Shell,
    },
    /// List connected worker nodes
    #[command(hide = true)]
    ListNodes,
    /// List all jobs
    #[command(hide = true)]
    ListJobs {
        /// Filter by state: pending, running, completed, failed, killed
        #[arg(long)]
        state: Option<String>,
        /// Filter by submitting user
        #[arg(long = "user", value_name = "USER")]
        user: Option<String>,
        /// Filter to jobs submitted by the current OS user
        #[arg(long)]
        mine: bool,
    },
    /// Kill a job
    #[command(hide = true)]
    KillJob { job_id: u64 },
    /// Fetch job logs
    #[command(hide = true)]
    Logs {
        job_id: u64,
        /// Log type: stdout or stderr
        #[arg(default_value = "stdout")]
        log_type: String,
        /// Worker/rank index for multi-node job logs
        #[arg(long)]
        rank: Option<usize>,
        /// Follow log output
        #[arg(short, long)]
        follow: bool,
    },
    /// Query job history
    History {
        /// Specific Job ID
        #[arg(short, long)]
        id: Option<u64>,
        /// Range of Job IDs (e.g., "10-20")
        #[arg(short, long)]
        range: Option<String>,
    },
    /// Query cluster metrics history
    Metrics {
        /// Comma-separated list of node IDs
        #[arg(long)]
        nodes: Option<String>,
        /// Start timestamp (Unix epoch)
        #[arg(long)]
        start: Option<u64>,
        /// End timestamp (Unix epoch)
        #[arg(long)]
        end: Option<u64>,
        /// Aggregate results
        #[arg(short, long)]
        aggregate: bool,
    },
    /// Reserve nodes for a specific user and time window
    #[command(hide = true)]
    Reserve {
        /// Comma-separated list of nodes to reserve
        #[arg(short, long)]
        nodes: String,
        /// Start time of the reservation (RFC3339 format, e.g. "2026-05-18T12:00:00Z" or relative seconds e.g. "+3600")
        #[arg(short, long)]
        start: String,
        /// End time of the reservation (RFC3339 format, e.g. "2026-05-18T14:00:00Z" or relative seconds e.g. "+7200")
        #[arg(short, long)]
        end: String,
        /// Owner of the reservation
        #[arg(short, long)]
        owner: String,
    },
    /// List all reservations
    #[command(hide = true)]
    ListReservations,
    /// Delete a reservation
    #[command(hide = true)]
    DeleteReservation {
        /// ID of the reservation to delete
        id: String,
    },
    /// Manage Apptainer Container Registry
    Containers {
        #[command(subcommand)]
        command: ContainerCommands,
    },
}

#[derive(Subcommand, Clone)]
enum JobCommands {
    /// List all jobs
    List {
        /// Filter by state: pending, running, completed, failed, killed
        #[arg(long)]
        state: Option<String>,
        /// Filter by submitting user
        #[arg(long = "user", value_name = "USER")]
        user: Option<String>,
        /// Filter to jobs submitted by the current OS user
        #[arg(long)]
        mine: bool,
    },
    /// Show detailed information for one job
    #[command(alias = "describe")]
    Show { job_id: String },
    /// Show domain events for one job
    Events { job_id: u64 },
    /// Submit a fresh job using a previous job's specification
    #[command(alias = "clone")]
    Resubmit {
        job_id: u64,
        /// Override the copied job name
        #[arg(long = "name", value_name = "NAME")]
        job_name: Option<String>,
        /// Override the copied job comment
        #[arg(long = "comment", value_name = "COMMENT")]
        job_comment: Option<String>,
        /// Override the copied submitter identity
        #[arg(long = "user", value_name = "USER")]
        user_id: Option<String>,
    },
    /// Fetch job logs
    Logs {
        job_id: String,
        /// Log type: stdout or stderr
        #[arg(default_value = "stdout")]
        log_type: String,
        /// Worker/rank index for multi-node job logs
        #[arg(long)]
        rank: Option<usize>,
        /// Follow log output
        #[arg(short, long)]
        follow: bool,
    },
    /// Kill a job
    Kill { job_id: String },
}

#[derive(Subcommand, Clone)]
enum NodeCommands {
    /// List connected worker nodes
    List,
}

#[derive(Subcommand, Clone)]
enum ReservationCommands {
    /// Create a reservation
    Create {
        /// Comma-separated list of nodes to reserve
        #[arg(short, long)]
        nodes: String,
        /// Start time of the reservation (RFC3339 format, e.g. "2026-05-18T12:00:00Z" or relative seconds e.g. "+3600")
        #[arg(short, long)]
        start: String,
        /// End time of the reservation (RFC3339 format, e.g. "2026-05-18T14:00:00Z" or relative seconds e.g. "+7200")
        #[arg(short, long)]
        end: String,
        /// Owner of the reservation
        #[arg(short, long)]
        owner: String,
    },
    /// List all reservations
    List,
    /// Delete a reservation
    Delete {
        /// ID of the reservation to delete
        id: String,
    },
}

#[derive(Subcommand, Clone)]
enum ContainerCommands {
    /// List all registered containers
    List,
    /// Register a new container image
    Register {
        /// Name of the container
        name: String,
        /// Path to the .sif image file
        sif_path: String,
        /// Path to the .json manifest file
        manifest_path: String,
    },
    /// Delete a container
    Delete {
        /// Name of the container to delete
        name: String,
    },
}

fn format_job_status(status: &JobStatus) -> String {
    let s = match status {
        JobStatus::Pending => "Pending".to_string(),
        JobStatus::Running => "Running".to_string(),
        JobStatus::Completed(code) => format!("Completed({})", code),
        JobStatus::Failed(err) => format!("Failed({})", err),
        JobStatus::Killed => "Cancelled".to_string(),
    };
    // Ensure the status string doesn't exceed a certain length for alignment
    if s.len() > 22 {
        format!("{}...", &s[..19]) // Truncate and add "..."
    } else {
        s
    }
}

fn format_time(ts: u64) -> String {
    if ts == 0 {
        return "-".to_string();
    }
    match Local.timestamp_opt(ts as i64, 0) {
        chrono::LocalResult::Single(dt) => dt.format("%Y-%m-%d %H:%M:%S").to_string(),
        _ => ts.to_string(),
    }
}

fn format_opt_time(ts: Option<u64>) -> String {
    ts.map(format_time).unwrap_or_else(|| "-".to_string())
}

fn format_duration_secs(seconds: u64) -> String {
    if seconds == 0 {
        "-".to_string()
    } else if seconds < 60 {
        format!("{}s", seconds)
    } else if seconds < 3600 {
        format!("{}m {}s", seconds / 60, seconds % 60)
    } else {
        format!(
            "{}h {}m {}s",
            seconds / 3600,
            (seconds % 3600) / 60,
            seconds % 60
        )
    }
}

fn format_string_list(values: &[String]) -> String {
    if values.is_empty() {
        "-".to_string()
    } else {
        values.join(", ")
    }
}

fn format_gres_req(gres: &std::collections::BTreeMap<String, u64>) -> String {
    if gres.is_empty() {
        "-".to_string()
    } else {
        gres.iter()
            .map(|(name, count)| format!("{}:{}", name, count))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

fn default_submitter_identity() -> Result<String> {
    for key in ["USER", "LOGNAME", "USERNAME"] {
        if let Ok(value) = std::env::var(key) {
            let trimmed = value.trim();
            if !trimmed.is_empty() {
                return Ok(trimmed.to_string());
            }
        }
    }

    anyhow::bail!("Unable to determine current user; pass --user explicitly")
}

fn normalize_optional_text(value: Option<String>, field_name: &str) -> Result<Option<String>> {
    if let Some(value) = value {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            anyhow::bail!("{} cannot be empty", field_name);
        }
        Ok(Some(trimmed.to_string()))
    } else {
        Ok(None)
    }
}

fn normalize_optional_user(value: Option<String>) -> Result<Option<String>> {
    if let Some(value) = value {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            anyhow::bail!("--user cannot be empty");
        }
        Ok(Some(trimmed.to_string()))
    } else {
        Ok(None)
    }
}

fn parse_history_command_line(command_line: &str) -> Result<(String, Vec<String>)> {
    let parts = shell_words::split(command_line)
        .with_context(|| format!("Failed to parse historical command line: {}", command_line))?;
    let (binary, args) = parts
        .split_first()
        .ok_or_else(|| anyhow::anyhow!("Historical job has an empty command line"))?;
    Ok((binary.clone(), args.to_vec()))
}

fn split_command(command: Vec<String>, context: &str) -> Result<(String, Vec<String>)> {
    let (binary, args) = command
        .split_first()
        .ok_or_else(|| anyhow::anyhow!("{context} executable is required"))?;
    Ok((binary.clone(), args.to_vec()))
}

fn print_job_info(job: &veloce_common::JobInfo) {
    let command = if job.args.is_empty() {
        job.binary.clone()
    } else {
        format!("{} {}", job.binary, job.args.join(" "))
    };
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let elapsed = job
        .start_time
        .map(|start| job.end_time.unwrap_or(now).saturating_sub(start))
        .unwrap_or(0);

    println!("Job {}", job.id);
    println!("  Name: {}", job.job_name.as_deref().unwrap_or("-"));
    println!("  Comment: {}", job.job_comment.as_deref().unwrap_or("-"));
    println!("  Status: {}", format_job_status(&job.status));
    println!("  Reason: {}", job.reason.as_deref().unwrap_or("-"));
    println!("  User: {}", job.user_id);
    println!("  Command: {}", command);
    println!(
        "  Resources: {} node(s), {} core(s)/node, {} MB/node",
        job.req_nodes, job.req_cores, job.req_memory
    );
    println!("  QoS: {:?}", job.qos);
    println!("  Priority: {}", job.priority);
    println!("  GRES: {}", format_gres_req(&job.gres_req));
    println!("  Workers: {}", format_string_list(&job.assigned_workers));
    println!("  Working directory: {}", job.working_directory);
    println!("  Queued: {}", format_time(job.queued_time));
    println!("  Started: {}", format_opt_time(job.start_time));
    println!("  Ended: {}", format_opt_time(job.end_time));
    println!("  Elapsed: {}", format_duration_secs(elapsed));
    println!(
        "  Walltime: {}",
        if job.walltime == 0 {
            "unlimited".to_string()
        } else {
            format_duration_secs(job.walltime)
        }
    );
    if let (Some(array_id), Some(task_id)) = (job.array_id, job.array_task_id) {
        println!("  Array: {} task {}", array_id, task_id);
    }
    println!(
        "  Dependencies: {}",
        job.dependency_specs
            .as_ref()
            .map(|deps| format_string_list(deps))
            .unwrap_or_else(|| "-".to_string())
    );
    println!(
        "  Container: {}",
        job.container_asset
            .as_ref()
            .map(|asset| format!("{} ({})", asset.name, asset.image_uri))
            .unwrap_or_else(|| "-".to_string())
    );
    println!(
        "  Cgroup isolation: {}",
        if job.cgroup_active { "yes" } else { "no" }
    );
    println!(
        "  VNC: {}",
        if job.vnc_enabled {
            "enabled"
        } else {
            "disabled"
        }
    );
    println!(
        "  Stdout file: {}",
        job.stdout_file_id.as_deref().unwrap_or("-")
    );
    println!(
        "  Stderr file: {}",
        job.stderr_file_id.as_deref().unwrap_or("-")
    );
    println!(
        "  Workdir file: {}",
        job.workdir_file_id.as_deref().unwrap_or("-")
    );
}

fn print_job_usage(job: &veloce_common::JobUsage) {
    let elapsed = job
        .start_time
        .map(|start| job.end_time.unwrap_or(start).saturating_sub(start))
        .unwrap_or(0);

    println!("Job {} (history)", job.job_id);
    println!("  Name: {}", job.job_name.as_deref().unwrap_or("-"));
    println!("  Comment: {}", job.job_comment.as_deref().unwrap_or("-"));
    println!("  Status: {}", format_job_status(&job.status));
    println!(
        "  Exit code: {}",
        job.exit_code
            .map(|code| code.to_string())
            .unwrap_or_else(|| "-".to_string())
    );
    println!("  User: {}", job.user_id);
    println!("  Command: {}", job.command_line);
    println!(
        "  Resources: {} node(s), {} core(s)/node, {} MB/node",
        job.req_nodes, job.req_cores, job.req_memory
    );
    println!("  QoS: {:?}", job.qos);
    println!("  GRES: {}", format_gres_req(&job.gres_req));
    println!("  Workers: {}", format_string_list(&job.assigned_workers));
    println!("  Submitted: {}", format_time(job.submission_time));
    println!("  Started: {}", format_opt_time(job.start_time));
    println!("  Ended: {}", format_opt_time(job.end_time));
    println!("  Elapsed: {}", format_duration_secs(elapsed));
    println!("  CPU time: {} ms", job.cpu_time_ms);
    println!("  Max memory: {} MB", job.max_memory_bytes / 1024 / 1024);
    if let (Some(array_id), Some(task_id)) = (job.array_id, job.array_task_id) {
        println!("  Array: {} task {}", array_id, task_id);
    }
    println!(
        "  Dependencies: {}",
        job.dependency_specs
            .as_ref()
            .map(|deps| format_string_list(deps))
            .unwrap_or_else(|| "-".to_string())
    );
    println!(
        "  Container: {}",
        job.container_asset
            .as_ref()
            .map(|asset| format!("{} ({})", asset.name, asset.image_uri))
            .unwrap_or_else(|| "-".to_string())
    );
    println!(
        "  Cgroup isolation: {}",
        if job.cgroup_active { "yes" } else { "no" }
    );
    println!(
        "  Stdout file: {}",
        job.stdout_file_id.as_deref().unwrap_or("-")
    );
    println!(
        "  Stderr file: {}",
        job.stderr_file_id.as_deref().unwrap_or("-")
    );
    println!(
        "  Workdir file: {}",
        job.workdir_file_id.as_deref().unwrap_or("-")
    );
}

fn print_job_events(events: &[veloce_common::JobEvent]) -> Result<()> {
    if events.is_empty() {
        println!("No events recorded.");
        return Ok(());
    }

    println!(
        "{:<20} {:<10} {:<18} {}",
        "TIME", "SEVERITY", "TYPE", "MESSAGE"
    );
    for event in events {
        let metadata = if event.metadata.is_empty() {
            String::new()
        } else {
            format!(" ({})", serde_json::to_string(&event.metadata)?)
        };
        println!(
            "{:<20} {:<10} {:<18} {}{}",
            format_time(event.timestamp),
            event.severity,
            event.event_type,
            event.message,
            metadata
        );
    }

    Ok(())
}

fn parse_array_range(array_str: &str) -> Result<Vec<u32>> {
    let mut indices = Vec::new();
    for part in array_str.split(',') {
        if part.contains('-') {
            let inner_parts: Vec<&str> = part.split(':').collect();
            let mut step = 1;
            if inner_parts.len() > 1 {
                step = inner_parts[1].parse().context("Invalid array step size")?;
            }
            let range_parts: Vec<&str> = inner_parts[0].split('-').collect();
            if range_parts.len() != 2 {
                anyhow::bail!("Invalid array range: {}", inner_parts[0]);
            }
            let start: u32 = range_parts[0].parse().context("Invalid array start")?;
            let end: u32 = range_parts[1].parse().context("Invalid array end")?;
            for i in (start..=end).step_by(step as usize) {
                indices.push(i);
            }
        } else {
            let val: u32 = part
                .parse()
                .context("Invalid array discrete single value")?;
            indices.push(val);
        }
    }
    indices.sort_unstable();
    indices.dedup();
    if indices.is_empty() {
        anyhow::bail!("Array range is empty");
    }
    if indices.len() > 10000 {
        anyhow::bail!("Array range is too large (max 10000 tasks)");
    }
    Ok(indices)
}

fn parse_gres_req(s: &str) -> Result<(String, u64), String> {
    let parts: Vec<&str> = s.split(':').collect();
    if parts.len() != 2 {
        return Err("GRES must be in format name:count".to_string());
    }
    let count = parts[1]
        .parse()
        .map_err(|e| format!("Invalid count: {}", e))?;
    Ok((parts[0].to_string(), count))
}

fn parse_env_var(s: &str) -> Result<(String, String), String> {
    let parts: Vec<&str> = s.splitn(2, '=').collect();
    if parts.len() != 2 {
        return Err("Environment variables must be in format KEY=VALUE".to_string());
    }
    Ok((parts[0].to_string(), parts[1].to_string()))
}

fn parse_qos(s: &str) -> Result<veloce_common::QosLevel, String> {
    match s.to_lowercase().as_str() {
        "interactive" => Ok(veloce_common::QosLevel::Interactive),
        "production" => Ok(veloce_common::QosLevel::Production),
        "preemptible" => Ok(veloce_common::QosLevel::Preemptible),
        "background" => Ok(veloce_common::QosLevel::Background),
        _ => Err(format!(
            "Invalid QoS level '{}'. Expected interactive, production, preemptible, background",
            s
        )),
    }
}

fn compress_dir(path: &std::path::Path) -> Result<std::path::PathBuf> {
    let tmp_dir = std::env::temp_dir();
    let archive_path = tmp_dir.join(format!("veloce_input_{}.tar.gz", uuid::Uuid::new_v4()));

    let file = std::fs::File::create(&archive_path)?;
    let enc = flate2::write::GzEncoder::new(file, flate2::Compression::default());
    let mut tar = tar::Builder::new(enc);

    // We want to archive the contents of the directory, not the directory itself
    tar.append_dir_all(".", path)?;
    tar.finish()?;

    Ok(archive_path)
}

#[derive(Debug, Default, Deserialize)]
struct CliConfig {
    controller: Option<String>,
    controllers: Option<String>,
    secret: Option<String>,
    fileserver: Option<String>,
    fileserver_key: Option<String>,
    api_key: Option<String>,
    controller_api: Option<String>,
    client_id: Option<String>,
    client_token: Option<String>,
}

fn cli_config_candidates() -> Vec<PathBuf> {
    if let Ok(path) = std::env::var("VELOCE_CONFIG") {
        let trimmed = path.trim();
        if !trimmed.is_empty() {
            return vec![PathBuf::from(trimmed)];
        }
    }

    let mut candidates = Vec::new();
    if let Ok(xdg_config_home) = std::env::var("XDG_CONFIG_HOME") {
        let trimmed = xdg_config_home.trim();
        if !trimmed.is_empty() {
            candidates.push(PathBuf::from(trimmed).join("veloce").join("config.toml"));
        }
    }
    if let Ok(home) = std::env::var("HOME") {
        let trimmed = home.trim();
        if !trimmed.is_empty() {
            candidates.push(PathBuf::from(trimmed).join(".veloce").join("config.toml"));
        }
    }

    candidates
}

fn load_cli_config() -> Result<Option<(PathBuf, CliConfig)>> {
    for path in cli_config_candidates() {
        if path.exists() {
            let contents = std::fs::read_to_string(&path)
                .with_context(|| format!("Failed to read CLI config {}", path.display()))?;
            let config = toml::from_str::<CliConfig>(&contents)
                .with_context(|| format!("Failed to parse CLI config {}", path.display()))?;
            return Ok(Some((path, config)));
        }
    }

    Ok(None)
}

fn set_env_from_config(env_key: &str, value: Option<&String>) {
    if std::env::var_os(env_key).is_some() {
        return;
    }

    if let Some(value) = value {
        let trimmed = value.trim();
        if !trimmed.is_empty() {
            std::env::set_var(env_key, trimmed);
        }
    }
}

fn apply_cli_config() -> Result<Option<PathBuf>> {
    let Some((path, config)) = load_cli_config()? else {
        return Ok(None);
    };

    set_env_from_config("VELOCE_CONTROLLER", config.controller.as_ref());
    set_env_from_config("VELOCE_CONTROLLERS", config.controllers.as_ref());
    set_env_from_config("VELOCE_SECRET", config.secret.as_ref());
    set_env_from_config("VELOCE_FILESERVER", config.fileserver.as_ref());
    set_env_from_config("VELOCE_FILESERVER_KEY", config.fileserver_key.as_ref());
    set_env_from_config("VELOCE_API_KEY", config.api_key.as_ref());
    set_env_from_config("VELOCE_CONTROLLER_API", config.controller_api.as_ref());
    set_env_from_config("VELOCE_CLIENT_ID", config.client_id.as_ref());
    set_env_from_config("VELOCE_CLIENT_TOKEN", config.client_token.as_ref());

    Ok(Some(path))
}

fn should_skip_cli_config() -> bool {
    std::env::args_os().skip(1).any(|arg| {
        matches!(
            arg.to_str(),
            Some("-h") | Some("--help") | Some("-V") | Some("--version")
        )
    })
}

type ClientFramed = Framed<noise::NoiseStream<TcpStream>, MessageCodec>;

#[derive(Debug, Clone, Serialize)]
struct DiagnosticCheck {
    name: String,
    ok: bool,
    detail: String,
}

#[derive(Debug, Serialize)]
struct DoctorReport {
    ok: bool,
    connected_controller: Option<String>,
    worker_count: Option<usize>,
    checks: Vec<DiagnosticCheck>,
    hints: Vec<String>,
}

fn diagnostic_check(
    name: impl Into<String>,
    ok: bool,
    detail: impl Into<String>,
) -> DiagnosticCheck {
    DiagnosticCheck {
        name: name.into(),
        ok,
        detail: detail.into(),
    }
}

fn diagnostic_hints(checks: &[DiagnosticCheck]) -> Vec<String> {
    let mut hints = Vec::new();

    if checks
        .iter()
        .any(|check| check.name.ends_with(":tcp") && !check.ok)
    {
        hints.push(
            "Verify --controller/VELOCE_CONTROLLERS, controller process status, and network/firewall reachability.".to_string(),
        );
    }
    if checks
        .iter()
        .any(|check| check.name.ends_with(":noise") && !check.ok)
    {
        hints.push("Verify VELOCE_SECRET matches the controller cluster secret.".to_string());
    }
    if checks.iter().any(|check| {
        check.name.ends_with(":hello") && !check.ok && check.detail.to_lowercase().contains("token")
    }) {
        hints.push(
            "Set VELOCE_CLIENT_ID and VELOCE_CLIENT_TOKEN when client token enforcement is enabled."
                .to_string(),
        );
    }
    if checks
        .iter()
        .any(|check| check.name == "controller-read-probe" && !check.ok)
    {
        hints.push(
            "Ensure the client token has viewer, submitter, or operator role for read-only diagnostics."
                .to_string(),
        );
    }
    if checks
        .iter()
        .any(|check| check.name == "fileserver-reachability" && !check.ok)
    {
        hints.push(
            "Verify VELOCE_FILESERVER, fileserver process health, and TLS/certificate configuration."
                .to_string(),
        );
    }

    hints
}

fn print_connection_failure(controller_list: &[String], checks: &[DiagnosticCheck]) {
    eprintln!(
        "Unable to connect to a Veloce controller from: {}",
        controller_list.join(", ")
    );

    let failed_checks: Vec<&DiagnosticCheck> = checks.iter().filter(|check| !check.ok).collect();
    if !failed_checks.is_empty() {
        eprintln!("Connection attempts:");
        for check in failed_checks {
            eprintln!("  - {}: {}", check.name, check.detail);
        }
    }

    let hints = diagnostic_hints(checks);
    if !hints.is_empty() {
        eprintln!("Connection hints:");
        for hint in hints {
            eprintln!("  - {}", hint);
        }
    }

    eprintln!("Run `veloce doctor` for a full sanitized connectivity report.");
}

fn parse_controller_list(cli: &Cli) -> Result<Vec<String>> {
    let raw_controllers = cli
        .controllers
        .as_deref()
        .unwrap_or(cli.controller.as_str());
    let controllers: Vec<String> = raw_controllers
        .split(',')
        .map(str::trim)
        .filter(|addr| !addr.is_empty())
        .map(ToString::to_string)
        .collect();

    if controllers.is_empty() {
        anyhow::bail!("No controller addresses configured");
    }

    Ok(controllers)
}

async fn connect_to_controller(
    cli: &Cli,
    controller_list: &[String],
    show_redirects: bool,
) -> (Option<(ClientFramed, String)>, Vec<DiagnosticCheck>) {
    let mut checks = Vec::new();
    let mut current_controllers = controller_list.to_vec();
    let mut attempt_idx = 0;

    while attempt_idx < current_controllers.len() {
        let addr = current_controllers[attempt_idx].clone();
        debug!("Attempting to connect to controller at {}...", addr);

        let stream = match TcpStream::connect(&addr).await {
            Ok(stream) => {
                checks.push(diagnostic_check(
                    format!("{}:tcp", addr),
                    true,
                    "TCP connection established",
                ));
                stream
            }
            Err(e) => {
                checks.push(diagnostic_check(
                    format!("{}:tcp", addr),
                    false,
                    format!("TCP connection failed: {}", e),
                ));
                attempt_idx += 1;
                continue;
            }
        };

        let noise_stream = match noise::upgrade_initiator(stream, &cli.secret).await {
            Ok(noise_stream) => {
                checks.push(diagnostic_check(
                    format!("{}:noise", addr),
                    true,
                    "Noise handshake completed",
                ));
                noise_stream
            }
            Err(e) => {
                checks.push(diagnostic_check(
                    format!("{}:noise", addr),
                    false,
                    format!("Noise handshake failed: {}", e),
                ));
                attempt_idx += 1;
                continue;
            }
        };

        let mut framed = Framed::new(noise_stream, MessageCodec::new());
        let hello = Message::HelloClient {
            client_id: cli.client_id.clone(),
            registration_token: cli.client_token.clone(),
        };

        if let Err(e) = framed.send(hello).await {
            checks.push(diagnostic_check(
                format!("{}:hello", addr),
                false,
                format!("Failed to send client hello: {}", e),
            ));
            attempt_idx += 1;
            continue;
        }

        match framed.next().await {
            Some(Ok(Message::LeaderRedirect { leader_addr })) if !leader_addr.is_empty() => {
                checks.push(diagnostic_check(
                    format!("{}:redirect", addr),
                    true,
                    format!("Redirected to leader at {}", leader_addr),
                ));
                if !current_controllers.contains(&leader_addr) {
                    if show_redirects {
                        eprintln!("Info: Redirected to leader at {}", leader_addr);
                    }
                    current_controllers.push(leader_addr);
                }
            }
            Some(Ok(Message::LeaderRedirect { .. })) => {
                checks.push(diagnostic_check(
                    format!("{}:redirect", addr),
                    false,
                    "Controller reported follower state but did not provide a leader address",
                ));
            }
            Some(Ok(Message::Error(e))) if e == "Not a leader" => {
                checks.push(diagnostic_check(
                    format!("{}:hello", addr),
                    false,
                    "Controller is not the leader",
                ));
            }
            Some(Ok(Message::Error(e))) => {
                checks.push(diagnostic_check(format!("{}:hello", addr), false, e));
            }
            Some(Ok(Message::Ack)) => {
                checks.push(diagnostic_check(
                    format!("{}:hello", addr),
                    true,
                    "Client registration accepted",
                ));
                return (Some((framed, addr)), checks);
            }
            Some(Ok(_msg)) => {
                checks.push(diagnostic_check(
                    format!("{}:hello", addr),
                    true,
                    "Connected; controller sent a non-ack hello response",
                ));
                return (Some((framed, addr)), checks);
            }
            Some(Err(e)) => {
                checks.push(diagnostic_check(
                    format!("{}:hello", addr),
                    false,
                    format!("Failed to read hello response: {}", e),
                ));
            }
            None => {
                checks.push(diagnostic_check(
                    format!("{}:hello", addr),
                    false,
                    "Controller closed connection during client hello",
                ));
            }
        }

        attempt_idx += 1;
    }

    (None, checks)
}

async fn check_fileserver(fileserver: &str) -> DiagnosticCheck {
    let client = match reqwest::Client::builder()
        .danger_accept_invalid_certs(!cfg!(feature = "production"))
        .timeout(std::time::Duration::from_secs(5))
        .build()
    {
        Ok(client) => client,
        Err(e) => {
            return diagnostic_check(
                "fileserver-reachability",
                false,
                format!("Failed to build HTTP client: {}", e),
            )
        }
    };

    match client.get(fileserver).send().await {
        Ok(response) => {
            let status = response.status();
            diagnostic_check(
                "fileserver-reachability",
                !status.is_server_error(),
                format!("HTTP {}", status),
            )
        }
        Err(e) => diagnostic_check(
            "fileserver-reachability",
            false,
            format!("Request failed: {}", e),
        ),
    }
}

async fn run_doctor(cli: &Cli, controller_list: &[String]) -> Result<bool> {
    let (connected, mut checks) = connect_to_controller(cli, controller_list, false).await;
    let mut connected_controller = None;
    let mut worker_count = None;

    if let Some((mut framed, addr)) = connected {
        connected_controller = Some(addr);
        framed.send(Message::ListWorkers).await?;
        match framed.next().await {
            Some(Ok(Message::WorkerList(workers))) => {
                worker_count = Some(workers.len());
                checks.push(diagnostic_check(
                    "controller-read-probe",
                    true,
                    format!("Controller returned {} worker(s)", workers.len()),
                ));
            }
            Some(Ok(Message::Error(e))) => {
                checks.push(diagnostic_check("controller-read-probe", false, e));
            }
            Some(Ok(msg)) => {
                checks.push(diagnostic_check(
                    "controller-read-probe",
                    false,
                    format!("Unexpected response: {:?}", msg),
                ));
            }
            Some(Err(e)) => {
                checks.push(diagnostic_check(
                    "controller-read-probe",
                    false,
                    format!("Failed to read controller response: {}", e),
                ));
            }
            None => {
                checks.push(diagnostic_check(
                    "controller-read-probe",
                    false,
                    "Controller closed connection during read probe",
                ));
            }
        }

        let mut stream = framed.into_inner();
        let _ = stream.shutdown().await;
    }

    checks.push(check_fileserver(&cli.fileserver).await);
    let hints = diagnostic_hints(&checks);
    let ok = connected_controller.is_some() && checks.iter().all(|check| check.ok);
    let report = DoctorReport {
        ok,
        connected_controller,
        worker_count,
        checks,
        hints,
    };

    if cli.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        println!("Veloce doctor: {}", if report.ok { "OK" } else { "FAILED" });
        if let Some(addr) = &report.connected_controller {
            println!("Connected controller: {}", addr);
        }
        if let Some(count) = report.worker_count {
            println!("Workers visible: {}", count);
        }
        for check in &report.checks {
            println!(
                "  [{}] {} - {}",
                if check.ok { "ok" } else { "fail" },
                check.name,
                check.detail
            );
        }
        if !report.hints.is_empty() {
            println!("Hints:");
            for hint in &report.hints {
                println!("  - {}", hint);
            }
        }
    }

    Ok(report.ok)
}

#[tokio::main]
async fn main() -> Result<()> {
    if !should_skip_cli_config() {
        if let Some(path) = apply_cli_config()? {
            debug!("Loaded CLI config from {}", path.display());
        }
    }

    let cli = Cli::parse();

    if let Commands::Completions { shell } = &cli.command {
        let mut cmd = Cli::command();
        generate(*shell, &mut cmd, "veloce", &mut std::io::stdout());
        return Ok(());
    }

    let secret = cli.secret.clone();
    let fileserver_key = cli.fileserver_key.clone().unwrap_or_else(|| secret.clone());
    let controller_list = parse_controller_list(&cli)?;

    if matches!(&cli.command, Commands::Doctor) {
        let ok = run_doctor(&cli, &controller_list).await?;
        if !ok {
            std::process::exit(1);
        }
        return Ok(());
    }

    let (framed, checks) = connect_to_controller(&cli, &controller_list, true).await;
    let mut framed = if let Some((framed, _addr)) = framed {
        framed
    } else {
        for check in &checks {
            debug!("{}: {}", check.name, check.detail);
        }
        print_connection_failure(&controller_list, &checks);
        anyhow::bail!("Unable to connect to a Veloce controller");
    };

    match cli.command.clone() {
        Commands::Doctor => unreachable!("doctor is handled before controller command dispatch"),
        Commands::Completions { .. } => {
            unreachable!("completions is handled before controller command dispatch")
        }
        Commands::History { id, range } => {
            let filter = if let Some(job_id) = id {
                HistoryFilter::Single(job_id)
            } else if let Some(r) = range {
                let parts: Vec<&str> = r.split('-').collect();
                if parts.len() != 2 {
                    anyhow::bail!("Invalid range format. Use start-end (e.g. 10-20)");
                }
                let start = parts[0].parse()?;
                let end = parts[1].parse()?;
                HistoryFilter::Range(start, end)
            } else {
                HistoryFilter::All
            };

            framed.send(Message::GetJobHistory { filter }).await?;

            if let Some(res) = framed.next().await {
                match res? {
                    Message::JobHistoryResponse(mut history) => {
                        if cli.json {
                            for job in &mut history {
                                job.secret.clear();
                            }
                            println!("{}", serde_json::to_string_pretty(&history)?);
                        } else {
                            history.sort_by_key(|h| h.job_id);
                            println!(
                                "{:<5} {:<18} {:<20} {:<20} {:<10} {:<10} {:<15} {:<15} {:<40}",
                                "ID",
                                "Name",
                                "Submitted",
                                "Finished",
                                "CPU(ms)",
                                "Mem(MB)",
                                "Status",
                                "Code",
                                "Command"
                            );

                            for job in history {
                                let status_str = format_job_status(&job.status);
                                let code_str = job
                                    .exit_code
                                    .map(|c| c.to_string())
                                    .unwrap_or_else(|| "-".to_string());

                                let cmd_display = if job.command_line.len() > 37 {
                                    format!("{}...", &job.command_line[..37])
                                } else {
                                    job.command_line
                                };

                                let name_display = job
                                    .job_name
                                    .as_deref()
                                    .map(|name| {
                                        if name.len() > 15 {
                                            format!("{}...", &name[..15])
                                        } else {
                                            name.to_string()
                                        }
                                    })
                                    .unwrap_or_else(|| "-".to_string());

                                println!(
                                    "{:<5} {:<18} {:<20} {:<20} {:<10} {:<10} {:<15} {:<15} {:<40}",
                                    job.job_id,
                                    name_display,
                                    format_time(job.submission_time),
                                    job.end_time.map(format_time).unwrap_or("-".to_string()),
                                    job.cpu_time_ms,
                                    job.max_memory_bytes / 1024 / 1024,
                                    status_str,
                                    code_str,
                                    cmd_display
                                );
                            }
                        }
                    }
                    Message::Error(e) => eprintln!("Error: {}", e),
                    _ => eprintln!("Unexpected response"),
                }
            }
        }
        Commands::Metrics {
            nodes,
            start,
            end,
            aggregate,
        } => {
            let node_list = nodes.map(|s| s.split(',').map(|s| s.trim().to_string()).collect());

            framed
                .send(Message::GetMetrics {
                    start_time: start,
                    end_time: end,
                    nodes: node_list,
                    aggregate,
                })
                .await?;

            if let Some(res) = framed.next().await {
                match res? {
                    Message::MetricsData(metrics) => {
                        if cli.json {
                            println!("{}", serde_json::to_string_pretty(&metrics)?);
                        } else {
                            println!(
                                "{:<10} {:<20} {:<5} {:<8} {:<10} {:<10} {:<10}",
                                "Node", "Time", "Jobs", "CPU%", "Mem(MB)", "Rx(KB)", "Tx(KB)"
                            );

                            for m in metrics {
                                let mem_mb = m.memory_usage / 1024 / 1024;
                                let rx_kb = m.net_rx_rate as u64 / 1024;
                                let tx_kb = m.net_tx_rate as u64 / 1024;
                                let short_id = if m.node_id.len() > 8 {
                                    &m.node_id[..8]
                                } else {
                                    &m.node_id
                                };

                                println!(
                                    "{:<10} {:<20} {:<5} {:<8.2} {:<10} {:<10} {:<10}",
                                    short_id,
                                    format_time(m.timestamp),
                                    m.running_jobs,
                                    m.cpu_load,
                                    mem_mb,
                                    rx_kb,
                                    tx_kb
                                );
                            }
                        }
                    }
                    Message::Error(e) => eprintln!("Error: {}", e),
                    _ => eprintln!("Unexpected response"),
                }
            }
        }
        Commands::Submit {
            job_name,
            job_comment,
            cwl,
            nodes,
            cores,
            mem,
            walltime,
            estimated_walltime,
            priority,
            as_user,
            array,
            wait,
            input_dir,
            gres,
            env,
            dependency,
            qos,
            image,
            vnc,
            command,
        } => {
            let user_id = match as_user {
                Some(user) if !user.trim().is_empty() => user,
                Some(_) => anyhow::bail!("--user cannot be empty"),
                None => default_submitter_identity()?,
            };
            let job_name = normalize_optional_text(job_name, "--name")?;
            let job_comment = normalize_optional_text(job_comment, "--comment")?;
            let working_directory = std::env::var("VELOCE_WORKDIR").unwrap_or_else(|_| {
                std::env::current_dir()
                    .unwrap_or_else(|_| std::path::PathBuf::from("/"))
                    .to_string_lossy()
                    .into_owned()
            });

            if let Some(cwl_file) = cwl {
                let content = std::fs::read_to_string(&cwl_file)
                    .with_context(|| format!("Failed to read CWL file: {}", cwl_file))?;

                let dag =
                    veloce_common::cwl::parse_cwl_to_dag(&content, &user_id, &working_directory)
                        .context("Failed to parse CWL workflow")?;

                framed.send(Message::SubmitDag(dag)).await?;

                if let Some(res) = framed.next().await {
                    match res? {
                        Message::DagSubmitted {
                            base_job_id,
                            task_count,
                        } => {
                            if cli.json {
                                println!(
                                    "{}",
                                    serde_json::json!({
                                        "type": "dag",
                                        "base_job_id": base_job_id,
                                        "task_count": task_count
                                    })
                                );
                            } else {
                                println!("CWL DAG submitted successfully.");
                                println!("Base Job ID: {}", base_job_id);
                                println!("Tasks in DAG: {}", task_count);
                            }
                        }
                        Message::Error(e) => {
                            eprintln!("Error submitting DAG: {}", e);
                            std::process::exit(1);
                        }
                        _ => {
                            eprintln!("Unexpected response from controller");
                            std::process::exit(1);
                        }
                    }
                }
                return Ok(());
            }

            let (binary, args) = split_command(command, "Submit")?;

            let array_indices = if let Some(arr_str) = array {
                Some(parse_array_range(&arr_str)?)
            } else {
                None
            };

            let (dependencies, dependency_specs) = if let Some(ref dep_str) = dependency {
                let mut specs = Vec::new();
                let mut ids = Vec::new();
                for part in dep_str.split(',') {
                    let trimmed = part.trim();
                    if !trimmed.is_empty() {
                        specs.push(trimmed.to_string());
                        if let Ok(spec) = veloce_common::DependencySpec::parse(trimmed) {
                            ids.push(spec.parent_id);
                        }
                    }
                }
                (
                    if ids.is_empty() { None } else { Some(ids) },
                    if specs.is_empty() { None } else { Some(specs) },
                )
            } else {
                (None, None)
            };

            let mut inputs = Vec::new();
            if let Some(dir) = input_dir {
                let path = std::path::Path::new(&dir);
                if !path.exists() {
                    anyhow::bail!("Input directory does not exist: {}", dir);
                }
                if cli.json {
                    eprintln!("Compressing input directory: {}...", dir);
                } else {
                    println!("Compressing input directory: {}...", dir);
                }
                let archive = compress_dir(path)?;

                if cli.json {
                    eprintln!("Uploading to fileserver: {}...", cli.fileserver);
                } else {
                    println!("Uploading to fileserver: {}...", cli.fileserver);
                }
                let file_client = veloce_common::file_client::FileClient::new(
                    cli.fileserver.clone(),
                    fileserver_key.clone(),
                );
                let handle = file_client.upload_file(&archive, false, true).await?;
                inputs.push(handle);

                // Cleanup local temp archive
                let _ = std::fs::remove_file(archive);
            }

            let gres_req: std::collections::BTreeMap<String, u64> = gres.into_iter().collect();

            framed
                .send(Message::Submit {
                    job_name,
                    job_comment,
                    binary,
                    args,
                    req_nodes: nodes,
                    req_cores: cores,
                    req_memory: mem,
                    walltime,
                    priority,
                    user_id,
                    working_directory,
                    array_indices,
                    inputs,
                    gres_req,
                    env_vars: env,
                    wait_for_licenses: false,
                    estimated_walltime,
                    priority_offset: None,
                    dependencies,
                    dependency_specs,
                    qos,
                    image_uri: image,
                    vnc_enabled: vnc,
                    inherit_host_env: false,
                    env_allowlist: None,
                    job_profile: None,
                })
                .await?;
            if let Some(res) = framed.next().await {
                match res? {
                    Message::JobId { job_id } => {
                        if !wait && cli.json {
                            println!(
                                "{}",
                                serde_json::json!({
                                    "type": "job",
                                    "job_id": job_id
                                })
                            );
                        } else if !cli.json {
                            println!("Job submitted successfully. ID: {}", job_id);
                        }
                        if wait {
                            if !cli.json {
                                println!("Waiting for job {} to complete...", job_id);
                            }
                            loop {
                                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

                                // Check active jobs
                                framed
                                    .send(Message::ListJobs { state_filter: None })
                                    .await?;
                                let mut found = false;

                                // We need to consume the response
                                if let Some(res) = framed.next().await {
                                    match res? {
                                        Message::JobList(jobs) => {
                                            if let Some(job) = jobs.iter().find(|j| j.id == job_id)
                                            {
                                                found = true;
                                                match job.status {
                                                    JobStatus::Completed(code) => {
                                                        if cli.json {
                                                            println!(
                                                                "{}",
                                                                serde_json::json!({
                                                                    "type": "job",
                                                                    "job_id": job_id,
                                                                    "status": "completed",
                                                                    "exit_code": code
                                                                })
                                                            );
                                                        } else {
                                                            println!(
                                                                "Job {} completed with exit code {}",
                                                                job_id, code
                                                            );
                                                        }
                                                        std::process::exit(code);
                                                    }
                                                    JobStatus::Failed(ref err) => {
                                                        if cli.json {
                                                            println!(
                                                                "{}",
                                                                serde_json::json!({
                                                                    "type": "job",
                                                                    "job_id": job_id,
                                                                    "status": "failed",
                                                                    "error": err
                                                                })
                                                            );
                                                        } else {
                                                            eprintln!(
                                                                "Job {} failed: {}",
                                                                job_id, err
                                                            );
                                                        }
                                                        std::process::exit(1);
                                                    }
                                                    JobStatus::Killed => {
                                                        if cli.json {
                                                            println!(
                                                                "{}",
                                                                serde_json::json!({
                                                                    "type": "job",
                                                                    "job_id": job_id,
                                                                    "status": "killed"
                                                                })
                                                            );
                                                        } else {
                                                            eprintln!("Job {} was killed", job_id);
                                                        }
                                                        std::process::exit(137);
                                                        // SIGKILL
                                                    }
                                                    _ => {} // Still running/pending
                                                }
                                            }
                                        }
                                        Message::Error(e) => {
                                            eprintln!("Error polling status: {}", e);
                                            break;
                                        }
                                        _ => {}
                                    }
                                }

                                if !found {
                                    // Check history just in case it was moved quickly
                                    framed
                                        .send(Message::GetJobHistory {
                                            filter: HistoryFilter::Single(job_id),
                                        })
                                        .await?;
                                    if let Some(res) = framed.next().await {
                                        match res? {
                                            Message::JobHistoryResponse(history) => {
                                                if let Some(job) = history.first() {
                                                    match job.status {
                                                        JobStatus::Completed(code) => {
                                                            if cli.json {
                                                                println!(
                                                                    "{}",
                                                                    serde_json::json!({
                                                                        "type": "job",
                                                                        "job_id": job_id,
                                                                        "status": "completed",
                                                                        "exit_code": code
                                                                    })
                                                                );
                                                            } else {
                                                                println!("Job {} completed with exit code {}", job_id, code);
                                                            }
                                                            std::process::exit(code);
                                                        }
                                                        JobStatus::Failed(ref err) => {
                                                            if cli.json {
                                                                println!(
                                                                    "{}",
                                                                    serde_json::json!({
                                                                        "type": "job",
                                                                        "job_id": job_id,
                                                                        "status": "failed",
                                                                        "error": err
                                                                    })
                                                                );
                                                            } else {
                                                                eprintln!(
                                                                    "Job {} failed: {}",
                                                                    job_id, err
                                                                );
                                                            }
                                                            std::process::exit(1);
                                                        }
                                                        JobStatus::Killed => {
                                                            if cli.json {
                                                                println!(
                                                                    "{}",
                                                                    serde_json::json!({
                                                                        "type": "job",
                                                                        "job_id": job_id,
                                                                        "status": "killed"
                                                                    })
                                                                );
                                                            } else {
                                                                eprintln!(
                                                                    "Job {} was killed",
                                                                    job_id
                                                                );
                                                            }
                                                            std::process::exit(137);
                                                        }
                                                        _ => {}
                                                    }
                                                }
                                            }
                                            _ => {}
                                        }
                                    }
                                    // If still not found, assume failure or pruned (or just not synced yet?)
                                    // Just loop again or maybe warn?
                                    // If it's truly gone, we might loop forever.
                                    // But if it was submitted, it should be somewhere.
                                }
                            }
                        }
                    }
                    Message::ArrayJobSubmitted {
                        base_job_id,
                        task_count,
                    } => {
                        if !wait && cli.json {
                            println!(
                                "{}",
                                serde_json::json!({
                                    "type": "array",
                                    "base_job_id": base_job_id,
                                    "task_count": task_count
                                })
                            );
                        } else if !cli.json {
                            println!(
                                "Array Job submitted successfully! Base ID: {} ({} tasks)",
                                base_job_id, task_count
                            );
                        }
                        if wait {
                            if !cli.json {
                                println!("Waiting for array job {} to complete...", base_job_id);
                            }
                            loop {
                                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

                                // Check active jobs
                                framed
                                    .send(Message::ListJobs { state_filter: None })
                                    .await?;
                                let mut all_done = true;
                                let mut has_errors = false;
                                let mut found_any = false;

                                if let Some(res) = framed.next().await {
                                    match res? {
                                        Message::JobList(jobs) => {
                                            for job in jobs {
                                                if job.array_id == Some(base_job_id) {
                                                    found_any = true;
                                                    match job.status {
                                                        JobStatus::Pending | JobStatus::Running => {
                                                            all_done = false
                                                        }
                                                        JobStatus::Failed(_)
                                                        | JobStatus::Killed => has_errors = true,
                                                        JobStatus::Completed(_) => {}
                                                    }
                                                }
                                            }
                                        }
                                        Message::Error(e) => {
                                            eprintln!("Error polling status: {}", e);
                                            break;
                                        }
                                        _ => {}
                                    }
                                }

                                if !found_any {
                                    // if totally gone, break with success (or error if we assume it got pruned)
                                    if cli.json {
                                        println!(
                                            "{}",
                                            serde_json::json!({
                                                "type": "array",
                                                "base_job_id": base_job_id,
                                                "task_count": task_count,
                                                "status": if has_errors { "failed" } else { "completed" }
                                            })
                                        );
                                    } else {
                                        println!("Array jobs are no longer active.");
                                    }
                                    std::process::exit(if has_errors { 1 } else { 0 });
                                }

                                if all_done {
                                    if has_errors {
                                        if cli.json {
                                            println!(
                                                "{}",
                                                serde_json::json!({
                                                    "type": "array",
                                                    "base_job_id": base_job_id,
                                                    "task_count": task_count,
                                                    "status": "failed"
                                                })
                                            );
                                        } else {
                                            eprintln!(
                                                "Array job {} completed with some errors.",
                                                base_job_id
                                            );
                                        }
                                        std::process::exit(1);
                                    } else {
                                        if cli.json {
                                            println!(
                                                "{}",
                                                serde_json::json!({
                                                    "type": "array",
                                                    "base_job_id": base_job_id,
                                                    "task_count": task_count,
                                                    "status": "completed"
                                                })
                                            );
                                        } else {
                                            println!(
                                                "Array job {} completed successfully.",
                                                base_job_id
                                            );
                                        }
                                        std::process::exit(0);
                                    }
                                }
                            }
                        }
                    }
                    Message::Error(e) => eprintln!("Error: {}", e),
                    _ => eprintln!("Unexpected response"),
                }
            }
        }
        Commands::Step {
            nodes,
            cores,
            ntasks,
            job_id,
            input_dir,
            gres,
            env,
            wait,
            follow,
            log_type,
            command,
        } => {
            let (binary, args) = split_command(command, "Step")?;
            let parent_job_id = job_id
                .or_else(|| {
                    std::env::var("VELOCE_JOB_ID")
                        .ok()
                        .and_then(|v| v.parse().ok())
                })
                .context("Job ID not provided and VELOCE_JOB_ID environment variable not set")?;

            let mut inputs = Vec::new();
            if let Some(dir) = input_dir {
                let path = std::path::Path::new(&dir);
                if !path.exists() {
                    anyhow::bail!("Input directory does not exist: {}", dir);
                }
                if cli.json {
                    eprintln!("Compressing input directory: {}...", dir);
                } else {
                    println!("Compressing input directory: {}...", dir);
                }
                let archive = compress_dir(path)?;

                if cli.json {
                    eprintln!("Uploading to fileserver: {}...", cli.fileserver);
                } else {
                    println!("Uploading to fileserver: {}...", cli.fileserver);
                }
                let file_client = veloce_common::file_client::FileClient::new(
                    cli.fileserver,
                    fileserver_key.clone(),
                );
                let handle = file_client.upload_file(&archive, false, true).await?;
                inputs.push(handle);

                // Cleanup local temp archive
                let _ = std::fs::remove_file(archive);
            }

            let gres_req: std::collections::BTreeMap<String, u64> = gres.into_iter().collect();

            framed
                .send(Message::SubmitStep {
                    parent_job_id,
                    binary,
                    args,
                    req_nodes: nodes,
                    req_cores: cores,
                    ntasks,
                    inputs,
                    gres_req,
                    env_vars: env,
                })
                .await?;

            if let Some(res) = framed.next().await {
                match res? {
                    Message::StepId { step_id } => {
                        if cli.json {
                            println!(
                                "{}",
                                serde_json::json!({
                                    "type": "step",
                                    "parent_job_id": parent_job_id,
                                    "step_id": step_id
                                })
                            );
                        } else {
                            println!("Step submitted successfully. Step ID: {}", step_id);
                            println!("Logs will be visible in the parent job's log files: {}-stdout.log, {}-stderr.log", parent_job_id, parent_job_id);
                        }
                        if !(wait || follow) {
                            std::process::exit(0);
                        }

                        let l_type = match log_type.to_lowercase().as_str() {
                            "stdout" => veloce_common::LogType::Stdout,
                            "stderr" => veloce_common::LogType::Stderr,
                            _ => anyhow::bail!("Invalid log type. Use 'stdout' or 'stderr'."),
                        };
                        let mut offset = 0u64;
                        loop {
                            if follow {
                                let request_id = SystemTime::now()
                                    .duration_since(UNIX_EPOCH)
                                    .unwrap()
                                    .as_nanos()
                                    as u64;
                                framed
                                    .send(Message::GetLogs {
                                        request_id,
                                        job_id: parent_job_id,
                                        log_type: l_type.clone(),
                                        offset,
                                        length: None,
                                        working_directory: None,
                                        rank: None,
                                    })
                                    .await?;
                                if let Some(res) = framed.next().await {
                                    match res? {
                                        Message::LogData { content, .. } => {
                                            if !content.is_empty() {
                                                if cli.json {
                                                    println!(
                                                        "{}",
                                                        serde_json::json!({
                                                            "type": "step_log",
                                                            "parent_job_id": parent_job_id,
                                                            "step_id": step_id,
                                                            "log_type": &log_type,
                                                            "offset": offset,
                                                            "bytes": content.len(),
                                                            "content": String::from_utf8_lossy(&content),
                                                            "content_base64": general_purpose::STANDARD.encode(&content)
                                                        })
                                                    );
                                                } else {
                                                    std::io::stdout().write_all(&content)?;
                                                    std::io::stdout().flush()?;
                                                }
                                                offset += content.len() as u64;
                                            }
                                        }
                                        Message::Error(e) => {
                                            eprintln!("Error reading parent job log: {}", e);
                                        }
                                        _ => eprintln!("Unexpected response while reading logs"),
                                    }
                                }
                            }

                            framed
                                .send(Message::GetJobSteps {
                                    job_id: parent_job_id,
                                })
                                .await?;
                            if let Some(res) = framed.next().await {
                                match res? {
                                    Message::JobSteps(steps) => {
                                        let Some(step) =
                                            steps.into_iter().find(|s| s.step_id == step_id)
                                        else {
                                            eprintln!("Step {} not found", step_id);
                                            std::process::exit(1);
                                        };
                                        match step.status {
                                            veloce_common::StepStatus::Completed(exit_code) => {
                                                if cli.json {
                                                    println!(
                                                        "{}",
                                                        serde_json::json!({
                                                            "type": "step",
                                                            "parent_job_id": parent_job_id,
                                                            "step_id": step_id,
                                                            "status": "completed",
                                                            "exit_code": exit_code
                                                        })
                                                    );
                                                } else {
                                                    println!(
                                                        "Step {} finished with exit code {}.",
                                                        step_id, exit_code
                                                    );
                                                }
                                                std::process::exit(exit_code);
                                            }
                                            veloce_common::StepStatus::Failed(error) => {
                                                if cli.json {
                                                    println!(
                                                        "{}",
                                                        serde_json::json!({
                                                            "type": "step",
                                                            "parent_job_id": parent_job_id,
                                                            "step_id": step_id,
                                                            "status": "failed",
                                                            "error": error
                                                        })
                                                    );
                                                } else {
                                                    eprintln!("Step {} failed: {}", step_id, error);
                                                }
                                                std::process::exit(1);
                                            }
                                            veloce_common::StepStatus::Killed => {
                                                if cli.json {
                                                    println!(
                                                        "{}",
                                                        serde_json::json!({
                                                            "type": "step",
                                                            "parent_job_id": parent_job_id,
                                                            "step_id": step_id,
                                                            "status": "killed"
                                                        })
                                                    );
                                                } else {
                                                    eprintln!("Step {} was killed.", step_id);
                                                }
                                                std::process::exit(1);
                                            }
                                            veloce_common::StepStatus::Pending
                                            | veloce_common::StepStatus::Running => {}
                                        }
                                    }
                                    Message::Error(e) => {
                                        eprintln!("Error waiting for step: {}", e);
                                        std::process::exit(1);
                                    }
                                    _ => eprintln!("Unexpected response while waiting for step"),
                                }
                            }
                            tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
                        }
                    }
                    Message::Error(e) => eprintln!("Error submitting step: {}", e),
                    _ => eprintln!("Unexpected response"),
                }
            }
        }
        Commands::ListNodes
        | Commands::Nodes {
            command: NodeCommands::List,
        } => {
            framed.send(Message::ListWorkers).await?;
            if let Some(res) = framed.next().await {
                match res? {
                    Message::WorkerList(nodes) => {
                        if cli.json {
                            println!("{}", serde_json::to_string_pretty(&nodes)?);
                        } else {
                            println!(
                                "{:<10} {:<20} {:<15} {:<15} {:<20} {:<20} {:<25} {:<10}",
                                "Node",
                                "Hostname",
                                "IP",
                                "Cores (A/T)",
                                "Memory (A/T)",
                                "Disk (Free/Tot)",
                                "Model",
                                "Arch"
                            );
                            for node in nodes {
                                let allocated_cores = node.total_cores - node.available_cores;
                                let total_mem_mb = node.total_memory / 1024 / 1024;
                                let allocated_mem_mb = node.allocated_memory;

                                let total_disk_gb = node.disk_total / 1024 / 1024 / 1024;
                                let free_disk_gb = node.disk_free / 1024 / 1024 / 1024;

                                // Shorten ID to first 8 chars for display
                                let short_id = if node.id.len() > 8 {
                                    &node.id[..8]
                                } else {
                                    &node.id
                                };

                                let model_str = node.cpu_model.trim();
                                let display_model = if model_str.is_empty() {
                                    "Generic".to_string()
                                } else {
                                    model_str.to_string()
                                };

                                let short_model = if display_model.len() > 22 {
                                    format!("{}...", &display_model[..22])
                                } else {
                                    display_model
                                };

                                println!(
                                    "{:<10} {:<20} {:<15} {:<15} {:<20} {:<20} {:<25} {:<10}",
                                    short_id,
                                    node.hostname,
                                    node.ip_address,
                                    format!("{}/{}", allocated_cores, node.total_cores),
                                    format!("{}/{} MB", allocated_mem_mb, total_mem_mb),
                                    format!("{}/{} GB", free_disk_gb, total_disk_gb),
                                    short_model,
                                    node.arch
                                );
                            }
                        }
                    }
                    Message::Error(e) => eprintln!("Error: {}", e),
                    _ => eprintln!("Unexpected response"),
                }
            }
        }
        Commands::ListJobs { state, user, mine }
        | Commands::Jobs {
            command: JobCommands::List { state, user, mine },
        } => {
            if mine && user.is_some() {
                anyhow::bail!("Use either --mine or --user, not both");
            }
            let user_filter = if mine {
                Some(default_submitter_identity()?)
            } else {
                normalize_optional_text(user, "--user")?
            };

            let filter = match state.as_deref() {
                Some("pending") => Some(JobStateFilter::Pending),
                Some("running") => Some(JobStateFilter::Running),
                Some("completed") => Some(JobStateFilter::Completed),
                Some("failed") => Some(JobStateFilter::Failed),
                Some("killed") => Some(JobStateFilter::Killed),
                Some(other) => anyhow::bail!(
                    "Invalid state: {}. Valid values: pending, running, completed, failed, killed",
                    other
                ),
                None => None,
            };

            framed
                .send(Message::ListJobs {
                    state_filter: filter,
                })
                .await?;
            if let Some(res) = framed.next().await {
                match res? {
                    Message::JobList(mut jobs) => {
                        if let Some(user_filter) = &user_filter {
                            jobs.retain(|job| job.user_id == *user_filter);
                        }
                        if cli.json {
                            for job in &mut jobs {
                                job.secret.clear();
                            }
                            println!("{}", serde_json::to_string_pretty(&jobs)?);
                        } else {
                            jobs.sort_by(|a, b| a.id.cmp(&b.id)); // Sort by ID ascending
                            println!("{:<5} {:<18} {:<15} {:<25} {:<8} {:<8} {:<20} {:<10} {:<10} {:<30} {:<30}", "ID", "Name", "User", "Status", "Nodes", "Cores", "Workers", "Walltime", "Elapsed", "Reason", "Command");
                            let now = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap()
                                .as_secs();

                            for job in jobs {
                                let workers = if job.assigned_workers.is_empty() {
                                    "None".to_string()
                                } else {
                                    job.assigned_workers.join(", ")
                                };
                                let cmd = format!("{} {}", job.binary, job.args.join(" ")); // Changed to join args with space
                                let name_display = job
                                    .job_name
                                    .as_deref()
                                    .map(|name| {
                                        if name.len() > 15 {
                                            format!("{}...", &name[..15])
                                        } else {
                                            name.to_string()
                                        }
                                    })
                                    .unwrap_or_else(|| "-".to_string());

                                // Truncate workers list if too long
                                let workers_display = if workers.len() > 17 {
                                    // New width for Workers is 20, so 17 + ...
                                    format!("{}...", &workers[..17])
                                } else {
                                    workers
                                };

                                let command_display = if cmd.len() > 27 {
                                    // Width 30
                                    format!("{}...", &cmd[..27])
                                } else {
                                    cmd
                                };

                                let reason_display = if let Some(r) = &job.reason {
                                    if r.len() > 27 {
                                        format!("{}...", &r[..27])
                                    } else {
                                        r.clone()
                                    }
                                } else {
                                    "-".to_string()
                                };

                                let elapsed = if let Some(start) = job.start_time {
                                    if job.status == JobStatus::Running {
                                        now.saturating_sub(start)
                                    } else if let Some(end) = job.end_time {
                                        end.saturating_sub(start)
                                    } else {
                                        0
                                    }
                                } else {
                                    0
                                };

                                let walltime_display = if job.walltime == 0 {
                                    "∞".to_string()
                                } else {
                                    format!("{}s", job.walltime)
                                };

                                println!("{:<5} {:<18} {:<15} {:<25} {:<8} {:<8} {:<20} {:<10} {:<10} {:<30} {:<30}",
                                    job.id,
                                    name_display,
                                    job.user_id,
                                    format_job_status(&job.status), // Use the helper function
                                    job.req_nodes,
                                    job.req_cores,
                                    workers_display,
                                    walltime_display,
                                    format!("{}s", elapsed),
                                    reason_display,
                                    command_display
                                );
                            }
                        }
                    }
                    Message::Error(e) => eprintln!("Error: {}", e),
                    _ => eprintln!("Unexpected response"),
                }
            }
        }
        Commands::Jobs {
            command: JobCommands::Show { job_id },
        } => {
            if job_id.contains(':') {
                anyhow::bail!("federated job IDs are not supported in Community Edition");
            }
            let job_id = job_id
                .parse::<u64>()
                .with_context(|| format!("invalid local job id '{job_id}'"))?;
            framed
                .send(Message::ListJobs { state_filter: None })
                .await?;

            let active_job = if let Some(res) = framed.next().await {
                match res? {
                    Message::JobList(mut jobs) => jobs
                        .iter()
                        .position(|job| job.id == job_id)
                        .map(|idx| jobs.remove(idx)),
                    Message::Error(e) => anyhow::bail!("Error listing active jobs: {}", e),
                    _ => anyhow::bail!("Unexpected response while listing active jobs"),
                }
            } else {
                anyhow::bail!("Controller closed connection while listing active jobs");
            };

            if let Some(mut job) = active_job {
                job.secret.clear();
                if cli.json {
                    println!("{}", serde_json::to_string_pretty(&job)?);
                } else {
                    print_job_info(&job);
                }
                return Ok(());
            }

            framed
                .send(Message::GetJobHistory {
                    filter: HistoryFilter::Single(job_id),
                })
                .await?;

            if let Some(res) = framed.next().await {
                match res? {
                    Message::JobHistoryResponse(mut history) => {
                        if let Some(mut job) = history.pop() {
                            job.secret.clear();
                            if cli.json {
                                println!("{}", serde_json::to_string_pretty(&job)?);
                            } else {
                                print_job_usage(&job);
                            }
                        } else {
                            anyhow::bail!("Job {} not found in active jobs or history", job_id);
                        }
                    }
                    Message::Error(e) => anyhow::bail!("Error fetching job history: {}", e),
                    _ => anyhow::bail!("Unexpected response while fetching job history"),
                }
            } else {
                anyhow::bail!("Controller closed connection while fetching job history");
            }
        }
        Commands::Jobs {
            command: JobCommands::Events { job_id },
        } => {
            framed.send(Message::GetJobEvents { job_id }).await?;

            if let Some(res) = framed.next().await {
                match res? {
                    Message::JobEvents(events) => {
                        if cli.json {
                            println!("{}", serde_json::to_string_pretty(&events)?);
                        } else {
                            print_job_events(&events)?;
                        }
                    }
                    Message::Error(e) => anyhow::bail!("Error fetching job events: {}", e),
                    _ => anyhow::bail!("Unexpected response while fetching job events"),
                }
            } else {
                anyhow::bail!("Controller closed connection while fetching job events");
            }
        }
        Commands::Jobs {
            command:
                JobCommands::Resubmit {
                    job_id,
                    job_name,
                    job_comment,
                    user_id,
                },
        } => {
            let job_name_override = normalize_optional_text(job_name, "--name")?;
            let job_comment_override = normalize_optional_text(job_comment, "--comment")?;
            let user_override = normalize_optional_user(user_id)?;

            framed
                .send(Message::ListJobs { state_filter: None })
                .await?;

            let active_job = if let Some(res) = framed.next().await {
                match res? {
                    Message::JobList(mut jobs) => jobs
                        .iter()
                        .position(|job| job.id == job_id)
                        .map(|idx| jobs.remove(idx)),
                    Message::Error(e) => anyhow::bail!("Error listing active jobs: {}", e),
                    _ => anyhow::bail!("Unexpected response while listing active jobs"),
                }
            } else {
                anyhow::bail!("Controller closed connection while listing active jobs");
            };

            let submit_msg = if let Some(job) = active_job {
                Message::Submit {
                    job_name: job_name_override.or_else(|| job.job_name.clone()),
                    job_comment: job_comment_override.or_else(|| job.job_comment.clone()),
                    binary: job.binary.clone(),
                    args: job.args.clone(),
                    req_nodes: job.req_nodes,
                    req_cores: job.req_cores,
                    req_memory: job.req_memory,
                    walltime: job.walltime,
                    priority: job.priority,
                    user_id: user_override.unwrap_or_else(|| job.user_id.clone()),
                    working_directory: job.working_directory.clone(),
                    array_indices: None,
                    inputs: job.inputs.clone(),
                    gres_req: job.gres_req.clone(),
                    env_vars: job.env_vars.clone(),
                    wait_for_licenses: job.wait_for_licenses,
                    estimated_walltime: job.estimated_walltime,
                    priority_offset: job.priority_offset,
                    dependencies: job.dependencies.clone(),
                    dependency_specs: job.dependency_specs.clone(),
                    qos: job.qos.clone(),
                    image_uri: job.container_asset.as_ref().map(|asset| asset.id.clone()),
                    vnc_enabled: job.vnc_enabled,
                    inherit_host_env: job.inherit_host_env,
                    env_allowlist: job.env_allowlist.clone(),
                    job_profile: job.job_profile.clone(),
                }
            } else {
                framed
                    .send(Message::GetJobHistory {
                        filter: HistoryFilter::Single(job_id),
                    })
                    .await?;

                let history_job = if let Some(res) = framed.next().await {
                    match res? {
                        Message::JobHistoryResponse(mut history) => history.pop(),
                        Message::Error(e) => anyhow::bail!("Error fetching job history: {}", e),
                        _ => anyhow::bail!("Unexpected response while fetching job history"),
                    }
                } else {
                    anyhow::bail!("Controller closed connection while fetching job history");
                };

                let history_job = history_job.ok_or_else(|| {
                    anyhow::anyhow!("Job {} not found in active jobs or history", job_id)
                })?;
                let (binary, args) = parse_history_command_line(&history_job.command_line)?;

                if !cli.json {
                    eprintln!(
                        "Note: resubmitting from history cannot restore transient env vars, input handles, or working directory; using command/resources from accounting."
                    );
                }

                Message::Submit {
                    job_name: job_name_override.or(history_job.job_name),
                    job_comment: job_comment_override.or(history_job.job_comment),
                    binary,
                    args,
                    req_nodes: history_job.req_nodes,
                    req_cores: history_job.req_cores,
                    req_memory: history_job.req_memory,
                    walltime: 0,
                    priority: 0,
                    user_id: user_override.unwrap_or(history_job.user_id),
                    working_directory: "/scratch".to_string(),
                    array_indices: None,
                    inputs: Vec::new(),
                    gres_req: history_job.gres_req,
                    env_vars: Vec::new(),
                    wait_for_licenses: history_job.wait_for_licenses,
                    estimated_walltime: history_job.estimated_walltime,
                    priority_offset: history_job.priority_offset,
                    dependencies: history_job.dependencies,
                    dependency_specs: history_job.dependency_specs,
                    qos: history_job.qos,
                    image_uri: history_job
                        .container_asset
                        .as_ref()
                        .map(|asset| asset.id.clone()),
                    vnc_enabled: false,
                    inherit_host_env: false,
                    env_allowlist: None,
                    job_profile: None,
                }
            };

            framed.send(submit_msg).await?;
            if let Some(res) = framed.next().await {
                match res? {
                    Message::JobId { job_id: new_job_id } => {
                        if cli.json {
                            println!(
                                "{}",
                                serde_json::json!({
                                    "job_id": new_job_id,
                                    "resubmitted_from": job_id
                                })
                            );
                        } else {
                            println!("Job {} resubmitted as {}.", job_id, new_job_id);
                        }
                    }
                    Message::ArrayJobSubmitted {
                        base_job_id,
                        task_count,
                    } => {
                        if cli.json {
                            println!(
                                "{}",
                                serde_json::json!({
                                    "base_job_id": base_job_id,
                                    "task_count": task_count,
                                    "resubmitted_from": job_id
                                })
                            );
                        } else {
                            println!(
                                "Job {} resubmitted as array {} ({} tasks).",
                                job_id, base_job_id, task_count
                            );
                        }
                    }
                    Message::Error(e) => anyhow::bail!("Error resubmitting job: {}", e),
                    _ => anyhow::bail!("Unexpected response while resubmitting job"),
                }
            }
        }
        Commands::KillJob { job_id } => {
            framed.send(Message::CancelJob { job_id }).await?;
            if let Some(res) = framed.next().await {
                match res? {
                    Message::Ack => {
                        if cli.json {
                            println!(
                                "{}",
                                serde_json::json!({
                                    "ok": true,
                                    "action": "kill_job",
                                    "job_id": job_id
                                })
                            );
                        } else {
                            println!("Kill command sent.");
                        }
                    }
                    Message::Error(e) => eprintln!("Error: {}", e),
                    _ => eprintln!("Unexpected response"),
                }
            }
        }
        Commands::Jobs {
            command: JobCommands::Kill { job_id },
        } => {
            if job_id.contains(':') {
                anyhow::bail!("federated job IDs are not supported in Community Edition");
            }
            let job_id = job_id
                .parse::<u64>()
                .with_context(|| format!("invalid local job id '{job_id}'"))?;
            framed.send(Message::CancelJob { job_id }).await?;
            if let Some(res) = framed.next().await {
                match res? {
                    Message::Ack => {
                        if cli.json {
                            println!(
                                "{}",
                                serde_json::json!({
                                    "ok": true,
                                    "action": "kill_job",
                                    "job_id": job_id
                                })
                            );
                        } else {
                            println!("Kill command sent.");
                        }
                    }
                    Message::Error(e) => eprintln!("Error: {}", e),
                    _ => eprintln!("Unexpected response"),
                }
            }
        }
        Commands::Logs {
            job_id,
            log_type,
            rank,
            follow,
        } => {
            let l_type = match log_type.to_lowercase().as_str() {
                "stdout" => veloce_common::LogType::Stdout,
                "stderr" => veloce_common::LogType::Stderr,
                _ => anyhow::bail!("Invalid log type. Use 'stdout' or 'stderr'."),
            };

            let mut offset = 0;
            loop {
                let request_id = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as u64;
                framed
                    .send(Message::GetLogs {
                        request_id,
                        job_id,
                        log_type: l_type.clone(),
                        offset,
                        length: None,
                        working_directory: None,
                        rank,
                    })
                    .await?;

                if let Some(res) = framed.next().await {
                    match res? {
                        Message::LogData {
                            request_id: _,
                            job_id: _,
                            content,
                        } => {
                            if !content.is_empty() {
                                if cli.json {
                                    println!(
                                        "{}",
                                        serde_json::json!({
                                            "job_id": job_id,
                                            "log_type": &log_type,
                                            "rank": rank,
                                            "offset": offset,
                                            "bytes": content.len(),
                                            "content": String::from_utf8_lossy(&content),
                                            "content_base64": general_purpose::STANDARD.encode(&content)
                                        })
                                    );
                                } else {
                                    std::io::stdout().write_all(&content)?;
                                    std::io::stdout().flush()?;
                                }
                                offset += content.len() as u64;
                            } else if cli.json && !follow {
                                println!(
                                    "{}",
                                    serde_json::json!({
                                        "job_id": job_id,
                                        "log_type": &log_type,
                                        "rank": rank,
                                        "offset": offset,
                                        "bytes": 0,
                                        "content": "",
                                        "content_base64": ""
                                    })
                                );
                            }
                        }
                        Message::Error(e) => {
                            eprintln!("Error: {}", e);
                            break;
                        }
                        _ => {
                            eprintln!("Unexpected response");
                            break;
                        }
                    }
                } else {
                    break;
                }

                if !follow {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
            }
        }
        Commands::Jobs {
            command:
                JobCommands::Logs {
                    job_id,
                    log_type,
                    rank,
                    follow,
                },
        } => {
            if job_id.contains(':') {
                anyhow::bail!("federated job IDs are not supported in Community Edition");
            }

            let job_id = job_id
                .parse::<u64>()
                .with_context(|| format!("invalid local job id '{job_id}'"))?;
            let l_type = match log_type.to_lowercase().as_str() {
                "stdout" => veloce_common::LogType::Stdout,
                "stderr" => veloce_common::LogType::Stderr,
                _ => anyhow::bail!("Invalid log type. Use 'stdout' or 'stderr'."),
            };

            let mut offset = 0;
            loop {
                let request_id = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as u64;
                framed
                    .send(Message::GetLogs {
                        request_id,
                        job_id,
                        log_type: l_type.clone(),
                        offset,
                        length: None,
                        working_directory: None,
                        rank,
                    })
                    .await?;
                if let Some(res) = framed.next().await {
                    match res? {
                        Message::LogData { content, .. } => {
                            if !content.is_empty() {
                                if cli.json {
                                    println!(
                                        "{}",
                                        serde_json::json!({
                                            "job_id": job_id,
                                            "log_type": &log_type,
                                            "rank": rank,
                                            "offset": offset,
                                            "bytes": content.len(),
                                            "content": String::from_utf8_lossy(&content),
                                            "content_base64": general_purpose::STANDARD.encode(&content)
                                        })
                                    );
                                } else {
                                    std::io::stdout().write_all(&content)?;
                                    std::io::stdout().flush()?;
                                }
                                offset += content.len() as u64;
                            }
                        }
                        Message::Error(e) => {
                            eprintln!("Error: {}", e);
                            break;
                        }
                        _ => {
                            eprintln!("Unexpected response");
                            break;
                        }
                    }
                } else {
                    break;
                }
                if !follow {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
            }
        }
        Commands::Reserve {
            nodes,
            start,
            end,
            owner,
        }
        | Commands::Reservations {
            command:
                ReservationCommands::Create {
                    nodes,
                    start,
                    end,
                    owner,
                },
        } => {
            let now_secs = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();

            let parse_time = |s: &str| -> Result<u64> {
                if s.starts_with('+') {
                    let secs: u64 = s[1..].parse().context("Invalid relative seconds format")?;
                    Ok(now_secs + secs)
                } else {
                    let dt = chrono::DateTime::parse_from_rfc3339(s)
                        .context("Invalid time format. Use RFC3339 (e.g. 2026-05-18T12:00:00Z) or relative seconds (e.g. +3600)")?;
                    Ok(dt.timestamp() as u64)
                }
            };

            let start_time = parse_time(&start)?;
            let end_time = parse_time(&end)?;

            let node_set: std::collections::HashSet<String> = nodes
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();

            if node_set.is_empty() {
                anyhow::bail!("At least one node must be specified.");
            }

            framed
                .send(Message::CreateReservation {
                    nodes: node_set,
                    start_time,
                    end_time,
                    owner,
                })
                .await?;

            if let Some(res) = framed.next().await {
                match res? {
                    Message::ReservationCreated { id } => {
                        if cli.json {
                            println!(
                                "{}",
                                serde_json::json!({
                                    "ok": true,
                                    "reservation_id": id
                                })
                            );
                        } else {
                            println!("Reservation created successfully! ID: {}", id);
                        }
                    }
                    Message::Error(e) => {
                        eprintln!("Error creating reservation: {}", e);
                    }
                    _ => {
                        eprintln!("Unexpected response from controller.");
                    }
                }
            }
        }
        Commands::ListReservations
        | Commands::Reservations {
            command: ReservationCommands::List,
        } => {
            framed.send(Message::ListReservations).await?;
            if let Some(res) = framed.next().await {
                match res? {
                    Message::ReservationList(reservations) => {
                        if cli.json {
                            println!("{}", serde_json::to_string_pretty(&reservations)?);
                        } else if reservations.is_empty() {
                            println!("No reservations found.");
                        } else {
                            println!(
                                "{:<40} {:<30} {:<25} {:<25} {:<15}",
                                "ID", "Nodes", "Start", "End", "Owner"
                            );
                            println!("{}", "-".repeat(140));
                            for r in reservations {
                                let mut nodes_vec: Vec<String> = r.nodes.into_iter().collect();
                                nodes_vec.sort();
                                let nodes_str = nodes_vec.join(", ");
                                let nodes_display = if nodes_str.len() > 28 {
                                    format!("{}...", &nodes_str[..25])
                                } else {
                                    nodes_str
                                };
                                println!(
                                    "{:<40} {:<30} {:<25} {:<25} {:<15}",
                                    r.id,
                                    nodes_display,
                                    format_time(r.start_time),
                                    format_time(r.end_time),
                                    r.owner
                                );
                            }
                        }
                    }
                    Message::Error(e) => {
                        eprintln!("Error listing reservations: {}", e);
                    }
                    _ => {
                        eprintln!("Unexpected response from controller.");
                    }
                }
            }
        }
        Commands::DeleteReservation { id }
        | Commands::Reservations {
            command: ReservationCommands::Delete { id },
        } => {
            framed
                .send(Message::DeleteReservation { id: id.clone() })
                .await?;
            if let Some(res) = framed.next().await {
                match res? {
                    Message::Ack => {
                        if cli.json {
                            println!(
                                "{}",
                                serde_json::json!({
                                    "ok": true,
                                    "action": "delete_reservation",
                                    "reservation_id": id
                                })
                            );
                        } else {
                            println!("Reservation deleted successfully.");
                        }
                    }
                    Message::Error(e) => {
                        eprintln!("Error deleting reservation: {}", e);
                    }
                    _ => {
                        eprintln!("Unexpected response from controller.");
                    }
                }
            }
        }
        Commands::Containers { command } => {
            let api_key_val = cli.api_key.clone().unwrap_or_default();
            match command {
                ContainerCommands::List => {
                    let controller_addr = if controller_list.is_empty() {
                        "127.0.0.1:9000"
                    } else {
                        &controller_list[0]
                    };
                    let host = controller_addr.split(':').next().unwrap_or("127.0.0.1");
                    let controller_url = format!("https://{}:8080", host);
                    let url = format!("{}/api/v1/containers", controller_url);
                    let client = reqwest::Client::builder()
                        .danger_accept_invalid_certs(!cfg!(feature = "production"))
                        .build()
                        .unwrap_or_default();
                    let res = client
                        .get(&url)
                        .header("X-API-KEY", &api_key_val)
                        .send()
                        .await?;
                    if res.status().is_success() {
                        let containers: Vec<veloce_common::apptainer::ContainerAsset> =
                            res.json().await?;
                        if cli.json {
                            println!("{}", serde_json::to_string_pretty(&containers)?);
                        } else {
                            println!("{:<40} {:<30} {:<40}", "ID", "NAME", "IMAGE URI");
                            println!("{:-<110}", "");
                            for asset in containers {
                                println!(
                                    "{:<40} {:<30} {:<40}",
                                    asset.id, asset.name, asset.image_uri
                                );
                            }
                        }
                    } else {
                        anyhow::bail!("Failed to list containers: {}", res.text().await?);
                    }
                }
                ContainerCommands::Register {
                    name,
                    sif_path,
                    manifest_path,
                } => {
                    if api_key_val.is_empty() {
                        anyhow::bail!(
                            "VELOCE_API_KEY (or --api-key) is required to register containers with the Controller. \
                             Fileserver upload uses VELOCE_FILESERVER_KEY; do not use VELOCE_SECRET. \
                             See docs/apptainer.md §3."
                        );
                    }
                    let manifest_content = std::fs::read_to_string(&manifest_path)?;
                    let manifest: veloce_common::apptainer::SolverManifest =
                        serde_json::from_str(&manifest_content)?;

                    if cli.json {
                        eprintln!("Uploading {} to S3 fileserver...", sif_path);
                    } else {
                        println!("Uploading {} to S3 fileserver...", sif_path);
                    }
                    let sif_file = tokio::fs::File::open(&sif_path).await?;
                    let sif_stream = tokio_util::io::ReaderStream::new(sif_file);
                    let put_url = format!(
                        "{}/s3/veloce-system-containers/{}.sif",
                        cli.fileserver.clone(),
                        name
                    );
                    let client = reqwest::Client::builder()
                        .danger_accept_invalid_certs(!cfg!(feature = "production"))
                        .build()
                        .unwrap_or_default();
                    let res = client
                        .put(&put_url)
                        .header("X-API-KEY", &fileserver_key)
                        .body(reqwest::Body::wrap_stream(sif_stream))
                        .send()
                        .await?;
                    if !res.status().is_success() {
                        anyhow::bail!("Failed to upload to S3: {}", res.text().await?);
                    }

                    if cli.json {
                        eprintln!("Registering container with Controller...");
                    } else {
                        println!("Registering container with Controller...");
                    }
                    let controller_addr = if controller_list.is_empty() {
                        "127.0.0.1:9000"
                    } else {
                        &controller_list[0]
                    };
                    let host = controller_addr.split(':').next().unwrap_or("127.0.0.1");
                    let controller_url = format!("https://{}:8080", host);
                    let url = format!("{}/api/v1/containers/register", controller_url);
                    let image_uri = format!("s3://veloce-system-containers/{}.sif", name);
                    let req = serde_json::json!({
                        "name": name.clone(),
                        "image_uri": image_uri.clone(),
                        "manifest": manifest
                    });
                    let client = reqwest::Client::builder()
                        .danger_accept_invalid_certs(!cfg!(feature = "production"))
                        .build()
                        .unwrap_or_default();
                    let res = client
                        .post(&url)
                        .header("X-API-KEY", &api_key_val)
                        .json(&req)
                        .send()
                        .await?;
                    if res.status().is_success() {
                        if cli.json {
                            println!(
                                "{}",
                                serde_json::json!({
                                    "ok": true,
                                    "action": "register_container",
                                    "name": name,
                                    "image_uri": image_uri
                                })
                            );
                        } else {
                            println!("Container '{}' registered successfully!", name);
                        }
                    } else {
                        anyhow::bail!("Failed to register container: {}", res.text().await?);
                    }
                }
                ContainerCommands::Delete { name } => {
                    if cli.json {
                        eprintln!("Deleting container '{}'...", name);
                    } else {
                        println!("Deleting container '{}'...", name);
                    }
                    let controller_addr = if controller_list.is_empty() {
                        "127.0.0.1:9000"
                    } else {
                        &controller_list[0]
                    };
                    let host = controller_addr.split(':').next().unwrap_or("127.0.0.1");
                    let controller_url = format!("https://{}:8080", host);
                    let url = format!("{}/api/v1/containers/{}", controller_url, name);

                    let client = reqwest::Client::builder()
                        .danger_accept_invalid_certs(!cfg!(feature = "production"))
                        .build()
                        .unwrap_or_default();
                    let res = client
                        .delete(&url)
                        .header("X-API-KEY", &api_key_val)
                        .send()
                        .await?;
                    if res.status().is_success() {
                        if cli.json {
                            println!(
                                "{}",
                                serde_json::json!({
                                    "ok": true,
                                    "action": "delete_container",
                                    "name": name
                                })
                            );
                        } else {
                            println!("Container '{}' deleted successfully!", name);
                        }
                    } else {
                        anyhow::bail!("Failed to delete container: {}", res.text().await?);
                    }
                }
            }
        }
    }

    // Clean shutdown to send TLS close_notify
    let mut stream = framed.into_inner();
    let _ = stream.shutdown().await;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn submit_accepts_command_without_separator() {
        let cli = Cli::try_parse_from(["veloce", "submit", "-c", "4", "/bin/sleep", "600"])
            .expect("submit command should parse");

        match cli.command {
            Commands::Submit { cores, command, .. } => {
                assert_eq!(cores, 4);
                assert_eq!(command, vec!["/bin/sleep".to_string(), "600".to_string()]);
            }
            _ => panic!("expected submit command"),
        }
    }

    #[test]
    fn submit_captures_solver_flags_after_executable() {
        let cli = Cli::try_parse_from([
            "veloce", "submit", "--gres", "gpu:1", "./solver", "--epochs", "10",
        ])
        .expect("submit command with solver flags should parse");

        match cli.command {
            Commands::Submit { gres, command, .. } => {
                assert_eq!(gres, vec![("gpu".to_string(), 1)]);
                assert_eq!(
                    command,
                    vec![
                        "./solver".to_string(),
                        "--epochs".to_string(),
                        "10".to_string()
                    ]
                );
            }
            _ => panic!("expected submit command"),
        }
    }

    #[test]
    fn submit_still_accepts_separator_escape_hatch() {
        let cli = Cli::try_parse_from(["veloce", "submit", "--", "./solver", "--solver-flag"])
            .expect("submit command with separator should parse");

        match cli.command {
            Commands::Submit { command, .. } => {
                assert_eq!(
                    command,
                    vec!["./solver".to_string(), "--solver-flag".to_string()]
                );
            }
            _ => panic!("expected submit command"),
        }
    }

    #[test]
    fn submit_cwl_does_not_require_command() {
        let cli = Cli::try_parse_from(["veloce", "submit", "--cwl", "workflow.cwl"])
            .expect("cwl submit should parse without command");

        match cli.command {
            Commands::Submit { cwl, command, .. } => {
                assert_eq!(cwl.as_deref(), Some("workflow.cwl"));
                assert!(command.is_empty());
            }
            _ => panic!("expected submit command"),
        }
    }

    #[test]
    fn step_accepts_command_without_separator() {
        let cli = Cli::try_parse_from([
            "veloce",
            "step",
            "--job-id",
            "1",
            "./rank-task",
            "--rank-flag",
            "value",
        ])
        .expect("step command should parse");

        match cli.command {
            Commands::Step {
                job_id, command, ..
            } => {
                assert_eq!(job_id, Some(1));
                assert_eq!(
                    command,
                    vec![
                        "./rank-task".to_string(),
                        "--rank-flag".to_string(),
                        "value".to_string()
                    ]
                );
            }
            _ => panic!("expected step command"),
        }
    }
}
