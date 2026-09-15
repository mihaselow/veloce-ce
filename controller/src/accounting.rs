use anyhow::{Context, Result};
use sqlx::postgres::PgPoolOptions;
use sqlx::sqlite::SqlitePoolOptions;
use sqlx::{Pool, Postgres, Sqlite};
use std::collections::{BTreeMap, VecDeque};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::sync::{Arc, Mutex as StdMutex};
use tracing::{error, info, warn};
use veloce_common::{HistoryFilter, JobStatus, JobUsage, QosLevel};

use crate::job_secrets;

const ACCOUNTING_FILE: &str = "data/veloce_accounting.bin";
const MAX_RECORD_SIZE: usize = 100 * 1024 * 1024; // 100MB
static ACCOUNTING_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[async_trait::async_trait]
pub trait AccountingStore: Send + Sync {
    async fn record_job(&self, record: &JobUsage) -> Result<()>;
    async fn query_history(&self, filter: &HistoryFilter) -> Result<Vec<JobUsage>>;
}

pub struct FileAccountingStore;

#[async_trait::async_trait]
impl AccountingStore for FileAccountingStore {
    async fn record_job(&self, record: &JobUsage) -> Result<()> {
        let _lock = ACCOUNTING_LOCK.lock().unwrap();
        let mut stored = record.clone();
        job_secrets::global_cipher().seal_usage_secret(&mut stored);
        if let Ok(encoded) = bincode::serialize(&stored) {
            if let Ok(mut file) = OpenOptions::new()
                .create(true)
                .append(true)
                .open(ACCOUNTING_FILE)
            {
                let len = encoded.len() as u64;
                let mut combined = Vec::with_capacity(8 + encoded.len());
                combined.extend_from_slice(&len.to_le_bytes());
                combined.extend_from_slice(&encoded);
                file.write_all(&combined)?;
                let _ = file.sync_all();
            } else {
                anyhow::bail!("Failed to open accounting file for appending");
            }
        }
        Ok(())
    }

    async fn query_history(&self, filter: &HistoryFilter) -> Result<Vec<JobUsage>> {
        let _lock = ACCOUNTING_LOCK.lock().unwrap();
        let mut results = Vec::new();
        if let Ok(mut file) = File::open(ACCOUNTING_FILE) {
            loop {
                let mut len_bytes = [0u8; 8];
                if file.read_exact(&mut len_bytes).is_err() {
                    break;
                }
                let len = u64::from_le_bytes(len_bytes) as usize;
                if len > MAX_RECORD_SIZE {
                    break;
                }
                let mut buffer = vec![0u8; len];
                if file.read_exact(&mut buffer).is_err() {
                    break;
                }

                if let Ok(mut usage) = bincode::deserialize::<JobUsage>(&buffer) {
                    job_secrets::global_cipher().open_usage_secret(&mut usage);
                    let include = match filter {
                        HistoryFilter::All => true,
                        HistoryFilter::Single(id) => usage.job_id == *id,
                        HistoryFilter::Range(start, end) => {
                            usage.job_id >= *start && usage.job_id <= *end
                        }
                    };
                    if include {
                        results.push(usage);
                    }
                }
            }
        }
        Ok(results)
    }
}

pub enum DbPool {
    Sqlite(Pool<Sqlite>),
    Postgres(Pool<Postgres>),
}

pub struct SqlxAccountingStore {
    pool: DbPool,
}

impl SqlxAccountingStore {
    pub async fn new(database_url: &str, max_connections: u32, timeout_secs: u64) -> Result<Self> {
        let pool = if database_url.starts_with("sqlite:") || database_url.starts_with("sqlite://") {
            let conn_url = if database_url.starts_with("sqlite://") {
                database_url.to_string()
            } else {
                format!("sqlite://{}", database_url.trim_start_matches("sqlite:"))
            };

            // For SQLite, ensure the file and parent directory are created if they don't exist
            let db_path = conn_url.trim_start_matches("sqlite://");
            if !db_path.is_empty() && db_path != ":memory:" {
                if let Some(parent) = std::path::Path::new(db_path).parent() {
                    let _ = std::fs::create_dir_all(parent);
                }
            }

            let pool = SqlitePoolOptions::new()
                .max_connections(max_connections)
                .acquire_timeout(std::time::Duration::from_secs(timeout_secs))
                .connect(&conn_url)
                .await
                .context("Failed to connect to SQLite")?;

            // Auto-initialize SQLite table structure
            sqlx::query(
                "CREATE TABLE IF NOT EXISTS veloce_accounting (
                    job_id INTEGER PRIMARY KEY,
                    job_name TEXT,
                    job_comment TEXT,
                    command_line TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    submission_time INTEGER NOT NULL,
                    start_time INTEGER,
                    end_time INTEGER,
                    exit_code INTEGER,
                    status TEXT NOT NULL,
                    cpu_time_ms INTEGER NOT NULL,
                    max_memory_bytes INTEGER NOT NULL,
                    req_nodes INTEGER NOT NULL,
                    req_cores INTEGER NOT NULL,
                    req_memory INTEGER NOT NULL,
                    array_id INTEGER,
                    array_task_id INTEGER,
                    assigned_workers TEXT NOT NULL,
                    gres_req TEXT NOT NULL,
                    cgroup_active BOOLEAN NOT NULL,
                    secret TEXT NOT NULL,
                    stdout_file_id TEXT,
                    stderr_file_id TEXT,
                    workdir_file_id TEXT,
                    output_artifacts TEXT NOT NULL DEFAULT '[]',
                    wait_for_licenses BOOLEAN NOT NULL,
                    estimated_walltime INTEGER,
                    priority_offset INTEGER,
                    dependencies TEXT,
                    dependency_specs TEXT,
                    qos TEXT NOT NULL DEFAULT 'Production'
                );",
            )
            .execute(&pool)
            .await
            .context("Failed to create SQLite table structure")?;

            // Ensure the qos column exists if the table already existed
            let _ = sqlx::query(
                "ALTER TABLE veloce_accounting ADD COLUMN qos TEXT NOT NULL DEFAULT 'Production';",
            )
            .execute(&pool)
            .await;

            let _ = sqlx::query("ALTER TABLE veloce_accounting ADD COLUMN job_name TEXT;")
                .execute(&pool)
                .await;
            let _ = sqlx::query("ALTER TABLE veloce_accounting ADD COLUMN job_comment TEXT;")
                .execute(&pool)
                .await;

            let _ = sqlx::query("ALTER TABLE veloce_accounting ADD COLUMN secret_enc TEXT;")
                .execute(&pool)
                .await;
            let _ = sqlx::query("ALTER TABLE veloce_accounting ADD COLUMN output_artifacts TEXT NOT NULL DEFAULT '[]';")
                .execute(&pool)
                .await;

            // Create indexes
            let _ =
                sqlx::query("CREATE INDEX IF NOT EXISTS idx_status ON veloce_accounting (status);")
                    .execute(&pool)
                    .await;
            let _ =
                sqlx::query("CREATE INDEX IF NOT EXISTS idx_user ON veloce_accounting (user_id);")
                    .execute(&pool)
                    .await;

            DbPool::Sqlite(pool)
        } else if database_url.starts_with("postgres:") || database_url.starts_with("postgresql:") {
            let pool = PgPoolOptions::new()
                .min_connections(10)
                .max_connections(max_connections.max(500))
                .acquire_timeout(std::time::Duration::from_secs(timeout_secs))
                .idle_timeout(std::time::Duration::from_secs(300))
                .max_lifetime(std::time::Duration::from_secs(1800))
                .connect(database_url)
                .await
                .context("Failed to connect to PostgreSQL")?;

            // Auto-initialize PostgreSQL table structure
            sqlx::query(
                "CREATE TABLE IF NOT EXISTS veloce_accounting (
                    job_id BIGINT PRIMARY KEY,
                    job_name TEXT,
                    job_comment TEXT,
                    command_line TEXT NOT NULL,
                    user_id VARCHAR(255) NOT NULL,
                    submission_time BIGINT NOT NULL,
                    start_time BIGINT,
                    end_time BIGINT,
                    exit_code INTEGER,
                    status VARCHAR(50) NOT NULL,
                    cpu_time_ms BIGINT NOT NULL,
                    max_memory_bytes BIGINT NOT NULL,
                    req_nodes INTEGER NOT NULL,
                    req_cores INTEGER NOT NULL,
                    req_memory BIGINT NOT NULL,
                    array_id BIGINT,
                    array_task_id INTEGER,
                    assigned_workers TEXT NOT NULL,
                    gres_req TEXT NOT NULL,
                    cgroup_active BOOLEAN NOT NULL,
                    secret VARCHAR(255) NOT NULL,
                    stdout_file_id VARCHAR(255),
                    stderr_file_id VARCHAR(255),
                    workdir_file_id VARCHAR(255),
                    output_artifacts TEXT NOT NULL DEFAULT '[]',
                    wait_for_licenses BOOLEAN NOT NULL,
                    estimated_walltime BIGINT,
                    priority_offset INTEGER,
                    dependencies TEXT,
                    dependency_specs TEXT,
                    qos VARCHAR(50) NOT NULL DEFAULT 'Production'
                );",
            )
            .execute(&pool)
            .await
            .context("Failed to create PostgreSQL table structure")?;

            // Ensure the qos column exists if the table already existed
            let _ = sqlx::query("ALTER TABLE veloce_accounting ADD COLUMN IF NOT EXISTS qos VARCHAR(50) NOT NULL DEFAULT 'Production';")
                .execute(&pool)
                .await;

            let _ = sqlx::query(
                "ALTER TABLE veloce_accounting ADD COLUMN IF NOT EXISTS job_name TEXT;",
            )
            .execute(&pool)
            .await;
            let _ = sqlx::query(
                "ALTER TABLE veloce_accounting ADD COLUMN IF NOT EXISTS job_comment TEXT;",
            )
            .execute(&pool)
            .await;

            let _ = sqlx::query(
                "ALTER TABLE veloce_accounting ADD COLUMN IF NOT EXISTS secret_enc TEXT;",
            )
            .execute(&pool)
            .await;
            let _ = sqlx::query("ALTER TABLE veloce_accounting ADD COLUMN IF NOT EXISTS output_artifacts TEXT NOT NULL DEFAULT '[]';")
                .execute(&pool)
                .await;

            // Create indexes
            let _ =
                sqlx::query("CREATE INDEX IF NOT EXISTS idx_status ON veloce_accounting (status);")
                    .execute(&pool)
                    .await;
            let _ =
                sqlx::query("CREATE INDEX IF NOT EXISTS idx_user ON veloce_accounting (user_id);")
                    .execute(&pool)
                    .await;
            let _ = sqlx::query("CREATE INDEX IF NOT EXISTS idx_accounting_user_status ON veloce_accounting (user_id, status);").execute(&pool).await;
            let _ = sqlx::query("CREATE INDEX IF NOT EXISTS idx_accounting_submission ON veloce_accounting (submission_time DESC);").execute(&pool).await;

            DbPool::Postgres(pool)
        } else {
            anyhow::bail!("Unsupported database connection URL: {}", database_url);
        };

        info!("Accounting RDBMS backend initialized successfully.");
        Ok(Self { pool })
    }

    #[cfg(test)]
    async fn fetch_secret_columns(&self, job_id: u64) -> Result<(String, Option<String>)> {
        match &self.pool {
            DbPool::Sqlite(pool) => {
                let row: (String, Option<String>) = sqlx::query_as(
                    "SELECT secret, secret_enc FROM veloce_accounting WHERE job_id = ?",
                )
                .bind(job_id as i64)
                .fetch_one(pool)
                .await?;
                Ok(row)
            }
            DbPool::Postgres(pool) => {
                let row: (String, Option<String>) = sqlx::query_as(
                    "SELECT secret, secret_enc FROM veloce_accounting WHERE job_id = $1",
                )
                .bind(job_id as i64)
                .fetch_one(pool)
                .await?;
                Ok(row)
            }
        }
    }
}

fn parse_job_status(status_str: &str, exit_code: Option<i32>) -> JobStatus {
    if status_str == "Pending" {
        JobStatus::Pending
    } else if status_str == "Running" {
        JobStatus::Running
    } else if status_str == "Cancelled" || status_str == "Killed" {
        JobStatus::Killed
    } else if status_str.starts_with("Failed") {
        let err = status_str
            .trim_start_matches("Failed(")
            .trim_end_matches(")")
            .to_string();
        JobStatus::Failed(err)
    } else {
        JobStatus::Completed(exit_code.unwrap_or(0))
    }
}

fn parse_qos_level(s: &str) -> QosLevel {
    match s {
        "Interactive" => QosLevel::Interactive,
        "Preemptible" => QosLevel::Preemptible,
        "Background" => QosLevel::Background,
        _ => QosLevel::Production,
    }
}

fn map_sqlite_row(row: &sqlx::sqlite::SqliteRow) -> Result<JobUsage> {
    use sqlx::Row;
    let job_id: i64 = row.try_get("job_id")?;
    let job_name: Option<String> = row.try_get("job_name").ok();
    let job_comment: Option<String> = row.try_get("job_comment").ok();
    let command_line: String = row.try_get("command_line")?;
    let user_id: String = row.try_get("user_id")?;
    let submission_time: i64 = row.try_get("submission_time")?;
    let start_time: Option<i64> = row.try_get("start_time")?;
    let end_time: Option<i64> = row.try_get("end_time")?;
    let exit_code: Option<i32> = row.try_get("exit_code")?;
    let status_str: String = row.try_get("status")?;
    let cpu_time_ms: i64 = row.try_get("cpu_time_ms")?;
    let max_memory_bytes: i64 = row.try_get("max_memory_bytes")?;
    let req_nodes: i32 = row.try_get("req_nodes")?;
    let req_cores: i32 = row.try_get("req_cores")?;
    let req_memory: i64 = row.try_get("req_memory")?;
    let array_id: Option<i64> = row.try_get("array_id")?;
    let array_task_id: Option<i32> = row.try_get("array_task_id")?;
    let assigned_workers_str: String = row.try_get("assigned_workers")?;
    let gres_req_str: String = row.try_get("gres_req")?;
    let cgroup_active: bool = row.try_get("cgroup_active")?;
    let secret_legacy: String = row.try_get("secret")?;
    let secret_enc: Option<String> = row.try_get("secret_enc").ok();
    let secret = job_secrets::global_cipher().decode_at_rest(secret_enc.as_deref(), &secret_legacy);
    let stdout_file_id: Option<String> = row.try_get("stdout_file_id")?;
    let stderr_file_id: Option<String> = row.try_get("stderr_file_id")?;
    let workdir_file_id: Option<String> = row.try_get("workdir_file_id")?;
    let output_artifacts_str: String = row
        .try_get("output_artifacts")
        .unwrap_or_else(|_| "[]".to_string());
    let wait_for_licenses: bool = row.try_get("wait_for_licenses")?;
    let estimated_walltime: Option<i64> = row.try_get("estimated_walltime")?;
    let priority_offset: Option<i32> = row.try_get("priority_offset")?;
    let dependencies_str: Option<String> = row.try_get("dependencies")?;
    let dependency_specs_str: Option<String> = row.try_get("dependency_specs")?;

    let qos_str: String = row
        .try_get("qos")
        .unwrap_or_else(|_| "Production".to_string());

    let assigned_workers: Vec<String> =
        serde_json::from_str(&assigned_workers_str).unwrap_or_default();
    let gres_req: BTreeMap<String, u64> = serde_json::from_str(&gres_req_str).unwrap_or_default();
    let dependencies: Option<Vec<u64>> =
        dependencies_str.and_then(|s| serde_json::from_str(&s).ok());
    let dependency_specs: Option<Vec<String>> =
        dependency_specs_str.and_then(|s| serde_json::from_str(&s).ok());
    let output_artifacts = serde_json::from_str(&output_artifacts_str).unwrap_or_default();

    let status = parse_job_status(&status_str, exit_code);

    Ok(JobUsage {
        container_asset: None,
        job_id: job_id as u64,
        job_name,
        job_comment,
        command_line,
        user_id,
        submission_time: submission_time as u64,
        start_time: start_time.map(|t| t as u64),
        end_time: end_time.map(|t| t as u64),
        exit_code,
        status,
        cpu_time_ms: cpu_time_ms as u64,
        max_memory_bytes: max_memory_bytes as u64,
        req_nodes: req_nodes as usize,
        req_cores: req_cores as u32,
        req_memory: req_memory as u64,
        array_id: array_id.map(|t| t as u64),
        array_task_id: array_task_id.map(|t| t as u32),
        assigned_workers,
        gres_req,
        cgroup_active,
        secret,
        stdout_file_id,
        stderr_file_id,
        workdir_file_id,
        output_artifacts,
        wait_for_licenses,
        estimated_walltime: estimated_walltime.map(|t| t as u64),
        priority_offset,
        dependencies,
        dependency_specs,
        qos: parse_qos_level(&qos_str),
    })
}

fn map_postgres_row(row: &sqlx::postgres::PgRow) -> Result<JobUsage> {
    use sqlx::Row;
    let job_id: i64 = row.try_get("job_id")?;
    let job_name: Option<String> = row.try_get("job_name").ok();
    let job_comment: Option<String> = row.try_get("job_comment").ok();
    let command_line: String = row.try_get("command_line")?;
    let user_id: String = row.try_get("user_id")?;
    let submission_time: i64 = row.try_get("submission_time")?;
    let start_time: Option<i64> = row.try_get("start_time")?;
    let end_time: Option<i64> = row.try_get("end_time")?;
    let exit_code: Option<i32> = row.try_get("exit_code")?;
    let status_str: String = row.try_get("status")?;
    let cpu_time_ms: i64 = row.try_get("cpu_time_ms")?;
    let max_memory_bytes: i64 = row.try_get("max_memory_bytes")?;
    let req_nodes: i32 = row.try_get("req_nodes")?;
    let req_cores: i32 = row.try_get("req_cores")?;
    let req_memory: i64 = row.try_get("req_memory")?;
    let array_id: Option<i64> = row.try_get("array_id")?;
    let array_task_id: Option<i32> = row.try_get("array_task_id")?;
    let assigned_workers_str: String = row.try_get("assigned_workers")?;
    let gres_req_str: String = row.try_get("gres_req")?;
    let cgroup_active: bool = row.try_get("cgroup_active")?;
    let secret_legacy: String = row.try_get("secret")?;
    let secret_enc: Option<String> = row.try_get("secret_enc").ok();
    let secret = job_secrets::global_cipher().decode_at_rest(secret_enc.as_deref(), &secret_legacy);
    let stdout_file_id: Option<String> = row.try_get("stdout_file_id")?;
    let stderr_file_id: Option<String> = row.try_get("stderr_file_id")?;
    let workdir_file_id: Option<String> = row.try_get("workdir_file_id")?;
    let output_artifacts_str: String = row
        .try_get("output_artifacts")
        .unwrap_or_else(|_| "[]".to_string());
    let wait_for_licenses: bool = row.try_get("wait_for_licenses")?;
    let estimated_walltime: Option<i64> = row.try_get("estimated_walltime")?;
    let priority_offset: Option<i32> = row.try_get("priority_offset")?;
    let dependencies_str: Option<String> = row.try_get("dependencies")?;
    let dependency_specs_str: Option<String> = row.try_get("dependency_specs")?;

    let qos_str: String = row
        .try_get("qos")
        .unwrap_or_else(|_| "Production".to_string());

    let assigned_workers: Vec<String> =
        serde_json::from_str(&assigned_workers_str).unwrap_or_default();
    let gres_req: BTreeMap<String, u64> = serde_json::from_str(&gres_req_str).unwrap_or_default();
    let dependencies: Option<Vec<u64>> =
        dependencies_str.and_then(|s| serde_json::from_str(&s).ok());
    let dependency_specs: Option<Vec<String>> =
        dependency_specs_str.and_then(|s| serde_json::from_str(&s).ok());
    let output_artifacts = serde_json::from_str(&output_artifacts_str).unwrap_or_default();

    let status = parse_job_status(&status_str, exit_code);

    Ok(JobUsage {
        container_asset: None,
        job_id: job_id as u64,
        job_name,
        job_comment,
        command_line,
        user_id,
        submission_time: submission_time as u64,
        start_time: start_time.map(|t| t as u64),
        end_time: end_time.map(|t| t as u64),
        exit_code,
        status,
        cpu_time_ms: cpu_time_ms as u64,
        max_memory_bytes: max_memory_bytes as u64,
        req_nodes: req_nodes as usize,
        req_cores: req_cores as u32,
        req_memory: req_memory as u64,
        array_id: array_id.map(|t| t as u64),
        array_task_id: array_task_id.map(|t| t as u32),
        assigned_workers,
        gres_req,
        cgroup_active,
        secret,
        stdout_file_id,
        stderr_file_id,
        workdir_file_id,
        output_artifacts,
        wait_for_licenses,
        estimated_walltime: estimated_walltime.map(|t| t as u64),
        priority_offset,
        dependencies,
        dependency_specs,
        qos: parse_qos_level(&qos_str),
    })
}

#[async_trait::async_trait]
impl AccountingStore for SqlxAccountingStore {
    async fn record_job(&self, record: &JobUsage) -> Result<()> {
        let command_line = record.command_line.clone();
        let user_id = record.user_id.clone();
        let status_str = match &record.status {
            JobStatus::Pending => "Pending".to_string(),
            JobStatus::Running => "Running".to_string(),
            JobStatus::Completed(code) => format!("Completed({})", code),
            JobStatus::Failed(err) => format!("Failed({})", err),
            JobStatus::Killed => "Cancelled".to_string(),
        };

        let assigned_workers = serde_json::to_string(&record.assigned_workers)?;
        let gres_req = serde_json::to_string(&record.gres_req)?;
        let dependencies = record
            .dependencies
            .as_ref()
            .map(|d| serde_json::to_string(d).unwrap());
        let dependency_specs = record
            .dependency_specs
            .as_ref()
            .map(|d| serde_json::to_string(d).unwrap());
        let output_artifacts = serde_json::to_string(&record.output_artifacts)?;

        let at_rest = job_secrets::global_cipher().encode_for_storage(&record.secret);

        let job_id_i64 = record.job_id as i64;
        let submission_time_i64 = record.submission_time as i64;
        let start_time_i64 = record.start_time.map(|t| t as i64);
        let end_time_i64 = record.end_time.map(|t| t as i64);
        let cpu_time_ms_i64 = record.cpu_time_ms as i64;
        let max_memory_bytes_i64 = record.max_memory_bytes as i64;
        let req_nodes_i32 = record.req_nodes as i32;
        let req_cores_i32 = record.req_cores as i32;
        let req_memory_i64 = record.req_memory as i64;
        let array_id_i64 = record.array_id.map(|t| t as i64);
        let array_task_id_i32 = record.array_task_id.map(|t| t as i32);
        let estimated_walltime_i64 = record.estimated_walltime.map(|t| t as i64);
        let priority_offset_i32 = record.priority_offset.map(|t| t as i32);
        let qos_str = match record.qos {
            QosLevel::Interactive => "Interactive",
            QosLevel::Production => "Production",
            QosLevel::Preemptible => "Preemptible",
            QosLevel::Background => "Background",
        };

        match &self.pool {
            DbPool::Sqlite(pool) => {
                sqlx::query(
                    "INSERT INTO veloce_accounting (
                        job_id, job_name, job_comment, command_line, user_id, submission_time, start_time, end_time, exit_code, status,
                        cpu_time_ms, max_memory_bytes, req_nodes, req_cores, req_memory, array_id, array_task_id,
                        assigned_workers, gres_req, cgroup_active, secret, secret_enc, stdout_file_id, stderr_file_id,
                        workdir_file_id, output_artifacts, wait_for_licenses, estimated_walltime, priority_offset, dependencies, dependency_specs, qos
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32)
                    ON CONFLICT (job_id) DO UPDATE SET
                        job_name = EXCLUDED.job_name,
                        job_comment = EXCLUDED.job_comment,
                        command_line = EXCLUDED.command_line,
                        user_id = EXCLUDED.user_id,
                        submission_time = EXCLUDED.submission_time,
                        start_time = EXCLUDED.start_time,
                        end_time = EXCLUDED.end_time,
                        exit_code = EXCLUDED.exit_code,
                        status = EXCLUDED.status,
                        cpu_time_ms = EXCLUDED.cpu_time_ms,
                        max_memory_bytes = EXCLUDED.max_memory_bytes,
                        req_nodes = EXCLUDED.req_nodes,
                        req_cores = EXCLUDED.req_cores,
                        req_memory = EXCLUDED.req_memory,
                        array_id = EXCLUDED.array_id,
                        array_task_id = EXCLUDED.array_task_id,
                        assigned_workers = EXCLUDED.assigned_workers,
                        gres_req = EXCLUDED.gres_req,
                        cgroup_active = EXCLUDED.cgroup_active,
                        secret = EXCLUDED.secret,
                        secret_enc = EXCLUDED.secret_enc,
                        stdout_file_id = EXCLUDED.stdout_file_id,
                        stderr_file_id = EXCLUDED.stderr_file_id,
                        workdir_file_id = EXCLUDED.workdir_file_id,
                        output_artifacts = EXCLUDED.output_artifacts,
                        wait_for_licenses = EXCLUDED.wait_for_licenses,
                        estimated_walltime = EXCLUDED.estimated_walltime,
                        priority_offset = EXCLUDED.priority_offset,
                        dependencies = EXCLUDED.dependencies,
                        dependency_specs = EXCLUDED.dependency_specs,
                        qos = EXCLUDED.qos;"
                )
                .bind(job_id_i64)
                .bind(&record.job_name)
                .bind(&record.job_comment)
                .bind(command_line)
                .bind(user_id)
                .bind(submission_time_i64)
                .bind(start_time_i64)
                .bind(end_time_i64)
                .bind(record.exit_code)
                .bind(status_str)
                .bind(cpu_time_ms_i64)
                .bind(max_memory_bytes_i64)
                .bind(req_nodes_i32)
                .bind(req_cores_i32)
                .bind(req_memory_i64)
                .bind(array_id_i64)
                .bind(array_task_id_i32)
                .bind(assigned_workers)
                .bind(gres_req)
                .bind(record.cgroup_active)
                .bind(&at_rest.secret_column)
                .bind(&at_rest.secret_enc)
                .bind(&record.stdout_file_id)
                .bind(&record.stderr_file_id)
                .bind(&record.workdir_file_id)
                .bind(&output_artifacts)
                .bind(record.wait_for_licenses)
                .bind(estimated_walltime_i64)
                .bind(priority_offset_i32)
                .bind(dependencies)
                .bind(dependency_specs)
                .bind(qos_str)
                .execute(pool)
                .await?;
            }
            DbPool::Postgres(pool) => {
                sqlx::query(
                    "INSERT INTO veloce_accounting (
                        job_id, job_name, job_comment, command_line, user_id, submission_time, start_time, end_time, exit_code, status,
                        cpu_time_ms, max_memory_bytes, req_nodes, req_cores, req_memory, array_id, array_task_id,
                        assigned_workers, gres_req, cgroup_active, secret, secret_enc, stdout_file_id, stderr_file_id,
                        workdir_file_id, output_artifacts, wait_for_licenses, estimated_walltime, priority_offset, dependencies, dependency_specs, qos
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32)
                    ON CONFLICT (job_id) DO UPDATE SET
                        job_name = EXCLUDED.job_name,
                        job_comment = EXCLUDED.job_comment,
                        command_line = EXCLUDED.command_line,
                        user_id = EXCLUDED.user_id,
                        submission_time = EXCLUDED.submission_time,
                        start_time = EXCLUDED.start_time,
                        end_time = EXCLUDED.end_time,
                        exit_code = EXCLUDED.exit_code,
                        status = EXCLUDED.status,
                        cpu_time_ms = EXCLUDED.cpu_time_ms,
                        max_memory_bytes = EXCLUDED.max_memory_bytes,
                        req_nodes = EXCLUDED.req_nodes,
                        req_cores = EXCLUDED.req_cores,
                        req_memory = EXCLUDED.req_memory,
                        array_id = EXCLUDED.array_id,
                        array_task_id = EXCLUDED.array_task_id,
                        assigned_workers = EXCLUDED.assigned_workers,
                        gres_req = EXCLUDED.gres_req,
                        cgroup_active = EXCLUDED.cgroup_active,
                        secret = EXCLUDED.secret,
                        secret_enc = EXCLUDED.secret_enc,
                        stdout_file_id = EXCLUDED.stdout_file_id,
                        stderr_file_id = EXCLUDED.stderr_file_id,
                        workdir_file_id = EXCLUDED.workdir_file_id,
                        output_artifacts = EXCLUDED.output_artifacts,
                        wait_for_licenses = EXCLUDED.wait_for_licenses,
                        estimated_walltime = EXCLUDED.estimated_walltime,
                        priority_offset = EXCLUDED.priority_offset,
                        dependencies = EXCLUDED.dependencies,
                        dependency_specs = EXCLUDED.dependency_specs,
                        qos = EXCLUDED.qos;"
                )
                .bind(job_id_i64)
                .bind(&record.job_name)
                .bind(&record.job_comment)
                .bind(command_line)
                .bind(user_id)
                .bind(submission_time_i64)
                .bind(start_time_i64)
                .bind(end_time_i64)
                .bind(record.exit_code)
                .bind(status_str)
                .bind(cpu_time_ms_i64)
                .bind(max_memory_bytes_i64)
                .bind(req_nodes_i32)
                .bind(req_cores_i32)
                .bind(req_memory_i64)
                .bind(array_id_i64)
                .bind(array_task_id_i32)
                .bind(assigned_workers)
                .bind(gres_req)
                .bind(record.cgroup_active)
                .bind(&at_rest.secret_column)
                .bind(&at_rest.secret_enc)
                .bind(&record.stdout_file_id)
                .bind(&record.stderr_file_id)
                .bind(&record.workdir_file_id)
                .bind(&output_artifacts)
                .bind(record.wait_for_licenses)
                .bind(estimated_walltime_i64)
                .bind(priority_offset_i32)
                .bind(dependencies)
                .bind(dependency_specs)
                .bind(qos_str)
                .execute(pool)
                .await?;
            }
        }
        Ok(())
    }

    async fn query_history(&self, filter: &HistoryFilter) -> Result<Vec<JobUsage>> {
        let (sql, bind_id, bind_start, bind_end) = match filter {
            HistoryFilter::All => (
                "SELECT * FROM veloce_accounting ORDER BY job_id ASC".to_string(),
                None, None, None
            ),
            HistoryFilter::Single(id) => (
                "SELECT * FROM veloce_accounting WHERE job_id = $1".to_string(),
                Some(*id as i64), None, None
            ),
            HistoryFilter::Range(start, end) => (
                "SELECT * FROM veloce_accounting WHERE job_id >= $1 AND job_id <= $2 ORDER BY job_id ASC".to_string(),
                None, Some(*start as i64), Some(*end as i64)
            ),
        };

        let mut results = Vec::new();
        match &self.pool {
            DbPool::Sqlite(pool) => {
                let mut query = sqlx::query(&sql);
                if let Some(id) = bind_id {
                    query = query.bind(id);
                } else if let Some(start) = bind_start {
                    if let Some(end) = bind_end {
                        query = query.bind(start).bind(end);
                    }
                }
                let rows = query.fetch_all(pool).await?;
                for row in rows {
                    results.push(map_sqlite_row(&row)?);
                }
            }
            DbPool::Postgres(pool) => {
                let mut query = sqlx::query(&sql);
                if let Some(id) = bind_id {
                    query = query.bind(id);
                } else if let Some(start) = bind_start {
                    if let Some(end) = bind_end {
                        query = query.bind(start).bind(end);
                    }
                }
                let rows = query.fetch_all(pool).await?;
                for row in rows {
                    results.push(map_postgres_row(&row)?);
                }
            }
        }
        Ok(results)
    }
}

pub struct ResilientAccountingStore {
    inner: Arc<dyn AccountingStore>,
    buffer: Arc<StdMutex<VecDeque<JobUsage>>>,
}

impl ResilientAccountingStore {
    pub fn new(inner: Arc<dyn AccountingStore>) -> Arc<Self> {
        let store = Arc::new(Self {
            inner,
            buffer: Arc::new(StdMutex::new(VecDeque::new())),
        });

        // Spawn background retry loop
        let store_clone = store.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
                let to_retry = {
                    let mut buf = store_clone.buffer.lock().unwrap();
                    let items: Vec<JobUsage> = buf.drain(..).collect();
                    items
                };

                if !to_retry.is_empty() {
                    info!(
                        "Attempting to flush {} buffered accounting records to main store...",
                        to_retry.len()
                    );
                    let mut failed = Vec::new();
                    for record in to_retry {
                        if let Err(e) = store_clone.inner.record_job(&record).await {
                            error!(
                                "Failed to record buffered job {}: {}. Re-buffering.",
                                record.job_id, e
                            );
                            failed.push(record);
                        }
                    }
                    if !failed.is_empty() {
                        let mut buf = store_clone.buffer.lock().unwrap();
                        for f in failed.into_iter().rev() {
                            buf.push_front(f);
                        }
                    } else {
                        info!("Successfully flushed all buffered accounting records.");
                    }
                }
            }
        });

        store
    }
}

#[async_trait::async_trait]
impl AccountingStore for ResilientAccountingStore {
    async fn record_job(&self, record: &JobUsage) -> Result<()> {
        if let Err(e) = self.inner.record_job(record).await {
            error!(
                "Primary accounting store record failed: {}. Buffering locally.",
                e
            );
            {
                let mut buf = self.buffer.lock().unwrap();
                if buf.len() < 1000 {
                    buf.push_back(record.clone());
                } else {
                    warn!("Local accounting retry buffer is full! Dropping oldest record to prevent memory leak.");
                    buf.pop_front();
                    buf.push_back(record.clone());
                }
            }
            // Always persist to local file fallback as well to ensure zero loss
            let _ = FileAccountingStore.record_job(record).await;
        }
        Ok(())
    }

    async fn query_history(&self, filter: &HistoryFilter) -> Result<Vec<JobUsage>> {
        match self.inner.query_history(filter).await {
            Ok(results) => Ok(results),
            Err(e) => {
                warn!(
                    "Failed to query main accounting store: {}. Querying local file fallback.",
                    e
                );
                FileAccountingStore.query_history(filter).await
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;
    use veloce_common::{JobStatus, QosLevel};

    fn sample_usage(secret: &str) -> JobUsage {
        JobUsage {
            container_asset: None,
            job_id: 42,
            job_name: Some("sample".into()),
            job_comment: None,
            command_line: "echo hi".into(),
            user_id: "alice".into(),
            submission_time: 1,
            start_time: Some(2),
            end_time: Some(3),
            exit_code: Some(0),
            status: JobStatus::Completed(0),
            cpu_time_ms: 10,
            max_memory_bytes: 1024,
            req_nodes: 1,
            req_cores: 1,
            req_memory: 512,
            array_id: None,
            array_task_id: None,
            assigned_workers: vec!["worker-1".into()],
            gres_req: BTreeMap::new(),
            cgroup_active: false,
            secret: secret.into(),
            stdout_file_id: None,
            stderr_file_id: None,
            workdir_file_id: None,
            output_artifacts: Vec::new(),
            wait_for_licenses: false,
            estimated_walltime: None,
            priority_offset: None,
            dependencies: None,
            dependency_specs: None,
            qos: QosLevel::Production,
        }
    }

    #[tokio::test]
    async fn sqlx_accounting_encrypts_secret_at_rest() {
        let key = base64::engine::general_purpose::STANDARD.encode([9u8; 32]);
        std::env::set_var("VELOCE_SECRETS_MASTER_KEY", &key);

        let store = SqlxAccountingStore::new("sqlite::memory:", 1, 5)
            .await
            .unwrap();

        let usage = sample_usage("launch-secret-xyz");
        store.record_job(&usage).await.unwrap();

        let row = store.fetch_secret_columns(42).await.unwrap();

        assert!(row.0.is_empty());
        let secret_enc = row.1.as_ref().unwrap();
        assert!(crate::job_secrets::JobSecretsCipher::is_encrypted(
            secret_enc
        ));

        let history = store
            .query_history(&HistoryFilter::Single(42))
            .await
            .unwrap();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].secret, "launch-secret-xyz");

        let export = crate::job_secrets::redact_job_usages(history);
        assert!(export[0].secret.is_empty());
    }
}
