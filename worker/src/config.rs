#![allow(clippy::all)]
use clap::Parser;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::Read;
use tracing::{error, info};

pub const CONFIG_FILE: &str = "veloce-worker.toml";

pub(crate) fn worker_env_policy() -> (bool, Vec<String>, bool) {
    let allowlist_enabled = std::env::var("VELOCE_JOB_ENV_ALLOWLIST")
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false);
    let defaults = veloce_common::job_policy::parse_env_defaults(
        &std::env::var("VELOCE_JOB_ENV_DEFAULTS")
            .unwrap_or_else(|_| "PATH,LANG,LC_ALL,TMPDIR".to_string()),
    );
    let require_impersonation = std::env::var("VELOCE_REQUIRE_USER_IMPERSONATION")
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false);
    (allowlist_enabled, defaults, require_impersonation)
}

pub(crate) fn filtered_host_env_for_launch(
    exec: &veloce_common::job_policy::JobExecutionOptions,
    impersonating: bool,
) -> std::collections::HashMap<String, String> {
    let (allowlist_enabled, defaults, _) = worker_env_policy();
    veloce_common::job_policy::filter_host_env_for_job(
        std::env::vars(),
        allowlist_enabled,
        &defaults,
        exec,
        impersonating,
    )
}

#[derive(Deserialize, Clone)]
pub struct Config {
    pub controller: Option<String>,
    pub prolog: Option<String>,
    pub epilog: Option<String>,
    pub cluster_secret: Option<String>,
    #[serde(default)]
    pub controllers: Vec<String>,
    #[serde(default)]
    pub run_as_worker_user: bool,
    pub fileserver_url: Option<String>,
    pub fileserver_api_key: Option<String>,
    pub idle_threshold_cpu: Option<f32>,
    pub idle_timeout_seconds: Option<u64>,
    #[serde(default)]
    pub gres: BTreeMap<String, u64>,
    pub worker_token: Option<String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            controller: None,
            prolog: None,
            epilog: None,
            cluster_secret: None,
            run_as_worker_user: false,
            fileserver_url: None,
            fileserver_api_key: None,
            idle_threshold_cpu: Some(0.5),
            idle_timeout_seconds: Some(600),
            gres: BTreeMap::new(),
            controllers: Vec::new(),
            worker_token: None,
        }
    }
}

pub fn load_config() -> Config {
    if let Ok(mut file) = File::open(CONFIG_FILE) {
        let mut content = String::new();
        if file.read_to_string(&mut content).is_ok() {
            if let Ok(config) = toml::from_str(&content) {
                info!("Loaded configuration from {}", CONFIG_FILE);
                return config;
            } else {
                error!("Failed to parse configuration file {}", CONFIG_FILE);
            }
        }
    }
    Config::default()
}

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// Enable simulation mode
    #[arg(long)]
    pub simulate: bool,
    /// Simulated worker ID
    #[arg(long)]
    pub sim_id: Option<String>,
    /// Simulated CPU cores
    #[arg(long, default_value_t = 64)]
    pub sim_cores: usize,
    /// Simulated memory in MB
    #[arg(long, default_value_t = 256000)]
    pub sim_memory: u64,
    /// Simulated job duration in seconds
    #[arg(long, default_value_t = 10)]
    pub sim_job_duration: u64,
    /// Controller address to connect to
    #[arg(short, long, env = "VELOCE_CONTROLLER_ADDR")]
    pub controller: Option<String>,
    /// Fileserver URL
    #[arg(long, env = "VELOCE_FILESERVER")]
    pub fileserver_url: Option<String>,
    /// Fileserver API key
    #[arg(long, env = "VELOCE_FILESERVER_KEY")]
    pub fileserver_api_key: Option<String>,
    /// Cluster secret
    #[arg(long, env = "VELOCE_SECRET")]
    pub cluster_secret: Option<String>,
    /// Idle CPU threshold
    #[arg(long, env = "VELOCE_IDLE_CPU")]
    pub idle_threshold_cpu: Option<f32>,
    /// Idle timeout in seconds
    #[arg(long, env = "VELOCE_IDLE_TIMEOUT")]
    pub idle_timeout_seconds: Option<u64>,
    /// Comma-separated list of controller addresses for HA
    #[arg(long, env = "VELOCE_CONTROLLERS")]
    pub controllers: Option<String>,
    /// CA certificate path for verifying controller
    #[arg(long, env = "VELOCE_CA_PATH")]
    pub ca_path: Option<String>,
    /// Worker registration token
    #[arg(long, env = "VELOCE_WORKER_TOKEN")]
    pub worker_token: Option<String>,
}
