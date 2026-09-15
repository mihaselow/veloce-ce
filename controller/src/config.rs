//! Controller configuration loading and validation.

use crate::auth;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use tracing::{error, info, warn};

pub const CONFIG_FILE: &str = "veloce.toml";

pub fn cluster_env_allowlist_enabled() -> bool {
    std::env::var("VELOCE_JOB_ENV_ALLOWLIST")
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false)
}

pub fn validate_job_submission(
    binary: &str,
    env_vars: &[(String, String)],
    exec: &veloce_common::job_policy::JobExecutionOptions,
    allowed_binaries_prefixes: &[String],
) -> Result<(), String> {
    veloce_common::job_policy::validate_binary_prefix(binary, allowed_binaries_prefixes)?;
    veloce_common::job_policy::validate_submitted_env_vars(
        env_vars,
        exec,
        cluster_env_allowlist_enabled(),
    )?;
    Ok(())
}

#[derive(Deserialize, Clone, Serialize, Debug)]
#[serde(default)]
pub struct WebConfig {
    pub controller_api_key: String,
    pub fileserver_url: String,
    pub fileserver_api_key: String,
}

impl Default for WebConfig {
    fn default() -> Self {
        Self {
            controller_api_key: String::new(),
            fileserver_url: "https://127.0.0.1:9001".to_string(),
            fileserver_api_key: String::new(),
        }
    }
}

#[derive(Deserialize, Clone, Serialize, Debug)]
#[serde(default)]
pub struct AccountingConfig {
    pub backend: String,
    pub database_url: String,
    pub connection_timeout_seconds: u64,
    pub max_connections: u32,
}

impl Default for AccountingConfig {
    fn default() -> Self {
        Self {
            backend: "file".to_string(),
            database_url: String::new(),
            connection_timeout_seconds: 10,
            max_connections: 20,
        }
    }
}

#[derive(Deserialize, Clone, Default, Debug)]
#[serde(default)]
pub struct JobProfileSettings {
    pub allow_root_fallback: bool,
    pub require_impersonation: bool,
}

#[derive(Deserialize, Clone)]
#[serde(default)]
pub struct Config {
    pub(crate) bind_address: String,
    pub(crate) cert_path: String,
    pub(crate) key_path: String,
    pub api_port: Option<u16>,
    pub api_key: Option<String>,
    pub api_keys: Option<Vec<auth::StaticApiKey>>,
    pub web_dist_path: Option<String>,
    pub fileserver_url: Option<String>,
    pub fileserver_api_key: Option<String>,
    pub web_config: Option<WebConfig>,
    pub cluster_secret: Option<String>,
    pub job_retention_seconds: Option<u64>,
    pub accounting: Option<AccountingConfig>,
    pub job_profiles: HashMap<String, JobProfileSettings>,
    pub allowed_binaries_prefixes: Vec<String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            bind_address: "0.0.0.0:9000".to_string(),
            cert_path: "cert.pem".to_string(),
            key_path: "key.pem".to_string(),
            api_port: Some(8080),
            api_key: None,
            api_keys: None,
            web_dist_path: None,
            fileserver_url: Some("https://127.0.0.1:9001".to_string()),
            fileserver_api_key: None,
            web_config: None,
            cluster_secret: None,
            job_retention_seconds: Some(86400), // 24 hours
            accounting: Some(AccountingConfig::default()),
            job_profiles: HashMap::new(),
            allowed_binaries_prefixes: Vec::new(),
        }
    }
}

pub fn load_config() -> Config {
    let mut config = if let Ok(mut file) = File::open(CONFIG_FILE) {
        let mut content = String::new();
        if file.read_to_string(&mut content).is_ok() {
            if let Ok(c) = toml::from_str(&content) {
                info!("Loaded configuration from {}", CONFIG_FILE);
                c
            } else {
                error!("Failed to parse configuration file {}", CONFIG_FILE);
                Config::default()
            }
        } else {
            Config::default()
        }
    } else {
        info!("Using default configuration");
        Config::default()
    };

    // Load veloce-web.toml if exists
    if let Ok(mut file) = File::open("veloce-web.toml") {
        let mut content = String::new();
        if file.read_to_string(&mut content).is_ok() {
            if let Ok(web_conf) = toml::from_str::<WebConfig>(&content) {
                info!("Loaded web configuration from veloce-web.toml");
                config.web_config = Some(web_conf);
            } else {
                error!("Failed to parse veloce-web.toml");
            }
        }
    }

    // Wire api_key from veloce-web.toml if not configured yet
    if config.api_key.is_none() {
        if let Some(ref web_conf) = config.web_config {
            if !web_conf.controller_api_key.is_empty() {
                config.api_key = Some(web_conf.controller_api_key.clone());
            }
        }
    }

    // Load environment variable override for api_key
    if let Ok(api_key) = std::env::var("VELOCE_API_KEY") {
        config.api_key = Some(api_key.clone());
        if let Some(ref mut web_conf) = config.web_config {
            web_conf.controller_api_key = api_key;
        }
    }

    // Load static API keys from environment variable VELOCE_API_KEYS if present
    if let Ok(keys_env) = std::env::var("VELOCE_API_KEYS") {
        match serde_json::from_str::<Vec<auth::StaticApiKey>>(&keys_env) {
            Ok(parsed_keys) => {
                let mut current_keys = config.api_keys.unwrap_or_default();
                current_keys.extend(parsed_keys);
                config.api_keys = Some(current_keys);
            }
            Err(e) => {
                error!(
                    "Failed to parse VELOCE_API_KEYS JSON environment variable: {}",
                    e
                );
            }
        }
    }

    // Load environment variables override for fileserver
    if let Ok(fs_url) = std::env::var("VELOCE_FILESERVER_URL") {
        config.fileserver_url = Some(fs_url.clone());
        if let Some(ref mut web_conf) = config.web_config {
            web_conf.fileserver_url = fs_url;
        }
    }
    if let Ok(fs_key) = std::env::var("VELOCE_FILESERVER_KEY") {
        config.fileserver_api_key = Some(fs_key.clone());
        if let Some(ref mut web_conf) = config.web_config {
            web_conf.fileserver_api_key = fs_key;
        }
    }

    // Load environment variables override for accounting
    if let Ok(backend) = std::env::var("VELOCE_ACCOUNTING_BACKEND") {
        if config.accounting.is_none() {
            config.accounting = Some(AccountingConfig::default());
        }
        if let Some(ref mut acc) = config.accounting {
            acc.backend = backend;
        }
    }
    if let Ok(db_url) = std::env::var("VELOCE_ACCOUNTING_DB_URL") {
        if config.accounting.is_none() {
            config.accounting = Some(AccountingConfig::default());
        }
        if let Some(ref mut acc) = config.accounting {
            acc.database_url = db_url;
        }
    }
    if let Ok(timeout) = std::env::var("VELOCE_ACCOUNTING_TIMEOUT") {
        if let Ok(t) = timeout.parse::<u64>() {
            if config.accounting.is_none() {
                config.accounting = Some(AccountingConfig::default());
            }
            if let Some(ref mut acc) = config.accounting {
                acc.connection_timeout_seconds = t;
            }
        }
    }
    if let Ok(max_conn) = std::env::var("VELOCE_ACCOUNTING_MAX_CONNS") {
        if let Ok(m) = max_conn.parse::<u32>() {
            if config.accounting.is_none() {
                config.accounting = Some(AccountingConfig::default());
            }
            if let Some(ref mut acc) = config.accounting {
                acc.max_connections = m;
            }
        }
    }

    config
}

pub fn sanitize_database_url(url: &str) -> String {
    if !url.contains("://") {
        return url.to_string();
    }
    let parts: Vec<&str> = url.splitn(2, "://").collect();
    let scheme = parts[0];
    let rest = parts[1];

    if let Some(last_at_idx) = rest.rfind('@') {
        let credentials = &rest[..last_at_idx];
        let host_part = &rest[last_at_idx + 1..];

        if let Some(first_colon_idx) = credentials.find(':') {
            let username = &credentials[..first_colon_idx];
            let password = &credentials[first_colon_idx + 1..];
            let encoded_password = urlencoding::encode(password);
            return format!(
                "{}://{}:{}@{}",
                scheme, username, encoded_password, host_part
            );
        }
    }
    url.to_string()
}

/// SQLite in-memory databases are per-connection; pooled connections must not exceed 1.
pub fn sqlite_pool_max_connections(url: &str) -> u32 {
    if url.contains("mode=memory") || url.contains("test_containers") || url == "sqlite://" {
        1
    } else {
        5
    }
}

pub fn validate_production_config(config: &Config) {
    let allow_insecure = std::env::var("VELOCE_ALLOW_INSECURE")
        .map(|v| v == "true")
        .unwrap_or(false);

    if allow_insecure {
        warn!("VELOCE_ALLOW_INSECURE=true is set. Bypassing production configuration checks.");
        return;
    }

    if let Some(ref cluster_secret) = config.cluster_secret {
        if let Some(ref api_key) = config.api_key {
            if api_key == cluster_secret {
                error!("CRITICAL: api_key must not be equal to cluster_secret (VELOCE_SECRET). The cluster shared secret cannot be reused as a REST API key.");
                std::process::exit(1);
            }
        }

        if let Some(ref api_keys) = config.api_keys {
            let secret_hash = crate::component_registry::hash_token(cluster_secret);
            for key in api_keys {
                if key.key_hash == secret_hash {
                    error!("CRITICAL: Static API key '{}' hash matches cluster_secret (VELOCE_SECRET). The cluster shared secret cannot be reused as a REST API key.", key.id);
                    std::process::exit(1);
                }
            }
        }
    }

    if config.api_port.is_some() {
        match &config.api_key {
            None => {
                error!("CRITICAL: api_port is enabled but api_key is not configured. Configure VELOCE_API_KEY environment variable or api_key in veloce.toml.");
                std::process::exit(1);
            }
            Some(key) if key.is_empty() => {
                error!("CRITICAL: api_port is enabled but api_key is empty.");
                std::process::exit(1);
            }
            Some(key) if key == "your_controller_api_key_here" || key == "default_api_key" => {
                warn!(
                    "Using a default/example api_key is extremely insecure! Update VELOCE_API_KEY."
                );
            }
            _ => {}
        }
    }

    if config.fileserver_url.is_some() {
        match &config.fileserver_api_key {
            None => {
                error!("CRITICAL: fileserver_url is configured but fileserver_api_key is not set. Configure VELOCE_FILESERVER_KEY environment variable or fileserver_api_key in veloce.toml.");
                std::process::exit(1);
            }
            Some(key) if key.is_empty() => {
                error!("CRITICAL: fileserver_url is configured but fileserver_api_key is empty.");
                std::process::exit(1);
            }
            Some(key)
                if key == "secret-fileserver-key"
                    || key == "default_fileserver_key"
                    || key == "your_fileserver_key_here" =>
            {
                warn!("Using a default/example fileserver_api_key is extremely insecure! Update VELOCE_FILESERVER_KEY.");
            }
            _ => {}
        }
    }

    if std::env::var("VELOCE_SECRETS_MASTER_KEY")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .is_none()
    {
        warn!(
            "VELOCE_SECRETS_MASTER_KEY is not set. Job launch secrets are stored in plaintext at rest in accounting and veloce_state.bin."
        );
    }
}
