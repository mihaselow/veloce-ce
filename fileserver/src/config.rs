use clap::Parser;
use serde::Deserialize;
use std::path::PathBuf;

/// Represents the configuration for the fileserver, loaded from `veloce-fileserver.toml`.
#[derive(Deserialize)]
pub(crate) struct Config {
    pub(crate) bind_address: String,
    pub(crate) staging_dir: PathBuf,
    pub(crate) cert_path: PathBuf,
    pub(crate) key_path: PathBuf,
    pub(crate) api_key: String,
    // S3 Proxy Configuration (optional)
    #[serde(default)]
    pub(crate) s3_proxy_enabled: bool,
    #[serde(default)]
    pub(crate) s3_endpoint: String,
    #[serde(default)]
    pub(crate) s3_region: String,
    #[serde(default)]
    pub(crate) s3_access_key: String,
    #[serde(default)]
    pub(crate) s3_secret_key: String,
}

#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) staging_dir: PathBuf,
    pub(crate) api_key: String,
    pub(crate) allow_insecure: bool,
    pub(crate) s3_proxy_enabled: bool,
    pub(crate) s3_endpoint: String,
    pub(crate) s3_region: String,
    pub(crate) s3_access_key: String,
    pub(crate) s3_secret_key: String,
    pub(crate) proxy_client: reqwest::Client,
}
pub(crate) fn allow_insecure_from_env() -> bool {
    std::env::var("VELOCE_ALLOW_INSECURE")
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false)
}
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub(crate) struct Args {
    /// Bind address for the fileserver
    #[arg(short, long)]
    bind: Option<String>,
    /// API key for authentication
    #[arg(long, env = "VELOCE_FILESERVER_KEY")]
    api_key: Option<String>,
}

impl Config {
    pub fn load(args: &Args) -> Self {
        let mut config = match std::fs::read_to_string("veloce-fileserver.toml") {
            Ok(config_str) => match toml::from_str::<Config>(&config_str) {
                Ok(config) => config,
                Err(e) => {
                    tracing::error!("Failed to parse veloce-fileserver.toml: {}", e);
                    std::process::exit(1);
                }
            },
            Err(_) => Config {
                bind_address: "0.0.0.0:9001".to_string(),
                staging_dir: "staging".into(),
                cert_path: "cert.pem".into(),
                key_path: "key.pem".into(),
                api_key: "veloce_default_secret_change_me".to_string(),
                s3_proxy_enabled: false,
                s3_endpoint: "".to_string(),
                s3_region: "".to_string(),
                s3_access_key: "".to_string(),
                s3_secret_key: "".to_string(),
            },
        };

        if let Some(b) = &args.bind {
            config.bind_address = b.clone();
        }
        if let Some(key) = &args.api_key {
            config.api_key = key.clone();
        }

        if std::env::var("VELOCE_S3_PROXY_ENABLED")
            .map(|v| v == "true" || v == "1")
            .unwrap_or(false)
        {
            config.s3_proxy_enabled = true;
        }
        if let Ok(endpoint) = std::env::var("VELOCE_S3_ENDPOINT") {
            config.s3_endpoint = endpoint;
        }
        if let Ok(region) = std::env::var("VELOCE_S3_REGION") {
            config.s3_region = region;
        }
        if let Ok(access) = std::env::var("VELOCE_S3_ACCESS_KEY") {
            config.s3_access_key = access;
        }
        if let Ok(secret) = std::env::var("VELOCE_S3_SECRET_KEY") {
            config.s3_secret_key = secret;
        }

        config
    }

    pub fn into_app_state(self, allow_insecure: bool) -> AppState {
        AppState {
            staging_dir: self.staging_dir.clone(),
            api_key: self.api_key.clone(),
            allow_insecure,
            s3_proxy_enabled: self.s3_proxy_enabled,
            s3_endpoint: self.s3_endpoint,
            s3_region: self.s3_region,
            s3_access_key: self.s3_access_key,
            s3_secret_key: self.s3_secret_key,
            proxy_client: reqwest::Client::builder()
                .danger_accept_invalid_certs(true)
                .build()
                .unwrap_or_default(),
        }
    }
}
