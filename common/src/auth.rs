use serde::{Deserialize, Serialize};

/// Normalize a secret loaded from environment (docker `.env` / compose).
/// Keeps the first line only, strips surrounding quotes, rejects empty or `${...}` templates.
pub fn normalize_env_secret(value: &str) -> Option<String> {
    let first_line = value.lines().next()?.trim();
    if first_line.is_empty() || first_line.contains("${") {
        return None;
    }
    let mut normalized = first_line.to_string();
    if (normalized.starts_with('"') && normalized.ends_with('"'))
        || (normalized.starts_with('\'') && normalized.ends_with('\''))
    {
        normalized = normalized[1..normalized.len() - 1].to_string();
    }
    if normalized.is_empty() {
        None
    } else {
        Some(normalized)
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub fn hash_api_key(key: &str) -> String {
    use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(key.as_bytes());
    BASE64.encode(hasher.finalize())
}

pub const ROLE_VIEWER: &str = "viewer";
pub const ROLE_SUBMITTER: &str = "submitter";
pub const ROLE_OPERATOR: &str = "operator";
pub const ROLE_ADMIN: &str = "admin";
pub const ROLE_WORKER: &str = "worker";
pub const ROLE_CLI: &str = "cli";
pub const ROLE_MCP: &str = "mcp";
pub const ROLE_FILESERVER: &str = "fileserver";
pub const ROLE_WEB: &str = "web";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ComponentType {
    Worker,
    Client,
    Peer,
}

impl std::fmt::Display for ComponentType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ComponentType::Worker => write!(f, "worker"),
            ComponentType::Client => write!(f, "client"),
            ComponentType::Peer => write!(f, "peer"),
        }
    }
}

impl std::str::FromStr for ComponentType {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "worker" => Ok(ComponentType::Worker),
            "client" => Ok(ComponentType::Client),
            "peer" => Ok(ComponentType::Peer),
            _ => anyhow::bail!("Invalid component type: {}", s),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisteredComponent {
    pub component_id: String,
    pub component_type: ComponentType,
    pub roles: Vec<String>,
    pub created_at: u64,
    pub last_seen_at: Option<u64>,
    pub revoked: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NoiseSession {
    pub component: RegisteredComponent,
    pub remote_addr: String,
    pub authenticated_at: u64,
}
