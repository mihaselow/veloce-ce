use aes_gcm::{
    aead::{Aead, AeadCore, KeyInit, OsRng},
    Aes256Gcm, Nonce,
};
use anyhow::{anyhow, bail, Context, Result};
use base64::Engine;
use std::sync::{Arc, OnceLock};
use tracing::warn;
use veloce_common::{JobInfo, JobUsage, PersistedState};

const PREFIX: &str = "v1:";
const NONCE_LEN: usize = 12;

static GLOBAL_CIPHER: OnceLock<Arc<JobSecretsCipher>> = OnceLock::new();

/// Ciphertext layout for accounting `secret_enc` and in-place state/file sealing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecretAtRest {
    pub secret_column: String,
    pub secret_enc: Option<String>,
}

pub struct JobSecretsCipher {
    cipher: Option<Aes256Gcm>,
}

impl JobSecretsCipher {
    pub fn from_env() -> Self {
        match std::env::var("VELOCE_SECRETS_MASTER_KEY") {
            Ok(raw) if !raw.trim().is_empty() => match Self::parse_key(&raw) {
                Ok(key) => Self {
                    cipher: Some(Aes256Gcm::new(&key.into())),
                },
                Err(e) => {
                    warn!(
                        "Invalid VELOCE_SECRETS_MASTER_KEY: {}. Job secret encryption disabled.",
                        e
                    );
                    Self { cipher: None }
                }
            },
            _ => Self { cipher: None },
        }
    }

    #[cfg(test)]
    pub fn from_test_key(key: [u8; 32]) -> Self {
        Self {
            cipher: Some(Aes256Gcm::new(&key.into())),
        }
    }

    pub fn enabled(&self) -> bool {
        self.cipher.is_some()
    }

    pub fn is_encrypted(stored: &str) -> bool {
        stored.starts_with(PREFIX)
    }

    fn parse_key(raw: &str) -> Result<[u8; 32]> {
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(raw.trim())
            .context("VELOCE_SECRETS_MASTER_KEY must be valid base64")?;
        if bytes.len() != 32 {
            bail!(
                "VELOCE_SECRETS_MASTER_KEY must decode to 32 bytes, got {}",
                bytes.len()
            );
        }
        let mut key = [0u8; 32];
        key.copy_from_slice(&bytes);
        Ok(key)
    }

    pub fn encrypt(&self, plaintext: &str) -> Result<String> {
        if plaintext.is_empty() {
            return Ok(String::new());
        }
        let cipher = self
            .cipher
            .as_ref()
            .ok_or_else(|| anyhow!("VELOCE_SECRETS_MASTER_KEY not configured"))?;
        let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
        let ciphertext = cipher
            .encrypt(&nonce, plaintext.as_bytes())
            .map_err(|_| anyhow!("job secret encryption failed"))?;
        let mut blob = nonce.to_vec();
        blob.extend(ciphertext);
        Ok(format!(
            "{}{}",
            PREFIX,
            base64::engine::general_purpose::STANDARD.encode(blob)
        ))
    }

    pub fn decrypt(&self, stored: &str) -> Result<String> {
        if stored.is_empty() {
            return Ok(String::new());
        }
        if !Self::is_encrypted(stored) {
            return Ok(stored.to_string());
        }
        let cipher = self
            .cipher
            .as_ref()
            .ok_or_else(|| anyhow!("VELOCE_SECRETS_MASTER_KEY not configured"))?;
        let blob = base64::engine::general_purpose::STANDARD
            .decode(stored.trim_start_matches(PREFIX))
            .context("invalid job secret ciphertext encoding")?;
        if blob.len() <= NONCE_LEN {
            bail!("job secret ciphertext too short");
        }
        let (nonce_bytes, ct) = blob.split_at(NONCE_LEN);
        let nonce = Nonce::from_slice(nonce_bytes);
        let plaintext = cipher
            .decrypt(nonce, ct)
            .map_err(|_| anyhow!("job secret decryption failed"))?;
        Ok(String::from_utf8(plaintext).context("job secret is not valid UTF-8")?)
    }

    pub fn encode_for_storage(&self, plaintext: &str) -> SecretAtRest {
        if plaintext.is_empty() {
            return SecretAtRest {
                secret_column: String::new(),
                secret_enc: None,
            };
        }
        if !self.enabled() {
            return SecretAtRest {
                secret_column: plaintext.to_string(),
                secret_enc: None,
            };
        }
        match self.encrypt(plaintext) {
            Ok(enc) => SecretAtRest {
                secret_column: String::new(),
                secret_enc: Some(enc),
            },
            Err(e) => {
                warn!(
                    "Failed to encrypt job secret at rest: {}. Storing legacy plaintext.",
                    e
                );
                SecretAtRest {
                    secret_column: plaintext.to_string(),
                    secret_enc: None,
                }
            }
        }
    }

    pub fn decode_at_rest(&self, secret_enc: Option<&str>, secret_legacy: &str) -> String {
        if let Some(enc) = secret_enc.filter(|s| !s.is_empty()) {
            match self.decrypt(enc) {
                Ok(s) => return s,
                Err(e) => warn!("Failed to decrypt secret_enc column: {}", e),
            }
        }
        if Self::is_encrypted(secret_legacy) {
            return self.decrypt(secret_legacy).unwrap_or_default();
        }
        secret_legacy.to_string()
    }

    pub fn seal_usage_secret(&self, usage: &mut JobUsage) {
        if usage.secret.is_empty() || Self::is_encrypted(&usage.secret) {
            return;
        }
        if let Ok(enc) = self.encrypt(&usage.secret) {
            usage.secret = enc;
        }
    }

    pub fn open_usage_secret(&self, usage: &mut JobUsage) {
        if usage.secret.is_empty() {
            return;
        }
        if Self::is_encrypted(&usage.secret) {
            if let Ok(plain) = self.decrypt(&usage.secret) {
                usage.secret = plain;
            }
        }
    }

    pub fn prepare_persisted_state_for_disk(&self, state: &mut PersistedState) {
        for job in state.jobs.values_mut() {
            self.seal_job_secret(job);
        }
    }

    pub fn restore_persisted_state(&self, state: &mut PersistedState) {
        for job in state.jobs.values_mut() {
            self.open_job_secret(job);
        }
    }

    fn seal_job_secret(&self, job: &mut JobInfo) {
        if job.secret.is_empty() || Self::is_encrypted(&job.secret) {
            return;
        }
        if let Ok(enc) = self.encrypt(&job.secret) {
            job.secret = enc;
        }
    }

    fn open_job_secret(&self, job: &mut JobInfo) {
        if job.secret.is_empty() {
            return;
        }
        if Self::is_encrypted(&job.secret) {
            if let Ok(plain) = self.decrypt(&job.secret) {
                job.secret = plain;
            }
        }
    }
}

pub fn global_cipher() -> Arc<JobSecretsCipher> {
    GLOBAL_CIPHER
        .get_or_init(|| Arc::new(JobSecretsCipher::from_env()))
        .clone()
}

pub fn prepare_persisted_state_for_disk(state: &mut PersistedState) {
    global_cipher().prepare_persisted_state_for_disk(state);
}

pub fn restore_persisted_state(state: &mut PersistedState) {
    global_cipher().restore_persisted_state(state);
}

pub fn redact_job_usages(mut usages: Vec<JobUsage>) -> Vec<JobUsage> {
    for usage in &mut usages {
        usage.secret.clear();
    }
    usages
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;

    fn test_cipher() -> JobSecretsCipher {
        JobSecretsCipher::from_test_key([7u8; 32])
    }

    #[test]
    fn encrypt_decrypt_roundtrip() {
        let cipher = test_cipher();
        let enc = cipher.encrypt("launch-token-42").unwrap();
        assert!(JobSecretsCipher::is_encrypted(&enc));
        assert_eq!(cipher.decrypt(&enc).unwrap(), "launch-token-42");
    }

    #[test]
    fn encode_for_storage_clears_plaintext_column() {
        let cipher = test_cipher();
        let at_rest = cipher.encode_for_storage("sensitive");
        assert!(at_rest.secret_column.is_empty());
        assert!(at_rest.secret_enc.as_ref().unwrap().starts_with(PREFIX));
    }

    #[test]
    fn decode_at_rest_prefers_secret_enc() {
        let cipher = test_cipher();
        let at_rest = cipher.encode_for_storage("from-enc");
        let plain = cipher.decode_at_rest(at_rest.secret_enc.as_deref(), "legacy");
        assert_eq!(plain, "from-enc");
    }

    #[test]
    fn state_roundtrip_no_plaintext_on_disk() {
        let cipher = test_cipher();
        let mut state = PersistedState {
            jobs: [(
                1u64,
                JobInfo {
                    container_asset: None,
                    id: 1,
                    job_name: Some("secret-test".into()),
                    job_comment: None,
                    binary: "test".into(),
                    args: vec![],
                    status: veloce_common::JobStatus::Pending,
                    req_nodes: 1,
                    req_cores: 1,
                    req_memory: 100,
                    assigned_workers: vec![],
                    walltime: 0,
                    start_time: None,
                    priority: 0,
                    user_id: "alice".into(),
                    working_directory: "/".into(),
                    queued_time: 0,
                    current_cpu_usage: 0.0,
                    current_memory_usage: 0,
                    is_idle: false,
                    idle_duration: 0,
                    end_time: None,
                    reason: None,
                    array_id: None,
                    array_task_id: None,
                    inputs: Vec::new(),
                    cgroup_active: false,
                    gres_req: Default::default(),
                    allocated_cores: Default::default(),
                    allocated_gres: Default::default(),
                    mpi_stats: None,
                    env_vars: Vec::new(),
                    secret: "super-secret-launch-token".into(),
                    stdout_file_id: None,
                    stderr_file_id: None,
                    workdir_file_id: None,
                    output_artifacts: Vec::new(),
                    wait_for_licenses: false,
                    estimated_walltime: None,
                    priority_offset: None,
                    dependencies: None,
                    dependency_specs: None,
                    qos: veloce_common::QosLevel::Production,
                    vnc_enabled: false,
                    inherit_host_env: false,
                    env_allowlist: None,
                    job_profile: None,
                    interactive_port: None,
                },
            )]
            .into_iter()
            .collect(),
            queue: VecDeque::new(),
            next_job_id: 2,
            usage_tracker: veloce_common::UsageTracker::new(),
            steps: Default::default(),
            next_step_id: 1,
            reservations: Default::default(),
        };

        cipher.prepare_persisted_state_for_disk(&mut state);
        let on_disk = bincode::serialize(&state).unwrap();
        let blob = String::from_utf8_lossy(&on_disk);
        assert!(!blob.contains("super-secret-launch-token"));
        assert!(blob.contains(PREFIX));

        cipher.restore_persisted_state(&mut state);
        assert_eq!(
            state.jobs.get(&1).unwrap().secret,
            "super-secret-launch-token"
        );
    }

    #[test]
    fn redact_job_usages_clears_secret() {
        let usages = vec![JobUsage {
            container_asset: None,
            job_id: 1,
            job_name: Some("usage-test".into()),
            job_comment: None,
            command_line: "echo".into(),
            user_id: "u".into(),
            submission_time: 0,
            start_time: None,
            end_time: None,
            exit_code: None,
            status: veloce_common::JobStatus::Completed(0),
            cpu_time_ms: 0,
            max_memory_bytes: 0,
            req_nodes: 1,
            req_cores: 1,
            req_memory: 0,
            array_id: None,
            array_task_id: None,
            assigned_workers: vec![],
            gres_req: Default::default(),
            cgroup_active: false,
            secret: "hidden".into(),
            stdout_file_id: None,
            stderr_file_id: None,
            workdir_file_id: None,
            output_artifacts: Vec::new(),
            wait_for_licenses: false,
            estimated_walltime: None,
            priority_offset: None,
            dependencies: None,
            dependency_specs: None,
            qos: veloce_common::QosLevel::Production,
        }];
        let redacted = redact_job_usages(usages);
        assert!(redacted[0].secret.is_empty());
    }
}
