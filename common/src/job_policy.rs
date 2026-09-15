use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// Per-job execution options propagated from submit → controller → worker.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct JobExecutionOptions {
    /// When false and cluster allowlist mode is on, host environment is not inherited.
    pub inherit_host_env: bool,
    /// Optional per-job allowlist; merged with cluster defaults when allowlist mode is on.
    pub env_allowlist: Option<Vec<String>>,
    /// Optional profile name (e.g. `system` allows root fallback when impersonation is required).
    pub job_profile: Option<String>,
}

impl JobExecutionOptions {
    pub fn from_fields(
        inherit_host_env: bool,
        env_allowlist: &Option<Vec<String>>,
        job_profile: &Option<String>,
    ) -> Self {
        Self {
            inherit_host_env,
            env_allowlist: env_allowlist.clone(),
            job_profile: job_profile.clone(),
        }
    }
}

/// Returns true when the variable name is always blocked unless explicitly allowlisted.
pub fn is_blocked_env_key(key: &str) -> bool {
    match key {
        "LD_PRELOAD" | "LD_LIBRARY_PATH" | "BASH_ENV" | "ENV" | "PERL5OPT" | "PYTHONPATH" => true,
        _ if key.starts_with("DYLD_") => true,
        _ => false,
    }
}

pub fn is_system_job_profile(profile: Option<&str>) -> bool {
    profile.map(|p| p == "system").unwrap_or(false)
}

pub fn parse_env_defaults(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect()
}

fn effective_allowlist_keys(
    cluster_defaults: &[String],
    job: &JobExecutionOptions,
) -> HashSet<String> {
    let mut keys: HashSet<String> = cluster_defaults.iter().cloned().collect();
    if let Some(ref extra) = job.env_allowlist {
        keys.extend(extra.iter().cloned());
    }
    keys
}

/// Reject client-supplied env vars that use blocked keys (unless explicitly allowlisted on the job).
pub fn validate_submitted_env_vars(
    env_vars: &[(String, String)],
    job: &JobExecutionOptions,
    cluster_allowlist_enabled: bool,
) -> Result<(), String> {
    if !cluster_allowlist_enabled {
        return Ok(());
    }

    let allowlist = job
        .env_allowlist
        .as_ref()
        .map(|v| v.iter().cloned().collect::<HashSet<_>>());

    for (key, _) in env_vars {
        if is_blocked_env_key(key) {
            let explicitly_allowed = allowlist.as_ref().map(|a| a.contains(key)).unwrap_or(false);
            if !explicitly_allowed {
                return Err(format!(
                    "Environment variable '{}' is blocked by cluster policy",
                    key
                ));
            }
        }
    }
    Ok(())
}

/// Filter host environment variables before merging into the job process environment.
pub fn filter_host_env_for_job(
    host_env: impl IntoIterator<Item = (String, String)>,
    cluster_allowlist_enabled: bool,
    cluster_defaults: &[String],
    job: &JobExecutionOptions,
    impersonating: bool,
) -> HashMap<String, String> {
    let host: Vec<(String, String)> = host_env.into_iter().collect();
    let mut out = HashMap::new();

    if job.inherit_host_env || !cluster_allowlist_enabled {
        for (k, v) in &host {
            if !is_blocked_env_key(k) {
                out.insert(k.clone(), v.clone());
            }
        }
        return out;
    }

    let allowed = effective_allowlist_keys(cluster_defaults, job);
    for (k, v) in &host {
        if allowed.contains(k) && !is_blocked_env_key(k) {
            out.insert(k.clone(), v.clone());
        }
    }

    if impersonating {
        for key in ["HOME", "USER", "TMPDIR"] {
            if !out.contains_key(key) {
                if let Some((_, v)) = host.iter().find(|(k, _)| k == key) {
                    out.insert(key.to_string(), v.clone());
                }
            }
        }
    }

    out
}

/// Returns true when a missing passwd entry must fail the launch (no root fallback).
pub fn must_fail_missing_user(
    require_impersonation: bool,
    job_profile: Option<&str>,
    user_exists: bool,
) -> bool {
    require_impersonation && !user_exists && !is_system_job_profile(job_profile)
}

/// Reject submit when `binary` is outside configured prefix allowlist (empty list = allow all).
pub fn validate_binary_prefix(binary: &str, allowed_prefixes: &[String]) -> Result<(), String> {
    if allowed_prefixes.is_empty() {
        return Ok(());
    }
    if allowed_prefixes
        .iter()
        .any(|prefix| !prefix.is_empty() && binary.starts_with(prefix))
    {
        Ok(())
    } else {
        Err(format!(
            "Binary '{}' is not allowed by cluster policy (allowed_binaries_prefixes)",
            binary
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blocked_ld_preload_stripped_from_host_env() {
        let host = vec![
            ("LD_PRELOAD".into(), "/evil.so".into()),
            ("PATH".into(), "/usr/bin".into()),
            ("SECRET".into(), "leak".into()),
        ];
        let job = JobExecutionOptions::default();
        let filtered = filter_host_env_for_job(
            host.clone(),
            true,
            &parse_env_defaults("PATH,LANG,LC_ALL,TMPDIR"),
            &job,
            false,
        );
        assert!(!filtered.contains_key("LD_PRELOAD"));
        assert!(!filtered.contains_key("SECRET"));
        assert_eq!(filtered.get("PATH").unwrap(), "/usr/bin");
    }

    #[test]
    fn inherit_host_env_bypasses_allowlist_when_cluster_flag_off() {
        let host = vec![
            ("SECRET".into(), "leak".into()),
            ("PATH".into(), "/usr/bin".into()),
        ];
        let job = JobExecutionOptions {
            inherit_host_env: true,
            ..Default::default()
        };
        let filtered = filter_host_env_for_job(host, false, &[], &job, false);
        assert_eq!(filtered.get("SECRET").unwrap(), "leak");
        assert_eq!(filtered.get("PATH").unwrap(), "/usr/bin");
    }

    #[test]
    fn impersonation_adds_home_user_tmpdir_when_allowlisted() {
        let host = vec![
            ("HOME".into(), "/home/alice".into()),
            ("USER".into(), "alice".into()),
            ("TMPDIR".into(), "/tmp".into()),
            ("PATH".into(), "/usr/bin".into()),
        ];
        let job = JobExecutionOptions::default();
        let filtered = filter_host_env_for_job(
            host,
            true,
            &parse_env_defaults("PATH,HOME,USER,TMPDIR"),
            &job,
            true,
        );
        assert_eq!(filtered.get("HOME").unwrap(), "/home/alice");
        assert_eq!(filtered.get("USER").unwrap(), "alice");
        assert_eq!(filtered.get("TMPDIR").unwrap(), "/tmp");
    }

    #[test]
    fn validate_submitted_env_vars_rejects_ld_preload() {
        let job = JobExecutionOptions::default();
        let env = vec![("LD_PRELOAD".into(), "/evil.so".into())];
        let err = validate_submitted_env_vars(&env, &job, true).unwrap_err();
        assert!(err.contains("LD_PRELOAD"));
    }

    #[test]
    fn validate_submitted_env_vars_allows_explicit_job_allowlist() {
        let job = JobExecutionOptions {
            env_allowlist: Some(vec!["LD_PRELOAD".into()]),
            ..Default::default()
        };
        let env = vec![("LD_PRELOAD".into(), "/trusted.so".into())];
        validate_submitted_env_vars(&env, &job, true).unwrap();
    }

    #[test]
    fn must_fail_missing_user_when_required_and_not_system() {
        assert!(must_fail_missing_user(true, None, false));
        assert!(must_fail_missing_user(true, Some("trusted"), false));
        assert!(!must_fail_missing_user(true, Some("system"), false));
        assert!(!must_fail_missing_user(false, None, false));
        assert!(!must_fail_missing_user(true, None, true));
    }

    #[test]
    fn validate_binary_prefix_empty_allows_all() {
        validate_binary_prefix("/any/path", &[]).unwrap();
    }

    #[test]
    fn validate_binary_prefix_rejects_outside_prefix() {
        let prefixes = vec!["/usr/bin/".into(), "/opt/veloce/".into()];
        validate_binary_prefix("/bin/bash", &prefixes).unwrap_err();
        validate_binary_prefix("/usr/bin/python3", &prefixes).unwrap();
    }
}
