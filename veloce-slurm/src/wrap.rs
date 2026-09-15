//! Build a `/bin/sh` wrapper that copies `VELOCE_*` into `SLURM_*` at job start.

/// Environment aliases documented in `docs/slurm-cheat-sheet.md`.
pub const SLURM_ENV_EXPORT: &str = concat!(
    r#"export SLURM_JOB_ID="${VELOCE_JOB_ID:-}" "#,
    r#"SLURM_ARRAY_JOB_ID="${VELOCE_ARRAY_JOB_ID:-}" "#,
    r#"SLURM_ARRAY_TASK_ID="${VELOCE_ARRAY_TASK_ID:-}" "#,
    r#"SLURM_NODELIST="${VELOCE_NODES:-}" "#,
    r#"SLURM_NNODES="${VELOCE_NODE_COUNT:-}" "#,
    r#"SLURM_PROCID="${VELOCE_RANK:-}"; exec "$@""#
);

/// Prefix `command` so the running job sees Slurm-named env vars.
pub fn wrap_command(command: &[String]) -> Vec<String> {
    let mut out = vec![
        "/bin/sh".to_string(),
        "-c".to_string(),
        SLURM_ENV_EXPORT.to_string(),
        "slurm-env".to_string(),
    ];
    out.extend(command.iter().cloned());
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wraps_script_argv() {
        let wrapped = wrap_command(&["./run.sh".into(), "--case".into(), "a".into()]);
        assert_eq!(wrapped[0], "/bin/sh");
        assert_eq!(wrapped[1], "-c");
        assert!(wrapped[2].contains("SLURM_JOB_ID"));
        assert_eq!(&wrapped[3..], &["slurm-env", "./run.sh", "--case", "a"]);
    }
}
