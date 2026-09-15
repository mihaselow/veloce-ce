//! Translate a parsed Slurm invocation into `veloce --json …` argv.

use crate::wrap::wrap_command;
use crate::SubmitSpec;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VeloceInvocation {
    pub args: Vec<String>,
}

pub fn submit(spec: &SubmitSpec, command: &[String], wait: bool) -> VeloceInvocation {
    let mut args = vec!["--json".to_string(), "submit".to_string()];
    push_spec(&mut args, spec);
    if wait {
        args.push("--wait".to_string());
    }
    args.extend(wrap_command(command));
    VeloceInvocation { args }
}

pub fn step(spec: &SubmitSpec, job_id: &str, command: &[String], wait: bool) -> VeloceInvocation {
    let mut args = vec!["--json".to_string(), "step".to_string()];
    if let Some(n) = spec.nodes {
        args.push("--nodes".into());
        args.push(n.to_string());
    }
    if let Some(c) = spec.cores {
        args.push("--cores".into());
        args.push(c.to_string());
    }
    for (k, n) in &spec.gres {
        args.push("--gres".into());
        args.push(format!("{k}:{n}"));
    }
    for (k, v) in &spec.extra_env {
        args.push("-e".into());
        args.push(format!("{k}={v}"));
    }
    args.push("--job-id".into());
    args.push(job_id.to_string());
    if wait {
        args.push("--wait".into());
    }
    args.extend(wrap_command(command));
    VeloceInvocation { args }
}

pub fn jobs_list(user: Option<&str>) -> VeloceInvocation {
    let mut args = vec!["--json".to_string(), "jobs".to_string(), "list".to_string()];
    match user {
        Some("__mine__") => args.push("--mine".into()),
        Some(u) => {
            args.push("--user".into());
            args.push(u.to_string());
        }
        None => {}
    }
    VeloceInvocation { args }
}

pub fn jobs_kill(job_id: &str) -> VeloceInvocation {
    VeloceInvocation {
        args: vec!["--json".into(), "jobs".into(), "kill".into(), job_id.into()],
    }
}

pub fn nodes_list() -> VeloceInvocation {
    VeloceInvocation {
        args: vec!["--json".into(), "nodes".into(), "list".into()],
    }
}

fn push_spec(args: &mut Vec<String>, spec: &SubmitSpec) {
    if let Some(n) = &spec.job_name {
        args.push("--name".into());
        args.push(n.clone());
    }
    if let Some(c) = &spec.comment {
        args.push("--comment".into());
        args.push(c.clone());
    }
    if let Some(n) = spec.nodes {
        args.push("--nodes".into());
        args.push(n.to_string());
    }
    if let Some(c) = spec.cores {
        args.push("--cores".into());
        args.push(c.to_string());
    }
    if let Some(m) = spec.mem_mb {
        args.push("--mem".into());
        args.push(m.to_string());
    }
    if let Some(t) = spec.walltime_secs {
        args.push("--walltime".into());
        args.push(t.to_string());
    }
    for (k, n) in &spec.gres {
        args.push("--gres".into());
        args.push(format!("{k}:{n}"));
    }
    if let Some(a) = &spec.array {
        args.push("--array".into());
        args.push(a.clone());
    }
    if let Some(d) = &spec.dependency {
        args.push("--dependency".into());
        args.push(d.clone());
    }
    if let Some(q) = &spec.qos {
        args.push("--qos".into());
        args.push(q.clone());
    }
    for (k, v) in &spec.extra_env {
        args.push("-e".into());
        args.push(format!("{k}={v}"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn submit_wraps_and_maps() {
        let spec = SubmitSpec {
            job_name: Some("demo".into()),
            nodes: Some(2),
            cores: Some(8),
            gres: vec![("gpu".into(), 1)],
            ..SubmitSpec::default()
        };
        let inv = submit(&spec, &["./job.sh".into()], false);
        assert_eq!(inv.args[0], "--json");
        assert_eq!(inv.args[1], "submit");
        assert!(inv.args.contains(&"--name".to_string()));
        assert!(inv.args.contains(&"--gres".to_string()));
        assert!(inv
            .args
            .windows(2)
            .any(|w| w[0] == "/bin/sh" && w[1] == "-c"));
        assert!(inv.args.iter().any(|a| a == "./job.sh"));
    }
}
