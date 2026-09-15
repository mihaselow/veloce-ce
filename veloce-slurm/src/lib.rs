//! Slurm-shaped CLI facade over `veloce --json`.

mod format;
mod gres;
mod invoke;
mod parse;
mod timeparse;
mod wrap;

pub use format::{
    format_sbatch_submitted, format_scancel, format_sinfo, format_skip, format_squeue,
};
pub use invoke::VeloceInvocation;
pub use parse::{parse_sbatch, parse_scancel, parse_sinfo, parse_squeue, parse_srun, Tool};
pub use wrap::wrap_command;

use anyhow::{bail, Context, Result};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

/// Mapped `veloce submit` fields (honor set only).
#[derive(Debug, Default, Clone)]
pub struct SubmitSpec {
    pub job_name: Option<String>,
    pub comment: Option<String>,
    pub nodes: Option<usize>,
    pub cores: Option<u32>,
    pub mem_mb: Option<u64>,
    pub walltime_secs: Option<u64>,
    pub gres: Vec<(String, u64)>,
    pub array: Option<String>,
    pub dependency: Option<String>,
    pub qos: Option<String>,
    pub extra_env: Vec<(String, String)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Skip {
    pub option: String,
    pub reason: String,
}

#[derive(Debug, Clone)]
pub struct VeloceOutput {
    pub status: i32,
    pub stdout: String,
    pub stderr: String,
}

pub trait VeloceRunner {
    fn run(&self, bin: &Path, inv: &VeloceInvocation) -> Result<VeloceOutput>;
}

pub struct ProcessRunner;

impl VeloceRunner for ProcessRunner {
    fn run(&self, bin: &Path, inv: &VeloceInvocation) -> Result<VeloceOutput> {
        let out = std::process::Command::new(bin)
            .args(&inv.args)
            .output()
            .with_context(|| format!("failed to execute {}", bin.display()))?;
        Ok(VeloceOutput {
            status: out.status.code().unwrap_or(1),
            stdout: String::from_utf8_lossy(&out.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
        })
    }
}

#[derive(Debug)]
pub struct FacadeOutput {
    pub stdout: String,
    pub stderr: String,
    pub exit_code: i32,
}

pub struct FacadeRequest<'a> {
    pub argv0: &'a str,
    pub args: &'a [String],
    pub veloce_bin: &'a Path,
    pub job_id_env: Option<&'a str>,
    pub now_unix: u64,
    pub script_body: Option<String>,
}

pub fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

pub fn dispatch_tool<'a>(argv0: &str, rest: &'a [String]) -> Result<(Tool, &'a [String])> {
    if let Some(tool) = Tool::from_argv0(argv0) {
        return Ok((tool, rest));
    }
    let cmd = rest.first().map(String::as_str).unwrap_or("");
    let tool = match cmd {
        "sbatch" => Tool::Sbatch,
        "srun" => Tool::Srun,
        "squeue" => Tool::Squeue,
        "scancel" => Tool::Scancel,
        "sinfo" => Tool::Sinfo,
        "-h" | "--help" | "help" | "" => bail!("usage"),
        other => bail!("unknown command '{other}' (try sbatch, srun, squeue, scancel, sinfo)"),
    };
    Ok((tool, &rest[1..]))
}

pub fn run_facade(req: FacadeRequest<'_>, runner: &dyn VeloceRunner) -> Result<FacadeOutput> {
    let (tool, args) = match dispatch_tool(req.argv0, req.args) {
        Ok(v) => v,
        Err(e) if e.to_string() == "usage" => {
            return Ok(FacadeOutput {
                stdout: HELP.to_string(),
                stderr: String::new(),
                exit_code: 0,
            });
        }
        Err(e) => return Err(e),
    };

    match tool {
        Tool::Sbatch => run_sbatch(req, args, runner),
        Tool::Srun => run_srun(req, args, runner),
        Tool::Squeue => run_squeue(req, args, runner),
        Tool::Scancel => run_scancel(req, args, runner),
        Tool::Sinfo => run_sinfo(req, args, runner),
    }
}

fn skip_stderr(skips: &[Skip]) -> String {
    skips.iter().map(format_skip).map(|s| s + "\n").collect()
}

fn run_sbatch(
    req: FacadeRequest<'_>,
    args: &[String],
    runner: &dyn VeloceRunner,
) -> Result<FacadeOutput> {
    let mut parsed = parse::parse_sbatch(args, None)?;
    let body = if let Some(body) = req.script_body.clone() {
        Some(body)
    } else if parsed.wrap.is_none() {
        parsed
            .script
            .as_ref()
            .filter(|p| Path::new(p).is_file())
            .map(std::fs::read_to_string)
            .transpose()
            .context("failed to read batch script")?
    } else {
        None
    };
    if body.is_some() {
        parsed = parse::parse_sbatch(args, body.as_deref())?;
    }
    let mut stderr = skip_stderr(&parsed.skips);
    let cmd = parsed.command_tokens();
    let inv = invoke::submit(&parsed.spec, &cmd, false);
    let out = runner.run(req.veloce_bin, &inv)?;
    stderr.push_str(&out.stderr);
    if out.status != 0 {
        return Ok(FacadeOutput {
            stdout: out.stdout,
            stderr,
            exit_code: out.status,
        });
    }
    let stdout = format_sbatch_submitted(&out.stdout)?;
    Ok(FacadeOutput {
        stdout: stdout + "\n",
        stderr,
        exit_code: 0,
    })
}

fn run_srun(
    req: FacadeRequest<'_>,
    args: &[String],
    runner: &dyn VeloceRunner,
) -> Result<FacadeOutput> {
    let parsed = parse::parse_srun(args)?;
    let mut stderr = skip_stderr(&parsed.skips);
    let cmd = parsed.command_tokens();
    let inv = if let Some(id) = req.job_id_env.filter(|s| !s.is_empty()) {
        invoke::step(&parsed.spec, id, &cmd, true)
    } else {
        invoke::submit(&parsed.spec, &cmd, true)
    };
    let out = runner.run(req.veloce_bin, &inv)?;
    stderr.push_str(&out.stderr);
    if out.status != 0 {
        return Ok(FacadeOutput {
            stdout: out.stdout,
            stderr,
            exit_code: out.status,
        });
    }
    let (line, code) = format::format_srun_wait(&out.stdout)?;
    Ok(FacadeOutput {
        stdout: line + "\n",
        stderr,
        exit_code: code,
    })
}

fn run_squeue(
    req: FacadeRequest<'_>,
    args: &[String],
    runner: &dyn VeloceRunner,
) -> Result<FacadeOutput> {
    let parsed = parse::parse_squeue(args)?;
    let mut stderr = skip_stderr(&parsed.skips);
    let inv = invoke::jobs_list(parsed.user.as_deref());
    let out = runner.run(req.veloce_bin, &inv)?;
    stderr.push_str(&out.stderr);
    if out.status != 0 {
        return Ok(FacadeOutput {
            stdout: out.stdout,
            stderr,
            exit_code: out.status,
        });
    }
    let jobs: serde_json::Value = serde_json::from_str(out.stdout.trim())
        .with_context(|| format!("jobs list JSON: {}", out.stdout))?;
    let stdout = format_squeue(&jobs, req.now_unix)? + "\n";
    Ok(FacadeOutput {
        stdout,
        stderr,
        exit_code: 0,
    })
}

fn run_scancel(
    req: FacadeRequest<'_>,
    args: &[String],
    runner: &dyn VeloceRunner,
) -> Result<FacadeOutput> {
    let parsed = parse::parse_scancel(args)?;
    let mut stderr = skip_stderr(&parsed.skips);
    let mut stdout = String::new();
    for id in &parsed.job_ids {
        let inv = invoke::jobs_kill(id);
        let out = runner.run(req.veloce_bin, &inv)?;
        stderr.push_str(&out.stderr);
        if out.status != 0 {
            return Ok(FacadeOutput {
                stdout: out.stdout,
                stderr,
                exit_code: out.status,
            });
        }
        stdout.push_str(&format_scancel(&out.stdout)?);
        stdout.push('\n');
    }
    Ok(FacadeOutput {
        stdout,
        stderr,
        exit_code: 0,
    })
}

fn run_sinfo(
    req: FacadeRequest<'_>,
    args: &[String],
    runner: &dyn VeloceRunner,
) -> Result<FacadeOutput> {
    let skips = parse::parse_sinfo(args)?;
    let mut stderr = skip_stderr(&skips);
    let inv = invoke::nodes_list();
    let out = runner.run(req.veloce_bin, &inv)?;
    stderr.push_str(&out.stderr);
    if out.status != 0 {
        return Ok(FacadeOutput {
            stdout: out.stdout,
            stderr,
            exit_code: out.status,
        });
    }
    let nodes: serde_json::Value = serde_json::from_str(out.stdout.trim())
        .with_context(|| format!("nodes list JSON: {}", out.stdout))?;
    Ok(FacadeOutput {
        stdout: format_sinfo(&nodes)? + "\n",
        stderr,
        exit_code: 0,
    })
}

pub const HELP: &str = "\
veloce-slurm — Slurm-shaped front door for Veloce (not Slurm)

Usage:
  veloce-slurm <sbatch|srun|squeue|scancel|sinfo> [args]
  sbatch|srun|squeue|scancel|sinfo   (argv0 symlink)

Honors a subset of sbatch flags and maps them to `veloce --json`.
Unsupported options are skipped with a warning. See docs/veloce-slurm.md.

Environment:
  VELOCE_BIN     path to the veloce CLI (default: veloce on PATH)
  VELOCE_JOB_ID  when set, srun uses `veloce step` instead of submit
";

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::path::PathBuf;

    struct Fake {
        stdout: String,
        recorded: RefCell<Vec<Vec<String>>>,
    }

    impl VeloceRunner for Fake {
        fn run(&self, _bin: &Path, inv: &VeloceInvocation) -> Result<VeloceOutput> {
            self.recorded.borrow_mut().push(inv.args.clone());
            Ok(VeloceOutput {
                status: 0,
                stdout: self.stdout.clone(),
                stderr: String::new(),
            })
        }
    }

    fn req<'a>(argv0: &'a str, args: &'a [String], fake_bin: &'a Path) -> FacadeRequest<'a> {
        FacadeRequest {
            argv0,
            args,
            veloce_bin: fake_bin,
            job_id_env: None,
            now_unix: 2000,
            script_body: None,
        }
    }

    #[test]
    fn sbatch_skip_partition_and_submit() {
        let fake = Fake {
            stdout: r#"{"type":"job","job_id":42}"#.into(),
            recorded: RefCell::new(Vec::new()),
        };
        let args = vec![
            "--partition=gpu".into(),
            "--job-name".into(),
            "demo".into(),
            "--wrap".into(),
            "hostname".into(),
        ];
        let bin = PathBuf::from("/tmp/fake-veloce");
        let out = run_facade(req("sbatch", &args, &bin), &fake).unwrap();
        assert_eq!(out.stdout.trim(), "Submitted batch job 42");
        assert!(out
            .stderr
            .contains("skipping unsupported option --partition=gpu"));
        let rec = fake.recorded.borrow();
        assert!(rec[0].contains(&"--name".to_string()));
        assert!(rec[0].contains(&"demo".to_string()));
        assert!(!rec[0].iter().any(|a| a.contains("partition")));
    }

    #[test]
    fn argv0_vs_subcommand() {
        let fake = Fake {
            stdout: "[]".into(),
            recorded: RefCell::new(Vec::new()),
        };
        let bin = PathBuf::from("/tmp/fake-veloce");
        let args = vec!["squeue".into()];
        let out = run_facade(req("veloce-slurm", &args, &bin), &fake).unwrap();
        assert!(out.stdout.contains("JOBID PARTITION"));
        assert_eq!(fake.recorded.borrow()[0], vec!["--json", "jobs", "list"]);
    }

    #[test]
    fn srun_uses_step_when_job_env_set() {
        let fake = Fake {
            stdout: r#"{"type":"job","job_id":9,"status":"completed","exit_code":0}"#.into(),
            recorded: RefCell::new(Vec::new()),
        };
        let args = vec!["hostname".into()];
        let bin = PathBuf::from("/tmp/fake-veloce");
        let mut r = req("srun", &args, &bin);
        r.job_id_env = Some("9");
        let out = run_facade(r, &fake).unwrap();
        assert_eq!(out.exit_code, 0);
        let rec = &fake.recorded.borrow()[0];
        assert_eq!(rec[1], "step");
        assert!(rec.windows(2).any(|w| w[0] == "--job-id" && w[1] == "9"));
    }
}
