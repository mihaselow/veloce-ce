use anyhow::{Context, Result};
use std::env;
use std::path::PathBuf;
use veloce_slurm::{now_unix, run_facade, FacadeRequest, ProcessRunner, HELP};

fn main() {
    if let Err(err) = run() {
        eprintln!("veloce-slurm: {err:#}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let mut argv = env::args();
    let argv0 = argv.next().unwrap_or_else(|| "veloce-slurm".into());
    let rest: Vec<String> = argv.collect();

    if wants_top_level_help(&argv0, &rest) {
        print!("{HELP}");
        return Ok(());
    }

    let bin = veloce_bin()?;
    let job_id = env::var("VELOCE_JOB_ID").ok();
    let out = run_facade(
        FacadeRequest {
            argv0: &argv0,
            args: &rest,
            veloce_bin: &bin,
            job_id_env: job_id.as_deref(),
            now_unix: now_unix(),
            script_body: None,
        },
        &ProcessRunner,
    )?;
    eprint!("{}", out.stderr);
    print!("{}", out.stdout);
    if out.exit_code != 0 {
        std::process::exit(out.exit_code);
    }
    Ok(())
}

fn wants_top_level_help(argv0: &str, rest: &[String]) -> bool {
    if veloce_slurm::Tool::from_argv0(argv0).is_some() {
        return false;
    }
    rest.is_empty()
        || (rest.iter().any(|a| a == "--help" || a == "-h")
            && rest.first().is_none_or(|c| {
                !matches!(
                    c.as_str(),
                    "sbatch" | "srun" | "squeue" | "scancel" | "sinfo"
                )
            }))
}

fn veloce_bin() -> Result<PathBuf> {
    if let Ok(p) = env::var("VELOCE_BIN") {
        return Ok(PathBuf::from(p));
    }
    which("veloce").context("veloce not found on PATH (set VELOCE_BIN)")
}

fn which(name: &str) -> Option<PathBuf> {
    let path = env::var_os("PATH")?;
    env::split_paths(&path).find_map(|dir| {
        let candidate = dir.join(name);
        candidate.is_file().then_some(candidate)
    })
}
