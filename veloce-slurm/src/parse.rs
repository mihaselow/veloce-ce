//! Parse `sbatch` / `srun` argv and `#SBATCH` directives.

use anyhow::{bail, Result};

use crate::gres::parse_gres;
use crate::timeparse::parse_slurm_time;
use crate::{Skip, SubmitSpec};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tool {
    Sbatch,
    Srun,
    Squeue,
    Scancel,
    Sinfo,
}

impl Tool {
    pub fn from_argv0(argv0: &str) -> Option<Self> {
        let name = std::path::Path::new(argv0)
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or(argv0);
        match name {
            "sbatch" => Some(Self::Sbatch),
            "srun" => Some(Self::Srun),
            "squeue" => Some(Self::Squeue),
            "scancel" => Some(Self::Scancel),
            "sinfo" => Some(Self::Sinfo),
            _ => None,
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct ParsedBatch {
    pub spec: SubmitSpec,
    pub skips: Vec<Skip>,
    pub wrap: Option<String>,
    pub script: Option<String>,
    pub script_args: Vec<String>,
}

#[derive(Debug, Default, Clone)]
pub struct ParsedQueue {
    pub user: Option<String>,
    pub skips: Vec<Skip>,
}

#[derive(Debug, Default, Clone)]
pub struct ParsedCancel {
    pub job_ids: Vec<String>,
    pub skips: Vec<Skip>,
}

pub fn parse_sbatch(args: &[String], script_body: Option<&str>) -> Result<ParsedBatch> {
    let mut parsed = parse_submit_flags(args, true)?;
    if let Some(body) = script_body {
        let mut from_script = parse_submit_flags(&sbatch_lines_to_args(body), false)?;
        from_script = merge_batch(from_script, parsed);
        parsed = from_script;
    }
    if parsed.wrap.is_some() && parsed.script.is_some() {
        bail!("sbatch: cannot combine --wrap with a batch script");
    }
    if parsed.wrap.is_none() && parsed.script.is_none() {
        bail!("sbatch: missing batch script or --wrap");
    }
    Ok(parsed)
}

pub fn parse_srun(args: &[String]) -> Result<ParsedBatch> {
    let parsed = parse_submit_flags(args, true)?;
    if parsed.command_tokens().is_empty() && parsed.wrap.is_none() {
        bail!("srun: missing command");
    }
    Ok(parsed)
}

pub fn parse_squeue(args: &[String]) -> Result<ParsedQueue> {
    let mut out = ParsedQueue::default();
    let mut i = 0;
    while i < args.len() {
        let a = &args[i];
        if a == "--" {
            i += 1;
            out.skips.extend(skip_unknown_positionals(&args[i..]));
            break;
        }
        if let Some(v) = take_opt(args, &mut i, &["-u", "--user"])? {
            out.user = Some(v);
            continue;
        }
        if a == "--me" {
            out.user = Some("__mine__".to_string());
            i += 1;
            continue;
        }
        if a.starts_with('-') {
            let (opt, consumed) = skip_unknown_flag(args, i);
            out.skips.push(opt);
            i += consumed;
            continue;
        }
        out.skips.push(Skip {
            option: a.clone(),
            reason: "squeue positional arguments are not used".to_string(),
        });
        i += 1;
    }
    Ok(out)
}

pub fn parse_scancel(args: &[String]) -> Result<ParsedCancel> {
    let mut out = ParsedCancel::default();
    let mut i = 0;
    while i < args.len() {
        let a = &args[i];
        if a.starts_with('-') && a != "--" {
            let (opt, consumed) = skip_unknown_flag(args, i);
            out.skips.push(opt);
            i += consumed;
            continue;
        }
        if a == "--" {
            i += 1;
            continue;
        }
        out.job_ids.push(a.clone());
        i += 1;
    }
    if out.job_ids.is_empty() {
        bail!("scancel: missing job id");
    }
    Ok(out)
}

pub fn parse_sinfo(args: &[String]) -> Result<Vec<Skip>> {
    let mut skips = Vec::new();
    let mut i = 0;
    while i < args.len() {
        if args[i].starts_with('-') {
            let (opt, consumed) = skip_unknown_flag(args, i);
            skips.push(opt);
            i += consumed;
        } else {
            skips.push(Skip {
                option: args[i].clone(),
                reason: "sinfo positional arguments are not used".to_string(),
            });
            i += 1;
        }
    }
    Ok(skips)
}

fn merge_batch(script: ParsedBatch, cli: ParsedBatch) -> ParsedBatch {
    let mut spec = script.spec;
    let mut skips = script.skips;
    skips.extend(cli.skips);
    if cli.spec.job_name.is_some() {
        spec.job_name = cli.spec.job_name;
    }
    if cli.spec.comment.is_some() {
        spec.comment = cli.spec.comment;
    }
    if cli.spec.nodes.is_some() {
        spec.nodes = cli.spec.nodes;
    }
    if cli.spec.cores.is_some() {
        spec.cores = cli.spec.cores;
    }
    if cli.spec.mem_mb.is_some() {
        spec.mem_mb = cli.spec.mem_mb;
    }
    if cli.spec.walltime_secs.is_some() {
        spec.walltime_secs = cli.spec.walltime_secs;
    }
    if !cli.spec.gres.is_empty() {
        spec.gres = cli.spec.gres;
    }
    if cli.spec.array.is_some() {
        spec.array = cli.spec.array;
    }
    if cli.spec.dependency.is_some() {
        spec.dependency = cli.spec.dependency;
    }
    if cli.spec.qos.is_some() {
        spec.qos = cli.spec.qos;
    }
    spec.extra_env.extend(cli.spec.extra_env);
    ParsedBatch {
        spec,
        skips,
        wrap: cli.wrap.or(script.wrap),
        script: cli.script.or(script.script),
        script_args: if cli.script_args.is_empty() {
            script.script_args
        } else {
            cli.script_args
        },
    }
}

impl ParsedBatch {
    pub fn command_tokens(&self) -> Vec<String> {
        if let Some(wrap) = &self.wrap {
            vec!["/bin/sh".to_string(), "-lc".to_string(), wrap.clone()]
        } else if let Some(script) = &self.script {
            let mut cmd = vec![script.clone()];
            cmd.extend(self.script_args.iter().cloned());
            cmd
        } else {
            self.script_args.clone()
        }
    }
}

fn sbatch_lines_to_args(body: &str) -> Vec<String> {
    let mut args = Vec::new();
    for line in body.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if trimmed.starts_with("#!") {
            continue;
        }
        if let Some(rest) = trimmed.strip_prefix("#SBATCH") {
            args.extend(shellish_split(rest.trim()));
            continue;
        }
        if trimmed.starts_with('#') {
            continue;
        }
        break;
    }
    args
}

/// Split on whitespace; keep simple quoted tokens. Not a full shell parser.
fn shellish_split(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut quote: Option<char> = None;
    for c in s.chars() {
        match quote {
            Some(q) if c == q => quote = None,
            Some(_) => cur.push(c),
            None if c == '\'' || c == '"' => quote = Some(c),
            None if c.is_whitespace() => {
                if !cur.is_empty() {
                    out.push(std::mem::take(&mut cur));
                }
            }
            None => cur.push(c),
        }
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out
}

fn parse_submit_flags(args: &[String], allow_positionals: bool) -> Result<ParsedBatch> {
    let mut out = ParsedBatch::default();
    let mut i = 0;
    let mut positionals = Vec::new();
    while i < args.len() {
        let a = &args[i];
        if a == "--" {
            positionals.extend(args[i + 1..].iter().cloned());
            break;
        }
        if let Some(v) = take_opt(args, &mut i, &["-J", "--job-name"])? {
            out.spec.job_name = Some(v);
            continue;
        }
        if let Some(v) = take_opt(args, &mut i, &["-N", "--nodes"])? {
            out.spec.nodes = Some(parse_usize(&v, "--nodes")?);
            continue;
        }
        if let Some(v) = take_opt(args, &mut i, &["-c", "--cpus-per-task"])? {
            out.spec.cores = Some(parse_u32(&v, "--cpus-per-task")?);
            continue;
        }
        if let Some(v) = take_opt(args, &mut i, &["--mem"])? {
            out.spec.mem_mb = Some(parse_mem_mb(&v)?);
            continue;
        }
        if let Some(v) = take_opt(args, &mut i, &["-t", "--time"])? {
            out.spec.walltime_secs = Some(parse_slurm_time(&v)?);
            continue;
        }
        if let Some(v) = take_opt(args, &mut i, &["--gres"])? {
            let (g, skips) = parse_gres(&v)?;
            out.spec.gres.extend(g);
            out.skips.extend(skips);
            continue;
        }
        if let Some(v) = take_opt(args, &mut i, &["-a", "--array"])? {
            validate_array(&v)?;
            out.spec.array = Some(v);
            continue;
        }
        if let Some(v) = take_opt(args, &mut i, &["-d", "--dependency"])? {
            validate_dependency(&v)?;
            out.spec.dependency = Some(v);
            continue;
        }
        if let Some(v) = take_opt(args, &mut i, &["--comment"])? {
            out.spec.comment = Some(v);
            continue;
        }
        if let Some(v) = take_opt(args, &mut i, &["--wrap"])? {
            out.wrap = Some(v);
            continue;
        }
        if let Some(v) = take_opt(args, &mut i, &["--qos"])? {
            if let Some(q) = veloce_qos(&v) {
                out.spec.qos = Some(q);
            } else {
                out.skips.push(Skip {
                    option: format!("--qos={v}"),
                    reason: "not a Veloce QoS (interactive, production, preemptible, background)"
                        .to_string(),
                });
            }
            continue;
        }
        if let Some(v) = take_opt(args, &mut i, &["-p", "--partition"])? {
            if let Some(q) = veloce_qos(&v) {
                if out.spec.qos.is_none() {
                    out.spec.qos = Some(q);
                }
            } else {
                out.skips.push(Skip {
                    option: format!("--partition={v}"),
                    reason: "Veloce has no Slurm partitions; use --qos".to_string(),
                });
            }
            continue;
        }
        if let Some(v) = take_opt(args, &mut i, &["--export"])? {
            apply_export(&v, &mut out)?;
            continue;
        }
        if a == "--licenses" || a.starts_with("--licenses=") || a == "-L" {
            let (val, consumed) = take_raw_value(args, i, a, "--licenses")?;
            out.skips.push(Skip {
                option: format!("--licenses={val}"),
                reason: "Slurm license tokens are not FlexLM; use Veloce license wait when you want a real checkout".to_string(),
            });
            i += consumed;
            continue;
        }
        if a.starts_with('-') {
            if let Some((name, reason)) = known_skip_flag(a) {
                let consumed = skip_flag_arity(args, i, name);
                let shown = display_skipped(args, i, consumed);
                out.skips.push(Skip {
                    option: shown,
                    reason: reason.to_string(),
                });
                i += consumed;
                continue;
            }
            let (opt, consumed) = skip_unknown_flag(args, i);
            out.skips.push(opt);
            i += consumed;
            continue;
        }
        if allow_positionals {
            positionals.push(a.clone());
            i += 1;
            continue;
        }
        bail!("unexpected token '{a}' in #SBATCH");
    }
    if allow_positionals && !positionals.is_empty() {
        if out.wrap.is_some() {
            out.script_args = positionals;
        } else {
            out.script = Some(positionals[0].clone());
            out.script_args = positionals[1..].to_vec();
        }
    }
    Ok(out)
}

fn apply_export(v: &str, out: &mut ParsedBatch) -> Result<()> {
    let upper = v.to_ascii_uppercase();
    if upper == "NONE" || upper == "NONE," || v.is_empty() {
        return Ok(());
    }
    if upper == "ALL" || upper.starts_with("ALL,") {
        out.skips.push(Skip {
            option: format!("--export={v}"),
            reason: "Veloce does not inherit the submitter host environment (--export=ALL)"
                .to_string(),
        });
        return Ok(());
    }
    for piece in v.split(',') {
        let piece = piece.trim();
        if piece.is_empty() || piece.eq_ignore_ascii_case("NONE") {
            continue;
        }
        if piece.eq_ignore_ascii_case("ALL") {
            out.skips.push(Skip {
                option: format!("--export={piece}"),
                reason: "Veloce does not inherit the submitter host environment".to_string(),
            });
            continue;
        }
        if let Some((k, val)) = piece.split_once('=') {
            out.spec.extra_env.push((k.to_string(), val.to_string()));
        } else {
            out.skips.push(Skip {
                option: format!("--export={piece}"),
                reason: "bare --export NAME is not mapped; use NAME=value".to_string(),
            });
        }
    }
    Ok(())
}

fn veloce_qos(v: &str) -> Option<String> {
    match v.to_ascii_lowercase().as_str() {
        "interactive" | "production" | "preemptible" | "background" => Some(v.to_ascii_lowercase()),
        _ => None,
    }
}

fn validate_array(v: &str) -> Result<()> {
    if v.is_empty() {
        bail!("empty --array value");
    }
    let ok = v
        .chars()
        .all(|c| c.is_ascii_digit() || matches!(c, '-' | ',' | ':' | '%'));
    if !ok {
        bail!("malformed --array value '{v}'");
    }
    Ok(())
}

fn validate_dependency(v: &str) -> Result<()> {
    for piece in v.split(',') {
        let piece = piece.trim();
        if piece.is_empty() {
            continue;
        }
        let Some((kind, rest)) = piece.split_once(':') else {
            bail!("malformed --dependency '{piece}'");
        };
        match kind {
            "afterok" | "afterany" | "afternotok" => {
                if rest.is_empty() || !rest.chars().all(|c| c.is_ascii_digit() || c == ':') {
                    bail!("malformed --dependency '{piece}'");
                }
            }
            _ => bail!(
                "unsupported --dependency kind '{kind}' (honor afterok, afterany, afternotok only)"
            ),
        }
    }
    Ok(())
}

fn parse_mem_mb(raw: &str) -> Result<u64> {
    let s = raw.trim();
    let (num, suffix) = s
        .char_indices()
        .find(|(_, c)| c.is_ascii_alphabetic())
        .map(|(i, _)| (&s[..i], s[i..].to_ascii_uppercase()))
        .unwrap_or((s, String::new()));
    let n: f64 = num
        .parse()
        .map_err(|_| anyhow::anyhow!("invalid --mem value '{raw}'"))?;
    let mb = match suffix.trim_end_matches('B') {
        "" | "M" => n,
        "K" => n / 1024.0,
        "G" => n * 1024.0,
        "T" => n * 1024.0 * 1024.0,
        _ => bail!("invalid --mem suffix in '{raw}'"),
    };
    Ok(mb.ceil() as u64)
}

fn parse_usize(v: &str, name: &str) -> Result<usize> {
    v.parse()
        .map_err(|_| anyhow::anyhow!("invalid {name} value '{v}'"))
}

fn parse_u32(v: &str, name: &str) -> Result<u32> {
    v.parse()
        .map_err(|_| anyhow::anyhow!("invalid {name} value '{v}'"))
}

fn take_opt(args: &[String], i: &mut usize, names: &[&str]) -> Result<Option<String>> {
    let a = &args[*i];
    for name in names {
        if let Some(v) = a.strip_prefix(&format!("{name}=")) {
            *i += 1;
            return Ok(Some(v.to_string()));
        }
        if a == name {
            let v = args
                .get(*i + 1)
                .ok_or_else(|| anyhow::anyhow!("missing value for {name}"))?;
            if v.starts_with('-') && *name != "--comment" {
                bail!("missing value for {name}");
            }
            *i += 2;
            return Ok(Some(v.clone()));
        }
        // bundled short: -N2 -c8 -Jname -t10
        if name.len() == 2 && name.starts_with('-') && !name.starts_with("--") {
            if let Some(rest) = a.strip_prefix(name) {
                if !rest.is_empty() && !rest.starts_with('-') {
                    *i += 1;
                    return Ok(Some(rest.to_string()));
                }
            }
        }
    }
    Ok(None)
}

fn take_raw_value(args: &[String], i: usize, a: &str, name: &str) -> Result<(String, usize)> {
    if let Some(v) = a.strip_prefix(&format!("{name}=")) {
        return Ok((v.to_string(), 1));
    }
    let v = args
        .get(i + 1)
        .ok_or_else(|| anyhow::anyhow!("missing value for {name}"))?;
    Ok((v.clone(), 2))
}

fn known_skip_flag(a: &str) -> Option<(&'static str, &'static str)> {
    let key = a.split_once('=').map(|(k, _)| k).unwrap_or(a);
    const SKIP: &[(&[&str], &str)] = &[
        (
            &["--account", "-A"],
            "Slurm accounts are not a Veloce object",
        ),
        (
            &["--constraint", "-C"],
            "node features/constraints are not mapped",
        ),
        (&["--nodelist", "-w"], "explicit nodelist is not mapped"),
        (&["--exclude", "-x"], "node exclude lists are not mapped"),
        (
            &["--exclusive"],
            "exclusive node allocation is not a Veloce submit flag",
        ),
        (
            &["--oversubscribe"],
            "oversubscribe is not a Veloce submit flag",
        ),
        (
            &["--contiguous"],
            "contiguous allocation is not a Veloce submit flag",
        ),
        (
            &["--switches"],
            "topology/switches are intentionally out of scope",
        ),
        (&["--cpu-bind", "--cpu_bind"], "CPU bind is not mapped"),
        (&["--mem-bind", "--mem_bind"], "memory bind is not mapped"),
        (&["--hint"], "CPU hint is not mapped"),
        (&["--ntasks-per-socket"], "socket topology is not mapped"),
        (&["--ntasks-per-node"], "ntasks-per-node is not mapped"),
        (&["-n", "--ntasks"], "ntasks is not mapped; use -N/-c"),
        (&["--mem-per-cpu"], "mem-per-cpu is not mapped; use --mem"),
        (&["--mail-user"], "mail is not mapped"),
        (&["--mail-type"], "mail is not mapped"),
        (&["--begin"], "deferred start (--begin) is not mapped"),
        (&["--requeue"], "requeue is not mapped"),
        (&["--signal"], "job signals are not mapped"),
        (&["--open-mode"], "log open-mode is not mapped"),
        (&["-o", "--output"], "stdout path is not mapped"),
        (&["-e", "--error"], "stderr path is not mapped"),
        (&["-i", "--input"], "stdin path is not mapped"),
        (&["--get-user-env"], "host env import is not mapped"),
        (&["--network"], "network hints are not mapped"),
        (&["--mpi"], "MPI plugin selection is not mapped"),
        (&["--spank"], "SPANK plugins are not supported"),
        (&["-D", "--chdir"], "chdir is not mapped"),
        (&["--gpus", "--gpus-per-node"], "use --gres gpu:N"),
        (&["--tmp"], "tmp disk GRES is not mapped"),
        (&["--profile"], "Slurm acct_gather profile is not mapped"),
    ];
    for (names, reason) in SKIP {
        if names.contains(&key) {
            return Some((names[0], reason));
        }
    }
    None
}

fn skip_flag_arity(args: &[String], i: usize, canonical: &str) -> usize {
    let a = &args[i];
    if a.contains('=') {
        return 1;
    }
    if matches!(
        canonical,
        "--exclusive"
            | "--oversubscribe"
            | "--contiguous"
            | "--requeue"
            | "--get-user-env"
            | "--spank"
    ) {
        return 1;
    }
    match args.get(i + 1) {
        Some(next) if !next.starts_with('-') => 2,
        _ => 1,
    }
}

fn display_skipped(args: &[String], i: usize, consumed: usize) -> String {
    args[i..i + consumed].join("=").replace("==", "=")
}

fn skip_unknown_flag(args: &[String], i: usize) -> (Skip, usize) {
    let a = &args[i];
    let consumed = if a.contains('=') {
        1
    } else if a.starts_with("--") {
        match args.get(i + 1) {
            Some(next) if !next.starts_with('-') => 2,
            _ => 1,
        }
    } else {
        1
    };
    let option = args[i..i + consumed].join("=");
    (
        Skip {
            option,
            reason: "unknown Slurm option (not mapped to veloce submit)".to_string(),
        },
        consumed,
    )
}

fn skip_unknown_positionals(args: &[String]) -> Vec<Skip> {
    args.iter()
        .map(|a| Skip {
            option: a.clone(),
            reason: "ignored extra argument".to_string(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(args: &[&str]) -> Vec<String> {
        args.iter().map(|a| (*a).to_string()).collect()
    }

    #[test]
    fn argv0_dispatch() {
        assert_eq!(Tool::from_argv0("/usr/bin/sbatch"), Some(Tool::Sbatch));
        assert_eq!(Tool::from_argv0("veloce-slurm"), None);
    }

    #[test]
    fn honors_core_flags_and_skips_partition() {
        let body = "#!/bin/bash\n#SBATCH --job-name=demo\n#SBATCH --nodes=2\n#SBATCH --partition=gpu\n#SBATCH --time=01:00:00\necho hi\n";
        let p = parse_sbatch(&s(&["--cpus-per-task", "8", "job.sh"]), Some(body)).unwrap();
        assert_eq!(p.spec.job_name.as_deref(), Some("demo"));
        assert_eq!(p.spec.nodes, Some(2));
        assert_eq!(p.spec.cores, Some(8));
        assert_eq!(p.spec.walltime_secs, Some(3600));
        assert!(p.skips.iter().any(|s| s.option.contains("partition")));
        assert_eq!(p.script.as_deref(), Some("job.sh"));
    }

    #[test]
    fn partition_qos_translates() {
        let p = parse_sbatch(&s(&["-p", "production", "--wrap", "true"]), None).unwrap();
        assert_eq!(p.spec.qos.as_deref(), Some("production"));
        assert!(p.skips.is_empty());
    }

    #[test]
    fn cli_overrides_script() {
        let body = "#SBATCH --job-name=fromfile\n";
        let p = parse_sbatch(&s(&["-J", "fromcli", "--wrap", "hostname"]), Some(body)).unwrap();
        assert_eq!(p.spec.job_name.as_deref(), Some("fromcli"));
    }

    #[test]
    fn rejects_bad_time() {
        let err = parse_sbatch(&s(&["-t", "nope", "--wrap", "x"]), None).unwrap_err();
        assert!(err.to_string().contains("time"));
    }

    #[test]
    fn rejects_unknown_dependency_kind() {
        let err = parse_sbatch(&s(&["-d", "aftercorr:1", "--wrap", "x"]), None).unwrap_err();
        assert!(err.to_string().contains("aftercorr"));
    }

    #[test]
    fn licenses_skip() {
        let p = parse_sbatch(&s(&["--licenses=fluent:2", "--wrap", "x"]), None).unwrap();
        assert!(p.skips.iter().any(|s| s.option.contains("licenses")));
        assert!(p.skips.iter().any(|s| s.reason.contains("FlexLM")));
    }
}
