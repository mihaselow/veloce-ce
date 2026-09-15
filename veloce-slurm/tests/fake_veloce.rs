//! Subprocess contract against a fake `veloce` on PATH. No controller.

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::Command;

fn write_fake_veloce(dir: &std::path::Path) -> PathBuf {
    let path = dir.join("veloce");
    fs::write(
        &path,
        r#"#!/bin/sh
set -eu
printf '%s\n' "$@" > "${VELOCE_FAKE_ARGV}"
mode=submit
prev=
for a in "$@"; do
  if [ "$prev" = "jobs" ] && [ "$a" = "list" ]; then mode=jobs; fi
  if [ "$prev" = "jobs" ] && [ "$a" = "kill" ]; then mode=kill; fi
  if [ "$a" = "nodes" ]; then mode=nodes; fi
  if [ "$a" = "step" ]; then mode=step; fi
  if [ "$a" = "--wait" ]; then mode=wait; fi
  prev=$a
done
case "$mode" in
  jobs) printf '%s\n' '[{"id":42,"job_name":"demo","user_id":"alice","qos":"Production","status":"Pending","req_nodes":1,"assigned_workers":[],"start_time":null}]' ;;
  nodes) printf '%s\n' '[{"id":"w1","hostname":"node-a","total_cores":8,"available_cores":8,"total_memory":8589934592,"online":true,"gres":{"gpu":1}}]' ;;
  kill) printf '%s\n' '{"ok":true,"action":"kill_job","job_id":42}' ;;
  wait) printf '%s\n' '{"type":"job","job_id":7,"status":"completed","exit_code":0}' ;;
  step) printf '%s\n' '{"type":"step","parent_job_id":7,"step_id":1,"status":"completed","exit_code":0}' ;;
  *) printf '%s\n' '{"type":"job","job_id":42}' ;;
esac
"#,
    )
    .unwrap();
    let mut perms = fs::metadata(&path).unwrap().permissions();
    perms.set_mode(0o755);
    fs::set_permissions(&path, perms).unwrap();
    path
}

fn bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_veloce-slurm"))
}

use std::sync::atomic::{AtomicU64, Ordering};

static TEST_DIR: AtomicU64 = AtomicU64::new(0);

fn tmpdir() -> PathBuf {
    let n = TEST_DIR.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("veloce-slurm-test-{}-{n}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn fake_veloce_sbatch_and_squeue() {
    let dir = tmpdir();
    let fake = write_fake_veloce(&dir);
    let argv_log = dir.join("argv.txt");
    let path = format!("{}:{}", dir.display(), std::env::var("PATH").unwrap());

    let out = Command::new(bin())
        .args(["sbatch", "--licenses=fluent:2", "--wrap", "true"])
        .env("VELOCE_BIN", &fake)
        .env("VELOCE_FAKE_ARGV", &argv_log)
        .env("PATH", &path)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "stderr={} stdout={}",
        String::from_utf8_lossy(&out.stderr),
        String::from_utf8_lossy(&out.stdout)
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("--licenses=fluent:2"));
    assert!(stderr.contains("FlexLM"));
    assert_eq!(
        String::from_utf8_lossy(&out.stdout).trim(),
        "Submitted batch job 42"
    );
    let recorded = fs::read_to_string(&argv_log).unwrap();
    assert!(recorded.contains("submit"));
    assert!(recorded.contains("--json"));

    let out = Command::new(bin())
        .args(["squeue"])
        .env("VELOCE_BIN", &fake)
        .env("VELOCE_FAKE_ARGV", &argv_log)
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("JOBID PARTITION NAME USER ST TIME NODES NODELIST"));
    assert!(stdout.contains("42 Production demo alice PD"));
}

#[test]
fn fake_veloce_sinfo_and_scancel() {
    let dir = tmpdir();
    let fake = write_fake_veloce(&dir);
    let argv_log = dir.join("argv.txt");

    let out = Command::new(bin())
        .args(["sinfo"])
        .env("VELOCE_BIN", &fake)
        .env("VELOCE_FAKE_ARGV", &argv_log)
        .output()
        .unwrap();
    assert!(out.status.success());
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("node-a idle 8"));

    let out = Command::new(bin())
        .args(["scancel", "42"])
        .env("VELOCE_BIN", &fake)
        .env("VELOCE_FAKE_ARGV", &argv_log)
        .output()
        .unwrap();
    assert!(out.status.success());
    assert!(String::from_utf8_lossy(&out.stdout).contains("Terminating job 42"));
}
