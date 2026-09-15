//! Slurm-looking stdout from Veloce `--json` payloads.

use serde_json::Value;

pub fn format_skip(skip: &crate::Skip) -> String {
    format!(
        "veloce-slurm: skipping unsupported option {} ({})",
        skip.option, skip.reason
    )
}

pub fn format_sbatch_submitted(stdout: &str) -> anyhow::Result<String> {
    let v: Value = serde_json::from_str(stdout.trim()).map_err(|e| {
        anyhow::anyhow!("veloce submit did not return JSON: {e}; stdout was: {stdout}")
    })?;
    if let Some(id) = v.get("job_id").and_then(Value::as_u64) {
        return Ok(format!("Submitted batch job {id}"));
    }
    if let Some(id) = v.get("base_job_id").and_then(Value::as_u64) {
        return Ok(format!("Submitted batch job {id}"));
    }
    anyhow::bail!("veloce submit JSON missing job_id: {stdout}");
}

pub fn format_srun_wait(stdout: &str) -> anyhow::Result<(String, i32)> {
    let v: Value = serde_json::from_str(stdout.trim()).map_err(|e| {
        anyhow::anyhow!("veloce wait did not return JSON: {e}; stdout was: {stdout}")
    })?;
    let id = v
        .get("job_id")
        .or_else(|| v.get("parent_job_id"))
        .and_then(Value::as_u64);
    let status = v.get("status").and_then(Value::as_str).unwrap_or("unknown");
    let exit = v.get("exit_code").and_then(Value::as_i64).unwrap_or(0) as i32;
    let code = match status {
        "failed" => {
            if exit == 0 {
                1
            } else {
                exit
            }
        }
        "killed" => 1,
        _ => exit,
    };
    let line = match id {
        Some(id) => format!("srun: job {id} {status}"),
        None => format!("srun: {status}"),
    };
    Ok((line, code))
}

pub fn format_squeue(jobs: &Value, now_unix: u64) -> anyhow::Result<String> {
    let arr = jobs
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("veloce jobs list JSON was not an array"))?;
    let mut lines = vec!["JOBID PARTITION NAME USER ST TIME NODES NODELIST".to_string()];
    for job in arr {
        let id = json_u64(job, &["id", "job_id"]).unwrap_or(0);
        let name = json_str(job, &["job_name"]).unwrap_or_else(|| "unnamed".into());
        let user = json_str(job, &["user_id"]).unwrap_or_else(|| "-".into());
        let qos = json_str(job, &["qos"]).unwrap_or_else(|| "-".into());
        let st = status_code(job.get("status"));
        let start = json_u64(job, &["start_time"]).unwrap_or(0);
        let time = elapsed(start, now_unix, &st);
        let workers = job
            .get("assigned_workers")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(Value::as_str)
                    .map(strip_port)
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .unwrap_or_default();
        let nodes = job
            .get("req_nodes")
            .and_then(Value::as_u64)
            .unwrap_or_else(|| workers.split(',').filter(|s| !s.is_empty()).count() as u64);
        lines.push(format!(
            "{id} {qos} {name} {user} {st} {time} {nodes} {workers}"
        ));
    }
    Ok(lines.join("\n"))
}

pub fn format_sinfo(nodes: &Value) -> anyhow::Result<String> {
    let arr = nodes
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("veloce nodes list JSON was not an array"))?;
    let mut lines = vec!["NODELIST STATE CPUS MEMORY AVAIL_CPUS GRES".to_string()];
    for n in arr {
        let host = json_str(n, &["hostname"])
            .or_else(|| json_str(n, &["id"]))
            .unwrap_or_else(|| "-".into());
        let online = n.get("online").and_then(Value::as_bool).unwrap_or(false);
        let state = if online { "idle" } else { "down" };
        let cpus = json_u64(n, &["total_cores"]).unwrap_or(0);
        let avail = json_u64(n, &["available_cores"]).unwrap_or(0);
        let mem_b = json_u64(n, &["total_memory"]).unwrap_or(0);
        let mem_mb = mem_b / (1024 * 1024);
        let gres = n
            .get("gres")
            .and_then(Value::as_object)
            .map(|m| {
                m.iter()
                    .map(|(k, v)| format!("{k}:{}", v.as_u64().unwrap_or(0)))
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "(null)".into());
        lines.push(format!("{host} {state} {cpus} {mem_mb} {avail} {gres}"));
    }
    Ok(lines.join("\n"))
}

pub fn format_scancel(stdout: &str) -> anyhow::Result<String> {
    let v: Value = serde_json::from_str(stdout.trim()).map_err(|e| {
        anyhow::anyhow!("veloce kill did not return JSON: {e}; stdout was: {stdout}")
    })?;
    let id = json_u64(&v, &["job_id"]).unwrap_or(0);
    Ok(format!("scancel: Terminating job {id}"))
}

fn json_str(v: &Value, keys: &[&str]) -> Option<String> {
    for k in keys {
        if let Some(s) = v.get(*k).and_then(Value::as_str) {
            return Some(s.to_string());
        }
    }
    None
}

fn json_u64(v: &Value, keys: &[&str]) -> Option<u64> {
    for k in keys {
        if let Some(n) = v.get(*k).and_then(Value::as_u64) {
            return Some(n);
        }
    }
    None
}

fn strip_port(id: &str) -> &str {
    id.rsplit_once(':')
        .and_then(|(h, p)| {
            if p.chars().all(|c| c.is_ascii_digit()) {
                Some(h)
            } else {
                None
            }
        })
        .unwrap_or(id)
}

fn status_code(status: Option<&Value>) -> String {
    match status {
        Some(Value::String(s)) => match s.as_str() {
            "Pending" => "PD",
            "Running" => "R",
            "Killed" => "CA",
            _ => "UK",
        }
        .to_string(),
        Some(Value::Object(m)) if m.contains_key("Completed") => "CD".into(),
        Some(Value::Object(m)) if m.contains_key("Failed") => "F".into(),
        _ => "UK".into(),
    }
}

fn elapsed(start: u64, now: u64, st: &str) -> String {
    if st != "R" || start == 0 || now < start {
        return "0:00".into();
    }
    let secs = now - start;
    let h = secs / 3600;
    let m = (secs % 3600) / 60;
    let s = secs % 60;
    if h > 0 {
        format!("{h}:{m:02}:{s:02}")
    } else {
        format!("{m}:{s:02}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn squeue_columns() {
        let jobs = json!([{
            "id": 42,
            "job_name": "demo",
            "user_id": "alice",
            "qos": "Production",
            "status": "Running",
            "req_nodes": 2,
            "assigned_workers": ["worker-1:9002", "worker-2:9002"],
            "start_time": 1000
        }]);
        let out = format_squeue(&jobs, 1060).unwrap();
        assert!(out.contains("JOBID PARTITION NAME USER ST TIME NODES NODELIST"));
        assert!(out.contains("42 Production demo alice R 1:00 2 worker-1,worker-2"));
    }

    #[test]
    fn submit_envelope() {
        let line = format_sbatch_submitted(r#"{"type":"job","job_id":7}"#).unwrap();
        assert_eq!(line, "Submitted batch job 7");
    }
}
