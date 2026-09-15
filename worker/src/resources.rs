#![allow(clippy::all)]
use crate::config::Config;
use nvml_wrapper::Nvml;
use std::sync::{Arc, OnceLock};
use std::time::SystemTime;
use sysinfo::{CpuExt, DiskExt, Pid, PidExt, ProcessExt, System, SystemExt};
use veloce_common::Resources;

pub static START_TIME: OnceLock<SystemTime> = OnceLock::new();

pub(crate) fn read_cgroup_cpu_usec(job_id: u64) -> Option<u64> {
    let path = format!("/sys/fs/cgroup/veloce/job-{}/cpu.stat", job_id);
    if let Ok(content) = std::fs::read_to_string(path) {
        for line in content.lines() {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() == 2 && parts[0] == "usage_usec" {
                if let Ok(usec) = parts[1].parse::<u64>() {
                    return Some(usec);
                }
            }
        }
    }
    None
}

pub(crate) fn read_cgroup_memory_bytes(job_id: u64) -> Option<u64> {
    let path = format!("/sys/fs/cgroup/veloce/job-{}/memory.current", job_id);
    if let Ok(content) = std::fs::read_to_string(path) {
        if let Ok(bytes) = content.trim().parse::<u64>() {
            return Some(bytes);
        }
    }
    None
}

fn collect_cgroup_pids(path: &std::path::Path, pids: &mut std::collections::HashSet<Pid>) {
    let procs_path = path.join("cgroup.procs");
    if let Ok(content) = std::fs::read_to_string(&procs_path) {
        for line in content.lines() {
            if let Ok(pid) = line.trim().parse::<usize>() {
                pids.insert(Pid::from(pid));
            }
        }
    }

    if let Ok(entries) = std::fs::read_dir(path) {
        for entry in entries.flatten() {
            let child_path = entry.path();
            if child_path.is_dir() {
                collect_cgroup_pids(&child_path, pids);
            }
        }
    }
}

pub(crate) fn get_cgroup_process_resources(job_id: u64, sys: &sysinfo::System) -> (f32, u64) {
    let path = std::path::PathBuf::from(format!("/sys/fs/cgroup/veloce/job-{}", job_id));
    let mut pids = std::collections::HashSet::new();
    collect_cgroup_pids(&path, &mut pids);

    let mut total_cpu = 0.0f32;
    let mut total_mem = 0u64;
    for pid in pids {
        if let Some(process) = sys.process(pid) {
            total_cpu += process.cpu_usage();
            total_mem += process.memory();
        }
    }

    (total_cpu, total_mem)
}

fn read_process_group_id(pid: Pid) -> Option<i32> {
    let path = format!("/proc/{}/stat", pid.as_u32());
    let content = std::fs::read_to_string(path).ok()?;
    let end_comm = content.rfind(')')?;
    let fields_after_comm = content.get(end_comm + 1..)?;
    let mut fields = fields_after_comm.split_whitespace();
    let _state = fields.next()?;
    let _ppid = fields.next()?;
    fields.next()?.parse::<i32>().ok()
}

pub(crate) fn get_process_group_resources(launcher_pid: Pid, sys: &sysinfo::System) -> (f32, u64) {
    let Some(target_pgrp) = read_process_group_id(launcher_pid) else {
        return (0.0, 0);
    };

    let mut total_cpu = 0.0f32;
    let mut total_mem = 0u64;
    for (pid, process) in sys.processes() {
        if read_process_group_id(*pid) == Some(target_pgrp) {
            total_cpu += process.cpu_usage();
            total_mem += process.memory();
        }
    }

    (total_cpu, total_mem)
}

pub(crate) fn clock_ticks_per_second() -> u64 {
    static CLOCK_TICKS: OnceLock<u64> = OnceLock::new();
    *CLOCK_TICKS.get_or_init(|| {
        let ticks = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
        if ticks > 0 {
            ticks as u64
        } else {
            100
        }
    })
}

fn page_size_bytes() -> u64 {
    static PAGE_SIZE: OnceLock<u64> = OnceLock::new();
    *PAGE_SIZE.get_or_init(|| {
        let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        if page_size > 0 {
            page_size as u64
        } else {
            4096
        }
    })
}

fn parse_proc_stat_cpu_ticks(content: &str) -> Option<u64> {
    let end_comm = content.rfind(')')?;
    let fields_after_comm = content.get(end_comm + 1..)?;
    let fields: Vec<&str> = fields_after_comm.split_whitespace().collect();
    let utime = fields.get(11)?.parse::<u64>().ok()?;
    let stime = fields.get(12)?.parse::<u64>().ok()?;
    Some(utime.saturating_add(stime))
}

pub(crate) fn read_namespace_process_totals(launcher_pid: Pid) -> Option<(u64, u64)> {
    let proc_root = std::path::PathBuf::from(format!("/proc/{}/root/proc", launcher_pid.as_u32()));
    let entries = std::fs::read_dir(proc_root).ok()?;
    let mut total_cpu_ticks = 0u64;
    let mut total_memory_bytes = 0u64;
    let mut saw_process = false;

    for entry in entries.flatten() {
        let file_name = entry.file_name();
        let Some(pid_name) = file_name.to_str() else {
            continue;
        };
        if pid_name.parse::<u32>().is_err() {
            continue;
        }

        let process_path = entry.path();
        if let Ok(stat) = std::fs::read_to_string(process_path.join("stat")) {
            if let Some(ticks) = parse_proc_stat_cpu_ticks(&stat) {
                total_cpu_ticks = total_cpu_ticks.saturating_add(ticks);
                saw_process = true;
            }
        }

        if let Ok(statm) = std::fs::read_to_string(process_path.join("statm")) {
            if let Some(resident_pages) = statm
                .split_whitespace()
                .nth(1)
                .and_then(|v| v.parse::<u64>().ok())
            {
                total_memory_bytes = total_memory_bytes
                    .saturating_add(resident_pages.saturating_mul(page_size_bytes()));
            }
        }
    }

    saw_process.then_some((total_cpu_ticks, total_memory_bytes))
}

pub(crate) fn get_process_descendants_resources(
    launcher_pid: Pid,
    sys: &sysinfo::System,
) -> (f32, u64) {
    let mut total_cpu = 0.0f32;
    let mut total_mem = 0u64;

    let mut parent_to_children = std::collections::HashMap::new();
    for (pid, process) in sys.processes() {
        if let Some(ppid) = process.parent() {
            parent_to_children
                .entry(ppid)
                .or_insert_with(Vec::new)
                .push(*pid);
        }
    }

    let mut queue = vec![launcher_pid];
    let mut visited = std::collections::HashSet::new();
    visited.insert(launcher_pid);

    let mut idx = 0;
    while idx < queue.len() {
        let current_pid = queue[idx];
        idx += 1;

        if let Some(process) = sys.process(current_pid) {
            total_cpu += process.cpu_usage();
            total_mem += process.memory();
        }

        if let Some(children) = parent_to_children.get(&current_pid) {
            for child in children {
                if visited.insert(*child) {
                    queue.push(*child);
                }
            }
        }
    }

    (total_cpu, total_mem)
}

pub(crate) fn get_resources(
    sys: &System,
    config: &Config,
    cgroup_enabled: bool,
    nvml: Option<&Arc<Nvml>>,
) -> Resources {
    let cpu_model = sys
        .cpus()
        .first()
        .map(|cpu| cpu.brand().to_string())
        .unwrap_or_else(|| "Unknown CPU".to_string());

    let load = sys.load_average();

    let mut total_disk = 0;
    let mut free_disk = 0;

    // Find disk for "/"
    for disk in sys.disks() {
        if disk.mount_point() == std::path::Path::new("/") {
            total_disk = disk.total_space();
            free_disk = disk.available_space();
            break;
        }
    }

    // Fallback: sum all if "/" not found (e.g. Windows C:\)
    if total_disk == 0 {
        for disk in sys.disks() {
            total_disk += disk.total_space();
            free_disk += disk.available_space();
        }
    }

    Resources {
        cpu_cores: sys.cpus().len(),
        total_memory: sys.total_memory(),
        free_memory: sys.free_memory(),
        cpu_usage: sys.global_cpu_info().cpu_usage(),
        cpu_model,
        arch: std::env::consts::ARCH.to_string(),
        os_name: sys.name().unwrap_or_else(|| "Unknown".to_string()),
        os_version: sys.os_version().unwrap_or_else(|| "Unknown".to_string()),
        kernel_version: sys
            .kernel_version()
            .unwrap_or_else(|| "Unknown".to_string()),
        host_name: sys.host_name().unwrap_or_else(|| "Unknown".to_string()),
        load_avg: [load.one, load.five, load.fifteen],
        disk_total: total_disk,
        disk_free: free_disk,
        uptime: START_TIME
            .get()
            .map(|t| t.elapsed().unwrap_or_default().as_secs())
            .unwrap_or(0),
        boot_time: sys.boot_time(),
        process_count: sys.processes().len() as u32,
        swap_total: sys.total_swap(),
        swap_free: sys.free_swap(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        gres: {
            let mut gres = config.gres.clone();
            // Auto-discovery: NVIDIA GPUs
            if let Some(n) = nvml {
                if let Ok(count) = n.device_count() {
                    if count > 0 {
                        gres.insert("gpu".to_string(), count as u64);
                    }
                }
            }
            gres
        },
        cgroup_enabled,
    }
}

pub(crate) fn get_disk_io() -> (u64, u64) {
    #[cfg(target_os = "linux")]
    {
        if let Ok(content) = std::fs::read_to_string("/proc/diskstats") {
            let mut read_bytes = 0;
            let mut write_bytes = 0;
            for line in content.lines() {
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() >= 13 {
                    // 5th field is read sectors, 9th field is write sectors
                    // sector size is usually 512 bytes
                    if let (Ok(r_sect), Ok(w_sect)) =
                        (parts[5].parse::<u64>(), parts[9].parse::<u64>())
                    {
                        read_bytes += r_sect * 512;
                        write_bytes += w_sect * 512;
                    }
                }
            }
            return (read_bytes, write_bytes);
        }
    }
    (0, 0)
}

pub(crate) fn find_free_port() -> std::io::Result<u16> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let port = listener.local_addr()?.port();
    Ok(port)
}
