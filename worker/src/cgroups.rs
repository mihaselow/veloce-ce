use anyhow::{Context, Result};
use log::{debug, info, warn};
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};

pub struct CgroupManager {
    root_path: PathBuf,
    enabled: bool,
}

impl CgroupManager {
    pub fn new() -> Self {
        let root = PathBuf::from("/sys/fs/cgroup/veloce");
        Self {
            root_path: root,
            enabled: false,
        }
    }

    pub fn init(&mut self) -> Result<()> {
        info!("Initializing Cgroup v2 manager...");

        // 1. Check if cgroup v2 is mounted
        let mount_check = Path::new("/sys/fs/cgroup/cgroup.controllers");
        if !mount_check.exists() {
            warn!(
                "Cgroup v2 not detected ({} not found). Falling back to traditional limits.",
                mount_check.display()
            );
            self.enabled = false;
            return Ok(());
        }

        let mut available = String::new();
        if let Ok(_) =
            fs::File::open(mount_check).and_then(|mut f| f.read_to_string(&mut available))
        {
            info!("Available cgroup v2 controllers: {}", available.trim());
        }

        // 2. Move ourselves to a sub-cgroup to satisfy "no internal processes" rule
        let worker_cgroup = Path::new("/sys/fs/cgroup/worker");
        if !worker_cgroup.exists() {
            let _ = fs::create_dir(worker_cgroup);
        }
        let worker_procs = worker_cgroup.join("cgroup.procs");
        if worker_procs.exists() {
            debug!("Moving worker to its own cgroup to satisfy 'no internal processes' rule...");
            let _ = fs::write(worker_procs, "0"); // "0" moves the current process
        }

        // 3. Enable controllers in the container's root subtree_control
        let root_subtree = Path::new("/sys/fs/cgroup/cgroup.subtree_control");
        if root_subtree.exists() {
            let mut current = String::new();
            if let Ok(_) =
                fs::File::open(root_subtree).and_then(|mut f| f.read_to_string(&mut current))
            {
                let mut to_enable = Vec::new();
                if !current.contains("cpu") {
                    to_enable.push("+cpu");
                }
                if !current.contains("memory") {
                    to_enable.push("+memory");
                }
                if !current.contains("pids") {
                    to_enable.push("+pids");
                }

                if !to_enable.is_empty() {
                    let val = to_enable.join(" ");
                    debug!("Enabling controllers in /sys/fs/cgroup: {}", val);
                    if let Err(e) = fs::write(root_subtree, &val) {
                        warn!("Could not enable controllers in /sys/fs/cgroup: {}. Resource isolation might be limited.", e);
                    }
                }
            }
        }

        // 4. Create Veloce root directory
        if !self.root_path.exists() {
            debug!(
                "Creating Veloce cgroup root at {}",
                self.root_path.display()
            );
            fs::create_dir_all(&self.root_path).context("Failed to create Veloce cgroup root")?;
        }

        // 5. Enable controllers in Veloce subtree for job sub-cgroups
        let veloce_subtree = self.root_path.join("cgroup.subtree_control");
        if veloce_subtree.exists() {
            debug!("Enabling controllers in Veloce subtree: +cpu +memory +pids");
            if let Err(e) = fs::write(&veloce_subtree, "+cpu +memory +pids") {
                warn!("Failed to enable controllers in Veloce subtree {}: {}. Resource limits will NOT be enforced via cgroups.", veloce_subtree.display(), e);
                self.enabled = false;
                return Ok(());
            }
        } else {
            warn!(
                "Veloce subtree control file {} not found. Delegation failed.",
                veloce_subtree.display()
            );
            self.enabled = false;
            return Ok(());
        }

        self.enabled = true;
        info!("Cgroup v2 manager initialized successfully and enabled.");
        Ok(())
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    fn get_job_path(&self, job_id: u64) -> PathBuf {
        self.root_path.join(format!("job-{}", job_id))
    }

    pub fn create_job_cgroup(&self, job_id: u64) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }

        let path = self.get_job_path(job_id);
        info!("Creating cgroup for job {}: {}", job_id, path.display());

        if path.exists() {
            debug!(
                "Cgroup already exists for job {}, cleaning up first",
                job_id
            );
            let _ = self.cleanup_job_cgroup(job_id);
        }

        fs::create_dir(&path)
            .with_context(|| format!("Failed to create cgroup for job {}", job_id))?;
        Ok(())
    }

    pub fn add_process(&self, job_id: u64, pid: u32) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }

        let path = self.get_job_path(job_id).join("cgroup.procs");
        debug!("Adding PID {} to job {} cgroup", pid, job_id);

        fs::write(&path, pid.to_string())
            .with_context(|| format!("Failed to add PID {} to cgroup {}", pid, path.display()))?;

        Ok(())
    }

    pub fn set_limits(
        &self,
        job_id: u64,
        memory_mb: u64,
        cores: u32,
        allocated_gres: &std::collections::HashMap<String, Vec<u32>>,
    ) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }

        let path = self.get_job_path(job_id);

        // Memory Limit
        if memory_mb > 0 {
            let bytes = memory_mb * 1024 * 1024;
            let mem_max = path.join("memory.max");
            debug!(
                "Setting memory limit for job {} to {} MB",
                job_id, memory_mb
            );
            fs::write(&mem_max, bytes.to_string())?;
        }

        // CPU Limit
        if cores > 0 {
            let cpu_max = path.join("cpu.max");
            let quota = cores * 100000;
            let val = format!("{} 100000", quota);
            debug!(
                "Setting CPU quota for job {} to {} ({} cores)",
                job_id, val, cores
            );
            fs::write(&cpu_max, val)?;
        }

        // PIDs Limit (Safety)
        let pids_max = path.join("pids.max");
        let _ = fs::write(&pids_max, "10000"); // Standard safety limit

        // Device Isolation (GRES)
        for (name, ids) in allocated_gres {
            if name == "gpu" {
                info!("Job {} assigned GPUs: {:?}", job_id, ids);
                if let Err(e) = crate::bpf_filter::attach_gpu_filter(&path, ids) {
                    warn!("BPF cgroup device filtering failed for job {}: {}. Falling back to logical environment isolation.", job_id, e);
                } else {
                    info!(
                        "BPF device filter loaded successfully and attached to cgroup job-{}",
                        job_id
                    );
                }
            }
        }

        Ok(())
    }

    pub fn cleanup_job_cgroup(&self, job_id: u64) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }

        let path = self.get_job_path(job_id);
        if !path.exists() {
            return Ok(());
        }

        info!("Cleaning up cgroup for job {}: {}", job_id, path.display());

        // We should move any remaining processes out of the cgroup?
        // Usually, if the job is dead, cgroup.procs should be empty.
        // If not, we might need to kill them or move them to root.

        // Recursive removal of cgroups can be tricky if processes are still inside.
        // We'll try to remove it.
        let mut attempts = 0;
        while attempts < 5 {
            match fs::remove_dir(&path) {
                Ok(_) => {
                    debug!("Cgroup for job {} removed successfully", job_id);
                    return Ok(());
                }
                Err(e) => {
                    debug!(
                        "Attempt {} to remove cgroup for job {} failed: {}. Retrying in 100ms...",
                        attempts + 1,
                        job_id,
                        e
                    );
                    std::thread::sleep(std::time::Duration::from_millis(100));
                    attempts += 1;
                }
            }
        }

        warn!(
            "Failed to remove cgroup for job {} after {} attempts",
            job_id, attempts
        );
        Ok(())
    }
}
