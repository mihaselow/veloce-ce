//! Core job scheduler: queue ordering, backfill, and scheduling passes.

mod finalize;
mod steps;

pub use finalize::{check_timeouts, finalize_job, preempt_job, prune_jobs};
pub use steps::{schedule_steps, submit_step};

use crate::{
    save_state, send_to_worker,
    state::{
        sync_active_state_to_dashmaps, ControllerRole, GlobalState, SharedContext, SharedState,
    },
};
use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::sleep;
use tracing::info;
use veloce_common::{HistoryFilter, JobInfo, JobStatus, Message, QosLevel, UsageTracker};

pub async fn run_scheduling_pass(ctx: &SharedContext) {
    // 1. Extract queue and active IDs under a short lock
    let (_queue, active_deps, active_ids) = {
        let state_lock = ctx.state.lock().await;
        if state_lock.role != ControllerRole::Leader {
            return;
        }
        let queue = state_lock.queue.clone();
        let mut active_deps = HashMap::new();
        for &job_id in &queue {
            if let Some(job) = state_lock.jobs.get(&job_id) {
                if job.status == JobStatus::Pending {
                    active_deps.insert(
                        job_id,
                        (job.dependencies.clone(), job.dependency_specs.clone()),
                    );
                }
            }
        }
        let active_ids: std::collections::HashSet<u64> = state_lock.jobs.keys().cloned().collect();
        (queue, active_deps, active_ids)
    };

    // 2. Query database for missing parent job statuses asynchronously
    let mut needed_ids = std::collections::HashSet::new();
    for (_job_id, (deps, specs)) in &active_deps {
        if let Some(ref d) = deps {
            for &parent_id in d {
                if !active_ids.contains(&parent_id) {
                    needed_ids.insert(parent_id);
                }
            }
        }
        if let Some(ref s) = specs {
            for spec_str in s {
                if let Ok(spec) = veloce_common::DependencySpec::parse(spec_str) {
                    if !active_ids.contains(&spec.parent_id) {
                        needed_ids.insert(spec.parent_id);
                    }
                }
            }
        }
    }

    let mut db_cache = HashMap::new();
    for parent_id in needed_ids {
        if let Ok(history) = ctx
            .accounting_store
            .query_history(&HistoryFilter::Single(parent_id))
            .await
        {
            if let Some(h) = history.first() {
                db_cache.insert(parent_id, h.status.clone());
            }
        }
    }

    // 3. Acquire lock again to schedule and save state
    let mut state_lock = ctx.state.lock().await;
    if state_lock.role == ControllerRole::Leader {
        schedule_jobs(&mut state_lock, &db_cache);
        schedule_steps(&mut state_lock);
        sync_active_state_to_dashmaps(ctx, &state_lock);
        save_state(&state_lock);
    }
}
pub async fn run_scheduler(state: SharedState) {
    let notify = {
        let state_lock = state.state.lock().await;
        state_lock.scheduler_notify.clone()
    };

    // Initial run to clear any pending jobs from load
    run_scheduling_pass(&state).await;

    loop {
        notify.notified().await;

        // Debounce / Batching
        sleep(Duration::from_millis(50)).await;

        run_scheduling_pass(&state).await;
    }
}
pub(crate) fn calculate_effective_priority(job: &JobInfo, usage_tracker: &UsageTracker) -> f64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    // Constants for tuning
    const AGING_FACTOR: f64 = 0.5;
    const FAIRNESS_PENALTY: f64 = 0.01;

    let base_priority = job.priority as f64;
    let wait_time = now.saturating_sub(job.queued_time) as f64;
    let aging_bonus = wait_time * AGING_FACTOR;
    let user_usage = usage_tracker.get_usage(&job.user_id);
    let usage_penalty = user_usage * FAIRNESS_PENALTY;

    // Starvation dynamic priority boosting
    let starvation_threshold = std::env::var("VELOCE_STARVATION_THRESHOLD")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(1800); // 30 minutes default

    let boost_rate = std::env::var("VELOCE_BOOST_RATE")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(10.0); // 10.0 per minute default

    let mut boosting_bonus = 0.0;
    if wait_time > starvation_threshold as f64 {
        let excess_time = wait_time - starvation_threshold as f64;
        let excess_mins = excess_time / 60.0;
        // Non-linear boost to accelerate escalation as wait time grows
        boosting_bonus = excess_mins * excess_mins * boost_rate;
    }

    base_priority + aging_bonus + boosting_bonus - usage_penalty
}

pub(crate) fn has_cycle(
    current_id: u64,
    target_id: u64,
    jobs: &HashMap<u64, JobInfo>,
    visited: &mut HashSet<u64>,
) -> bool {
    if current_id == target_id {
        return true;
    }
    if !visited.insert(current_id) {
        return false;
    }
    if let Some(job) = jobs.get(&current_id) {
        if let Some(ref deps) = job.dependencies {
            for &dep in deps {
                if has_cycle(dep, target_id, jobs, visited) {
                    return true;
                }
            }
        }
        if let Some(ref specs) = job.dependency_specs {
            for spec_str in specs {
                if let Ok(spec) = veloce_common::DependencySpec::parse(spec_str) {
                    if has_cycle(spec.parent_id, target_id, jobs, visited) {
                        return true;
                    }
                }
            }
        }
    }
    false
}

pub(crate) fn are_dependencies_satisfied(
    job_info: &JobInfo,
    jobs: &HashMap<u64, JobInfo>,
    db_cache: &HashMap<u64, JobStatus>,
) -> Result<bool, String> {
    // 1. Evaluate raw dependencies (defaulting to afterok)
    if let Some(ref deps) = job_info.dependencies {
        for &parent_id in deps {
            match jobs.get(&parent_id) {
                Some(parent) => {
                    match &parent.status {
                        JobStatus::Completed(code) => {
                            if *code != 0 {
                                return Err(format!("DependencyNeverSatisfied: parent job {} failed with exit code {}", parent_id, code));
                            }
                        }
                        JobStatus::Failed(err) => {
                            return Err(format!(
                                "DependencyNeverSatisfied: parent job {} failed with error: {}",
                                parent_id, err
                            ));
                        }
                        JobStatus::Killed => {
                            return Err(format!(
                                "DependencyNeverSatisfied: parent job {} was cancelled",
                                parent_id
                            ));
                        }
                        _ => {
                            return Ok(false);
                        }
                    }
                }
                None => {
                    if let Some(status) = db_cache.get(&parent_id) {
                        match &status {
                            JobStatus::Completed(code) => {
                                if *code != 0 {
                                    return Err(format!("DependencyNeverSatisfied: parent job {} failed with exit code {}", parent_id, code));
                                }
                            }
                            JobStatus::Failed(err) => {
                                return Err(format!(
                                    "DependencyNeverSatisfied: parent job {} failed with error: {}",
                                    parent_id, err
                                ));
                            }
                            JobStatus::Killed => {
                                return Err(format!(
                                    "DependencyNeverSatisfied: parent job {} was cancelled",
                                    parent_id
                                ));
                            }
                            _ => return Ok(false),
                        }
                    } else {
                        return Ok(false);
                    }
                }
            }
        }
    }

    // 2. Evaluate dependency_specs (afterok, afternotok, afterany)
    if let Some(ref specs) = job_info.dependency_specs {
        for spec_str in specs {
            let spec = match veloce_common::DependencySpec::parse(spec_str) {
                Ok(s) => s,
                Err(e) => return Err(format!("Invalid dependency spec '{}': {}", spec_str, e)),
            };

            let parent_status = match jobs.get(&spec.parent_id) {
                Some(parent) => Some(parent.status.clone()),
                None => db_cache.get(&spec.parent_id).cloned(),
            };

            match parent_status {
                Some(status) => {
                    match spec.condition {
                        veloce_common::DependencyCondition::AfterOk => {
                            match status {
                                JobStatus::Completed(code) => {
                                    if code != 0 {
                                        return Err(format!("DependencyNeverSatisfied: parent job {} failed with exit code {}", spec.parent_id, code));
                                    }
                                }
                                JobStatus::Failed(err) => {
                                    return Err(format!("DependencyNeverSatisfied: parent job {} failed with error: {}", spec.parent_id, err));
                                }
                                JobStatus::Killed => {
                                    return Err(format!(
                                        "DependencyNeverSatisfied: parent job {} was cancelled",
                                        spec.parent_id
                                    ));
                                }
                                _ => return Ok(false),
                            }
                        }
                        veloce_common::DependencyCondition::AfterNotOk => {
                            match status {
                                JobStatus::Completed(code) => {
                                    if code == 0 {
                                        return Err(format!("DependencyNeverSatisfied: parent job {} completed successfully but expected failure", spec.parent_id));
                                    }
                                }
                                JobStatus::Failed(_) | JobStatus::Killed => {
                                    // Condition met
                                }
                                _ => return Ok(false),
                            }
                        }
                        veloce_common::DependencyCondition::AfterAny => {
                            match status {
                                JobStatus::Completed(_)
                                | JobStatus::Failed(_)
                                | JobStatus::Killed => {
                                    // Condition met
                                }
                                _ => return Ok(false),
                            }
                        }
                    }
                }
                None => return Ok(false),
            }
        }
    }

    Ok(true)
}

struct ProjectedWorkerResources {
    cores_available: usize,
    memory_available_mb: u64,
    gres_available: HashMap<String, usize>,
}

fn calculate_earliest_start_time(job: &JobInfo, state: &GlobalState, now: u64) -> u64 {
    // 1. Identify running jobs and estimate their completion times
    let running_jobs: Vec<&JobInfo> = state
        .jobs
        .values()
        .filter(|j| j.status == JobStatus::Running)
        .collect();

    // Candidate times are `now` and all expected job end times
    let mut candidate_times = vec![now];
    for rjob in &running_jobs {
        let duration = rjob.estimated_walltime.unwrap_or(if rjob.walltime > 0 {
            rjob.walltime
        } else {
            3600
        });
        let end_time_est = rjob.start_time.unwrap_or(now) + duration;
        candidate_times.push(std::cmp::max(end_time_est, now + 1));
    }
    candidate_times.sort_unstable();
    candidate_times.dedup();

    for t_start in candidate_times {
        // 2. Project worker resources at t_start
        let mut worker_projections: HashMap<String, ProjectedWorkerResources> = state
            .workers
            .iter()
            .filter(|(_, w)| !w.draining && w.connected)
            .map(|(id, w)| {
                let initial = ProjectedWorkerResources {
                    cores_available: w.available_core_ids.len(),
                    memory_available_mb: (w.resources.total_memory / 1024 / 1024)
                        .saturating_sub(w.allocated_memory),
                    gres_available: w
                        .available_gres_ids
                        .iter()
                        .map(|(name, ids)| (name.clone(), ids.len()))
                        .collect(),
                };
                (id.clone(), initial)
            })
            .collect();

        // Release resources of all running jobs ending at or before t_start
        for rjob in &running_jobs {
            let duration = rjob.estimated_walltime.unwrap_or(if rjob.walltime > 0 {
                rjob.walltime
            } else {
                3600
            });
            let end_time_est = rjob.start_time.unwrap_or(now) + duration;
            let end_time_est = std::cmp::max(end_time_est, now + 1);

            if end_time_est <= t_start {
                for worker_id in &rjob.assigned_workers {
                    if let Some(proj) = worker_projections.get_mut(worker_id) {
                        let cores_freed = if let Some(w) = state.workers.get(worker_id) {
                            w.job_core_assignments
                                .get(&rjob.id)
                                .map(|c| c.len())
                                .unwrap_or(rjob.req_cores as usize)
                        } else {
                            rjob.req_cores as usize
                        };
                        proj.cores_available += cores_freed;
                        proj.memory_available_mb += rjob.req_memory;

                        for (name, &req_count) in &rjob.gres_req {
                            let entry = proj.gres_available.entry(name.clone()).or_insert(0);
                            let gres_freed = if let Some(w) = state.workers.get(worker_id) {
                                w.job_gres_assignments
                                    .get(&rjob.id)
                                    .and_then(|g| g.get(name))
                                    .map(|g| g.len())
                                    .unwrap_or(req_count as usize)
                            } else {
                                req_count as usize
                            };
                            *entry += gres_freed;
                        }
                    }
                }
            }
        }

        // 3. Check if the job fits on the projected workers at t_start
        let mut candidate_workers = Vec::new();
        for (id, proj) in &worker_projections {
            // Check Reservations
            let mut is_reserved = false;
            for res in state.reservations.values() {
                if res.nodes.contains(id) {
                    if res.start_time <= t_start && t_start < res.end_time {
                        if res.owner != job.user_id {
                            is_reserved = true;
                            break;
                        }
                    }
                    if res.start_time > t_start && res.owner != job.user_id {
                        let job_walltime = if job.walltime == 0 {
                            24 * 3600
                        } else {
                            job.walltime
                        };
                        if t_start + job_walltime > res.start_time {
                            is_reserved = true;
                            break;
                        }
                    }
                }
            }
            if is_reserved {
                continue;
            }

            // Check CPU
            if proj.cores_available < job.req_cores as usize {
                continue;
            }

            // Check Memory
            if proj.memory_available_mb < job.req_memory {
                continue;
            }

            // Check GRES
            let mut gres_ok = true;
            for (name, &req_count) in &job.gres_req {
                if *proj.gres_available.get(name).unwrap_or(&0) < req_count as usize {
                    gres_ok = false;
                    break;
                }
            }
            if !gres_ok {
                continue;
            }

            candidate_workers.push(id.clone());
        }

        // Check if we can satisfy req_nodes
        if candidate_workers.len() >= job.req_nodes {
            if job.req_nodes > 1 {
                // Check homogeneous grouping
                let mut groups: HashMap<(String, String), Vec<String>> = HashMap::new();
                for id in &candidate_workers {
                    if let Some(w) = state.workers.get(id) {
                        let key = (w.resources.cpu_model.clone(), w.resources.arch.clone());
                        groups.entry(key).or_default().push(id.clone());
                    }
                }
                let mut best_group_exists = false;
                for (_, ids) in groups {
                    if ids.len() >= job.req_nodes {
                        best_group_exists = true;
                        break;
                    }
                }
                if best_group_exists {
                    return t_start;
                }
            } else {
                return t_start;
            }
        }
    }

    // If never satisfiable, return a time far in the future
    now + 365 * 24 * 3600
}

// Gang Scheduler with Priority and Resource Awareness
pub fn schedule_jobs(state: &mut GlobalState, db_cache: &HashMap<u64, JobStatus>) {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    state.reservations.retain(|_, res| res.end_time > now);

    state.usage_tracker.apply_decay();

    let mut pending_jobs: Vec<u64> = state.queue.drain(..).collect();

    // Sort by Effective Priority + QoS Boost
    let mut job_scores: HashMap<u64, f64> = HashMap::new();
    for &job_id in &pending_jobs {
        if let Some(job) = state.jobs.get(&job_id) {
            let qos_boost = match job.qos {
                QosLevel::Interactive => 10000.0,
                QosLevel::Production => 0.0,
                QosLevel::Preemptible => -5000.0,
                QosLevel::Background => -10000.0,
            };
            let score = calculate_effective_priority(job, &state.usage_tracker) + qos_boost;
            job_scores.insert(job_id, score);
        }
    }

    pending_jobs.sort_by(|&a, &b| {
        let score_a = job_scores.get(&a).unwrap_or(&0.0);
        let score_b = job_scores.get(&b).unwrap_or(&0.0);
        score_b
            .partial_cmp(score_a)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.cmp(&b))
    });

    let mut still_pending = VecDeque::new();
    let mut top_job_earliest_start: Option<u64> = None;
    let mut top_job_id: Option<u64> = None;

    for &job_id in &pending_jobs {
        let job_info = match state.jobs.get(&job_id) {
            Some(j) => j.clone(),
            None => continue,
        };

        if job_info.status != JobStatus::Pending {
            continue;
        }

        // Evaluate job dependencies
        let dependency_eval = are_dependencies_satisfied(&job_info, &state.jobs, db_cache);
        match dependency_eval {
            Ok(false) => {
                still_pending.push_back(job_id);
                if let Some(j) = state.jobs.get_mut(&job_id) {
                    j.reason = Some("Waiting for dependencies".to_string());
                }
                continue;
            }
            Err(e) => {
                still_pending.push_back(job_id);
                if let Some(j) = state.jobs.get_mut(&job_id) {
                    j.reason = Some(e);
                }
                continue;
            }
            Ok(true) => {
                if let Some(j) = state.jobs.get_mut(&job_id) {
                    if j.reason.as_deref() == Some("Waiting for dependencies") {
                        j.reason = None;
                    }
                }
            }
        }

        // Fit Test for Backfilling
        if let Some(top_est) = top_job_earliest_start {
            let duration = job_info
                .estimated_walltime
                .unwrap_or(if job_info.walltime > 0 {
                    job_info.walltime
                } else {
                    3600
                });
            if now + duration > top_est {
                still_pending.push_back(job_id);
                if let Some(j) = state.jobs.get_mut(&job_id) {
                    j.reason = Some(format!(
                        "Backfill denied: would delay top job {} (earliest start: {})",
                        top_job_id.unwrap(),
                        top_est
                    ));
                }
                continue;
            }
        }

        // Find nodes with enough resources (CPU, Memory, and GRES)
        let mut available_workers = Vec::new();
        let mut fail_reasons = Vec::new();

        for (id, worker) in state.workers.iter() {
            if worker.draining || !worker.connected {
                continue;
            }

            let mut worker_fail_reasons = Vec::new();

            // Check Reservations
            let mut is_reserved = false;
            for res in state.reservations.values() {
                if res.nodes.contains(id) {
                    // Active reservation: only owner can run
                    if res.start_time <= now && now < res.end_time {
                        if res.owner != job_info.user_id {
                            is_reserved = true;
                            break;
                        }
                    }
                    // Upcoming reservation: if job overlaps and owner is different
                    if res.start_time > now && res.owner != job_info.user_id {
                        let job_walltime = if job_info.walltime == 0 {
                            24 * 3600
                        } else {
                            job_info.walltime
                        };
                        if now + job_walltime > res.start_time {
                            is_reserved = true;
                            break;
                        }
                    }
                }
            }
            if is_reserved {
                worker_fail_reasons.push("Reserved");
            }

            // Check CPU
            if (worker.available_core_ids.len() as u32) < job_info.req_cores {
                worker_fail_reasons.push("CPU");
            }

            // Check Memory
            let total_mb = worker.resources.total_memory / 1024 / 1024;
            if (total_mb.saturating_sub(worker.allocated_memory)) < job_info.req_memory {
                worker_fail_reasons.push("Mem");
            }

            // Check GRES
            for (name, &req_count) in &job_info.gres_req {
                let available_count = worker
                    .available_gres_ids
                    .get(name)
                    .map(|ids: &Vec<u32>| ids.len())
                    .unwrap_or(0);
                if (available_count as u64) < req_count {
                    worker_fail_reasons.push("GRES");
                }
            }

            if worker_fail_reasons.is_empty() {
                available_workers.push(id.clone());
            } else {
                fail_reasons.push(format!("{}: [{}]", id, worker_fail_reasons.join(",")));
            }
        }

        // Sort candidates by available resources (Most Available First)
        available_workers.sort_by(|a, b| {
            let worker_a = state.workers.get(a).unwrap();
            let worker_b = state.workers.get(b).unwrap();
            worker_b
                .available_core_ids
                .len()
                .cmp(&worker_a.available_core_ids.len())
        });

        let mut preemption_triggered = false;
        let mut selected_ids_opt = None;
        let mut jobs_to_preempt = std::collections::HashSet::new();

        if available_workers.len() >= job_info.req_nodes {
            selected_ids_opt = if job_info.req_nodes > 1 {
                // Simple homogeneous grouping logic
                let mut best_group = None;
                let mut groups: HashMap<(String, String), Vec<String>> = HashMap::new();
                for id in &available_workers {
                    if let Some(w) = state.workers.get(id) {
                        let key = (w.resources.cpu_model.clone(), w.resources.arch.clone());
                        groups.entry(key).or_default().push(id.clone());
                    }
                }
                for (_, ids) in groups {
                    if ids.len() >= job_info.req_nodes {
                        let mut selected = ids;
                        selected.truncate(job_info.req_nodes);
                        best_group = Some(selected);
                        break;
                    }
                }
                best_group
            } else {
                Some(vec![available_workers[0].clone()])
            };
        }

        if selected_ids_opt.is_none() && job_info.qos == QosLevel::Interactive {
            // Try to find candidate workers with preemption
            let mut preempt_candidates = Vec::new();
            for (id, worker) in state.workers.iter() {
                if worker.draining || !worker.connected {
                    continue;
                }

                // Calculate resources available if we preempt ALL preemptible jobs on this worker
                let mut preemptible_cores_count = 0;
                let mut preemptible_mem = 0;
                let mut preemptible_gres = HashMap::new();
                let mut preemptible_jobs_on_worker = Vec::new();

                for &run_job_id in &worker.assigned_jobs {
                    if let Some(run_job) = state.jobs.get(&run_job_id) {
                        if run_job.qos == QosLevel::Preemptible {
                            preemptible_jobs_on_worker.push(run_job_id);
                            if let Some(cores) = worker.job_core_assignments.get(&run_job_id) {
                                preemptible_cores_count += cores.len();
                            }
                            preemptible_mem += run_job.req_memory;
                            if let Some(gres_map) = worker.job_gres_assignments.get(&run_job_id) {
                                for (name, ids) in gres_map {
                                    *preemptible_gres.entry(name.clone()).or_insert(0) += ids.len();
                                }
                            }
                        }
                    }
                }

                let total_mb = worker.resources.total_memory / 1024 / 1024;
                let eligible_cores = worker.available_core_ids.len() + preemptible_cores_count;
                let eligible_memory =
                    total_mb.saturating_sub(worker.allocated_memory) + preemptible_mem;

                let mut gres_ok = true;
                for (name, &req_count) in &job_info.gres_req {
                    let available_count = worker
                        .available_gres_ids
                        .get(name)
                        .map(|ids| ids.len())
                        .unwrap_or(0);
                    let preempt_count = preemptible_gres.get(name).cloned().unwrap_or(0);
                    if (available_count + preempt_count) < req_count as usize {
                        gres_ok = false;
                        break;
                    }
                }

                // Check reservations
                let mut is_reserved = false;
                for res in state.reservations.values() {
                    if res.nodes.contains(id) {
                        if res.start_time <= now && now < res.end_time {
                            if res.owner != job_info.user_id {
                                is_reserved = true;
                                break;
                            }
                        }
                        if res.start_time > now && res.owner != job_info.user_id {
                            let job_walltime = if job_info.walltime == 0 {
                                24 * 3600
                            } else {
                                job_info.walltime
                            };
                            if now + job_walltime > res.start_time {
                                is_reserved = true;
                                break;
                            }
                        }
                    }
                }

                if !is_reserved
                    && eligible_cores >= job_info.req_cores as usize
                    && eligible_memory >= job_info.req_memory
                    && gres_ok
                {
                    preempt_candidates.push((id.clone(), preemptible_jobs_on_worker));
                }
            }

            if preempt_candidates.len() >= job_info.req_nodes {
                // Sort preempt_candidates by number of preemptible jobs to evict (least first)
                preempt_candidates.sort_by_key(|c| c.1.len());

                let selected_preempt_candidates = if job_info.req_nodes > 1 {
                    let mut best_group = None;
                    let mut groups: HashMap<(String, String), Vec<(String, Vec<u64>)>> =
                        HashMap::new();
                    for (id, jobs) in &preempt_candidates {
                        if let Some(w) = state.workers.get(id) {
                            let key = (w.resources.cpu_model.clone(), w.resources.arch.clone());
                            groups
                                .entry(key)
                                .or_default()
                                .push((id.clone(), jobs.clone()));
                        }
                    }
                    for (_, ids) in groups {
                        if ids.len() >= job_info.req_nodes {
                            let mut selected = ids;
                            selected.truncate(job_info.req_nodes);
                            best_group = Some(selected);
                            break;
                        }
                    }
                    best_group
                } else {
                    Some(vec![preempt_candidates[0].clone()])
                };

                if let Some(selected_nodes) = selected_preempt_candidates {
                    preemption_triggered = true;
                    let mut node_ids = Vec::new();
                    for (node_id, jobs) in selected_nodes {
                        node_ids.push(node_id);
                        for j_id in jobs {
                            jobs_to_preempt.insert(j_id);
                        }
                    }
                    selected_ids_opt = Some(node_ids);
                }
            }
        }

        let reason = if let Some(selected_ids) = selected_ids_opt {
            if preemption_triggered {
                info!(
                    "Preemption triggered for Interactive job {}. Evicting jobs: {:?}",
                    job_id, jobs_to_preempt
                );
                for preempt_job_id in &jobs_to_preempt {
                    preempt_job(state, *preempt_job_id);
                }
            }

            let mut all_allocated_cores = HashMap::new();
            let mut all_allocated_gres = HashMap::new();
            let mut node_list_ips = Vec::new();

            for id in &selected_ids {
                if let Some(worker) = state.workers.get_mut(id) {
                    worker.assigned_jobs.insert(job_id);
                    node_list_ips.push(worker.addr.to_string());

                    // Allocate Cores
                    let assigned_cores: Vec<usize> = worker
                        .available_core_ids
                        .drain(0..(job_info.req_cores as usize))
                        .collect();
                    worker
                        .job_core_assignments
                        .insert(job_id, assigned_cores.clone());
                    all_allocated_cores.insert(id.clone(), assigned_cores);

                    // Allocate Memory
                    worker.allocated_memory += job_info.req_memory;

                    // Allocate GRES
                    let mut worker_allocated_gres = HashMap::new();
                    for (name, &req_count) in &job_info.gres_req {
                        if let Some(available_ids) = worker.available_gres_ids.get_mut(name) {
                            let assigned_ids: Vec<u32> = available_ids
                                .drain(0..(req_count as usize))
                                .collect::<Vec<u32>>();
                            worker_allocated_gres.insert(name.clone(), assigned_ids);
                        }
                    }
                    worker
                        .job_gres_assignments
                        .insert(job_id, worker_allocated_gres.clone());
                    all_allocated_gres.insert(id.clone(), worker_allocated_gres.clone());
                }
            }

            let mut env_vars = job_info.env_vars.clone();
            let mut job_args = job_info.args.clone();

            // Automatically allow root execution for Docker environments
            env_vars.push(("OMPI_ALLOW_RUN_AS_ROOT".into(), "1".into()));
            env_vars.push(("OMPI_ALLOW_RUN_AS_ROOT_CONFIRM".into(), "1".into()));

            // Prepare environment and MPI injection
            if job_info.req_nodes > 1 {
                let hostfile_path = format!("/scratch/jobs/{}/hostfile", job_id);

                // If running mpirun/mpiexec, inject explicit flags
                if job_info.binary == "mpirun" || job_info.binary == "mpiexec" {
                    job_args.insert(0, "--mca".into());
                    job_args.insert(1, "plm_rsh_agent".into());
                    job_args.insert(2, "veloce-exec".into());
                    job_args.insert(3, "--hostfile".into());
                    job_args.insert(4, hostfile_path.clone());
                    job_args.insert(5, "--mca".into());
                    job_args.insert(6, "btl".into());
                    job_args.insert(7, "tcp,self".into());
                }

                env_vars.push(("OMPI_MCA_plm_rsh_agent".into(), "veloce-exec".into()));
                env_vars.push(("OMPI_MCA_ras_base_nodefile".into(), hostfile_path.clone()));
                env_vars.push((
                    "VELOCE_CONTROLLER_URL".into(),
                    format!("https://veloce-controller-ha-1:{}", state.api_port),
                ));

                let mut hostfile_content = String::new();
                for id in &selected_ids {
                    if let Some(w) = state.workers.get(id) {
                        let slots = w
                            .job_core_assignments
                            .get(&job_id)
                            .map(|c| c.len())
                            .unwrap_or(job_info.req_cores as usize);
                        hostfile_content.push_str(&format!("{} slots={}\n", w.hostname, slots));
                    }
                }

                let job_scratch_dir = format!("/scratch/jobs/{}", job_id);
                let _ = std::fs::create_dir_all(&job_scratch_dir);
                let _ = std::fs::write(format!("{}/hostfile", job_scratch_dir), hostfile_content);
            }

            // Dispatch
            let is_mpi_launcher = job_info.binary == "mpirun" || job_info.binary == "mpiexec";
            let mut dispatched_workers = Vec::new();

            for (i, id) in selected_ids.iter().enumerate() {
                // For MPI launchers, only dispatch to the head node
                if is_mpi_launcher && i > 0 {
                    continue;
                }
                dispatched_workers.push(id.clone());

                let run_msg = if let Some(worker) = state.workers.get_mut(id) {
                    let assigned_cores = worker.job_core_assignments.get(&job_id).cloned();
                    let worker_allocated_gres =
                        all_allocated_gres.get(id).cloned().unwrap_or_default();

                    let mut worker_env = env_vars.clone();
                    worker_env.push(("VELOCE_RANK".into(), i.to_string()));
                    worker_env.push(("VELOCE_SIZE".into(), selected_ids.len().to_string()));

                    Some(Message::RunJob {
                        job_id,
                        binary: job_info.binary.clone(),
                        args: job_args.clone(),
                        node_list: node_list_ips.clone(),
                        assigned_cores,
                        req_memory: job_info.req_memory,
                        working_directory: job_info.working_directory.clone(),
                        user_id: job_info.user_id.clone(),
                        walltime: job_info.walltime,
                        submission_time: job_info.queued_time,
                        array_id: job_info.array_id,
                        array_task_id: job_info.array_task_id,
                        inputs: job_info.inputs.clone(),
                        allocated_gres: worker_allocated_gres,
                        gres_req: job_info.gres_req.clone(),
                        secret: job_info.secret.clone(),
                        controller_url: format!(
                            "https://veloce-controller-ha-1:{}",
                            state.api_port
                        ),
                        env_vars: worker_env,
                        wait_for_licenses: job_info.wait_for_licenses,
                        estimated_walltime: job_info.estimated_walltime,
                        priority_offset: job_info.priority_offset,
                        dependencies: job_info.dependencies.clone(),
                        dependency_specs: job_info.dependency_specs.clone(),
                        qos: job_info.qos,
                        container_asset: job_info.container_asset.clone(),
                        vnc_enabled: job_info.vnc_enabled,
                        inherit_host_env: job_info.inherit_host_env,
                        env_allowlist: job_info.env_allowlist.clone(),
                        job_profile: job_info.job_profile.clone(),
                    })
                } else {
                    None
                };

                if let Some(run_msg) = run_msg {
                    let _ = send_to_worker(state, id, run_msg);
                }
            }

            crate::job_logs::record_dispatched_workers(state, job_id, dispatched_workers);

            println!("selected_ids: {:?}", selected_ids);
            if let Some(j) = state.jobs.get_mut(&job_id) {
                println!(
                    "Updating job {} status to Running inside schedule_jobs",
                    job_id
                );
                j.status = JobStatus::Running;
                j.assigned_workers = selected_ids;
                j.allocated_cores = all_allocated_cores;
                j.allocated_gres = all_allocated_gres;
                j.start_time = Some(
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                );
                j.reason = None;
            }
            None
        } else {
            still_pending.push_back(job_id);
            if available_workers.len() < job_info.req_nodes {
                let mut summary = format!(
                    "Waiting for resources (CPU/Mem/GRES). Nodes: {}/{}",
                    available_workers.len(),
                    job_info.req_nodes
                );
                if !fail_reasons.is_empty() {
                    summary.push_str(&format!(
                        ". Detail: {}",
                        fail_reasons
                            .iter()
                            .take(3)
                            .cloned()
                            .collect::<Vec<_>>()
                            .join("; ")
                    ));
                }
                Some(summary)
            } else {
                Some(format!(
                    "Waiting for {} homogeneous nodes",
                    job_info.req_nodes
                ))
            }
        };

        if let Some(r) = reason {
            let mut r_final = r;
            if top_job_earliest_start.is_none() {
                let est = calculate_earliest_start_time(&job_info, state, now);
                top_job_earliest_start = Some(est);
                top_job_id = Some(job_id);
                r_final = format!("{} (Earliest start: {})", r_final, est);
            }
            if let Some(j) = state.jobs.get_mut(&job_id) {
                j.reason = Some(r_final);
            }
        }
    }
    state.queue = still_pending;
}
