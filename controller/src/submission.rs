//! Job submission batch processor.

use crate::{
    scheduler::run_scheduling_pass,
    state::{sync_active_state_to_dashmaps, ControllerRole, SharedContext},
};
use std::sync::atomic::Ordering;
use tokio::sync::mpsc;
use tracing::{debug, info};
use veloce_common::JobInfo;

pub async fn submission_processor(ctx: SharedContext, mut rx: mpsc::Receiver<JobInfo>) {
    info!("Submission processor started");
    // Batches up to 100 jobs or until empty
    let mut batch = Vec::with_capacity(100);

    while let Some(job) = rx.recv().await {
        batch.push(job);

        // Drain channel
        while batch.len() < 100 {
            match rx.try_recv() {
                Ok(j) => batch.push(j),
                Err(_) => break, // Empty or Closed
            }
        }

        // Process Batch
        if !batch.is_empty() {
            let mut state_lock = ctx.state.lock().await;
            if state_lock.role != ControllerRole::Leader {
                debug!(
                    "Follower received {} job submissions. Dropping.",
                    batch.len()
                );
                batch.clear();
                continue;
            }

            // Update atomic tracker in state to match atomic (eventual consistency for persistence)
            state_lock.next_job_id = ctx.next_job_id.load(Ordering::Relaxed);

            let count = batch.len();
            for job in batch.drain(..) {
                state_lock.jobs.insert(job.id, job.clone());
                state_lock.queue.push_back(job.id);
            }
            sync_active_state_to_dashmaps(&ctx, &state_lock);

            drop(state_lock); // release the lock

            // Run scheduling pass asynchronously
            run_scheduling_pass(&ctx).await;

            info!("Processed batch of {} submissions", count);
        }
    }
}
