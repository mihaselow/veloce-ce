use veloce_common::{JobInfo, JobStatus};
use web_sys::{Notification, NotificationOptions, NotificationPermission};

pub fn request_notification_permission() {
    let _ = Notification::request_permission();
}

fn is_terminal_status(status: &JobStatus) -> bool {
    matches!(
        status,
        JobStatus::Completed(_) | JobStatus::Failed(_) | JobStatus::Killed
    )
}

pub fn notify_job_status_changes(previous: &[JobInfo], current: &[JobInfo]) {
    if Notification::permission() != NotificationPermission::Granted {
        return;
    }

    for job in current {
        let Some(old) = previous.iter().find(|j| j.id == job.id) else {
            continue;
        };
        if old.status == job.status || !is_terminal_status(&job.status) {
            continue;
        }

        let title = format!("Job {} {}", job.id, status_label(&job.status));
        let body = job
            .job_name
            .clone()
            .or_else(|| job.job_comment.clone())
            .unwrap_or_else(|| job.binary.clone());

        let opts = NotificationOptions::new();
        opts.set_body(&body);
        if Notification::new_with_options(&title, &opts).is_ok() {
            // Browser groups notifications by title when the tab is in background.
        }
    }
}

fn status_label(status: &JobStatus) -> String {
    match status {
        JobStatus::Completed(code) => format!("completed (exit {code})"),
        JobStatus::Failed(reason) => format!("failed ({reason})"),
        JobStatus::Killed => "killed".to_string(),
        JobStatus::Pending => "pending".to_string(),
        JobStatus::Running => "running".to_string(),
    }
}
