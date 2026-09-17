mod containers;
mod dashboard;
mod job_details;
mod jobs;
mod nodes;
mod submit;

pub(crate) use containers::ContainersList;
pub(crate) use dashboard::Dashboard;
pub(crate) use job_details::JobDetails;
pub(crate) use jobs::JobsList;
pub(crate) use nodes::NodesList;
pub(crate) use submit::SubmitJob;

use leptos::*;

pub(crate) fn format_duration(secs: u64) -> String {
    let days = secs / 86400;
    let hours = (secs % 86400) / 3600;
    let minutes = (secs % 3600) / 60;
    let seconds = secs % 60;
    if days > 0 {
        format!("{}d {}h {}m", days, hours, minutes)
    } else if hours > 0 {
        format!("{}h {}m {}s", hours, minutes, seconds)
    } else if minutes > 0 {
        format!("{}m {}s", minutes, seconds)
    } else {
        format!("{}s", seconds)
    }
}

pub(crate) fn format_bytes(bytes: u64) -> String {
    if bytes < 1024 {
        format!("{} B", bytes)
    } else if bytes < 1024 * 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else if bytes < 1024 * 1024 * 1024 {
        format!("{:.1} MB", bytes as f64 / (1024.0 * 1024.0))
    } else {
        format!("{:.1} GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    }
}

#[component]
fn Sparkline(data: Vec<f32>, width: u32, height: u32, color: String) -> impl IntoView {
    if data.is_empty() {
        return view! { <svg width=width height=height></svg> }.into_view();
    }
    let max = data.iter().cloned().fold(0.0f32, f32::max);
    let min = data.iter().cloned().fold(f32::MAX, f32::min);
    let range = if max - min < 0.1 { 1.0 } else { max - min };
    let points = data
        .iter()
        .enumerate()
        .map(|(i, &v)| {
            let x = (i as f32 / (data.len().saturating_sub(1) as f32).max(1.0)) * width as f32;
            let y = height as f32 - ((v - min) / range) * height as f32;
            format!("{},{}", x, y)
        })
        .collect::<Vec<_>>()
        .join(" ");
    view! {
        <svg width=width height=height viewBox=format!("0 0 {} {}", width, height) preserveAspectRatio="none" style="overflow: visible;">
            <polyline fill="none" stroke=color stroke-width="1.5" points=points />
        </svg>
    }.into_view()
}
