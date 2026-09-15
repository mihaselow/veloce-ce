use leptos::*;
use veloce_common::JobStats;
use wasm_bindgen::closure::Closure;
use wasm_bindgen::JsCast;

#[component]
pub fn JobMetricsChart(job_id: u64) -> impl IntoView {
    let (stats, set_stats) = create_signal(Vec::<JobStats>::new());

    create_effect(move |_| {
        let url = format!("/api/v1/jobs/{}/metrics/stream", job_id);

        if let Ok(es) = web_sys::EventSource::new(&url) {
            let set_stats_init = set_stats;
            let on_init = Closure::wrap(Box::new(move |event: web_sys::MessageEvent| {
                if let Some(txt) = event.data().as_string() {
                    if let Ok(data) = serde_json::from_str::<Vec<JobStats>>(&txt) {
                        set_stats_init.set(data);
                    }
                }
            }) as Box<dyn FnMut(_)>);

            let set_stats_upd = set_stats;
            let on_update = Closure::wrap(Box::new(move |event: web_sys::MessageEvent| {
                if let Some(txt) = event.data().as_string() {
                    if let Ok(data) = serde_json::from_str::<JobStats>(&txt) {
                        set_stats_upd.update(|v| {
                            if v.last().is_some_and(|last| {
                                last.job_id == data.job_id
                                    && last.cpu_usage_percent == data.cpu_usage_percent
                                    && last.memory_usage_bytes == data.memory_usage_bytes
                                    && last.is_idle == data.is_idle
                                    && last.idle_duration == data.idle_duration
                                    && last.cgroup_active == data.cgroup_active
                            }) {
                                return;
                            }
                            if v.len() >= 300 {
                                v.remove(0);
                            }
                            v.push(data);
                        });
                    }
                }
            }) as Box<dyn FnMut(_)>);

            let _ = es.add_event_listener_with_callback("init", on_init.as_ref().unchecked_ref());
            let _ =
                es.add_event_listener_with_callback("update", on_update.as_ref().unchecked_ref());

            on_cleanup(move || {
                es.close();
                drop(on_init);
                drop(on_update);
            });
        }
    });

    let observed_max = move || {
        stats
            .get()
            .iter()
            .map(|st| st.cpu_usage_percent)
            .fold(0.0f32, f32::max)
    };

    let max_val = move || observed_max().clamp(1.0, 100.0);

    let path_data = move || {
        let s = stats.get();
        if s.is_empty() {
            return String::new();
        }

        let scale = max_val();
        let denominator = (s.len().saturating_sub(1)).max(1) as f32;
        s.iter()
            .enumerate()
            .map(|(i, st)| {
                let x = (i as f32 / denominator) * 100.0;
                let mut y = 100.0 - (st.cpu_usage_percent / scale * 100.0);
                if y < 0.0 {
                    y = 0.0;
                }
                if i == 0 {
                    format!("M {:.3} {:.3}", x, y)
                } else {
                    format!("L {:.3} {:.3}", x, y)
                }
            })
            .collect::<Vec<_>>()
            .join(" ")
    };

    let current_label = move || {
        let current_cpu = stats
            .get()
            .last()
            .map(|s| s.cpu_usage_percent)
            .unwrap_or(0.0);
        if current_cpu < 1.0 {
            format!("{:.2}%", current_cpu)
        } else {
            format!("{:.1}%", current_cpu)
        }
    };

    view! {
        <div class="glass-panel" style="margin-top: 15px; border-left: 4px solid var(--warning); background: rgba(245, 158, 11, 0.08); min-height: 235px;">
            <h4 style="margin-top: 0; color: var(--warning);"><i class="ph ph-chart-line-up"></i> " Real-Time Telemetry (CPU)"</h4>
            <div style="padding: 10px 0; min-height: 180px; display: flex; flex-direction: column;">
                <div style="position: relative; height: 150px; min-height: 150px; flex: 0 0 150px;">
                    <svg width="100%" height="150" viewBox="0 0 100 100" preserveAspectRatio="none" style="display: block; height: 150px; border-bottom: 1px solid var(--line); border-left: 1px solid var(--line);">
                        <path d=path_data fill="none" stroke="var(--warning)" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round" vector-effect="non-scaling-stroke" />
                    </svg>
                    <div
                        style=move || format!(
                            "position: absolute; inset: 0; display: flex; align-items: center; justify-content: center; color: var(--text-muted); pointer-events: none; opacity: {}; transition: opacity 0.15s;",
                            if stats.get().is_empty() { "1" } else { "0" }
                        )
                    >
                        "Waiting for telemetry data..."
                    </div>
                </div>
                <div style="display: flex; justify-content: space-between; font-size: 0.75rem; color: var(--text-muted); margin-top: 5px; min-height: 1rem;">
                    <span>"T-5m"</span>
                    <span>"Scale: 0-" {move || format!("{:.1}%", max_val())} " · Current: " {current_label}</span>
                </div>
            </div>
        </div>
    }
}
