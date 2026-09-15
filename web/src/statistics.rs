use crate::api_get;
use leptos::*;
use veloce_common::ControllerInfo;

#[derive(Clone, Debug, serde::Deserialize, PartialEq)]
struct NodeMetricsHistoryResponse {
    start_time: u64,
    end_time: u64,
    bucket_seconds: u64,
    node_count: usize,
    sample_count: usize,
    management_node_history: bool,
    coverage_note: String,
    nodes: Vec<NodeMetricsNodeSeries>,
    cluster: Vec<ClusterMetricsBucket>,
}

#[derive(Clone, Debug, serde::Deserialize, PartialEq)]
struct NodeMetricsNodeSeries {
    node_id: String,
    hostname: Option<String>,
    online: Option<bool>,
    sample_count: usize,
    samples: Vec<NodeMetricsBucket>,
}

#[derive(Clone, Debug, serde::Deserialize, PartialEq)]
struct NodeMetricsBucket {
    timestamp: u64,
    sample_count: u32,
    cpu_avg: f32,
    cpu_max: f32,
    memory_used_avg: u64,
    memory_total_max: u64,
    memory_pct_avg: f32,
    memory_pct_max: f32,
    running_jobs_avg: f32,
    running_jobs_max: u32,
    net_rx_avg: u64,
    net_tx_avg: u64,
    disk_read_avg: u64,
    disk_write_avg: u64,
    swap_usage_avg: u64,
    process_count_avg: u32,
    load1_avg: f32,
    gpu_avg: Option<f32>,
}

#[derive(Clone, Debug, serde::Deserialize, PartialEq)]
struct ClusterMetricsBucket {
    timestamp: u64,
    node_count: u32,
    cpu_avg: f32,
    cpu_peak: f32,
    memory_pct_avg: f32,
    memory_pct_peak: f32,
    running_jobs_avg: f32,
    net_rx_avg: u64,
    net_tx_avg: u64,
    disk_read_avg: u64,
    disk_write_avg: u64,
}

#[derive(Clone)]
struct TopMover {
    label: String,
    value: String,
    detail: String,
    accent: &'static str,
}

#[component]
pub fn StatisticsPage() -> impl IntoView {
    let (range, set_range) = create_signal("6h".to_string());
    let (history, set_history) = create_signal(None::<NodeMetricsHistoryResponse>);
    let (controllers, set_controllers) = create_signal(Vec::<ControllerInfo>::new());
    let (loading, set_loading) = create_signal(false);
    let (error, set_error) = create_signal(None::<String>);

    create_effect(move |_| {
        let selected_range = range.get();
        fetch_statistics(
            selected_range.clone(),
            set_history,
            set_controllers,
            set_loading,
            set_error,
            true,
        );

        let handle = crate::set_interval_with_handle(
            move || {
                fetch_statistics(
                    selected_range.clone(),
                    set_history,
                    set_controllers,
                    set_loading,
                    set_error,
                    false,
                );
            },
            30_000,
        );

        on_cleanup(move || {
            if let Ok(id) = handle {
                if let Some(win) = web_sys::window() {
                    win.clear_interval_with_handle(id);
                }
            }
        });
    });

    view! {
        <div class="statistics-page telemetry-theme">
            <header class="statistics-header">
                <div>
                    <div class="statistics-kicker">"Historical Operations"</div>
                    <h1>"Cluster Statistics"</h1>
                    <p>
                        "Longitudinal usage, pressure, and waste signals from worker-emitted node history."
                    </p>
                </div>
                <div class="statistics-controls">
                    <label for="stats-range">"Window"</label>
                    <select
                        id="stats-range"
                        prop:value=move || range.get()
                        on:change=move |ev| set_range.set(event_target_value(&ev))
                    >
                        <option value="1h">"1 hour"</option>
                        <option value="6h">"6 hours"</option>
                        <option value="24h">"24 hours"</option>
                        <option value="7d">"7 days"</option>
                    </select>
                </div>
            </header>

            {move || error.get().map(|message| view! {
                <div class="statistics-alert statistics-alert-error">
                    <i class="ph ph-warning-circle"></i>
                    <span>{message}</span>
                </div>
            })}

            {move || if loading.get() {
                view! {
                    <div class="statistics-alert">
                        <i class="ph ph-spinner-gap"></i>
                        <span>"Loading historical metrics..."</span>
                    </div>
                }.into_view()
            } else {
                ().into_view()
            }}

            {move || match history.get() {
                Some(data) if data.sample_count > 0 => view! {
                    <StatsSummary data=data.clone()/>
                    <div class="statistics-grid">
                        <ClusterTimeline data=data.clone()/>
                        <NodeHeatmap data=data.clone()/>
                        <IoRidges data=data.clone()/>
                        <TopMovers data=data.clone()/>
                        <CoveragePanel data=data controllers=controllers.get()/>
                    </div>
                }.into_view(),
                Some(data) => view! {
                    <div class="statistics-empty glass-panel">
                        <i class="ph ph-chart-line"></i>
                        <h3>"No historical samples in this window"</h3>
                        <p>{data.coverage_note}</p>
                    </div>
                }.into_view(),
                None => ().into_view(),
            }}
        </div>
    }
}

fn fetch_statistics(
    selected_range: String,
    set_history: WriteSignal<Option<NodeMetricsHistoryResponse>>,
    set_controllers: WriteSignal<Vec<ControllerInfo>>,
    set_loading: WriteSignal<bool>,
    set_error: WriteSignal<Option<String>>,
    show_loading: bool,
) {
    spawn_local(async move {
        if show_loading {
            set_loading.set(true);
        }
        set_error.set(None);

        let now = (js_sys::Date::now() / 1000.0) as u64;
        let window = range_seconds(&selected_range);
        let start = now.saturating_sub(window);
        let bucket_seconds = bucket_seconds_for_window(window);
        let url = format!(
            "/api/v1/metrics/nodes?start={}&end={}&bucket_seconds={}",
            start, now, bucket_seconds
        );

        match api_get(&url).send().await {
            Ok(resp) if resp.ok() => match resp.json::<NodeMetricsHistoryResponse>().await {
                Ok(data) => set_history.set(Some(data)),
                Err(err) => set_error.set(Some(format!("Failed to decode metrics: {err}"))),
            },
            Ok(resp) => set_error.set(Some(format!(
                "Metrics request failed with HTTP {}",
                resp.status()
            ))),
            Err(err) => set_error.set(Some(format!("Metrics request failed: {err}"))),
        }

        if let Ok(resp) = api_get("/api/v1/controllers").send().await {
            if resp.ok() {
                if let Ok(data) = resp.json::<Vec<ControllerInfo>>().await {
                    set_controllers.set(data);
                }
            }
        }

        if show_loading {
            set_loading.set(false);
        }
    });
}

#[component]
fn StatsSummary(data: NodeMetricsHistoryResponse) -> impl IntoView {
    let latest = data.cluster.last();
    let avg_cpu = latest.map(|b| b.cpu_avg).unwrap_or_default();
    let peak_cpu = data
        .cluster
        .iter()
        .map(|bucket| bucket.cpu_peak)
        .fold(0.0, f32::max);
    let avg_mem = latest.map(|b| b.memory_pct_avg).unwrap_or_default();
    let total_io = latest
        .map(|b| b.net_rx_avg + b.net_tx_avg + b.disk_read_avg + b.disk_write_avg)
        .unwrap_or_default();

    view! {
        <section class="statistics-summary">
            <MetricCard label="Nodes Tracked" value=data.node_count.to_string() detail=format!("{} raw samples", data.sample_count)/>
            <MetricCard label="Latest Avg CPU" value=format!("{avg_cpu:.1}%") detail=format!("Peak {peak_cpu:.1}%")/>
            <MetricCard label="Latest Memory" value=format!("{avg_mem:.1}%") detail=format!("Bucket {}s", data.bucket_seconds)/>
            <MetricCard label="Latest I/O Rate" value=format_rate(total_io) detail=format!("{} to {}", format_time(data.start_time), format_time(data.end_time))/>
        </section>
    }
}

#[component]
fn MetricCard(label: &'static str, value: String, detail: String) -> impl IntoView {
    view! {
        <article class="statistics-card">
            <span>{label}</span>
            <strong>{value}</strong>
            <small>{detail}</small>
        </article>
    }
}

#[component]
fn ClusterTimeline(data: NodeMetricsHistoryResponse) -> impl IntoView {
    let cpu_path = cluster_path(&data.cluster, |bucket| bucket.cpu_avg, 100.0);
    let peak_path = cluster_path(&data.cluster, |bucket| bucket.cpu_peak, 100.0);
    let mem_path = cluster_path(&data.cluster, |bucket| bucket.memory_pct_avg, 100.0);
    let jobs_max = data
        .cluster
        .iter()
        .map(|bucket| bucket.running_jobs_avg)
        .fold(1.0, f32::max);
    let jobs_path = cluster_path(&data.cluster, |bucket| bucket.running_jobs_avg, jobs_max);

    view! {
        <section class="statistics-panel statistics-panel-wide">
            <PanelHeader title="Cluster Utilization Timeline" icon="ph ph-wave-sine"/>
            <div class="statistics-chart-frame">
                <svg viewBox="0 0 100 100" preserveAspectRatio="none">
                    <path d=peak_path class="stats-line stats-line-muted"/>
                    <path d=mem_path class="stats-line stats-line-memory"/>
                    <path d=jobs_path class="stats-line stats-line-jobs"/>
                    <path d=cpu_path class="stats-line stats-line-cpu"/>
                </svg>
            </div>
            <div class="statistics-legend">
                <span><i class="legend-cpu"></i>"CPU avg"</span>
                <span><i class="legend-muted"></i>"CPU peak"</span>
                <span><i class="legend-memory"></i>"Memory"</span>
                <span><i class="legend-jobs"></i>"Running jobs"</span>
            </div>
        </section>
    }
}

#[component]
fn NodeHeatmap(data: NodeMetricsHistoryResponse) -> impl IntoView {
    let max_columns = data
        .nodes
        .iter()
        .map(|node| node.samples.len())
        .max()
        .unwrap_or(1)
        .max(1);
    let rows = data
        .nodes
        .iter()
        .take(18)
        .map(|node| {
            let cells = node
                .samples
                .iter()
                .map(|sample| {
                    let intensity = (sample.cpu_avg / 100.0).clamp(0.0, 1.0);
                    let color = heat_color(intensity);
                    let width = 100.0 / max_columns as f32;
                    view! {
                        <div
                            class="heat-cell"
                            title=format!("{} · CPU {:.1}% · MEM {:.1}%", node.node_id, sample.cpu_avg, sample.memory_pct_avg)
                            style=format!("width: {:.3}%; background: {};", width, color)
                        ></div>
                    }
                })
                .collect::<Vec<_>>();
            view! {
                <div class="heat-row">
                    <span>{node_label(node)}</span>
                    <div class="heat-strip">{cells}</div>
                </div>
            }
        })
        .collect::<Vec<_>>();

    view! {
        <section class="statistics-panel statistics-panel-wide">
            <PanelHeader title="Node Heatmap" icon="ph ph-grid-nine"/>
            <div class="heatmap">{rows}</div>
        </section>
    }
}

#[component]
fn IoRidges(data: NodeMetricsHistoryResponse) -> impl IntoView {
    let mut nodes = data.nodes.clone();
    nodes.sort_by_key(|b| std::cmp::Reverse(avg_total_io(b)));
    let max_io = nodes.iter().map(avg_total_io).max().unwrap_or(1).max(1);
    let rows = nodes
        .into_iter()
        .take(6)
        .map(|node| {
            let net_pct = avg_net_io(&node) as f64 / max_io as f64 * 100.0;
            let disk_pct = avg_disk_io(&node) as f64 / max_io as f64 * 100.0;
            view! {
                <div class="ridge-row">
                    <span>{node_label(&node)}</span>
                    <div class="ridge-bars">
                        <div class="ridge-bar ridge-net" style=format!("width: {:.2}%;", net_pct)></div>
                        <div class="ridge-bar ridge-disk" style=format!("width: {:.2}%;", disk_pct)></div>
                    </div>
                    <strong>{format_rate(avg_total_io(&node))}</strong>
                </div>
            }
        })
        .collect::<Vec<_>>();

    view! {
        <section class="statistics-panel">
            <PanelHeader title="I/O Throughput Ridges" icon="ph ph-hard-drive"/>
            <div class="ridge-list">{rows}</div>
            <div class="statistics-legend">
                <span><i class="legend-net"></i>"Network"</span>
                <span><i class="legend-disk"></i>"Disk"</span>
            </div>
        </section>
    }
}

#[component]
fn TopMovers(data: NodeMetricsHistoryResponse) -> impl IntoView {
    let movers = top_movers(&data);
    let cards = movers
        .into_iter()
        .map(|mover| {
            view! {
                <article class="top-mover" style=format!("border-color: {};", mover.accent)>
                    <span>{mover.label}</span>
                    <strong>{mover.value}</strong>
                    <small>{mover.detail}</small>
                </article>
            }
        })
        .collect::<Vec<_>>();

    view! {
        <section class="statistics-panel">
            <PanelHeader title="Top Movers" icon="ph ph-ranking"/>
            <div class="top-movers">{cards}</div>
        </section>
    }
}

#[component]
fn CoveragePanel(
    data: NodeMetricsHistoryResponse,
    controllers: Vec<ControllerInfo>,
) -> impl IntoView {
    let controller_rows = controllers
        .into_iter()
        .map(|controller| {
            let status = if controller.online { "ONLINE" } else { "OFFLINE" };
            view! {
                <div class="controller-row">
                    <span>{controller.hostname}</span>
                    <small>{controller.role}</small>
                    <strong class=if controller.online { "online" } else { "offline" }>{status}</strong>
                </div>
            }
        })
        .collect::<Vec<_>>();

    view! {
        <section class="statistics-panel statistics-panel-wide coverage-panel">
            <PanelHeader title="Telemetry Coverage" icon="ph ph-radar"/>
            <div class="coverage-grid">
                <div>
                    <h3>"Worker history"</h3>
                    <p>"Recorded through worker `NodeMetrics` pulses and stored by the controller for historical analysis."</p>
                    <strong>{if data.management_node_history { "MANAGEMENT HOSTS INCLUDED" } else { "WORKER HOSTS ONLY" }}</strong>
                </div>
                <div>
                    <h3>"Management nodes"</h3>
                    <p>{data.coverage_note}</p>
                </div>
                <div>
                    <h3>"HA controllers"</h3>
                    <div class="controller-list">{controller_rows}</div>
                </div>
            </div>
        </section>
    }
}

#[component]
fn PanelHeader(title: &'static str, icon: &'static str) -> impl IntoView {
    view! {
        <div class="statistics-panel-header">
            <span>{title}</span>
            <i class=icon></i>
        </div>
    }
}

fn range_seconds(range: &str) -> u64 {
    match range {
        "1h" => 60 * 60,
        "24h" => 24 * 60 * 60,
        "7d" => 7 * 24 * 60 * 60,
        _ => 6 * 60 * 60,
    }
}

fn bucket_seconds_for_window(window: u64) -> u64 {
    (window / 180).clamp(30, 6 * 60 * 60)
}

fn cluster_path<F>(samples: &[ClusterMetricsBucket], value: F, max_value: f32) -> String
where
    F: Fn(&ClusterMetricsBucket) -> f32,
{
    if samples.is_empty() {
        return String::new();
    }
    let denominator = samples.len().saturating_sub(1).max(1) as f32;
    samples
        .iter()
        .enumerate()
        .map(|(index, sample)| {
            let x = index as f32 / denominator * 100.0;
            let y = 100.0 - (value(sample) / max_value.max(1.0)).clamp(0.0, 1.0) * 100.0;
            if index == 0 {
                format!("M {:.3} {:.3}", x, y)
            } else {
                format!("L {:.3} {:.3}", x, y)
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn heat_color(intensity: f32) -> String {
    let alpha = 0.18 + intensity * 0.72;
    if intensity > 0.82 {
        format!("rgba(251, 113, 133, {alpha:.3})")
    } else if intensity > 0.55 {
        format!("rgba(245, 158, 11, {alpha:.3})")
    } else {
        format!("rgba(56, 189, 248, {alpha:.3})")
    }
}

fn node_label(node: &NodeMetricsNodeSeries) -> String {
    node.hostname
        .as_ref()
        .filter(|hostname| !hostname.is_empty())
        .cloned()
        .unwrap_or_else(|| node.node_id.clone())
}

fn avg_net_io(node: &NodeMetricsNodeSeries) -> u64 {
    avg_by(node, |sample| sample.net_rx_avg + sample.net_tx_avg)
}

fn avg_disk_io(node: &NodeMetricsNodeSeries) -> u64 {
    avg_by(node, |sample| sample.disk_read_avg + sample.disk_write_avg)
}

fn avg_total_io(node: &NodeMetricsNodeSeries) -> u64 {
    avg_net_io(node) + avg_disk_io(node)
}

fn avg_by<F>(node: &NodeMetricsNodeSeries, value: F) -> u64
where
    F: Fn(&NodeMetricsBucket) -> u64,
{
    if node.samples.is_empty() {
        return 0;
    }
    node.samples.iter().map(value).sum::<u64>() / node.samples.len() as u64
}

fn top_movers(data: &NodeMetricsHistoryResponse) -> Vec<TopMover> {
    let mut highest_cpu = None::<(&NodeMetricsNodeSeries, f32)>;
    let mut highest_mem = None::<(&NodeMetricsNodeSeries, f32)>;
    let mut highest_io = None::<(&NodeMetricsNodeSeries, u64)>;
    let mut quietest = None::<(&NodeMetricsNodeSeries, f32)>;

    for node in &data.nodes {
        let cpu_peak = node.samples.iter().map(|s| s.cpu_max).fold(0.0, f32::max);
        let mem_peak = node
            .samples
            .iter()
            .map(|s| s.memory_pct_max)
            .fold(0.0, f32::max);
        let io = avg_total_io(node);
        let avg_cpu = if node.samples.is_empty() {
            0.0
        } else {
            node.samples.iter().map(|s| s.cpu_avg).sum::<f32>() / node.samples.len() as f32
        };

        if highest_cpu
            .map(|(_, value)| cpu_peak > value)
            .unwrap_or(true)
        {
            highest_cpu = Some((node, cpu_peak));
        }
        if highest_mem
            .map(|(_, value)| mem_peak > value)
            .unwrap_or(true)
        {
            highest_mem = Some((node, mem_peak));
        }
        if highest_io.map(|(_, value)| io > value).unwrap_or(true) {
            highest_io = Some((node, io));
        }
        if quietest.map(|(_, value)| avg_cpu < value).unwrap_or(true) {
            quietest = Some((node, avg_cpu));
        }
    }

    let mut movers = Vec::new();
    if let Some((node, value)) = highest_cpu {
        movers.push(TopMover {
            label: "CPU peak".to_string(),
            value: format!("{value:.1}%"),
            detail: node_label(node),
            accent: "var(--primary)",
        });
    }
    if let Some((node, value)) = highest_mem {
        movers.push(TopMover {
            label: "Memory peak".to_string(),
            value: format!("{value:.1}%"),
            detail: node_label(node),
            accent: "var(--warning)",
        });
    }
    if let Some((node, value)) = highest_io {
        movers.push(TopMover {
            label: "I/O average".to_string(),
            value: format_rate(value),
            detail: node_label(node),
            accent: "var(--success)",
        });
    }
    if let Some((node, value)) = quietest {
        movers.push(TopMover {
            label: "Quietest node".to_string(),
            value: format!("{value:.1}%"),
            detail: node_label(node),
            accent: "var(--text-muted)",
        });
    }
    movers
}

fn format_rate(bytes_per_second: u64) -> String {
    let value = bytes_per_second as f64;
    if value >= 1024.0 * 1024.0 * 1024.0 {
        format!("{:.1} GiB/s", value / 1024.0 / 1024.0 / 1024.0)
    } else if value >= 1024.0 * 1024.0 {
        format!("{:.1} MiB/s", value / 1024.0 / 1024.0)
    } else if value >= 1024.0 {
        format!("{:.1} KiB/s", value / 1024.0)
    } else {
        format!("{} B/s", bytes_per_second)
    }
}

fn format_time(timestamp: u64) -> String {
    let date = js_sys::Date::new(&wasm_bindgen::JsValue::from_f64(timestamp as f64 * 1000.0));
    format!("{:02}:{:02}", { date.get_hours() }, { date.get_minutes() })
}
