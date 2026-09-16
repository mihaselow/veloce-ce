use crate::WebConfig;
use gloo_net::http::Request;
use leptos::*;
use std::collections::{BTreeMap, VecDeque};
use veloce_common::{ControllerInfo, JobInfo, JobStatus, WorkerInfo};

// Helper to generate sparkline SVG paths
pub(crate) fn generate_sparkline_path(history: &VecDeque<f32>, max_val: f32) -> (String, String) {
    if history.is_empty() {
        return (
            "M 0 40 L 200 40".to_string(),
            "M 0 40 L 200 40 Z".to_string(),
        );
    }
    let mut points = Vec::new();
    let count = history.len();
    for (i, &val) in history.iter().enumerate() {
        let x = if count > 1 {
            (i as f32 / (count - 1) as f32) * 200.0
        } else {
            0.0
        };
        let pct = (val / max_val).clamp(0.0, 1.0);
        let y = 38.0 - pct * 33.0; // Leave margin at top and bottom
        points.push(format!("{:.1},{:.1}", x, y));
    }
    let line_path = points.join(" L ");
    let area_path = format!("M 0 40 L {} L 200 40 Z", line_path);
    (format!("M {}", line_path), area_path)
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum WorkerVisualState {
    Free,
    Partial,
    Busy,
    Offline,
}

impl WorkerVisualState {
    fn label(self) -> &'static str {
        match self {
            Self::Free => "FREE",
            Self::Partial => "PARTIAL",
            Self::Busy => "BUSY",
            Self::Offline => "OFFLINE",
        }
    }

    fn colors(self) -> (&'static str, &'static str) {
        match self {
            Self::Free => ("#4caf50", "#a5d6a7"),
            Self::Partial => ("#00bcd4", "var(--ds-cyan-glow)"),
            Self::Busy => ("#ffb74d", "var(--ds-orange-glow)"),
            Self::Offline => ("#78909c", "#b0bec5"),
        }
    }
}

#[derive(Clone)]
struct ComputeVisualItem {
    x: f64,
    y: f64,
    label: String,
    detail: String,
    state: WorkerVisualState,
    count: usize,
    activity: u64,
}

#[derive(Clone)]
struct WorkerAggregate {
    count: usize,
    total_cores: usize,
    busy_cores: usize,
    state: WorkerVisualState,
    activity: u64,
}

impl Default for WorkerAggregate {
    fn default() -> Self {
        Self {
            count: 0,
            total_cores: 0,
            busy_cores: 0,
            state: WorkerVisualState::Free,
            activity: 0,
        }
    }
}

fn worker_visual_state(worker: &WorkerInfo) -> WorkerVisualState {
    if !worker.online {
        WorkerVisualState::Offline
    } else {
        let busy_cores = worker.total_cores.saturating_sub(worker.available_cores);
        if busy_cores == 0 {
            WorkerVisualState::Free
        } else if busy_cores >= worker.total_cores {
            WorkerVisualState::Busy
        } else {
            WorkerVisualState::Partial
        }
    }
}

fn orbital_position(index: usize, total: usize, center_x: f64, center_y: f64) -> (f64, f64) {
    let (ring_index, ring_total, radius) = if total <= 12 {
        (index, total.max(1), 42.0)
    } else if index < 12 {
        (index, 12, 34.0)
    } else {
        (index - 12, total.saturating_sub(12).max(1), 60.0)
    };
    let angle = -std::f64::consts::FRAC_PI_2
        + (ring_index as f64 / ring_total as f64) * std::f64::consts::TAU;
    (
        center_x + radius * angle.cos(),
        center_y + radius * angle.sin(),
    )
}

fn build_compute_visual_items(
    workers: Vec<WorkerInfo>,
    center_x: f64,
    center_y: f64,
) -> Vec<ComputeVisualItem> {
    let worker_count = workers.len();
    if worker_count <= 32 {
        return workers
            .into_iter()
            .enumerate()
            .map(|(idx, worker)| {
                let (x, y) = orbital_position(idx, worker_count, center_x, center_y);
                let state = worker_visual_state(&worker);
                ComputeVisualItem {
                    x,
                    y,
                    label: worker.hostname,
                    detail: if worker.online {
                        format!("{:.0}% CPU", worker.cpu_usage)
                    } else {
                        "OFFLINE".to_string()
                    },
                    state,
                    count: 1,
                    activity: worker.net_rx_rate + worker.net_tx_rate,
                }
            })
            .collect();
    }

    let mut controller_status_groups =
        BTreeMap::<(String, WorkerVisualState), WorkerAggregate>::new();
    for worker in &workers {
        let controller = worker
            .controller_id
            .clone()
            .unwrap_or_else(|| "unassigned".to_string());
        let state = worker_visual_state(worker);
        let entry = controller_status_groups
            .entry((controller, state))
            .or_default();
        entry.count += 1;
        entry.total_cores += worker.total_cores;
        entry.busy_cores += worker.total_cores.saturating_sub(worker.available_cores);
        entry.state = state;
        entry.activity += worker.net_rx_rate + worker.net_tx_rate;
    }

    let grouped = if workers.len() <= 500 && controller_status_groups.len() <= 32 {
        controller_status_groups
            .into_iter()
            .map(|((controller, state), aggregate)| {
                (
                    format!("{} {}", aggregate.count, state.label()),
                    controller,
                    aggregate,
                )
            })
            .collect::<Vec<_>>()
    } else {
        let mut status_groups = BTreeMap::<WorkerVisualState, WorkerAggregate>::new();
        for worker in &workers {
            let state = worker_visual_state(worker);
            let entry = status_groups.entry(state).or_default();
            entry.count += 1;
            entry.total_cores += worker.total_cores;
            entry.busy_cores += worker.total_cores.saturating_sub(worker.available_cores);
            entry.state = state;
            entry.activity += worker.net_rx_rate + worker.net_tx_rate;
        }
        status_groups
            .into_iter()
            .map(|(state, aggregate)| {
                (
                    format!("{} {}", aggregate.count, state.label()),
                    "cluster aggregate".to_string(),
                    aggregate,
                )
            })
            .collect::<Vec<_>>()
    };

    let total_groups = grouped.len();
    grouped
        .into_iter()
        .enumerate()
        .map(|(idx, (label, detail, aggregate))| {
            let (x, y) = orbital_position(idx, total_groups, center_x, center_y);
            ComputeVisualItem {
                x,
                y,
                label,
                detail: format!(
                    "{} · {}/{} cores",
                    detail, aggregate.busy_cores, aggregate.total_cores
                ),
                state: aggregate.state,
                count: aggregate.count,
                activity: aggregate.activity,
            }
        })
        .collect()
}

#[component]
pub fn TelemetryPage(
    jobs: Signal<Vec<JobInfo>>,
    nodes: Signal<Vec<WorkerInfo>>,
    set_nodes: WriteSignal<Vec<WorkerInfo>>,
    controller_online: Signal<bool>,
    fileserver_online: Signal<bool>,
) -> impl IntoView {
    let (cpu_history, set_cpu_history) = create_signal(VecDeque::<f32>::new());
    let (mem_history, set_mem_history) = create_signal(VecDeque::<f32>::new());
    let (net_history, set_net_history) = create_signal(VecDeque::<f32>::new());
    let (controllers, set_controllers) = create_signal(Vec::<ControllerInfo>::new());

    // History update effect (records telemetry every 2 seconds)
    create_effect(move |_| {
        let n = nodes.get();
        let avg_cpu = if n.is_empty() {
            0.0
        } else {
            n.iter().map(|x| x.cpu_usage).sum::<f32>() / n.len() as f32
        };
        let avg_mem = if n.is_empty() {
            0.0
        } else {
            n.iter()
                .map(|x| x.used_memory as f32 / x.total_memory.max(1) as f32 * 100.0)
                .sum::<f32>()
                / n.len() as f32
        };
        let total_net = n
            .iter()
            .map(|x| (x.net_rx_rate + x.net_tx_rate) as f32 / 1024.0 / 1024.0)
            .sum::<f32>(); // MB/s

        set_cpu_history.update(|h| {
            h.push_back(avg_cpu);
            if h.len() > 30 {
                h.pop_front();
            }
        });
        set_mem_history.update(|h| {
            h.push_back(avg_mem);
            if h.len() > 30 {
                h.pop_front();
            }
        });
        set_net_history.update(|h| {
            h.push_back(total_net);
            if h.len() > 30 {
                h.pop_front();
            }
        });
    });

    let config =
        use_context::<Resource<bool, Option<WebConfig>>>().expect("WebConfig context missing");

    // Periodic telemetry loop for controllers and nodes (2 seconds)
    let set_nodes_clone = set_nodes;
    create_effect(move |_| {
        let conf = config.get().flatten();
        if let Some(c) = conf {
            let handle = crate::set_interval_with_handle(
                move || {
                    let c_clone = c.clone();
                    let set_nodes_inner = set_nodes_clone;
                    spawn_local(async move {
                        if let Ok(resp) = Request::get("/api/v1/nodes")
                            .header("X-API-KEY", &c_clone.controller_api_key)
                            .send()
                            .await
                        {
                            if let Ok(data) = resp.json::<Vec<WorkerInfo>>().await {
                                set_nodes_inner.set(data);
                            }
                        }
                        if let Ok(resp) = Request::get("/api/v1/controllers")
                            .header("X-API-KEY", &c_clone.controller_api_key)
                            .send()
                            .await
                        {
                            if let Ok(data) = resp.json::<Vec<ControllerInfo>>().await {
                                set_controllers.set(data);
                            }
                        }
                    });
                },
                2000,
            );
            on_cleanup(move || {
                if let Ok(id) = handle {
                    if let Some(win) = web_sys::window() {
                        win.clear_interval_with_handle(id);
                    }
                }
            });
        }
    });

    // Helper statistics
    let running_jobs = move || {
        jobs.get()
            .iter()
            .filter(|j| matches!(j.status, JobStatus::Running))
            .count()
    };
    let total_jobs = move || jobs.get().len();
    let running_job_list = move || {
        let mut list = jobs
            .get()
            .into_iter()
            .filter(|j| matches!(j.status, JobStatus::Running))
            .collect::<Vec<_>>();

        list.sort_by(|a, b| {
            let b_time = b.start_time.unwrap_or(b.queued_time);
            let a_time = a.start_time.unwrap_or(a.queued_time);
            b_time.cmp(&a_time).then_with(|| b.id.cmp(&a.id))
        });
        list
    };

    let total_cores = move || nodes.get().iter().map(|n| n.total_cores).sum::<usize>();
    let used_cores = move || {
        nodes
            .get()
            .iter()
            .map(|n| n.total_cores - n.available_cores)
            .sum::<usize>()
    };

    let total_mem_gb = move || {
        nodes
            .get()
            .iter()
            .map(|n| n.total_memory as f64 / 1024.0 / 1024.0 / 1024.0)
            .sum::<f64>()
    };
    let used_mem_gb = move || {
        nodes
            .get()
            .iter()
            .map(|n| n.used_memory as f64 / 1024.0 / 1024.0 / 1024.0)
            .sum::<f64>()
    };

    let total_net_mb_s = move || {
        nodes
            .get()
            .iter()
            .map(|n| (n.net_rx_rate + n.net_tx_rate) as f64 / 1024.0 / 1024.0)
            .sum::<f64>()
    };

    let cpu_load_pct = move || {
        let t = total_cores();
        if t > 0 {
            (used_cores() as f32 / t as f32) * 100.0
        } else {
            0.0
        }
    };

    let mem_load_pct = move || {
        let t = total_mem_gb();
        if t > 0.0 {
            (used_mem_gb() / t) as f32 * 100.0
        } else {
            0.0
        }
    };

    view! {
        <div class="telemetry-theme">
            <header class="ds-flex ds-justify-between ds-items-center" style="border-bottom: 1px solid rgba(45, 120, 160, 0.2); padding-bottom: 0.5rem; margin-bottom: 0.25rem;">
                <div class="ds-flex ds-items-center ds-gap-3">
                    <svg width="24" height="24" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg" class="ds-svg-glow-cyan">
                        <path d="M12 2L2 22H22L12 2Z" stroke="var(--ds-cyan-glow)" stroke-width="2" fill="rgba(0, 229, 255, 0.2)"/>
                        <path d="M12 8L6 19H18L12 8Z" stroke="var(--ds-cyan-glow)" stroke-width="1"/>
                    </svg>
                    <h1 class="ds-text-xl ds-font-bold ds-tracking-widest ds-uppercase" style="margin: 0;">
                        "Veloce HPC Cluster " <span class="ds-text-muted" style="font-weight: 500;">"- Telemetry"</span>
                    </h1>
                </div>
                <div class="ds-flex ds-items-center ds-gap-4">
                    <div class="ds-text-xs ds-font-mono ds-text-cyan">
                        "STATUS: TELEMETRY LINK STABLE"
                        {move || {
                            let list = controllers.get();
                            if list.is_empty() {
                                "".to_string()
                            } else {
                                format!(" | CTRLS: {}", list.iter().map(|c| format!("{} ({})", c.hostname, if c.online { "ON" } else { "OFF" })).collect::<Vec<_>>().join(", "))
                            }
                        }}
                    </div>
                </div>
            </header>

            <div class="telemetry-grid">

                <div class="telemetry-panel grid-thermal">
                    <div class="telemetry-panel-header">
                        <span>
                            {move || {
                                let w_list = nodes.get();
                                let total_cores: usize = w_list.iter().map(|n| n.total_cores).sum();
                                if total_cores < 1000 {
                                    "Real-Time CPU Cores Grid"
                                } else {
                                    "Real-Time Cluster Nodes Grid"
                                }
                            }}
                        </span>
                        <i class="ph ph-cpu"></i>
                    </div>
                    <div class="ds-flex-grow ds-flex ds-items-center ds-justify-center" style="max-height: 160px; overflow: hidden;">
                        {move || {
                            let w_list = nodes.get();
                            if w_list.is_empty() {
                                view! { <div class="ds-text-muted ds-text-xs">"Waiting for compute nodes..."</div> }.into_view()
                            } else {
                                let total_cores: usize = w_list.iter().map(|n| n.total_cores).sum();
                                let mut items = Vec::new();

                                if total_cores < 1000 {
                                    for node in &w_list {
                                        let busy_cores = node.total_cores - node.available_cores;
                                        let is_online = node.online;
                                        for i in 0..node.total_cores {
                                            let state = if !is_online {
                                                "offline"
                                            } else if i < busy_cores {
                                                "busy"
                                            } else {
                                                "free"
                                            };
                                            items.push((state, format!("Node: {} | Core {} Status: {}", node.hostname, i, state)));
                                        }
                                    }
                                } else {
                                    for node in &w_list {
                                        let is_online = node.online;
                                        let state = if !is_online {
                                            "offline"
                                        } else {
                                            let busy_cores = node.total_cores - node.available_cores;
                                            if busy_cores == 0 {
                                                "free"
                                            } else if busy_cores == node.total_cores {
                                                "busy"
                                            } else {
                                                "partial"
                                            }
                                        };
                                        items.push((state, format!("Node: {} | Status: {}", node.hostname, state)));
                                    }
                                }

                                let count = items.len();
                                let size = if count == 0 {
                                    24.0
                                } else {
                                    let w = 310.0;
                                    let h = 140.0;
                                    let g = 3.0;
                                    let mut best_size = 5.0;
                                    for cols in 1..=count {
                                        let rows = count.div_ceil(cols);
                                        let size_w = (w - (cols - 1) as f64 * g) / cols as f64;
                                        let size_h = (h - (rows - 1) as f64 * g) / rows as f64;
                                        let size = size_w.min(size_h);
                                        if size > best_size {
                                            best_size = size;
                                        }
                                    }
                                    best_size.clamp(5.0, 64.0)
                                };

                                let grid_cells = items.into_iter().map(|(state, title)| {
                                    view! {
                                        <div class={format!("ds-core-square {}", state)}
                                             style={format!("width: {:.1}px; height: {:.1}px; border-radius: 2px;", size, size)}
                                             title={title}></div>
                                    }
                                }).collect::<Vec<_>>();

                                view! {
                                    <div style="display: flex; flex-wrap: wrap; gap: 3px; align-content: flex-start; justify-content: center; width: 100%; height: 100%; max-height: 140px; overflow-y: auto; padding: 4px;">
                                        {grid_cells}
                                    </div>
                                }.into_view()
                            }
                        }}
                    </div>
                </div>

                <div class="telemetry-panel grid-nodes-status">
                    <div class="telemetry-panel-header">
                        <span>"Active Nodal Inventory"</span>
                        <i class="ph ph-list-dashes"></i>
                    </div>
                    <div class="ds-flex-grow" style="overflow-y: auto; font-family: monospace; font-size: 0.75rem;">
                        <table style="width: 100%; border-collapse: collapse; text-align: left;">
                            <thead>
                                <tr style="color: var(--ds-text-muted); border-bottom: 1px solid rgba(45, 120, 160, 0.2);">
                                    <th style="padding: 2px 4px;">"NODE"</th>
                                    <th style="padding: 2px 4px; text-align: center;">"CORES"</th>
                                    <th style="padding: 2px 4px; text-align: right;">"MEM"</th>
                                    <th style="padding: 2px 4px; text-align: right;">"UPTIME"</th>
                                </tr>
                            </thead>
                            <tbody>
                                {move || {
                                    let w_list = nodes.get();
                                    if w_list.is_empty() {
                                        view! {
                                            <tr>
                                                <td colspan="4" style="text-align: center; color: var(--ds-text-muted); padding: 8px;">
                                                    "No connected nodes"
                                                </td>
                                            </tr>
                                        }.into_view()
                                    } else {
                                        w_list.into_iter().map(|node| {
                                            let allocated_cores = node.total_cores - node.available_cores;
                                            let total_mem_gb = node.total_memory as f64 / 1024.0 / 1024.0 / 1024.0;
                                            let allocated_mem_gb = node.allocated_memory as f64;
                                            let uptime_hours = node.uptime as f64 / 3600.0;
                                            let uptime_display = if uptime_hours >= 1.0 {
                                                format!("{:.1}h", uptime_hours)
                                            } else {
                                                format!("{}m", node.uptime / 60)
                                            };
                                            view! {
                                                <tr style="border-bottom: 1px solid rgba(255, 255, 255, 0.02); height: 22px;">
                                                    <td style="padding: 2px 4px; color: var(--ds-cyan-glow); font-weight: bold;">
                                                        {node.hostname}
                                                    </td>
                                                    <td style="padding: 2px 4px; text-align: center;">
                                                        {format!("{}/{}", allocated_cores, node.total_cores)}
                                                    </td>
                                                    <td style="padding: 2px 4px; text-align: right;">
                                                        {format!("{:.0}/{:.0}G", allocated_mem_gb, total_mem_gb)}
                                                    </td>
                                                    <td style="padding: 2px 4px; text-align: right; color: var(--ds-text-muted);">
                                                        {uptime_display}
                                                    </td>
                                                </tr>
                                            }
                                        }).collect::<Vec<_>>().into_view()
                                    }
                                }}
                            </tbody>
                        </table>
                    </div>
                </div>

                <div class="telemetry-panel grid-network">
                    <div class="telemetry-panel-header">
                        <span>"Infrastructure Nodal Topology"</span>
                        <i class="ph ph-git-branch"></i>
                    </div>

                    <div class="ds-flex-grow ds-relative ds-flex ds-items-center ds-justify-center" style="min-height: 280px;">
                        <svg viewBox="0 0 600 370" class="ds-w-full ds-h-full" style="overflow: visible;">
                            <defs>
                                <filter id="ds-glow-cyan" x="-20%" y="-20%" width="140%" height="140%">
                                    <feGaussianBlur stdDeviation="3" result="blur" />
                                    <feComposite in="SourceGraphic" in2="blur" operator="over" />
                                </filter>
                                <filter id="ds-glow-orange" x="-20%" y="-20%" width="140%" height="140%">
                                    <feGaussianBlur stdDeviation="3" result="blur" />
                                    <feComposite in="SourceGraphic" in2="blur" operator="over" />
                                </filter>
                            </defs>

                            {move || {
                                let workers = nodes.get();
                                let worker_count = workers.len();
                                let worker_cores = workers.iter().map(|worker| worker.total_cores).sum::<usize>();
                                let worker_busy_cores = workers
                                    .iter()
                                    .map(|worker| worker.total_cores.saturating_sub(worker.available_cores))
                                    .sum::<usize>();
                                let ctrl_online_status = controller_online.get();
                                let fs_online_status = fileserver_online.get();

                                let fs_x = 410.0;
                                let fs_y = 205.0;
                                let compute_x = 300.0;
                                let compute_y = 280.0;
                                let compute_items =
                                    build_compute_visual_items(workers, compute_x, compute_y);

                                let mut connections = Vec::new();
                                let mut nodes_rendered = Vec::new();

                                let ctrls = controllers.get();
                                let display_ctrls = if ctrls.is_empty() {
                                    vec![ControllerInfo {
                                        hostname: "CTRL-HA-1".to_string(),
                                        role: "Leader".to_string(),
                                        online: ctrl_online_status,
                                    }]
                                } else {
                                    ctrls
                                };

                                let num_ctrls = display_ctrls.len();
                                let mut ctrl_positions = Vec::new();
                                for (i, ctrl) in display_ctrls.iter().enumerate() {
                                    let (cx, cy) = if num_ctrls <= 1 {
                                        (300.0, 120.0)
                                    } else {
                                        let t = i as f64 / (num_ctrls - 1) as f64;
                                        (220.0 + 160.0 * t, 120.0)
                                    };
                                    ctrl_positions.push((cx, cy, ctrl.clone()));
                                }

                                for &(cx, cy, ref ctrl) in ctrl_positions.iter() {
                                    if ctrl.online && fs_online_status {
                                        connections.push(view! {
                                            <line x1={cx} y1={cy} x2={fs_x} y2={fs_y} stroke="var(--ds-orange-glow)" stroke-width="1.2" opacity="0.7"/>
                                        });
                                    }

                                    if ctrl.online {
                                        connections.push(view! {
                                            <line x1={cx} y1={cy} x2={compute_x} y2={compute_y} stroke="var(--ds-cyan-glow)" stroke-width="1.1" opacity="0.45"/>
                                        });
                                    } else {
                                        connections.push(view! {
                                            <line x1={cx} y1={cy} x2={compute_x} y2={compute_y} stroke="#4a5d6e" stroke-width="0.8" stroke-dasharray="2 4" opacity="0.35"/>
                                        });
                                    }
                                }

                                if fs_online_status {
                                    connections.push(view! {
                                        <line x1={fs_x} y1={fs_y} x2={compute_x} y2={compute_y} stroke="var(--ds-orange-glow)" stroke-width="1" opacity="0.45"/>
                                    });
                                }

                                for item in &compute_items {
                                    let (fill, stroke) = item.state.colors();
                                    let speed_class = if item.activity > 5 * 1024 * 1024 {
                                        "ds-flow-line"
                                    } else {
                                        ""
                                    };
                                    connections.push(view! {
                                        <line x1={compute_x} y1={compute_y} x2={item.x} y2={item.y} stroke={stroke} stroke-width="0.9" opacity="0.45" class={speed_class}/>
                                    });

                                    let radius = if item.count > 1 { 15.0 } else { 8.0 };
                                    let count_label = if item.count > 1 {
                                        item.count.to_string()
                                    } else {
                                        "".to_string()
                                    };
                                    nodes_rendered.push(view! {
                                        <g transform={format!("translate({}, {})", item.x, item.y)}>
                                            <circle r={radius} fill={fill} stroke={stroke} stroke-width="1.5" opacity="0.95"/>
                                            <text x="0" y="3" fill="#071018" font-size="9" font-family="monospace" font-weight="800" text-anchor="middle">
                                                {count_label}
                                            </text>
                                            <text x="0" y={format!("{}", radius + 12.0)} fill="#e2e8f0" font-size="8.5" font-weight="600" text-anchor="middle">
                                                {item.label.clone()}
                                            </text>
                                            <text x="0" y={format!("{}", radius + 22.0)} fill="var(--ds-text-muted)" font-size="7.5" font-family="monospace" text-anchor="middle">
                                                {item.detail.clone()}
                                            </text>
                                        </g>
                                    });
                                }

                                nodes_rendered.push(view! {
                                    <g transform={format!("translate({}, {})", compute_x, compute_y)} filter="url(#ds-glow-cyan)">
                                        <circle r="18" fill="rgba(0, 188, 212, 0.22)" stroke="var(--ds-cyan-glow)" stroke-width="1.8"/>
                                        <circle r="7" fill="var(--ds-cyan-glow)" opacity="0.85"/>
                                        <text x="0" y="-26" fill="var(--ds-cyan-glow)" font-size="11" font-weight="700" text-anchor="middle" class="ds-svg-glow-cyan">
                                            "COMPUTE POOL"
                                        </text>
                                        <text x="0" y="34" fill="var(--ds-text-muted)" font-size="8" font-family="monospace" text-anchor="middle">
                                            {format!("{} NODES · {}/{} CORES", worker_count, worker_busy_cores, worker_cores)}
                                        </text>
                                    </g>
                                });

                                for &(cx, cy, ref ctrl) in &ctrl_positions {
                                    let ctrl_top = if ctrl.online { "#fff3e0" } else { "#eceff1" };
                                    let ctrl_left = if ctrl.online { "#ffb74d" } else { "#90a4ae" };
                                    let ctrl_right = if ctrl.online { "#f57c00" } else { "#78909c" };
                                    let ctrl_filter = if ctrl.online { "url(#ds-glow-orange)" } else { "" };
                                    let role_str = if ctrl.online { ctrl.role.to_uppercase() } else { "OFFLINE".to_string() };
                                    nodes_rendered.push(view! {
                                        <g transform={format!("translate({}, {})", cx, cy)} filter={ctrl_filter}>
                                            <g>
                                                <path d="M-12,-6 L0,-12 L12,-6 L0,0 Z" fill={ctrl_top}/>
                                                <path d="M-12,-6 L-12,6 L0,12 L0,0 Z" fill={ctrl_left}/>
                                                <path d="M12,-6 L12,6 L0,12 L0,0 Z" fill={ctrl_right}/>
                                            </g>
                                            <text x="0" y="-18" fill="var(--ds-orange-glow)" font-size="11" font-weight="700" text-anchor="middle" class="ds-svg-glow-orange">
                                                {ctrl.hostname.clone()}
                                            </text>
                                            <text x="0" y="22" fill="var(--ds-text-muted)" font-size="8" font-family="monospace" text-anchor="middle">
                                                {role_str}
                                            </text>
                                        </g>
                                    });
                                }

                                let fs_top = if fs_online_status { "#e8f5e9" } else { "#eceff1" };
                                let fs_left = if fs_online_status { "#4caf50" } else { "#90a4ae" };
                                let fs_right = if fs_online_status { "#388e3c" } else { "#78909c" };
                                let fs_filter = if fs_online_status { "url(#ds-glow-cyan)" } else { "" };
                                nodes_rendered.push(view! {
                                    <g transform={format!("translate({}, {})", fs_x, fs_y)} filter={fs_filter}>
                                        <g>
                                            <path d="M-12,-6 L0,-12 L12,-6 L0,0 Z" fill={fs_top}/>
                                            <path d="M-12,-6 L-12,6 L0,12 L0,0 Z" fill={fs_left}/>
                                            <path d="M12,-6 L12,6 L0,12 L0,0 Z" fill={fs_right}/>
                                        </g>
                                        <text x="0" y="-18" fill="#a5d6a7" font-size="11" font-weight="700" text-anchor="middle">
                                            "FILESERVER"
                                        </text>
                                        <text x="0" y="22" fill="var(--ds-text-muted)" font-size="8" font-family="monospace" text-anchor="middle">
                                            {if fs_online_status { "ONLINE" } else { "OFFLINE" }}
                                        </text>
                                    </g>
                                });

                                view! {
                                    <g>
                                        {connections}
                                        {nodes_rendered}
                                    </g>
                                }
                            }}
                        </svg>
                    </div>
                </div>

                <div class="telemetry-panel grid-sys-right">
                    <div class="telemetry-panel-header">
                        <span>"Compute Resource Telemetry"</span>
                        <i class="ph ph-activity"></i>
                    </div>

                    <div class="ds-flex-grow ds-flex ds-flex-col ds-gap-4" style="font-family: monospace; font-size: 0.75rem; justify-content: space-around;">
                        <div>
                            <div class="ds-flex ds-justify-between ds-text-xs ds-text-muted" style="margin-bottom: 2px;">
                                <span>"CPU LOAD"</span>
                                <span class="ds-text-cyan">{move || format!("{:.1}%", cpu_load_pct())}</span>
                            </div>
                            <div class="ds-relative ds-w-full ds-h-7" style="overflow: visible;">
                                {move || {
                                    let (line, _) = generate_sparkline_path(&cpu_history.get(), 100.0);
                                    view! {
                                        <svg viewBox="0 0 200 40" class="ds-w-full ds-h-full" preserveAspectRatio="none" style="overflow: visible;">
                                            <path d={line} fill="none" stroke="var(--ds-cyan-glow)" stroke-width="1.5" class="ds-svg-glow-cyan"/>
                                        </svg>
                                    }
                                }}
                            </div>
                        </div>

                        <div>
                            <div class="ds-flex ds-justify-between ds-text-xs ds-text-muted" style="margin-bottom: 2px;">
                                <span>"MEMORY USAGE"</span>
                                <span class="ds-text-orange">{move || format!("{:.1}%", mem_load_pct())}</span>
                            </div>
                            <div class="ds-relative ds-w-full ds-h-7" style="overflow: visible;">
                                {move || {
                                    let (line, _) = generate_sparkline_path(&mem_history.get(), 100.0);
                                    view! {
                                        <svg viewBox="0 0 200 40" class="ds-w-full ds-h-full" preserveAspectRatio="none" style="overflow: visible;">
                                            <path d={line} fill="none" stroke="var(--ds-orange-glow)" stroke-width="1.5" class="ds-svg-glow-orange"/>
                                        </svg>
                                    }
                                }}
                            </div>
                        </div>

                        <div>
                            <div class="ds-flex ds-justify-between ds-text-xs ds-text-muted" style="margin-bottom: 2px;">
                                <span>"NETWORK I/O"</span>
                                <span style="color: #00ff88; filter: drop-shadow(0 0 4px rgba(0, 255, 136, 0.8)); font-weight: bold;">
                                    {move || {
                                        let val = total_net_mb_s();
                                        if val > 1.0 {
                                            format!("{:.1} MB/s", val)
                                        } else {
                                            format!("{:.1} KB/s", val * 1024.0)
                                        }
                                    }}
                                </span>
                            </div>
                            <div class="ds-relative ds-w-full ds-h-7" style="overflow: visible;">
                                {move || {
                                    let history_max = net_history.get().iter().cloned().fold(1.0f32, |m, x| m.max(x));
                                    let (line, _) = generate_sparkline_path(&net_history.get(), history_max);
                                    view! {
                                        <svg viewBox="0 0 200 40" class="ds-w-full ds-h-full" preserveAspectRatio="none" style="overflow: visible;">
                                            <path d={line} fill="none" stroke="#00ff88" stroke-width="1.5" style="filter: drop-shadow(0 0 4px rgba(0, 255, 136, 0.8));"/>
                                        </svg>
                                    }
                                }}
                            </div>
                        </div>
                    </div>
                </div>

                <div class="telemetry-panel grid-resources-extended">
                    <div class="telemetry-panel-header">
                        <span>"Hardware Resource Metrics"</span>
                        <i class="ph ph-hard-drives"></i>
                    </div>
                    <div class="ds-flex-grow ds-flex ds-flex-col ds-gap-3" style="font-family: monospace; font-size: 0.75rem;">
                        <div>
                            <div class="ds-flex ds-justify-between ds-text-xs ds-text-muted" style="margin-bottom: 2px;">
                                <span>"CLUSTER STORAGE CAPACITY"</span>
                                <span class="ds-text-orange">
                                    {move || {
                                        let total = nodes.get().iter().map(|n| n.disk_total).sum::<u64>() as f64 / 1024.0 / 1024.0 / 1024.0;
                                        let free = nodes.get().iter().map(|n| n.disk_free).sum::<u64>() as f64 / 1024.0 / 1024.0 / 1024.0;
                                        let used = total - free;
                                        format!("{:.0}/{:.0} GB", used, total)
                                    }}
                                </span>
                            </div>
                            <div class="ds-progress-bar">
                                <div class="ds-progress-fill-orange" style={move || {
                                    let total = nodes.get().iter().map(|n| n.disk_total).sum::<u64>();
                                    let free = nodes.get().iter().map(|n| n.disk_free).sum::<u64>();
                                    let used = total.saturating_sub(free);
                                    let pct = if total > 0 { (used as f64 / total as f64) * 100.0 } else { 0.0 };
                                    format!("width: {}%;", pct as u32)
                                }}></div>
                            </div>
                        </div>

                        <div class="ds-flex ds-justify-between ds-items-center">
                            <div class="ds-flex ds-flex-col">
                                <span class="ds-text-xs ds-text-muted">"LOAD AVERAGE (1m/5m/15m)"</span>
                                <span class="ds-text-sm ds-font-bold ds-text-cyan">
                                    {move || {
                                        let w_list = nodes.get();
                                        if w_list.is_empty() {
                                            "0.00 / 0.00 / 0.00".to_string()
                                        } else {
                                            let l1 = w_list.iter().map(|n| n.load_avg[0]).sum::<f64>() / w_list.len() as f64;
                                            let l5 = w_list.iter().map(|n| n.load_avg[1]).sum::<f64>() / w_list.len() as f64;
                                            let l15 = w_list.iter().map(|n| n.load_avg[2]).sum::<f64>() / w_list.len() as f64;
                                            format!("{:.2} / {:.2} / {:.2}", l1, l5, l15)
                                        }
                                    }}
                                </span>
                            </div>
                            <div class="ds-flex ds-flex-col" style="text-align: right;">
                                <span class="ds-text-xs ds-text-muted">"TOTAL PROCESSES"</span>
                                <span class="ds-text-sm ds-font-bold ds-text-orange">
                                    {move || nodes.get().iter().map(|n| n.process_count).sum::<u32>()}
                                </span>
                            </div>
                        </div>

                        <div style="margin-top: auto; border-top: 1px solid rgba(45, 120, 160, 0.15); padding-top: 6px;">
                            <div class="ds-flex ds-justify-between ds-text-xs ds-text-muted">
                                <span>"CO-PROCESSORS / GRES"</span>
                                <span class="ds-text-cyan">
                                    {move || {
                                        let total_gpus: u64 = nodes.get().iter().map(|n| {
                                            n.gres.get("gpu").cloned().unwrap_or(0)
                                        }).sum();
                                        if total_gpus > 0 {
                                            format!("{} x NVIDIA GPU Active", total_gpus)
                                        } else {
                                            "No accelerators detected".to_string()
                                        }
                                    }}
                                </span>
                            </div>
                        </div>
                    </div>
                </div>

                <div class="telemetry-panel grid-sys-left">
                    <div class="telemetry-panel-header">
                        <span>"Job Queue Operations"</span>
                        <i class="ph ph-squares-four"></i>
                    </div>
                    <div class="ds-flex ds-justify-between ds-items-center" style="margin-bottom: 0.5rem;">
                        <div class="ds-flex ds-flex-col">
                            <span class="ds-text-xs ds-text-muted">"ACTIVE RUNNING JOBS"</span>
                            <span class="ds-text-3xl ds-font-bold ds-text-cyan" style="line-height: 1;">
                                {running_jobs}
                            </span>
                        </div>
                        <div class="ds-flex ds-flex-col" style="text-align: right;">
                            <span class="ds-text-xs ds-text-muted">"TOTAL REGISTERED"</span>
                            <span class="ds-text-xl ds-font-bold ds-text-orange" style="line-height: 1.2;">
                                {total_jobs}
                            </span>
                        </div>
                    </div>

                    <div class="ds-job-list">
                        {move || {
                            let list = running_job_list();
                            if list.is_empty() {
                                view! {
                                    <div class="ds-job-empty">"No running jobs"</div>
                                }.into_view()
                            } else {
                                list.into_iter().map(|job| {
                                    let display_name = job
                                        .job_name
                                        .clone()
                                        .filter(|name| !name.trim().is_empty())
                                        .unwrap_or_else(|| job.binary.clone());
                                    let workers = if job.assigned_workers.is_empty() {
                                        "workers pending".to_string()
                                    } else {
                                        job.assigned_workers.join(", ")
                                    };
                                    let resources = format!("{}n x {}c", job.req_nodes, job.req_cores);

                                    view! {
                                        <div class="ds-job-row" title={format!("Workers: {}", workers)}>
                                            <div class="ds-flex ds-justify-between ds-gap-2">
                                                <span class="ds-job-title">{format!("#{} {}", job.id, display_name)}</span>
                                                <span class="ds-text-orange ds-flex-shrink-0">{resources}</span>
                                            </div>
                                            <div class="ds-flex ds-justify-between ds-gap-2 ds-job-meta">
                                                <span>{job.user_id}</span>
                                                <span>{workers}</span>
                                            </div>
                                        </div>
                                    }
                                }).collect::<Vec<_>>().into_view()
                            }
                        }}
                    </div>

                    <div class="ds-w-full" style="margin-top: 0.35rem; flex-shrink: 0;">
                        <div class="ds-flex ds-justify-between ds-text-xs ds-text-muted" style="margin-bottom: 4px;">
                            <span>"Compute Cores: Free vs Used"</span>
                            <span>{move || format!("{}/{}", used_cores(), total_cores())}</span>
                        </div>
                        <div class="ds-progress-bar">
                            <div class="ds-progress-fill-cyan" style={move || format!("width: {}%;", (cpu_load_pct() as u32).min(100))}></div>
                        </div>
                    </div>
                </div>

                <div class="telemetry-panel grid-streams">
                    <div class="telemetry-panel-header">
                        <span>"Integrated Datacenter Pipeline"</span>
                        <i class="ph ph-globe"></i>
                    </div>

                    <div class="ds-absolute" style="top: 0.85rem; right: 1rem;">
                        <span class="ds-text-lg ds-font-bold ds-text-orange ds-svg-glow-orange">
                            {move || {
                                let total = total_net_mb_s();
                                if total > 1.0 {
                                    format!("{:.1} MB/s", total)
                                } else {
                                    format!("{:.1} KB/s", total * 1024.0)
                                }
                            }}
                        </span>
                    </div>

                    <div class="ds-flex-grow ds-flex ds-items-center ds-justify-center" style="min-height: 90px; margin-top: 10px;">
                        <svg viewBox="0 0 600 100" class="ds-w-full ds-h-full">
                            <path d="M 10 50 Q 150 20 300 50 T 590 50" fill="none" stroke="var(--ds-cyan-glow)" stroke-width="0.7" opacity="0.2"/>
                            <path d="M 10 50 Q 150 80 300 50 T 590 50" fill="none" stroke="var(--ds-orange-glow)" stroke-width="0.7" opacity="0.2"/>

                            {move || {
                                let rate = total_net_mb_s();
                                let animation_dur = if rate > 10.0 {
                                    "0.5s"
                                } else if rate > 1.0 {
                                    "1.2s"
                                } else if rate > 0.1 {
                                    "2.5s"
                                } else {
                                    "6s"
                                };

                                view! {
                                    <g>
                                        <path d="M 10 50 L 590 50" fill="none" stroke="var(--ds-cyan-glow)" stroke-width="1.8"
                                              class="ds-flow-line ds-svg-glow-cyan" style={format!("animation-duration: {};", animation_dur)}/>
                                        <path d="M 10 50 L 590 50" fill="none" stroke="var(--ds-orange-glow)" stroke-width="1.2"
                                              class="ds-flow-line ds-svg-glow-orange" style={format!("animation-duration: {};", animation_dur)}/>
                                    </g>
                                }
                            }}

                            <g transform="translate(260, 20)">
                                <rect x="0" y="0" width="80" height="60" fill="rgba(10, 20, 40, 0.85)" stroke="var(--ds-border-color)" stroke-width="1" rx="4"/>
                                <rect x="5" y="5" width="70" height="50" fill="none" stroke="var(--ds-cyan-glow)" stroke-width="0.8" opacity="0.4" rx="2"/>
                                <text x="40" y="35" fill="var(--ds-cyan-glow)" font-size="10" font-weight="700" text-anchor="middle" class="ds-svg-glow-cyan">
                                    "V-SWITCH"
                                </text>
                            </g>
                        </svg>
                    </div>
                </div>

                <div class="telemetry-panel grid-hpc ds-flex ds-items-center ds-justify-center">
                    <div class="telemetry-panel-header" style="width: 100%; margin-bottom: 0.25rem;">
                        <span>"System Health Monitor"</span>
                        <i class="ph ph-shield-check"></i>
                    </div>

                    <div class="ds-relative ds-w-32 ds-h-32">
                        <svg viewBox="0 0 100 100" class="ds-w-full ds-h-full" style="transform: rotate(-90deg);">
                            <circle cx="50" cy="50" r="40" fill="none" stroke="#1a2634" stroke-width="6"/>

                            {move || {
                                let controller_online_state = controller_online.get();
                                let cyan_val = if controller_online_state { 180.0f32 } else { 0.0f32 };
                                let orange_val = if fileserver_online.get() { 50.0f32 } else { 0.0f32 };

                                view! {
                                    <g>
                                        <circle cx="50" cy="50" r="40" fill="none" stroke="var(--ds-cyan-glow)" stroke-width="6"
                                                stroke-dasharray={format!("{:.1} 251.2", cyan_val)} stroke-dashoffset="0" class="ds-svg-glow-cyan"/>
                                        <circle cx="50" cy="50" r="40" fill="none" stroke="var(--ds-orange-glow)" stroke-width="6"
                                                stroke-dasharray={format!("{:.1} 251.2", orange_val)} stroke-dashoffset="-190" class="ds-svg-glow-orange"/>
                                    </g>
                                }
                            }}

                            <circle cx="50" cy="50" r="32" fill="none" stroke="rgba(45, 120, 160, 0.2)" stroke-width="1" stroke-dasharray="2 4"/>
                        </svg>

                        <div class="ds-absolute" style="inset: 0; display: flex; align-items: center; justify-content: center; flex-direction: column;">
                            <span class="ds-text-lg ds-font-bold ds-text-cyan ds-svg-glow-cyan">"V-HPC"</span>
                            <span class="ds-text-muted" style="font-size: 7.5px; text-transform: uppercase;">
                                {move || if controller_online.get() { "ONLINE" } else { "OFFLINE" }}
                            </span>
                        </div>
                    </div>
                </div>

            </div>
        </div>
    }
}
