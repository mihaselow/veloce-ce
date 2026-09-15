use crate::api_client::{api_get, api_post};
use crate::app::{set_interval_with_handle, WebConfig};
use crate::pages::{format_bytes, Sparkline};
use leptos::*;
use veloce_common::{ControllerInfo, JobInfo, WorkerInfo};

#[component]
pub fn CoreGrid(nodes: Signal<Vec<WorkerInfo>>) -> impl IntoView {
    let (search_query, set_search_query) = create_signal("".to_string());
    let (status_filter, set_status_filter) = create_signal("All".to_string());
    let (current_page, set_current_page) = create_signal(0usize);
    let page_size = 10;

    let filtered_nodes = move || {
        let q = search_query.get().to_lowercase();
        let filter = status_filter.get();
        let all_nodes = nodes.get();

        all_nodes
            .into_iter()
            .filter(|node| {
                // Search filter
                let hostname_match = node.hostname.to_lowercase().contains(&q);
                let ip_match = node.ip_address.to_lowercase().contains(&q);
                if !q.is_empty() && !hostname_match && !ip_match {
                    return false;
                }

                // Status filter
                match filter.as_str() {
                    "All" => true,
                    "Online" => node.online,
                    "Offline" => !node.online,
                    "Busy" => {
                        if !node.online {
                            false
                        } else {
                            let used_cores = node.total_cores - node.available_cores;
                            used_cores == node.total_cores && node.total_cores > 0
                        }
                    }
                    _ => true,
                }
            })
            .collect::<Vec<_>>()
    };

    let total_filtered = move || filtered_nodes().len();

    let paginated_nodes = move || {
        let list = filtered_nodes();
        let start = current_page.get() * page_size;
        if start >= list.len() {
            return Vec::new();
        }
        let end = (start + page_size).min(list.len());
        list[start..end].to_vec()
    };

    let total_pages = move || {
        let len = total_filtered();
        if len == 0 {
            1
        } else {
            len.div_ceil(page_size)
        }
    };

    let can_go_prev = move || current_page.get() > 0;
    let can_go_next = move || (current_page.get() + 1) < total_pages();

    view! {
        <div class="node-cores-card" style="width: 100%; margin-top: 1.5rem;">
            {move || {
                let n = nodes.get();
                let total: usize = n.iter().map(|x| x.total_cores).sum();
                let used: usize = n.iter().map(|x| x.total_cores - x.available_cores).sum();
                let is_node_representation = total > 1000;

                view! {
                    <h4>
                        <span><i class="ph ph-cpu"></i> " Cluster Compute Grid"</span>
                        <span style="font-size: 0.75rem;">
                            {if is_node_representation {
                                format!("{} / {} Nodes Active", n.iter().filter(|x| x.online).count(), n.len()).into_view()
                            } else {
                                format!("{} / {} Cores Active", used, total).into_view()
                            }}
                        </span>
                    </h4>
                    <div style="display: flex; gap: 3rem; flex-wrap: wrap; margin-top: 1rem; align-items: flex-start;">
                        <div class="core-grid-wrapper" style="display: flex; flex-direction: column; gap: 1rem;">
                            <div class="core-grid">
                                {if is_node_representation {
                                    n.iter().map(|node| {
                                        if !node.online {
                                            "offline"
                                        } else {
                                            let node_used = node.total_cores - node.available_cores;
                                            if node_used == 0 {
                                                "free"
                                            } else if node_used == node.total_cores {
                                                "busy"
                                            } else {
                                                "partial"
                                            }
                                        }
                                    }).map(|state| {
                                        view! { <div class=format!("core-square {}", state)></div> }
                                    }).collect_view()
                                } else {
                                    n.iter().flat_map(|node| {
                                        let node_total = node.total_cores;
                                        let node_used = node_total - node.available_cores;
                                        let node_free = node.available_cores;
                                        let node_online = node.online;

                                        let mut squares = Vec::new();
                                        if !node_online {
                                            squares.extend(std::iter::repeat_n("offline", node_total));
                                        } else {
                                            squares.extend(std::iter::repeat_n("busy", node_used));
                                            squares.extend(std::iter::repeat_n("free", node_free));
                                        }
                                        squares
                                    }).map(|state| {
                                        view! { <div class=format!("core-square {}", state)></div> }
                                    }).collect_view()
                                }}
                            </div>
                            <div class="core-legend" style="display: flex; gap: 1rem; font-size: 0.8rem; margin-top: 0.5rem; flex-wrap: wrap;">
                                <div style="display: flex; align-items: center; gap: 6px;">
                                    <div class="core-square free" style="margin: 0;"></div>
                                    <span style="color: var(--text-muted);">{if is_node_representation { "Idle Node" } else { "Idle Core" }}</span>
                                </div>
                                {if is_node_representation {
                                    view! {
                                        <div style="display: flex; align-items: center; gap: 6px;">
                                            <div class="core-square partial" style="margin: 0;"></div>
                                            <span style="color: var(--text-muted);">"Partially Active"</span>
                                        </div>
                                    }.into_view()
                                } else {
                                    ().into_view()
                                }}
                                <div style="display: flex; align-items: center; gap: 6px;">
                                    <div class="core-square busy" style="margin: 0;"></div>
                                    <span style="color: var(--text-muted);">{if is_node_representation { "Fully Active" } else { "Active Core" }}</span>
                                </div>
                                <div style="display: flex; align-items: center; gap: 6px;">
                                    <div class="core-square offline" style="margin: 0;"></div>
                                    <span style="color: var(--text-muted);">"Offline"</span>
                                </div>
                            </div>
                        </div>

                        <div class="node-list-panel" style="flex: 1; min-width: 300px; display: flex; flex-direction: column; gap: 0.8rem; align-self: stretch;">
                            <div class="node-list-controls">
                                <input
                                    type="text"
                                    placeholder="Search by hostname / IP"
                                    class="node-search-input"
                                    prop:value=search_query
                                    on:input=move |ev| {
                                        set_search_query.set(event_target_value(&ev));
                                        set_current_page.set(0);
                                    }
                                />
                                <div class="node-filter-group">
                                    {vec!["All", "Online", "Offline", "Busy"].into_iter().map(|filter_name| {
                                        let active = move || status_filter.get() == filter_name;
                                        view! {
                                            <button
                                                class=move || format!("node-filter-btn{}", if active() { " active" } else { "" })
                                                on:click=move |_| {
                                                    set_status_filter.set(filter_name.to_string());
                                                    set_current_page.set(0);
                                                }
                                            >
                                                {filter_name}
                                            </button>
                                        }
                                    }).collect_view()}
                                </div>
                            </div>

                            <div style="display: flex; flex-direction: column; gap: 0.8rem; flex: 1;">
                                {move || {
                                    let p_nodes = paginated_nodes();
                                    if p_nodes.is_empty() {
                                        view! {
                                            <div style="text-align: center; padding: 2rem; color: var(--text-muted); font-size: 0.85rem;">
                                                "No worker nodes match the filters."
                                            </div>
                                        }.into_view()
                                    } else {
                                        p_nodes.into_iter().map(|node| {
                                            let node_used = node.total_cores - node.available_cores;
                                            let pct_cores = if node.total_cores > 0 { (node_used as f32 / node.total_cores as f32) * 100.0 } else { 0.0 };
                                            let mem_used_gb = node.allocated_memory as f32 / 1024.0 / 1024.0 / 1024.0;
                                            let mem_total_gb = node.total_memory as f32 / 1024.0 / 1024.0 / 1024.0;
                                            let pct_mem = if node.total_memory > 0 { (node.allocated_memory as f32 / node.total_memory as f32) * 100.0 } else { 0.0 };

                                            view! {
                                                <div class="node-row" style="background: rgba(255, 255, 255, 0.02); border: 1px solid var(--card-border); padding: 0.8rem 1.2rem; border-radius: 8px; display: flex; align-items: center; justify-content: space-between; gap: 1.5rem;">
                                                    <div style="display: flex; flex-direction: column; gap: 2px; min-width: 140px;">
                                                        <span style="font-weight: 600; font-size: 0.9rem; color: var(--text-main); display: flex; align-items: center; gap: 8px;">
                                                            <span class=format!("status-dot {}", if node.online { "online" } else { "offline" }) style="width: 8px; height: 8px; border-radius: 50%; display: inline-block;"></span>
                                                            {node.hostname.clone()}
                                                        </span>
                                                        <span style="font-size: 0.75rem; color: var(--text-muted);">{node.ip_address.clone()}</span>
                                                    </div>

                                                    <div style="flex: 1; display: flex; flex-direction: column; gap: 4px;">
                                                        <div style="display: flex; justify-content: space-between; font-size: 0.75rem; color: var(--text-muted);">
                                                            <span>"Cores: " {node_used} " / " {node.total_cores}</span>
                                                            <span>{format!("{:.0}%", pct_cores)}</span>
                                                        </div>
                                                        <div style="width: 100%; height: 6px; background: rgba(150, 150, 150, 0.1); border-radius: 3px; overflow: hidden;">
                                                            <div style=format!("width: {}%; height: 100%; background: var(--primary); border-radius: 3px;", pct_cores)></div>
                                                        </div>
                                                    </div>

                                                    <div style="flex: 1; display: flex; flex-direction: column; gap: 4px;">
                                                        <div style="display: flex; justify-content: space-between; font-size: 0.75rem; color: var(--text-muted);">
                                                            <span>"Memory: " {format!("{:.1} GB / {:.1} GB", mem_used_gb, mem_total_gb)}</span>
                                                            <span>{format!("{:.0}%", pct_mem)}</span>
                                                        </div>
                                                        <div style="width: 100%; height: 6px; background: rgba(150, 150, 150, 0.1); border-radius: 3px; overflow: hidden;">
                                                            <div style=format!("width: {}%; height: 100%; background: var(--success); border-radius: 3px;", pct_mem)></div>
                                                        </div>
                                                    </div>
                                                </div>
                                            }
                                        }).collect_view()
                                    }
                                }}
                            </div>

                            <div class="node-pagination">
                                <button
                                    class="node-pagination-btn"
                                    disabled=move || !can_go_prev()
                                    on:click=move |_| set_current_page.update(|p| *p -= 1)
                                >
                                    "< Prev"
                                </button>
                                <span>
                                    {move || format!("Page {} of {}", current_page.get() + 1, total_pages())}
                                </span>
                                <button
                                    class="node-pagination-btn"
                                    disabled=move || !can_go_next()
                                    on:click=move |_| set_current_page.update(|p| *p += 1)
                                >
                                    "Next >"
                                </button>
                            </div>
                        </div>
                    </div>
                }
            }}
        </div>
    }
}

#[component]
pub(crate) fn Dashboard(
    jobs: Signal<Vec<JobInfo>>,
    nodes: Signal<Vec<WorkerInfo>>,
    set_nodes: WriteSignal<Vec<WorkerInfo>>,
    controller_online: Signal<bool>,
    fileserver_online: Signal<bool>,
) -> impl IntoView {
    let (cpu_history, set_cpu_history) = create_signal(std::collections::VecDeque::<f32>::new());
    let (mem_history, set_mem_history) = create_signal(std::collections::VecDeque::<f32>::new());
    let (net_history, set_net_history) = create_signal(std::collections::VecDeque::<f32>::new());
    let (controllers, set_controllers) = create_signal(Vec::<ControllerInfo>::new());

    // History update effect
    create_effect(move |_| {
        let n = nodes.get();
        let avg_cpu = if n.is_empty() {
            0.0
        } else {
            n.iter().map(|x| x.load_avg[0] as f32).sum::<f32>() / n.len() as f32
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
            .map(|x| (x.net_rx_rate + x.net_tx_rate) as f32 / 1024.0)
            .sum::<f32>(); // KB/s

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

    // Periodic telemetry polling loop specifically while Dashboard is active
    let set_nodes_clone = set_nodes;
    create_effect(move |_| {
        let conf = config.get().flatten();
        if conf.is_some() {
            let handle = set_interval_with_handle(
                move || {
                    spawn_local(async move {
                        if let Ok(resp) = api_get("/api/v1/nodes").send().await {
                            if let Ok(data) = resp.json::<Vec<WorkerInfo>>().await {
                                set_nodes_clone.set(data);
                            }
                        }
                        if let Ok(resp) = api_get("/api/v1/controllers").send().await {
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

    let trigger_restart = move |component: String, target: Option<String>| {
        let conf = config.get();
        spawn_local(async move {
            if conf.flatten().is_some() {
                let url = match component.as_str() {
                    "controller" => {
                        if let Some(ref t) = target {
                            let encoded = js_sys::encode_uri_component(t)
                                .as_string()
                                .unwrap_or_else(|| t.clone());
                            format!("/api/v1/system/restart?target={}", encoded)
                        } else {
                            "/api/v1/system/restart".to_string()
                        }
                    }
                    "fileserver" => "/api/v1/fileserver/restart".to_string(),
                    _ => return,
                };

                let _ = api_post(&url).send().await;
            }
        });
    };

    let total_jobs = move || jobs.get().len();
    let running_jobs = move || {
        jobs.get()
            .iter()
            .filter(|j| matches!(j.status, veloce_common::JobStatus::Running))
            .count()
    };
    let pending_jobs = move || {
        jobs.get()
            .iter()
            .filter(|j| matches!(j.status, veloce_common::JobStatus::Pending))
            .count()
    };
    let failed_jobs = move || {
        jobs.get()
            .iter()
            .filter(|j| matches!(j.status, veloce_common::JobStatus::Failed(_)))
            .count()
    };

    let workers_count = move || nodes.get().len();
    let total_cores = move || nodes.get().iter().map(|n| n.total_cores).sum::<usize>();
    let used_cores = move || {
        nodes
            .get()
            .iter()
            .map(|n| n.total_cores - n.available_cores)
            .sum::<usize>()
    };
    let alloc_mem = move || nodes.get().iter().map(|n| n.allocated_memory).sum::<u64>();
    let total_mem = move || {
        nodes
            .get()
            .iter()
            .map(|n| n.total_memory / 1024 / 1024)
            .sum::<u64>()
    };
    let used_mem = move || {
        nodes
            .get()
            .iter()
            .map(|n| n.used_memory / 1024 / 1024)
            .sum::<u64>()
    };

    let cpu_alloc_pct = move || {
        let t = total_cores();
        if t > 0 {
            (used_cores() as f32 / t as f32) * 100.0
        } else {
            0.0
        }
    };

    let cpu_real_pct = move || {
        let t = total_cores();
        let total_load: f32 = nodes.get().iter().map(|n| n.load_avg[0] as f32).sum();
        if t > 0 {
            (total_load / t as f32) * 100.0
        } else {
            0.0
        }
    };

    let mem_alloc_pct = move || {
        let t = total_mem();
        if t > 0 {
            (alloc_mem() as f32 / t as f32) * 100.0
        } else {
            0.0
        }
    };
    let mem_real_pct = move || {
        let t = total_mem();
        if t > 0 {
            (used_mem() as f32 / t as f32) * 100.0
        } else {
            0.0
        }
    };

    let avg_load = move || {
        let n = nodes.get();
        if n.is_empty() {
            return vec![0.0, 0.0, 0.0];
        }
        let l1 = n.iter().map(|x| x.load_avg[0]).sum::<f64>() / n.len() as f64;
        let l5 = n.iter().map(|x| x.load_avg[1]).sum::<f64>() / n.len() as f64;
        let l15 = n.iter().map(|x| x.load_avg[2]).sum::<f64>() / n.len() as f64;
        vec![l1, l5, l15]
    };

    let net_throughput = move || {
        let n = nodes.get();
        let rx: u64 = n.iter().map(|x| x.net_rx_rate).sum();
        let tx: u64 = n.iter().map(|x| x.net_tx_rate).sum();
        (rx, tx)
    };

    let disk_throughput = move || {
        let n = nodes.get();
        let r: u64 = n.iter().map(|x| x.disk_read_rate).sum();
        let w: u64 = n.iter().map(|x| x.disk_write_rate).sum();
        (r, w)
    };

    view! {
        <h2>"Dashboard"</h2>

        <h3 style="margin-top: 1rem; color: var(--text-muted);"><i class="ph ph-activity"></i> "Job Telemetry"</h3>
        <div class="stats-bar">
            <div class="stat-item">
                <span class="label">"Total Jobs"</span>
                <span class="value">{total_jobs}</span>
            </div>
            <div class="stat-item">
                <span class="label">"Running"</span>
                <span class="value" style="color: var(--primary-hover);">{running_jobs}</span>
            </div>
            <div class="stat-item">
                <span class="label">"Pending"</span>
                <span class="value" style="color: var(--warning);">{pending_jobs}</span>
            </div>
             <div class="stat-item">
                <span class="label">"Failed"</span>
                <span class="value" style="color: var(--danger);">{failed_jobs}</span>
            </div>
        </div>

        <h3 style="margin-top: 2rem; color: var(--text-muted);"><i class="ph ph-cpu"></i> "Hardware Telemetry"</h3>
        <div class="stats-bar" style="grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));">
            <div class="stat-item" style="gap: 5px;">
                <span class="label" style="display: flex; justify-content: space-between;"><span>"CPU Load"</span> <span style="font-size: 0.7rem;">"1m"</span></span>
                <span class="value" style="height: 25px; display: flex; align-items: flex-end;">
                    {move || {
                        let h = cpu_history.get();
                        let d = h.iter().cloned().collect::<Vec<_>>();
                        view! { <Sparkline data=d width=120 height=25 color="var(--primary)".to_string() /> }
                    }}
                </span>
                <span class="label" style="display: flex; justify-content: space-between; margin-top: 2px;"><span>"Memory Usage"</span> <span style="font-size: 0.7rem;">"%"</span></span>
                <span class="value" style="height: 25px; display: flex; align-items: flex-end;">
                    {move || {
                        let h = mem_history.get();
                        let d = h.iter().cloned().collect::<Vec<_>>();
                        view! { <Sparkline data=d width=120 height=25 color="var(--warning)".to_string() /> }
                    }}
                </span>
                <span class="label" style="display: flex; justify-content: space-between; margin-top: 2px;"><span>"Network I/O"</span> <span style="font-size: 0.7rem;">"KB/s"</span></span>
                <span class="value" style="height: 25px; display: flex; align-items: flex-end;">
                    {move || {
                        let h = net_history.get();
                        let d = h.iter().cloned().collect::<Vec<_>>();
                        view! { <Sparkline data=d width=120 height=25 color="var(--success)".to_string() /> }
                    }}
                </span>
            </div>
            <div class="stat-item">
                <span class="label">"Avg Cluster Load"</span>
                <span class="value">{move || {
                    let l = avg_load();
                    format!("{:.2}, {:.2}, {:.2}", l[0], l[1], l[2])
                }}</span>
            </div>
            <div class="stat-item">
                <span class="label" style="display: flex; justify-content: space-between; width: 100%;">
                    <span>"Compute Cores"</span>
                    <span style="font-size: 0.8rem; color: var(--text-muted);">{used_cores} " / " {total_cores}</span>
                </span>
                <span class="value" style="width: 100%; margin-top: 0.5rem;">
                    <div style="display: flex; justify-content: space-between; font-size: 0.75rem; color: var(--text-muted); margin-bottom: 2px;">
                        <span>"Alloc: " {move || format!("{:.1}%", cpu_alloc_pct())}</span>
                        <span>"Real: " {move || format!("{:.1}%", cpu_real_pct())}</span>
                    </div>
                    <div class="progress-bar-bg" style="width: 100%; height: 8px; background: rgba(255,255,255,0.1); border-radius: 4px; overflow: hidden; position: relative;">
                        <div class="progress-bar-fill" style=move || format!("width: {}%; height: 100%; background: var(--primary); position: absolute; opacity: 0.5;", cpu_alloc_pct())></div>
                        <div class="progress-bar-fill" style=move || format!("width: {}%; height: 100%; background: var(--success); position: absolute; top: 0; height: 8px;", cpu_real_pct())></div>
                    </div>
                </span>
            </div>
            <div class="stat-item">
                <span class="label" style="display: flex; justify-content: space-between; width: 100%;">
                    <span>"Memory Allocation"</span>
                    <span style="font-size: 0.8rem; color: var(--text-muted);">{used_mem} " / " {total_mem} " MB"</span>
                </span>
                <span class="value" style="width: 100%; margin-top: 0.5rem;">
                    <div style="display: flex; justify-content: space-between; font-size: 0.75rem; color: var(--text-muted); margin-bottom: 2px;">
                        <span>"Alloc: " {move || format!("{:.1}%", mem_alloc_pct())}</span>
                        <span>"Real: " {move || format!("{:.1}%", mem_real_pct())}</span>
                    </div>
                    <div class="progress-bar-bg" style="width: 100%; height: 8px; background: rgba(255,255,255,0.1); border-radius: 4px; overflow: hidden; position: relative;">
                        <div class="progress-bar-fill" style=move || format!("width: {}%; height: 100%; background: var(--primary); position: absolute; opacity: 0.5;", mem_alloc_pct())></div>
                        <div class="progress-bar-fill" style=move || format!("width: {}%; height: 100%; background: var(--success); position: absolute; top: 0; height: 8px;", mem_real_pct())></div>
                    </div>
                </span>
            </div>
            <div class="stat-item">
                <span class="label">"Network Throughput"</span>
                <span class="value" style="font-size: 1.5rem !important; display: flex; flex-direction: column; gap: 4px;">
                    <div style="display: flex; align-items: center; gap: 8px; color: var(--info);">
                        <i class="ph ph-arrow-down-right"></i>
                        {move || format_bytes(net_throughput().0)} "/s"
                    </div>
                    <div style="display: flex; align-items: center; gap: 8px; color: var(--accent);">
                        <i class="ph ph-arrow-up-right"></i>
                        {move || format_bytes(net_throughput().1)} "/s"
                    </div>
                </span>
            </div>
            <div class="stat-item">
                <span class="label">"Disk IO"</span>
                <span class="value" style="font-size: 1.5rem !important; display: flex; flex-direction: column; gap: 4px;">
                    <div style="display: flex; align-items: center; gap: 8px;">
                        <i class="ph ph-hard-drive"></i>
                        <span>"R: " {move || format_bytes(disk_throughput().0)} "/s"</span>
                    </div>
                    <div style="display: flex; align-items: center; gap: 8px;">
                        <i class="ph ph-pencil-simple-line"></i>
                        <span>"W: " {move || format_bytes(disk_throughput().1)} "/s"</span>
                    </div>
                </span>
            </div>
        </div>

        <CoreGrid nodes=nodes />

        <h3 style="margin-top: 2rem; color: var(--text-muted);"><i class="ph ph-heartbeat"></i> "System Status"</h3>
        <div class="stats-bar" style="grid-template-columns: repeat(auto-fit, minmax(130px, 1fr));">
            <div class="stat-item">
                <span class="label">"Active Nodes"</span>
                <span class="value">{workers_count}</span>
            </div>
            {move || {
                let list = controllers.get();
                if list.is_empty() {
                    view! {
                        <div class="stat-item">
                            <span class="label" style="display: flex; justify-content: space-between; align-items: center; width: 100%;">
                                <span>"Controller"</span>
                                <button
                                    class="icon-button"
                                    title="Restart Controller"
                                    on:click=move |_| trigger_restart("controller".to_string(), None)
                                    style="padding: 2px; font-size: 0.8rem; background: none; border: none; cursor: pointer; color: var(--text-muted);"
                                >
                                    <i class="ph ph-arrow-counter-clockwise"></i>
                                </button>
                            </span>
                            <span class="value" style=move || if controller_online.get() { "color: var(--success);" } else { "color: var(--danger);" }>
                                {move || if controller_online.get() { "Online" } else { "Offline" }}
                            </span>
                        </div>
                    }.into_view()
                } else {
                    list.into_iter().map(|info| {
                        let hostname = info.hostname.clone();
                        let role = info.role.clone();
                        let online = info.online;
                        let h = hostname.clone();
                        view! {
                            <div class="stat-item">
                                <span class="label" style="display: flex; justify-content: space-between; align-items: center; width: 100%;">
                                    <span>{format!("Controller ({})", role)}</span>
                                    <button
                                        class="icon-button"
                                        title=format!("Restart {}", h)
                                        on:click=move |_| trigger_restart("controller".to_string(), Some(h.clone()))
                                        style="padding: 2px; font-size: 0.8rem; background: none; border: none; cursor: pointer; color: var(--text-muted);"
                                    >
                                        <i class="ph ph-arrow-counter-clockwise"></i>
                                    </button>
                                </span>
                                <span class="value" style=move || if online { "color: var(--success);" } else { "color: var(--danger);" }>
                                    {if online { "Online" } else { "Offline" }}
                                </span>
                            </div>
                        }.into_view()
                    }).collect_view()
                }
            }}
            <div class="stat-item">
                <span class="label" style="display: flex; justify-content: space-between; align-items: center; width: 100%;">
                    <span>"Fileserver"</span>
                    <button
                        class="icon-button"
                        title="Restart Fileserver"
                        on:click=move |_| trigger_restart("fileserver".to_string(), None)
                        style="padding: 2px; font-size: 0.8rem; background: none; border: none; cursor: pointer; color: var(--text-muted);"
                    >
                        <i class="ph ph-arrow-counter-clockwise"></i>
                    </button>
                </span>
                <span class="value" style=move || if fileserver_online.get() { "color: var(--success);" } else { "color: var(--danger);" }>
                    {move || if fileserver_online.get() { "Online" } else { "Offline" }}
                </span>
            </div>
            <div class="stat-item">
                <span class="label">"Web UI"</span>
                <span class="value" style="color: var(--success);">"Online"</span>
            </div>
        </div>
    }
}
