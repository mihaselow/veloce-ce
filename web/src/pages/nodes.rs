use crate::api_client::api_get;
use crate::app::{set_interval_with_handle, WebConfig};
use crate::pages::{format_bytes, format_duration, Sparkline};
use leptos::*;
use veloce_common::WorkerInfo;

#[component]
pub(crate) fn NodesList() -> impl IntoView {
    let config =
        use_context::<Resource<bool, Option<WebConfig>>>().expect("WebConfig context missing");
    let (nodes, set_nodes) = create_signal(Vec::<WorkerInfo>::new());
    let (selected_node, set_selected_node) = create_signal(None::<WorkerInfo>);
    let (history, set_history) = create_signal(std::collections::HashMap::<
        String,
        std::collections::VecDeque<f32>,
    >::new());

    let (search_query, set_search_query) = create_signal("".to_string());
    let (status_filter, set_status_filter) = create_signal("All".to_string());
    let (current_page, set_current_page) = create_signal(0usize);
    let page_size = 15;

    let is_alive = std::rc::Rc::new(std::cell::Cell::new(true));
    {
        let is_alive = is_alive.clone();
        on_cleanup(move || {
            is_alive.set(false);
        });
    }

    let fetch_nodes = std::rc::Rc::new({
        let is_alive = is_alive.clone();
        move || {
            let conf = config.get().flatten();
            if conf.is_some() {
                let is_alive = is_alive.clone();
                spawn_local(async move {
                    if let Ok(resp) = api_get("/api/v1/nodes").send().await {
                        if let Ok(data) = resp.json::<Vec<WorkerInfo>>().await {
                            if !is_alive.get() {
                                return;
                            }
                            set_nodes.set(data.clone());
                            set_history.update(|h| {
                                for n in data {
                                    let entry = h.entry(n.id.clone()).or_insert_with(|| {
                                        std::collections::VecDeque::with_capacity(20)
                                    });
                                    entry.push_back(n.cpu_usage);
                                    if entry.len() > 20 {
                                        entry.pop_front();
                                    }
                                }
                            });
                        }
                    }
                });
            }
        }
    });

    let fetch_nodes_clone = fetch_nodes.clone();
    create_effect(move |_| {
        let fetch_nodes = fetch_nodes_clone.clone();
        fetch_nodes();
        let fetch_nodes_for_interval = fetch_nodes.clone();
        let handle = set_interval_with_handle(
            move || {
                fetch_nodes_for_interval();
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
    });

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
                let id_match = node.id.to_lowercase().contains(&q);
                let os_match = node.os_name.to_lowercase().contains(&q);
                if !q.is_empty() && !hostname_match && !ip_match && !id_match && !os_match {
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
        <div style="display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 1rem; margin-bottom: 1rem;">
            <h2>"Cluster Nodes"</h2>
            <div style="display: flex; gap: 1rem; align-items: center; flex-wrap: wrap;">
                <input
                    type="text"
                    placeholder="Search by hostname / IP / OS"
                    class="node-search-input"
                    style="width: 250px;"
                    prop:value=search_query
                    on:input=move |ev| {
                        set_search_query.set(event_target_value(&ev));
                        set_current_page.set(0);
                    }
                />
                <div class="node-filter-group" style="width: auto;">
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
        </div>

        <table>
            <thead>
                <tr>
                    <th>"ID"</th>
                    <th>"Hostname"</th>
                    <th>"Activity"</th>
                    <th>"Load"</th>
                    <th>"CPU usage"</th>
                    <th>"Memory"</th>
                    <th>"Net RX/TX"</th>
                    <th>"Disk R/W"</th>
                    <th>"OS"</th>
                </tr>
            </thead>
            <tbody>
                {move || {
                    let p_nodes = paginated_nodes();
                    if p_nodes.is_empty() {
                        view! {
                            <tr>
                                <td colspan="9" style="text-align: center; padding: 2rem; color: var(--text-muted);">
                                    "No nodes match the selected filters."
                                </td>
                            </tr>
                        }.into_view()
                    } else {
                        p_nodes.into_iter().map(|n| {
                            let n_clone = n.clone();
                            let cpu_alloc_pct = if n.total_cores > 0 {
                                ((n.total_cores - n.available_cores) as f32 / n.total_cores as f32) * 100.0
                            } else { 0.0 };
                            let cpu_real_pct = n.cpu_usage;

                            let total_mem_mb = n.total_memory / 1024 / 1024;
                            let mem_alloc_pct = if total_mem_mb > 0 {
                                (n.allocated_memory as f32 / total_mem_mb as f32) * 100.0
                            } else { 0.0 };

                            let mem_real_pct = if n.total_memory > 0 {
                                (n.used_memory as f32 / n.total_memory as f32) * 100.0
                            } else { 0.0 };

                            let load_avg_str = format!("{:.2}, {:.2}, {:.2}", n.load_avg[0], n.load_avg[1], n.load_avg[2]);

                            view! {
                                <tr
                                    on:click=move |_| set_selected_node.set(Some(n_clone.clone()))
                                    style="cursor: pointer; transition: background-color 0.1s;"
                                    class="node-row"
                                >
                                    <td>{n.id.clone()}</td>
                                    <td>{n.hostname.clone()}</td>
                                    <td>
                                        {move || {
                                            let h = history.get();
                                            let node_history = h.get(&n.id).map(|v| v.iter().cloned().collect::<Vec<_>>()).unwrap_or_default();
                                            view! { <Sparkline data=node_history width=80 height=20 color="var(--primary)".to_string() /> }
                                        }}
                                    </td>
                                    <td>{load_avg_str}</td>
                                    <td>
                                        <div style="display: flex; flex-direction: column; gap: 4px; font-size: 0.8rem;">
                                            <div style="display: flex; justify-content: space-between;">
                                                <span>"Alloc: " {format!("{:.1}%", cpu_alloc_pct)}</span>
                                                <span>"Real: " {format!("{:.1}%", cpu_real_pct)}</span>
                                            </div>
                                            <div class="progress-bar-bg" style="width: 100%; height: 6px; background: rgba(255,255,255,0.1); border-radius: 3px; overflow: hidden; position: relative;">
                                                <div class="progress-bar-fill" style=format!("width: {}%; height: 100%; background: var(--primary); position: absolute; opacity: 0.5;", cpu_alloc_pct)></div>
                                                <div class="progress-bar-fill" style=format!("width: {}%; height: 100%; background: var(--success); position: absolute; top: 0; height: 3px;", cpu_real_pct)></div>
                                            </div>
                                        </div>
                                    </td>
                                    <td>
                                        <div style="display: flex; flex-direction: column; gap: 4px; font-size: 0.8rem;">
                                             <div style="display: flex; justify-content: space-between;">
                                                <span>{format!("{} MB", n.allocated_memory)}</span>
                                                <span>{format!("{} MB", n.used_memory / 1024 / 1024)}</span>
                                             </div>
                                            <div class="progress-bar-bg" style="width: 100%; height: 6px; background: rgba(255,255,255,0.1); border-radius: 3px; overflow: hidden; position: relative;">
                                                <div class="progress-bar-fill" style=format!("width: {}%; height: 100%; background: var(--primary); position: absolute; opacity: 0.5;", mem_alloc_pct)></div>
                                                <div class="progress-bar-fill" style=format!("width: {}%; height: 100%; background: var(--success); position: absolute; top: 0; height: 3px;", mem_real_pct)></div>
                                            </div>
                                        </div>
                                    </td>
                                     <td>
                                        <div style="font-size: 0.8rem; display: flex; flex-direction: column;">
                                            <span style="color: var(--info);"><i class="ph ph-arrow-down-right"></i> {format_bytes(n.net_rx_rate)} "/s"</span>
                                            <span style="color: var(--accent);"><i class="ph ph-arrow-up-right"></i> {format_bytes(n.net_tx_rate)} "/s"</span>
                                        </div>
                                     </td>
                                     <td>
                                        <div style="font-size: 0.8rem; display: flex; flex-direction: column;">
                                            <span>"R: " {format_bytes(n.disk_read_rate)} "/s"</span>
                                            <span>"W: " {format_bytes(n.disk_write_rate)} "/s"</span>
                                        </div>
                                     </td>
                                     <td>{if n.os_name.is_empty() { "Unknown".to_string() } else { n.os_name.clone() }}</td>
                                </tr>
                            }
                        }).collect_view()
                    }
                }}
            </tbody>
        </table>

        <div class="node-pagination" style="margin-top: 1rem;">
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

        {move || selected_node.get().map(|n| view! {
            <div class="modal-overlay" on:click=move |_| set_selected_node.set(None)>
                <div class="modal-content" on:click=move |e| e.stop_propagation()>
                    <button class="modal-close" on:click=move |_| set_selected_node.set(None)>"×"</button>
                    <h3>"Node Details: " {n.hostname}</h3>
                    <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 10px;">
                        <div><strong>"ID:"</strong></div> <div>{n.id}</div>
                        <div><strong>"IP Address:"</strong></div> <div>{n.ip_address}</div>
                        <div><strong>"OS:"</strong></div> <div>{n.os_name} " " {n.os_version}</div>
                        <div><strong>"Kernel:"</strong></div> <div>{n.kernel_version}</div>
                        <div><strong>"CPU Model:"</strong></div> <div>{if n.cpu_model.is_empty() { "Generic".to_string() } else { n.cpu_model }}</div>
                        <div><strong>"Architecture:"</strong></div> <div>{n.arch}</div>
                        <div><strong>"Cores:"</strong></div> <div>{n.total_cores}</div>
                        <div><strong>"Total Memory:"</strong></div> <div>{n.total_memory / 1024 / 1024} " MB"</div>
                        <div><strong>"Allocated Memory:"</strong></div> <div>{n.allocated_memory} " MB"</div>
                        <div><strong>"Load Avg:"</strong></div> <div>{format!("{:.2}, {:.2}, {:.2}", n.load_avg[0], n.load_avg[1], n.load_avg[2])}</div>
                        <div><strong>"Uptime:"</strong></div> <div>{format_duration(n.uptime)}</div>
                        <div><strong>"Disk Usage:"</strong></div>
                        <div>
                            {let used = n.disk_total.saturating_sub(n.disk_free);
                             let pct = if n.disk_total > 0 { (used as f64 / n.disk_total as f64) * 100.0 } else { 0.0 };
                             view! {
                                <div style="width: 100%; background: #222; border-radius: 4px; height: 12px; margin-top: 4px;">
                                    <div style=format!("width: {}%; background: #3b82f6; height: 100%; border-radius: 4px;", pct)></div>
                                </div>
                                <div style="font-size: 10px; margin-top: 2px;">
                                    {used / 1024 / 1024 / 1024} " GB / " {n.disk_total / 1024 / 1024 / 1024} " GB (" {format!("{:.1}", pct)} "%)"
                                </div>
                             }}
                        </div>
                        <div><strong>"Swap:"</strong></div> <div>{(n.swap_total.saturating_sub(n.swap_free)) / 1024 / 1024} " MB / " {n.swap_total / 1024 / 1024} " MB"</div>
                        <div><strong>"Processes:"</strong></div> <div>{n.process_count}</div>
                        <div><strong>"Cgroups:"</strong></div> <div>{if n.cgroup_enabled { "Enabled (v2)" } else { "Disabled" }}</div>
                        <div><strong>"Version:"</strong></div> <div>{n.version}</div>
                    </div>
                </div>
            </div>
        })}
    }
}
