use crate::{api_get, set_interval_with_handle, WebConfig};
use leptos::*;
use veloce_common::{ControllerInfo, WorkerInfo};

#[component]
pub fn AdminPage() -> impl IntoView {
    let config =
        use_context::<Resource<bool, Option<WebConfig>>>().expect("WebConfig context missing");

    let (nodes, set_nodes) = create_signal(Vec::<WorkerInfo>::new());
    let (node_search_query, set_node_search_query) = create_signal("".to_string());
    let (node_status_filter, set_node_status_filter) = create_signal("All".to_string());
    let (node_current_page, set_node_current_page) = create_signal(0usize);
    let node_page_size = 3;

    let filtered_telemetry_nodes = move || {
        let q = node_search_query.get().to_lowercase();
        let filter = node_status_filter.get();
        let all_nodes = nodes.get();

        all_nodes
            .into_iter()
            .filter(|node| {
                // Search filter
                let hostname_match = node.hostname.to_lowercase().contains(&q);
                let id_match = node.id.to_lowercase().contains(&q);
                if !q.is_empty() && !hostname_match && !id_match {
                    return false;
                }

                // Status filter
                match filter.as_str() {
                    "All" => true,
                    "Online" => node.online,
                    "Offline" => !node.online,
                    _ => true,
                }
            })
            .collect::<Vec<_>>()
    };

    let total_telemetry_filtered = move || filtered_telemetry_nodes().len();

    let paginated_telemetry_nodes = move || {
        let list = filtered_telemetry_nodes();
        let start = node_current_page.get() * node_page_size;
        if start >= list.len() {
            return Vec::new();
        }
        let end = (start + node_page_size).min(list.len());
        list[start..end].to_vec()
    };

    let total_telemetry_pages = move || {
        let len = total_telemetry_filtered();
        if len == 0 {
            1
        } else {
            len.div_ceil(node_page_size)
        }
    };

    let can_telemetry_go_prev = move || node_current_page.get() > 0;
    let can_telemetry_go_next = move || (node_current_page.get() + 1) < total_telemetry_pages();

    let storage = window().local_storage().ok().flatten();

    let (selected_component, set_selected_component) = create_signal(
        storage
            .as_ref()
            .and_then(|s| s.get_item("veloce_admin_component").ok().flatten())
            .unwrap_or_else(|| "veloce-controller-ha-1".to_string()),
    );
    let (controllers, set_controllers) = create_signal(Vec::<ControllerInfo>::new());
    let (log_lines, set_log_lines) = create_signal(
        storage
            .as_ref()
            .and_then(|s| s.get_item("veloce_admin_lines").ok().flatten())
            .and_then(|v| v.parse().ok())
            .unwrap_or(100usize),
    );
    let (logs, set_logs) = create_signal("No logs fetched yet.".to_string());
    let (is_loading_logs, _) = create_signal(false);

    let (remediation_target, set_remediation_target) = create_signal(
        storage
            .as_ref()
            .and_then(|s| s.get_item("veloce_admin_rem_target").ok().flatten())
            .unwrap_or_else(|| "veloce-controller-ha-1".to_string()),
    );
    let (_remediation_reason, set_remediation_reason) = create_signal("".to_string());
    let (_force_critical, set_force_critical) = create_signal(false);
    let (remediation_status, set_remediation_status) = create_signal("".to_string());

    let (log_type, set_log_type) = create_signal(
        storage
            .as_ref()
            .and_then(|s| s.get_item("veloce_admin_log_type").ok().flatten())
            .unwrap_or_else(|| "app".to_string()),
    );
    let (log_source, set_log_source) = create_signal(
        storage
            .as_ref()
            .and_then(|s| s.get_item("veloce_admin_log_source").ok().flatten())
            .unwrap_or_else(|| "dmesg".to_string()),
    );

    // Persistence effects
    create_effect(move |_| {
        if let Some(s) = window().local_storage().ok().flatten() {
            let _ = s.set_item("veloce_admin_component", &selected_component.get());
        }
    });
    create_effect(move |_| {
        if let Some(s) = window().local_storage().ok().flatten() {
            let _ = s.set_item("veloce_admin_lines", &log_lines.get().to_string());
        }
    });
    create_effect(move |_| {
        if let Some(s) = window().local_storage().ok().flatten() {
            let _ = s.set_item("veloce_admin_rem_target", &remediation_target.get());
        }
    });
    create_effect(move |_| {
        if let Some(s) = window().local_storage().ok().flatten() {
            let _ = s.set_item("veloce_admin_log_type", &log_type.get());
        }
    });
    create_effect(move |_| {
        if let Some(s) = window().local_storage().ok().flatten() {
            let _ = s.set_item("veloce_admin_log_source", &log_source.get());
        }
    });

    let fetch_logs = move |_| {
        set_logs.set(
            "Use the veloce CLI for component logs (for example `veloce doctor` and host journalctl).".to_string(),
        );
    };

    // Polling for cluster nodes state
    let fetch_nodes = move || {
        let conf = config.get().flatten();
        if conf.is_some() {
            spawn_local(async move {
                if let Ok(resp) = api_get("/api/v1/nodes").send().await {
                    if let Ok(data) = resp.json::<Vec<WorkerInfo>>().await {
                        set_nodes.set(data);
                    }
                }
            });
        }
    };

    // Polling for controllers list
    let fetch_controllers = move || {
        let conf = config.get().flatten();
        if conf.is_some() {
            spawn_local(async move {
                if let Ok(resp) = api_get("/api/v1/controllers").send().await {
                    if let Ok(data) = resp.json::<Vec<ControllerInfo>>().await {
                        set_controllers.set(data);
                    }
                }
            });
        }
    };

    create_effect(move |_| {
        fetch_nodes();
        fetch_controllers();
        let handle = set_interval_with_handle(
            move || {
                fetch_nodes();
                fetch_controllers();
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

    let trigger_remediation = move |_| {
        set_remediation_status
            .set("Autonomous node remediation is not available in this build.".to_string());
    };

    view! {
        <div class="admin-page-container">
            <style>
                {r#"
                .admin-split-layout {
                    display: grid;
                    grid-template-columns: 1fr 1fr;
                    gap: 1.5rem;
                    height: calc(100vh - 180px);
                    min-height: 700px;
                }
                .log-panel-enterprise {
                    display: flex;
                    flex-direction: column;
                    height: 100%;
                }
                .logs-scroll-area {
                    height: 620px;
                    background: #0d0d0e;
                    border: 1px solid rgba(255, 255, 255, 0.1);
                    border-radius: 8px;
                    padding: 1.25rem;
                    font-family: 'SF Mono', 'JetBrains Mono', monospace;
                    font-size: 0.8rem;
                    line-height: 1.4;
                    color: #d1d1d1;
                    overflow-y: scroll;
                    overflow-x: hidden;
                    white-space: pre-wrap;
                    word-break: break-all;
                    box-shadow: inset 0 2px 10px rgba(0,0,0,0.5);
                }
                .right-panel-stack {
                    display: flex;
                    flex-direction: column;
                    gap: 1.5rem;
                    height: 100%;
                    overflow-y: auto;
                }
                .telemetry-node-card {
                    background: rgba(255, 255, 255, 0.03);
                    border: 1px solid rgba(255, 255, 255, 0.08);
                    border-radius: 10px;
                    padding: 1rem;
                    margin-bottom: 0.75rem;
                }
                .metric-bar-bg {
                    height: 6px;
                    background: rgba(255, 255, 255, 0.1);
                    border-radius: 3px;
                    margin: 4px 0 10px 0;
                    overflow: hidden;
                }
                .metric-bar-fill {
                    height: 100%;
                    background: var(--primary);
                    transition: width 0.3s ease;
                }
                .metric-row {
                    display: flex;
                    justify-content: space-between;
                    font-size: 0.75rem;
                    color: var(--text-muted);
                    margin-bottom: 2px;
                }
                .metric-value {
                    color: var(--text-main);
                    font-weight: 600;
                }
                "#}
            </style>

            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 2rem;">
                <div>
                    <h2 style="margin: 0; font-weight: 700; font-size: 1.75rem;">"Admin Portal"</h2>
                    <p class="subtitle" style="margin: 0.25rem 0 0 0; opacity: 0.7;">"Node telemetry and diagnostics"</p>
                </div>
                <div class="badge running" style="padding: 0.5rem 1rem;">
                    <i class="ph ph-shield-checkered"></i> "CLUSTER ADMIN"
                </div>
            </div>

            <div class="admin-split-layout">
                // Left Column: Logs (50%)
                <div class="log-panel-enterprise glass-panel">
                    <div class="submit-section" style="padding: 0; margin-bottom: 1.25rem;">
                        <h3 style="margin-bottom: 1rem;"><i class="ph ph-terminal-window"></i> "Component Log Streaming"</h3>
                        <div style="display: flex; gap: 10px;">
                            <div style="flex: 1;">
                                <label style="font-size: 0.7rem; color: var(--text-muted); text-transform: uppercase;">"Log Type"</label>
                                <select prop:value=log_type on:change=move |ev| set_log_type.set(event_target_value(&ev))>
                                    <option value="app">"Application Logs"</option>
                                    <option value="system">"System Logs (Pull)"</option>
                                </select>
                            </div>
                            {move || if log_type.get() == "system" {
                                view! {
                                    <div style="flex: 1;">
                                        <label style="font-size: 0.7rem; color: var(--text-muted); text-transform: uppercase;">"Source"</label>
                                        <select prop:value=log_source on:change=move |ev| set_log_source.set(event_target_value(&ev))>
                                            <option value="dmesg">"dmesg"</option>
                                            <option value="syslog">"syslog"</option>
                                            <option value="journal">"journalctl"</option>
                                        </select>
                                    </div>
                                }.into_view()
                            } else {
                                view! { <></> };
                                ().into_view()
                            }}
                        </div>

                        <div style="display: flex; gap: 10px; margin-top: 10px;">
                            <div style="flex: 2;">
                                <label style="font-size: 0.7rem; color: var(--text-muted); text-transform: uppercase;">"Component"</label>
                                <select prop:value=selected_component on:change=move |ev| set_selected_component.set(event_target_value(&ev))>
                                    <optgroup label="Infrastructure">
                                        {move || {
                                            let current = selected_component.get();
                                            let list = controllers.get();
                                            if list.is_empty() {
                                                view! {
                                                    <option value="veloce-controller-ha-1" selected=move || current == "veloce-controller-ha-1">"veloce-controller-ha-1"</option>
                                                }.into_view()
                                            } else {
                                                list.into_iter().map(|info| {
                                                    let val = info.hostname.clone();
                                                    let display = format!("{} ({})", info.hostname, info.role);
                                                    view! { <option value=val.clone() selected=move || selected_component.get() == val>{display}</option> }
                                                }).collect_view()
                                            }
                                        }}
                                        <option value="fileserver" selected=move || selected_component.get() == "fileserver">"Fileserver"</option>
                                    </optgroup>
                                    <optgroup label="Compute Nodes">
                                        {move || nodes.get().into_iter().map(|n| {
                                            let id = n.id.clone();
                                            view! { <option value=id.clone() selected=move || selected_component.get() == id>{format!("Worker: {}", n.hostname)}</option> }
                                        }).collect_view()}
                                    </optgroup>
                                </select>
                            </div>
                            <div style="flex: 1;">
                                <label style="font-size: 0.7rem; color: var(--text-muted); text-transform: uppercase;">"Lines"</label>
                                <input type="number" prop:value=log_lines on:input=move |ev| set_log_lines.set(event_target_value(&ev).parse().unwrap_or(100)) />
                            </div>
                            <div style="display: flex; align-items: flex-end;">
                                <button on:click=fetch_logs disabled=is_loading_logs style="height: 42px; min-width: 130px; justify-content: center;">
                                    {move || if is_loading_logs.get() {
                                        view! { <><i class="ph ph-circle-notch animate-spin"></i> "Streaming"</> }.into_view()
                                    } else {
                                        view! { <><i class="ph ph-cloud-arrow-down"></i> "Fetch Logs"</> }.into_view()
                                    }}
                                </button>
                            </div>
                        </div>
                    </div>

                    <div class="logs-scroll-area">
                        {move || logs.get()}
                    </div>
                </div>

                // Right Column: Remediation & Telemetry (50%)
                <div class="right-panel-stack">
                    // Remediation Panel
                    <div class="glass-panel" style="margin-bottom: 0;">
                        <div class="submit-section" style="padding: 0;">
                            <h3 style="margin-bottom: 1rem;"><i class="ph ph-wrench"></i> "System Remediation"</h3>

                            <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 1rem;">
                                <div class="form-group">
                                    <label>"Target Node / Component"</label>
                                    <select prop:value=remediation_target on:change=move |ev| set_remediation_target.set(event_target_value(&ev))>
                                        <optgroup label="Infrastructure">
                                            {move || {
                                                let list = controllers.get();
                                                if list.is_empty() {
                                                    view! {
                                                        <option value="veloce-controller-ha-1">"veloce-controller-ha-1"</option>
                                                    }.into_view()
                                                } else {
                                                    list.into_iter().map(|info| {
                                                        let val = info.hostname.clone();
                                                        let display = format!("{} ({})", info.hostname, info.role);
                                                        view! { <option value=val>{display}</option> }
                                                    }).collect_view()
                                                }
                                            }}
                                            <option value="fileserver">"Fileserver"</option>
                                        </optgroup>
                                        <optgroup label="Compute Nodes">
                                            {move || nodes.get().into_iter().map(|n| {
                                                view! { <option value=n.id.clone()>{format!("Worker: {}", n.hostname)}</option> }
                                            }).collect_view()}
                                        </optgroup>
                                    </select>
                                </div>
                                <div class="form-group" style="display: flex; align-items: flex-end;">
                                     <button class="danger" style="width: 100%; justify-content: center; height: 42px;" on:click=trigger_remediation>
                                        <i class="ph ph-warning"></i> "Execute Task"
                                    </button>
                                </div>
                            </div>

                            <div class="form-group">
                                <label>"Remediation Reason / Ticket ID"</label>
                                <textarea
                                    placeholder="Describe the failure or provide a tracking ID..."
                                    style="height: 60px; resize: none;"
                                    on:input=move |ev| set_remediation_reason.set(event_target_value(&ev))
                                ></textarea>
                            </div>

                            <div style="display: flex; justify-content: space-between; align-items: center;">
                                <div style="display: flex; align-items: center; gap: 8px;">
                                    <input
                                        type="checkbox"
                                        id="force-crit"
                                        style="width: 16px; height: 16px;"
                                        on:change=move |ev| set_force_critical.set(event_target_checked(&ev))
                                    />
                                    <label for="force-crit" style="margin: 0; font-size: 0.75rem; color: var(--danger); font-weight: 600;">"Critical Infrastructure Restart"</label>
                                </div>
                                {move || {
                                    let status = remediation_status.get();
                                    if !status.is_empty() {
                                        let is_error = status.to_lowercase().contains("error");
                                        view! {
                                            <div class=if is_error { "badge failed" } else { "badge running" }
                                                 style="padding: 0.4rem 0.8rem; font-size: 0.7rem;">
                                                {status}
                                            </div>
                                        }.into_view()
                                    } else {
                                        view! { <></> };
                                        ().into_view()
                                    }
                                }}
                            </div>
                        </div>
                    </div>

                    // Enhanced Telemetry Panel
                    <div class="glass-panel" style="flex: 1;">
                        <div class="submit-section" style="padding: 0;">
                            <h3 style="margin-bottom: 1.25rem;"><i class="ph ph-gauge"></i> "Node Performance Telemetry"</h3>

                            <div style="display: flex; gap: 8px; margin-bottom: 1rem; flex-wrap: wrap;">
                                <input
                                    type="text"
                                    placeholder="Search by hostname / ID"
                                    class="node-search-input"
                                    style="flex: 1; min-width: 150px; font-size: 0.8rem; padding: 0.4rem 0.6rem;"
                                    prop:value=node_search_query
                                    on:input=move |ev| {
                                        set_node_search_query.set(event_target_value(&ev));
                                        set_node_current_page.set(0);
                                    }
                                />
                                <div class="node-filter-group" style="width: auto;">
                                    {vec!["All", "Online", "Offline"].into_iter().map(|filter_name| {
                                        let active = move || node_status_filter.get() == filter_name;
                                        view! {
                                            <button
                                                class=move || format!("node-filter-btn{}", if active() { " active" } else { "" })
                                                style="padding: 0.35rem 0.55rem; font-size: 0.7rem;"
                                                on:click=move |_| {
                                                    set_node_status_filter.set(filter_name.to_string());
                                                    set_node_current_page.set(0);
                                                }
                                            >
                                                {filter_name}
                                            </button>
                                        }
                                    }).collect_view()}
                                </div>
                            </div>

                            <div style="max-height: 400px; overflow-y: auto; padding-right: 5px; display: flex; flex-direction: column; gap: 0.75rem; margin-bottom: 1rem;">
                                {move || {
                                    let p_nodes = paginated_telemetry_nodes();
                                    if p_nodes.is_empty() {
                                        view! {
                                            <div style="text-align: center; padding: 2rem; color: var(--text-muted); font-size: 0.85rem;">
                                                "No worker nodes match the filters."
                                            </div>
                                        }.into_view()
                                    } else {
                                        p_nodes.into_iter().map(|n| {
                                            let cpu_pct = n.cpu_usage;
                                            let mem_pct = (n.used_memory as f64 / n.total_memory as f64 * 100.0) as f32;
                                            let rx_kb = n.net_rx_rate / 1024;
                                            let tx_kb = n.net_tx_rate / 1024;

                                            view! {
                                                <div class="telemetry-node-card" style="margin-bottom: 0;">
                                                    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px;">
                                                        <div style="display: flex; align-items: center; gap: 8px;">
                                                            <span style="font-weight: 700; font-size: 0.9rem;">{n.hostname}</span>
                                                            <span class="badge running" style="font-size: 0.6rem; padding: 1px 6px;">{n.arch.clone()}</span>
                                                        </div>
                                                        <span class=if n.online { "badge completed" } else { "badge failed" } style="font-size: 0.65rem;">
                                                            {if n.online { "ONLINE" } else { "OFFLINE" }}
                                                        </span>
                                                    </div>

                                                    <div class="metric-row">
                                                        <span>"CPU Usage"</span>
                                                        <span class="metric-value">{format!("{:.1}%", cpu_pct)}</span>
                                                    </div>
                                                    <div class="metric-bar-bg">
                                                        <div class="metric-bar-fill" style=format!("width: {}%; background: {}", cpu_pct, if cpu_pct > 80.0 { "var(--danger)" } else { "var(--primary)" })></div>
                                                    </div>

                                                    <div class="metric-row">
                                                        <span>"Memory"</span>
                                                        <span class="metric-value">{format!("{:.1} / {:.1} GB ({:.0}%)", n.used_memory as f64 / 1e9, n.total_memory as f64 / 1e9, mem_pct)}</span>
                                                    </div>
                                                    <div class="metric-bar-bg">
                                                        <div class="metric-bar-fill" style=format!("width: {}%; background: {}", mem_pct, if mem_pct > 90.0 { "var(--danger)" } else { "var(--primary)" })></div>
                                                    </div>

                                                    <div class="metric-row" style="margin-top: 8px;">
                                                        <span>"GPU Status"</span>
                                                        <span class="metric-value" style=format!("color: {}", if n.gres.contains_key("gpu") { "var(--success)" } else { "var(--text-muted)" })>
                                                            {if n.gres.contains_key("gpu") { "Accelerated" } else { "No GPU Hardware" }}
                                                        </span>
                                                    </div>

                                                    <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 10px; margin-top: 5px;">
                                                        <div style="background: rgba(0,0,0,0.1); padding: 8px; border-radius: 6px;">
                                                            <div style="font-size: 0.65rem; color: var(--text-muted);">"NET RX"</div>
                                                            <div style="font-size: 0.85rem; font-weight: 700; color: var(--success);">{format!("{:.1} MB/s", rx_kb as f64 / 1024.0)}</div>
                                                        </div>
                                                        <div style="background: rgba(0,0,0,0.1); padding: 8px; border-radius: 6px;">
                                                            <div style="font-size: 0.65rem; color: var(--text-muted);">"NET TX"</div>
                                                            <div style="font-size: 0.85rem; font-weight: 700; color: var(--primary);">{format!("{:.1} MB/s", tx_kb as f64 / 1024.0)}</div>
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
                                    disabled=move || !can_telemetry_go_prev()
                                    on:click=move |_| set_node_current_page.update(|p| *p -= 1)
                                >
                                    "< Prev"
                                </button>
                                <span>
                                    {move || format!("Page {} of {}", node_current_page.get() + 1, total_telemetry_pages())}
                                </span>
                                <button
                                    class="node-pagination-btn"
                                    disabled=move || !can_telemetry_go_next()
                                    on:click=move |_| set_node_current_page.update(|p| *p += 1)
                                >
                                    "Next >"
                                </button>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    }
}
