use crate::api_client::{api_get, fetch_ws_ticket};
use crate::api_error::{clear_api_error, describe_http_failure, describe_request_failure};
use crate::app::WebConfig;
use crate::notifications::notify_job_status_changes;
use futures::StreamExt;
use gloo_net::websocket::futures::WebSocket;
use leptos::*;
use leptos_router::A;
use veloce_common::JobInfo;

#[component]
pub(crate) fn JobsList() -> impl IntoView {
    let (jobs, set_jobs) = create_signal(Vec::<JobInfo>::new());
    let (_controller_online, set_controller_online) = create_signal(false);

    let (filter_user, set_filter_user) = create_signal("".to_string());
    let (filter_status, set_filter_status) = create_signal("All".to_string());
    let (page, set_page) = create_signal(0usize);
    let (sort_col, set_sort_col) = create_signal("id".to_string());
    let (sort_desc, set_sort_desc) = create_signal(true);
    let page_size = 10;

    let config =
        use_context::<Resource<bool, Option<WebConfig>>>().expect("WebConfig context missing");
    let set_api_error =
        use_context::<WriteSignal<Option<String>>>().expect("API error context missing");

    let is_alive = std::rc::Rc::new(std::cell::Cell::new(true));
    {
        let is_alive = is_alive.clone();
        on_cleanup(move || {
            is_alive.set(false);
        });
    }

    // WebSocket updates for JobsList
    let is_alive_clone = is_alive.clone();
    create_effect(move |_| {
        let conf = config.get();
        let is_alive = is_alive_clone.clone();
        if conf.flatten().is_some() {
            spawn_local(async move {
                // Initial fetch
                if !is_alive.get() {
                    return;
                }
                let init_jobs = api_get("/api/v1/jobs").send();

                match init_jobs.await {
                    Ok(resp) if resp.ok() => {
                        if !is_alive.get() {
                            return;
                        }
                        set_controller_online.set(true);
                        clear_api_error(set_api_error);
                        if let Ok(data) = resp.json::<Vec<JobInfo>>().await {
                            let previous = jobs.get();
                            notify_job_status_changes(&previous, &data);
                            set_jobs.set(data);
                        }
                    }
                    Ok(resp) => {
                        if !is_alive.get() {
                            return;
                        }
                        set_controller_online.set(false);
                        set_api_error.set(Some(describe_http_failure(
                            "Failed to refresh jobs list",
                            &resp,
                        )));
                    }
                    Err(err) => {
                        if !is_alive.get() {
                            return;
                        }
                        set_controller_online.set(false);
                        set_api_error.set(Some(describe_request_failure(
                            "Failed to refresh jobs list",
                            &err,
                        )));
                    }
                }

                // WebSocket listener loop
                loop {
                    if !is_alive.get() {
                        break;
                    }
                    let loc = match web_sys::window().map(|w| w.location()) {
                        Some(l) => l,
                        None => break,
                    };
                    let protocol = match loc.protocol() {
                        Ok(ref p) if p == "https:" => "wss:",
                        _ => "ws:",
                    };
                    let host = match loc.host() {
                        Ok(h) => h,
                        Err(_) => break,
                    };
                    let ticket = match fetch_ws_ticket("events", None).await {
                        Ok(t) => t,
                        Err(status) => {
                            if !is_alive.get() {
                                break;
                            }
                            set_controller_online.set(false);
                            if status == 403 {
                                set_api_error.set(Some(
                                    "Live job updates unavailable (insufficient role for WebSocket ticket)"
                                        .to_string(),
                                ));
                            } else if status == 401 {
                                set_api_error.set(Some(
                                    "Live job updates unavailable (session expired; sign in again)"
                                        .to_string(),
                                ));
                            }
                            gloo_timers::future::TimeoutFuture::new(5000).await;
                            continue;
                        }
                    };
                    let ws_url =
                        format!("{}//{}/api/v1/ws/events?ticket={}", protocol, host, ticket);

                    let ws = match WebSocket::open(&ws_url) {
                        Ok(w) => w,
                        Err(_) => {
                            if !is_alive.get() {
                                break;
                            }
                            set_controller_online.set(false);
                            gloo_timers::future::TimeoutFuture::new(5000).await;
                            continue;
                        }
                    };

                    let (_, mut rx) = ws.split();
                    if !is_alive.get() {
                        break;
                    }
                    set_controller_online.set(true);

                    while let Some(msg) = rx.next().await {
                        if !is_alive.get() {
                            break;
                        }
                        match msg {
                            Ok(gloo_net::websocket::Message::Text(text))
                                if text == "jobs_updated" =>
                            {
                                if let Ok(resp) = api_get("/api/v1/jobs").send().await {
                                    if let Ok(data) = resp.json::<Vec<JobInfo>>().await {
                                        if !is_alive.get() {
                                            break;
                                        }
                                        set_jobs.set(data);
                                    }
                                }
                            }
                            Err(_) => break,
                            _ => {}
                        }
                    }

                    if !is_alive.get() {
                        break;
                    }
                    set_controller_online.set(false);
                    gloo_timers::future::TimeoutFuture::new(2000).await;
                }
            });
        }
    });

    let filtered_jobs_all = move || {
        let mut filtered = jobs
            .get()
            .into_iter()
            .filter(|job| {
                let user_match =
                    filter_user.get().is_empty() || job.user_id.contains(&filter_user.get());
                let status_match = match filter_status.get().as_str() {
                    "All" => true,
                    "Pending" => matches!(job.status, veloce_common::JobStatus::Pending),
                    "Running" => matches!(job.status, veloce_common::JobStatus::Running),
                    "Completed" => matches!(job.status, veloce_common::JobStatus::Completed(_)),
                    "Failed" => matches!(job.status, veloce_common::JobStatus::Failed(_)),
                    "Killed" => matches!(job.status, veloce_common::JobStatus::Killed),
                    _ => true,
                };
                user_match && status_match
            })
            .collect::<Vec<_>>();

        filtered.sort_by(|a, b| {
            let cmp = match sort_col.get().as_str() {
                "id" => a.id.cmp(&b.id),
                "name" => a.job_name.cmp(&b.job_name),
                "user" => a.user_id.cmp(&b.user_id),
                "status" => format!("{:?}", a.status).cmp(&format!("{:?}", b.status)),
                "nodes" => a.req_nodes.cmp(&b.req_nodes),
                "cores" => a.req_cores.cmp(&b.req_cores),
                "priority" => a.priority.cmp(&b.priority),
                "submitted" => a.queued_time.cmp(&b.queued_time),
                _ => std::cmp::Ordering::Equal,
            };
            if sort_desc.get() {
                cmp.reverse()
            } else {
                cmp
            }
        });

        filtered
    };

    let total_filtered_count = move || filtered_jobs_all().len();

    let paginated_jobs = move || {
        let all = filtered_jobs_all();
        let start = page.get() * page_size;
        if start >= all.len() {
            return Vec::new();
        }
        let end = (start + page_size).min(all.len());
        all[start..end].to_vec()
    };

    let can_go_prev = move || page.get() > 0;
    let can_go_next = move || (page.get() + 1) * page_size < total_filtered_count();

    // Global Stats
    let _total_jobs = move || jobs.get().len();
    let _running_jobs = move || {
        jobs.get()
            .iter()
            .filter(|j| matches!(j.status, veloce_common::JobStatus::Running))
            .count()
    };
    let _pending_jobs = move || {
        jobs.get()
            .iter()
            .filter(|j| matches!(j.status, veloce_common::JobStatus::Pending))
            .count()
    };
    let _failed_jobs = move || {
        jobs.get()
            .iter()
            .filter(|j| matches!(j.status, veloce_common::JobStatus::Failed(_)))
            .count()
    };

    view! {
        <h2>"Jobs"</h2>
        <div class="filters" style="margin-bottom: 2rem; display: flex; gap: 1rem; align-items: center; flex-wrap: wrap;">
            <input
                type="text"
                placeholder="Filter by User ID"
                on:input=move |ev| { set_filter_user.set(event_target_value(&ev)); set_page.set(0); }
                prop:value=filter_user
                class="filter-input"
            />

            <select on:change=move |ev| { set_filter_status.set(event_target_value(&ev)); set_page.set(0); }>
                <option value="All">"All Statuses"</option>
                <option value="Pending">"Pending"</option>
                <option value="Running">"Running"</option>
                <option value="Completed">"Completed"</option>
                <option value="Failed">"Failed"</option>
                <option value="Killed">"Killed"</option>
            </select>
        </div>

        <table>
            <thead>
                <tr>
                    <th on:click=move |_| { if sort_col.get() == "id" { set_sort_desc.update(|d| *d = !*d); } else { set_sort_col.set("id".to_string()); set_sort_desc.set(true); } } style="cursor: pointer">
                        "ID" {move || if sort_col.get() == "id" { if sort_desc.get() { " ▼" } else { " ▲" } } else { "" }}
                    </th>
                    <th on:click=move |_| { if sort_col.get() == "name" { set_sort_desc.update(|d| *d = !*d); } else { set_sort_col.set("name".to_string()); set_sort_desc.set(false); } } style="cursor: pointer">
                        "Name" {move || if sort_col.get() == "name" { if sort_desc.get() { " ▼" } else { " ▲" } } else { "" }}
                    </th>
                    <th on:click=move |_| { if sort_col.get() == "user" { set_sort_desc.update(|d| *d = !*d); } else { set_sort_col.set("user".to_string()); set_sort_desc.set(false); } } style="cursor: pointer">
                        "User" {move || if sort_col.get() == "user" { if sort_desc.get() { " ▼" } else { " ▲" } } else { "" }}
                    </th>
                    <th>"Binary"</th>
                    <th on:click=move |_| { if sort_col.get() == "status" { set_sort_desc.update(|d| *d = !*d); } else { set_sort_col.set("status".to_string()); set_sort_desc.set(false); } } style="cursor: pointer">
                        "Status" {move || if sort_col.get() == "status" { if sort_desc.get() { " ▼" } else { " ▲" } } else { "" }}
                    </th>
                    <th on:click=move |_| { if sort_col.get() == "submitted" { set_sort_desc.update(|d| *d = !*d); } else { set_sort_col.set("submitted".to_string()); set_sort_desc.set(true); } } style="cursor: pointer">
                        "Submitted" {move || if sort_col.get() == "submitted" { if sort_desc.get() { " ▼" } else { " ▲" } } else { "" }}
                    </th>
                    <th>"Duration"</th>
                    <th on:click=move |_| { if sort_col.get() == "nodes" { set_sort_desc.update(|d| *d = !*d); } else { set_sort_col.set("nodes".to_string()); set_sort_desc.set(true); } } style="cursor: pointer">
                        "Nodes" {move || if sort_col.get() == "nodes" { if sort_desc.get() { " ▼" } else { " ▲" } } else { "" }}
                    </th>
                    <th on:click=move |_| { if sort_col.get() == "cores" { set_sort_desc.update(|d| *d = !*d); } else { set_sort_col.set("cores".to_string()); set_sort_desc.set(true); } } style="cursor: pointer">
                        "Cores" {move || if sort_col.get() == "cores" { if sort_desc.get() { " ▼" } else { " ▲" } } else { "" }}
                    </th>
                    <th on:click=move |_| { if sort_col.get() == "priority" { set_sort_desc.update(|d| *d = !*d); } else { set_sort_col.set("priority".to_string()); set_sort_desc.set(true); } } style="cursor: pointer">
                        "Priority" {move || if sort_col.get() == "priority" { if sort_desc.get() { " ▼" } else { " ▲" } } else { "" }}
                    </th>
                    <th>"Array"</th>
                    <th>"Container"</th>
                    <th>"Deps"</th>
                </tr>
            </thead>
            <tbody>
                {move || paginated_jobs().into_iter().map(|job| {
                        let status_class = format!("badge {}", format!("{:?}", job.status).split('(').next().unwrap_or("").to_lowercase());
                        let status_text = match &job.status {
                            veloce_common::JobStatus::Killed => "CANCELLED".to_string(),
                            _ => format!("{:?}", job.status),
                        };

                        let submitted_text = chrono::DateTime::from_timestamp(job.queued_time as i64, 0)
                            .map(|dt| {
                                // In WASM, chrono::Local works if the 'js' feature is enabled.
                                // We'll use a reliable way to show local time.
                                let local_dt: chrono::DateTime<chrono::Local> = dt.into();
                                local_dt.format("%Y-%m-%d %H:%M:%S").to_string()
                            })
                            .unwrap_or_else(|| "-".to_string());

                        let duration = if let Some(start) = job.start_time {
                            let end = job.end_time.unwrap_or_else(|| chrono::Utc::now().timestamp() as u64);
                            let dur = end.saturating_sub(start);
                            if dur < 60 {
                                format!("{}s", dur)
                            } else {
                                format!("{}m {}s", dur / 60, dur % 60)
                            }
                        } else {
                            "-".to_string()
                        };

                        let array_text = match (job.array_id, job.array_task_id) {
                            (Some(id), Some(task)) => format!("{}:{}", id, task),
                            _ => "-".to_string(),
                        };

                        let container_text = if let Some(asset) = &job.container_asset {
                            asset.name.clone()
                        } else {
                            "-".to_string()
                        };

                        let deps_text = if let Some(deps) = &job.dependencies {
                            if deps.is_empty() {
                                "-".to_string()
                            } else {
                                deps.iter().map(|d| d.to_string()).collect::<Vec<_>>().join(", ")
                            }
                        } else {
                            "-".to_string()
                        };
                        let name_text = job.job_name.clone().unwrap_or_else(|| "-".to_string());
                        let reason_text = job.reason.clone().unwrap_or_default();
                        let show_reason = matches!(job.status, veloce_common::JobStatus::Pending) && !reason_text.is_empty();

                        view! {
                            <tr>
                                <td><A href=format!("/jobs/{}", job.id)>{job.id}</A></td>
                                <td><div style="max-width: 160px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;" title=name_text.clone()>{name_text}</div></td>
                                <td>{job.user_id}</td>
                                <td><div style="max-width: 200px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;" title=job.binary.clone()>{job.binary}</div></td>
                                <td>
                                    <span class=status_class>{status_text}</span>
                                    {if job.is_idle {
                                        view! { <span class="badge idle" style="margin-left: 4px; font-size: 0.7rem; padding: 2px 4px;">"IDLE " {job.idle_duration} "s"</span> }.into_view()
                                    } else {
                                        ().into_view()
                                    }}
                                    {if job.mpi_stats.is_some() {
                                        view! { <span class="badge info" style="margin-left: 4px; font-size: 0.7rem; padding: 2px 4px;">"MPI"</span> }.into_view()
                                    } else {
                                        ().into_view()
                                    }}
                                    {if show_reason {
                                        view! {
                                            <div
                                                title=reason_text.clone()
                                                style="max-width: 240px; margin-top: 0.35rem; color: var(--text-dim); font-size: 0.75rem; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;"
                                            >
                                                <i class="ph ph-info"></i> {reason_text}
                                            </div>
                                        }.into_view()
                                    } else {
                                        ().into_view()
                                    }}
                                </td>
                                <td>{submitted_text}</td>
                                <td>{duration}</td>
                                <td>{job.req_nodes}</td>
                                <td>{job.req_cores}</td>
                                <td>{job.priority}</td>
                                <td>{array_text}</td>
                                <td><div style="max-width: 150px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;" title=container_text.clone()>{container_text}</div></td>
                                <td><div style="max-width: 150px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;" title=deps_text.clone()>{deps_text}</div></td>
                            </tr>
                        }
                    }).collect_view()}
            </tbody>
        </table>

        <div class="node-pagination" style="margin-top: 1rem;">
            <button
                class="node-pagination-btn"
                disabled=move || !can_go_prev()
                on:click=move |_| set_page.update(|p| *p -= 1)
            >
                "< Prev"
            </button>
            <span>
                {move || format!("Page {} of {}", page.get() + 1, std::cmp::max(1, total_filtered_count().div_ceil(page_size)))}
            </span>
            <button
                class="node-pagination-btn"
                disabled=move || !can_go_next()
                on:click=move |_| set_page.update(|p| *p += 1)
            >
                "Next >"
            </button>
        </div>
    }
}
