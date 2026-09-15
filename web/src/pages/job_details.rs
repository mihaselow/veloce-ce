use crate::api_client::{api_delete, api_get, api_post, fileserver_get};
use crate::app::{set_interval_with_handle, WebConfig};
use crate::consoles::{InteractiveProxyFrame, TerminalConsole, VncConsole};
use crate::metrics_chart::JobMetricsChart;
use leptos::*;
use leptos_router::{use_navigate, use_params_map};
use veloce_common::{JobInfo, JobStatus, StepInfo};
use wasm_bindgen::JsCast;

fn job_endpoint(id: &str) -> String {
    format!("/api/v1/jobs/{id}")
}

fn job_child_endpoint(id: &str, child: &str) -> String {
    format!("/api/v1/jobs/{id}/{child}")
}
fn job_details_changed(prev: &JobInfo, next: &JobInfo) -> bool {
    let mut prev = prev.clone();
    let mut next = next.clone();

    // High-frequency telemetry is rendered through JobMetricsChart; don't let it
    // rebuild the whole details page every poll.
    prev.current_cpu_usage = 0.0;
    next.current_cpu_usage = 0.0;
    prev.current_memory_usage = 0;
    next.current_memory_usage = 0;
    prev.idle_duration = 0;
    next.idle_duration = 0;

    prev != next
}
#[component]
pub(crate) fn JobDetails() -> impl IntoView {
    let params = use_params_map();
    let config =
        use_context::<Resource<bool, Option<WebConfig>>>().expect("WebConfig context missing");
    let navigate = use_navigate();
    let id = move || params.with(|params| params.get("id").cloned().unwrap_or_default());
    let id_untracked =
        move || params.with_untracked(|params| params.get("id").cloned().unwrap_or_default());

    let (job, set_job) = create_signal(None::<JobInfo>);
    let (steps, set_steps) = create_signal(Vec::<StepInfo>::new());
    let (logs, set_logs) = create_signal("Loading logs...".to_string());
    let (log_type, set_log_type) = create_signal("stdout".to_string());
    let (log_rank, set_log_rank) = create_signal(0usize);
    let (result_file_id, set_result_file_id) = create_signal(None::<String>);
    let (active_tab, set_active_tab) = create_signal("logs".to_string());

    let is_alive = std::rc::Rc::new(std::cell::Cell::new(true));
    {
        let is_alive = is_alive.clone();
        on_cleanup(move || {
            is_alive.set(false);
        });
    }

    // Submit Step Signal
    let (step_binary, set_step_binary) = create_signal("".to_string());
    let (step_args, set_step_args) = create_signal("".to_string());
    let (step_ntasks, set_step_ntasks) = create_signal(1u32);

    let fetch_job = std::rc::Rc::new({
        let is_alive = is_alive.clone();
        move || {
            let job_id = id_untracked();
            let conf = config.get().flatten();
            if job_id.is_empty() {
                return;
            }
            if conf.is_some() {
                let is_alive = is_alive.clone();
                spawn_local(async move {
                    if let Ok(resp) = api_get(&job_endpoint(&job_id)).send().await {
                        let data = resp.json::<JobInfo>().await.ok();
                        if let Some(data) = data {
                            if !is_alive.get() {
                                return;
                            }
                            set_job.update(|j| {
                                if j.as_ref()
                                    .map(|current| job_details_changed(current, &data))
                                    .unwrap_or(true)
                                {
                                    *j = Some(data);
                                }
                            });
                        }
                    }
                    if let Ok(resp) = api_get(&job_child_endpoint(&job_id, "steps")).send().await {
                        if let Ok(data) = resp.json::<Vec<StepInfo>>().await {
                            if !is_alive.get() {
                                return;
                            }
                            set_steps.update(|s| {
                                if s != &data {
                                    *s = data;
                                }
                            });
                        }
                    }
                });
            }
        }
    });

    let fetch_logs = std::rc::Rc::new({
        let is_alive = is_alive.clone();
        move || {
            let job_id = id_untracked();
            let ltype = log_type.get_untracked();
            let rank = log_rank.get_untracked();
            let conf = config.get().flatten();
            if job_id.is_empty() {
                return;
            }
            if conf.is_some() {
                let is_alive = is_alive.clone();
                spawn_local(async move {
                    let url = format!(
                        "{}?type={}&rank={}",
                        job_child_endpoint(&job_id, "logs"),
                        ltype,
                        rank
                    );
                    if let Ok(resp) = api_get(&url).send().await {
                        if let Ok(text) = resp.text().await {
                            if !is_alive.get() {
                                return;
                            }
                            // Check for result ID in stdout
                            if ltype == "stdout" {
                                if let Some(start) = text.find("[VELOCE_RESULT_ID: ") {
                                    let rest = &text[start + 19..];
                                    if let Some(end) = rest.find(']') {
                                        let id = &rest[..end];
                                        set_result_file_id.set(Some(id.to_string()));
                                    }
                                }
                            }
                            set_logs.set(text);
                        }
                    }
                });
            }
        }
    });

    let fetch_job_clone = fetch_job.clone();
    let fetch_logs_clone = fetch_logs.clone();
    create_effect(move |_| {
        let _ = id(); // Explicitly track ID changes
        let _ = log_type.get(); // Explicitly track log_type
        let _ = log_rank.get(); // Explicitly track log_rank
        let fetch_job = fetch_job_clone.clone();
        let fetch_logs = fetch_logs_clone.clone();
        fetch_job();
        fetch_logs();

        let fetch_job_for_interval = fetch_job.clone();
        let details_handle = set_interval_with_handle(
            move || {
                fetch_job_for_interval();
            },
            5000,
        );

        let fetch_logs_for_interval = fetch_logs.clone();
        let active_tab_for_interval = active_tab;
        let logs_handle = set_interval_with_handle(
            move || {
                if active_tab_for_interval.get_untracked() == "logs" {
                    fetch_logs_for_interval();
                }
            },
            2000,
        );
        on_cleanup(move || {
            if let Ok(id) = details_handle {
                if let Some(win) = web_sys::window() {
                    win.clear_interval_with_handle(id);
                }
            }
            if let Ok(id) = logs_handle {
                if let Some(win) = web_sys::window() {
                    win.clear_interval_with_handle(id);
                }
            }
        });
    });

    let cancel_action = create_action(move |job_id: &String| {
        let jid = job_id.clone();
        let conf = config.get().flatten();
        async move {
            if conf.is_some() {
                let _ = api_delete(&job_endpoint(&jid)).send().await;
            }
        }
    });

    let clone_action = create_action(move |_: &()| {
        let conf = config.get().flatten();
        let navigate = navigate.clone();
        let source_job = job.get_untracked();
        async move {
            let Some(source_job) = source_job else {
                gloo_dialogs::alert("Job details are still loading.");
                return;
            };

            if conf.is_none() {
                gloo_dialogs::alert("Controller configuration is not loaded.");
                return;
            }

            let image_uri = source_job
                .container_asset
                .as_ref()
                .map(|asset| format!("veloce://{}", asset.id));
            let payload = serde_json::json!({
                "job_name": source_job.job_name,
                "job_comment": source_job.job_comment,
                "binary": source_job.binary,
                "args": source_job.args,
                "req_nodes": source_job.req_nodes,
                "req_cores": source_job.req_cores,
                "req_memory": source_job.req_memory,
                "walltime": source_job.walltime,
                "priority": source_job.priority,
                "user_id": source_job.user_id,
                "working_directory": source_job.working_directory,
                "array_indices": source_job.array_task_id.map(|task_id| vec![task_id]),
                "inputs": source_job.inputs,
                "gres_req": source_job.gres_req,
                "env_vars": source_job.env_vars,
                "wait_for_licenses": source_job.wait_for_licenses,
                "dependencies": source_job.dependencies,
                "dependency_specs": source_job.dependency_specs,
                "qos": source_job.qos,
                "image_uri": image_uri,
                "vnc_enabled": source_job.vnc_enabled,
                "inherit_host_env": source_job.inherit_host_env,
                "env_allowlist": source_job.env_allowlist,
                "job_profile": source_job.job_profile,
            });

            match api_post("/api/v1/jobs")
                .header("Content-Type", "application/json")
                .body(payload.to_string())
                .unwrap()
                .send()
                .await
            {
                Ok(resp) if resp.ok() => {
                    let status = resp.status();
                    let body = resp
                        .json::<serde_json::Value>()
                        .await
                        .unwrap_or(serde_json::Value::Null);
                    let new_id = body
                        .get("job_id")
                        .or_else(|| body.get("base_job_id"))
                        .and_then(|id| id.as_u64());
                    if let Some(new_id) = new_id {
                        navigate(&format!("/jobs/{}", new_id), Default::default());
                    } else {
                        gloo_dialogs::alert(&format!(
                            "Job cloned, but response did not include a job ID (HTTP {}).",
                            status
                        ));
                    }
                }
                Ok(resp) => {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    if body.is_empty() {
                        gloo_dialogs::alert(&format!("Clone failed: HTTP {}", status));
                    } else {
                        gloo_dialogs::alert(&format!("Clone failed: HTTP {} - {}", status, body));
                    }
                }
                Err(_) => gloo_dialogs::alert("Clone failed: network error"),
            }
        }
    });

    let submit_step_action = create_action(move |_: &()| {
        let job_id = id_untracked();
        let conf = config.get().flatten();
        async move {
            let _ = conf;
            let payload = serde_json::json!({
                "binary": step_binary.get_untracked(),
                "args": step_args.get_untracked().split_whitespace().map(String::from).collect::<Vec<_>>(),
                "req_nodes": 1,
                "req_cores": 1,
                "ntasks": step_ntasks.get_untracked(),
            });

            if let Ok(resp) = api_post(&job_child_endpoint(&job_id, "steps"))
                .header("Content-Type", "application/json")
                .body(payload.to_string())
                .unwrap()
                .send()
                .await
            {
                if resp.ok() {
                    set_step_binary.set("".to_string());
                    set_step_args.set("".to_string());
                } else {
                    gloo_dialogs::alert(&format!("Step submission failed: {}", resp.status()));
                }
            }
        }
    });

    let download_action = create_action(move |(file_id, job_id): &(String, String)| {
        let fid = file_id.clone();
        let jid = job_id.clone();
        async move {
            let url = format!("/api/v1/files/{}", fid);
            match fileserver_get(&url).send().await {
                Ok(resp) => {
                    if resp.ok() {
                        if let Ok(data) = resp.binary().await {
                            let array = js_sys::Uint8Array::from(&data[..]);
                            let parts = js_sys::Array::new();
                            parts.push(&array);
                            if let Ok(blob) = web_sys::Blob::new_with_u8_array_sequence(&parts) {
                                if let Ok(url) = web_sys::Url::create_object_url_with_blob(&blob) {
                                    if let Some(win) = web_sys::window() {
                                        if let Some(doc) = win.document() {
                                            if let Ok(elem) = doc.create_element("a") {
                                                let a = elem
                                                    .unchecked_into::<web_sys::HtmlAnchorElement>();
                                                a.set_href(&url);
                                                a.set_download(&format!(
                                                    "results_job_{}.tar.gz",
                                                    jid
                                                ));
                                                a.click();
                                                let _ = web_sys::Url::revoke_object_url(&url);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                Err(_) => gloo_dialogs::alert("Download network error"),
            }
        }
    });

    let download_file_action = create_action(move |(file_id, filename): &(String, String)| {
        let fid = file_id.clone();
        let fname = filename.clone();
        async move {
            let url = format!("/api/v1/files/{}", fid);
            match fileserver_get(&url).send().await {
                Ok(resp) => {
                    if resp.ok() {
                        if let Ok(data) = resp.binary().await {
                            let array = js_sys::Uint8Array::from(&data[..]);
                            let parts = js_sys::Array::new();
                            parts.push(&array);
                            if let Ok(blob) = web_sys::Blob::new_with_u8_array_sequence(&parts) {
                                if let Ok(url) = web_sys::Url::create_object_url_with_blob(&blob) {
                                    if let Some(win) = web_sys::window() {
                                        if let Some(doc) = win.document() {
                                            if let Ok(elem) = doc.create_element("a") {
                                                let a = elem
                                                    .unchecked_into::<web_sys::HtmlAnchorElement>();
                                                a.set_href(&url);
                                                a.set_download(&fname);
                                                a.click();
                                                let _ = web_sys::Url::revoke_object_url(&url);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                Err(_) => gloo_dialogs::alert("Download network error"),
            }
        }
    });

    let is_running = create_memo(move |_| {
        job.get()
            .as_ref()
            .map(|j| matches!(j.status, JobStatus::Running))
            .unwrap_or(false)
    });

    view! {
        <div>
            <h2>"Job Details: " {id}</h2>
            {move || {
                if let Some(j) = job.get() {
                    let submitted_at = chrono::DateTime::from_timestamp(j.queued_time as i64, 0)
                            .map(|dt| {
                                let local_dt: chrono::DateTime<chrono::Local> = dt.into();
                                local_dt.format("%Y-%m-%d %H:%M:%S").to_string()
                            })
                            .unwrap_or_else(|| "-".to_string());

                    let started_at = j.start_time.and_then(|t| chrono::DateTime::from_timestamp(t as i64, 0))
                            .map(|dt| {
                                let local_dt: chrono::DateTime<chrono::Local> = dt.into();
                                local_dt.format("%Y-%m-%d %H:%M:%S").to_string()
                            })
                            .unwrap_or_else(|| "-".to_string());

                    let ended_at = j.end_time.and_then(|t| chrono::DateTime::from_timestamp(t as i64, 0))
                            .map(|dt| {
                                let local_dt: chrono::DateTime<chrono::Local> = dt.into();
                                local_dt.format("%Y-%m-%d %H:%M:%S").to_string()
                            })
                            .unwrap_or_else(|| "-".to_string());

                    let duration = if let Some(start) = j.start_time {
                        let end = j.end_time.unwrap_or_else(|| chrono::Utc::now().timestamp() as u64);
                        let dur = end.saturating_sub(start);
                        if dur < 60 {
                            format!("{}s", dur)
                        } else {
                            format!("{}m {}s", dur / 60, dur % 60)
                        }
                    } else {
                        "-".to_string()
                    };

                    view! {
                        <div class="glass-panel">
                            <div style="display: flex; align-items: center; gap: 10px;">
                                <p style="margin: 0;"><strong>"Status: "</strong>
                                    {match &j.status {
                                        veloce_common::JobStatus::Killed => "CANCELLED".to_string(),
                                        _ => format!("{:?}", j.status),
                                    }}
                                </p>
                                {if j.is_idle {
                                    view! { <span class="badge" style="background: #f1c40f; color: black; padding: 4px 8px;">"IDLE DETECTED (" {j.idle_duration} "s)"</span> }.into_view()
                                } else {
                                    ().into_view()
                                }}
                            </div>
                            <p><strong>"Name: "</strong> {j.job_name.clone().unwrap_or_else(|| "-".to_string())}</p>
                            <p><strong>"Comment: "</strong> {j.job_comment.clone().unwrap_or_else(|| "-".to_string())}</p>
                            <p><strong>"Binary: "</strong> {j.binary}</p>
                            <p><strong>"Args: "</strong> {j.args.join(" ")}</p>
                            <p><strong>"Resources: "</strong> {format!("{} Nodes, {} Cores, {} MB Mem", j.req_nodes, j.req_cores, j.req_memory)}</p>
                            <p><strong>"Enforcement: "</strong> {move || if j.cgroup_active { "Cgroup v2 Isolation" } else { "Traditional (setrlimit)" }}</p>
                            {
                                if let (Some(aid), Some(tid)) = (j.array_id, j.array_task_id) {
                                    view! { <p><strong>"Array Info: "</strong> {format!("Array ID: {}, Task ID: {}", aid, tid)}</p> }.into_view()
                                } else {
                                    ().into_view()
                                }
                            }
                            {
                                if let Some(asset) = &j.container_asset {
                                    view! { <p><strong>"Container: "</strong> {asset.name.clone()}</p> }.into_view()
                                } else {
                                    ().into_view()
                                }
                            }
                            {
                                if let Some(deps) = &j.dependencies {
                                    if !deps.is_empty() {
                                        let deps_str = deps.iter().map(|d| d.to_string()).collect::<Vec<_>>().join(", ");
                                        view! { <p><strong>"Dependencies: "</strong> {deps_str}</p> }.into_view()
                                    } else {
                                        ().into_view()
                                    }
                                } else {
                                    ().into_view()
                                }
                            }
                            <p><strong>"Reason: "</strong> {j.reason.unwrap_or("-".to_string())}</p>

                            {move || j.mpi_stats.as_ref().map(|stats| {
                                view! {
                                    <div class="glass-panel" style="margin-top: 15px; border-left: 4px solid var(--primary); background: rgba(56, 189, 248, 0.1);">
                                        <h4 style="margin-top: 0; color: var(--primary);">"MPI Performance Insights"</h4>
                                        <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 10px;">
                                            <div><strong>"Total Send Calls: "</strong> {stats.send_calls}</div>
                                            <div><strong>"Aggregated Data: "</strong> {format!("{:.2} MB", stats.send_bytes as f64 / 1024.0 / 1024.0)}</div>
                                            <div><strong>"Max MPI Time: "</strong> {format!("{:.6} s", stats.send_time_secs)}</div>
                                        </div>
                                    </div>
                                }.into_view()
                            })}

                            {move || if is_running.get() {
                                view! { <JobMetricsChart job_id=j.id /> }.into_view()
                            } else {
                                ().into_view()
                            }}

                            <hr/>
                            <p><strong>"Submitted: "</strong> {submitted_at}</p>
                            <p><strong>"Started: "</strong> {started_at}</p>
                            <p><strong>"Ended: "</strong> {ended_at}</p>
                            <p><strong>"Duration: "</strong> {duration}</p>

                            <div class="actions">
                                <button
                                    class="button"
                                    disabled=move || clone_action.pending().get()
                                    on:click=move |_| clone_action.dispatch(())
                                >
                                    <i class="ph ph-copy"></i>
                                    {move || if clone_action.pending().get() { " Cloning..." } else { " Clone Job" }}
                                </button>
                                {
                                    if matches!(&j.status, JobStatus::Pending | JobStatus::Running) {
                                        let cancel_id = id();
                                        view! { <button class="danger" on:click=move |_| cancel_action.dispatch(cancel_id.clone())><i class="ph ph-x-circle"></i> "Cancel Job"</button> }.into_view()
                                    } else {
                                        ().into_view()
                                    }
                                }
                                {move || result_file_id.get().map(|fid| {
                                     let jid = id();
                                     view! {
                                         <button
                                            class="button"
                                            style="margin-left: 10px;"
                                            on:click=move |_| download_action.dispatch((fid.clone(), jid.clone()))
                                         >
                                            <i class="ph ph-download-simple"></i> "Download Results"
                                         </button>
                                     }
                                })}
                                {let jid = id();
                                 if let Some(fid) = &j.stdout_file_id {
                                      let fid = fid.clone();
                                      view! {
                                          <button
                                             class="button"
                                             style="margin-left: 10px;"
                                             on:click=move |_| download_file_action.dispatch((fid.clone(), format!("job_{}_stdout.log", jid)))
                                          >
                                             <i class="ph ph-file-text"></i> "Download Stdout"
                                          </button>
                                      }.into_view()
                                 } else {
                                      ().into_view()
                                 }}
                                {let jid = id();
                                 if let Some(fid) = &j.stderr_file_id {
                                      let fid = fid.clone();
                                      view! {
                                          <button
                                             class="button"
                                             style="margin-left: 10px;"
                                             on:click=move |_| download_file_action.dispatch((fid.clone(), format!("job_{}_stderr.log", jid)))
                                          >
                                             <i class="ph ph-file-text"></i> "Download Stderr"
                                          </button>
                                      }.into_view()
                                 } else {
                                      ().into_view()
                                 }}
                                {let jid = id();
                                 if let Some(fid) = &j.workdir_file_id {
                                      let fid = fid.clone();
                                      view! {
                                          <button
                                             class="button"
                                             style="margin-left: 10px;"
                                             on:click=move |_| download_file_action.dispatch((fid.clone(), format!("job_{}_workdir.tar.gz", jid)))
                                          >
                                             <i class="ph ph-file-archive"></i> "Download Working Dir"
                                          </button>
                                      }.into_view()
                                 } else {
                                      ().into_view()
                                 }}
                            </div>
                        </div>
                    }.into_view()
                } else {
                    view! { <p>"Loading..."</p> }.into_view()
                }
            }}

            {move || {
                let n_count = job.get().map(|j| j.req_nodes).unwrap_or(1);
                if n_count > 1 {
                    view! {
                        <div class="glass-panel" style="margin-top: 1rem; border-left: 4px solid var(--primary); padding: 0.5rem 1rem; display: flex; align-items: center; justify-content: space-between;">
                            <div style="display: flex; align-items: center; gap: 10px;">
                                <i class="ph ph-nodes" style="color: var(--primary); font-size: 1.2rem;"></i>
                                <div>
                                    <h4 style="margin: 0; font-size: 0.9rem;">"Multi-Node Job Detected"</h4>
                                    <p style="margin: 0; font-size: 0.8rem; color: var(--text-dim);">"This job is running across " {n_count} " nodes. Use the selector below to view specific rank logs."</p>
                                </div>
                            </div>
                        </div>
                    }.into_view()
                } else {
                    ().into_view()
                }
            }}

            <div class="steps-section" style="margin-top: 2rem;">
                <h3>"Steps"</h3>
                {move || {
                    let current_steps = steps.get();
                    if current_steps.is_empty() {
                        view! { <p>"No steps executed yet."</p> }.into_view()
                    } else {
                        view! {
                            <table>
                                <thead>
                                    <tr>
                                        <th>"Step ID"</th>
                                        <th>"Binary"</th>
                                        <th>"Status"</th>
                                        <th>"Nodes"</th>
                                        <th>"Tasks"</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {current_steps.into_iter().map(|s| {
                                        let status_class = format!("badge {}", format!("{:?}", s.status).split('(').next().unwrap_or("").to_lowercase());
                                        view! {
                                            <tr>
                                                <td>{s.step_id}</td>
                                                <td>{s.binary}</td>
                                                <td><span class=status_class>{format!("{:?}", s.status)}</span></td>
                                                <td>{s.req_nodes}</td>
                                                <td>{s.ntasks}</td>
                                            </tr>
                                        }
                                    }).collect_view()}
                                </tbody>
                            </table>
                        }.into_view()
                    }
                }}

                <Show
                    when=move || is_running.get()
                    fallback=|| ().into_view()
                >
                    <div class="glass-panel" style="margin-top: 1rem; border: 1px dashed var(--primary);">
                        <h4>"Submit New Step"</h4>
                        <div class="form-group">
                            <label>"Step Binary"</label>
                            <input type="text" on:input=move |ev| set_step_binary.set(event_target_value(&ev)) prop:value=step_binary placeholder="sh"/>
                        </div>
                        <div class="form-group">
                            <label>"Arguments"</label>
                            <input type="text" on:input=move |ev| set_step_args.set(event_target_value(&ev)) prop:value=step_args placeholder="-c 'echo hello'"/>
                        </div>
                        <div class="form-group">
                            <label>"Number of Tasks"</label>
                            <input type="number" on:input=move |ev| set_step_ntasks.set(event_target_value(&ev).parse().unwrap_or(1)) prop:value=step_ntasks/>
                        </div>
                        <button on:click=move |_| submit_step_action.dispatch(())>"Launch Step"</button>
                    </div>
                </Show>
            </div>

            <div class="tab-header" style="display: flex; gap: 10px; margin-top: 2rem; border-bottom: 1px solid rgba(255,255,255,0.1); padding-bottom: 10px;">
                <button
                    class=move || if active_tab.get() == "logs" { "tab-btn active" } else { "tab-btn" }
                    on:click=move |_| set_active_tab.set("logs".to_string())
                    style=move || format!(
                        "padding: 8px 16px; font-weight: 500; font-size: 0.85rem; border-radius: 6px; border: none; cursor: pointer; transition: all 0.2s; {}",
                        if active_tab.get() == "logs" {
                            "background: var(--primary); color: white; box-shadow: 0 4px 12px rgba(0,0,0,0.15);"
                        } else {
                            "background: transparent; color: var(--text-dim);"
                        }
                    )
                >
                    <i class="ph ph-file-text"></i> " Logs"
                </button>

                <Show
                    when=move || is_running.get()
                    fallback=|| ().into_view()
                >
                    <button
                        class=move || if active_tab.get() == "terminal" { "tab-btn active" } else { "tab-btn" }
                        on:click=move |_| set_active_tab.set("terminal".to_string())
                        style=move || format!(
                            "padding: 8px 16px; font-weight: 500; font-size: 0.85rem; border-radius: 6px; border: none; cursor: pointer; transition: all 0.2s; {}",
                            if active_tab.get() == "terminal" {
                                "background: var(--primary); color: white; box-shadow: 0 4px 12px rgba(0,0,0,0.15);"
                            } else {
                                "background: transparent; color: var(--text-dim);"
                            }
                        )
                    >
                        <i class="ph ph-terminal-window"></i> " Interactive Shell"
                    </button>

                    <Show
                        when=move || job.get().map(|j| j.vnc_enabled).unwrap_or(false)
                        fallback=|| ().into_view()
                    >
                        <button
                            class=move || if active_tab.get() == "vnc" { "tab-btn active" } else { "tab-btn" }
                            on:click=move |_| set_active_tab.set("vnc".to_string())
                            style=move || format!(
                                "padding: 8px 16px; font-weight: 500; font-size: 0.85rem; border-radius: 6px; border: none; cursor: pointer; transition: all 0.2s; {}",
                                if active_tab.get() == "vnc" {
                                    "background: var(--primary); color: white; box-shadow: 0 4px 12px rgba(0,0,0,0.15);"
                                } else {
                                    "background: transparent; color: var(--text-dim);"
                                }
                            )
                        >
                            <i class="ph ph-desktop"></i> " Interactive VNC"
                        </button>
                    </Show>

                    <Show
                        when=move || job.get().and_then(|j| j.interactive_port).is_some()
                        fallback=|| ().into_view()
                    >
                        <button
                            class=move || if active_tab.get() == "jupyter" { "tab-btn active" } else { "tab-btn" }
                            on:click=move |_| set_active_tab.set("jupyter".to_string())
                            style=move || format!(
                                "padding: 8px 16px; font-weight: 500; font-size: 0.85rem; border-radius: 6px; border: none; cursor: pointer; transition: all 0.2s; {}",
                                if active_tab.get() == "jupyter" {
                                    "background: var(--primary); color: white; box-shadow: 0 4px 12px rgba(0,0,0,0.15);"
                                } else {
                                    "background: transparent; color: var(--text-dim);"
                                }
                            )
                        >
                            <i class="ph ph-notebook"></i> " JupyterLab"
                        </button>
                    </Show>
                </Show>
            </div>

            {move || {
                let job_id = id();
                let api_key = config.get().flatten().map(|c| c.controller_api_key).unwrap_or_default();
                match active_tab.get().as_str() {
                    "logs" => view! {
                        <LogConsole
                            logs=Signal::from(logs)
                            log_type=log_type
                            set_log_type=set_log_type
                            nodes_count=Signal::from(move || job.get().map(|j| j.req_nodes).unwrap_or(1))
                            log_rank=log_rank
                            set_log_rank=set_log_rank
                        />
                    }.into_view(),
                    "terminal" => {
                        match job_id.parse::<u64>() {
                            Ok(local_job_id) => view! { <TerminalConsole job_id=local_job_id api_key=api_key /> }.into_view(),
                            Err(_) => ().into_view(),
                        }
                    },
                    "vnc" => {
                        match job_id.parse::<u64>() {
                            Ok(local_job_id) => view! { <VncConsole job_id=local_job_id api_key=api_key /> }.into_view(),
                            Err(_) => ().into_view(),
                        }
                    },
                    "jupyter" => {
                        match job_id.parse::<u64>() {
                            Ok(local_job_id) => view! { <InteractiveProxyFrame job_id=local_job_id /> }.into_view(),
                            Err(_) => ().into_view(),
                        }
                    },
                    _ => ().into_view(),
                }
            }}
        </div>
    }
}
#[component]
fn LogConsole(
    logs: Signal<String>,
    log_type: ReadSignal<String>,
    set_log_type: WriteSignal<String>,
    nodes_count: Signal<usize>,
    log_rank: ReadSignal<usize>,
    set_log_rank: WriteSignal<usize>,
) -> impl IntoView {
    view! {
        <div class="log-console glass-panel" style="margin-top: 1rem;">
            <div class="log-header" style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.5rem; padding: 0.5rem 1rem; background: rgba(255,255,255,0.05); border-radius: 4px 4px 0 0; border-bottom: 1px solid rgba(255,255,255,0.1);">
                <h3 style="margin: 0; font-size: 0.9rem; text-transform: uppercase; letter-spacing: 0.05em; color: var(--text-dim);">"Log Stream"</h3>
                <div class="log-controls" style="display: flex; gap: 0.5rem; align-items: center;">
                    {move || if nodes_count.get() > 1 {
                        view! {
                            <div class="rank-tabs" style="display: flex; gap: 4px; background: rgba(0,0,0,0.2); padding: 3px; border-radius: 6px; border: 1px solid rgba(255,255,255,0.1); margin-right: 15px;">
                                {(0..nodes_count.get()).map(|r| {
                                    let is_active = move || log_rank.get() == r;
                                    view! {
                                        <button
                                            class=move || if is_active() { "active" } else { "" }
                                            on:click=move |_| set_log_rank.set(r)
                                            style=move || format!(
                                                "padding: 4px 10px; font-size: 0.7rem; border-radius: 4px; border: none; cursor: pointer; transition: all 0.1s; {}",
                                                if is_active() {
                                                    "background: var(--primary); color: white; box-shadow: 0 2px 4px rgba(0,0,0,0.2);"
                                                } else {
                                                    "background: transparent; color: var(--text-dim);"
                                                }
                                            )
                                        >
                                            {format!("Rank {}", r)}
                                        </button>
                                    }
                                }).collect_view()}
                            </div>
                        }.into_view()
                    } else {
                        ().into_view()
                    }}
                    <button
                        class=move || if log_type.get() == "stdout" { "active" } else { "" }
                        on:click=move |_| set_log_type.set("stdout".to_string())
                        style="padding: 2px 10px; font-size: 0.7rem; border-radius: 4px;"
                    >
                        "STDOUT"
                    </button>
                    <button
                        class=move || if log_type.get() == "stderr" { "active" } else { "" }
                        on:click=move |_| set_log_type.set("stderr".to_string())
                        style="padding: 2px 10px; font-size: 0.7rem; border-radius: 4px;"
                    >
                        "STDERR"
                    </button>
                </div>
            </div>
            <div class="log-body" style="background: #0d1117; border-radius: 0 0 4px 4px; padding: 1rem; border: 1px solid rgba(255,255,255,0.1); border-top: none; max-height: 600px; overflow-y: auto;">
                <pre class="logs-container" style="margin: 0; font-family: 'JetBrains Mono', 'Fira Code', monospace; font-size: 0.8rem; line-height: 1.4; color: #e6edf3; white-space: pre-wrap; word-break: break-all;">
                    {move || if logs.get().is_empty() { "No logs available.".to_string() } else { logs.get() }}
                </pre>
            </div>
        </div>
    }
}
