use crate::api_client::api_post;
use crate::app::WebConfig;
use crate::login::{get_session_storage_item, has_admin_operator_privileges};
use crate::templates::{template_cards, template_group_name, template_version};
use gloo_net::http::Request;
use leptos::*;
use leptos_router::use_navigate;
use veloce_common::apptainer::ContainerAsset;

#[component]
pub(crate) fn SubmitJob() -> impl IntoView {
    let config =
        use_context::<Resource<bool, Option<WebConfig>>>().expect("WebConfig context missing");
    let containers_res =
        use_context::<Resource<Option<Option<WebConfig>>, Option<Vec<ContainerAsset>>>>()
            .expect("Containers context missing");

    let (selected_template, set_selected_template) = create_signal("General".to_string());
    let (selected_version, set_selected_version) = create_signal("".to_string());

    let (job_name, set_job_name) = create_signal("".to_string());
    let (job_comment, set_job_comment) = create_signal("".to_string());
    let (binary, set_binary) = create_signal("".to_string());
    let (args, set_args) = create_signal("".to_string());
    let (raw_command_template, set_raw_command_template) = create_signal("".to_string());
    let (nodes, set_nodes) = create_signal(1usize);
    let (cores, set_cores) = create_signal(1u32);
    let (memory, set_memory) = create_signal(1024u64);
    let (walltime, set_walltime) = create_signal(3600u64);
    let (priority, set_priority) = create_signal(0u32);
    let initial_user = get_session_storage_item("veloce_session_user_id")
        .unwrap_or_else(|| "web_user".to_string());
    let (user_id, set_user_id) = create_signal(initial_user);
    let (cwd, set_cwd) = create_signal("/tmp".to_string());
    let (array_indices, set_array_indices) = create_signal("".to_string());
    let (image_uri, set_image_uri) = create_signal(None::<String>);
    let (cwl_content, set_cwl_content) = create_signal("".to_string());
    let (param_values, set_param_values) =
        create_signal(std::collections::HashMap::<String, String>::new());
    let visible_template_cards =
        create_memo(move |_| template_cards(containers_res.get().flatten().unwrap_or_default()));
    let visible_templates =
        create_memo(move |_| containers_res.get().flatten().unwrap_or_default());

    // File Upload
    let (selected_file, set_selected_file) = create_signal(None::<web_sys::File>);
    let file_input_ref = create_node_ref::<leptos::html::Input>();

    let is_admin = move || has_admin_operator_privileges();

    // Template Specific Fields
    let (template_file, set_template_file) = create_signal("".to_string());

    let navigate = use_navigate();

    create_effect(move |_| {
        let cards = visible_template_cards.get();
        let current_template = selected_template.get();
        let current_version = selected_version.get();
        let manual_local_template =
            current_template == "General" || current_template == "CWL Workflow";

        if manual_local_template {
            return;
        }

        if let Some(card) = cards.iter().find(|card| card.name == current_template) {
            if !card.versions.contains(&current_version) {
                set_selected_version.set(card.first_version.clone());
            }
            return;
        }

        if let Some(first) = cards.first() {
            set_selected_template.set(first.name.clone());
            set_selected_version.set(first.first_version.clone());
        } else {
            set_selected_template.set("General".to_string());
            set_selected_version.set("".to_string());
        }
    });

    // Effect to update binary/args based on template
    create_effect(move |prev: Option<(String, String)>| {
        let tmpl_name = selected_template.get();
        let version_name = selected_version.get();

        if prev.as_ref() == Some(&(tmpl_name.clone(), version_name.clone())) {
            return (tmpl_name, version_name);
        }

        // Reset template-specific fields when switching cards
        set_template_file.set("".to_string());
        set_selected_file.set(None);

        if tmpl_name == "General" {
            set_binary.set("".to_string());
            set_args.set("".to_string());
            set_raw_command_template.set("".to_string());
            set_image_uri.set(None);
            set_cwl_content.set("".to_string());
            return (tmpl_name, version_name);
        }

        if tmpl_name == "CWL Workflow" {
            set_binary.set("".to_string());
            set_args.set("".to_string());
            set_image_uri.set(None);
            return (tmpl_name, version_name);
        }

        let templates = visible_templates.get();
        let matching_container = templates
            .iter()
            .find(|c| {
                let group = template_group_name(c);
                group == tmpl_name && c.manifest.solver_identity.version == version_name
            })
            .or_else(|| {
                templates
                    .iter()
                    .find(|c| template_group_name(c) == tmpl_name)
            });

        if let Some(container) = matching_container {
            set_binary.set(container.manifest.execution.entrypoint.clone());
            let command_template = container.manifest.execution.command_template.clone();
            set_raw_command_template.set(command_template.clone());
            set_image_uri.set(Some(format!("veloce://{}", container.id)));
            set_cwd.set("/scratch".to_string());

            let mut initial_params = std::collections::HashMap::new();
            for (k, v) in &container.manifest.parameter_mapping {
                if let Some(def) = &v.default {
                    initial_params.insert(k.clone(), def.clone());
                }
            }
            set_param_values.set(initial_params);

            if version_name != container.manifest.solver_identity.version {
                set_selected_version
                    .set_untracked(container.manifest.solver_identity.version.clone());
            }
        }
        (tmpl_name, version_name)
    });

    create_effect(move |_| {
        let tmpl_name = selected_template.get();
        if tmpl_name != "General" && tmpl_name != "CWL Workflow" {
            let mut resolved = raw_command_template.get();
            resolved = resolved.replace("{{executable}}", &binary.get());
            resolved = resolved.replace("{{entrypoint}}", &binary.get());

            let input_val = template_file.get();
            let input_str = if input_val.is_empty() {
                "none".to_string()
            } else {
                input_val
            };
            resolved = resolved.replace("{{input_file}}", &input_str);
            resolved = resolved.replace("{{custom_args}}", "");

            resolved = resolved.replace("{{memory}}", &memory.get().to_string());
            resolved = resolved.replace("{{cpus}}", &cores.get().to_string());

            let params = param_values.get();
            let templates = visible_templates.get();
            let matching_container = templates
                .iter()
                .find(|c| {
                    let group = template_group_name(c);
                    group == tmpl_name
                        && c.manifest.solver_identity.version == selected_version.get()
                })
                .or_else(|| {
                    templates
                        .iter()
                        .find(|c| template_group_name(c) == tmpl_name)
                });

            if let Some(container) = matching_container {
                for (k, param_def) in &container.manifest.parameter_mapping {
                    let val = params
                        .get(k)
                        .cloned()
                        .unwrap_or_else(|| param_def.default.clone().unwrap_or_default());
                    let replacement = if param_def.param_type.as_deref() == Some("boolean") {
                        if val == "true" {
                            param_def.flag.clone().unwrap_or_default()
                        } else {
                            "".to_string()
                        }
                    } else {
                        if let Some(flag) = &param_def.flag {
                            if val.is_empty() || val == "none" {
                                "".to_string()
                            } else {
                                format!("{} {}", flag, val)
                            }
                        } else {
                            if val == "none" {
                                "".to_string()
                            } else {
                                val
                            }
                        }
                    };
                    resolved = resolved.replace(&format!("{{{{{}}}}}", k), &replacement);
                }
            }

            set_args.set(resolved);
        }
    });

    let submit_action = create_action(move |_: &()| {
        let navigate = navigate.clone();

        async move {
            let tmpl_name = selected_template.get_untracked();
            let mut final_binary = binary.get_untracked();

            let mut final_args = shell_words::split(&args.get_untracked()).unwrap_or_else(|_| {
                args.get_untracked()
                    .split_whitespace()
                    .map(String::from)
                    .collect()
            });

            // If the resolved command starts with the binary name, strip it to prevent doubling
            if !final_args.is_empty() && !final_binary.is_empty() && final_args[0] == final_binary {
                final_args.remove(0);
            }

            let mut final_cwd = cwd.get_untracked();
            if final_cwd.is_empty() {
                final_cwd = "/scratch".to_string();
            }

            if tmpl_name == "CWL Workflow" {
                let payload = serde_json::json!({
                    "cwl_content": cwl_content.get_untracked(),
                    "user_id": user_id.get_untracked(),
                    "working_directory": final_cwd,
                });
                let res = api_post("/api/v1/jobs/cwl")
                    .header("Content-Type", "application/json")
                    .body(payload.to_string())
                    .unwrap()
                    .send()
                    .await;
                if let Ok(resp) = res {
                    if resp.ok() {
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
                                "Job submitted, but response did not include a job ID (HTTP {}).",
                                status
                            ));
                        }
                    } else {
                        gloo_dialogs::alert(&format!("Error: {}", resp.status()));
                    }
                } else {
                    gloo_dialogs::alert("Network Error");
                }
                return;
            }

            let is_container_job = image_uri.get_untracked().is_some();
            let mut job_inputs: Vec<serde_json::Value> = Vec::new();

            // Handle File Upload
            if let Some(file) = selected_file.get_untracked() {
                let _conf = config.get().flatten().expect("Config not loaded");
                let form_data = web_sys::FormData::new().unwrap();
                // We use append_with_blob because web_sys::File implements Blob
                let _ = form_data.append_with_blob("file", &file);

                let upload_url = "/api/v1/files".to_string();

                let mut upload_req = Request::post(&upload_url);
                if let Some(key) = get_session_storage_item("veloce_controller_api_key") {
                    upload_req = upload_req.header("X-API-KEY", &key);
                }
                match upload_req.body(form_data).unwrap().send().await {
                    Ok(resp) => {
                        if resp.ok() {
                            let json_res: serde_json::Value =
                                resp.json().await.unwrap_or(serde_json::Value::Null);
                            // Expecting UUID string or {"file_id": "UUID"}
                            let file_id = if let Some(s) = json_res.as_str() {
                                s.to_string()
                            } else if let Some(s) = json_res.get("file_id").and_then(|v| v.as_str())
                            {
                                s.to_string()
                            } else {
                                "".to_string()
                            };

                            if !file_id.is_empty() {
                                let filename = file.name();
                                let is_archive = filename.ends_with(".tar.gz")
                                    || filename.ends_with(".tgz")
                                    || filename.ends_with(".tar")
                                    || filename.ends_with(".zip");

                                if is_container_job {
                                    // Container images may not include curl.
                                    // Stage inputs on the worker before apptainer exec instead.
                                    job_inputs.push(serde_json::json!({
                                        "file_id": file_id,
                                        "original_name": filename,
                                        "is_executable": false,
                                        "is_archive": is_archive,
                                    }));
                                } else {
                                    let workdir = "/tmp/veloce_work_$RANDOM".to_string();
                                    final_cwd = "/tmp".to_string();

                                    let script = format!(
                                        r#"#!/bin/bash
set -e
WORKDIR="{}"
mkdir -p "$WORKDIR"
cd "$WORKDIR"

# Download
echo "Downloading input file..."
curl -k -v -f -H "X-API-KEY: $VELOCE_FILESERVER_KEY" -o "downloaded_file" "$VELOCE_FILESERVER_URL/api/v1/files/{}"

# Restore Filename
FILENAME="{}"
mv "downloaded_file" "$FILENAME"

# Unpack if archive
if [[ "$FILENAME" == *.tar.gz || "$FILENAME" == *.tgz || "$FILENAME" == *.tar ]]; then
    echo "Unpacking archive..."
    tar -xf "$FILENAME"
elif [[ "$FILENAME" == *.zip ]]; then
    echo "Unzipping archive..."
    unzip "$FILENAME"
fi

# Run Original Command
echo "Running payload..."
set +e
{} {}
EXIT_CODE=$?
set -e

# Upload Results
echo "Creating result archive..."
RESULT_ARCHIVE="results_$RANDOM.tar.gz"
tar -czf "$RESULT_ARCHIVE" .

echo "Uploading results..."
UPLOAD_RESPONSE=$(curl -k -v -f -H "X-API-KEY: $VELOCE_FILESERVER_KEY" -F "file=@$RESULT_ARCHIVE" "$VELOCE_FILESERVER_URL/api/v1/files")

# Extract File ID
RESULT_ID=$(echo "$UPLOAD_RESPONSE" | grep -oE '[0-9a-f]{{8}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{12}}')

if [ ! -z "$RESULT_ID" ]; then
    echo "[VELOCE_RESULT_ID: $RESULT_ID]"
else
    echo "Failed to upload results or parse ID."
fi

# Cleanup
cd ..
rm -rf "$WORKDIR"

exit $EXIT_CODE
"#,
                                        workdir,
                                        file_id,
                                        filename,
                                        &final_binary,
                                        shell_words::join(&final_args)
                                    );

                                    final_binary = "/bin/bash".to_string();
                                    final_args = vec!["-c".to_string(), script];
                                }
                            } else {
                                gloo_dialogs::alert("Upload failed: No File ID returned");
                                return;
                            }
                        } else {
                            gloo_dialogs::alert(&format!("Upload failed: {}", resp.status()));
                            return;
                        }
                    }
                    Err(_) => {
                        gloo_dialogs::alert("Upload network error");
                        return;
                    }
                }
            }

            let array_indices_parsed = if array_indices.get_untracked().is_empty() {
                None
            } else {
                // Parse "1-5" or "1,2,5"
                let mut indices = Vec::new();
                for part in array_indices.get_untracked().split(',') {
                    if part.contains('-') {
                        let bounds: Vec<&str> = part.split('-').collect();
                        if bounds.len() == 2 {
                            if let (Ok(start), Ok(end)) = (
                                bounds[0].trim().parse::<u32>(),
                                bounds[1].trim().parse::<u32>(),
                            ) {
                                for i in start..=end {
                                    indices.push(i);
                                }
                            }
                        }
                    } else {
                        if let Ok(val) = part.trim().parse::<u32>() {
                            indices.push(val);
                        }
                    }
                }
                if indices.is_empty() {
                    None
                } else {
                    Some(indices)
                }
            };

            let submitted_job_name = {
                let value = job_name.get_untracked().trim().to_string();
                if value.is_empty() {
                    None
                } else {
                    Some(value)
                }
            };
            let submitted_job_comment = {
                let value = job_comment.get_untracked().trim().to_string();
                if value.is_empty() {
                    None
                } else {
                    Some(value)
                }
            };

            let payload = serde_json::json!({
                "job_name": submitted_job_name,
                "job_comment": submitted_job_comment,
                "binary": final_binary,
                "args": final_args,
                "req_nodes": nodes.get_untracked(),
                "req_cores": cores.get_untracked(),
                "req_memory": memory.get_untracked(),
                "walltime": walltime.get_untracked(),
                "priority": priority.get_untracked(),
                "user_id": user_id.get_untracked(),
                "working_directory": final_cwd,
                "array_indices": array_indices_parsed,
                "image_uri": image_uri.get_untracked(),
                "inputs": job_inputs,
            });

            let res = api_post("/api/v1/jobs")
                .header("Content-Type", "application/json")
                .body(payload.to_string())
                .unwrap()
                .send()
                .await;

            if let Ok(resp) = res {
                if resp.ok() {
                    let status = resp.status();
                    let body = resp
                        .json::<serde_json::Value>()
                        .await
                        .unwrap_or(serde_json::Value::Null);
                    let new_id = body
                        .get("federated_id")
                        .and_then(|id| id.as_str())
                        .map(str::to_string)
                        .or_else(|| {
                            body.get("job_id")
                                .or_else(|| body.get("base_job_id"))
                                .and_then(|id| id.as_u64())
                                .map(|id| id.to_string())
                        });
                    if let Some(new_id) = new_id {
                        navigate(&format!("/jobs/{}", new_id), Default::default());
                    } else {
                        gloo_dialogs::alert(&format!(
                            "Job submitted, but response did not include a job ID (HTTP {}).",
                            status
                        ));
                    }
                } else {
                    gloo_dialogs::alert(&format!("Error: {}", resp.status()));
                }
            } else {
                gloo_dialogs::alert("Network Error");
            }
        }
    });

    view! {
        <h2>"Submit Job"</h2>

        <div class="template-cards">
            <div
                class=move || if selected_template.get() == "General" { "template-card active" } else { "template-card" }
                on:click=move |_| set_selected_template.set("General".to_string())
            >
                <i class="ph ph-terminal-window"></i>
                <div class="title">"General"</div>
                <div class="desc">"Standard shell execution"</div>
            </div>
            <div
                class=move || if selected_template.get() == "CWL Workflow" { "template-card active" } else { "template-card" }
                on:click=move |_| set_selected_template.set("CWL Workflow".to_string())
            >
                <i class="ph ph-graph"></i>
                <div class="title">"CWL Workflow"</div>
                <div class="desc">"Submit a DAG using Common Workflow Language"</div>
            </div>
            {move || {
                visible_template_cards.get().into_iter().map(|card| {
                    let group_name = card.name.clone();
                    let name_for_active = card.name.clone();
                    let is_active = move || selected_template.get() == name_for_active;
                    let name_for_click = card.name.clone();
                    let icon = "ph-cube".to_string();
                    let version_desc = if card.versions.len() == 1 {
                        format!("{} Version {}", group_name, card.versions[0])
                    } else {
                        format!("{} ({} versions)", group_name, card.versions.len())
                    };
                    let desc = card.availability
                        .as_ref()
                        .map(|availability| format!("{} · {}", version_desc, availability))
                        .unwrap_or(version_desc);
                    let first_version = card.first_version.clone();

                    view! {
                         <div
                            class=move || if is_active() { "template-card active" } else { "template-card" }
                            on:click=move |_| {
                                set_selected_template.set(name_for_click.clone());
                                set_selected_version.set(first_version.clone());
                            }
                         >
                            <i class=format!("ph {}", icon)></i>
                            <div class="title">{group_name}</div>
                            <div class="desc">{desc}</div>
                         </div>
                    }
                }).collect_view()
            }}
        </div>

        <Show when=move || selected_template.get() != "General" && selected_template.get() != "CWL Workflow" fallback=|| ().into_view()>
            <div class="template-specific-fields glass-panel" style="margin-bottom: 2rem;">
                <h3 style="margin-top: 0; display: flex; align-items: center; gap: 8px;"><i class="ph ph-sliders"></i> "Template Settings"</h3>
                <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 1.5rem;">
                    <div class="form-group" style="margin-bottom: 0;">
                        <label>"Software Version"</label>
                        <select
                            prop:value=selected_version
                            on:change=move |ev| set_selected_version.set(event_target_value(&ev))
                        >
                            {move || {
                                let tmpl = selected_template.get();
                                let mut versions = visible_templates.get()
                                    .into_iter()
                                    .filter(move |item| template_group_name(item) == tmpl)
                                    .map(|item| template_version(&item))
                                    .fold(Vec::<String>::new(), |mut versions, version| {
                                        if !versions.contains(&version) {
                                            versions.push(version);
                                        }
                                        versions
                                    });
                                versions.sort();
                                versions.into_iter()
                                    .map(|v| {
                                        view! { <option value=v.clone()>{v}</option> }
                                    }).collect_view()
                            }}
                        </select>
                    </div>
                    <div class="form-group" style="margin-bottom: 0;">
                        <label>"Target Case / Input File"</label>
                        <input type="text" on:input=move |ev| set_template_file.set(event_target_value(&ev)) prop:value=template_file/>
                    </div>
                    {move || {
                        let tmpl_name = selected_template.get();
                        let v_name = selected_version.get();
                        let templates = visible_templates.get();
                        let matching_container = templates.iter().find(|c| {
                            template_group_name(c) == tmpl_name && c.manifest.solver_identity.version == v_name
                        }).or_else(|| {
                            templates.iter().find(|c| template_group_name(c) == tmpl_name)
                        });

                        if let Some(container) = matching_container {
                            let mut params_views = Vec::new();
                            for (k, param_def) in &container.manifest.parameter_mapping {
                                let key_for_read = k.clone();
                                let key_for_write = k.clone();

                                    let current_val = move || param_values.get().get(&key_for_read).cloned().unwrap_or_default();

                                    let update_val = move |new_val: String| {
                                        set_param_values.update(|m| { m.insert(key_for_write.clone(), new_val); });
                                    };

                                    let label_text = k.replace("_", " ").to_uppercase();

                                    let input_view = if let Some(options) = &param_def.options {
                                        view! {
                                            <div class="form-group" style="margin-bottom: 0;">
                                                <label>{label_text}</label>
                                                <select
                                                    prop:value=current_val
                                                    on:change=move |ev| update_val(event_target_value(&ev))
                                                >
                                                    {options.iter().map(|opt| {
                                                        let o = opt.clone();
                                                        view! { <option value=o.clone()>{o}</option> }
                                                    }).collect_view()}
                                                </select>
                                            </div>
                                        }.into_view()
                                    } else if param_def.param_type.as_deref() == Some("boolean") {
                                        view! {
                                            <div class="form-group" style="margin-bottom: 0; display: flex; align-items: center; gap: 8px; margin-top: 1.5rem;">
                                                <input
                                                    type="checkbox"
                                                    style="width: auto; margin: 0;"
                                                    prop:checked=move || current_val() == "true"
                                                    on:change=move |ev| update_val(if event_target_checked(&ev) { "true".to_string() } else { "false".to_string() })
                                                />
                                                <label style="margin: 0;">{label_text}</label>
                                            </div>
                                        }.into_view()
                                    } else if param_def.param_type.as_deref() == Some("integer") {
                                        view! {
                                            <div class="form-group" style="margin-bottom: 0;">
                                                <label>{label_text}</label>
                                                <input
                                                    type="number"
                                                    prop:value=current_val
                                                    on:input=move |ev| update_val(event_target_value(&ev))
                                                />
                                            </div>
                                        }.into_view()
                                    } else {
                                        view! {
                                            <div class="form-group" style="margin-bottom: 0;">
                                                <label>{label_text}</label>
                                                <input
                                                    type="text"
                                                    prop:value=current_val
                                                    on:input=move |ev| update_val(event_target_value(&ev))
                                                />
                                            </div>
                                        }.into_view()
                                    };

                                params_views.push(input_view);
                            }
                            return params_views.into_view();
                        }
                        ().into_view()
                    }}
                </div>

                <div class="command-preview">
                    <div class="preview-label">"Command Preview"</div>
                    <code>
                        {move || {
                            let tmpl = selected_template.get();
                            if tmpl == "General" || tmpl == "CWL Workflow" {
                                format!("{} {}", binary.get(), args.get())
                            } else {
                                args.get()
                            }
                        }}
                    </code>
                </div>

                {move || {
                    let tmpl_name = selected_template.get();
                    let templates = visible_templates.get();
                    if let Some(container) = templates.iter().find(|c| template_group_name(c) == tmpl_name) {
                        if container.manifest.solver_identity.product.to_lowercase().contains("fluent") {
                            return view! {
                                <div style="margin-top: 1rem; font-size: 0.8rem; color: var(--warning); display: flex; align-items: center; gap: 6px;">
                                    <i class="ph ph-info"></i>
                                    "Commercial license tokens may be checked by this container."
                                </div>
                            }.into_view();
                        }
                    }
                    ().into_view()
                }}
            </div>
        </Show>

        <div class="submit-grid">
            <div class="submit-section glass-panel">
                <h3><i class="ph ph-tag"></i> "Job Metadata"</h3>
                <div class="form-group">
                    <label>"Job Name"</label>
                    <input
                        type="text"
                        on:input=move |ev| set_job_name.set(event_target_value(&ev))
                        prop:value=job_name
                        placeholder="fluent-baseline"
                    />
                </div>
                <div class="form-group">
                    <label>"Comment"</label>
                    <textarea
                        on:input=move |ev| set_job_comment.set(event_target_value(&ev))
                        prop:value=job_comment
                        placeholder="mesh v3, inlet sweep"
                        style="min-height: 80px;"
                    ></textarea>
                </div>
            </div>

            <div class="submit-section glass-panel">
                <h3><i class="ph ph-rocket"></i> "Payload"</h3>
                <Show when=move || selected_template.get() == "CWL Workflow" fallback=move || view! {
                    <div>
                        <div class="form-group">
                            <label>"Binary Path"</label>
                            <input type="text" on:input=move |ev| set_binary.set(event_target_value(&ev)) prop:value=binary placeholder="/bin/echo" prop:disabled=move || selected_template.get() != "General"/>
                        </div>
                        <div class="form-group">
                            <label>"Arguments (space separated)"</label>
                            <input type="text" on:input=move |ev| set_args.set(event_target_value(&ev)) prop:value=args placeholder="Hello World" prop:disabled=move || selected_template.get() != "General"/>
                        </div>
                        <div class="form-group">
                            <label>"Input File (Optional)"</label>
                            <input
                                type="file"
                                node_ref=file_input_ref
                                on:change=move |_| {
                                    if let Some(input) = file_input_ref.get() {
                                        if let Some(files) = input.files() {
                                            if let Some(file) = files.get(0) {
                                                set_template_file.set(file.name());
                                                set_selected_file.set(Some(file));
                                            }
                                        }
                                    }
                                }
                            />
                        </div>
                    </div>
                }>
                    <div class="form-group">
                        <label>"CWL Workflow Definition"</label>
                        <textarea
                            on:input=move |ev| set_cwl_content.set(event_target_value(&ev))
                            prop:value=cwl_content
                            placeholder="cwlVersion: v1.0\nclass: Workflow\n..."
                            style="min-height: 200px; font-family: monospace; white-space: pre;"
                        ></textarea>
                    </div>
                </Show>
            </div>

            <div class="submit-section glass-panel">
                <h3><i class="ph ph-cpu"></i> "Resources"</h3>
                <div class="form-group">
                    <label>"Nodes"</label>
                    <input type="number" on:input=move |ev| set_nodes.set(event_target_value(&ev).parse().unwrap_or(1)) prop:value=nodes/>
                </div>
                <div class="form-group">
                    <label>"Cores (per node)"</label>
                    <input type="number" on:input=move |ev| set_cores.set(event_target_value(&ev).parse().unwrap_or(1)) prop:value=cores/>
                </div>
                <div class="resource-summary">
                    <span>"Total Slots: " {move || nodes.get() * cores.get() as usize}</span>
                </div>
                <div class="form-group">
                    <label>"Memory (MB per node)"</label>
                    <input type="number" on:input=move |ev| set_memory.set(event_target_value(&ev).parse().unwrap_or(1024)) prop:value=memory/>
                </div>
                 <div class="form-group">
                    <label>"Walltime (seconds)"</label>
                    <input type="number" on:input=move |ev| set_walltime.set(event_target_value(&ev).parse().unwrap_or(3600)) prop:value=walltime/>
                </div>
            </div>

            <div class="submit-section glass-panel">
                <h3><i class="ph ph-sliders"></i> "Environment"</h3>
                <div class="form-group">
                    <label>"Working Directory"</label>
                    <input type="text" on:input=move |ev| set_cwd.set(event_target_value(&ev)) prop:value=cwd />
                </div>
                <div class="form-group">
                    <label>"User ID"</label>
                    <input type="text" on:input=move |ev| set_user_id.set(event_target_value(&ev)) prop:value=user_id readonly=move || !is_admin() disabled=move || !is_admin()/>
                </div>
                <div class="form-group">
                    <label>"Priority"</label>
                    <input type="number" on:input=move |ev| set_priority.set(event_target_value(&ev).parse().unwrap_or(0)) prop:value=priority/>
                </div>
                <div class="form-group">
                    <label>"Job Array Indices (e.g. 1-10)"</label>
                    <input type="text" on:input=move |ev| set_array_indices.set(event_target_value(&ev)) prop:value=array_indices placeholder="1-5"/>
                </div>
            </div>
        </div>

        <button on:click=move |_| submit_action.dispatch(())>
            <i class="ph ph-paper-plane-right"></i> "Submit Job"
        </button>
    }
}
