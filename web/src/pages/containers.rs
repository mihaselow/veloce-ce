use crate::api_client::{api_delete, api_get, api_post};
use crate::api_error::{clear_api_error, describe_http_failure, describe_request_failure};
use crate::app::{set_interval_with_handle, WebConfig};
use crate::login::has_admin_privileges;
use leptos::*;
use veloce_common::apptainer::{ContainerAsset, SolverManifest};

#[component]
pub(crate) fn ContainersList() -> impl IntoView {
    let config =
        use_context::<Resource<bool, Option<WebConfig>>>().expect("WebConfig context missing");
    let set_api_error =
        use_context::<WriteSignal<Option<String>>>().expect("API error context missing");
    let (containers, set_containers) = create_signal(Vec::<ContainerAsset>::new());

    let (search_query, set_search_query) = create_signal("".to_string());
    let (current_page, set_current_page) = create_signal(0usize);
    let (show_register, set_show_register) = create_signal(false);
    let (register_name, set_register_name) = create_signal(String::new());
    let (register_uri, set_register_uri) = create_signal(String::new());
    let (register_manifest, set_register_manifest) = create_signal(String::new());
    let (register_message, set_register_message) = create_signal(None::<String>);
    let page_size = 15;
    let can_manage = move || has_admin_privileges();

    let is_alive = std::rc::Rc::new(std::cell::Cell::new(true));
    {
        let is_alive = is_alive.clone();
        on_cleanup(move || {
            is_alive.set(false);
        });
    }

    let fetch_containers_fn = std::rc::Rc::new({
        let is_alive = is_alive.clone();
        move || {
            let conf = config.get().flatten();
            if conf.is_some() {
                let is_alive = is_alive.clone();
                spawn_local(async move {
                    match api_get("/api/v1/containers").send().await {
                        Ok(resp) if resp.ok() => {
                            if let Ok(data) = resp.json::<Vec<ContainerAsset>>().await {
                                if !is_alive.get() {
                                    return;
                                }
                                clear_api_error(set_api_error);
                                set_containers.set(data);
                            }
                        }
                        Ok(resp) => {
                            if !is_alive.get() {
                                return;
                            }
                            set_api_error.set(Some(describe_http_failure(
                                "Failed to load containers",
                                &resp,
                            )));
                        }
                        Err(err) => {
                            if !is_alive.get() {
                                return;
                            }
                            set_api_error.set(Some(describe_request_failure(
                                "Failed to load containers",
                                &err,
                            )));
                        }
                    }
                });
            }
        }
    });
    let fetch_containers = store_value(fetch_containers_fn);

    let fetch_containers_clone = fetch_containers;
    create_effect(move |_| {
        fetch_containers_clone.with_value(|fetch| fetch());
        let fetch_containers_for_interval = fetch_containers_clone;
        let handle = set_interval_with_handle(
            move || {
                fetch_containers_for_interval.with_value(|fetch| fetch());
            },
            5000,
        );
        on_cleanup(move || {
            if let Ok(id) = handle {
                if let Some(win) = web_sys::window() {
                    win.clear_interval_with_handle(id);
                }
            }
        });
    });

    let filtered_containers = move || {
        let q = search_query.get().to_lowercase();
        let all = containers.get();
        all.into_iter()
            .filter(|c| {
                if q.is_empty() {
                    return true;
                }
                c.name.to_lowercase().contains(&q)
                    || c.manifest
                        .solver_identity
                        .vendor
                        .to_lowercase()
                        .contains(&q)
                    || c.manifest
                        .solver_identity
                        .product
                        .to_lowercase()
                        .contains(&q)
                    || c.manifest
                        .solver_identity
                        .version
                        .to_lowercase()
                        .contains(&q)
                    || c.manifest
                        .solver_identity
                        .capabilities
                        .join(" ")
                        .to_lowercase()
                        .contains(&q)
                    || c.image_uri.to_lowercase().contains(&q)
            })
            .collect::<Vec<_>>()
    };

    let total_filtered = move || filtered_containers().len();

    let paginated_containers = move || {
        let list = filtered_containers();
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
        <div>
            <div style="display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 1rem; margin-bottom: 1rem;">
                <h2>"Container Registry"</h2>
                <div style="display: flex; gap: 0.75rem; align-items: center; flex-wrap: wrap;">
                    {move || if can_manage() {
                        view! {
                            <button
                                class="btn-secondary"
                                type="button"
                                on:click=move |_| set_show_register.update(|v| *v = !*v)
                            >
                                {move || if show_register.get() { "Hide Register" } else { "Register Container" }}
                            </button>
                        }.into_view()
                    } else {
                        ().into_view()
                    }}
                    <input
                        type="text"
                        placeholder="Search containers..."
                        class="node-search-input"
                        style="width: 300px;"
                        prop:value=search_query
                        on:input=move |ev| {
                            set_search_query.set(event_target_value(&ev));
                            set_current_page.set(0);
                        }
                    />
                </div>
            </div>

            <Show when=move || can_manage() && show_register.get()>
                <div class="glass-panel" style="margin-bottom: 1rem;">
                    <h3 style="margin-top: 0;">"Register Apptainer Container"</h3>
                    <div class="form-group">
                        <label>"Name"</label>
                        <input
                            type="text"
                            prop:value=register_name
                            on:input=move |ev| set_register_name.set(event_target_value(&ev))
                            placeholder="jupyter-lab"
                        />
                    </div>
                    <div class="form-group">
                        <label>"Image URI"</label>
                        <input
                            type="text"
                            prop:value=register_uri
                            on:input=move |ev| set_register_uri.set(event_target_value(&ev))
                            placeholder="s3://veloce-system-containers/jupyter-lab.sif"
                        />
                    </div>
                    <div class="form-group">
                        <label>"Solver manifest (JSON)"</label>
                        <textarea
                            rows="12"
                            prop:value=register_manifest
                            on:input=move |ev| set_register_manifest.set(event_target_value(&ev))
                            placeholder=r#"{"manifest_version":"1.0", ...}"#
                            style="width: 100%; font-family: monospace;"
                        ></textarea>
                    </div>
                    {move || register_message.get().map(|msg| view! {
                        <p style="color: var(--text-muted); margin: 0 0 0.75rem 0;">{msg}</p>
                    })}
                    <button
                        class="btn-primary"
                        type="button"
                        on:click=move |_| {
                            let name = register_name.get().trim().to_string();
                            let image_uri = register_uri.get().trim().to_string();
                            let manifest_text = register_manifest.get();
                            if name.is_empty() || image_uri.is_empty() {
                                set_register_message.set(Some(
                                    "Name and image URI are required.".to_string(),
                                ));
                                return;
                            }
                            let manifest: SolverManifest = match serde_json::from_str(&manifest_text) {
                                Ok(m) => m,
                                Err(err) => {
                                    set_register_message.set(Some(format!(
                                        "Invalid manifest JSON: {err}"
                                    )));
                                    return;
                                }
                            };
                            let fetch_containers = fetch_containers;
                            spawn_local(async move {
                                set_register_message
                                    .set(Some("Registering container...".to_string()));
                                let payload = serde_json::json!({
                                    "name": name,
                                    "image_uri": image_uri,
                                    "manifest": manifest,
                                });
                                match api_post("/api/v1/containers/register")
                                    .header("Content-Type", "application/json")
                                    .json(&payload)
                                {
                                    Ok(req) => match req.send().await {
                                        Ok(resp) if resp.status() == 201 => {
                                            set_register_message
                                                .set(Some("Container registered.".to_string()));
                                            set_register_name.set(String::new());
                                            set_register_uri.set(String::new());
                                            set_register_manifest.set(String::new());
                                            fetch_containers.with_value(|fetch| fetch());
                                        }
                                        Ok(resp) => {
                                            let status = resp.status();
                                            let body = resp.text().await.unwrap_or_default();
                                            set_register_message.set(Some(format!(
                                                "Registration failed: HTTP {} — {}",
                                                status, body
                                            )));
                                        }
                                        Err(err) => {
                                            set_register_message.set(Some(format!(
                                                "Registration failed: {err}"
                                            )));
                                        }
                                    },
                                    Err(err) => {
                                        set_register_message.set(Some(format!(
                                            "Registration failed: {err}"
                                        )));
                                    }
                                }
                            });
                        }
                    >
                        "Register"
                    </button>
                </div>
            </Show>

            <table>
                <thead>
                    <tr>
                        <th>"Name"</th>
                        <th>"Vendor"</th>
                        <th>"Product"</th>
                        <th>"Version"</th>
                        <th>"Capabilities"</th>
                        <th>"S3 Key"</th>
                        {move || if can_manage() {
                            view! { <th>"Actions"</th> }.into_view()
                        } else {
                            ().into_view()
                        }}
                    </tr>
                </thead>
                <tbody>
                    {move || {
                        let p_containers = paginated_containers();
                        if p_containers.is_empty() {
                            view! {
                                <tr>
                                    <td colspan="7" style="text-align: center; padding: 2rem; color: var(--text-muted);">
                                        "No containers match the search query."
                                    </td>
                                </tr>
                            }.into_view()
                        } else {
                            p_containers.into_iter().map(|c| {
                                let vendor = c.manifest.solver_identity.vendor.clone();
                                let product = c.manifest.solver_identity.product.clone();
                                let version = c.manifest.solver_identity.version.clone();
                                let capabilities = c.manifest.solver_identity.capabilities.join(", ");
                                let image_uri = c.image_uri.clone();
                                let name_disp = c.name.clone();
                                let name_for_delete = c.name.clone();

                                view! {
                                    <tr>
                                        <td><strong>{name_disp}</strong></td>
                                        <td>{vendor}</td>
                                        <td>{product}</td>
                                        <td>{version}</td>
                                        <td>{capabilities}</td>
                                        <td style="font-family: monospace; font-size: 0.85rem;">{image_uri}</td>
                                        {move || if can_manage() {
                                            let name_for_delete = name_for_delete.clone();
                                            let fetch_containers = fetch_containers;
                                            view! {
                                                <td>
                                                    <button
                                                        class="danger"
                                                        type="button"
                                                        on:click=move |_| {
                                                            let name = name_for_delete.clone();
                                                            let fetch_containers = fetch_containers;
                                                            spawn_local(async move {
                                                                let url = format!("/api/v1/containers/{}", name);
                                                                match api_delete(&url).send().await {
                                                                    Ok(resp) if resp.ok() => {
                                                                        fetch_containers.with_value(|fetch| fetch());
                                                                    }
                                                                    Ok(resp) => {
                                                                        set_api_error.set(Some(describe_http_failure(
                                                                            "Failed to delete container",
                                                                            &resp,
                                                                        )));
                                                                    }
                                                                    Err(err) => {
                                                                        set_api_error.set(Some(describe_request_failure(
                                                                            "Failed to delete container",
                                                                            &err,
                                                                        )));
                                                                    }
                                                                }
                                                            });
                                                        }
                                                    >
                                                        "Delete"
                                                    </button>
                                                </td>
                                            }.into_view()
                                        } else {
                                            ().into_view()
                                        }}
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
        </div>
    }
}
