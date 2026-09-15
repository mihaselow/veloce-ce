use crate::admin;
use crate::api_client::{api_get, fetch_ws_ticket, fileserver_get};
use crate::api_error::{
    clear_api_error, describe_http_failure, describe_request_failure, ApiErrorBanner,
};
use crate::consoles::{FullscreenTerminal, FullscreenVnc};
use crate::login::{
    clear_session_storage, get_session_storage_item, has_admin_operator_privileges,
    set_session_storage_item, AdminPrivilegeRequired, LoginGate,
};
use crate::notifications::{notify_job_status_changes, request_notification_permission};
use crate::pages::{ContainersList, Dashboard, JobDetails, JobsList, NodesList, SubmitJob};
use crate::statistics;
use crate::telemetry;
use futures::StreamExt;
use gloo_net::http::Request;
use gloo_net::websocket::futures::WebSocket;
use leptos::*;
use leptos_router::*;
use serde::{Deserialize, Serialize};
use veloce_common::apptainer::ContainerAsset;
use veloce_common::{JobInfo, WorkerInfo};
use wasm_bindgen::JsCast;

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
#[serde(default)]
pub struct WebConfig {
    pub controller_api_key: String,
    pub fileserver_url: String,
    pub fileserver_api_key: String,
    pub oidc_enabled: bool,
}
#[component]
pub(crate) fn App() -> impl IntoView {
    view! {
        <Router>
            <AppContent />
        </Router>
    }
}
#[component]
fn AppContent() -> impl IntoView {
    let (logged_in, set_logged_in) = create_signal(
        get_session_storage_item("veloce_controller_api_key").is_some()
            || get_session_storage_item("veloce_oidc_logged_in").is_some(),
    );
    let (oidc_enabled, set_oidc_enabled) = create_signal(false);

    // Shared State for Dashboards
    let (jobs, set_jobs) = create_signal(Vec::<JobInfo>::new());
    let (nodes, set_nodes) = create_signal(Vec::<WorkerInfo>::new());
    let (controller_online, set_controller_online) = create_signal(false);
    let (fileserver_online, set_fileserver_online) = create_signal(false);
    let (api_error, set_api_error) = create_signal(None::<String>);
    provide_context(set_api_error);

    let config = create_resource(
        move || logged_in.get(),
        move |_| async move {
            match Request::get("/web-config.json")
                .credentials(web_sys::RequestCredentials::Include)
                .send()
                .await
            {
                Ok(resp) => {
                    if !resp.ok() {
                        set_api_error.set(Some(describe_http_failure(
                            "Failed to load web configuration",
                            &resp,
                        )));
                        return None;
                    }
                    match resp.json::<WebConfig>().await {
                        Ok(mut web_conf) => {
                            clear_api_error(set_api_error);
                            // In legacy mode, populate API keys from sessionStorage
                            if !web_conf.oidc_enabled {
                                web_conf.controller_api_key =
                                    get_session_storage_item("veloce_controller_api_key")
                                        .unwrap_or_default();
                            }
                            Some(web_conf)
                        }
                        Err(err) => {
                            set_api_error
                                .set(Some(format!("Failed to parse web configuration: {err}")));
                            None
                        }
                    }
                }
                Err(err) => {
                    set_api_error.set(Some(describe_request_failure(
                        "Failed to load web configuration",
                        &err,
                    )));
                    None
                }
            }
        },
    );
    provide_context(config);

    // Bootstrap OIDC session: check if /auth/session returns a valid session
    create_effect(move |_| {
        if let Some(Some(conf)) = config.get() {
            set_oidc_enabled.set(conf.oidc_enabled);
            if conf.oidc_enabled {
                spawn_local(async move {
                    if let Ok(resp) = Request::get("/auth/session")
                        .credentials(web_sys::RequestCredentials::Include)
                        .send()
                        .await
                    {
                        #[derive(Deserialize)]
                        struct SessionStatus {
                            authenticated: bool,
                            user_id: Option<String>,
                            roles: Option<Vec<String>>,
                        }
                        if let Ok(status) = resp.json::<SessionStatus>().await {
                            if status.authenticated {
                                set_session_storage_item("veloce_oidc_logged_in", "true");
                                if let Some(ref uid) = status.user_id {
                                    set_session_storage_item("veloce_session_user_id", uid);
                                }
                                if let Some(ref roles) = status.roles {
                                    set_session_storage_item(
                                        "veloce_session_roles",
                                        &roles.join(","),
                                    );
                                }
                                set_logged_in.set(true);
                                request_notification_permission();
                            } else {
                                clear_session_storage();
                                set_logged_in.set(false);
                            }
                        } else {
                            clear_session_storage();
                            set_logged_in.set(false);
                        }
                    } else {
                        clear_session_storage();
                        set_logged_in.set(false);
                    }
                });
            }
        }
    });

    let solvers = create_resource(
        move || config.get(),
        move |conf_opt| async move {
            if conf_opt.flatten().is_some() {
                match api_get("/api/v1/containers").send().await {
                    Ok(resp) if resp.ok() => resp.json::<Vec<ContainerAsset>>().await.ok(),
                    Ok(resp) => {
                        set_api_error.set(Some(describe_http_failure(
                            "Failed to load container templates",
                            &resp,
                        )));
                        None
                    }
                    Err(err) => {
                        set_api_error.set(Some(describe_request_failure(
                            "Failed to load container templates",
                            &err,
                        )));
                        None
                    }
                }
            } else {
                None
            }
        },
    );
    provide_context(solvers);

    // WebSocket and periodic health check logic shared across app
    create_effect(move |_| {
        let conf = config.get();
        if conf.clone().flatten().is_some() {
            spawn_local(async move {
                // Initial fetch to populate data immediately
                let init_jobs = api_get("/api/v1/jobs").send();
                let init_nodes = api_get("/api/v1/nodes").send();

                match init_jobs.await {
                    Ok(resp) if resp.ok() => {
                        set_controller_online.set(true);
                        clear_api_error(set_api_error);
                        if let Ok(data) = resp.json::<Vec<JobInfo>>().await {
                            let previous = jobs.get();
                            notify_job_status_changes(&previous, &data);
                            set_jobs.set(data);
                        }
                    }
                    Ok(resp) => {
                        set_controller_online.set(false);
                        set_api_error.set(Some(describe_http_failure(
                            "Controller jobs API unreachable",
                            &resp,
                        )));
                    }
                    Err(err) => {
                        set_controller_online.set(false);
                        set_api_error.set(Some(describe_request_failure(
                            "Controller jobs API unreachable",
                            &err,
                        )));
                    }
                }
                match init_nodes.await {
                    Ok(resp) if resp.ok() => {
                        if let Ok(data) = resp.json::<Vec<WorkerInfo>>().await {
                            set_nodes.set(data);
                        }
                    }
                    Ok(resp) => {
                        set_api_error.set(Some(describe_http_failure(
                            "Controller nodes API unreachable",
                            &resp,
                        )));
                    }
                    Err(err) => {
                        set_api_error.set(Some(describe_request_failure(
                            "Controller nodes API unreachable",
                            &err,
                        )));
                    }
                }

                // WebSocket event listener loop
                loop {
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
                            set_controller_online.set(false);
                            let message = if status == 403 {
                                "Controller event stream unavailable (insufficient role for WebSocket ticket)"
                            } else if status == 401 {
                                "Controller event stream unavailable (session expired; sign in again)"
                            } else if status == 502 || status == 503 {
                                "Controller event stream unavailable (controller unreachable; retrying)"
                            } else if status == 0 {
                                "Controller event stream unavailable (network error)"
                            } else {
                                "Controller event stream unavailable (WebSocket ticket denied)"
                            };
                            set_api_error.set(Some(message.to_string()));
                            gloo_timers::future::TimeoutFuture::new(5000).await;
                            continue;
                        }
                    };
                    let ws_url =
                        format!("{}//{}/api/v1/ws/events?ticket={}", protocol, host, ticket);

                    let ws = match WebSocket::open(&ws_url) {
                        Ok(w) => w,
                        Err(_) => {
                            set_controller_online.set(false);
                            gloo_timers::future::TimeoutFuture::new(5000).await;
                            continue;
                        }
                    };

                    let (_, mut rx) = ws.split();
                    set_controller_online.set(true);

                    while let Some(msg) = rx.next().await {
                        match msg {
                            Ok(gloo_net::websocket::Message::Text(text)) => {
                                if text == "jobs_updated" {
                                    if let Ok(resp) = api_get("/api/v1/jobs").send().await {
                                        if let Ok(data) = resp.json::<Vec<JobInfo>>().await {
                                            set_jobs.set(data);
                                        }
                                    }
                                } else if text == "nodes_updated" {
                                    if let Ok(resp) = api_get("/api/v1/nodes").send().await {
                                        if let Ok(data) = resp.json::<Vec<WorkerInfo>>().await {
                                            set_nodes.set(data);
                                        }
                                    }
                                }
                            }
                            Err(_) => break,
                            _ => {}
                        }
                    }

                    set_controller_online.set(false);
                    gloo_timers::future::TimeoutFuture::new(2000).await;
                }
            });
        }

        // Periodic fileserver and MCP health checks (run every 5 seconds)
        let handle = set_interval_with_handle(
            move || {
                if conf.clone().flatten().is_some() {
                    spawn_local(async move {
                        match fileserver_get("/api/v1/files").send().await {
                            Ok(resp) => {
                                set_fileserver_online.set(resp.status() < 500);
                            }
                            Err(_) => set_fileserver_online.set(false),
                        }
                    });
                }
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

    let location = use_location();
    let is_fullscreen_interactive = move || {
        let path = location.pathname.get();
        path.ends_with("/vnc") || path.ends_with("/terminal")
    };

    view! {
        <div class="app-container">
            {move || if !logged_in.get() {
                view! { <LoginGate set_logged_in=set_logged_in oidc_enabled=MaybeSignal::from(oidc_enabled) /> }.into_view()
            } else if is_fullscreen_interactive() {
                view! {
                    <main style="width: 100vw; height: 100vh; margin: 0; padding: 0; overflow: hidden; background: #0d1117;">
                        <Routes>
                            <Route path="/jobs/:id/vnc" view=FullscreenVnc/>
                            <Route path="/jobs/:id/terminal" view=FullscreenTerminal/>
                            <Route path="*/*" view=|| ().into_view()/>
                        </Routes>
                    </main>
                }.into_view()
            } else {
                view! {
                    <div class="macos-window">
                        <nav class="sidebar">
                            <h1>
                                <span class="brand-mark">"V"</span>
                                <span class="brand-text">"Veloce"</span>
                            </h1>
                            <A href="/" class="nav-link"><i class="ph ph-squares-four"></i>"Dashboard"</A>
                            <A href="/jobs" class="nav-link"><i class="ph ph-list-dashes"></i>"Jobs"</A>
                            <A href="/submit" class="nav-link"><i class="ph ph-paper-plane-tilt"></i>"Submit Job"</A>
                            <A href="/containers" class="nav-link"><i class="ph ph-package"></i>"Containers"</A>
                            <A href="/cluster" class="nav-link"><i class="ph ph-hard-drives"></i>"Cluster"</A>
                            {move || if has_admin_operator_privileges() {
                                view! { <A href="/telemetry" class="nav-link"><i class="ph ph-activity"></i>"Telemetry"</A> }.into_view()
                            } else {
                                ().into_view()
                            }}
                            {move || if has_admin_operator_privileges() {
                                view! { <A href="/statistics" class="nav-link"><i class="ph ph-chart-line-up"></i>"Statistics"</A> }.into_view()
                            } else {
                                ().into_view()
                            }}
                            {move || if has_admin_operator_privileges() {
                                view! { <A href="/admin" class="nav-link"><i class="ph ph-shield-checkered"></i>"Admin"</A> }.into_view()
                            } else {
                                ().into_view()
                            }}
                            <div style="flex-grow: 1;"></div>
                            <button
                                on:click=move |_| {
                                    if oidc_enabled.get() {
                                        // OIDC logout — redirect to BFF logout endpoint
                                        if let Some(win) = web_sys::window() {
                                            let _ = win.location().set_href("/auth/logout");
                                        }
                                    } else {
                                        clear_session_storage();
                                        set_logged_in.set(false);
                                    }
                                }
                                class="logout-btn"
                            >
                                <i class="ph ph-power" style="font-size: 1.1rem;"></i>
                                {move || if oidc_enabled.get() { "Sign Out" } else { "Disconnect" }}
                            </button>
                        </nav>
                        <main class="content" style="position: relative;">
                            <ApiErrorBanner
                                error=api_error
                                on_dismiss=move || clear_api_error(set_api_error)
                            />
                            <div style="position: absolute; top: 1rem; right: 1.5rem; font-size: 0.75rem; color: var(--text-muted); opacity: 0.5; pointer-events: none; z-index: 1000;">
                                "v" {env!("CARGO_PKG_VERSION")}
                            </div>
                            <Routes>
                                <Route path="/" view=move || view! { <Dashboard jobs=jobs.into() nodes=nodes.into() set_nodes=set_nodes controller_online=controller_online.into() fileserver_online=fileserver_online.into() /> } />
                                <Route path="/jobs" view=JobsList/>
                                <Route path="/submit" view=SubmitJob/>
                                <Route path="/containers" view=ContainersList/>
                                <Route path="/jobs/:id" view=JobDetails/>
                                <Route path="/cluster" view=NodesList/>
                                <Route path="/telemetry" view=move || {
                                    if has_admin_operator_privileges() {
                                        view! { <telemetry::TelemetryPage jobs=jobs.into() nodes=nodes.into() set_nodes=set_nodes controller_online=controller_online.into() fileserver_online=fileserver_online.into() /> }.into_view()
                                    } else {
                                        view! { <AdminPrivilegeRequired /> }.into_view()
                                    }
                                } />
                                <Route path="/statistics" view=move || {
                                    if has_admin_operator_privileges() {
                                        view! { <statistics::StatisticsPage/> }.into_view()
                                    } else {
                                        view! { <AdminPrivilegeRequired /> }.into_view()
                                    }
                                } />
                                <Route path="/admin" view=move || {
                                    if has_admin_operator_privileges() {
                                        view! { <admin::AdminPage/> }.into_view()
                                    } else {
                                        view! { <AdminPrivilegeRequired /> }.into_view()
                                    }
                                }/>
                            </Routes>
                        </main>
                    </div>
                }.into_view()
            }}
        </div>
    }
}
pub fn main() {
    console_error_panic_hook::set_once();
    mount_to_body(|| view! { <App/> })
}

// Helper for polling
pub fn set_interval_with_handle<F>(handler: F, timeout: i32) -> Result<i32, String>
where
    F: FnMut() + 'static,
{
    let window = web_sys::window().ok_or("no global `window` exists")?;
    let closure = wasm_bindgen::closure::Closure::wrap(Box::new(handler) as Box<dyn FnMut()>);
    let id = window
        .set_interval_with_callback_and_timeout_and_arguments_0(
            closure.as_ref().unchecked_ref(),
            timeout,
        )
        .map_err(|_| "failed to set interval")?;
    closure.forget();
    Ok(id)
}
