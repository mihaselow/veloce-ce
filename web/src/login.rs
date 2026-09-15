use crate::notifications::request_notification_permission;
use gloo_net::http::Request;
use leptos::*;

pub fn get_session_storage_item(key: &str) -> Option<String> {
    web_sys::window()?
        .session_storage()
        .ok()??
        .get_item(key)
        .ok()?
}

pub fn set_session_storage_item(key: &str, val: &str) {
    if let Some(win) = web_sys::window() {
        if let Ok(Some(storage)) = win.session_storage() {
            let _ = storage.set_item(key, val);
        }
    }
}

pub fn clear_session_storage() {
    if let Some(win) = web_sys::window() {
        if let Ok(Some(storage)) = win.session_storage() {
            let _ = storage.clear();
        }
    }
}

pub fn has_admin_operator_privileges() -> bool {
    let oidc_logged_in = get_session_storage_item("veloce_oidc_logged_in")
        .map(|v| v == "true")
        .unwrap_or(false);

    if oidc_logged_in {
        get_session_storage_item("veloce_session_roles")
            .map(|roles| {
                roles
                    .split(',')
                    .map(str::trim)
                    .any(|role| role == "admin" || role == "operator")
            })
            .unwrap_or(false)
    } else {
        true
    }
}

pub fn has_admin_privileges() -> bool {
    let oidc_logged_in = get_session_storage_item("veloce_oidc_logged_in")
        .map(|v| v == "true")
        .unwrap_or(false);

    if oidc_logged_in {
        get_session_storage_item("veloce_session_roles")
            .map(|roles| roles.split(',').map(str::trim).any(|role| role == "admin"))
            .unwrap_or(false)
    } else {
        true
    }
}

#[component]
pub fn AdminPrivilegeRequired() -> impl IntoView {
    view! {
        <div class="glass-panel" style="max-width: 560px; margin: 3rem auto; text-align: center;">
            <h2 style="margin-top: 0;">"Admin Privileges Required"</h2>
            <p style="color: var(--text-muted); margin-bottom: 0;">
                "Federation, Telemetry, and Admin views are available to users with admin or operator privileges."
            </p>
        </div>
    }
}

#[component]
pub fn LoginGate(
    set_logged_in: WriteSignal<bool>,
    #[prop(into)] oidc_enabled: MaybeSignal<bool>,
) -> impl IntoView {
    let (c_key, set_c_key) = create_signal("".to_string());
    let (error_msg, set_error_msg) = create_signal(None::<String>);
    let (is_loading, set_is_loading) = create_signal(false);

    let handle_login = move |ev: leptos::ev::SubmitEvent| {
        ev.prevent_default();
        let controller_val = c_key.get();

        if controller_val.is_empty() {
            set_error_msg.set(Some("Controller API Key is required".to_string()));
            return;
        }

        set_is_loading.set(true);
        set_error_msg.set(None);

        spawn_local(async move {
            match Request::get("/api/v1/jobs")
                .header("X-API-KEY", &controller_val)
                .send()
                .await
            {
                Ok(resp) if resp.ok() => {
                    set_session_storage_item("veloce_controller_api_key", &controller_val);
                    request_notification_permission();
                    set_logged_in.set(true);
                }
                Ok(resp) => {
                    if resp.status() == 401 {
                        set_error_msg.set(Some("Invalid Controller API Key".to_string()));
                    } else {
                        set_error_msg
                            .set(Some(format!("Server returned error: {}", resp.status())));
                    }
                    set_is_loading.set(false);
                }
                Err(e) => {
                    set_error_msg.set(Some(format!("Connection error: {}", e)));
                    set_is_loading.set(false);
                }
            }
        });
    };

    view! {
        <div class="login-container">
            <div class="login-card glass-panel">
                <div class="login-brand-lockup">
                    <div class="login-brand">
                        <span class="brand-mark">"V"</span>
                        <span class="brand-text">"Veloce"</span>
                    </div>
                    <span class="login-subtitle">"High Performance Compute Cluster Management"</span>
                </div>

                {move || if oidc_enabled.get() {
                    view! {
                        <div class="login-form-stack">
                            <button
                                type="button"
                                on:click=move |_| {
                                    if let Some(win) = web_sys::window() {
                                        let _ = win.location().set_href("/auth/login");
                                    }
                                }
                                class="login-primary-btn btn-hover-opacity"
                            >
                                <i class="ph ph-sign-in" style="font-size: 1.2rem;"></i>
                                "Sign In with SSO / OIDC"
                            </button>
                        </div>
                    }.into_view()
                } else {
                    view! {
                        <form on:submit=handle_login class="login-form-stack">
                            <div class="login-field">
                                <label>"Controller API Key"</label>
                                <div class="login-input-wrap">
                                    <i class="ph ph-key" style="position: absolute; left: 1rem; color: var(--text-muted); font-size: 1.2rem;"></i>
                                    <input
                                        type="password"
                                        placeholder="Enter controller X-API-KEY..."
                                        prop:value=c_key
                                        on:input=move |ev| set_c_key.set(event_target_value(&ev))
                                        class="login-input"
                                    />
                                </div>
                            </div>

                            {move || error_msg.get().map(|err| view! {
                                <div class="login-error">
                                    <i class="ph ph-warning-circle" style="font-size: 1.1rem;"></i>
                                    {err}
                                </div>
                            })}

                            <button
                                type="submit"
                                disabled=is_loading
                                class="login-primary-btn btn-hover-opacity"
                            >
                                {move || if is_loading.get() {
                                    view! {
                                        <i class="ph ph-circle-notch animate-spin" style="font-size: 1.2rem;"></i>
                                        "Authenticating..."
                                    }.into_view()
                                } else {
                                    view! {
                                        <i class="ph ph-sign-in" style="font-size: 1.2rem;"></i>
                                        "Sign In"
                                    }.into_view()
                                }}
                            </button>
                        </form>
                    }.into_view()
                }}
            </div>
        </div>
    }
}
