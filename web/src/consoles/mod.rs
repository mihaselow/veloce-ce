use crate::app::WebConfig;
use leptos::*;
use leptos_router::use_params_map;
use std::cell::Cell;
use std::rc::Rc;
use wasm_bindgen::prelude::wasm_bindgen;

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = veloceTerminal)]
    fn init(element_id: &str, job_id: u64);

    #[wasm_bindgen(js_namespace = veloceTerminal)]
    fn destroy(element_id: &str);

    #[wasm_bindgen(js_name = init, js_namespace = veloceVnc)]
    fn vnc_init(element_id: &str, job_id: u64);

    #[wasm_bindgen(js_name = destroy, js_namespace = veloceVnc)]
    fn vnc_destroy(element_id: &str);
}

fn mount_interactive(start: impl FnOnce() + 'static, stop: impl FnOnce() + 'static) {
    let cancelled = Rc::new(Cell::new(false));
    let cancelled_spawn = cancelled.clone();
    spawn_local(async move {
        gloo_timers::future::TimeoutFuture::new(0).await;
        if cancelled_spawn.get() {
            return;
        }
        start();
    });
    on_cleanup(move || {
        cancelled.set(true);
        stop();
    });
}

#[component]
pub(crate) fn TerminalConsole(
    job_id: u64,
    #[allow(unused_variables)] api_key: String,
) -> impl IntoView {
    let element_id = "veloce-terminal-container";

    create_effect(move |_| {
        mount_interactive(
            move || init(element_id, job_id),
            move || destroy(element_id),
        );
    });

    view! {
        <div class="log-console glass-panel" style="margin-top: 1rem;">
            <div class="log-header" style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.5rem; padding: 0.5rem 1rem; background: rgba(255,255,255,0.05); border-radius: 4px 4px 0 0; border-bottom: 1px solid rgba(255,255,255,0.1);">
                <h3 style="margin: 0; font-size: 0.9rem; text-transform: uppercase; letter-spacing: 0.05em; color: var(--text-dim);">"Interactive Shell (Bash)"</h3>
                <div style="display: flex; align-items: center; gap: 10px;">
                    <button
                        on:click=move |_| {
                            let url = format!("/jobs/{}/terminal", job_id);
                            if let Some(win) = web_sys::window() {
                                let _ = win.open_with_url_and_target_and_features(
                                    &url,
                                    "_blank",
                                    "width=1024,height=720,resizable=yes"
                                );
                            }
                        }
                        style="padding: 4px 10px; font-size: 0.75rem; border-radius: 4px; border: 1px solid rgba(255,255,255,0.2); background: rgba(255,255,255,0.05); color: var(--text-normal); cursor: pointer; display: flex; align-items: center; gap: 4px; transition: all 0.2s;"
                        class="btn-hover-opacity"
                    >
                        <i class="ph ph-corners-out"></i> " Pop Out Window"
                    </button>
                    <span style="font-size: 0.75rem; color: var(--text-muted); display: flex; align-items: center; gap: 4px;">
                        <i class="ph ph-lock" style="color: var(--success);"></i> " Encrypted Sandbox"
                    </span>
                </div>
            </div>
            <div
                id=element_id
                style="background: #0d1117; border-radius: 0 0 4px 4px; padding: 10px; border: 1px solid rgba(255,255,255,0.1); border-top: none; height: 450px; width: 100%; box-sizing: border-box;"
            >
            </div>
        </div>
    }
}

#[component]
pub(crate) fn InteractiveProxyFrame(job_id: u64) -> impl IntoView {
    let src = format!("/api/v1/jobs/{job_id}/proxy/lab");
    view! {
        <iframe
            src=src
            title="JupyterLab"
            style="width: 100%; height: 70vh; border: none; border-radius: 8px; background: var(--bg-card);"
        ></iframe>
    }
}

#[component]
pub(crate) fn VncConsole(job_id: u64, #[allow(unused_variables)] api_key: String) -> impl IntoView {
    let element_id = "veloce-vnc-container";

    create_effect(move |_| {
        mount_interactive(
            move || vnc_init(element_id, job_id),
            move || vnc_destroy(element_id),
        );
    });

    view! {
        <div class="log-console glass-panel" style="margin-top: 1rem; max-width: 1120px; margin-left: auto; margin-right: auto;">
            <div class="log-header" style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.5rem; padding: 0.5rem 1rem; background: rgba(255,255,255,0.05); border-radius: 4px 4px 0 0; border-bottom: 1px solid rgba(255,255,255,0.1);">
                <h3 style="margin: 0; font-size: 0.9rem; text-transform: uppercase; letter-spacing: 0.05em; color: var(--text-dim);">"Interactive VNC Desktop (XFCE)"</h3>
                <div style="display: flex; align-items: center; gap: 10px;">
                    <button
                        on:click=move |_| {
                            let url = format!("/jobs/{}/vnc", job_id);
                            if let Some(win) = web_sys::window() {
                                let _ = win.open_with_url_and_target_and_features(
                                    &url,
                                    "_blank",
                                    "width=1280,height=850,resizable=yes"
                                );
                            }
                        }
                        style="padding: 4px 10px; font-size: 0.75rem; border-radius: 4px; border: 1px solid rgba(255,255,255,0.2); background: rgba(255,255,255,0.05); color: var(--text-normal); cursor: pointer; display: flex; align-items: center; gap: 4px; transition: all 0.2s;"
                        class="btn-hover-opacity"
                    >
                        <i class="ph ph-corners-out"></i> " Pop Out Window"
                    </button>
                    <span style="font-size: 0.75rem; color: var(--text-muted); display: flex; align-items: center; gap: 4px;">
                        <i class="ph ph-lock" style="color: var(--success);"></i> " Encrypted Sandbox"
                    </span>
                </div>
            </div>
            <div
                id=element_id
                style="background: #0d1117; border-radius: 0 0 4px 4px; border: 1px solid rgba(255,255,255,0.1); border-top: none; width: 100%; aspect-ratio: 1280 / 800; max-height: 700px; box-sizing: border-box; overflow: auto; display: flex; align-items: center; justify-content: center; color: var(--text-muted); font-family: 'JetBrains Mono', monospace;"
            >
                <div class="vnc-placeholder" style="display: flex; flex-direction: column; align-items: center; gap: 1rem;">
                    <i class="ph ph-circle-notch animate-spin" style="font-size: 2rem; color: var(--primary);"></i>
                    "Establishing Secure VNC Tunnel..."
                </div>
            </div>
        </div>
    }
}

#[component]
pub(crate) fn FullscreenVnc() -> impl IntoView {
    let params = use_params_map();
    let id_opt = move || {
        params
            .get()
            .get("id")
            .cloned()
            .and_then(|id| id.parse::<u64>().ok())
    };
    let config =
        use_context::<Resource<bool, Option<WebConfig>>>().expect("WebConfig context missing");

    let element_id = "veloce-fullscreen-vnc-container";

    create_effect(move |_| {
        if let (Some(job_id), Some(Some(_conf))) = (id_opt(), config.get()) {
            mount_interactive(
                move || vnc_init(element_id, job_id),
                move || vnc_destroy(element_id),
            );
        }
    });

    view! {
        <div style="width: 100vw; height: 100vh; display: flex; flex-direction: column; background: #0d1117;">
            <div style="display: flex; justify-content: space-between; align-items: center; padding: 0.5rem 1rem; background: rgba(255,255,255,0.05); border-bottom: 1px solid rgba(255,255,255,0.1); height: 40px; box-sizing: border-box;">
                <h3 style="margin: 0; font-size: 0.85rem; text-transform: uppercase; letter-spacing: 0.05em; color: var(--text-dim); display: flex; align-items: center; gap: 8px;">
                    <i class="ph ph-desktop"></i>
                    {move || format!("Job {} - Fullscreen VNC Desktop", id_opt().unwrap_or(0))}
                </h3>
                <span style="font-size: 0.7rem; color: var(--text-muted); display: flex; align-items: center; gap: 4px;">
                    <i class="ph ph-lock" style="color: var(--success);"></i> " Encrypted Sandbox"
                </span>
            </div>
            <div
                id=element_id
                style="flex: 1; width: 100%; height: calc(100vh - 40px); background: #0d1117; display: flex; align-items: center; justify-content: center; overflow: hidden; box-sizing: border-box;"
            >
                <div class="vnc-placeholder" style="display: flex; flex-direction: column; align-items: center; gap: 1rem;">
                    <i class="ph ph-circle-notch animate-spin" style="font-size: 2rem; color: var(--primary);"></i>
                    "Establishing Secure VNC Tunnel..."
                </div>
            </div>
        </div>
    }
}

#[component]
pub(crate) fn FullscreenTerminal() -> impl IntoView {
    let params = use_params_map();
    let id_opt = move || {
        params
            .get()
            .get("id")
            .cloned()
            .and_then(|id| id.parse::<u64>().ok())
    };
    let config =
        use_context::<Resource<bool, Option<WebConfig>>>().expect("WebConfig context missing");

    let element_id = "veloce-fullscreen-terminal-container";

    create_effect(move |_| {
        if let (Some(job_id), Some(Some(_conf))) = (id_opt(), config.get()) {
            mount_interactive(
                move || init(element_id, job_id),
                move || destroy(element_id),
            );
        }
    });

    view! {
        <div style="width: 100vw; height: 100vh; display: flex; flex-direction: column; background: #0d1117;">
            <div style="display: flex; justify-content: space-between; align-items: center; padding: 0.5rem 1rem; background: rgba(255,255,255,0.05); border-bottom: 1px solid rgba(255,255,255,0.1); height: 40px; box-sizing: border-box;">
                <h3 style="margin: 0; font-size: 0.85rem; text-transform: uppercase; letter-spacing: 0.05em; color: var(--text-dim); display: flex; align-items: center; gap: 8px;">
                    <i class="ph ph-terminal-window"></i>
                    {move || format!("Job {} - Interactive Shell", id_opt().unwrap_or(0))}
                </h3>
                <span style="font-size: 0.7rem; color: var(--text-muted); display: flex; align-items: center; gap: 4px;">
                    <i class="ph ph-lock" style="color: var(--success);"></i> " Encrypted Sandbox"
                </span>
            </div>
            <div
                id=element_id
                style="flex: 1; width: 100%; height: calc(100vh - 40px); background: #0d1117; padding: 10px; box-sizing: border-box; overflow: hidden;"
            >
            </div>
        </div>
    }
}
