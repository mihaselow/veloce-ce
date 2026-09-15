use gloo_net::http::Response;
use leptos::*;

/// App-wide API error banner state (provided from `AppContent`).
#[allow(dead_code)]
pub fn use_api_error() -> Option<WriteSignal<Option<String>>> {
    use_context::<WriteSignal<Option<String>>>()
}

#[allow(dead_code)]
pub fn set_api_error(set_error: WriteSignal<Option<String>>, message: impl Into<String>) {
    set_error.set(Some(message.into()));
}

pub fn clear_api_error(set_error: WriteSignal<Option<String>>) {
    set_error.set(None);
}

pub fn describe_http_failure(context: &str, resp: &Response) -> String {
    format!("{}: HTTP {}", context, resp.status())
}

pub fn describe_request_failure(context: &str, err: &gloo_net::Error) -> String {
    format!("{}: {}", context, err)
}

#[component]
pub fn ApiErrorBanner(
    error: ReadSignal<Option<String>>,
    on_dismiss: impl Fn() + 'static + Clone,
) -> impl IntoView {
    move || {
        error.get().map(|message| {
            let on_dismiss = on_dismiss.clone();
            view! {
                <div class="api-error-banner" role="alert">
                    <i class="ph ph-warning-circle"></i>
                    <span class="api-error-banner-text">{message}</span>
                    <button
                        type="button"
                        class="api-error-dismiss"
                        aria-label="Dismiss alert"
                        on:click=move |_| on_dismiss()
                    >
                        <i class="ph ph-x"></i>
                    </button>
                </div>
            }
        })
    }
}
