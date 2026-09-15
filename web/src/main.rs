mod admin;
mod api_client;
mod api_error;
mod app;
mod consoles;
mod login;
mod markdown;
mod metrics_chart;
mod notifications;
mod pages;
mod statistics;
mod telemetry;
mod templates;

pub use api_client::{api_delete, api_get, api_post, fileserver_get};
pub use app::{set_interval_with_handle, WebConfig};

fn main() {
    app::main();
}
