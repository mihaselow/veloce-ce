use opentelemetry::KeyValue;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::trace::Tracer;
use opentelemetry_sdk::Resource;

pub fn otlp_disabled() -> bool {
    std::env::var("OTEL_SDK_DISABLED")
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false)
}

pub fn otlp_endpoint() -> Option<String> {
    if otlp_disabled() {
        return None;
    }
    std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .ok()
        .filter(|e| !e.is_empty())
}

fn service_name(worker_id: &str) -> String {
    std::env::var("OTEL_SERVICE_NAME").unwrap_or_else(|_| format!("veloce-worker-{worker_id}"))
}

/// Install an OTLP tracer when `OTEL_EXPORTER_OTLP_ENDPOINT` is set and export is not disabled.
pub fn try_otlp_tracer(worker_id: &str) -> Option<Tracer> {
    let endpoint = otlp_endpoint()?;
    let service_name = service_name(worker_id);

    let exporter = opentelemetry_otlp::new_exporter()
        .tonic()
        .with_endpoint(endpoint.clone());

    match opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(exporter)
        .with_trace_config(
            opentelemetry_sdk::trace::config().with_resource(Resource::new(vec![KeyValue::new(
                "service.name",
                service_name,
            )])),
        )
        .install_batch(opentelemetry_sdk::runtime::Tokio)
    {
        Ok(tracer) => Some(tracer),
        Err(e) => {
            eprintln!("OTLP tracing disabled (endpoint={endpoint}): {e}");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn otlp_env_helpers() {
        let prev_endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").ok();
        let prev_disabled = std::env::var("OTEL_SDK_DISABLED").ok();

        std::env::remove_var("OTEL_EXPORTER_OTLP_ENDPOINT");
        std::env::remove_var("OTEL_SDK_DISABLED");
        assert_eq!(otlp_endpoint(), None);

        std::env::set_var("OTEL_SDK_DISABLED", "true");
        std::env::set_var("OTEL_EXPORTER_OTLP_ENDPOINT", "http://jaeger:4317");
        assert!(otlp_disabled());
        assert_eq!(otlp_endpoint(), None);

        std::env::remove_var("OTEL_SDK_DISABLED");
        assert_eq!(otlp_endpoint().as_deref(), Some("http://jaeger:4317"));

        match prev_endpoint {
            Some(v) => std::env::set_var("OTEL_EXPORTER_OTLP_ENDPOINT", v),
            None => std::env::remove_var("OTEL_EXPORTER_OTLP_ENDPOINT"),
        }
        match prev_disabled {
            Some(v) => std::env::set_var("OTEL_SDK_DISABLED", v),
            None => std::env::remove_var("OTEL_SDK_DISABLED"),
        }
    }
}
