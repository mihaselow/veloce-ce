use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::trace::{SdkTracerProvider, Tracer};
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

    let exporter = match opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint.clone())
        .build()
    {
        Ok(exporter) => exporter,
        Err(e) => {
            eprintln!("OTLP tracing disabled (endpoint={endpoint}): {e}");
            return None;
        }
    };

    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(Resource::builder().with_service_name(service_name).build())
        .build();
    let tracer = provider.tracer("veloce-worker");
    opentelemetry::global::set_tracer_provider(provider);
    Some(tracer)
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
