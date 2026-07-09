//! Leveled logging (`tracing`, filtered via `RUST_LOG`, default `info`) plus
//! optional OTLP log export for WARN+ events.
//!
//! The OTel pipeline is armed at runtime by `OTEL_EXPORTER_OTLP_ENDPOINT` being
//! set (same convention the agents use); without it, logs go to stdout only.
//! The batch processor exports from its own background thread, so the blocking
//! HTTP client never runs on an async worker.

use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};

pub struct Telemetry {
    provider: Option<opentelemetry_sdk::logs::SdkLoggerProvider>,
}

impl Telemetry {
    /// Flush any buffered OTel log records. Call before process exit —
    /// especially in oneshot mode, where the process ends right after the sync.
    pub fn shutdown(self) {
        if let Some(provider) = self.provider {
            let _ = provider.shutdown();
        }
    }
}

pub fn init() -> Telemetry {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let fmt_layer = tracing_subscriber::fmt::layer().with_target(false);
    let registry = tracing_subscriber::registry().with(filter).with(fmt_layer);

    let endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").unwrap_or_default();
    let endpoint = endpoint.trim().trim_end_matches('/').to_string();
    if endpoint.is_empty() {
        registry.init();
        return Telemetry { provider: None };
    }

    match build_provider(&endpoint) {
        Ok(provider) => {
            let bridge =
                opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge::new(&provider);
            // WARN and above only — the OTel sink is for error capture, not chatter.
            registry
                .with(bridge.with_filter(tracing_subscriber::filter::LevelFilter::WARN))
                .init();
            tracing::info!(endpoint, "otel log export enabled (warn and above)");
            Telemetry {
                provider: Some(provider),
            }
        }
        Err(e) => {
            registry.init();
            tracing::warn!("otel init failed, logging to stdout only: {e}");
            Telemetry { provider: None }
        }
    }
}

fn build_provider(
    endpoint: &str,
) -> Result<opentelemetry_sdk::logs::SdkLoggerProvider, Box<dyn std::error::Error>> {
    use opentelemetry_otlp::WithExportConfig;

    let exporter = opentelemetry_otlp::LogExporter::builder()
        .with_http()
        .with_endpoint(format!("{endpoint}/v1/logs"))
        .build()?;
    let service =
        std::env::var("OTEL_SERVICE_NAME").unwrap_or_else(|_| "agent-sandbox-controller".into());
    let resource = opentelemetry_sdk::Resource::builder()
        .with_service_name(service)
        .build();
    Ok(opentelemetry_sdk::logs::SdkLoggerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(resource)
        .build())
}
