// Copyright 2025 The kmesh Authors
// Copyright 2026 The arion-gateway Authors
//
// Modified by arion-gateway Authors.
//
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
//

#[macro_use]
pub mod macros;
pub mod metrics;
pub mod sharded;
pub mod str_pair;

use arion_configuration::config::{metrics::StatsSink, Bootstrap};
use serde::{Deserialize, Serialize};

use {
    opentelemetry::global,
    opentelemetry_otlp::{Protocol, WithExportConfig},
    opentelemetry_sdk::metrics::PeriodicReader,
    parking_lot::{Condvar, Mutex},
    std::sync::LazyLock,
    std::time::Duration,
    tracing::info,
};

const DEFAULT_EXPORT_PERIOD: std::time::Duration = std::time::Duration::from_secs(5);
static SETUP_BARRIER: LazyLock<(Mutex<bool>, Condvar)> = LazyLock::new(|| (Mutex::new(false), Condvar::new()));

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct OtelExporterConfig {
    pub prefix: String,
    pub endpoint: String,
    pub stat_prefix: Option<String>,
    pub export_period: Option<std::time::Duration>,
}

impl OtelExporterConfig {
    pub fn extract_from_bootstrap(bootstrap: &Bootstrap) -> Vec<Self> {
        bootstrap
            .stats_sinks
            .iter()
            .filter_map(|sink| match sink {
                StatsSink::OpenTelemetry(config) => {
                    let endpoint = config.grpc_service.google_grpc.as_ref().map(|g| g.target_uri.clone());
                    let stat_prefix = config.grpc_service.google_grpc.as_ref().map(|g| g.stat_prefix.clone());
                    let prefix = config.prefix.clone();
                    let export_period = bootstrap.stats_flush_interval;
                    endpoint.map(|endpoint| OtelExporterConfig { prefix, endpoint, stat_prefix, export_period })
                },
            })
            .collect()
    }
}

// This function launches the metrics exporter based on the provided configuration.
// It's designed to be called during the application startup to initialize the OpenTelemetry metrics exporter.
//
pub async fn otel_launch_exporter(
    multi_metrics: &[OtelExporterConfig],
) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    info!("OTEL metrics: initializing exporter...");
    let mut provider_builder = opentelemetry_sdk::metrics::SdkMeterProvider::builder();

    for metrics in multi_metrics {
        info!("Building gRPC exporter {}, with endpoint {}...", metrics.prefix, metrics.endpoint);
        let exporter = opentelemetry_otlp::MetricExporter::builder()
            .with_tonic()
            .with_endpoint(&metrics.endpoint)
            .with_protocol(Protocol::Grpc)
            .with_timeout(Duration::from_secs(5))
            .build()?;

        let exporter_period = metrics.export_period.unwrap_or(DEFAULT_EXPORT_PERIOD);

        info!("Building periodic reader, with period {:?}...", exporter_period);
        let periodic_reader = PeriodicReader::builder(exporter).with_interval(exporter_period).build();

        info!("Updating provider with the periodic_reader...");
        provider_builder = provider_builder.with_reader(periodic_reader);
    }

    info!("Building and setting global Meter provider...");
    let provider = provider_builder.build();
    global::set_meter_provider(provider);

    signal_setup_complete();
    info!("OTEL metrics: exporter launched successfully.");

    Ok(())
}

pub fn wait_for_metrics_setup() {
    let (lock, cvar) = &*SETUP_BARRIER;
    let mut setup_complete = lock.lock();
    while !*setup_complete {
        cvar.wait(&mut setup_complete);
    }
}

fn signal_setup_complete() {
    let (lock, cvar) = &*SETUP_BARRIER;
    let mut setup_complete = lock.lock();
    *setup_complete = true;
    cvar.notify_all();
}
