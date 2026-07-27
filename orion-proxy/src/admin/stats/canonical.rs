use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use opentelemetry::KeyValue;
use orion_metrics::metrics::server::update_server_metrics;
use std::collections::HashMap;
use std::io::{self, Write};
use std::sync::OnceLock;
use tracing::debug;

use crate::admin::AdminState;
use orion_metrics::{
    metrics::{clusters, custom, filters, http, listeners, server, tcp, tls, user, Metric},
    sharded::{Gauge, ShardedHistogram, ShardedU64},
};

/// Formats the line in native Envoy format:
/// prefix.[`tag0_value`].[`tag1_value`]...name: value
/// Since `KeyValues` are already hierarchical, we simply iterate in order.
fn format_canonical_line(
    out: &mut impl Write,
    prefix: &str,
    name: &str,
    labels: &[KeyValue],
    value: u64,
) -> io::Result<()> {
    write!(out, "{prefix}")?;

    // Add hierarchical tag values
    for kv in labels {
        write!(out, ".{}", kv.value.as_str())?;
    }

    // Add metric name and final value
    writeln!(out, ".{name}: {value}")?;
    Ok(())
}

fn process_metric_canonical<S: Eq + std::hash::Hash>(
    out: &mut impl Write,
    source: &OnceLock<Metric<ShardedU64<S>>>,
) -> io::Result<()> {
    if let Some(metric) = source.get() {
        let data = metric.value.load_all();
        for (labels, value) in data {
            format_canonical_line(out, metric.prefix, metric.name, &labels, value)?;
        }
    }
    Ok(())
}

fn process_gauge_canonical(out: &mut impl Write, source: &OnceLock<Metric<Gauge>>) -> io::Result<()> {
    if let Some(metric) = source.get() {
        let data = metric.value.load_all();
        for (labels, value) in data {
            format_canonical_line(out, metric.prefix, metric.name, &labels, value)?;
        }
    }
    Ok(())
}

fn process_histogram_canonical<S: Eq + std::hash::Hash + Clone + Copy>(
    out: &mut impl Write,
    source: &OnceLock<Metric<ShardedHistogram<S>>>,
) -> io::Result<()> {
    if let Some(metric) = source.get() {
        // Native Envoy prints percentiles (e.g., P50, P99) on a single line for histograms.
        // Since ShardedHistogram is based on explicit buckets (Prometheus style),
        // we emit at least the sum and the count to maintain a useful canonical representation.

        let sum_data = metric.value.sum().load_all();
        for (labels, value) in sum_data {
            let sum_name = format!("{}_sum", metric.name);
            format_canonical_line(out, metric.prefix, &sum_name, &labels, value)?;
        }

        let count_data = metric.value.count().load_all();
        for (labels, value) in count_data {
            let count_name = format!("{}_count", metric.name);
            format_canonical_line(out, metric.prefix, &count_name, &labels, value)?;
        }
    }
    Ok(())
}

#[allow(clippy::too_many_lines)]
fn build_canonical_output() -> io::Result<String> {
    debug!(target: "stats", "stats_canonical_handler: running");
    let mut out: Vec<u8> = Vec::with_capacity(16384);

    // listeners metrics
    process_metric_canonical(&mut out, &listeners::DOWNSTREAM_CX_TOTAL)?;
    process_metric_canonical(&mut out, &listeners::DOWNSTREAM_CX_DESTROY)?;
    process_metric_canonical(&mut out, &listeners::DOWNSTREAM_CX_ACTIVE)?;
    process_metric_canonical(&mut out, &listeners::NO_FILTER_CHAIN_MATCH)?;
    process_histogram_canonical(&mut out, &listeners::DOWNSTREAM_CX_LENGTH_MS)?;

    // clusters metrics
    process_metric_canonical(&mut out, &clusters::UPSTREAM_RQ_TOTAL)?;
    process_metric_canonical(&mut out, &clusters::UPSTREAM_RQ_ACTIVE)?;
    process_metric_canonical(&mut out, &clusters::UPSTREAM_RQ_TIMEOUT)?;
    process_metric_canonical(&mut out, &clusters::UPSTREAM_RQ_PER_TRY_TIMEOUT)?;
    process_metric_canonical(&mut out, &clusters::UPSTREAM_RQ_RETRY)?;

    process_metric_canonical(&mut out, &clusters::UPSTREAM_CX_TOTAL)?;
    process_metric_canonical(&mut out, &clusters::UPSTREAM_CX_ACTIVE)?;
    process_metric_canonical(&mut out, &clusters::UPSTREAM_CX_DESTROY)?;
    process_metric_canonical(&mut out, &clusters::UPSTREAM_CX_IDLE_TIMEOUT)?;
    process_metric_canonical(&mut out, &clusters::UPSTREAM_CX_CONNECT_FAIL)?;
    process_metric_canonical(&mut out, &clusters::UPSTREAM_CX_CONNECT_TIMEOUT)?;
    process_metric_canonical(&mut out, &clusters::UPSTREAM_CX_RX_BYTES_TOTAL)?;
    process_metric_canonical(&mut out, &clusters::UPSTREAM_CX_TX_BYTES_TOTAL)?;
    process_metric_canonical(&mut out, &clusters::UPSTREAM_CX_OVERFLOW)?;
    process_metric_canonical(&mut out, &clusters::UPSTREAM_RQ_OVERFLOW)?;
    process_metric_canonical(&mut out, &clusters::UPSTREAM_RQ_RETRY_OVERFLOW)?;

    // http metrics
    process_metric_canonical(&mut out, &http::DOWNSTREAM_CX_TOTAL)?;
    process_metric_canonical(&mut out, &http::DOWNSTREAM_CX_SSL_TOTAL)?;
    process_metric_canonical(&mut out, &http::DOWNSTREAM_CX_SSL_ACTIVE)?;
    process_metric_canonical(&mut out, &http::DOWNSTREAM_CX_DESTROY)?;
    process_metric_canonical(&mut out, &http::DOWNSTREAM_CX_ACTIVE)?;
    process_metric_canonical(&mut out, &http::DOWNSTREAM_RQ_1XX)?;
    process_metric_canonical(&mut out, &http::DOWNSTREAM_RQ_2XX)?;
    process_metric_canonical(&mut out, &http::DOWNSTREAM_RQ_3XX)?;
    process_metric_canonical(&mut out, &http::DOWNSTREAM_RQ_4XX)?;
    process_metric_canonical(&mut out, &http::DOWNSTREAM_RQ_5XX)?;
    process_metric_canonical(&mut out, &http::DOWNSTREAM_RQ_TOTAL)?;
    process_metric_canonical(&mut out, &http::DOWNSTREAM_RQ_ACTIVE)?;
    process_metric_canonical(&mut out, &http::DOWNSTREAM_CX_RX_BYTES_TOTAL)?;
    process_metric_canonical(&mut out, &http::DOWNSTREAM_CX_TX_BYTES_TOTAL)?;
    process_histogram_canonical(&mut out, &http::DOWNSTREAM_CX_LENGTH_MS)?;

    // server metrics
    process_gauge_canonical(&mut out, &server::UPTIME)?;
    process_gauge_canonical(&mut out, &server::CONCURRENCY)?;
    process_gauge_canonical(&mut out, &server::MEMORY_HEAP_SIZE)?;
    process_gauge_canonical(&mut out, &server::MEMORY_PHYSICAL_SIZE)?;
    process_gauge_canonical(&mut out, &server::MEMORY_ALLOCATED)?;

    // tcp metrics
    process_metric_canonical(&mut out, &tcp::DOWNSTREAM_CX_TOTAL)?;
    process_metric_canonical(&mut out, &tcp::DOWNSTREAM_CX_DESTROY)?;
    process_metric_canonical(&mut out, &tcp::DOWNSTREAM_CX_ACTIVE)?;
    process_histogram_canonical(&mut out, &tcp::DOWNSTREAM_CX_LENGTH_MS)?;
    process_metric_canonical(&mut out, &tcp::CX_RX_BYTES_RECEIVED)?;
    process_metric_canonical(&mut out, &tcp::CX_TX_BYTES_SENT)?;

    // tls
    process_metric_canonical(&mut out, &tls::HANDSHAKES)?;

    // websocket
    process_metric_canonical(&mut out, &http::DOWNSTREAM_CX_WS_UPGRADES_TOTAL)?;
    process_metric_canonical(&mut out, &http::DOWNSTREAM_CX_WS_UPGRADES_ACTIVE)?;
    process_metric_canonical(&mut out, &http::DOWNSTREAM_RQ_WS_ON_NON_WS_ROUTE)?;

    // user
    process_metric_canonical(&mut out, &user::INVOCATIONS)?;
    process_metric_canonical(&mut out, &user::THROTTLES)?;
    process_metric_canonical(&mut out, &user::SYSTEM_ERRORS)?;
    process_metric_canonical(&mut out, &user::USER_ERRORS)?;
    process_metric_canonical(&mut out, &user::TOTAL_ERRORS)?;
    process_metric_canonical(&mut out, &user::BYTES_TX)?;
    process_metric_canonical(&mut out, &user::BYTES_RX)?;
    process_metric_canonical(&mut out, &user::INBOUND_STREAMING_BYTES_PROCESSED)?;
    process_metric_canonical(&mut out, &user::OUTBOUND_STREAMING_BYTES_PROCESSED)?;
    process_metric_canonical(&mut out, &user::CONNECTIONS)?;
    process_metric_canonical(&mut out, &user::CONNECTIONS_ACTIVE)?;
    process_metric_canonical(&mut out, &user::HTTP_1XX_RESPONSES)?;
    process_metric_canonical(&mut out, &user::HTTP_2XX_RESPONSES)?;
    process_metric_canonical(&mut out, &user::HTTP_3XX_RESPONSES)?;
    process_metric_canonical(&mut out, &user::HTTP_4XX_RESPONSES)?;
    process_metric_canonical(&mut out, &user::HTTP_5XX_RESPONSES)?;
    process_metric_canonical(&mut out, &user::HTTP_404_RESPONSES)?;
    process_metric_canonical(&mut out, &user::HTTP_502_RESPONSES)?;
    process_metric_canonical(&mut out, &user::HTTP_504_RESPONSES)?;
    process_histogram_canonical(&mut out, &user::LATENCY)?;

    // filters
    process_metric_canonical(&mut out, &filters::CONNECTION_RATE_LIMIT)?;
    process_metric_canonical(&mut out, &filters::LOCAL_RATE_LIMIT)?;
    process_metric_canonical(&mut out, &filters::USER_RATE_LIMIT)?;

    // dynamic metrics
    if let Some(custom_metrics) = custom::CUSTOM_METRICS.get() {
        for counter in custom_metrics.counters() {
            let metric = &counter.metric;
            let data = metric.value.load_all();
            for (labels, value) in data {
                format_canonical_line(&mut out, metric.prefix, metric.name, &labels, value)?;
            }
        }
        for histogram in custom_metrics.histograms() {
            let metric = &histogram.metric;
            let sum_data = metric.value.sum().load_all();
            for (labels, value) in sum_data {
                let sum_name = format!("{}_sum", metric.name);
                format_canonical_line(&mut out, metric.prefix, &sum_name, &labels, value)?;
            }
            let count_data = metric.value.count().load_all();
            for (labels, value) in count_data {
                let count_name = format!("{}_count", metric.name);
                format_canonical_line(&mut out, metric.prefix, &count_name, &labels, value)?;
            }
        }
        for gauge in custom_metrics.gauges() {
            let metric = &gauge.metric;
            let data = metric.value.load_all();
            for (labels, value) in data {
                format_canonical_line(&mut out, metric.prefix, metric.name, &labels, value)?;
            }
        }
    }

    Ok(String::from_utf8_lossy(&out).into_owned())
}

pub(crate) async fn stats_handler(State(_): State<AdminState>) -> Result<(HeaderMap, String), (StatusCode, String)> {
    debug!(target: "stats", "stats_handler: running");
    update_server_metrics(&HashMap::new());

    let out = build_canonical_output().map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let mut headers = HeaderMap::new();
    // In native Envoy, the /stats endpoint simply returns text/plain
    #[allow(clippy::unwrap_used)]
    headers.insert(::http::header::CONTENT_TYPE, "text/plain".parse().unwrap());

    Ok((headers, out))
}
