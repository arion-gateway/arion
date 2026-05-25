// Copyright 2025 The kmesh Authors
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

use std::hash::Hash;
use std::io::{self, Write};
use std::sync::OnceLock;
use std::{borrow::Cow, collections::HashMap};

use ::http::{header::HeaderMap, StatusCode};
use axum::extract::State;
use opentelemetry::KeyValue;
use tracing::debug;

use crate::admin::AdminState;
use orion_metrics::{
    metrics::{
        clusters, custom, filters, http, listeners,
        server::{self, update_server_metrics},
        tcp, tls, user, Metric,
    },
    sharded::{Gauge, ShardedHistogram, ShardedU64},
};

/// Escapes special characters in label values according to Prometheus specifications.
fn escape_label_value(val: &str) -> Cow<'_, str> {
    if val.contains(['\\', '"', '\n']) {
        Cow::Owned(val.replace('\\', "\\\\").replace('\n', "\\n").replace('"', "\\\""))
    } else {
        Cow::Borrowed(val)
    }
}

fn write_metric_labels(out: &mut impl Write, labels: &[KeyValue]) -> io::Result<()> {
    if labels.is_empty() {
        return Ok(());
    }
    write!(out, "{{")?;
    for (i, kv) in labels.iter().enumerate() {
        if i > 0 {
            write!(out, ",")?;
        }
        let val = kv.value.as_str();
        let escaped_val = escape_label_value(val.as_ref());
        write!(out, "{}=\"{}\"", kv.key.as_str(), escaped_val)?;
    }
    write!(out, "}}")?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn write_metric_labels_with_extra(
    out: &mut impl Write,
    labels: &[KeyValue],
    extra_key: &str,
    extra_val: &str,
) -> io::Result<()> {
    write!(out, "{{")?;
    let mut first = true;
    for kv in labels {
        if !first {
            write!(out, ",")?;
        }
        first = false;
        let val = kv.value.as_str();
        let escaped_val = escape_label_value(val.as_ref());
        write!(out, "{}=\"{}\"", kv.key.as_str(), escaped_val)?;
    }
    if !first {
        write!(out, ",")?;
    }
    let escaped_extra_val = escape_label_value(extra_val);
    write!(out, "{extra_key}=\"{escaped_extra_val}\"")?;
    write!(out, "}}")?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn format_metric<S: Eq + Hash>(
    out: &mut impl Write,
    prefix: &str,
    name: &str,
    desc: &str,
    metric_type: &str,
    metric_source: &ShardedU64<S>,
) -> io::Result<()> {
    let data = metric_source.load_all();
    if data.is_empty() {
        return Ok(());
    }

    let full_name = format!("{prefix}_{name}");
    writeln!(out, "# HELP {full_name} {desc}")?;
    writeln!(out, "# TYPE {full_name} {metric_type}")?;

    for (labels, value) in data {
        write!(out, "{full_name}")?;
        write_metric_labels(out, &labels)?;
        writeln!(out, " {value}")?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn format_gauge(
    out: &mut impl Write,
    prefix: &str,
    name: &str,
    desc: &str,
    metric_type: &str,
    metric_source: &Gauge,
) -> io::Result<()> {
    let data = metric_source.load_all();
    if data.is_empty() {
        return Ok(());
    }

    let full_name = format!("{prefix}_{name}");
    writeln!(out, "# HELP {full_name} {desc}")?;
    writeln!(out, "# TYPE {full_name} {metric_type}")?;

    for (labels, value) in data {
        write!(out, "{full_name}")?;
        write_metric_labels(out, &labels)?;
        writeln!(out, " {value}")?;
    }
    Ok(())
}

fn format_histogram<S: Eq + Hash + Clone + Copy>(
    out: &mut impl Write,
    prefix: &str,
    name: &str,
    desc: &str,
    metric_source: &ShardedHistogram<S>,
) -> io::Result<()> {
    let count_data = metric_source.count().load_all();
    if count_data.is_empty() {
        return Ok(());
    }

    let full_name = format!("{prefix}_{name}");

    writeln!(out, "# HELP {full_name} {desc}")?;
    writeln!(out, "# TYPE {full_name} histogram")?;

    for (&bound, count) in metric_source.buckets().iter().zip(metric_source.counts().iter()) {
        let bound_str = if bound == u64::MAX { "+Inf".to_owned() } else { bound.to_string() };
        let bucket_data = count.load_all();
        for (labels, value) in bucket_data {
            write!(out, "{full_name}_bucket")?;
            write_metric_labels_with_extra(out, &labels, "le", &bound_str)?;
            writeln!(out, " {value}")?;
        }
    }

    let sum_data = metric_source.sum().load_all();
    for (labels, value) in sum_data {
        write!(out, "{full_name}_sum")?;
        write_metric_labels(out, &labels)?;
        writeln!(out, " {value}")?;
    }

    for (labels, value) in count_data {
        write!(out, "{full_name}_count")?;
        write_metric_labels(out, &labels)?;
        writeln!(out, " {value}")?;
    }
    Ok(())
}

fn process_metric_as_counter<S: Eq + Hash>(
    out: &mut impl Write,
    source: &OnceLock<Metric<ShardedU64<S>>>,
) -> io::Result<()> {
    if let Some(metric) = source.get() {
        format_metric(out, metric.prefix, metric.name, metric.descr, "counter", &metric.value)?;
    }
    Ok(())
}

fn process_metric_as_gauge<S: Eq + Hash>(
    out: &mut impl Write,
    source: &OnceLock<Metric<ShardedU64<S>>>,
) -> io::Result<()> {
    if let Some(metric) = source.get() {
        format_metric(out, metric.prefix, metric.name, metric.descr, "gauge", &metric.value)?;
    }
    Ok(())
}

fn process_gauge(out: &mut impl Write, source: &OnceLock<Metric<Gauge>>) -> io::Result<()> {
    if let Some(metric) = source.get() {
        format_gauge(out, metric.prefix, metric.name, metric.descr, "gauge", &metric.value)?;
    }
    Ok(())
}

fn process_histogram<S: Eq + Hash + Clone + Copy>(
    out: &mut impl Write,
    source: &OnceLock<Metric<ShardedHistogram<S>>>,
) -> io::Result<()> {
    if let Some(metric) = source.get() {
        format_histogram(out, metric.prefix, metric.name, metric.descr, &metric.value)?;
    }
    Ok(())
}

fn build_prometheus_output() -> io::Result<String> {
    debug!(target: "prometheus", "prometheus_handler: running");
    let mut out: Vec<u8> = Vec::with_capacity(16384);

    // listeners metrics
    process_metric_as_counter(&mut out, &listeners::DOWNSTREAM_CX_TOTAL)?;
    process_metric_as_counter(&mut out, &listeners::DOWNSTREAM_CX_DESTROY)?;
    process_metric_as_gauge(&mut out, &listeners::DOWNSTREAM_CX_ACTIVE)?;
    process_metric_as_counter(&mut out, &listeners::NO_FILTER_CHAIN_MATCH)?;
    process_histogram(&mut out, &listeners::DOWNSTREAM_CX_LENGTH_MS)?;

    // clusters metrics
    process_metric_as_counter(&mut out, &clusters::UPSTREAM_RQ_TOTAL)?;
    process_metric_as_gauge(&mut out, &clusters::UPSTREAM_RQ_ACTIVE)?;
    process_metric_as_counter(&mut out, &clusters::UPSTREAM_RQ_TIMEOUT)?;
    process_metric_as_counter(&mut out, &clusters::UPSTREAM_RQ_PER_TRY_TIMEOUT)?;
    process_metric_as_counter(&mut out, &clusters::UPSTREAM_RQ_RETRY)?;

    process_metric_as_counter(&mut out, &clusters::UPSTREAM_CX_TOTAL)?;
    process_metric_as_gauge(&mut out, &clusters::UPSTREAM_CX_ACTIVE)?;
    process_metric_as_counter(&mut out, &clusters::UPSTREAM_CX_DESTROY)?;
    process_metric_as_counter(&mut out, &clusters::UPSTREAM_CX_IDLE_TIMEOUT)?;
    process_metric_as_counter(&mut out, &clusters::UPSTREAM_CX_CONNECT_FAIL)?;
    process_metric_as_counter(&mut out, &clusters::UPSTREAM_CX_CONNECT_TIMEOUT)?;
    process_metric_as_counter(&mut out, &clusters::UPSTREAM_CX_RX_BYTES_TOTAL)?;
    process_metric_as_counter(&mut out, &clusters::UPSTREAM_CX_TX_BYTES_TOTAL)?;
    process_metric_as_counter(&mut out, &clusters::UPSTREAM_CX_OVERFLOW)?;
    process_metric_as_counter(&mut out, &clusters::UPSTREAM_RQ_OVERFLOW)?;
    process_metric_as_counter(&mut out, &clusters::UPSTREAM_RQ_RETRY_OVERFLOW)?;

    // http metrics
    process_metric_as_counter(&mut out, &http::DOWNSTREAM_CX_TOTAL)?;
    process_metric_as_counter(&mut out, &http::DOWNSTREAM_CX_SSL_TOTAL)?;
    process_metric_as_gauge(&mut out, &http::DOWNSTREAM_CX_SSL_ACTIVE)?;
    process_metric_as_counter(&mut out, &http::DOWNSTREAM_CX_DESTROY)?;
    process_metric_as_gauge(&mut out, &http::DOWNSTREAM_CX_ACTIVE)?;
    process_metric_as_counter(&mut out, &http::DOWNSTREAM_RQ_1XX)?;
    process_metric_as_counter(&mut out, &http::DOWNSTREAM_RQ_2XX)?;
    process_metric_as_counter(&mut out, &http::DOWNSTREAM_RQ_3XX)?;
    process_metric_as_counter(&mut out, &http::DOWNSTREAM_RQ_4XX)?;
    process_metric_as_counter(&mut out, &http::DOWNSTREAM_RQ_5XX)?;
    process_metric_as_counter(&mut out, &http::DOWNSTREAM_RQ_TOTAL)?;
    process_metric_as_gauge(&mut out, &http::DOWNSTREAM_RQ_ACTIVE)?;
    process_metric_as_counter(&mut out, &http::DOWNSTREAM_CX_RX_BYTES_TOTAL)?;
    process_metric_as_counter(&mut out, &http::DOWNSTREAM_CX_TX_BYTES_TOTAL)?;
    process_histogram(&mut out, &http::DOWNSTREAM_CX_LENGTH_MS)?;

    // server metrics
    process_gauge(&mut out, &server::UPTIME)?;
    process_gauge(&mut out, &server::CONCURRENCY)?;
    process_gauge(&mut out, &server::MEMORY_HEAP_SIZE)?;
    process_gauge(&mut out, &server::MEMORY_PHYSICAL_SIZE)?;
    process_gauge(&mut out, &server::MEMORY_ALLOCATED)?;

    // tcp metrics
    process_metric_as_counter(&mut out, &tcp::DOWNSTREAM_CX_TOTAL)?;
    process_metric_as_counter(&mut out, &tcp::DOWNSTREAM_CX_DESTROY)?;
    process_metric_as_gauge(&mut out, &tcp::DOWNSTREAM_CX_ACTIVE)?;
    process_histogram(&mut out, &tcp::DOWNSTREAM_CX_LENGTH_MS)?;
    process_metric_as_counter(&mut out, &tcp::CX_RX_BYTES_RECEIVED)?;
    process_metric_as_counter(&mut out, &tcp::CX_TX_BYTES_SENT)?;

    // tls
    process_metric_as_counter(&mut out, &tls::HANDSHAKES)?;

    // websocket
    process_metric_as_counter(&mut out, &http::DOWNSTREAM_CX_WS_UPGRADES_TOTAL)?;
    process_metric_as_gauge(&mut out, &http::DOWNSTREAM_CX_WS_UPGRADES_ACTIVE)?;
    process_metric_as_counter(&mut out, &http::DOWNSTREAM_RQ_WS_ON_NON_WS_ROUTE)?;

    // user/agentrun
    process_metric_as_counter(&mut out, &user::INVOCATIONS)?;
    process_metric_as_counter(&mut out, &user::THROTTLES)?;
    process_metric_as_counter(&mut out, &user::SYSTEM_ERRORS)?;
    process_metric_as_counter(&mut out, &user::USER_ERRORS)?;
    process_metric_as_counter(&mut out, &user::TOTAL_ERRORS)?;
    process_metric_as_counter(&mut out, &user::BYTES_TX)?;
    process_metric_as_counter(&mut out, &user::BYTES_RX)?;
    process_metric_as_counter(&mut out, &user::INBOUND_STREAMING_BYTES_PROCESSED)?;
    process_metric_as_counter(&mut out, &user::OUTBOUND_STREAMING_BYTES_PROCESSED)?;
    process_metric_as_counter(&mut out, &user::HTTP_1XX_RESPONSES)?;
    process_metric_as_counter(&mut out, &user::HTTP_2XX_RESPONSES)?;
    process_metric_as_counter(&mut out, &user::HTTP_3XX_RESPONSES)?;
    process_metric_as_counter(&mut out, &user::HTTP_4XX_RESPONSES)?;
    process_metric_as_counter(&mut out, &user::HTTP_5XX_RESPONSES)?;
    process_histogram(&mut out, &user::LATENCY)?;

    // filters
    process_metric_as_counter(&mut out, &filters::CONNECTION_RATE_LIMIT)?;
    process_metric_as_counter(&mut out, &filters::LOCAL_RATE_LIMIT)?;
    process_metric_as_counter(&mut out, &filters::USER_RATE_LIMIT)?;

    // dynamic metrics
    if let Some(custom_metrics) = custom::CUSTOM_METRICS.get() {
        for counter in custom_metrics.counters() {
            format_metric(
                &mut out,
                counter.metric.prefix,
                counter.metric.name,
                counter.metric.descr,
                "counter",
                &counter.metric.value,
            )?;
        }
        for histogram in custom_metrics.histograms() {
            format_histogram(
                &mut out,
                histogram.metric.prefix,
                histogram.metric.name,
                histogram.metric.descr,
                &histogram.metric.value,
            )?;
        }
        for gauge in custom_metrics.gauges() {
            format_gauge(
                &mut out,
                gauge.metric.prefix,
                gauge.metric.name,
                gauge.metric.descr,
                "gauge",
                &gauge.metric.value,
            )?;
        }
    }

    String::from_utf8(out).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

pub(crate) async fn prometheus_handler(
    State(_): State<AdminState>,
) -> Result<(HeaderMap, String), (StatusCode, String)> {
    debug!(target: "prometheus", "prometheus_handler: running");
    update_server_metrics(&HashMap::new());

    let out = build_prometheus_output().map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let mut headers = HeaderMap::new();
    #[allow(clippy::unwrap_used)]
    headers.insert(::http::header::CONTENT_TYPE, "text/plain; version=0.0.4".parse().unwrap());

    Ok((headers, out))
}
