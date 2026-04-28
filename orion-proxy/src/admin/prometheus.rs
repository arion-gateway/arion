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

use std::borrow::Cow;
use std::fmt::Write;
use std::hash::Hash;
use std::sync::OnceLock;

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
    if val.contains(|c| c == '\\' || c == '"' || c == '\n') {
        Cow::Owned(val.replace('\\', "\\\\").replace('\n', "\\n").replace('"', "\\\""))
    } else {
        Cow::Borrowed(val)
    }
}

fn write_metric_labels(out: &mut String, labels: &[KeyValue]) {
    if labels.is_empty() {
        return;
    }
    let _ = write!(out, "{{");
    for (i, kv) in labels.iter().enumerate() {
        if i > 0 {
            let _ = write!(out, ",");
        }
        let val = kv.value.as_str();
        let escaped_val = escape_label_value(val.as_ref());
        let _ = write!(out, "{}=\"{}\"", kv.key.as_str(), escaped_val);
    }
    let _ = write!(out, "}}");
}

fn write_metric_labels_with_extra(out: &mut String, labels: &[KeyValue], extra_key: &str, extra_val: &str) {
    let _ = write!(out, "{{");
    let mut first = true;
    for kv in labels.iter() {
        if !first {
            let _ = write!(out, ",");
        }
        first = false;
        let val = kv.value.as_str();
        let escaped_val = escape_label_value(val.as_ref());
        let _ = write!(out, "{}=\"{}\"", kv.key.as_str(), escaped_val);
    }
    if !first {
        let _ = write!(out, ",");
    }
    let escaped_extra_val = escape_label_value(extra_val);
    let _ = write!(out, "{}=\"{}\"", extra_key, escaped_extra_val);
    let _ = write!(out, "}}");
}

fn format_metric<S: Eq + Hash>(
    out: &mut String,
    prefix: &str,
    name: &str,
    desc: &str,
    metric_type: &str,
    metric_source: &ShardedU64<S>,
) {
    let data = metric_source.load_all();
    if data.is_empty() {
        return;
    }

    let full_name = format!("{prefix}_{name}");
    let _ = writeln!(out, "# HELP {full_name} {desc}");
    let _ = writeln!(out, "# TYPE {full_name} {metric_type}");

    for (labels, value) in data {
        let _ = write!(out, "{full_name}");
        write_metric_labels(out, &labels);
        let _ = writeln!(out, " {value}");
    }
}

fn format_gauge(out: &mut String, prefix: &str, name: &str, desc: &str, metric_type: &str, metric_source: &Gauge) {
    let data = metric_source.load_all();
    if data.is_empty() {
        return;
    }

    let full_name = format!("{prefix}_{name}");
    let _ = writeln!(out, "# HELP {full_name} {desc}");
    let _ = writeln!(out, "# TYPE {full_name} {metric_type}");

    for (labels, value) in data {
        let _ = write!(out, "{full_name}");
        write_metric_labels(out, &labels);
        let _ = writeln!(out, " {value}");
    }
}

fn format_histogram<S: Eq + Hash + Clone + Copy>(
    out: &mut String,
    prefix: &str,
    name: &str,
    desc: &str,
    metric_source: &ShardedHistogram<S>,
) {
    let count_data = metric_source.count().load_all();
    if count_data.is_empty() {
        return;
    }

    let full_name = format!("{prefix}_{name}");

    // Write HELP and TYPE for the histogram
    let _ = writeln!(out, "# HELP {full_name} {desc}");
    let _ = writeln!(out, "# TYPE {full_name} histogram");

    // Write buckets
    for (i, &bound) in metric_source.buckets().iter().enumerate() {
        let bound_str = if bound == u64::MAX { "+Inf".to_string() } else { bound.to_string() };
        let bucket_data = metric_source.counts()[i].load_all();
        for (labels, value) in bucket_data {
            let _ = write!(out, "{full_name}_bucket");
            write_metric_labels_with_extra(out, &labels, "le", &bound_str);
            let _ = writeln!(out, " {value}");
        }
    }

    // Write sum
    let sum_data = metric_source.sum().load_all();
    for (labels, value) in sum_data {
        let _ = write!(out, "{full_name}_sum");
        write_metric_labels(out, &labels);
        let _ = writeln!(out, " {value}");
    }

    // Write count
    for (labels, value) in count_data {
        let _ = write!(out, "{full_name}_count");
        write_metric_labels(out, &labels);
        let _ = writeln!(out, " {value}");
    }
}

fn process_counter<S: Eq + Hash>(out: &mut String, source: &OnceLock<Metric<ShardedU64<S>>>) {
    if let Some(metric) = source.get() {
        format_metric(out, metric.prefix, metric.name, metric.descr, "counter", &metric.value);
    }
}

fn process_gauge<S: Eq + Hash>(out: &mut String, source: &OnceLock<Metric<ShardedU64<S>>>) {
    if let Some(metric) = source.get() {
        format_metric(out, metric.prefix, metric.name, metric.descr, "gauge", &metric.value);
    }
}

fn process_histogram<S: Eq + Hash + Clone + Copy>(out: &mut String, source: &OnceLock<Metric<ShardedHistogram<S>>>) {
    if let Some(metric) = source.get() {
        format_histogram(out, metric.prefix, metric.name, metric.descr, &metric.value);
    }
}

pub(crate) async fn prometheus_handler(
    State(_): State<AdminState>,
) -> Result<(HeaderMap, String), (StatusCode, String)> {
    debug!(target: "prometheus", "prometheus_handler: running");
    update_server_metrics();

    // Pre-allocate a reasonable sized string buffer to avoid reallocations
    let mut out = String::with_capacity(16384);

    // listeners metrics
    process_counter(&mut out, &listeners::DOWNSTREAM_CX_TOTAL);
    process_counter(&mut out, &listeners::DOWNSTREAM_CX_DESTROY);
    process_gauge(&mut out, &listeners::DOWNSTREAM_CX_ACTIVE);
    process_counter(&mut out, &listeners::NO_FILTER_CHAIN_MATCH);
    process_histogram(&mut out, &listeners::DOWNSTREAM_CX_LENGTH_MS);

    // clusters metrics
    process_counter(&mut out, &clusters::UPSTREAM_RQ_TOTAL);
    process_gauge(&mut out, &clusters::UPSTREAM_RQ_ACTIVE);
    process_counter(&mut out, &clusters::UPSTREAM_RQ_TIMEOUT);
    process_counter(&mut out, &clusters::UPSTREAM_RQ_PER_TRY_TIMEOUT);
    process_counter(&mut out, &clusters::UPSTREAM_RQ_RETRY);

    process_counter(&mut out, &clusters::UPSTREAM_CX_TOTAL);
    process_counter(&mut out, &clusters::UPSTREAM_CX_IDLE_TIMEOUT);
    process_counter(&mut out, &clusters::UPSTREAM_CX_CONNECT_FAIL);
    process_counter(&mut out, &clusters::UPSTREAM_CX_CONNECT_TIMEOUT);
    process_counter(&mut out, &clusters::UPSTREAM_CX_DESTROY);
    process_gauge(&mut out, &clusters::UPSTREAM_CX_ACTIVE);

    // http metrics
    process_counter(&mut out, &http::DOWNSTREAM_CX_TOTAL);
    process_counter(&mut out, &http::DOWNSTREAM_CX_SSL_TOTAL);
    process_gauge(&mut out, &http::DOWNSTREAM_CX_SSL_ACTIVE);
    process_counter(&mut out, &http::DOWNSTREAM_CX_DESTROY);
    process_gauge(&mut out, &http::DOWNSTREAM_CX_ACTIVE);
    process_counter(&mut out, &http::DOWNSTREAM_RQ_1XX);
    process_counter(&mut out, &http::DOWNSTREAM_RQ_2XX);
    process_counter(&mut out, &http::DOWNSTREAM_RQ_3XX);
    process_counter(&mut out, &http::DOWNSTREAM_RQ_4XX);
    process_counter(&mut out, &http::DOWNSTREAM_RQ_5XX);
    process_counter(&mut out, &http::DOWNSTREAM_RQ_TOTAL);
    process_gauge(&mut out, &http::DOWNSTREAM_RQ_ACTIVE);
    process_counter(&mut out, &http::DOWNSTREAM_CX_RX_BYTES_TOTAL);
    process_counter(&mut out, &http::DOWNSTREAM_CX_TX_BYTES_TOTAL);
    process_histogram(&mut out, &http::DOWNSTREAM_CX_LENGTH_MS);

    // server metrics
    process_gauge(&mut out, &server::UPTIME);
    process_gauge(&mut out, &server::CONCURRENCY);
    process_gauge(&mut out, &server::MEMORY_HEAP_SIZE);
    process_gauge(&mut out, &server::MEMORY_PHYSICAL_SIZE);
    process_gauge(&mut out, &server::MEMORY_ALLOCATED);

    // tcp metrics
    process_counter(&mut out, &tcp::DOWNSTREAM_CX_TOTAL);
    process_counter(&mut out, &tcp::DOWNSTREAM_CX_DESTROY);
    process_gauge(&mut out, &tcp::DOWNSTREAM_CX_ACTIVE);
    process_histogram(&mut out, &tcp::DOWNSTREAM_CX_LENGTH_MS);
    process_gauge(&mut out, &tcp::CX_RX_BYTES_RECEIVED);
    process_gauge(&mut out, &tcp::CX_TX_BYTES_SENT);

    // tls
    process_counter(&mut out, &tls::HANDSHAKES);

    // websocket
    process_counter(&mut out, &http::DOWNSTREAM_CX_WS_UPGRADES_TOTAL);
    process_counter(&mut out, &http::DOWNSTREAM_CX_WS_UPGRADES_ACTIVE);
    process_counter(&mut out, &http::DOWNSTREAM_RQ_WS_ON_NON_WS_ROUTE);

    // user/agentrun
    process_counter(&mut out, &user::INVOCATIONS);
    process_counter(&mut out, &user::THROTTLES);
    process_counter(&mut out, &user::SYSTEM_ERRORS);
    process_counter(&mut out, &user::USER_ERRORS);
    process_counter(&mut out, &user::TOTAL_ERRORS);
    process_counter(&mut out, &user::BYTES_TX);
    process_counter(&mut out, &user::BYTES_RX);
    process_counter(&mut out, &user::INBOUND_STREAMING_BYTES_PROCESSED);
    process_counter(&mut out, &user::OUTBOUND_STREAMING_BYTES_PROCESSED);
    process_histogram(&mut out, &user::LATENCY);

    // filters
    process_counter(&mut out, &filters::CONNECTION_RATE_LIMIT);
    process_counter(&mut out, &filters::LOCAL_RATE_LIMIT);
    process_counter(&mut out, &filters::USER_RATE_LIMIT);

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
            );
        }
        for histogram in custom_metrics.histograms() {
            format_histogram(
                &mut out,
                histogram.metric.prefix,
                histogram.metric.name,
                histogram.metric.descr,
                &histogram.metric.value,
            );
        }
        for gauge in custom_metrics.gauges() {
            format_gauge(
                &mut out,
                gauge.metric.prefix,
                gauge.metric.name,
                gauge.metric.descr,
                "gauge",
                &gauge.metric.value,
            );
        }
    }

    let mut headers = HeaderMap::new();
    headers.insert(::http::header::CONTENT_TYPE, "text/plain; version=0.0.4".parse().unwrap());

    Ok((headers, out))
}
