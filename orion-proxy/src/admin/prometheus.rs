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

use ahash::RandomState;
use std::collections::HashMap;

//use ::http::{header, StatusCode};
use axum::extract::State;
use orion_metrics::{
    metrics::{
        clusters, filters, http, listeners,
        server::{self, update_server_metrics},
        tcp, tls, user,
    },
    sharded::ShardedU64,
};
use prometheus::{Encoder, IntCounterVec, IntGaugeVec, Opts, Registry, TextEncoder};
use smallvec::SmallVec;

use crate::admin::AdminState;
use ::http::{header::HeaderMap, StatusCode};
use opentelemetry::KeyValue;
use std::hash::Hash;
use tracing::debug;

/// Populates a Prometheus `IntCounterVec` by reading from a `ShardedU64`.
fn populate_counter_vec<S: Eq + Hash>(metric_source: &ShardedU64<S>, prom_metric: &IntCounterVec, label_keys: &[&str]) {
    let data = metric_source.load_all();
    for (key_values, value) in data {
        let mut cow_values = smallvec::SmallVec::<[std::borrow::Cow<'_, str>; 4]>::new();
        for k in label_keys {
            let val = key_values
                .iter()
                .find(|kv| kv.key.as_str() == *k)
                .map(|kv| kv.value.as_str())
                .unwrap_or(std::borrow::Cow::Borrowed(""));
            cow_values.push(val);
        }
        let label_values: smallvec::SmallVec<[&str; 4]> = cow_values.iter().map(|cow| cow.as_ref()).collect();

        prom_metric.with_label_values(&label_values).inc_by(value);
    }
}

/// Populates a Prometheus `IntGaugeVec` by reading from a `ShardedU64`.
fn populate_gauge_vec<S: Eq + Hash>(metric_source: &ShardedU64<S>, prom_metric: &IntGaugeVec, label_keys: &[&str]) {
    let data = metric_source.load_all();
    for (key_values, value) in data {
        let mut cow_values = smallvec::SmallVec::<[std::borrow::Cow<'_, str>; 4]>::new();
        for k in label_keys {
            let val = key_values
                .iter()
                .find(|kv| kv.key.as_str() == *k)
                .map(|kv| kv.value.as_str())
                .unwrap_or(std::borrow::Cow::Borrowed(""));
            cow_values.push(val);
        }
        let label_values: smallvec::SmallVec<[&str; 4]> = cow_values.iter().map(|cow| cow.as_ref()).collect();

        prom_metric.with_label_values(&label_values).set(value as i64);
    }
}

/// Extracts all unique, sorted label keys from a given counter's data.
fn get_label_keys(data: &HashMap<Vec<KeyValue>, u64, RandomState>) -> SmallVec<[&str; 4]> {
    let mut sorted_keys = SmallVec::new();
    for kvs in data.keys() {
        for kv in kvs {
            let k = kv.key.as_str();
            if !sorted_keys.contains(&k) {
                sorted_keys.push(k);
            }
        }
    }
    sorted_keys.sort_unstable();
    sorted_keys
}

/// A macro to process and register a metric if its source is initialized.
macro_rules! process_metric {
    ($registry:expr, $source:expr, $metric_type:ty, $populate_fn:ident) => {
        if let Some(counter) = $source.get() {
            let data = counter.value.load_all();
            if !data.is_empty() {
                let label_keys = get_label_keys(&data);
                let prom_metric = <$metric_type>::new(
                    Opts::new(format!("{}_{}", counter.prefix, counter.name), counter.descr),
                    &label_keys,
                )
                .unwrap();
                $registry.register(Box::new(prom_metric.clone())).unwrap();
                $populate_fn(&counter.value, &prom_metric, &label_keys);
            }
        }
    };
}

macro_rules! process_histogram {
    ($registry:expr, $source:expr) => {
        if let Some(metric) = $source.get() {
            let count_data = metric.value.count().load_all();
            if !count_data.is_empty() {
                let label_keys = get_label_keys(&count_data);

                let count_metric = IntCounterVec::new(
                    Opts::new(format!("{}_{}_count", metric.prefix, metric.name), metric.descr),
                    &label_keys,
                )
                .unwrap();
                $registry.register(Box::new(count_metric.clone())).unwrap();
                populate_counter_vec(metric.value.count(), &count_metric, &label_keys);

                let sum_metric = IntCounterVec::new(
                    Opts::new(format!("{}_{}_sum", metric.prefix, metric.name), metric.descr),
                    &label_keys,
                )
                .unwrap();
                $registry.register(Box::new(sum_metric.clone())).unwrap();
                populate_counter_vec(metric.value.sum(), &sum_metric, &label_keys);

                let mut bucket_label_keys = label_keys.clone();
                bucket_label_keys.push("le");
                let bucket_metric = IntCounterVec::new(
                    Opts::new(format!("{}_{}_bucket", metric.prefix, metric.name), metric.descr),
                    &bucket_label_keys,
                )
                .unwrap();
                $registry.register(Box::new(bucket_metric.clone())).unwrap();

                for (i, &bound) in metric.value.buckets().iter().enumerate() {
                    let bound_str = if bound == u64::MAX { "+Inf".to_string() } else { bound.to_string() };
                    let bucket_data = metric.value.counts()[i].load_all();
                    for (key_values, value) in bucket_data {
                        let mut cow_values = smallvec::SmallVec::<[std::borrow::Cow<'_, str>; 4]>::new();
                        for k in &label_keys {
                            let val = key_values
                                .iter()
                                .find(|kv| kv.key.as_str() == *k)
                                .map(|kv| kv.value.as_str())
                                .unwrap_or(std::borrow::Cow::Borrowed(""));
                            cow_values.push(val);
                        }
                        cow_values.push(std::borrow::Cow::Owned(bound_str.clone()));
                        let label_values: smallvec::SmallVec<[&str; 4]> =
                            cow_values.iter().map(|cow| cow.as_ref()).collect();
                        bucket_metric.with_label_values(&label_values).inc_by(value);
                    }
                }
            }
        }
    };
}

pub(crate) async fn prometheus_handler(
    State(_): State<AdminState>,
) -> Result<(HeaderMap, String), (StatusCode, String)> {
    let registry = Registry::new();

    debug!(target: "prometheus", "prometheus_handler: running");
    update_server_metrics();

    // listeners metrics
    process_metric!(registry, &listeners::DOWNSTREAM_CX_TOTAL, IntCounterVec, populate_counter_vec);
    process_metric!(registry, &listeners::DOWNSTREAM_CX_DESTROY, IntCounterVec, populate_counter_vec);
    process_metric!(registry, &listeners::DOWNSTREAM_CX_ACTIVE, IntGaugeVec, populate_gauge_vec);
    process_metric!(registry, &listeners::NO_FILTER_CHAIN_MATCH, IntCounterVec, populate_counter_vec);
    process_histogram!(registry, &listeners::DOWNSTREAM_CX_LENGTH_MS);

    // clusters metrics
    process_metric!(registry, &clusters::UPSTREAM_RQ_TOTAL, IntCounterVec, populate_counter_vec);
    process_metric!(registry, &clusters::UPSTREAM_RQ_ACTIVE, IntGaugeVec, populate_gauge_vec);
    process_metric!(registry, &clusters::UPSTREAM_RQ_TIMEOUT, IntCounterVec, populate_counter_vec);
    process_metric!(registry, &clusters::UPSTREAM_RQ_PER_TRY_TIMEOUT, IntCounterVec, populate_counter_vec);
    process_metric!(registry, &clusters::UPSTREAM_RQ_RETRY, IntCounterVec, populate_counter_vec);

    process_metric!(registry, &clusters::UPSTREAM_CX_TOTAL, IntCounterVec, populate_counter_vec);
    process_metric!(registry, &clusters::UPSTREAM_CX_IDLE_TIMEOUT, IntCounterVec, populate_counter_vec);
    process_metric!(registry, &clusters::UPSTREAM_CX_CONNECT_FAIL, IntCounterVec, populate_counter_vec);
    process_metric!(registry, &clusters::UPSTREAM_CX_CONNECT_TIMEOUT, IntCounterVec, populate_counter_vec);
    process_metric!(registry, &clusters::UPSTREAM_CX_DESTROY, IntCounterVec, populate_counter_vec);
    process_metric!(registry, &clusters::UPSTREAM_CX_ACTIVE, IntGaugeVec, populate_gauge_vec);

    // http metrics
    process_metric!(registry, &http::DOWNSTREAM_CX_TOTAL, IntCounterVec, populate_counter_vec);
    process_metric!(registry, &http::DOWNSTREAM_CX_SSL_TOTAL, IntCounterVec, populate_counter_vec);
    process_metric!(registry, &http::DOWNSTREAM_CX_SSL_ACTIVE, IntGaugeVec, populate_gauge_vec);
    process_metric!(registry, &http::DOWNSTREAM_CX_DESTROY, IntCounterVec, populate_counter_vec);
    process_metric!(registry, &http::DOWNSTREAM_CX_ACTIVE, IntGaugeVec, populate_gauge_vec);
    process_metric!(registry, &http::DOWNSTREAM_RQ_1XX, IntCounterVec, populate_counter_vec);
    process_metric!(registry, &http::DOWNSTREAM_RQ_2XX, IntCounterVec, populate_counter_vec);
    process_metric!(registry, &http::DOWNSTREAM_RQ_3XX, IntCounterVec, populate_counter_vec);
    process_metric!(registry, &http::DOWNSTREAM_RQ_4XX, IntCounterVec, populate_counter_vec);
    process_metric!(registry, &http::DOWNSTREAM_RQ_5XX, IntCounterVec, populate_counter_vec);
    process_metric!(registry, &http::DOWNSTREAM_RQ_TOTAL, IntCounterVec, populate_counter_vec);
    process_metric!(registry, &http::DOWNSTREAM_RQ_ACTIVE, IntGaugeVec, populate_gauge_vec);
    process_metric!(registry, &http::DOWNSTREAM_CX_RX_BYTES_TOTAL, IntCounterVec, populate_counter_vec);
    process_metric!(registry, &http::DOWNSTREAM_CX_TX_BYTES_TOTAL, IntCounterVec, populate_counter_vec);
    process_histogram!(registry, &http::DOWNSTREAM_CX_LENGTH_MS);

    // server metrics
    process_metric!(registry, &server::UPTIME, IntGaugeVec, populate_gauge_vec);
    process_metric!(registry, &server::CONCURRENCY, IntGaugeVec, populate_gauge_vec);
    process_metric!(registry, &server::MEMORY_HEAP_SIZE, IntGaugeVec, populate_gauge_vec);
    process_metric!(registry, &server::MEMORY_PHYSICAL_SIZE, IntGaugeVec, populate_gauge_vec);
    process_metric!(registry, &server::MEMORY_ALLOCATED, IntGaugeVec, populate_gauge_vec);

    // tcp metrics
    process_metric!(registry, &tcp::DOWNSTREAM_CX_TOTAL, IntCounterVec, populate_counter_vec);
    process_metric!(registry, &tcp::DOWNSTREAM_CX_DESTROY, IntCounterVec, populate_counter_vec);
    process_metric!(registry, &tcp::DOWNSTREAM_CX_ACTIVE, IntGaugeVec, populate_gauge_vec);
    process_histogram!(registry, &tcp::DOWNSTREAM_CX_LENGTH_MS);
    process_metric!(registry, &tcp::CX_RX_BYTES_RECEIVED, IntGaugeVec, populate_gauge_vec);
    process_metric!(registry, &tcp::CX_TX_BYTES_SENT, IntGaugeVec, populate_gauge_vec);

    // tls
    process_metric!(registry, &tls::HANDSHAKES, IntCounterVec, populate_counter_vec);

    // websocket
    process_metric!(registry, http::DOWNSTREAM_CX_WS_UPGRADES_TOTAL, IntCounterVec, populate_counter_vec);
    process_metric!(registry, http::DOWNSTREAM_CX_WS_UPGRADES_ACTIVE, IntCounterVec, populate_counter_vec);
    process_metric!(registry, http::DOWNSTREAM_RQ_WS_ON_NON_WS_ROUTE, IntCounterVec, populate_counter_vec);

    // user/agentrun
    process_metric!(registry, &user::INVOCATIONS, IntCounterVec, populate_counter_vec);
    process_metric!(registry, &user::THROTTLES, IntCounterVec, populate_counter_vec);
    process_metric!(registry, &user::SYSTEM_ERRORS, IntCounterVec, populate_counter_vec);
    process_metric!(registry, &user::USER_ERRORS, IntCounterVec, populate_counter_vec);
    process_metric!(registry, &user::TOTAL_ERRORS, IntCounterVec, populate_counter_vec);
    process_metric!(registry, &user::BYTES_TX, IntCounterVec, populate_counter_vec);
    process_metric!(registry, &user::BYTES_RX, IntCounterVec, populate_counter_vec);
    process_metric!(registry, &user::INBOUND_STREAMING_BYTES_PROCESSED, IntCounterVec, populate_counter_vec);
    process_metric!(registry, &user::OUTBOUND_STREAMING_BYTES_PROCESSED, IntCounterVec, populate_counter_vec);
    process_histogram!(registry, &user::LATENCY);

    // filters
    process_metric!(registry, &filters::CONNECTION_RATE_LIMIT, IntCounterVec, populate_counter_vec);
    process_metric!(registry, &filters::LOCAL_RATE_LIMIT, IntCounterVec, populate_counter_vec);
    process_metric!(registry, &filters::USER_RATE_LIMIT, IntCounterVec, populate_counter_vec);

    // Encode and return

    let encoder = TextEncoder::new();
    let mut buffer = vec![];

    encoder
        .encode(&registry.gather(), &mut buffer)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to encode metrics: {e}")))?;

    let body = String::from_utf8(buffer)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Metrics not valid UTF-8: {e}")))?;

    let mut headers = HeaderMap::new();
    let content_type = encoder
        .format_type()
        .parse()
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to parse content type: {e}")))?;
    headers.insert(::http::header::CONTENT_TYPE, content_type);

    Ok((headers, body))
}
