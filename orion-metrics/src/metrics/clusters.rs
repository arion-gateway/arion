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

use crate::{
    metrics::Metric,
    sharded::{ShardedHistogram, ShardedU64},
};
use opentelemetry::global;
use std::{sync::OnceLock, thread::ThreadId};

pub static UPSTREAM_RQ_TOTAL: OnceLock<Metric<ShardedU64<ThreadId>>> = OnceLock::new();
pub static UPSTREAM_RQ_ACTIVE: OnceLock<Metric<ShardedU64<ThreadId>>> = OnceLock::new();
pub static UPSTREAM_RQ_TIME: OnceLock<Metric<ShardedHistogram<ThreadId>>> = OnceLock::new();

pub static UPSTREAM_RQ_TIMEOUT: OnceLock<Metric<ShardedU64<ThreadId>>> = OnceLock::new();
pub static UPSTREAM_RQ_PER_TRY_TIMEOUT: OnceLock<Metric<ShardedU64<ThreadId>>> = OnceLock::new();
pub static UPSTREAM_RQ_RETRY: OnceLock<Metric<ShardedU64<ThreadId>>> = OnceLock::new();

// Currently, we use the `http::uri::Authority` type to identify endpoints.
// Each thread runs a separate Hyper client instance for each endpoint,
// so the shard ID is derived from a combination of the thread ID and the authority.
// Metrics are aggregated using the `ShardedU64` type.

pub static UPSTREAM_CX_TOTAL: OnceLock<Metric<ShardedU64<ThreadId>>> = OnceLock::new();
pub static UPSTREAM_CX_ACTIVE: OnceLock<Metric<ShardedU64<ThreadId>>> = OnceLock::new();
pub static UPSTREAM_CX_DESTROY: OnceLock<Metric<ShardedU64<ThreadId>>> = OnceLock::new();
pub static UPSTREAM_CX_IDLE_TIMEOUT: OnceLock<Metric<ShardedU64<ThreadId>>> = OnceLock::new();
pub static UPSTREAM_CX_CONNECT_FAIL: OnceLock<Metric<ShardedU64<ThreadId>>> = OnceLock::new();
pub static UPSTREAM_CX_CONNECT_TIMEOUT: OnceLock<Metric<ShardedU64<ThreadId>>> = OnceLock::new();
pub static UPSTREAM_CX_RX_BYTES_TOTAL: OnceLock<Metric<ShardedU64<ThreadId>>> = OnceLock::new();
pub static UPSTREAM_CX_TX_BYTES_TOTAL: OnceLock<Metric<ShardedU64<ThreadId>>> = OnceLock::new();
pub static UPSTREAM_CX_OVERFLOW: OnceLock<Metric<ShardedU64<ThreadId>>> = OnceLock::new();
pub static UPSTREAM_RQ_OVERFLOW: OnceLock<Metric<ShardedU64<ThreadId>>> = OnceLock::new();
pub static UPSTREAM_RQ_RETRY_OVERFLOW: OnceLock<Metric<ShardedU64<ThreadId>>> = OnceLock::new();

pub(crate) fn init_metrics(rename: &std::collections::HashMap<String, String>) {
    init_observable_counter!(
        UPSTREAM_RQ_TOTAL,
        crate::metrics::PREFIX_CLUSTER,
        crate::metrics::resolve_metric_name(rename, "upstream_rq_total"),
        "Total number of upstream requests"
    );
    init_observable_gauge!(
        UPSTREAM_RQ_ACTIVE,
        crate::metrics::PREFIX_CLUSTER,
        crate::metrics::resolve_metric_name(rename, "upstream_rq_active"),
        "Number of active upstream requests"
    );
    init_observable_histogram!(
        UPSTREAM_RQ_TIME,
        crate::metrics::PREFIX_CLUSTER,
        crate::metrics::resolve_metric_name(rename, "upstream_rq_time"),
        "Upstream request time in milliseconds",
        vec![5, 10, 50, 100, 500, 1000, 5000, 10000, u64::MAX]
    );
    init_observable_counter!(
        UPSTREAM_RQ_TIMEOUT,
        crate::metrics::PREFIX_CLUSTER,
        crate::metrics::resolve_metric_name(rename, "upstream_rq_timeout"),
        "Total upstream request timeouts"
    );
    init_observable_counter!(
        UPSTREAM_RQ_PER_TRY_TIMEOUT,
        crate::metrics::PREFIX_CLUSTER,
        crate::metrics::resolve_metric_name(rename, "upstream_rq_per_try_timeout"),
        "Total upstream request timeouts per try"
    );
    init_observable_counter!(
        UPSTREAM_RQ_RETRY,
        crate::metrics::PREFIX_CLUSTER,
        crate::metrics::resolve_metric_name(rename, "upstream_rq_retry"),
        "Total upstream request retries"
    );
    init_observable_counter!(
        UPSTREAM_CX_TOTAL,
        crate::metrics::PREFIX_CLUSTER,
        crate::metrics::resolve_metric_name(rename, "upstream_cx_total"),
        "Total upstream connections"
    );
    init_observable_counter!(
        UPSTREAM_CX_CONNECT_FAIL,
        crate::metrics::PREFIX_CLUSTER,
        crate::metrics::resolve_metric_name(rename, "upstream_cx_connect_fail"),
        "Total upstream connection failures"
    );
    init_observable_counter!(
        UPSTREAM_CX_CONNECT_TIMEOUT,
        crate::metrics::PREFIX_CLUSTER,
        crate::metrics::resolve_metric_name(rename, "upstream_cx_connect_timeout"),
        "Total upstream connection timeouts"
    );
    init_observable_counter!(
        UPSTREAM_CX_IDLE_TIMEOUT,
        crate::metrics::PREFIX_CLUSTER,
        crate::metrics::resolve_metric_name(rename, "upstream_cx_idle_timeout"),
        "Total upstream connections idle timeout"
    );
    init_observable_counter!(
        UPSTREAM_CX_DESTROY,
        crate::metrics::PREFIX_CLUSTER,
        crate::metrics::resolve_metric_name(rename, "upstream_cx_destroy"),
        "Total upstream connections destroyed"
    );
    init_observable_gauge!(
        UPSTREAM_CX_ACTIVE,
        crate::metrics::PREFIX_CLUSTER,
        crate::metrics::resolve_metric_name(rename, "upstream_cx_active"),
        "Number of active connections"
    );

    init_observable_counter!(
        UPSTREAM_CX_OVERFLOW,
        "cluster",
        "upstream_cx_overflow",
        "Total connections rejected by circuit breaker"
    );
    init_observable_counter!(
        UPSTREAM_RQ_OVERFLOW,
        "cluster",
        "upstream_rq_overflow",
        "Total requests rejected by circuit breaker"
    );
    init_observable_counter!(
        UPSTREAM_RQ_RETRY_OVERFLOW,
        "cluster",
        "upstream_rq_retry_overflow",
        "Total retries rejected by circuit breaker"
    );
    init_observable_counter!(
        UPSTREAM_CX_RX_BYTES_TOTAL,
        crate::metrics::PREFIX_CLUSTER,
        crate::metrics::resolve_metric_name(rename, "upstream_cx_rx_bytes_total"),
        "Total upstream bytes received"
    );
    init_observable_counter!(
        UPSTREAM_CX_TX_BYTES_TOTAL,
        crate::metrics::PREFIX_CLUSTER,
        crate::metrics::resolve_metric_name(rename, "upstream_cx_tx_bytes_total"),
        "Total upstream bytes sent"
    );
}

pub fn reset_metrics() {
    use crate::sharded::Clearable;
    let metrics: &[&dyn Clearable] = &[
        &UPSTREAM_RQ_TOTAL,
        &UPSTREAM_RQ_ACTIVE,
        &UPSTREAM_RQ_TIMEOUT,
        &UPSTREAM_RQ_PER_TRY_TIMEOUT,
        &UPSTREAM_RQ_RETRY,
        &UPSTREAM_CX_TOTAL,
        &UPSTREAM_CX_ACTIVE,
        &UPSTREAM_CX_DESTROY,
        &UPSTREAM_CX_IDLE_TIMEOUT,
        &UPSTREAM_CX_CONNECT_FAIL,
        &UPSTREAM_CX_CONNECT_TIMEOUT,
        &UPSTREAM_CX_RX_BYTES_TOTAL,
        &UPSTREAM_CX_TX_BYTES_TOTAL,
        &UPSTREAM_CX_OVERFLOW,
        &UPSTREAM_RQ_OVERFLOW,
        &UPSTREAM_RQ_RETRY_OVERFLOW,
        &UPSTREAM_RQ_TIME,
    ];
    for metric in metrics {
        metric.clear();
    }
}
