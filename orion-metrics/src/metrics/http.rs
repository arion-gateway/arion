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

pub static DOWNSTREAM_CX_TOTAL: OnceLock<Metric<ShardedU64<ThreadId>>> = OnceLock::new();
pub static DOWNSTREAM_CX_SSL_TOTAL: OnceLock<Metric<ShardedU64<ThreadId>>> = OnceLock::new();
pub static DOWNSTREAM_CX_SSL_ACTIVE: OnceLock<Metric<ShardedU64<ThreadId>>> = OnceLock::new();
pub static DOWNSTREAM_CX_DESTROY: OnceLock<Metric<ShardedU64<ThreadId>>> = OnceLock::new();
pub static DOWNSTREAM_CX_ACTIVE: OnceLock<Metric<ShardedU64<ThreadId>>> = OnceLock::new();
pub static DOWNSTREAM_CX_LENGTH_MS: OnceLock<Metric<ShardedHistogram<ThreadId>>> = OnceLock::new();

pub static DOWNSTREAM_CX_WS_UPGRADES_TOTAL: OnceLock<Metric<ShardedU64<ThreadId>>> = OnceLock::new();
pub static DOWNSTREAM_CX_WS_UPGRADES_ACTIVE: OnceLock<Metric<ShardedU64<ThreadId>>> = OnceLock::new();
pub static DOWNSTREAM_RQ_WS_ON_NON_WS_ROUTE: OnceLock<Metric<ShardedU64<ThreadId>>> = OnceLock::new();

pub static DOWNSTREAM_RQ_1XX: OnceLock<Metric<ShardedU64<ThreadId>>> = OnceLock::new();
pub static DOWNSTREAM_RQ_2XX: OnceLock<Metric<ShardedU64<ThreadId>>> = OnceLock::new();
pub static DOWNSTREAM_RQ_3XX: OnceLock<Metric<ShardedU64<ThreadId>>> = OnceLock::new();
pub static DOWNSTREAM_RQ_4XX: OnceLock<Metric<ShardedU64<ThreadId>>> = OnceLock::new();
pub static DOWNSTREAM_RQ_5XX: OnceLock<Metric<ShardedU64<ThreadId>>> = OnceLock::new();

pub static DOWNSTREAM_RQ_TOTAL: OnceLock<Metric<ShardedU64<ThreadId>>> = OnceLock::new();
pub static DOWNSTREAM_RQ_ACTIVE: OnceLock<Metric<ShardedU64<ThreadId>>> = OnceLock::new();
pub static DOWNSTREAM_CX_RX_BYTES_TOTAL: OnceLock<Metric<ShardedU64<ThreadId>>> = OnceLock::new();
pub static DOWNSTREAM_CX_TX_BYTES_TOTAL: OnceLock<Metric<ShardedU64<ThreadId>>> = OnceLock::new();

pub(crate) fn init_metrics(rename: &std::collections::HashMap<String, String>) {
    init_observable_histogram!(
        DOWNSTREAM_CX_LENGTH_MS,
        crate::metrics::PREFIX_HTTP,
        crate::metrics::resolve_metric_name(rename, "downstream_cx_length_ms"),
        "Connection length milliseconds",
        vec![5, 10, 50, 100, 500, 1000, 5000, 10000, u64::MAX]
    );

    init_observable_counter!(
        DOWNSTREAM_RQ_1XX,
        crate::metrics::PREFIX_HTTP,
        crate::metrics::resolve_metric_name(rename, "downstream_rq_1xx"),
        "1xx responses to downstream HTTP requests"
    );
    init_observable_counter!(
        DOWNSTREAM_RQ_2XX,
        crate::metrics::PREFIX_HTTP,
        crate::metrics::resolve_metric_name(rename, "downstream_rq_2xx"),
        "2xx responses to downstream HTTP requests"
    );
    init_observable_counter!(
        DOWNSTREAM_RQ_3XX,
        crate::metrics::PREFIX_HTTP,
        crate::metrics::resolve_metric_name(rename, "downstream_rq_3xx"),
        "3xx responses to downstream HTTP requests"
    );
    init_observable_counter!(
        DOWNSTREAM_RQ_4XX,
        crate::metrics::PREFIX_HTTP,
        crate::metrics::resolve_metric_name(rename, "downstream_rq_4xx"),
        "4xx responses to downstream HTTP requests"
    );
    init_observable_counter!(
        DOWNSTREAM_RQ_5XX,
        crate::metrics::PREFIX_HTTP,
        crate::metrics::resolve_metric_name(rename, "downstream_rq_5xx"),
        "5xx responses to downstream HTTP requests"
    );
    init_observable_counter!(
        DOWNSTREAM_CX_TOTAL,
        crate::metrics::PREFIX_HTTP,
        crate::metrics::resolve_metric_name(rename, "downstream_cx_total"),
        "Total number of downstream HTTP connections"
    );
    init_observable_counter!(
        DOWNSTREAM_CX_SSL_TOTAL,
        crate::metrics::PREFIX_HTTP,
        crate::metrics::resolve_metric_name(rename, "downstream_cx_ssl_total"),
        "Total number of downstream HTTP connections with TLS"
    );
    init_observable_gauge!(
        DOWNSTREAM_CX_SSL_ACTIVE,
        crate::metrics::PREFIX_HTTP,
        crate::metrics::resolve_metric_name(rename, "downstream_cx_ssl_active"),
        "Active downstream HTTP connections with TLS"
    );

    init_observable_counter!(
        DOWNSTREAM_RQ_TOTAL,
        crate::metrics::PREFIX_HTTP,
        crate::metrics::resolve_metric_name(rename, "downstream_rq_total"),
        "Total number of downstream HTTP requests"
    );
    init_observable_gauge!(
        DOWNSTREAM_RQ_ACTIVE,
        crate::metrics::PREFIX_HTTP,
        crate::metrics::resolve_metric_name(rename, "downstream_rq_active"),
        "Active downstream HTTP requests"
    );

    init_observable_counter!(
        DOWNSTREAM_CX_DESTROY,
        crate::metrics::PREFIX_HTTP,
        crate::metrics::resolve_metric_name(rename, "downstream_cx_destroy"),
        "Number of destroyed downstream HTTP connections"
    );
    init_observable_gauge!(
        DOWNSTREAM_CX_ACTIVE,
        crate::metrics::PREFIX_HTTP,
        crate::metrics::resolve_metric_name(rename, "downstream_cx_active"),
        "Active downstream HTTP connections"
    );

    init_observable_counter!(
        DOWNSTREAM_CX_RX_BYTES_TOTAL,
        crate::metrics::PREFIX_HTTP,
        crate::metrics::resolve_metric_name(rename, "downstream_cx_rx_bytes_total"),
        "Total number of bytes received on downstream HTTP connections"
    );
    init_observable_counter!(
        DOWNSTREAM_CX_TX_BYTES_TOTAL,
        crate::metrics::PREFIX_HTTP,
        crate::metrics::resolve_metric_name(rename, "downstream_cx_tx_bytes_total"),
        "Total number of bytes sent on downstream HTTP connections"
    );

    init_observable_counter!(
        DOWNSTREAM_CX_WS_UPGRADES_TOTAL,
        crate::metrics::PREFIX_HTTP,
        crate::metrics::resolve_metric_name(rename, "downstream_cx_ws_upgrades_total"),
        "Total successfully upgraded connections"
    );
    init_observable_gauge!(
        DOWNSTREAM_CX_WS_UPGRADES_ACTIVE,
        crate::metrics::PREFIX_HTTP,
        crate::metrics::resolve_metric_name(rename, "downstream_cx_ws_upgrades_active"),
        "Total active upgraded connections"
    );
    init_observable_counter!(
        DOWNSTREAM_RQ_WS_ON_NON_WS_ROUTE,
        crate::metrics::PREFIX_HTTP,
        crate::metrics::resolve_metric_name(rename, "downstream_rq_ws_on_non_ws_route"),
        "Total upgrade requests rejected by non upgrade routes"
    );
}
