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
pub static DOWNSTREAM_CX_DESTROY: OnceLock<Metric<ShardedU64<ThreadId>>> = OnceLock::new();
pub static DOWNSTREAM_CX_ACTIVE: OnceLock<Metric<ShardedU64<ThreadId>>> = OnceLock::new();
pub static DOWNSTREAM_CX_LENGTH_MS: OnceLock<Metric<ShardedHistogram<ThreadId>>> = OnceLock::new();

pub static CX_RX_BYTES_RECEIVED: OnceLock<Metric<ShardedU64<ThreadId>>> = OnceLock::new();
pub static CX_TX_BYTES_SENT: OnceLock<Metric<ShardedU64<ThreadId>>> = OnceLock::new();

pub(crate) fn init_metrics(rename: &std::collections::HashMap<String, String>) {
    init_observable_histogram!(
        DOWNSTREAM_CX_LENGTH_MS,
        crate::metrics::PREFIX_TCP,
        crate::metrics::resolve_metric_name(rename, "downstream_cx_length_ms"),
        "Connection length milliseconds",
        vec![5, 10, 50, 100, 500, 1000, 5000, 10000, u64::MAX]
    );

    init_observable_counter!(
        DOWNSTREAM_CX_TOTAL,
        crate::metrics::PREFIX_TCP,
        crate::metrics::resolve_metric_name(rename, "downstream_cx_total"),
        "Total number of downstream TCP connections"
    );
    init_observable_counter!(
        DOWNSTREAM_CX_DESTROY,
        crate::metrics::PREFIX_TCP,
        crate::metrics::resolve_metric_name(rename, "downstream_cx_destroy"),
        "Total number of destroyed downstream TCP connections"
    );
    init_observable_gauge!(
        DOWNSTREAM_CX_ACTIVE,
        crate::metrics::PREFIX_TCP,
        crate::metrics::resolve_metric_name(rename, "downstream_cx_active"),
        "Current number of active downstream TCP connections"
    );
    init_observable_counter!(
        CX_RX_BYTES_RECEIVED,
        crate::metrics::PREFIX_TCP,
        crate::metrics::resolve_metric_name(rename, "cx_rx_bytes_received"),
        "Total number of bytes received in TCP connections"
    );
    init_observable_counter!(
        CX_TX_BYTES_SENT,
        crate::metrics::PREFIX_TCP,
        crate::metrics::resolve_metric_name(rename, "cx_tx_bytes_sent"),
        "Total number of bytes sent in TCP connections"
    );
}

pub fn reset_metrics() {
    use crate::sharded::Clearable;
    let metrics: &[&dyn Clearable] = &[
        &DOWNSTREAM_CX_TOTAL,
        &DOWNSTREAM_CX_DESTROY,
        &DOWNSTREAM_CX_ACTIVE,
        &DOWNSTREAM_CX_LENGTH_MS,
        &CX_RX_BYTES_RECEIVED,
        &CX_TX_BYTES_SENT,
    ];
    for metric in metrics {
        metric.clear();
    }
}
