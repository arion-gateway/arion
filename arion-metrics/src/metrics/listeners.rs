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

use std::{sync::OnceLock, thread::ThreadId};

use crate::{
    metrics::Metric,
    sharded::{ShardedHistogram, ShardedU64},
};
use opentelemetry::global;

pub static DOWNSTREAM_CX_TOTAL: OnceLock<Metric<ShardedU64<ThreadId>>> = OnceLock::new();
pub static DOWNSTREAM_CX_DESTROY: OnceLock<Metric<ShardedU64<ThreadId>>> = OnceLock::new();
pub static DOWNSTREAM_CX_ACTIVE: OnceLock<Metric<ShardedU64<ThreadId>>> = OnceLock::new();
pub static NO_FILTER_CHAIN_MATCH: OnceLock<Metric<ShardedU64<ThreadId>>> = OnceLock::new();
pub static DOWNSTREAM_CX_LENGTH_MS: OnceLock<Metric<ShardedHistogram<ThreadId>>> = OnceLock::new();

pub(crate) fn init_metrics(rename: &std::collections::HashMap<String, String>) {
    init_observable_histogram!(
        DOWNSTREAM_CX_LENGTH_MS,
        crate::metrics::PREFIX_LISTENERS,
        crate::metrics::resolve_metric_name(rename, "downstream_cx_length_ms"),
        "Connection length milliseconds",
        vec![5, 10, 50, 100, 500, 1000, 5000, 10000, u64::MAX]
    );

    init_observable_counter!(
        DOWNSTREAM_CX_TOTAL,
        crate::metrics::PREFIX_LISTENERS,
        crate::metrics::resolve_metric_name(rename, "downstream_cx_total"),
        "Total downstream connections"
    );
    init_observable_counter!(
        DOWNSTREAM_CX_DESTROY,
        crate::metrics::PREFIX_LISTENERS,
        crate::metrics::resolve_metric_name(rename, "downstream_cx_destroy"),
        "Total destroyed downstream connections"
    );
    init_observable_counter!(
        NO_FILTER_CHAIN_MATCH,
        crate::metrics::PREFIX_LISTENERS,
        crate::metrics::resolve_metric_name(rename, "no_filter_chain_match"),
        "Total connections with no filter chain match"
    );
    init_observable_gauge!(
        DOWNSTREAM_CX_ACTIVE,
        crate::metrics::PREFIX_LISTENERS,
        crate::metrics::resolve_metric_name(rename, "downstream_cx_active"),
        "Total active connections"
    );
}

pub fn reset_metrics() {
    use crate::sharded::Clearable;
    let metrics: &[&dyn Clearable] = &[
        &DOWNSTREAM_CX_TOTAL,
        &DOWNSTREAM_CX_DESTROY,
        &DOWNSTREAM_CX_ACTIVE,
        &NO_FILTER_CHAIN_MATCH,
        &DOWNSTREAM_CX_LENGTH_MS,
    ];
    for metric in metrics {
        metric.clear();
    }
}
