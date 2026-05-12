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

pub(crate) fn init_metrics() {
    init_observable_histogram!(
        DOWNSTREAM_CX_LENGTH_MS,
        crate::metrics::PREFIX_LISTENERS,
        "downstream_cx_length_ms",
        "Connection length milliseconds",
        vec![5, 10, 50, 100, 500, 1000, 5000, 10000, u64::MAX]
    );

    init_observable_counter!(DOWNSTREAM_CX_TOTAL, crate::metrics::PREFIX_LISTENERS, "downstream_cx_total", "Total downstream connections");
    init_observable_counter!(
        DOWNSTREAM_CX_DESTROY,
        crate::metrics::PREFIX_LISTENERS,
        "downstream_cx_destroy",
        "Total destroyed downstream connections"
    );
    init_observable_counter!(
        NO_FILTER_CHAIN_MATCH,
        crate::metrics::PREFIX_LISTENERS,
        "no_filter_chain_match",
        "Total connections with no filter chain match"
    );
    init_observable_gauge!(DOWNSTREAM_CX_ACTIVE, crate::metrics::PREFIX_LISTENERS, "downstream_cx_active", "Total active connections");
}
