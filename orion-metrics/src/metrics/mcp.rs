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

pub static EMBEDDING_FAILURES_TOTAL: OnceLock<Metric<ShardedU64<ThreadId>>> = OnceLock::new();
pub static TOOL_RQ_TOTAL: OnceLock<Metric<ShardedU64<ThreadId>>> = OnceLock::new();
pub static TOOL_RQ_FAILURES_TOTAL: OnceLock<Metric<ShardedU64<ThreadId>>> = OnceLock::new();
pub static TOOL_RQ_TIME: OnceLock<Metric<ShardedHistogram<ThreadId>>> = OnceLock::new();
pub static TOOL_RESPONSE_BYTES: OnceLock<Metric<ShardedHistogram<ThreadId>>> = OnceLock::new();

pub(crate) fn init_metrics() {
    init_observable_counter!(
        EMBEDDING_FAILURES_TOTAL,
        "mcp",
        "embedding_failures_total",
        "Total number of MCP tool embedding generation failures"
    );
    init_observable_counter!(
        TOOL_RQ_TOTAL,
        "mcp",
        "tool_rq_total",
        "Total number of MCP tool invocations dispatched to an upstream backend"
    );
    init_observable_counter!(
        TOOL_RQ_FAILURES_TOTAL,
        "mcp",
        "tool_rq_failures_total",
        "Total number of MCP tool invocations that produced a tool error, keyed by error code"
    );
    init_observable_histogram!(
        TOOL_RQ_TIME,
        "mcp",
        "tool_rq_time",
        "MCP tool invocation time in milliseconds, including upstream acquisition",
        vec![5, 10, 50, 100, 500, 1000, 5000, 10000, u64::MAX]
    );
    init_observable_histogram!(
        TOOL_RESPONSE_BYTES,
        "mcp",
        "tool_response_bytes",
        "Bytes of upstream tool response payload counted against max_upstream_response_bytes",
        vec![256, 1024, 4096, 16384, 65536, 262144, 1048576, 4194304, u64::MAX]
    );
}
