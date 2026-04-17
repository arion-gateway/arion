use crate::{
    metrics::Metric,
    sharded::{ShardedHistogram, ShardedU64},
};

use opentelemetry::global;
//use opentelemetry::metrics::Histogram;
use std::{sync::OnceLock, thread::ThreadId};

pub static INVOCATIONS: OnceLock<Metric<ShardedU64<ThreadId>>> = OnceLock::new();
pub static THROTTLES: OnceLock<Metric<ShardedU64<ThreadId>>> = OnceLock::new();
pub static SYSTEM_ERRORS: OnceLock<Metric<ShardedU64<ThreadId>>> = OnceLock::new();
pub static USER_ERRORS: OnceLock<Metric<ShardedU64<ThreadId>>> = OnceLock::new();
pub static TOTAL_ERRORS: OnceLock<Metric<ShardedU64<ThreadId>>> = OnceLock::new();
pub static BYTES_TX: OnceLock<Metric<ShardedU64<ThreadId>>> = OnceLock::new();
pub static BYTES_RX: OnceLock<Metric<ShardedU64<ThreadId>>> = OnceLock::new();

pub static INBOUND_STREAMING_BYTES_PROCESSED: OnceLock<Metric<ShardedU64<ThreadId>>> = OnceLock::new();
pub static OUTBOUND_STREAMING_BYTES_PROCESSED: OnceLock<Metric<ShardedU64<ThreadId>>> = OnceLock::new();

pub static LATENCY: OnceLock<Metric<ShardedHistogram<ThreadId>>> = OnceLock::new();

pub(crate) fn init_metrics() {
    init_observable_counter!(INVOCATIONS, "user", "invocations", "Total number of API calls");
    init_observable_counter!(THROTTLES, "user", "throttles", "Total number of API calls that were throttled");
    init_observable_counter!(
        SYSTEM_ERRORS,
        "user",
        "system_errors",
        "Total number of API calls that resulted in a system error"
    );
    init_observable_counter!(
        USER_ERRORS,
        "user",
        "user_errors",
        "Total number of API calls that resulted in a user error"
    );
    init_observable_counter!(
        TOTAL_ERRORS,
        "user",
        "total_errors",
        "Total number of API calls that resulted in any error"
    );
    init_observable_counter!(BYTES_TX, "user", "bytes_tx", "Total number of bytes transmitted in API calls");
    init_observable_counter!(BYTES_RX, "user", "bytes_rx", "Total number of bytes received in API calls");

    init_observable_counter!(
        INBOUND_STREAMING_BYTES_PROCESSED,
        "user",
        "inbound_streaming_bytes_processed",
        "Total number of bytes processed in inbound streaming API calls"
    );

    init_observable_counter!(
        OUTBOUND_STREAMING_BYTES_PROCESSED,
        "user",
        "outbound_streaming_bytes_processed",
        "Total number of bytes processed in outbound streaming API calls"
    );

    init_observable_histogram!(
        LATENCY,
        "user",
        "latency",
        "Latency of API calls in milliseconds",
        vec![5, 10, 50, 100, 500, 1000, 5000, 10000, u64::MAX]
    );
}
