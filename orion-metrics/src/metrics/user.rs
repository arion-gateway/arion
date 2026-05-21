use crate::{
    metrics::Metric,
    sharded::{ShardedHistogram, ShardedU64},
};

use opentelemetry::global;
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

pub static HTTP_1XX_RESPONSES: OnceLock<Metric<ShardedU64<ThreadId>>> = OnceLock::new();
pub static HTTP_2XX_RESPONSES: OnceLock<Metric<ShardedU64<ThreadId>>> = OnceLock::new();
pub static HTTP_3XX_RESPONSES: OnceLock<Metric<ShardedU64<ThreadId>>> = OnceLock::new();
pub static HTTP_4XX_RESPONSES: OnceLock<Metric<ShardedU64<ThreadId>>> = OnceLock::new();
pub static HTTP_5XX_RESPONSES: OnceLock<Metric<ShardedU64<ThreadId>>> = OnceLock::new();

pub(crate) fn init_metrics(rename: &std::collections::HashMap<String, String>) {
    init_observable_counter!(
        INVOCATIONS,
        crate::metrics::PREFIX_USER,
        crate::metrics::resolve_metric_name(rename, "invocations"),
        "Total number of API calls"
    );
    init_observable_counter!(
        THROTTLES,
        crate::metrics::PREFIX_USER,
        crate::metrics::resolve_metric_name(rename, "throttles"),
        "Total number of API calls that were throttled"
    );
    init_observable_counter!(
        SYSTEM_ERRORS,
        crate::metrics::PREFIX_USER,
        crate::metrics::resolve_metric_name(rename, "system_errors"),
        "Total number of API calls that resulted in a system error"
    );
    init_observable_counter!(
        USER_ERRORS,
        crate::metrics::PREFIX_USER,
        crate::metrics::resolve_metric_name(rename, "user_errors"),
        "Total number of API calls that resulted in a user error"
    );
    init_observable_counter!(
        TOTAL_ERRORS,
        crate::metrics::PREFIX_USER,
        crate::metrics::resolve_metric_name(rename, "total_errors"),
        "Total number of API calls that resulted in any error"
    );
    init_observable_counter!(
        BYTES_TX,
        crate::metrics::PREFIX_USER,
        crate::metrics::resolve_metric_name(rename, "bytes_tx"),
        "Total number of bytes transmitted in API calls"
    );
    init_observable_counter!(
        BYTES_RX,
        crate::metrics::PREFIX_USER,
        crate::metrics::resolve_metric_name(rename, "bytes_rx"),
        "Total number of bytes received in API calls"
    );

    init_observable_counter!(
        INBOUND_STREAMING_BYTES_PROCESSED,
        crate::metrics::PREFIX_USER,
        crate::metrics::resolve_metric_name(rename, "inbound_streaming_bytes_processed"),
        "Total number of bytes processed in inbound streaming API calls"
    );

    init_observable_counter!(
        OUTBOUND_STREAMING_BYTES_PROCESSED,
        crate::metrics::PREFIX_USER,
        crate::metrics::resolve_metric_name(rename, "outbound_streaming_bytes_processed"),
        "Total number of bytes processed in outbound streaming API calls"
    );

    init_observable_histogram!(
        LATENCY,
        crate::metrics::PREFIX_USER,
        crate::metrics::resolve_metric_name(rename, "latency"),
        "Latency of API calls in milliseconds",
        vec![5, 10, 50, 100, 500, 1000, 5000, 10000, u64::MAX]
    );
    init_observable_counter!(
        HTTP_1XX_RESPONSES,
        crate::metrics::PREFIX_USER,
        crate::metrics::resolve_metric_name(rename, "http_1xx_response"),
        "Total number of API calls that resulted in a 1xx HTTP response"
    );
    init_observable_counter!(
        HTTP_2XX_RESPONSES,
        crate::metrics::PREFIX_USER,
        crate::metrics::resolve_metric_name(rename, "http_2xx_response"),
        "Total number of API calls that resulted in a 2xx HTTP response"
    );
    init_observable_counter!(
        HTTP_3XX_RESPONSES,
        crate::metrics::PREFIX_USER,
        crate::metrics::resolve_metric_name(rename, "http_3xx_response"),
        "Total number of API calls that resulted in a 3xx HTTP response"
    );
    init_observable_counter!(
        HTTP_4XX_RESPONSES,
        crate::metrics::PREFIX_USER,
        crate::metrics::resolve_metric_name(rename, "http_4xx_response"),
        "Total number of API calls that resulted in a 4xx HTTP response"
    );
    init_observable_counter!(
        HTTP_5XX_RESPONSES,
        crate::metrics::PREFIX_USER,
        crate::metrics::resolve_metric_name(rename, "http_5xx_response"),
        "Total number of API calls that resulted in a 5xx HTTP response"
    );
}
