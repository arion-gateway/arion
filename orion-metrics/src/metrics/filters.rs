use crate::{metrics::Metric, sharded::ShardedU64};
use std::{sync::OnceLock, thread::ThreadId};

use opentelemetry::global;

pub static CONNECTION_RATE_LIMIT: OnceLock<Metric<ShardedU64<ThreadId>>> = OnceLock::new();
pub static LOCAL_RATE_LIMIT: OnceLock<Metric<ShardedU64<ThreadId>>> = OnceLock::new();
pub static USER_RATE_LIMIT: OnceLock<Metric<ShardedU64<ThreadId>>> = OnceLock::new();

pub static EVENT_OK: &str = "ok";
pub static EVENT_RATE_LIMITED: &str = "rate_limited";
pub static EVENT_NOT_APPLICABLE: &str = "not_applicable";

pub(crate) fn init_metrics(rename: &std::collections::HashMap<String, String>) {
    init_observable_counter!(
        CONNECTION_RATE_LIMIT,
        crate::metrics::PREFIX_FILTER,
        crate::metrics::resolve_metric_name(rename, "connection_rate_limit"),
        "Connections rate limit filter invocations"
    );
    init_observable_counter!(
        LOCAL_RATE_LIMIT,
        crate::metrics::PREFIX_FILTER,
        crate::metrics::resolve_metric_name(rename, "local_rate_limit"),
        "Local rate limit filter invocations"
    );
    init_observable_counter!(
        USER_RATE_LIMIT,
        crate::metrics::PREFIX_FILTER,
        crate::metrics::resolve_metric_name(rename, "user_rate_limit"),
        "User rate limit filter invocations"
    );
}

pub fn reset_metrics() {
    use crate::sharded::Clearable;
    let metrics: &[&dyn Clearable] = &[&CONNECTION_RATE_LIMIT, &LOCAL_RATE_LIMIT, &USER_RATE_LIMIT];
    for metric in metrics {
        metric.clear();
    }
}
