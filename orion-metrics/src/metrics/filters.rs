use crate::{metrics::Metric, sharded::ShardedU64};
use std::{sync::OnceLock, thread::ThreadId};

use opentelemetry::global;

pub static CONNECTION_RATE_LIMIT: OnceLock<Metric<ShardedU64<ThreadId>>> = OnceLock::new();
pub static LOCAL_RATE_LIMIT: OnceLock<Metric<ShardedU64<ThreadId>>> = OnceLock::new();
pub static USER_RATE_LIMIT: OnceLock<Metric<ShardedU64<ThreadId>>> = OnceLock::new();

pub static EVENT_OK: &'static str = "ok";
pub static EVENT_RATE_LIMITED: &'static str = "rate_limited";
pub static EVENT_NOT_APPLICABLE: &'static str = "not_applicable";

pub(crate) fn init_metrics() {
    init_observable_counter!(
        CONNECTION_RATE_LIMIT,
        crate::metrics::PREFIX_FILTER,
        "connection_rate_limit",
        "Connections rate limit filter invocations"
    );
    init_observable_counter!(LOCAL_RATE_LIMIT, crate::metrics::PREFIX_FILTER, "local_rate_limit", "Local rate limit filter invocations");
    init_observable_counter!(USER_RATE_LIMIT, crate::metrics::PREFIX_FILTER, "user_rate_limit", "User rate limit filter invocations");
}
