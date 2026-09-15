use std::sync::{LazyLock, Once};
use std::time::{Duration, Instant};
use triomphe::Arc;

use orion_configuration::config::{
    network_filters::http_connection_manager::http_filters::user_rate_limit::UserRateLimiter as OrionUserRateLimiter,
    GenericError,
};
use papaya::HashMap as PapayaMap;
use smol_str::SmolStr;
use tracing::debug;

use crate::listeners::rate_limiter::token_bucket::TokenBucket;
use crate::{listeners::http_filters::FilterDecision, OrionRequestBody};
use orion_configuration::config::network_filters::http_connection_manager::http_filters::user_rate_limit::Limit;

use orion_interner::StringInterner;
#[cfg(feature = "metrics")]
use {
    crate::{get_shard_id, with_metric},
    opentelemetry::KeyValue,
    orion_metrics::metrics::filters,
};

static USER_RATE_LIMITERS: LazyLock<PapayaMap<SmolStr, TokenBucket, ahash::RandomState>> =
    LazyLock::new(|| PapayaMap::with_hasher(ahash::RandomState::new()));
static CLEANER_ONCE: Once = Once::new();
static CLEANER_PERIOD: Duration = Duration::from_secs(10);
static CLEANER_TOKEN_BUCKET_IDLE: Duration = Duration::from_secs(60);

#[derive(Clone, Debug)]
pub struct UserRateLimiter {
    inner: Arc<OrionUserRateLimiter>,
}

impl TryFrom<OrionUserRateLimiter> for UserRateLimiter {
    type Error = GenericError;

    fn try_from(conf: OrionUserRateLimiter) -> Result<Self, Self::Error> {
        Ok(UserRateLimiter::new(conf))
    }
}

impl UserRateLimiter {
    pub fn new(conf: OrionUserRateLimiter) -> Self {
        Self { inner: Arc::new(conf) }
    }

    #[allow(clippy::too_many_lines)]
    pub fn apply_request(&mut self, request: &mut http::Request<OrionRequestBody>) -> FilterDecision {
        debug!(target: "user_rate_limiter", "apply_request: processing request: {:?}", request);
        debug!(target: "user_rate_limiter", "{:#?}", self.inner);

        CLEANER_ONCE.call_once(|| {
            tokio::spawn(async {
                loop {
                    debug!(target: "user_rate_limiter", "running cleaner: with period {CLEANER_PERIOD:?} and idle lifetime {CLEANER_TOKEN_BUCKET_IDLE:?}");
                    pingora_timeout::sleep(CLEANER_PERIOD).await;
                    let now = Instant::now();
                    USER_RATE_LIMITERS.pin().retain(|usr, tb| {
                        let res = now - tb.last_consume() < CLEANER_TOKEN_BUCKET_IDLE;
                        if !res {
                            debug!(target: "user_rate_limiter", "cleaning up token bucket for user: {usr}");
                        }
                        res
                    });
                }
            });
        });

        // Obtain the user-id from header (using the value of user_id_header)
        //

        let Some(user) = request.headers().get(self.inner.user_id_header.as_str()).and_then(|v| v.to_str().ok()) else {
            debug!(target: "user_rate_limiter", "no user found in request headers");
            #[cfg(feature = "metrics")]
            with_metric!(
                filters::USER_RATE_LIMIT,
                add,
                1,
                get_shard_id!(),
                &[
                    KeyValue::new("filter", self.inner.stat_prefix.0),
                    KeyValue::new("result", filters::EVENT_NOT_APPLICABLE)
                ]
            );
            return FilterDecision::Continue;
        };

        let static_user = user.to_static_str();

        // Try to look up the TokenBucket for the given user in the global USER_RATE_LIMITERS map.
        // If found, return a reference to the corresponding token bucket.
        // Otherwise, look up the user configuration in the filter and create a new TokenBucket.
        // If no configuration is found, or not configured, simply return Continue.
        //

        // Happy-path: an entry for the user already exists in the global map...
        //
        // `pin().get(...).map(...)` clones nothing: `consume` returns a plain
        // `bool`, so the papaya guard is released before any early return.
        if let Some(allowed) = USER_RATE_LIMITERS.pin().get(static_user).map(|tb| tb.consume(1)) {
            if allowed {
                debug!(target: "user_rate_limiter", "consumed token for user: {static_user}");
                #[cfg(feature = "metrics")]
                with_metric!(
                    filters::USER_RATE_LIMIT,
                    add,
                    1,
                    get_shard_id!(),
                    &[
                        KeyValue::new("filter", self.inner.stat_prefix.0),
                        KeyValue::new("user", static_user),
                        KeyValue::new("result", filters::EVENT_OK)
                    ]
                );
                return FilterDecision::Continue;
            }
            debug!(target: "user_rate_limiter", "rate limited for user: {static_user}");
            #[cfg(feature = "metrics")]
            with_metric!(
                filters::USER_RATE_LIMIT,
                add,
                1,
                get_shard_id!(),
                &[
                    KeyValue::new("filter", self.inner.stat_prefix.0),
                    KeyValue::new("user", static_user),
                    KeyValue::new("result", filters::EVENT_RATE_LIMITED)
                ]
            );

            return FilterDecision::rate_limited("Rate limited", Some(self.inner.status), request.version());
        }

        // If the entry for the user does not exist in the global map, resolve the
        // configured limit first (without mutating the map), then insert the new
        // bucket with `get_or_insert`. On a concurrent race the winner's bucket
        // is reused, which matches the old `entry().or_try_insert_with()` semantics.
        //
        let limit = if let Some(limit) = self.inner.user_rate_limits.get(&Some(static_user.into())) {
            limit
        } else if let Some(limit) = self.inner.user_rate_limits.get(&None) {
            limit
        } else {
            // Configuration not found for this user... return Continue
            debug!(target: "user_rate_limiter", "no rate limit found for user: {static_user}");
            #[cfg(feature = "metrics")]
            with_metric!(
                filters::USER_RATE_LIMIT,
                add,
                1,
                get_shard_id!(),
                &[
                    KeyValue::new("filter", self.inner.stat_prefix.0),
                    KeyValue::new("user", static_user),
                    KeyValue::new("result", filters::EVENT_NOT_APPLICABLE)
                ]
            );
            return FilterDecision::Continue;
        };

        let new_bucket = match limit {
            Limit::LocalRateLimit(l) => {
                let Some(tb) = &l.token_bucket else {
                    // Token bucket is not configured for this user, return Continue.
                    debug!(target: "user_rate_limiter", "no token bucket found for user: {static_user}");
                    #[cfg(feature = "metrics")]
                    with_metric!(
                        filters::USER_RATE_LIMIT,
                        add,
                        1,
                        get_shard_id!(),
                        &[
                            KeyValue::new("filter", self.inner.stat_prefix.0),
                            KeyValue::new("user", static_user),
                            KeyValue::new("result", filters::EVENT_NOT_APPLICABLE)
                        ]
                    );
                    return FilterDecision::Continue;
                };
                TokenBucket::new(tb.max_tokens, tb.tokens_per_fill, tb.fill_interval)
            },
            Limit::SimpleRateLimit(s) => match TokenBucket::with_rate_and_capacity(s.max_tokens, s.rate) {
                Ok(tb) => tb,
                Err(_) => return FilterDecision::Continue,
            },
        };

        let pinned = USER_RATE_LIMITERS.pin();
        let tb = pinned.get_or_insert(static_user.into(), new_bucket);
        if tb.consume(1) {
            debug!(target: "user_rate_limiter", "consumed token for user: {static_user}");
            #[cfg(feature = "metrics")]
            with_metric!(
                filters::USER_RATE_LIMIT,
                add,
                1,
                get_shard_id!(),
                &[
                    KeyValue::new("filter", self.inner.stat_prefix.0),
                    KeyValue::new("user", static_user),
                    KeyValue::new("result", filters::EVENT_OK)
                ]
            );
            FilterDecision::Continue
        } else {
            debug!(target: "user_rate_limiter", "rate limited for user: {static_user}");
            #[cfg(feature = "metrics")]
            with_metric!(
                filters::USER_RATE_LIMIT,
                add,
                1,
                get_shard_id!(),
                &[
                    KeyValue::new("filter", self.inner.stat_prefix.0),
                    KeyValue::new("user", static_user),
                    KeyValue::new("result", filters::EVENT_RATE_LIMITED)
                ]
            );
            FilterDecision::rate_limited("Rate limited", Some(self.inner.status), request.version())
        }
    }
}
