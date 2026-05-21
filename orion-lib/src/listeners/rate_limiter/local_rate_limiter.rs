use std::sync::Arc;

use http::{status::StatusCode, Request};
use orion_interner::InternedStr;
use tracing::warn;

#[cfg(feature = "metrics")]
use {
    crate::{get_shard_id, with_metric},
    opentelemetry::KeyValue,
    orion_metrics::metrics::filters,
};

use orion_configuration::config::{
    listener_filters::ListenerLocalRateLimitConfig,
    network_filters::http_connection_manager::http_filters::local_rate_limit::{
        LocalRateLimit as LocalRateLimitConfig, TokenBucket as TokenBucketConfig,
    },
};

use crate::{
    body::response_flags::ResponseFlags, event_error::EventFailure, listeners::rate_limiter::token_bucket::TokenBucket,
};
use orion_format::types::ResponseFlags as FmtResponseFlags;

use crate::{
    listeners::{http_filters::FilterDecision, synthetic_http_response::SyntheticHttpResponse},
    runtime_config,
};

#[derive(Debug, Clone)]
pub struct LocalRateLimitInner {
    #[allow(unused)]
    pub stat_prefix: InternedStr,
    pub status: StatusCode,
    pub token_bucket: Option<TokenBucket>,
}

#[derive(Debug, Clone)]
pub struct LocalRateLimit {
    pub inner: Arc<LocalRateLimitInner>, // shared across sessions...
}

#[derive(Debug, Clone)]
pub struct ListenerLocalRateLimit {
    #[allow(unused)]
    pub stat_prefix: InternedStr,
    pub token_bucket: TokenBucket,
}

impl LocalRateLimit {
    pub fn run<B>(&self, req: &Request<B>) -> FilterDecision {
        if let Some(token_bucket) = &self.inner.token_bucket {
            if token_bucket.consume(1) {
                #[cfg(feature = "metrics")]
                with_metric!(
                    filters::LOCAL_RATE_LIMIT,
                    add,
                    1,
                    get_shard_id!(),
                    &[KeyValue::new("filter", self.inner.stat_prefix.0), KeyValue::new("result", filters::EVENT_OK)]
                );
                return FilterDecision::Continue;
            }
            let status = self.inner.status;
            #[cfg(feature = "metrics")]
            with_metric!(
                filters::LOCAL_RATE_LIMIT,
                add,
                1,
                get_shard_id!(),
                &[
                    KeyValue::new("filter", self.inner.stat_prefix.0),
                    KeyValue::new("result", filters::EVENT_RATE_LIMITED)
                ]
            );
            return FilterDecision::DirectResponse(Box::new(
                SyntheticHttpResponse::custom_error(
                    status,
                    None,
                    EventFailure::RateLimited.into(),
                    ResponseFlags(FmtResponseFlags::RATE_LIMITED),
                )
                .into_response(req.version()),
            ));
        }
        #[cfg(feature = "metrics")]
        with_metric!(
            filters::LOCAL_RATE_LIMIT,
            add,
            1,
            get_shard_id!(),
            &[
                KeyValue::new("filter", self.inner.stat_prefix.0),
                KeyValue::new("result", filters::EVENT_NOT_APPLICABLE)
            ]
        );
        FilterDecision::Continue
    }
}

impl ListenerLocalRateLimit {
    #[inline]
    pub fn allow(&self) -> bool {
        self.token_bucket.consume(1)
    }
}

fn build_token_bucket(tb_conf: TokenBucketConfig) -> TokenBucket {
    let max_tokens = tb_conf.max_tokens;
    let tokens_per_fill = tb_conf.tokens_per_fill;
    let fill_interval = tb_conf.fill_interval;
    let adjusted_fill_interval = fill_interval.checked_mul(runtime_config().num_runtimes.into());
    let fill_interval = if let Some(value) = adjusted_fill_interval {
        value
    } else {
        warn!("failed to adjust fill interval to number of configured runtimes (overflow)");
        fill_interval
    };
    TokenBucket::new(max_tokens, tokens_per_fill, fill_interval)
}

impl From<LocalRateLimitConfig> for LocalRateLimit {
    fn from(rate_limit: LocalRateLimitConfig) -> Self {
        let LocalRateLimitConfig { status, stat_prefix, token_bucket } = rate_limit;
        if let Some(tb_conf) = token_bucket {
            let token_bucket = build_token_bucket(tb_conf);
            return Self {
                inner: Arc::new(LocalRateLimitInner { status, token_bucket: Some(token_bucket), stat_prefix }),
            };
        }
        Self { inner: Arc::new(LocalRateLimitInner { status, token_bucket: None, stat_prefix }) }
    }
}

impl From<ListenerLocalRateLimitConfig> for ListenerLocalRateLimit {
    fn from(rate_limit: ListenerLocalRateLimitConfig) -> Self {
        let token_bucket = build_token_bucket(rate_limit.token_bucket);
        Self { token_bucket, stat_prefix: rate_limit.stat_prefix }
    }
}
