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

mod token_bucket;
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

pub(crate) use token_bucket::TokenBucket;

use orion_configuration::config::{
    listener_filters::ListenerLocalRateLimitConfig,
    network_filters::http_connection_manager::http_filters::local_rate_limit::{
        LocalRateLimit as LocalRateLimitConfig, TokenBucket as TokenBucketConfig,
    },
};

use crate::{body::response_flags::ResponseFlags, event_error::EventFailure};
use orion_format::types::ResponseFlags as FmtResponseFlags;

use crate::{
    listeners::{http_filters::FilterDecision, synthetic_http_response::SyntheticHttpResponse},
    runtime_config,
};

#[derive(Debug, Clone)]
pub struct LocalRateLimitInner {
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
    pub token_bucket: Option<TokenBucket>,
}

impl LocalRateLimit {
    pub fn run<B>(&self, req: &Request<B>) -> FilterDecision {
        if let Some(token_bucket) = &self.inner.token_bucket {
            if !token_bucket.consume(1) {
                let status = self.inner.status;
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
                return FilterDecision::DirectResponse(
                    SyntheticHttpResponse::custom_error(
                        status,
                        None,
                        EventFailure::RateLimited.into(),
                        ResponseFlags(FmtResponseFlags::RATE_LIMITED),
                    )
                    .into_response(req.version()),
                );
            } else {
                with_metric!(
                    filters::LOCAL_RATE_LIMIT,
                    add,
                    1,
                    get_shard_id!(),
                    &[KeyValue::new("filter", self.inner.stat_prefix.0), KeyValue::new("result", filters::EVENT_OK)]
                );
                return FilterDecision::Continue;
            }
        }
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
    pub fn allow(&self) -> bool {
        if let Some(token_bucket) = &self.token_bucket {
            if !token_bucket.consume(1) {
                return false;
            }
        }
        true
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
        if let Some(tb_conf) = rate_limit.token_bucket {
            let token_bucket = build_token_bucket(tb_conf);
            return Self { token_bucket: Some(token_bucket) };
        }
        Self { token_bucket: None }
    }
}
