// Copyright 2025 The kmesh Authors
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

use orion_data_plane_api::envoy_data_plane_api::{
    envoy::{
        extensions::filters::http::local_ratelimit::v3::LocalRateLimit as EnvoyLocalRateLimit,
        r#type::v3::TokenBucket as EnvoyTokenBucket,
    },
    google::protobuf::{Duration as ProtoDuration, UInt32Value},
    orion::extensions::filters::http::user_rate_limit::v3::{
        user_rate_limit::Limit as UserLimit, SimpleRateLimit, UserRateLimit as UserRateLimitEntry,
        UserRateLimiter as OrionUserRateLimiter,
    },
};

use orion_data_plane_api::envoy_data_plane_api::envoy::extensions::filters::http::local_ratelimit::v3::LocalRateLimit as UserLocalRateLimit;

pub type LocalRateLimit = EnvoyLocalRateLimit;
pub type UserRateLimiter = OrionUserRateLimiter;
pub type TokenBucket = EnvoyTokenBucket;

/// Builder for `TokenBucket` configuration
#[derive(Debug, Clone)]
pub struct TokenBucketBuilder {
    max_tokens: u32,
    tokens_per_fill: u32,
    fill_interval_secs: u64,
}

impl TokenBucketBuilder {
    #[must_use]
    pub fn new(max_tokens: u32, tokens_per_fill: u32, fill_interval_secs: u64) -> Self {
        Self { max_tokens, tokens_per_fill, fill_interval_secs }
    }

    #[must_use]
    pub fn max_tokens(mut self, max_tokens: u32) -> Self {
        self.max_tokens = max_tokens;
        self
    }

    #[must_use]
    pub fn tokens_per_fill(mut self, tokens_per_fill: u32) -> Self {
        self.tokens_per_fill = tokens_per_fill;
        self
    }

    #[must_use]
    pub fn fill_interval_secs(mut self, fill_interval_secs: u64) -> Self {
        self.fill_interval_secs = fill_interval_secs;
        self
    }

    #[must_use]
    pub fn build(self) -> EnvoyTokenBucket {
        EnvoyTokenBucket {
            max_tokens: self.max_tokens,
            tokens_per_fill: Some(UInt32Value { value: self.tokens_per_fill }),
            fill_interval: Some(ProtoDuration { seconds: self.fill_interval_secs as i64, nanos: 0 }),
        }
    }
}

impl From<TokenBucketBuilder> for EnvoyTokenBucket {
    fn from(builder: TokenBucketBuilder) -> Self {
        builder.build()
    }
}

/// Builder for `LocalRateLimit` configuration (HCM-level or per-route)
#[derive(Debug, Clone)]
pub struct LocalRateLimitBuilder {
    proto: EnvoyLocalRateLimit,
}

impl LocalRateLimitBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self {
            proto: EnvoyLocalRateLimit {
                stat_prefix: "http_local_rate_limiter".to_owned(),
                status: None,
                token_bucket: None,
                filter_enabled: None,
                filter_enforced: None,
                request_headers_to_add_when_not_enforced: Vec::new(),
                response_headers_to_add: Vec::new(),
                descriptors: Vec::new(),
                stage: 0,
                local_rate_limit_per_downstream_connection: false,
                enable_x_ratelimit_headers: 0,
                vh_rate_limits: 0,
                rate_limited_as_resource_exhausted: false,
                local_cluster_rate_limit: None,
                always_consume_default_token_bucket: None,
                max_dynamic_descriptors: None,
                rate_limits: Vec::new(),
            },
        }
    }

    #[must_use]
    pub fn stat_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.proto.stat_prefix = prefix.into();
        self
    }

    #[must_use]
    pub fn status_code(mut self, code: u32) -> Self {
        self.proto.status =
            Some(orion_data_plane_api::envoy_data_plane_api::envoy::r#type::v3::HttpStatus { code: code as i32 });
        self
    }

    #[must_use]
    pub fn token_bucket(mut self, max_tokens: u32, tokens_per_fill: u32, fill_interval_secs: u64) -> Self {
        self.proto.token_bucket =
            Some(TokenBucketBuilder::new(max_tokens, tokens_per_fill, fill_interval_secs).build());
        self
    }

    #[must_use]
    pub fn build(self) -> EnvoyLocalRateLimit {
        self.proto
    }
}

impl Default for LocalRateLimitBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl From<LocalRateLimitBuilder> for EnvoyLocalRateLimit {
    fn from(builder: LocalRateLimitBuilder) -> Self {
        builder.build()
    }
}

/// Builder for `UserRateLimiter` configuration
#[derive(Debug, Clone)]
pub struct UserRateLimiterBuilder {
    stat_prefix: String,
    user_id_header: String,
    status_code: u32,
    user_rate_limits: Vec<UserRateLimitEntry>,
}

impl UserRateLimiterBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self {
            stat_prefix: "user_rate_limiter".to_owned(),
            user_id_header: "x-user-id".to_owned(),
            status_code: 429,
            user_rate_limits: Vec::new(),
        }
    }

    #[must_use]
    pub fn stat_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.stat_prefix = prefix.into();
        self
    }

    #[must_use]
    pub fn user_id_header(mut self, header: impl Into<String>) -> Self {
        self.user_id_header = header.into();
        self
    }

    #[must_use]
    pub fn status_code(mut self, code: u32) -> Self {
        self.status_code = code;
        self
    }

    #[must_use]
    pub fn add_user_limit_local(
        mut self,
        user: Option<impl Into<String>>,
        max_tokens: u32,
        tokens_per_fill: u32,
        fill_interval_secs: u64,
    ) -> Self {
        let token_bucket = TokenBucketBuilder::new(max_tokens, tokens_per_fill, fill_interval_secs).build();
        let local_rate_limit = UserLocalRateLimit {
            stat_prefix: "user_rate_limit".to_owned(),
            status: None,
            token_bucket: Some(token_bucket),
            filter_enabled: None,
            filter_enforced: None,
            request_headers_to_add_when_not_enforced: Vec::new(),
            response_headers_to_add: Vec::new(),
            descriptors: Vec::new(),
            stage: 0,
            local_rate_limit_per_downstream_connection: false,
            enable_x_ratelimit_headers: 0,
            vh_rate_limits: 0,
            rate_limited_as_resource_exhausted: false,
            local_cluster_rate_limit: None,
            always_consume_default_token_bucket: None,
            max_dynamic_descriptors: None,
            rate_limits: Vec::new(),
        };
        let limit = UserLimit::LocalRateLimit(local_rate_limit);
        self.user_rate_limits.push(UserRateLimitEntry { user_id: user.map(Into::into), limit: Some(limit) });
        self
    }

    #[must_use]
    pub fn add_user_limit_simple(
        mut self,
        user: Option<impl Into<String>>,
        max_tokens: u32,
        rate_per_sec: u32,
    ) -> Self {
        let limit = UserLimit::SimpleRateLimit(SimpleRateLimit { max_tokens, rate: rate_per_sec });
        self.user_rate_limits.push(UserRateLimitEntry { user_id: user.map(Into::into), limit: Some(limit) });
        self
    }

    #[must_use]
    pub fn build(self) -> OrionUserRateLimiter {
        OrionUserRateLimiter {
            stat_prefix: self.stat_prefix,
            user_id_header_name: self.user_id_header,
            status: Some(orion_data_plane_api::envoy_data_plane_api::envoy::r#type::v3::HttpStatus {
                code: self.status_code as i32,
            }),
            user_rate_limits: self.user_rate_limits,
        }
    }
}

impl Default for UserRateLimiterBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl From<UserRateLimiterBuilder> for OrionUserRateLimiter {
    fn from(builder: UserRateLimiterBuilder) -> Self {
        builder.build()
    }
}
