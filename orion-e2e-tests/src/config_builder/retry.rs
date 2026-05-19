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

use std::time::Duration;

use orion_data_plane_api::envoy_data_plane_api::{
    envoy::config::route::v3::RetryPolicy as EnvoyRetryPolicy, google::protobuf::UInt32Value,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryOn {
    Err5xx,
    GatewayError,
    Retriable4xx,
    Reset,
    ConnectFailure,
    RefusedStream,
    RetriableStatusCodes,
}

impl RetryOn {
    fn as_str(self) -> &'static str {
        match self {
            Self::Err5xx => "5xx",
            Self::GatewayError => "gateway-error",
            Self::Retriable4xx => "retriable-4xx",
            Self::Reset => "reset",
            Self::ConnectFailure => "connect-failure",
            Self::RefusedStream => "refused-stream",
            Self::RetriableStatusCodes => "retriable-status-codes",
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct RetryPolicyBuilder {
    retry_on: Vec<RetryOn>,
    num_retries: Option<u32>,
    per_try_timeout: Option<Duration>,
    retriable_status_codes: Vec<u32>,
}

impl RetryPolicyBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn retry_on<I>(mut self, conditions: I) -> Self
    where
        I: IntoIterator<Item = RetryOn>,
    {
        self.retry_on = conditions.into_iter().collect();
        self
    }

    #[must_use]
    pub fn add_retry_on(mut self, condition: RetryOn) -> Self {
        if !self.retry_on.contains(&condition) {
            self.retry_on.push(condition);
        }
        self
    }

    #[must_use]
    pub fn on_5xx(self) -> Self {
        self.add_retry_on(RetryOn::Err5xx)
    }

    #[must_use]
    pub fn on_gateway_error(self) -> Self {
        self.add_retry_on(RetryOn::GatewayError)
    }

    #[must_use]
    pub fn on_connect_failure(self) -> Self {
        self.add_retry_on(RetryOn::ConnectFailure)
    }

    #[must_use]
    pub fn on_reset(self) -> Self {
        self.add_retry_on(RetryOn::Reset)
    }

    #[must_use]
    pub fn num_retries(mut self, num: u32) -> Self {
        self.num_retries = Some(num);
        self
    }

    #[must_use]
    pub fn per_try_timeout(mut self, timeout: Duration) -> Self {
        self.per_try_timeout = Some(timeout);
        self
    }

    #[must_use]
    pub fn retriable_status_code(mut self, code: u32) -> Self {
        if !self.retry_on.contains(&RetryOn::RetriableStatusCodes) {
            self.retry_on.push(RetryOn::RetriableStatusCodes);
        }
        if !self.retriable_status_codes.contains(&code) {
            self.retriable_status_codes.push(code);
        }
        self
    }

    #[must_use]
    pub fn retriable_status_codes<I>(mut self, codes: I) -> Self
    where
        I: IntoIterator<Item = u32>,
    {
        if !self.retry_on.contains(&RetryOn::RetriableStatusCodes) {
            self.retry_on.push(RetryOn::RetriableStatusCodes);
        }
        for code in codes {
            if !self.retriable_status_codes.contains(&code) {
                self.retriable_status_codes.push(code);
            }
        }
        self
    }

    #[must_use]
    pub fn build(self) -> EnvoyRetryPolicy {
        let retry_on = self.retry_on.iter().copied().map(RetryOn::as_str).collect::<Vec<_>>().join(",");

        EnvoyRetryPolicy {
            retry_on,
            num_retries: self.num_retries.map(|n| UInt32Value { value: n }),
            per_try_timeout: self.per_try_timeout.map(super::duration_to_proto),
            retriable_status_codes: self.retriable_status_codes,
            ..Default::default()
        }
    }
}

impl From<RetryPolicyBuilder> for EnvoyRetryPolicy {
    fn from(builder: RetryPolicyBuilder) -> Self {
        builder.build()
    }
}

pub type RetryPolicy = EnvoyRetryPolicy;
