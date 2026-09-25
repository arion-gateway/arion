// Copyright 2025-2026 The arion-gateway Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use http::StatusCode;
use orion_interner::InternedStr;
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;
use std::collections::HashMap;

use crate::config::network_filters::http_connection_manager::http_filters::local_rate_limit::LocalRateLimit;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SimpleRateLimit {
    pub max_tokens: u32,
    pub rate: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Limit {
    LocalRateLimit(LocalRateLimit),
    SimpleRateLimit(SimpleRateLimit),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UserRateLimiter {
    #[serde(with = "http_serde_ext::status_code")]
    pub status: StatusCode,
    pub stat_prefix: InternedStr,
    pub user_id_header: SmolStr,
    pub user_rate_limits: HashMap<Option<SmolStr>, Limit, ahash::RandomState>,
}

#[cfg(feature = "envoy-conversions")]
mod envoy_conversions {
    use http::StatusCode;
    use orion_data_plane_api::envoy_data_plane_api::orion::extensions::filters::http::user_rate_limit::v3::SimpleRateLimit as OrionSimpleRateLimit;
    use orion_data_plane_api::envoy_data_plane_api::orion::extensions::filters::http::user_rate_limit::v3::UserRateLimit as OrionUserRateLimit;
    use orion_data_plane_api::envoy_data_plane_api::orion::extensions::filters::http::user_rate_limit::v3::UserRateLimiter as OrionUserRateLimiter;

    use crate::config::WithNodeOnResult;
    use crate::config::common::envoy_conversions::IsUsed;
    use crate::config::core::RustType;
    use crate::config::{GenericError, required};
    use super::{UserRateLimiter, Limit, SimpleRateLimit};
    use orion_data_plane_api::envoy_data_plane_api::orion::extensions::filters::http::user_rate_limit::v3::user_rate_limit::Limit as OrionLimit;
    use orion_interner::{StringInterner, InternedStr};

    impl TryFrom<OrionSimpleRateLimit> for SimpleRateLimit {
        type Error = GenericError;

        fn try_from(value: OrionSimpleRateLimit) -> Result<Self, Self::Error> {
            Ok(Self { max_tokens: value.max_tokens, rate: value.rate })
        }
    }

    impl TryFrom<OrionUserRateLimiter> for UserRateLimiter {
        type Error = GenericError;

        fn try_from(value: OrionUserRateLimiter) -> Result<Self, Self::Error> {
            let OrionUserRateLimiter { user_id_header_name, stat_prefix, user_rate_limits, status } = value;

            let mut mapped_limits = std::collections::HashMap::with_hasher(ahash::RandomState::new());
            for limit_entry in user_rate_limits {
                let OrionUserRateLimit { user_id, limit } = limit_entry;
                let limit = required!(limit)?;
                let limit = match limit {
                    OrionLimit::LocalRateLimit(l) => Limit::LocalRateLimit(l.try_into()?),
                    OrionLimit::SimpleRateLimit(s) => Limit::SimpleRateLimit(s.try_into()?),
                };
                mapped_limits.insert(user_id.map(Into::into), limit);
            }

            let stat_prefix = InternedStr(stat_prefix.to_static_str());
            let status = status
                .map(RustType::<StatusCode>::try_from)
                .transpose()
                .with_node("status")?
                .map(RustType::into_inner)
                .unwrap_or(StatusCode::TOO_MANY_REQUESTS);

            Ok(Self {
                user_id_header: user_id_header_name.into(),
                user_rate_limits: mapped_limits,
                stat_prefix,
                status,
            })
        }
    }
}
