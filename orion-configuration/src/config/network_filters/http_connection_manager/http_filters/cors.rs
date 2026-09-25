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

use http::Method;
use orion_data_plane_api::envoy_data_plane_api::envoy::extensions::filters::http::cors::v3::Cors as EnvoyCors;
use orion_data_plane_api::envoy_data_plane_api::envoy::extensions::filters::http::cors::v3::CorsPolicy as EnvoyCorsPolicy;
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

use crate::config::core::StringMatcher;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CorsConfig {
    /// List of allowed origins. Examples: `<https://foo.com>`, "*".
    pub allow_origins: Vec<StringMatcher>,
    /// List of allowed methods. Examples: "GET", "POST".
    #[serde(with = "http_serde_ext::method::vec", default)]
    pub allow_methods: Vec<http::Method>,
    /// List of allowed headers in requests.
    pub allow_headers: Vec<SmolStr>,
    /// List of headers exposed to the client JS.
    pub expose_headers: Vec<SmolStr>,
    /// Whether to allow credentials (cookies, auth headers).
    pub allow_credentials: bool,
    /// Max age for preflight caching in seconds.
    pub max_age: Option<u64>,
}

impl Default for CorsConfig {
    fn default() -> Self {
        Self {
            allow_origins: vec![StringMatcher::new("*")],
            allow_methods: vec![
                Method::GET,
                Method::POST,
                Method::PUT,
                Method::PATCH,
                Method::DELETE,
                Method::HEAD,
                Method::OPTIONS,
            ],
            allow_headers: vec!["*".into()],
            expose_headers: vec!["*".into()],
            allow_credentials: false,
            max_age: Some(86400),
        }
    }
}

#[cfg(feature = "envoy-conversions")]
mod envoy_conversions {
    use super::*;
    use crate::config::common::envoy_conversions::IsUsed;
    use crate::config::{unsupported_field, GenericError};
    use std::str::FromStr;

    impl TryFrom<EnvoyCors> for CorsConfig {
        type Error = GenericError;
        fn try_from(_: EnvoyCors) -> Result<Self, Self::Error> {
            Ok(CorsConfig::default())
        }
    }

    impl TryFrom<EnvoyCorsPolicy> for CorsConfig {
        type Error = GenericError;
        fn try_from(policy: EnvoyCorsPolicy) -> Result<Self, Self::Error> {
            let EnvoyCorsPolicy {
                allow_origin_string_match,
                allow_methods,
                allow_headers,
                expose_headers,
                max_age,
                allow_credentials,
                filter_enabled,
                shadow_enabled,
                allow_private_network_access,
                forward_not_matching_preflights,
            } = policy;

            unsupported_field!(
                filter_enabled,
                shadow_enabled,
                allow_private_network_access,
                forward_not_matching_preflights
            )?;

            Ok(CorsConfig {
                allow_origins: allow_origin_string_match
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<Result<Vec<_>, _>>()?,
                allow_methods: allow_methods
                    .split(',')
                    .map(str::trim)
                    .map(http::Method::from_str)
                    .collect::<Result<Vec<_>, _>>()?,
                allow_headers: allow_headers.split(',').map(str::trim).map(Into::into).collect(),
                expose_headers: expose_headers.split(',').map(str::trim).map(Into::into).collect(),
                allow_credentials: allow_credentials.unwrap_or_default().value,
                max_age: (!max_age.is_empty()).then(|| max_age.parse::<u64>()).transpose()?,
            })
        }
    }
}
