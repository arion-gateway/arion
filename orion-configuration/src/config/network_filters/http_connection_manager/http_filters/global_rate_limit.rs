use orion_interner::InternedStr;
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

use super::ext_proc::GrpcService;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GlobalRateLimit {
    pub stat_prefix: InternedStr,
    pub domain: SmolStr,
    pub grpc_service: GrpcService,
    #[serde(default)]
    pub failure_mode_deny: bool,
}

#[cfg(feature = "envoy-conversions")]
mod envoy_conversions {

    use super::GlobalRateLimit;
    use crate::config::common::*;
    use orion_data_plane_api::envoy_data_plane_api::envoy::extensions::filters::http::ratelimit::v3::RateLimit as EnvoyRateLimit;

    impl TryFrom<EnvoyRateLimit> for GlobalRateLimit {
        type Error = GenericError;
        fn try_from(value: EnvoyRateLimit) -> Result<Self, Self::Error> {
            let EnvoyRateLimit {
                domain,
                stage,
                request_type,
                timeout,
                failure_mode_deny,
                rate_limited_as_resource_exhausted,
                rate_limit_service,
                enable_x_ratelimit_headers,
                disable_x_envoy_ratelimited_header,
                rate_limited_status,
                response_headers_to_add,
                status_on_error,
                stat_prefix,
                filter_enabled,
                filter_enforced,
                failure_mode_deny_percent,
                rate_limits,
            } = value;

            unsupported_field!(
                stage,
                request_type,
                rate_limited_as_resource_exhausted,
                enable_x_ratelimit_headers,
                disable_x_envoy_ratelimited_header,
                rate_limited_status,
                response_headers_to_add,
                status_on_error,
                filter_enabled,
                filter_enforced,
                failure_mode_deny_percent,
                rate_limits,
                timeout
            )?;

            if domain.is_empty() {
                return Err(GenericError::from_msg("domain must not be empty"));
            }

            let stat_prefix = if stat_prefix.is_empty() { domain.clone() } else { stat_prefix };

            let rls_config = required!(rate_limit_service).with_node("rate_limit_service")?;
            let grpc_service = rls_config.try_into()?;

            Ok(Self { stat_prefix: stat_prefix.into(), domain: domain.into(), grpc_service, failure_mode_deny })
        }
    }
}
