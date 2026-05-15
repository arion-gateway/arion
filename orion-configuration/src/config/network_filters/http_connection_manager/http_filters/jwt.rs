use std::{collections::HashMap, sync::Arc};

use crate::config::{
    core::{DataSource, StringMatcher},
    network_filters::http_connection_manager::{route::RouteMatch, RetryPolicy},
};
use http::HeaderName;
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JwtClaimToHeader {
    #[serde(with = "http_serde_ext::header_name")]
    pub header_name: HeaderName,
    pub claim_name: SmolStr,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JwtHeader {
    #[serde(with = "http_serde_ext::header_name")]
    pub name: HeaderName,
    pub value_prefix: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct JwtProvider {
    pub issuer: SmolStr,
    pub audiences: Vec<SmolStr>,
    pub subjects: Option<StringMatcher>,
    pub forward: bool,
    pub from_headers: Vec<JwtHeader>,
    pub from_params: Vec<SmolStr>,
    pub from_cookies: Vec<SmolStr>,
    #[serde(with = "http_serde_ext::header_name::option")]
    pub forward_payload_header: Option<HeaderName>,
    pub pad_forward_payload_header: bool,
    pub payload_in_metadata: Option<SmolStr>,
    pub header_in_metadata: Option<SmolStr>,
    pub failed_status_in_metadata: Option<SmolStr>,
    pub clock_skew_seconds: u32,
    pub clear_route_cache: bool,
    pub claim_to_headers: Vec<JwtClaimToHeader>,
    pub jwks_source_specifier: JwksSourceSpecifier,
    // pub require_expiration: bool,
    // pub max_lifetime: Option<Duration>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HttpUri {
    pub uri: String,
    pub cluster: String,
    pub timeout: std::time::Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RemoteJwks {
    pub http_uri: HttpUri,
    pub cache_duration: std::time::Duration,
    pub retry_policy: Option<RetryPolicy>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum JwksSourceSpecifier {
    RemoteJwks(RemoteJwks),
    LocalJwks(DataSource),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RequiresType {
    ProviderName(SmolStr),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JwtRequirement {
    pub requires_type: Option<RequiresType>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RequirementType {
    Requires(JwtRequirement),
    RequirementName(SmolStr),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RequirementRule {
    pub r#match: Option<RouteMatch>,
    pub requirement_type: Option<RequirementType>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JwtAuthentication {
    pub providers: HashMap<SmolStr, Arc<JwtProvider>>,
    pub rules: Vec<RequirementRule>,
}

#[cfg(feature = "envoy-conversions")]
mod envoy_conversions {
    use super::*;
    use std::str::FromStr;
    use std::time::Duration;

    use crate::config::common::envoy_conversions::IsUsed;
    use crate::config::core::RustType;
    use crate::config::network_filters::http_connection_manager::{RetryBackoff, RetryOn};
    use crate::config::{required, unsupported_field, GenericError, WithNodeOnResult};
    use http::HeaderName;
    use orion_data_plane_api::envoy_data_plane_api::envoy::config::core::v3::http_uri::HttpUpstreamType as EnvoyHttpClusterType;
    use orion_data_plane_api::envoy_data_plane_api::envoy::config::core::v3::BackoffStrategy as EnvoyCoreBackoffStrategy;
    use orion_data_plane_api::envoy_data_plane_api::envoy::config::core::v3::HttpUri as EnvoyHttpUri;
    use orion_data_plane_api::envoy_data_plane_api::envoy::config::core::v3::RetryPolicy as EnvoyCoreRetryPolicy;
    use orion_data_plane_api::envoy_data_plane_api::envoy::extensions::filters::http::jwt_authn::v3::jwt_provider::JwksSourceSpecifier as EnvoyJwksSourceSpecifier;
    use orion_data_plane_api::envoy_data_plane_api::envoy::extensions::filters::http::jwt_authn::v3::jwt_requirement::RequiresType as EnvoyRequiresType;
    use orion_data_plane_api::envoy_data_plane_api::envoy::extensions::filters::http::jwt_authn::v3::requirement_rule::RequirementType as EnvoyRequirementType;
    use orion_data_plane_api::envoy_data_plane_api::envoy::extensions::filters::http::jwt_authn::v3::JwtAuthentication as EnvoyJwtAuthentication;
    use orion_data_plane_api::envoy_data_plane_api::envoy::extensions::filters::http::jwt_authn::v3::JwtClaimToHeader as EnvoyJwtClaimToHeader;
    use orion_data_plane_api::envoy_data_plane_api::envoy::extensions::filters::http::jwt_authn::v3::JwtHeader as EnvoyJwtHeader;
    use orion_data_plane_api::envoy_data_plane_api::envoy::extensions::filters::http::jwt_authn::v3::JwtProvider as EnvoyJwtProvider;
    use orion_data_plane_api::envoy_data_plane_api::envoy::extensions::filters::http::jwt_authn::v3::JwtRequirement as EnvoyJwtRequirement;
    use orion_data_plane_api::envoy_data_plane_api::envoy::extensions::filters::http::jwt_authn::v3::RemoteJwks as EnvoyRemoteJwks;
    use orion_data_plane_api::envoy_data_plane_api::envoy::extensions::filters::http::jwt_authn::v3::RequirementRule as EnvoyRequirementRule;

    impl TryFrom<EnvoyHttpUri> for HttpUri {
        type Error = GenericError;
        fn try_from(value: EnvoyHttpUri) -> Result<Self, Self::Error> {
            let EnvoyHttpUri { uri, timeout, http_upstream_type } = value;
            let timeout = timeout.map(TryInto::try_into).transpose()?.map(RustType::<Duration>::into_inner);
            let timeout = required!(timeout)?;
            let http_upstream_type = required!(http_upstream_type)?;
            Ok(HttpUri {
                uri,
                cluster: {
                    let EnvoyHttpClusterType::Cluster(cluster) = http_upstream_type;
                    cluster
                },
                timeout,
            })
        }
    }

    // NOTE: envoy makes use of two different retry policies: one for route and one for core.
    // Core is much simpler than route, as it only supports only a subset of fields.
    // The good news is that we can use the same orion RetryPolicy for both, defaulting few missing fields.
    // See for more information:
    // https://www.envoyproxy.io/docs/envoy/latest/api-v3/config/core/v3/base.proto#envoy-v3-api-msg-config-core-v3-retrypolicy
    // https://www.envoyproxy.io/docs/envoy/latest/api-v3/config/route/v3/route_components.proto#envoy-v3-api-msg-config-route-v3-retrypolicy-retrypriority
    //
    impl TryFrom<EnvoyCoreRetryPolicy> for RetryPolicy {
        type Error = GenericError;
        fn try_from(value: EnvoyCoreRetryPolicy) -> Result<Self, Self::Error> {
            let EnvoyCoreRetryPolicy {
                retry_back_off,
                num_retries,
                retry_on,
                retry_priority,
                retry_host_predicate,
                host_selection_retry_max_attempts,
            } = value;
            unsupported_field!(
                // retry_on,
                // num_retries,
                // retry_back_off,
                retry_priority,
                retry_host_predicate,
                host_selection_retry_max_attempts
            )?;

            let retry_on =
                retry_on.split(',').map(RetryOn::from_str).collect::<Result<Vec<_>, _>>().with_node("retry_on")?;
            let num_retries = num_retries.map(|v| v.value).unwrap_or(1);
            let retry_backoff =
                retry_back_off.map(RetryBackoff::try_from).transpose().with_node("retry_backoff")?.unwrap_or_default();

            Ok(Self {
                retry_on,
                num_retries,
                retry_backoff,
                per_try_timeout: None,
                retriable_status_codes: vec![],
                retriable_request_headers: vec![],
                retriable_headers: vec![],
            })
        }
    }

    impl TryFrom<EnvoyCoreBackoffStrategy> for RetryBackoff {
        type Error = GenericError;
        fn try_from(value: EnvoyCoreBackoffStrategy) -> Result<Self, Self::Error> {
            let EnvoyCoreBackoffStrategy { base_interval, max_interval } = value;
            //note: envoy docs says this can't be zero, but also that less than 1ms gets rounded up
            // so for simplicity we just round up zero too.
            let base_interval = RustType::<Duration>::try_from(required!(base_interval)?)
                .with_node("base_interval")?
                .into_inner()
                .max(Duration::from_millis(1));
            let max_interval = max_interval
                .map(RustType::<Duration>::try_from)
                .transpose()
                .map_err(|_e| GenericError::from_msg("failed to convert into Duration"))
                .with_node("max_interval")?
                .map(RustType::into_inner)
                .unwrap_or(base_interval * 10);
            if max_interval < base_interval {
                return Err(GenericError::from_msg(format!(
                    "max_interval ({}ms) is less than base_interval ({}ms)",
                    max_interval.as_millis(),
                    base_interval.as_millis()
                )));
            }
            Ok(Self { base_interval, max_interval })
        }
    }

    impl TryFrom<EnvoyRemoteJwks> for RemoteJwks {
        type Error = GenericError;
        fn try_from(value: EnvoyRemoteJwks) -> Result<Self, Self::Error> {
            let EnvoyRemoteJwks { http_uri, cache_duration, async_fetch, retry_policy } = value;
            unsupported_field!(
                // http_uri,
                // cache_duration,
                async_fetch // retry_policy
            )?;
            let cache_dur: RustType<Duration> =
                cache_duration.map(TryInto::try_into).transpose()?.unwrap_or(RustType(Duration::from_secs(60)));
            let http_uri = http_uri.map(TryInto::try_into).transpose()?;
            let http_uri = required!(http_uri)?;
            let retry_policy = retry_policy.map(TryInto::try_into).transpose()?;

            Ok(RemoteJwks { http_uri, cache_duration: cache_dur.into_inner(), retry_policy })
        }
    }

    impl TryFrom<EnvoyJwksSourceSpecifier> for JwksSourceSpecifier {
        type Error = GenericError;
        fn try_from(value: EnvoyJwksSourceSpecifier) -> Result<Self, Self::Error> {
            match value {
                EnvoyJwksSourceSpecifier::RemoteJwks(remote_jwks) => {
                    Ok(JwksSourceSpecifier::RemoteJwks(remote_jwks.try_into()?))
                },
                EnvoyJwksSourceSpecifier::LocalJwks(local_jwks) => {
                    Ok(JwksSourceSpecifier::LocalJwks(local_jwks.try_into()?))
                },
            }
        }
    }

    impl TryFrom<EnvoyJwtClaimToHeader> for JwtClaimToHeader {
        type Error = GenericError;
        fn try_from(value: EnvoyJwtClaimToHeader) -> Result<Self, Self::Error> {
            let EnvoyJwtClaimToHeader { header_name, claim_name } = value;
            let header_name = HeaderName::from_str(&header_name)?;
            let claim_name = claim_name.into();
            Ok(JwtClaimToHeader { claim_name, header_name })
        }
    }

    impl TryFrom<EnvoyJwtHeader> for JwtHeader {
        type Error = GenericError;
        fn try_from(value: EnvoyJwtHeader) -> Result<Self, Self::Error> {
            let EnvoyJwtHeader { name, value_prefix } = value;
            let name = HeaderName::from_str(&name)?;
            Ok(JwtHeader { name, value_prefix })
        }
    }

    impl TryFrom<EnvoyJwtProvider> for JwtProvider {
        type Error = GenericError;
        fn try_from(value: EnvoyJwtProvider) -> Result<Self, Self::Error> {
            let EnvoyJwtProvider {
                issuer,
                audiences,
                subjects,
                require_expiration,
                max_lifetime,
                forward,
                from_headers,
                from_params,
                from_cookies,
                forward_payload_header,
                pad_forward_payload_header,
                payload_in_metadata,
                normalize_payload_in_metadata,
                header_in_metadata,
                failed_status_in_metadata,
                clock_skew_seconds,
                jwt_cache_config,
                claim_to_headers,
                clear_route_cache,
                jwks_source_specifier,
            } = value;

            unsupported_field!(
                //issuer,
                //audiences,
                //subjects,
                require_expiration,
                max_lifetime,
                //forward,
                //from_headers,
                //from_params,
                //from_cookies,
                //forward_payload_header,
                //pad_forward_payload_header,
                //payload_in_metadata,
                //header_in_metadata,
                //failed_status_in_metadata,
                //clock_skew_seconds,
                //claim_to_headers,
                //clear_route_cache,
                //jwks_source_specifier
                normalize_payload_in_metadata,
                jwt_cache_config
            )?;

            Ok(JwtProvider {
                issuer: issuer.into(),
                audiences: audiences.into_iter().map(Into::into).collect(),
                subjects: subjects.map(TryInto::try_into).transpose()?,
                // require_expiration,
                // max_lifetime: max_lifetime.map(TryInto::try_into).transpose()?,
                forward,
                from_headers: from_headers.into_iter().map(TryInto::try_into).collect::<Result<Vec<JwtHeader>, _>>()?,
                from_params: from_params.into_iter().map(Into::into).collect(),
                from_cookies: from_cookies.into_iter().map(Into::into).collect(),
                forward_payload_header: if forward_payload_header.is_empty() {
                    None
                } else {
                    Some(HeaderName::from_str(&forward_payload_header)?)
                },
                pad_forward_payload_header,
                payload_in_metadata: if payload_in_metadata.is_empty() {
                    None
                } else {
                    Some(payload_in_metadata.into())
                },
                // normalize_payload_in_metadata,
                header_in_metadata: if header_in_metadata.is_empty() { None } else { Some(header_in_metadata.into()) },
                failed_status_in_metadata: if failed_status_in_metadata.is_empty() {
                    None
                } else {
                    Some(failed_status_in_metadata.into())
                },
                clock_skew_seconds,
                clear_route_cache,
                claim_to_headers: claim_to_headers
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<Result<Vec<JwtClaimToHeader>, _>>()?,
                jwks_source_specifier: required!(jwks_source_specifier)?.try_into()?,
            })
        }
    }

    impl TryFrom<EnvoyRequiresType> for RequiresType {
        type Error = GenericError;
        fn try_from(value: EnvoyRequiresType) -> Result<Self, Self::Error> {
            match value {
                EnvoyRequiresType::ProviderName(provider_name) => Ok(Self::ProviderName(provider_name.into())),
                EnvoyRequiresType::ProviderAndAudiences(_) => {
                    Err(GenericError::unsupported_variant("ProviderAndAudiences"))
                },
                EnvoyRequiresType::RequiresAny(_) => Err(GenericError::unsupported_variant("RequiresAny")),
                EnvoyRequiresType::RequiresAll(_) => Err(GenericError::unsupported_variant("RequiresAll")),
                EnvoyRequiresType::AllowMissingOrFailed(_) => {
                    Err(GenericError::unsupported_variant("AllowMissingOrFailed"))
                },
                EnvoyRequiresType::AllowMissing(_) => Err(GenericError::unsupported_variant("AllowMissing")),
                EnvoyRequiresType::ExtractOnlyWithoutValidation(_) => {
                    Err(GenericError::unsupported_variant("ExtractOnlyWithoutValidation"))
                },
            }
        }
    }

    impl TryFrom<EnvoyJwtRequirement> for JwtRequirement {
        type Error = GenericError;
        fn try_from(value: EnvoyJwtRequirement) -> Result<Self, Self::Error> {
            let EnvoyJwtRequirement { requires_type } = value;
            Ok(Self { requires_type: requires_type.map(TryInto::try_into).transpose()? })
        }
    }

    impl TryFrom<EnvoyRequirementType> for RequirementType {
        type Error = GenericError;
        fn try_from(value: EnvoyRequirementType) -> Result<Self, Self::Error> {
            match value {
                EnvoyRequirementType::Requires(jwt_requirement) => {
                    Ok(RequirementType::Requires(jwt_requirement.try_into()?))
                },
                EnvoyRequirementType::RequirementName(_) => Err(GenericError::unsupported_variant("RequirementName")),
            }
        }
    }

    impl TryFrom<EnvoyRequirementRule> for RequirementRule {
        type Error = GenericError;
        fn try_from(value: EnvoyRequirementRule) -> Result<Self, Self::Error> {
            let EnvoyRequirementRule { r#match, requirement_type } = value;
            let route_match: Option<RouteMatch> = r#match.map(TryInto::try_into).transpose()?;
            let requirement_type = requirement_type.map(TryInto::try_into).transpose()?;
            Ok(Self { r#match: route_match, requirement_type })
        }
    }

    impl TryFrom<EnvoyJwtAuthentication> for JwtAuthentication {
        type Error = GenericError;
        fn try_from(value: EnvoyJwtAuthentication) -> Result<Self, Self::Error> {
            let EnvoyJwtAuthentication {
                providers,
                rules,
                filter_state_rules,
                bypass_cors_preflight,
                requirement_map,
                strip_failure_response,
                stat_prefix,
            } = value;

            unsupported_field!(
                // providers,
                // rules,
                filter_state_rules,
                bypass_cors_preflight,
                requirement_map,
                strip_failure_response,
                stat_prefix
            )?;

            let providers: HashMap<SmolStr, Arc<JwtProvider>> = providers
                .into_iter()
                .map(|(k, v)| -> Result<_, GenericError> { Ok((k.into(), Arc::new(JwtProvider::try_from(v)?))) })
                .collect::<Result<HashMap<_, _>, _>>()?;

            let rules: Vec<RequirementRule> =
                rules.into_iter().map(RequirementRule::try_from).collect::<Result<Vec<_>, _>>()?;

            Ok(JwtAuthentication { providers, rules })
        }
    }
}
