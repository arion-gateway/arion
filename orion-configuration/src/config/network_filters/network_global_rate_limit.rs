use orion_interner::InternedStr;
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

use crate::config::network_filters::http_connection_manager::http_filters::ext_proc::GrpcService;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NetworkGlobalRateLimit {
    pub stat_prefix: InternedStr,
    pub domain: SmolStr,
    pub grpc_service: GrpcService,
    #[serde(default)]
    pub failure_mode_deny: bool,
}

#[cfg(feature = "envoy-conversions")]
mod envoy_conversions {
    use std::time::Duration;

    use super::NetworkGlobalRateLimit;
    use crate::config::network_filters::http_connection_manager::http_filters::ext_proc::{
        ClusterGrpc, GoogleGrpc, GrpcService, GrpcServiceSpecifier,
    };
    use crate::config::{common::*, core::RustType};
    use orion_data_plane_api::envoy_data_plane_api::envoy::{
        config::ratelimit::v3::RateLimitServiceConfig,
        extensions::filters::network::ratelimit::v3::RateLimit as EnvoyRateLimit,
    };

    impl TryFrom<EnvoyRateLimit> for NetworkGlobalRateLimit {
        type Error = GenericError;
        fn try_from(value: EnvoyRateLimit) -> Result<Self, Self::Error> {
            let EnvoyRateLimit { stat_prefix, domain, descriptors, timeout, failure_mode_deny, rate_limit_service } =
                value;

            unsupported_field!(
                //stat_prefix,
                //domain,
                timeout, // timeout is already part of the grpc service configuration
                //failure_mode_deny,
                //rate_limit_service,
                descriptors // descriptors are not supported on network global rate limiter for now
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

    impl TryFrom<RateLimitServiceConfig> for GrpcService {
        type Error = GenericError;
        fn try_from(rls_config: RateLimitServiceConfig) -> Result<Self, Self::Error> {
            use orion_data_plane_api::envoy_data_plane_api::envoy::config::core::v3::grpc_service::TargetSpecifier;
            let inner = rls_config.grpc_service;
            let grpc_service = required!(inner).with_node("grpc_service")?;
            let timeout = grpc_service
                .timeout
                .map(|d| RustType::<Duration>::try_from(d).with_node("timeout").map(RustType::into_inner))
                .transpose()?;

            let target_specifier = grpc_service.target_specifier;
            let specifier = match required!(target_specifier).with_node("target_specifier")? {
                TargetSpecifier::EnvoyGrpc(envoy_grpc) => GrpcServiceSpecifier::Cluster(ClusterGrpc {
                    cluster_name: envoy_grpc.cluster_name.into(),
                    max_receive_message_length: None,
                }),
                TargetSpecifier::GoogleGrpc(google_grpc) => {
                    GrpcServiceSpecifier::GoogleGrpc(GoogleGrpc { target_uri: google_grpc.target_uri })
                },
            };

            Ok(GrpcService { service_specifier: specifier, timeout })
        }
    }
}
