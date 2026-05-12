use orion_data_plane_api::envoy_data_plane_api::envoy::{
    config::{
        core::v3::{
            grpc_service::{EnvoyGrpc, TargetSpecifier},
            GrpcService,
        },
        ratelimit::v3::RateLimitServiceConfig,
    },
    extensions::{
        common::ratelimit::v3::{rate_limit_descriptor::Entry, RateLimitDescriptor},
        filters::network::ratelimit::v3::RateLimit as EnvoyNetworkRateLimit,
    },
};

#[derive(Debug, Clone)]
pub struct NetworkGlobalRateLimitBuilder {
    stat_prefix: String,
    domain: Option<String>,
    rls_cluster: String,
    failure_mode_deny: bool,
    descriptors: Vec<RateLimitDescriptor>,
}

impl NetworkGlobalRateLimitBuilder {
    #[must_use]
    pub fn new(stat_prefix: impl Into<String>) -> Self {
        Self {
            stat_prefix: stat_prefix.into(),
            domain: None,
            rls_cluster: "rls_cluster".into(),
            failure_mode_deny: false,
            descriptors: vec![],
        }
    }

    #[must_use]
    pub fn domain(mut self, domain: impl Into<String>) -> Self {
        self.domain = Some(domain.into());
        self
    }

    #[must_use]
    pub fn rls_cluster(mut self, cluster: impl Into<String>) -> Self {
        self.rls_cluster = cluster.into();
        self
    }

    #[must_use]
    pub fn failure_mode_deny(mut self) -> Self {
        self.failure_mode_deny = true;
        self
    }

    #[must_use]
    pub fn descriptor(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.descriptors.push(RateLimitDescriptor {
            entries: vec![Entry { key: key.into(), value: value.into() }],
            ..Default::default()
        });
        self
    }

    #[must_use]
    pub fn build(self) -> EnvoyNetworkRateLimit {
        EnvoyNetworkRateLimit {
            stat_prefix: self.stat_prefix,
            domain: self.domain.unwrap_or_default(),
            rate_limit_service: Some(RateLimitServiceConfig {
                grpc_service: Some(GrpcService {
                    target_specifier: Some(TargetSpecifier::EnvoyGrpc(EnvoyGrpc {
                        cluster_name: self.rls_cluster,
                        ..Default::default()
                    })),
                    ..Default::default()
                }),
                transport_api_version: 2, // V3
            }),
            failure_mode_deny: self.failure_mode_deny,
            descriptors: self.descriptors,
            ..Default::default()
        }
    }
}

impl From<NetworkGlobalRateLimitBuilder> for EnvoyNetworkRateLimit {
    fn from(builder: NetworkGlobalRateLimitBuilder) -> Self {
        builder.build()
    }
}

pub type NetworkGlobalRateLimit = EnvoyNetworkRateLimit;
