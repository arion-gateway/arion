use std::{
    sync::{
        atomic::{AtomicI64, AtomicU64, Ordering},
        Arc, LazyLock,
    },
    time::{SystemTime, UNIX_EPOCH},
};

use orion_configuration::config::{
    cluster::ClusterSpecifier,
    network_filters::{
        http_connection_manager::http_filters::{
            ext_proc::GrpcServiceSpecifier, global_rate_limit::GlobalRateLimit as GlobalRateLimitConfig,
        },
        network_global_rate_limit::NetworkGlobalRateLimit as NetworkGlobalRateLimitConfig,
    },
};
use orion_data_plane_api::envoy_data_plane_api::envoy::{
    extensions::common::ratelimit::v3::{rate_limit_descriptor::Entry as DescriptorEntry, RateLimitDescriptor},
    service::ratelimit::v3::{
        rate_limit_response, rate_limit_service_client::RateLimitServiceClient, RateLimitRequest, RateLimitResponse,
    },
};
use papaya::HashMap as PapayaMap;
use smol_str::SmolStr;
use tonic::transport::Channel;

use crate::clusters::clusters_manager::{self, RoutingContext};
use tokio::sync::Mutex as AsyncMutex;

struct QuotaBucket {
    remaining: AtomicI64,
    valid_until_ms: AtomicU64,
    update_lock: AsyncMutex<()>,
}

const INITIAL_CAPACITY: usize = 10_000;

static GLOBAL_QUOTAS: LazyLock<PapayaMap<SmolStr, Arc<QuotaBucket>, ahash::RandomState>> =
    LazyLock::new(|| PapayaMap::with_capacity_and_hasher(INITIAL_CAPACITY, ahash::RandomState::new()));

#[derive(Debug, Clone)]
enum RlsClient {
    Cluster(SmolStr),
    GoogleGrpc(RateLimitServiceClient<Channel>),
}

#[derive(Debug, Clone)]
pub struct NetworkGlobalRateLimit {
    domain: SmolStr,
    failure_mode_deny: bool,
    rls_client: RlsClient,
}

// Not implemented yet, this will be a hybrid with UserRateLimiter
pub struct GlobalRateLimit {
    config: GlobalRateLimitConfig,
}

impl TryFrom<NetworkGlobalRateLimitConfig> for NetworkGlobalRateLimit {
    type Error = crate::Error;

    fn try_from(config: NetworkGlobalRateLimitConfig) -> crate::Result<Self> {
        let rls_client = match config.grpc_service.service_specifier {
            GrpcServiceSpecifier::GoogleGrpc(g) => {
                let channel = Channel::from_shared(g.target_uri.clone())
                    .map_err(|e| crate::Error::from(format!("invalid RLS endpoint '{}': {e}", g.target_uri)))?
                    .connect_lazy();
                RlsClient::GoogleGrpc(RateLimitServiceClient::new(channel))
            },
            GrpcServiceSpecifier::Cluster(c) => RlsClient::Cluster(c.cluster_name),
        };

        Ok(Self { domain: config.domain, failure_mode_deny: config.failure_mode_deny, rls_client })
    }
}

impl NetworkGlobalRateLimit {
    pub fn domain(&self) -> &SmolStr {
        &self.domain
    }

    /// Returns `Ok(())` to allow the connection, `Err` to drop the TCP connection.
    pub async fn check(&self, target_domain: SmolStr) -> crate::Result<()> {
        let now = now_ms();

        // 1. Fast path: fully lock-free check while holding the pin guard
        let bucket = {
            let map = GLOBAL_QUOTAS.pin();
            let bucket_ref = map.get_or_insert_with(target_domain.clone(), || {
                Arc::new(QuotaBucket {
                    remaining: AtomicI64::new(0),
                    valid_until_ms: AtomicU64::new(0),
                    update_lock: AsyncMutex::new(()),
                })
            });

            if bucket_ref.valid_until_ms.load(Ordering::Acquire) > now {
                if bucket_ref.remaining.fetch_sub(1, Ordering::AcqRel) > 0 {
                    return Ok(());
                }
            }

            // If we reach here, we need the slow path. Clone the Arc so we can drop the map guard.
            bucket_ref.clone()
        };

        // 3. Slow path: acquire the async lock to prevent thundering herd
        let _guard = bucket.update_lock.lock().await;

        // 4. Double-check: another thread might have successfully called RLS and
        // replenished the quota while this thread was waiting for the lock
        if bucket.valid_until_ms.load(Ordering::Acquire) > now {
            if bucket.remaining.fetch_sub(1, Ordering::AcqRel) > 0 {
                return Ok(());
            }
        }

        // 5. Actually call the RLS (only one thread per domain enters here at a time)
        match self.call_rls(&target_domain).await {
            Ok(response) => {
                if let Some((requests, valid_until_ms)) = extract_quota(&response) {
                    // Update expiry first, then remaining tokens
                    bucket.valid_until_ms.store(valid_until_ms, Ordering::Release);
                    bucket.remaining.store(requests as i64, Ordering::Release);
                }

                let code = rate_limit_response::Code::try_from(response.overall_code)
                    .unwrap_or(rate_limit_response::Code::Unknown);
                if code == rate_limit_response::Code::OverLimit {
                    return Err("rate limited by global rate limiter".into());
                }
                Ok(())
            },
            Err(e) => {
                if self.failure_mode_deny {
                    Err(e)
                } else {
                    Ok(())
                }
            },
        }
    }

    async fn call_rls(&self, target_domain: &str) -> crate::Result<RateLimitResponse> {
        let rls_request = RateLimitRequest {
            domain: self.domain.to_string(),
            descriptors: vec![RateLimitDescriptor {
                entries: vec![DescriptorEntry { key: "domain".into(), value: target_domain.to_string() }],
                ..Default::default()
            }],
            hits_addend: 1,
        };

        let resp = match &self.rls_client {
            RlsClient::Cluster(cluster_name) => {
                let spec = ClusterSpecifier::Cluster(cluster_name.clone());
                let cluster_id = clusters_manager::resolve_cluster(&spec, None)
                    .ok_or_else(|| crate::Error::from(format!("RLS cluster '{cluster_name}' not found")))?;
                let svc = clusters_manager::get_grpc_connection(cluster_id, RoutingContext::None)?;
                RateLimitServiceClient::new(svc)
                    .should_rate_limit(rls_request)
                    .await
                    .map_err(|e| crate::Error::from(format!("RLS call failed: {e}")))?
            },
            RlsClient::GoogleGrpc(client) => client
                .clone()
                .should_rate_limit(rls_request)
                .await
                .map_err(|e| crate::Error::from(format!("RLS call failed: {e}")))?,
        };
        Ok(resp.into_inner())
    }
}

fn extract_quota(response: &RateLimitResponse) -> Option<(u32, u64)> {
    use orion_data_plane_api::envoy_data_plane_api::envoy::service::ratelimit::v3::rate_limit_response::quota::ExpirationSpecifier;

    let quota = response.quota.as_ref().filter(|q| q.requests > 0)?;
    let valid_until_ms = match quota.expiration_specifier.as_ref()? {
        ExpirationSpecifier::ValidUntil(ts) => proto_timestamp_to_ms(ts)?,
    };
    Some((quota.requests.saturating_sub(1), valid_until_ms))
}

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

fn proto_timestamp_to_ms(ts: &orion_data_plane_api::envoy_data_plane_api::google::protobuf::Timestamp) -> Option<u64> {
    let secs: u64 = ts.seconds.try_into().ok()?;
    let nanos: u32 = ts.nanos.try_into().ok()?;
    secs.checked_mul(1_000)?.checked_add(u64::from(nanos / 1_000_000))
}
