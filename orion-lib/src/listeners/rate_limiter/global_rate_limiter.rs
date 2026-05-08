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
        http_connection_manager::http_filters::ext_proc::GrpcServiceSpecifier,
        network_global_rate_limit::{
            DescriptorEntry as ConfigDescriptorEntry, NetworkGlobalRateLimit as NetworkGlobalRateLimitConfig,
        },
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

use crate::{
    clusters::clusters_manager::{self, RoutingContext},
    instrument_function,
};
use tokio::sync::Mutex as AsyncMutex;

struct QuotaBucket {
    remaining: AtomicI64,
    valid_until_ms: AtomicU64,
    update_lock: AsyncMutex<()>,
}

impl QuotaBucket {
    /// Attempts to consume one ticket from the bucket.
    /// Returns `true` (ticket granted) only if the quota is still valid AND there was a
    /// positive count before decrement. Going negative is benign — the next quota refresh resets it.
    fn try_consume(&self, now: u64) -> bool {
        self.valid_until_ms.load(Ordering::Acquire) > now && self.remaining.fetch_sub(1, Ordering::AcqRel) > 0
    }
}

const INITIAL_CAPACITY: usize = 10_000;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Domain(SmolStr);

static GLOBAL_QUOTAS: LazyLock<PapayaMap<Domain, Arc<QuotaBucket>, ahash::RandomState>> =
    LazyLock::new(|| PapayaMap::with_capacity_and_hasher(INITIAL_CAPACITY, ahash::RandomState::new()));

#[derive(Debug, Clone)]
enum RlsClient {
    Cluster(SmolStr),
    GoogleGrpc(RateLimitServiceClient<Channel>),
}

#[derive(Debug, Clone)]
pub struct NetworkGlobalRateLimit {
    domain: Domain,
    failure_mode_deny: bool,
    rls_client: RlsClient,
    descriptors: Vec<RateLimitDescriptor>,
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

        let descriptors = config
            .descriptors
            .into_iter()
            .map(|d| RateLimitDescriptor {
                entries: d
                    .entries
                    .into_iter()
                    .map(|ConfigDescriptorEntry { key, value }| DescriptorEntry {
                        key: key.to_string(),
                        value: value.to_string(),
                    })
                    .collect(),
                ..Default::default()
            })
            .collect();

        Ok(Self { domain: Domain(config.domain), failure_mode_deny: config.failure_mode_deny, rls_client, descriptors })
    }
}

impl NetworkGlobalRateLimit {
    pub fn domain(&self) -> &Domain {
        &self.domain
    }

    /// Returns `Ok(())` to allow the connection, `Err` to drop the TCP connection.
    pub async fn check(&self) -> crate::Result<()> {
        let now = now_ms();

        // Fast path: consume from an existing quota bucket without any locking.
        let maybe_bucket = GLOBAL_QUOTAS.pin().get(&self.domain).cloned();
        if let Some(bucket) = maybe_bucket {
            if bucket.try_consume(now) {
                return Ok(());
            }
            // Quota exhausted or expired: serialize RLS refreshes under the bucket lock so
            // only one task calls the RLS while others wait and re-check.
            let _guard = bucket.update_lock.lock().await;
            if bucket.try_consume(now) {
                return Ok(());
            }
            return match self.call_rls().await {
                Ok(response) => {
                    if let Some((requests, valid_until_ms)) = extract_quota(&response) {
                        bucket.valid_until_ms.store(valid_until_ms, Ordering::Release);
                        bucket.remaining.store(requests as i64, Ordering::Release);
                    }
                    Self::eval_rls_response(response)
                },
                Err(e) => {
                    if self.failure_mode_deny {
                        Err(e)
                    } else {
                        Ok(())
                    }
                },
            };
        }

        // No bucket yet: call the RLS directly. Multiple concurrent connections may reach
        // here before the first quota is established; that is acceptable — once the first
        // quota response arrives and the bucket is inserted, they all hit the fast path.
        match self.call_rls().await {
            Ok(response) => {
                if let Some((requests, valid_until_ms)) = extract_quota(&response) {
                    let map = GLOBAL_QUOTAS.pin();
                    let bucket = map.get_or_insert_with(self.domain.clone(), || {
                        Arc::new(QuotaBucket {
                            remaining: AtomicI64::new(0),
                            valid_until_ms: AtomicU64::new(0),
                            update_lock: AsyncMutex::new(()),
                        })
                    });
                    bucket.valid_until_ms.store(valid_until_ms, Ordering::Release);
                    bucket.remaining.store(requests as i64, Ordering::Release);
                }
                Self::eval_rls_response(response)
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

    fn eval_rls_response(response: RateLimitResponse) -> crate::Result<()> {
        let code =
            rate_limit_response::Code::try_from(response.overall_code).unwrap_or(rate_limit_response::Code::Unknown);
        if code == rate_limit_response::Code::OverLimit {
            return Err("rate limited by global rate limiter".into());
        }
        Ok(())
    }

    async fn call_rls(&self) -> crate::Result<RateLimitResponse> {
        let clock = quanta::Clock::new();
        instrument_function!(clock, |nanos| {
            crate::instrumentation::metrics::SEND_RLS_REQUEST.observe(nanos as usize)
        });
        let rls_request = RateLimitRequest {
            domain: self.domain.0.to_string(),
            descriptors: self.descriptors.clone(),
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
