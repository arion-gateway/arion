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

use crate::clusters::clusters_manager::{self, RoutingContext};
use tokio::sync::Mutex as AsyncMutex;

struct QuotaBucket {
    remaining: AtomicI64,
    valid_until_ms: AtomicU64,
    update_lock: AsyncMutex<()>,
}

impl QuotaBucket {
    fn try_consume(&self, now: u64) -> bool {
        self.valid_until_ms.load(Ordering::Acquire) > now && self.remaining.fetch_sub(1, Ordering::AcqRel) > 0
    }
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
    pub stat_prefix: SmolStr,
    domain: Option<SmolStr>,
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

        Ok(Self {
            stat_prefix: config.stat_prefix.to_string().into(),
            domain: config.domain,
            failure_mode_deny: config.failure_mode_deny,
            rls_client,
            descriptors,
        })
    }
}

impl NetworkGlobalRateLimit {
    pub async fn check(&self, sni: Option<&SmolStr>) -> crate::Result<()> {
        let now = now_ms();

        let domain: &SmolStr = self
            .domain
            .as_ref()
            .or(sni)
            .ok_or_else(|| crate::Error::from("rate limit: no SNI and no domain configured"))?;

        // Fast path: consume from an existing quota bucket without any locking.
        let maybe_bucket = GLOBAL_QUOTAS.pin().get(domain).cloned();
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
            return match self.call_rls(domain).await {
                Ok(response) => {
                    if let Some((requests, valid_until_ms)) = extract_quota(&response) {
                        bucket.valid_until_ms.store(valid_until_ms, Ordering::Release);
                        bucket.remaining.store(i64::from(requests), Ordering::Release);
                    }
                    Self::eval_rls_response(&response)
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
        // here before the first quota is established, aka we will have a thundering herd
        // if quota is configured on a given response from the RLS. This is acceptable
        // since once the first quota response arrives and the bucket is inserted,
        // they all hit the fast path where we have a lock to call_rls when quota is
        // exhausted. This is an optimization for the general path where no quota is
        // emitted and we call the RLS on each request: we avoid taking the lock in this
        // case because we do not really need to.
        match self.call_rls(domain).await {
            Ok(response) => {
                if let Some((requests, valid_until_ms)) = extract_quota(&response) {
                    let map = GLOBAL_QUOTAS.pin();
                    let bucket = map.get_or_insert_with(domain.clone(), || {
                        Arc::new(QuotaBucket {
                            remaining: AtomicI64::new(0),
                            valid_until_ms: AtomicU64::new(0),
                            update_lock: AsyncMutex::new(()),
                        })
                    });
                    bucket.valid_until_ms.store(valid_until_ms, Ordering::Release);
                    bucket.remaining.store(i64::from(requests), Ordering::Release);
                }
                Self::eval_rls_response(&response)
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

    fn eval_rls_response(response: &RateLimitResponse) -> crate::Result<()> {
        let code =
            rate_limit_response::Code::try_from(response.overall_code).unwrap_or(rate_limit_response::Code::Unknown);
        if code == rate_limit_response::Code::OverLimit {
            return Err("rate limited by global rate limiter".into());
        }
        Ok(())
    }

    async fn call_rls(&self, domain: &SmolStr) -> crate::Result<RateLimitResponse> {
        #[cfg(feature = "instrumentation")]
        let clock = quanta::Clock::new();
        #[cfg(feature = "instrumentation")]
        let start_clock = clock.raw();

        let rls_request =
            RateLimitRequest { domain: domain.to_string(), descriptors: self.descriptors.clone(), hits_addend: 1 };

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

        #[cfg(feature = "instrumentation")]
        #[allow(clippy::cast_possible_truncation)]
        crate::instrumentation::metrics::SEND_RLS_REQUEST
            .observe(clock.delta_as_nanos(start_clock, clock.raw()) as usize);

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
    #[allow(clippy::cast_possible_truncation)]
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

fn proto_timestamp_to_ms(ts: &orion_data_plane_api::envoy_data_plane_api::google::protobuf::Timestamp) -> Option<u64> {
    let secs: u64 = ts.seconds.try_into().ok()?;
    let nanos: u32 = ts.nanos.try_into().ok()?;
    secs.checked_mul(1_000)?.checked_add(u64::from(nanos / 1_000_000))
}

#[cfg(test)]
mod tests {
    use super::*;
    use orion_data_plane_api::envoy_data_plane_api::{
        envoy::service::ratelimit::v3::rate_limit_service_server::{RateLimitService, RateLimitServiceServer},
        google::protobuf::Timestamp,
    };
    use std::{collections::VecDeque, sync::atomic::AtomicUsize};
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::Server;

    static DOMAIN_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn unique_domain() -> SmolStr {
        let n = DOMAIN_COUNTER.fetch_add(1, Ordering::Relaxed);
        SmolStr::new(format!("test-{n}.rls.test"))
    }

    // Mock RLS server that returns pre-queued responses in order.
    #[derive(Clone)]
    struct MockRls {
        responses: Arc<AsyncMutex<VecDeque<RateLimitResponse>>>,
        call_count: Arc<AtomicUsize>,
        captured_domains: Arc<AsyncMutex<Vec<String>>>,
        fail_with_error: bool,
    }

    impl MockRls {
        fn with_responses(responses: Vec<RateLimitResponse>) -> Self {
            Self {
                responses: Arc::new(AsyncMutex::new(responses.into())),
                call_count: Arc::new(AtomicUsize::new(0)),
                captured_domains: Arc::new(AsyncMutex::new(Vec::new())),
                fail_with_error: false,
            }
        }

        fn always_failing() -> Self {
            Self {
                responses: Arc::new(AsyncMutex::new(VecDeque::new())),
                call_count: Arc::new(AtomicUsize::new(0)),
                captured_domains: Arc::new(AsyncMutex::new(Vec::new())),
                fail_with_error: true,
            }
        }

        fn call_count(&self) -> usize {
            self.call_count.load(Ordering::SeqCst)
        }

        async fn domains_seen(&self) -> Vec<String> {
            self.captured_domains.lock().await.clone()
        }
    }

    #[tonic::async_trait]
    impl RateLimitService for MockRls {
        async fn should_rate_limit(
            &self,
            request: tonic::Request<RateLimitRequest>,
        ) -> Result<tonic::Response<RateLimitResponse>, tonic::Status> {
            self.call_count.fetch_add(1, Ordering::SeqCst);
            self.captured_domains.lock().await.push(request.get_ref().domain.clone());
            if self.fail_with_error {
                return Err(tonic::Status::unavailable("simulated RLS failure"));
            }
            let resp = self.responses.lock().await.pop_front().unwrap_or_default();
            Ok(tonic::Response::new(resp))
        }
    }

    async fn start_mock_rls(mock: MockRls) -> (String, MockRls) {
        let server_mock = mock.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = Server::builder()
                .add_service(RateLimitServiceServer::new(server_mock))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .ok();
        });
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        (format!("http://{addr}"), mock)
    }

    fn make_filter(uri: impl Into<String>, domain: Option<&str>, failure_mode_deny: bool) -> NetworkGlobalRateLimit {
        let channel = Channel::from_shared(uri.into()).unwrap().connect_lazy();
        NetworkGlobalRateLimit {
            domain: domain.map(SmolStr::new),
            failure_mode_deny,
            rls_client: RlsClient::GoogleGrpc(RateLimitServiceClient::new(channel)),
            descriptors: vec![],
        }
    }

    fn ok_response() -> RateLimitResponse {
        RateLimitResponse { overall_code: rate_limit_response::Code::Ok as i32, ..Default::default() }
    }

    fn over_limit_response() -> RateLimitResponse {
        RateLimitResponse { overall_code: rate_limit_response::Code::OverLimit as i32, ..Default::default() }
    }

    fn ok_response_with_quota(requests: u32) -> RateLimitResponse {
        let valid_secs = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs().cast_signed() + 3600;
        RateLimitResponse {
            overall_code: rate_limit_response::Code::Ok as i32,
            quota: Some(rate_limit_response::Quota {
                requests,
                expiration_specifier: Some(rate_limit_response::quota::ExpirationSpecifier::ValidUntil(Timestamp {
                    seconds: valid_secs,
                    nanos: 0,
                })),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    // --- QuotaBucket unit tests ---

    #[test]
    fn quota_bucket_expired_returns_false() {
        let bucket = QuotaBucket {
            remaining: AtomicI64::new(10),
            valid_until_ms: AtomicU64::new(0),
            update_lock: AsyncMutex::new(()),
        };
        assert!(!bucket.try_consume(now_ms()));
    }

    #[test]
    fn quota_bucket_depleted_returns_false() {
        let bucket = QuotaBucket {
            remaining: AtomicI64::new(0),
            valid_until_ms: AtomicU64::new(u64::MAX),
            update_lock: AsyncMutex::new(()),
        };
        assert!(!bucket.try_consume(now_ms()));
    }

    #[test]
    fn quota_bucket_valid_consumes_until_depleted() {
        let bucket = QuotaBucket {
            remaining: AtomicI64::new(3),
            valid_until_ms: AtomicU64::new(u64::MAX),
            update_lock: AsyncMutex::new(()),
        };
        let now = now_ms();
        assert!(bucket.try_consume(now));
        assert!(bucket.try_consume(now));
        assert!(bucket.try_consume(now));
        assert!(!bucket.try_consume(now));
    }

    // --- extract_quota unit tests ---

    #[test]
    fn extract_quota_without_quota_field_returns_none() {
        assert!(extract_quota(&ok_response()).is_none());
    }

    #[test]
    fn extract_quota_with_zero_requests_returns_none() {
        let resp = RateLimitResponse {
            overall_code: rate_limit_response::Code::Ok as i32,
            quota: Some(rate_limit_response::Quota {
                requests: 0,
                expiration_specifier: Some(rate_limit_response::quota::ExpirationSpecifier::ValidUntil(Timestamp {
                    seconds: 9_999_999_999,
                    nanos: 0,
                })),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(extract_quota(&resp).is_none());
    }

    #[test]
    fn extract_quota_returns_decremented_requests_and_milliseconds() {
        let resp = RateLimitResponse {
            overall_code: rate_limit_response::Code::Ok as i32,
            quota: Some(rate_limit_response::Quota {
                requests: 5,
                expiration_specifier: Some(rate_limit_response::quota::ExpirationSpecifier::ValidUntil(Timestamp {
                    seconds: 1_000_000,
                    nanos: 500_000_000,
                })),
                ..Default::default()
            }),
            ..Default::default()
        };
        let (remaining, valid_until_ms) = extract_quota(&resp).unwrap();
        assert_eq!(remaining, 4);
        assert_eq!(valid_until_ms, 1_000_000_500); // 1_000_000 * 1000 + 500ms
    }

    // --- check() integration tests with mock RLS ---

    #[tokio::test]
    async fn check_ok_allows_connection() {
        let domain = unique_domain();
        let (uri, mock) = start_mock_rls(MockRls::with_responses(vec![ok_response()])).await;
        let filter = make_filter(uri, Some(domain.as_str()), false);
        filter.check(None).await.unwrap();
        assert_eq!(mock.call_count(), 1);
    }

    #[tokio::test]
    async fn check_over_limit_denies_connection() {
        let domain = unique_domain();
        let (uri, mock) = start_mock_rls(MockRls::with_responses(vec![over_limit_response()])).await;
        let filter = make_filter(uri, Some(domain.as_str()), false);
        let err = filter.check(None).await.unwrap_err();
        assert!(err.to_string().contains("rate limited"));
        assert_eq!(mock.call_count(), 1);
    }

    #[tokio::test]
    async fn check_failure_mode_allow_passes_on_rls_error() {
        let domain = unique_domain();
        let (uri, mock) = start_mock_rls(MockRls::always_failing()).await;
        let filter = make_filter(uri, Some(domain.as_str()), false);
        filter.check(None).await.unwrap();
        assert_eq!(mock.call_count(), 1);
    }

    #[tokio::test]
    async fn check_failure_mode_deny_blocks_on_rls_error() {
        let domain = unique_domain();
        let (uri, mock) = start_mock_rls(MockRls::always_failing()).await;
        let filter = make_filter(uri, Some(domain.as_str()), true);
        assert!(filter.check(None).await.is_err());
        assert_eq!(mock.call_count(), 1);
    }

    #[tokio::test]
    async fn check_no_domain_no_sni_errors_without_rls_call() {
        // Fails before any network call; the URI is never contacted.
        let filter = make_filter("http://127.0.0.1:1", None, false);
        let err = filter.check(None).await.unwrap_err();
        assert!(err.to_string().contains("no SNI and no domain configured"));
    }

    #[tokio::test]
    async fn check_uses_sni_as_domain_when_unconfigured() {
        let sni = unique_domain();
        let (uri, mock) = start_mock_rls(MockRls::with_responses(vec![ok_response()])).await;
        let filter = make_filter(uri, None, false);
        filter.check(Some(&sni)).await.unwrap();
        assert_eq!(mock.domains_seen().await, [sni.as_str()]);
    }

    #[tokio::test]
    async fn check_configured_domain_takes_priority_over_sni() {
        let static_domain = unique_domain();
        let sni = unique_domain();
        let (uri, mock) = start_mock_rls(MockRls::with_responses(vec![ok_response()])).await;
        let filter = make_filter(uri, Some(static_domain.as_str()), false);
        filter.check(Some(&sni)).await.unwrap();
        assert_eq!(mock.domains_seen().await, [static_domain.as_str()]);
    }

    #[tokio::test]
    async fn check_quota_caching_skips_rls_within_window() {
        let domain = unique_domain();
        // quota.requests=3: the initiating call itself is counted as 1, so remaining is stored
        // as 2.  Calls 2 and 3 consume from the bucket; call 4 exhausts it and hits the RLS.
        let (uri, mock) =
            start_mock_rls(MockRls::with_responses(vec![ok_response_with_quota(3), ok_response_with_quota(3)])).await;
        let filter = make_filter(uri, Some(domain.as_str()), false);
        for _ in 0..4 {
            filter.check(None).await.unwrap();
        }
        assert_eq!(mock.call_count(), 2);
    }

    #[tokio::test]
    async fn check_over_limit_after_quota_exhausted() {
        let domain = unique_domain();
        // quota.requests=2: stored as remaining=1.  Call 2 consumes it; call 3 exhausts
        // the bucket and re-calls the RLS, which now returns OVER_LIMIT.
        let (uri, mock) =
            start_mock_rls(MockRls::with_responses(vec![ok_response_with_quota(2), over_limit_response()])).await;
        let filter = make_filter(uri, Some(domain.as_str()), false);
        filter.check(None).await.unwrap(); // call 1: RLS hit, remaining stored as 1
        filter.check(None).await.unwrap(); // call 2: bucket, remaining 1→0
        assert!(filter.check(None).await.is_err()); // call 3: exhausted, RLS hit → OVER_LIMIT
        assert_eq!(mock.call_count(), 2);
    }
}
