use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use orion_data_plane_api::envoy_data_plane_api::envoy::service::ratelimit::v3::{
    rate_limit_response,
    rate_limit_service_server::{RateLimitService, RateLimitServiceServer},
    RateLimitRequest, RateLimitResponse,
};
use tokio::net::TcpListener;
use tokio::sync::{Mutex, Notify};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tonic::{async_trait, Request, Response, Status};
use tracing::{error, info};

use crate::Result;

pub struct RlsTestServer {
    addr: SocketAddr,
    shutdown: Arc<Notify>,
    call_count: Arc<AtomicUsize>,
}

impl RlsTestServer {
    #[must_use]
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    #[must_use]
    pub fn call_count(&self) -> usize {
        self.call_count.load(Ordering::SeqCst)
    }
}

impl Drop for RlsTestServer {
    fn drop(&mut self) {
        self.shutdown.notify_one();
    }
}

pub struct RlsTestServerBuilder {
    responses: VecDeque<RateLimitResponse>,
    always_fail: bool,
}

impl RlsTestServerBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self { responses: VecDeque::new(), always_fail: false }
    }

    #[must_use]
    pub fn always_failing() -> Self {
        Self { responses: VecDeque::new(), always_fail: true }
    }

    #[must_use]
    pub fn with_response(mut self, response: RateLimitResponse) -> Self {
        self.responses.push_back(response);
        self
    }

    #[must_use]
    pub fn with_responses(mut self, responses: impl IntoIterator<Item = RateLimitResponse>) -> Self {
        self.responses.extend(responses);
        self
    }

    pub async fn start(self) -> Result<RlsTestServer> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let shutdown = Arc::new(Notify::new());
        let call_count = Arc::new(AtomicUsize::new(0));

        let service = RlsServiceImpl {
            responses: Arc::new(Mutex::new(self.responses)),
            call_count: Arc::clone(&call_count),
            always_fail: self.always_fail,
        };

        let shutdown_clone = Arc::clone(&shutdown);

        info!(?addr, "Starting RLS test server");

        tokio::spawn(async move {
            let router = Server::builder().add_service(RateLimitServiceServer::new(service));
            tokio::select! {
                result = router.serve_with_incoming(TcpListenerStream::new(listener)) => {
                    if let Err(e) = result {
                        error!(?e, "RLS test server error");
                    }
                }
                () = shutdown_clone.notified() => {
                    info!("RLS test server shutting down");
                }
            }
        });

        Ok(RlsTestServer { addr, shutdown, call_count })
    }
}

impl Default for RlsTestServerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

struct RlsServiceImpl {
    responses: Arc<Mutex<VecDeque<RateLimitResponse>>>,
    call_count: Arc<AtomicUsize>,
    always_fail: bool,
}

#[async_trait]
impl RateLimitService for RlsServiceImpl {
    async fn should_rate_limit(
        &self,
        _request: Request<RateLimitRequest>,
    ) -> std::result::Result<Response<RateLimitResponse>, Status> {
        self.call_count.fetch_add(1, Ordering::SeqCst);
        if self.always_fail {
            return Err(Status::unavailable("simulated RLS failure"));
        }
        let resp = self.responses.lock().await.pop_front().unwrap_or_else(|| RateLimitResponse {
            overall_code: rate_limit_response::Code::Ok as i32,
            ..Default::default()
        });
        Ok(Response::new(resp))
    }
}

pub mod rls_responses {
    use std::time::{SystemTime, UNIX_EPOCH};

    use orion_data_plane_api::envoy_data_plane_api::{
        envoy::service::ratelimit::v3::{rate_limit_response, RateLimitResponse},
        google::protobuf::Timestamp,
    };

    pub fn ok() -> RateLimitResponse {
        RateLimitResponse { overall_code: rate_limit_response::Code::Ok as i32, ..Default::default() }
    }

    pub fn over_limit() -> RateLimitResponse {
        RateLimitResponse { overall_code: rate_limit_response::Code::OverLimit as i32, ..Default::default() }
    }

    pub fn ok_with_quota(requests: u32) -> RateLimitResponse {
        #[allow(clippy::unwrap_used)]
        #[allow(clippy::cast_possible_wrap, reason = "Unix timestamp in seconds fits comfortably in i64")]
        let valid_secs = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64 + 3600;
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
}
