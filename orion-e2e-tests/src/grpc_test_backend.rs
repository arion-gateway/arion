// Copyright 2025 The kmesh Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::sync::{Mutex, Notify};
use tokio_stream::Stream;
use tonic::transport::Server;
use tonic::{Request, Response, Status};
use tonic_health::pb::health_check_response::ServingStatus as PbServingStatus;
use tonic_health::pb::health_server::{Health, HealthServer};
use tonic_health::pb::{HealthCheckRequest, HealthCheckResponse};
pub use tonic_health::ServingStatus;
use tracing::{debug, error, info};

use crate::{Error, Result};

pub mod test_proto {
    tonic::include_proto!("orion.test");
}

use test_proto::test_service_server::{TestService, TestServiceServer};
use test_proto::{EchoRequest, EchoResponse};

#[derive(Clone)]
struct TrackingHealthState {
    status_map: Arc<Mutex<HashMap<String, ServingStatus>>>,
    check_count: Arc<AtomicUsize>,
}

impl TrackingHealthState {
    fn new() -> Self {
        Self { status_map: Arc::new(Mutex::new(HashMap::new())), check_count: Arc::new(AtomicUsize::new(0)) }
    }

    async fn set_status(&self, service: &str, status: ServingStatus) {
        self.status_map.lock().await.insert(service.to_owned(), status);
    }

    async fn get_status(&self, service: &str) -> ServingStatus {
        self.status_map.lock().await.get(service).copied().unwrap_or(ServingStatus::Unknown)
    }

    fn increment_check_count(&self) {
        self.check_count.fetch_add(1, Ordering::SeqCst);
    }

    fn get_check_count(&self) -> usize {
        self.check_count.load(Ordering::SeqCst)
    }

    fn reset_check_count(&self) {
        self.check_count.store(0, Ordering::SeqCst);
    }
}

struct TrackingHealthService {
    state: TrackingHealthState,
}

impl TrackingHealthService {
    fn new(state: TrackingHealthState) -> Self {
        Self { state }
    }
}

#[tonic::async_trait]
impl Health for TrackingHealthService {
    async fn check(
        &self,
        request: Request<HealthCheckRequest>,
    ) -> std::result::Result<Response<HealthCheckResponse>, Status> {
        self.state.increment_check_count();

        let service_name = request.into_inner().service;
        let status = self.state.get_status(&service_name).await;

        debug!(service = %service_name, ?status, "Health check request");

        let pb_status = match status {
            ServingStatus::Unknown => PbServingStatus::Unknown,
            ServingStatus::Serving => PbServingStatus::Serving,
            ServingStatus::NotServing => PbServingStatus::NotServing,
        };

        Ok(Response::new(HealthCheckResponse { status: pb_status.into() }))
    }

    type WatchStream = Pin<Box<dyn Stream<Item = std::result::Result<HealthCheckResponse, Status>> + Send>>;

    async fn watch(
        &self,
        _request: Request<HealthCheckRequest>,
    ) -> std::result::Result<Response<Self::WatchStream>, Status> {
        Err(Status::unimplemented("watch is not implemented"))
    }
}

struct TestServiceImpl {
    backend_id: String,
    request_count: Arc<AtomicUsize>,
}

#[tonic::async_trait]
impl TestService for TestServiceImpl {
    async fn echo(&self, request: Request<EchoRequest>) -> std::result::Result<Response<EchoResponse>, Status> {
        let req = request.into_inner();
        debug!(backend_id = %self.backend_id, message = %req.message, "Received echo request");

        self.request_count.fetch_add(1, Ordering::SeqCst);

        let response = EchoResponse { message: req.message, backend_id: self.backend_id.clone() };

        Ok(Response::new(response))
    }
}

pub struct GrpcTestBackendBuilder {
    port: Option<u16>,
    enable_health: bool,
    enable_test_service: bool,
    backend_id: String,
}

impl GrpcTestBackendBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self { port: None, enable_health: false, enable_test_service: false, backend_id: String::new() }
    }

    #[must_use]
    pub fn port(mut self, port: u16) -> Self {
        self.port = Some(port);
        self
    }

    #[must_use]
    pub fn with_health(mut self) -> Self {
        self.enable_health = true;
        self
    }

    #[must_use]
    pub fn with_test_service(mut self) -> Self {
        self.enable_test_service = true;
        self
    }

    #[must_use]
    pub fn backend_id(mut self, id: impl Into<String>) -> Self {
        self.backend_id = id.into();
        self
    }

    pub async fn start(self) -> Result<GrpcTestBackend> {
        let addr = match self.port {
            Some(port) => SocketAddr::from(([127, 0, 0, 1], port)),
            None => SocketAddr::from(([127, 0, 0, 1], 0)),
        };

        let listener = TcpListener::bind(addr).await?;
        let actual_addr = listener.local_addr()?;

        info!(?actual_addr, backend_id = %self.backend_id, "Starting gRPC test backend");

        let shutdown = Arc::new(Notify::new());
        let request_count = Arc::new(AtomicUsize::new(0));

        let test_service_enabled = self.enable_test_service;

        let health_state = self.enable_health.then(TrackingHealthState::new);

        let test_service = self.enable_test_service.then(|| TestServiceImpl { backend_id: self.backend_id.clone(), request_count: Arc::clone(&request_count) });

        let shutdown_clone = Arc::clone(&shutdown);
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

        match (health_state.as_ref().map(|s| HealthServer::new(TrackingHealthService::new(s.clone()))), test_service) {
            (Some(health_svc), Some(test_svc)) => {
                tokio::spawn(async move {
                    let router =
                        Server::builder().add_service(health_svc).add_service(TestServiceServer::new(test_svc));

                    tokio::select! {
                        result = router.serve_with_incoming(incoming) => {
                            if let Err(e) = result {
                                error!(?e, "gRPC server error");
                            }
                        }
                        () = shutdown_clone.notified() => {
                            info!("gRPC test backend shutting down");
                        }
                    }
                });
            },
            (Some(health_svc), None) => {
                tokio::spawn(async move {
                    let router = Server::builder().add_service(health_svc);

                    tokio::select! {
                        result = router.serve_with_incoming(incoming) => {
                            if let Err(e) = result {
                                error!(?e, "gRPC server error");
                            }
                        }
                        () = shutdown_clone.notified() => {
                            info!("gRPC test backend shutting down");
                        }
                    }
                });
            },
            (None, Some(test_svc)) => {
                tokio::spawn(async move {
                    let router = Server::builder().add_service(TestServiceServer::new(test_svc));

                    tokio::select! {
                        result = router.serve_with_incoming(incoming) => {
                            if let Err(e) = result {
                                error!(?e, "gRPC server error");
                            }
                        }
                        () = shutdown_clone.notified() => {
                            info!("gRPC test backend shutting down");
                        }
                    }
                });
            },
            (None, None) => {},
        }

        Ok(GrpcTestBackend { addr: actual_addr, health_state, shutdown, request_count, test_service_enabled })
    }
}

impl Default for GrpcTestBackendBuilder {
    fn default() -> Self {
        Self::new()
    }
}

pub struct GrpcTestBackend {
    addr: SocketAddr,
    health_state: Option<TrackingHealthState>,
    shutdown: Arc<Notify>,
    request_count: Arc<AtomicUsize>,
    test_service_enabled: bool,
}

impl GrpcTestBackend {
    #[must_use]
    pub fn builder() -> GrpcTestBackendBuilder {
        GrpcTestBackendBuilder::new()
    }

    #[must_use]
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    #[must_use]
    pub fn port(&self) -> u16 {
        self.addr.port()
    }

    pub async fn set_serving(&self, service: &str) -> Result<()> {
        let state = self.health_state.as_ref().ok_or(Error::HealthServiceNotEnabled)?;
        state.set_status(service, ServingStatus::Serving).await;
        Ok(())
    }

    pub async fn set_not_serving(&self, service: &str) -> Result<()> {
        let state = self.health_state.as_ref().ok_or(Error::HealthServiceNotEnabled)?;
        state.set_status(service, ServingStatus::NotServing).await;
        Ok(())
    }

    pub async fn set_health_status(&self, service: &str, status: ServingStatus) -> Result<()> {
        let state = self.health_state.as_ref().ok_or(Error::HealthServiceNotEnabled)?;
        state.set_status(service, status).await;
        Ok(())
    }

    pub async fn clear_health_status(&self, service: &str) -> Result<()> {
        let state = self.health_state.as_ref().ok_or(Error::HealthServiceNotEnabled)?;
        state.set_status(service, ServingStatus::Unknown).await;
        Ok(())
    }

    pub fn health_check_count(&self) -> Result<usize> {
        let state = self.health_state.as_ref().ok_or(Error::HealthServiceNotEnabled)?;
        Ok(state.get_check_count())
    }

    pub async fn await_health_check_count(&self, count: usize, timeout: Duration) -> Result<()> {
        let state = self.health_state.as_ref().ok_or(Error::HealthServiceNotEnabled)?;

        let deadline = tokio::time::Instant::now() + timeout;
        while state.get_check_count() < count {
            if tokio::time::Instant::now() >= deadline {
                return Err(Error::HealthCheckCountTimeout {
                    expected: count,
                    actual: state.get_check_count(),
                    timeout,
                });
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok(())
    }

    pub fn reset_health_check_count(&self) {
        if let Some(state) = &self.health_state {
            state.reset_check_count();
        }
    }

    pub fn request_count(&self) -> Result<usize> {
        if !self.test_service_enabled {
            return Err(Error::TestServiceNotEnabled);
        }
        Ok(self.request_count.load(Ordering::SeqCst))
    }

    pub async fn await_request_count(&self, count: usize, timeout: Duration) -> Result<()> {
        if !self.test_service_enabled {
            return Err(Error::TestServiceNotEnabled);
        }

        let deadline = tokio::time::Instant::now() + timeout;
        while self.request_count.load(Ordering::SeqCst) < count {
            if tokio::time::Instant::now() >= deadline {
                return Err(Error::RequestCountTimeout {
                    expected: count,
                    actual: self.request_count.load(Ordering::SeqCst),
                    timeout,
                });
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok(())
    }

    pub fn reset_request_count(&self) {
        self.request_count.store(0, Ordering::SeqCst);
    }

    pub fn shutdown(&self) {
        self.shutdown.notify_one();
    }
}

impl Drop for GrpcTestBackend {
    fn drop(&mut self) {
        self.shutdown.notify_one();
    }
}
