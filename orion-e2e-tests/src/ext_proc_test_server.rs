use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use orion_data_plane_api::envoy_data_plane_api::envoy::{
    config::core::v3::{header_value_option::HeaderAppendAction, HeaderValue as EnvoyHeaderValue, HeaderValueOption},
    r#type::v3::HttpStatus as EnvoyHttpStatus,
    service::ext_proc::v3::{
        body_mutation::Mutation,
        common_response::ResponseStatus,
        external_processor_server::{ExternalProcessor, ExternalProcessorServer},
        processing_request::Request as ProcessingRequestType,
        processing_response::Response as ProcessingResponseType,
        BodyMutation, BodyResponse, CommonResponse, HeaderMutation, HeadersResponse, ImmediateResponse,
        ProcessingRequest, ProcessingResponse,
    },
};
use tokio::net::TcpListener;
use tokio::sync::{Mutex, Notify};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Server;
use tonic::{async_trait, Request, Response, Status, Streaming};
use tracing::{error, info};

use crate::Result;

pub struct ExtProcTestServer {
    addr: SocketAddr,
    shutdown: Arc<Notify>,
    captured_requests: Arc<Mutex<Vec<ProcessingRequest>>>,
}

impl ExtProcTestServer {
    #[must_use]
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub async fn captured_requests(&self) -> Vec<CapturedProcessingRequest> {
        self.captured_requests.lock().await.iter().cloned().map(CapturedProcessingRequest).collect()
    }
}

impl Drop for ExtProcTestServer {
    fn drop(&mut self) {
        self.shutdown.notify_one();
    }
}

pub struct ExtProcTestServerBuilder {
    responses: VecDeque<ProcessingResponse>,
    delay: Option<Duration>,
}

impl ExtProcTestServerBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self { responses: VecDeque::new(), delay: None }
    }

    #[must_use]
    pub fn with_response(mut self, response: ProcessingResponse) -> Self {
        self.responses.push_back(response);
        self
    }

    #[must_use]
    pub fn with_responses(mut self, responses: impl IntoIterator<Item = ProcessingResponse>) -> Self {
        self.responses.extend(responses);
        self
    }

    #[must_use]
    pub fn with_delay(mut self, delay: Duration) -> Self {
        self.delay = Some(delay);
        self
    }

    pub async fn start(self) -> Result<ExtProcTestServer> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let shutdown = Arc::new(Notify::new());
        let captured_requests = Arc::new(Mutex::new(Vec::new()));

        let service = ExtProcServiceImpl {
            responses: Arc::new(Mutex::new(self.responses)),
            captured_requests: Arc::clone(&captured_requests),
            delay: self.delay,
        };

        let shutdown_clone = Arc::clone(&shutdown);
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

        info!(?addr, "Starting ext_proc test server");

        tokio::spawn(async move {
            let router = Server::builder().add_service(ExternalProcessorServer::new(service));
            tokio::select! {
                result = router.serve_with_incoming(incoming) => {
                    if let Err(e) = result {
                        error!(?e, "ext_proc test server error");
                    }
                }
                () = shutdown_clone.notified() => {
                    info!("ext_proc test server shutting down");
                }
            }
        });

        Ok(ExtProcTestServer { addr, shutdown, captured_requests })
    }
}

impl Default for ExtProcTestServerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

struct ExtProcServiceImpl {
    responses: Arc<Mutex<VecDeque<ProcessingResponse>>>,
    captured_requests: Arc<Mutex<Vec<ProcessingRequest>>>,
    delay: Option<Duration>,
}

#[async_trait]
impl ExternalProcessor for ExtProcServiceImpl {
    type ProcessStream = ReceiverStream<std::result::Result<ProcessingResponse, Status>>;

    async fn process(
        &self,
        request: Request<Streaming<ProcessingRequest>>,
    ) -> std::result::Result<Response<Self::ProcessStream>, Status> {
        let mut inbound = request.into_inner();
        let responses = Arc::clone(&self.responses);
        let captured = Arc::clone(&self.captured_requests);
        let delay = self.delay;
        let (tx, rx) = tokio::sync::mpsc::channel(16);

        tokio::spawn(async move {
            while let Ok(Some(req)) = inbound.message().await {
                captured.lock().await.push(req.clone());

                if let Some(delay) = delay {
                    tokio::time::sleep(delay).await;
                }

                let response = responses.lock().await.pop_front();
                if let Some(resp) = response {
                    if tx.send(Ok(resp)).await.is_err() {
                        break;
                    }
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

#[derive(Clone)]
pub struct CapturedProcessingRequest(ProcessingRequest);

impl CapturedProcessingRequest {
    pub fn as_request_headers(
        &self,
    ) -> Option<&orion_data_plane_api::envoy_data_plane_api::envoy::service::ext_proc::v3::HttpHeaders> {
        match &self.0.request {
            Some(ProcessingRequestType::RequestHeaders(h)) => Some(h),
            _ => None,
        }
    }

    pub fn as_response_headers(
        &self,
    ) -> Option<&orion_data_plane_api::envoy_data_plane_api::envoy::service::ext_proc::v3::HttpHeaders> {
        match &self.0.request {
            Some(ProcessingRequestType::ResponseHeaders(h)) => Some(h),
            _ => None,
        }
    }

    pub fn as_request_body(
        &self,
    ) -> Option<&orion_data_plane_api::envoy_data_plane_api::envoy::service::ext_proc::v3::HttpBody> {
        match &self.0.request {
            Some(ProcessingRequestType::RequestBody(b)) => Some(b),
            _ => None,
        }
    }

    pub fn as_response_body(
        &self,
    ) -> Option<&orion_data_plane_api::envoy_data_plane_api::envoy::service::ext_proc::v3::HttpBody> {
        match &self.0.request {
            Some(ProcessingRequestType::ResponseBody(b)) => Some(b),
            _ => None,
        }
    }

    pub fn is_request_headers(&self) -> bool {
        matches!(&self.0.request, Some(ProcessingRequestType::RequestHeaders(_)))
    }

    pub fn is_response_headers(&self) -> bool {
        matches!(&self.0.request, Some(ProcessingRequestType::ResponseHeaders(_)))
    }

    pub fn is_request_body(&self) -> bool {
        matches!(&self.0.request, Some(ProcessingRequestType::RequestBody(_)))
    }

    pub fn is_response_body(&self) -> bool {
        matches!(&self.0.request, Some(ProcessingRequestType::ResponseBody(_)))
    }
}

pub mod ext_proc_responses {
    use super::{
        BodyMutation, BodyResponse, CommonResponse, EnvoyHeaderValue, EnvoyHttpStatus, HeaderAppendAction,
        HeaderMutation, HeaderValueOption, HeadersResponse, ImmediateResponse, Mutation, ProcessingResponse,
        ProcessingResponseType, ResponseStatus,
    };

    fn header_value_option(key: &str, value: &str) -> HeaderValueOption {
        HeaderValueOption {
            header: Some(EnvoyHeaderValue { key: key.into(), value: value.into(), ..Default::default() }),
            append_action: HeaderAppendAction::OverwriteIfExistsOrAdd.into(),
            ..Default::default()
        }
    }

    pub fn continue_request_headers() -> ProcessingResponse {
        ProcessingResponse {
            response: Some(ProcessingResponseType::RequestHeaders(HeadersResponse {
                response: Some(CommonResponse { status: ResponseStatus::Continue.into(), ..Default::default() }),
            })),
            ..Default::default()
        }
    }

    pub fn continue_response_headers() -> ProcessingResponse {
        ProcessingResponse {
            response: Some(ProcessingResponseType::ResponseHeaders(HeadersResponse {
                response: Some(CommonResponse { status: ResponseStatus::Continue.into(), ..Default::default() }),
            })),
            ..Default::default()
        }
    }

    pub fn mutate_request_headers(set: &[(&str, &str)], remove: &[&str]) -> ProcessingResponse {
        ProcessingResponse {
            response: Some(ProcessingResponseType::RequestHeaders(HeadersResponse {
                response: Some(CommonResponse {
                    status: ResponseStatus::Continue.into(),
                    header_mutation: Some(HeaderMutation {
                        set_headers: set.iter().map(|(k, v)| header_value_option(k, v)).collect(),
                        remove_headers: remove.iter().map(ToString::to_string).collect(),
                    }),
                    ..Default::default()
                }),
            })),
            ..Default::default()
        }
    }

    pub fn mutate_response_headers(set: &[(&str, &str)], remove: &[&str]) -> ProcessingResponse {
        ProcessingResponse {
            response: Some(ProcessingResponseType::ResponseHeaders(HeadersResponse {
                response: Some(CommonResponse {
                    status: ResponseStatus::Continue.into(),
                    header_mutation: Some(HeaderMutation {
                        set_headers: set.iter().map(|(k, v)| header_value_option(k, v)).collect(),
                        remove_headers: remove.iter().map(ToString::to_string).collect(),
                    }),
                    ..Default::default()
                }),
            })),
            ..Default::default()
        }
    }

    pub fn immediate_response(status_code: u32, body: &str) -> ProcessingResponse {
        ProcessingResponse {
            response: Some(ProcessingResponseType::ImmediateResponse(ImmediateResponse {
                status: Some(EnvoyHttpStatus { code: status_code as i32 }),
                body: body.as_bytes().to_vec(),
                ..Default::default()
            })),
            ..Default::default()
        }
    }

    pub fn immediate_response_with_headers(
        status_code: u32,
        body: &str,
        headers: &[(&str, &str)],
    ) -> ProcessingResponse {
        ProcessingResponse {
            response: Some(ProcessingResponseType::ImmediateResponse(ImmediateResponse {
                status: Some(EnvoyHttpStatus { code: status_code as i32 }),
                body: body.as_bytes().to_vec(),
                headers: Some(HeaderMutation {
                    set_headers: headers.iter().map(|(k, v)| header_value_option(k, v)).collect(),
                    remove_headers: vec![],
                }),
                ..Default::default()
            })),
            ..Default::default()
        }
    }

    pub fn continue_request_body(replacement: Option<&str>) -> ProcessingResponse {
        ProcessingResponse {
            response: Some(ProcessingResponseType::RequestBody(BodyResponse {
                response: Some(CommonResponse {
                    status: ResponseStatus::Continue.into(),
                    body_mutation: replacement
                        .map(|b| BodyMutation { mutation: Some(Mutation::Body(b.as_bytes().to_vec())) }),
                    ..Default::default()
                }),
            })),
            ..Default::default()
        }
    }

    pub fn continue_response_body(replacement: Option<&str>) -> ProcessingResponse {
        ProcessingResponse {
            response: Some(ProcessingResponseType::ResponseBody(BodyResponse {
                response: Some(CommonResponse {
                    status: ResponseStatus::Continue.into(),
                    body_mutation: replacement
                        .map(|b| BodyMutation { mutation: Some(Mutation::Body(b.as_bytes().to_vec())) }),
                    ..Default::default()
                }),
            })),
            ..Default::default()
        }
    }
}
