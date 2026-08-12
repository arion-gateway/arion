#![allow(clippy::similar_names)]

use super::*;
use crate::{
    body::{
        instrumented_body::InstrumentedBody, poly_body::PolyBodyError, response_flags::BodyKind,
        timeout_body::TimeoutBodyError,
    },
    listeners::http_connection_manager::ext_proc::{
        kind::MessageKind, mutation::apply_header_mutations, r#override::ModeSelector,
    },
};
use http::{Method, Version};
use http_body_util::{Empty, StreamBody};
use orion_configuration::config::network_filters::http_connection_manager::http_filters::ext_proc::{
    BodyProcessingMode, ExternalProcessor as ExternalProcessorConfig, GoogleGrpc, GrpcService, HeaderProcessingMode,
    RouteCacheAction, TrailerProcessingMode,
};
use orion_data_plane_api::envoy_data_plane_api::envoy::extensions::filters::http::ext_proc::v3::{
    processing_mode, ProcessingMode as EnvoyProcessingMode,
};
use orion_data_plane_api::envoy_data_plane_api::{
    envoy::{
        config::core::v3::{
            header_value_option::HeaderAppendAction, HeaderValue as EnvoyHeaderValue, HeaderValueOption,
        },
        r#type::v3::HttpStatus as EnvoyHttpStatus,
        service::ext_proc::v3::{
            body_mutation::Mutation,
            common_response::ResponseStatus,
            external_processor_server::{ExternalProcessor as ExternalProcessorService, ExternalProcessorServer},
            processing_request::Request as ProcessingRequestType,
            processing_response::Response as ProcessingResponseType,
            BodyMutation, BodyResponse, CommonResponse, HeaderMutation, HeadersResponse, StreamedBodyResponse,
            TrailersResponse,
        },
    },
    tonic::{
        async_trait,
        transport::{Error as TonicError, Server},
        Request as TonicRequest, Response as TonicResponse,
    },
};
use pingora::prelude::fast_timeout::fast_timeout;
use std::{
    collections::VecDeque,
    net::SocketAddr,
    ops::{Deref, DerefMut},
    str::FromStr,
};
use tokio::{net::TcpListener, task::JoinHandle};
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};

use tokio::select;
use tokio::sync::mpsc::Sender;
use tokio_util::sync::CancellationToken;

pub struct OutState {
    last_end_of_stream: Option<bool>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MockProcessingResponse {
    response: ProcessingResponse,
    delay: Option<Duration>,
    expected_end_of_stream: Option<bool>,
}

impl Deref for MockProcessingResponse {
    type Target = ProcessingResponse;

    fn deref(&self) -> &Self::Target {
        &self.response
    }
}

impl DerefMut for MockProcessingResponse {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.response
    }
}

impl MockProcessingResponse {
    #[allow(dead_code)]
    pub fn new(response: ProcessingResponse) -> Self {
        Self { response, delay: None, expected_end_of_stream: None }
    }

    pub fn with_delay(mut self, delay: Duration) -> Self {
        self.delay = Some(delay);
        self
    }

    pub fn with_expected_end_of_stream(mut self, value: bool) -> Self {
        self.expected_end_of_stream = Some(value);
        self
    }

    pub fn delay(&self) -> Option<Duration> {
        self.delay
    }

    pub fn expected_end_of_stream(&self) -> Option<bool> {
        self.expected_end_of_stream
    }

    pub fn into_inner(self) -> ProcessingResponse {
        self.response
    }
}

impl From<ProcessingResponse> for MockProcessingResponse {
    fn from(response: ProcessingResponse) -> Self {
        Self { response, delay: None, expected_end_of_stream: None }
    }
}

#[derive(Clone)]
pub struct MockExternalProcessorState {
    responses: VecDeque<MockProcessingResponse>,
    token: CancellationToken,
    sender: Option<Sender<OutState>>,
    observability_mode: bool,
}

impl std::fmt::Debug for MockExternalProcessorState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MockExternalProcessorState")
            .field("responses", &self.responses)
            .field("token", &self.token)
            .field("sender", &"Sender<OutState>")
            .field("observability_mode", &self.observability_mode)
            .finish()
    }
}

impl PartialEq for MockExternalProcessorState {
    fn eq(&self, other: &Self) -> bool {
        self.responses == other.responses
    }
}

impl MockExternalProcessorState {
    pub fn new() -> Self {
        Self { responses: VecDeque::new(), token: CancellationToken::new(), sender: None, observability_mode: false }
    }

    pub fn with_observability(self, mode: bool) -> Self {
        Self { observability_mode: mode, ..self }
    }

    pub fn with_sender(mut self, sender: Sender<OutState>) -> Self {
        self.sender = Some(sender);
        self
    }

    pub fn add_response(mut self, response: MockProcessingResponse) -> Self {
        self.responses.push_back(response);
        self
    }

    pub fn get_next_response(&mut self) -> Option<MockProcessingResponse> {
        self.responses.pop_front()
    }

    #[allow(dead_code)]
    pub fn responses_len(&self) -> usize {
        self.responses.len()
    }

    pub fn is_empty(&self) -> bool {
        self.responses.is_empty()
    }

    pub fn is_observability_mode(&self) -> bool {
        self.observability_mode
    }
}

#[derive(Debug)]
pub struct MockExternalProcessor {
    state: MockExternalProcessorState,
}

impl MockExternalProcessor {
    pub fn new(state: MockExternalProcessorState) -> Self {
        Self { state }
    }
}

pub fn processing_match(req: &ProcessingRequest, res: &ProcessingResponse) -> bool {
    matches!(
        (&req.request, &res.response),
        (
            Some(ProcessingRequestType::RequestHeaders(_)),
            Some(ProcessingResponseType::RequestHeaders(_) | ProcessingResponseType::StreamedImmediateResponse(_))
        ) | (Some(ProcessingRequestType::RequestBody(_)), Some(ProcessingResponseType::RequestBody(_)))
            | (Some(ProcessingRequestType::RequestTrailers(_)), Some(ProcessingResponseType::RequestTrailers(_)))
            | (Some(ProcessingRequestType::ResponseHeaders(_)), Some(ProcessingResponseType::ResponseHeaders(_)))
            | (Some(ProcessingRequestType::ResponseBody(_)), Some(ProcessingResponseType::ResponseBody(_)))
            | (Some(ProcessingRequestType::ResponseTrailers(_)), Some(ProcessingResponseType::ResponseTrailers(_)))
            | (Some(_), Some(ProcessingResponseType::ImmediateResponse(_)))
    )
}

#[async_trait]
impl ExternalProcessorService for MockExternalProcessor {
    type ProcessStream = ReceiverStream<Result<ProcessingResponse, Status>>;

    async fn process(
        &self,
        request: TonicRequest<Streaming<ProcessingRequest>>,
    ) -> Result<TonicResponse<Self::ProcessStream>, Status> {
        let mut inbound = request.into_inner();
        let mut state = self.state.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(16);
        let mut last_end_of_stream = None;
        let mut cancelled = false;

        tokio::spawn(async move {
            loop {
                select! {
                     result_msg = inbound.message() => {
                        let Ok(Some(processing_request)) = result_msg else { break }; // Ok(None) or Err(_) indicates stream end or error, we break the loop in both cases
                        let end_of_stream = processing_request.request.as_ref().map(|req| {
                            match req {
                               ProcessingRequestType::RequestHeaders(hdrs) | ProcessingRequestType::ResponseHeaders(hdrs) => {
                                   Some(hdrs.end_of_stream)
                               }
                               ProcessingRequestType::RequestBody(http_body) | ProcessingRequestType::ResponseBody(http_body) => {
                                   Some(http_body.end_of_stream)
                               },
                               ProcessingRequestType::RequestTrailers(_) | ProcessingRequestType::ResponseTrailers(_) => None,
                            }
                        }).unwrap();

                        if end_of_stream.is_some() {
                            last_end_of_stream = end_of_stream;
                        }

                        if let Some(processing_response) = state.get_next_response() {
                            if !processing_match(&processing_request, &processing_response) {
                                let _ = tx.send(Err(Status::internal(format!("MockExternalProcessor: Received a processing request that does not match the expected response type. Request: {processing_request:#?}, Response: {processing_response:#?}")))).await.ok();
                                break;
                            }

                            if let Some(expected_end_of_stream) = processing_response.expected_end_of_stream() {
                                if end_of_stream != Some(expected_end_of_stream) {
                                    let _ = tx.send(Err(Status::internal(format!("MockExternalProcessor: Received a processing request with end_of_stream={end_of_stream:?} but expected end_of_stream={expected_end_of_stream:?}. Request: {processing_request:#?}, Response: {processing_response:#?}")))).await.ok();
                                    break;
                                }
                            }

                            if processing_response.mode_override.is_some() {
                                last_end_of_stream = None;
                            }

                            if let Some(delay) = processing_response.delay() {
                                tokio::time::sleep(delay).await;
                            }

                            if !state.is_observability_mode()
                                && tx.send(Ok(processing_response.into_inner())).await.is_err() {
                                    break;
                                }
                        } else {
                            let _ = tx.send(Err(Status::internal("MockExternalProcessor: Received more processing requests than configured responses."))).await.ok();
                            break;
                        }
                     }
                     () = state.token.cancelled() => {
                         cancelled = true;
                         break;
                     }
                }
            }

            if !cancelled && !state.is_empty() {
                let _ = tx
                    .send(Err(Status::internal(
                        "MockExternalProcessor: Stream ended but there are still unprocessed responses.",
                    )))
                    .await
                    .ok();
            }

            if let Some(ref sender) = state.sender {
                _ = sender.send(OutState { last_end_of_stream }).await;
            }
        });

        let output_stream = ReceiverStream::new(rx);
        Ok(TonicResponse::new(output_stream))
    }
}

async fn start_mock_server(state: MockExternalProcessorState) -> (SocketAddr, JoinHandle<Result<(), TonicError>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let socket_addr = listener.local_addr().unwrap();
    let incoming = TcpListenerStream::new(listener);
    let mock_service = MockExternalProcessor::new(state);
    let server =
        Server::builder().add_service(ExternalProcessorServer::new(mock_service)).serve_with_incoming(incoming);
    let server_handle = tokio::spawn(server);
    (socket_addr, server_handle)
}

#[derive(Debug, Default)]
struct MockMessage<M: MessageKind> {
    headers: Vec<Option<(&'static str, &'static str)>>,
    body: Vec<&'static str>,
    trailers: Vec<Option<(&'static str, &'static str)>>,
    _marker: std::marker::PhantomData<M>,
}

impl<M: MessageKind> MockMessage<M> {
    fn new(
        headers: Vec<Option<(&'static str, &'static str)>>,
        body: Vec<&'static str>,
        trailers: Vec<Option<(&'static str, &'static str)>>,
    ) -> Self {
        Self { headers, body, trailers, _marker: std::marker::PhantomData }
    }

    fn orig_headers_map(&self) -> http::HeaderMap {
        self.apply_header_mutation(None)
    }

    fn orig_trailers_map(&self) -> http::HeaderMap {
        self.apply_trailer_mutation(None)
    }

    fn apply_header_mutation(&self, header_mutation: Option<&HeaderMutation>) -> http::HeaderMap {
        let mut header_map = http::HeaderMap::with_capacity(self.headers.len());
        for (key, value) in self.headers.iter().flatten() {
            header_map.insert(http::HeaderName::from_static(key), http::HeaderValue::from_static(value));
        }
        if let Some(mutation) = header_mutation.as_ref() {
            apply_header_mutations(&mut header_map, (*mutation).clone(), None).unwrap();
        }
        header_map
    }

    fn apply_header_mutation_from_header_map(header_mutation: Option<&HeaderMutation>, headers: &mut http::HeaderMap) {
        if let Some(mutation) = header_mutation.as_ref() {
            apply_header_mutations(headers, (*mutation).clone(), None).unwrap();
        }
    }

    fn apply_body_mutation(&self, body_mutation: Option<&Mutation>, idx: usize) -> Bytes {
        let body_replacement: Option<Bytes> = match body_mutation {
            Some(Mutation::Body(bytes)) => Some(bytes.clone().into()),
            Some(Mutation::ClearBody(true)) => Some(Bytes::new()),
            Some(Mutation::ClearBody(false)) | None => None,
            Some(Mutation::StreamedResponse(chunk)) => Some(chunk.clone().body.into()),
        };

        if let Some(chunk) = self.body.get(idx) {
            let b = *chunk;
            body_replacement.unwrap_or(b.into())
        } else {
            body_replacement.unwrap()
        }
    }

    fn apply_trailer_mutation(&self, trailer_mutation: Option<&HeaderMutation>) -> http::HeaderMap {
        let mut trailer_map = http::HeaderMap::with_capacity(self.trailers.len());
        for (key, value) in self.trailers.iter().flatten() {
            trailer_map.insert(http::HeaderName::from_static(key), http::HeaderValue::from_static(value));
        }
        if let Some(mutation) = trailer_mutation.as_ref() {
            apply_header_mutations(&mut trailer_map, (*mutation).clone(), None).unwrap();
        }
        trailer_map
    }
}

async fn build_request_from_mock(mock_request: &MockMessage<RequestMsg>) -> Request<OrionRequestBody> {
    let mut req = Request::builder().method(Method::GET).uri("http://example.com/test").version(Version::HTTP_11);

    if let Some(headers) = transform(mock_request.headers.clone()) {
        for (name, value) in headers {
            req = req.header(name, value);
        }
    }

    let mut trailers_map = http::HeaderMap::with_capacity(mock_request.trailers.len());
    if let Some(trailers) = transform(mock_request.trailers.clone()) {
        trailers_map = if trailers.is_empty() {
            http::HeaderMap::default()
        } else {
            let mut map = http::header::HeaderMap::new();
            for (name, value) in trailers {
                map.append(
                    http::header::HeaderName::from_str(name).unwrap(),
                    http::header::HeaderValue::from_str(value).unwrap().clone(),
                );
            }
            map
        };
    }

    let body = create_collected_body_with_trailers(
        mock_request.body.clone(),
        if trailers_map.is_empty() { None } else { Some(trailers_map) },
    )
    .await;

    req.body(InstrumentedBody::new(BodyKind::Request, TimeoutBody::new(None, body), None, |_, _, _, _| {})).unwrap()
}

async fn build_response_from_mock(mock_response: &MockMessage<ResponseMsg>) -> Response<OrionResponseBody> {
    let mut resp = Response::builder().version(Version::HTTP_11);

    if let Some(headers) = transform(mock_response.headers.clone()) {
        for (name, value) in headers {
            resp = resp.header(name, value);
        }
    }

    let mut trailers_map = http::HeaderMap::with_capacity(mock_response.trailers.len());
    if let Some(trailers) = transform(mock_response.trailers.clone()) {
        trailers_map = if trailers.is_empty() {
            http::HeaderMap::default()
        } else {
            let mut map = http::header::HeaderMap::new();
            for (name, value) in trailers {
                map.append(
                    http::header::HeaderName::from_str(name).unwrap(),
                    http::header::HeaderValue::from_str(value).unwrap().clone(),
                );
            }
            map
        };
    }

    let body = create_collected_body_with_trailers(
        mock_response.body.clone(),
        if trailers_map.is_empty() { None } else { Some(trailers_map) },
    )
    .await;

    resp.body(TimeoutBody::new(None, body)).unwrap()
}

fn create_default_config_for_ext_proc_filter(
    server_addr: SocketAddr,
    processing_mode: ProcessingMode,
) -> ExternalProcessorConfig {
    ExternalProcessorConfig {
        grpc_service: GrpcService {
            service_specifier: GrpcServiceSpecifier::GoogleGrpc(GoogleGrpc {
                target_uri: format!("http://{server_addr}"),
            }),
            timeout: Some(Duration::from_secs(1)),
        },
        processing_mode: Some(processing_mode),
        observability_mode: false,
        failure_mode_allow: false,
        disable_immediate_response: false,
        forward_rules: None,
        mutation_rules: None,
        message_timeout: Some(Duration::from_secs(5)),
        max_message_timeout: None,
        allowed_override_modes: vec![],
        allow_mode_override: false,
        route_cache_action: RouteCacheAction::Default,
        send_body_without_waiting_for_header_response: false,
        deferred_close_timeout: None,
    }
}

#[inline]
fn create_header_mutation(headers: Vec<(&str, &str)>) -> Option<HeaderMutation> {
    if headers.is_empty() {
        None
    } else {
        Some(HeaderMutation {
            set_headers: headers
                .into_iter()
                .map(|(key, value)| HeaderValueOption {
                    header: Some(EnvoyHeaderValue { key: key.to_owned(), value: value.to_owned(), raw_value: vec![] }),
                    #[allow(deprecated)]
                    append: None,
                    append_action: HeaderAppendAction::OverwriteIfExistsOrAdd as i32,
                    keep_empty_value: false,
                })
                .collect(),
            remove_headers: vec![],
        })
    }
}

#[inline]
fn create_body_mutation(body: Vec<u8>, end_of_stream: Option<bool>) -> BodyMutation {
    if let Some(end_of_stream) = end_of_stream {
        BodyMutation {
            mutation: Some(Mutation::StreamedResponse(StreamedBodyResponse {
                body,
                end_of_stream,
                end_of_stream_without_message: false,
                grpc_message_compressed: false,
            })),
        }
    } else {
        BodyMutation { mutation: Some(Mutation::Body(body)) }
    }
}

#[inline]
fn create_trailer_mutation(trailers: Vec<(&str, &str)>) -> Option<HeaderMutation> {
    create_header_mutation(trailers)
}

#[inline]
fn convert_trailers_to_envoy_header_map(trailers: Vec<(&str, &str)>) -> ProstHeaderMap {
    ProstHeaderMap {
        headers: trailers
            .into_iter()
            .map(|(key, value)| EnvoyHeaderValue { key: key.to_owned(), value: value.to_owned(), raw_value: vec![] })
            .collect(),
    }
}

fn create_immediate_response(
    headers: Vec<Option<(&str, &str)>>,
    body_data: Option<Vec<u8>>,
    status: i32,
) -> MockProcessingResponse {
    let response_headers = transform(headers).and_then(|hdrs| create_header_mutation(hdrs));
    let response_body = body_data.unwrap_or_default();

    let immediate_response = ImmediateResponse {
        status: Some(EnvoyHttpStatus { code: status }),
        headers: response_headers,
        body: response_body,
        grpc_status: None,
        details: "immediate_response".to_owned(),
    };

    ProcessingResponse {
        response: Some(ProcessingResponseType::ImmediateResponse(immediate_response)),
        mode_override: None,
        dynamic_metadata: None,
        override_message_timeout: None,
        request_drain: false,
    }
    .into()
}

#[allow(clippy::too_many_arguments)]
fn create_headers_response<M: MessageKind>(
    headers: Vec<Option<(&str, &str)>>,
    body_data: Option<Vec<u8>>,
    trailers: Vec<Option<(&str, &str)>>,
    status: i32,
    end_of_stream: Option<bool>,
) -> MockProcessingResponse {
    let header_mutation = transform(headers).and_then(|hdrs| create_header_mutation(hdrs));
    let body_mutation = body_data.map(|body| create_body_mutation(body, end_of_stream));
    let mut trailers_new = None;
    if status == ResponseStatus::ContinueAndReplace as i32 {
        if let Some(trailers) = transform(trailers) {
            trailers_new = Some(convert_trailers_to_envoy_header_map(trailers));
        }
    }

    let header_response = HeadersResponse {
        response: Some(CommonResponse {
            status,
            header_mutation,
            body_mutation,
            trailers: trailers_new,
            clear_route_cache: false,
        }),
    };

    let response = if M::IS_RESPONSE {
        Some(ProcessingResponseType::ResponseHeaders(header_response))
    } else {
        Some(ProcessingResponseType::RequestHeaders(header_response))
    };

    ProcessingResponse {
        response,
        mode_override: None,
        dynamic_metadata: None,
        override_message_timeout: None,
        request_drain: false,
    }
    .into()
}

pub async fn create_collected_body_with_trailers(frames: Vec<&str>, trailers: Option<http::HeaderMap>) -> PolyBody {
    let mut chunks: Vec<Result<http_body::Frame<Bytes>, Infallible>> = Vec::new();

    for frame in frames {
        let chunk_bytes = Bytes::from(frame.to_owned());
        chunks.push(Ok(http_body::Frame::data(chunk_bytes)));
    }

    if let Some(t_map) = trailers {
        chunks.push(Ok(http_body::Frame::trailers(t_map)));
    }

    // Collected body does not implement is_end_stream method, which always returns false by default (even for empty bodies).
    // For this reason, we create the Collected body only when the vector of chunks is not empty.
    // Empty instead implements is_end_stream correctly which always returns true.

    if chunks.is_empty() {
        PolyBody::from(Empty::new())
    } else {
        let body_stream = futures_util::stream::iter(chunks);
        PolyBody::from(BodyExt::collect(StreamBody::new(body_stream)).await.unwrap())
    }
}

pub async fn to_body_data_chunks(mut body: Collected<Bytes>) -> Vec<Bytes> {
    let mut chunks = Vec::new();
    while let Some(frame_result) = body.frame().await {
        let Ok(frame) = frame_result;
        if let Some(data) = frame.data_ref() {
            chunks.push(data.clone());
        }
    }

    chunks
}

#[allow(clippy::too_many_arguments)]
fn create_body_response<M: MessageKind>(
    headers: Vec<Option<(&str, &str)>>,
    body_data: Option<Vec<u8>>,
    trailers: Vec<Option<(&str, &str)>>,
    status: i32,
    end_of_stream: Option<bool>,
) -> MockProcessingResponse {
    let header_mutation = transform(headers).and_then(|hdrs| create_header_mutation(hdrs));
    let body_mutation = body_data.map(|body| create_body_mutation(body, end_of_stream));
    let mut trailers_new = None;
    if status == ResponseStatus::ContinueAndReplace as i32 {
        if let Some(trailers) = transform(trailers) {
            trailers_new = Some(convert_trailers_to_envoy_header_map(trailers));
        }
    }

    let body_response = BodyResponse {
        response: Some(CommonResponse {
            status,
            header_mutation,
            body_mutation,
            trailers: trailers_new,
            clear_route_cache: false,
        }),
    };

    let response = if M::IS_RESPONSE {
        Some(ProcessingResponseType::ResponseBody(body_response))
    } else {
        Some(ProcessingResponseType::RequestBody(body_response))
    };

    ProcessingResponse {
        response,
        mode_override: None,
        dynamic_metadata: None,
        override_message_timeout: None,
        request_drain: false,
    }
    .into()
}

fn create_trailers_response<M: MessageKind>(trailers: Vec<Option<(&str, &str)>>) -> MockProcessingResponse {
    let trailer_mutation = transform(trailers).and_then(|trls| create_trailer_mutation(trls));

    let trailers_response = TrailersResponse { header_mutation: trailer_mutation };
    let response = if M::IS_RESPONSE {
        Some(ProcessingResponseType::ResponseTrailers(trailers_response))
    } else {
        Some(ProcessingResponseType::RequestTrailers(trailers_response))
    };
    ProcessingResponse {
        response,
        mode_override: None,
        dynamic_metadata: None,
        override_message_timeout: None,
        request_drain: false,
    }
    .into()
}

static HEADER_PROCESSING_MODE: [HeaderProcessingMode; 3] =
    [HeaderProcessingMode::Default, HeaderProcessingMode::Send, HeaderProcessingMode::Skip];
static BODY_PROCESSING_MODE: [BodyProcessingMode; 5] = [
    BodyProcessingMode::None,
    BodyProcessingMode::Streamed,
    BodyProcessingMode::Buffered,
    BodyProcessingMode::BufferedPartial,
    BodyProcessingMode::FullDuplexStreamed,
];
static TRAILER_PROCESSING_MODE: [TrailerProcessingMode; 3] =
    [TrailerProcessingMode::Default, TrailerProcessingMode::Skip, TrailerProcessingMode::Send];

fn generate_processing_mode_configurations<M: MessageKind>() -> Vec<ProcessingMode> {
    if M::IS_RESPONSE {
        HEADER_PROCESSING_MODE
            .iter()
            .flat_map(|&header_mode| {
                BODY_PROCESSING_MODE.iter().flat_map(move |&body_mode| {
                    TRAILER_PROCESSING_MODE.iter().map(move |&trailer_mode| ProcessingMode {
                        request_header_mode: HeaderProcessingMode::Skip,
                        request_body_mode: BodyProcessingMode::None,
                        request_trailer_mode: TrailerProcessingMode::Skip,
                        response_header_mode: header_mode,
                        response_body_mode: body_mode,
                        response_trailer_mode: trailer_mode,
                    })
                })
            })
            .collect()
    } else {
        HEADER_PROCESSING_MODE
            .iter()
            .flat_map(|&header_mode| {
                BODY_PROCESSING_MODE.iter().flat_map(move |&body_mode| {
                    TRAILER_PROCESSING_MODE.iter().map(move |&trailer_mode| ProcessingMode {
                        request_header_mode: header_mode,
                        request_body_mode: body_mode,
                        request_trailer_mode: trailer_mode,
                        response_header_mode: HeaderProcessingMode::Skip,
                        response_body_mode: BodyProcessingMode::None,
                        response_trailer_mode: TrailerProcessingMode::Skip,
                    })
                })
            })
            .collect()
    }
}

// we always include extra headers in the generated request
static REQUEST_HEADERS: [Option<(&str, &str)>; 1] = [Some(("x-test-header", "original-header-value"))];
static REQUEST_BODIES: [&[&str]; 2] = [&[], &["original body"]];
static REQUEST_TRAILERS: [Option<(&str, &str)>; 2] = [None, Some(("x-test-trailer", "original-trailer-value"))];

fn generate_mock_messages<M: MessageKind>() -> Vec<MockMessage<M>> {
    REQUEST_HEADERS
        .iter()
        .flat_map(|&headers| {
            REQUEST_BODIES.iter().flat_map(move |&body| {
                REQUEST_TRAILERS.iter().map(move |&trailers| MockMessage::<M> {
                    headers: vec![headers],
                    body: body.into(),
                    trailers: vec![trailers],
                    _marker: std::marker::PhantomData,
                })
            })
        })
        .collect()
}

static HEADER_MODIFICATIONS: [Option<(&str, &str)>; 2] = [None, Some(("x-test-header", "ext-proc-header-value"))];
static BODY_MODIFICATIONS: [Option<&str>; 2] = [None, Some("external processor body")];
static TRAILER_MODIFICATIONS: [Option<(&str, &str)>; 2] = [None, Some(("x-test-trailer", "ext-proc-trailer-value"))];

fn transform<T>(vec: Vec<Option<T>>) -> Option<Vec<T>> {
    let filtered: Vec<T> = vec.into_iter().flatten().collect();
    (!filtered.is_empty()).then_some(filtered)
}

fn generate_header_processing_response<M: MessageKind>(status: ResponseStatus) -> Vec<MockProcessingResponse> {
    HEADER_MODIFICATIONS
        .iter()
        .flat_map(|&headers| {
            BODY_MODIFICATIONS.iter().flat_map(move |&body| {
                TRAILER_MODIFICATIONS.iter().map(move |&trailers| {
                    create_headers_response::<M>(
                        vec![headers],
                        body.map(Into::into),
                        // trailers modifications are NYI in body response in Envoy but Orion supports them
                        vec![trailers],
                        status as i32,
                        None,
                    )
                })
            })
        })
        .collect()
}

fn generate_body_processing_response<M: MessageKind>(status: ResponseStatus) -> Vec<MockProcessingResponse> {
    HEADER_MODIFICATIONS
        .iter()
        .flat_map(|&headers| {
            BODY_MODIFICATIONS.iter().flat_map(move |&body| {
                TRAILER_MODIFICATIONS.iter().map(move |&trailers| {
                    create_body_response::<M>(
                        vec![headers],
                        body.map(Into::into),
                        // trailers modifications are NYI in body response in Envoy but Orion supports them
                        vec![trailers],
                        status as i32,
                        None,
                    )
                })
            })
        })
        .collect()
}

fn generate_trailer_processing_response<M: MessageKind>() -> Vec<MockProcessingResponse> {
    TRAILER_MODIFICATIONS.iter().map(move |&trailers| create_trailers_response::<M>(vec![trailers])).collect()
}

fn generate_mock_external_processors_states<M: MessageKind + ModeSelector>(
    status: ResponseStatus,
    mock_msg: &MockMessage<M>,
    processing_mode: &ProcessingMode,
    observability: bool,
) -> Vec<MockExternalProcessorState> {
    let has_headers = transform(mock_msg.headers.clone()).is_some();
    let has_body = !mock_msg.body.is_empty();
    let has_trailers = transform(mock_msg.trailers.clone()).is_some();

    let should_send_headers =
        has_headers && !matches!(<M as ModeSelector>::header_mode(processing_mode), HeaderProcessingMode::Skip);
    let should_send_body =
        has_body && !matches!(<M as ModeSelector>::body_mode(processing_mode), BodyProcessingMode::None);
    let should_send_trailers =
        has_trailers && matches!(<M as ModeSelector>::trailer_mode(processing_mode), TrailerProcessingMode::Send);

    let mut mock_ext_proc_state = vec![];
    for header_response in generate_header_processing_response::<M>(status) {
        for body_response in generate_body_processing_response::<M>(status) {
            for trailer_response in generate_trailer_processing_response::<M>() {
                let mut state = MockExternalProcessorState::new();
                if observability {
                    state = state.with_observability(true);
                }
                if should_send_headers {
                    state = state.add_response(header_response.clone());
                }
                if should_send_body {
                    state = state.add_response(body_response.clone());
                }
                if should_send_trailers {
                    state = state.add_response(trailer_response);
                }
                if !mock_ext_proc_state.contains(&state) {
                    mock_ext_proc_state.push(state);
                }
            }
        }
    }
    mock_ext_proc_state
}

#[allow(clippy::too_many_arguments)]
fn assert_result<M>(
    test_case_num: i32,
    mock: &MockMessage<M>,
    headers: &http::HeaderMap,
    body: &[Bytes],
    trailers: &http::HeaderMap,
    mock_state: &MockExternalProcessorState,
    processing_mode: &ProcessingMode,
) where
    M: std::fmt::Debug + MessageKind + ModeSelector,
{
    // ProcessingMode predicates
    let has_headers = transform(mock.headers.clone()).is_some();
    let has_body = !mock.body.is_empty();
    let has_trailers = transform(mock.trailers.clone()).is_some();

    let should_send_headers =
        has_headers && !matches!(<M as ModeSelector>::header_mode(processing_mode), HeaderProcessingMode::Skip);
    let should_send_body =
        has_body && !matches!(<M as ModeSelector>::body_mode(processing_mode), BodyProcessingMode::None);
    let should_send_trailers =
        has_trailers && matches!(<M as ModeSelector>::trailer_mode(processing_mode), TrailerProcessingMode::Send);

    // Compute expected values to assert over modified request
    let mut expected_headers = mock.orig_headers_map();
    let mut expected_body: Vec<Bytes> = mock.body.clone().into_iter().map(Into::into).collect::<Vec<_>>();
    let mut expected_trailers = mock.orig_trailers_map();
    let mut idx = 0;
    debug!(target: "ext_proc", "original_headers: {expected_headers:?}");
    if !mock_state.observability_mode {
        // apply the expected mutations
        //
        for response_opt in &mock_state.responses {
            if let Some(response) = response_opt.deref().response.as_ref() {
                match response {
                    ProcessingResponseType::RequestHeaders(HeadersResponse { response: Some(common_response) })
                    | ProcessingResponseType::ResponseHeaders(HeadersResponse { response: Some(common_response) }) => {
                        let mutation =
                            should_send_headers.then_some(common_response.header_mutation.as_ref()).flatten();
                        expected_headers = mock.apply_header_mutation(mutation);
                    },
                    ProcessingResponseType::RequestBody(BodyResponse {
                        response: Some(CommonResponse { header_mutation, body_mutation, .. }),
                    })
                    | ProcessingResponseType::ResponseBody(BodyResponse {
                        response: Some(CommonResponse { header_mutation, body_mutation, .. }),
                    }) => {
                        if matches!(
                            <M as ModeSelector>::body_mode(processing_mode),
                            BodyProcessingMode::Buffered | BodyProcessingMode::BufferedPartial
                        ) {
                            // Orion can perform header mutations on body response only in BodyProcessingMode::Buffered
                            if let Some(header_mut) = header_mutation {
                                let header_mutation = should_send_body.then_some(header_mut);
                                MockMessage::<M>::apply_header_mutation_from_header_map(
                                    header_mutation,
                                    &mut expected_headers,
                                );
                            }
                        }
                        if let Some(body_mutation) = body_mutation {
                            let body_mutation = should_send_body.then_some(body_mutation.mutation.as_ref()).flatten();
                            if let Some(body) = expected_body.get_mut(idx) {
                                *body = mock.apply_body_mutation(body_mutation, idx);
                            }
                            idx += 1;
                        }
                    },
                    ProcessingResponseType::RequestTrailers(TrailersResponse { header_mutation })
                    | ProcessingResponseType::ResponseTrailers(TrailersResponse { header_mutation }) => {
                        let mutation = should_send_trailers.then_some(header_mutation.as_ref()).flatten();
                        expected_trailers = mock.apply_trailer_mutation(mutation);
                    },
                    _ => (),
                }
            }
        }
    }

    assert_eq!(
        *headers,
        expected_headers,
        "test_case #: {}, asserting headers: original {} {:#?}, ext_proc server conf: {:#?}, ext_proc_filter processing mode: {:#?}",
        test_case_num,
        <M as MessageKind>::NAME,
        mock,
        mock_state,
        processing_mode
    );
    assert_eq!(
        body,
        expected_body,
        "test_case #: {}, asserting body, original {} {:#?}, ext_proc server conf: {:#?}, ext_proc_filter processing mode: {:#?}",
        test_case_num,
        <M as MessageKind>::NAME,
        mock,
        mock_state,
        processing_mode
    );
    assert_eq!(
        *trailers,
        expected_trailers,
        "test_case #: {}, asserting trailers, original {} {:#?}, ext_proc server conf: {:#?}, ext_proc_filter processing mode: {:#?}",
        test_case_num,
        <M as MessageKind>::NAME,
        mock,
        mock_state,
        processing_mode
    );
}

#[tokio::test]
#[test_log::test]
async fn test_request_combinatorial_processing() {
    let status = ResponseStatus::Continue;
    let mut test_case_num = 0;
    let mut timed_out_tests = vec![];

    let processing_modes = generate_processing_mode_configurations::<RequestMsg>();
    let mock_requests = generate_mock_messages::<RequestMsg>();

    for processing_mode in &processing_modes {
        for mock_request in &mock_requests {
            let mock_states = generate_mock_external_processors_states(status, mock_request, processing_mode, false);
            for mock_state in &mock_states {
                debug!(target: "ext_proc_tests", "test_request_combinatorial_modes_continue: ################ test case ##############: {test_case_num}");
                debug!(target: "ext_proc_tests", "test_request_combinatorial_modes_continue: filter processing mode: {processing_mode:#?}");
                debug!(target: "ext_proc_tests", "test_request_combinatorial_modes_continue: mock request: {mock_request:#?}");
                debug!(target: "ext_proc_tests", "test_request_combinatorial_modes_continue: mock ext_proc state: {mock_state:#?}");
                let (server_addr, server_handle) = start_mock_server(mock_state.clone()).await;
                let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode.clone());
                config.observability_mode = false;
                config.failure_mode_allow = false;
                let mut ext_proc = ExternalProcessor::from(config);
                let mut request = build_request_from_mock(mock_request).await;
                let result =
                    fast_timeout(Duration::from_secs(2), ext_proc.apply_request(&mut request, &RequestCtx::default()))
                        .await;
                let Ok(result) = result else {
                    warn!(target: "ext_proc_tests", "test_request_combinatorial_modes_continue: ############ test {test_case_num} HANGS ############");
                    timed_out_tests.push(test_case_num);
                    continue;
                };
                server_handle.abort();
                assert_matches!(result, FilterDecision::Continue);

                let (parts, body) = request.into_parts();
                let request_headers = parts.headers;
                let collected = body.collect().await.unwrap();
                let request_trailers = if let Some(trailers) = collected.trailers() {
                    trailers.clone()
                } else {
                    http::HeaderMap::default()
                };

                let request_body = to_body_data_chunks(collected).await;

                assert_result(
                    test_case_num,
                    mock_request,
                    &request_headers,
                    &request_body,
                    &request_trailers,
                    mock_state,
                    processing_mode,
                );
                test_case_num += 1;
            }
        }
    }
    info!(target: "ext_proc_tests", "test_request_combinatorial_modes_continue: total test cases executed: {test_case_num}");
    assert!(
        timed_out_tests.is_empty(),
        "test_request_combinatorial_modes_continue: tests cases stuck: {timed_out_tests:?}"
    );
}

#[tokio::test]
#[test_log::test]
async fn test_response_combinatorial_processing() {
    let status = ResponseStatus::Continue;
    let mut test_case_num = 0;
    let mut timed_out_tests = vec![];

    let processing_modes = generate_processing_mode_configurations::<ResponseMsg>();
    let mock_responses = generate_mock_messages::<ResponseMsg>();

    for processing_mode in &processing_modes {
        for mock_response in &mock_responses {
            let mock_states = generate_mock_external_processors_states(status, mock_response, processing_mode, false);
            for mock_state in &mock_states {
                debug!(target: "ext_proc_tests", "test_response_combinatorial_modes_continue: test case #: {test_case_num}");
                debug!(target: "ext_proc_tests", "test_response_combinatorial_modes_continue: filter processing mode: {processing_mode:#?}");
                debug!(target: "ext_proc_tests", "test_response_combinatorial_modes_continue: mock request: {mock_response:#?}");
                debug!(target: "ext_proc_tests", "test_response_combinatorial_modes_continue: mock ext_proc state: {mock_state:#?}");
                let (server_addr, server_handle) = start_mock_server(mock_state.clone()).await;
                let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode.clone());
                config.observability_mode = false;
                config.failure_mode_allow = false;
                let mut ext_proc = ExternalProcessor::from(config);
                let mut response = build_response_from_mock(mock_response).await;
                let result = fast_timeout(
                    Duration::from_secs(2),
                    ext_proc.apply_response(&mut response, &RequestCtx::default()),
                )
                .await;
                let Ok(result) = result else {
                    warn!(target: "ext_proc_tests", "test_response_combinatorial_modes_continue: ############ test {test_case_num} HANGS ############");
                    timed_out_tests.push(test_case_num);
                    continue;
                };
                server_handle.abort();
                assert_matches!(result, FilterDecision::Continue);

                let (parts, body) = response.into_parts();
                let response_headers = parts.headers;
                let collected = body.collect().await.unwrap();
                let response_trailers = if let Some(trailers) = collected.trailers() {
                    trailers.clone()
                } else {
                    http::HeaderMap::default()
                };

                let response_body = to_body_data_chunks(collected).await;

                assert_result(
                    test_case_num,
                    mock_response,
                    &response_headers,
                    &response_body,
                    &response_trailers,
                    mock_state,
                    processing_mode,
                );
                test_case_num += 1;
            }
        }
    }
    info!(target: "ext_proc_tests", "test_response_combinatorial_modes_continue: total test cases executed: {test_case_num}");
    assert!(
        timed_out_tests.is_empty(),
        "test_response_combinatorial_modes_continue: tests cases stuck: {timed_out_tests:?}"
    );
}

#[tokio::test]
#[test_log::test]
async fn test_request_combinatorial_observability() {
    let status = ResponseStatus::Continue;
    let mut test_case_num = 0;
    let mut timed_out_tests = vec![];

    let processing_modes = generate_processing_mode_configurations::<RequestMsg>();
    let mock_requests = generate_mock_messages::<RequestMsg>();

    for processing_mode in &processing_modes {
        for mock_request in &mock_requests {
            let mock_states = generate_mock_external_processors_states(status, mock_request, processing_mode, true);
            for mock_state in &mock_states {
                debug!(target: "ext_proc_tests", "test_request_combinatorial_modes_observability: ################ test case ##############: {test_case_num}");
                debug!(target: "ext_proc_tests", "test_request_combinatorial_modes_observability: filter processing mode: {processing_mode:#?}");
                debug!(target: "ext_proc_tests", "test_request_combinatorial_modes_observability: mock request: {mock_request:#?}");
                debug!(target: "ext_proc_tests", "test_request_combinatorial_modes_observability: mock ext_proc state: {mock_state:#?}");
                let (server_addr, server_handle) = start_mock_server(mock_state.clone()).await;
                let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode.clone());
                config.observability_mode = true;
                config.failure_mode_allow = false;
                let mut ext_proc = ExternalProcessor::from(config);
                let mut request = build_request_from_mock(mock_request).await;
                let result =
                    fast_timeout(Duration::from_secs(2), ext_proc.apply_request(&mut request, &RequestCtx::default()))
                        .await;
                let Ok(result) = result else {
                    warn!(target: "ext_proc_tests", "test_request_combinatorial_modes_observability: ############ test {test_case_num} HANGS ############");
                    timed_out_tests.push(test_case_num);
                    continue;
                };
                server_handle.abort();
                assert_matches!(result, FilterDecision::Continue);

                let (parts, body) = request.into_parts();
                let request_headers = parts.headers;
                let collected = body.collect().await.unwrap();
                let request_trailers = if let Some(trailers) = collected.trailers() {
                    trailers.clone()
                } else {
                    http::HeaderMap::default()
                };

                let request_body = to_body_data_chunks(collected).await;

                assert_result(
                    test_case_num,
                    mock_request,
                    &request_headers,
                    &request_body,
                    &request_trailers,
                    mock_state,
                    processing_mode,
                );
                test_case_num += 1;
            }
        }
    }
    info!(target: "ext_proc_tests", "test_request_combinatorial_modes_observability: total test cases executed: {test_case_num}");
    assert!(
        timed_out_tests.is_empty(),
        "test_request_combinatorial_modes_observability: tests cases stuck: {timed_out_tests:?}"
    );
}

#[tokio::test]
#[test_log::test]
async fn test_response_combinatorial_observability() {
    let status = ResponseStatus::Continue;
    let mut test_case_num = 0;
    let mut timed_out_tests = vec![];

    let processing_modes = generate_processing_mode_configurations::<ResponseMsg>();
    let mock_responses = generate_mock_messages::<ResponseMsg>();

    for processing_mode in &processing_modes {
        for mock_response in &mock_responses {
            let mock_states = generate_mock_external_processors_states(status, mock_response, processing_mode, true);
            for mock_state in &mock_states {
                debug!(target: "ext_proc_tests", "test_response_combinatorial_observability: test case #: {test_case_num}");
                debug!(target: "ext_proc_tests", "test_response_combinatorial_observability: filter processing mode: {processing_mode:#?}");
                debug!(target: "ext_proc_tests", "test_response_combinatorial_observability: mock request: {mock_response:#?}");
                debug!(target: "ext_proc_tests", "test_response_combinatorial_observability: mock ext_proc state: {mock_state:#?}");
                let (server_addr, server_handle) = start_mock_server(mock_state.clone()).await;
                let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode.clone());
                config.observability_mode = true;
                config.failure_mode_allow = false;
                let mut ext_proc = ExternalProcessor::from(config);
                let mut response = build_response_from_mock(mock_response).await;
                let result = fast_timeout(
                    Duration::from_secs(2),
                    ext_proc.apply_response(&mut response, &RequestCtx::default()),
                )
                .await;
                let Ok(result) = result else {
                    warn!(target: "ext_proc_tests", "test_response_combinatorial_observability: ############ test {test_case_num} HANGS ############");
                    timed_out_tests.push(test_case_num);
                    continue;
                };
                server_handle.abort();
                assert_matches!(result, FilterDecision::Continue);

                let (parts, body) = response.into_parts();
                let response_headers = parts.headers;
                let collected = body.collect().await.unwrap();
                let response_trailers = if let Some(trailers) = collected.trailers() {
                    trailers.clone()
                } else {
                    http::HeaderMap::default()
                };

                let response_body = to_body_data_chunks(collected).await;

                assert_result(
                    test_case_num,
                    mock_response,
                    &response_headers,
                    &response_body,
                    &response_trailers,
                    mock_state,
                    processing_mode,
                );
                test_case_num += 1;
            }
        }
    }
    info!(target: "ext_proc_tests", "test_response_combinatorial_observability: total test cases executed: {test_case_num}");
    assert!(
        timed_out_tests.is_empty(),
        "test_response_combinatorial_observability: tests cases stuck: {timed_out_tests:?}"
    );
}

#[tokio::test]
#[test_log::test]
async fn test_request_header_skip_body_buffered_empty() {
    let mock_state = MockExternalProcessorState::new();
    let (server_addr, _) = start_mock_server(mock_state).await;
    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Skip,
        request_body_mode: BodyProcessingMode::Buffered,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Skip,
        response_body_mode: BodyProcessingMode::None,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.observability_mode = false;
    config.failure_mode_allow = false;
    let mut ext_proc = ExternalProcessor::from(config);

    let mut request = build_request_from_mock(&MockMessage::<RequestMsg> {
        headers: vec![Some(("content-type", "application/json"))],
        body: vec![],
        trailers: vec![],
        _marker: std::marker::PhantomData,
    })
    .await;

    let result = ext_proc.apply_request(&mut request, &RequestCtx::default()).await;
    assert_matches!(result, FilterDecision::Continue);
}

#[tokio::test]
#[test_log::test]
async fn test_request_header_mutation_in_buffered_body_response_with_trailers() {
    let mock_state = MockExternalProcessorState::new()
        .add_response(create_headers_response::<RequestMsg>(
            vec![],
            None,
            vec![],
            ResponseStatus::Continue as i32,
            None,
        ))
        .add_response(create_body_response::<RequestMsg>(
            vec![Some(("x-test-header", "ext-proc-header-value"))],
            None,
            vec![],
            ResponseStatus::Continue as i32,
            None,
        ))
        .add_response(create_trailers_response::<RequestMsg>(vec![]));
    let (server_addr, _) = start_mock_server(mock_state).await;
    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Default,
        request_body_mode: BodyProcessingMode::Buffered,
        request_trailer_mode: TrailerProcessingMode::Send,
        response_header_mode: HeaderProcessingMode::Skip,
        response_body_mode: BodyProcessingMode::None,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.observability_mode = false;
    config.failure_mode_allow = false;
    let mut ext_proc = ExternalProcessor::from(config);

    let mut request = build_request_from_mock(&MockMessage::<RequestMsg> {
        headers: vec![Some(("x-test-header", "original-header-value"))],
        body: vec!["original body"],
        trailers: vec![Some(("x-test-trailer", "original-trailer-value"))],
        _marker: std::marker::PhantomData,
    })
    .await;

    let result = ext_proc.apply_request(&mut request, &RequestCtx::default()).await;

    let body = std::mem::take(&mut request.body_mut().inner.inner).collect().await.unwrap();
    let trailers = body.trailers().cloned();
    assert!(trailers.is_some());
    let trailers = trailers.unwrap();

    assert_matches!(result, FilterDecision::Continue);
    assert_eq!(request.headers().get("x-test-header").unwrap(), "ext-proc-header-value");
    assert_eq!(body.to_bytes(), "original body");
    assert_eq!(trailers.get("x-test-trailer").unwrap(), "original-trailer-value");
}

#[tokio::test]
#[test_log::test]
async fn test_request_header_mutation() {
    let mock_state = MockExternalProcessorState::new().add_response(create_headers_response::<RequestMsg>(
        vec![Some(("x-processed", "true")), Some(("x-custom-header", "custom-value"))],
        None,
        vec![],
        ResponseStatus::Continue as i32,
        None,
    ));
    let (server_addr, _) = start_mock_server(mock_state).await;
    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Send,
        request_body_mode: BodyProcessingMode::None,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Skip,
        response_body_mode: BodyProcessingMode::None,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.observability_mode = false;
    config.failure_mode_allow = false;
    let mut ext_proc = ExternalProcessor::from(config);

    let mut request = build_request_from_mock(&MockMessage::<RequestMsg> {
        headers: vec![Some(("content-type", "application/json"))],
        body: vec![],
        trailers: vec![],
        _marker: std::marker::PhantomData,
    })
    .await;

    let result = ext_proc.apply_request(&mut request, &RequestCtx::default()).await;

    assert_matches!(result, FilterDecision::Continue);
    assert_eq!(request.headers().get("x-processed").unwrap(), "true");
    assert_eq!(request.headers().get("x-custom-header").unwrap(), "custom-value");
    assert_eq!(request.headers().get("content-type").unwrap(), "application/json");
}

#[tokio::test]
#[test_log::test]
async fn test_request_header_mutation_pseudo_headers() {
    let mock_state = MockExternalProcessorState::new().add_response(create_headers_response::<RequestMsg>(
        vec![
            Some((":method", "POST")),
            Some((":path", "/ext-proc-html-path")),
            Some((":scheme", "https")),
            Some((":authority", "ext-proc.com")),
        ],
        None,
        vec![],
        ResponseStatus::Continue as i32,
        None,
    ));
    let (server_addr, _) = start_mock_server(mock_state).await;
    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Send,
        request_body_mode: BodyProcessingMode::None,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Skip,
        response_body_mode: BodyProcessingMode::None,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode.clone());
    config.observability_mode = false;
    config.failure_mode_allow = false;
    let mut ext_proc = ExternalProcessor::from(config);

    let mut request = build_request_from_mock(&MockMessage::<RequestMsg> {
        headers: vec![Some(("content-type", "application/json"))],
        body: vec![],
        trailers: vec![],
        _marker: std::marker::PhantomData,
    })
    .await;

    let result = ext_proc.apply_request(&mut request, &RequestCtx::default()).await;
    let (parts, _) = request.into_parts();

    assert_matches!(result, FilterDecision::Continue);
    assert_eq!(parts.method.as_str(), "POST");
    assert_eq!(parts.uri.authority().unwrap().as_str(), "ext-proc.com");
    assert_eq!(parts.uri.scheme().unwrap().as_str(), "https");
    assert_eq!(parts.uri.path_and_query().unwrap().as_str(), "/ext-proc-html-path");
}

#[tokio::test]
#[test_log::test]
async fn test_request_trailer_mutation() {
    let mock_state = MockExternalProcessorState::new()
        .add_response(create_headers_response::<RequestMsg>(
            vec![],
            None,
            vec![],
            ResponseStatus::Continue as i32,
            None,
        ))
        .add_response(create_body_response::<RequestMsg>(vec![], None, vec![], ResponseStatus::Continue as i32, None))
        .add_response(create_trailers_response::<RequestMsg>(vec![
            Some(("x-processed", "true")),
            Some(("x-custom-trailer", "modified-value")),
        ]));
    let (server_addr, _) = start_mock_server(mock_state).await;
    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Send,
        request_body_mode: BodyProcessingMode::Buffered,
        request_trailer_mode: TrailerProcessingMode::Send,
        response_header_mode: HeaderProcessingMode::Skip,
        response_body_mode: BodyProcessingMode::None,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.observability_mode = false;
    config.failure_mode_allow = false;
    let mut ext_proc = ExternalProcessor::from(config);

    let mut request = build_request_from_mock(&MockMessage::<RequestMsg> {
        headers: vec![Some(("content-type", "application/json"))],
        body: vec!["body"],
        trailers: vec![Some(("x-custom-trailer", "original-value"))],
        _marker: std::marker::PhantomData,
    })
    .await;

    let result = ext_proc.apply_request(&mut request, &RequestCtx::default()).await;
    let body = std::mem::take(&mut request.body_mut().inner.inner).collect().await.unwrap();
    let trailers = body.trailers().cloned();

    assert!(trailers.is_some());
    let trailers = trailers.unwrap();
    assert_matches!(result, FilterDecision::Continue);
    assert_eq!(request.headers().get("content-type").unwrap(), "application/json");
    assert_eq!(body.to_bytes(), "body");
    assert_eq!(trailers.get("x-processed").unwrap(), "true");
    assert_eq!(trailers.get("x-custom-trailer").unwrap(), "modified-value");
}

#[tokio::test]
#[test_log::test]
async fn test_request_body_buffered_continue_and_replace_on_headers_response() {
    let new_body = "modified body content";
    let mock_state = MockExternalProcessorState::new().add_response(create_headers_response::<RequestMsg>(
        vec![Some(("y-custom-header", "true"))],
        Some(new_body.as_bytes().into()),
        vec![],
        ResponseStatus::ContinueAndReplace as i32,
        None,
    ));
    let (server_addr, _) = start_mock_server(mock_state).await;
    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Send,
        request_body_mode: BodyProcessingMode::Buffered,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Skip,
        response_body_mode: BodyProcessingMode::None,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.observability_mode = false;
    config.failure_mode_allow = false;
    let mut ext_proc = ExternalProcessor::from(config);

    let mut request = build_request_from_mock(&MockMessage::<RequestMsg> {
        headers: vec![],
        body: vec!["original body"],
        trailers: vec![],
        _marker: std::marker::PhantomData,
    })
    .await;

    let result = ext_proc.apply_request(&mut request, &RequestCtx::default()).await;

    assert_matches!(result, FilterDecision::Continue);
    assert_eq!(request.method(), Method::GET);
    assert_eq!(request.headers().get("y-custom-header").unwrap(), "true");
    let body_bytes = std::mem::take(&mut request.body_mut().inner.inner).collect().await.unwrap().to_bytes();
    assert_eq!(body_bytes, new_body.as_bytes());
}

#[tokio::test]
#[test_log::test]
async fn test_request_body_buffered_continue_and_replace_on_body_response() {
    let new_body = "modified body content";
    let mock_state = MockExternalProcessorState::new()
        .add_response(create_headers_response::<RequestMsg>(
            vec![],
            None,
            vec![],
            ResponseStatus::Continue as i32,
            None,
        ))
        .add_response(create_body_response::<RequestMsg>(
            // TODO support headers modifications on body responses
            //vec![("y-custom-header", "true")],
            vec![],
            Some(new_body.as_bytes().into()),
            vec![],
            ResponseStatus::ContinueAndReplace as i32,
            None,
        ));
    let (server_addr, _) = start_mock_server(mock_state).await;
    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Send,
        request_body_mode: BodyProcessingMode::Buffered,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Skip,
        response_body_mode: BodyProcessingMode::None,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.observability_mode = false;
    config.failure_mode_allow = false;
    let mut ext_proc = ExternalProcessor::from(config);

    let mut request = build_request_from_mock(&MockMessage::<RequestMsg> {
        headers: vec![],
        body: vec!["original body"],
        trailers: vec![],
        _marker: std::marker::PhantomData,
    })
    .await;

    let result = ext_proc.apply_request(&mut request, &RequestCtx::default()).await;

    assert_matches!(result, FilterDecision::Continue);
    assert_eq!(request.method(), Method::GET);
    let body_bytes = std::mem::take(&mut request.body_mut().inner.inner).collect().await.unwrap().to_bytes();
    assert_eq!(body_bytes, new_body.as_bytes());
}

#[tokio::test]
#[test_log::test]
async fn test_request_body_buffered_mode() {
    let mock_state = MockExternalProcessorState::new()
        .add_response(create_headers_response::<RequestMsg>(
            vec![Some(("x-stream-processed", "true")), Some(("y-custom-header", "true"))],
            None,
            vec![],
            ResponseStatus::Continue as i32,
            None,
        ))
        .add_response(create_body_response::<RequestMsg>(
            vec![],
            Some("body data from external processor".as_bytes().into()),
            vec![],
            ResponseStatus::Continue as i32,
            None,
        ));
    let (server_addr, _) = start_mock_server(mock_state).await;
    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Send,
        request_body_mode: BodyProcessingMode::Buffered,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Skip,
        response_body_mode: BodyProcessingMode::None,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.observability_mode = false;
    config.failure_mode_allow = false;
    let mut ext_proc = ExternalProcessor::from(config);

    let mut request = build_request_from_mock(&MockMessage::<RequestMsg> {
        headers: vec![],
        body: vec!["buffered body data"],
        trailers: vec![],
        _marker: std::marker::PhantomData,
    })
    .await;

    let result = ext_proc.apply_request(&mut request, &RequestCtx::default()).await;

    assert_matches!(result, FilterDecision::Continue);
    assert_eq!(request.headers().get("x-stream-processed").unwrap(), "true");
    let body_bytes = std::mem::take(&mut request.body_mut().inner.inner).collect().await.unwrap().to_bytes();
    assert_eq!(body_bytes, "body data from external processor".as_bytes());
}

#[tokio::test]
#[test_log::test]
async fn test_request_body_buffered_mode_header_mutations_on_body() {
    let mock_state = MockExternalProcessorState::new()
        .add_response(create_headers_response::<RequestMsg>(
            vec![Some(("mutation-in-header-response", "true"))],
            None,
            vec![],
            ResponseStatus::Continue as i32,
            None,
        ))
        .add_response(create_body_response::<RequestMsg>(
            vec![Some(("mutation-in-body-response", "true"))],
            Some("body data from external processor".as_bytes().into()),
            vec![],
            ResponseStatus::Continue as i32,
            None,
        ));
    let (server_addr, _) = start_mock_server(mock_state).await;
    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Send,
        request_body_mode: BodyProcessingMode::Buffered,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Skip,
        response_body_mode: BodyProcessingMode::None,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.observability_mode = false;
    config.failure_mode_allow = false;
    let mut ext_proc = ExternalProcessor::from(config);

    let mut request = build_request_from_mock(&MockMessage::<RequestMsg> {
        headers: vec![],
        body: vec!["buffered body data"],
        trailers: vec![],
        _marker: std::marker::PhantomData,
    })
    .await;

    let result = ext_proc.apply_request(&mut request, &RequestCtx::default()).await;

    assert_matches!(result, FilterDecision::Continue);
    assert_eq!(request.headers().get("mutation-in-header-response").unwrap(), "true");
    assert_eq!(request.headers().get("mutation-in-body-response").unwrap(), "true");
    let body_bytes = std::mem::take(&mut request.body_mut().inner.inner).collect().await.unwrap().to_bytes();
    assert_eq!(body_bytes, "body data from external processor".as_bytes());
}

#[tokio::test]
#[test_log::test]
async fn test_request_body_buffered_mode_header_mutations_on_body_no_header_response() {
    let mock_state = MockExternalProcessorState::new().add_response(create_body_response::<RequestMsg>(
        vec![Some(("mutation-in-body-response", "true"))],
        Some("body data from external processor".as_bytes().into()),
        vec![],
        ResponseStatus::Continue as i32,
        None,
    ));
    let (server_addr, _) = start_mock_server(mock_state).await;
    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Skip,
        request_body_mode: BodyProcessingMode::Buffered,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Skip,
        response_body_mode: BodyProcessingMode::None,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.observability_mode = false;
    config.failure_mode_allow = false;
    let mut ext_proc = ExternalProcessor::from(config);

    let mut request = build_request_from_mock(&MockMessage::<RequestMsg> {
        headers: vec![],
        body: vec!["buffered body data"],
        trailers: vec![],
        _marker: std::marker::PhantomData,
    })
    .await;

    let result = ext_proc.apply_request(&mut request, &RequestCtx::default()).await;

    assert_matches!(result, FilterDecision::Continue);
    assert_eq!(request.headers().get("mutation-in-body-response").unwrap(), "true");
    let body_bytes = std::mem::take(&mut request.body_mut().inner.inner).collect().await.unwrap().to_bytes();
    assert_eq!(body_bytes, "body data from external processor".as_bytes());
}

#[tokio::test]
#[test_log::test]
async fn test_request_body_buffered_mode_send_body_without_waiting_for_header_response() {
    let mock_state = MockExternalProcessorState::new()
        .add_response(create_headers_response::<RequestMsg>(
            vec![Some(("x-stream-processed", "true")), Some(("y-custom-header", "true"))],
            None,
            vec![],
            ResponseStatus::Continue as i32,
            None,
        ))
        .add_response(create_body_response::<RequestMsg>(
            vec![],
            Some("body data from external processor".as_bytes().into()),
            vec![],
            ResponseStatus::Continue as i32,
            None,
        ));
    let (server_addr, _) = start_mock_server(mock_state).await;
    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Send,
        request_body_mode: BodyProcessingMode::Buffered,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Skip,
        response_body_mode: BodyProcessingMode::None,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.observability_mode = false;
    config.failure_mode_allow = false;
    config.send_body_without_waiting_for_header_response = true;
    let mut ext_proc = ExternalProcessor::from(config);

    let mut request = build_request_from_mock(&MockMessage::<RequestMsg> {
        headers: vec![],
        body: vec!["buffered body data"],
        trailers: vec![],
        _marker: std::marker::PhantomData,
    })
    .await;

    let result = ext_proc.apply_request(&mut request, &RequestCtx::default()).await;

    assert_matches!(result, FilterDecision::Continue);
    assert_eq!(request.headers().get("x-stream-processed").unwrap(), "true");
    let body_bytes = std::mem::take(&mut request.body_mut().inner.inner).collect().await.unwrap().to_bytes();
    assert_eq!(body_bytes, "body data from external processor".as_bytes());
}

#[tokio::test]
#[test_log::test]
async fn test_request_body_streaming_mode() {
    let mock_state = MockExternalProcessorState::new()
        .add_response(create_headers_response::<RequestMsg>(
            vec![Some(("x-stream-processed", "true")), Some(("y-custom-header", "true"))],
            None,
            vec![],
            ResponseStatus::Continue as i32,
            // even though we will stream the body, no body modification is
            // performed in the headers response so no need to set the flag
            None,
        ))
        .add_response(create_body_response::<RequestMsg>(
            vec![],
            Some("body data from external processor".as_bytes().into()),
            vec![],
            ResponseStatus::Continue as i32,
            // the response for the streaming body is still just a normal Body
            // no need to set end_of_stream, which is only for FULL_DUPLEX_STREAMED
            None,
        ));
    let (server_addr, _) = start_mock_server(mock_state).await;
    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Send,
        request_body_mode: BodyProcessingMode::Streamed,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Skip,
        response_body_mode: BodyProcessingMode::None,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.observability_mode = false;
    config.failure_mode_allow = false;
    let mut ext_proc = ExternalProcessor::from(config);

    let mut request = build_request_from_mock(&MockMessage::<RequestMsg> {
        headers: vec![],
        body: vec!["streaming body data"],
        trailers: vec![],
        _marker: std::marker::PhantomData,
    })
    .await;
    let result = ext_proc.apply_request(&mut request, &RequestCtx::default()).await;

    assert_matches!(result, FilterDecision::Continue);
    assert_eq!(request.headers().get("x-stream-processed").unwrap(), "true");
    let body_bytes = std::mem::take(&mut request.body_mut().inner.inner).collect().await.unwrap().to_bytes();
    assert_eq!(body_bytes, "body data from external processor".as_bytes());
}

#[tokio::test]
#[test_log::test]
async fn test_request_full_duplex_streaming_mode_header_only() {
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    let mock_state = MockExternalProcessorState::new()
        .add_response(create_headers_response::<RequestMsg>(
            vec![Some(("x-stream-processed", "true")), Some(("y-custom-header", "true"))],
            None,
            vec![],
            ResponseStatus::Continue as i32,
            // even though we will stream the body, no body modification is
            // performed in the headers response so no need to set the flag
            None,
        ))
        .with_sender(tx);

    let (server_addr, _) = start_mock_server(mock_state.clone()).await;

    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Send,
        request_body_mode: BodyProcessingMode::FullDuplexStreamed,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Skip,
        response_body_mode: BodyProcessingMode::None,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.observability_mode = false;
    config.failure_mode_allow = false;
    let mut ext_proc = ExternalProcessor::from(config);

    let mut request = build_request_from_mock(&MockMessage::<RequestMsg> {
        headers: vec![],
        body: vec![],
        trailers: vec![],
        _marker: std::marker::PhantomData,
    })
    .await;

    let result = ext_proc.apply_request(&mut request, &RequestCtx::default()).await;

    assert_matches!(result, FilterDecision::Continue);
    assert_eq!(request.headers().get("x-stream-processed").unwrap(), "true");

    mock_state.token.cancel();

    if let Some(state) = rx.recv().await {
        assert_matches!(state.last_end_of_stream, Some(true), "Expected end of stream true");
    }
}

#[tokio::test]
#[test_log::test]
async fn test_request_full_duplex_streaming_mode_with_body() {
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    let mock_state = MockExternalProcessorState::new()
        .add_response(create_headers_response::<RequestMsg>(
            vec![Some(("x-stream-processed", "true")), Some(("y-custom-header", "true"))],
            None,
            vec![],
            ResponseStatus::Continue as i32,
            // even though we will stream the body, no body modification is
            // performed in the headers response so no need to set the flag
            None,
        ))
        .add_response(create_body_response::<RequestMsg>(
            vec![],
            Some("body data from external processor".as_bytes().into()),
            vec![],
            ResponseStatus::Continue as i32,
            // the response for the streaming body is still just a normal Body
            // no need to set end_of_stream, which is only for FULL_DUPLEX_STREAMED
            None,
        ))
        .with_sender(tx);

    let (server_addr, _) = start_mock_server(mock_state.clone()).await;

    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Send,
        request_body_mode: BodyProcessingMode::FullDuplexStreamed,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Skip,
        response_body_mode: BodyProcessingMode::None,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.observability_mode = false;
    config.failure_mode_allow = false;
    let mut ext_proc = ExternalProcessor::from(config);

    let mut request = build_request_from_mock(&MockMessage::<RequestMsg> {
        headers: vec![],
        body: vec!["this is the body"],
        trailers: vec![],
        _marker: std::marker::PhantomData,
    })
    .await;

    let result = ext_proc.apply_request(&mut request, &RequestCtx::default()).await;

    assert_matches!(result, FilterDecision::Continue);
    assert_eq!(request.headers().get("x-stream-processed").unwrap(), "true");

    let body_bytes = std::mem::take(&mut request.body_mut().inner.inner).collect().await.unwrap().to_bytes();
    assert_eq!(body_bytes, "body data from external processor".as_bytes());

    mock_state.token.cancel();

    if let Some(state) = rx.recv().await {
        assert_matches!(state.last_end_of_stream, Some(true), "Expected end of stream true");
    }
}

#[tokio::test]
#[test_log::test]
async fn test_request_full_duplex_streaming_mode_with_body_and_trailers() {
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    let mock_state = MockExternalProcessorState::new()
        .add_response(create_headers_response::<RequestMsg>(
            vec![Some(("x-stream-processed", "true")), Some(("y-custom-header", "true"))],
            None,
            vec![],
            ResponseStatus::Continue as i32,
            // even though we will stream the body, no body modification is
            // performed in the headers response so no need to set the flag
            None,
        ))
        .add_response(create_body_response::<RequestMsg>(
            vec![],
            Some("body data from external processor".as_bytes().into()),
            vec![],
            ResponseStatus::Continue as i32,
            // the response for the streaming body is still just a normal Body
            // no need to set end_of_stream, which is only for FULL_DUPLEX_STREAMED
            None,
        ))
        .add_response(create_trailers_response::<RequestMsg>(vec![]))
        .with_sender(tx);

    let (server_addr, _) = start_mock_server(mock_state.clone()).await;

    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Send,
        request_body_mode: BodyProcessingMode::FullDuplexStreamed,
        request_trailer_mode: TrailerProcessingMode::Send,
        response_header_mode: HeaderProcessingMode::Skip,
        response_body_mode: BodyProcessingMode::None,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.observability_mode = false;
    config.failure_mode_allow = false;
    let mut ext_proc = ExternalProcessor::from(config);

    let mut request = build_request_from_mock(&MockMessage::<RequestMsg> {
        headers: vec![],
        body: vec!["this is the body"],
        trailers: vec![Some(("x-custom-trailer", "original-value"))],
        _marker: std::marker::PhantomData,
    })
    .await;

    let result = ext_proc.apply_request(&mut request, &RequestCtx::default()).await;

    assert_matches!(result, FilterDecision::Continue);
    assert_eq!(request.headers().get("x-stream-processed").unwrap(), "true");

    let body_bytes = std::mem::take(&mut request.body_mut().inner.inner).collect().await.unwrap().to_bytes();
    assert_eq!(body_bytes, "body data from external processor".as_bytes());

    mock_state.token.cancel();

    if let Some(state) = rx.recv().await {
        assert_matches!(state.last_end_of_stream, Some(false), "Expected end of stream false");
    }
}

#[tokio::test]
#[test_log::test]
async fn test_request_buffered_mode_header_only() {
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    let mock_state = MockExternalProcessorState::new()
        .add_response(create_headers_response::<RequestMsg>(
            vec![Some(("x-stream-processed", "true")), Some(("y-custom-header", "true"))],
            None,
            vec![],
            ResponseStatus::Continue as i32,
            // even though we will stream the body, no body modification is
            // performed in the headers response so no need to set the flag
            None,
        ))
        .with_sender(tx);

    let (server_addr, _) = start_mock_server(mock_state.clone()).await;

    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Send,
        request_body_mode: BodyProcessingMode::Buffered,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Skip,
        response_body_mode: BodyProcessingMode::None,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.observability_mode = false;
    config.failure_mode_allow = false;
    let mut ext_proc = ExternalProcessor::from(config);

    let mut request = build_request_from_mock(&MockMessage::<RequestMsg> {
        headers: vec![],
        body: vec![],
        trailers: vec![],
        _marker: std::marker::PhantomData,
    })
    .await;

    let result = ext_proc.apply_request(&mut request, &RequestCtx::default()).await;

    assert_matches!(result, FilterDecision::Continue);
    assert_eq!(request.headers().get("x-stream-processed").unwrap(), "true");

    mock_state.token.cancel();

    if let Some(state) = rx.recv().await {
        assert_matches!(state.last_end_of_stream, Some(true), "Expected end of stream true");
    }
}

#[tokio::test]
#[test_log::test]
async fn test_request_buffered_mode_with_body() {
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    let mock_state = MockExternalProcessorState::new()
        .add_response(create_headers_response::<RequestMsg>(
            vec![Some(("x-stream-processed", "true")), Some(("y-custom-header", "true"))],
            None,
            vec![],
            ResponseStatus::Continue as i32,
            // even though we will stream the body, no body modification is
            // performed in the headers response so no need to set the flag
            None,
        ))
        .add_response(create_body_response::<RequestMsg>(
            vec![],
            Some("body data from external processor".as_bytes().into()),
            vec![],
            ResponseStatus::Continue as i32,
            // the response for the streaming body is still just a normal Body
            // no need to set end_of_stream, which is only for FULL_DUPLEX_STREAMED
            None,
        ))
        .with_sender(tx);

    let (server_addr, _) = start_mock_server(mock_state.clone()).await;

    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Send,
        request_body_mode: BodyProcessingMode::Buffered,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Skip,
        response_body_mode: BodyProcessingMode::None,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.observability_mode = false;
    config.failure_mode_allow = false;
    let mut ext_proc = ExternalProcessor::from(config);

    let mut request = build_request_from_mock(&MockMessage::<RequestMsg> {
        headers: vec![],
        body: vec!["this is the body"],
        trailers: vec![],
        _marker: std::marker::PhantomData,
    })
    .await;

    let result = ext_proc.apply_request(&mut request, &RequestCtx::default()).await;

    assert_matches!(result, FilterDecision::Continue);
    assert_eq!(request.headers().get("x-stream-processed").unwrap(), "true");

    let body_bytes = std::mem::take(&mut request.body_mut().inner.inner).collect().await.unwrap().to_bytes();
    assert_eq!(body_bytes, "body data from external processor".as_bytes());

    mock_state.token.cancel();

    if let Some(state) = rx.recv().await {
        assert_matches!(state.last_end_of_stream, Some(true), "Expected end of stream true");
    }
}

#[tokio::test]
#[test_log::test]
async fn test_request_buffered_mode_with_body_and_trailers() {
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    let mock_state = MockExternalProcessorState::new()
        .add_response(create_headers_response::<RequestMsg>(
            vec![Some(("x-stream-processed", "true")), Some(("y-custom-header", "true"))],
            None,
            vec![],
            ResponseStatus::Continue as i32,
            // even though we will stream the body, no body modification is
            // performed in the headers response so no need to set the flag
            None,
        ))
        .add_response(create_body_response::<RequestMsg>(
            vec![],
            Some("body data from external processor".as_bytes().into()),
            vec![],
            ResponseStatus::Continue as i32,
            // the response for the streaming body is still just a normal Body
            // no need to set end_of_stream, which is only for FULL_DUPLEX_STREAMED
            None,
        ))
        .add_response(create_trailers_response::<RequestMsg>(vec![]))
        .with_sender(tx);

    let (server_addr, _) = start_mock_server(mock_state.clone()).await;

    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Send,
        request_body_mode: BodyProcessingMode::Buffered,
        request_trailer_mode: TrailerProcessingMode::Send,
        response_header_mode: HeaderProcessingMode::Skip,
        response_body_mode: BodyProcessingMode::None,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.observability_mode = false;
    config.failure_mode_allow = false;
    let mut ext_proc = ExternalProcessor::from(config);

    let mut request = build_request_from_mock(&MockMessage::<RequestMsg> {
        headers: vec![],
        body: vec!["this is the body"],
        trailers: vec![Some(("x-custom-trailer", "original-value"))],
        _marker: std::marker::PhantomData,
    })
    .await;

    let result = ext_proc.apply_request(&mut request, &RequestCtx::default()).await;

    assert_matches!(result, FilterDecision::Continue);
    assert_eq!(request.headers().get("x-stream-processed").unwrap(), "true");

    let body_bytes = std::mem::take(&mut request.body_mut().inner.inner).collect().await.unwrap().to_bytes();
    assert_eq!(body_bytes, "body data from external processor".as_bytes());

    mock_state.token.cancel();

    if let Some(state) = rx.recv().await {
        assert_matches!(state.last_end_of_stream, Some(false), "Expected end of stream false");
    }
}

#[tokio::test]
#[test_log::test]
async fn test_response_header_mutation_pseudo_headers() {
    let mock_state = MockExternalProcessorState::new().add_response(create_headers_response::<ResponseMsg>(
        vec![Some((":status", "404"))],
        None,
        vec![],
        ResponseStatus::Continue as i32,
        None,
    ));
    let (server_addr, _) = start_mock_server(mock_state).await;
    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Skip,
        request_body_mode: BodyProcessingMode::None,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Send,
        response_body_mode: BodyProcessingMode::None,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.observability_mode = false;
    config.failure_mode_allow = false;
    let mut ext_proc = ExternalProcessor::from(config);

    let mut response = build_response_from_mock(&MockMessage::<ResponseMsg> {
        headers: vec![Some(("content-type", "application/json"))],
        body: vec![],
        trailers: vec![],
        _marker: std::marker::PhantomData,
    })
    .await;
    let result = ext_proc.apply_response(&mut response, &RequestCtx::default()).await;
    let (parts, _) = response.into_parts();

    assert_matches!(result, FilterDecision::Continue);
    assert_eq!(parts.status.as_str(), "404");
}

#[tokio::test]
#[test_log::test]
async fn test_response_body_streaming_mode() {
    let mock_state = MockExternalProcessorState::new()
        .add_response(create_headers_response::<ResponseMsg>(
            vec![Some(("x-stream-processed", "true")), Some(("y-custom-header", "true"))],
            None,
            vec![],
            ResponseStatus::Continue as i32,
            // even though we will stream the body, no body modification is
            // performed in the headers response so no need to set the flag
            None,
        ))
        .add_response(create_body_response::<ResponseMsg>(
            vec![],
            Some("body data from external processor".as_bytes().into()),
            vec![],
            ResponseStatus::Continue as i32,
            // the response for the streaming body is still just a normal Body
            // no need to set end_of_stream, which is only for FULL_DUPLEX_STREAMED
            None,
        ));
    let (server_addr, _) = start_mock_server(mock_state).await;
    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Skip,
        request_body_mode: BodyProcessingMode::None,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Send,
        response_body_mode: BodyProcessingMode::Streamed,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.observability_mode = false;
    config.failure_mode_allow = false;
    let mut ext_proc = ExternalProcessor::from(config);

    let mut response =
        build_response_from_mock(&MockMessage::<ResponseMsg>::new(vec![], vec!["streaming body data"], vec![])).await;
    let result = ext_proc.apply_response(&mut response, &RequestCtx::default()).await;

    assert_matches!(result, FilterDecision::Continue);
    assert_eq!(response.headers().get("x-stream-processed").unwrap(), "true");
    let body_bytes = &mut response.body_mut().collect().await.unwrap().to_bytes();
    assert_eq!(body_bytes, "body data from external processor".as_bytes());
}

#[tokio::test]
#[test_log::test]
async fn test_response_body_streaming_mode_observability() {
    let mock_state =
        MockExternalProcessorState::new().with_observability(true).add_response(create_body_response::<ResponseMsg>(
            vec![],
            Some("body".as_bytes().into()),
            vec![],
            ResponseStatus::Continue as i32,
            None,
        ));
    let (server_addr, _) = start_mock_server(mock_state).await;
    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Skip,
        request_body_mode: BodyProcessingMode::None,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Skip,
        response_body_mode: BodyProcessingMode::Streamed,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.observability_mode = true;
    config.failure_mode_allow = false;
    let mut ext_proc = ExternalProcessor::from(config);

    let mut response =
        build_response_from_mock(&MockMessage::<ResponseMsg>::new(vec![], vec!["streaming body data"], vec![])).await;
    let result = ext_proc.apply_response(&mut response, &RequestCtx::default()).await;

    assert_matches!(result, FilterDecision::Continue);
    let body_bytes = &mut response.body_mut().collect().await.unwrap().to_bytes();
    assert_eq!(body_bytes, "streaming body data".as_bytes());
}

#[tokio::test]
#[test_log::test]
async fn test_request_header_timeout() {
    let mock_state = MockExternalProcessorState::new().add_response(
        create_headers_response::<RequestMsg>(
            vec![Some(("x-test-header", "new value"))],
            None,
            vec![],
            ResponseStatus::Continue as i32,
            None,
        )
        .with_delay(Duration::from_secs(10)),
    );

    let (server_addr, _) = start_mock_server(mock_state).await;
    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Send,
        request_body_mode: BodyProcessingMode::None,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Skip,
        response_body_mode: BodyProcessingMode::None,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.observability_mode = false;
    config.failure_mode_allow = false;
    let mut ext_proc = ExternalProcessor::from(config);

    let mut request = build_request_from_mock(&MockMessage::<RequestMsg>::new(
        vec![Some(("x-test-header", "original value"))],
        vec![],
        vec![],
    ))
    .await;

    let result = ext_proc.apply_request(&mut request, &RequestCtx::default()).await;
    assert_matches!(result, FilterDecision::DirectResponse(dr) => {
        assert_eq!(dr.status(), http::StatusCode::GATEWAY_TIMEOUT);
    });
}

#[tokio::test]
#[test_log::test]
async fn test_request_header_timeout_failure_mode_allow_true() {
    let mock_state = MockExternalProcessorState::new().add_response(
        create_headers_response::<RequestMsg>(
            vec![Some(("x-test-header", "new_value"))],
            None,
            vec![],
            ResponseStatus::Continue as i32,
            None,
        )
        .with_delay(Duration::from_secs(10)),
    );

    let (server_addr, _) = start_mock_server(mock_state).await;
    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Send,
        request_body_mode: BodyProcessingMode::None,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Skip,
        response_body_mode: BodyProcessingMode::None,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.observability_mode = false;
    config.failure_mode_allow = true;
    let mut ext_proc = ExternalProcessor::from(config);

    let mut request =
        build_request_from_mock(&MockMessage::<RequestMsg>::new(vec![], vec!["streaming body data"], vec![])).await;
    let result = ext_proc.apply_request(&mut request, &RequestCtx::default()).await;
    assert_matches!(result, FilterDecision::Continue);

    let (_, body) = request.into_parts();
    let body_bytes: Result<Collected<Bytes>, TimeoutBodyError<PolyBodyError>> = body.into_inner().collect().await;

    assert!(body_bytes.is_ok());
    if let Ok(bytes) = body_bytes {
        assert_eq!(bytes.to_bytes(), "streaming body data".as_bytes());
    }
}

#[tokio::test]
#[test_log::test]
async fn test_request_body_timeout_failure_mode_allow_false() {
    let mock_state = MockExternalProcessorState::new().add_response(
        create_body_response::<RequestMsg>(
            vec![],
            Some("new-body".as_bytes().into()),
            vec![],
            ResponseStatus::Continue as i32,
            None,
        )
        .with_delay(Duration::from_secs(10)),
    );
    let (server_addr, _) = start_mock_server(mock_state).await;
    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Skip,
        request_body_mode: BodyProcessingMode::Streamed,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Skip,
        response_body_mode: BodyProcessingMode::None,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.observability_mode = false;
    config.failure_mode_allow = false;
    let mut ext_proc = ExternalProcessor::from(config);

    let mut request =
        build_request_from_mock(&MockMessage::<RequestMsg>::new(vec![], vec!["streaming body data"], vec![])).await;
    let result = ext_proc.apply_request(&mut request, &RequestCtx::default()).await;
    assert_matches!(result, FilterDecision::Continue);

    let body_bytes = &mut request.body_mut().collect().await;
    assert!(body_bytes.is_err());
}

#[tokio::test]
#[test_log::test]
async fn test_request_body_timeout_failure_mode_allow_true() {
    let mock_state = MockExternalProcessorState::new().add_response(
        create_body_response::<RequestMsg>(
            vec![],
            Some("new-body".as_bytes().into()),
            vec![],
            ResponseStatus::Continue as i32,
            None,
        )
        .with_delay(Duration::from_secs(10)),
    );

    let (server_addr, _) = start_mock_server(mock_state).await;
    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Skip,
        request_body_mode: BodyProcessingMode::Streamed,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Skip,
        response_body_mode: BodyProcessingMode::None,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.observability_mode = false;
    config.failure_mode_allow = true;
    let mut ext_proc = ExternalProcessor::from(config);

    let mut request =
        build_request_from_mock(&MockMessage::<RequestMsg>::new(vec![], vec!["streaming body data"], vec![])).await;
    let result = ext_proc.apply_request(&mut request, &RequestCtx::default()).await;
    assert_matches!(result, FilterDecision::Continue);

    let (_, body) = request.into_parts();
    let body_bytes: Result<Collected<Bytes>, TimeoutBodyError<PolyBodyError>> = body.into_inner().collect().await;

    assert!(body_bytes.is_ok());
    if let Ok(bytes) = body_bytes {
        assert_eq!(bytes.to_bytes(), "streaming body data".as_bytes());
    }
}

#[tokio::test]
#[test_log::test]
async fn test_request_multichunk_body_with_mutation() {
    let mock_state = MockExternalProcessorState::new()
        .add_response(create_body_response::<RequestMsg>(
            vec![],
            Some("CHUNK1".into()),
            vec![],
            ResponseStatus::Continue as i32,
            None,
        ))
        .add_response(create_body_response::<RequestMsg>(
            vec![],
            Some("CHUNK2".into()),
            vec![],
            ResponseStatus::Continue as i32,
            None,
        ))
        .add_response(create_body_response::<RequestMsg>(
            vec![],
            Some("CHUNK3".into()),
            vec![],
            ResponseStatus::Continue as i32,
            None,
        ));

    let (server_addr, _) = start_mock_server(mock_state).await;
    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Skip,
        request_body_mode: BodyProcessingMode::Streamed,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Skip,
        response_body_mode: BodyProcessingMode::None,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.observability_mode = false;
    config.failure_mode_allow = false;

    let ext_config = ExternalProcessorConfigExt { frame_merge_limit: 1, frame_merge_window: Duration::from_millis(0) };
    let mut ext_proc = ExternalProcessor::from((config, None, Some(ext_config)));

    let mut request =
        build_request_from_mock(&MockMessage::<RequestMsg>::new(vec![], vec!["chunk1", "chunk2", "chunk3"], vec![]))
            .await;
    let result = ext_proc.apply_request(&mut request, &RequestCtx::default()).await;
    assert_matches!(result, FilterDecision::Continue);

    let body_chunks =
        to_body_data_chunks(std::mem::take(&mut request.body_mut().inner.inner).collect().await.unwrap()).await;

    assert_eq!(
        body_chunks,
        ["CHUNK1", "CHUNK2", "CHUNK3"].into_iter().map(std::convert::Into::into).collect::<Vec<Bytes>>()
    );
}

#[tokio::test]
#[test_log::test]
async fn test_request_multichunk_body_timeout_failure_mode_allow_true() {
    let mock_state = MockExternalProcessorState::new().add_response(
        create_body_response::<RequestMsg>(
            vec![],
            Some("modified_chunk".into()),
            vec![],
            ResponseStatus::Continue as i32,
            None,
        )
        .with_delay(Duration::from_secs(10)),
    );
    let (server_addr, _) = start_mock_server(mock_state).await;
    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Skip,
        request_body_mode: BodyProcessingMode::Streamed,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Skip,
        response_body_mode: BodyProcessingMode::None,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.observability_mode = false;
    config.failure_mode_allow = true;

    let ext_config = ExternalProcessorConfigExt { frame_merge_limit: 1, frame_merge_window: Duration::from_millis(0) };
    let mut ext_proc = ExternalProcessor::from((config, None, Some(ext_config)));

    let mut request =
        build_request_from_mock(&MockMessage::<RequestMsg>::new(vec![], vec!["chunk1", "chunk2", "chunk3"], vec![]))
            .await;
    let result = ext_proc.apply_request(&mut request, &RequestCtx::default()).await;
    assert_matches!(result, FilterDecision::Continue);

    let body_chunks =
        to_body_data_chunks(std::mem::take(&mut request.body_mut().inner.inner).collect().await.unwrap()).await;
    assert_eq!(
        body_chunks,
        ["chunk1", "chunk2", "chunk3"].into_iter().map(std::convert::Into::into).collect::<Vec<Bytes>>()
    );
}

#[tokio::test]
#[test_log::test]
async fn test_response_header_timeout() {
    let mock_state = MockExternalProcessorState::new().add_response(
        create_headers_response::<ResponseMsg>(
            vec![Some(("x-test-header", "new value"))],
            None,
            vec![],
            ResponseStatus::Continue as i32,
            None,
        )
        .with_delay(Duration::from_secs(10)),
    );
    let (server_addr, _) = start_mock_server(mock_state).await;
    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Skip,
        request_body_mode: BodyProcessingMode::None,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Send,
        response_body_mode: BodyProcessingMode::None,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.observability_mode = false;
    config.failure_mode_allow = false;
    let mut ext_proc = ExternalProcessor::from(config);

    let mut response =
        build_response_from_mock(&MockMessage::<ResponseMsg>::new(vec![], vec!["streaming body data"], vec![])).await;
    let result = ext_proc.apply_response(&mut response, &RequestCtx::default()).await;
    assert_matches!(result, FilterDecision::DirectResponse(dr) => {
        assert_eq!(dr.status(), http::StatusCode::GATEWAY_TIMEOUT);
    });
}

#[tokio::test]
#[test_log::test]
async fn test_response_header_timeout_failure_mode_allow_true() {
    let mock_state = MockExternalProcessorState::new().add_response(
        create_headers_response::<ResponseMsg>(
            vec![Some(("x-test-header", "new value"))],
            None,
            vec![],
            ResponseStatus::Continue as i32,
            None,
        )
        .with_delay(Duration::from_secs(10)),
    );
    let (server_addr, _) = start_mock_server(mock_state).await;
    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Skip,
        request_body_mode: BodyProcessingMode::None,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Send,
        response_body_mode: BodyProcessingMode::None,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.observability_mode = false;
    config.failure_mode_allow = true;
    let mut ext_proc = ExternalProcessor::from(config);

    let mut response =
        build_response_from_mock(&MockMessage::<ResponseMsg>::new(vec![], vec!["streaming body data"], vec![])).await;
    let result = ext_proc.apply_response(&mut response, &RequestCtx::default()).await;
    assert_matches!(result, FilterDecision::Continue);

    let (_, body) = response.into_parts();
    let body_bytes = body.collect().await;

    assert!(body_bytes.is_ok());
    if let Ok(bytes) = body_bytes {
        assert_eq!(bytes.to_bytes(), "streaming body data".as_bytes());
    }
}

#[tokio::test]
#[test_log::test]
async fn test_response_body_timeout_failure_mode_allow_false() {
    let mock_state = MockExternalProcessorState::new().add_response(
        create_body_response::<ResponseMsg>(
            vec![],
            Some("new-body".as_bytes().into()),
            vec![],
            ResponseStatus::Continue as i32,
            None,
        )
        .with_delay(Duration::from_secs(10)),
    );

    let (server_addr, _) = start_mock_server(mock_state).await;
    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Skip,
        request_body_mode: BodyProcessingMode::None,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Skip,
        response_body_mode: BodyProcessingMode::Streamed,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.observability_mode = false;
    config.failure_mode_allow = false;
    let mut ext_proc = ExternalProcessor::from(config);

    let mut response =
        build_response_from_mock(&MockMessage::<ResponseMsg>::new(vec![], vec!["streaming body data"], vec![])).await;
    let result = ext_proc.apply_response(&mut response, &RequestCtx::default()).await;
    assert_matches!(result, FilterDecision::Continue);

    let body_bytes = &mut response.body_mut().collect().await;
    assert!(body_bytes.is_err());
}

#[tokio::test]
#[test_log::test]
async fn test_response_body_timeout_failure_mode_allow_true() {
    let mock_state = MockExternalProcessorState::new().add_response(
        create_body_response::<ResponseMsg>(
            vec![],
            Some("new-body".as_bytes().into()),
            vec![],
            ResponseStatus::Continue as i32,
            None,
        )
        .with_delay(Duration::from_secs(10)),
    );

    let (server_addr, _) = start_mock_server(mock_state).await;
    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Skip,
        request_body_mode: BodyProcessingMode::None,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Skip,
        response_body_mode: BodyProcessingMode::Streamed,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.observability_mode = false;
    config.failure_mode_allow = true;
    let mut ext_proc = ExternalProcessor::from(config);

    let mut response =
        build_response_from_mock(&MockMessage::<ResponseMsg>::new(vec![], vec!["streaming body data"], vec![])).await;
    let result = ext_proc.apply_response(&mut response, &RequestCtx::default()).await;
    assert_matches!(result, FilterDecision::Continue);

    let (_, body) = response.into_parts();
    let body_bytes = body.collect().await;

    assert!(body_bytes.is_ok());
    if let Ok(bytes) = body_bytes {
        assert_eq!(bytes.to_bytes(), "streaming body data".as_bytes());
    }
}

#[tokio::test]
#[test_log::test]
async fn test_immediate_response_request_header() {
    let mock_state = MockExternalProcessorState::new().add_response(create_immediate_response(
        vec![Some(("x-immediate-header", "immediate value"))],
        Some("immediate body".as_bytes().into()),
        302,
    ));
    let (server_addr, _) = start_mock_server(mock_state).await;
    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Send,
        request_body_mode: BodyProcessingMode::None,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Skip,
        response_body_mode: BodyProcessingMode::None,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.disable_immediate_response = false;
    let mut ext_proc = ExternalProcessor::from(config);

    let mut request =
        build_request_from_mock(&MockMessage::<RequestMsg>::new(vec![], vec!["streaming body data"], vec![])).await;
    let result = ext_proc.apply_request(&mut request, &RequestCtx::default()).await;
    assert_matches!(result, FilterDecision::DirectResponse(dr) => {
        assert_eq!(dr.status(), StatusCode::from_u16(302).unwrap());
        assert_eq!(dr.headers().get("x-immediate-header").unwrap(), "immediate value");
        let (_, body) = dr.into_parts();
        let body_bytes = body.collect().await.unwrap().to_bytes();
        assert_eq!(body_bytes, "immediate body".as_bytes());
    });
}

#[tokio::test]
#[test_log::test]
async fn test_immediate_response_request_header_disable_immediate_response() {
    let mock_state = MockExternalProcessorState::new().add_response(create_immediate_response(
        vec![Some(("x-immediate-header", "immediate value"))],
        Some("immediate body".as_bytes().into()),
        302,
    ));
    let (server_addr, _) = start_mock_server(mock_state).await;
    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Send,
        request_body_mode: BodyProcessingMode::None,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Skip,
        response_body_mode: BodyProcessingMode::None,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.failure_mode_allow = false;
    config.disable_immediate_response = true;
    let mut ext_proc = ExternalProcessor::from(config);

    let mut request = build_request_from_mock(&MockMessage::<RequestMsg>::new(
        vec![Some(("x-test-header", "original value"))],
        vec!["streaming body data"],
        vec![],
    ))
    .await;
    let result = ext_proc.apply_request(&mut request, &RequestCtx::default()).await;

    assert_matches!(result, FilterDecision::DirectResponse(dr) => {
        assert_eq!(dr.status(), StatusCode::from_u16(500).unwrap());
    });
}

#[tokio::test]
#[test_log::test]
async fn test_immediate_response_request_header_disable_immediate_response_failure_allow_mode() {
    let mock_state = MockExternalProcessorState::new().add_response(create_immediate_response(
        vec![Some(("x-immediate-header", "immediate value"))],
        Some("immediate body".as_bytes().into()),
        302,
    ));
    let (server_addr, _) = start_mock_server(mock_state).await;
    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Send,
        request_body_mode: BodyProcessingMode::None,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Skip,
        response_body_mode: BodyProcessingMode::None,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.failure_mode_allow = true;
    config.disable_immediate_response = true;
    let mut ext_proc = ExternalProcessor::from(config);

    let mut request = build_request_from_mock(&MockMessage::<RequestMsg>::new(
        vec![Some(("x-test-header", "original value"))],
        vec!["streaming body data"],
        vec![],
    ))
    .await;
    let result = ext_proc.apply_request(&mut request, &RequestCtx::default()).await;

    assert_matches!(result, FilterDecision::Continue);
    let (parts, body) = request.into_parts();
    assert_eq!(parts.headers.get("x-test-header").unwrap(), "original value");
    let body_bytes = body.collect().await;
    assert!(body_bytes.is_ok());
    if let Ok(bytes) = body_bytes {
        assert_eq!(bytes.to_bytes(), "streaming body data".as_bytes());
    }
}

#[tokio::test]
#[test_log::test]
async fn test_request_header_override_request_body() {
    // Original processing mode
    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Send,
        request_body_mode: BodyProcessingMode::None,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Skip,
        response_body_mode: BodyProcessingMode::None,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    // ext-proc override
    let mut header_response =
        create_headers_response::<RequestMsg>(vec![], None, vec![], ResponseStatus::Continue as i32, None);
    header_response.mode_override = Some(EnvoyProcessingMode {
        request_header_mode: processing_mode::HeaderSendMode::Send as i32,
        request_body_mode: processing_mode::BodySendMode::Buffered as i32,
        request_trailer_mode: processing_mode::HeaderSendMode::Skip as i32,
        response_header_mode: processing_mode::HeaderSendMode::Skip as i32,
        response_body_mode: processing_mode::BodySendMode::None as i32,
        response_trailer_mode: processing_mode::HeaderSendMode::Skip as i32,
    });
    let mock_state = MockExternalProcessorState::new().add_response(header_response).add_response(
        create_body_response::<RequestMsg>(
            vec![],
            Some("ext-proc body".into()),
            vec![],
            ResponseStatus::Continue as i32,
            None,
        ),
    );
    let (server_addr, _) = start_mock_server(mock_state).await;

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.allow_mode_override = true;
    let mut ext_proc = ExternalProcessor::from(config);

    let mut request = build_request_from_mock(&MockMessage::<RequestMsg> {
        headers: vec![Some(("content-type", "application/json"))],
        body: vec!["original body"],
        trailers: vec![],
        _marker: std::marker::PhantomData,
    })
    .await;

    let result = ext_proc.apply_request(&mut request, &RequestCtx::default()).await;
    assert_matches!(result, FilterDecision::Continue);

    assert_eq!(request.headers().get("content-type").unwrap(), "application/json");

    let (_, body) = request.into_parts();
    let body_bytes = body.collect().await;
    assert!(body_bytes.is_ok());
    if let Ok(bytes) = body_bytes {
        assert_eq!(bytes.to_bytes(), "ext-proc body".as_bytes());
    }
}

#[tokio::test]
#[test_log::test]
async fn test_request_header_override_request_trailers() {
    // Original processing mode
    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Send,
        request_body_mode: BodyProcessingMode::None,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Skip,
        response_body_mode: BodyProcessingMode::None,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    // ext-proc override
    let mut header_response =
        create_headers_response::<RequestMsg>(vec![], None, vec![], ResponseStatus::Continue as i32, None);

    header_response.mode_override = Some(EnvoyProcessingMode {
        request_header_mode: processing_mode::HeaderSendMode::Send as i32,
        request_body_mode: processing_mode::BodySendMode::None as i32,
        request_trailer_mode: processing_mode::HeaderSendMode::Send as i32,
        response_header_mode: processing_mode::HeaderSendMode::Skip as i32,
        response_body_mode: processing_mode::BodySendMode::None as i32,
        response_trailer_mode: processing_mode::HeaderSendMode::Skip as i32,
    });
    let mock_state =
        MockExternalProcessorState::new().add_response(header_response).add_response(create_trailers_response::<
            RequestMsg,
        >(vec![Some((
            "x-ext-proc-trailer",
            "ext-proc trailer value",
        ))]));
    let (server_addr, _) = start_mock_server(mock_state).await;

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.allow_mode_override = true;
    let mut ext_proc = ExternalProcessor::from(config);

    let mut request = build_request_from_mock(&MockMessage::<RequestMsg> {
        headers: vec![Some(("content-type", "application/json"))],
        body: vec![],
        trailers: vec![Some(("x-original-trailer", "original trailer value"))],
        _marker: std::marker::PhantomData,
    })
    .await;

    let result = ext_proc.apply_request(&mut request, &RequestCtx::default()).await;
    assert_matches!(result, FilterDecision::Continue);
    assert_eq!(request.headers().get("content-type").unwrap(), "application/json");

    let body = std::mem::take(&mut request.body_mut().inner.inner).collect().await.unwrap();
    let trailers = body.trailers().cloned();

    assert!(trailers.is_some());
    let trailers = trailers.unwrap();
    assert_matches!(result, FilterDecision::Continue);
    assert_eq!(trailers.get("x-original-trailer").unwrap(), "original trailer value");
    assert_eq!(trailers.get("x-ext-proc-trailer").unwrap(), "ext-proc trailer value");
}

#[tokio::test]
#[test_log::test]
async fn test_response_header_override_response_body() {
    // Original processing mode
    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Skip,
        request_body_mode: BodyProcessingMode::None,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Send,
        response_body_mode: BodyProcessingMode::None,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    // ext-proc override
    let mut header_response =
        create_headers_response::<ResponseMsg>(vec![], None, vec![], ResponseStatus::Continue as i32, None);

    header_response.mode_override = Some(EnvoyProcessingMode {
        request_header_mode: processing_mode::HeaderSendMode::Skip as i32,
        request_body_mode: processing_mode::BodySendMode::None as i32,
        request_trailer_mode: processing_mode::HeaderSendMode::Skip as i32,
        response_header_mode: processing_mode::HeaderSendMode::Send as i32,
        response_body_mode: processing_mode::BodySendMode::Buffered as i32,
        response_trailer_mode: processing_mode::HeaderSendMode::Skip as i32,
    });
    let mock_state = MockExternalProcessorState::new().add_response(header_response).add_response(
        create_body_response::<ResponseMsg>(
            vec![],
            Some("ext-proc body".into()),
            vec![],
            ResponseStatus::Continue as i32,
            None,
        ),
    );
    let (server_addr, _) = start_mock_server(mock_state).await;

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.allow_mode_override = true;
    let mut ext_proc = ExternalProcessor::from(config);

    let mut response = build_response_from_mock(&MockMessage::<ResponseMsg> {
        headers: vec![Some(("content-type", "application/json"))],
        body: vec!["original body"],
        trailers: vec![],
        _marker: std::marker::PhantomData,
    })
    .await;

    let result = ext_proc.apply_response(&mut response, &RequestCtx::default()).await;
    assert_matches!(result, FilterDecision::Continue);

    assert_eq!(response.headers().get("content-type").unwrap(), "application/json");
    let (_, body) = response.into_parts();
    let body_bytes = body.collect().await;
    assert!(body_bytes.is_ok());
    if let Ok(bytes) = body_bytes {
        assert_eq!(bytes.to_bytes(), "ext-proc body".as_bytes());
    }
}

#[tokio::test]
#[test_log::test]
async fn test_response_header_override_response_trailers() {
    // Original processing mode
    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Skip,
        request_body_mode: BodyProcessingMode::None,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Send,
        response_body_mode: BodyProcessingMode::None,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    // ext-proc override
    let mut header_response =
        create_headers_response::<ResponseMsg>(vec![], None, vec![], ResponseStatus::Continue as i32, None);
    header_response.mode_override = Some(EnvoyProcessingMode {
        request_header_mode: processing_mode::HeaderSendMode::Skip as i32,
        request_body_mode: processing_mode::BodySendMode::None as i32,
        request_trailer_mode: processing_mode::HeaderSendMode::Skip as i32,
        response_header_mode: processing_mode::HeaderSendMode::Send as i32,
        response_body_mode: processing_mode::BodySendMode::None as i32,
        response_trailer_mode: processing_mode::HeaderSendMode::Send as i32,
    });
    let mock_state =
        MockExternalProcessorState::new().add_response(header_response).add_response(create_trailers_response::<
            ResponseMsg,
        >(vec![Some((
            "x-ext-proc-trailer",
            "ext-proc trailer value",
        ))]));
    let (server_addr, _) = start_mock_server(mock_state).await;

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.allow_mode_override = true;
    let mut ext_proc = ExternalProcessor::from(config);

    let mut response = build_response_from_mock(&MockMessage::<ResponseMsg> {
        headers: vec![Some(("content-type", "application/json"))],
        body: vec![],
        trailers: vec![Some(("x-original-trailer", "original trailer value"))],
        _marker: std::marker::PhantomData,
    })
    .await;

    let result = ext_proc.apply_response(&mut response, &RequestCtx::default()).await;
    assert_matches!(result, FilterDecision::Continue);
    assert_eq!(response.headers().get("content-type").unwrap(), "application/json");

    let (_, body) = response.into_parts();
    let collected_body = body.collect().await.unwrap();
    let trailers = collected_body.trailers().cloned();
    assert!(trailers.is_some());
    let trailers = trailers.unwrap();
    assert_matches!(result, FilterDecision::Continue);
    assert_eq!(trailers.get("x-ext-proc-trailer").unwrap(), "ext-proc trailer value");
}

#[tokio::test]
#[test_log::test]
async fn test_request_header_override_response_header() {
    // Original processing mode
    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Send,
        request_body_mode: BodyProcessingMode::None,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Skip,
        response_body_mode: BodyProcessingMode::None,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    // ext-proc override
    let mut request_header_response =
        create_headers_response::<RequestMsg>(vec![], None, vec![], ResponseStatus::Continue as i32, None);
    request_header_response.mode_override = Some(EnvoyProcessingMode {
        request_header_mode: processing_mode::HeaderSendMode::Send as i32,
        request_body_mode: processing_mode::BodySendMode::None as i32,
        request_trailer_mode: processing_mode::HeaderSendMode::Skip as i32,
        response_header_mode: processing_mode::HeaderSendMode::Send as i32,
        response_body_mode: processing_mode::BodySendMode::Buffered as i32,
        response_trailer_mode: processing_mode::HeaderSendMode::Send as i32,
    });
    let mock_state = MockExternalProcessorState::new()
        .add_response(request_header_response)
        .add_response(create_headers_response::<ResponseMsg>(
            vec![Some(("x-custom-header", "ext-proc header value"))],
            None,
            vec![],
            ResponseStatus::Continue as i32,
            None,
        ))
        .add_response(create_body_response::<ResponseMsg>(
            vec![],
            Some("ext-proc body value".into()),
            vec![],
            ResponseStatus::Continue as i32,
            None,
        ))
        .add_response(create_trailers_response::<ResponseMsg>(vec![Some((
            "x-custom-trailer",
            "ext-proc trailer value",
        ))]));
    let (server_addr, _) = start_mock_server(mock_state).await;

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.allow_mode_override = true;
    let mut ext_proc = ExternalProcessor::from(config);

    let mut request = build_request_from_mock(&MockMessage::<RequestMsg> {
        headers: vec![Some(("content-type", "application/json"))],
        body: vec![],
        trailers: vec![],
        _marker: std::marker::PhantomData,
    })
    .await;

    let request_result = ext_proc.apply_request(&mut request, &RequestCtx::default()).await;
    assert_matches!(request_result, FilterDecision::Continue);
    assert_eq!(request.headers().get("content-type").unwrap(), "application/json");

    let mut response = build_response_from_mock(&MockMessage::<ResponseMsg> {
        headers: vec![Some(("content-type", "application/json"))],
        body: vec!["original response body"],
        trailers: vec![Some(("x-custom-trailer", "original trailer value"))],
        _marker: std::marker::PhantomData,
    })
    .await;

    let result = ext_proc.apply_response(&mut response, &RequestCtx::default()).await;
    assert_matches!(result, FilterDecision::Continue);
    assert_eq!(response.headers().get("content-type").unwrap(), "application/json");
    assert_eq!(response.headers().get("x-custom-header").unwrap(), "ext-proc header value");

    let (_, body) = response.into_parts();
    let collected_body = body.collect().await.unwrap();
    let trailers = collected_body.trailers().cloned();
    assert!(trailers.is_some());
    assert_eq!(collected_body.to_bytes(), "ext-proc body value");
    let trailers = trailers.unwrap();
    assert_matches!(result, FilterDecision::Continue);
    assert_eq!(trailers.get("x-custom-trailer").unwrap(), "ext-proc trailer value");
}

#[tokio::test]
#[test_log::test]
async fn test_request_body_buffered_too_large() {
    let mock_state = MockExternalProcessorState::new()
        .add_response(create_headers_response::<RequestMsg>(
            vec![],
            None,
            vec![],
            ResponseStatus::Continue as i32,
            None,
        ))
        .add_response(create_body_response::<RequestMsg>(vec![], None, vec![], ResponseStatus::Continue as i32, None));

    let (server_addr, _) = start_mock_server(mock_state).await;
    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Send,
        request_body_mode: BodyProcessingMode::Buffered,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Skip,
        response_body_mode: BodyProcessingMode::None,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.observability_mode = false;
    config.failure_mode_allow = false;
    let mut ext_proc = ExternalProcessor::from(config);

    let very_large_body = "a".repeat(200 * 1024 * 1024); // 200 MB body
    let boxed_str: Box<str> = very_large_body.clone().into_boxed_str();

    let mut request = build_request_from_mock(&MockMessage::<RequestMsg> {
        headers: vec![],
        body: vec![Box::leak(boxed_str)],
        trailers: vec![],
        _marker: std::marker::PhantomData,
    })
    .await;
    let result = ext_proc.apply_request(&mut request, &RequestCtx::default()).await;

    assert_matches!(result, FilterDecision::DirectResponse(_));
    match result {
        FilterDecision::DirectResponse(response) => {
            assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        },
        _ => panic!("Unexpected filter decision"),
    }
}

#[tokio::test]
#[test_log::test]
async fn test_request_multichunk_merged_body_streaming_mode() {
    let mock_state = MockExternalProcessorState::new()
        .add_response(create_headers_response::<RequestMsg>(
            vec![Some(("x-stream-processed", "true")), Some(("y-custom-header", "true"))],
            None,
            vec![],
            ResponseStatus::Continue as i32,
            // even though we will stream the body, no body modification is
            // performed in the headers response so no need to set the flag
            None,
        ))
        .add_response(create_body_response::<RequestMsg>(
            vec![],
            Some("body data from external processor".as_bytes().into()),
            vec![],
            ResponseStatus::Continue as i32,
            // the response for the streaming body is still just a normal Body
            // no need to set end_of_stream, which is only for FULL_DUPLEX_STREAMED
            None,
        ));
    let (server_addr, _) = start_mock_server(mock_state).await;
    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Send,
        request_body_mode: BodyProcessingMode::Streamed,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Skip,
        response_body_mode: BodyProcessingMode::None,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.observability_mode = false;
    config.failure_mode_allow = false;
    let mut ext_proc = ExternalProcessor::from(config);

    let mut request = build_request_from_mock(&MockMessage::<RequestMsg> {
        headers: vec![],
        body: vec!["streaming", "body data"],
        trailers: vec![],
        _marker: std::marker::PhantomData,
    })
    .await;
    let result = ext_proc.apply_request(&mut request, &RequestCtx::default()).await;

    assert_matches!(result, FilterDecision::Continue);
    assert_eq!(request.headers().get("x-stream-processed").unwrap(), "true");
    let body_bytes = std::mem::take(&mut request.body_mut().inner.inner).collect().await.unwrap().to_bytes();
    assert_eq!(body_bytes, "body data from external processor".as_bytes());
}

#[tokio::test]
#[test_log::test]
async fn test_request_multichunk_merged_body_buffered_mode() {
    let mock_state = MockExternalProcessorState::new()
        .add_response(create_headers_response::<RequestMsg>(
            vec![Some(("x-stream-processed", "true")), Some(("y-custom-header", "true"))],
            None,
            vec![],
            ResponseStatus::Continue as i32,
            // even though we will stream the body, no body modification is
            // performed in the headers response so no need to set the flag
            None,
        ))
        .add_response(create_body_response::<RequestMsg>(
            vec![],
            Some("body data from external processor".as_bytes().into()),
            vec![],
            ResponseStatus::Continue as i32,
            // the response for the streaming body is still just a normal Body
            // no need to set end_of_stream, which is only for FULL_DUPLEX_STREAMED
            None,
        ));
    let (server_addr, _) = start_mock_server(mock_state).await;
    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Send,
        request_body_mode: BodyProcessingMode::Buffered,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Skip,
        response_body_mode: BodyProcessingMode::None,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.observability_mode = false;
    config.failure_mode_allow = false;
    let mut ext_proc = ExternalProcessor::from(config);

    let mut request = build_request_from_mock(&MockMessage::<RequestMsg> {
        headers: vec![],
        body: vec!["streaming", "body data"],
        trailers: vec![],
        _marker: std::marker::PhantomData,
    })
    .await;
    let result = ext_proc.apply_request(&mut request, &RequestCtx::default()).await;

    assert_matches!(result, FilterDecision::Continue);
    assert_eq!(request.headers().get("x-stream-processed").unwrap(), "true");
    let body_bytes = std::mem::take(&mut request.body_mut().inner.inner).collect().await.unwrap().to_bytes();
    assert_eq!(body_bytes, "body data from external processor".as_bytes());
}

#[tokio::test]
#[test_log::test]
async fn test_request_multichunk_not_merged_body_streaming_mode() {
    let mock_state = MockExternalProcessorState::new()
        .add_response(create_headers_response::<RequestMsg>(
            vec![Some(("x-stream-processed", "true")), Some(("y-custom-header", "true"))],
            None,
            vec![],
            ResponseStatus::Continue as i32,
            // even though we will stream the body, no body modification is
            // performed in the headers response so no need to set the flag
            None,
        ))
        .add_response(create_body_response::<RequestMsg>(
            vec![],
            Some("body data".as_bytes().into()),
            vec![],
            ResponseStatus::Continue as i32,
            // the response for the streaming body is still just a normal Body
            // no need to set end_of_stream, which is only for FULL_DUPLEX_STREAMED
            None,
        ))
        .add_response(create_body_response::<RequestMsg>(
            vec![],
            Some("external processor".as_bytes().into()),
            vec![],
            ResponseStatus::Continue as i32,
            // the response for the streaming body is still just a normal Body
            // no need to set end_of_stream, which is only for FULL_DUPLEX_STREAMED
            None,
        ));
    let (server_addr, _) = start_mock_server(mock_state).await;
    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Send,
        request_body_mode: BodyProcessingMode::Streamed,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Skip,
        response_body_mode: BodyProcessingMode::None,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.observability_mode = false;
    config.failure_mode_allow = false;

    // build external processor, with extended configuration that prevent
    // chunks aggregation

    let ext_config = ExternalProcessorConfigExt { frame_merge_limit: 1, frame_merge_window: Duration::from_millis(0) };

    let mut ext_proc = ExternalProcessor::from((config, None, Some(ext_config)));

    let mut request = build_request_from_mock(&MockMessage::<RequestMsg> {
        headers: vec![],
        body: vec!["streaming", "body"],
        trailers: vec![],
        _marker: std::marker::PhantomData,
    })
    .await;
    let result = ext_proc.apply_request(&mut request, &RequestCtx::default()).await;

    assert_matches!(result, FilterDecision::Continue);
    assert_eq!(request.headers().get("x-stream-processed").unwrap(), "true");

    let body_chunks =
        to_body_data_chunks(std::mem::take(&mut request.body_mut().inner.inner).collect().await.unwrap()).await;
    assert_eq!(
        body_chunks,
        ["body data", "external processor"].into_iter().map(std::convert::Into::into).collect::<Vec<Bytes>>()
    );
}

use futures::task::noop_waker;
use std::pin::Pin;
use std::task::Context;

#[allow(clippy::indexing_slicing)]
async fn assert_body_frames<B>(
    mut body: B,
    expected_data: &[Bytes],
    expected_trailers: Option<http::HeaderMap>,
) -> Result<(), String>
where
    B: Body + Unpin,
    B: Body<Data = Bytes>,
    B::Error: Sized + Unpin + std::fmt::Debug,
{
    // Setup for manual polling
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);

    let mut pinned_body = Pin::new(&mut body);
    let mut data_index = 0;
    let mut trailers_seen = None;

    loop {
        // Simulates the call to poll_frame from the asynchronous runtime
        let poll = pinned_body.as_mut().poll_frame(&mut cx);

        match poll {
            // Case 1: Frame is ready
            std::task::Poll::Ready(maybe_frame) => {
                match maybe_frame {
                    // Body is fully consumed: End the loop
                    None => {
                        break;
                    },

                    Some(Ok(frame)) => {
                        if frame.is_data() {
                            // Control 1: DATA Frame
                            if trailers_seen.is_some() {
                                return Err(format!("Error: DATA received after TRAILERS. Data Index: {data_index}"));
                            }

                            // Optional: Verification of DATA content
                            let data = frame.into_data().unwrap();

                            if data_index < expected_data.len() {
                                if data != expected_data[data_index] {
                                    return Err(format!("Error: DATA content mismatch for chunk {data_index}"));
                                }
                            } else {
                                // Received more DATA than expected (malformed)
                                return Err("Error: More DATA chunks than expected.".to_owned());
                            }

                            data_index += 1;
                        } else if frame.is_trailers() {
                            // Control 2: TRAILERS Frame
                            if trailers_seen.is_some() {
                                return Err("Error: Received a second Frame::trailers.".to_owned());
                            }
                            if data_index < expected_data.len() {
                                // Trailer frame received before all expected data chunks were seen
                                return Err("Error: TRAILERS received prematurely before all DATA.".to_owned());
                            }

                            trailers_seen = Some(frame.into_trailers().unwrap());
                        } else {
                            // Unexpected frame type
                            return Err(format!("DEBUG: Received unexpected Frame: {frame:?}"));
                        }
                    },

                    Some(Err(e)) => {
                        // Error occurred while reading the body
                        return Err(format!("Body Error: {e:?}"));
                    },
                }
            },

            // Case 2: Not ready, Body is pending (waiting for data)
            std::task::Poll::Pending => {
                // Even if we are in a test, ext_proc uses a ChannelBody
                // that might delay delivery of data chunks, so we could hit
                // a Poll::Pending. If that happens, yield to allow the body
                // to make progress.
                tokio::task::yield_now().await;
            },
        }
    }

    if trailers_seen != expected_trailers {
        return Err(format!("Trailers {trailers_seen:?} did not match expected state. Expected {expected_trailers:?}"));
    }

    // Final check: ensure all expected data was processed
    if data_index == expected_data.len() {
        Ok(())
    } else {
        Err("Body was not consumed completely.".to_owned())
    }
}

#[tokio::test]
#[test_log::test]
async fn test_request_body_and_trailer_processing_out_of_order() {
    let mock_state = MockExternalProcessorState::new()
        .add_response(
            create_headers_response::<RequestMsg>(vec![], None, vec![], ResponseStatus::Continue as i32, None)
                .with_delay(Duration::from_millis(500)),
        )
        .add_response(create_body_response::<RequestMsg>(vec![], None, vec![], ResponseStatus::Continue as i32, None))
        .add_response(create_body_response::<RequestMsg>(vec![], None, vec![], ResponseStatus::Continue as i32, None))
        .add_response(create_body_response::<RequestMsg>(vec![], None, vec![], ResponseStatus::Continue as i32, None));

    let (server_addr, _) = start_mock_server(mock_state).await;
    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Send,
        request_body_mode: BodyProcessingMode::Streamed,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Skip,
        response_body_mode: BodyProcessingMode::None,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.observability_mode = false;
    config.failure_mode_allow = false;

    let ext_config = ExternalProcessorConfigExt { frame_merge_limit: 1, frame_merge_window: Duration::from_millis(0) };

    let mut ext_proc = ExternalProcessor::from((config, None, Some(ext_config)));

    let mut request = build_request_from_mock(&MockMessage::<RequestMsg> {
        headers: vec![Some(("content-type", "application/json"))],
        body: vec!["chunk1", "chunk2", "chunk3"],
        trailers: vec![Some(("x-custom-trailer", "original-value"))],
        _marker: std::marker::PhantomData,
    })
    .await;

    let result = ext_proc.apply_request(&mut request, &RequestCtx::default()).await;
    assert_matches!(result, FilterDecision::Continue);

    let body = std::mem::take(&mut request.body_mut().inner.inner);

    let expected_trailers = http::HeaderMap::from_iter(vec![(
        http::HeaderName::from_static("x-custom-trailer"),
        http::HeaderValue::from_static("original-value"),
    )]);
    let expected_data = &["chunk1".into(), "chunk2".into(), "chunk3".into()];

    assert_matches!(assert_body_frames(body, expected_data, Some(expected_trailers)).await, Ok(()));
}

#[tokio::test]
#[test_log::test]
async fn test_request_multichunk_body_no_truncate_body() {
    let mock_state = MockExternalProcessorState::new()
        .add_response(create_body_response::<RequestMsg>(
            vec![],
            Some("CHUNK1".into()),
            vec![],
            ResponseStatus::Continue as i32,
            None,
        ))
        .add_response(create_body_response::<RequestMsg>(
            vec![],
            Some("CHUNK2".into()),
            vec![],
            ResponseStatus::Continue as i32,
            None,
        ))
        .add_response(create_body_response::<RequestMsg>(
            vec![],
            Some("CHUNK3".into()),
            vec![],
            ResponseStatus::Continue as i32,
            None,
        ));

    let (server_addr, _) = start_mock_server(mock_state).await;
    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Skip,
        request_body_mode: BodyProcessingMode::Streamed,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Skip,
        response_body_mode: BodyProcessingMode::None,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.observability_mode = false;
    config.failure_mode_allow = false;

    let ext_config = ExternalProcessorConfigExt { frame_merge_limit: 1, frame_merge_window: Duration::from_millis(0) };
    let mut ext_proc = ExternalProcessor::from((config, None, Some(ext_config)));

    let mut request =
        build_request_from_mock(&MockMessage::<RequestMsg>::new(vec![], vec!["chunk1", "chunk2", "chunk3"], vec![]))
            .await;
    let result = ext_proc.apply_request(&mut request, &RequestCtx::default()).await;
    assert_matches!(result, FilterDecision::Continue);

    let body_chunks =
        to_body_data_chunks(std::mem::take(&mut request.body_mut().inner.inner).collect().await.unwrap()).await;

    assert_eq!(
        body_chunks,
        ["CHUNK1", "CHUNK2", "CHUNK3"].into_iter().map(std::convert::Into::into).collect::<Vec<Bytes>>()
    );
}

#[tokio::test]
#[test_log::test]
async fn test_response_header_mutation() {
    let mock_state = MockExternalProcessorState::new().add_response(create_headers_response::<ResponseMsg>(
        vec![Some(("x-processed", "true")), Some(("x-custom-header", "custom-value"))],
        None,
        vec![],
        ResponseStatus::Continue as i32,
        None,
    ));
    let (server_addr, _) = start_mock_server(mock_state).await;
    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Skip,
        request_body_mode: BodyProcessingMode::None,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Send,
        response_body_mode: BodyProcessingMode::None,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.observability_mode = false;
    config.failure_mode_allow = false;
    let mut ext_proc = ExternalProcessor::from(config);

    let mut response = build_response_from_mock(&MockMessage::<ResponseMsg> {
        headers: vec![Some(("content-type", "application/json"))],
        body: vec![],
        trailers: vec![],
        _marker: std::marker::PhantomData,
    })
    .await;

    let result = ext_proc.apply_response(&mut response, &RequestCtx::default()).await;

    assert_matches!(result, FilterDecision::Continue);
    assert_eq!(response.headers().get("x-processed").unwrap(), "true");
    assert_eq!(response.headers().get("x-custom-header").unwrap(), "custom-value");
    assert_eq!(response.headers().get("content-type").unwrap(), "application/json");
}

#[tokio::test]
#[test_log::test]
async fn test_response_trailer_mutation() {
    let mock_state = MockExternalProcessorState::new()
        .add_response(create_headers_response::<ResponseMsg>(
            vec![],
            None,
            vec![],
            ResponseStatus::Continue as i32,
            None,
        ))
        .add_response(create_body_response::<ResponseMsg>(vec![], None, vec![], ResponseStatus::Continue as i32, None))
        .add_response(create_trailers_response::<ResponseMsg>(vec![
            Some(("x-processed", "true")),
            Some(("x-custom-trailer", "modified-value")),
        ]));
    let (server_addr, _) = start_mock_server(mock_state).await;
    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Skip,
        request_body_mode: BodyProcessingMode::None,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Send,
        response_body_mode: BodyProcessingMode::Buffered,
        response_trailer_mode: TrailerProcessingMode::Send,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.observability_mode = false;
    config.failure_mode_allow = false;
    let mut ext_proc = ExternalProcessor::from(config);

    let mut response = build_response_from_mock(&MockMessage::<ResponseMsg> {
        headers: vec![Some(("content-type", "application/json"))],
        body: vec!["body"],
        trailers: vec![Some(("x-custom-trailer", "original-value"))],
        _marker: std::marker::PhantomData,
    })
    .await;

    let result = ext_proc.apply_response(&mut response, &RequestCtx::default()).await;
    let (parts, body) = response.into_parts();
    let collected = body.collect().await.unwrap();
    let trailers = collected.trailers().cloned();

    assert!(trailers.is_some());
    let trailers = trailers.unwrap();
    assert_matches!(result, FilterDecision::Continue);
    assert_eq!(parts.headers.get("content-type").unwrap(), "application/json");
    assert_eq!(collected.to_bytes(), "body");
    assert_eq!(trailers.get("x-processed").unwrap(), "true");
    assert_eq!(trailers.get("x-custom-trailer").unwrap(), "modified-value");
}

#[tokio::test]
#[test_log::test]
async fn test_response_body_buffered_continue_and_replace_on_headers_response() {
    let new_body = "modified body content";
    let mock_state = MockExternalProcessorState::new().add_response(create_headers_response::<ResponseMsg>(
        vec![Some(("y-custom-header", "true"))],
        Some(new_body.as_bytes().into()),
        vec![],
        ResponseStatus::ContinueAndReplace as i32,
        None,
    ));
    let (server_addr, _) = start_mock_server(mock_state).await;
    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Skip,
        request_body_mode: BodyProcessingMode::None,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Send,
        response_body_mode: BodyProcessingMode::Buffered,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.observability_mode = false;
    config.failure_mode_allow = false;
    let mut ext_proc = ExternalProcessor::from(config);

    let mut response = build_response_from_mock(&MockMessage::<ResponseMsg> {
        headers: vec![],
        body: vec!["original body"],
        trailers: vec![],
        _marker: std::marker::PhantomData,
    })
    .await;

    let result = ext_proc.apply_response(&mut response, &RequestCtx::default()).await;

    assert_matches!(result, FilterDecision::Continue);
    assert_eq!(response.headers().get("y-custom-header").unwrap(), "true");
    let body_bytes = response.body_mut().collect().await.unwrap().to_bytes();
    assert_eq!(body_bytes, new_body.as_bytes());
}

#[tokio::test]
#[test_log::test]
async fn test_response_body_buffered_continue_and_replace_on_body_response() {
    let new_body = "modified body content";
    let mock_state = MockExternalProcessorState::new()
        .add_response(create_headers_response::<ResponseMsg>(
            vec![],
            None,
            vec![],
            ResponseStatus::Continue as i32,
            None,
        ))
        .add_response(create_body_response::<ResponseMsg>(
            vec![],
            Some(new_body.as_bytes().into()),
            vec![],
            ResponseStatus::ContinueAndReplace as i32,
            None,
        ));
    let (server_addr, _) = start_mock_server(mock_state).await;
    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Skip,
        request_body_mode: BodyProcessingMode::None,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Send,
        response_body_mode: BodyProcessingMode::Buffered,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.observability_mode = false;
    config.failure_mode_allow = false;
    let mut ext_proc = ExternalProcessor::from(config);

    let mut response = build_response_from_mock(&MockMessage::<ResponseMsg> {
        headers: vec![],
        body: vec!["original body"],
        trailers: vec![],
        _marker: std::marker::PhantomData,
    })
    .await;

    let result = ext_proc.apply_response(&mut response, &RequestCtx::default()).await;

    assert_matches!(result, FilterDecision::Continue);
    let body_bytes = response.body_mut().collect().await.unwrap().to_bytes();
    assert_eq!(body_bytes, new_body.as_bytes());
}

#[tokio::test]
#[test_log::test]
async fn test_response_body_buffered_mode() {
    let mock_state = MockExternalProcessorState::new()
        .add_response(create_headers_response::<ResponseMsg>(
            vec![Some(("x-stream-processed", "true")), Some(("y-custom-header", "true"))],
            None,
            vec![],
            ResponseStatus::Continue as i32,
            None,
        ))
        .add_response(create_body_response::<ResponseMsg>(
            vec![],
            Some("body data from external processor".as_bytes().into()),
            vec![],
            ResponseStatus::Continue as i32,
            None,
        ));

    let (server_addr, _) = start_mock_server(mock_state).await;
    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Skip,
        request_body_mode: BodyProcessingMode::None,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Send,
        response_body_mode: BodyProcessingMode::Buffered,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.observability_mode = false;
    config.failure_mode_allow = false;
    let mut ext_proc = ExternalProcessor::from(config);

    let mut response = build_response_from_mock(&MockMessage::<ResponseMsg> {
        headers: vec![],
        body: vec!["buffered body data"],
        trailers: vec![],
        _marker: std::marker::PhantomData,
    })
    .await;

    let result = ext_proc.apply_response(&mut response, &RequestCtx::default()).await;

    assert_matches!(result, FilterDecision::Continue);
    assert_eq!(response.headers().get("x-stream-processed").unwrap(), "true");
    let body_bytes = response.body_mut().collect().await.unwrap().to_bytes();
    assert_eq!(body_bytes, "body data from external processor".as_bytes());
}

#[tokio::test]
#[test_log::test]
async fn test_response_body_buffered_mode_header_mutations_on_body() {
    let mock_state = MockExternalProcessorState::new()
        .add_response(create_headers_response::<ResponseMsg>(
            vec![Some(("mutation-in-header-response", "true"))],
            None,
            vec![],
            ResponseStatus::Continue as i32,
            None,
        ))
        .add_response(create_body_response::<ResponseMsg>(
            vec![Some(("mutation-in-body-response", "true"))],
            Some("body data from external processor".as_bytes().into()),
            vec![],
            ResponseStatus::Continue as i32,
            None,
        ));

    let (server_addr, _) = start_mock_server(mock_state).await;
    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Skip,
        request_body_mode: BodyProcessingMode::None,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Send,
        response_body_mode: BodyProcessingMode::Buffered,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.observability_mode = false;
    config.failure_mode_allow = false;
    let mut ext_proc = ExternalProcessor::from(config);

    let mut response = build_response_from_mock(&MockMessage::<ResponseMsg> {
        headers: vec![],
        body: vec!["buffered body data"],
        trailers: vec![],
        _marker: std::marker::PhantomData,
    })
    .await;

    let result = ext_proc.apply_response(&mut response, &RequestCtx::default()).await;

    assert_matches!(result, FilterDecision::Continue);
    assert_eq!(response.headers().get("mutation-in-header-response").unwrap(), "true");
    assert_eq!(response.headers().get("mutation-in-body-response").unwrap(), "true");
    let body_bytes = response.body_mut().collect().await.unwrap().to_bytes();
    assert_eq!(body_bytes, "body data from external processor".as_bytes());
}

#[tokio::test]
#[test_log::test]
async fn test_response_body_buffered_mode_header_mutations_on_body_no_header_response() {
    let mock_state = MockExternalProcessorState::new().add_response(create_body_response::<ResponseMsg>(
        vec![Some(("mutation-in-body-response", "true"))],
        Some("body data from external processor".as_bytes().into()),
        vec![],
        ResponseStatus::Continue as i32,
        None,
    ));

    let (server_addr, _) = start_mock_server(mock_state).await;
    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Skip,
        request_body_mode: BodyProcessingMode::None,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Skip,
        response_body_mode: BodyProcessingMode::Buffered,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.observability_mode = false;
    config.failure_mode_allow = false;
    let mut ext_proc = ExternalProcessor::from(config);

    let mut response = build_response_from_mock(&MockMessage::<ResponseMsg> {
        headers: vec![],
        body: vec!["buffered body data"],
        trailers: vec![],
        _marker: std::marker::PhantomData,
    })
    .await;

    let result = ext_proc.apply_response(&mut response, &RequestCtx::default()).await;

    assert_matches!(result, FilterDecision::Continue);
    assert_eq!(response.headers().get("mutation-in-body-response").unwrap(), "true");
    let body_bytes = response.body_mut().collect().await.unwrap().to_bytes();
    assert_eq!(body_bytes, "body data from external processor".as_bytes());
}

#[tokio::test]
#[test_log::test]
async fn test_response_body_buffered_mode_send_body_without_waiting_for_header_response() {
    let mock_state = MockExternalProcessorState::new()
        .add_response(create_headers_response::<ResponseMsg>(
            vec![Some(("x-stream-processed", "true")), Some(("y-custom-header", "true"))],
            None,
            vec![],
            ResponseStatus::Continue as i32,
            None,
        ))
        .add_response(create_body_response::<ResponseMsg>(
            vec![],
            Some("body data from external processor".as_bytes().into()),
            vec![],
            ResponseStatus::Continue as i32,
            None,
        ));
    let (server_addr, _) = start_mock_server(mock_state).await;
    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Skip,
        request_body_mode: BodyProcessingMode::None,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Send,
        response_body_mode: BodyProcessingMode::Buffered,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.observability_mode = false;
    config.failure_mode_allow = false;
    config.send_body_without_waiting_for_header_response = true;
    let mut ext_proc = ExternalProcessor::from(config);

    let mut response = build_response_from_mock(&MockMessage::<ResponseMsg> {
        headers: vec![],
        body: vec!["buffered body data"],
        trailers: vec![],
        _marker: std::marker::PhantomData,
    })
    .await;

    let result = ext_proc.apply_response(&mut response, &RequestCtx::default()).await;

    assert_matches!(result, FilterDecision::Continue);
    assert_eq!(response.headers().get("x-stream-processed").unwrap(), "true");
    let body_bytes = response.body_mut().collect().await.unwrap().to_bytes();
    assert_eq!(body_bytes, "body data from external processor".as_bytes());
}

#[tokio::test]
#[test_log::test]
async fn test_request_body_streaming_mode_observability() {
    let mock_state =
        MockExternalProcessorState::new().with_observability(true).add_response(create_body_response::<ResponseMsg>(
            vec![],
            Some("body".as_bytes().into()),
            vec![],
            ResponseStatus::Continue as i32,
            None,
        ));

    let (server_addr, _) = start_mock_server(mock_state).await;
    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Skip,
        request_body_mode: BodyProcessingMode::Streamed,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Skip,
        response_body_mode: BodyProcessingMode::None,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.observability_mode = true;
    config.failure_mode_allow = false;
    let mut ext_proc = ExternalProcessor::from(config);

    let mut request =
        build_request_from_mock(&MockMessage::<RequestMsg>::new(vec![], vec!["streaming body data"], vec![])).await;

    let result = ext_proc.apply_request(&mut request, &RequestCtx::default()).await;

    assert_matches!(result, FilterDecision::Continue);
    let body_bytes = std::mem::take(&mut request.body_mut().inner.inner).collect().await.unwrap().to_bytes();
    assert_eq!(body_bytes, "streaming body data".as_bytes());
}

#[tokio::test]
#[test_log::test]
async fn test_immediate_response_response_header() {
    let mock_state = MockExternalProcessorState::new().add_response(create_immediate_response(
        vec![Some(("x-immediate-header", "immediate value"))],
        Some("immediate body".as_bytes().into()),
        302,
    ));
    let (server_addr, _) = start_mock_server(mock_state).await;
    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Skip,
        request_body_mode: BodyProcessingMode::None,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Send,
        response_body_mode: BodyProcessingMode::None,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.disable_immediate_response = false;
    let mut ext_proc = ExternalProcessor::from(config);

    let mut response =
        build_response_from_mock(&MockMessage::<ResponseMsg>::new(vec![], vec!["streaming body data"], vec![])).await;
    let result = ext_proc.apply_response(&mut response, &RequestCtx::default()).await;
    assert_matches!(result, FilterDecision::DirectResponse(dr) => {
        assert_eq!(dr.status(), StatusCode::from_u16(302).unwrap());
        assert_eq!(dr.headers().get("x-immediate-header").unwrap(), "immediate value");
        let (_, body) = dr.into_parts();
        let body_bytes = body.collect().await.unwrap().to_bytes();
        assert_eq!(body_bytes, "immediate body".as_bytes());
    });
}

#[tokio::test]
#[test_log::test]
async fn test_immediate_response_response_header_disable_immediate_response() {
    let mock_state = MockExternalProcessorState::new().add_response(create_immediate_response(
        vec![Some(("x-immediate-header", "immediate value"))],
        Some("immediate body".as_bytes().into()),
        302,
    ));
    let (server_addr, _) = start_mock_server(mock_state).await;
    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Skip,
        request_body_mode: BodyProcessingMode::None,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Send,
        response_body_mode: BodyProcessingMode::None,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.failure_mode_allow = false;
    config.disable_immediate_response = true;
    let mut ext_proc = ExternalProcessor::from(config);

    let mut response = build_response_from_mock(&MockMessage::<ResponseMsg>::new(
        vec![Some(("x-test-header", "original value"))],
        vec!["streaming body data"],
        vec![],
    ))
    .await;
    let result = ext_proc.apply_response(&mut response, &RequestCtx::default()).await;

    assert_matches!(result, FilterDecision::DirectResponse(dr) => {
        assert_eq!(dr.status(), StatusCode::from_u16(500).unwrap());
    });
}

#[tokio::test]
#[test_log::test]
async fn test_immediate_response_response_header_disable_immediate_response_failure_allow_mode() {
    let mock_state = MockExternalProcessorState::new().add_response(create_immediate_response(
        vec![Some(("x-immediate-header", "immediate value"))],
        Some("immediate body".as_bytes().into()),
        302,
    ));
    let (server_addr, _) = start_mock_server(mock_state).await;
    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Skip,
        request_body_mode: BodyProcessingMode::None,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Send,
        response_body_mode: BodyProcessingMode::None,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.failure_mode_allow = true;
    config.disable_immediate_response = true;
    let mut ext_proc = ExternalProcessor::from(config);

    let mut response = build_response_from_mock(&MockMessage::<ResponseMsg>::new(
        vec![Some(("x-test-header", "original value"))],
        vec!["streaming body data"],
        vec![],
    ))
    .await;
    let result = ext_proc.apply_response(&mut response, &RequestCtx::default()).await;

    assert_matches!(result, FilterDecision::Continue);
    let (parts, body) = response.into_parts();
    assert_eq!(parts.headers.get("x-test-header").unwrap(), "original value");
    let body_bytes = body.collect().await;
    assert!(body_bytes.is_ok());
    if let Ok(bytes) = body_bytes {
        assert_eq!(bytes.to_bytes(), "streaming body data".as_bytes());
    }
}

#[tokio::test]
#[test_log::test]
async fn test_response_body_buffered_too_large() {
    let mock_state = MockExternalProcessorState::new()
        .add_response(create_headers_response::<ResponseMsg>(
            vec![],
            None,
            vec![],
            ResponseStatus::Continue as i32,
            None,
        ))
        .add_response(create_body_response::<ResponseMsg>(vec![], None, vec![], ResponseStatus::Continue as i32, None));

    let (server_addr, _) = start_mock_server(mock_state).await;
    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Skip,
        request_body_mode: BodyProcessingMode::None,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Send,
        response_body_mode: BodyProcessingMode::Buffered,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.observability_mode = false;
    config.failure_mode_allow = false;
    let mut ext_proc = ExternalProcessor::from(config);

    let very_large_body = "a".repeat(200 * 1024 * 1024); // 200 MB body
    let boxed_str: Box<str> = very_large_body.clone().into_boxed_str();

    let mut response = build_response_from_mock(&MockMessage::<ResponseMsg> {
        headers: vec![],
        body: vec![Box::leak(boxed_str)],
        trailers: vec![],
        _marker: std::marker::PhantomData,
    })
    .await;
    let result = ext_proc.apply_response(&mut response, &RequestCtx::default()).await;

    assert_matches!(result, FilterDecision::DirectResponse(_));
    match result {
        FilterDecision::DirectResponse(direct_response) => {
            assert_eq!(direct_response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        },
        _ => panic!("Unexpected filter decision"),
    }
}

#[tokio::test]
#[test_log::test]
async fn test_response_multichunk_merged_body_streaming_mode() {
    let mock_state = MockExternalProcessorState::new()
        .add_response(create_headers_response::<ResponseMsg>(
            vec![Some(("x-stream-processed", "true")), Some(("y-custom-header", "true"))],
            None,
            vec![],
            ResponseStatus::Continue as i32,
            None,
        ))
        .add_response(create_body_response::<ResponseMsg>(
            vec![],
            Some("body data from external processor".as_bytes().into()),
            vec![],
            ResponseStatus::Continue as i32,
            None,
        ));
    let (server_addr, _) = start_mock_server(mock_state).await;
    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Skip,
        request_body_mode: BodyProcessingMode::None,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Send,
        response_body_mode: BodyProcessingMode::Streamed,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.observability_mode = false;
    config.failure_mode_allow = false;
    let mut ext_proc = ExternalProcessor::from(config);

    let mut response = build_response_from_mock(&MockMessage::<ResponseMsg> {
        headers: vec![],
        body: vec!["streaming", "body data"],
        trailers: vec![],
        _marker: std::marker::PhantomData,
    })
    .await;
    let result = ext_proc.apply_response(&mut response, &RequestCtx::default()).await;

    assert_matches!(result, FilterDecision::Continue);
    assert_eq!(response.headers().get("x-stream-processed").unwrap(), "true");
    let body_bytes = response.body_mut().collect().await.unwrap().to_bytes();
    assert_eq!(body_bytes, "body data from external processor".as_bytes());
}

#[tokio::test]
#[test_log::test]
async fn test_response_multichunk_merged_body_buffered_mode() {
    let mock_state = MockExternalProcessorState::new()
        .add_response(create_headers_response::<ResponseMsg>(
            vec![Some(("x-stream-processed", "true")), Some(("y-custom-header", "true"))],
            None,
            vec![],
            ResponseStatus::Continue as i32,
            None,
        ))
        .add_response(create_body_response::<ResponseMsg>(
            vec![],
            Some("body data from external processor".as_bytes().into()),
            vec![],
            ResponseStatus::Continue as i32,
            None,
        ));
    let (server_addr, _) = start_mock_server(mock_state).await;
    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Skip,
        request_body_mode: BodyProcessingMode::None,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Send,
        response_body_mode: BodyProcessingMode::Buffered,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.observability_mode = false;
    config.failure_mode_allow = false;
    let mut ext_proc = ExternalProcessor::from(config);

    let mut response = build_response_from_mock(&MockMessage::<ResponseMsg> {
        headers: vec![],
        body: vec!["streaming", "body data"],
        trailers: vec![],
        _marker: std::marker::PhantomData,
    })
    .await;
    let result = ext_proc.apply_response(&mut response, &RequestCtx::default()).await;

    assert_matches!(result, FilterDecision::Continue);
    assert_eq!(response.headers().get("x-stream-processed").unwrap(), "true");
    let body_bytes = response.body_mut().collect().await.unwrap().to_bytes();
    assert_eq!(body_bytes, "body data from external processor".as_bytes());
}

#[tokio::test]
#[test_log::test]
async fn test_response_multichunk_not_merged_body_streaming_mode() {
    let mock_state = MockExternalProcessorState::new()
        .add_response(create_headers_response::<ResponseMsg>(
            vec![Some(("x-stream-processed", "true")), Some(("y-custom-header", "true"))],
            None,
            vec![],
            ResponseStatus::Continue as i32,
            None,
        ))
        .add_response(create_body_response::<ResponseMsg>(
            vec![],
            Some("body data".as_bytes().into()),
            vec![],
            ResponseStatus::Continue as i32,
            None,
        ))
        .add_response(create_body_response::<ResponseMsg>(
            vec![],
            Some("external processor".as_bytes().into()),
            vec![],
            ResponseStatus::Continue as i32,
            None,
        ));
    let (server_addr, _) = start_mock_server(mock_state).await;
    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Skip,
        request_body_mode: BodyProcessingMode::None,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Send,
        response_body_mode: BodyProcessingMode::Streamed,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.observability_mode = false;
    config.failure_mode_allow = false;

    let ext_config = ExternalProcessorConfigExt { frame_merge_limit: 1, frame_merge_window: Duration::from_millis(0) };

    let mut ext_proc = ExternalProcessor::from((config, None, Some(ext_config)));

    let mut response = build_response_from_mock(&MockMessage::<ResponseMsg> {
        headers: vec![],
        body: vec!["streaming", "body"],
        trailers: vec![],
        _marker: std::marker::PhantomData,
    })
    .await;
    let result = ext_proc.apply_response(&mut response, &RequestCtx::default()).await;

    assert_matches!(result, FilterDecision::Continue);
    assert_eq!(response.headers().get("x-stream-processed").unwrap(), "true");

    let body_chunks = to_body_data_chunks(response.body_mut().collect().await.unwrap()).await;
    assert_eq!(
        body_chunks,
        ["body data", "external processor"].into_iter().map(std::convert::Into::into).collect::<Vec<Bytes>>()
    );
}

#[tokio::test]
#[test_log::test]
async fn test_response_multichunk_body_with_mutation() {
    let mock_state = MockExternalProcessorState::new()
        .add_response(create_body_response::<ResponseMsg>(
            vec![],
            Some("CHUNK1".into()),
            vec![],
            ResponseStatus::Continue as i32,
            None,
        ))
        .add_response(create_body_response::<ResponseMsg>(
            vec![],
            Some("CHUNK2".into()),
            vec![],
            ResponseStatus::Continue as i32,
            None,
        ))
        .add_response(create_body_response::<ResponseMsg>(
            vec![],
            Some("CHUNK3".into()),
            vec![],
            ResponseStatus::Continue as i32,
            None,
        ));

    let (server_addr, _) = start_mock_server(mock_state).await;
    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Skip,
        request_body_mode: BodyProcessingMode::None,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Skip,
        response_body_mode: BodyProcessingMode::Streamed,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.observability_mode = false;
    config.failure_mode_allow = false;

    let ext_config = ExternalProcessorConfigExt { frame_merge_limit: 1, frame_merge_window: Duration::from_millis(0) };
    let mut ext_proc = ExternalProcessor::from((config, None, Some(ext_config)));

    let mut response =
        build_response_from_mock(&MockMessage::<ResponseMsg>::new(vec![], vec!["chunk1", "chunk2", "chunk3"], vec![]))
            .await;
    let result = ext_proc.apply_response(&mut response, &RequestCtx::default()).await;
    assert_matches!(result, FilterDecision::Continue);

    let body_chunks = to_body_data_chunks(response.body_mut().collect().await.unwrap()).await;

    assert_eq!(
        body_chunks,
        ["CHUNK1", "CHUNK2", "CHUNK3"].into_iter().map(std::convert::Into::into).collect::<Vec<Bytes>>()
    );
}

#[tokio::test]
#[test_log::test]
async fn test_response_multichunk_body_timeout_failure_mode_allow_true() {
    let mock_state = MockExternalProcessorState::new().add_response(
        create_body_response::<ResponseMsg>(
            vec![],
            Some("modified_chunk".into()),
            vec![],
            ResponseStatus::Continue as i32,
            None,
        )
        .with_delay(Duration::from_secs(10)),
    );
    let (server_addr, _) = start_mock_server(mock_state).await;
    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Skip,
        request_body_mode: BodyProcessingMode::None,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Skip,
        response_body_mode: BodyProcessingMode::Streamed,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.observability_mode = false;
    config.failure_mode_allow = true;

    let ext_config = ExternalProcessorConfigExt { frame_merge_limit: 1, frame_merge_window: Duration::from_millis(0) };
    let mut ext_proc = ExternalProcessor::from((config, None, Some(ext_config)));

    let mut response =
        build_response_from_mock(&MockMessage::<ResponseMsg>::new(vec![], vec!["chunk1", "chunk2", "chunk3"], vec![]))
            .await;
    let result = ext_proc.apply_response(&mut response, &RequestCtx::default()).await;
    assert_matches!(result, FilterDecision::Continue);

    let body_chunks = to_body_data_chunks(response.body_mut().collect().await.unwrap()).await;
    assert_eq!(
        body_chunks,
        ["chunk1", "chunk2", "chunk3"].into_iter().map(std::convert::Into::into).collect::<Vec<Bytes>>()
    );
}

#[tokio::test]
#[test_log::test]
async fn test_response_body_and_trailer_out_of_order() {
    let mock_state = MockExternalProcessorState::new()
        .add_response(
            create_headers_response::<ResponseMsg>(vec![], None, vec![], ResponseStatus::Continue as i32, None)
                .with_delay(Duration::from_millis(500)),
        )
        .add_response(create_body_response::<ResponseMsg>(vec![], None, vec![], ResponseStatus::Continue as i32, None))
        .add_response(create_body_response::<ResponseMsg>(vec![], None, vec![], ResponseStatus::Continue as i32, None))
        .add_response(create_body_response::<ResponseMsg>(vec![], None, vec![], ResponseStatus::Continue as i32, None));

    let (server_addr, _) = start_mock_server(mock_state).await;
    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Skip,
        request_body_mode: BodyProcessingMode::None,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Send,
        response_body_mode: BodyProcessingMode::Streamed,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.observability_mode = false;
    config.failure_mode_allow = false;

    let ext_config = ExternalProcessorConfigExt { frame_merge_limit: 1, frame_merge_window: Duration::from_millis(0) };

    let mut ext_proc = ExternalProcessor::from((config, None, Some(ext_config)));

    let mut response = build_response_from_mock(&MockMessage::<ResponseMsg> {
        headers: vec![Some(("content-type", "application/json"))],
        body: vec!["chunk1", "chunk2", "chunk3"],
        trailers: vec![Some(("x-custom-trailer", "original-value"))],
        _marker: std::marker::PhantomData,
    })
    .await;

    let result = ext_proc.apply_response(&mut response, &RequestCtx::default()).await;
    assert_matches!(result, FilterDecision::Continue);

    let (_, body) = response.into_parts();

    let expected_trailers = http::HeaderMap::from_iter(vec![(
        http::HeaderName::from_static("x-custom-trailer"),
        http::HeaderValue::from_static("original-value"),
    )]);
    let expected_data = &["chunk1".into(), "chunk2".into(), "chunk3".into()];

    assert_matches!(assert_body_frames(body, expected_data, Some(expected_trailers)).await, Ok(()));
}

#[tokio::test]
#[test_log::test]
async fn test_response_multichunk_body_no_truncate_body() {
    let mock_state = MockExternalProcessorState::new()
        .add_response(create_body_response::<ResponseMsg>(
            vec![],
            Some("CHUNK1".into()),
            vec![],
            ResponseStatus::Continue as i32,
            None,
        ))
        .add_response(create_body_response::<ResponseMsg>(
            vec![],
            Some("CHUNK2".into()),
            vec![],
            ResponseStatus::Continue as i32,
            None,
        ))
        .add_response(create_body_response::<ResponseMsg>(
            vec![],
            Some("CHUNK3".into()),
            vec![],
            ResponseStatus::Continue as i32,
            None,
        ));

    let (server_addr, _) = start_mock_server(mock_state).await;
    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Skip,
        request_body_mode: BodyProcessingMode::None,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Skip,
        response_body_mode: BodyProcessingMode::Streamed,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.observability_mode = false;
    config.failure_mode_allow = false;

    let ext_config = ExternalProcessorConfigExt { frame_merge_limit: 1, frame_merge_window: Duration::from_millis(0) };
    let mut ext_proc = ExternalProcessor::from((config, None, Some(ext_config)));

    let mut response =
        build_response_from_mock(&MockMessage::<ResponseMsg>::new(vec![], vec!["chunk1", "chunk2", "chunk3"], vec![]))
            .await;
    let result = ext_proc.apply_response(&mut response, &RequestCtx::default()).await;
    assert_matches!(result, FilterDecision::Continue);

    let body_chunks = to_body_data_chunks(response.body_mut().collect().await.unwrap()).await;

    assert_eq!(
        body_chunks,
        ["CHUNK1", "CHUNK2", "CHUNK3"].into_iter().map(std::convert::Into::into).collect::<Vec<Bytes>>()
    );
}

#[tokio::test]
#[test_log::test]
async fn test_request_trailer_timeout() {
    let mock_state = MockExternalProcessorState::new()
        .add_response(create_headers_response::<RequestMsg>(
            vec![],
            None,
            vec![],
            ResponseStatus::Continue as i32,
            None,
        ))
        .add_response(create_body_response::<RequestMsg>(vec![], None, vec![], ResponseStatus::Continue as i32, None))
        .add_response(create_trailers_response::<RequestMsg>(vec![]).with_delay(Duration::from_secs(10)));

    let (server_addr, _) = start_mock_server(mock_state).await;
    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Send,
        request_body_mode: BodyProcessingMode::Streamed,
        request_trailer_mode: TrailerProcessingMode::Send,
        response_header_mode: HeaderProcessingMode::Skip,
        response_body_mode: BodyProcessingMode::None,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.observability_mode = false;
    config.failure_mode_allow = false;
    let mut ext_proc = ExternalProcessor::from(config);

    let mut request = build_request_from_mock(&MockMessage::<RequestMsg> {
        headers: vec![Some(("content-type", "application/json"))],
        body: vec!["body"],
        trailers: vec![Some(("x-custom-trailer", "original-value"))],
        _marker: std::marker::PhantomData,
    })
    .await;

    let result = ext_proc.apply_request(&mut request, &RequestCtx::default()).await;
    // Trailer processing happens in the body stream, so the initial result is Continue
    // but the body collection will fail due to the trailer timeout
    assert_matches!(result, FilterDecision::Continue);

    let body_result = std::mem::take(&mut request.body_mut().inner.inner).collect().await;
    body_result.unwrap_err();
}

#[tokio::test]
#[test_log::test]
async fn test_request_trailer_timeout_failure_mode_allow_true() {
    let mock_state = MockExternalProcessorState::new()
        .add_response(create_headers_response::<RequestMsg>(
            vec![],
            None,
            vec![],
            ResponseStatus::Continue as i32,
            None,
        ))
        .add_response(create_body_response::<RequestMsg>(vec![], None, vec![], ResponseStatus::Continue as i32, None))
        .add_response(create_trailers_response::<RequestMsg>(vec![]).with_delay(Duration::from_secs(10)));

    let (server_addr, _) = start_mock_server(mock_state).await;
    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Send,
        request_body_mode: BodyProcessingMode::Buffered,
        request_trailer_mode: TrailerProcessingMode::Send,
        response_header_mode: HeaderProcessingMode::Skip,
        response_body_mode: BodyProcessingMode::None,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.observability_mode = false;
    config.failure_mode_allow = true;
    let mut ext_proc = ExternalProcessor::from(config);

    let mut request = build_request_from_mock(&MockMessage::<RequestMsg> {
        headers: vec![Some(("content-type", "application/json"))],
        body: vec!["body"],
        trailers: vec![Some(("x-custom-trailer", "original-value"))],
        _marker: std::marker::PhantomData,
    })
    .await;

    let result = ext_proc.apply_request(&mut request, &RequestCtx::default()).await;
    assert_matches!(result, FilterDecision::Continue);

    let body = std::mem::take(&mut request.body_mut().inner.inner).collect().await.unwrap();
    let trailers = body.trailers().cloned();
    assert!(trailers.is_some());
    let trailers = trailers.unwrap();
    assert_eq!(trailers.get("x-custom-trailer").unwrap(), "original-value");
}

#[tokio::test]
#[test_log::test]
async fn test_response_trailer_timeout() {
    let mock_state = MockExternalProcessorState::new()
        .add_response(create_headers_response::<ResponseMsg>(
            vec![],
            None,
            vec![],
            ResponseStatus::Continue as i32,
            None,
        ))
        .add_response(create_body_response::<ResponseMsg>(vec![], None, vec![], ResponseStatus::Continue as i32, None))
        .add_response(create_trailers_response::<ResponseMsg>(vec![]).with_delay(Duration::from_secs(10)));

    let (server_addr, _) = start_mock_server(mock_state).await;
    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Skip,
        request_body_mode: BodyProcessingMode::None,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Send,
        response_body_mode: BodyProcessingMode::Buffered,
        response_trailer_mode: TrailerProcessingMode::Send,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.observability_mode = false;
    config.failure_mode_allow = false;
    let mut ext_proc = ExternalProcessor::from(config);

    let mut response = build_response_from_mock(&MockMessage::<ResponseMsg> {
        headers: vec![Some(("content-type", "application/json"))],
        body: vec!["body"],
        trailers: vec![Some(("x-custom-trailer", "original-value"))],
        _marker: std::marker::PhantomData,
    })
    .await;

    let result = ext_proc.apply_response(&mut response, &RequestCtx::default()).await;
    // Trailer processing happens in the body stream, so the initial result is Continue
    // but the body collection will fail due to the trailer timeout
    assert_matches!(result, FilterDecision::Continue);

    let (_, body) = response.into_parts();
    let body_result = body.collect().await;
    body_result.unwrap_err();
}

#[tokio::test]
#[test_log::test]
async fn test_response_trailer_timeout_failure_mode_allow_true() {
    let mock_state = MockExternalProcessorState::new()
        .add_response(create_headers_response::<ResponseMsg>(
            vec![],
            None,
            vec![],
            ResponseStatus::Continue as i32,
            None,
        ))
        .add_response(create_body_response::<ResponseMsg>(vec![], None, vec![], ResponseStatus::Continue as i32, None))
        .add_response(create_trailers_response::<ResponseMsg>(vec![]).with_delay(Duration::from_secs(10)));

    let (server_addr, _) = start_mock_server(mock_state).await;
    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Skip,
        request_body_mode: BodyProcessingMode::None,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Send,
        response_body_mode: BodyProcessingMode::Buffered,
        response_trailer_mode: TrailerProcessingMode::Send,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.observability_mode = false;
    config.failure_mode_allow = true;
    let mut ext_proc = ExternalProcessor::from(config);

    let mut response = build_response_from_mock(&MockMessage::<ResponseMsg> {
        headers: vec![Some(("content-type", "application/json"))],
        body: vec!["body"],
        trailers: vec![Some(("x-custom-trailer", "original-value"))],
        _marker: std::marker::PhantomData,
    })
    .await;

    let result = ext_proc.apply_response(&mut response, &RequestCtx::default()).await;
    assert_matches!(result, FilterDecision::Continue);

    let (_, body) = response.into_parts();
    let collected = body.collect().await.unwrap();
    let trailers = collected.trailers().cloned();
    assert!(trailers.is_some());
    let trailers = trailers.unwrap();
    assert_eq!(trailers.get("x-custom-trailer").unwrap(), "original-value");
}

#[tokio::test]
#[test_log::test]
async fn test_immediate_response_request_body() {
    let mock_state = MockExternalProcessorState::new()
        .add_response(create_headers_response::<RequestMsg>(
            vec![],
            None,
            vec![],
            ResponseStatus::Continue as i32,
            None,
        ))
        .add_response(create_immediate_response(
            vec![Some(("x-immediate-header", "immediate value"))],
            Some("immediate body from body phase".as_bytes().into()),
            403,
        ));

    let (server_addr, _) = start_mock_server(mock_state).await;
    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Send,
        request_body_mode: BodyProcessingMode::Buffered,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Skip,
        response_body_mode: BodyProcessingMode::None,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.disable_immediate_response = false;
    let mut ext_proc = ExternalProcessor::from(config);

    let mut request = build_request_from_mock(&MockMessage::<RequestMsg> {
        headers: vec![Some(("content-type", "application/json"))],
        body: vec!["request body"],
        trailers: vec![],
        _marker: std::marker::PhantomData,
    })
    .await;
    let result = ext_proc.apply_request(&mut request, &RequestCtx::default()).await;
    assert_matches!(result, FilterDecision::DirectResponse(dr) => {
        assert_eq!(dr.status(), StatusCode::from_u16(403).unwrap());
        assert_eq!(dr.headers().get("x-immediate-header").unwrap(), "immediate value");
        let (_, body) = dr.into_parts();
        let body_bytes = body.collect().await.unwrap().to_bytes();
        assert_eq!(body_bytes, "immediate body from body phase".as_bytes());
    });
}

#[tokio::test]
#[test_log::test]
async fn test_immediate_response_response_body() {
    let mock_state = MockExternalProcessorState::new()
        .add_response(create_headers_response::<ResponseMsg>(
            vec![],
            None,
            vec![],
            ResponseStatus::Continue as i32,
            None,
        ))
        .add_response(create_immediate_response(
            vec![Some(("x-immediate-header", "immediate value"))],
            Some("immediate body from body phase".as_bytes().into()),
            403,
        ));
    let (server_addr, _) = start_mock_server(mock_state).await;
    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Skip,
        request_body_mode: BodyProcessingMode::None,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Send,
        response_body_mode: BodyProcessingMode::Buffered,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.disable_immediate_response = false;
    let mut ext_proc = ExternalProcessor::from(config);

    let mut response = build_response_from_mock(&MockMessage::<ResponseMsg> {
        headers: vec![Some(("content-type", "application/json"))],
        body: vec!["response body"],
        trailers: vec![],
        _marker: std::marker::PhantomData,
    })
    .await;
    let result = ext_proc.apply_response(&mut response, &RequestCtx::default()).await;
    assert_matches!(result, FilterDecision::DirectResponse(dr) => {
        assert_eq!(dr.status(), StatusCode::from_u16(403).unwrap());
        assert_eq!(dr.headers().get("x-immediate-header").unwrap(), "immediate value");
        let (_, body) = dr.into_parts();
        let body_bytes = body.collect().await.unwrap().to_bytes();
        assert_eq!(body_bytes, "immediate body from body phase".as_bytes());
    });
}

fn create_clear_body_mutation() -> BodyMutation {
    BodyMutation { mutation: Some(Mutation::ClearBody(true)) }
}

fn create_body_response_with_clear_body<M: MessageKind>(
    headers: Vec<Option<(&str, &str)>>,
    status: i32,
) -> MockProcessingResponse {
    let header_mutation = transform(headers).and_then(|hdrs| create_header_mutation(hdrs));

    let body_response = BodyResponse {
        response: Some(CommonResponse {
            status,
            header_mutation,
            body_mutation: Some(create_clear_body_mutation()),
            trailers: None,
            clear_route_cache: false,
        }),
    };

    let response = if M::IS_RESPONSE {
        Some(ProcessingResponseType::ResponseBody(body_response))
    } else {
        Some(ProcessingResponseType::RequestBody(body_response))
    };

    ProcessingResponse {
        response,
        mode_override: None,
        dynamic_metadata: None,
        override_message_timeout: None,
        request_drain: false,
    }
    .into()
}

#[tokio::test]
#[test_log::test]
async fn test_request_body_clear_body_mutation() {
    let mock_state = MockExternalProcessorState::new()
        .add_response(create_headers_response::<RequestMsg>(
            vec![],
            None,
            vec![],
            ResponseStatus::Continue as i32,
            None,
        ))
        .add_response(create_body_response_with_clear_body::<RequestMsg>(vec![], ResponseStatus::Continue as i32));
    let (server_addr, _) = start_mock_server(mock_state).await;
    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Send,
        request_body_mode: BodyProcessingMode::Buffered,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Skip,
        response_body_mode: BodyProcessingMode::None,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.observability_mode = false;
    config.failure_mode_allow = false;
    let mut ext_proc = ExternalProcessor::from(config);

    let mut request = build_request_from_mock(&MockMessage::<RequestMsg> {
        headers: vec![Some(("content-type", "application/json"))],
        body: vec!["original body content that should be cleared"],
        trailers: vec![],
        _marker: std::marker::PhantomData,
    })
    .await;

    let result = ext_proc.apply_request(&mut request, &RequestCtx::default()).await;

    assert_matches!(result, FilterDecision::Continue);
    let body_bytes = std::mem::take(&mut request.body_mut().inner.inner).collect().await.unwrap().to_bytes();
    assert_eq!(body_bytes, Bytes::new());
}

#[tokio::test]
#[test_log::test]
async fn test_response_body_clear_body_mutation() {
    let mock_state = MockExternalProcessorState::new()
        .add_response(create_headers_response::<ResponseMsg>(
            vec![],
            None,
            vec![],
            ResponseStatus::Continue as i32,
            None,
        ))
        .add_response(create_body_response_with_clear_body::<ResponseMsg>(vec![], ResponseStatus::Continue as i32));
    let (server_addr, _) = start_mock_server(mock_state).await;
    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Skip,
        request_body_mode: BodyProcessingMode::None,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Send,
        response_body_mode: BodyProcessingMode::Buffered,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.observability_mode = false;
    config.failure_mode_allow = false;
    let mut ext_proc = ExternalProcessor::from(config);

    let mut response = build_response_from_mock(&MockMessage::<ResponseMsg> {
        headers: vec![Some(("content-type", "application/json"))],
        body: vec!["original body content that should be cleared"],
        trailers: vec![],
        _marker: std::marker::PhantomData,
    })
    .await;

    let result = ext_proc.apply_response(&mut response, &RequestCtx::default()).await;

    assert_matches!(result, FilterDecision::Continue);
    let body_bytes = response.body_mut().collect().await.unwrap().to_bytes();
    assert_eq!(body_bytes, Bytes::new());
}

fn create_headers_response_with_clear_body<M: MessageKind>(
    headers: Vec<Option<(&str, &str)>>,
    status: i32,
) -> MockProcessingResponse {
    let header_mutation = transform(headers).and_then(|hdrs| create_header_mutation(hdrs));

    let header_response = HeadersResponse {
        response: Some(CommonResponse {
            status,
            header_mutation,
            body_mutation: Some(create_clear_body_mutation()),
            trailers: None,
            clear_route_cache: false,
        }),
    };

    let response = if M::IS_RESPONSE {
        Some(ProcessingResponseType::ResponseHeaders(header_response))
    } else {
        Some(ProcessingResponseType::RequestHeaders(header_response))
    };

    ProcessingResponse {
        response,
        mode_override: None,
        dynamic_metadata: None,
        override_message_timeout: None,
        request_drain: false,
    }
    .into()
}

#[tokio::test]
#[test_log::test]
async fn test_request_header_clear_body_mutation_continue_and_replace() {
    let mock_state =
        MockExternalProcessorState::new().add_response(create_headers_response_with_clear_body::<RequestMsg>(
            vec![Some(("x-body-cleared", "true"))],
            ResponseStatus::ContinueAndReplace as i32,
        ));
    let (server_addr, _) = start_mock_server(mock_state).await;
    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Send,
        request_body_mode: BodyProcessingMode::Buffered,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Skip,
        response_body_mode: BodyProcessingMode::None,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.observability_mode = false;
    config.failure_mode_allow = false;
    let mut ext_proc = ExternalProcessor::from(config);

    let mut request = build_request_from_mock(&MockMessage::<RequestMsg> {
        headers: vec![Some(("content-type", "application/json"))],
        body: vec!["original body content that should be cleared"],
        trailers: vec![],
        _marker: std::marker::PhantomData,
    })
    .await;

    let result = ext_proc.apply_request(&mut request, &RequestCtx::default()).await;

    assert_matches!(result, FilterDecision::Continue);
    assert_eq!(request.headers().get("x-body-cleared").unwrap(), "true");
    let body_bytes = std::mem::take(&mut request.body_mut().inner.inner).collect().await.unwrap().to_bytes();
    assert_eq!(body_bytes, Bytes::new());
}

#[tokio::test]
#[test_log::test]
async fn test_response_header_clear_body_mutation_continue_and_replace() {
    let mock_state =
        MockExternalProcessorState::new().add_response(create_headers_response_with_clear_body::<ResponseMsg>(
            vec![Some(("x-body-cleared", "true"))],
            ResponseStatus::ContinueAndReplace as i32,
        ));
    let (server_addr, _) = start_mock_server(mock_state).await;
    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Skip,
        request_body_mode: BodyProcessingMode::None,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Send,
        response_body_mode: BodyProcessingMode::Buffered,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.observability_mode = false;
    config.failure_mode_allow = false;
    let mut ext_proc = ExternalProcessor::from(config);

    let mut response = build_response_from_mock(&MockMessage::<ResponseMsg> {
        headers: vec![Some(("content-type", "application/json"))],
        body: vec!["original body content that should be cleared"],
        trailers: vec![],
        _marker: std::marker::PhantomData,
    })
    .await;

    let result = ext_proc.apply_response(&mut response, &RequestCtx::default()).await;

    assert_matches!(result, FilterDecision::Continue);
    assert_eq!(response.headers().get("x-body-cleared").unwrap(), "true");
    let body_bytes = response.body_mut().collect().await.unwrap().to_bytes();
    assert_eq!(body_bytes, Bytes::new());
}

#[tokio::test]
#[test_log::test]
async fn test_response_full_duplex_streaming_mode_header_only() {
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    let mock_state = MockExternalProcessorState::new()
        .add_response(create_headers_response::<ResponseMsg>(
            vec![Some(("x-stream-processed", "true")), Some(("y-custom-header", "true"))],
            None,
            vec![],
            ResponseStatus::Continue as i32,
            // even though we will stream the body, no body modification is
            // performed in the headers response so no need to set the flag
            None,
        ))
        .with_sender(tx);

    let (server_addr, _) = start_mock_server(mock_state.clone()).await;

    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Skip,
        request_body_mode: BodyProcessingMode::None,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Send,
        response_body_mode: BodyProcessingMode::FullDuplexStreamed,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.observability_mode = false;
    config.failure_mode_allow = false;
    let mut ext_proc = ExternalProcessor::from(config);

    let mut response = build_response_from_mock(&MockMessage::<ResponseMsg> {
        headers: vec![],
        body: vec![],
        trailers: vec![],
        _marker: std::marker::PhantomData,
    })
    .await;

    let result = ext_proc.apply_response(&mut response, &RequestCtx::default()).await;

    assert_matches!(result, FilterDecision::Continue);
    assert_eq!(response.headers().get("x-stream-processed").unwrap(), "true");

    mock_state.token.cancel();

    if let Some(state) = rx.recv().await {
        assert_matches!(state.last_end_of_stream, Some(true), "Expected end of stream true");
    }
}

#[tokio::test]
#[test_log::test]
async fn test_response_full_duplex_streaming_mode_with_body() {
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    let mock_state = MockExternalProcessorState::new()
        .add_response(create_headers_response::<ResponseMsg>(
            vec![Some(("x-stream-processed", "true")), Some(("y-custom-header", "true"))],
            None,
            vec![],
            ResponseStatus::Continue as i32,
            // even though we will stream the body, no body modification is
            // performed in the headers response so no need to set the flag
            None,
        ))
        .add_response(create_body_response::<ResponseMsg>(
            vec![],
            Some("body data from external processor".as_bytes().into()),
            vec![],
            ResponseStatus::Continue as i32,
            // the response for the streaming body is still just a normal Body
            // no need to set end_of_stream, which is only for FULL_DUPLEX_STREAMED
            None,
        ))
        .with_sender(tx);

    let (server_addr, _) = start_mock_server(mock_state.clone()).await;

    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Skip,
        request_body_mode: BodyProcessingMode::None,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Send,
        response_body_mode: BodyProcessingMode::FullDuplexStreamed,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.observability_mode = false;
    config.failure_mode_allow = false;
    let mut ext_proc = ExternalProcessor::from(config);

    let mut response = build_response_from_mock(&MockMessage::<ResponseMsg> {
        headers: vec![],
        body: vec!["this is the body"],
        trailers: vec![],
        _marker: std::marker::PhantomData,
    })
    .await;

    let result = ext_proc.apply_response(&mut response, &RequestCtx::default()).await;

    assert_matches!(result, FilterDecision::Continue);
    assert_eq!(response.headers().get("x-stream-processed").unwrap(), "true");

    let body_bytes = response.body_mut().collect().await.unwrap().to_bytes();
    assert_eq!(body_bytes, "body data from external processor".as_bytes());

    mock_state.token.cancel();

    if let Some(state) = rx.recv().await {
        assert_matches!(state.last_end_of_stream, Some(true), "Expected end of stream true");
    }
}

#[tokio::test]
#[test_log::test]
async fn test_response_full_duplex_streaming_mode_with_body_and_trailers() {
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    let mock_state = MockExternalProcessorState::new()
        .add_response(create_headers_response::<ResponseMsg>(
            vec![Some(("x-stream-processed", "true")), Some(("y-custom-header", "true"))],
            None,
            vec![],
            ResponseStatus::Continue as i32,
            // even though we will stream the body, no body modification is
            // performed in the headers response so no need to set the flag
            None,
        ))
        .add_response(create_body_response::<ResponseMsg>(
            vec![],
            Some("body data from external processor".as_bytes().into()),
            vec![],
            ResponseStatus::Continue as i32,
            // the response for the streaming body is still just a normal Body
            // no need to set end_of_stream, which is only for FULL_DUPLEX_STREAMED
            None,
        ))
        .add_response(create_trailers_response::<ResponseMsg>(vec![]))
        .with_sender(tx);

    let (server_addr, _) = start_mock_server(mock_state.clone()).await;

    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Skip,
        request_body_mode: BodyProcessingMode::None,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Send,
        response_body_mode: BodyProcessingMode::FullDuplexStreamed,
        response_trailer_mode: TrailerProcessingMode::Send,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.observability_mode = false;
    config.failure_mode_allow = false;
    let mut ext_proc = ExternalProcessor::from(config);

    let mut response = build_response_from_mock(&MockMessage::<ResponseMsg> {
        headers: vec![],
        body: vec!["this is the body"],
        trailers: vec![Some(("x-custom-trailer", "original-value"))],
        _marker: std::marker::PhantomData,
    })
    .await;

    let result = ext_proc.apply_response(&mut response, &RequestCtx::default()).await;

    assert_matches!(result, FilterDecision::Continue);
    assert_eq!(response.headers().get("x-stream-processed").unwrap(), "true");

    let body_bytes = response.body_mut().collect().await.unwrap().to_bytes();
    assert_eq!(body_bytes, "body data from external processor".as_bytes());

    mock_state.token.cancel();

    if let Some(state) = rx.recv().await {
        assert_matches!(state.last_end_of_stream, Some(false), "Expected end of stream false");
    }
}

#[tokio::test]
#[test_log::test]
async fn test_response_buffered_mode_header_only() {
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    let mock_state = MockExternalProcessorState::new()
        .add_response(create_headers_response::<ResponseMsg>(
            vec![Some(("x-stream-processed", "true")), Some(("y-custom-header", "true"))],
            None,
            vec![],
            ResponseStatus::Continue as i32,
            // even though we will stream the body, no body modification is
            // performed in the headers response so no need to set the flag
            None,
        ))
        .with_sender(tx);

    let (server_addr, _) = start_mock_server(mock_state.clone()).await;

    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Skip,
        request_body_mode: BodyProcessingMode::None,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Send,
        response_body_mode: BodyProcessingMode::Buffered,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.observability_mode = false;
    config.failure_mode_allow = false;
    let mut ext_proc = ExternalProcessor::from(config);

    let mut response = build_response_from_mock(&MockMessage::<ResponseMsg> {
        headers: vec![],
        body: vec![],
        trailers: vec![],
        _marker: std::marker::PhantomData,
    })
    .await;

    let result = ext_proc.apply_response(&mut response, &RequestCtx::default()).await;

    assert_matches!(result, FilterDecision::Continue);
    assert_eq!(response.headers().get("x-stream-processed").unwrap(), "true");

    mock_state.token.cancel();

    if let Some(state) = rx.recv().await {
        assert_matches!(state.last_end_of_stream, Some(true), "Expected end of stream true");
    }
}

#[tokio::test]
#[test_log::test]
async fn test_response_buffered_mode_with_body() {
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    let mock_state = MockExternalProcessorState::new()
        .add_response(create_headers_response::<ResponseMsg>(
            vec![Some(("x-stream-processed", "true")), Some(("y-custom-header", "true"))],
            None,
            vec![],
            ResponseStatus::Continue as i32,
            // even though we will stream the body, no body modification is
            // performed in the headers response so no need to set the flag
            None,
        ))
        .add_response(create_body_response::<ResponseMsg>(
            vec![],
            Some("body data from external processor".as_bytes().into()),
            vec![],
            ResponseStatus::Continue as i32,
            // the response for the streaming body is still just a normal Body
            // no need to set end_of_stream, which is only for FULL_DUPLEX_STREAMED
            None,
        ))
        .with_sender(tx);

    let (server_addr, _) = start_mock_server(mock_state.clone()).await;

    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Skip,
        request_body_mode: BodyProcessingMode::None,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Send,
        response_body_mode: BodyProcessingMode::Buffered,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.observability_mode = false;
    config.failure_mode_allow = false;
    let mut ext_proc = ExternalProcessor::from(config);

    let mut response = build_response_from_mock(&MockMessage::<ResponseMsg> {
        headers: vec![],
        body: vec!["this is the body"],
        trailers: vec![],
        _marker: std::marker::PhantomData,
    })
    .await;

    let result = ext_proc.apply_response(&mut response, &RequestCtx::default()).await;

    assert_matches!(result, FilterDecision::Continue);
    assert_eq!(response.headers().get("x-stream-processed").unwrap(), "true");

    let body_bytes = response.body_mut().collect().await.unwrap().to_bytes();
    assert_eq!(body_bytes, "body data from external processor".as_bytes());

    mock_state.token.cancel();

    if let Some(state) = rx.recv().await {
        assert_matches!(state.last_end_of_stream, Some(true), "Expected end of stream true");
    }
}

#[tokio::test]
#[test_log::test]
async fn test_response_buffered_mode_with_body_and_trailers() {
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    let mock_state = MockExternalProcessorState::new()
        .add_response(create_headers_response::<ResponseMsg>(
            vec![Some(("x-stream-processed", "true")), Some(("y-custom-header", "true"))],
            None,
            vec![],
            ResponseStatus::Continue as i32,
            // even though we will stream the body, no body modification is
            // performed in the headers response so no need to set the flag
            None,
        ))
        .add_response(create_body_response::<ResponseMsg>(
            vec![],
            Some("body data from external processor".as_bytes().into()),
            vec![],
            ResponseStatus::Continue as i32,
            // the response for the streaming body is still just a normal Body
            // no need to set end_of_stream, which is only for FULL_DUPLEX_STREAMED
            None,
        ))
        .add_response(create_trailers_response::<ResponseMsg>(vec![]))
        .with_sender(tx);

    let (server_addr, _) = start_mock_server(mock_state.clone()).await;

    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Skip,
        request_body_mode: BodyProcessingMode::None,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Send,
        response_body_mode: BodyProcessingMode::Buffered,
        response_trailer_mode: TrailerProcessingMode::Send,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.observability_mode = false;
    config.failure_mode_allow = false;
    let mut ext_proc = ExternalProcessor::from(config);

    let mut response = build_response_from_mock(&MockMessage::<ResponseMsg> {
        headers: vec![],
        body: vec!["this is the body"],
        trailers: vec![Some(("x-custom-trailer", "original-value"))],
        _marker: std::marker::PhantomData,
    })
    .await;

    let result = ext_proc.apply_response(&mut response, &RequestCtx::default()).await;

    assert_matches!(result, FilterDecision::Continue);
    assert_eq!(response.headers().get("x-stream-processed").unwrap(), "true");

    let body_bytes = response.body_mut().collect().await.unwrap().to_bytes();
    assert_eq!(body_bytes, "body data from external processor".as_bytes());

    mock_state.token.cancel();

    if let Some(state) = rx.recv().await {
        assert_matches!(state.last_end_of_stream, Some(false), "Expected end of stream false");
    }
}

#[tokio::test]
#[test_log::test]
async fn test_request_body_timeout_scenarios() {
    let scenarios = vec![
        (HeaderProcessingMode::Send, BodyProcessingMode::Buffered),
        (HeaderProcessingMode::Skip, BodyProcessingMode::Buffered),
        (HeaderProcessingMode::Send, BodyProcessingMode::Streamed),
        (HeaderProcessingMode::Skip, BodyProcessingMode::Streamed),
    ];

    for (idx, (header_mode, body_mode)) in scenarios.into_iter().enumerate() {
        println!("Testing Request Body Timeout Scenario {idx}: Header={header_mode:?}, Body={body_mode:?}");

        let mut mock_state = MockExternalProcessorState::new();
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        mock_state = mock_state.with_sender(tx);

        if header_mode == HeaderProcessingMode::Send {
            mock_state = mock_state.add_response(create_headers_response::<RequestMsg>(
                vec![],
                None,
                vec![],
                ResponseStatus::Continue as i32,
                None,
            ));
        }

        mock_state = mock_state.add_response(
            create_body_response::<RequestMsg>(vec![], None, vec![], ResponseStatus::Continue as i32, None)
                .with_delay(Duration::from_secs(10)),
        );

        let (server_addr, _) = start_mock_server(mock_state).await;

        let processing_mode = ProcessingMode {
            request_header_mode: header_mode,
            request_body_mode: body_mode,
            request_trailer_mode: TrailerProcessingMode::Skip,
            response_header_mode: HeaderProcessingMode::Skip,
            response_body_mode: BodyProcessingMode::None,
            response_trailer_mode: TrailerProcessingMode::Skip,
        };

        let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
        config.failure_mode_allow = false;
        config.message_timeout = Some(Duration::from_millis(2500));
        let mut ext_proc = ExternalProcessor::from(config);

        let mut request = build_request_from_mock(&MockMessage::<RequestMsg>::new(
            vec![Some(("content-type", "application/json"))],
            vec!["body chunk 1", "body chunk 2"],
            vec![],
        ))
        .await;

        let result = ext_proc.apply_request(&mut request, &RequestCtx::default()).await;

        if body_mode == BodyProcessingMode::Buffered {
            assert_matches!(result, FilterDecision::DirectResponse(dr) => {
                assert_eq!(dr.status(), http::StatusCode::GATEWAY_TIMEOUT);
            });
        } else {
            assert_matches!(result, FilterDecision::Continue);
            let body_result = std::mem::take(&mut request.body_mut().inner.inner).collect().await;
            assert!(body_result.is_err(), "Expected body to fail due to timeout in Streamed mode");
        }
    }
}

#[tokio::test]
#[test_log::test]
async fn test_request_trailer_timeout_scenarios() {
    let scenarios = vec![
        (HeaderProcessingMode::Send, BodyProcessingMode::Buffered),
        (HeaderProcessingMode::Skip, BodyProcessingMode::Buffered),
        (HeaderProcessingMode::Send, BodyProcessingMode::Streamed),
        (HeaderProcessingMode::Skip, BodyProcessingMode::Streamed),
    ];

    for (idx, (header_mode, body_mode)) in scenarios.into_iter().enumerate() {
        println!("Testing Request Trailer Timeout Scenario {idx}: Header={header_mode:?}, Body={body_mode:?}");

        let mut mock_state = MockExternalProcessorState::new();
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        mock_state = mock_state.with_sender(tx);

        if header_mode == HeaderProcessingMode::Send {
            mock_state = mock_state.add_response(create_headers_response::<RequestMsg>(
                vec![],
                None,
                vec![],
                ResponseStatus::Continue as i32,
                None,
            ));
        }
        if body_mode != BodyProcessingMode::None {
            mock_state = mock_state.add_response(create_body_response::<RequestMsg>(
                vec![],
                None,
                vec![],
                ResponseStatus::Continue as i32,
                None,
            ));
        }

        mock_state =
            mock_state.add_response(create_trailers_response::<RequestMsg>(vec![]).with_delay(Duration::from_secs(10)));

        let (server_addr, _) = start_mock_server(mock_state).await;

        let processing_mode = ProcessingMode {
            request_header_mode: header_mode,
            request_body_mode: body_mode,
            request_trailer_mode: TrailerProcessingMode::Send,
            response_header_mode: HeaderProcessingMode::Skip,
            response_body_mode: BodyProcessingMode::None,
            response_trailer_mode: TrailerProcessingMode::Skip,
        };

        let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
        config.failure_mode_allow = false;
        config.message_timeout = Some(Duration::from_millis(2500));
        let mut ext_proc = ExternalProcessor::from(config);

        let mut request = build_request_from_mock(&MockMessage::<RequestMsg> {
            headers: vec![Some(("content-type", "application/json"))],
            body: vec!["body chunk"],
            trailers: vec![Some(("x-trailer", "value"))],
            _marker: std::marker::PhantomData,
        })
        .await;

        let result = ext_proc.apply_request(&mut request, &RequestCtx::default()).await;

        assert_matches!(result, FilterDecision::Continue);
        let body_result = std::mem::take(&mut request.body_mut().inner.inner).collect().await;
        assert!(body_result.is_err(), "Expected body collect to fail due to trailer timeout");
    }
}

#[tokio::test]
#[test_log::test]
async fn test_response_body_timeout_scenarios() {
    let scenarios = vec![
        (HeaderProcessingMode::Send, BodyProcessingMode::Buffered),
        (HeaderProcessingMode::Skip, BodyProcessingMode::Buffered),
        (HeaderProcessingMode::Send, BodyProcessingMode::Streamed),
        (HeaderProcessingMode::Skip, BodyProcessingMode::Streamed),
    ];

    for (idx, (header_mode, body_mode)) in scenarios.into_iter().enumerate() {
        println!("Testing Response Body Timeout Scenario {idx}: Header={header_mode:?}, Body={body_mode:?}");

        let mut mock_state = MockExternalProcessorState::new();
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        mock_state = mock_state.with_sender(tx);

        if header_mode == HeaderProcessingMode::Send {
            mock_state = mock_state.add_response(create_headers_response::<ResponseMsg>(
                vec![],
                None,
                vec![],
                ResponseStatus::Continue as i32,
                None,
            ));
        }

        mock_state = mock_state.add_response(
            create_body_response::<ResponseMsg>(vec![], None, vec![], ResponseStatus::Continue as i32, None)
                .with_delay(Duration::from_secs(10)),
        );

        let (server_addr, _) = start_mock_server(mock_state).await;

        let processing_mode = ProcessingMode {
            request_header_mode: HeaderProcessingMode::Skip,
            request_body_mode: BodyProcessingMode::None,
            request_trailer_mode: TrailerProcessingMode::Skip,
            response_header_mode: header_mode,
            response_body_mode: body_mode,
            response_trailer_mode: TrailerProcessingMode::Skip,
        };

        let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
        config.failure_mode_allow = false;
        config.message_timeout = Some(Duration::from_millis(2500));
        let mut ext_proc = ExternalProcessor::from(config);

        let mut response = build_response_from_mock(&MockMessage::<ResponseMsg>::new(
            vec![Some(("content-type", "application/json"))],
            vec!["body chunk 1", "body chunk 2"],
            vec![],
        ))
        .await;

        let result = ext_proc.apply_response(&mut response, &RequestCtx::default()).await;

        if body_mode == BodyProcessingMode::Buffered {
            assert_matches!(result, FilterDecision::DirectResponse(dr) => {
                assert_eq!(dr.status(), http::StatusCode::GATEWAY_TIMEOUT);
            });
        } else {
            assert_matches!(result, FilterDecision::Continue);
            let body_result = response.body_mut().collect().await;
            assert!(body_result.is_err(), "Expected body to fail due to timeout in Streamed mode");
        }
    }
}

#[tokio::test]
#[test_log::test]
async fn test_response_trailer_timeout_scenarios() {
    let scenarios = vec![
        (HeaderProcessingMode::Send, BodyProcessingMode::Buffered),
        (HeaderProcessingMode::Skip, BodyProcessingMode::Buffered),
        (HeaderProcessingMode::Send, BodyProcessingMode::Streamed),
        (HeaderProcessingMode::Skip, BodyProcessingMode::Streamed),
    ];

    for (idx, (header_mode, body_mode)) in scenarios.into_iter().enumerate() {
        println!("Testing Response Trailer Timeout Scenario {idx}: Header={header_mode:?}, Body={body_mode:?}");

        let mut mock_state = MockExternalProcessorState::new();
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        mock_state = mock_state.with_sender(tx);

        if header_mode == HeaderProcessingMode::Send {
            mock_state = mock_state.add_response(create_headers_response::<ResponseMsg>(
                vec![],
                None,
                vec![],
                ResponseStatus::Continue as i32,
                None,
            ));
        }
        if body_mode != BodyProcessingMode::None {
            mock_state = mock_state.add_response(create_body_response::<ResponseMsg>(
                vec![],
                None,
                vec![],
                ResponseStatus::Continue as i32,
                None,
            ));
        }

        mock_state = mock_state
            .add_response(create_trailers_response::<ResponseMsg>(vec![]).with_delay(Duration::from_secs(10)));

        let (server_addr, _) = start_mock_server(mock_state).await;

        let processing_mode = ProcessingMode {
            request_header_mode: HeaderProcessingMode::Skip,
            request_body_mode: BodyProcessingMode::None,
            request_trailer_mode: TrailerProcessingMode::Skip,
            response_header_mode: header_mode,
            response_body_mode: body_mode,
            response_trailer_mode: TrailerProcessingMode::Send,
        };

        let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
        config.failure_mode_allow = false;
        config.message_timeout = Some(Duration::from_millis(2500));
        let mut ext_proc = ExternalProcessor::from(config);

        let mut response = build_response_from_mock(&MockMessage::<ResponseMsg> {
            headers: vec![Some(("content-type", "application/json"))],
            body: vec!["body chunk"],
            trailers: vec![Some(("x-trailer", "value"))],
            _marker: std::marker::PhantomData,
        })
        .await;

        let result = ext_proc.apply_response(&mut response, &RequestCtx::default()).await;

        assert_matches!(result, FilterDecision::Continue);
        let body_result = response.body_mut().collect().await;
        assert!(body_result.is_err(), "Expected body collect to fail due to trailer timeout");
    }
}

#[tokio::test]
#[test_log::test]
async fn test_request_response_headers_processing_mode() {
    let mock_state = MockExternalProcessorState::new()
        .add_response(
            create_headers_response::<RequestMsg>(
                vec![Some(("x-req-mutated", "true"))],
                None,
                vec![],
                ResponseStatus::Continue as i32,
                None,
            )
            .with_expected_end_of_stream(true),
        )
        .add_response(
            create_headers_response::<ResponseMsg>(
                vec![Some(("x-res-mutated", "true"))],
                None,
                vec![],
                ResponseStatus::Continue as i32,
                None,
            )
            .with_expected_end_of_stream(true),
        );

    let (server_addr, _) = start_mock_server(mock_state).await;

    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Send,
        request_body_mode: BodyProcessingMode::None,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Send,
        response_body_mode: BodyProcessingMode::None,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    let mut ext_proc = ExternalProcessor::from(config);

    let mut request = build_request_from_mock(&MockMessage::<RequestMsg>::new(
        vec![Some(("x-req-header", "req-value"))],
        vec![],
        vec![],
    ))
    .await;

    let req_result = ext_proc.apply_request(&mut request, &RequestCtx::default()).await;
    assert_matches!(req_result, FilterDecision::Continue);
    assert_eq!(request.headers().get("x-req-mutated").unwrap(), "true");

    let mut response = build_response_from_mock(&MockMessage::<ResponseMsg>::new(
        vec![Some(("x-res-header", "res-value"))],
        vec![],
        vec![],
    ))
    .await;

    let res_result = ext_proc.apply_response(&mut response, &RequestCtx::default()).await;
    assert_matches!(res_result, FilterDecision::Continue);
    assert_eq!(response.headers().get("x-res-mutated").unwrap(), "true");
}

#[tokio::test]
#[test_log::test]
async fn test_request_response_headers_observability_mode() {
    let mock_state = MockExternalProcessorState::new()
        .with_observability(true)
        .add_response(
            create_headers_response::<RequestMsg>(vec![], None, vec![], ResponseStatus::Continue as i32, None)
                .with_expected_end_of_stream(true),
        )
        .add_response(
            create_headers_response::<ResponseMsg>(vec![], None, vec![], ResponseStatus::Continue as i32, None)
                .with_expected_end_of_stream(true),
        );

    let (server_addr, _) = start_mock_server(mock_state).await;

    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Send,
        request_body_mode: BodyProcessingMode::None,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Send,
        response_body_mode: BodyProcessingMode::None,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.observability_mode = true;
    let mut ext_proc = ExternalProcessor::from(config);

    let mut request = build_request_from_mock(&MockMessage::<RequestMsg>::new(
        vec![Some(("x-req-header", "req-value"))],
        vec![],
        vec![],
    ))
    .await;

    let req_result = ext_proc.apply_request(&mut request, &RequestCtx::default()).await;
    assert_matches!(req_result, FilterDecision::Continue);
    assert!(request.headers().get("x-req-mutated").is_none());

    let mut response = build_response_from_mock(&MockMessage::<ResponseMsg>::new(
        vec![Some(("x-res-header", "res-value"))],
        vec![],
        vec![],
    ))
    .await;

    let res_result = ext_proc.apply_response(&mut response, &RequestCtx::default()).await;
    assert_matches!(res_result, FilterDecision::Continue);
    assert!(response.headers().get("x-res-mutated").is_none());
}

#[tokio::test]
#[test_log::test]
async fn test_request_response_body_buffered_processing_mode() {
    let mock_state = MockExternalProcessorState::new()
        .add_response(create_body_response::<RequestMsg>(
            vec![],
            Some("req-mutated-body".as_bytes().to_vec()),
            vec![],
            ResponseStatus::Continue as i32,
            None,
        ))
        .add_response(create_body_response::<ResponseMsg>(
            vec![],
            Some("res-mutated-body".as_bytes().to_vec()),
            vec![],
            ResponseStatus::Continue as i32,
            None,
        ));

    let (server_addr, _) = start_mock_server(mock_state).await;

    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Skip,
        request_body_mode: BodyProcessingMode::Buffered,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Skip,
        response_body_mode: BodyProcessingMode::Buffered,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    let mut ext_proc = ExternalProcessor::from(config);

    let mut request = build_request_from_mock(&MockMessage::<RequestMsg>::new(
        vec![Some(("x-req-header", "req-value"))],
        vec!["req-body"],
        vec![],
    ))
    .await;

    let req_result = ext_proc.apply_request(&mut request, &RequestCtx::default()).await;
    assert_matches!(req_result, FilterDecision::Continue);
    let body_bytes = std::mem::take(&mut request.body_mut().inner.inner).collect().await.unwrap().to_bytes();
    assert_eq!(body_bytes, "req-mutated-body".as_bytes());

    let mut response = build_response_from_mock(&MockMessage::<ResponseMsg>::new(
        vec![Some(("x-res-header", "res-value"))],
        vec!["res-body"],
        vec![],
    ))
    .await;

    let res_result = ext_proc.apply_response(&mut response, &RequestCtx::default()).await;
    assert_matches!(res_result, FilterDecision::Continue);
    let body_bytes = response.body_mut().collect().await.unwrap().to_bytes();
    assert_eq!(body_bytes, "res-mutated-body".as_bytes());
}

#[tokio::test]
#[test_log::test]
async fn test_request_response_body_buffered_observability_mode() {
    let mock_state = MockExternalProcessorState::new()
        .with_observability(true)
        .add_response(create_body_response::<RequestMsg>(vec![], None, vec![], ResponseStatus::Continue as i32, None))
        .add_response(create_body_response::<ResponseMsg>(vec![], None, vec![], ResponseStatus::Continue as i32, None));

    let (server_addr, _) = start_mock_server(mock_state).await;

    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Skip,
        request_body_mode: BodyProcessingMode::Buffered,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Skip,
        response_body_mode: BodyProcessingMode::Buffered,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.observability_mode = true;
    let mut ext_proc = ExternalProcessor::from(config);

    let mut request = build_request_from_mock(&MockMessage::<RequestMsg>::new(
        vec![Some(("x-req-header", "req-value"))],
        vec!["req-body"],
        vec![],
    ))
    .await;

    let req_result = ext_proc.apply_request(&mut request, &RequestCtx::default()).await;
    assert_matches!(req_result, FilterDecision::Continue);
    let body_bytes = std::mem::take(&mut request.body_mut().inner.inner).collect().await.unwrap().to_bytes();
    assert_eq!(body_bytes, "req-body".as_bytes());

    let mut response = build_response_from_mock(&MockMessage::<ResponseMsg>::new(
        vec![Some(("x-res-header", "res-value"))],
        vec!["res-body"],
        vec![],
    ))
    .await;

    let res_result = ext_proc.apply_response(&mut response, &RequestCtx::default()).await;
    assert_matches!(res_result, FilterDecision::Continue);
    let body_bytes = response.body_mut().collect().await.unwrap().to_bytes();
    assert_eq!(body_bytes, "res-body".as_bytes());
}

#[tokio::test]
#[test_log::test]
async fn test_request_response_body_streamed_processing_mode() {
    let mock_state = MockExternalProcessorState::new()
        .add_response(create_body_response::<RequestMsg>(
            vec![],
            Some("req-mutated-chunk".as_bytes().to_vec()),
            vec![],
            ResponseStatus::Continue as i32,
            None,
        ))
        .add_response(create_body_response::<ResponseMsg>(
            vec![],
            Some("res-mutated-chunk".as_bytes().to_vec()),
            vec![],
            ResponseStatus::Continue as i32,
            None,
        ));

    let (server_addr, _) = start_mock_server(mock_state).await;

    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Skip,
        request_body_mode: BodyProcessingMode::Streamed,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Skip,
        response_body_mode: BodyProcessingMode::Streamed,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    let mut ext_proc = ExternalProcessor::from(config);

    let mut request = build_request_from_mock(&MockMessage::<RequestMsg>::new(
        vec![Some(("x-req-header", "req-value"))],
        vec!["req-chunk"],
        vec![],
    ))
    .await;

    let req_result = ext_proc.apply_request(&mut request, &RequestCtx::default()).await;
    assert_matches!(req_result, FilterDecision::Continue);
    let body_bytes = std::mem::take(&mut request.body_mut().inner.inner).collect().await.unwrap().to_bytes();
    assert_eq!(body_bytes, "req-mutated-chunk".as_bytes());

    let mut response = build_response_from_mock(&MockMessage::<ResponseMsg>::new(
        vec![Some(("x-res-header", "res-value"))],
        vec!["res-chunk"],
        vec![],
    ))
    .await;

    let res_result = ext_proc.apply_response(&mut response, &RequestCtx::default()).await;
    assert_matches!(res_result, FilterDecision::Continue);
    let body_bytes = response.body_mut().collect().await.unwrap().to_bytes();
    assert_eq!(body_bytes, "res-mutated-chunk".as_bytes());
}

#[tokio::test]
#[test_log::test]
async fn test_request_response_body_streamed_observability_mode() {
    let mock_state = MockExternalProcessorState::new()
        .with_observability(true)
        .add_response(create_body_response::<RequestMsg>(vec![], None, vec![], ResponseStatus::Continue as i32, None))
        .add_response(create_body_response::<ResponseMsg>(vec![], None, vec![], ResponseStatus::Continue as i32, None));

    let (server_addr, _) = start_mock_server(mock_state).await;

    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Skip,
        request_body_mode: BodyProcessingMode::Streamed,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Skip,
        response_body_mode: BodyProcessingMode::Streamed,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.observability_mode = true;
    let mut ext_proc = ExternalProcessor::from(config);

    let mut request = build_request_from_mock(&MockMessage::<RequestMsg>::new(
        vec![Some(("x-req-header", "req-value"))],
        vec!["req-chunk"],
        vec![],
    ))
    .await;

    let req_result = ext_proc.apply_request(&mut request, &RequestCtx::default()).await;
    assert_matches!(req_result, FilterDecision::Continue);
    let body_bytes = std::mem::take(&mut request.body_mut().inner.inner).collect().await.unwrap().to_bytes();
    assert_eq!(body_bytes, "req-chunk".as_bytes());

    let mut response = build_response_from_mock(&MockMessage::<ResponseMsg>::new(
        vec![Some(("x-res-header", "res-value"))],
        vec!["res-chunk"],
        vec![],
    ))
    .await;

    let res_result = ext_proc.apply_response(&mut response, &RequestCtx::default()).await;
    assert_matches!(res_result, FilterDecision::Continue);
    let body_bytes = response.body_mut().collect().await.unwrap().to_bytes();
    assert_eq!(body_bytes, "res-chunk".as_bytes());
}

#[tokio::test]
#[test_log::test]
async fn test_request_response_trailers_processing_mode() {
    let mock_state = MockExternalProcessorState::new()
        .add_response(create_trailers_response::<RequestMsg>(vec![Some(("x-req-trailer-mutated", "true"))]))
        .add_response(create_trailers_response::<ResponseMsg>(vec![Some(("x-res-trailer-mutated", "true"))]));

    let (server_addr, _) = start_mock_server(mock_state).await;

    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Skip,
        request_body_mode: BodyProcessingMode::None,
        request_trailer_mode: TrailerProcessingMode::Send,
        response_header_mode: HeaderProcessingMode::Skip,
        response_body_mode: BodyProcessingMode::None,
        response_trailer_mode: TrailerProcessingMode::Send,
    };

    let config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    let mut ext_proc = ExternalProcessor::from(config);

    let mut request = build_request_from_mock(&MockMessage::<RequestMsg>::new(
        vec![],
        vec!["req-body"],
        vec![Some(("x-req-trailer", "req-val"))],
    ))
    .await;

    let req_result = ext_proc.apply_request(&mut request, &RequestCtx::default()).await;
    assert_matches!(req_result, FilterDecision::Continue);
    let req_body = std::mem::take(&mut request.body_mut().inner.inner).collect().await.unwrap();
    let req_trailers = req_body.trailers().unwrap();
    assert_eq!(req_trailers.get("x-req-trailer-mutated").unwrap(), "true");

    let mut response = build_response_from_mock(&MockMessage::<ResponseMsg>::new(
        vec![],
        vec!["res-body"],
        vec![Some(("x-res-trailer", "res-val"))],
    ))
    .await;

    let res_result = ext_proc.apply_response(&mut response, &RequestCtx::default()).await;
    assert_matches!(res_result, FilterDecision::Continue);
    let res_body = response.body_mut().collect().await.unwrap();
    let res_trailers = res_body.trailers().unwrap();
    assert_eq!(res_trailers.get("x-res-trailer-mutated").unwrap(), "true");
}

#[tokio::test]
#[test_log::test]
async fn test_request_response_trailers_observability_mode() {
    let mock_state = MockExternalProcessorState::new()
        .with_observability(true)
        .add_response(create_trailers_response::<RequestMsg>(vec![]))
        .add_response(create_trailers_response::<ResponseMsg>(vec![]));

    let (server_addr, _) = start_mock_server(mock_state).await;

    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Skip,
        request_body_mode: BodyProcessingMode::None,
        request_trailer_mode: TrailerProcessingMode::Send,
        response_header_mode: HeaderProcessingMode::Skip,
        response_body_mode: BodyProcessingMode::None,
        response_trailer_mode: TrailerProcessingMode::Send,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.observability_mode = true;
    let mut ext_proc = ExternalProcessor::from(config);

    let mut request = build_request_from_mock(&MockMessage::<RequestMsg>::new(
        vec![],
        vec!["req-body"],
        vec![Some(("x-req-trailer", "req-val"))],
    ))
    .await;

    let req_result = ext_proc.apply_request(&mut request, &RequestCtx::default()).await;
    assert_matches!(req_result, FilterDecision::Continue);
    let req_body = std::mem::take(&mut request.body_mut().inner.inner).collect().await.unwrap();
    let req_trailers = req_body.trailers().unwrap();
    assert_eq!(req_trailers.get("x-req-trailer").unwrap(), "req-val");
    assert!(req_trailers.get("x-req-trailer-mutated").is_none());

    let mut response = build_response_from_mock(&MockMessage::<ResponseMsg>::new(
        vec![],
        vec!["res-body"],
        vec![Some(("x-res-trailer", "res-val"))],
    ))
    .await;

    let res_result = ext_proc.apply_response(&mut response, &RequestCtx::default()).await;
    assert_matches!(res_result, FilterDecision::Continue);
    let res_body = response.body_mut().collect().await.unwrap();
    let res_trailers = res_body.trailers().unwrap();
    assert_eq!(res_trailers.get("x-res-trailer").unwrap(), "res-val");
    assert!(res_trailers.get("x-res-trailer-mutated").is_none());
}

#[tokio::test]
#[test_log::test]
async fn test_request_header_mutation_with_large_body() {
    let mock_state = MockExternalProcessorState::new().add_response(create_headers_response::<RequestMsg>(
        vec![Some(("x-processed", "true")), Some(("x-custom-header", "custom-value"))],
        None,
        vec![],
        ResponseStatus::Continue as i32,
        None,
    ));
    let (server_addr, _) = start_mock_server(mock_state).await;
    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Send,
        request_body_mode: BodyProcessingMode::None,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Skip,
        response_body_mode: BodyProcessingMode::None,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.observability_mode = false;
    config.failure_mode_allow = false;
    let mut ext_proc = ExternalProcessor::from(config);

    // 1MB buffer
    let large_body = Box::leak(vec!['a'; 1024 * 1024].into_iter().collect::<String>().into_boxed_str());

    let mut request = build_request_from_mock(&MockMessage::<RequestMsg> {
        headers: vec![Some(("content-type", "text/plain"))],
        body: vec![large_body],
        trailers: vec![],
        _marker: std::marker::PhantomData,
    })
    .await;

    let result = ext_proc.apply_request(&mut request, &RequestCtx::default()).await;

    assert_matches!(result, FilterDecision::Continue);
    assert_eq!(request.headers().get("x-processed").unwrap(), "true");
    assert_eq!(request.headers().get("x-custom-header").unwrap(), "custom-value");
    assert_eq!(request.headers().get("content-type").unwrap(), "text/plain");

    let body_chunks =
        to_body_data_chunks(std::mem::take(&mut request.body_mut().inner.inner).collect().await.unwrap()).await;
    let mut actual_body = String::new();
    for chunk in body_chunks {
        actual_body.push_str(std::str::from_utf8(&chunk).unwrap());
    }
    assert_eq!(actual_body.len(), 1024 * 1024);
    assert_eq!(actual_body, &*large_body);
}

#[tokio::test]
#[test_log::test]
async fn test_request_header_mutation_with_multichunk_large_body() {
    let mock_state = MockExternalProcessorState::new().add_response(create_headers_response::<RequestMsg>(
        vec![Some(("x-processed", "true")), Some(("x-custom-header", "custom-value"))],
        None,
        vec![],
        ResponseStatus::Continue as i32,
        None,
    ));
    let (server_addr, _) = start_mock_server(mock_state).await;
    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Send,
        request_body_mode: BodyProcessingMode::None,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Skip,
        response_body_mode: BodyProcessingMode::None,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.observability_mode = false;
    config.failure_mode_allow = false;
    let mut ext_proc = ExternalProcessor::from(config);

    // 1MB buffer divided in 1024 chunks of 1024 bytes each
    let chunk_str: &'static str = Box::leak(vec!['a'; 1024].into_iter().collect::<String>().into_boxed_str());
    let body_chunks_input = vec![chunk_str; 1024];

    let mut request = build_request_from_mock(&MockMessage::<RequestMsg> {
        headers: vec![Some(("content-type", "text/plain"))],
        body: body_chunks_input.clone(),
        trailers: vec![],
        _marker: std::marker::PhantomData,
    })
    .await;

    let result = ext_proc.apply_request(&mut request, &RequestCtx::default()).await;

    assert_matches!(result, FilterDecision::Continue);
    assert_eq!(request.headers().get("x-processed").unwrap(), "true");
    assert_eq!(request.headers().get("x-custom-header").unwrap(), "custom-value");
    assert_eq!(request.headers().get("content-type").unwrap(), "text/plain");

    let body_chunks =
        to_body_data_chunks(std::mem::take(&mut request.body_mut().inner.inner).collect().await.unwrap()).await;
    let mut actual_body = String::new();
    for chunk in body_chunks {
        actual_body.push_str(std::str::from_utf8(&chunk).unwrap());
    }
    assert_eq!(actual_body.len(), 1024 * 1024);
    assert_eq!(actual_body, body_chunks_input.join(""));
}

#[tokio::test]
#[test_log::test]
async fn test_request_mutation_with_streamed_10m_body_4k_chunks() {
    const BODY_SIZE: usize = 10 * 1024 * 1024;
    let chunk_size = 4 * 1024; // 4 KB per chunk
    let num_chunks = BODY_SIZE / chunk_size;

    // Create the chunk strings
    let req_chunk_str: &'static str = Box::leak(vec!['a'; chunk_size].into_iter().collect::<String>().into_boxed_str());
    let req_body_chunks_input = vec![req_chunk_str; num_chunks];

    let req_chunk_upper_bytes = req_chunk_str.to_uppercase().into_bytes();

    let mut mock_state = MockExternalProcessorState::new();

    // 1. Mock response for Request Headers
    mock_state = mock_state.add_response(create_headers_response::<RequestMsg>(
        vec![],
        None,
        vec![],
        ResponseStatus::Continue as i32,
        None,
    ));

    // 2. Mock responses for Request Body chunks in Streamed mode
    for _ in 0..num_chunks {
        mock_state = mock_state.add_response(create_body_response::<RequestMsg>(
            vec![],
            Some(req_chunk_upper_bytes.clone()),
            vec![],
            ResponseStatus::Continue as i32,
            None,
        ));
    }

    let (server_addr, _) = start_mock_server(mock_state).await;

    // Configuration: Send for headers, Streamed for body, Skip for trailers
    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Send,
        request_body_mode: BodyProcessingMode::Streamed,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Skip,
        response_body_mode: BodyProcessingMode::Streamed,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.observability_mode = false;
    config.failure_mode_allow = false;

    let ext_config = ExternalProcessorConfigExt { frame_merge_limit: 1, frame_merge_window: Duration::from_millis(0) };
    let mut ext_proc = ExternalProcessor::from((config, None, Some(ext_config)));

    // Create request
    let mut request = build_request_from_mock(&MockMessage::<RequestMsg> {
        headers: vec![Some(("content-type", "text/plain"))],
        body: req_body_chunks_input.clone(),
        trailers: vec![],
        _marker: std::marker::PhantomData,
    })
    .await;

    // Process Request
    let req_result = ext_proc.apply_request(&mut request, &RequestCtx::default()).await;
    assert_matches!(req_result, FilterDecision::Continue);

    // Consume and verify the mutated request body
    let req_inner = std::mem::take(&mut request.body_mut().inner.inner);
    let req_body_chunks = to_body_data_chunks(req_inner.collect().await.unwrap()).await;

    let mut actual_req_body_len = 0;
    for chunk in req_body_chunks {
        let chunk_str = std::str::from_utf8(&chunk).unwrap();
        assert!(chunk_str.chars().all(|c| c == 'A'), "Request characters must be transformed to 'A'");
        actual_req_body_len += chunk_str.len();
    }
    assert_eq!(actual_req_body_len, BODY_SIZE);
}

#[tokio::test]
#[test_log::test]
async fn test_response_mutation_with_streamed_10m_body_4k_chunks() {
    const BODY_SIZE: usize = 10 * 1024 * 1024;
    let chunk_size = 4 * 1024; // 4 KB per chunk
    let num_chunks = BODY_SIZE / chunk_size;

    let res_chunk_str: &'static str = Box::leak(vec!['b'; chunk_size].into_iter().collect::<String>().into_boxed_str());
    let res_body_chunks_input = vec![res_chunk_str; num_chunks];
    let res_chunk_upper_bytes = res_chunk_str.to_uppercase().into_bytes();

    let mut mock_state = MockExternalProcessorState::new();

    // 1. Mock response for Response Headers
    mock_state = mock_state.add_response(create_headers_response::<ResponseMsg>(
        vec![],
        None,
        vec![],
        ResponseStatus::Continue as i32,
        None,
    ));

    // 2. Mock responses for Response Body chunks in Streamed mode
    for _ in 0..num_chunks {
        mock_state = mock_state.add_response(create_body_response::<ResponseMsg>(
            vec![],
            Some(res_chunk_upper_bytes.clone()),
            vec![],
            ResponseStatus::Continue as i32,
            None,
        ));
    }

    let (server_addr, _) = start_mock_server(mock_state).await;

    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Skip,
        request_body_mode: BodyProcessingMode::None,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Send,
        response_body_mode: BodyProcessingMode::Streamed,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.observability_mode = false;
    config.failure_mode_allow = false;

    let ext_config = ExternalProcessorConfigExt { frame_merge_limit: 1, frame_merge_window: Duration::from_millis(0) };
    let mut ext_proc = ExternalProcessor::from((config, None, Some(ext_config)));

    let mut response = build_response_from_mock(&MockMessage::<ResponseMsg> {
        headers: vec![Some(("content-type", "text/plain"))],
        body: res_body_chunks_input.clone(),
        trailers: vec![],
        _marker: std::marker::PhantomData,
    })
    .await;

    let res_result = ext_proc.apply_response(&mut response, &RequestCtx::default()).await;
    assert_matches!(res_result, FilterDecision::Continue);

    let res_inner = std::mem::take(&mut response.body_mut().inner);
    let res_body_chunks = to_body_data_chunks(res_inner.collect().await.unwrap()).await;

    let mut actual_res_body_len = 0;
    for chunk in res_body_chunks {
        let chunk_str = std::str::from_utf8(&chunk).unwrap();
        assert!(chunk_str.chars().all(|c| c == 'B'), "Response characters must be transformed to 'B'");
        actual_res_body_len += chunk_str.len();
    }
    assert_eq!(actual_res_body_len, BODY_SIZE);
}

#[tokio::test]
#[test_log::test]
async fn test_request_and_response_mutation_with_streamed_10m_body_4k_chunks() {
    const BODY_SIZE: usize = 10 * 1024 * 1024;
    let chunk_size = 4 * 1024; // 4 KB per chunk
    let num_chunks = BODY_SIZE / chunk_size;

    // Create the chunk strings
    let req_chunk_str: &'static str = Box::leak(vec!['a'; chunk_size].into_iter().collect::<String>().into_boxed_str());
    let req_body_chunks_input = vec![req_chunk_str; num_chunks];

    let res_chunk_str: &'static str = Box::leak(vec!['b'; chunk_size].into_iter().collect::<String>().into_boxed_str());
    let res_body_chunks_input = vec![res_chunk_str; num_chunks];

    let req_chunk_upper_bytes = req_chunk_str.to_uppercase().into_bytes();
    let res_chunk_upper_bytes = res_chunk_str.to_uppercase().into_bytes();

    let mut mock_state = MockExternalProcessorState::new();

    // 1. Mock response for Request Headers
    mock_state = mock_state.add_response(create_headers_response::<RequestMsg>(
        vec![],
        None,
        vec![],
        ResponseStatus::Continue as i32,
        None,
    ));

    // 2. Mock responses for Request Body chunks in Streamed mode
    for _ in 0..num_chunks {
        mock_state = mock_state.add_response(create_body_response::<RequestMsg>(
            vec![],
            Some(req_chunk_upper_bytes.clone()),
            vec![],
            ResponseStatus::Continue as i32,
            None,
        ));
    }

    // 3. Mock response for Response Headers
    mock_state = mock_state.add_response(create_headers_response::<ResponseMsg>(
        vec![],
        None,
        vec![],
        ResponseStatus::Continue as i32,
        None,
    ));

    // 4. Mock responses for Response Body chunks in Streamed mode
    for _ in 0..num_chunks {
        mock_state = mock_state.add_response(create_body_response::<ResponseMsg>(
            vec![],
            Some(res_chunk_upper_bytes.clone()),
            vec![],
            ResponseStatus::Continue as i32,
            None,
        ));
    }

    let (server_addr, _) = start_mock_server(mock_state).await;

    // Configuration: Send for headers, Streamed for body, Skip for trailers
    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Send,
        request_body_mode: BodyProcessingMode::Streamed,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Send,
        response_body_mode: BodyProcessingMode::Streamed,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.observability_mode = false;
    config.failure_mode_allow = false;

    let ext_config = ExternalProcessorConfigExt { frame_merge_limit: 1, frame_merge_window: Duration::from_millis(0) };
    let mut ext_proc = ExternalProcessor::from((config, None, Some(ext_config)));

    // Create request
    let mut request = build_request_from_mock(&MockMessage::<RequestMsg> {
        headers: vec![Some(("content-type", "text/plain"))],
        body: req_body_chunks_input.clone(),
        trailers: vec![],
        _marker: std::marker::PhantomData,
    })
    .await;

    // Create response
    let mut response = build_response_from_mock(&MockMessage::<ResponseMsg> {
        headers: vec![Some(("content-type", "text/plain"))],
        body: res_body_chunks_input.clone(),
        trailers: vec![],
        _marker: std::marker::PhantomData,
    })
    .await;

    // Process Request
    let req_result = ext_proc.apply_request(&mut request, &RequestCtx::default()).await;
    assert_matches!(req_result, FilterDecision::Continue);

    // Consume and verify the mutated request body
    let req_inner = std::mem::take(&mut request.body_mut().inner.inner);
    let req_body_chunks = to_body_data_chunks(req_inner.collect().await.unwrap()).await;

    let mut actual_req_body_len = 0;
    for chunk in req_body_chunks {
        let chunk_str = std::str::from_utf8(&chunk).unwrap();
        assert!(chunk_str.chars().all(|c| c == 'A'), "Request characters must be transformed to 'A'");
        actual_req_body_len += chunk_str.len();
    }
    assert_eq!(actual_req_body_len, BODY_SIZE);

    // Process Response
    let res_result = ext_proc.apply_response(&mut response, &RequestCtx::default()).await;
    assert_matches!(res_result, FilterDecision::Continue);

    // Consume and verify the mutated response body
    let res_inner = std::mem::take(&mut response.body_mut().inner);
    let res_body_chunks = to_body_data_chunks(res_inner.collect().await.unwrap()).await;

    let mut actual_res_body_len = 0;
    for chunk in res_body_chunks {
        let chunk_str = std::str::from_utf8(&chunk).unwrap();
        assert!(chunk_str.chars().all(|c| c == 'B'), "Response characters must be transformed to 'B'");
        actual_res_body_len += chunk_str.len();
    }
    assert_eq!(actual_res_body_len, BODY_SIZE);
}

use orion_configuration::config::core::{StringMatcher, StringMatcherPattern};
use smol_str::SmolStr;

#[tokio::test]
async fn test_forward_rules_allowed_headers() {
    let mock_state = MockExternalProcessorState::new().add_response(create_headers_response::<RequestMsg>(
        vec![],
        None,
        vec![],
        ResponseStatus::Continue as i32,
        None,
    ));
    let (server_addr, _) = start_mock_server(mock_state).await;
    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Send,
        request_body_mode: BodyProcessingMode::None,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Skip,
        response_body_mode: BodyProcessingMode::None,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let mut config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    config.forward_rules = Some(HeaderForwardingRules {
        allowed_headers: vec![StringMatcher {
            ignore_case: true,
            pattern: StringMatcherPattern::Exact(SmolStr::new("x-allowed-header")),
        }],
        disallowed_headers: vec![],
    });

    let mut ext_proc = ExternalProcessor::from(config);
    let mut request = build_request_from_mock(&MockMessage::<RequestMsg> {
        headers: vec![
            Some(("x-allowed-header", "allowed")),
            Some(("x-disallowed-header", "disallowed")),
            Some(("other-header", "other")),
        ],
        body: vec![],
        trailers: vec![],
        _marker: std::marker::PhantomData,
    })
    .await;

    let result = ext_proc.apply_request(&mut request, &RequestCtx::default()).await;
    assert_matches!(result, FilterDecision::Continue);

    // Assert the original headers are preserved in the proxy's request
    assert_eq!(request.headers().get("x-allowed-header").unwrap(), "allowed");
    assert_eq!(request.headers().get("x-disallowed-header").unwrap(), "disallowed");
    assert_eq!(request.headers().get("other-header").unwrap(), "other");
}

#[tokio::test]
#[allow(clippy::indexing_slicing)]
async fn test_header_append_action_append_if_exists_or_add() {
    // We need to create a custom response to test AppendIfExistsOrAdd
    let mut header_mutation = HeaderMutation::default();
    header_mutation.set_headers.push(HeaderValueOption {
        header: Some(EnvoyHeaderValue {
            key: "x-appended-header".to_owned(),
            value: "new-value".to_owned(),
            raw_value: vec![],
        }),
        append_action: HeaderAppendAction::AppendIfExistsOrAdd as i32,
        keep_empty_value: false,
        #[allow(deprecated)]
        append: None,
    });

    let response = ProcessingResponseType::RequestHeaders(
        orion_data_plane_api::envoy_data_plane_api::envoy::service::ext_proc::v3::HeadersResponse {
            response: Some(CommonResponse {
                status: ResponseStatus::Continue as i32,
                header_mutation: Some(header_mutation),
                body_mutation: None,
                trailers: None,
                clear_route_cache: false,
            }),
        },
    );

    let mock_state = MockExternalProcessorState::new().add_response(MockProcessingResponse::new(ProcessingResponse {
        response: Some(response),
        mode_override: None,
        dynamic_metadata: None,
        override_message_timeout: None,
        request_drain: false,
    }));

    let (server_addr, _) = start_mock_server(mock_state).await;
    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Send,
        request_body_mode: BodyProcessingMode::None,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Skip,
        response_body_mode: BodyProcessingMode::None,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    let mut ext_proc = ExternalProcessor::from(config);

    let mut request = build_request_from_mock(&MockMessage::<RequestMsg> {
        headers: vec![Some(("x-appended-header", "original-value"))],
        body: vec![],
        trailers: vec![],
        _marker: std::marker::PhantomData,
    })
    .await;

    let result = ext_proc.apply_request(&mut request, &RequestCtx::default()).await;
    assert_matches!(result, FilterDecision::Continue);

    // AppendIfExistsOrAdd: since "x-appended-header" exists ("original-value"),
    // the new value ("new-value") should be appended.
    let values: Vec<_> = request.headers().get_all("x-appended-header").iter().collect();
    assert_eq!(values.len(), 2);
    assert_eq!(values[0], "original-value");
    assert_eq!(values[1], "new-value");
}

#[tokio::test]
async fn test_clear_route_cache() {
    let response = ProcessingResponseType::RequestHeaders(HeadersResponse {
        response: Some(CommonResponse {
            status: ResponseStatus::Continue as i32,
            header_mutation: None,
            body_mutation: None,
            trailers: None,
            clear_route_cache: true,
        }),
    });

    let mock_state = MockExternalProcessorState::new().add_response(MockProcessingResponse::new(ProcessingResponse {
        response: Some(response),
        mode_override: None,
        dynamic_metadata: None,
        override_message_timeout: None,
        request_drain: false,
    }));

    let (server_addr, _) = start_mock_server(mock_state).await;
    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Send,
        request_body_mode: BodyProcessingMode::None,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Skip,
        response_body_mode: BodyProcessingMode::None,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    let mut ext_proc = ExternalProcessor::from(config);
    let mut request = build_request_from_mock(&MockMessage::<RequestMsg> {
        headers: vec![Some(("content-type", "application/json"))],
        body: vec![],
        trailers: vec![],
        _marker: std::marker::PhantomData,
    })
    .await;

    let result = ext_proc.apply_request(&mut request, &RequestCtx::default()).await;

    // When clear_route_cache is true, the filter should return FilterDecision::Reroute
    assert_matches!(result, FilterDecision::Reroute);
}

#[tokio::test]
async fn test_immediate_response_with_grpc_status() {
    let immediate_response = ImmediateResponse {
        status: Some(EnvoyHttpStatus { code: 503 }),
        headers: None,
        body: "grpc error body".as_bytes().to_vec(),
        grpc_status: Some(orion_data_plane_api::envoy_data_plane_api::envoy::service::ext_proc::v3::GrpcStatus {
            status: 14, // UNAVAILABLE
        }),
        details: "grpc_error_details".to_owned(),
    };

    let mock_state = MockExternalProcessorState::new().add_response(MockProcessingResponse::new(ProcessingResponse {
        response: Some(ProcessingResponseType::ImmediateResponse(immediate_response)),
        mode_override: None,
        dynamic_metadata: None,
        override_message_timeout: None,
        request_drain: false,
    }));

    let (server_addr, _) = start_mock_server(mock_state).await;
    let processing_mode = ProcessingMode {
        request_header_mode: HeaderProcessingMode::Send,
        request_body_mode: BodyProcessingMode::None,
        request_trailer_mode: TrailerProcessingMode::Skip,
        response_header_mode: HeaderProcessingMode::Skip,
        response_body_mode: BodyProcessingMode::None,
        response_trailer_mode: TrailerProcessingMode::Skip,
    };

    let config = create_default_config_for_ext_proc_filter(server_addr, processing_mode);
    let mut ext_proc = ExternalProcessor::from(config);
    let mut request = build_request_from_mock(&MockMessage::<RequestMsg> {
        headers: vec![Some(("content-type", "application/json"))],
        body: vec![],
        trailers: vec![],
        _marker: std::marker::PhantomData,
    })
    .await;

    let result = ext_proc.apply_request(&mut request, &RequestCtx::default()).await;

    // An ImmediateResponse triggers a DirectResponse
    assert_matches!(result, FilterDecision::DirectResponse(response) => {
        assert_eq!(response.status(), 503);
        let (_, _body) = response.into_parts();
        // Since we are not running this in a context that eagerly resolves, we might not unwrap it directly,
        // but typically in testing it works or we just check status.
    });
}
