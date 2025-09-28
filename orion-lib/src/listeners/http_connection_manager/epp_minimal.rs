use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use http::{HeaderMap, HeaderName, HeaderValue, Request, Response, Version};
use http_body::Body;
use http_body_util::{BodyStream, Full};
use orion_configuration::config::{
    cluster::ClusterSpecifier as ClusterSpecifierConfig,
    network_filters::http_connection_manager::http_filters::ext_proc::{
        BodyProcessingMode, ExtProcOverrides, ExternalProcessor as ExternalProcessorConfig, GrpcServiceSpecifier,
        HeaderForwardingRules, HeaderProcessingMode,
    },
};
use orion_data_plane_api::envoy_data_plane_api::envoy::{
    config::core::v3::{HeaderMap as ExtProcHeaderMap, HeaderValue as ExtProcHeaderValue},
    service::ext_proc::v3::{
        body_mutation::Mutation, external_processor_client::ExternalProcessorClient,
        processing_request::Request as ProcessingRequestType, processing_response::Response as ProcessingResponseType,
        BodyMutation, BodyResponse, HeaderMutation, HeadersResponse, HttpBody, HttpHeaders, HttpTrailers,
        ProcessingRequest, ProcessingResponse,
    },
};
use orion_format::types::ResponseFlags as FmtResponseFlags;
use std::collections::HashMap;
use std::result::Result;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tracing::{debug, info, warn};

use crate::{
    body::{
        body_with_metrics::BodyWithMetrics,
        poly_body::{BodySender, PolyBody},
        response_flags::ResponseFlags,
    },
    clusters::clusters_manager::{self, RoutingContext},
    event_error::EventFailure,
    listeners::{http_connection_manager::FilterDecision, synthetic_http_response::SyntheticHttpResponse},
    Error,
};

const MAX_BUFFER_SIZE: usize = 2 * 1024 * 1024;

#[derive(Debug)]
pub struct SimplifiedEppProcessor {
    cluster_id: &'static str,
    failure_mode_allow: bool,
    forward_rules: Option<Arc<HeaderForwardingRules>>,
    bidi_stream: Option<ExtProcStream>,
}

impl SimplifiedEppProcessor {
    pub async fn apply_request(&mut self, request: &mut Request<BodyWithMetrics<PolyBody>>) -> FilterDecision {
        let headers = self.build_http_headers(request.headers(), request.body().inner.is_end_stream());
        let failure_mode_allow = self.failure_mode_allow;

        let bidi_stream = match self.get_external_processor(request.version()) {
            Ok(proc) => proc,
            Err(decision) => return decision,
        };

        match bidi_stream.apply_request_processing_steps(request, headers, failure_mode_allow).await {
            Ok(()) => FilterDecision::Continue,
            Err(FilterError::HaltedOnError { reason }) => {
                warn!("External processor halted on error during request processing: {}", reason);
                FilterDecision::Continue
            },
            Err(FilterError::DirectResponse { reason }) => FilterDecision::DirectResponse(
                SyntheticHttpResponse::internal_error_with_msg(
                    &reason,
                    EventFailure::ExtProcError.into(),
                    ResponseFlags(FmtResponseFlags::UPSTREAM_CONNECTION_FAILURE),
                )
                .into_response(request.version()),
            ),
        }
    }

    pub async fn apply_response(&mut self, response: &mut Response<PolyBody>) -> FilterDecision {
        let headers = self.build_http_headers(response.headers(), response.body().is_end_stream());
        let Some(bidi_stream) = self.bidi_stream.as_mut() else { return FilterDecision::Continue };
        if bidi_stream.is_skipped() {
            return FilterDecision::Continue;
        }

        let failure_mode_allow = self.failure_mode_allow;
        match bidi_stream.apply_response_processing_steps(response, headers, failure_mode_allow).await {
            Ok(()) => FilterDecision::Continue,
            Err(FilterError::HaltedOnError { reason }) => {
                warn!("External processor halted on error during response processing: {}", reason);
                FilterDecision::Continue
            },
            Err(FilterError::DirectResponse { reason }) => FilterDecision::DirectResponse(
                SyntheticHttpResponse::internal_error_with_msg(
                    &reason,
                    EventFailure::ExtProcError.into(),
                    ResponseFlags(FmtResponseFlags::UPSTREAM_CONNECTION_FAILURE),
                )
                .into_response(response.version()),
            ),
        }
    }

    fn get_external_processor(&mut self, http_version: Version) -> Result<&mut ExtProcStream, FilterDecision> {
        if let Some(ref mut bidi_stream) = self.bidi_stream {
            Ok(bidi_stream)
        } else {
            let bidi_stream = match ExtProcStream::connect(self.cluster_id) {
                Ok(stream) => stream,
                Err(err) => {
                    let msg = format!("Failed to connect to external processor service: {err}");
                    let response = SyntheticHttpResponse::internal_error_with_msg(
                        &msg,
                        EventFailure::ExtProcError.into(),
                        ResponseFlags(FmtResponseFlags::UPSTREAM_CONNECTION_FAILURE),
                    )
                    .into_response(http_version);
                    return Err(FilterDecision::DirectResponse(response));
                },
            };
            Ok(self.bidi_stream.insert(bidi_stream))
        }
    }

    fn build_http_headers(&self, headers: &HeaderMap, end_of_stream: bool) -> HttpHeaders {
        let mut header_values = Vec::new();
        for (name, value) in headers {
            if !self.should_forward_header(name.as_str()) {
                continue;
            }
            let proto_value = if let Ok(as_str) = value.to_str() {
                ExtProcHeaderValue { key: name.as_str().to_owned(), value: as_str.to_owned(), raw_value: Vec::new() }
            } else {
                ExtProcHeaderValue {
                    key: name.as_str().to_owned(),
                    value: String::new(),
                    raw_value: value.as_bytes().to_vec(),
                }
            };
            header_values.push(proto_value);
        }
        let header_map = ExtProcHeaderMap { headers: header_values };
        HttpHeaders { headers: Some(header_map), attributes: HashMap::default(), end_of_stream }
    }

    fn should_forward_header(&self, header: &str) -> bool {
        if let Some(rules) = &self.forward_rules {
            if !rules.disallowed_headers.is_empty()
                && rules.disallowed_headers.iter().any(|matcher| matcher.matches(header))
            {
                return false;
            }
            if !rules.allowed_headers.is_empty() {
                return rules.allowed_headers.iter().any(|matcher| matcher.matches(header));
            }
        }
        true
    }
}

impl Clone for SimplifiedEppProcessor {
    fn clone(&self) -> Self {
        Self {
            cluster_id: self.cluster_id,
            failure_mode_allow: self.failure_mode_allow,
            forward_rules: self.forward_rules.clone(),
            bidi_stream: None,
        }
    }
}

impl TryFrom<(ExternalProcessorConfig, Option<ExtProcOverrides>)> for SimplifiedEppProcessor {
    type Error = Error;
    fn try_from((config, overrides): (ExternalProcessorConfig, Option<ExtProcOverrides>)) -> Result<Self, Self::Error> {
        let ExternalProcessorConfig {
            grpc_service,
            failure_mode_allow,
            processing_mode,
            forward_rules,
            observability_mode,
            allow_mode_override,
            ..
        } = config;

        let mut effective_grpc_service = grpc_service;
        let mut effective_failure_mode_allow = failure_mode_allow;
        let mut effective_processing_mode = processing_mode.unwrap_or_default();

        if let Some(overrides) = overrides {
            if let Some(override_service) = overrides.grpc_service {
                effective_grpc_service = override_service;
            }
            if let Some(override_mode) = overrides.processing_mode {
                effective_processing_mode = override_mode;
            }
            if let Some(override_fail) = overrides.failure_mode_allow {
                effective_failure_mode_allow = override_fail;
            }
        }

        let headers_mode_ok = matches!(effective_processing_mode.request_header_mode, HeaderProcessingMode::Send)
            && matches!(effective_processing_mode.response_header_mode, HeaderProcessingMode::Send);
        let body_mode_ok = matches!(
            effective_processing_mode.request_body_mode,
            BodyProcessingMode::Buffered | BodyProcessingMode::Streamed | BodyProcessingMode::FullDuplexStreamed
        ) && matches!(
            effective_processing_mode.response_body_mode,
            BodyProcessingMode::Buffered | BodyProcessingMode::Streamed | BodyProcessingMode::FullDuplexStreamed
        );
        if !(headers_mode_ok && body_mode_ok) {
            return Err(Error::from(
                "Simplified EPP filter only support SEND header mode, BUFFERED | STREAMED body mode",
            ));
        }

        if allow_mode_override {
            return Err(Error::from("Simplified EPP filter does not support allow_mode_override"));
        }
        if observability_mode {
            return Err(Error::from("Simplified EPP filter does not support observability_mode"));
        }

        let cluster_spec = match &effective_grpc_service.service_specifier {
            GrpcServiceSpecifier::Cluster(cluster) => ClusterSpecifierConfig::Cluster(cluster.clone()),
            GrpcServiceSpecifier::GoogleGrpc(_) => {
                return Err(Error::from("Simplified EPP filter supports only cluster-based gRPC services"));
            },
        };
        let cluster_id = clusters_manager::resolve_cluster(&cluster_spec).ok_or_else(|| {
            Error::from(format!("Failed to resolve cluster '{}' for Simplified EPP filter", cluster_spec.name()))
        })?;

        info!("Using Simplified EPP compatible external processing filter");
        Ok(Self {
            cluster_id,
            failure_mode_allow: effective_failure_mode_allow,
            forward_rules: forward_rules.map(Arc::new),
            bidi_stream: None,
        })
    }
}

#[derive(Debug)]
struct ExtProcStream {
    skipped: bool,
    tx: mpsc::Sender<ProcessingRequest>,
    rx: mpsc::Receiver<ProcessingResponse>,
}

impl ExtProcStream {
    fn connect(cluster_id: &'static str) -> crate::Result<Self> {
        let grpc_service = clusters_manager::get_grpc_connection(cluster_id, RoutingContext::None)?;
        let mut client = ExternalProcessorClient::new(grpc_service);
        let (tx_req, rx_req) = mpsc::channel::<ProcessingRequest>(8);
        let (tx_resp, rx_resp) = mpsc::channel::<ProcessingResponse>(8);
        let request_stream = tokio_stream::wrappers::ReceiverStream::new(rx_req);

        tokio::spawn(async move {
            match client.process(request_stream).await {
                Ok(stream) => {
                    let mut inbound = stream.into_inner();
                    while let Ok(Some(message)) = inbound.message().await {
                        if tx_resp.send(message).await.is_err() {
                            break;
                        }
                    }
                },
                Err(err) => warn!("Failed to establish bidi stream with external processor: {err}"),
            }
        });

        Ok(Self { skipped: false, tx: tx_req, rx: rx_resp })
    }

    fn is_skipped(&self) -> bool {
        self.skipped
    }

    async fn apply_request_processing_steps(
        &mut self,
        request: &mut Request<BodyWithMetrics<PolyBody>>,
        headers: HttpHeaders,
        failure_mode_allow: bool,
    ) -> FilterResult<()> {
        let body = std::mem::take(&mut request.body_mut().inner);
        let (body_for_stream, body_to_restore) = buffer_body_if_needed(body, failure_mode_allow).await?;
        let end_of_stream = body_for_stream.is_end_stream();

        let request_message = ProcessingRequest {
            observability_mode: false,
            attributes: HashMap::default(),
            protocol_config: Option::default(),
            metadata_context: Option::default(),
            request: Some(ProcessingRequestType::RequestHeaders(headers)),
        };
        self.send(request_message).await?;

        if !end_of_stream {
            let tx = self.tx.clone();
            tokio::spawn(stream_body_to_ext_proc(body_for_stream, tx, false));
        }

        let (replacement_body, replacement_tx) = PolyBody::channel(1);
        let body_sender = BodySender::new(replacement_tx);
        request.body_mut().inner = replacement_body;

        match self.handle_incoming_replies_for_request(request.headers_mut(), body_sender, failure_mode_allow).await {
            Ok(()) => {
                request.headers_mut().remove(http::header::CONTENT_LENGTH);
                Ok(())
            },
            Err(FilterError::HaltedOnError { reason }) => {
                self.skipped = true;
                restore_request_body(request, body_to_restore);
                Err(FilterError::HaltedOnError { reason })
            },
            Err(other) => Err(other),
        }
    }

    async fn handle_incoming_replies_for_request(
        &mut self,
        headers: &mut HeaderMap,
        body_sender: BodySender,
        failure_mode_allow: bool,
    ) -> FilterResult<()> {
        loop {
            let response = match self.recv().await {
                Ok(resp) => resp,
                Err(reason) => {
                    return if failure_mode_allow {
                        Err(FilterError::HaltedOnError { reason })
                    } else {
                        Err(FilterError::DirectResponse { reason })
                    };
                },
            };

            if handle_reply_for_request_processing(response, headers, &body_sender).await? {
                break;
            }
        }
        Ok(())
    }

    async fn apply_response_processing_steps(
        &mut self,
        response: &mut Response<PolyBody>,
        headers: HttpHeaders,
        failure_mode_allow: bool,
    ) -> FilterResult<()> {
        let body = std::mem::take(response.body_mut());
        let (body_for_stream, body_to_restore) = buffer_body_if_needed(body, failure_mode_allow).await?;
        let end_of_stream = body_for_stream.is_end_stream();

        let request_message = ProcessingRequest {
            observability_mode: false,
            attributes: HashMap::default(),
            protocol_config: Option::default(),
            metadata_context: Option::default(),
            request: Some(ProcessingRequestType::ResponseHeaders(headers)),
        };
        self.send(request_message).await?;

        if !end_of_stream {
            let tx = self.tx.clone();
            tokio::spawn(stream_body_to_ext_proc(body_for_stream, tx, true));
        }

        let (replacement_body, replacement_tx) = PolyBody::channel(1);
        let body_sender = BodySender::new(replacement_tx);
        *response.body_mut() = replacement_body;

        match self.handle_incoming_replies_for_response(response.headers_mut(), body_sender, failure_mode_allow).await {
            Ok(()) => {
                response.headers_mut().remove(http::header::CONTENT_LENGTH);
                Ok(())
            },
            Err(FilterError::HaltedOnError { reason }) => {
                restore_response_body(response, body_to_restore);
                Err(FilterError::HaltedOnError { reason })
            },
            Err(other) => Err(other),
        }
    }

    async fn handle_incoming_replies_for_response(
        &mut self,
        headers: &mut HeaderMap,
        body_sender: BodySender,
        failure_mode_allow: bool,
    ) -> FilterResult<()> {
        loop {
            let response = match self.recv().await {
                Ok(resp) => resp,
                Err(reason) => {
                    return if failure_mode_allow {
                        Err(FilterError::HaltedOnError { reason })
                    } else {
                        Err(FilterError::DirectResponse { reason })
                    };
                },
            };

            if handle_reply_for_response_processing(response, headers, &body_sender).await? {
                break;
            }
        }
        Ok(())
    }

    async fn recv(&mut self) -> Result<ProcessingResponse, String> {
        self.rx.recv().await.ok_or_else(|| "External processor stream terminated".to_owned())
    }

    async fn send(&mut self, message: ProcessingRequest) -> FilterResult<()> {
        self.tx.send(message).await.map_err(|e| FilterError::DirectResponse {
            reason: format!("Failed to send processing request to external processor service : {e}"),
        })
    }
}

#[derive(Debug)]
pub enum FilterError {
    HaltedOnError { reason: String },
    DirectResponse { reason: String },
}
type FilterResult<T> = Result<T, FilterError>;

async fn buffer_body_if_needed(body: PolyBody, failure_mode_allow: bool) -> FilterResult<(PolyBody, Option<Bytes>)> {
    if failure_mode_allow {
        let (replay_body, copy) = buffer_body(body).await?;
        Ok((replay_body, Some(copy)))
    } else {
        Ok((body, None))
    }
}

#[allow(clippy::expect_used)]
async fn buffer_body(body: PolyBody) -> FilterResult<(PolyBody, Bytes)> {
    let mut stream = BodyStream::new(body);
    let mut buffer = BytesMut::new();
    while let Some(frame) = stream.next().await {
        let frame = frame.map_err(|err| FilterError::DirectResponse {
            reason: format!("External processor body read failed: {err}"),
        })?;
        if frame.is_data() {
            let data = frame.into_data().expect("frame known to contain data");
            if buffer.len() + data.len() > MAX_BUFFER_SIZE {
                return Err(FilterError::DirectResponse {
                    reason: "External processor body bufferring reached max buffer limit".into(),
                });
            }
            buffer.extend_from_slice(&data);
        }
    }
    let bytes = buffer.freeze();
    let replay_body = PolyBody::from(Full::new(bytes.clone()));
    Ok((replay_body, bytes))
}

fn restore_request_body(request: &mut Request<BodyWithMetrics<PolyBody>>, copy: Option<Bytes>) {
    if let Some(bytes) = copy {
        request.body_mut().inner = PolyBody::from(Full::new(bytes));
    } else {
        request.body_mut().inner = PolyBody::default();
    }
}

fn restore_response_body(response: &mut Response<PolyBody>, copy: Option<Bytes>) {
    if let Some(bytes) = copy {
        *response.body_mut() = PolyBody::from(Full::new(bytes));
    } else {
        *response.body_mut() = PolyBody::default();
    }
}

#[allow(clippy::expect_used)]
async fn stream_body_to_ext_proc(body: PolyBody, tx: mpsc::Sender<ProcessingRequest>, is_response: bool) {
    let mut stream = BodyStream::new(body);
    while let Some(frame) = stream.next().await {
        match frame {
            Ok(frame) if frame.is_data() => {
                let data = frame.into_data().expect("frame known to contain data");
                let request = ProcessingRequest {
                    observability_mode: false,
                    attributes: HashMap::default(),
                    protocol_config: Option::default(),
                    metadata_context: Option::default(),
                    request: Some(if is_response {
                        ProcessingRequestType::ResponseBody(HttpBody { body: data.into(), end_of_stream: false })
                    } else {
                        ProcessingRequestType::RequestBody(HttpBody { body: data.into(), end_of_stream: false })
                    }),
                };
                if tx.send(request).await.is_err() {
                    break;
                }
            },
            Ok(frame) if frame.is_trailers() => {
                let trailers = frame.into_trailers().expect("frame known to contain trailers");
                let request = ProcessingRequest {
                    observability_mode: false,
                    attributes: HashMap::default(),
                    protocol_config: Option::default(),
                    metadata_context: Option::default(),
                    request: Some(if is_response {
                        ProcessingRequestType::ResponseTrailers(HttpTrailers { trailers: to_header_map(&trailers) })
                    } else {
                        ProcessingRequestType::RequestTrailers(HttpTrailers { trailers: to_header_map(&trailers) })
                    }),
                };
                if tx.send(request).await.is_err() {
                    break;
                }
            },
            Ok(_) => {},
            Err(err) => {
                debug!("Failed to read body frame for external processing: {err}");
                break;
            },
        }
    }

    let final_request = ProcessingRequest {
        observability_mode: false,
        attributes: HashMap::default(),
        protocol_config: Option::default(),
        metadata_context: Option::default(),
        request: Some(if is_response {
            ProcessingRequestType::ResponseBody(HttpBody { body: Vec::default(), end_of_stream: true })
        } else {
            ProcessingRequestType::RequestBody(HttpBody { body: Vec::default(), end_of_stream: true })
        }),
    };
    let _ = tx.send(final_request).await;
}

async fn handle_reply_for_request_processing(
    response: ProcessingResponse,
    headers: &mut HeaderMap,
    body_sender: &BodySender,
) -> FilterResult<bool> {
    match response.response {
        Some(
            ProcessingResponseType::RequestHeaders(HeadersResponse { response: Some(inner) })
            | ProcessingResponseType::RequestBody(BodyResponse { response: Some(inner) }),
        ) => {
            if let Some(mutation) = inner.header_mutation {
                apply_header_mutations(headers, mutation);
            }
            if let Some(BodyMutation { mutation: Some(body_mutation) }) = inner.body_mutation {
                if let Mutation::StreamedResponse(stream) = body_mutation {
                    body_sender.send_data(Bytes::from(stream.body)).await.map_err(|e| FilterError::DirectResponse {
                        reason: format!("External processor request body forwarding failed: {e}"),
                    })?;
                    return Ok(stream.end_of_stream);
                }
                warn!("Simplified EPP filter ignores non-streamed body mutations for requests");
            }
            Ok(false)
        },
        _ => Ok(false),
    }
}

async fn handle_reply_for_response_processing(
    response: ProcessingResponse,
    headers: &mut HeaderMap,
    body_sender: &BodySender,
) -> FilterResult<bool> {
    match response.response {
        Some(
            ProcessingResponseType::ResponseHeaders(HeadersResponse { response: Some(inner) })
            | ProcessingResponseType::ResponseBody(BodyResponse { response: Some(inner) }),
        ) => {
            if let Some(mutation) = inner.header_mutation {
                apply_header_mutations(headers, mutation);
            }
            if let Some(BodyMutation { mutation: Some(body_mutation) }) = inner.body_mutation {
                if let Mutation::StreamedResponse(stream) = body_mutation {
                    body_sender.send_data(Bytes::from(stream.body)).await.map_err(|e| FilterError::DirectResponse {
                        reason: format!("External processor response body forwarding failed: {e}"),
                    })?;
                    return Ok(stream.end_of_stream);
                }
                warn!("Simplified EPP filter ignores non-streamed body mutations for responses");
            }
            Ok(false)
        },
        _ => Ok(false),
    }
}

fn apply_header_mutations(headers: &mut HeaderMap, mutation: HeaderMutation) {
    for header_to_remove in mutation.remove_headers {
        if let Ok(name) = HeaderName::from_bytes(header_to_remove.as_bytes()) {
            headers.remove(name);
        }
    }
    for header_value_option in mutation.set_headers {
        let Some(proto_header) = header_value_option.header else { continue };
        let Ok(name) = HeaderName::from_bytes(proto_header.key.as_bytes()) else { continue };
        if name == http::header::CONTENT_LENGTH {
            continue;
        }
        let value = if proto_header.raw_value.is_empty() {
            HeaderValue::from_str(&proto_header.value).ok()
        } else {
            HeaderValue::from_bytes(&proto_header.raw_value).ok()
        };
        if let Some(value) = value {
            headers.insert(name, value);
        }
    }
}

fn to_header_map(headers: &HeaderMap) -> Option<ExtProcHeaderMap> {
    if headers.is_empty() {
        None
    } else {
        let proto_headers = headers
            .iter()
            .map(|(name, value)| {
                if let Ok(as_str) = value.to_str() {
                    ExtProcHeaderValue {
                        key: name.as_str().to_owned(),
                        value: as_str.to_owned(),
                        raw_value: Vec::new(),
                    }
                } else {
                    ExtProcHeaderValue {
                        key: name.as_str().to_owned(),
                        value: String::new(),
                        raw_value: value.as_bytes().to_vec(),
                    }
                }
            })
            .collect();
        Some(ExtProcHeaderMap { headers: proto_headers })
    }
}
