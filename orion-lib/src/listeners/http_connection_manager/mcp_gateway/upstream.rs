use std::{io, sync::Arc as StdArc, time::Instant};

use bytes::Bytes;
use http::{header::HOST, HeaderValue, Request, Response, StatusCode};
use opentelemetry::KeyValue;
use orion_configuration::config::{
    cluster::ClusterSpecifier,
    network_filters::http_connection_manager::http_filters::mcp_gateway::{UpstreamBackend, UpstreamLimits},
};
use orion_http_header::X_REQUEST_ID;
use orion_interner::StringInterner;
use rmcp::model::{Annotated, CallToolResult, RawContent, RawTextContent};
use serde::Serialize;
use serde_json::{json, Value};
use tracing::warn;

#[cfg(feature = "metrics")]
use {
    crate::{get_shard_id, with_histogram, with_metric},
    orion_metrics::metrics::mcp as mcp_metrics,
};

use super::{
    tools::ToolEntry,
    transcoder::{Transcoder, TranscoderType},
};
use crate::{
    body::{poly_body::PolyBodyError, timeout_body::TimeoutBodyError},
    clusters::{
        clusters_manager::RoutingContextError,
        http_upstream::{acquire_http_upstream, AcquireHttpUpstreamError, AcquireHttpUpstreamErrorKind},
        RoutingPriority,
    },
    event_error::{TryInferFrom, UpstreamError},
    listeners::http_connection_manager::{http_modifiers, RequestCtx, RequestHandler},
    OrionRequestBody, OrionResponseBody, UpstreamCallOpts,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolInvocationErrorCode {
    InvalidOutput,
    UpstreamError,
    UpstreamTimeout,
    Overflow,
    ResponseTooLarge,
}

impl ToolInvocationErrorCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidOutput => "invalid_output",
            Self::UpstreamError => "upstream_error",
            Self::UpstreamTimeout => "upstream_timeout",
            Self::Overflow => "overflow",
            Self::ResponseTooLarge => "response_too_large",
        }
    }
}

#[derive(Debug)]
pub struct ToolInvocationFailure {
    pub code: ToolInvocationErrorCode,
    pub message: String,
    pub status: Option<StatusCode>,
    pub retryable: bool,
}

impl ToolInvocationFailure {
    pub fn upstream_error() -> Self {
        Self {
            code: ToolInvocationErrorCode::UpstreamError,
            message: "The upstream tool is unavailable".to_owned(),
            status: Some(StatusCode::BAD_GATEWAY),
            retryable: true,
        }
    }

    pub fn upstream_timeout() -> Self {
        Self {
            code: ToolInvocationErrorCode::UpstreamTimeout,
            message: "The upstream tool call timed out".to_owned(),
            status: Some(StatusCode::GATEWAY_TIMEOUT),
            retryable: true,
        }
    }

    pub fn response_too_large(limit: usize) -> Self {
        Self {
            code: ToolInvocationErrorCode::ResponseTooLarge,
            message: format!("The upstream tool result exceeded the configured {limit}-byte limit"),
            status: Some(StatusCode::PAYLOAD_TOO_LARGE),
            retryable: false,
        }
    }

    pub fn into_call_tool_result(self) -> CallToolResult {
        let mut error = serde_json::Map::from_iter([
            ("code".to_owned(), Value::String(self.code.as_str().to_owned())),
            ("message".to_owned(), Value::String(self.message)),
            ("retryable".to_owned(), Value::Bool(self.retryable)),
        ]);
        if let Some(status) = self.status {
            error.insert("status".to_owned(), Value::from(status.as_u16()));
        }

        CallToolResult::structured_error(json!({
            "error": error,
        }))
    }
}

#[derive(Debug)]
pub enum ToolInvocationOutcome {
    Success(CallToolResult),
    Failure(ToolInvocationFailure),
}

pub async fn invoke_rest_tool(
    tool: &StdArc<ToolEntry>,
    request: Request<OrionRequestBody>,
    upstream_limits: &UpstreamLimits,
    req_ctx: &RequestCtx,
) -> ToolInvocationOutcome {
    let started = Instant::now();
    let cluster = match &tool.conf.backend {
        UpstreamBackend::Rest { cluster, .. } => cluster.as_str(),
        _ => unreachable!("invoke_rest_tool called for a non-REST tool"),
    };
    let mut span = req_ctx.begin_upstream_span(cluster);
    span.set_attributes([
        KeyValue::new("mcp.tool.name", tool.conf.name.to_static_str()),
        KeyValue::new("mcp.backend", "rest"),
        KeyValue::new("mcp.upstream", cluster.to_static_str()),
    ]);
    let outcome = dispatch_rest_tool(tool, request, upstream_limits, req_ctx, &mut span).await;
    if let Some(code) = outcome_failure_code(&outcome) {
        span.set_attribute(KeyValue::new("mcp.error.code", code));
        span.set_error(code);
    }
    span.complete();
    record_tool_invocation(&tool.conf.name, started, &outcome);
    outcome
}

async fn dispatch_rest_tool(
    tool: &StdArc<ToolEntry>,
    mut request: Request<OrionRequestBody>,
    upstream_limits: &UpstreamLimits,
    req_ctx: &RequestCtx,
    span: &mut orion_tracing::http_tracer::ScopedClientSpan,
) -> ToolInvocationOutcome {
    let UpstreamBackend::Rest { cluster, upstream_policy, .. } = &tool.conf.backend else {
        unreachable!("invoke_rest_tool called for a non-REST tool")
    };

    if let Some(authority) = upstream_policy.authority.as_ref() {
        let Ok(host) = HeaderValue::from_str(authority.as_str()) else {
            return ToolInvocationOutcome::Failure(ToolInvocationFailure {
                code: ToolInvocationErrorCode::UpstreamError,
                message: "The configured upstream authority is invalid".to_owned(),
                status: None,
                retryable: false,
            });
        };
        request.headers_mut().insert(HOST, host);
    } else {
        request.headers_mut().remove(HOST);
    }

    if let Some(request_id) = req_ctx.propagated_request_id() {
        request.headers_mut().insert(X_REQUEST_ID, request_id.clone());
    } else {
        request.headers_mut().remove(X_REQUEST_ID);
    }
    let cluster_specifier = ClusterSpecifier::Cluster(cluster.clone());
    let acquired = match acquire_http_upstream(
        &cluster_specifier,
        &request,
        &[],
        req_ctx.conn.downstream_peer_address(),
        RoutingPriority::Default,
    ) {
        Ok(acquired) => acquired,
        Err(error) => return ToolInvocationOutcome::Failure(acquire_failure(&tool.conf.name, &error)),
    };

    if upstream_policy.authority.is_none() {
        let endpoint_authority = acquired.channels().upstream_authority().as_str();
        let Ok(host) = HeaderValue::from_str(endpoint_authority) else {
            return ToolInvocationOutcome::Failure(ToolInvocationFailure {
                code: ToolInvocationErrorCode::UpstreamError,
                message: "The selected upstream endpoint is invalid".to_owned(),
                status: None,
                retryable: false,
            });
        };
        request.headers_mut().insert(HOST, host);
    }
    *request.version_mut() = acquired.channels().http_version().into();
    span.set_http_request(&request);
    span.set_endpoint(acquired.cluster_id(), acquired.channels().upstream_authority().as_str());
    span.inject_headers(request.headers_mut());

    if let Some(response) = http_modifiers::apply_preflight_functions(&mut request) {
        drop(acquired);
        span.set_http_status(response.status());
        return response_to_outcome(tool, response, upstream_limits.max_response_bytes).await;
    }

    let timeout = upstream_policy.timeout.unwrap_or(upstream_limits.timeout);
    let response = acquired
        .channels()
        .to_response(
            req_ctx,
            request,
            UpstreamCallOpts {
                route_timeout: Some(timeout),
                retry_policy: upstream_policy.retry_policy.as_ref(),
                priority: RoutingPriority::Default,
            },
        )
        .await;
    drop(acquired);

    match response {
        Ok(response) => {
            span.set_http_status(response.status());
            response_to_outcome(tool, response, upstream_limits.max_response_bytes).await
        },
        Err(error) => {
            let inferred = UpstreamError::try_infer_from(error.as_ref());
            if matches!(inferred, Some(UpstreamError::RouteTimeout | UpstreamError::PerTryTimeout)) {
                ToolInvocationOutcome::Failure(ToolInvocationFailure::upstream_timeout())
            } else {
                ToolInvocationOutcome::Failure(ToolInvocationFailure::upstream_error())
            }
        },
    }
}

fn acquire_failure(tool_name: &str, error: &AcquireHttpUpstreamError) -> ToolInvocationFailure {
    match error.kind() {
        AcquireHttpUpstreamErrorKind::CircuitBreakerOverflow => ToolInvocationFailure {
            code: ToolInvocationErrorCode::Overflow,
            message: "The upstream tool is temporarily at capacity".to_owned(),
            status: Some(StatusCode::SERVICE_UNAVAILABLE),
            retryable: true,
        },
        AcquireHttpUpstreamErrorKind::RoutingContext => {
            if let AcquireHttpUpstreamError::RoutingContext {
                cluster_id,
                source: RoutingContextError::MissingAuthority,
            } = error
            {
                warn!(
                    target: "mcp_gateway",
                    "tool '{tool_name}' call failed: cluster '{cluster_id}' requires a request authority \
                     (ORIGINAL_DST routing) but none was set; configure `upstream_policy.authority` for this tool"
                );
            }
            ToolInvocationFailure::upstream_error()
        },
        AcquireHttpUpstreamErrorKind::ClusterNotFound | AcquireHttpUpstreamErrorKind::Connection => {
            ToolInvocationFailure::upstream_error()
        },
    }
}

async fn response_to_outcome(
    tool: &ToolEntry,
    response: Response<OrionResponseBody>,
    max_response_bytes: usize,
) -> ToolInvocationOutcome {
    match collect_response_body(response, max_response_bytes).await {
        Ok((status, body)) => call_tool_outcome_from_upstream(tool, status, body),
        Err(failure) => ToolInvocationOutcome::Failure(failure),
    }
}

impl ToolInvocationOutcome {
    pub fn failure(failure: ToolInvocationFailure) -> Self {
        Self::Failure(failure)
    }

    pub fn into_call_tool_result(self) -> CallToolResult {
        match self {
            Self::Success(result) => result,
            Self::Failure(failure) => failure.into_call_tool_result(),
        }
    }
}

pub(crate) fn call_tool_outcome_from_upstream(
    tool: &ToolEntry,
    status: StatusCode,
    body: Bytes,
) -> ToolInvocationOutcome {
    record_tool_response_bytes(&tool.conf.name, body.len());

    if !status.is_success() {
        return ToolInvocationOutcome::Failure(ToolInvocationFailure {
            code: if status == StatusCode::GATEWAY_TIMEOUT {
                ToolInvocationErrorCode::UpstreamTimeout
            } else {
                ToolInvocationErrorCode::UpstreamError
            },
            message: extract_body_string(body, status),
            status: Some(status),
            retryable: status.is_server_error() || status == StatusCode::TOO_MANY_REQUESTS,
        });
    }

    if tool.output_schema_validator.is_none() {
        let content = RawContent::Text(RawTextContent { text: extract_body_string(body, status), meta: None });
        return ToolInvocationOutcome::Success(CallToolResult::success(vec![Annotated::new(content, None)]));
    }

    let value = match &tool.transcoder {
        TranscoderType::Rest(transcoder) => match transcoder.decode(body, status) {
            Ok(value) => value,
            Err(error) => {
                return invalid_output(format!("Failed to decode upstream response: {error}"), status);
            },
        },
        TranscoderType::FunctionGraph(_) => unreachable!("FunctionGraph backend is not implemented"),
        TranscoderType::NoTranscoder => Value::Null,
    };

    if let Err(error) = tool.validate_against_output_schema(&value) {
        return invalid_output(format!("Failed to validate upstream response against output schema: {error}"), status);
    }

    ToolInvocationOutcome::Success(CallToolResult::structured(value))
}

#[inline]
fn invalid_output(message: String, status: StatusCode) -> ToolInvocationOutcome {
    ToolInvocationOutcome::Failure(ToolInvocationFailure {
        code: ToolInvocationErrorCode::InvalidOutput,
        message,
        status: Some(status),
        retryable: false,
    })
}

#[inline]
fn extract_body_string(body: Bytes, status: StatusCode) -> String {
    if body.is_empty() {
        return if status == StatusCode::OK {
            "OK".to_owned()
        } else {
            format!("Upstream Error: {}", status.canonical_reason().unwrap_or("Unknown"))
        };
    }
    // `Bytes -> Vec<u8>` reuses the upstream buffer when uniquely owned
    // (the common case: fresh `collect().to_bytes()`), so the success path
    // builds the `String` with zero copies instead of `from_utf8_lossy` + `String::from`.
    match String::from_utf8(body.into()) {
        Ok(text) => text,
        Err(error) => String::from_utf8_lossy(error.as_bytes()).into_owned(),
    }
}

#[inline]
pub(crate) fn outcome_failure_code(outcome: &ToolInvocationOutcome) -> Option<&'static str> {
    match outcome {
        ToolInvocationOutcome::Success(result) if result.is_error == Some(true) => {
            Some(ToolInvocationErrorCode::UpstreamError.as_str())
        },
        ToolInvocationOutcome::Success(_) => None,
        ToolInvocationOutcome::Failure(failure) => Some(failure.code.as_str()),
    }
}

#[cfg(feature = "metrics")]
pub(crate) fn record_tool_invocation(tool_name: &str, started: Instant, outcome: &ToolInvocationOutcome) {
    let shard_id = get_shard_id!();
    let elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    let static_tool_name = tool_name.to_static_str();
    let tool_attrs = &[KeyValue::new("tool", static_tool_name)];
    with_metric!(mcp_metrics::TOOL_RQ_TOTAL, add, 1, shard_id, tool_attrs);
    with_histogram!(mcp_metrics::TOOL_RQ_TIME, record, elapsed_ms, shard_id, tool_attrs);
    if let Some(error_code) = outcome_failure_code(outcome) {
        with_metric!(
            mcp_metrics::TOOL_RQ_FAILURES_TOTAL,
            add,
            1,
            shard_id,
            &[KeyValue::new("tool", static_tool_name), KeyValue::new("error", error_code)]
        );
    }
}

#[cfg(not(feature = "metrics"))]
pub(crate) fn record_tool_invocation(_tool_name: &str, _started: Instant, _outcome: &ToolInvocationOutcome) {}

#[cfg(feature = "metrics")]
pub(crate) fn record_tool_response_bytes(tool_name: &str, bytes: usize) {
    let bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
    with_histogram!(
        mcp_metrics::TOOL_RESPONSE_BYTES,
        record,
        bytes,
        get_shard_id!(),
        &[KeyValue::new("tool", tool_name.to_static_str())]
    );
}

#[cfg(not(feature = "metrics"))]
pub(crate) fn record_tool_response_bytes(_tool_name: &str, _bytes: usize) {}

#[derive(Default)]
struct CountingWriter(usize);

impl io::Write for CountingWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0 = self.0.checked_add(buf.len()).ok_or_else(|| io::Error::other("serialized size overflow"))?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub(crate) fn serialized_json_size(value: &impl Serialize) -> Result<usize, serde_json::Error> {
    let mut writer = CountingWriter::default();
    serde_json::to_writer(&mut writer, value)?;
    Ok(writer.0)
}

pub(crate) async fn collect_response_body(
    response: Response<OrionResponseBody>,
    max_response_bytes: usize,
) -> Result<(StatusCode, Bytes), ToolInvocationFailure> {
    use http_body_util::{BodyExt, LengthLimitError, Limited};

    let status = response.status();
    let limited = Limited::new(response.into_body(), max_response_bytes);
    match limited.collect().await {
        Ok(body) => Ok((status, body.to_bytes())),
        Err(error) if error.downcast_ref::<LengthLimitError>().is_some() => {
            Err(ToolInvocationFailure::response_too_large(max_response_bytes))
        },
        Err(error)
            if matches!(
                error.downcast_ref::<TimeoutBodyError<PolyBodyError>>(),
                Some(TimeoutBodyError::TimedOut | TimeoutBodyError::BodyError(PolyBodyError::TimedOut))
            ) =>
        {
            Err(ToolInvocationFailure::upstream_timeout())
        },
        Err(_) => Err(ToolInvocationFailure {
            code: ToolInvocationErrorCode::UpstreamError,
            message: "The upstream tool response could not be read".to_owned(),
            status: Some(status),
            retryable: true,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{body::timeout_body::TimeoutBody, PolyBody};
    use http_body::Frame;
    use http_body_util::StreamBody;
    use tokio_stream::wrappers::ReceiverStream;

    #[test]
    fn counting_writer_matches_compact_json() {
        let value = json!({"nested": ["escaped\ntext", "é", {"emoji": "🦀"}], "number": 42});
        assert_eq!(serialized_json_size(&value).unwrap(), serde_json::to_vec(&value).unwrap().len());
    }

    #[tokio::test]
    async fn response_body_timeout_is_reported_as_upstream_timeout() {
        let (_sender, receiver) = tokio::sync::mpsc::channel::<Result<Frame<Bytes>, crate::Error>>(1);
        let body = StreamBody::new(ReceiverStream::new(receiver));
        let response = Response::builder()
            .status(StatusCode::OK)
            .body(TimeoutBody::new(Some(std::time::Duration::from_millis(1)), PolyBody::from(body)).into())
            .unwrap();

        let failure = collect_response_body(response, 1024).await.unwrap_err();
        assert_eq!(failure.code, ToolInvocationErrorCode::UpstreamTimeout);
        assert_eq!(failure.status, Some(StatusCode::GATEWAY_TIMEOUT));
    }
}
