// Copyright 2025-2026 The arion-gateway Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Compiled REST tool backend: request rendering, upstream I/O, and response interpretation.

use std::{borrow::Cow, time::Duration};

use arion_configuration::config::{
    cluster::ClusterSpecifier,
    network_filters::http_connection_manager::{
        RetryPolicy,
        http_filters::mcp_gateway::{HttpUpstreamPolicy, McpRestQueryParams, UpstreamLimits},
    },
};
use arion_http_header::X_REQUEST_ID;
use bytes::Bytes;
use http::{HeaderValue, Request, Response, StatusCode, header::HOST};
use http_body_util::Full;
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use rmcp::model::{CallToolResult, ContentBlock};
use serde_json::{Map, Value};
use smol_str::SmolStr;
use tracing::warn;
use upon::{Engine, Value as TemplateValue};

use super::{
    entry::ToolEntry,
    errors::{CallToolError, ToolBuilderError, ToolInvocationErrorCode, ToolInvocationFailure, ToolInvocationOutcome},
    invoke::{Invocation, ToolCallContext},
};
use crate::{
    ArionRequestBody, ArionResponseBody, UpstreamCallOpts,
    body::{
        instrumented_body::InstrumentedBody,
        poly_body::{PolyBody, PolyBodyError},
        response_flags::BodyKind,
        timeout_body::{TimeoutBody, TimeoutBodyError},
    },
    clusters::{
        RoutingPriority,
        clusters_manager::RoutingContextError,
        http_upstream::{AcquireHttpUpstreamError, AcquireHttpUpstreamErrorKind, acquire_http_upstream},
    },
    event_error::UpstreamError,
    listeners::http_connection_manager::{RequestHandler, http_modifiers},
};

pub(super) const DEFAULT_USER_AGENT: &str = concat!("arion/", env!("CARGO_PKG_VERSION"));
const PATH_TEMPLATE_NAME: &str = "path";
const BODY_TEMPLATE_NAME: &str = "body";
const URI_COMPONENT_SET: &AsciiSet = &NON_ALPHANUMERIC.remove(b'-').remove(b'_').remove(b'.').remove(b'~');

#[derive(Debug)]
enum CompiledTemplate<T> {
    Static(T),
    Dynamic,
}

#[derive(Debug)]
pub(super) struct RestBackend {
    method: http::Method,
    query_params: Vec<McpRestQueryParams>,
    path: CompiledTemplate<SmolStr>,
    body: Option<CompiledTemplate<Bytes>>,
    body_json: Option<SmolStr>,
    template_engine: Engine<'static>,
    cluster: SmolStr,
    authority: Option<HeaderValue>,
    timeout: Duration,
    retry_policy: Option<RetryPolicy>,
    max_response_bytes: usize,
}

#[derive(Debug, thiserror::Error)]
enum RequestBuildError {
    #[error("template render error: {0}")]
    Template(#[from] upon::Error),
    #[error("HTTP request error: {0}")]
    Http(#[from] http::Error),
}

impl RestBackend {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn compile(
        method: http::Method,
        path: SmolStr,
        query_params: Vec<McpRestQueryParams>,
        cluster: SmolStr,
        policy: HttpUpstreamPolicy,
        body_template: Option<SmolStr>,
        body_json: Option<SmolStr>,
        limits: &UpstreamLimits,
    ) -> Result<Self, ToolBuilderError> {
        if body_template.is_some() && body_json.is_some() {
            return Err(ToolBuilderError::ConflictingBodyModes);
        }
        let mut template_engine = Engine::new();
        template_engine.add_filter("encode_path_segment", encode_path_segment);
        let path = if path.contains("{{") {
            template_engine.add_template(PATH_TEMPLATE_NAME, path.to_string())?;
            CompiledTemplate::Dynamic
        } else if path.starts_with('/') {
            CompiledTemplate::Static(path)
        } else {
            CompiledTemplate::Static(format!("/{path}").into())
        };
        let body = match body_template {
            None => None,
            Some(template) if template.contains("{{") => {
                template_engine.add_template(BODY_TEMPLATE_NAME, template.to_string())?;
                Some(CompiledTemplate::Dynamic)
            },
            Some(template) => Some(CompiledTemplate::Static(Bytes::copy_from_slice(template.as_bytes()))),
        };
        let authority = policy
            .authority
            .map(|authority| HeaderValue::from_str(authority.as_str()))
            .transpose()
            .map_err(ToolBuilderError::InvalidUpstreamAuthority)?;

        Ok(Self {
            method,
            query_params,
            path,
            body,
            body_json,
            template_engine,
            cluster,
            authority,
            timeout: policy.timeout.unwrap_or(limits.timeout),
            retry_policy: policy.retry_policy,
            max_response_bytes: limits.max_response_bytes,
        })
    }

    pub(super) async fn invoke(
        &self,
        entry: &ToolEntry,
        arguments: Value,
        cx: ToolCallContext<'_>,
    ) -> Result<CallToolResult, CallToolError> {
        let empty_arguments = Map::new();
        let request =
            self.build_request(cx.headers, arguments.as_object().unwrap_or(&empty_arguments)).map_err(|error| {
                CallToolError::RequestEncoding { tool: entry.id.qualified_name(), reason: error.to_string() }
            })?;

        let tool_name = entry.tool.name.as_ref();
        let mut invocation = Invocation::start(cx.request, tool_name, "rest", &self.cluster);
        let outcome = match self.invoke_upstream(request, cx, &mut invocation, tool_name).await {
            Ok(response) => self.interpret_response(entry, response, &mut invocation).await,
            Err(failure) => Err(failure),
        };
        Ok(invocation.complete(outcome))
    }

    fn build_request(
        &self,
        http_headers: &http::HeaderMap,
        arguments: &Map<String, Value>,
    ) -> Result<Request<ArionRequestBody>, RequestBuildError> {
        let mut uri = String::with_capacity(
            match &self.path {
                CompiledTemplate::Static(path) => path.len(),
                CompiledTemplate::Dynamic => 64,
            } + self.query_params.len() * 32
                + 2,
        );
        match &self.path {
            CompiledTemplate::Static(path) => uri.push_str(path),
            CompiledTemplate::Dynamic => {
                let rendered_path = self.render_template(PATH_TEMPLATE_NAME, arguments)?;
                if !rendered_path.starts_with('/') {
                    uri.push('/');
                }
                uri.push_str(&rendered_path);
            },
        }
        append_query_string(&mut uri, &self.query_params, arguments);

        let user_agent =
            http_headers.get(http::header::USER_AGENT).and_then(|ua| ua.to_str().ok()).unwrap_or(DEFAULT_USER_AGENT);
        let mut builder = Request::builder().method(&self.method).uri(uri).header(http::header::USER_AGENT, user_agent);
        let rendered = match &self.body {
            Some(CompiledTemplate::Static(body)) => Some(body.clone()),
            Some(CompiledTemplate::Dynamic) => Some(Bytes::from(self.render_template(BODY_TEMPLATE_NAME, arguments)?)),
            None => self
                .body_json
                .as_ref()
                .and_then(|source| resolve_variable(source, arguments))
                .map(|value| Bytes::from(value.to_string())),
        };
        let body = match rendered {
            Some(rendered) => {
                builder = builder.header(http::header::CONTENT_TYPE, "application/json");
                InstrumentedBody::new(
                    BodyKind::Request,
                    TimeoutBody::new(None, PolyBody::from(Full::new(rendered))),
                    None,
                    |_, _, _, _| {},
                )
            },
            None => InstrumentedBody::default(),
        };
        Ok(builder.body(body)?)
    }

    async fn invoke_upstream(
        &self,
        mut request: Request<ArionRequestBody>,
        cx: ToolCallContext<'_>,
        invocation: &mut Invocation<'_>,
        tool_name: &str,
    ) -> Result<Response<ArionResponseBody>, ToolInvocationFailure> {
        match &self.authority {
            Some(authority) => {
                request.headers_mut().insert(HOST, authority.clone());
            },
            None => {
                request.headers_mut().remove(HOST);
            },
        }
        match cx.request.propagated_request_id() {
            Some(request_id) => {
                request.headers_mut().insert(X_REQUEST_ID, request_id.clone());
            },
            None => {
                request.headers_mut().remove(X_REQUEST_ID);
            },
        }

        let cluster = ClusterSpecifier::Cluster(self.cluster.clone().into());
        let acquired = acquire_http_upstream(
            &cluster,
            &request,
            &[],
            cx.request.conn.downstream_peer_address(),
            RoutingPriority::Default,
        )
        .map_err(|error| map_acquisition_failure(tool_name, &error))?;

        if self.authority.is_none() {
            let authority = acquired.channels().upstream_authority().as_str();
            let host = HeaderValue::from_str(authority).map_err(|_invalid_authority| ToolInvocationFailure {
                code: ToolInvocationErrorCode::UpstreamError,
                message: "The selected upstream endpoint is invalid".to_owned(),
                status: None,
                retryable: false,
            })?;
            request.headers_mut().insert(HOST, host);
        }
        *request.version_mut() = acquired.channels().http_version().into();
        invocation.span().set_http_request(&request);
        invocation.span().set_endpoint(acquired.cluster_name(), acquired.channels().upstream_authority().as_str());
        invocation.span().inject_headers(request.headers_mut());

        if let Some(response) = http_modifiers::apply_preflight_functions(&mut request) {
            return Ok(response);
        }

        let response = acquired
            .channels()
            .to_response(
                cx.request,
                request,
                UpstreamCallOpts {
                    route_timeout: Some(self.timeout),
                    retry_policy: self.retry_policy.as_ref(),
                    priority: RoutingPriority::Default,
                },
            )
            .await;
        drop(acquired);

        response.map_err(|error| match error.as_upstream_error() {
            Some(UpstreamError::RouteTimeout | UpstreamError::PerTryTimeout) => {
                ToolInvocationFailure::upstream_timeout()
            },
            _ => ToolInvocationFailure::upstream_error(),
        })
    }

    async fn interpret_response(
        &self,
        entry: &ToolEntry,
        response: Response<ArionResponseBody>,
        invocation: &mut Invocation<'_>,
    ) -> ToolInvocationOutcome {
        let status = response.status();
        invocation.span().set_http_status(status);
        let body = collect_response_body(response, self.max_response_bytes).await?;
        invocation.record_response_bytes(body.len());

        if !status.is_success() {
            return Err(ToolInvocationFailure {
                code: if status == StatusCode::GATEWAY_TIMEOUT {
                    ToolInvocationErrorCode::UpstreamTimeout
                } else {
                    ToolInvocationErrorCode::UpstreamError
                },
                message: response_text(body, status),
                status: Some(status),
                retryable: status.is_server_error() || status == StatusCode::TOO_MANY_REQUESTS,
            });
        }

        if entry.output_schema_validator.is_none() {
            let content = ContentBlock::text(response_text(body, status));
            return Ok(CallToolResult::success(vec![content]));
        }

        let value = if body.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&body).map_err(|error| ToolInvocationFailure {
                code: ToolInvocationErrorCode::InvalidOutput,
                message: format!(
                    "Failed to decode upstream response: Failed to parse upstream response as JSON: {error}"
                ),
                status: Some(status),
                retryable: false,
            })?
        };
        entry.validate_against_output_schema(&value).map_err(|error| ToolInvocationFailure {
            code: ToolInvocationErrorCode::InvalidOutput,
            message: format!("Failed to validate upstream response against output schema: {error}"),
            status: Some(status),
            retryable: false,
        })?;
        Ok(CallToolResult::structured(value))
    }

    fn render_template(&self, name: &str, arguments: &Map<String, Value>) -> Result<String, upon::Error> {
        self.template_engine.template(name).render(arguments).to_string()
    }
}

async fn collect_response_body(
    response: Response<ArionResponseBody>,
    max_bytes: usize,
) -> Result<Bytes, ToolInvocationFailure> {
    use http_body_util::{BodyExt, LengthLimitError, Limited};

    let status = response.status();
    match Limited::new(response.into_body(), max_bytes).collect().await {
        Ok(body) => Ok(body.to_bytes()),
        Err(error) if error.downcast_ref::<LengthLimitError>().is_some() => {
            Err(ToolInvocationFailure::response_too_large(max_bytes))
        },
        Err(error) if response_body_timed_out(error.as_ref()) => Err(ToolInvocationFailure::upstream_timeout()),
        Err(_) => Err(ToolInvocationFailure {
            code: ToolInvocationErrorCode::UpstreamError,
            message: "The upstream tool response could not be read".to_owned(),
            status: Some(status),
            retryable: true,
        }),
    }
}

fn map_acquisition_failure(tool_name: &str, error: &AcquireHttpUpstreamError) -> ToolInvocationFailure {
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
            } = &error
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

fn response_body_timed_out(error: &(dyn std::error::Error + Send + Sync + 'static)) -> bool {
    matches!(
        error.downcast_ref::<TimeoutBodyError<PolyBodyError>>(),
        Some(TimeoutBodyError::TimedOut | TimeoutBodyError::BodyError(PolyBodyError::TimedOut))
    )
}

#[inline]
fn response_text(body: Bytes, status: StatusCode) -> String {
    if body.is_empty() {
        return if status == StatusCode::OK {
            "OK".to_owned()
        } else {
            format!("Upstream Error: {}", status.canonical_reason().unwrap_or("Unknown"))
        };
    }
    match String::from_utf8(body.into()) {
        Ok(text) => text,
        Err(error) => String::from_utf8_lossy(error.as_bytes()).into_owned(),
    }
}

fn encode_path_segment(value: &TemplateValue) -> Result<String, &'static str> {
    let value = match value {
        TemplateValue::None => Cow::Borrowed("null"),
        TemplateValue::Bool(value) => Cow::Owned(value.to_string()),
        TemplateValue::Integer(value) => Cow::Owned(value.to_string()),
        TemplateValue::Float(value) => Cow::Owned(value.to_string()),
        TemplateValue::String(value) => Cow::Borrowed(value.as_str()),
        TemplateValue::List(_) | TemplateValue::Map(_) => return Err("path parameters must be scalar values"),
    };
    Ok(utf8_percent_encode(&value, URI_COMPONENT_SET).to_string())
}

/// Resolves a variable path like "user.name" against the arguments map.
fn resolve_variable<'a>(path: &str, arguments: &'a serde_json::Map<String, Value>) -> Option<&'a Value> {
    let mut parts = path.split('.');

    // Get the first part directly from the map to avoid cloning the entire arguments object
    let mut current = parts.next().and_then(|first| arguments.get(first));

    // Iterate through the rest of the path without allocating a Vec
    for part in parts {
        current = current.and_then(|v| v.as_object()).and_then(|map| map.get(part));
    }

    current
}

fn format_variable(value: &Value) -> Cow<'_, str> {
    // Format output: avoid quotes for strings, use default to_string() for other JSON types
    match value {
        Value::String(s) => Cow::Borrowed(s.as_str()),
        Value::Null => Cow::Borrowed("null"),
        Value::Bool(true) => Cow::Borrowed("true"),
        Value::Bool(false) => Cow::Borrowed("false"),
        v => Cow::Owned(v.to_string()),
    }
}

/// Appends a query string from query params configuration with variable substitution.
fn append_query_string(
    uri: &mut String,
    query_params: &[McpRestQueryParams],
    arguments: &serde_json::Map<String, Value>,
) {
    let mut first = true;

    for param in query_params {
        let values = match resolve_variable(&param.source, arguments) {
            Some(Value::Array(values)) => values.as_slice(),
            Some(value) => std::slice::from_ref(value),
            None => continue,
        };
        for value in values {
            if first {
                uri.push('?');
                first = false;
            } else {
                uri.push('&');
            }
            uri.extend(utf8_percent_encode(&param.name, URI_COMPONENT_SET));
            uri.push('=');
            uri.extend(utf8_percent_encode(&format_variable(value), URI_COMPONENT_SET));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn create_http_request() -> http::Request<ArionRequestBody> {
        http::Request::builder().method(http::Method::GET).uri("/test").body(ArionRequestBody::default()).unwrap()
    }

    fn create_backend(
        method: http::Method,
        path: String,
        query_params: Vec<McpRestQueryParams>,
        _has_body_template: bool,
        body_template: Option<String>,
    ) -> RestBackend {
        RestBackend::compile(
            method,
            path.into(),
            query_params,
            "test_cluster".into(),
            HttpUpstreamPolicy::default(),
            body_template.map(Into::into),
            None,
            &UpstreamLimits::default(),
        )
        .unwrap()
    }

    #[test]
    fn malformed_dynamic_templates_return_compilation_errors() {
        for (path, body_template) in [("/api/{{", None), ("/api", Some("{{"))] {
            let error = RestBackend::compile(
                http::Method::POST,
                path.into(),
                Vec::new(),
                "test_cluster".into(),
                HttpUpstreamPolicy::default(),
                body_template.map(Into::into),
                None,
                &UpstreamLimits::default(),
            )
            .unwrap_err();
            assert!(matches!(error, ToolBuilderError::FailedCompilingTemplate(_)), "{error:?}");
        }
    }

    #[tokio::test]
    async fn response_body_timeout_is_reported_as_upstream_timeout() {
        let (body, _bridge) = crate::body::channel_body::ChannelBody::new(
            http_body_util::Empty::<Bytes>::new(),
            None,
            std::num::NonZeroUsize::new(1).unwrap(),
        );
        let response = Response::builder()
            .status(StatusCode::OK)
            .body(TimeoutBody::new(Some(Duration::from_millis(1)), PolyBody::from(body)).into())
            .unwrap();

        let failure = collect_response_body(response, 1024).await.unwrap_err();
        assert_eq!(failure.code, ToolInvocationErrorCode::UpstreamTimeout);
        assert_eq!(failure.status, Some(StatusCode::GATEWAY_TIMEOUT));
    }

    #[test]
    fn test_body_template_simple_substitution() {
        // Test simple variable substitution in body template (JSON format)
        let mut args = serde_json::Map::new();
        args.insert("username".to_owned(), json!("john_doe"));
        args.insert("age".to_owned(), json!(30));
        let http_request = create_http_request();
        let query_params: Vec<McpRestQueryParams> = vec![];

        let body_template = r#"{"name": "{{username}}", "years": {{age}}}"#.to_owned();
        let backend =
            create_backend(http::Method::POST, "/api/users".to_owned(), query_params, true, Some(body_template));

        let result = backend.build_request(http_request.headers(), &args);
        assert!(result.is_ok(), "Expected successful encoding but got: {result:?}");

        let request = result.unwrap();
        assert_eq!(request.method(), http::Method::POST);
        assert_eq!(request.uri(), "/api/users");
        assert_eq!(request.headers().get(http::header::CONTENT_TYPE).unwrap(), "application/json");
    }

    #[tokio::test]
    async fn body_json_serializes_the_resolved_argument() {
        let mut args = serde_json::Map::new();
        args.insert("body".to_owned(), json!({"user": {"name": "john"}, "tags": ["a", "b"], "count": 3}));
        let http_request = create_http_request();

        let mut backend = create_backend(http::Method::POST, "/api/users".to_owned(), vec![], false, None);
        backend.body_json = Some("body".into());

        let request = backend.build_request(http_request.headers(), &args).unwrap();
        assert_eq!(request.headers().get(http::header::CONTENT_TYPE).unwrap(), "application/json");
        let bytes = http_body_util::BodyExt::collect(request.into_body()).await.unwrap().to_bytes();
        let sent: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(sent, args["body"]);
    }

    #[tokio::test]
    async fn body_json_sends_no_body_when_the_argument_is_absent() {
        let args = serde_json::Map::new();
        let http_request = create_http_request();

        let mut backend = create_backend(http::Method::POST, "/api/users".to_owned(), vec![], false, None);
        backend.body_json = Some("body".into());

        let request = backend.build_request(http_request.headers(), &args).unwrap();
        assert!(request.headers().get(http::header::CONTENT_TYPE).is_none());
        let bytes = http_body_util::BodyExt::collect(request.into_body()).await.unwrap().to_bytes();
        assert!(bytes.is_empty());
    }

    #[test]
    fn compile_rejects_body_template_and_body_json_together() {
        let result = RestBackend::compile(
            http::Method::POST,
            "/api/users".into(),
            vec![],
            "test_cluster".into(),
            HttpUpstreamPolicy::default(),
            Some(r#"{"a": "{{a}}"}"#.into()),
            Some("body".into()),
            &UpstreamLimits::default(),
        );
        assert!(matches!(result.unwrap_err(), ToolBuilderError::ConflictingBodyModes));
    }

    #[test]
    fn test_body_template_nested_path() {
        // Test nested path access in body template (JSON format)
        let mut args = serde_json::Map::new();
        let mut user = serde_json::Map::new();
        user.insert("name".to_owned(), json!("john"));
        user.insert("email".to_owned(), json!("john@example.com"));
        args.insert("user".to_owned(), Value::Object(user));
        let http_request = create_http_request();
        let query_params: Vec<McpRestQueryParams> = vec![];

        let body_template = r#"{"username": "{{user.name}}", "contact": "{{user.email}}}"#.to_owned();
        let backend =
            create_backend(http::Method::POST, "/api/users".to_owned(), query_params, true, Some(body_template));

        let result = backend.build_request(http_request.headers(), &args);
        assert!(result.is_ok(), "Expected successful encoding but got: {result:?}");
    }

    #[test]
    fn test_body_template_missing_variable() {
        // Test that missing variables are replaced with "null" in JSON template
        let mut args = serde_json::Map::new();
        args.insert("username".to_owned(), json!("john_doe"));
        let http_request = create_http_request();
        let query_params: Vec<McpRestQueryParams> = vec![];

        let body_template = r#"{"name": "{{username}}", "missing": {{nonexistent}}}"#.to_owned();
        let backend =
            create_backend(http::Method::POST, "/api/users".to_owned(), query_params, true, Some(body_template));

        // Missing variables now cause template render errors (schema validation should catch these)
        let result = backend.build_request(http_request.headers(), &args);
        assert!(result.is_err(), "Expected error for missing variable but got: {result:?}");
    }

    #[test]
    fn test_body_template_complex_types() {
        // Test that arrays and objects can be rendered in JSON template
        // Note: upon template engine requires explicit handling of complex types
        let mut args = serde_json::Map::new();
        args.insert("tags".to_owned(), json!(["rust", "mcp", "api"]));
        let mut metadata = serde_json::Map::new();
        metadata.insert("version".to_owned(), json!("1.0"));
        args.insert("meta".to_owned(), Value::Object(metadata));
        let http_request = create_http_request();
        let query_params: Vec<McpRestQueryParams> = vec![];

        // upon doesn't have a json filter - complex types need to be handled differently
        // This test verifies that the template engine is strict about type formatting
        let body_template = r#"{"tags": {{tags}}, "metadata": {{meta}}}"#.to_owned();
        let backend =
            create_backend(http::Method::POST, "/api/data".to_owned(), query_params, true, Some(body_template));

        // Complex types without proper formatting will cause render errors
        let result = backend.build_request(http_request.headers(), &args);
        assert!(result.is_err(), "Expected error for unformatted complex types: {result:?}");
    }

    fn render_template_with_engine(template_str: String, arguments: &serde_json::Map<String, Value>) -> String {
        let mut engine: Engine<'static> = upon::Engine::new();
        engine.add_template("test", template_str).unwrap();
        engine.template("test").render(arguments).to_string().unwrap()
    }

    #[test]
    fn test_render_template_simple() {
        // Test JSON template rendering
        let mut args = serde_json::Map::new();
        args.insert("name".to_owned(), json!("Alice"));
        args.insert("count".to_owned(), json!(42));

        let template = r#"{"user": "{{name}}", "value": {{count}}}"#.to_owned();
        let result = render_template_with_engine(template, &args);

        assert_eq!(result, r#"{"user": "Alice", "value": 42}"#);
    }

    #[test]
    fn test_render_template_non_json() {
        // Test that templates work with non-JSON formats (plain text, XML, etc.)
        let mut args = serde_json::Map::new();
        args.insert("username".to_owned(), json!("john_doe"));
        args.insert("action".to_owned(), json!("login"));

        // Plain text template
        let template = "User {{username}} performed {{action}}".to_owned();
        let result = render_template_with_engine(template, &args);
        assert_eq!(result, "User john_doe performed login");

        // XML template
        let xml_template = r#"<user><name>{{username}}</name><action>{{action}}</action></user>"#.to_owned();
        let result = render_template_with_engine(xml_template, &args);
        assert_eq!(result, r#"<user><name>john_doe</name><action>login</action></user>"#);

        // URL template
        let url_template = "/api/users/{{username}}/{{action}}".to_owned();
        let result = render_template_with_engine(url_template, &args);
        assert_eq!(result, "/api/users/john_doe/login");
    }

    #[test]
    fn test_render_template_sanitized_openapi_ident() {
        // OpenAPI expansion sanitizes parameter names to [A-Za-z0-9_] with a
        // '_' prefix for leading digits; upon must accept such identifiers.
        let mut args = serde_json::Map::new();
        args.insert("_5pet_id".to_owned(), json!("42"));

        let template = "/api/{{_5pet_id}}".to_owned();
        assert_eq!(render_template_with_engine(template, &args), "/api/42");
    }

    #[test]
    fn test_render_template_nested() {
        // Test nested variable access in JSON template
        let mut args = serde_json::Map::new();
        let mut user = serde_json::Map::new();
        user.insert("name".to_owned(), json!("Bob"));
        user.insert("id".to_owned(), json!(123));
        args.insert("user".to_owned(), Value::Object(user));

        let template = r#"{"username": "{{user.name}}", "user_id": {{user.id}}}"#.to_owned();
        let result = render_template_with_engine(template, &args);

        assert_eq!(result, r#"{"username": "Bob", "user_id": 123}"#);
    }

    #[test]
    fn test_render_template_missing_var() {
        // In reality, missing variables should be caught by schema validation.
        // This test verifies that upon leaves missing variables as-is (template error).
        let args = serde_json::Map::new();

        let template = r#"{"value": {{missing}}}"#.to_owned();
        // Missing variables now cause template render errors (schema validation should catch these)
        let result = std::panic::catch_unwind(|| render_template_with_engine(template, &args));
        assert!(result.is_err(), "Expected panic for missing variable");
    }

    #[test]
    fn test_render_template_boolean_and_null() {
        // Test boolean and null values in JSON template
        let mut args = serde_json::Map::new();
        args.insert("active".to_owned(), json!(true));
        args.insert("deleted".to_owned(), json!(false));
        args.insert("empty".to_owned(), Value::Null);

        let template = r#"{"is_active": {{active}}, "is_deleted": {{deleted}}, "empty_field": {{empty}}}"#.to_owned();
        let result = render_template_with_engine(template, &args);

        // Note: upon renders null as empty string, not "null"
        // This is acceptable since schema validation ensures required fields exist
        assert_eq!(result, r#"{"is_active": true, "is_deleted": false, "empty_field": }"#);
    }

    #[test]
    fn test_resolve_variable_simple() {
        let mut args = serde_json::Map::new();
        args.insert("name".to_owned(), json!("Test"));

        assert_eq!(resolve_variable("name", &args), Some(&json!("Test")));
    }

    #[test]
    fn test_resolve_variable_nested() {
        let mut args = serde_json::Map::new();
        let mut nested = serde_json::Map::new();
        nested.insert("value".to_owned(), json!(42));
        args.insert("config".to_owned(), Value::Object(nested));

        assert_eq!(resolve_variable("config.value", &args), Some(&json!(42)));
    }

    #[test]
    fn test_resolve_variable_missing() {
        let args = serde_json::Map::new();

        assert_eq!(resolve_variable("missing", &args), None);
    }

    #[test]
    fn test_resolve_variable_not_an_object() {
        let mut args = serde_json::Map::new();
        args.insert("name".to_owned(), json!("value"));

        // Trying to access a field on a string should return null
        assert_eq!(resolve_variable("name.invalid", &args), None);
    }

    #[test]
    fn test_path_template_substitution() {
        // Test variable substitution in path template
        let mut args = serde_json::Map::new();
        args.insert("user_id".to_owned(), json!("12345"));
        args.insert("action".to_owned(), json!("profile"));
        let http_request = create_http_request();

        let path_template = "/api/users/{{user_id}}/{{action}}";
        let query_params: Vec<McpRestQueryParams> = vec![];
        let backend = create_backend(http::Method::GET, path_template.to_owned(), query_params, false, None);

        let result = backend.build_request(http_request.headers(), &args);
        assert!(result.is_ok(), "Expected successful encoding but got: {result:?}");

        let request = result.unwrap();
        assert_eq!(request.uri(), "/api/users/12345/profile");
    }

    #[test]
    fn path_segment_filter_prevents_values_from_changing_the_uri_structure() {
        let mut args = serde_json::Map::new();
        let http_request = create_http_request();
        let backend = create_backend(
            http::Method::GET,
            "/api/pets/{{pet_id | encode_path_segment}}".to_owned(),
            vec![],
            false,
            None,
        );

        for (value, expected) in [
            (json!("../admin/records?all=true#top"), "..%2Fadmin%2Frecords%3Fall%3Dtrue%23top"),
            (json!("München 🚀"), "M%C3%BCnchen%20%F0%9F%9A%80"),
            (json!(42), "42"),
        ] {
            args.insert("pet_id".to_owned(), value);
            let request = backend.build_request(http_request.headers(), &args).unwrap();
            assert_eq!(request.uri().path(), format!("/api/pets/{expected}"));
        }
    }

    #[test]
    fn test_path_template_with_nested_args() {
        // Test nested path access in path template
        let mut args = serde_json::Map::new();
        let mut location = serde_json::Map::new();
        location.insert("city".to_owned(), json!("dublin"));
        args.insert("location".to_owned(), Value::Object(location));
        let http_request = create_http_request();

        let path_template = "/api/weather/{{location.city}}";
        let query_params: Vec<McpRestQueryParams> = vec![];
        let backend = create_backend(http::Method::GET, path_template.to_owned(), query_params, false, None);

        let result = backend.build_request(http_request.headers(), &args);
        assert!(result.is_ok(), "Expected successful encoding but got: {result:?}");

        let request = result.unwrap();
        assert_eq!(request.uri(), "/api/weather/dublin");
    }

    #[test]
    fn test_query_params_substitution() {
        // Test variable substitution in query params
        let mut args = serde_json::Map::new();
        args.insert("search".to_owned(), json!("rust language"));
        args.insert("limit".to_owned(), json!(10));
        let http_request = create_http_request();

        let query_params = vec![
            McpRestQueryParams { name: "q".into(), source: "search".into() },
            McpRestQueryParams { name: "limit".into(), source: "limit".into() },
        ];
        let backend = create_backend(http::Method::GET, "/api/search".to_owned(), query_params, false, None);

        let result = backend.build_request(http_request.headers(), &args);
        assert!(result.is_ok(), "Expected successful encoding but got: {result:?}");

        let request = result.unwrap();
        let uri = request.uri().to_string();
        assert!(uri.starts_with("/api/search?"), "URI should start with /api/search?: {uri}");
        assert!(uri.contains("q=rust%20language"), "URI should contain q=rust%20language: {uri}");
        assert!(uri.contains("limit=10"), "URI should contain limit=10: {uri}");
    }

    #[test]
    fn test_query_params_with_nested_source() {
        // Test nested path access in query params source
        let mut args = serde_json::Map::new();
        let mut coords = serde_json::Map::new();
        coords.insert("lat".to_owned(), json!("51.5074"));
        coords.insert("lon".to_owned(), json!("-0.1278"));
        args.insert("coordinates".to_owned(), Value::Object(coords));
        args.insert("forecast_days".to_owned(), json!(5));
        let http_request = create_http_request();

        let query_params = vec![
            McpRestQueryParams { name: "latitude".into(), source: "coordinates.lat".into() },
            McpRestQueryParams { name: "longitude".into(), source: "coordinates.lon".into() },
            McpRestQueryParams { name: "days".into(), source: "forecast_days".into() },
        ];
        let backend = create_backend(http::Method::GET, "/api/weather".to_owned(), query_params, false, None);

        let result = backend.build_request(http_request.headers(), &args);
        assert!(result.is_ok(), "Expected successful encoding but got: {result:?}");

        let request = result.unwrap();
        let uri = request.uri().to_string();
        assert!(uri.contains("latitude=51.5074"), "URI should contain latitude=51.5074: {uri}");
        assert!(uri.contains("longitude=-0.1278"), "URI should contain longitude=-0.1278: {uri}");
        assert!(uri.contains("days=5"), "URI should contain days=5: {uri}");
    }

    #[test]
    fn test_query_params_skips_null_values() {
        // Test that null values are skipped in query params
        let mut args = serde_json::Map::new();
        args.insert("city".to_owned(), json!("London"));
        // "country" is not set, so it should be skipped
        let http_request = create_http_request();

        let query_params = vec![
            McpRestQueryParams { name: "city".into(), source: "city".into() },
            McpRestQueryParams { name: "country".into(), source: "missing_country".into() },
        ];
        let backend = create_backend(http::Method::GET, "/api/locations".to_owned(), query_params, false, None);

        let result = backend.build_request(http_request.headers(), &args);
        assert!(result.is_ok(), "Expected successful encoding but got: {result:?}");

        let request = result.unwrap();
        let uri = request.uri().to_string();
        assert_eq!(uri, "/api/locations?city=London", "URI should only contain city param: {uri}");
    }

    #[test]
    fn test_path_and_query_params_combined() {
        // Test both path template and query params together
        let mut args = serde_json::Map::new();
        args.insert("user_id".to_owned(), json!("42"));
        let mut filters = serde_json::Map::new();
        filters.insert("status".to_owned(), json!("active"));
        args.insert("filters".to_owned(), Value::Object(filters));
        let http_request = create_http_request();

        let query_params = vec![McpRestQueryParams { name: "status".into(), source: "filters.status".into() }];
        let backend =
            create_backend(http::Method::GET, "/api/users/{{user_id}}/orders".to_owned(), query_params, false, None);

        let result = backend.build_request(http_request.headers(), &args);
        assert!(result.is_ok(), "Expected successful encoding but got: {result:?}");

        let request = result.unwrap();
        let uri = request.uri().to_string();
        assert_eq!(uri, "/api/users/42/orders?status=active", "URI should have path and query: {uri}");
    }

    #[test]
    fn test_render_path_template_simple() {
        let mut args = serde_json::Map::new();
        args.insert("id".to_owned(), json!("123"));

        let template = "/api/items/{{id}}".to_owned();
        let result = render_template_with_engine(template, &args);

        assert_eq!(result, "/api/items/123");
    }

    #[test]
    fn test_build_query_string_simple() {
        let mut args = serde_json::Map::new();
        args.insert("lat".to_owned(), json!("53.3498"));
        args.insert("lon".to_owned(), json!("-6.2603"));

        let query_params = vec![
            McpRestQueryParams { name: "lat".into(), source: "lat".into() },
            McpRestQueryParams { name: "lon".into(), source: "lon".into() },
        ];

        let mut result = String::new();
        append_query_string(&mut result, &query_params, &args);
        assert!(result.contains("lat=53.3498"), "Result should contain lat=53.3498: {result}");
        assert!(result.contains("lon=-6.2603"), "Result should contain lon=-6.2603: {result}");
    }

    #[test]
    fn test_build_query_string_encoding() {
        // Test special characters are properly encoded
        let mut args = serde_json::Map::new();
        args.insert("space".to_owned(), json!("hello world"));
        args.insert("amp".to_owned(), json!("foo&bar"));
        args.insert("equal".to_owned(), json!("a=b"));
        args.insert("slash".to_owned(), json!("test/value"));

        let query_params = vec![
            McpRestQueryParams { name: "space".into(), source: "space".into() },
            McpRestQueryParams { name: "amp".into(), source: "amp".into() },
            McpRestQueryParams { name: "equal".into(), source: "equal".into() },
            McpRestQueryParams { name: "slash".into(), source: "slash".into() },
        ];

        let mut result = String::new();
        append_query_string(&mut result, &query_params, &args);
        assert!(result.contains("space=hello%20world"), "Space should be encoded as %20: {result}");
        assert!(result.contains("amp=foo%26bar"), "& should be encoded: {result}");
        assert!(result.contains("equal=a%3Db"), "= should be encoded: {result}");
        assert!(result.contains("slash=test%2Fvalue"), "/ should be encoded: {result}");
    }

    #[test]
    fn query_arrays_use_repeated_parameters() {
        let args = serde_json::Map::from_iter([("tag".to_owned(), json!(["rust", "mcp gateway"]))]);
        let query_params = vec![McpRestQueryParams { name: "tag".into(), source: "tag".into() }];

        let mut result = String::new();
        append_query_string(&mut result, &query_params, &args);

        assert_eq!(result, "?tag=rust&tag=mcp%20gateway");
    }

    #[test]
    fn test_build_query_string_encoding_names_and_non_ascii() {
        let mut args = serde_json::Map::new();
        args.insert("city".to_owned(), json!("München"));
        args.insert("emoji".to_owned(), json!("🚀"));

        let query_params = vec![
            McpRestQueryParams { name: "city name".into(), source: "city".into() },
            McpRestQueryParams { name: "emoji param".into(), source: "emoji".into() },
        ];

        let mut result = String::new();
        append_query_string(&mut result, &query_params, &args);
        assert!(
            result.contains("city%20name=M%C3%BCnchen"),
            "Non-ASCII and spaces in names should be encoded: {result}"
        );
        assert!(result.contains("emoji%20param=%F0%9F%9A%80"), "Emojis should be encoded: {result}");
    }

    #[test]
    fn test_build_query_string_unreserved_chars() {
        // Test RFC 3986 unreserved characters are not encoded
        let mut args = serde_json::Map::new();
        args.insert("alpha".to_owned(), json!("ABCxyz"));
        args.insert("numeric".to_owned(), json!("123"));
        args.insert("special".to_owned(), json!("-_.~"));

        let query_params = vec![
            McpRestQueryParams { name: "alpha".into(), source: "alpha".into() },
            McpRestQueryParams { name: "numeric".into(), source: "numeric".into() },
            McpRestQueryParams { name: "special".into(), source: "special".into() },
        ];

        let mut result = String::new();
        append_query_string(&mut result, &query_params, &args);
        assert!(result.contains("alpha=ABCxyz"), "Alphanumeric should not be encoded: {result}");
        assert!(result.contains("numeric=123"), "Numeric should not be encoded: {result}");
        assert!(result.contains("special=-_.~"), "Unreserved chars -_.~ should not be encoded: {result}");
    }
}
