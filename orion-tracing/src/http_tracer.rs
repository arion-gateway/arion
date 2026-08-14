// Copyright 2025 The kmesh Authors
//
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
//

use bounded_integer::BoundedU16;
use http::header::HOST;
use http::{HeaderMap, HeaderValue, Request, StatusCode};
use opentelemetry::global::BoxedSpan;

use opentelemetry::trace::{Span, SpanContext, Status, TraceContextExt, TraceState, Tracer};
use opentelemetry::{Context, KeyValue, SpanId, TraceFlags, TraceId};
use rand::Rng;
use std::str::FromStr;

use orion_configuration::config::network_filters::tracing::{TracingConfig, TracingKey};
use orion_http_header::*;

use crate::trace_context::TraceContext;
use crate::{
    request_id::RequestId,
    trace_info::{FromHeaderValue, TraceInfo, TraceProvider},
};

pub use opentelemetry::trace::SpanKind as OtelSpanKind;

#[derive(Debug, Default, PartialEq, Eq)]
pub struct HttpTracer {
    tracing: Option<TracingConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SpanKind {
    Server,
    Client,
}

#[derive(Debug)]
pub enum SpanName<'a, B> {
    Host(&'a Request<B>),
    Str(&'a str),
}

/// A client span scoped to one upstream operation.
///
/// This guard owns its span and propagation context, allowing independent guards
/// to be used by multiple upstream operations. Dropping the guard ends a recorded span.
pub struct ScopedClientSpan {
    propagation: Option<TraceInfo>,
    tracestate: Option<HeaderValue>,
    span: Option<BoxedSpan>,
}

impl std::fmt::Debug for ScopedClientSpan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScopedClientSpan")
            .field("propagation", &self.propagation)
            .field("tracestate", &self.tracestate)
            .field("recording", &self.span.is_some())
            .finish()
    }
}

impl ScopedClientSpan {
    pub fn disabled() -> Self {
        Self { propagation: None, tracestate: None, span: None }
    }

    fn propagating(propagation: TraceInfo, tracestate: Option<HeaderValue>) -> Self {
        Self { propagation: Some(propagation), tracestate, span: None }
    }

    pub fn inject_headers(&self, headers: &mut HeaderMap) {
        if let Some(propagation) = self.propagation.as_ref() {
            _ = propagation.update_headers_with_tracestate(headers, self.tracestate.as_ref());
        }
    }

    pub fn set_http_request<B>(&mut self, request: &Request<B>) {
        let Some(span) = self.span.as_mut() else {
            return;
        };

        span.set_attributes([
            KeyValue::new("http.request.method", request.method().as_str().to_owned()),
            KeyValue::new("url.full", request.uri().to_string()),
            KeyValue::new("url.path", request.uri().path().to_owned()),
            KeyValue::new("network.protocol.name", "http"),
            KeyValue::new("network.protocol.version", format!("{:?}", request.version())),
            KeyValue::new(
                "user_agent.original",
                request
                    .headers()
                    .get(http::header::USER_AGENT)
                    .and_then(|value| value.to_str().ok())
                    .unwrap_or("unknown")
                    .to_owned(),
            ),
        ]);

        if let Some(query) = request.uri().query() {
            span.set_attribute(KeyValue::new("url.query", query.to_owned()));
        }
        if let Some(scheme) = request.uri().scheme() {
            span.set_attribute(KeyValue::new("url.scheme", scheme.as_str().to_owned()));
        }
    }

    pub fn set_endpoint(&mut self, cluster_name: &str, authority: &str) {
        if let Some(span) = self.span.as_mut() {
            span.set_attributes([
                KeyValue::new("upstream.cluster.name", cluster_name.to_owned()),
                KeyValue::new("upstream.address", authority.to_owned()),
            ]);
        }
    }

    pub fn set_attribute(&mut self, attribute: KeyValue) {
        if let Some(span) = self.span.as_mut() {
            span.set_attribute(attribute);
        }
    }

    pub fn set_attributes(&mut self, attributes: impl IntoIterator<Item = KeyValue>) {
        if let Some(span) = self.span.as_mut() {
            span.set_attributes(attributes);
        }
    }

    pub fn set_http_status(&mut self, status: StatusCode) {
        if let Some(span) = self.span.as_mut() {
            span.set_attribute(KeyValue::new("http.response.status_code", i64::from(status.as_u16())));
            if !status.is_success() {
                span.set_status(Status::error(status.to_string()));
            }
        }
    }

    pub fn set_error(&mut self, error_type: &str) {
        if let Some(span) = self.span.as_mut() {
            span.set_attribute(KeyValue::new("error.type", error_type.to_owned()));
            span.set_status(Status::error(error_type.to_owned()));
        }
    }

    pub fn complete(mut self) {
        self.finish();
    }

    fn finish(&mut self) {
        if let Some(mut span) = self.span.take() {
            span.end();
        }
    }
}

impl Drop for ScopedClientSpan {
    fn drop(&mut self) {
        self.finish();
    }
}

impl From<SpanKind> for OtelSpanKind {
    fn from(value: SpanKind) -> Self {
        match value {
            SpanKind::Server => OtelSpanKind::Server,
            SpanKind::Client => OtelSpanKind::Client,
        }
    }
}

impl HttpTracer {
    /// Creates a new `Tracer` instance with the provided tracing configuration.
    pub fn new() -> Self {
        Self { tracing: None }
    }

    pub fn with_config(self, tracing: TracingConfig) -> Self {
        Self { tracing: Some(tracing) }
    }

    pub fn upstream_spans_enabled(&self) -> bool {
        self.tracing.as_ref().is_some_and(|tracing| tracing.spawn_upstream_span)
    }

    /// Analyzes request headers to decide if and how to trace.
    ///
    pub fn try_build_trace_context<B>(&self, req: &Request<B>, request_id: Option<RequestId>) -> Option<TraceContext> {
        let tracing = self.tracing.as_ref()?;
        let headers = req.headers();
        let x_client_trace_id = headers.get(X_CLIENT_TRACE_ID);

        if let Some(parent_trace_id) = TraceInfo::extract_from(headers).ok().flatten() {
            let tracestate = (parent_trace_id.provider() == TraceProvider::W3CTraceContext)
                .then(|| {
                    headers
                        .get(TRACESTATE)
                        .filter(|value| {
                            value.to_str().ok().and_then(|value| TraceState::from_str(value).ok()).is_some()
                        })
                        .cloned()
                })
                .flatten();
            return Some(
                TraceContext::new(None)
                    .with_parent(parent_trace_id.clone())
                    .with_request_id(request_id)
                    .with_client_trace_id(x_client_trace_id.cloned())
                    .with_tracestate(tracestate),
            );
        }

        // No tracer was found. Let's analyze the headers to see if we should emit a new trace ID.

        let forced = headers.contains_key(X_ENVOY_FORCE_TRACE);

        let dont_trace = || {
            TraceContext::new(None).with_client_trace_id(x_client_trace_id.cloned()).with_request_id(request_id.clone())
        };

        // 1. trigger: x_client_trace_id...

        if let Some(trace_id) = x_client_trace_id.and_then(|val| <u128 as FromHeaderValue>::from(val).ok()) {
            if forced || (Self::should_sample(tracing.client_sampling) && Self::should_sample(tracing.overall_sampling))
            {
                return Some(
                    TraceContext::new(Some(trace_id))
                        .with_request_id(request_id)
                        .with_should_sample(true)
                        .with_client_trace_id(x_client_trace_id.cloned()),
                );
            }

            return Some(dont_trace());
        }

        // Trigger: x_request_id...

        if let Some(trace_id) = request_id.as_ref().and_then(|val| <u128 as FromHeaderValue>::from(val.as_ref()).ok()) {
            if forced || (Self::should_sample(tracing.random_sampling) && Self::should_sample(tracing.overall_sampling))
            {
                return Some(
                    TraceContext::new(Some(trace_id))
                        .with_request_id(request_id)
                        .with_should_sample(true)
                        .with_client_trace_id(x_client_trace_id.cloned()),
                );
            }

            return Some(dont_trace());
        }

        // last resort: if forced, create a new trace ID.
        //

        if forced {
            return Some(
                TraceContext::new(None)
                    .with_child(TraceInfo::new(true, TraceProvider::W3CTraceContext, None))
                    .with_request_id(request_id)
                    .with_should_sample(true)
                    .with_client_trace_id(x_client_trace_id.cloned()),
            );
        }

        // ... otherwise don't trace.
        //

        Some(dont_trace())
    }

    #[inline]
    fn should_sample(sampling: Option<BoundedU16<0, 100>>) -> bool {
        sampling.is_none_or(|s| {
            let random_value: u16 = rand::rng().random_range(0..100); // Random value between 0 and 99
            random_value < s.get()
        })
    }

    pub fn update_tracing_headers<B>(&self, context: &TraceContext, request: &mut Request<B>) {
        let headers = request.headers_mut();
        context.map_child(|child| {
            _ = child.update_headers_with_tracestate(headers, context.tracestate());
        });
    }

    /// Starts an independently-owned client span beneath the active server
    /// hop, without mutating the transaction's child slot.
    pub fn begin_scoped_client_span(
        trace_context: Option<&TraceContext>,
        tracing_key: Option<&TracingKey>,
        span_name: &str,
    ) -> ScopedClientSpan {
        let Some(trace_context) = trace_context else {
            return ScopedClientSpan::disabled();
        };
        let Some(server) = trace_context.map_child(Clone::clone) else {
            return ScopedClientSpan::disabled();
        };
        let tracestate = trace_context.tracestate().cloned();

        if !trace_context.should_sample() {
            return ScopedClientSpan::propagating(server, tracestate);
        }

        let Some(tracing_key) = tracing_key else {
            return ScopedClientSpan::propagating(server, tracestate);
        };
        let Some(tracer) = crate::get_otel_tracer(tracing_key) else {
            return ScopedClientSpan::propagating(server, tracestate);
        };
        let client = server.clone().into_child();

        let span_builder = tracer
            .span_builder(span_name.to_owned())
            .with_span_id(SpanId::from_bytes(client.span_id().unwrap_or(0).to_be_bytes()))
            .with_trace_id(TraceId::from_bytes(client.trace_id().to_be_bytes()))
            .with_kind(OtelSpanKind::Client);

        let parent_context = context_from_trace_info(&server, trace_context.tracestate(), false);
        let span = span_builder.start_with_context(tracer.as_ref(), &parent_context);

        ScopedClientSpan { propagation: Some(client), tracestate, span: Some(span) }
    }

    /// Builds a span from the given request headers and the TraceContext.
    ///
    pub fn try_create_span<B>(
        &self,
        trace_context: Option<&TraceContext>,
        tracing_key: &TracingKey,
        span_kind: SpanKind,
        span_name: SpanName<'_, B>,
    ) -> Option<BoxedSpan> {
        // if trace_context is None, don't create a span
        let trace_context = trace_context?;
        let tracing_conf = self.tracing.as_ref()?;

        // Create the server propagation context even when it will not be
        // recorded, so upstream calls remain attached to the inbound trace.
        if matches!(span_kind, SpanKind::Server) && (trace_context.should_sample() || trace_context.parent().is_some())
        {
            trace_context.spawn_child();
        }

        // if trace_context should not sample, don't create a span
        if !trace_context.should_sample() {
            return None;
        }

        // if span type is client and tracing config does not spawn upstream span, don't create a span
        if matches!(span_kind, SpanKind::Client) && !tracing_conf.spawn_upstream_span {
            return None;
        }

        if matches!(span_kind, SpanKind::Client) {
            trace_context.spawn_child();
        }

        // Get the tracer and the plan for the child span.
        let tracer = crate::get_otel_tracer(tracing_key)?;

        let (span_id, trace_id) = trace_context.map_child(|child| (child.span_id(), child.trace_id()))?;

        let span_name = match span_name {
            SpanName::Host(request) => get_host_from_request(request).unwrap_or("no-host").to_string(),
            SpanName::Str(s) => s.to_string(),
        };

        // Build the span configuration from the trace context.
        let span_builder = tracer
            .span_builder(span_name)
            .with_span_id(SpanId::from_bytes(span_id.unwrap_or(0).to_be_bytes()))
            .with_trace_id(TraceId::from_bytes(trace_id.to_be_bytes()))
            .with_kind(span_kind.into());

        let parent_context = if let Some(parent_info) = trace_context.parent() {
            context_from_trace_info(parent_info, trace_context.tracestate(), true)
        } else {
            // If there's no parent, we start from a clean root context.
            Context::new()
        };

        // Start the span and immediately wrap it for manual management.
        let span: BoxedSpan = span_builder.start_with_context(tracer.as_ref(), &parent_context);

        // Wrap it in a Arc/Mutex for safe mutation and for shared ownership.
        Some(span)
    }
}

fn context_from_trace_info(trace_info: &TraceInfo, tracestate: Option<&HeaderValue>, is_remote: bool) -> Context {
    let trace_state = tracestate
        .and_then(|value| value.to_str().ok())
        .and_then(|value| TraceState::from_str(value).ok())
        .unwrap_or_default();
    let span_context = SpanContext::new(
        TraceId::from_bytes(trace_info.trace_id().to_be_bytes()),
        SpanId::from_bytes(trace_info.span_id().unwrap_or(0).to_be_bytes()),
        if trace_info.sampled() { TraceFlags::SAMPLED } else { TraceFlags::default() },
        is_remote,
        trace_state,
    );

    Context::new().with_remote_span_context(span_context)
}

pub fn get_host_from_request<B>(request: &Request<B>) -> Option<&str> {
    // 1. Prefer the ':authority' header, common in HTTP/2.
    if let Some(authority) = request.headers().get(":authority") {
        if let Ok(host) = authority.to_str() {
            return Some(host);
        }
    }

    // 2. Fallback to the 'Host' header, standard for HTTP/1.1.
    if let Some(host_header) = request.headers().get(HOST) {
        if let Ok(host) = host_header.to_str() {
            return Some(host);
        }
    }

    // 3. As a last resort, try to get the host from the URI.
    request.uri().host()
}

#[cfg(test)]
mod tests {
    use super::*;
    use bounded_integer::BoundedU16;
    use http::HeaderValue;

    fn recording_tracer_config() -> TracingConfig {
        TracingConfig {
            client_sampling: BoundedU16::<0, 100>::new(100),
            random_sampling: BoundedU16::<0, 100>::new(100),
            overall_sampling: BoundedU16::<0, 100>::new(100),
            verbose: false,
            max_path_tag_length: None,
            spawn_upstream_span: true,
            provider: None,
        }
    }

    #[test]
    fn build_no_tracer() {
        let no_tracer = HttpTracer::new();
        let req = Request::builder().uri("http://example.com").body(()).unwrap();
        let context = no_tracer.try_build_trace_context(&req, None);
        assert!(context.is_none());
    }

    #[test]
    fn build_default_http_tracer() {
        let config = TracingConfig {
            client_sampling: None,
            random_sampling: None,
            overall_sampling: None,
            verbose: false,
            max_path_tag_length: None,
            spawn_upstream_span: false,
            provider: None,
        };

        let tracer = HttpTracer::new().with_config(config);
        let req = Request::builder().uri("http://example.com").body(()).unwrap();
        let context = tracer.try_build_trace_context(&req, None);

        let Some(ref ctx) = context else {
            panic!("context should be Some");
        };

        assert!(ctx.parent().is_none());
        assert!(ctx.map_child(|_| ()).is_none());
        assert!(ctx.is_root_node());
        assert!(!ctx.should_sample());
    }

    #[test]
    fn http_tracer_and_req_with_invalid_x_request_id() {
        let config = TracingConfig {
            client_sampling: None,
            random_sampling: None,
            overall_sampling: None,
            verbose: false,
            max_path_tag_length: None,
            spawn_upstream_span: false,
            provider: None,
        };

        let tracer = HttpTracer::new().with_config(config);
        let req = Request::builder().uri("http://example.com").header("X-Request-ID", "12345").body(()).unwrap();
        let context = tracer.try_build_trace_context(&req, None);

        let Some(ref ctx) = context else {
            panic!("context should be Some");
        };

        assert!(ctx.parent().is_none());
        assert!(ctx.map_child(|_| ()).is_none());
        assert!(ctx.is_root_node());
        assert!(!ctx.should_sample());
    }

    #[test]
    fn http_tracer_and_req_with_valid_x_request_id() {
        let config = TracingConfig {
            client_sampling: None,
            random_sampling: None,
            overall_sampling: None,
            verbose: false,
            max_path_tag_length: None,
            spawn_upstream_span: false,
            provider: None,
        };

        let tracer = HttpTracer::new().with_config(config);
        let req = Request::builder()
            .uri("http://example.com")
            .header("X-Request-ID", "9b13f5d0-9c42-4d27-a34f-08b7a8a5601a")
            .body(())
            .unwrap();

        let context = tracer.try_build_trace_context(
            &req,
            Some(RequestId::Propagate(HeaderValue::from_static("9b13f5d0-9c42-4d27-a34f-08b7a8a5601a"))),
        );

        let span = tracer.try_create_span(
            context.as_ref(),
            &TracingKey("test", 0),
            SpanKind::Server,
            SpanName::Str::<()>("test"),
        );
        assert!(span.is_none()); // None because OTEL is not configured. The child node is created anyway.

        let Some(ref ctx) = context else {
            panic!("context should be Some");
        };

        assert!(ctx.parent().is_none());
        assert!(ctx.map_child(|_| ()).is_some());
        assert!(ctx.is_root_node());
        assert_eq!(ctx.map_child(|child| child.sampled()), Some(true));
        assert!(ctx.should_sample());
    }

    #[test]
    fn http_tracer_and_req_with_traceparent_with_sampling_100_percent() {
        let config = TracingConfig {
            client_sampling: BoundedU16::<0, 100>::new(100),
            random_sampling: BoundedU16::<0, 100>::new(100),
            overall_sampling: BoundedU16::<0, 100>::new(100),
            verbose: false,
            max_path_tag_length: None,
            spawn_upstream_span: false,
            provider: None,
        };

        let tracer = HttpTracer::new().with_config(config);
        let req = Request::builder()
            .uri("http://example.com")
            .header("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
            .body(())
            .unwrap();

        let context = tracer.try_build_trace_context(&req, None);

        let span = tracer.try_create_span(
            context.as_ref(),
            &TracingKey("test", 0),
            SpanKind::Server,
            SpanName::Str::<()>("test"),
        );
        assert!(span.is_none()); // None because OTEL is not configured. The child node is created anyway.

        let Some(ref ctx) = context else {
            panic!("context should be Some");
        };

        assert!(ctx.parent().is_some());
        assert!(ctx.map_child(|_| ()).is_some());
        assert!(!ctx.is_root_node());
        assert_eq!(ctx.map_child(|child| child.sampled()), Some(true));
        assert!(ctx.should_sample());
    }

    #[test]
    fn invalid_w3c_tracestate_is_not_retained_or_propagated() {
        let tracer = HttpTracer::new().with_config(recording_tracer_config());
        let request = Request::builder()
            .header(TRACEPARENT, "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
            .header(TRACESTATE, "Vendor=value")
            .body(())
            .unwrap();
        let context = tracer.try_build_trace_context(&request, None).unwrap();

        assert!(context.tracestate().is_none());
        assert!(tracer
            .try_create_span(Some(&context), &TracingKey("test", 0), SpanKind::Server, SpanName::Str::<()>("server"),)
            .is_none());

        let guard = HttpTracer::begin_scoped_client_span(Some(&context), Some(&TracingKey("test", 0)), "upstream");
        let mut headers = HeaderMap::new();
        guard.inject_headers(&mut headers);

        assert!(!headers.contains_key(TRACESTATE));
    }

    #[test]
    fn forced_root_server_context_has_span_id() {
        let tracer = HttpTracer::new().with_config(recording_tracer_config());
        let request = Request::builder().header(X_ENVOY_FORCE_TRACE, "true").body(()).unwrap();
        let context = tracer.try_build_trace_context(&request, None).unwrap();

        assert!(tracer
            .try_create_span(Some(&context), &TracingKey("test", 0), SpanKind::Server, SpanName::Str::<()>("server"),)
            .is_none());

        let server = context.map_child(Clone::clone).expect("forced tracing must create a server propagation context");
        assert_ne!(server.trace_id(), 0);
        assert!(server.span_id().is_some());
    }

    #[test]
    fn http_tracer_and_req_with_traceparent_and_sampling_0_percent() {
        let config = TracingConfig {
            client_sampling: BoundedU16::<0, 100>::new(0),
            random_sampling: BoundedU16::<0, 100>::new(0),
            overall_sampling: BoundedU16::<0, 100>::new(0),
            verbose: false,
            max_path_tag_length: None,
            spawn_upstream_span: false,
            provider: None,
        };

        let tracer = HttpTracer::new().with_config(config);
        let req = Request::builder()
            .uri("http://example.com")
            .header("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
            .body(())
            .unwrap();

        let context = tracer.try_build_trace_context(&req, None);

        let span = tracer.try_create_span(
            context.as_ref(),
            &TracingKey("test", 0),
            SpanKind::Server,
            SpanName::Str::<()>("test"),
        );
        assert!(span.is_none()); // None because OTEL is not configured. The child node is created anyway.

        let Some(ref ctx) = context else {
            panic!("context should be Some");
        };

        assert!(ctx.parent().is_some());
        assert!(ctx.map_child(|_| ()).is_some());
        assert!(!ctx.is_root_node());
        assert!(ctx.should_sample());
    }

    #[test]
    fn http_tracer_and_req_with_traceparent_with_sampled_false() {
        let config = TracingConfig {
            client_sampling: BoundedU16::<0, 100>::new(100),
            random_sampling: BoundedU16::<0, 100>::new(100),
            overall_sampling: BoundedU16::<0, 100>::new(100),
            verbose: false,
            max_path_tag_length: None,
            spawn_upstream_span: false,
            provider: None,
        };

        let tracer = HttpTracer::new().with_config(config);
        let req = Request::builder()
            .uri("http://example.com")
            .header("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00")
            .body(())
            .unwrap();

        let context = tracer.try_build_trace_context(&req, None);

        let span = tracer.try_create_span(
            context.as_ref(),
            &TracingKey("test", 0),
            SpanKind::Server,
            SpanName::Str::<()>("test"),
        );
        assert!(span.is_none()); // Unsampled contexts propagate but do not create a local span.

        let Some(ref ctx) = context else {
            panic!("context should be Some");
        };

        assert!(ctx.parent().is_some());
        assert!(ctx.map_child(|_| ()).is_some());
        assert_eq!(ctx.map_child(TraceInfo::sampled), Some(false));
        assert!(!ctx.is_root_node());
        assert!(!ctx.should_sample());
    }

    #[test]
    fn build_no_tracer_create_span() {
        let no_tracer = HttpTracer::new();
        let req = Request::builder().uri("http://example.com").body(()).unwrap();
        let trace_context = no_tracer.try_build_trace_context(&req, None);
        assert!(trace_context.is_none());
        let span = no_tracer.try_create_span(
            trace_context.as_ref(),
            &TracingKey("test", 0),
            SpanKind::Server,
            SpanName::Str::<()>("test"),
        );
        assert!(span.is_none());
    }

    #[test]
    fn build_tracer_no_sample_create_none_span() {
        let config = TracingConfig {
            client_sampling: BoundedU16::<0, 100>::new(100),
            random_sampling: BoundedU16::<0, 100>::new(100),
            overall_sampling: BoundedU16::<0, 100>::new(100),
            verbose: false,
            max_path_tag_length: None,
            spawn_upstream_span: false,
            provider: None,
        };

        let tracer = HttpTracer::new().with_config(config);
        let req = Request::builder()
            .uri("http://example.com")
            .header("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00")
            .body(())
            .unwrap();

        let trace_context = tracer.try_build_trace_context(&req, None);
        assert!(trace_context.is_some());

        let span = tracer.try_create_span(
            trace_context.as_ref(),
            &TracingKey("test", 0),
            SpanKind::Server,
            SpanName::Str::<()>("test"),
        );
        assert!(span.is_none());
        assert!(trace_context.as_ref().and_then(|ctx| ctx.parent()).is_some());

        println!("PARENT: {:?}", trace_context.as_ref().map(|ctx| ctx.parent()));
        println!("CHILD : {:?}", trace_context.as_ref().map(|ctx| ctx.map_child(|c| c.clone())));

        assert!(trace_context.as_ref().and_then(|ctx| ctx.map_child(|_| ())).is_some());
        assert_eq!(trace_context.as_ref().and_then(|ctx| ctx.map_child(TraceInfo::sampled)), Some(false));
    }

    #[test]
    fn scoped_client_without_recording_propagates_server_context_and_tracestate() {
        let config = TracingConfig {
            client_sampling: BoundedU16::<0, 100>::new(100),
            random_sampling: BoundedU16::<0, 100>::new(100),
            overall_sampling: BoundedU16::<0, 100>::new(100),
            verbose: false,
            max_path_tag_length: None,
            spawn_upstream_span: false,
            provider: None,
        };
        let tracer = HttpTracer::new().with_config(config);
        let request = Request::builder()
            .header(TRACEPARENT, "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
            .header(TRACESTATE, "vendor=value")
            .body(())
            .unwrap();
        let context = tracer.try_build_trace_context(&request, None).unwrap();
        assert!(tracer
            .try_create_span(Some(&context), &TracingKey("test", 0), SpanKind::Server, SpanName::Str::<()>("server"),)
            .is_none());
        let server = context.map_child(Clone::clone).unwrap();

        let guard = HttpTracer::begin_scoped_client_span(Some(&context), None, "upstream");
        let mut headers = HeaderMap::new();
        guard.inject_headers(&mut headers);

        let propagated = TraceInfo::extract_from(&headers).unwrap().unwrap();
        assert_eq!(propagated.trace_id(), server.trace_id());
        assert_eq!(propagated.span_id(), server.span_id());
        assert_eq!(propagated.provider(), server.provider());
        assert_eq!(propagated.sampled(), server.sampled());
        assert_eq!(headers.get(TRACESTATE), Some(&HeaderValue::from_static("vendor=value")));
        assert_eq!(context.map_child(Clone::clone), Some(server));
    }

    #[test]
    fn scoped_client_preserves_inbound_propagation_provider() {
        let config = TracingConfig {
            client_sampling: BoundedU16::<0, 100>::new(100),
            random_sampling: BoundedU16::<0, 100>::new(100),
            overall_sampling: BoundedU16::<0, 100>::new(100),
            verbose: false,
            max_path_tag_length: None,
            spawn_upstream_span: false,
            provider: None,
        };
        let tracer = HttpTracer::new().with_config(config);

        // Per-provider parsing and injection is covered by the trace_info tests; this checks that
        // the scoped client span carries the inbound provider through rather than downgrading it.
        let cases = [
            (
                HeaderMap::from_iter([(
                    TRACEPARENT,
                    HeaderValue::from_static("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"),
                )]),
                TraceProvider::W3CTraceContext,
                TRACEPARENT,
            ),
            (
                HeaderMap::from_iter([
                    (X_B3_TRACEID, HeaderValue::from_static("4bf92f3577b34da6a3ce929d0e0e4736")),
                    (X_B3_SPANID, HeaderValue::from_static("00f067aa0ba902b7")),
                    (X_B3_SAMPLED, HeaderValue::from_static("1")),
                ]),
                TraceProvider::B3Multi,
                X_B3_TRACEID,
            ),
        ];

        for (headers, provider, expected_header) in cases {
            let mut request = Request::new(());
            *request.headers_mut() = headers;
            let context = tracer.try_build_trace_context(&request, None).unwrap();
            assert!(tracer
                .try_create_span(
                    Some(&context),
                    &TracingKey("provider-test", 0),
                    SpanKind::Server,
                    SpanName::Str::<()>("server"),
                )
                .is_none());

            let guard = HttpTracer::begin_scoped_client_span(Some(&context), None, "upstream");
            let mut outbound = HeaderMap::new();
            guard.inject_headers(&mut outbound);

            let propagated = TraceInfo::extract_from(&outbound).unwrap().unwrap();
            assert_eq!(propagated.provider(), provider);
            assert_eq!(propagated.trace_id(), context.map_child(TraceInfo::trace_id).unwrap());
            assert!(outbound.contains_key(expected_header));
        }
    }

    #[test]
    fn scoped_client_propagates_unsampled_inbound_context_without_recording() {
        let config = TracingConfig {
            client_sampling: BoundedU16::<0, 100>::new(100),
            random_sampling: BoundedU16::<0, 100>::new(100),
            overall_sampling: BoundedU16::<0, 100>::new(100),
            verbose: false,
            max_path_tag_length: None,
            spawn_upstream_span: true,
            provider: None,
        };
        let tracer = HttpTracer::new().with_config(config);
        let request = Request::builder()
            .header(TRACEPARENT, "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00")
            .body(())
            .unwrap();
        let context = tracer.try_build_trace_context(&request, None).unwrap();
        assert!(tracer
            .try_create_span(Some(&context), &TracingKey("test", 0), SpanKind::Server, SpanName::Str::<()>("server"),)
            .is_none());

        let guard = HttpTracer::begin_scoped_client_span(Some(&context), Some(&TracingKey("test", 0)), "upstream");
        let mut headers = HeaderMap::new();
        guard.inject_headers(&mut headers);
        let propagated = TraceInfo::extract_from(&headers).unwrap().unwrap();

        assert!(!propagated.sampled());
        let server = context.map_child(Clone::clone).unwrap();
        assert_eq!(propagated.trace_id(), server.trace_id());
        assert_eq!(propagated.span_id(), server.span_id());
    }

    #[test]
    fn disabled_scoped_client_does_not_modify_headers() {
        let mut headers = HeaderMap::new();
        headers
            .insert(TRACEPARENT, HeaderValue::from_static("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"));
        let original = headers.clone();

        ScopedClientSpan::disabled().inject_headers(&mut headers);

        assert_eq!(headers, original);
    }
}
