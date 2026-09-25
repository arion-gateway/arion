// Copyright 2025 The kmesh Authors
// Copyright 2026 The arion-gateway Authors
//
// Modified by arion-gateway Authors.
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

#[cfg(feature = "tracing")]
use http::Request;
#[cfg(feature = "tracing")]
use opentelemetry::global::BoxedSpan;

pub const HTTP_REQUEST_METHOD: &str = "http.request.method";
pub const HTTP_REQUEST_METHOD_ORIGINAL: &str = "http.request.method_original";
pub const HTTP_RESPONSE_STATUS_CODE: &str = "http.response.status_code";
pub const HTTP_REQUEST_RESEND_COUNT: &str = "http.request.resend_count";
pub const HTTP_REQUEST_BODY_SIZE: &str = "http.request.body.size";
pub const HTTP_REQUEST_SIZE: &str = "http.request.size";
pub const HTTP_RESPONSE_BODY_SIZE: &str = "http.response.body.size";
pub const HTTP_RESPONSE_SIZE: &str = "http.response.size";
pub const URL_FULL: &str = "url.full";
pub const URL_PATH: &str = "url.path";
pub const URL_QUERY: &str = "url.query";
pub const URL_SCHEME: &str = "url.scheme";
pub const USER_AGENT_ORIGINAL: &str = "user_agent.original";
pub const NETWORK_PROTOCOL_NAME: &str = "network.protocol.name";
pub const NETWORK_PROTOCOL_VERSION: &str = "network.protocol.version";
pub const UPSTREAM_CLUSTER_NAME: &str = "upstream.cluster.name";
pub const UPSTREAM_ADDRESS: &str = "upstream.address";

#[macro_export]
#[cfg(feature = "tracing")]
macro_rules! with_server_span {
    ($span_state:expr, $closure:expr) => {
        if let Some(valid_span_state) = $span_state.as_ref() {
            if let Some(span) = valid_span_state.server_span.lock().as_mut() {
                ($closure)(span);
            }
        }
    };
}

#[macro_export]
#[cfg(feature = "tracing")]
macro_rules! with_client_span {
    ($span_state:expr, $closure:expr) => {
        if let Some(valid_span_state) = $span_state.as_ref() {
            if let Some(span) = valid_span_state.client_span.lock().as_mut() {
                ($closure)(span);
            }
        }
    };
}

#[macro_export]
#[cfg(not(feature = "tracing"))]
macro_rules! with_server_span {
    ($span_state:expr, $closure:expr) => {
        ();
    };
}

#[macro_export]
#[cfg(not(feature = "tracing"))]
macro_rules! with_client_span {
    ($span_state:expr, $closure:expr) => {
        ();
    };
}

#[cfg(feature = "tracing")]
pub fn set_attributes_from_request<B>(span: &mut BoxedSpan, request: &Request<B>) {
    // Add few attributes, based on the request

    use http::HeaderValue;
    use opentelemetry::{trace::Span, KeyValue};
    use orion_interner::StringInterner;

    span.set_attributes([
        KeyValue::new(HTTP_REQUEST_METHOD, request.method().as_str().to_static_str()), // the number of HTTP methods is small, hence we can use the string interner here..
        KeyValue::new(URL_FULL, request.uri().to_string()),
        KeyValue::new(URL_PATH, request.uri().path().to_owned()),
        KeyValue::new(NETWORK_PROTOCOL_NAME, "http"),
        KeyValue::new(
            NETWORK_PROTOCOL_VERSION,
            request.version().to_static_str().split_once('/').map(|(_, ver)| ver).unwrap_or("unknow"),
        ),
        KeyValue::new(
            USER_AGENT_ORIGINAL,
            request
                .headers()
                .get(::http::header::USER_AGENT)
                .unwrap_or(&HeaderValue::from_static("unknown"))
                .to_str()
                .unwrap_or("invalid-user-agent")
                .to_owned(),
        ),
    ]);

    request.uri().query().inspect(|q| span.set_attribute(KeyValue::new(URL_QUERY, q.to_string())));
    request.uri().scheme().inspect(|s| span.set_attribute(KeyValue::new(URL_SCHEME, s.as_str().to_static_str())));
}
