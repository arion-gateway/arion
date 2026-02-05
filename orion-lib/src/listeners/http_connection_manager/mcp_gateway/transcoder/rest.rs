use super::{RestTranscoder, Transcoder, TranscoderError};
use crate::{body::instrumented_body::InstrumentedBody, OrionRequestBody};
use rmcp::model::{JsonObject, Request};
use std::borrow::Cow;
use url::form_urlencoded;

const DEFAULT_USER_AGENT: &str = concat!("orion/", env!("CARGO_PKG_VERSION"));

impl Transcoder for RestTranscoder<'_> {
    fn encode(
        &self,
        input_schema: &JsonObject,
        http_request: &http::Request<OrionRequestBody>,
        mcp_request: &Request,
    ) -> Result<http::Request<OrionRequestBody>, TranscoderError> {
        // Build a path-only URI (no authority/scheme) - Orion will route to the correct
        // upstream cluster based on configuration. Including authority without scheme
        // causes "invalid format" error in http::Uri parser.
        let arguments = mcp_request.params.get("arguments").and_then(|v| v.as_object());

        // TODO: validate arguments against input schema!

        let has_args = arguments.is_some_and(|m| !m.is_empty());

        // Estimate capacity: path + '?' + ~24 chars per argument (key=value&)
        let capacity = self.path.len() + if has_args { 1 + arguments.map_or(0, |m| m.len() * 24) } else { 0 };

        // Ensure path starts with '/' for a valid path-only URI
        let mut uri = String::with_capacity(capacity);
        if !self.path.starts_with('/') {
            uri.push('/');
        }
        uri.push_str(self.path);

        // Build query string directly using lazy iterator
        if has_args {
            uri.push('?');
            uri = form_urlencoded::Serializer::new(uri).extend_pairs(extract_arguments(&mcp_request)).finish();
        }

        // Orion will override the authority with the correct upstream endpoint.
        // This is required for the match_virtual_host to work properly.

        let headers = http_request.headers();
        let user_agent =
            headers.get(http::header::USER_AGENT).and_then(|ua| ua.to_str().ok()).unwrap_or(DEFAULT_USER_AGENT);

        let mut builder =
            http::Request::builder().method(self.method.clone()).uri(uri).header(http::header::USER_AGENT, user_agent);

        if let Some(host) = headers.get(http::header::HOST).and_then(|h| h.to_str().ok()) {
            builder = builder.header(http::header::HOST, host);
        }

        let body = InstrumentedBody::default();

        Ok(builder.body(body)?)
    }
}

/// Returns an iterator over arguments as (key, value) pairs without allocating a Vec.
/// Values are borrowed when possible (strings), owned only when conversion is needed.
fn extract_arguments(mcp_request: &Request) -> impl Iterator<Item = (&str, Cow<'_, str>)> {
    mcp_request.params.get("arguments").and_then(|v| v.as_object()).into_iter().flatten().map(|(k, v)| {
        let value = match v {
            serde_json::Value::String(s) => Cow::Borrowed(s.as_str()),
            serde_json::Value::Null => Cow::Borrowed("null"),
            serde_json::Value::Bool(true) => Cow::Borrowed("true"),
            serde_json::Value::Bool(false) => Cow::Borrowed("false"),
            other => Cow::Owned(other.to_string()),
        };
        (k.as_str(), value)
    })
}

//fn build_rest_request(
//    request: &http::Request<OrionRequestBody>,
//    mcp_request: &Request,
//    method: &http::Method,
//    path: &str,
//    _query_params: &Vec<McpRestQueryParams>,
//) -> Result<http::Request<OrionRequestBody>, http::Error> {
//    todo!()
//}
