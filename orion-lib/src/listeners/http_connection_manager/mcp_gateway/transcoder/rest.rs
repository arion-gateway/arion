use super::{RestTranscoder, Transcoder, TranscoderError};
use crate::{body::instrumented_body::InstrumentedBody, OrionRequestBody};
use jsonschema::Validator;
use rmcp::model::{JsonObject, Request};
use serde_json::Value;
use std::borrow::Cow;
use url::form_urlencoded;

pub const DEFAULT_USER_AGENT: &str = concat!("orion/", env!("CARGO_PKG_VERSION"));

impl Transcoder for RestTranscoder<'_> {
    fn encode(
        &self,
        input_schema: &JsonObject,
        http_headers: http::HeaderMap,
        mcp_request: &Request,
    ) -> Result<http::Request<OrionRequestBody>, TranscoderError> {
        // Build a path-only URI (no authority/scheme) - Orion will route to the correct
        // upstream cluster based on configuration. Including authority without scheme
        // causes "invalid format" error in http::Uri parser.
        let arguments = mcp_request.params.get("arguments").and_then(|v| v.as_object());

        // Validate arguments against input schema
        if !input_schema.is_empty() {
            let schema_value = Value::Object(input_schema.clone());
            let validator = Validator::new(&schema_value)
                .map_err(|e| TranscoderError::ValidationError(format!("Invalid input schema: {e}")))?;
            let args_to_validate =
                arguments.map_or_else(|| Value::Object(serde_json::Map::new()), |a| Value::Object(a.clone()));
            let errors: Vec<String> = validator.iter_errors(&args_to_validate).map(|e| e.to_string()).collect();
            if !errors.is_empty() {
                return Err(TranscoderError::ValidationError(errors.join("; ")));
            }
        }

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
            uri = form_urlencoded::Serializer::new(uri).extend_pairs(extract_arguments(mcp_request)).finish();
        }

        // Orion will override the authority with the correct upstream endpoint.
        // This is required for the match_virtual_host to work properly.

        let user_agent =
            http_headers.get(http::header::USER_AGENT).and_then(|ua| ua.to_str().ok()).unwrap_or(DEFAULT_USER_AGENT);

        let mut builder =
            http::Request::builder().method(self.method.clone()).uri(uri).header(http::header::USER_AGENT, user_agent);

        if let Some(host) = http_headers.get(http::header::HOST).and_then(|h| h.to_str().ok()) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use rmcp::model::Request;
    use serde_json::json;

    fn create_test_request(arguments: Option<serde_json::Map<String, Value>>) -> Request {
        let mut params = serde_json::Map::new();
        params.insert("name".to_string(), json!("test_tool"));
        if let Some(args) = arguments {
            params.insert("arguments".to_string(), Value::Object(args));
        }
        Request { method: "tools/call".into(), params, extensions: Default::default() }
    }

    fn create_http_request() -> http::Request<OrionRequestBody> {
        http::Request::builder().method(http::Method::GET).uri("/test").body(OrionRequestBody::default()).unwrap()
    }

    #[test]
    fn test_validation_fails_when_required_field_missing() {
        // Schema requires "username" field
        let input_schema: JsonObject = serde_json::from_value(json!({
            "type": "object",
            "properties": {
                "username": { "type": "string" }
            },
            "required": ["username"]
        }))
        .unwrap();

        // Request with empty arguments - missing required "username"
        let mcp_request = create_test_request(Some(serde_json::Map::new()));
        let http_request = create_http_request();
        let query_params: Vec<super::super::McpRestQueryParams> = vec![];
        let transcoder = RestTranscoder { method: &http::Method::GET, path: "/api/test", query_params: &query_params };

        let result = transcoder.encode(&input_schema, &http_request, &mcp_request);

        assert!(matches!(result, Err(TranscoderError::ValidationError(_))));
        if let Err(TranscoderError::ValidationError(msg)) = result {
            assert!(msg.contains("username"), "Error message should mention missing field: {}", msg);
        }
    }

    #[test]
    fn test_validation_fails_when_wrong_type() {
        // Schema requires "count" to be a number
        let input_schema: JsonObject = serde_json::from_value(json!({
            "type": "object",
            "properties": {
                "count": { "type": "number" }
            }
        }))
        .unwrap();

        // Request with string instead of number
        let mut args = serde_json::Map::new();
        args.insert("count".to_string(), json!("not a number"));
        let mcp_request = create_test_request(Some(args));
        let http_request = create_http_request();
        let query_params: Vec<super::super::McpRestQueryParams> = vec![];
        let transcoder = RestTranscoder { method: &http::Method::GET, path: "/api/test", query_params: &query_params };

        let result = transcoder.encode(&input_schema, &http_request, &mcp_request);

        assert!(matches!(result, Err(TranscoderError::ValidationError(_))));
        if let Err(TranscoderError::ValidationError(msg)) = result {
            assert!(msg.contains("number"), "Error message should mention type mismatch: {}", msg);
        }
    }

    #[test]
    fn test_validation_passes_with_valid_arguments() {
        // Schema with required fields
        let input_schema: JsonObject = serde_json::from_value(json!({
            "type": "object",
            "properties": {
                "username": { "type": "string" },
                "age": { "type": "integer" }
            },
            "required": ["username"]
        }))
        .unwrap();

        // Valid request with all required fields and correct types
        let mut args = serde_json::Map::new();
        args.insert("username".to_string(), json!("john_doe"));
        args.insert("age".to_string(), json!(25));
        let mcp_request = create_test_request(Some(args));
        let http_request = create_http_request();
        let query_params: Vec<super::super::McpRestQueryParams> = vec![];
        let transcoder = RestTranscoder { method: &http::Method::GET, path: "/api/test", query_params: &query_params };

        let result = transcoder.encode(&input_schema, &http_request, &mcp_request);

        assert!(result.is_ok(), "Expected validation to pass but got: {:?}", result);
    }

    #[test]
    fn test_empty_schema_skips_validation() {
        // Empty schema - no validation should occur
        let input_schema: JsonObject = serde_json::Map::new();
        let mut args = serde_json::Map::new();
        args.insert("any_field".to_string(), json!("any_value"));
        let mcp_request = create_test_request(Some(args));
        let http_request = create_http_request();
        let query_params: Vec<super::super::McpRestQueryParams> = vec![];
        let transcoder = RestTranscoder { method: &http::Method::GET, path: "/api/test", query_params: &query_params };

        let result = transcoder.encode(&input_schema, &http_request, &mcp_request);

        assert!(result.is_ok(), "Expected no validation for empty schema but got: {:?}", result);
    }
}
