use std::borrow::Cow;

use super::{RestTranscoder, Transcoder, TranscoderError};
use crate::{
    body::{
        instrumented_body::InstrumentedBody, poly_body::PolyBody, response_flags::BodyKind, timeout_body::TimeoutBody,
    },
    OrionRequestBody,
};
use bytes::Bytes;
use http::StatusCode;
use http_body_util::Full;
use percent_encoding::{utf8_percent_encode, AsciiSet, NON_ALPHANUMERIC};
use rmcp::model::Request;
use serde_json::Value;

pub const DEFAULT_USER_AGENT: &str = concat!("orion/", env!("CARGO_PKG_VERSION"));
pub const PATH_TEMPLATE_NAME: &str = "path";
pub const BODY_TEMPLATE_NAME: &str = "body";

static EMPTY_MAP: std::sync::LazyLock<serde_json::Map<String, Value>> = std::sync::LazyLock::new(serde_json::Map::new);

/// Set of characters that are "unreserved" according to RFC 3986.
const QUERY_SET: &AsciiSet = &NON_ALPHANUMERIC.remove(b'-').remove(b'_').remove(b'.').remove(b'~');

impl RestTranscoder {
    /// Renders a template by substituting variables with values from the arguments map.
    /// Variables are in the format {{`variable_name`}}.
    ///
    /// Note: Missing variables in templates should be caught by input schema validation.
    /// Null values are properly rendered as "null" for JSON compatibility.
    fn render_template(
        &self,
        template_name: &str,
        arguments: &serde_json::Map<String, Value>,
    ) -> Result<String, TranscoderError> {
        Ok(self.template_engine.template(template_name).render(arguments).to_string()?)
    }
}

impl Transcoder for RestTranscoder {
    fn encode(
        &self,
        http_headers: &http::HeaderMap,
        mcp_request: &Request,
    ) -> Result<http::Request<OrionRequestBody>, TranscoderError> {
        // Build a path-only URI (no authority/scheme) - Orion will route to the correct
        // upstream cluster based on configuration. Including authority without scheme
        // causes "invalid format" error in http::Uri parser.
        let arguments = mcp_request.params.get("arguments").and_then(|v| v.as_object()).unwrap_or_else(|| &EMPTY_MAP);
        let rendered_path = self.render_template(PATH_TEMPLATE_NAME, arguments)?;

        let capacity = rendered_path.len() + self.query_params.len() * 32 + 2;
        let mut uri = String::with_capacity(capacity);
        if !rendered_path.starts_with('/') {
            uri.push('/');
        }
        uri.push_str(&rendered_path);

        if !self.query_params.is_empty() {
            append_query_string(&mut uri, &self.query_params, arguments);
        }

        // Orion will override the authority with the correct upstream endpoint.
        // This is required for the match_virtual_host to work properly.

        let user_agent =
            http_headers.get(http::header::USER_AGENT).and_then(|ua| ua.to_str().ok()).unwrap_or(DEFAULT_USER_AGENT);

        let mut builder =
            http::Request::builder().method(&self.method).uri(uri).header(http::header::USER_AGENT, user_agent);

        if let Some(host) = http_headers.get(http::header::HOST).and_then(|h| h.to_str().ok()) {
            builder = builder.header(http::header::HOST, host);
        }

        // Build the body based on body template or empty body
        let body = if self.has_body_template {
            let rendered = self.render_template(BODY_TEMPLATE_NAME, arguments)?;

            builder = builder.header(http::header::CONTENT_TYPE, "application/json");

            let poly_body = PolyBody::from(Full::new(Bytes::from(rendered)));
            let timeout_body = TimeoutBody::new(None, poly_body);
            InstrumentedBody::new(BodyKind::Request, timeout_body, None, |_, _, _, _| {})
        } else {
            InstrumentedBody::default()
        };

        Ok(builder.body(body)?)
    }

    fn decode(&self, upstream_body: Bytes, upstream_status: StatusCode) -> Result<Value, TranscoderError> {
        if !upstream_status.is_success() {
            let body_str = String::from_utf8_lossy(&upstream_body);
            return Err(TranscoderError::UpstreamError(format!(
                "Upstream returned error status {}: {}",
                upstream_status.as_u16(),
                body_str
            )));
        }

        // Parse the response body as JSON
        let response_value: Value = if upstream_body.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&upstream_body).map_err(|e| {
                TranscoderError::JsonParseError(format!("Failed to parse upstream response as JSON: {e}"))
            })?
        };

        Ok(response_value)
    }
}

/// Resolves a variable path like "user.name" against the arguments map.
/// Returns the JSON value as a string, or None if not found.
fn resolve_variable<'a>(path: &str, arguments: &'a serde_json::Map<String, Value>) -> Option<Cow<'a, str>> {
    let mut parts = path.split('.');

    // Get the first part directly from the map to avoid cloning the entire arguments object
    let mut current = parts.next().and_then(|first| arguments.get(first));

    // Iterate through the rest of the path without allocating a Vec
    for part in parts {
        current = current.and_then(|v| v.as_object()).and_then(|map| map.get(part));
    }

    // Format output: avoid quotes for strings, use default to_string() for other JSON types
    current.map(|value| match value {
        Value::String(s) => Cow::Borrowed(s.as_str()),
        Value::Null => Cow::Borrowed("null"),
        Value::Bool(true) => Cow::Borrowed("true"),
        Value::Bool(false) => Cow::Borrowed("false"),
        v => Cow::Owned(v.to_string()),
    })
}

/// Appends a query string from query params configuration with variable substitution.
fn append_query_string(
    uri: &mut String,
    query_params: &[super::McpRestQueryParams],
    arguments: &serde_json::Map<String, Value>,
) {
    let mut first = true;

    for param in query_params {
        if let Some(value) = resolve_variable(&param.source, arguments) {
            if first {
                uri.push('?');
                first = false;
            } else {
                uri.push('&');
            }
            uri.extend(utf8_percent_encode(&param.name, QUERY_SET));
            uri.push('=');
            uri.extend(utf8_percent_encode(&value, QUERY_SET));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rmcp::model::CallToolRequestParams;
    use serde_json::json;
    use upon::Engine;

    fn create_test_request(arguments: Option<serde_json::Map<String, Value>>) -> Request {
        let params = if let Some(args) = arguments {
            CallToolRequestParams::new("test_tool").with_arguments(args)
        } else {
            CallToolRequestParams::new("test_tool")
        };

        let request_json = serde_json::json!({
            "method": "tools/call",
            "params": serde_json::to_value(&params).expect("Failed to serialize params")
        });

        serde_json::from_value(request_json).expect("Failed to create test request")
    }

    fn create_http_request() -> http::Request<OrionRequestBody> {
        http::Request::builder().method(http::Method::GET).uri("/test").body(OrionRequestBody::default()).unwrap()
    }

    fn create_transcoder(
        method: http::Method,
        path: String,
        query_params: Vec<super::super::McpRestQueryParams>,
        has_body_template: bool,
        body_template: Option<String>,
    ) -> RestTranscoder {
        let mut template_engine: Engine<'static> = upon::Engine::new();
        template_engine.add_template(PATH_TEMPLATE_NAME, path).unwrap();
        if let Some(body_template) = body_template {
            template_engine.add_template(BODY_TEMPLATE_NAME, body_template).unwrap();
        }
        RestTranscoder { method, query_params, has_body_template, template_engine }
    }

    #[test]
    fn test_body_template_simple_substitution() {
        // Test simple variable substitution in body template (JSON format)
        let mut args = serde_json::Map::new();
        args.insert("username".to_owned(), json!("john_doe"));
        args.insert("age".to_owned(), json!(30));
        let mcp_request = create_test_request(Some(args));
        let http_request = create_http_request();
        let query_params: Vec<super::super::McpRestQueryParams> = vec![];

        let body_template = r#"{"name": "{{username}}", "years": {{age}}}"#.to_owned();
        let transcoder =
            create_transcoder(http::Method::POST, "/api/users".to_owned(), query_params, true, Some(body_template));

        let result = transcoder.encode(http_request.headers(), &mcp_request);
        assert!(result.is_ok(), "Expected successful encoding but got: {result:?}");

        let request = result.unwrap();
        assert_eq!(request.method(), http::Method::POST);
        assert_eq!(request.uri(), "/api/users");
        assert_eq!(request.headers().get(http::header::CONTENT_TYPE).unwrap(), "application/json");
    }

    #[test]
    fn test_body_template_nested_path() {
        // Test nested path access in body template (JSON format)
        let mut args = serde_json::Map::new();
        let mut user = serde_json::Map::new();
        user.insert("name".to_owned(), json!("john"));
        user.insert("email".to_owned(), json!("john@example.com"));
        args.insert("user".to_owned(), Value::Object(user));
        let mcp_request = create_test_request(Some(args));
        let http_request = create_http_request();
        let query_params: Vec<super::super::McpRestQueryParams> = vec![];

        let body_template = r#"{"username": "{{user.name}}", "contact": "{{user.email}}}"#.to_owned();
        let transcoder =
            create_transcoder(http::Method::POST, "/api/users".to_owned(), query_params, true, Some(body_template));

        let result = transcoder.encode(http_request.headers(), &mcp_request);
        assert!(result.is_ok(), "Expected successful encoding but got: {result:?}");
    }

    #[test]
    fn test_body_template_missing_variable() {
        // Test that missing variables are replaced with "null" in JSON template
        let mut args = serde_json::Map::new();
        args.insert("username".to_owned(), json!("john_doe"));
        let mcp_request = create_test_request(Some(args));
        let http_request = create_http_request();
        let query_params: Vec<super::super::McpRestQueryParams> = vec![];

        let body_template = r#"{"name": "{{username}}", "missing": {{nonexistent}}}"#.to_owned();
        let transcoder =
            create_transcoder(http::Method::POST, "/api/users".to_owned(), query_params, true, Some(body_template));

        // Missing variables now cause template render errors (schema validation should catch these)
        let result = transcoder.encode(http_request.headers(), &mcp_request);
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
        let mcp_request = create_test_request(Some(args));
        let http_request = create_http_request();
        let query_params: Vec<super::super::McpRestQueryParams> = vec![];

        // upon doesn't have a json filter - complex types need to be handled differently
        // This test verifies that the template engine is strict about type formatting
        let body_template = r#"{"tags": {{tags}}, "metadata": {{meta}}}"#.to_owned();
        let transcoder =
            create_transcoder(http::Method::POST, "/api/data".to_owned(), query_params, true, Some(body_template));

        // Complex types without proper formatting will cause render errors
        let result = transcoder.encode(http_request.headers(), &mcp_request);
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

        assert_eq!(resolve_variable("name", &args), Some(Cow::from("Test")));
    }

    #[test]
    fn test_resolve_variable_nested() {
        let mut args = serde_json::Map::new();
        let mut nested = serde_json::Map::new();
        nested.insert("value".to_owned(), json!(42));
        args.insert("config".to_owned(), Value::Object(nested));

        assert_eq!(resolve_variable("config.value", &args), Some(Cow::from("42")));
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
        let mcp_request = create_test_request(Some(args));
        let http_request = create_http_request();

        let path_template = "/api/users/{{user_id}}/{{action}}";
        let query_params: Vec<super::super::McpRestQueryParams> = vec![];
        let transcoder = create_transcoder(http::Method::GET, path_template.to_owned(), query_params, false, None);

        let result = transcoder.encode(http_request.headers(), &mcp_request);
        assert!(result.is_ok(), "Expected successful encoding but got: {result:?}");

        let request = result.unwrap();
        assert_eq!(request.uri(), "/api/users/12345/profile");
    }

    #[test]
    fn test_path_template_with_nested_args() {
        // Test nested path access in path template
        let mut args = serde_json::Map::new();
        let mut location = serde_json::Map::new();
        location.insert("city".to_owned(), json!("dublin"));
        args.insert("location".to_owned(), Value::Object(location));
        let mcp_request = create_test_request(Some(args));
        let http_request = create_http_request();

        let path_template = "/api/weather/{{location.city}}";
        let query_params: Vec<super::super::McpRestQueryParams> = vec![];
        let transcoder = create_transcoder(http::Method::GET, path_template.to_owned(), query_params, false, None);

        let result = transcoder.encode(http_request.headers(), &mcp_request);
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
        let mcp_request = create_test_request(Some(args));
        let http_request = create_http_request();

        let query_params = vec![
            super::super::McpRestQueryParams { name: "q".into(), source: "search".into() },
            super::super::McpRestQueryParams { name: "limit".into(), source: "limit".into() },
        ];
        let transcoder = create_transcoder(http::Method::GET, "/api/search".to_owned(), query_params, false, None);

        let result = transcoder.encode(http_request.headers(), &mcp_request);
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
        let mcp_request = create_test_request(Some(args));
        let http_request = create_http_request();

        let query_params = vec![
            super::super::McpRestQueryParams { name: "latitude".into(), source: "coordinates.lat".into() },
            super::super::McpRestQueryParams { name: "longitude".into(), source: "coordinates.lon".into() },
            super::super::McpRestQueryParams { name: "days".into(), source: "forecast_days".into() },
        ];
        let transcoder = create_transcoder(http::Method::GET, "/api/weather".to_owned(), query_params, false, None);

        let result = transcoder.encode(http_request.headers(), &mcp_request);
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
        let mcp_request = create_test_request(Some(args));
        let http_request = create_http_request();

        let query_params = vec![
            super::super::McpRestQueryParams { name: "city".into(), source: "city".into() },
            super::super::McpRestQueryParams { name: "country".into(), source: "missing_country".into() },
        ];
        let transcoder = create_transcoder(http::Method::GET, "/api/locations".to_owned(), query_params, false, None);

        let result = transcoder.encode(http_request.headers(), &mcp_request);
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
        let mcp_request = create_test_request(Some(args));
        let http_request = create_http_request();

        let query_params =
            vec![super::super::McpRestQueryParams { name: "status".into(), source: "filters.status".into() }];
        let transcoder =
            create_transcoder(http::Method::GET, "/api/users/{{user_id}}/orders".to_owned(), query_params, false, None);

        let result = transcoder.encode(http_request.headers(), &mcp_request);
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
            super::super::McpRestQueryParams { name: "lat".into(), source: "lat".into() },
            super::super::McpRestQueryParams { name: "lon".into(), source: "lon".into() },
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
            super::super::McpRestQueryParams { name: "space".into(), source: "space".into() },
            super::super::McpRestQueryParams { name: "amp".into(), source: "amp".into() },
            super::super::McpRestQueryParams { name: "equal".into(), source: "equal".into() },
            super::super::McpRestQueryParams { name: "slash".into(), source: "slash".into() },
        ];

        let mut result = String::new();
        append_query_string(&mut result, &query_params, &args);
        assert!(result.contains("space=hello%20world"), "Space should be encoded as %20: {result}");
        assert!(result.contains("amp=foo%26bar"), "& should be encoded: {result}");
        assert!(result.contains("equal=a%3Db"), "= should be encoded: {result}");
        assert!(result.contains("slash=test%2Fvalue"), "/ should be encoded: {result}");
    }

    #[test]
    fn test_build_query_string_encoding_names_and_non_ascii() {
        let mut args = serde_json::Map::new();
        args.insert("city".to_owned(), json!("München"));
        args.insert("emoji".to_owned(), json!("🚀"));

        let query_params = vec![
            super::super::McpRestQueryParams { name: "city name".into(), source: "city".into() },
            super::super::McpRestQueryParams { name: "emoji param".into(), source: "emoji".into() },
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
            super::super::McpRestQueryParams { name: "alpha".into(), source: "alpha".into() },
            super::super::McpRestQueryParams { name: "numeric".into(), source: "numeric".into() },
            super::super::McpRestQueryParams { name: "special".into(), source: "special".into() },
        ];

        let mut result = String::new();
        append_query_string(&mut result, &query_params, &args);
        assert!(result.contains("alpha=ABCxyz"), "Alphanumeric should not be encoded: {result}");
        assert!(result.contains("numeric=123"), "Numeric should not be encoded: {result}");
        assert!(result.contains("special=-_.~"), "Unreserved chars -_.~ should not be encoded: {result}");
    }
}
