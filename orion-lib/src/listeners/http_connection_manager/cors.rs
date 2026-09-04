use std::sync::Arc;

use http::{
    header::{
        ACCESS_CONTROL_ALLOW_CREDENTIALS, ACCESS_CONTROL_ALLOW_HEADERS, ACCESS_CONTROL_ALLOW_METHODS,
        ACCESS_CONTROL_ALLOW_ORIGIN, ACCESS_CONTROL_EXPOSE_HEADERS, ACCESS_CONTROL_MAX_AGE,
        ACCESS_CONTROL_REQUEST_HEADERS, ACCESS_CONTROL_REQUEST_METHOD, ORIGIN, VARY,
    },
    HeaderValue, Method, Request, Response, StatusCode,
};
use http_body_util::Empty;
use orion_configuration::config::network_filters::http_connection_manager::http_filters::cors::CorsConfig;
use str_utils::ToLowercase;
use tracing::debug;

use crate::{
    body::timeout_body::TimeoutBody, listeners::http_filters::FilterDecision, OrionRequestBody, OrionResponseBody,
    PolyBody,
};

#[derive(Debug, Clone)]
pub struct Cors {
    inner: Arc<CorsConfig>,
    validated_origin: Option<HeaderValue>,
    is_wildcard_response: bool,
}

impl From<CorsConfig> for Cors {
    fn from(conf: CorsConfig) -> Self {
        Cors { inner: Arc::new(conf), validated_origin: None, is_wildcard_response: false }
    }
}

impl Cors {
    pub fn apply_request(&mut self, req: &mut Request<OrionRequestBody>) -> FilterDecision {
        debug!(target: "cors", "applying CORS filter to request");
        // 1. Extract Origin Header.
        // If missing, it's not a CORS request (or it is same-origin).
        let Some(origin_header) = req.headers().get(ORIGIN) else {
            return FilterDecision::Continue;
        };

        let Ok(origin_str) = origin_header.to_str() else {
            return FilterDecision::Continue;
        };

        // 2. Validate Origin and save state.
        match self.determine_allowed_origin(origin_str) {
            Some((val, is_wildcard)) => {
                self.validated_origin = Some(val);
                self.is_wildcard_response = is_wildcard;
            },
            None => {
                // Origin not allowed: ignore. Browser will block response due to missing headers.
                return FilterDecision::Continue;
            },
        }

        // 3. Preflight Handling (OPTIONS + Access-Control-Request-Method)
        let is_preflight = req.method() == Method::OPTIONS && req.headers().contains_key(ACCESS_CONTROL_REQUEST_METHOD);

        if is_preflight {
            // Strict Validation: Check if the requested method is actually allowed.
            let Some(req_method_hdr) = req.headers().get(ACCESS_CONTROL_REQUEST_METHOD) else {
                return FilterDecision::Continue; // Should not happen given is_preflight check
            };

            let Ok(req_method) = Method::from_bytes(req_method_hdr.as_bytes()) else {
                debug!(target: "cors", "Preflight failed: Invalid method in Access-Control-Request-Method");
                return FilterDecision::Continue;
            };

            if !self.inner.allow_methods.contains(&req_method) {
                debug!(target: "cors", "Preflight failed: Method {:?} not allowed", req_method);
                return FilterDecision::Continue;
            }

            // Validate Access-Control-Request-Headers if present
            // Note: "*" wildcard in allow_headers is only valid when credentials are disabled
            let has_headers_wildcard =
                !self.inner.allow_credentials && self.inner.allow_headers.iter().any(|h| h == "*");

            if !has_headers_wildcard {
                if let Some(req_headers_hdr) = req.headers().get(ACCESS_CONTROL_REQUEST_HEADERS) {
                    if let Ok(req_headers_str) = req_headers_hdr.to_str() {
                        // Parse comma-separated header names and validate each
                        for requested_header in req_headers_str.split(',').map(|s| s.trim().to_ascii_lowercase_cow()) {
                            if requested_header.is_empty() {
                                continue;
                            }
                            // Check if the requested header is in allowed headers (case-insensitive)
                            // We implicitly allow mcp-session-id to support the MCP Gateway
                            let is_allowed = requested_header == "mcp-session-id"
                                || self
                                    .inner
                                    .allow_headers
                                    .iter()
                                    .any(|h| h.to_ascii_lowercase_cow() == requested_header);
                            if !is_allowed {
                                debug!(target: "cors", "Preflight failed: Header '{}' not allowed", requested_header);
                                return FilterDecision::Continue;
                            }
                        }
                    }
                }
            }

            // If valid, generate the response immediately.
            return self.generate_preflight_response(req.version());
        }

        FilterDecision::Continue
    }

    pub fn apply_response(&mut self, response: &mut Response<OrionResponseBody>) -> FilterDecision {
        debug!(target: "cors", "applying CORS filter to response");
        // If we didn't validate an origin during the request phase, do nothing.
        let Some(allowed_origin) = &self.validated_origin else {
            return FilterDecision::Continue;
        };

        let headers = response.headers_mut();
        let conf = &self.inner;

        // 1. Set Access-Control-Allow-Origin
        headers.insert(ACCESS_CONTROL_ALLOW_ORIGIN, allowed_origin.clone());

        // 2. Set Access-Control-Allow-Credentials
        if conf.allow_credentials {
            headers.insert(ACCESS_CONTROL_ALLOW_CREDENTIALS, HeaderValue::from_static("true"));
        }

        // 3. Set Vary: Origin
        // Logic: If we are reflecting a specific origin (is_wildcard_response == false),
        // OR if credentials are allowed, the response must vary based on Origin.
        if !self.is_wildcard_response || conf.allow_credentials {
            headers.append(VARY, HeaderValue::from_static("Origin"));
        }

        // 4. Set Access-Control-Expose-Headers
        let mut expose_headers = conf.expose_headers.clone();
        if !expose_headers.iter().any(|h| h.eq_ignore_ascii_case("mcp-session-id")) {
            expose_headers.push("mcp-session-id".into());
        }

        if !expose_headers.is_empty() {
            let expose_str = expose_headers.join(", ");
            if let Ok(val) = HeaderValue::from_str(&expose_str) {
                headers.insert(ACCESS_CONTROL_EXPOSE_HEADERS, val);
            }
        }

        FilterDecision::Continue
    }

    fn generate_preflight_response(&self, ver: http::Version) -> FilterDecision {
        debug!(target: "cors", "generating preflight response");
        let Some(allowed_origin) = self.validated_origin.as_ref() else {
            return FilterDecision::internal_server_error(
                "CORS preflight generation called without a validated origin",
                ver,
            );
        };

        let conf = &self.inner;

        // Use 204 No Content for Preflight (Best Practice).
        let mut builder = Response::builder().status(StatusCode::NO_CONTENT);
        let Some(headers) = builder.headers_mut() else {
            unreachable!("failed to get headers mut reference from response builder");
        };

        // A. Origin
        headers.insert(ACCESS_CONTROL_ALLOW_ORIGIN, allowed_origin.clone());

        // B. Credentials
        if conf.allow_credentials {
            headers.insert(ACCESS_CONTROL_ALLOW_CREDENTIALS, HeaderValue::from_static("true"));
        }

        // C. Methods
        let methods_str = conf.allow_methods.iter().map(http::Method::as_str).collect::<Vec<_>>().join(", ");
        if let Ok(val) = HeaderValue::from_str(&methods_str) {
            headers.insert(ACCESS_CONTROL_ALLOW_METHODS, val);
        }

        // D. Headers
        // If wildcard is configured and credentials are disabled, return "*"
        // Otherwise, return the configured list
        let mut allow_headers = conf.allow_headers.clone();
        if !allow_headers.iter().any(|h| h.eq_ignore_ascii_case("mcp-session-id")) {
            allow_headers.push("mcp-session-id".into());
        }

        if !allow_headers.is_empty() {
            let has_headers_wildcard = !conf.allow_credentials && allow_headers.iter().any(|h| h == "*");
            if has_headers_wildcard {
                headers.insert(ACCESS_CONTROL_ALLOW_HEADERS, HeaderValue::from_static("*"));
            } else {
                let headers_str = allow_headers.join(", ");
                if let Ok(val) = HeaderValue::from_str(&headers_str) {
                    headers.insert(ACCESS_CONTROL_ALLOW_HEADERS, val);
                }
            }
        }

        // E. Max Age
        if let Some(age) = conf.max_age {
            if let Ok(val) = HeaderValue::from_str(&age.to_string()) {
                headers.insert(ACCESS_CONTROL_MAX_AGE, val);
            }
        }

        // F. Vary Headers
        // 1. Vary on Origin (Same logic as apply_response)
        if !self.is_wildcard_response || conf.allow_credentials {
            headers.append(VARY, HeaderValue::from_static("Origin"));
        }
        // 2. Always vary on ACR-Method and ACR-Headers for proper preflight caching
        headers.append(VARY, HeaderValue::from_static("Access-Control-Request-Method"));
        headers.append(VARY, HeaderValue::from_static("Access-Control-Request-Headers"));

        // Construct empty body for Orion
        let Ok(response) = builder.version(ver).body(TimeoutBody::new(None, PolyBody::from(Empty::new())).into())
        else {
            return FilterDecision::internal_server_error("failed to build CORS response", ver);
        };

        FilterDecision::DirectResponse(Box::new(response))
    }

    fn determine_allowed_origin(&self, request_origin: &str) -> Option<(HeaderValue, bool)> {
        let conf = &self.inner;

        // Case A: Wildcard allowed AND credentials disabled.
        // We return "*" literally.
        if !conf.allow_credentials && conf.allow_origins.iter().any(|o| o.matches("*")) {
            return Some((HeaderValue::from_static("*"), true));
        }

        // Case B: Exact match (or Reflection if wildcard is used WITH credentials).
        // We return the specific request origin.
        if conf.allow_origins.iter().any(|o| o.matches("*") || o.matches(request_origin)) {
            match HeaderValue::from_str(request_origin) {
                Ok(v) => return Some((v, false)),
                Err(_) => return None,
            }
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orion_configuration::config::core::StringMatcher;

    fn mock_req(method: Method, origin: Option<&str>, acr_method: Option<&str>) -> Request<OrionRequestBody> {
        mock_req_full(method, origin, acr_method, None)
    }

    fn mock_req_full(
        method: Method,
        origin: Option<&str>,
        acr_method: Option<&str>,
        acr_headers: Option<&str>,
    ) -> Request<OrionRequestBody> {
        let mut builder = Request::builder().method(method).uri("https://api.example.com/resource");

        if let Some(o) = origin {
            builder = builder.header(ORIGIN, o);
        }
        if let Some(m) = acr_method {
            builder = builder.header(ACCESS_CONTROL_REQUEST_METHOD, m);
        }
        if let Some(h) = acr_headers {
            builder = builder.header(ACCESS_CONTROL_REQUEST_HEADERS, h);
        }

        let body = OrionRequestBody::default();
        builder.body(body).unwrap()
    }

    // Helper config
    fn basic_config() -> CorsConfig {
        CorsConfig {
            allow_origins: vec![StringMatcher::new("https://allowed.com")],
            allow_methods: vec![Method::GET, Method::POST, Method::OPTIONS],
            allow_credentials: true,
            ..Default::default()
        }
    }

    #[test]
    fn test_allow_specific_origin_with_credentials() {
        let mut cors = Cors::from(basic_config());
        let mut req = mock_req(Method::GET, Some("https://allowed.com"), None);

        // 1. Request Phase
        let decision = cors.apply_request(&mut req);
        assert!(matches!(decision, FilterDecision::Continue));
        assert!(cors.validated_origin.is_some());
        assert!(!cors.is_wildcard_response);

        // 2. Response Phase
        let mut resp = Response::new(OrionResponseBody::default());
        cors.apply_response(&mut resp);

        let headers = resp.headers();
        assert_eq!(headers.get(ACCESS_CONTROL_ALLOW_ORIGIN).unwrap(), "https://allowed.com");
        assert_eq!(headers.get(ACCESS_CONTROL_ALLOW_CREDENTIALS).unwrap(), "true");
        let vary = headers.get_all(VARY).iter().find(|v| *v == "Origin");
        assert!(vary.is_some(), "Vary: Origin missing with credentials!");
    }

    #[test]
    fn test_block_unknown_origin() {
        let mut cors = Cors::from(basic_config());
        let mut req = mock_req(Method::GET, Some("https://evil.com"), None);

        cors.apply_request(&mut req);

        assert!(cors.validated_origin.is_none());

        let mut resp = Response::new(OrionResponseBody::default());
        cors.apply_response(&mut resp);

        assert!(resp.headers().get(ACCESS_CONTROL_ALLOW_ORIGIN).is_none());
    }

    #[test]
    fn test_preflight_success() {
        let mut cors = Cors::from(basic_config());
        let mut req = mock_req(Method::OPTIONS, Some("https://allowed.com"), Some("POST"));

        let decision = cors.apply_request(&mut req);

        if let FilterDecision::DirectResponse(resp) = decision {
            assert_eq!(resp.status(), StatusCode::NO_CONTENT); // Fix 204
            assert_eq!(resp.headers().get(ACCESS_CONTROL_ALLOW_ORIGIN).unwrap(), "https://allowed.com");
            assert_eq!(resp.headers().get(ACCESS_CONTROL_ALLOW_METHODS).unwrap(), "GET, POST, OPTIONS");
        } else {
            panic!("Expected DirectResponse for Preflight");
        }
    }

    #[test]
    fn test_preflight_fail_bad_method() {
        let mut cors = Cors::from(basic_config());
        let mut req = mock_req(Method::OPTIONS, Some("https://allowed.com"), Some("DELETE"));

        let decision = cors.apply_request(&mut req);

        assert!(matches!(decision, FilterDecision::Continue));
    }

    #[test]
    fn test_wildcard_no_credentials() {
        let conf =
            CorsConfig { allow_origins: vec![StringMatcher::new("*")], allow_credentials: false, ..Default::default() };
        let mut cors = Cors::from(conf);
        let mut req = mock_req(Method::GET, Some("https://anyone.com"), None);

        cors.apply_request(&mut req);

        assert!(cors.is_wildcard_response);

        let mut resp = Response::new(OrionResponseBody::default());
        cors.apply_response(&mut resp);

        assert_eq!(resp.headers().get(ACCESS_CONTROL_ALLOW_ORIGIN).unwrap(), "*");
        let vary_origin = resp.headers().get_all(VARY).iter().any(|v| v == "Origin");
        assert!(!vary_origin, "Vary: Origin should not be present for pure wildcard without credentials");
    }

    #[test]
    fn test_no_origin_header_not_cors() {
        let mut cors = Cors::from(basic_config());
        let mut req = mock_req(Method::GET, None, None);

        let decision = cors.apply_request(&mut req);

        assert!(matches!(decision, FilterDecision::Continue));
        assert!(cors.validated_origin.is_none(), "No origin should mean no CORS processing");
    }

    #[test]
    fn test_wildcard_with_credentials_echoes_origin() {
        // When wildcard is used WITH credentials, we must echo the origin, not "*"
        let conf = CorsConfig {
            allow_origins: vec![StringMatcher::new("*")],
            allow_credentials: true,
            allow_methods: vec![Method::GET],
            ..Default::default()
        };
        let mut cors = Cors::from(conf);
        let mut req = mock_req(Method::GET, Some("https://specific.com"), None);

        cors.apply_request(&mut req);

        assert!(!cors.is_wildcard_response, "Should NOT be wildcard when credentials enabled");

        let mut resp = Response::new(OrionResponseBody::default());
        cors.apply_response(&mut resp);

        // Must echo origin, not "*"
        assert_eq!(resp.headers().get(ACCESS_CONTROL_ALLOW_ORIGIN).unwrap(), "https://specific.com");
        assert_eq!(resp.headers().get(ACCESS_CONTROL_ALLOW_CREDENTIALS).unwrap(), "true");
    }

    #[test]
    fn test_preflight_invalid_method_rejected() {
        let mut cors = Cors::from(basic_config());
        // Use an invalid/unparseable method
        let mut req = mock_req(Method::OPTIONS, Some("https://allowed.com"), Some("INVALID METHOD!"));

        let decision = cors.apply_request(&mut req);

        // Should NOT generate preflight response for invalid method
        assert!(matches!(decision, FilterDecision::Continue));
    }

    #[test]
    fn test_preflight_with_allowed_headers() {
        let conf = CorsConfig {
            allow_origins: vec![StringMatcher::new("https://allowed.com")],
            allow_methods: vec![Method::POST],
            allow_headers: vec!["X-Custom-Header".into(), "Content-Type".into()],
            allow_credentials: false,
            ..Default::default()
        };
        let mut cors = Cors::from(conf);
        let mut req = mock_req_full(
            Method::OPTIONS,
            Some("https://allowed.com"),
            Some("POST"),
            Some("X-Custom-Header, Content-Type"),
        );

        let decision = cors.apply_request(&mut req);

        if let FilterDecision::DirectResponse(resp) = decision {
            // mcp-session-id is always appended so MCP Gateway clients can use it cross-origin.
            assert_eq!(
                resp.headers().get(ACCESS_CONTROL_ALLOW_HEADERS).unwrap(),
                "X-Custom-Header, Content-Type, mcp-session-id"
            );
        } else {
            panic!("Expected DirectResponse for valid preflight with headers");
        }
    }

    #[test]
    fn test_preflight_rejects_disallowed_header() {
        let conf = CorsConfig {
            allow_origins: vec![StringMatcher::new("https://allowed.com")],
            allow_methods: vec![Method::POST],
            allow_headers: vec!["Content-Type".into()],
            allow_credentials: false,
            ..Default::default()
        };
        let mut cors = Cors::from(conf);
        // Request a header that's not allowed
        let mut req = mock_req_full(Method::OPTIONS, Some("https://allowed.com"), Some("POST"), Some("X-Not-Allowed"));

        let decision = cors.apply_request(&mut req);

        assert!(matches!(decision, FilterDecision::Continue), "Should reject preflight with disallowed header");
    }

    #[test]
    fn test_preflight_with_max_age() {
        let conf = CorsConfig {
            allow_origins: vec![StringMatcher::new("https://allowed.com")],
            allow_methods: vec![Method::GET],
            max_age: Some(3600),
            ..Default::default()
        };
        let mut cors = Cors::from(conf);
        let mut req = mock_req(Method::OPTIONS, Some("https://allowed.com"), Some("GET"));

        let decision = cors.apply_request(&mut req);

        if let FilterDecision::DirectResponse(resp) = decision {
            assert_eq!(resp.headers().get(ACCESS_CONTROL_MAX_AGE).unwrap(), "3600");
        } else {
            panic!("Expected DirectResponse");
        }
    }

    #[test]
    fn test_response_with_expose_headers() {
        let conf = CorsConfig {
            allow_origins: vec![StringMatcher::new("https://allowed.com")],
            allow_methods: vec![Method::GET],
            expose_headers: vec!["X-Request-Id".into(), "X-Trace-Id".into()],
            ..Default::default()
        };
        let mut cors = Cors::from(conf);
        let mut req = mock_req(Method::GET, Some("https://allowed.com"), None);

        cors.apply_request(&mut req);

        let mut resp = Response::new(OrionResponseBody::default());
        cors.apply_response(&mut resp);

        // mcp-session-id is always appended so MCP Gateway clients can read it cross-origin.
        assert_eq!(
            resp.headers().get(ACCESS_CONTROL_EXPOSE_HEADERS).unwrap(),
            "X-Request-Id, X-Trace-Id, mcp-session-id"
        );
    }

    #[test]
    fn test_preflight_vary_headers() {
        let mut cors = Cors::from(basic_config());
        let mut req = mock_req(Method::OPTIONS, Some("https://allowed.com"), Some("POST"));

        let decision = cors.apply_request(&mut req);

        if let FilterDecision::DirectResponse(resp) = decision {
            let vary_values: Vec<_> = resp.headers().get_all(VARY).iter().collect();
            assert!(vary_values.iter().any(|v| *v == "Origin"), "Missing Vary: Origin");
            assert!(
                vary_values.iter().any(|v| *v == "Access-Control-Request-Method"),
                "Missing Vary: Access-Control-Request-Method"
            );
            assert!(
                vary_values.iter().any(|v| *v == "Access-Control-Request-Headers"),
                "Missing Vary: Access-Control-Request-Headers"
            );
        } else {
            panic!("Expected DirectResponse");
        }
    }

    #[test]
    fn test_options_without_acr_method_not_preflight() {
        // OPTIONS request without Access-Control-Request-Method is NOT a preflight
        let mut cors = Cors::from(basic_config());
        let mut req = mock_req(Method::OPTIONS, Some("https://allowed.com"), None);

        let decision = cors.apply_request(&mut req);

        // Should continue to backend, not generate preflight response
        assert!(matches!(decision, FilterDecision::Continue));
        // But origin should still be validated
        assert!(cors.validated_origin.is_some());
    }

    #[test]
    fn test_multiple_allowed_origins() {
        let conf = CorsConfig {
            allow_origins: vec![
                StringMatcher::new("https://first.com"),
                StringMatcher::new("https://second.com"),
                StringMatcher::new("https://third.com"),
            ],
            allow_methods: vec![Method::GET],
            ..Default::default()
        };

        // Test first origin
        let mut cors1 = Cors::from(conf.clone());
        let mut req1 = mock_req(Method::GET, Some("https://first.com"), None);
        cors1.apply_request(&mut req1);
        assert!(cors1.validated_origin.is_some());

        // Test second origin
        let mut cors2 = Cors::from(conf.clone());
        let mut req2 = mock_req(Method::GET, Some("https://second.com"), None);
        cors2.apply_request(&mut req2);
        assert!(cors2.validated_origin.is_some());

        // Test unlisted origin
        let mut cors3 = Cors::from(conf);
        let mut req3 = mock_req(Method::GET, Some("https://notlisted.com"), None);
        cors3.apply_request(&mut req3);
        assert!(cors3.validated_origin.is_none());
    }

    #[test]
    fn test_vary_origin_for_specific_origins_without_credentials() {
        // Even without credentials, if we echo a specific origin (not "*"),
        // we need Vary: Origin for caching correctness
        let conf = CorsConfig {
            allow_origins: vec![StringMatcher::new("https://specific.com")],
            allow_credentials: false,
            allow_methods: vec![Method::GET],
            ..Default::default()
        };
        let mut cors = Cors::from(conf);
        let mut req = mock_req(Method::GET, Some("https://specific.com"), None);

        cors.apply_request(&mut req);

        let mut resp = Response::new(OrionResponseBody::default());
        cors.apply_response(&mut resp);

        // Should have Vary: Origin because we're echoing a specific origin
        let vary_origin = resp.headers().get_all(VARY).iter().any(|v| v == "Origin");
        assert!(vary_origin, "Vary: Origin required when echoing specific origin");
    }

    #[test]
    fn test_preflight_header_validation_case_insensitive() {
        let conf = CorsConfig {
            allow_origins: vec![StringMatcher::new("https://allowed.com")],
            allow_methods: vec![Method::POST],
            allow_headers: vec!["Content-Type".into()],
            allow_credentials: false,
            ..Default::default()
        };
        let mut cors = Cors::from(conf);
        // Request header with different case
        let mut req = mock_req_full(
            Method::OPTIONS,
            Some("https://allowed.com"),
            Some("POST"),
            Some("content-type"), // lowercase
        );

        let decision = cors.apply_request(&mut req);

        // Should succeed - header comparison is case-insensitive
        assert!(matches!(decision, FilterDecision::DirectResponse(_)), "Header validation should be case-insensitive");
    }

    #[test]
    fn test_wildcard_allow_headers_without_credentials() {
        // "*" in allow_headers should allow any header when credentials are disabled
        let conf = CorsConfig {
            allow_origins: vec![StringMatcher::new("https://allowed.com")],
            allow_methods: vec![Method::POST],
            allow_headers: vec!["*".into()],
            allow_credentials: false,
            ..Default::default()
        };
        let mut cors = Cors::from(conf);
        let mut req = mock_req_full(
            Method::OPTIONS,
            Some("https://allowed.com"),
            Some("POST"),
            Some("X-Any-Custom-Header, X-Another-Header"),
        );

        let decision = cors.apply_request(&mut req);

        if let FilterDecision::DirectResponse(resp) = decision {
            // Should return "*" in the response
            assert_eq!(resp.headers().get(ACCESS_CONTROL_ALLOW_HEADERS).unwrap(), "*");
        } else {
            panic!("Expected DirectResponse for wildcard headers");
        }
    }

    #[test]
    fn test_wildcard_allow_headers_with_credentials_not_wildcard() {
        // "*" with credentials enabled should NOT act as wildcard
        // It should be treated as a literal "*" header name
        let conf = CorsConfig {
            allow_origins: vec![StringMatcher::new("https://allowed.com")],
            allow_methods: vec![Method::POST],
            allow_headers: vec!["*".into()],
            allow_credentials: true,
            ..Default::default()
        };
        let mut cors = Cors::from(conf);
        let mut req = mock_req_full(
            Method::OPTIONS,
            Some("https://allowed.com"),
            Some("POST"),
            Some("X-Custom-Header"), // This is not "*"
        );

        let decision = cors.apply_request(&mut req);

        // Should FAIL because "*" is literal when credentials enabled
        assert!(matches!(decision, FilterDecision::Continue), "Wildcard should not work with credentials enabled");
    }

    #[test]
    fn test_default_config_is_permissive() {
        // Test that the default configuration is truly permissive
        let conf = CorsConfig::default();
        let mut cors = Cors::from(conf);

        // Test preflight with any origin, common method, and custom headers
        let mut req = mock_req_full(
            Method::OPTIONS,
            Some("https://any-origin.com"),
            Some("PUT"),
            Some("X-Custom-Header, Authorization, Content-Type"),
        );

        let decision = cors.apply_request(&mut req);

        if let FilterDecision::DirectResponse(resp) = decision {
            assert_eq!(resp.headers().get(ACCESS_CONTROL_ALLOW_ORIGIN).unwrap(), "*");
            assert_eq!(resp.headers().get(ACCESS_CONTROL_ALLOW_HEADERS).unwrap(), "*");
            // Should contain PUT in methods
            let methods = resp.headers().get(ACCESS_CONTROL_ALLOW_METHODS).unwrap().to_str().unwrap();
            assert!(methods.contains("PUT"), "Default config should allow PUT");
            assert!(methods.contains("PATCH"), "Default config should allow PATCH");
            assert!(methods.contains("DELETE"), "Default config should allow DELETE");
        } else {
            panic!("Default config should allow permissive preflight");
        }
    }

    #[test]
    fn test_default_config_simple_request() {
        let conf = CorsConfig::default();
        let mut cors = Cors::from(conf);

        // Simple GET request from any origin
        let mut req = mock_req(Method::GET, Some("https://random-site.com"), None);
        cors.apply_request(&mut req);

        let mut resp = Response::new(OrionResponseBody::default());
        cors.apply_response(&mut resp);

        // Should allow with wildcard
        assert_eq!(resp.headers().get(ACCESS_CONTROL_ALLOW_ORIGIN).unwrap(), "*");
    }
}
