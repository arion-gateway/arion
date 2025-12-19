pub mod claims;
use claims::JwtClaims;

use std::{
    borrow::{Borrow, Cow},
    collections::HashMap,
    str::FromStr,
    string::FromUtf8Error,
    sync::Arc,
};

use crate::{
    body::{instrumented_body::InstrumentedBody, timeout_body::TimeoutBody},
    event_error::EventFailure,
    listeners::{http_connection_manager::FilterDecision, synthetic_http_response::SyntheticHttpResponse},
    PolyBody,
};
use http::{HeaderMap, HeaderName, HeaderValue, Request};
use jsonwebtoken::{decode, decode_header, jwk::Jwk, Algorithm, DecodingKey, TokenData, Validation};
use orion_configuration::config::{
    core::DataSourceReadError,
    network_filters::http_connection_manager::http_filters::jwt::{
        JwksSourceSpecifier, JwtAuthentication as JwtAuthenticationConfig, JwtProvider, RequirementType, RequiresType,
    },
};
use ref_cast::RefCast;
use smol_str::SmolStr;
use thiserror::Error;
use tracing::{debug, error, info, warn};

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Kid(SmolStr);

#[derive(Debug, PartialEq, Eq, Hash, RefCast)]
#[repr(transparent)]
pub struct KidStr(str);

impl Borrow<KidStr> for Kid {
    fn borrow(&self) -> &KidStr {
        KidStr::ref_cast(&self.0)
    }
}

impl std::fmt::Display for Kid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone)]
pub struct ValidationKey {
    decoding_key: DecodingKey,
    validation: Validation,
}

#[derive(Debug, Clone)]
struct ProviderContext {
    /// Valid keys from static config or remote endpoint derived from Jwk
    keys: HashMap<Kid, ValidationKey, ahash::RandomState>,
}

impl ProviderContext {
    pub fn validation_key_lookup(&self, kid: Option<&KidStr>) -> Option<&ValidationKey> {
        match kid {
            Some(kid) => self.keys.get(kid),
            None => {
                if self.keys.len() == 1 {
                    self.keys.values().next()
                } else {
                    None
                }
            },
        }
    }
}

#[derive(Debug, Error)]
enum JwkError {
    #[error("Invalid JWK: {0}")]
    InvalidJWK(#[from] serde_json::Error),

    #[error("MissingKeyId")]
    MissingKeyId,

    #[error("No alg in JWK")]
    NoAlgInJwk,

    #[error("Missing keys array")]
    MissingKeysArray,

    #[error("JsonWebToken: {0}")]
    JsonWebToken(#[from] jsonwebtoken::errors::Error),

    #[error("Data source: {0}")]
    DataSourceError(#[from] DataSourceReadError),

    #[error("Utf8: {0}")]
    FromUtf8Error(#[from] FromUtf8Error),
}

#[derive(Debug, Clone)]
pub struct JwtAuthenticationBuilder {
    config: JwtAuthenticationConfig,
}

impl JwtAuthenticationBuilder {
    pub fn new(config: JwtAuthenticationConfig) -> Self {
        JwtAuthenticationBuilder { config }
    }

    pub fn build_validation(alg: Algorithm, provider: &JwtProvider) -> Validation {
        let mut validation = Validation::new(alg);

        if !provider.issuer.is_empty() {
            validation.set_issuer(std::slice::from_ref(&provider.issuer));
        }

        if provider.audiences.is_empty() {
            validation.validate_aud = false;
        } else {
            validation.set_audience(&provider.audiences);
            validation.validate_aud = true;
        }

        validation.leeway = u64::from(provider.clock_skew_seconds);
        validation.validate_exp = true;

        validation
    }

    fn parse_and_validate_keys(
        provider: &JwtProvider,
    ) -> Result<HashMap<Kid, ValidationKey, ahash::RandomState>, JwkError> {
        match &provider.jwks_source_specifier {
            JwksSourceSpecifier::LocalJwks(data_src) => {
                let bytes = data_src.to_bytes_blocking()?;
                let string = String::from_utf8(bytes)?;
                let jwks: serde_json::Value = serde_json::from_str(&string)?;
                let mut keys = HashMap::<Kid, ValidationKey, ahash::RandomState>::default();
                if let Some(keys_array) = jwks.get("keys").and_then(|v| v.as_array()) {
                    for key_value in keys_array {
                        info!(target: "jwt", "JWK: {:?}", key_value);
                        let jwk: Jwk = serde_json::from_str(&key_value.to_string())?;
                        let kid = Kid(jwk.common.key_id.as_ref().ok_or(JwkError::MissingKeyId)?.into());
                        debug!(target: "jwt", "Kid: {}", kid);
                        let key_alg = jwk.common.key_algorithm.ok_or(JwkError::NoAlgInJwk)?;
                        let alg = Algorithm::from_str(&key_alg.to_string())?;
                        let decoding_key = DecodingKey::from_jwk(&jwk)?;
                        let validation = Self::build_validation(alg, provider);
                        keys.insert(kid, ValidationKey { decoding_key, validation });
                    }
                } else {
                    return Err(JwkError::MissingKeysArray);
                }

                Ok(keys)
            },
            JwksSourceSpecifier::RemoteJwks(_conf) => {
                warn!(target: "jwt", "RemoteJwks not implemented yet");
                Ok(HashMap::default())
            },
        }
    }

    pub fn build(self) -> JwtAuthentication {
        debug!(target: "jwt", "Creating new JWT authentication filter");
        let config = self.config;

        let mut providers = HashMap::<SmolStr, ProviderContext, ahash::RandomState>::default();
        for (provider, prov_config) in &config.providers {
            match Self::parse_and_validate_keys(prov_config) {
                Ok(keys) => {
                    providers.insert(provider.to_owned(), ProviderContext { keys });
                },
                Err(e) => {
                    error!(target: "jwt", "Provider '{}'. {}", provider, e);
                },
            }
        }

        JwtAuthentication { inner: Arc::new(JwtAuthenticationInner { config, context: providers }) }
    }
}

#[derive(Debug, Clone)]
pub struct JwtAuthenticationInner {
    config: JwtAuthenticationConfig,
    context: HashMap<SmolStr, ProviderContext, ahash::RandomState>,
}

#[derive(Debug, Clone)]
pub struct JwtAuthentication {
    inner: Arc<JwtAuthenticationInner>,
}

pub enum JwtExtract<'a, 'b> {
    Header(&'a HeaderName, Cow<'b, str>),
    QueryParams(Cow<'b, str>, Cow<'b, str>),
    Cookie(Cow<'b, str>),
}

impl JwtExtract<'_, '_> {
    pub fn token(&self) -> &str {
        match self {
            JwtExtract::Header(_, token) | JwtExtract::QueryParams(_, token) | JwtExtract::Cookie(token) => token,
        }
    }
}

impl JwtAuthentication {
    pub fn extract_token<'a, 'b, B>(&'a self, provider_name: &str, req: &'b Request<B>) -> Option<JwtExtract<'a, 'b>> {
        let provider = self.inner.config.providers.get(provider_name)?;

        // Try to extract token from headers
        for hdr in &provider.from_headers {
            if let Some(header_value) = req.headers().get(&hdr.name) {
                if let Ok(header_str) = header_value.to_str() {
                    // Check if the header value starts with the expected prefix
                    if let Some(token) = header_str.strip_prefix(&hdr.value_prefix) {
                        let token = token.trim();
                        if !token.is_empty() {
                            debug!(target: "jwt", "Token extracted from header '{}' with prefix '{}'", hdr.name, hdr.value_prefix);
                            return Some(JwtExtract::Header(&hdr.name, Cow::Borrowed(token)));
                        }
                    }
                }
            }
        }

        // Try to extract token from query parameters
        if let Some(query) = req.uri().query() {
            for param_name in &provider.from_params {
                for (key, value) in url::form_urlencoded::parse(query.as_bytes()) {
                    if key == param_name.as_str() && !value.is_empty() {
                        debug!(target: "jwt", "Token extracted from query parameter '{}'", param_name);
                        return Some(JwtExtract::QueryParams(key, value));
                    }
                }
            }
        }

        // Try to extract token from cookies
        if let Some(cookie_header) = req.headers().get(http::header::COOKIE) {
            if let Ok(cookie_str) = cookie_header.to_str() {
                for cookie_name in &provider.from_cookies {
                    for cookie in cookie_str.split(';') {
                        let cookie = cookie.trim();
                        if let Some((name, value)) = cookie.split_once('=') {
                            if name == cookie_name.as_str() && !value.is_empty() {
                                debug!(target: "jwt", "Token extracted from cookie '{}'", cookie_name);
                                return Some(JwtExtract::Cookie(Cow::Borrowed(value)));
                            }
                        }
                    }
                }
            }
        }

        debug!(target: "jwt", "No token found for provider '{}'", provider_name);
        None
    }

    pub fn provider_lookup<B>(&self, request: &Request<B>) -> Option<&str> {
        // Iterate through the rules to find a matching provider
        for rule in &self.inner.config.rules {
            // Check if the request matches the rule's route match criteria
            // if the rule has no route match, it applies to all requests
            let matches = rule.r#match.as_ref().is_none_or(|route_match| {
                route_match.match_request(request).matched()
            });

            if matches {
                // Extract the provider name from the requirement type
                if let Some(requirement_type) = &rule.requirement_type {
                    match requirement_type {
                        RequirementType::Requires(jwt_req) => {
                            if let Some(requires_type) = &jwt_req.requires_type {
                                match requires_type {
                                    RequiresType::ProviderName(provider_name) => {
                                        debug!(target: "jwt", "Provider '{}' matched for request", provider_name);
                                        return Some(provider_name.as_str());
                                    },
                                }
                            }
                        },
                        RequirementType::RequirementName(_name) => {
                            // RequirementName would need to be resolved from requirement_map
                            // which is not currently supported
                            warn!(target: "jwt", "RequirementName not supported yet");
                        },
                    }
                }
            }
        }

        None
    }

    #[inline]
    fn unauthorized(ver: http::Version, msg: &str) -> FilterDecision {
        FilterDecision::DirectResponse(
            SyntheticHttpResponse::unauthorized(EventFailure::RbacAccessDenied(msg.into()).into(), msg)
                .into_response(ver),
        )
    }

    fn claims_to_headers(jwt_provider: &JwtProvider, claims: &JwtClaims, headers: &mut HeaderMap<HeaderValue>) {
        for claim_to_header in &jwt_provider.claim_to_headers {
            match claim_to_header.claim_name.as_str() {
                "iss" => {
                    if let Some(value) = &claims.iss {
                        let header_value = http::HeaderValue::from_str(value);
                        if let Ok(header_value) = header_value {
                            headers.insert(&claim_to_header.header_name, header_value);
                        }
                    }
                },
                "sub" => {
                    if let Some(value) = &claims.sub {
                        let header_value = http::HeaderValue::from_str(value);
                        if let Ok(header_value) = header_value {
                            headers.insert(&claim_to_header.header_name, header_value);
                        }
                    }
                },
                "aud" => {
                    if let Some(value) = &claims.aud {
                        let header_value = http::HeaderValue::from_str(&format!("{value:?}"));
                        if let Ok(header_value) = header_value {
                            headers.insert(&claim_to_header.header_name, header_value);
                        }
                    }
                },
                "exp" => {
                    if let Some(value) = &claims.exp {
                        let header_value = http::HeaderValue::from_str(&value.to_string());
                        if let Ok(header_value) = header_value {
                            headers.insert(&claim_to_header.header_name, header_value);
                        }
                    }
                },
                "iat" => {
                    if let Some(value) = &claims.iat {
                        let header_value = http::HeaderValue::from_str(&value.to_string());
                        if let Ok(header_value) = header_value {
                            headers.insert(&claim_to_header.header_name, header_value);
                        }
                    }
                },
                "nbf" => {
                    if let Some(value) = &claims.nbf {
                        let header_value = http::HeaderValue::from_str(&value.to_string());
                        if let Ok(header_value) = header_value {
                            headers.insert(&claim_to_header.header_name, header_value);
                        }
                    }
                },
                "jti" => {
                    if let Some(value) = &claims.jti {
                        let header_value = http::HeaderValue::from_str(value);
                        if let Ok(header_value) = header_value {
                            headers.insert(&claim_to_header.header_name, header_value);
                        }
                    }
                },
                _ => {
                    if let Some(value) = &claims.extra.get(claim_to_header.claim_name.as_str()) {
                        let header_value = http::HeaderValue::from_str(&value.to_string());
                        if let Ok(header_value) = header_value {
                            headers.insert(&claim_to_header.header_name, header_value);
                        }
                    }
                },
            }
        }
    }

    #[allow(clippy::too_many_lines)]
    pub fn apply_request(&mut self, req: &mut Request<InstrumentedBody<TimeoutBody<PolyBody>>>) -> FilterDecision {
        debug!(target: "jwt", "Applying JWT authentication filter: {:#?}", self.inner.config);

        // lookup the provider name...
        let Some(provider_name) = self.provider_lookup(req) else {
            warn!(target: "jwt", "JWT no provider configured");
            return Self::unauthorized(req.version(), "JWT no provider configured");
        };

        // extract token from the request...
        let Some(jwt_extract) = self.extract_token(provider_name, req) else {
            warn!(target: "jwt", "JWT no token found");
            return Self::unauthorized(req.version(), "JWT no token found");
        };

        // decode the header token...
        let Ok(header) = decode_header(jwt_extract.token()) else {
            warn!(target: "jwt", "JWT failed to decode token header");
            return Self::unauthorized(req.version(), "JWT failed to decode token header");
        };

        // get the validation_key for this provider...
        let validation_key = self.inner.context.get(provider_name).and_then(|context| {
            let kid_ref = header.kid.as_ref().map(|kid| KidStr::ref_cast(kid));
            context.validation_key_lookup(kid_ref)
        });

        // get the associated validation key...
        let Some(val_key) = validation_key else {
            warn!(target: "jwt", "JWT no validation key found");
            return Self::unauthorized(req.version(), "JWT no validation key found");
        };

        // finally decode the JWT token...
        let jwt: TokenData<JwtClaims> = match decode(jwt_extract.token(), &val_key.decoding_key, &val_key.validation) {
            Ok(jwt) => jwt,
            Err(err) => {
                warn!(target: "jwt", "JWT failed to decode token: {}", err);
                return Self::unauthorized(req.version(), "JWT failed to decode token");
            },
        };

        // retrieve configuration for this provider...
        let Some(jwt_provider) = self.inner.config.providers.get(provider_name) else {
            warn!(target: "jwt", "JWT no provider found");
            return Self::unauthorized(req.version(), "JWT no provider found");
        };

        // handle forward option
        if !jwt_provider.forward {
            match jwt_extract {
                JwtExtract::Header(name, _) => {
                    // Remove the Header token parameter from the query string
                    _ = req.headers_mut().remove(name);
                },
                JwtExtract::QueryParams(key, _) => {
                    // Remove the JWT token parameter from the query string
                    if let Some(query) = req.uri().query() {
                        let filtered_params: Vec<(Cow<str>, Cow<str>)> =
                            url::form_urlencoded::parse(query.as_bytes()).filter(|(k, _)| k != key.as_ref()).collect();

                        // Rebuild the URI without the JWT parameter
                        let mut parts = req.uri().clone().into_parts();

                        let new_path_and_query = if filtered_params.is_empty() {
                            // No query parameters left, just keep the path
                            http::uri::PathAndQuery::from_str(req.uri().path())
                        } else {
                            // Reconstruct query string with remaining parameters
                            let new_query = url::form_urlencoded::Serializer::new(String::new())
                                .extend_pairs(filtered_params)
                                .finish();
                            let path = req.uri().path();
                            http::uri::PathAndQuery::from_str(&format!("{path}?{new_query}"))
                        };

                        match new_path_and_query {
                            Ok(pq) => {
                                parts.path_and_query = Some(pq);
                                match http::Uri::from_parts(parts) {
                                    Ok(new_uri) => *req.uri_mut() = new_uri,
                                    Err(e) => warn!(target: "jwt", "Failed to rebuild URI from parts: {}", e),
                                }
                            },
                            Err(e) => {
                                warn!(target: "jwt", "Failed to parse path and query after removing JWT parameter: {}", e)
                            },
                        }
                    }
                },
                JwtExtract::Cookie(_) => (),
            }
        }

        // handle claim_to_headers

        if !jwt_provider.claim_to_headers.is_empty() {
            Self::claims_to_headers(jwt_provider, &jwt.claims, req.headers_mut());
        }

        if jwt_provider.header_in_metadata.is_some() {
            req.extensions_mut().insert(header);
        }

        if jwt_provider.payload_in_metadata.is_some() {
            req.extensions_mut().insert(jwt.claims.clone());
        }

        debug!(target: "jwt", "{:#?}", jwt);

        // handle clear_route_cache
        if jwt_provider.clear_route_cache {
            FilterDecision::Reroute
        } else {
            FilterDecision::Continue
        }
    }
}
