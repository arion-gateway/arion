pub mod claims;
mod error;
mod jwks;

use arc_swap::ArcSwap;
use claims::JwtClaims;
use tokio::sync::OnceCell;

use std::{
    borrow::{Borrow, Cow},
    collections::HashMap,
    str::FromStr,
    sync::Arc,
    time::Instant,
};

use crate::listeners::http_connection_manager::jwt_authn::{
    error::JwkError,
    jwks::{fetch_remote_jwks, parse_jwks},
};
use crate::{
    event_error::EventFailure,
    listeners::{
        http_filters::{FilterDecision, FilterFactory},
        synthetic_http_response::SyntheticHttpResponse,
    },
    OrionRequestBody,
};
use http::{HeaderMap, HeaderName, HeaderValue, Request};
use jsonwebtoken::{decode, decode_header, DecodingKey, Header, TokenData, Validation};
use orion_configuration::config::network_filters::http_connection_manager::http_filters::jwt::{
    JwksSourceSpecifier, JwtAuthentication as JwtAuthenticationConfig, JwtProvider, RequirementType, RequiresType,
};
use ref_cast::RefCast;
use smol_str::SmolStr;
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

#[derive(Debug)]
enum Asset<T> {
    Permanent(T),
    Expiring((T, Instant)),
    Pending,
}

type ValidationKeyMap = Asset<HashMap<Kid, Arc<ValidationKey>, ahash::RandomState>>;

#[derive(Debug)]
struct ProviderContext {
    /// Valid keys from static config or remote endpoint derived from Jwk
    key_map: ArcSwap<ValidationKeyMap>,
}

impl ProviderContext {
    pub async fn validation_key_lookup(
        &self,
        kid: Option<&KidStr>,
        provider_name: &str,
        provider_config: &JwtProvider,
    ) -> Option<Asset<Arc<ValidationKey>>> {
        if let Some(kid) = kid {
            let m = self.key_map.load_full();
            match m.as_ref() {
                Asset::Permanent(keys) => keys.get(kid).cloned().map(Asset::Permanent),
                Asset::Expiring((keys, expiration)) => {
                    if Instant::now() > *expiration {
                        // refresh keys if necessary...
                        if let JwksSourceSpecifier::RemoteJwks(remote) = &provider_config.jwks_source_specifier {
                            if let Ok(new_keys) = fetch_remote_jwks(remote, provider_name, provider_config).await {
                                let deadline = Instant::now() + remote.cache_duration;
                                self.key_map.store(Arc::new(Asset::Expiring((new_keys, deadline))));
                            }
                        }
                    }

                    // for simplicity, validate this token with with the key set,
                    // no matter if the cache is expired, the key is still valid.
                    // next requests will be validated with the fresh key set

                    keys.get(kid).cloned().map(|x| Asset::Expiring((x, *expiration)))
                },
                Asset::Pending => {
                    // force remote key fetching...
                    if let JwksSourceSpecifier::RemoteJwks(remote) = &provider_config.jwks_source_specifier {
                        if let Ok(new_keys) = fetch_remote_jwks(remote, provider_name, provider_config).await {
                            let deadline = Instant::now() + remote.cache_duration;
                            let res = new_keys.get(kid).cloned().map(|x| Asset::Expiring((x, deadline)));
                            self.key_map.store(Arc::new(Asset::Expiring((new_keys, deadline))));
                            return res;
                        }
                    }

                    Some(Asset::Pending)
                },
            }
        } else {
            let m = self.key_map.load_full();
            match m.as_ref() {
                Asset::Permanent(keys) => keys.values().next().cloned().map(Asset::Permanent),
                Asset::Expiring((keys, expiration)) => {
                    // Regardless of whether the keys are expiring, we will continue validating with them
                    // until the fetcher updates them.
                    keys.values().next().cloned().map(|x| Asset::Expiring((x, *expiration)))
                },
                Asset::Pending => Some(Asset::Pending), // validation key is pending...
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct JwtAuthenticationBuilder {
    config: JwtAuthenticationConfig,
}

struct JwtCachedEntry {
    header: Header,
    jwt: TokenData<JwtClaims>,
}

thread_local! {
    static JWT_CACHE: moka::sync::Cache<String, Arc<JwtCachedEntry>, ahash::RandomState> =
        moka::sync::CacheBuilder::new(128)
            .time_to_live(std::time::Duration::from_secs(1))
            .build_with_hasher(ahash::RandomState::new());
}

impl JwtAuthenticationBuilder {
    pub fn new(config: JwtAuthenticationConfig) -> Self {
        JwtAuthenticationBuilder { config }
    }

    fn parse_and_validate_keys(provider: &JwtProvider) -> Result<ValidationKeyMap, JwkError> {
        match &provider.jwks_source_specifier {
            JwksSourceSpecifier::LocalJwks(data_src) => {
                let bytes = data_src.to_bytes_blocking()?;
                let parsed_keys = parse_jwks(&bytes, provider)?;
                info!(target: "jwt", "using permanent local JWKS");
                Ok(Asset::Permanent(parsed_keys))
            },
            JwksSourceSpecifier::RemoteJwks(_conf) => {
                info!(target: "jwt", "using remote JWKS");
                Ok(Asset::Pending)
            },
        }
    }

    pub fn build(self) -> JwtAuthentication {
        debug!(target: "jwt", "Creating new JWT authentication filter");
        let config = self.config;

        let mut providers = HashMap::<SmolStr, Arc<ProviderContext>, ahash::RandomState>::default();
        let mut has_remote_jwks = false;
        for (provider, prov_config) in &config.providers {
            match Self::parse_and_validate_keys(prov_config) {
                Ok(keys) => {
                    if matches!(keys, Asset::Expiring(_)) || matches!(keys, Asset::Pending) {
                        has_remote_jwks = true;
                    }
                    providers.insert(
                        provider.to_owned(),
                        Arc::new(ProviderContext { key_map: ArcSwap::new(Arc::new(keys)) }),
                    );
                },
                Err(e) => {
                    error!(target: "jwt", "{provider}: {}", e);
                },
            }
        }

        let inner = Arc::new(JwtAuthenticationInner {
            config,
            context: JwtAuthenticationContext { providers },
            has_remote_jwks,
            jwks_fetchers: OnceCell::new(),
        });

        JwtAuthentication { inner }
    }
}

#[derive(Debug)]
pub struct JwtAuthenticationContext {
    providers: HashMap<SmolStr, Arc<ProviderContext>, ahash::RandomState>,
}

#[derive(Debug)]
pub struct JwtAuthenticationInner {
    config: JwtAuthenticationConfig,
    context: JwtAuthenticationContext,
    has_remote_jwks: bool,
    jwks_fetchers: OnceCell<HashMap<SmolStr, tokio_util::task::AbortOnDropHandle<()>>>,
}

#[derive(Debug)]
pub struct JwtAuthentication {
    inner: Arc<JwtAuthenticationInner>,
}

impl FilterFactory for JwtAuthentication {
    fn new_from(&self) -> Self {
        JwtAuthentication { inner: Arc::clone(&self.inner) }
    }
}

impl Clone for JwtAuthentication {
    fn clone(&self) -> Self {
        JwtAuthentication { inner: Arc::clone(&self.inner) }
    }
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
                            debug!(target: "jwt", "token extracted from header '{}' with prefix '{}'", hdr.name, hdr.value_prefix);
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
                        debug!(target: "jwt", "token extracted from query parameter '{}'", param_name);
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
                                debug!(target: "jwt", "token extracted from cookie '{}'", cookie_name);
                                return Some(JwtExtract::Cookie(Cow::Borrowed(value)));
                            }
                        }
                    }
                }
            }
        }

        debug!(target: "jwt", "no token found for provider '{}'", provider_name);
        None
    }

    pub fn provider_lookup<B>(&self, request: &Request<B>) -> Option<&str> {
        // Iterate through the rules to find a matching provider
        for rule in &self.inner.config.rules {
            // Check if the request matches the rule's route match criteria
            // if the rule has no route match, it applies to all requests
            let matches = rule.r#match.as_ref().is_none_or(|route_match| route_match.match_request(request).matched());

            if matches {
                // Extract the provider name from the requirement type
                if let Some(requirement_type) = &rule.requirement_type {
                    match requirement_type {
                        RequirementType::Requires(jwt_req) => {
                            if let Some(requires_type) = &jwt_req.requires_type {
                                match requires_type {
                                    RequiresType::ProviderName(provider_name) => {
                                        debug!(target: "jwt", "provider '{}' matched for request", provider_name);
                                        return Some(provider_name.as_str());
                                    },
                                }
                            }
                        },
                        RequirementType::RequirementName(_name) => {
                            // RequirementName would need to be resolved from requirement_map
                            // which is not currently supported
                            warn!(target: "jwt", "requirement name not supported yet");
                        },
                    }
                }
            }
        }

        None
    }

    fn write_claims_to_headers(jwt_provider: &JwtProvider, claims: &JwtClaims, headers: &mut HeaderMap<HeaderValue>) {
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

    async fn start_jwks_fetcher(&mut self) {
        let inner_clone = Arc::clone(&self.inner);
        let _ = self
            .inner
            .jwks_fetchers
            .get_or_init(|| async move {
                debug!(target: "jwt", "starting JWKS fetchers");
                let mut fetchers = HashMap::new();
                for (provider, context) in &inner_clone.context.providers {
                    if let Some(conf) = inner_clone.config.providers.get(provider) {
                        let provider_name = provider.clone();
                        let provider_config = Arc::clone(conf);
                        let context = Arc::clone(context);

                        let remote = match inner_clone
                            .config
                            .providers
                            .get(&provider_name).map(|config| &config.jwks_source_specifier)
                        {
                            Some(JwksSourceSpecifier::RemoteJwks(remote)) => remote.clone(),
                            _ => continue,
                        };

                        let task = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(async move {
                            let mut interval = tokio::time::interval(remote.cache_duration);

                            loop {
                                match context.key_map.load().as_ref() {
                                    Asset::Expiring(_) | Asset::Pending => {
                                        debug!(target: "jwt", "{provider_name}: updating remote provider JWKS...");

                                        let new_keys = match fetch_remote_jwks(&remote, &provider_name, &provider_config).await {
                                            Ok(res) => res,
                                            Err(err) => {
                                                error!(target: "jwt", "{provider_name}: failed to fetch JWKS from {}: {}", remote.http_uri.uri, err);
                                                interval.tick().await;
                                                continue;
                                            }
                                        };

                                        // store the new keys...

                                        let deadline = Instant::now() + remote.cache_duration;
                                        context.key_map.store(Arc::new(Asset::Expiring((new_keys, deadline))));

                                        // sleep until next tick
                                        interval.tick().await;
                                    },
                                    Asset::Permanent(_) => (), // let's skip this
                                }
                            }
                        }));

                        fetchers.insert(provider.clone(), task);
                    }
                }

                fetchers
            })
            .await;
    }

    async fn decode_jwt_token(&self, token: &str, provider_name: &str) -> Result<Arc<JwtCachedEntry>, JwkError> {
        // using the last part of the token as the cache key improves lookup performance significantly
        //
        let cache_key = match token.rsplit_once('.') {
            Some((_, signature)) => signature,
            None => token,
        };

        if let Some(result) = JWT_CACHE.with(|cache| cache.get(cache_key)) {
            return Ok(result);
        }

        // decode the header token...
        let header = decode_header(token).inspect_err(|err| {
            info!(target: "jwt", "failed to decode token header: {err}");
        })?;

        // get the validation_key for this provider...
        //

        let provider_config =
            self.inner.config.providers.get(provider_name).ok_or(JwkError::NoProviderFound(provider_name.into()))?;

        let validation_key = match self.inner.context.providers.get(provider_name) {
            Some(context) => {
                let kid_ref = header.kid.as_ref().map(|kid| KidStr::ref_cast(kid));
                match context.validation_key_lookup(kid_ref, provider_name, provider_config).await {
                    Some(Asset::Permanent(key) | Asset::Expiring((key, _))) => Ok(key),
                    Some(Asset::Pending) | None => Err(JwkError::NoValidationKey),
                }
            },
            None => Err(JwkError::NoValidationKey),
        }?;

        // finally decode the JWT token...
        let res = Arc::new(JwtCachedEntry {
            header,
            jwt: decode(token, &validation_key.decoding_key, &validation_key.validation)?,
        });

        JWT_CACHE.with(|cache| {
            cache.insert(cache_key.to_owned(), Arc::clone(&res));
        });
        Ok(res)
    }

    #[allow(clippy::too_many_lines)]
    pub async fn apply_request(&mut self, req: &mut Request<OrionRequestBody>) -> FilterDecision {
        debug!(target: "jwt", "applying authentication filter");

        // start jwks fetcher if not started yet..
        if self.inner.has_remote_jwks && !self.inner.jwks_fetchers.initialized() {
            self.start_jwks_fetcher().await;
        }

        // lookup the provider name...
        let Some(provider_name) = self.provider_lookup(req) else {
            info!(target: "jwt", "no provider found for request");
            return Self::unauthorized(req.version(), "no provider found for request");
        };

        // extract token from the request...
        let Some(jwt_extract) = self.extract_token(provider_name, req) else {
            info!(target: "jwt", "could not extract token for provider from request ({provider_name})");
            return Self::unauthorized(req.version(), &format!("no token found for provider {provider_name}"));
        };

        //
        // heavy computation part: decode header + decode token
        //
        let entry = match self.decode_jwt_token(jwt_extract.token(), provider_name).await {
            Ok(a) => a,
            Err(err) => {
                info!(target: "jwt", "failed to decode token: {err}");
                return Self::unauthorized(req.version(), &format!("failed to decode JWT token: {err}"));
            },
        };

        // retrieve configuration for this provider...
        let Some(jwt_provider) = self.inner.config.providers.get(provider_name) else {
            info!(target: "jwt", "no provider found: {provider_name}");
            return Self::unauthorized(req.version(), &format!("no provider found: {provider_name}"));
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
                                    Err(e) => info!(target: "jwt", "failed to rebuild URI from parts: {e}"),
                                }
                            },
                            Err(e) => {
                                info!(target: "jwt", "failed to parse path and query after removing JWT parameter: {e}")
                            },
                        }
                    }
                },
                JwtExtract::Cookie(_) => (),
            }
        }

        // handle claim_to_headers

        if !jwt_provider.claim_to_headers.is_empty() {
            Self::write_claims_to_headers(jwt_provider, &entry.jwt.claims, req.headers_mut());
        }

        if jwt_provider.header_in_metadata.is_some() {
            req.extensions_mut().insert(entry.header.clone());
        }

        if jwt_provider.payload_in_metadata.is_some() {
            req.extensions_mut().insert(entry.jwt.claims.clone());
        }

        //debug!(target: "jwt", "{:#?}", entry.jwt);

        // handle clear_route_cache
        if jwt_provider.clear_route_cache {
            FilterDecision::Reroute
        } else {
            FilterDecision::Continue
        }
    }

    #[inline]
    fn unauthorized(ver: http::Version, msg: &str) -> FilterDecision {
        FilterDecision::DirectResponse(Box::new(
            SyntheticHttpResponse::unauthorized(EventFailure::RbacAccessDenied(msg.into()).into(), msg)
                .into_response(ver),
        ))
    }
}
