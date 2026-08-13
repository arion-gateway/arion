#[cfg(feature = "wasm")]
use crate::listeners::http_connection_manager::wasm::WasmFilter;
use std::{collections::HashMap, sync::Arc};

use crate::{
    body::response_flags::ResponseFlags,
    event_error::EventFailure,
    listeners::{
        http_connection_manager::{
            cedar_policy::CedarHttpFilter,
            cors::Cors,
            ext_proc::ExternalProcessor,
            jwt_authn::{JwtAuthentication, JwtAuthenticationBuilder},
            mcp_gateway::mcp::McpGateway,
            user_rate_limiter::UserRateLimiter,
            RequestCtx,
        },
        rate_limiter::local_rate_limiter::LocalRateLimit,
        rbac::HttpRbac,
        synthetic_http_response::SyntheticHttpResponse,
    },
    OrionRequestBody, OrionResponseBody,
};
use http::{HeaderMap, HeaderValue, Request, Response, StatusCode};
use orion_format::types::ResponseFlags as FmtResponseFlags;
use smol_str::SmolStr;
use tracing::debug;

use orion_configuration::config::network_filters::http_connection_manager::{
    http_filters::{FilterConfigOverride, FilterOverride, HttpFilter as HttpFilterConfig, HttpFilterType},
    route::RouteMatch,
    RouteConfiguration,
};

use crate::Result;

#[derive(Debug, Default)]
#[allow(dead_code)]
pub enum FilterDecision {
    #[default]
    Continue,
    Reroute,
    DirectResponse(Box<Response<OrionResponseBody>>),
}

impl FilterDecision {
    // extract http headers from filter decision. Note that if the variant is Continue or Reroute,
    // headers must be extracted from the original request or response.
    #[inline]
    #[allow(unused)]
    pub fn headers(&self) -> Option<&HeaderMap<HeaderValue>> {
        match self {
            FilterDecision::Continue | FilterDecision::Reroute => None,
            FilterDecision::DirectResponse(response) => Some(response.headers()),
        }
    }

    #[inline]
    pub fn internal_server_error(msg: &str, ver: http::Version) -> Self {
        FilterDecision::DirectResponse(Box::new(
            SyntheticHttpResponse::internal_server_error(EventFailure::DirectResponse.into(), ResponseFlags::default())
                .with_body(msg.to_string())
                .into_response(ver),
        ))
    }

    #[inline]
    pub fn bad_request(msg: &str, ver: http::Version) -> Self {
        FilterDecision::DirectResponse(Box::new(
            SyntheticHttpResponse::bad_request(EventFailure::DirectResponse.into())
                .with_body(msg.to_string())
                .into_response(ver),
        ))
    }

    #[inline]
    #[allow(dead_code)]
    pub fn not_found(msg: &str, ver: http::Version) -> Self {
        FilterDecision::DirectResponse(Box::new(
            SyntheticHttpResponse::not_found(EventFailure::DirectResponse.into(), ResponseFlags::default())
                .with_body(msg.to_string())
                .into_response(ver),
        ))
    }

    #[inline]
    pub fn method_not_allowed(msg: &str, ver: http::Version) -> Self {
        FilterDecision::DirectResponse(Box::new(
            SyntheticHttpResponse::custom_error(
                StatusCode::METHOD_NOT_ALLOWED,
                None,
                EventFailure::RouteNotFound.into(),
                ResponseFlags(FmtResponseFlags::NO_ROUTE_FOUND),
            )
            .with_body(msg.to_string())
            .into_response(ver),
        ))
    }

    #[inline]
    pub fn no_route_found(msg: &str, ver: http::Version) -> Self {
        FilterDecision::DirectResponse(Box::new(
            SyntheticHttpResponse::not_found(
                EventFailure::RouteNotFound.into(),
                ResponseFlags(FmtResponseFlags::NO_ROUTE_FOUND),
            )
            .with_body(msg.to_string())
            .into_response(ver),
        ))
    }

    #[inline]
    pub fn rate_limited(msg: &str, status: Option<StatusCode>, ver: http::Version) -> Self {
        FilterDecision::DirectResponse(Box::new(
            SyntheticHttpResponse::custom_error(
                status.unwrap_or(http::StatusCode::TOO_MANY_REQUESTS),
                None,
                EventFailure::RateLimited.into(),
                ResponseFlags(FmtResponseFlags::RATE_LIMITED),
            )
            .with_body(msg.to_string())
            .into_response(ver),
        ))
    }

    #[inline]
    #[allow(dead_code)]
    pub fn unauthorized(msg: &str, ver: http::Version) -> Self {
        FilterDecision::DirectResponse(Box::new(
            SyntheticHttpResponse::unauthorized(EventFailure::ExtProcError.into())
                .with_body(msg.to_string())
                .into_response(ver),
        ))
    }
}

#[derive(Debug, Clone)]
pub struct HttpFilter {
    pub name: SmolStr,
    pub disabled: bool,
    pub filter: Option<HttpFilterValue>,
    pub filter_config: Option<Box<HttpFilterConfig>>,
}

#[derive(Debug, Clone)]
pub enum HttpFilterValue {
    #[cfg(feature = "wasm")]
    Wasm(WasmFilter),
    RateLimit(LocalRateLimit),
    Rbac(HttpRbac),
    ExternalProcessor(ExternalProcessor),
    JwtAuthentication(JwtAuthentication),
    Cors(Cors),
    McpGateway(Box<McpGateway>),
    UserRateLimit(UserRateLimiter),
    CedarPolicy(CedarHttpFilter),
}

pub trait FilterFactory {
    fn new_from(&self) -> Self;
}

impl FilterFactory for HttpFilterValue {
    fn new_from(&self) -> Self {
        match self {
            HttpFilterValue::RateLimit(conf) => HttpFilterValue::RateLimit(conf.clone()),
            HttpFilterValue::Rbac(conf) => HttpFilterValue::Rbac(conf.clone()),
            HttpFilterValue::ExternalProcessor(conf) => HttpFilterValue::ExternalProcessor(conf.clone()),
            HttpFilterValue::JwtAuthentication(conf) => HttpFilterValue::JwtAuthentication(conf.new_from()),
            HttpFilterValue::McpGateway(conf) => HttpFilterValue::McpGateway(Box::new(conf.new_from())),
            HttpFilterValue::Cors(conf) => HttpFilterValue::Cors(conf.clone()),
            HttpFilterValue::UserRateLimit(conf) => HttpFilterValue::UserRateLimit(conf.clone()),
            HttpFilterValue::CedarPolicy(conf) => HttpFilterValue::CedarPolicy(conf.clone()),
            #[cfg(feature = "wasm")]
            HttpFilterValue::Wasm(conf) => HttpFilterValue::Wasm(conf.new_from()),
        }
    }
}

impl TryFrom<HttpFilterConfig> for HttpFilter {
    type Error = crate::Error;

    fn try_from(value: HttpFilterConfig) -> Result<Self> {
        let hcm_config = match &value.filter {
            HttpFilterType::ExternalProcessor(_) => Some(value.clone()),
            _ => None,
        };

        let HttpFilterConfig { name, disabled, filter } = value;

        let filter = match filter {
            #[cfg(feature = "wasm")]
            HttpFilterType::Wasm(conf) => HttpFilterValue::Wasm(WasmFilter::try_new(conf)?),
            #[cfg(not(feature = "wasm"))]
            HttpFilterType::Wasm(_) => {
                return Err("HTTP Wasm filter requires building Orion with the `wasm` Cargo feature \
(e.g. `cargo build -p orion-proxy --features wasm`)"
                    .into());
            },
            HttpFilterType::RateLimit(conf) => HttpFilterValue::RateLimit(conf.into()),
            HttpFilterType::Rbac(conf) => HttpFilterValue::Rbac(HttpRbac::new(&conf)),
            HttpFilterType::ExternalProcessor(conf) => HttpFilterValue::ExternalProcessor(conf.into()),
            HttpFilterType::JwtAuthentication(conf) => {
                let builder = JwtAuthenticationBuilder::new(conf);
                HttpFilterValue::JwtAuthentication(builder.build())
            },
            HttpFilterType::Cors(conf) | HttpFilterType::CorsPolicy(conf) => HttpFilterValue::Cors(conf.into()),
            HttpFilterType::McpGateway(mcp) => HttpFilterValue::McpGateway(Box::new(mcp.try_into()?)),
            HttpFilterType::UserRateLimit(user_rate_limit) => {
                HttpFilterValue::UserRateLimit(user_rate_limit.try_into()?)
            },
            HttpFilterType::CedarPolicy(conf) => HttpFilterValue::CedarPolicy(CedarHttpFilter::try_from_config(conf)?),
        };
        Ok(Self { name, disabled, filter: Some(filter), filter_config: hcm_config.map(Box::new) })
    }
}

impl HttpFilterValue {
    pub async fn apply_request(&mut self, request: &mut Request<OrionRequestBody>, ctx: &RequestCtx) -> FilterDecision {
        match self {
            HttpFilterValue::Rbac(rbac) => apply_authorization_rules(rbac, request),
            HttpFilterValue::RateLimit(rl) => rl.run(request),
            HttpFilterValue::ExternalProcessor(ext_proc) => ext_proc.apply_request(request, ctx).await,
            HttpFilterValue::JwtAuthentication(jwt) => jwt.apply_request(request).await,
            HttpFilterValue::Cors(cors) => cors.apply_request(request),
            HttpFilterValue::McpGateway(mcp) => mcp.apply_request(request, ctx).await,
            HttpFilterValue::UserRateLimit(user_rate_limiter) => user_rate_limiter.apply_request(request),
            HttpFilterValue::CedarPolicy(cedar) => cedar.apply_request(request),
            #[cfg(feature = "wasm")]
            HttpFilterValue::Wasm(wasm) => wasm.apply_request(request, ctx).await,
        }
    }
    pub async fn apply_response(
        &mut self,
        response: &mut Response<OrionResponseBody>,
        ctx: &RequestCtx,
    ) -> FilterDecision {
        match self {
            // RBAC and RateLimit do not apply on the response path
            HttpFilterValue::ExternalProcessor(ext_proc) => ext_proc.apply_response(response, ctx).await,
            HttpFilterValue::McpGateway(mcp) => mcp.apply_response(response).await,
            HttpFilterValue::Cors(cors) => cors.apply_response(response),
            #[cfg(feature = "wasm")]
            HttpFilterValue::Wasm(wasm) => wasm.apply_response(response, ctx).await,
            HttpFilterValue::Rbac(_)
            | HttpFilterValue::RateLimit(_)
            | HttpFilterValue::UserRateLimit(_)
            | HttpFilterValue::JwtAuthentication(_)
            | HttpFilterValue::CedarPolicy(_) => FilterDecision::Continue,
        }
    }
    pub(crate) fn from_filter_override(
        value: &FilterOverride,
        filter_config: Option<&HttpFilterConfig>,
    ) -> Option<Self> {
        match &value.filter_settings {
            Some(filter_settings) => match filter_settings {
                FilterConfigOverride::LocalRateLimit(rl) => Some(HttpFilterValue::RateLimit(rl.clone().into())),
                FilterConfigOverride::Rbac(Some(rbac)) => Some(HttpFilterValue::Rbac(HttpRbac::new(rbac))),
                FilterConfigOverride::Rbac(None) => None,
                FilterConfigOverride::ExternalProcessor(ext_proc_per_route) => {
                    if let Some(HttpFilterConfig { filter: HttpFilterType::ExternalProcessor(filter_config), .. }) =
                        filter_config
                    {
                        let filter_value = HttpFilterValue::ExternalProcessor(
                            (filter_config.clone(), Some(ext_proc_per_route.clone()), None).into(),
                        );
                        Some(filter_value)
                    } else {
                        None
                    }
                },
            },
            None => None,
        }
    }
}

fn apply_authorization_rules<B>(rbac: &HttpRbac, req: &Request<B>) -> FilterDecision {
    debug!("Applying authorization rules {rbac:?} {:?}", &req.headers());
    let (permitted, enforced_policy) = rbac.inner.is_permitted(req);
    if permitted {
        FilterDecision::Continue
    } else {
        FilterDecision::DirectResponse(Box::new(
            SyntheticHttpResponse::forbidden(
                EventFailure::RbacAccessDenied(enforced_policy.unwrap_or(SmolStr::new_static("unknown"))).into(),
            )
            .with_body("RBAC: access denied")
            .into_response(req.version()),
        ))
    }
}

// `RouteMatch` contains `Regex` which has interior mutability, but its `Hash` and `PartialEq`
// implementations use only `as_str()` (the immutable pattern string), so this is safe.
#[allow(clippy::mutable_key_type)]
pub(crate) fn per_route_http_filters(
    route_config: &RouteConfiguration,
    hcm_filters: &[Arc<HttpFilter>],
) -> HashMap<RouteMatch, Vec<Arc<HttpFilter>>> {
    let mut per_route_filters: HashMap<RouteMatch, Vec<Arc<HttpFilter>>> = HashMap::new();
    for vh in &route_config.virtual_hosts {
        for route in &vh.routes {
            for hcm_filter in hcm_filters {
                let effective_filter = match route.typed_per_filter_config.get(&hcm_filter.name) {
                    Some(override_config) => Arc::new(HttpFilter {
                        name: hcm_filter.name.clone(),
                        disabled: override_config.disabled,
                        filter: HttpFilterValue::from_filter_override(
                            override_config,
                            hcm_filter.filter_config.as_deref(),
                        ),
                        filter_config: hcm_filter.filter_config.clone(),
                    }),
                    None => Arc::clone(hcm_filter),
                };
                per_route_filters.entry(route.route_match.clone()).or_default().push(effective_filter);
            }
        }
    }
    per_route_filters
}
