use std::{collections::HashMap, sync::Arc};

use crate::{
    body::response_flags::ResponseFlags,
    event_error::EventFailure,
    listeners::{
        http_connection_manager::{
            cors::Cors,
            ext_proc::ExternalProcessor,
            jwt_authn::{JwtAuthentication, JwtAuthenticationBuilder},
            mcp_gateway::mcp::McpGateway,
            user_rate_limiter::UserRateLimiter,
        },
        rate_limiter::LocalRateLimit,
        rbac::HttpRbac,
        synthetic_http_response::SyntheticHttpResponse,
    },
    OrionRequestBody, OrionResponseBody,
};
use http::{Request, Response, StatusCode};
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
    DirectResponse(Response<OrionResponseBody>),
    AsyncRequest(Response<OrionResponseBody>, Option<Request<OrionRequestBody>>),
}

impl FilterDecision {
    #[inline]
    pub fn internal_server_error(msg: &str, ver: http::Version) -> FilterDecision {
        FilterDecision::DirectResponse(
            SyntheticHttpResponse::internal_server_error(
                EventFailure::DirectResponse.into(),
                ResponseFlags::default(),
                msg,
            )
            .into_response(ver),
        )
    }

    #[inline]
    pub fn bad_request(ver: http::Version) -> FilterDecision {
        FilterDecision::DirectResponse(
            SyntheticHttpResponse::bad_request(EventFailure::DirectResponse.into()).into_response(ver),
        )
    }

    #[inline]
    #[allow(dead_code)]
    pub fn not_found(ver: http::Version) -> FilterDecision {
        FilterDecision::DirectResponse(
            SyntheticHttpResponse::not_found(EventFailure::DirectResponse.into(), ResponseFlags::default())
                .into_response(ver),
        )
    }

    #[inline]
    pub fn method_not_allowed(ver: http::Version) -> FilterDecision {
        FilterDecision::DirectResponse(
            SyntheticHttpResponse::custom_error(
                StatusCode::METHOD_NOT_ALLOWED,
                None,
                EventFailure::RouteNotFound.into(),
                ResponseFlags(FmtResponseFlags::NO_ROUTE_FOUND),
            )
            .into_response(ver),
        )
    }

    #[inline]
    pub fn no_route_found(ver: http::Version) -> FilterDecision {
        FilterDecision::DirectResponse(
            SyntheticHttpResponse::not_found(
                EventFailure::RouteNotFound.into(),
                ResponseFlags(FmtResponseFlags::NO_ROUTE_FOUND),
            )
            .into_response(ver),
        )
    }

    #[inline]
    pub fn rate_limited(ver: http::Version) -> FilterDecision {
        FilterDecision::DirectResponse(
            SyntheticHttpResponse::custom_error(
                http::StatusCode::TOO_MANY_REQUESTS,
                None,
                EventFailure::RateLimited.into(),
                ResponseFlags(FmtResponseFlags::RATE_LIMITED),
            )
            .into_response(ver),
        )
    }

    #[inline]
    #[allow(dead_code)]
    pub fn unauthorized(msg: &str, ver: http::Version) -> FilterDecision {
        FilterDecision::DirectResponse(
            SyntheticHttpResponse::unauthorized(EventFailure::ExtProcError.into(), msg).into_response(ver),
        )
    }
}

#[derive(Debug, Clone)]
pub struct HttpFilter {
    pub name: SmolStr,
    pub disabled: bool,
    pub filter: Option<HttpFilterValue>,
    pub base_config: Option<HttpFilterConfig>,
}

#[derive(Debug, Clone)]
pub enum HttpFilterValue {
    RateLimit(LocalRateLimit),
    Rbac(HttpRbac),
    ExternalProcessor(ExternalProcessor),
    JwtAuthentication(JwtAuthentication),
    Cors(Cors),
    McpGateway(McpGateway),
    UserRateLimit(UserRateLimiter),
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
            HttpFilterValue::McpGateway(conf) => HttpFilterValue::McpGateway(conf.new_from()),
            HttpFilterValue::Cors(conf) => HttpFilterValue::Cors(conf.clone()),
            HttpFilterValue::UserRateLimit(conf) => HttpFilterValue::UserRateLimit(conf.clone()),
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
            HttpFilterType::RateLimit(conf) => HttpFilterValue::RateLimit(conf.into()),
            HttpFilterType::Rbac(conf) => HttpFilterValue::Rbac(HttpRbac::new(&conf)),
            HttpFilterType::ExternalProcessor(conf) => HttpFilterValue::ExternalProcessor(conf.into()),
            HttpFilterType::JwtAuthentication(conf) => {
                let builder = JwtAuthenticationBuilder::new(conf);
                HttpFilterValue::JwtAuthentication(builder.build())
            },
            HttpFilterType::Cors(conf) => HttpFilterValue::Cors(conf.into()),
            HttpFilterType::CorsPolicy(conf) => HttpFilterValue::Cors(conf.into()),
            HttpFilterType::McpGateway(mcp) => HttpFilterValue::McpGateway(mcp.try_into()?),
            HttpFilterType::UserRateLimit(user_rate_limit) => {
                HttpFilterValue::UserRateLimit(user_rate_limit.try_into()?)
            },
        };
        Ok(Self { name, disabled, filter: Some(filter), base_config: hcm_config })
    }
}

impl HttpFilterValue {
    pub async fn apply_request(&mut self, request: &mut Request<OrionRequestBody>) -> FilterDecision {
        match self {
            HttpFilterValue::Rbac(rbac) => apply_authorization_rules(rbac, request),
            HttpFilterValue::RateLimit(rl) => rl.run(request),
            HttpFilterValue::ExternalProcessor(ext_proc) => ext_proc.apply_request(request).await,
            HttpFilterValue::JwtAuthentication(jwt) => jwt.apply_request(request).await,
            HttpFilterValue::Cors(cors) => cors.apply_request(request),
            HttpFilterValue::McpGateway(mcp) => mcp.apply_request(request).await,
            HttpFilterValue::UserRateLimit(user_rate_limiter) => user_rate_limiter.apply_request(request).await,
        }
    }
    pub async fn apply_response(&mut self, response: &mut Response<OrionResponseBody>) -> FilterDecision {
        match self {
            // RBAC and RateLimit do not apply on the response path
            HttpFilterValue::Rbac(_) | HttpFilterValue::RateLimit(_) => FilterDecision::Continue,
            HttpFilterValue::ExternalProcessor(ext_proc) => ext_proc.apply_response(response).await,
            HttpFilterValue::JwtAuthentication(_) => FilterDecision::Continue,
            HttpFilterValue::McpGateway(mcp) => mcp.apply_response(response).await,
            HttpFilterValue::Cors(cors) => cors.apply_response(response),
            HttpFilterValue::UserRateLimit(_) => FilterDecision::Continue,
        }
    }
    pub(crate) fn from_filter_override(value: &FilterOverride, base_config: Option<&HttpFilterConfig>) -> Option<Self> {
        match &value.filter_settings {
            Some(filter_settings) => match filter_settings {
                FilterConfigOverride::LocalRateLimit(rl) => Some(HttpFilterValue::RateLimit(rl.clone().into())),
                FilterConfigOverride::Rbac(Some(rbac)) => Some(HttpFilterValue::Rbac(HttpRbac::new(&rbac))),
                FilterConfigOverride::Rbac(None) => None,
                FilterConfigOverride::ExternalProcessor(ext_proc_per_route) => {
                    if let Some(HttpFilterConfig { filter: HttpFilterType::ExternalProcessor(base_config), .. }) =
                        base_config
                    {
                        let filter_value = HttpFilterValue::ExternalProcessor(
                            (base_config.clone(), Some(ext_proc_per_route.clone()), None).into(),
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
        FilterDecision::DirectResponse(
            SyntheticHttpResponse::forbidden(
                EventFailure::RbacAccessDenied(enforced_policy.unwrap_or(SmolStr::new_static("unknown"))).into(),
                "RBAC: access denied",
            )
            .into_response(req.version()),
        )
    }
}

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
                        filter: HttpFilterValue::from_filter_override(override_config, hcm_filter.base_config.as_ref()),
                        base_config: hcm_filter.base_config.clone(),
                    }),
                    None => Arc::clone(hcm_filter),
                };
                per_route_filters.entry(route.route_match.clone()).or_default().push(effective_filter);
            }
        }
    }
    per_route_filters
}
