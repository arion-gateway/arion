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
//
use super::{http_modifiers, upgrades as upgrade_utils, RequestHandler, TransactionContext};
use crate::event_error::{EventFailure, EventKind, TryInferFrom, UpstreamError};
use crate::{
    body::response_flags::ResponseFlags,
    clusters::{
        balancers::hash_policy::HashState,
        clusters_manager::{self, RoutingContext},
        decrement_requests, try_increment_requests,
    },
    listeners::{http_connection_manager::HttpConnectionManager, synthetic_http_response::SyntheticHttpResponse},
    Result,
};

#[cfg(feature = "access-log")]
use crate::with_access_log;

#[cfg(feature = "metrics")]
use crate::{clusters::CircuitBreakerDenial, get_shard_id, with_metric};
use crate::{instrument_block, instrument_function, OrionRequestBody, OrionResponseBody, RequestContext};
use http::{uri::Parts as UriParts, Uri};
use hyper::{Request, Response};
#[cfg(any(feature = "metrics", feature = "tracing"))]
use opentelemetry::KeyValue;
use orion_configuration::config::network_filters::http_connection_manager::{
    route::{RouteAction, RouteMatchResult},
    RetryPolicy,
};
use orion_error::Context;
#[cfg(feature = "metrics")]
use orion_metrics::metrics::clusters;
use scopeguard::defer;

#[cfg(feature = "access-log")]
use orion_format::context::{UpstreamContext, UpstreamRequestContext};
use orion_format::types::{ResponseFlags as FmtResponseFlags, ResponseFlagsLong, ResponseFlagsShort};

#[cfg(feature = "tracing")]
use {
    crate::tracing_attributes::set_attributes_from_request,
    crate::tracing_attributes::{UPSTREAM_ADDRESS, UPSTREAM_CLUSTER_NAME},
    opentelemetry::trace::Span,
    orion_tracing::http_tracer::{SpanKind, SpanName},
};

use smol_str::ToSmolStr;
use std::net::SocketAddr;
use tracing::debug;

pub struct RouteContext<'a> {
    pub retry_policy: Option<&'a RetryPolicy>,
    pub route_name: &'a str,
    pub remote_address: SocketAddr,
    pub route_match: &'a RouteMatchResult,
    pub websocket_enabled_by_default: bool,
}

impl<'a> RequestHandler<Request<OrionRequestBody>, (RouteContext<'a>, &HttpConnectionManager)> for &RouteAction {
    #[allow(clippy::too_many_lines)]
    #[allow(unused_variables)]
    async fn to_response(
        self,
        trans_handler: &TransactionContext,
        request: Request<OrionRequestBody>,
        (route_context, connection_manager): (RouteContext<'a>, &HttpConnectionManager),
    ) -> Result<Response<OrionResponseBody>> {
        instrument_function!(trans_handler.clock, |nanos| {
            crate::instrumentation::metrics::TOTAL_ROUTE_ACTION.observe(nanos as usize)
        });

        #[allow(unused_variables)]
        let RouteContext { route_name, retry_policy, remote_address, route_match, websocket_enabled_by_default } =
            route_context;

        let Some(cluster_id) = clusters_manager::resolve_cluster(&self.cluster_specifier, Some(request.headers()))
        else {
            debug!("Failed to resolve cluster from specifier {:?}", self.cluster_specifier);
            return Ok(SyntheticHttpResponse::internal_server_error(
                EventKind::Failure(EventFailure::ClusterNotFound),
                ResponseFlags(FmtResponseFlags::NO_CLUSTER_FOUND),
                "Failed to resolve cluster",
            )
            .into_response(request.version()));
        };

        let priority = self.priority;
        #[allow(unused_variables)]
        if let Err(denial) = try_increment_requests(cluster_id, priority) {
            debug!("Circuit breaker overflow for cluster {}", cluster_id);
            #[cfg(feature = "metrics")]
            {
                let shard_id = get_shard_id!();
                let attrs = &[KeyValue::new("cluster", cluster_id.to_string())];
                match denial {
                    CircuitBreakerDenial::MaxConnections => {
                        with_metric!(clusters::UPSTREAM_CX_OVERFLOW, add, 1, shard_id, attrs);
                    },
                    CircuitBreakerDenial::MaxRequests => {
                        with_metric!(clusters::UPSTREAM_RQ_OVERFLOW, add, 1, shard_id, attrs);
                    },
                    CircuitBreakerDenial::MaxRetries => {},
                }
            }
            return Ok(SyntheticHttpResponse::circuit_breaker_overflow(
                EventKind::Failure(EventFailure::UpstreamOverflow),
                ResponseFlags(FmtResponseFlags::UPSTREAM_OVERFLOW),
            )
            .into_circuit_breaker_response(request.version()));
        }

        defer! {
            decrement_requests(cluster_id, priority);
        }

        let routing_requirement = clusters_manager::get_cluster_routing_requirements(cluster_id);
        let hash_state = HashState::new(self.hash_policy.as_slice(), &request, remote_address);
        let routing_context = RoutingContext::try_from((&routing_requirement, &request, hash_state))?;

        let maybe_channel = instrument_block!(
            trans_handler.clock,
            |nanos| {
                crate::instrumentation::metrics::LOAD_BALANCING_SRV.observe(nanos as usize);
            },
            { clusters_manager::get_http_connection(cluster_id, routing_context) }
        );

        match maybe_channel {
            Ok(svc_channel) => {
                #[cfg(feature = "access-log")]
                with_access_log!(
                    &mut trans_handler.trans_ctx.lock().loggers,
                    UpstreamContext {
                        authority: Some(svc_channel.upstream_authority()),
                        cluster_name: Some(svc_channel.cluster_name()),
                        route_name,
                    }
                );

                let ver = request.version();

                let mut upstream_request: Request<OrionRequestBody> = {
                    let (mut parts, body) = request.into_parts();
                    let path_and_query_replacement = if let Some(rewrite) = &self.rewrite {
                        rewrite
                            .apply(parts.uri.path_and_query(), route_match)
                            .with_context_msg("invalid path after rewrite")?
                    } else {
                        None
                    };
                    if path_and_query_replacement.is_some() {
                        parts.uri = {
                            let UriParts { scheme, authority, .. } = parts.uri.into_parts();
                            let mut new_parts = UriParts::default();
                            new_parts.scheme = scheme;
                            new_parts.authority = authority;
                            new_parts.path_and_query = path_and_query_replacement;
                            Uri::from_parts(new_parts).with_context_msg("failed to replace request path_and_query")?
                        }
                    }
                    parts.version = svc_channel.http_version().into();
                    Request::from_parts(parts, body.map_into())
                };

                #[cfg(feature = "tracing")]
                let mut client_span = connection_manager.http_tracer.try_create_span(
                    trans_handler.trace_ctx.as_ref(),
                    &connection_manager.get_tracing_key(),
                    SpanKind::Client,
                    SpanName::Str::<()>(svc_channel.upstream_authority().as_str()),
                );

                #[cfg(feature = "tracing")]
                if let Some(ref mut client_span) = client_span {
                    // set default attributes to span, using upstream request information...
                    set_attributes_from_request(client_span, &upstream_request);

                    // set additional attributes for client span...
                    client_span.set_attributes([
                        KeyValue::new(UPSTREAM_CLUSTER_NAME, svc_channel.cluster_name()),
                        KeyValue::new(UPSTREAM_ADDRESS, svc_channel.upstream_authority().to_string()),
                    ]);
                }

                // ... store the span in the span_state
                #[cfg(feature = "tracing")]
                if let Some(ref span_state) = trans_handler.span_state {
                    *span_state.client_span.lock() = client_span;
                }

                #[cfg(feature = "access-log")]
                with_access_log!(
                    &mut trans_handler.trans_ctx.lock().loggers,
                    UpstreamRequestContext(&upstream_request)
                );

                let websocket_enabled = if let Some(upgrade_config) = self.upgrade_config {
                    upgrade_config.is_websocket_enabled(websocket_enabled_by_default)
                } else {
                    websocket_enabled_by_default
                };
                let should_upgrade_websocket = if websocket_enabled {
                    match upgrade_utils::is_valid_websocket_upgrade_request(upstream_request.headers()) {
                        Ok(maybe_upgrade) => maybe_upgrade,
                        Err(upgrade_error) => {
                            debug!("Failed to upgrade to websockets {upgrade_error}");
                            return Ok(SyntheticHttpResponse::bad_request(EventFailure::UpgradeFailed.into())
                                .into_response(ver));
                        },
                    }
                } else {
                    false
                };

                if should_upgrade_websocket {
                    return upgrade_utils::handle_websocket_upgrade(
                        trans_handler,
                        upstream_request,
                        &svc_channel,
                        #[cfg(feature = "metrics")]
                        connection_manager.listener_name,
                    )
                    .await;
                }

                if let Some(direct_response) = http_modifiers::apply_preflight_functions(&mut upstream_request) {
                    return Ok(direct_response);
                }

                // send the request to the upstream service channel and wait for the response...
                let resp = svc_channel
                    .to_response(
                        trans_handler,
                        upstream_request,
                        RequestContext { route_timeout: self.timeout, retry_policy, priority },
                    )
                    .await;
                match resp {
                    Err(err) => {
                        let err = err.into_inner();
                        let event_error = UpstreamError::try_infer_from(&err);
                        let flags = event_error.clone().map(ResponseFlags::from).unwrap_or_default();
                        let event_kind = event_error.map_or(EventFailure::ViaUpstream.into(), EventKind::Upstream);
                        debug!(
                            "HttpConnectionManager Error processing response {:?}: {}({})",
                            err,
                            ResponseFlagsLong(&flags.0).to_smolstr(),
                            ResponseFlagsShort(&flags.0).to_smolstr()
                        );
                        Ok(SyntheticHttpResponse::bad_gateway(event_kind, flags).into_response(ver))
                    },
                    resp => resp,
                }
            },
            // http connection not available from cluster...
            Err(err) => {
                let err = err.into_inner();
                let event_error = UpstreamError::try_infer_from(&err);
                let flags = event_error.clone().map(ResponseFlags::from).unwrap_or_default();
                let event_kind = event_error.map_or(EventFailure::ViaUpstream.into(), EventKind::Upstream);
                debug!(
                    "Failed to get an HTTP connection: {:?}: {}({})",
                    err,
                    ResponseFlagsLong(&flags.0).to_smolstr(),
                    ResponseFlagsShort(&flags.0).to_smolstr()
                );
                Ok(SyntheticHttpResponse::internal_server_error(
                    event_kind,
                    flags,
                    "Failed to connect to upstream cluster",
                )
                .into_response(request.version()))
            },
        }
    }
}
