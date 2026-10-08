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
//

use super::{RequestCtx, RequestHandler};

#[cfg(feature = "access-log")]
use arion_format::context::UpstreamContext;

#[cfg(feature = "access-log")]
use crate::with_access_log;

use crate::{body::timeout_body::TimeoutBody, ArionRequestBody, ArionResponseBody, Error, PolyBody, Result};
use arion_configuration::config::network_filters::http_connection_manager::route::{
    AuthorityRedirect, RedirectAction, RouteMatchResult,
};
use http::{
    header::{HOST, LOCATION},
    uri::{Authority, Parts as UriParts, PathAndQuery, Scheme},
    HeaderValue, StatusCode, Uri,
};
use hyper::{Request, Response};

use std::str::FromStr;

fn strip_default_port(authority: Authority, scheme: &Scheme) -> Authority {
    match (authority.port_u16(), scheme.as_str()) {
        (Some(80), "http") | (Some(443), "https") => Authority::from_str(authority.host()).unwrap_or(authority),
        _ => authority,
    }
}

impl<'a> RequestHandler<Request<ArionRequestBody>, (&'a RouteMatchResult, &'a str, Scheme)> for &RedirectAction {
    async fn to_response(
        self,
        #[allow(unused_variables)] ctx: &RequestCtx,
        request: Request<ArionRequestBody>,
        #[allow(unused_variables)] (route_match_result, route_name, downstream_scheme): (
            &'a RouteMatchResult,
            &'a str,
            Scheme,
        ),
    ) -> Result<Response<ArionResponseBody>> {
        #[cfg(feature = "access-log")]
        ctx.tx.with_loggers(|loggers| {
            with_access_log!(loggers, UpstreamContext { authority: None, cluster_name: None, route_name });
        });

        let (parts, _) = request.into_parts();
        let mut rsp = Response::builder().status(StatusCode::from(self.response_code)).version(parts.version);

        let UriParts { scheme: uri_scheme, authority: uri_authority, path_and_query: orig_path_and_query, .. } =
            parts.uri.into_parts();
        // Origin-form URIs have no scheme or authority; fall back to the Host header and listener scheme.
        let (orig_scheme, orig_authority) =
            if self.authority_redirect.is_some() || self.scheme_rewrite_specifier.is_some() {
                let authority = uri_authority.or_else(|| {
                    parts
                        .headers
                        .get(HOST)
                        .and_then(|host| host.to_str().ok())
                        .and_then(|host| Authority::from_str(host).ok())
                });
                (uri_scheme.or(Some(downstream_scheme)), authority)
            } else {
                (uri_scheme, uri_authority)
            };
        // A scheme rewrite moves to the new scheme's default port.
        let orig_authority = match (&self.scheme_rewrite_specifier, orig_authority) {
            (Some(_), Some(authority)) if authority.port_u16().is_some() => Authority::from_str(authority.host()).ok(),
            (_, authority) => authority,
        };
        let orig_host = orig_authority.as_ref().map(Authority::host);
        let orig_port = orig_authority.as_ref().and_then(Authority::port_u16);
        let authority = match (self.authority_redirect.as_ref(), (orig_host, orig_port)) {
            //no redirect
            (None, _) => orig_authority,
            //full authority redirect OR host redirect with no port in the original uri
            (Some(AuthorityRedirect::AuthorityRedirect(a)), _)
            | (Some(AuthorityRedirect::HostRedirect(a)), (_, None)) => Some(a.clone()),
            (Some(AuthorityRedirect::HostRedirect(h)), (_, Some(port))) => {
                if (orig_scheme == Some(Scheme::HTTP) && port == 80)
                    || (orig_scheme == Some(Scheme::HTTPS) && port == 443)
                {
                    //strip port
                    Some(h.clone())
                } else {
                    let uri = format!("{h}:{port}");
                    Some(Authority::from_str(&uri)?)
                }
            },
            // port redirect with a host in the original uri
            (Some(AuthorityRedirect::PortRedirect(port)), (Some(h), _)) => {
                let uri = format!("{h}:{port}");
                Some(Authority::from_str(&uri)?)
            },
            // a port redirection with no known host
            (Some(AuthorityRedirect::PortRedirect(_)), (None, _)) => {
                return Err("tried to perform a port redirection with no host given".into());
            },
        };

        // strip query if specified
        let orig_path_and_query = if let Some(orig) = orig_path_and_query {
            if orig.query().is_some() && self.strip_query {
                Some(PathAndQuery::from_str(orig.path())?)
            } else {
                Some(orig)
            }
        } else {
            None
        };

        let scheme = self.scheme_rewrite_specifier.clone().or(orig_scheme);

        let authority = match (authority, scheme.as_ref()) {
            (Some(authority), Some(scheme)) => Some(strip_default_port(authority, scheme)),
            (authority, _) => authority,
        };

        // if this replacement yields a query, it will always overwrite the existing query
        let path_and_query = if let Some(prs) = self.path_rewrite_specifier.as_ref() {
            if let Some(replacement) =
                prs.apply(orig_path_and_query.as_ref(), route_match_result).map_err(Error::from)?
            {
                Some(replacement)
            } else {
                orig_path_and_query
            }
        } else {
            orig_path_and_query
        };

        let new_uri = Uri::from_parts({
            let mut parts = UriParts::default();
            parts.authority = authority;
            parts.scheme = scheme;
            parts.path_and_query = path_and_query;
            parts
        })
        .map_err(Error::from)?;
        let redirect_target = HeaderValue::from_str(&new_uri.to_string())?;
        rsp.headers_mut().and_then(|hm| hm.insert(LOCATION, redirect_target));
        rsp.body(TimeoutBody::new(None, PolyBody::default()).into()).map_err(Error::from)
    }
}
