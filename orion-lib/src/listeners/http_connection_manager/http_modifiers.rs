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

use super::upgrade_utils;
use crate::{
    event_error::EventFailure, extensions_context::MetadataContext,
    listeners::synthetic_http_response::SyntheticHttpResponse, OrionResponseBody,
};
use http::{header, HeaderMap, HeaderName, HeaderValue, Method, Request, Response};
use orion_configuration::config::{
    cluster::http_protocol_options::Codec,
    network_filters::http_connection_manager::{
        header_modifier::{HeaderAppendAction, HeaderValueOption},
        HeaderModifiersAdd, HeaderModifiersRemove, Route, RouteConfiguration, VirtualHost, XffSettings,
    },
};
use orion_format::context::{DownstreamContext, DownstreamResponseContext, SocketAddrContext};
use orion_http_header::{X_ENVOY_EXTERNAL_ADDRESS, X_ENVOY_INTERNAL, X_FORWARDED_FOR};
use std::net::{IpAddr, SocketAddr};
use tracing::warn;

const HOP_BY_HOP_HEADERS: &[HeaderName] = &[
    header::CONNECTION,
    header::PROXY_AUTHENTICATE,
    header::PROXY_AUTHORIZATION,
    // NOTE: (nb) TE and TRAILER headers are intentionally left out as they are be needed for
    // proper handling of certain requests (e.g., propagating chunked transfer encoding + trailers toward the upstream).
    // header::TE,
    // header::TRAILER,
    header::TRANSFER_ENCODING,
    header::UPGRADE,
];
pub fn apply_prerouting_functions<T>(request: &mut Request<T>, downstream_addr: SocketAddr, xff_settings: XffSettings) {
    process_xff_headers(request, downstream_addr, xff_settings);
}

pub fn apply_preflight_functions<T>(request: &mut Request<T>) -> Option<Response<OrionResponseBody>> {
    if let Some(direct_response) = filter_disallowed_requests(request) {
        return Some(direct_response);
    }
    strip_hop_headers(request.headers_mut());
    None
}

fn filter_disallowed_requests<T>(request: &Request<T>) -> Option<Response<OrionResponseBody>> {
    if request.method() == Method::CONNECT {
        return Some(
            SyntheticHttpResponse::forbidden(EventFailure::UpgradeFailed.into(), "CONNECT not permitted")
                .into_response(request.version()),
        );
    }
    if let Some(connection_header) = request.headers().get(header::CONNECTION) {
        if upgrade_utils::is_upgrade_connection(connection_header.to_str().ok()?) {
            return Some(
                SyntheticHttpResponse::forbidden(EventFailure::UpgradeFailed.into(), "upgrade not permitted")
                    .into_response(request.version()),
            );
        }
    }
    None
}

#[inline]
fn strip_hop_headers(headers: &mut HeaderMap) {
    for header in HOP_BY_HOP_HEADERS {
        headers.remove(header);
    }
}

pub fn strip_trailers_headers(http_version: Codec, headers: &mut HeaderMap) {
    match http_version {
        Codec::Http1 => {
            headers.remove(header::TE);
            headers.remove(header::TRAILER);
        },
        // TE header is allowed in HTTP2 only if its value is "trailers"
        Codec::Http2 => {
            headers.remove(header::TRAILER);
            if let Some(hdr_value) = headers.get(header::TE) {
                if hdr_value != "trailers" {
                    headers.remove(header::TE);
                }
            }
        },
    }
}

fn process_xff_headers<T>(request: &mut Request<T>, downstream_addr: SocketAddr, xff_settings: XffSettings) {
    let headers = request.headers_mut();
    let downstream_is_internal = is_internal_ip(downstream_addr.ip());
    let downstream_is_external = !downstream_is_internal;

    if downstream_is_external {
        headers.remove(&X_ENVOY_EXTERNAL_ADDRESS);
        headers.remove(&X_ENVOY_INTERNAL);
    }

    let transparent_mode = !xff_settings.use_remote_address && xff_settings.skip_xff_append;
    if transparent_mode {
        return;
    }

    let existing_xff = headers.get(X_FORWARDED_FOR).and_then(|value| value.to_str().ok());
    let (trusted_client_address, xff_contains_single_ip) =
        determine_trusted_client_address(existing_xff, downstream_addr, xff_settings);
    let xff_contains_single_internal_ip = xff_contains_single_ip && is_internal_ip(trusted_client_address);
    let xff_contains_single_external_ip = xff_contains_single_ip && !is_internal_ip(trusted_client_address);
    let has_incoming_xff = existing_xff.is_some();

    let should_update_xff = xff_settings.use_remote_address && !xff_settings.skip_xff_append;
    let should_set_envoy_external = xff_settings.use_remote_address
        && !headers.contains_key(X_ENVOY_EXTERNAL_ADDRESS)
        && !is_internal_ip(trusted_client_address);
    let should_set_envoy_internal = (xff_settings.use_remote_address && downstream_is_internal && !has_incoming_xff)
        || xff_contains_single_internal_ip;
    let should_mark_envoy_internal_false =
        (xff_settings.use_remote_address && downstream_is_external && !has_incoming_xff)
            || xff_contains_single_external_ip;

    if should_update_xff {
        if let Ok(updated_xff) = HeaderValue::from_str(&append_hop_to_xff(existing_xff, downstream_addr.ip())) {
            headers.insert(X_FORWARDED_FOR, updated_xff);
        }
    }
    if should_set_envoy_external {
        if let Ok(envoy_external_addr) = HeaderValue::from_str(&trusted_client_address.to_string()) {
            headers.insert(X_ENVOY_EXTERNAL_ADDRESS, envoy_external_addr);
        }
    }
    if should_set_envoy_internal {
        headers.insert(X_ENVOY_INTERNAL, HeaderValue::from_static("true"));
    } else if should_mark_envoy_internal_false {
        headers.insert(X_ENVOY_INTERNAL, HeaderValue::from_static("false"));
    }
}

fn is_internal_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ipv4) => ipv4.is_private() || ipv4.is_loopback(),
        IpAddr::V6(ipv6) => {
            ipv6.segments()[0] & 0xfe00 == 0xfc00
                || ipv6.is_unspecified()
                || ipv6.is_loopback()
                || ipv6.is_unique_local()
                || ipv6.is_unicast_link_local()
        },
    }
}

fn append_hop_to_xff(existing_xff: Option<&str>, downstream_ip: IpAddr) -> String {
    let ip_str = downstream_ip.to_string();
    match existing_xff {
        Some(existing_str) => {
            if existing_str.is_empty() {
                ip_str
            } else {
                format!("{existing_str}, {ip_str}")
            }
        },
        _ => ip_str,
    }
}

fn determine_trusted_client_address(
    existing_xff: Option<&str>,
    downstream_addr: SocketAddr,
    xff_settings: XffSettings,
) -> (IpAddr, bool) {
    let mut trusted_client_address = downstream_addr.ip();
    let mut xff_contains_single_ip = false;
    let xff_ips = existing_xff
        .map_or_else(Vec::new, |value| value.split(',').filter_map(|ip_str| ip_str.trim().parse().ok()).collect());
    let num_xff_ips = xff_ips.len();
    if !xff_settings.use_remote_address && !xff_ips.is_empty() {
        if xff_settings.xff_num_trusted_hops > 0 {
            let required_index_from_right = xff_settings.xff_num_trusted_hops as usize + 1;
            if num_xff_ips >= required_index_from_right {
                trusted_client_address = xff_ips[num_xff_ips - required_index_from_right];
            }
        } else {
            trusted_client_address = xff_ips[num_xff_ips - 1];
            xff_contains_single_ip = num_xff_ips == 1;
        }
    } else if xff_settings.use_remote_address && xff_settings.xff_num_trusted_hops > 0 && !xff_ips.is_empty() {
        let required_index_from_right = xff_settings.xff_num_trusted_hops as usize;
        if num_xff_ips >= required_index_from_right {
            trusted_client_address = xff_ips[num_xff_ips - required_index_from_right];
        }
    }
    (trusted_client_address, xff_contains_single_ip)
}

pub trait HeaderMapModifier<M> {
    fn apply_mutation(&mut self, modifier: M);
}

impl<B> HeaderMapModifier<&HeaderModifiersRemove> for Request<B> {
    fn apply_mutation(&mut self, modifier: &HeaderModifiersRemove) {
        for name in &modifier.0 {
            self.headers_mut().remove(name);
        }
    }
}

impl<B> HeaderMapModifier<&HeaderModifiersRemove> for Response<B> {
    fn apply_mutation(&mut self, modifier: &HeaderModifiersRemove) {
        for name in &modifier.0 {
            self.headers_mut().remove(name);
        }
    }
}

impl<B> HeaderMapModifier<&HeaderModifiersAdd> for Request<B> {
    fn apply_mutation(&mut self, modifier: &HeaderModifiersAdd) {
        for modifier in &modifier.0 {
            modifier.apply_to_request(self);
        }
    }
}

impl<B> HeaderMapModifier<&HeaderModifiersAdd> for Response<B> {
    fn apply_mutation(&mut self, modifier: &HeaderModifiersAdd) {
        for modifier in &modifier.0 {
            modifier.apply_to_response(self);
        }
    }
}

impl<'m1, 'm2, M1, M2, B> HeaderMapModifier<(&'m1 M1, &'m2 M2)> for Request<B>
where
    Request<B>: HeaderMapModifier<&'m1 M1>,
    Request<B>: HeaderMapModifier<&'m2 M2>,
{
    fn apply_mutation(&mut self, (m1, m2): (&'m1 M1, &'m2 M2)) {
        self.apply_mutation(m1);
        self.apply_mutation(m2);
    }
}

impl<'m1, 'm2, M1, M2, B> HeaderMapModifier<(&'m1 M1, &'m2 M2)> for Response<B>
where
    Response<B>: HeaderMapModifier<&'m1 M1>,
    Response<B>: HeaderMapModifier<&'m2 M2>,
{
    fn apply_mutation(&mut self, (m1, m2): (&'m1 M1, &'m2 M2)) {
        self.apply_mutation(m1);
        self.apply_mutation(m2);
    }
}

pub trait ModifierType {}
impl<B> ModifierType for Request<B> {}
impl<B> ModifierType for Response<B> {}

pub trait ModifiersExtractor<T: ModifierType> {
    fn extract(&self) -> (&HeaderModifiersRemove, &HeaderModifiersAdd);
}

impl<B> ModifiersExtractor<Request<B>> for Route {
    fn extract(&self) -> (&HeaderModifiersRemove, &HeaderModifiersAdd) {
        (&self.request_headers_to_remove, &self.request_headers_to_add)
    }
}

impl<B> ModifiersExtractor<Response<B>> for Route {
    fn extract(&self) -> (&HeaderModifiersRemove, &HeaderModifiersAdd) {
        (&self.response_headers_to_remove, &self.response_headers_to_add)
    }
}

impl<B> ModifiersExtractor<Request<B>> for VirtualHost {
    fn extract(&self) -> (&HeaderModifiersRemove, &HeaderModifiersAdd) {
        (&self.request_headers_to_remove, &self.request_headers_to_add)
    }
}

impl<B> ModifiersExtractor<Response<B>> for VirtualHost {
    fn extract(&self) -> (&HeaderModifiersRemove, &HeaderModifiersAdd) {
        (&self.response_headers_to_remove, &self.response_headers_to_add)
    }
}

impl<B> ModifiersExtractor<Request<B>> for RouteConfiguration {
    fn extract(&self) -> (&HeaderModifiersRemove, &HeaderModifiersAdd) {
        (&self.request_headers_to_remove, &self.request_headers_to_add)
    }
}

impl<B> ModifiersExtractor<Response<B>> for RouteConfiguration {
    fn extract(&self) -> (&HeaderModifiersRemove, &HeaderModifiersAdd) {
        (&self.response_headers_to_remove, &self.response_headers_to_add)
    }
}

pub enum HeaderAction {
    Append(HeaderValue),
    Overwrite(HeaderValue),
    Nop,
}

pub trait HeaderValueModifier {
    fn apply_to_request<B>(&self, request: &mut Request<B>) -> bool;
    fn apply_to_response<B>(&self, response: &mut Response<B>) -> bool;
    fn run_action(
        &self,
        action: HeaderAction,
        hmap: &mut HeaderMap<HeaderValue>,
        keep_empty_value: bool,
        header: &HeaderName,
    ) -> bool {
        match action {
            HeaderAction::Append(header_value) => {
                if !keep_empty_value && header_value.is_empty() {
                    hmap.remove(header).is_some()
                } else {
                    hmap.append(header, header_value);
                    true
                }
            },
            HeaderAction::Overwrite(header_value) => {
                if !keep_empty_value && header_value.is_empty() {
                    hmap.remove(header).is_some()
                } else {
                    hmap.insert(header, header_value);
                    true
                }
            },
            HeaderAction::Nop => false,
        }
    }
}

impl HeaderValueModifier for HeaderValueOption {
    fn apply_to_request<B>(&self, request: &mut Request<B>) -> bool {
        let has_key_already = request.headers_mut().get(&self.header.key).is_some();

        let socket_address = || match request.extensions().get::<MetadataContext>() {
            Some(meta) => SocketAddrContext {
                downstream_local_addr: Some(meta.downstream.connection.local_address()),
                downstream_peer_addr: Some(meta.downstream.connection.peer_address()),
                upstream_local_addr: None,
                upstream_peer_addr: None,
            },
            None => SocketAddrContext::default(),
        };

        let get_header_value = |req: &Request<B>| -> HeaderValue {
            let mut formatter = self.header.value.clone();
            formatter.with_context(&DownstreamContext {
                request: req,
                request_head_size: 0,
                trace_id: None,
                server_name: None,
                socket_address: socket_address(),
            });
            formatter
                .into_header_value()
                .inspect_err(|e| {
                    warn!("apply_to_request: failed to convert to HeaderValue: {}", e);
                })
                .unwrap_or(HeaderValue::from_static(""))
        };

        let action = match self.append_action {
            HeaderAppendAction::AppendIfExistsOrAdd => HeaderAction::Append(get_header_value(request)),
            HeaderAppendAction::AppendIfAbsent => {
                if has_key_already {
                    HeaderAction::Nop
                } else {
                    HeaderAction::Append(get_header_value(request))
                }
            },
            HeaderAppendAction::OverwriteIfExistsOrAdd => HeaderAction::Overwrite(get_header_value(request)),
            HeaderAppendAction::OverwriteIfExists => {
                if has_key_already {
                    HeaderAction::Overwrite(get_header_value(request))
                } else {
                    HeaderAction::Nop
                }
            },
        };

        self.run_action(action, request.headers_mut(), self.keep_empty_value, &self.header.key)
    }

    fn apply_to_response<B>(&self, response: &mut Response<B>) -> bool {
        let has_key_already = response.headers_mut().get(&self.header.key).is_some();

        let get_header_value = |res: &Response<B>| -> HeaderValue {
            let mut formatter = self.header.value.clone();
            formatter.with_context(&DownstreamResponseContext { response: res, response_head_size: 0 });
            formatter
                .into_header_value()
                .inspect_err(|e| {
                    warn!("apply_to_response: failed to convert to HeaderValue: {}", e);
                })
                .unwrap_or(HeaderValue::from_static(""))
        };

        let action = match self.append_action {
            HeaderAppendAction::AppendIfExistsOrAdd => HeaderAction::Append(get_header_value(response)),
            HeaderAppendAction::AppendIfAbsent => {
                if has_key_already {
                    HeaderAction::Nop
                } else {
                    HeaderAction::Append(get_header_value(response))
                }
            },
            HeaderAppendAction::OverwriteIfExistsOrAdd => HeaderAction::Overwrite(get_header_value(response)),
            HeaderAppendAction::OverwriteIfExists => {
                if has_key_already {
                    HeaderAction::Overwrite(get_header_value(response))
                } else {
                    HeaderAction::Nop
                }
            },
        };

        self.run_action(action, response.headers_mut(), self.keep_empty_value, &self.header.key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orion_configuration::config::network_filters::http_connection_manager::header_modifier::HeaderKeyValue;
    use orion_format::header_formatter::HeaderFormatter;
    use std::net::Ipv4Addr;

    use http::header::{COOKIE, LOCATION, USER_AGENT};

    #[test]
    fn test_example_1_edge_proxy_no_trusted() {
        let mut request = Request::new(());
        request.headers_mut().insert("x-forwarded-for", "203.0.113.128, 203.0.113.10, 203.0.113.1".parse().unwrap());
        let downstream_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 5)), 80);
        let xff_settings = XffSettings { use_remote_address: true, skip_xff_append: false, xff_num_trusted_hops: 0 };

        process_xff_headers(&mut request, downstream_addr, xff_settings);

        assert_eq!(request.headers().get("x-envoy-external-address").unwrap(), "192.0.2.5");
        assert_eq!(
            request.headers().get("x-forwarded-for").unwrap(),
            "203.0.113.128, 203.0.113.10, 203.0.113.1, 192.0.2.5"
        );
        assert!(request.headers().get("x-envoy-internal").is_none());
    }

    #[test]
    fn test_example_2_internal_proxy_from_edge() {
        let mut request = Request::new(());
        request
            .headers_mut()
            .insert("x-forwarded-for", "203.0.113.128, 203.0.113.10, 203.0.113.1, 192.0.2.5".parse().unwrap());
        let downstream_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 11, 12, 13)), 80);
        let xff_settings = XffSettings { use_remote_address: false, skip_xff_append: false, xff_num_trusted_hops: 0 };

        process_xff_headers(&mut request, downstream_addr, xff_settings);

        assert!(request.headers().get("x-envoy-external-address").is_none());
        assert_eq!(
            request.headers().get("x-forwarded-for").unwrap(),
            "203.0.113.128, 203.0.113.10, 203.0.113.1, 192.0.2.5"
        );
        assert!(request.headers().get("x-envoy-internal").is_none());
    }

    #[test]
    fn test_example_3_edge_proxy_two_trusted_proxies() {
        let mut request = Request::new(());
        request.headers_mut().insert("x-forwarded-for", "203.0.113.128, 203.0.113.10, 203.0.113.1".parse().unwrap());
        let downstream_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 5)), 80);
        let xff_settings = XffSettings { use_remote_address: true, skip_xff_append: false, xff_num_trusted_hops: 2 };

        process_xff_headers(&mut request, downstream_addr, xff_settings);

        assert_eq!(request.headers().get("x-envoy-external-address").unwrap(), "203.0.113.10");
        assert_eq!(
            request.headers().get("x-forwarded-for").unwrap(),
            "203.0.113.128, 203.0.113.10, 203.0.113.1, 192.0.2.5"
        );
        assert!(request.headers().get("x-envoy-internal").is_none());
    }

    #[test]
    fn test_example_4_internal_proxy_from_trusted_edge() {
        let mut request = Request::new(());
        request
            .headers_mut()
            .insert("x-forwarded-for", "203.0.113.128, 203.0.113.10, 203.0.113.1, 192.0.2.5".parse().unwrap());
        let downstream_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 11, 12, 13)), 80);
        let xff_settings = XffSettings { use_remote_address: false, skip_xff_append: false, xff_num_trusted_hops: 0 };

        process_xff_headers(&mut request, downstream_addr, xff_settings);

        assert!(request.headers().get("x-envoy-external-address").is_none());
        assert_eq!(
            request.headers().get("x-forwarded-for").unwrap(),
            "203.0.113.128, 203.0.113.10, 203.0.113.1, 192.0.2.5"
        );
        assert!(request.headers().get("x-envoy-internal").is_none());
    }

    #[test]
    fn test_example_5_internal_proxy_from_internal_client_no_xff() {
        let mut request = Request::new(());
        let downstream_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 20, 30, 40)), 80);
        let xff_settings = XffSettings { use_remote_address: false, skip_xff_append: false, xff_num_trusted_hops: 0 };

        process_xff_headers(&mut request, downstream_addr, xff_settings);

        assert!(request.headers().get("x-envoy-external-address").is_none());
        assert!(request.headers().get("x-forwarded-for").is_none());
        assert!(request.headers().get("x-envoy-internal").is_none());
    }

    #[test]
    fn test_example_6_internal_proxy_from_another_proxy_with_xff() {
        let mut request = Request::new(());
        request.headers_mut().insert("x-forwarded-for", "10.20.30.40".parse().unwrap());
        let downstream_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 20, 30, 50)), 80);
        let xff_settings = XffSettings { use_remote_address: false, skip_xff_append: false, xff_num_trusted_hops: 0 };

        process_xff_headers(&mut request, downstream_addr, xff_settings);

        assert!(request.headers().get("x-envoy-external-address").is_none());
        assert_eq!(request.headers().get("x-forwarded-for").unwrap(), "10.20.30.40");
        //assert_eq!(request.headers().get("x-envoy-internal").unwrap(), "true");
    }

    #[test]
    fn test_header_mutation_append_if_exists_or_add() {
        let mut request = Request::builder().uri("http://example.com").body(()).unwrap();

        let hello = HeaderFormatter::try_new("hello").unwrap();
        let world = HeaderFormatter::try_new("world").unwrap();

        HeaderValueOption {
            header: HeaderKeyValue { key: LOCATION, value: hello.clone() },
            append_action: HeaderAppendAction::AppendIfExistsOrAdd,
            keep_empty_value: false,
        }
        .apply_to_request(&mut request);

        assert_eq!(request.headers().get(LOCATION), Some(&hello.clone().into_header_value().unwrap()));
        assert_eq!(request.headers().len(), 1);

        HeaderValueOption {
            header: HeaderKeyValue { key: LOCATION, value: world.clone() },
            append_action: HeaderAppendAction::AppendIfExistsOrAdd,
            keep_empty_value: false,
        }
        .apply_to_request(&mut request);

        assert_eq!(request.headers().len(), 2);

        let mut iter = request.headers().get_all(LOCATION).iter();
        assert_eq!(&hello.into_header_value().unwrap(), iter.next().unwrap());
        assert_eq!(&world.into_header_value().unwrap(), iter.next().unwrap());
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_header_mutation_inline_append() {
        let mut request = Request::builder().uri("http://example.com").body(()).unwrap();

        let hello = HeaderFormatter::try_new("hello").unwrap();
        let world = HeaderFormatter::try_new("world").unwrap();

        HeaderValueOption {
            header: HeaderKeyValue { key: USER_AGENT, value: hello.clone() },
            append_action: HeaderAppendAction::AppendIfExistsOrAdd,
            keep_empty_value: false,
        }
        .apply_to_request(&mut request);

        assert_eq!(request.headers().get(USER_AGENT), Some(&hello.into_header_value().unwrap()));
        assert_eq!(request.headers().len(), 1);

        HeaderValueOption {
            header: HeaderKeyValue { key: USER_AGENT, value: world.clone() },
            append_action: HeaderAppendAction::AppendIfExistsOrAdd,
            keep_empty_value: false,
        }
        .apply_to_request(&mut request);

        assert_eq!(request.headers().len(), 2);
    }

    #[test]
    fn test_header_mutation_cookie_append() {
        let mut request = Request::builder().uri("http://example.com").body(()).unwrap();

        let hello = HeaderFormatter::try_new("hello").unwrap();
        let world = HeaderFormatter::try_new("world").unwrap();

        HeaderValueOption {
            header: HeaderKeyValue { key: COOKIE, value: hello.clone() },
            append_action: HeaderAppendAction::AppendIfExistsOrAdd,
            keep_empty_value: false,
        }
        .apply_to_request(&mut request);

        assert_eq!(request.headers().get(COOKIE), Some(&hello.into_header_value().unwrap()));
        assert_eq!(request.headers().len(), 1);

        HeaderValueOption {
            header: HeaderKeyValue { key: COOKIE, value: world.clone() },
            append_action: HeaderAppendAction::AppendIfExistsOrAdd,
            keep_empty_value: false,
        }
        .apply_to_request(&mut request);

        assert_eq!(request.headers().len(), 2);
    }

    #[test]
    fn test_header_mutation_append_if_absent() {
        let mut request = Request::builder().uri("http://example.com").body(()).unwrap();

        let hello = HeaderFormatter::try_new("hello").unwrap();

        HeaderValueOption {
            header: HeaderKeyValue { key: USER_AGENT, value: hello.clone() },
            append_action: HeaderAppendAction::AppendIfAbsent,
            keep_empty_value: false,
        }
        .apply_to_request(&mut request);

        assert_eq!(request.headers().get(USER_AGENT), Some(&hello.clone().into_header_value().unwrap()));
        assert_eq!(request.headers().len(), 1);

        HeaderValueOption {
            header: HeaderKeyValue { key: USER_AGENT, value: hello.clone() },
            append_action: HeaderAppendAction::AppendIfAbsent,
            keep_empty_value: false,
        }
        .apply_to_request(&mut request);

        assert_eq!(request.headers().get(USER_AGENT), Some(&hello.into_header_value().unwrap()));
        assert_eq!(request.headers().len(), 1);
    }

    #[test]
    fn test_header_mutation_overwrite_if_exists_or_add() {
        let mut request = Request::builder().uri("http://example.com").body(()).unwrap();

        let hello = HeaderFormatter::try_new("hello").unwrap();
        let world = HeaderFormatter::try_new("world").unwrap();

        HeaderValueOption {
            header: HeaderKeyValue { key: USER_AGENT, value: hello.clone() },
            append_action: HeaderAppendAction::OverwriteIfExistsOrAdd,
            keep_empty_value: false,
        }
        .apply_to_request(&mut request);

        assert_eq!(request.headers().get(USER_AGENT), Some(&hello.into_header_value().unwrap()));
        assert_eq!(request.headers().len(), 1);

        HeaderValueOption {
            header: HeaderKeyValue { key: USER_AGENT, value: world.clone() },
            append_action: HeaderAppendAction::OverwriteIfExistsOrAdd,
            keep_empty_value: false,
        }
        .apply_to_request(&mut request);

        assert_eq!(request.headers().get(USER_AGENT), Some(&world.into_header_value().unwrap()));
        assert_eq!(request.headers().len(), 1);
    }

    #[test]
    fn test_header_mutation_overwrite_if_exists() {
        let mut request = Request::builder().uri("http://example.com").body(()).unwrap();

        let hello = HeaderFormatter::try_new("hello").unwrap();
        let world = HeaderFormatter::try_new("world").unwrap();

        HeaderValueOption {
            header: HeaderKeyValue { key: USER_AGENT, value: hello.clone() },
            append_action: HeaderAppendAction::OverwriteIfExists,
            keep_empty_value: false,
        }
        .apply_to_request(&mut request);

        assert_eq!(request.headers().get(USER_AGENT), None);
        assert!(request.headers().is_empty());
        HeaderValueOption {
            header: HeaderKeyValue { key: USER_AGENT, value: hello.clone() },
            append_action: HeaderAppendAction::AppendIfAbsent,
            keep_empty_value: false,
        }
        .apply_to_request(&mut request);

        assert_eq!(request.headers().get(USER_AGENT), Some(&hello.into_header_value().unwrap()));
        assert_eq!(request.headers().len(), 1);

        HeaderValueOption {
            header: HeaderKeyValue { key: USER_AGENT, value: world.clone() },
            append_action: HeaderAppendAction::OverwriteIfExists,
            keep_empty_value: false,
        }
        .apply_to_request(&mut request);

        assert_eq!(request.headers().get(USER_AGENT), Some(&world.into_header_value().unwrap()));
        assert_eq!(request.headers().len(), 1);
    }

    #[test]
    fn test_header_mutation_append_empty_value() {
        let mut request = Request::builder().uri("http://example.com").body(()).unwrap();

        let empty = HeaderFormatter::try_new("").unwrap();
        let test = HeaderFormatter::try_new("test").unwrap();

        HeaderValueOption {
            header: HeaderKeyValue { key: USER_AGENT, value: test.clone() },
            append_action: HeaderAppendAction::AppendIfExistsOrAdd,
            keep_empty_value: true,
        }
        .apply_to_request(&mut request);

        assert_eq!(request.headers().get(USER_AGENT), Some(&test.clone().into_header_value().unwrap()));
        assert_eq!(request.headers().len(), 1);

        HeaderValueOption {
            header: HeaderKeyValue { key: USER_AGENT, value: empty.clone() },
            append_action: HeaderAppendAction::AppendIfExistsOrAdd,
            keep_empty_value: true,
        }
        .apply_to_request(&mut request);

        assert_eq!(request.headers().get(USER_AGENT), Some(&test.into_header_value().unwrap()));
        assert_eq!(request.headers().len(), 2);
    }
}
