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

use super::upgrade_utils;
use crate::{
    event_error::EventFailure,
    listeners::{metadata::ConnMeta, synthetic_http_response::SyntheticHttpResponse},
    ArionResponseBody,
};
use arion_configuration::config::{
    cluster::http_protocol_options::Codec,
    core::{StringMatcher, StringMatcherPattern},
    network_filters::{
        early_header_mutation::EarlyHeaderMutation,
        http_connection_manager::{
            header_modifier::{HeaderAppendAction, HeaderValueOption},
            HeaderModifiersAdd, HeaderModifiersRemove, Route, RouteConfiguration, VirtualHost, XffSettings,
        },
    },
};
use arion_format::context::{DownstreamContext, DownstreamResponseContext};
use arion_http_header::{X_ENVOY_EXTERNAL_ADDRESS, X_ENVOY_INTERNAL, X_FORWARDED_FOR};
use http::{header, HeaderMap, HeaderName, HeaderValue, Method, Request, Response};
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

#[inline]
pub fn apply_prerouting_functions<T>(
    request: &mut Request<T>,
    downstream_addr: SocketAddr,
    xff_settings: &XffSettings,
    conn: &ConnMeta,
    early_mutations: &[EarlyHeaderMutation],
) {
    // Envoy parity: early header mutations run before any other HCM header
    // processing (XFF append, trusted address, ...).
    request.apply_mutation((early_mutations, conn));
    apply_xff_headers(request, downstream_addr, xff_settings);
}

#[inline]
pub fn apply_preflight_functions<T>(request: &mut Request<T>) -> Option<Response<ArionResponseBody>> {
    if let Some(direct_response) = filter_disallowed_requests(request) {
        return Some(direct_response);
    }
    strip_hop_headers(request.headers_mut());
    None
}

fn filter_disallowed_requests<T>(request: &Request<T>) -> Option<Response<ArionResponseBody>> {
    if request.method() == Method::CONNECT {
        return Some(
            SyntheticHttpResponse::forbidden(EventFailure::UpgradeFailed.into())
                .with_body("CONNECT method not permitted")
                .into_response(request.version()),
        );
    }
    if let Some(connection_header) = request.headers().get(header::CONNECTION) {
        if upgrade_utils::is_upgrade_connection(connection_header.to_str().ok()?) {
            return Some(
                SyntheticHttpResponse::forbidden(EventFailure::UpgradeFailed.into())
                    .with_body("Upgrade not permitted")
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

fn apply_xff_headers<T>(request: &mut Request<T>, downstream_addr: SocketAddr, xff_settings: &XffSettings) {
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

    let is_internal_trusted_client = is_internal_ip(trusted_client_address);
    let xff_contains_single_internal_ip = xff_contains_single_ip && is_internal_trusted_client;
    let xff_contains_single_external_ip = xff_contains_single_ip && !is_internal_trusted_client;

    let has_incoming_xff = existing_xff.is_some();

    let should_update_xff = xff_settings.use_remote_address && !xff_settings.skip_xff_append;
    let should_set_envoy_external = xff_settings.use_remote_address
        && !headers.contains_key(X_ENVOY_EXTERNAL_ADDRESS)
        && !is_internal_trusted_client;
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

pub(crate) fn is_internal_ip(ip: IpAddr) -> bool {
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
    xff_settings: &XffSettings,
) -> (IpAddr, bool) {
    let downstream_ip = downstream_addr.ip();

    // Fast path: with default edge settings (`use_remote_address` without trusted hops)
    // the XFF list is never consulted — skip parsing the header entirely.
    if xff_settings.use_remote_address && xff_settings.xff_num_trusted_hops == 0 {
        return (downstream_ip, false);
    }
    let Some(xff) = existing_xff else {
        return (downstream_ip, false);
    };

    // Valid XFF entries from right to left. Every case below only needs elements near
    // the right end, so iterate without collecting: zero allocation, and work bounded
    // by config (`hops`) instead of header length.
    let mut from_right = xff.split(',').rev().filter_map(|ip_str| ip_str.trim().parse::<IpAddr>().ok());

    if !xff_settings.use_remote_address {
        if xff_settings.xff_num_trusted_hops > 0 {
            // (hops+1)-th valid entry from the right == index `len-hops-1` from the left.
            let hops = xff_settings.xff_num_trusted_hops as usize;
            return (from_right.nth(hops).unwrap_or(downstream_ip), false);
        }
        return match from_right.next() {
            Some(last) => {
                let single = from_right.next().is_none();
                (last, single)
            },
            None => (downstream_ip, false),
        };
    }

    // `use_remote_address` with trusted hops (`hops >= 1` here): hops-th valid entry
    // from the right == index `len-hops` from the left.
    let hops = xff_settings.xff_num_trusted_hops as usize;
    (from_right.nth(hops - 1).unwrap_or(downstream_ip), false)
}

pub trait HeaderMapModifier<M> {
    fn apply_mutation(&mut self, modifier: M);
}

impl<B> HeaderMapModifier<&HeaderModifiersRemove> for Request<B> {
    #[inline]
    fn apply_mutation(&mut self, modifier: &HeaderModifiersRemove) {
        for name in &modifier.0 {
            self.headers_mut().remove(name);
        }
    }
}

impl<B> HeaderMapModifier<&HeaderModifiersRemove> for Response<B> {
    #[inline]
    fn apply_mutation(&mut self, modifier: &HeaderModifiersRemove) {
        for name in &modifier.0 {
            self.headers_mut().remove(name);
        }
    }
}

impl<B> HeaderMapModifier<(&HeaderModifiersAdd, &ConnMeta)> for Request<B> {
    #[inline]
    fn apply_mutation(&mut self, (modifier, conn): (&HeaderModifiersAdd, &ConnMeta)) {
        for modifier in &modifier.0 {
            modifier.apply_to_request(self, conn);
        }
    }
}

impl<B> HeaderMapModifier<(&HeaderModifiersAdd, &ConnMeta)> for Response<B> {
    #[inline]
    fn apply_mutation(&mut self, (modifier, conn): (&HeaderModifiersAdd, &ConnMeta)) {
        for modifier in &modifier.0 {
            modifier.apply_to_response(self, conn);
        }
    }
}

impl<B> HeaderMapModifier<(&[EarlyHeaderMutation], &ConnMeta)> for Request<B> {
    #[inline]
    fn apply_mutation(&mut self, (mutations, conn): (&[EarlyHeaderMutation], &ConnMeta)) {
        for mutation in mutations {
            match mutation {
                EarlyHeaderMutation::Remove(name) => {
                    self.headers_mut().remove(name);
                },
                EarlyHeaderMutation::Append(option) => {
                    option.apply_to_request(self, conn);
                },
                EarlyHeaderMutation::RemoveOnMatch(pattern) => {
                    remove_matching_headers(self.headers_mut(), pattern);
                },
            }
        }
    }
}

// Removes every header whose name matches `pattern`. Header names are always
// lowercase in `http::HeaderMap`, while the pattern may carry any casing, so
// the match is case-insensitive (header names are case-insensitive per
// RFC 9110; the pattern alone carries no case flag). `Regex` is unaffected
// by this, matching Envoy semantics where `ignore_case` does not apply to it.
fn remove_matching_headers(headers: &mut HeaderMap, pattern: &StringMatcherPattern) {
    let matcher = StringMatcher { ignore_case: true, pattern: pattern.clone() };
    let matched: Vec<HeaderName> = headers.keys().filter(|name| matcher.matches(name.as_str())).cloned().collect();
    for name in matched {
        headers.remove(name);
    }
}

impl<'m1, 'm2, M1, M2, B> HeaderMapModifier<(&'m1 M1, &'m2 M2)> for Request<B>
where
    Request<B>: HeaderMapModifier<&'m1 M1>,
    Request<B>: HeaderMapModifier<&'m2 M2>,
{
    #[inline]
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
    #[inline]
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
    #[inline]
    fn extract(&self) -> (&HeaderModifiersRemove, &HeaderModifiersAdd) {
        (&self.request_headers_to_remove, &self.request_headers_to_add)
    }
}

impl<B> ModifiersExtractor<Response<B>> for Route {
    #[inline]
    fn extract(&self) -> (&HeaderModifiersRemove, &HeaderModifiersAdd) {
        (&self.response_headers_to_remove, &self.response_headers_to_add)
    }
}

impl<B> ModifiersExtractor<Request<B>> for VirtualHost {
    #[inline]
    fn extract(&self) -> (&HeaderModifiersRemove, &HeaderModifiersAdd) {
        (&self.request_headers_to_remove, &self.request_headers_to_add)
    }
}

impl<B> ModifiersExtractor<Response<B>> for VirtualHost {
    #[inline]
    fn extract(&self) -> (&HeaderModifiersRemove, &HeaderModifiersAdd) {
        (&self.response_headers_to_remove, &self.response_headers_to_add)
    }
}

impl<B> ModifiersExtractor<Request<B>> for RouteConfiguration {
    #[inline]
    fn extract(&self) -> (&HeaderModifiersRemove, &HeaderModifiersAdd) {
        (&self.request_headers_to_remove, &self.request_headers_to_add)
    }
}

impl<B> ModifiersExtractor<Response<B>> for RouteConfiguration {
    #[inline]
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
    fn apply_to_request<B>(&self, request: &mut Request<B>, conn: &ConnMeta) -> bool;
    fn apply_to_response<B>(&self, response: &mut Response<B>, conn: &ConnMeta) -> bool;
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
    fn apply_to_request<B>(&self, request: &mut Request<B>, conn: &ConnMeta) -> bool {
        let has_key_already = request.headers_mut().get(&self.header.key).is_some();

        let get_header_value = |req: &Request<B>| -> HeaderValue {
            let mut formatter = self.header.value.clone();
            // Resolves dynamic variables in header values (e.g., %DOWNSTREAM_PEER_ADDRESS%).
            // request_head_size is set to 0 as a placeholder to avoid expensive and unnecessary calculations.
            formatter.with_context(&DownstreamContext {
                request: req,
                request_head_size: 0,
                trace_id: None,
                server_name: None,
                socket_address: conn.downstream_socket_addr_context(),
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

    fn apply_to_response<B>(&self, response: &mut Response<B>, _conn: &ConnMeta) -> bool {
        let has_key_already = response.headers_mut().get(&self.header.key).is_some();

        let get_header_value = |res: &Response<B>| -> HeaderValue {
            let mut formatter = self.header.value.clone();
            // Response formatters currently don't use connection addresses; `conn` keeps the API symmetric.
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
    use arion_configuration::config::network_filters::http_connection_manager::header_modifier::HeaderKeyValue;
    use arion_format::header_formatter::HeaderFormatter;
    use std::net::Ipv4Addr;

    use http::header::{COOKIE, LOCATION, USER_AGENT};

    #[test]
    fn test_example_1_edge_proxy_no_trusted() {
        let mut request = Request::new(());
        request.headers_mut().insert("x-forwarded-for", "203.0.113.128, 203.0.113.10, 203.0.113.1".parse().unwrap());
        let downstream_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 5)), 80);
        let xff_settings = XffSettings { use_remote_address: true, skip_xff_append: false, xff_num_trusted_hops: 0 };

        apply_xff_headers(&mut request, downstream_addr, &xff_settings);

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

        apply_xff_headers(&mut request, downstream_addr, &xff_settings);

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

        apply_xff_headers(&mut request, downstream_addr, &xff_settings);

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

        apply_xff_headers(&mut request, downstream_addr, &xff_settings);

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

        apply_xff_headers(&mut request, downstream_addr, &xff_settings);

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

        apply_xff_headers(&mut request, downstream_addr, &xff_settings);

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
        .apply_to_request(&mut request, &ConnMeta::default());

        assert_eq!(request.headers().get(LOCATION), Some(&hello.clone().into_header_value().unwrap()));
        assert_eq!(request.headers().len(), 1);

        HeaderValueOption {
            header: HeaderKeyValue { key: LOCATION, value: world.clone() },
            append_action: HeaderAppendAction::AppendIfExistsOrAdd,
            keep_empty_value: false,
        }
        .apply_to_request(&mut request, &ConnMeta::default());

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
        .apply_to_request(&mut request, &ConnMeta::default());

        assert_eq!(request.headers().get(USER_AGENT), Some(&hello.into_header_value().unwrap()));
        assert_eq!(request.headers().len(), 1);

        HeaderValueOption {
            header: HeaderKeyValue { key: USER_AGENT, value: world.clone() },
            append_action: HeaderAppendAction::AppendIfExistsOrAdd,
            keep_empty_value: false,
        }
        .apply_to_request(&mut request, &ConnMeta::default());

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
        .apply_to_request(&mut request, &ConnMeta::default());

        assert_eq!(request.headers().get(COOKIE), Some(&hello.into_header_value().unwrap()));
        assert_eq!(request.headers().len(), 1);

        HeaderValueOption {
            header: HeaderKeyValue { key: COOKIE, value: world.clone() },
            append_action: HeaderAppendAction::AppendIfExistsOrAdd,
            keep_empty_value: false,
        }
        .apply_to_request(&mut request, &ConnMeta::default());

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
        .apply_to_request(&mut request, &ConnMeta::default());

        assert_eq!(request.headers().get(USER_AGENT), Some(&hello.clone().into_header_value().unwrap()));
        assert_eq!(request.headers().len(), 1);

        HeaderValueOption {
            header: HeaderKeyValue { key: USER_AGENT, value: hello.clone() },
            append_action: HeaderAppendAction::AppendIfAbsent,
            keep_empty_value: false,
        }
        .apply_to_request(&mut request, &ConnMeta::default());

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
        .apply_to_request(&mut request, &ConnMeta::default());

        assert_eq!(request.headers().get(USER_AGENT), Some(&hello.into_header_value().unwrap()));
        assert_eq!(request.headers().len(), 1);

        HeaderValueOption {
            header: HeaderKeyValue { key: USER_AGENT, value: world.clone() },
            append_action: HeaderAppendAction::OverwriteIfExistsOrAdd,
            keep_empty_value: false,
        }
        .apply_to_request(&mut request, &ConnMeta::default());

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
        .apply_to_request(&mut request, &ConnMeta::default());

        assert_eq!(request.headers().get(USER_AGENT), None);
        assert!(request.headers().is_empty());
        HeaderValueOption {
            header: HeaderKeyValue { key: USER_AGENT, value: hello.clone() },
            append_action: HeaderAppendAction::AppendIfAbsent,
            keep_empty_value: false,
        }
        .apply_to_request(&mut request, &ConnMeta::default());

        assert_eq!(request.headers().get(USER_AGENT), Some(&hello.into_header_value().unwrap()));
        assert_eq!(request.headers().len(), 1);

        HeaderValueOption {
            header: HeaderKeyValue { key: USER_AGENT, value: world.clone() },
            append_action: HeaderAppendAction::OverwriteIfExists,
            keep_empty_value: false,
        }
        .apply_to_request(&mut request, &ConnMeta::default());

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
        .apply_to_request(&mut request, &ConnMeta::default());

        assert_eq!(request.headers().get(USER_AGENT), Some(&test.clone().into_header_value().unwrap()));
        assert_eq!(request.headers().len(), 1);

        HeaderValueOption {
            header: HeaderKeyValue { key: USER_AGENT, value: empty.clone() },
            append_action: HeaderAppendAction::AppendIfExistsOrAdd,
            keep_empty_value: true,
        }
        .apply_to_request(&mut request, &ConnMeta::default());

        assert_eq!(request.headers().get(USER_AGENT), Some(&test.into_header_value().unwrap()));
        assert_eq!(request.headers().len(), 2);
    }

    #[test]
    fn trusted_client_address_covers_all_branches() {
        let downstream = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 5)), 80);
        let downstream_ip = downstream.ip();
        let settings = |use_remote: bool, hops: u32| XffSettings {
            use_remote_address: use_remote,
            skip_xff_append: false,
            xff_num_trusted_hops: hops,
        };
        let v4 = |a: u8, b: u8, c: u8, d: u8| IpAddr::V4(Ipv4Addr::new(a, b, c, d));

        // Fast path: `use_remote_address` without trusted hops never consults XFF,
        // whatever the header contains.
        for xff in [None, Some(""), Some("203.0.113.7"), Some("garbage, also-garbage, 1.1.1.1")] {
            assert_eq!(
                determine_trusted_client_address(xff, downstream, &settings(true, 0)),
                (downstream_ip, false),
                "fast path, xff={xff:?}"
            );
        }

        // Missing, empty or fully-invalid XFF falls back to downstream in every mode.
        for (use_remote, hops) in [(true, 1), (true, 5), (false, 0), (false, 2)] {
            for xff in [None, Some(""), Some("   "), Some("not-an-ip, ???")] {
                assert_eq!(
                    determine_trusted_client_address(xff, downstream, &settings(use_remote, hops)),
                    (downstream_ip, false),
                    "fallback, use_remote={use_remote} hops={hops} xff={xff:?}"
                );
            }
        }

        // `!use_remote`, 0 hops: last valid entry + single-entry flag.
        let no_trusted = settings(false, 0);
        assert_eq!(
            determine_trusted_client_address(Some("10.0.0.1"), downstream, &no_trusted),
            (v4(10, 0, 0, 1), true)
        );
        assert_eq!(
            determine_trusted_client_address(Some("10.0.0.1, 10.0.0.2"), downstream, &no_trusted),
            (v4(10, 0, 0, 2), false)
        );
        assert_eq!(
            determine_trusted_client_address(Some("  10.0.0.1  ,  10.0.0.2 "), downstream, &no_trusted),
            (v4(10, 0, 0, 2), false)
        );
        // Invalid entries are skipped; a single remaining valid entry still counts as single.
        assert_eq!(
            determine_trusted_client_address(Some("10.0.0.1, garbage"), downstream, &no_trusted),
            (v4(10, 0, 0, 1), true)
        );
        assert_eq!(
            determine_trusted_client_address(Some("garbage, 10.0.0.1, 10.0.0.2"), downstream, &no_trusted),
            (v4(10, 0, 0, 2), false)
        );
        assert_eq!(
            determine_trusted_client_address(Some("::1"), downstream, &no_trusted),
            (IpAddr::from([0, 0, 0, 0, 0, 0, 0, 1]), true)
        );

        // `!use_remote` with trusted hops: index `len-hops-1` over valid entries only.
        let xff = Some("10.0.0.1, 10.0.0.2, 10.0.0.3");
        assert_eq!(determine_trusted_client_address(xff, downstream, &settings(false, 1)), (v4(10, 0, 0, 2), false));
        assert_eq!(determine_trusted_client_address(xff, downstream, &settings(false, 2)), (v4(10, 0, 0, 1), false));
        // Hops beyond the list length fall back to downstream.
        for hops in [3, 99, u32::MAX] {
            assert_eq!(
                determine_trusted_client_address(xff, downstream, &settings(false, hops)),
                (downstream_ip, false),
                "out-of-range hops={hops}"
            );
        }
        // Indexing counts valid entries only, like the old collected `Vec` did.
        assert_eq!(
            determine_trusted_client_address(
                Some("10.0.0.1, junk, 10.0.0.2, 10.0.0.3"),
                downstream,
                &settings(false, 1)
            ),
            (v4(10, 0, 0, 2), false)
        );

        // `use_remote` with trusted hops: index `len-hops` over valid entries only.
        assert_eq!(determine_trusted_client_address(xff, downstream, &settings(true, 1)), (v4(10, 0, 0, 3), false));
        assert_eq!(determine_trusted_client_address(xff, downstream, &settings(true, 2)), (v4(10, 0, 0, 2), false));
        assert_eq!(determine_trusted_client_address(xff, downstream, &settings(true, 3)), (v4(10, 0, 0, 1), false));
        assert_eq!(determine_trusted_client_address(xff, downstream, &settings(true, 4)), (downstream_ip, false));
        assert_eq!(
            determine_trusted_client_address(
                Some("junk, 10.0.0.1, 10.0.0.2, 10.0.0.3"),
                downstream,
                &settings(true, 3)
            ),
            (v4(10, 0, 0, 1), false)
        );
    }

    fn append_mutation(key: HeaderName, value: &str) -> EarlyHeaderMutation {
        EarlyHeaderMutation::Append(HeaderValueOption {
            header: HeaderKeyValue { key, value: HeaderFormatter::try_new(value).unwrap() },
            append_action: HeaderAppendAction::AppendIfExistsOrAdd,
            keep_empty_value: false,
        })
    }

    #[test]
    fn test_early_mutation_remove() {
        let mut request = Request::builder().uri("http://example.com").body(()).unwrap();
        request.headers_mut().insert("x-remove-me", "1".parse().unwrap());
        request.headers_mut().insert("x-keep-me", "2".parse().unwrap());

        let mutations = vec![EarlyHeaderMutation::Remove("x-remove-me".parse().unwrap())];
        request.apply_mutation((mutations.as_slice(), &ConnMeta::default()));

        assert!(request.headers().get("x-remove-me").is_none());
        assert_eq!(request.headers().get("x-keep-me").unwrap(), "2");
    }

    #[test]
    fn test_early_mutation_append() {
        let mut request = Request::builder().uri("http://example.com").body(()).unwrap();

        let mutations = vec![append_mutation(LOCATION, "hello")];
        request.apply_mutation((mutations.as_slice(), &ConnMeta::default()));

        assert_eq!(request.headers().get(LOCATION).unwrap(), "hello");
    }

    #[test]
    fn test_early_mutation_remove_on_match() {
        let mut request = Request::builder().uri("http://example.com").body(()).unwrap();
        for (name, value) in [("x-secret-token", "1"), ("x-secret-key", "2"), ("x-public", "3"), ("content-type", "4")]
        {
            request.headers_mut().insert(name, value.parse().unwrap());
        }

        // prefix match removes both `x-secret-*` headers, nothing else.
        let mutations = vec![EarlyHeaderMutation::RemoveOnMatch(StringMatcherPattern::Prefix("x-secret-".into()))];
        request.apply_mutation((mutations.as_slice(), &ConnMeta::default()));

        assert!(request.headers().get("x-secret-token").is_none());
        assert!(request.headers().get("x-secret-key").is_none());
        assert_eq!(request.headers().get("x-public").unwrap(), "3");
        assert_eq!(request.headers().get("content-type").unwrap(), "4");
    }

    #[test]
    fn test_early_mutation_remove_on_match_case_insensitive() {
        let mut request = Request::builder().uri("http://example.com").body(()).unwrap();
        request.headers_mut().insert("x-mixed", "1".parse().unwrap());

        // header names are lowercase in `HeaderMap`; an uppercase pattern must still match.
        let mutations = vec![EarlyHeaderMutation::RemoveOnMatch(StringMatcherPattern::Exact("X-MIXED".into()))];
        request.apply_mutation((mutations.as_slice(), &ConnMeta::default()));

        assert!(request.headers().get("x-mixed").is_none());
    }

    #[test]
    fn test_early_mutation_remove_on_match_regex() {
        let mut request = Request::builder().uri("http://example.com").body(()).unwrap();
        request.headers_mut().insert("x-trace-123", "1".parse().unwrap());
        request.headers_mut().insert("x-trace-abc", "2".parse().unwrap());
        request.headers_mut().insert("x-other", "3".parse().unwrap());

        let mutations = vec![EarlyHeaderMutation::RemoveOnMatch(StringMatcherPattern::Regex(
            regex::Regex::new(r"x-trace-\d+").unwrap(),
        ))];
        request.apply_mutation((mutations.as_slice(), &ConnMeta::default()));

        assert!(request.headers().get("x-trace-123").is_none());
        assert_eq!(request.headers().get("x-trace-abc").unwrap(), "2");
        assert_eq!(request.headers().get("x-other").unwrap(), "3");
    }

    #[test]
    fn test_early_mutations_apply_in_order_before_xff() {
        let mut request = Request::builder().uri("http://example.com").body(()).unwrap();
        request.headers_mut().insert("x-remove-me", "1".parse().unwrap());
        let downstream_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 5)), 80);
        let xff_settings = XffSettings { use_remote_address: true, skip_xff_append: false, xff_num_trusted_hops: 0 };

        let mutations =
            vec![EarlyHeaderMutation::Remove("x-remove-me".parse().unwrap()), append_mutation(LOCATION, "hello")];
        apply_prerouting_functions(&mut request, downstream_addr, &xff_settings, &ConnMeta::default(), &mutations);

        // early mutations applied ...
        assert!(request.headers().get("x-remove-me").is_none());
        assert_eq!(request.headers().get(LOCATION).unwrap(), "hello");
        // ... and XFF processing still ran afterwards (Envoy parity).
        assert_eq!(request.headers().get("x-envoy-external-address").unwrap(), "192.0.2.5");
    }
}
