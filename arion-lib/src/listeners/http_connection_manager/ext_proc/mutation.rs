// Copyright 2025-2026 The arion-gateway Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::borrow::Cow;

use arion_configuration::config::network_filters::http_connection_manager::http_filters::ext_proc::HeaderMutationRules;
use arion_data_plane_api::envoy_data_plane_api::envoy::{
    config::core::v3::{header_value_option::HeaderAppendAction, HeaderValueOption},
    service::ext_proc::v3::HeaderMutation,
};
use http::{
    uri::{Authority, PathAndQuery, Scheme, Uri},
    Request, Response,
};
use tracing::warn;

use crate::Error;

/// Extracts the string value from a `HeaderValueOption`.
/// Returns:
/// - None if the header is not present
/// - Some(Ok(s)) if the value was successfully extracted as a string
/// - Some(Err(())) if `raw_value` contains invalid UTF-8
fn extract_header_value_as_str(opt: &HeaderValueOption) -> Option<Cow<'_, str>> {
    match opt.header.as_ref() {
        Some(h) if !h.raw_value.is_empty() => Some(String::from_utf8_lossy(&h.raw_value)),
        Some(h) => Some(Cow::Borrowed(h.value.as_str())),
        None => None,
    }
}

/// Holds the pseudo-headers that need to be applied to a request
#[derive(Debug)]
struct PseudoHeaders {
    method: Option<HeaderValueOption>,
    scheme: Option<HeaderValueOption>,
    authority: Option<HeaderValueOption>,
    path: Option<HeaderValueOption>,
    status: Option<HeaderValueOption>,
}

impl PseudoHeaders {
    fn new() -> Self {
        PseudoHeaders { method: None, scheme: None, authority: None, path: None, status: None }
    }

    fn is_empty(&self) -> bool {
        self.method.is_none()
            && self.scheme.is_none()
            && self.authority.is_none()
            && self.path.is_none()
            && self.status.is_none()
    }
}

fn extract_pseudo_headers(mutation: &mut HeaderMutation) -> PseudoHeaders {
    let mut pseudo_headers = PseudoHeaders::new();
    let mut i = 0;

    while i < mutation.set_headers.len() {
        let is_pseudo = mutation
            .set_headers
            .get(i)
            .and_then(|h| h.header.as_ref())
            .map(|h| h.key.starts_with(':'))
            .unwrap_or(false);

        if is_pseudo {
            let header_to_set = mutation.set_headers.remove(i);
            let key = header_to_set.header.as_ref().map(|h| h.key.as_str()).unwrap_or("");

            if key == super::pseudo_header::METHOD {
                pseudo_headers.method = Some(header_to_set);
            } else if key == super::pseudo_header::SCHEME {
                pseudo_headers.scheme = Some(header_to_set);
            } else if key == super::pseudo_header::AUTHORITY {
                pseudo_headers.authority = Some(header_to_set);
            } else if key == super::pseudo_header::PATH {
                pseudo_headers.path = Some(header_to_set);
            } else if key == super::pseudo_header::STATUS {
                pseudo_headers.status = Some(header_to_set);
            }
        } else {
            i += 1;
        }
    }

    pseudo_headers
}

#[allow(clippy::str_to_string)]
#[allow(clippy::unnecessary_to_owned)]
#[allow(clippy::single_match)]
pub fn apply_request_header_mutations<B>(
    req: &mut Request<B>,
    mut mutation: HeaderMutation,
    mutation_rules: Option<&HeaderMutationRules>,
) -> Result<(), Error> {
    let pseudo_headers = extract_pseudo_headers(&mut mutation);

    // Apply pseudo-headers
    if !pseudo_headers.is_empty() {
        // Handle :method separately as it's not part of URI
        if let Some(method_opt) = pseudo_headers.method {
            // NOTE: if mutation rules is not specified, we allow any modification. This is not the
            // same behavior as envoy, but is more permissive for users who don't set mutation rules.
            if mutation_rules.map(|r| r.is_modification_permitted(super::pseudo_header::METHOD)).unwrap_or(true) {
                match extract_header_value_as_str(&method_opt) {
                    Some(method) => {
                        if let Ok(new_method) = http::Method::from_bytes(method.as_bytes()) {
                            *req.method_mut() = new_method;
                        } else {
                            warn!(target: "ext_proc", "Invalid method in request mutation: {}", method);
                        }
                    },
                    None => {},
                }
            }
        }

        // Handle URI-related pseudo-headers in a single pass
        if pseudo_headers.scheme.is_some() || pseudo_headers.authority.is_some() || pseudo_headers.path.is_some() {
            let mut parts = req.uri().clone().into_parts();

            if let Some(scheme_opt) = pseudo_headers.scheme {
                if mutation_rules.map(|r| r.is_modification_permitted(super::pseudo_header::SCHEME)).unwrap_or(true) {
                    match extract_header_value_as_str(&scheme_opt) {
                        Some(scheme) => {
                            if let Ok(scheme) = Scheme::try_from(scheme.as_ref()) {
                                parts.scheme = Some(scheme);
                            } else {
                                warn!(target: "ext_proc", "Invalid scheme in request mutation: {}", scheme);
                            }
                        },
                        None => {},
                    }
                }
            }

            if let Some(authority_opt) = pseudo_headers.authority {
                if mutation_rules.map(|r| r.is_modification_permitted(super::pseudo_header::AUTHORITY)).unwrap_or(true)
                {
                    match extract_header_value_as_str(&authority_opt) {
                        Some(authority) => {
                            if let Ok(authority) = Authority::try_from(authority.as_ref()) {
                                parts.authority = Some(authority);
                            } else {
                                warn!(target: "ext_proc", "Invalid authority in request mutation: {}", authority);
                            }
                        },
                        None => {},
                    }
                }
            }

            if let Some(path_opt) = pseudo_headers.path {
                if mutation_rules.map(|r| r.is_modification_permitted(super::pseudo_header::PATH)).unwrap_or(true) {
                    match extract_header_value_as_str(&path_opt) {
                        Some(path) => {
                            if let Ok(path_and_query) = PathAndQuery::try_from(path.as_ref()) {
                                parts.path_and_query = Some(path_and_query);
                            } else {
                                warn!(target: "ext_proc", "Invalid path in request mutation: {}", path);
                            }
                        },
                        None => {},
                    }
                }
            }

            if let Ok(new_uri) = Uri::from_parts(parts) {
                *req.uri_mut() = new_uri;
            }
        }
    }

    // Handle regular headers
    apply_header_mutations(req.headers_mut(), mutation, mutation_rules)
}

#[allow(clippy::single_match)]
pub fn apply_response_header_mutations<B>(
    resp: &mut Response<B>,
    mut mutation: HeaderMutation,
    mutation_rules: Option<&HeaderMutationRules>,
) -> Result<(), Error> {
    let pseudo_headers = extract_pseudo_headers(&mut mutation);

    // Handle :status pseudo-header
    if let Some(status_opt) = pseudo_headers.status {
        if mutation_rules.map(|r| r.is_modification_permitted(super::pseudo_header::STATUS)).unwrap_or(true) {
            match extract_header_value_as_str(&status_opt) {
                Some(status_str) => {
                    if let Ok(status_code) = status_str.parse::<u16>() {
                        if let Ok(new_code) = http::StatusCode::from_u16(status_code) {
                            *resp.status_mut() = new_code;
                        } else {
                            warn!(target: "ext_proc", "Invalid status code in response mutation: {}", status_code);
                        }
                    } else {
                        warn!(target: "ext_proc", "Failed to parse status code: {}", status_str);
                    }
                },
                None => {},
            }
        }
    }

    // Handle regular headers
    apply_header_mutations(resp.headers_mut(), mutation, mutation_rules)
}

#[inline]
pub fn apply_trailer_mutations(
    trailers: &mut http::HeaderMap,
    mutation: HeaderMutation,
    mutation_rules: Option<&HeaderMutationRules>,
) -> Result<(), Error> {
    apply_header_mutations(trailers, mutation, mutation_rules)
}

pub fn apply_header_mutations(
    headers: &mut http::HeaderMap,
    mutation: HeaderMutation,
    mutation_rules: Option<&HeaderMutationRules>,
) -> Result<(), Error> {
    for header_to_remove in mutation.remove_headers {
        if let Some(rules) = mutation_rules {
            if !rules.is_modification_permitted(&header_to_remove) {
                if rules.disallow_is_error {
                    return Err(Error::from(format!(
                        "Header removal not permitted by configuration: {header_to_remove}"
                    )));
                }
                continue;
            }
        }
        if let Ok(header_name) = http::HeaderName::from_bytes(header_to_remove.as_bytes()) {
            headers.remove(&header_name);
        }
    }
    for mut header_to_set in mutation.set_headers {
        let Some(header) = header_to_set.header.take() else { continue };
        if let Some(rules) = mutation_rules {
            if !rules.is_modification_permitted(&header.key) {
                if rules.disallow_is_error {
                    return Err(Error::from(format!(
                        "Header modification not permitted by configuration: {}",
                        header.key
                    )));
                }
                continue;
            }
        }
        let Ok(header_name) = http::HeaderName::from_bytes(header.key.as_bytes()) else { continue };
        let header_value = if header.raw_value.is_empty() {
            http::HeaderValue::from_maybe_shared(bytes::Bytes::from(header.value))
        } else {
            http::HeaderValue::from_maybe_shared(bytes::Bytes::from(header.raw_value))
        };
        let Ok(header_value) = header_value else { continue };
        match header_to_set.append_action() {
            HeaderAppendAction::AppendIfExistsOrAdd => {
                headers.append(header_name, header_value);
            },
            HeaderAppendAction::AddIfAbsent => {
                if !headers.contains_key(&header_name) {
                    headers.append(header_name, header_value);
                }
            },
            HeaderAppendAction::OverwriteIfExistsOrAdd => {
                headers.insert(header_name, header_value);
            },
            HeaderAppendAction::OverwriteIfExists => {
                if headers.contains_key(&header_name) {
                    headers.insert(header_name, header_value);
                }
            },
        }
    }
    Ok(())
}
