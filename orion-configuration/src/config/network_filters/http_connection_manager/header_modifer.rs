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

use crate::config::network_filters::http_connection_manager::{Route, RouteConfiguration, VirtualHost};

use super::GenericError;
use http::{HeaderName, HeaderValue, Request, Response};
use orion_format::{
    context::{DownstreamContext, DownstreamResponseContext},
    header_formatter::HeaderFormatter,
};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use tracing::warn;

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct HeaderModifiersRemove(
    #[serde(with = "http_serde_ext::header_name::vec", default, skip_serializing_if = "is_default")] pub Vec<HeaderName>,
);

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct HeaderModifiersAdd(
    #[serde(skip_serializing_if = "Vec::is_empty", default = "Default::default")] pub Vec<HeaderValueOption>,
);

pub trait HeaderMapModifier<M> {
    fn apply(&mut self, modifier: M);
}

impl<B> HeaderMapModifier<&HeaderModifiersRemove> for Request<B> {
    fn apply(&mut self, modifier: &HeaderModifiersRemove) {
        for name in &modifier.0 {
            self.headers_mut().remove(name);
        }
    }
}

impl<B> HeaderMapModifier<&HeaderModifiersRemove> for Response<B> {
    fn apply(&mut self, modifier: &HeaderModifiersRemove) {
        for name in &modifier.0 {
            self.headers_mut().remove(name);
        }
    }
}

impl<B> HeaderMapModifier<&HeaderModifiersAdd> for Request<B> {
    fn apply(&mut self, modifier: &HeaderModifiersAdd) {
        for modifier in &modifier.0 {
            modifier.apply_to_request(self);
        }
    }
}

impl<B> HeaderMapModifier<&HeaderModifiersAdd> for Response<B> {
    fn apply(&mut self, modifier: &HeaderModifiersAdd) {
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
    fn apply(&mut self, (m1, m2): (&'m1 M1, &'m2 M2)) {
        self.apply(m1);
        self.apply(m2);
    }
}

impl<'m1, 'm2, M1, M2, B> HeaderMapModifier<(&'m1 M1, &'m2 M2)> for Response<B>
where
    Response<B>: HeaderMapModifier<&'m1 M1>,
    Response<B>: HeaderMapModifier<&'m2 M2>,
{
    fn apply(&mut self, (m1, m2): (&'m1 M1, &'m2 M2)) {
        self.apply(m1);
        self.apply(m2);
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

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeaderValueOption {
    pub header: HeaderKeyValue,
    pub append_action: HeaderAppendAction,
    pub keep_empty_value: bool,
}

impl HeaderValueOption {
    pub fn apply_to_request<B>(&self, req: &mut Request<B>) -> bool {
        if self.header.value.is_empty() && !self.keep_empty_value {
            req.headers_mut().remove(&self.header.key).is_some()
        } else {
            match self.append_action {
                HeaderAppendAction::AppendIfExistsOrAdd => {
                    let mut formatter = self.header.value.clone();
                    formatter.with_context(&DownstreamContext {
                        request: &req,
                        request_head_size: 0,
                        trace_id: None,
                        server_name: None,
                        socket_address: Default::default(),
                    });
                    let header_value = formatter
                        .into_header_value()
                        .inspect_err(|e| {
                            warn!("HeaderValue: failed to convert to HeaderValue: {}", e);
                        })
                        .unwrap_or(HeaderValue::from_static(""));
                    req.headers_mut().append(&self.header.key, header_value);
                    true
                },
                HeaderAppendAction::AppendIfAbsent => {
                    if req.headers_mut().get(&self.header.key).is_none() {
                        let mut formatter = self.header.value.clone();
                        formatter.with_context(&DownstreamContext {
                            request: &req,
                            request_head_size: 0,
                            trace_id: None,
                            server_name: None,
                            socket_address: Default::default(),
                        });
                        let header_value = formatter
                            .into_header_value()
                            .inspect_err(|e| {
                                warn!("HeaderValue: failed to convert to HeaderValue: {}", e);
                            })
                            .unwrap_or(HeaderValue::from_static(""));
                        req.headers_mut().append(&self.header.key, header_value);
                        true
                    } else {
                        false
                    }
                },
                HeaderAppendAction::OverwriteIfExistsOrAdd => {
                    let mut formatter = self.header.value.clone();
                    formatter.with_context(&DownstreamContext {
                        request: &req,
                        request_head_size: 0,
                        trace_id: None,
                        server_name: None,
                        socket_address: Default::default(),
                    });
                    let header_value = formatter
                        .into_header_value()
                        .inspect_err(|e| {
                            warn!("HeaderValue: failed to convert to HeaderValue: {}", e);
                        })
                        .unwrap_or(HeaderValue::from_static(""));
                    req.headers_mut().insert(&self.header.key, header_value);
                    true
                },
                HeaderAppendAction::OverwriteIfExists => {
                    if req.headers_mut().get(&self.header.key).is_some() {
                        let mut formatter = self.header.value.clone();
                        formatter.with_context(&DownstreamContext {
                            request: &req,
                            request_head_size: 0,
                            trace_id: None,
                            server_name: None,
                            socket_address: Default::default(),
                        });

                        let header_value = formatter
                            .into_header_value()
                            .inspect_err(|e| {
                                warn!("HeaderValue: failed to convert to HeaderValue: {}", e);
                            })
                            .unwrap_or(HeaderValue::from_static(""));
                        req.headers_mut().insert(&self.header.key, header_value);
                        true
                    } else {
                        false
                    }
                },
            }
        }
    }

    pub fn apply_to_response<B>(&self, res: &mut Response<B>) -> bool {
        if self.header.value.is_empty() && !self.keep_empty_value {
            res.headers_mut().remove(&self.header.key).is_some()
        } else {
            match self.append_action {
                HeaderAppendAction::AppendIfExistsOrAdd => {
                    let mut formatter = self.header.value.clone();
                    formatter.with_context(&DownstreamResponseContext { response: &res, response_head_size: 0 });
                    let header_value = formatter
                        .into_header_value()
                        .inspect_err(|e| {
                            warn!("HeaderValue: failed to convert to HeaderValue: {}", e);
                        })
                        .unwrap_or(HeaderValue::from_static(""));
                    res.headers_mut().append(&self.header.key, header_value);
                    true
                },
                HeaderAppendAction::AppendIfAbsent => {
                    if res.headers_mut().get(&self.header.key).is_none() {
                        let mut formatter = self.header.value.clone();
                        formatter.with_context(&DownstreamResponseContext { response: &res, response_head_size: 0 });
                        let header_value = formatter
                            .into_header_value()
                            .inspect_err(|e| {
                                warn!("HeaderValue: failed to convert to HeaderValue: {}", e);
                            })
                            .unwrap_or(HeaderValue::from_static(""));
                        res.headers_mut().append(&self.header.key, header_value);
                        true
                    } else {
                        false
                    }
                },
                HeaderAppendAction::OverwriteIfExistsOrAdd => {
                    let mut formatter = self.header.value.clone();
                    formatter.with_context(&DownstreamResponseContext { response: &res, response_head_size: 0 });
                    let header_value = formatter
                        .into_header_value()
                        .inspect_err(|e| {
                            warn!("HeaderValue: failed to convert to HeaderValue: {}", e);
                        })
                        .unwrap_or(HeaderValue::from_static(""));
                    res.headers_mut().insert(&self.header.key, header_value);
                    true
                },
                HeaderAppendAction::OverwriteIfExists => {
                    if res.headers_mut().get(&self.header.key).is_some() {
                        let mut formatter = self.header.value.clone();
                        formatter.with_context(&DownstreamResponseContext { response: &res, response_head_size: 0 });
                        let header_value = formatter
                            .into_header_value()
                            .inspect_err(|e| {
                                warn!("HeaderValue: failed to convert to HeaderValue: {}", e);
                            })
                            .unwrap_or(HeaderValue::from_static(""));
                        res.headers_mut().insert(&self.header.key, header_value);
                        true
                    } else {
                        false
                    }
                },
            }
        }
    }
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, Deserialize, Serialize)]
pub enum HeaderAppendAction {
    AppendIfExistsOrAdd,
    AppendIfAbsent,
    OverwriteIfExistsOrAdd,
    OverwriteIfExists,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, Deserialize, Serialize)]
pub struct HeaderKeyValue {
    #[serde(with = "http_serde_ext::header_name")]
    pub key: HeaderName,
    pub value: HeaderFormatter,
}

impl TryFrom<(String, Vec<u8>)> for HeaderKeyValue {
    type Error = GenericError;
    fn try_from(value: (String, Vec<u8>)) -> Result<Self, Self::Error> {
        let key = HeaderName::from_str(&value.0).map_err(|e| {
            GenericError::from_msg_with_cause(format!("failed to parse \"{}\" as a HeaderName", value.0), e)
        })?;
        let value_str = String::from_utf8(value.1)
            .map_err(|e| GenericError::from_msg_with_cause(format!("failed to parse bytes as a utf"), e))?;

        let value = HeaderFormatter::try_new(&value_str).map_err(|e| {
            GenericError::from_msg_with_cause(format!("failed to parse \"{}\" as a HeaderFormatter", value_str), e)
        })?;

        Ok(Self { key, value })
    }
}
impl TryFrom<(String, String)> for HeaderKeyValue {
    type Error = GenericError;
    fn try_from(value: (String, String)) -> Result<Self, Self::Error> {
        let key = HeaderName::from_str(&value.0).map_err(|e| {
            GenericError::from_msg_with_cause(format!("failed to parse \"{}\" as a HeaderName", value.0), e)
        })?;

        let value = HeaderFormatter::try_new(&value.1).map_err(|e| {
            GenericError::from_msg_with_cause(format!("failed to parse \"{}\" as a HeaderFormatter", value.1), e)
        })?;

        Ok(Self { key, value })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::network_filters::http_connection_manager::header_modifer::HeaderKeyValue;
    use http::header::{COOKIE, LOCATION, USER_AGENT};

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

#[cfg(feature = "envoy-conversions")]
mod envoy_conversions {
    #![allow(deprecated)]
    use super::{HeaderAppendAction, HeaderKeyValue, HeaderValueOption};
    use crate::config::common::*;
    use orion_data_plane_api::envoy_data_plane_api::envoy::config::core::v3::{
        header_value_option::HeaderAppendAction as EnvoyHeaderAppendAction, HeaderValue as EnvoyHeaderValue,
        HeaderValueOption as EnvoyHeaderValueOption,
    };

    impl TryFrom<EnvoyHeaderValueOption> for HeaderValueOption {
        type Error = GenericError;
        fn try_from(value: EnvoyHeaderValueOption) -> Result<Self, Self::Error> {
            let EnvoyHeaderValueOption { header, append, append_action, keep_empty_value } = value;
            unsupported_field!(append)?;
            let header = convert_opt!(header)?;
            let append_action = HeaderAppendAction::try_from(append_action).with_node("append_action")?;
            Ok(Self { header, append_action, keep_empty_value })
        }
    }

    impl From<EnvoyHeaderAppendAction> for HeaderAppendAction {
        fn from(value: EnvoyHeaderAppendAction) -> Self {
            match value {
                EnvoyHeaderAppendAction::AppendIfExistsOrAdd => Self::AppendIfExistsOrAdd,
                EnvoyHeaderAppendAction::AddIfAbsent => Self::AppendIfAbsent,
                EnvoyHeaderAppendAction::OverwriteIfExists => Self::OverwriteIfExists,
                EnvoyHeaderAppendAction::OverwriteIfExistsOrAdd => Self::OverwriteIfExistsOrAdd,
            }
        }
    }

    impl TryFrom<i32> for HeaderAppendAction {
        type Error = GenericError;
        fn try_from(value: i32) -> Result<Self, Self::Error> {
            EnvoyHeaderAppendAction::from_i32(value)
                .ok_or(GenericError::unsupported_variant("[unknown header append action]"))
                .map(Self::from)
        }
    }

    impl TryFrom<EnvoyHeaderValue> for HeaderKeyValue {
        type Error = GenericError;
        fn try_from(value: EnvoyHeaderValue) -> Result<Self, Self::Error> {
            let EnvoyHeaderValue { key, value, raw_value } = value;
            match (value.is_used(), raw_value.is_used()) {
                (true, true) => {
                    Err(GenericError::from_msg(format!("both value ({value}) and raw_value ({raw_value:?}) were set"))
                        .with_node("value"))
                },
                (true, false) => Self::try_from((key, value)),
                (false, true) => Self::try_from((key, raw_value)),
                (false, false) => Err(GenericError::MissingField("value OR raw_value")),
            }
        }
    }
}
