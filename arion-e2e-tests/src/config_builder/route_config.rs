// Copyright 2025 The kmesh Authors
// Copyright 2026 The arion-gateway Authors
//
// Modified by arion-gateway Authors.
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

use arion_data_plane_api::envoy_data_plane_api::envoy::config::{
    core::v3::{header_value_option::HeaderAppendAction, HeaderValue, HeaderValueOption},
    route::v3::{RouteConfiguration as EnvoyRouteConfiguration, VirtualHost},
};

#[derive(Debug, Clone)]
pub struct RouteConfigBuilder {
    proto: EnvoyRouteConfiguration,
}

impl RouteConfigBuilder {
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self { proto: EnvoyRouteConfiguration { name: name.into(), ..Default::default() } }
    }

    #[must_use]
    pub fn virtual_host(mut self, vhost: impl Into<VirtualHost>) -> Self {
        self.proto.virtual_hosts.push(vhost.into());
        self
    }

    #[must_use]
    pub fn virtual_hosts<I, V>(mut self, vhosts: I) -> Self
    where
        I: IntoIterator<Item = V>,
        V: Into<VirtualHost>,
    {
        self.proto.virtual_hosts.extend(vhosts.into_iter().map(Into::into));
        self
    }

    #[must_use]
    pub fn with_proto<F: FnOnce(&mut EnvoyRouteConfiguration)>(mut self, f: F) -> Self {
        f(&mut self.proto);
        self
    }

    #[must_use]
    pub fn add_request_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.proto.request_headers_to_add.push(HeaderValueOption {
            header: Some(HeaderValue { key: name.into(), value: value.into(), ..Default::default() }),
            ..Default::default()
        });
        self
    }

    #[must_use]
    pub fn add_request_header_if_absent(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.proto.request_headers_to_add.push(HeaderValueOption {
            header: Some(HeaderValue { key: name.into(), value: value.into(), ..Default::default() }),
            append_action: HeaderAppendAction::AddIfAbsent as i32,
            ..Default::default()
        });
        self
    }

    #[must_use]
    pub fn overwrite_request_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.proto.request_headers_to_add.push(HeaderValueOption {
            header: Some(HeaderValue { key: name.into(), value: value.into(), ..Default::default() }),
            append_action: HeaderAppendAction::OverwriteIfExistsOrAdd as i32,
            ..Default::default()
        });
        self
    }

    #[must_use]
    pub fn remove_request_header(mut self, name: impl Into<String>) -> Self {
        self.proto.request_headers_to_remove.push(name.into());
        self
    }

    #[must_use]
    pub fn add_response_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.proto.response_headers_to_add.push(HeaderValueOption {
            header: Some(HeaderValue { key: name.into(), value: value.into(), ..Default::default() }),
            ..Default::default()
        });
        self
    }

    #[must_use]
    pub fn add_response_header_if_absent(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.proto.response_headers_to_add.push(HeaderValueOption {
            header: Some(HeaderValue { key: name.into(), value: value.into(), ..Default::default() }),
            append_action: HeaderAppendAction::AddIfAbsent as i32,
            ..Default::default()
        });
        self
    }

    #[must_use]
    pub fn overwrite_response_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.proto.response_headers_to_add.push(HeaderValueOption {
            header: Some(HeaderValue { key: name.into(), value: value.into(), ..Default::default() }),
            append_action: HeaderAppendAction::OverwriteIfExistsOrAdd as i32,
            ..Default::default()
        });
        self
    }

    #[must_use]
    pub fn remove_response_header(mut self, name: impl Into<String>) -> Self {
        self.proto.response_headers_to_remove.push(name.into());
        self
    }

    #[must_use]
    pub fn most_specific_header_mutations_wins(mut self, value: bool) -> Self {
        self.proto.most_specific_header_mutations_wins = value;
        self
    }

    #[must_use]
    pub fn build(self) -> EnvoyRouteConfiguration {
        self.proto
    }
}

impl From<RouteConfigBuilder> for EnvoyRouteConfiguration {
    fn from(builder: RouteConfigBuilder) -> Self {
        builder.build()
    }
}

pub type RouteConfig = EnvoyRouteConfiguration;
