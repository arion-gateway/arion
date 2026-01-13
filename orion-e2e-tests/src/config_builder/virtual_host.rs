// Copyright 2025 The kmesh Authors
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

use orion_data_plane_api::envoy_data_plane_api::envoy::config::{
    core::v3::{HeaderValue, HeaderValueOption},
    route::v3::{RetryPolicy, Route, VirtualHost as EnvoyVirtualHost},
};

#[derive(Debug, Clone)]
pub struct VirtualHostBuilder {
    proto: EnvoyVirtualHost,
}

impl VirtualHostBuilder {
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self { proto: EnvoyVirtualHost { name: name.into(), domains: vec!["*".into()], ..Default::default() } }
    }

    #[must_use]
    pub fn domains<I, S>(mut self, domains: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.proto.domains = domains.into_iter().map(Into::into).collect();
        self
    }

    #[must_use]
    pub fn domain(mut self, domain: impl Into<String>) -> Self {
        self.proto.domains.push(domain.into());
        self
    }

    #[must_use]
    pub fn route(mut self, route: impl Into<Route>) -> Self {
        self.proto.routes.push(route.into());
        self
    }

    #[must_use]
    pub fn routes<I, R>(mut self, routes: I) -> Self
    where
        I: IntoIterator<Item = R>,
        R: Into<Route>,
    {
        self.proto.routes.extend(routes.into_iter().map(Into::into));
        self
    }

    #[must_use]
    pub fn retry_policy(mut self, policy: impl Into<RetryPolicy>) -> Self {
        self.proto.retry_policy = Some(policy.into());
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
    pub fn remove_request_header(mut self, name: impl Into<String>) -> Self {
        self.proto.request_headers_to_remove.push(name.into());
        self
    }

    #[must_use]
    pub fn with_proto<F: FnOnce(&mut EnvoyVirtualHost)>(mut self, f: F) -> Self {
        f(&mut self.proto);
        self
    }

    #[must_use]
    pub fn build(self) -> EnvoyVirtualHost {
        self.proto
    }
}

impl From<VirtualHostBuilder> for EnvoyVirtualHost {
    fn from(builder: VirtualHostBuilder) -> Self {
        builder.build()
    }
}

pub type VirtualHost = EnvoyVirtualHost;
