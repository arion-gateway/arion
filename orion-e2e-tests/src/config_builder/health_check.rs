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

use std::ops::Range;
use std::time::Duration;

use orion_data_plane_api::envoy_data_plane_api::{
    envoy::{
        config::core::v3::{
            health_check::{
                payload::Payload as ProtoPayloadInner, GrpcHealthCheck as ProtoGrpcHealthCheck, HealthChecker,
                HttpHealthCheck as ProtoHttpHealthCheck, Payload as ProtoPayload,
                TcpHealthCheck as ProtoTcpHealthCheck,
            },
            HealthCheck as ProtoHealthCheck, RequestMethod,
        },
        r#type::v3::Int64Range,
    },
    google::protobuf::UInt32Value,
};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum HealthCheckMethod {
    #[default]
    Get,
    Head,
    Post,
    Put,
    Delete,
    Options,
    Trace,
    Patch,
}

impl HealthCheckMethod {
    fn to_proto(self) -> i32 {
        match self {
            Self::Get => RequestMethod::Get.into(),
            Self::Head => RequestMethod::Head.into(),
            Self::Post => RequestMethod::Post.into(),
            Self::Put => RequestMethod::Put.into(),
            Self::Delete => RequestMethod::Delete.into(),
            Self::Options => RequestMethod::Options.into(),
            Self::Trace => RequestMethod::Trace.into(),
            Self::Patch => RequestMethod::Patch.into(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct HttpHealthCheckBuilder {
    path: String,
    interval: Duration,
    timeout: Duration,
    healthy_threshold: u32,
    unhealthy_threshold: u32,
    method: HealthCheckMethod,
    host: Option<String>,
    expected_statuses: Vec<Range<u16>>,
}

impl HttpHealthCheckBuilder {
    #[must_use]
    pub fn new(path: impl Into<String>, interval: Duration, timeout: Duration) -> Self {
        Self {
            path: path.into(),
            interval,
            timeout,
            healthy_threshold: 1,
            unhealthy_threshold: 2,
            method: HealthCheckMethod::default(),
            host: None,
            expected_statuses: vec![],
        }
    }

    #[must_use]
    pub fn healthy_threshold(mut self, threshold: u32) -> Self {
        self.healthy_threshold = threshold;
        self
    }

    #[must_use]
    pub fn unhealthy_threshold(mut self, threshold: u32) -> Self {
        self.unhealthy_threshold = threshold;
        self
    }

    #[must_use]
    pub fn method(mut self, method: HealthCheckMethod) -> Self {
        self.method = method;
        self
    }

    #[must_use]
    pub fn get(self) -> Self {
        self.method(HealthCheckMethod::Get)
    }

    #[must_use]
    pub fn head(self) -> Self {
        self.method(HealthCheckMethod::Head)
    }

    #[must_use]
    pub fn host(mut self, host: impl Into<String>) -> Self {
        self.host = Some(host.into());
        self
    }

    #[must_use]
    pub fn expected_statuses(mut self, statuses: Vec<Range<u16>>) -> Self {
        self.expected_statuses = statuses;
        self
    }

    #[must_use]
    pub fn accept_2xx(mut self) -> Self {
        self.expected_statuses = vec![Range { start: 200, end: 300 }];
        self
    }

    #[must_use]
    pub fn build(self) -> ProtoHealthCheck {
        let expected_statuses: Vec<Int64Range> = self
            .expected_statuses
            .into_iter()
            .map(|range| Int64Range { start: i64::from(range.start), end: i64::from(range.end) })
            .collect();

        let http_health_check = ProtoHttpHealthCheck {
            path: self.path,
            host: self.host.unwrap_or_default(),
            method: self.method.to_proto(),
            expected_statuses,
            ..Default::default()
        };

        ProtoHealthCheck {
            timeout: Some(super::duration_to_proto(self.timeout)),
            interval: Some(super::duration_to_proto(self.interval)),
            unhealthy_threshold: Some(UInt32Value { value: self.unhealthy_threshold }),
            healthy_threshold: Some(UInt32Value { value: self.healthy_threshold }),
            health_checker: Some(HealthChecker::HttpHealthCheck(http_health_check)),
            ..Default::default()
        }
    }
}

impl From<HttpHealthCheckBuilder> for ProtoHealthCheck {
    fn from(builder: HttpHealthCheckBuilder) -> Self {
        builder.build()
    }
}

#[derive(Debug, Clone, Default)]
pub struct TcpHealthCheckBuilder {
    interval: Duration,
    timeout: Duration,
    healthy_threshold: u32,
    unhealthy_threshold: u32,
    send: Option<Vec<u8>>,
    receive: Vec<Vec<u8>>,
}

impl TcpHealthCheckBuilder {
    #[must_use]
    pub fn new(interval: Duration, timeout: Duration) -> Self {
        Self { interval, timeout, healthy_threshold: 1, unhealthy_threshold: 2, send: None, receive: vec![] }
    }

    #[must_use]
    pub fn healthy_threshold(mut self, threshold: u32) -> Self {
        self.healthy_threshold = threshold;
        self
    }

    #[must_use]
    pub fn unhealthy_threshold(mut self, threshold: u32) -> Self {
        self.unhealthy_threshold = threshold;
        self
    }

    #[must_use]
    pub fn send(mut self, payload: impl Into<Vec<u8>>) -> Self {
        self.send = Some(payload.into());
        self
    }

    #[must_use]
    pub fn receive(mut self, pattern: impl Into<Vec<u8>>) -> Self {
        self.receive.push(pattern.into());
        self
    }

    #[must_use]
    pub fn build(self) -> ProtoHealthCheck {
        let send = self.send.map(|data| ProtoPayload { payload: Some(ProtoPayloadInner::Binary(data)) });

        let receive: Vec<ProtoPayload> = self
            .receive
            .into_iter()
            .map(|data| ProtoPayload { payload: Some(ProtoPayloadInner::Binary(data)) })
            .collect();

        let tcp_health_check = ProtoTcpHealthCheck { send, receive, ..Default::default() };

        ProtoHealthCheck {
            timeout: Some(super::duration_to_proto(self.timeout)),
            interval: Some(super::duration_to_proto(self.interval)),
            unhealthy_threshold: Some(UInt32Value { value: self.unhealthy_threshold }),
            healthy_threshold: Some(UInt32Value { value: self.healthy_threshold }),
            health_checker: Some(HealthChecker::TcpHealthCheck(tcp_health_check)),
            ..Default::default()
        }
    }
}

impl From<TcpHealthCheckBuilder> for ProtoHealthCheck {
    fn from(builder: TcpHealthCheckBuilder) -> Self {
        builder.build()
    }
}

#[derive(Debug, Clone, Default)]
pub struct GrpcHealthCheckBuilder {
    interval: Duration,
    timeout: Duration,
    healthy_threshold: u32,
    unhealthy_threshold: u32,
    service_name: String,
    authority: Option<String>,
}

impl GrpcHealthCheckBuilder {
    #[must_use]
    pub fn new(interval: Duration, timeout: Duration) -> Self {
        Self {
            interval,
            timeout,
            healthy_threshold: 1,
            unhealthy_threshold: 2,
            service_name: String::new(),
            authority: None,
        }
    }

    #[must_use]
    pub fn healthy_threshold(mut self, threshold: u32) -> Self {
        self.healthy_threshold = threshold;
        self
    }

    #[must_use]
    pub fn unhealthy_threshold(mut self, threshold: u32) -> Self {
        self.unhealthy_threshold = threshold;
        self
    }

    #[must_use]
    pub fn service_name(mut self, name: impl Into<String>) -> Self {
        self.service_name = name.into();
        self
    }

    #[must_use]
    pub fn authority(mut self, authority: impl Into<String>) -> Self {
        self.authority = Some(authority.into());
        self
    }

    #[must_use]
    pub fn build(self) -> ProtoHealthCheck {
        let grpc_health_check = ProtoGrpcHealthCheck {
            service_name: self.service_name,
            authority: self.authority.unwrap_or_default(),
            ..Default::default()
        };

        ProtoHealthCheck {
            timeout: Some(super::duration_to_proto(self.timeout)),
            interval: Some(super::duration_to_proto(self.interval)),
            unhealthy_threshold: Some(UInt32Value { value: self.unhealthy_threshold }),
            healthy_threshold: Some(UInt32Value { value: self.healthy_threshold }),
            health_checker: Some(HealthChecker::GrpcHealthCheck(grpc_health_check)),
            ..Default::default()
        }
    }
}

impl From<GrpcHealthCheckBuilder> for ProtoHealthCheck {
    fn from(builder: GrpcHealthCheckBuilder) -> Self {
        builder.build()
    }
}
