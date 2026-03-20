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

use std::{
    net::SocketAddr,
    time::{Duration, SystemTime},
};

use crate::{
    operator::Operator,
    types::{ResponseFlags, ResponseFlagsLong, ResponseFlagsShort},
    StringType,
};
use ahash::AHasher;
use chrono::{DateTime, Datelike, Timelike, Utc};
use http::{uri::Authority, Request, Response};
use orion_http_header::{X_ENVOY_ORIGINAL_PATH, X_REQUEST_ID};
use orion_interner::StringInterner;
use smol_str::ToSmolStr;
use smol_str::{format_smolstr, SmolStr, SmolStrBuilder};
use uuid::Uuid;

pub trait Context {
    fn eval_part(&self, op: &Operator) -> StringType;
}

#[derive(Debug, Clone, Default)]
pub struct SocketAddrContext {
    pub downstream_local_addr: Option<SocketAddr>,
    pub downstream_peer_addr: Option<SocketAddr>,
    pub upstream_local_addr: Option<SocketAddr>,
    pub upstream_peer_addr: Option<SocketAddr>,
}

impl Context for SocketAddrContext {
    fn eval_part(&self, op: &Operator) -> StringType {
        match op {
            Operator::UpstreamHost | Operator::UpstreamRemoteAddress => {
                self.upstream_peer_addr.map_or(StringType::None, |addr| StringType::Smol(addr.to_smolstr()))
            },
            Operator::UpstreamRemoteAddressWithoutPort => {
                self.upstream_peer_addr.map_or(StringType::None, |addr| StringType::Smol(addr.ip().to_smolstr()))
            },
            Operator::UpstreamRemotePort => {
                self.upstream_peer_addr.map_or(StringType::None, |addr| StringType::Smol(addr.port().to_smolstr()))
            },
            Operator::UpstreamLocalAddress => {
                self.upstream_local_addr.map_or(StringType::None, |addr| StringType::Smol(addr.to_smolstr()))
            },
            Operator::UpstreamLocalAddressWithoutPort => {
                self.upstream_local_addr.map_or(StringType::None, |addr| StringType::Smol(addr.ip().to_smolstr()))
            },
            Operator::UpstreamLocalPort => {
                self.upstream_local_addr.map_or(StringType::None, |addr| StringType::Smol(addr.port().to_smolstr()))
            },
            Operator::DownstreamLocalAddress => {
                self.downstream_local_addr.map_or(StringType::None, |addr| StringType::Smol(addr.to_smolstr()))
            },
            Operator::DownstreamLocalAddressWithoutPort => {
                self.downstream_local_addr.map_or(StringType::None, |addr| StringType::Smol(addr.ip().to_smolstr()))
            },
            Operator::DownstreamLocalPort => {
                self.downstream_local_addr.map_or(StringType::None, |addr| StringType::Smol(addr.port().to_smolstr()))
            },

            Operator::DownstreamRemoteAddress => {
                self.downstream_peer_addr.map_or(StringType::None, |addr| StringType::Smol(addr.to_smolstr()))
            },

            Operator::DownstreamRemoteAddressWithoutPort => {
                self.downstream_peer_addr.map_or(StringType::None, |addr| StringType::Smol(addr.ip().to_smolstr()))
            },

            Operator::DownstreamRemotePort => {
                self.downstream_peer_addr.map_or(StringType::None, |addr| StringType::Smol(addr.port().to_smolstr()))
            },

            Operator::ConnectionId => {
                hash_connection(self.downstream_local_addr.as_ref(), self.downstream_peer_addr.as_ref(), &Protocol::Tcp)
            },
            Operator::UpstreamConnectionId => {
                hash_connection(self.upstream_local_addr.as_ref(), self.upstream_peer_addr.as_ref(), &Protocol::Tcp)
            },
            _ => StringType::None,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct TcpContext<'a> {
    pub socket_address: SocketAddrContext,
    pub cluster_name: &'a str,
}

impl Context for TcpContext<'_> {
    fn eval_part(&self, op: &Operator) -> StringType {
        match op {
            Operator::UpstreamCluster | Operator::UpstreamClusterRaw => {
                StringType::Smol(SmolStr::new(self.cluster_name))
            },
            _ => self.socket_address.eval_part(op),
        }
    }
}

#[allow(dead_code)]
#[derive(Clone, Debug, Hash)]
enum Protocol {
    Tcp,
    Udp,
}

#[inline]
fn hash_connection(local: Option<&SocketAddr>, peer: Option<&SocketAddr>, protocol: &Protocol) -> StringType {
    use std::hash::{Hash, Hasher};
    match (local, peer) {
        (Some(local), Some(peer)) => {
            let mut hasher = AHasher::default();
            local.hash(&mut hasher);
            peer.hash(&mut hasher);
            protocol.hash(&mut hasher);
            StringType::Smol(format_smolstr!("{:x}", hasher.finish()))
        },
        _ => StringType::None,
    }
}

#[derive(Debug, Clone, Default)]
pub struct UpstreamContext<'a> {
    pub authority: Option<&'a Authority>,
    pub cluster_name: Option<&'a str>,
    pub route_name: &'a str,
}

impl Context for UpstreamContext<'_> {
    fn eval_part(&self, op: &Operator) -> StringType {
        match op {
            Operator::UpstreamHost => {
                self.authority.map_or(StringType::None, |name| StringType::Smol(SmolStr::new(name)))
            },
            Operator::UpstreamCluster | Operator::UpstreamClusterRaw => {
                self.cluster_name.map_or(StringType::None, |cluster_name| StringType::Smol(SmolStr::new(cluster_name)))
            },
            Operator::UpstreamHostName => {
                self.authority.map_or(StringType::None, |auth| StringType::Smol(SmolStr::new(auth.as_str())))
            },
            Operator::UpstreamHostNameWithoutPort => {
                self.authority.map_or(StringType::None, |auth| StringType::Smol(SmolStr::new(auth.host())))
            },
            Operator::RouteName => StringType::Smol(SmolStr::new(self.route_name)),
            _ => StringType::None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct InitContext {
    pub start_time: SystemTime,
}

impl Context for InitContext {
    fn eval_part(&self, op: &Operator) -> StringType {
        match op {
            Operator::StartTime => StringType::Smol(format_system_time(self.start_time)),
            _ => StringType::None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct InitHttpContext<'a, T> {
    pub start_time: SystemTime,
    pub downstream_request: &'a Request<T>,
    pub request_head_size: usize,
    pub trace_id: Option<u128>,
    pub server_name: Option<&'a str>,
    pub socket_address: SocketAddrContext,
}

impl<T> Context for InitHttpContext<'_, T> {
    fn eval_part(&self, op: &Operator) -> StringType {
        match op {
            Operator::StartTime => StringType::Smol(format_system_time(self.start_time)),
            _ => DownstreamContext {
                request: self.downstream_request,
                trace_id: self.trace_id,
                request_head_size: self.request_head_size,
                server_name: self.server_name,
                socket_address: self.socket_address.clone(),
            }
            .eval_part(op),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct HttpRequestDurationContext {
    pub duration: Duration,
    pub tx_duration: Duration,
}

impl Context for HttpRequestDurationContext {
    fn eval_part(&self, op: &Operator) -> StringType {
        match op {
            Operator::RequestDuration => {
                let mut buffer = itoa::Buffer::new();
                StringType::Smol(SmolStr::new(buffer.format(self.duration.as_millis())))
            },
            Operator::RequestTxDuration => {
                let mut buffer = itoa::Buffer::new();
                StringType::Smol(SmolStr::new(buffer.format(self.tx_duration.as_millis())))
            },
            _ => StringType::None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct HttpResponseDurationContext {
    pub duration: Duration,
    pub tx_duration: Duration,
}

impl Context for HttpResponseDurationContext {
    fn eval_part(&self, op: &Operator) -> StringType {
        match op {
            Operator::ResponseDuration => {
                let mut buffer = itoa::Buffer::new();
                StringType::Smol(SmolStr::new(buffer.format(self.duration.as_millis())))
            },
            Operator::ResponseTxDuration => {
                let mut buffer = itoa::Buffer::new();
                StringType::Smol(SmolStr::new(buffer.format(self.tx_duration.as_millis())))
            },
            _ => StringType::None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct FinishContext {
    pub duration: Duration,
    pub bytes_received: u64,
    pub bytes_sent: u64,
    pub response_flags: ResponseFlags,
    pub upstream_failure: Option<&'static str>,
    pub response_code_details: Option<&'static str>,
    pub connection_termination_details: Option<&'static str>,
}

impl Context for FinishContext {
    fn eval_part(&self, op: &Operator) -> StringType {
        match op {
            Operator::ResponseFlags => StringType::Smol(ResponseFlagsShort(&self.response_flags).to_smolstr()),
            Operator::ResponseFlagsLong => StringType::Smol(ResponseFlagsLong(&self.response_flags).to_smolstr()),
            Operator::Duration => {
                let mut buffer = itoa::Buffer::new();
                StringType::Smol(SmolStr::new(buffer.format(self.duration.as_millis())))
            },
            Operator::BytesReceived => {
                let mut buffer = itoa::Buffer::new();
                StringType::Smol(SmolStr::new(buffer.format(self.bytes_received)))
            },
            Operator::BytesSent => {
                let mut buffer = itoa::Buffer::new();
                StringType::Smol(SmolStr::new(buffer.format(self.bytes_sent)))
            },
            Operator::UpstreamTransportFailureReason => {
                self.upstream_failure.map_or(StringType::None, |msg| StringType::Smol(SmolStr::new_static(msg)))
            },
            Operator::ResponseCodeDetails => {
                self.response_code_details.map_or(StringType::None, |msg| StringType::Smol(SmolStr::new_static(msg)))
            },
            Operator::ConnectionTerminationDetails => self
                .connection_termination_details
                .map_or(StringType::None, |msg| StringType::Smol(SmolStr::new_static(msg))),
            _ => StringType::None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct WireContext {
    pub wire_bytes_received: u64,
    pub wire_bytes_sent: u64,
}

impl Context for WireContext {
    fn eval_part(&self, op: &Operator) -> StringType {
        match op {
            Operator::DownstreamWireBytesReceived => {
                let mut buffer = itoa::Buffer::new();
                StringType::Smol(SmolStr::new(buffer.format(self.wire_bytes_received)))
            },
            Operator::DownstreamWireBytesSent => {
                let mut buffer = itoa::Buffer::new();
                StringType::Smol(SmolStr::new(buffer.format(self.wire_bytes_sent)))
            },
            _ => StringType::None,
        }
    }
}

pub struct DownstreamContext<'a, T> {
    pub request: &'a Request<T>,
    pub request_head_size: usize,
    pub trace_id: Option<u128>,
    pub server_name: Option<&'a str>,
    pub socket_address: SocketAddrContext,
}

pub struct DownstreamResponseContext<'a, T> {
    pub response: &'a Response<T>,
    pub response_head_size: usize,
}

pub struct UpstreamRequestContext<'a, T>(pub &'a Request<T>);
pub struct UpstreamResponseContext<'a, T>(pub &'a Response<T>);

impl<T> Context for DownstreamContext<'_, T> {
    fn eval_part(&self, op: &Operator) -> StringType {
        match op {
            Operator::RequestHeadersBytes => {
                let mut buffer = itoa::Buffer::new();
                StringType::Smol(SmolStr::new(buffer.format(self.request_head_size)))
            },
            Operator::RequestPath => StringType::Smol(SmolStr::new(self.request.uri().path())),
            Operator::RequestOriginalPathOrPath => {
                let path_str = self
                    .request
                    .headers()
                    .get(X_ENVOY_ORIGINAL_PATH)
                    .and_then(|p| p.to_str().ok())
                    .unwrap_or_else(|| self.request.uri().path());

                StringType::Smol(SmolStr::new(path_str))
            },
            Operator::RequestAuthority => {
                if let Some(a) = extract_authority_from_request(self.request) {
                    StringType::Smol(SmolStr::new(a))
                } else {
                    StringType::None
                }
            },
            Operator::RequestMethod => StringType::Smol(SmolStr::new(self.request.method().as_str())),
            Operator::RequestScheme => {
                if let Some(s) = self.request.uri().scheme() {
                    StringType::Smol(SmolStr::new(s.as_str()))
                } else {
                    StringType::None
                }
            },
            Operator::Request(h) => self
                .request
                .headers()
                .get(h.0.as_str())
                .map_or(StringType::None, |hv| StringType::Bytes(hv.as_bytes().into())),
            Operator::TraceId => self
                .trace_id
                .map_or(StringType::None, |trace_id| StringType::Smol(format_smolstr!("{:032x}", trace_id))),
            Operator::Protocol => StringType::Smol(SmolStr::new_static(self.request.version().to_static_str())),
            Operator::RequestedServerName => {
                self.server_name.map_or(StringType::None, |sni| StringType::Smol(SmolStr::new(sni)))
            },
            _ => self.socket_address.eval_part(op),
        }
    }
}

impl<T> Context for UpstreamRequestContext<'_, T> {
    fn eval_part(&self, op: &Operator) -> StringType {
        match op {
            Operator::UpstreamProtocol => StringType::Smol(SmolStr::new_static(self.0.version().to_static_str())),
            Operator::UniqueId => {
                let uuid = self
                    .0
                    .headers()
                    .get(X_REQUEST_ID)
                    .and_then(|id| id.to_str().ok())
                    .filter(|s| Uuid::parse_str(s).is_ok())
                    .map(SmolStr::new);
                match uuid {
                    Some(value) => StringType::Smol(value),
                    None => StringType::None,
                }
            },
            _ => StringType::None,
        }
    }
}

impl<T> Context for DownstreamResponseContext<'_, T> {
    fn eval_part(&self, op: &Operator) -> StringType {
        match op {
            Operator::ResponseHeadersBytes => {
                let mut buffer = itoa::Buffer::new();
                StringType::Smol(SmolStr::new(buffer.format(self.response_head_size)))
            },
            Operator::ResponseStatus | Operator::ResponseCode => {
                StringType::Smol(SmolStr::new_inline(self.response.status().as_str()))
            },
            Operator::Response(header_name) => self
                .response
                .headers()
                .get(header_name.0.as_str())
                .map_or(StringType::None, |hv| StringType::Bytes(hv.as_bytes().into())),
            _ => StringType::None,
        }
    }
}

pub fn extract_authority_from_request<T>(request: &Request<T>) -> Option<&str> {
    if let Some(authority) = request.uri().authority() {
        return Some(authority.as_str());
    }
    if let Some(host_header_value) = request.headers().get(http::header::HOST) {
        return host_header_value.to_str().ok();
    }

    None
}

const TWO_DIGITS: [&str; 100] = [
    "00", "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14", "15", "16", "17", "18",
    "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31", "32", "33", "34", "35", "36", "37",
    "38", "39", "40", "41", "42", "43", "44", "45", "46", "47", "48", "49", "50", "51", "52", "53", "54", "55", "56",
    "57", "58", "59", "60", "61", "62", "63", "64", "65", "66", "67", "68", "69", "70", "71", "72", "73", "74", "75",
    "76", "77", "78", "79", "80", "81", "82", "83", "84", "85", "86", "87", "88", "89", "90", "91", "92", "93", "94",
    "95", "96", "97", "98", "99",
];

pub fn format_system_time(time: SystemTime) -> SmolStr {
    let datetime: DateTime<Utc> = time.into();

    let mut builder = SmolStrBuilder::new();
    let mut buffer = itoa::Buffer::new();

    builder.push_str(buffer.format(datetime.year()));
    builder.push('-');
    builder.push_str(unsafe { TWO_DIGITS.get_unchecked(datetime.month() as usize) });
    builder.push('-');
    builder.push_str(unsafe { TWO_DIGITS.get_unchecked(datetime.day() as usize) });
    builder.push('T');
    builder.push_str(unsafe { TWO_DIGITS.get_unchecked(datetime.hour() as usize) });
    builder.push(':');
    builder.push_str(unsafe { TWO_DIGITS.get_unchecked(datetime.minute() as usize) });
    builder.push(':');
    builder.push_str(unsafe { TWO_DIGITS.get_unchecked(datetime.second() as usize) });
    builder.push(':');
    builder.push_str(buffer.format(datetime.nanosecond() / 1_000_000));
    builder.push('Z');

    builder.finish()
}

#[cfg(any())]
pub fn format_system_time_heapless(time: SystemTime) -> heapless::String<24> {
    let datetime: DateTime<Utc> = time.into();
    let mut rfc3999: heapless::String<24> = heapless::String::new();
    let mut buffer = itoa::Buffer::new();
    _ = rfc3999.push_str(buffer.format(datetime.year()));
    _ = rfc3999.push('-');
    _ = rfc3999.push_str(unsafe { TWO_DIGITS.get_unchecked(datetime.month() as usize) });
    _ = rfc3999.push('-');
    _ = rfc3999.push_str(unsafe { TWO_DIGITS.get_unchecked(datetime.day() as usize) });
    _ = rfc3999.push('T');
    _ = rfc3999.push_str(unsafe { TWO_DIGITS.get_unchecked(datetime.hour() as usize) });
    _ = rfc3999.push(':');
    _ = rfc3999.push_str(unsafe { TWO_DIGITS.get_unchecked(datetime.minute() as usize) });
    _ = rfc3999.push(':');
    _ = rfc3999.push_str(unsafe { TWO_DIGITS.get_unchecked(datetime.second() as usize) });
    _ = rfc3999.push(':');
    _ = rfc3999.push_str(buffer.format(datetime.nanosecond() / 1_000_000));
    _ = rfc3999.push('Z');
    rfc3999
}

#[cfg(any())]
pub fn format_system_time_compact(time: SystemTime) -> SmolStr {
    let datetime: DateTime<Utc> = time.into();
    let mut buffer = itoa::Buffer::new();
    let mut rfc3999 = SmolStr::default();

    _ = rfc3999.push_str(buffer.format(datetime.year()));
    _ = rfc3999.push('-');
    _ = rfc3999.push_str(unsafe { TWO_DIGITS.get_unchecked(datetime.month() as usize) });
    _ = rfc3999.push('-');
    _ = rfc3999.push_str(unsafe { TWO_DIGITS.get_unchecked(datetime.day() as usize) });
    _ = rfc3999.push('T');
    _ = rfc3999.push_str(unsafe { TWO_DIGITS.get_unchecked(datetime.hour() as usize) });
    _ = rfc3999.push(':');
    _ = rfc3999.push_str(unsafe { TWO_DIGITS.get_unchecked(datetime.minute() as usize) });
    _ = rfc3999.push(':');
    _ = rfc3999.push_str(unsafe { TWO_DIGITS.get_unchecked(datetime.second() as usize) });
    _ = rfc3999.push(':');
    _ = rfc3999.push_str(buffer.format(datetime.nanosecond() / 1_000_000));
    _ = rfc3999.push('Z');
    rfc3999
}
