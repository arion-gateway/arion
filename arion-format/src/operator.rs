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

use bitflags::bitflags;
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
    pub struct Category : u8 {
        const OPERATOR = 1 << 0;
        const ARGUMENT = 1 << 1;
        const UNSUPPORTED = 1 << 2;
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
pub struct HeaderName(pub SmolStr);

#[derive(Clone, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
pub enum Operator {
    RequestDuration,
    RequestHeadersBytes,
    RequestTxDuration,
    ResponseDuration,
    ResponseTxDuration,
    DownstreamHandshakeDuration,
    RoundtripDuration,
    BytesReceived,
    BytesRetransmitted,
    PacketsRetransmitted,
    UpstreamWireBytesReceived,
    UpstreamHeaderBytesReceived,
    DownstreamWireBytesReceived,
    DownstreamHeaderBytesReceived,
    Protocol,
    UpstreamProtocol,
    ResponseCode,
    ResponseCodeDetails,
    ResponseHeadersBytes,
    ConnectionTerminationDetails,
    BytesSent,
    UpstreamWireBytesSent,
    UpstreamHeaderBytesSent,
    DownstreamWireBytesSent,
    DownstreamHeaderBytesSent,
    Duration,
    CommonDuration,
    CustomFlags,
    ResponseFlags,
    ResponseFlagsLong,
    UpstreamHostName,
    UpstreamHostNameWithoutPort,
    UpstreamHost,
    UpstreamConnectionId,
    UpstreamCluster,
    UpstreamClusterRaw,
    UpstreamLocalAddress,
    UpstreamLocalAddressWithoutPort,
    UpstreamLocalPort,
    UpstreamRemoteAddress,
    UpstreamRemoteAddressWithoutPort,
    UpstreamRemotePort,
    UpstreamRequestAttemptCount,
    UpstreamTlsCipher,
    UpstreamTlsVersion,
    UpstreamTlsSessionId,
    UpstreamPeerIssuer,
    UpstreamPeerCert,
    UpstreamPeerSubject,
    DownstreamLocalAddress,
    DownstreamDirectLocalAddress,
    DownstreamLocalAddressWithoutPort,
    DownstreamDirectLocalAddressWithoutPort,
    DownstreamLocalPort,
    DownstreamDirectLocalPort,
    DownstreamRemoteAddress,
    DownstreamRemoteAddressWithoutPort,
    DownstreamRemotePort,
    DownstreamDirectRemoteAddress,
    DownstreamDirectRemoteAddressWithoutPort,
    DownstreamDirectRemotePort,
    ConnectionId,
    RequestedServerName,
    RouteName,
    UpstreamPeerUriSan,
    UpstreamPeerDnsSan,
    UpstreamPeerIpSan,
    UpstreamLocalUriSan,
    UpstreamLocalDnsSan,
    UpstreamLocalIpSan,
    DownstreamPeerUriSan,
    DownstreamPeerDnsSan,
    DownstreamPeerIpSan,
    DownstreamPeerEmailSan,
    DownstreamPeerOthernameSan,
    DownstreamLocalUriSan,
    DownstreamLocalDnsSan,
    DownstreamLocalIpSan,
    DownstreamLocalEmailSan,
    DownstreamLocalOthernameSan,
    DownstreamPeerSubject,
    DownstreamLocalSubject,
    DownstreamTlsSessionId,
    DownstreamTlsCipher,
    DownstreamTlsVersion,
    DownstreamPeerFingerprint256,
    DownstreamPeerFingerprint1,
    DownstreamPeerSerial,
    DownstreamPeerChainFingerprints256,
    DownstreamPeerChainFingerprints1,
    DownstreamPeerChainSerials,
    DownstreamPeerIssuer,
    DownstreamPeerCert,
    DownstreamTransportFailureReason,
    UpstreamTransportFailureReason,
    Hostname,
    FilterChainName,
    VirtualClusterName,
    TlsJa3Fingerprint,
    UniqueId,
    StreamId,
    TraceId,
    StartTime,
    StartTimeLocal,
    EmitTime,
    EmitTimeLocal,
    DynamicMetadata,
    ClusterMetadata,
    UpstreamMetadata,
    FilterState,
    UpstreamFilterState,
    DownstreamPeerCertVStart,
    DownstreamPeerCertVEnd,
    UpstreamPeerCertVStart,
    UpstreamPeerCertVEnd,
    Environment,
    UpstreamConnectionPoolReadyDuration,
    RequestScheme,
    RequestMethod,
    RequestPath,
    RequestOriginalPathOrPath,
    RequestAuthority,
    Request(HeaderName),
    ResponseStatus,
    Response(HeaderName),
}

#[derive(Clone, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
pub enum ReqArgument {
    Scheme,
    Method,
    Path,
    OriginalPathOrPath,
    Authority,
    Header(HeaderName),
}

#[derive(Clone, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
pub enum RespArgument {
    Status,
    Header(HeaderName),
}
