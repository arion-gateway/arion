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

use std::sync::LazyLock;

use crate::{
    operator::{Category, HeaderName, Operator, ReqArgument, RespArgument},
    FormatError, Grammar, Template, CUSTOM_OPERATORS,
};
use ptrie::Trie;
use smol_str::SmolStr;

macro_rules! trie_mapstr {
    ($trie:expr, $lit:literal, $tok:expr) => {
        $trie.insert($lit.bytes(), ($tok, Category::OPERATOR, $lit.len(), false));
    };
    ($trie:expr, $lit:literal, $tok:expr, $arg:expr ) => {
        $trie.insert($lit.bytes(), ($tok, Category::OPERATOR, $lit.len(), $arg));
    };
}

macro_rules! trie_mapstr_unsupported {
    ($trie:expr, $lit:literal, $tok:expr) => {
        $trie.insert($lit.bytes(), ($tok, Category::UNSUPPORTED, $lit.len(), false));
    };
}

macro_rules! trie_mapstr_argument {
    ($trie:expr, $lit:literal, $tok:expr) => {
        $trie.insert($lit.bytes(), ($tok, Category::ARGUMENT, $lit.len(), false));
    };
}

static ENVOY_REQ_ARGS: LazyLock<Trie<u8, (ReqArgument, Category, usize, bool)>> = LazyLock::new(|| {
    let mut trie = Trie::new();
    trie_mapstr_argument!(trie, ":SCHEME", ReqArgument::Scheme);
    trie_mapstr_argument!(trie, ":METHOD", ReqArgument::Method);
    trie_mapstr_argument!(trie, ":PATH", ReqArgument::Path);
    trie_mapstr_argument!(trie, ":AUTHORITY", ReqArgument::Authority);
    trie_mapstr_argument!(trie, "X-ENVOY-ORIGINAL-PATH?:PATH", ReqArgument::OriginalPathOrPath);
    trie
});

static ENVOY_RESP_ARGS: LazyLock<Trie<u8, (RespArgument, Category, usize, bool)>> = LazyLock::new(|| {
    let mut trie = Trie::new();
    trie_mapstr_argument!(trie, ":STATUS", RespArgument::Status);
    trie
});

static ENVOY_PATTERNS: LazyLock<Trie<u8, (Operator, Category, usize, bool)>> = LazyLock::new(|| {
    let mut trie = Trie::new();
    trie_mapstr!(trie, "REQUEST_DURATION", Operator::RequestDuration);
    trie_mapstr!(trie, "REQUEST_TX_DURATION", Operator::RequestTxDuration);
    trie_mapstr!(trie, "RESPONSE_DURATION", Operator::ResponseDuration);
    trie_mapstr!(trie, "TIME_TO_FIRST_BYTE", Operator::ResponseDuration); // alias for RESPONSE_DURATION
    trie_mapstr!(trie, "RESPONSE_TX_DURATION", Operator::ResponseTxDuration);
    trie_mapstr_unsupported!(trie, "DOWNSTREAM_HANDSHAKE_DURATION", Operator::DownstreamHandshakeDuration);
    trie_mapstr_unsupported!(trie, "ROUNDTRIP_DURATION", Operator::RoundtripDuration);
    trie_mapstr!(trie, "BYTES_RECEIVED", Operator::BytesReceived);
    trie_mapstr_unsupported!(trie, "BYTES_RETRANSMITTED", Operator::BytesRetransmitted);
    trie_mapstr_unsupported!(trie, "PACKETS_RETRANSMITTED", Operator::PacketsRetransmitted);
    trie_mapstr_unsupported!(trie, "UPSTREAM_WIRE_BYTES_RECEIVED", Operator::UpstreamWireBytesReceived);
    trie_mapstr_unsupported!(trie, "UPSTREAM_HEADER_BYTES_RECEIVED", Operator::UpstreamHeaderBytesReceived);
    trie_mapstr!(trie, "DOWNSTREAM_WIRE_BYTES_RECEIVED", Operator::DownstreamWireBytesReceived);
    trie_mapstr_unsupported!(trie, "DOWNSTREAM_HEADER_BYTES_RECEIVED", Operator::DownstreamHeaderBytesReceived);
    trie_mapstr!(trie, "PROTOCOL", Operator::Protocol);
    trie_mapstr!(trie, "UPSTREAM_PROTOCOL", Operator::UpstreamProtocol);
    trie_mapstr!(trie, "RESPONSE_CODE", Operator::ResponseCode);
    trie_mapstr!(trie, "RESPONSE_CODE_DETAILS", Operator::ResponseCodeDetails);
    trie_mapstr!(trie, "CONNECTION_TERMINATION_DETAILS", Operator::ConnectionTerminationDetails);
    trie_mapstr!(trie, "BYTES_SENT", Operator::BytesSent);
    trie_mapstr_unsupported!(trie, "UPSTREAM_WIRE_BYTES_SENT", Operator::UpstreamWireBytesSent);
    trie_mapstr_unsupported!(trie, "UPSTREAM_HEADER_BYTES_SENT", Operator::UpstreamHeaderBytesSent);
    trie_mapstr!(trie, "DOWNSTREAM_WIRE_BYTES_SENT", Operator::DownstreamWireBytesSent);
    trie_mapstr_unsupported!(trie, "DOWNSTREAM_HEADER_BYTES_SENT", Operator::DownstreamHeaderBytesSent);
    trie_mapstr!(trie, "DURATION", Operator::Duration);
    trie_mapstr_unsupported!(trie, "COMMON_DURATION", Operator::CommonDuration);
    trie_mapstr_unsupported!(trie, "CUSTOM_FLAGS", Operator::CustomFlags);
    trie_mapstr!(trie, "RESPONSE_FLAGS", Operator::ResponseFlags);
    trie_mapstr!(trie, "RESPONSE_FLAGS_LONG", Operator::ResponseFlagsLong);
    trie_mapstr!(trie, "UPSTREAM_HOST_NAME", Operator::UpstreamHostName);
    trie_mapstr!(trie, "UPSTREAM_HOST_NAME_WITHOUT_PORT", Operator::UpstreamHostNameWithoutPort);
    trie_mapstr!(trie, "UPSTREAM_HOST", Operator::UpstreamHost);
    trie_mapstr!(trie, "UPSTREAM_CONNECTION_ID", Operator::UpstreamConnectionId);
    trie_mapstr!(trie, "UPSTREAM_CLUSTER", Operator::UpstreamCluster);
    trie_mapstr!(trie, "UPSTREAM_CLUSTER_RAW", Operator::UpstreamClusterRaw);
    trie_mapstr!(trie, "UPSTREAM_LOCAL_ADDRESS", Operator::UpstreamLocalAddress);
    trie_mapstr!(trie, "UPSTREAM_LOCAL_ADDRESS_WITHOUT_PORT", Operator::UpstreamLocalAddressWithoutPort);
    trie_mapstr!(trie, "UPSTREAM_LOCAL_PORT", Operator::UpstreamLocalPort);
    trie_mapstr!(trie, "UPSTREAM_REMOTE_ADDRESS", Operator::UpstreamRemoteAddress);
    trie_mapstr!(trie, "UPSTREAM_REMOTE_ADDRESS_WITHOUT_PORT", Operator::UpstreamRemoteAddressWithoutPort);
    trie_mapstr!(trie, "UPSTREAM_REMOTE_PORT", Operator::UpstreamRemotePort);
    trie_mapstr!(trie, "DOWNSTREAM_LOCAL_ADDRESS", Operator::DownstreamLocalAddress);
    trie_mapstr!(trie, "DOWNSTREAM_LOCAL_ADDRESS_WITHOUT_PORT", Operator::DownstreamLocalAddressWithoutPort);
    trie_mapstr!(trie, "DOWNSTREAM_LOCAL_PORT", Operator::DownstreamLocalPort);
    trie_mapstr!(trie, "DOWNSTREAM_REMOTE_ADDRESS", Operator::DownstreamRemoteAddress);
    trie_mapstr!(trie, "DOWNSTREAM_REMOTE_ADDRESS_WITHOUT_PORT", Operator::DownstreamRemoteAddressWithoutPort);
    trie_mapstr!(trie, "DOWNSTREAM_REMOTE_PORT", Operator::DownstreamRemotePort);
    trie_mapstr_unsupported!(trie, "UPSTREAM_REQUEST_ATTEMPT_COUNT", Operator::UpstreamRequestAttemptCount);
    trie_mapstr_unsupported!(trie, "UPSTREAM_TLS_CIPHER", Operator::UpstreamTlsCipher);
    trie_mapstr_unsupported!(trie, "UPSTREAM_TLS_VERSION", Operator::UpstreamTlsVersion);
    trie_mapstr_unsupported!(trie, "UPSTREAM_TLS_SESSION_ID", Operator::UpstreamTlsSessionId);
    trie_mapstr_unsupported!(trie, "UPSTREAM_PEER_ISSUER", Operator::UpstreamPeerIssuer);
    trie_mapstr_unsupported!(trie, "UPSTREAM_PEER_CERT", Operator::UpstreamPeerCert);
    trie_mapstr_unsupported!(trie, "UPSTREAM_PEER_SUBJECT", Operator::UpstreamPeerSubject);
    trie_mapstr_unsupported!(trie, "DOWNSTREAM_DIRECT_LOCAL_ADDRESS", Operator::DownstreamDirectLocalAddress);
    trie_mapstr_unsupported!(
        trie,
        "DOWNSTREAM_DIRECT_LOCAL_ADDRESS_WITHOUT_PORT",
        Operator::DownstreamDirectLocalAddressWithoutPort
    );
    trie_mapstr_unsupported!(trie, "DOWNSTREAM_DIRECT_LOCAL_PORT", Operator::DownstreamDirectLocalPort);
    trie_mapstr_unsupported!(trie, "DOWNSTREAM_DIRECT_REMOTE_ADDRESS", Operator::DownstreamDirectRemoteAddress);
    trie_mapstr_unsupported!(
        trie,
        "DOWNSTREAM_DIRECT_REMOTE_ADDRESS_WITHOUT_PORT",
        Operator::DownstreamDirectRemoteAddressWithoutPort
    );
    trie_mapstr_unsupported!(trie, "DOWNSTREAM_DIRECT_REMOTE_PORT", Operator::DownstreamDirectRemotePort);
    trie_mapstr!(trie, "CONNECTION_ID", Operator::ConnectionId);
    trie_mapstr!(trie, "REQUEST_HEADERS_BYTES", Operator::RequestHeadersBytes);
    trie_mapstr!(trie, "RESPONSE_HEADERS_BYTES", Operator::ResponseHeadersBytes);
    trie_mapstr!(trie, "REQUESTED_SERVER_NAME", Operator::RequestedServerName);
    trie_mapstr!(trie, "ROUTE_NAME", Operator::RouteName);
    trie_mapstr_unsupported!(trie, "UPSTREAM_PEER_URI_SAN", Operator::UpstreamPeerUriSan);
    trie_mapstr_unsupported!(trie, "UPSTREAM_PEER_DNS_SAN", Operator::UpstreamPeerDnsSan);
    trie_mapstr_unsupported!(trie, "UPSTREAM_PEER_IP_SAN", Operator::UpstreamPeerIpSan);
    trie_mapstr_unsupported!(trie, "UPSTREAM_LOCAL_URI_SAN", Operator::UpstreamLocalUriSan);
    trie_mapstr_unsupported!(trie, "UPSTREAM_LOCAL_DNS_SAN", Operator::UpstreamLocalDnsSan);
    trie_mapstr_unsupported!(trie, "UPSTREAM_LOCAL_IP_SAN", Operator::UpstreamLocalIpSan);
    trie_mapstr_unsupported!(trie, "DOWNSTREAM_PEER_URI_SAN", Operator::DownstreamPeerUriSan);
    trie_mapstr_unsupported!(trie, "DOWNSTREAM_PEER_DNS_SAN", Operator::DownstreamPeerDnsSan);
    trie_mapstr_unsupported!(trie, "DOWNSTREAM_PEER_IP_SAN", Operator::DownstreamPeerIpSan);
    trie_mapstr_unsupported!(trie, "DOWNSTREAM_PEER_EMAIL_SAN", Operator::DownstreamPeerEmailSan);
    trie_mapstr_unsupported!(trie, "DOWNSTREAM_PEER_OTHERNAME_SAN", Operator::DownstreamPeerOthernameSan);
    trie_mapstr_unsupported!(trie, "DOWNSTREAM_LOCAL_URI_SAN", Operator::DownstreamLocalUriSan);
    trie_mapstr_unsupported!(trie, "DOWNSTREAM_LOCAL_DNS_SAN", Operator::DownstreamLocalDnsSan);
    trie_mapstr_unsupported!(trie, "DOWNSTREAM_LOCAL_IP_SAN", Operator::DownstreamLocalIpSan);
    trie_mapstr_unsupported!(trie, "DOWNSTREAM_LOCAL_EMAIL_SAN", Operator::DownstreamLocalEmailSan);
    trie_mapstr_unsupported!(trie, "DOWNSTREAM_LOCAL_OTHERNAME_SAN", Operator::DownstreamLocalOthernameSan);
    trie_mapstr_unsupported!(trie, "DOWNSTREAM_PEER_SUBJECT", Operator::DownstreamPeerSubject);
    trie_mapstr_unsupported!(trie, "DOWNSTREAM_LOCAL_SUBJECT", Operator::DownstreamLocalSubject);
    trie_mapstr_unsupported!(trie, "DOWNSTREAM_TLS_SESSION_ID", Operator::DownstreamTlsSessionId);
    trie_mapstr_unsupported!(trie, "DOWNSTREAM_TLS_CIPHER", Operator::DownstreamTlsCipher);
    trie_mapstr_unsupported!(trie, "DOWNSTREAM_TLS_VERSION", Operator::DownstreamTlsVersion);
    trie_mapstr_unsupported!(trie, "DOWNSTREAM_PEER_FINGERPRINT_256", Operator::DownstreamPeerFingerprint256);
    trie_mapstr_unsupported!(trie, "DOWNSTREAM_PEER_FINGERPRINT_1", Operator::DownstreamPeerFingerprint1);
    trie_mapstr_unsupported!(trie, "DOWNSTREAM_PEER_SERIAL", Operator::DownstreamPeerSerial);
    trie_mapstr_unsupported!(
        trie,
        "DOWNSTREAM_PEER_CHAIN_FINGERPRINTS_256",
        Operator::DownstreamPeerChainFingerprints256
    );
    trie_mapstr_unsupported!(trie, "DOWNSTREAM_PEER_CHAIN_FINGERPRINTS_1", Operator::DownstreamPeerChainFingerprints1);
    trie_mapstr_unsupported!(trie, "DOWNSTREAM_PEER_CHAIN_SERIALS", Operator::DownstreamPeerChainSerials);
    trie_mapstr_unsupported!(trie, "DOWNSTREAM_PEER_ISSUER", Operator::DownstreamPeerIssuer);
    trie_mapstr_unsupported!(trie, "DOWNSTREAM_PEER_CERT", Operator::DownstreamPeerCert);
    trie_mapstr_unsupported!(trie, "DOWNSTREAM_TRANSPORT_FAILURE_REASON", Operator::DownstreamTransportFailureReason);
    trie_mapstr!(trie, "UPSTREAM_TRANSPORT_FAILURE_REASON", Operator::UpstreamTransportFailureReason);
    trie_mapstr_unsupported!(trie, "HOSTNAME", Operator::Hostname);
    trie_mapstr_unsupported!(trie, "FILTER_CHAIN_NAME", Operator::FilterChainName);
    trie_mapstr_unsupported!(trie, "VIRTUAL_CLUSTER_NAME", Operator::VirtualClusterName);
    trie_mapstr_unsupported!(trie, "TLS_JA3_FINGERPRINT", Operator::TlsJa3Fingerprint);
    trie_mapstr!(trie, "UNIQUE_ID", Operator::UniqueId);
    trie_mapstr!(trie, "TRACE_ID", Operator::TraceId);
    trie_mapstr_unsupported!(trie, "STREAM_ID", Operator::StreamId);
    trie_mapstr!(trie, "START_TIME", Operator::StartTime);
    trie_mapstr_unsupported!(trie, "START_TIME_LOCAL", Operator::StartTimeLocal);
    trie_mapstr_unsupported!(trie, "EMIT_TIME", Operator::EmitTime);
    trie_mapstr_unsupported!(trie, "EMIT_TIME_LOCAL", Operator::EmitTimeLocal);
    trie_mapstr_unsupported!(trie, "DYNAMIC_METADATA", Operator::DynamicMetadata);
    trie_mapstr_unsupported!(trie, "CLUSTER_METADATA", Operator::ClusterMetadata);
    trie_mapstr_unsupported!(trie, "UPSTREAM_METADATA", Operator::UpstreamMetadata);
    trie_mapstr_unsupported!(trie, "FILTER_STATE", Operator::FilterState);
    trie_mapstr_unsupported!(trie, "UPSTREAM_FILTER_STATE", Operator::UpstreamFilterState);
    trie_mapstr_unsupported!(trie, "DOWNSTREAM_PEER_CERT_V_START", Operator::DownstreamPeerCertVStart);
    trie_mapstr_unsupported!(trie, "DOWNSTREAM_PEER_CERT_V_END", Operator::DownstreamPeerCertVEnd);
    trie_mapstr_unsupported!(trie, "UPSTREAM_PEER_CERT_V_START", Operator::UpstreamPeerCertVStart);
    trie_mapstr_unsupported!(trie, "UPSTREAM_PEER_CERT_V_END", Operator::UpstreamPeerCertVEnd);
    trie_mapstr_unsupported!(trie, "ENVIRONMENT", Operator::Environment);
    trie_mapstr_unsupported!(
        trie,
        "UPSTREAM_CONNECTION_POOL_READY_DURATION",
        Operator::UpstreamConnectionPoolReadyDuration
    );
    trie_mapstr!(trie, "REQ", Operator::Request(HeaderName(SmolStr::new_static("name"))), true); // %REQ(USER-AGENT)%
    trie_mapstr!(trie, "RESP", Operator::Response(HeaderName(SmolStr::new_static("name"))), true); // %RESP(X-ENVOY-UPSTREAM-SERVICE-TIME)%
    trie
});

pub struct AccessLogGrammar;

impl AccessLogGrammar {
    fn parse_request(arg: &str) -> Result<ReqArgument, FormatError> {
        if let Some((t, _, _, _)) = ENVOY_REQ_ARGS.find_longest_prefix(arg.bytes()) {
            Ok(t.clone())
        } else {
            let valid_name = http::HeaderName::from_bytes(arg.as_bytes())
                .map_err(|_e| FormatError::InvalidRequestArg(arg.into()))?;
            Ok(ReqArgument::Header(HeaderName(valid_name.as_str().into())))
        }
    }

    fn parse_response(arg: &str) -> Result<RespArgument, FormatError> {
        if let Some((t, _, _, _)) = ENVOY_RESP_ARGS.find_longest_prefix(arg.bytes()) {
            Ok(t.clone())
        } else {
            let valid_name = http::HeaderName::from_bytes(arg.as_bytes())
                .map_err(|_e| FormatError::InvalidResponseArg(arg.into()))?;
            Ok(RespArgument::Header(HeaderName(valid_name.as_str().into())))
        }
    }

    #[allow(clippy::string_slice)]
    fn extract_operator_arg(input: &str) -> Result<(&str, usize), FormatError> {
        if let Some(rest) = input.strip_prefix('(') {
            if let Some(end) = rest.find(')') {
                let arg = &rest[..end];
                if arg.is_empty() {
                    return Err(FormatError::EmptyArgument(input.into()));
                }
                let total_len = end + 2; // '(' + arg.len() + ')'
                return Ok((arg, total_len));
            }
        }
        Err(FormatError::MissingBracket(input.into()))
    }

    #[allow(clippy::string_slice)]
    pub fn parse_operator(key: &str) -> Option<Operator> {
        if let Some((placeholder, _category, placeholder_len, has_arg)) =
            ENVOY_PATTERNS.find_longest_prefix(key.bytes())
        {
            if key.len() == *placeholder_len && !*has_arg {
                return Some(placeholder.clone());
            } else if *has_arg {
                let after_placeholder = &key[*placeholder_len..];
                if let Ok((arg_value, arg_len)) = Self::extract_operator_arg(after_placeholder) {
                    if *placeholder_len + arg_len == key.len() {
                        match placeholder {
                            Operator::Request(_) => {
                                if let Ok(arg) = Self::parse_request(arg_value) {
                                    let op = match arg {
                                        ReqArgument::Scheme => Operator::RequestScheme,
                                        ReqArgument::Method => Operator::RequestMethod,
                                        ReqArgument::Path => Operator::RequestPath,
                                        ReqArgument::OriginalPathOrPath => Operator::RequestOriginalPathOrPath,
                                        ReqArgument::Authority => Operator::RequestAuthority,
                                        ReqArgument::Header(header_name) => Operator::Request(header_name),
                                    };
                                    return Some(op);
                                }
                            },
                            Operator::Response(_) => {
                                if let Ok(arg) = Self::parse_response(arg_value) {
                                    let op = match arg {
                                        RespArgument::Status => Operator::ResponseStatus,
                                        RespArgument::Header(header_name) => Operator::Response(header_name),
                                    };
                                    return Some(op);
                                }
                            },
                            _ => {},
                        }
                    }
                }
            }
        }
        None
    }
}

impl Grammar for AccessLogGrammar {
    #[allow(clippy::too_many_lines)]
    #[allow(clippy::string_slice)]
    fn parse(input: &str) -> Result<Vec<Template>, FormatError> {
        let mut parts = Vec::new();
        let mut literal_start = 0;
        let mut i = 0;

        while i < input.len() {
            let mut longest_placeholder: Option<(Operator, Category, usize)> = None;
            let mut skip = None;
            let mut free_placeholder: Option<SmolStr> = None;

            // find the longest placeholder starting from the current index i
            //
            if input[i..].starts_with('%') {
                let remainder = &input[i + 1..];
                if remainder.starts_with('%') {
                    skip = Some(2);
                } else if let Some((placeholder, category, placeholder_len, has_arg)) =
                    ENVOY_PATTERNS.find_longest_prefix(remainder.bytes())
                {
                    // ensure the placeholder is properly closed with a '%'
                    //

                    let operator = match remainder.find([' ', '%', '(']) {
                        Some(index) => &remainder[..index], // ...return a slice from the beginning to that index.
                        None => remainder,                  // In that case, return the whole string slice.
                    };

                    if operator.len() != *placeholder_len {
                        return Err(FormatError::InvalidOperator(
                            remainder.split_once('%').map(|(operator, _)| operator).unwrap_or(remainder).into(),
                        ));
                    }

                    let after_placeholder = &remainder[*placeholder_len..];
                    // placeholder found
                    if *has_arg {
                        let (arg_value, arg_len) = Self::extract_operator_arg(after_placeholder)?;

                        if longest_placeholder.as_ref().is_none_or(|(_, _, len)| *placeholder_len > *len) {
                            match placeholder {
                                Operator::Request(_) => {
                                    let arg = Self::parse_request(arg_value)?;
                                    longest_placeholder = match arg {
                                        ReqArgument::Scheme => {
                                            Some((Operator::RequestScheme, *category, *placeholder_len))
                                        },
                                        ReqArgument::Method => {
                                            Some((Operator::RequestMethod, *category, *placeholder_len))
                                        },
                                        ReqArgument::Path => Some((Operator::RequestPath, *category, *placeholder_len)),
                                        ReqArgument::OriginalPathOrPath => {
                                            Some((Operator::RequestOriginalPathOrPath, *category, *placeholder_len))
                                        },
                                        ReqArgument::Authority => {
                                            Some((Operator::RequestAuthority, *category, *placeholder_len))
                                        },
                                        ReqArgument::Header(header_name) => {
                                            Some((Operator::Request(header_name), *category, *placeholder_len))
                                        },
                                    }
                                },
                                Operator::Response(_) => {
                                    let arg = Self::parse_response(arg_value)?;
                                    longest_placeholder = match arg {
                                        RespArgument::Status => {
                                            Some((Operator::ResponseStatus, *category, *placeholder_len))
                                        },
                                        RespArgument::Header(header_name) => {
                                            Some((Operator::Response(header_name), *category, *placeholder_len))
                                        },
                                    }
                                },
                                _ => (),
                            }
                        }

                        if !after_placeholder[arg_len..].starts_with('%') {
                            return Err(FormatError::MissingDelimiter(remainder[..*placeholder_len].into()));
                        }

                        skip = Some(2 + *placeholder_len + arg_len);
                    } else {
                        longest_placeholder = Some((placeholder.clone(), *category, *placeholder_len));
                        if !after_placeholder.starts_with('%') {
                            return Err(FormatError::MissingDelimiter(remainder[..*placeholder_len].into()));
                        }
                        skip = Some(2 + *placeholder_len);
                    }
                } else if let Some(index) = remainder.find('%') {
                    let custom_operator = &remainder[..index];
                    let is_valid_custom =
                        CUSTOM_OPERATORS.get().map(|ops| ops.contains(custom_operator)).unwrap_or(false);

                    if is_valid_custom {
                        free_placeholder = Some(SmolStr::new(custom_operator));
                        skip = Some(2 + custom_operator.len());
                    } else {
                        return Err(FormatError::InvalidOperator(
                            remainder.split_once('%').map(|(operator, _)| operator).unwrap_or(remainder).into(),
                        ));
                    }
                } else {
                    return Err(FormatError::InvalidOperator(
                        remainder.split_once('%').map(|(operator, _)| operator).unwrap_or(remainder).into(),
                    ));
                }
            }

            if longest_placeholder.is_some() || free_placeholder.is_some() {
                // placeholder found
                if i > literal_start {
                    let literal_text = &input[literal_start..i];
                    if literal_text.chars().count() == 1 {
                        // if the literal is a single char, push it as a char
                        if let Some(c) = literal_text.chars().next() {
                            parts.push(Template::Char(c));
                        } else {
                            // This case should not happen, but just in case
                            parts.push(Template::Literal(literal_text.into()));
                        }
                    } else {
                        parts.push(Template::Literal(literal_text.into()));
                    }
                }

                if let Some(placeholder) = longest_placeholder.as_ref() {
                    // Add this placeholder.
                    parts.push(Template::Placeholder(
                        // input[i..i + skip.unwrap()].into() <- this is original placeholder
                        placeholder.0.clone(),
                        placeholder.1,
                    ));
                } else if let Some(name) = free_placeholder {
                    // Add custom/free placeholder.
                    parts.push(Template::Custom(name));
                }

                // advance the index beyond the current placeholder and possibly its argument.
                i += skip.unwrap_or(0);

                literal_start = i;
            } else {
                /* skip the specified number of bytes, or by default the next char */
                i += skip.unwrap_or(input[i..].chars().next().map(char::len_utf8).unwrap_or(0));
            }
        }

        // if there's some text remaining, it's a literal
        if i > literal_start {
            let literal_text = &input[literal_start..i].replace("%%", "%");
            if literal_text.chars().count() == 1 {
                if let Some(c) = literal_text.chars().next() {
                    // if the literal is a single char, push it as a char
                    parts.push(Template::Char(c));
                } else {
                    // This case should not happen, but just in case
                    parts.push(Template::Literal(literal_text.into()));
                }
            } else {
                parts.push(Template::Literal(literal_text.into()));
            }
        }

        Ok(parts)
    }
}

// Unit tests module
#[cfg(test)]
mod tests {
    use crate::DEFAULT_ACCESS_LOG_FORMAT;

    use super::*;

    fn init_custom_operators() {
        let mut ops = std::collections::HashSet::new();
        ops.insert(SmolStr::new("MY_CUSTOM_KEY"));
        ops.insert(SmolStr::new("KEY1"));
        ops.insert(SmolStr::new("KEY2"));
        ops.insert(SmolStr::new("KEY3"));
        ops.insert(SmolStr::new("KEY4"));
        _ = CUSTOM_OPERATORS.set(ops);
    }

    #[test]
    fn test_parse_free_placeholder() {
        init_custom_operators();

        let input = "%MY_CUSTOM_KEY%";
        let expected = vec![Template::Custom("MY_CUSTOM_KEY".into())];
        let actual = AccessLogGrammar::parse(input).unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_parse_multiple_free_placeholders() {
        init_custom_operators();

        let input = "Start %KEY1% middle %KEY2% end.";
        let actual = AccessLogGrammar::parse(input).unwrap();
        let has_key1 = actual.iter().any(|t| matches!(t, Template::Custom(name) if name.as_str() == "KEY1"));
        let has_key2 = actual.iter().any(|t| matches!(t, Template::Custom(name) if name.as_str() == "KEY2"));
        assert!(has_key1, "Should have parsed %KEY1% as Template::Free");
        assert!(has_key2, "Should have parsed %KEY2% as Template::Free");
    }

    #[test]
    fn test_parse_only_literals() {
        let input = "This is a plain literal string.";
        let expected = vec![Template::Literal("This is a plain literal string.".into())];
        let actual = AccessLogGrammar::parse(input).unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_parse_only_placeholders() {
        let input = "%START_TIME%%PROTOCOL%";
        let expected = vec![
            Template::Placeholder(Operator::StartTime, Category::OPERATOR),
            Template::Placeholder(Operator::Protocol, Category::OPERATOR),
        ];

        let actual = AccessLogGrammar::parse(input).unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_parse_mixed_literal_and_placeholders() {
        let input = "Start %REQ(:METHOD)% middle %PROTOCOL% end.";
        let expected = vec![
            Template::Literal("Start ".into()),
            Template::Placeholder(Operator::RequestMethod, Category::OPERATOR),
            Template::Literal(" middle ".into()),
            Template::Placeholder(Operator::Protocol, Category::OPERATOR),
            Template::Literal(" end.".into()),
        ];
        let actual = AccessLogGrammar::parse(input).unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_parse_starts_with_placeholder() {
        let input = "%START_TIME% literal after.";
        let expected = vec![
            Template::Placeholder(Operator::StartTime, Category::OPERATOR),
            Template::Literal(" literal after.".into()),
        ];
        let actual = AccessLogGrammar::parse(input).unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_parse_ends_with_placeholder() {
        let input = "Literal before %PROTOCOL%";
        let expected = vec![
            Template::Literal("Literal before ".into()),
            Template::Placeholder(Operator::Protocol, Category::OPERATOR),
        ];
        let actual = AccessLogGrammar::parse(input).unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_parse_empty_string() {
        let input = "";
        let expected: Vec<Template> = vec![]; // Expect an empty vector
        let actual = AccessLogGrammar::parse(input).unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_parse_with_special_chars_in_literal() {
        let input = "Literal with \"quotes\" and %%percent signs%% not placeholders.";
        let expected = vec![Template::Literal("Literal with \"quotes\" and %percent signs% not placeholders.".into())];
        let actual = AccessLogGrammar::parse(input).unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_parse_complex_envoy_string() {
        let input = r#"[%START_TIME%] "%REQ(:METHOD)% %REQ(:PATH)% %PROTOCOL%""#;
        let expected = vec![
            Template::Char('['),
            Template::Placeholder(Operator::StartTime, Category::OPERATOR),
            Template::Literal("] \"".into()),
            Template::Placeholder(Operator::RequestMethod, Category::OPERATOR),
            Template::Char(' '),
            Template::Placeholder(Operator::RequestPath, Category::OPERATOR),
            Template::Char(' '),
            Template::Placeholder(Operator::Protocol, Category::OPERATOR),
            Template::Char('"'),
        ];
        let actual = AccessLogGrammar::parse(input).unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_parse_request_header_bytes() {
        let input = "%REQUEST_HEADERS_BYTES%";
        let actual = AccessLogGrammar::parse(input).unwrap();
        let expected = vec![Template::Placeholder(Operator::RequestHeadersBytes, Category::OPERATOR)];
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_parse_response_header_bytes() {
        let input = "%RESPONSE_HEADERS_BYTES%";
        let actual = AccessLogGrammar::parse(input).unwrap();
        let expected = vec![Template::Placeholder(Operator::ResponseHeadersBytes, Category::OPERATOR)];
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_default_fmt() {
        _ = AccessLogGrammar::parse(DEFAULT_ACCESS_LOG_FORMAT).unwrap();
    }

    // bad patters..

    #[test]
    fn test_parse_unsupported_operator() {
        let input = "%UNSUPPORTED%";
        let result = AccessLogGrammar::parse(input);
        assert!(matches!(result, Err(FormatError::InvalidOperator(_))));
    }

    #[test]
    fn test_parse_unsupported_operator_with_partial_match() {
        let input = "%RESPONSE_CODE_UNSUPPORTED%";
        let result = AccessLogGrammar::parse(input);
        assert!(matches!(result, Err(FormatError::InvalidOperator(_))));
    }

    #[test]
    fn test_parse_error_empty_argument() {
        let input = "%REQ()%";
        let result = AccessLogGrammar::parse(input);
        assert!(matches!(result, Err(FormatError::EmptyArgument(_))));
    }

    #[test]
    fn test_parse_error_missing_bracket_1() {
        let input = "%REQ";
        let result = AccessLogGrammar::parse(input);
        assert!(matches!(result, Err(FormatError::MissingBracket(_))));
    }

    #[test]
    fn test_parse_error_missing_bracket_2() {
        let input = "%REQ(USER_AGENT%";
        let result = AccessLogGrammar::parse(input);
        assert!(matches!(result, Err(FormatError::MissingBracket(_))));
    }

    #[test]
    fn test_parse_error_missing_delimiter_1() {
        let input = "%UPSTREAM_HOST";
        let result = AccessLogGrammar::parse(input);
        assert!(matches!(result, Err(FormatError::MissingDelimiter(_))));
    }

    #[test]
    fn test_parse_error_missing_delimiter_2() {
        let input = "%REQ(USER_AGENT)";
        let result = AccessLogGrammar::parse(input);
        assert!(matches!(result, Err(FormatError::MissingDelimiter(_))));
    }

    #[test]
    fn test_parse_error_missing_delimiter_3() {
        let input = "%REQ(USER_AGENT) ";
        let result = AccessLogGrammar::parse(input);
        assert!(matches!(result, Err(FormatError::MissingDelimiter(_))));
    }

    #[test]
    fn test_parse_invalid_req_argument() {
        let input = "%REQ(<BAD>)%";
        let result = AccessLogGrammar::parse(input);
        assert!(matches!(result, Err(FormatError::InvalidRequestArg(_))));
    }

    #[test]
    fn test_parse_invalid_resp_argument() {
        let input = "%RESP(<BAD>)%";
        let result = AccessLogGrammar::parse(input);
        assert!(matches!(result, Err(FormatError::InvalidResponseArg(_))));
    }
}
