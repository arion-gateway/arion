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

#[cfg(feature = "metrics")]
use crate::metrics;

use super::{RequestCtx, RequestHandler};
use crate::{
    body::response_flags::ResponseFlags, event_error::EventFailure,
    listeners::synthetic_http_response::SyntheticHttpResponse, transport::HttpChannels,
    utils::instrumented_stream::InstrumentedStream, with_metric, OrionRequestBody, OrionResponseBody, Result,
    UpstreamCallOpts,
};
use orion_format::types::ResponseFlags as FmtResponseFlags;

use crate::get_shard_id;
use http::{header, HeaderMap, HeaderValue, StatusCode, Version};
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
#[cfg(feature = "metrics")]
use opentelemetry::KeyValue;
use orion_configuration::config::network_filters::http_connection_manager::UpgradeType;
use scopeguard::defer;
use tokio::io::copy_bidirectional;
use tracing::{debug, error};

#[cfg(feature = "metrics")]
use orion_metrics::metrics::{clusters, http as http_metrics, user};

const UPGRADE: &str = "upgrade";
const WEBSOCKET: &str = "websocket";

#[inline]
pub fn is_upgrade_connection(header_value: &str) -> bool {
    header_value.to_lowercase() == UPGRADE
}

#[inline]
pub fn is_websocket_upgrade(header_value: &str) -> bool {
    header_value.to_lowercase() == WEBSOCKET
}

#[inline]
pub fn is_valid_header(header_value: &HeaderValue) -> std::result::Result<&str, http::header::ToStrError> {
    header_value.to_str()
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum UpgradeError {
    #[error("Connection header value is not USASCII: {0}")]
    ConnectionNotAscii(String),
    #[error("Upgrade header value is not USASCII: {0}")]
    UpgradeNotAscii(String),
    #[error("Upgrade header value is not valid: {0}")]
    UnsupportedProtocol(String),
}

pub fn is_valid_websocket_upgrade_request(headers: &HeaderMap) -> std::result::Result<bool, UpgradeError> {
    match (headers.get(header::CONNECTION), headers.get(header::UPGRADE)) {
        (Some(connection_header), Some(upgrade_header)) => {
            let connection_header =
                is_valid_header(connection_header).map_err(|e| UpgradeError::ConnectionNotAscii(e.to_string()))?;
            let upgrade_header =
                is_valid_header(upgrade_header).map_err(|e| UpgradeError::UpgradeNotAscii(e.to_string()))?;
            let is_upgrade = is_upgrade_connection(connection_header);
            let is_websocket = is_websocket_upgrade(upgrade_header);
            match (is_upgrade, is_websocket) {
                (true, true) => Ok(true),
                (true, false) => Err(UpgradeError::UnsupportedProtocol(upgrade_header.to_owned())),
                (false, _) => Ok(false),
            }
        },
        _ => Ok(false),
    }
}

#[inline]
pub fn is_websocket_enabled_by_hcm(hcm_enabled_upgrades: &[UpgradeType]) -> bool {
    hcm_enabled_upgrades.iter().any(|upgrade| matches!(upgrade, UpgradeType::Websocket))
}

#[allow(clippy::too_many_lines)]
pub async fn handle_websocket_upgrade(
    req_ctx: &RequestCtx,
    mut request: Request<OrionRequestBody>,
    svc_channel: &HttpChannels,
    #[cfg(feature = "metrics")] listener_name: &'static str,
) -> Result<Response<OrionResponseBody>> {
    let version = request.version();
    match version {
        Version::HTTP_11 => {
            #[cfg(feature = "metrics")]
            let user_partition_key = req_ctx.tx.user_partition_key;

            let request_upgrade = hyper::upgrade::on(&mut request);
            match svc_channel.to_response(req_ctx, request, UpstreamCallOpts::default()).await {
                Ok(mut upstream_response) if upstream_response.status() == StatusCode::SWITCHING_PROTOCOLS => {
                    let response_upgrade = hyper::upgrade::on(&mut upstream_response);
                    #[cfg(feature = "metrics")]
                    let cluster_name = svc_channel.cluster_name();
                    tokio::spawn(async move {
                        #[cfg(feature = "metrics")]
                        let shard_id = get_shard_id!();

                        with_metric!(
                            http_metrics::DOWNSTREAM_CX_WS_UPGRADES_TOTAL,
                            add,
                            1,
                            shard_id,
                            &[KeyValue::new("listener", listener_name)]
                        );
                        with_metric!(
                            http_metrics::DOWNSTREAM_CX_WS_UPGRADES_ACTIVE,
                            add,
                            1,
                            shard_id,
                            &[KeyValue::new("listener", listener_name)]
                        );
                        defer! {
                            with_metric!(http_metrics::DOWNSTREAM_CX_WS_UPGRADES_ACTIVE, sub, 1, shard_id, &[KeyValue::new("listener", listener_name)]);
                        }

                        match (request_upgrade.await, response_upgrade.await) {
                            (Ok(request_upgraded), Ok(response_upgraded)) => {
                                let mut downstream = InstrumentedStream::new(TokioIo::new(request_upgraded));
                                let mut upstream = InstrumentedStream::new(TokioIo::new(response_upgraded));

                                #[allow(unused_variables)]
                                #[allow(clippy::let_unit_value)]
                                let shard_id = get_shard_id!();

                                let _ = copy_bidirectional(&mut downstream, &mut upstream)
                                    .await
                                    .map_err(|err| {
                                        error!("Upgrade failure, bidi copy failed for websocket {:?}", err);
                                        err
                                    })
                                    .ok();

                                #[allow(unused_variables)]
                                let bytes_received_down = downstream.metrics().bytes_read();
                                #[allow(unused_variables)]
                                let bytes_sent_down = downstream.metrics().bytes_written();
                                #[allow(unused_variables)]
                                let bytes_received_up = upstream.metrics().bytes_read();
                                #[allow(unused_variables)]
                                let bytes_sent_up = upstream.metrics().bytes_written();

                                debug!(target: "websocket", "downstream_rx: {bytes_received_down}, downstream_tx: {bytes_sent_down}, upstream_rx: {bytes_received_up}, upstream_tx: {bytes_sent_up}");

                                with_metric!(
                                    clusters::UPSTREAM_CX_RX_BYTES_TOTAL,
                                    add,
                                    bytes_received_up,
                                    shard_id,
                                    &[KeyValue::new("cluster", cluster_name)]
                                );
                                with_metric!(
                                    clusters::UPSTREAM_CX_TX_BYTES_TOTAL,
                                    add,
                                    bytes_sent_up,
                                    shard_id,
                                    &[KeyValue::new("cluster", cluster_name)]
                                );

                                #[cfg(feature = "metrics")]
                                if let Some(partition_key) = user_partition_key {
                                    with_metric!(
                                        user::INBOUND_STREAMING_BYTES_PROCESSED,
                                        add,
                                        bytes_received_down,
                                        shard_id,
                                        &[KeyValue::new(
                                            metrics::USER_KEY.attribute_name().unwrap_or("user"),
                                            partition_key
                                        )]
                                    );
                                    with_metric!(
                                        user::OUTBOUND_STREAMING_BYTES_PROCESSED,
                                        add,
                                        bytes_sent_down,
                                        shard_id,
                                        &[KeyValue::new(
                                            metrics::USER_KEY.attribute_name().unwrap_or("user"),
                                            partition_key
                                        )]
                                    );
                                    with_metric!(
                                        user::BYTES_RX,
                                        add,
                                        bytes_received_down,
                                        shard_id,
                                        &[
                                            KeyValue::new(
                                                metrics::USER_KEY.attribute_name().unwrap_or("user"),
                                                partition_key
                                            ),
                                            KeyValue::new("listener", listener_name)
                                        ]
                                    );
                                    with_metric!(
                                        user::BYTES_TX,
                                        add,
                                        bytes_sent_down,
                                        shard_id,
                                        &[
                                            KeyValue::new(
                                                metrics::USER_KEY.attribute_name().unwrap_or("user"),
                                                partition_key
                                            ),
                                            KeyValue::new("listener", listener_name)
                                        ]
                                    );
                                }
                            },
                            (req_state, resp_state) => {
                                with_metric!(
                                    http_metrics::DOWNSTREAM_RQ_WS_ON_NON_WS_ROUTE,
                                    add,
                                    1,
                                    shard_id,
                                    &[KeyValue::new("listener", listener_name)]
                                );
                                error!(
                                    "Upgrade attempt failure, occurred during connection upgrade {:?},{:?}",
                                    req_state, resp_state
                                );
                            },
                        }
                    });
                    Ok(upstream_response)
                },
                Ok(mut response) => {
                    error!(
                        "Upgrade attempt failure, upstream did not accept websocket upgrade, returned status code {:?}",
                        response.status()
                    );
                    if version == Version::HTTP_10 || version == Version::HTTP_11 {
                        response.headers_mut().insert(header::CONNECTION, HeaderValue::from_static("close"));
                    }
                    Ok(response)
                },
                Err(err) => {
                    error!("Upgrade failed in attempting to establish upstream websocket {:?}", err);
                    Ok(SyntheticHttpResponse::bad_gateway(
                        EventFailure::UpgradeFailed.into(),
                        ResponseFlags(FmtResponseFlags::UPSTREAM_CONNECTION_FAILURE),
                    )
                    .into_response(version))
                },
            }
        },
        _ => Ok(SyntheticHttpResponse::bad_request(EventFailure::UpgradeFailed.into()).into_response(version)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_valid_websocket_upgrade_request() {
        let mut header_map = HeaderMap::new();
        header_map.insert("connection", "upgrade".parse().unwrap());
        header_map.insert("upgrade", "websocket".parse().unwrap());
        is_valid_websocket_upgrade_request(&header_map).unwrap();

        let mut header_map = HeaderMap::new();
        header_map.insert("connection", "dfkdjkfjk".parse().unwrap());
        header_map.insert("upgrade", "websocket".parse().unwrap());
        assert_eq!(Ok(false), is_valid_websocket_upgrade_request(&header_map));

        let mut header_map = HeaderMap::new();
        header_map.insert("connection", "upgrade".parse().unwrap());
        header_map.insert("upgrade", "websocketsdklkd".parse().unwrap());

        if let Err(e) = is_valid_websocket_upgrade_request(&header_map) {
            assert!(matches!(e, UpgradeError::UnsupportedProtocol(_)));
        } else {
            unreachable!();
        }

        let mut header_map = HeaderMap::new();
        header_map.insert("connection", "无效的".parse().unwrap());
        header_map.insert("upgrade", "websocket".parse().unwrap());

        if let Err(e) = is_valid_websocket_upgrade_request(&header_map) {
            assert!(matches!(e, UpgradeError::ConnectionNotAscii(_)));
        } else {
            unreachable!();
        }

        let mut header_map = HeaderMap::new();
        header_map.insert("connection", "upgrade".parse().unwrap());
        header_map.insert("upgrade", "无效的".parse().unwrap());

        if let Err(e) = is_valid_websocket_upgrade_request(&header_map) {
            assert!(matches!(e, UpgradeError::UpgradeNotAscii(_)));
        } else {
            unreachable!();
        }
    }
}
