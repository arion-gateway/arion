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

#![recursion_limit = "128"]
extern crate ctor_0_6 as ctor;
#[allow(unused_imports)]
#[macro_use]
extern crate assert_matches;

pub mod configuration;
pub mod event_error;

pub mod access_log;
mod body;
pub(crate) mod cedar;
pub mod clusters;
pub mod instrumentation;
mod listeners;
pub mod metrics;
pub mod runtime_context;
mod secrets;
pub(crate) mod thread_local;
pub mod timezone;
pub mod tracing_attributes;
pub(crate) mod transport;
mod utils;

use std::sync::OnceLock;

use arion_configuration::config::Runtime;
use http_body_util::Empty;
use listeners::listeners_manager;
use serde::Serialize;
use tokio::sync::mpsc;

use crate::body::{
    instrumented_body::InstrumentedBody, on_end_body::OnEndBody, response_flags::BodyKind, timeout_body::TimeoutBody,
};
pub use crate::configuration::{build_listener_factories, get_listeners_and_clusters, get_secrets_and_clusters};

pub use arion_configuration::config::network_filters::http_connection_manager::RouteConfiguration;
use arion_configuration::config::{
    cluster::LocalityLbEndpoints as LocalityLbEndpointsConfig,
    network_filters::http_connection_manager::{http_filters::HttpFilter, RouteSpecifier},
    secret::Secret,
    Bootstrap, Cluster, Listener as ListenerConfig,
};
pub use clusters::{
    cluster::PartialClusterType,
    health::{EndpointHealthUpdate, HealthCheckManager},
    load_assignment::PartialClusterLoadAssignment,
    ClusterLoadAssignmentBuilder,
};
pub use event_error::{DownstreamError, UpstreamError};
pub use listeners::http_connection_manager::mcp_gateway::xds_handler as mcp_xds_handler;
pub use listeners::listener::ListenerFactory;
pub use listeners_manager::{ListenerConfigurationChange, ListenersManager, RouteConfigurationChange};
pub use secrets::{CertInfo, SecretManager};
pub(crate) use transport::AsyncInstrumentedStream;

use std::error::Error as StdError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ChannelError {
    #[error("broadcast receive error: {0}")]
    Broadcast(#[from] tokio::sync::broadcast::error::RecvError),
    #[error("oneshot receive error: {0}")]
    Oneshot(#[from] tokio::sync::oneshot::error::RecvError),
    #[error("mpsc try receive error: {0}")]
    MpscTryRecv(#[from] tokio::sync::mpsc::error::TryRecvError),
}

#[derive(Debug, Error)]
pub enum HttpError {
    #[error("{0}")]
    Http(#[from] http::Error),
    #[error("invalid URI: {0}")]
    InvalidUri(#[from] http::uri::InvalidUri),
    #[error("invalid URI parts: {0}")]
    InvalidUriParts(#[from] http::uri::InvalidUriParts),
    #[error("invalid header value: {0}")]
    InvalidHeaderValue(#[from] http::header::InvalidHeaderValue),
}

#[derive(Debug, Error)]
pub enum TlsError {
    #[error("{0}")]
    Rustls(#[from] rustls::Error),
    #[error("x509 parse error: {0}")]
    X509(#[from] x509_parser::asn1_rs::Err<x509_parser::error::X509Error>),
    #[error("verifier builder error: {0}")]
    VerifierBuilder(#[from] rustls::server::VerifierBuilderError),
    #[error("invalid DNS name: {0}")]
    InvalidDnsName(#[from] webpki::types::InvalidDnsNameError),
    #[error("certificate configuration error: {0}")]
    Certificate(String),
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("{0}")]
    DataSource(#[from] arion_configuration::config::core::DataSourceReadError),
    #[error("{0}")]
    Generic(#[from] arion_configuration::config::common::GenericError),
    #[error("{0}")]
    ClusterHostname(#[from] arion_configuration::config::cluster::health_check::ClusterHostnameError),
    #[error("{0}")]
    RoutingContext(#[from] crate::clusters::clusters_manager::RoutingContextError),
    #[error("invalid timezone: {0}")]
    ChronoTz(#[from] chrono_tz::ParseError),
    #[error("configuration error: {0}")]
    Validation(String),
}

#[derive(Debug, Error)]
pub enum FilterError {
    #[error("cedar policy error: {0}")]
    Cedar(#[from] crate::cedar::error::Error),
    #[error("mcp tool builder error: {0}")]
    McpToolBuilder(#[from] Box<crate::listeners::http_connection_manager::mcp_gateway::tools::ToolBuilderError>),
    #[cfg(feature = "wasm")]
    #[error("wasm filter error: {0}")]
    Wasm(#[from] crate::listeners::http_connection_manager::wasm::WasmError),
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    Io(#[from] std::io::Error),
    #[error("{0}")]
    Http(#[from] HttpError),
    #[error("{0}")]
    Tls(#[from] TlsError),
    #[error("{0}")]
    Config(#[from] ConfigError),
    #[error("{0}")]
    Filter(#[from] FilterError),
    #[error("{0}")]
    Channel(#[from] ChannelError),
    #[error("{0}")]
    ProxyProtocol(#[from] crate::transport::proxy_protocol::ProxyProtocolError),
    #[error("{0}")]
    TonicStatus(#[from] tonic::Status),
    #[error("{0}")]
    Regex(#[from] regex::Error),
    #[error("{0}")]
    Upstream(#[source] Box<crate::event_error::UpstreamError>),
    #[error("{0}")]
    Downstream(#[source] Box<crate::event_error::DownstreamError>),
}

impl Error {
    pub fn new(msg: impl Into<String>) -> Self {
        Self::Config(ConfigError::Validation(msg.into()))
    }

    pub fn upstream(error: impl Into<crate::event_error::UpstreamError>) -> Self {
        Self::Upstream(Box::new(error.into()))
    }

    pub fn find_source<T: StdError + 'static>(&self) -> Option<&T> {
        let mut curr: Option<&(dyn StdError + 'static)> = Some(self);
        while let Some(e) = curr {
            if let Some(downcasted) = e.downcast_ref::<T>() {
                return Some(downcasted);
            }
            curr = e.source();
        }
        None
    }

    #[inline]
    pub fn as_upstream_error(&self) -> Option<&crate::event_error::UpstreamError> {
        match self {
            Self::Upstream(err) => Some(err.as_ref()),
            _ => None,
        }
    }

    #[inline]
    pub fn as_downstream_error(&self) -> Option<&crate::event_error::DownstreamError> {
        match self {
            Self::Downstream(err) => Some(err.as_ref()),
            _ => None,
        }
    }

    /// Returns the [`TcpErrorContext`] carried by an upstream connection error, if present.
    #[inline]
    pub fn upstream_context(&self) -> Option<&crate::transport::connector::TcpErrorContext> {
        match self.as_upstream_error()? {
            crate::event_error::UpstreamError::Connect(c) => Some(&c.context),
            _ => None,
        }
    }
}

impl From<crate::event_error::UpstreamError> for Error {
    #[cold]
    #[inline(never)]
    fn from(err: crate::event_error::UpstreamError) -> Self {
        Self::upstream(err)
    }
}

impl From<crate::event_error::DownstreamError> for Error {
    #[cold]
    #[inline(never)]
    fn from(err: crate::event_error::DownstreamError) -> Self {
        Self::Downstream(Box::new(err))
    }
}

// Channels
impl From<tokio::sync::broadcast::error::RecvError> for Error {
    fn from(err: tokio::sync::broadcast::error::RecvError) -> Self {
        Self::Channel(ChannelError::Broadcast(err))
    }
}

impl From<tokio::sync::oneshot::error::RecvError> for Error {
    fn from(err: tokio::sync::oneshot::error::RecvError) -> Self {
        Self::Channel(ChannelError::Oneshot(err))
    }
}

impl From<tokio::sync::mpsc::error::TryRecvError> for Error {
    fn from(err: tokio::sync::mpsc::error::TryRecvError) -> Self {
        Self::Channel(ChannelError::MpscTryRecv(err))
    }
}

// Http
impl From<http::Error> for Error {
    #[cold]
    #[inline(never)]
    fn from(err: http::Error) -> Self {
        Self::Http(HttpError::Http(err))
    }
}

impl From<http::uri::InvalidUri> for Error {
    #[cold]
    #[inline(never)]
    fn from(err: http::uri::InvalidUri) -> Self {
        Self::Http(HttpError::InvalidUri(err))
    }
}

impl From<http::uri::InvalidUriParts> for Error {
    #[cold]
    #[inline(never)]
    fn from(err: http::uri::InvalidUriParts) -> Self {
        Self::Http(HttpError::InvalidUriParts(err))
    }
}

impl From<http::header::InvalidHeaderValue> for Error {
    #[cold]
    #[inline(never)]
    fn from(err: http::header::InvalidHeaderValue) -> Self {
        Self::Http(HttpError::InvalidHeaderValue(err))
    }
}

// Tls
impl From<rustls::Error> for Error {
    fn from(err: rustls::Error) -> Self {
        Self::Tls(TlsError::Rustls(err))
    }
}

impl From<x509_parser::asn1_rs::Err<x509_parser::error::X509Error>> for Error {
    fn from(err: x509_parser::asn1_rs::Err<x509_parser::error::X509Error>) -> Self {
        Self::Tls(TlsError::X509(err))
    }
}

impl From<rustls::server::VerifierBuilderError> for Error {
    fn from(err: rustls::server::VerifierBuilderError) -> Self {
        Self::Tls(TlsError::VerifierBuilder(err))
    }
}

impl From<webpki::types::InvalidDnsNameError> for Error {
    fn from(err: webpki::types::InvalidDnsNameError) -> Self {
        Self::Tls(TlsError::InvalidDnsName(err))
    }
}

// Config
impl From<arion_configuration::config::core::DataSourceReadError> for Error {
    fn from(err: arion_configuration::config::core::DataSourceReadError) -> Self {
        Self::Config(ConfigError::DataSource(err))
    }
}

impl From<arion_configuration::config::common::GenericError> for Error {
    fn from(err: arion_configuration::config::common::GenericError) -> Self {
        Self::Config(ConfigError::Generic(err))
    }
}

impl From<arion_configuration::config::cluster::health_check::ClusterHostnameError> for Error {
    fn from(err: arion_configuration::config::cluster::health_check::ClusterHostnameError) -> Self {
        Self::Config(ConfigError::ClusterHostname(err))
    }
}

impl From<crate::clusters::clusters_manager::RoutingContextError> for Error {
    fn from(err: crate::clusters::clusters_manager::RoutingContextError) -> Self {
        Self::Config(ConfigError::RoutingContext(err))
    }
}

impl From<chrono_tz::ParseError> for Error {
    fn from(err: chrono_tz::ParseError) -> Self {
        Self::Config(ConfigError::ChronoTz(err))
    }
}

// Filter
impl From<crate::cedar::error::Error> for Error {
    fn from(err: crate::cedar::error::Error) -> Self {
        Self::Filter(FilterError::Cedar(err))
    }
}

impl From<crate::listeners::http_connection_manager::mcp_gateway::tools::ToolBuilderError> for Error {
    fn from(err: crate::listeners::http_connection_manager::mcp_gateway::tools::ToolBuilderError) -> Self {
        Self::Filter(FilterError::McpToolBuilder(Box::new(err)))
    }
}

#[cfg(feature = "wasm")]
impl From<crate::listeners::http_connection_manager::wasm::WasmError> for Error {
    fn from(err: crate::listeners::http_connection_manager::wasm::WasmError) -> Self {
        Self::Filter(FilterError::Wasm(err))
    }
}

// Strings (Fallback as ConfigError::Validation)
impl From<String> for ConfigError {
    fn from(msg: String) -> Self {
        Self::Validation(msg)
    }
}

impl From<&str> for ConfigError {
    fn from(msg: &str) -> Self {
        Self::Validation(msg.to_owned())
    }
}

impl From<String> for Error {
    #[cold]
    #[inline(never)]
    fn from(msg: String) -> Self {
        Self::Config(ConfigError::Validation(msg))
    }
}

impl From<&str> for Error {
    #[cold]
    #[inline(never)]
    fn from(msg: &str) -> Self {
        Self::Config(ConfigError::Validation(msg.to_owned()))
    }
}

impl AsRef<dyn StdError + Send + Sync + 'static> for Error {
    fn as_ref(&self) -> &(dyn StdError + Send + Sync + 'static) {
        self
    }
}

pub use crate::transport::connector::TcpErrorContext;
pub type Result<T> = ::core::result::Result<T, Error>;

pub use crate::body::poly_body::PolyBody;
pub use crate::body::BodyError;

use arion_configuration::config::network_filters::http_connection_manager::RetryPolicy;
use std::time::Duration;

/// The Arion Request Body: a poly body with timeout and instrumentation
pub type ArionRequestBody = InstrumentedBody<TimeoutBody<PolyBody>>;
impl Default for ArionRequestBody {
    fn default() -> Self {
        InstrumentedBody::new(
            BodyKind::Request,
            TimeoutBody::new(None, PolyBody::from(Empty::new())),
            None,
            |_, _, _, _| {},
        )
    }
}

/// The Arion Response Body: a poly body with timeout and an optional pool permit.
pub type ArionResponseBody = OnEndBody<TimeoutBody<PolyBody>>;

/// Downstream response body sent to the client: instrumentation over [`ArionResponseBody`].
pub(crate) type ArionClientBody = InstrumentedBody<ArionResponseBody>;

/// Example with Result:
/// Captures the error in 'e' and returns early from the function `main()`
///    let _v1 = `unwrap_or_run!(result_val`, |e| {
///        println!("Error handled: {}", e);
///        return; // This returns from `main()`, unlike a closure!
///    });
#[macro_export]
macro_rules! unwrap_or_run {
    // Case for Result: expects pattern `|err_name| { code }`
    // Using simple token matching for the pipe syntax to allow variable binding.
    ($target:expr, |$err:ident| $block:block) => {
        match $target {
            Ok(v) => v,
            Err($err) => $block,
        }
    };

    // Case for Option: expects just `{ code }`
    ($target:expr, $block:block) => {
        match $target {
            Some(v) => v,
            None => $block,
        }
    };
}

#[derive(Clone, Debug, Default)]
pub struct UpstreamCallOpts<'a> {
    pub route_timeout: Option<Duration>,
    pub retry_policy: Option<&'a RetryPolicy>,
    pub priority: clusters::RoutingPriority,
}

pub static RUNTIME_CONFIG: OnceLock<Runtime> = OnceLock::new();

#[allow(clippy::expect_used, clippy::missing_panics_doc)]
pub fn runtime_config() -> &'static Runtime {
    RUNTIME_CONFIG.get().expect("Called runtime_config without setting RUNTIME_CONFIG first")
}

pub struct ConversionContext<'a, T> {
    envoy_object: T,
    secret_manager: &'a SecretManager,
}
impl<'a, T> ConversionContext<'a, T> {
    pub fn new(ctx: (T, &'a SecretManager)) -> Self {
        Self { envoy_object: ctx.0, secret_manager: ctx.1 }
    }
}

pub struct ConfigurationReceivers {
    listener_configuration_receiver: mpsc::Receiver<ListenerConfigurationChange>,
    route_configuration_receiver: mpsc::Receiver<RouteConfigurationChange>,
}

#[derive(Clone, Debug)]
pub struct ConfigurationSenders {
    pub listener_configuration_sender: mpsc::Sender<ListenerConfigurationChange>,
    pub route_configuration_sender: mpsc::Sender<RouteConfigurationChange>,
}

impl ConfigurationReceivers {
    pub fn new(
        listener_configuration_receiver: mpsc::Receiver<ListenerConfigurationChange>,
        route_configuration_receiver: mpsc::Receiver<RouteConfigurationChange>,
    ) -> Self {
        Self { listener_configuration_receiver, route_configuration_receiver }
    }
}

impl ConfigurationSenders {
    pub fn new(
        listener_configuration_sender: mpsc::Sender<ListenerConfigurationChange>,
        route_configuration_sender: mpsc::Sender<RouteConfigurationChange>,
    ) -> Self {
        Self { listener_configuration_sender, route_configuration_sender }
    }
}

#[derive(Debug, Default, Serialize, Clone)]
pub struct ConfigDump {
    pub bootstrap: Option<Bootstrap>,
    #[serde(skip_serializing_if = "Option::is_none", default = "Default::default")]
    pub listeners: Option<Vec<ListenerConfig>>,
    #[serde(skip_serializing_if = "Option::is_none", default = "Default::default")]
    pub clusters: Option<Vec<Cluster>>,
    #[serde(skip_serializing_if = "Option::is_none", default = "Default::default")]
    pub ecds_filter_http: Option<Vec<HttpFilter>>,
    #[serde(skip_serializing_if = "Option::is_none", default = "Default::default")]
    pub endpoints: Option<Vec<LocalityLbEndpointsConfig>>,
    #[serde(skip_serializing_if = "Option::is_none", default = "Default::default")]
    pub routes: Option<Vec<RouteSpecifier>>,
    #[serde(skip_serializing_if = "Option::is_none", default = "Default::default")]
    pub secrets: Option<Vec<Secret>>,
}

pub fn new_configuration_channel(capacity: usize) -> (ConfigurationSenders, ConfigurationReceivers) {
    let (listener_tx, listener_rx) = mpsc::channel::<ListenerConfigurationChange>(capacity);
    let (route_tx, route_rx) = mpsc::channel::<RouteConfigurationChange>(capacity);
    (ConfigurationSenders::new(listener_tx, route_tx), ConfigurationReceivers::new(listener_rx, route_rx))
}

/// Start the listeners manager directly without spawning a background task.
/// Caller must be inside a Tokio runtime and await this async function.
pub async fn start_listener_manager(configuration_receivers: ConfigurationReceivers) -> Result<()> {
    let ConfigurationReceivers { listener_configuration_receiver, route_configuration_receiver } =
        configuration_receivers;

    tracing::debug!("listeners manager starting");
    let mgr = ListenersManager::new(listener_configuration_receiver, route_configuration_receiver);
    mgr.start().await.map_err(|err| {
        tracing::warn!(error = %err, "listeners manager exited with error");
        err
    })?;
    tracing::debug!("listeners manager finished cleanly");
    Ok(())
}

use ctor::ctor;
#[ctor]
fn init() {
    //
    // intialise AWS-LC-RS as default crypto provider
    //
    #[allow(clippy::expect_used)]
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("Could not install crypto provider (aws-lc-rs)");
}
