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
pub use listeners::http_connection_manager::mcp_gateway::xds_handler as mcp_xds_handler;
pub use listeners::listener::ListenerFactory;
pub use listeners_manager::{ListenerConfigurationChange, ListenersManager, RouteConfigurationChange};
pub use secrets::{CertInfo, SecretManager};
pub(crate) use transport::AsyncInstrumentedStream;

use std::error::Error as StdError;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Upstream connection failed: {context:?}")]
    UpstreamConnection {
        context: crate::transport::connector::TcpErrorContext,
        #[source]
        source: Box<dyn StdError + Send + Sync>,
    },
    #[error("{message}")]
    WithSource {
        message: String,
        #[source]
        source: Box<dyn StdError + Send + Sync>,
    },
    #[error("{0}")]
    Message(String),
    #[error("{0}")]
    Io(#[from] std::io::Error),
    #[error("{0}")]
    Http(#[from] http::Error),
    #[error("{0}")]
    InvalidUri(#[from] http::uri::InvalidUri),
    /// Catch-all for leaf foreign errors. The concrete type is preserved inside
    /// the box, so it stays reachable via [`Error::find_source`] and the `source()` chain.
    /// Boxing (instead of one variant per type) keeps `Error` compact and avoids
    /// recursive-type issues (e.g. `UpstreamError` itself can contain an `Error`).
    /// NOTE: deliberately NOT `#[error(transparent)]`: transparent delegates `source()`
    /// to the inner error, skipping this level and breaking `downcast`-based
    /// classification (`UpstreamError::try_infer_from`, `find_source`).
    #[error("{0}")]
    Boxed(#[from] Box<dyn StdError + Send + Sync>),
}

impl Error {
    pub fn new(msg: impl Into<String>) -> Self {
        Self::Message(msg.into())
    }

    pub fn with_source(message: impl Into<String>, source: impl Into<Box<dyn StdError + Send + Sync>>) -> Self {
        Self::WithSource { message: message.into(), source: source.into() }
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

    /// Walks the `source()` chain looking for the [`TcpErrorContext`]
    /// carried by an [`Error::UpstreamConnection`].
    /// Replaces the old `arion_error::get_context_data::<TcpErrorContext>()` lookup.
    pub fn upstream_context(&self) -> Option<&crate::transport::connector::TcpErrorContext> {
        let mut curr: Option<&(dyn StdError + 'static)> = Some(self);
        while let Some(e) = curr {
            match e.downcast_ref::<Self>() {
                Some(Self::UpstreamConnection { context, .. }) => return Some(context),
                _ => curr = e.source(),
            }
        }
        None
    }
}

impl From<String> for Error {
    fn from(msg: String) -> Self {
        Self::Message(msg)
    }
}

impl From<&str> for Error {
    fn from(msg: &str) -> Self {
        Self::Message(msg.to_owned())
    }
}

impl AsRef<dyn StdError + Send + Sync + 'static> for Error {
    fn as_ref(&self) -> &(dyn StdError + Send + Sync + 'static) {
        self
    }
}

/// Generates `From` impls that box leaf foreign errors into [`Error::Boxed`].
/// Used for error types that are only propagated (never matched on) so they
/// don't each deserve a dedicated variant.
macro_rules! boxed_error_from {
    ($($t:ty),* $(,)?) => {
        $(impl From<$t> for Error {
            fn from(err: $t) -> Self {
                Self::Boxed(err.into())
            }
        })*
    };
}

boxed_error_from!(
    hyper::Error,
    http::uri::InvalidUriParts,
    webpki::types::InvalidDnsNameError,
    rustls::Error,
    tonic::Status,
    chrono_tz::ParseError,
    tokio::sync::broadcast::error::RecvError,
    tokio::sync::mpsc::error::TryRecvError,
    tokio::sync::oneshot::error::RecvError,
    x509_parser::asn1_rs::Err<x509_parser::error::X509Error>,
    arion_configuration::config::core::DataSourceReadError,
    arion_configuration::config::common::GenericError,
    arion_configuration::config::cluster::health_check::ClusterHostnameError,
    crate::clusters::clusters_manager::RoutingContextError,
    crate::event_error::UpstreamError,
);

pub use crate::transport::connector::TcpErrorContext;
pub type Result<T> = ::core::result::Result<T, Error>;

pub use crate::body::poly_body::PolyBody;

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
