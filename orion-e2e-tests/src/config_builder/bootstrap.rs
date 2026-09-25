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

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use orion_configuration::config::log::AccessLogConfig;
use serde::{Deserialize, Serialize};
use serde_yaml::Value;

use crate::{Error, Result};

use super::cluster::Cluster;
use super::listener::Listener;
use super::serialize::proto_to_yaml_value;
use orion_configuration::config::metrics::MetricsConfig;

static CONFIG_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone)]
pub struct XdsConfig {
    pub address: String,
    pub port: u16,
}

#[derive(Debug, Clone)]
pub struct BootstrapBuilder {
    listeners: Vec<Listener>,
    clusters: Vec<Cluster>,
    runtime_cpus: u32,
    runtime_count: u32,
    log_level: String,
    xds_config: Option<XdsConfig>,
    admin_config: Option<Admin>,
    metrics: MetricsConfig,
    access_log_config: Option<AccessLogConfig>,
}

impl Default for BootstrapBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl BootstrapBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self {
            listeners: Vec::new(),
            clusters: Vec::new(),
            runtime_cpus: 1,
            runtime_count: 1,
            log_level: "info".into(),
            xds_config: None,
            admin_config: None,
            metrics: MetricsConfig::default(),
            access_log_config: None,
        }
    }

    #[must_use]
    pub fn listener(mut self, listener: impl Into<Listener>) -> Self {
        self.listeners.push(listener.into());
        self
    }

    #[must_use]
    pub fn listeners<I, L>(mut self, listeners: I) -> Self
    where
        I: IntoIterator<Item = L>,
        L: Into<Listener>,
    {
        self.listeners.extend(listeners.into_iter().map(Into::into));
        self
    }

    #[must_use]
    pub fn cluster(mut self, cluster: impl Into<Cluster>) -> Self {
        self.clusters.push(cluster.into());
        self
    }

    #[must_use]
    pub fn clusters<I, C>(mut self, clusters: I) -> Self
    where
        I: IntoIterator<Item = C>,
        C: Into<Cluster>,
    {
        self.clusters.extend(clusters.into_iter().map(Into::into));
        self
    }

    #[must_use]
    pub fn runtime_cpus(mut self, cpus: u32) -> Self {
        self.runtime_cpus = cpus;
        self
    }

    #[must_use]
    pub fn runtime_count(mut self, count: u32) -> Self {
        self.runtime_count = count;
        self
    }

    #[must_use]
    pub fn log_level(mut self, level: impl Into<String>) -> Self {
        self.log_level = level.into();
        self
    }

    #[must_use]
    pub fn xds(mut self, address: impl Into<String>, port: u16) -> Self {
        self.xds_config = Some(XdsConfig { address: address.into(), port });
        self
    }

    #[must_use]
    pub fn admin(mut self, address: impl Into<String>, port: u16) -> Self {
        self.admin_config = Some(Admin {
            address: Address { socket_address: SocketAddress { address: address.into(), port_value: port } },
        });
        self
    }

    #[must_use]
    pub fn metrics(mut self, metrics: MetricsConfig) -> Self {
        self.metrics = metrics;
        self
    }

    #[must_use]
    pub fn access_log(mut self, access_log_config: AccessLogConfig) -> Self {
        self.access_log_config = Some(access_log_config);
        self
    }

    #[must_use]
    pub fn get_clusters(&self) -> &[Cluster] {
        &self.clusters
    }

    #[must_use]
    pub fn get_listeners(&self) -> &[Listener] {
        &self.listeners
    }

    pub fn build_to_file(&self, path: impl AsRef<Path>) -> Result<PathBuf> {
        let config = self.build_yaml()?;
        let path = path.as_ref().to_path_buf();

        std::fs::write(&path, config)?;
        Ok(path)
    }

    pub fn build_to_temp(&self) -> Result<PathBuf> {
        let config = self.build_yaml()?;

        let temp_dir = std::env::temp_dir();
        let counter = CONFIG_COUNTER.fetch_add(1, Ordering::SeqCst);
        let filename = format!("orion-test-{}-{}.yaml", std::process::id(), counter);
        let path = temp_dir.join(filename);

        std::fs::write(&path, config)?;
        Ok(path)
    }

    pub fn build_yaml(&self) -> Result<String> {
        if self.xds_config.is_none() && self.listeners.is_empty() {
            return Err(Error::Config("At least one listener is required".into()));
        }

        let bootstrap = self.build_bootstrap()?;

        serde_yaml::to_string(&bootstrap).map_err(Error::from)
    }

    #[allow(clippy::unnecessary_wraps)]
    fn build_bootstrap(&self) -> Result<OrionConfig> {
        let listeners: Vec<Value> = self.listeners.iter().filter_map(|l| proto_to_yaml_value(l).ok()).collect();
        let clusters: Vec<Value> = self.clusters.iter().filter_map(|c| proto_to_yaml_value(c).ok()).collect();

        let (xds_cluster, dynamic_resources) = if let Some(ref xds) = self.xds_config {
            let xds_cluster = build_xds_cluster(&xds.address, xds.port);
            let dynamic = DynamicResources {
                ads_config: AdsConfig {
                    grpc_services: vec![GrpcService { envoy_grpc: EnvoyGrpc { cluster_name: "xds_cluster".into() } }],
                },
            };
            (Some(xds_cluster), Some(dynamic))
        } else {
            (None, None)
        };

        let mut all_clusters = clusters;
        if let Some(xds_cluster) = xds_cluster {
            all_clusters.push(xds_cluster);
        }

        Ok(OrionConfig {
            runtime: RuntimeConfig { num_cpus: self.runtime_cpus, num_runtimes: self.runtime_count },
            logging: LoggingConfig { log_level: self.log_level.clone() },
            access_log_config: self.access_log_config.clone(),
            envoy_bootstrap: EnvoyBootstrap {
                admin: self.admin_config.clone(),
                dynamic_resources,
                static_resources: StaticResources { listeners, clusters: all_clusters, secrets: vec![] },
            },
            metrics: self.metrics.clone(),
        })
    }
}

fn build_xds_cluster(address: &str, port: u16) -> Value {
    use serde_yaml::{Mapping, Number};

    let mut cluster = Mapping::new();
    cluster.insert(Value::String("name".into()), Value::String("xds_cluster".into()));
    cluster.insert(Value::String("connect_timeout".into()), Value::String("0.25s".into()));
    cluster.insert(Value::String("type".into()), Value::String("STATIC".into()));
    cluster.insert(Value::String("lb_policy".into()), Value::String("ROUND_ROBIN".into()));

    let mut http2_options = Mapping::new();
    http2_options.insert(
        Value::String("@type".into()),
        Value::String("type.googleapis.com/envoy.extensions.upstreams.http.v3.HttpProtocolOptions".into()),
    );

    let mut explicit_config = Mapping::new();
    explicit_config.insert(Value::String("http2_protocol_options".into()), Value::Mapping(Mapping::new()));
    http2_options.insert(Value::String("explicit_http_config".into()), Value::Mapping(explicit_config));

    let mut extension_options = Mapping::new();
    extension_options.insert(
        Value::String("envoy.extensions.upstreams.http.v3.HttpProtocolOptions".into()),
        Value::Mapping(http2_options),
    );
    cluster.insert(Value::String("typed_extension_protocol_options".into()), Value::Mapping(extension_options));

    let mut socket_address = Mapping::new();
    socket_address.insert(Value::String("address".into()), Value::String(address.into()));
    socket_address.insert(Value::String("port_value".into()), Value::Number(Number::from(port)));

    let mut address_map = Mapping::new();
    address_map.insert(Value::String("socket_address".into()), Value::Mapping(socket_address));

    let mut endpoint = Mapping::new();
    endpoint.insert(Value::String("address".into()), Value::Mapping(address_map));

    let mut lb_endpoint = Mapping::new();
    lb_endpoint.insert(Value::String("endpoint".into()), Value::Mapping(endpoint));

    let mut endpoints = Mapping::new();
    endpoints.insert(Value::String("lb_endpoints".into()), Value::Sequence(vec![Value::Mapping(lb_endpoint)]));

    let mut load_assignment = Mapping::new();
    load_assignment.insert(Value::String("cluster_name".into()), Value::String("xds_cluster".into()));
    load_assignment.insert(Value::String("endpoints".into()), Value::Sequence(vec![Value::Mapping(endpoints)]));

    cluster.insert(Value::String("load_assignment".into()), Value::Mapping(load_assignment));

    Value::Mapping(cluster)
}

#[derive(Debug, Serialize, Deserialize)]
struct OrionConfig {
    runtime: RuntimeConfig,
    logging: LoggingConfig,
    #[serde(skip_serializing_if = "Option::is_none", rename = "access_logging")]
    access_log_config: Option<AccessLogConfig>,
    envoy_bootstrap: EnvoyBootstrap,
    metrics: MetricsConfig,
}

#[derive(Debug, Serialize, Deserialize)]
struct RuntimeConfig {
    num_cpus: u32,
    num_runtimes: u32,
}

#[derive(Debug, Serialize, Deserialize)]
struct LoggingConfig {
    log_level: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct EnvoyBootstrap {
    #[serde(skip_serializing_if = "Option::is_none")]
    admin: Option<Admin>,
    #[serde(skip_serializing_if = "Option::is_none")]
    dynamic_resources: Option<DynamicResources>,
    static_resources: StaticResources,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Admin {
    address: Address,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Address {
    socket_address: SocketAddress,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SocketAddress {
    address: String,
    port_value: u16,
}

#[derive(Debug, Serialize, Deserialize)]
struct DynamicResources {
    ads_config: AdsConfig,
}

#[derive(Debug, Serialize, Deserialize)]
struct AdsConfig {
    grpc_services: Vec<GrpcService>,
}

#[derive(Debug, Serialize, Deserialize)]
struct GrpcService {
    envoy_grpc: EnvoyGrpc,
}

#[derive(Debug, Serialize, Deserialize)]
struct EnvoyGrpc {
    cluster_name: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct StaticResources {
    listeners: Vec<Value>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    clusters: Vec<Value>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    secrets: Vec<Value>,
}
