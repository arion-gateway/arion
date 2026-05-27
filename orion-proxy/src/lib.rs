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

use orion_configuration::{config::Config, options::Options};
use orion_lib::{metrics, Result, RUNTIME_CONFIG};

#[macro_use]
mod admin;
mod core_affinity;
mod proxy;
mod runtime;
mod xds_configurator;

pub fn run() -> Result<()> {
    let mut tracing_manager = proxy_tracing::TracingManager::new();

    let options = Options::parse_options();
    let Config { runtime, logging, access_logging, metrics, bootstrap } = Config::new(&options)?;

    RUNTIME_CONFIG.set(runtime).map_err(|_e| "runtime config was somehow set before we had a chance to set it")?;

    // Set the header_name from which to extract the user_id
    //
    if let Some(source) = metrics.as_ref().and_then(|metrics| metrics.user_key.as_ref()).map(|key| &key.source).cloned()
    {
        metrics::USER_KEY.set_source(source);
    }

    // Set the header_names and attribute_names from which to extract the custom keys
    if let Some(metrics_config) = metrics.as_ref() {
        let mut custom_keys = Vec::with_capacity(metrics_config.custom_keys.len());
        for key in &metrics_config.custom_keys {
            let pk = metrics::PartitionKey::new();
            pk.set_source(key.source.clone());
            if let Some(attribute_name) = &key.attribute_name {
                pk.set_attribute_name(attribute_name.clone());
            }
            custom_keys.push(pk);
        }
        let _ = metrics::CUSTOM_KEYS.set(custom_keys).ok();
    }

    // Set the attribute key value used to partition user metrics.
    if let Some(attribute_name) = metrics
        .as_ref()
        .and_then(|metrics| metrics.user_key.as_ref())
        .and_then(|key| key.attribute_name.as_ref())
        .cloned()
    {
        metrics::USER_KEY.set_attribute_name(attribute_name);
    }

    tracing_manager.update(logging)?;

    #[cfg(target_os = "linux")]
    if !(caps::has_cap(None, caps::CapSet::Permitted, caps::Capability::CAP_NET_RAW)?) {
        tracing::warn!("CAP_NET_RAW is NOT available, SO_BINDTODEVICE will not work");
    }

    proxy::run_orion(bootstrap, metrics, access_logging);
    Ok(())
}

mod proxy_tracing {
    use tracing_appender::non_blocking::{NonBlocking, WorkerGuard};
    use tracing_subscriber::{
        fmt,
        fmt::format::{DefaultFields, Format},
        layer::Layered,
        reload,
        reload::Handle,
        EnvFilter, Registry,
    };

    use orion_configuration::config::LogConfig as LogConf;
    use orion_lib::Result;

    type RegistryLayer =
        fmt::Layer<Layered<reload::Layer<EnvFilter, Registry>, Registry>, DefaultFields, Format, NonBlocking>;
    type FilterReloadHandle = Handle<EnvFilter, Registry>;
    type LayerReloadHandle = Handle<
        fmt::Layer<Layered<reload::Layer<EnvFilter, Registry>, Registry>, DefaultFields, Format, NonBlocking>,
        Layered<reload::Layer<EnvFilter, Registry>, Registry>,
    >;

    pub struct TracingManager {
        guard: WorkerGuard,
        layer_reload_handle: LayerReloadHandle,
        filter_reload_handle: FilterReloadHandle,
    }

    impl TracingManager {
        pub fn new() -> Self {
            let level = EnvFilter::builder()
                .with_default_directive(tracing_subscriber::filter::LevelFilter::INFO.into())
                .parse_lossy("");
            let (guard, layer_reload_handle, filter_reload_handle) = Self::init_tracing(Registry::default(), level);
            TracingManager { guard, filter_reload_handle, layer_reload_handle }
        }

        pub fn update(&mut self, log_conf: LogConf) -> Result<()> {
            // Update log level
            self.filter_reload_handle.modify(|filter| {
                *filter = EnvFilter::try_from_default_env().ok().or(log_conf.log_level).unwrap_or_else(|| {
                    EnvFilter::builder()
                        .with_default_directive(tracing_subscriber::filter::LevelFilter::ERROR.into())
                        .parse_lossy("")
                });
            })?;

            // Update tracing layer if necessary (stdout -> file)
            if let Some(log_file) = log_conf.log_file {
                self.layer_reload_handle.modify(|layer| {
                    let (new_guard, new_layer) = Self::file_layer(&log_file, log_conf.log_directory.as_ref());
                    *layer = new_layer;
                    self.guard = new_guard;
                })?;
            }

            Ok(())
        }

        fn init_tracing(
            registry: Registry,
            log_level: EnvFilter,
        ) -> (WorkerGuard, LayerReloadHandle, FilterReloadHandle) {
            use tracing_subscriber::{layer::SubscriberExt as _, util::SubscriberInitExt as _};

            let env_filter = EnvFilter::try_from_default_env().ok().or(Some(log_level)).unwrap_or_else(|| {
                EnvFilter::builder()
                    .with_default_directive(tracing_subscriber::filter::LevelFilter::ERROR.into())
                    .parse_lossy("")
            });

            // Start as an stdout layer by default, after reading the configuration this can be upgraded to a file layer
            let (guard, layer) = Self::stdout_layer();
            let (layer, layer_reload_handle) = reload::Layer::new(layer);

            let (env_filter, filter_reload_handle) = reload::Layer::new(env_filter);

            registry.with(env_filter).with(layer).init();
            (guard, layer_reload_handle, filter_reload_handle)
        }

        fn stdout_layer() -> (WorkerGuard, RegistryLayer) {
            let out = std::io::stdout();
            let is_terminal = std::io::IsTerminal::is_terminal(&out);
            let (non_blocking, guard) = tracing_appender::non_blocking(out);
            let mut std_layer = fmt::layer().with_writer(non_blocking).with_thread_names(true);

            if !is_terminal {
                std_layer = std_layer.with_ansi(false);
            }

            (guard, std_layer)
        }

        fn file_layer(filename: &str, log_directory: Option<&String>) -> (WorkerGuard, RegistryLayer) {
            let file_appender = tracing_appender::rolling::hourly(log_directory.unwrap_or(&".".into()), filename);
            let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);
            let file_layer = fmt::layer().with_ansi(false).with_writer(non_blocking).with_thread_names(true);

            (guard, file_layer)
        }
    }
}
