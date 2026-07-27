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
use orion_stats::{set_proxy_state, ProxyState};

#[macro_use]
mod admin;
mod core_affinity;
mod proxy;
mod runtime;
mod xds_configurator;

pub fn run() -> bool {
    let Ok(mut tracing_manager) = proxy_tracing::TracingManager::new() else {
        return false;
    };

    let result = (|| -> Result<()> {
        let options = Options::parse_options();
        let Config { runtime, logging, access_log_config, metrics, timezone, bootstrap } = Config::new(&options)?;

        tracing_manager.update(logging)?;

        set_proxy_state(ProxyState::Initializing);

        RUNTIME_CONFIG.set(runtime).map_err(|_e| "runtime config was somehow set before we had a chance to set it")?;

        // Set the header_name from which to extract the user_id
        if let Some(source) =
            metrics.as_ref().and_then(|metrics| metrics.user_key.as_ref()).map(|key| &key.source).cloned()
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

        #[cfg(target_os = "linux")]
        if !(caps::has_cap(None, caps::CapSet::Permitted, caps::Capability::CAP_NET_RAW)?) {
            tracing::warn!("CAP_NET_RAW is NOT available, SO_BINDTODEVICE will not work");
        }

        proxy::run_orion(bootstrap, metrics, access_log_config, timezone)?;
        Ok(())
    })();

    if let Err(e) = result {
        tracing::error!("Orion proxy terminated with error: {e:?}");
        return false;
    }
    true
}

mod proxy_tracing {
    use tracing_appender::non_blocking::{NonBlocking, WorkerGuard};
    use tracing_subscriber::{
        fmt, fmt::format::DefaultFields, layer::Layered, reload, reload::Handle, EnvFilter, Registry,
    };

    use orion_configuration::config::{DesensitizationConfig, LogConfig as LogConf};
    use orion_lib::Result;
    use orion_log::{Desensitizer, MakeWriterType, RedactingMakeWriter};

    #[derive(Clone, Default)]
    struct CustomTime;

    impl fmt::time::FormatTime for CustomTime {
        fn format_time(&self, w: &mut fmt::format::Writer<'_>) -> std::fmt::Result {
            let offset_sec = orion_lib::timezone::local_offset_sec().unwrap_or(0);
            let tz =
                chrono::FixedOffset::east_opt(offset_sec).unwrap_or_else(|| chrono::FixedOffset::east_opt(0).unwrap());
            let now = chrono::Utc::now().with_timezone(&tz);
            write!(w, "{}", now.format("%Y-%m-%dT%H:%M:%S%.6f%z"))
        }
    }

    type RegistryLayer = fmt::Layer<
        Layered<reload::Layer<EnvFilter, Registry>, Registry>,
        DefaultFields,
        fmt::format::Format<fmt::format::Full, CustomTime>,
        MakeWriterType<NonBlocking>,
    >;
    type FilterReloadHandle = Handle<EnvFilter, Registry>;
    type LayerReloadHandle = Handle<
        fmt::Layer<
            Layered<reload::Layer<EnvFilter, Registry>, Registry>,
            DefaultFields,
            fmt::format::Format<fmt::format::Full, CustomTime>,
            MakeWriterType<NonBlocking>,
        >,
        Layered<reload::Layer<EnvFilter, Registry>, Registry>,
    >;

    pub struct TracingManager {
        guard: WorkerGuard,
        layer_reload_handle: LayerReloadHandle,
        filter_reload_handle: FilterReloadHandle,
    }

    impl TracingManager {
        pub fn new() -> Result<Self> {
            let level = EnvFilter::builder()
                .with_default_directive(tracing_subscriber::filter::LevelFilter::INFO.into())
                .parse_lossy("");
            let (guard, layer_reload_handle, filter_reload_handle) =
                Self::init_tracing(Registry::default(), level, &DesensitizationConfig::default())?;
            Ok(TracingManager { guard, filter_reload_handle, layer_reload_handle })
        }

        pub fn update(&mut self, log_conf: LogConf) -> Result<()> {
            let LogConf { log_level, log_directory, log_file, desensitization } = log_conf;

            self.filter_reload_handle.modify(|filter| {
                *filter = EnvFilter::try_from_default_env().ok().or(log_level).unwrap_or_else(|| {
                    EnvFilter::builder()
                        .with_default_directive(tracing_subscriber::filter::LevelFilter::ERROR.into())
                        .parse_lossy("")
                });
            })?;

            // Build the new layer before entering the modify closure so that
            // errors from Desensitizer::build() can be propagated with `?`.
            let (new_guard, new_layer) = if let Some(ref file) = log_file {
                Self::file_layer(file, log_directory.as_ref(), &desensitization)?
            } else {
                Self::stdout_layer(&desensitization)?
            };

            self.layer_reload_handle.modify(|layer| {
                *layer = new_layer;
            })?;
            self.guard = new_guard;

            Ok(())
        }

        fn init_tracing(
            registry: Registry,
            log_level: EnvFilter,
            desensitization: &DesensitizationConfig,
        ) -> Result<(WorkerGuard, LayerReloadHandle, FilterReloadHandle)> {
            use tracing_subscriber::{layer::SubscriberExt as _, util::SubscriberInitExt as _};

            let env_filter = EnvFilter::try_from_default_env().ok().or(Some(log_level)).unwrap_or_else(|| {
                EnvFilter::builder()
                    .with_default_directive(tracing_subscriber::filter::LevelFilter::ERROR.into())
                    .parse_lossy("")
            });

            let (guard, layer) = Self::stdout_layer(desensitization)?;
            let (layer, layer_reload_handle) = reload::Layer::new(layer);
            let (env_filter, filter_reload_handle) = reload::Layer::new(env_filter);

            registry.with(env_filter).with(layer).init();
            Ok((guard, layer_reload_handle, filter_reload_handle))
        }

        fn make_writer(
            desensitization: &DesensitizationConfig,
            inner: NonBlocking,
        ) -> Result<MakeWriterType<NonBlocking>> {
            if std::env::var_os("ORION_DEBUG").is_some() {
                return Ok(MakeWriterType::passthrough(inner));
            }
            let desensitizer = Desensitizer::builder(
                desensitization.log_max_length,
                desensitization.replace_length,
                desensitization.ignore_tag.clone(),
            )
            .allow_list(desensitization.allow_list.clone())
            .build()?;
            Ok(MakeWriterType::Redacting(RedactingMakeWriter::with_desensitizer(inner, desensitizer)))
        }

        fn stdout_layer(desensitization: &DesensitizationConfig) -> Result<(WorkerGuard, RegistryLayer)> {
            let out = std::io::stdout();
            let is_terminal = std::io::IsTerminal::is_terminal(&out);
            let (non_blocking, guard) = tracing_appender::non_blocking(out);
            let writer = Self::make_writer(desensitization, non_blocking)?;
            let mut std_layer = fmt::layer().with_timer(CustomTime).with_writer(writer).with_thread_names(true);

            if !is_terminal {
                std_layer = std_layer.with_ansi(false);
            }

            Ok((guard, std_layer))
        }

        fn file_layer(
            filename: &str,
            log_directory: Option<&String>,
            desensitization: &DesensitizationConfig,
        ) -> Result<(WorkerGuard, RegistryLayer)> {
            let file_appender = tracing_appender::rolling::hourly(log_directory.unwrap_or(&".".into()), filename);
            let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);
            let writer = Self::make_writer(desensitization, non_blocking)?;
            let file_layer =
                fmt::layer().with_timer(CustomTime).with_ansi(false).with_writer(writer).with_thread_names(true);

            Ok((guard, file_layer))
        }
    }
}
