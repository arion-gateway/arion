use crate::{
    listeners::http_filters::{FilterDecision, FilterFactory},
    OrionRequestBody, OrionResponseBody,
};
use bitflags::bitflags;
use crossbeam_queue::ArrayQueue;
use http::{Request, Response};
use orion_configuration::config::{
    core::DataSource, network_filters::http_connection_manager::http_filters::wasm::WasmConfig,
};
use orion_interner::StringInterner;
use orion_wasm_types::FilterAction;
use parking_lot::Mutex;
use std::sync::{Arc, LazyLock};
use thiserror::Error;
use tracing::{debug, warn};
use wasmtime::{Engine, Instance, Linker, Module, PoolingAllocationConfig, Store, TypedFunc};

mod hostcalls;
mod shared;
mod types;

const WASM_SHARED_MEMORY_VARIABLES: usize = 1024;
const WASM_INSTANCE_POOL_SIZE: usize = 1024;

#[derive(Error, Debug)]
pub enum WasmError {
    #[error("Wasmtime compilation or execution error: {0}")]
    Wasmtime(#[from] wasmtime::Error),
    #[error("Unsupported data source: {0}")]
    UnsupportedDataSource(String),
    #[error("Initialization error: {0}")]
    InitError(String),
}

// Lazily initialize the global engine and registry on first use
pub static GLOBAL_ENGINE: LazyLock<Engine> = LazyLock::new(|| {
    let mut config = wasmtime::Config::new();
    let mut pooling_config = PoolingAllocationConfig::default();
    pooling_config.total_stacks(1024);
    pooling_config.total_core_instances(1024);
    config.allocation_strategy(wasmtime::InstanceAllocationStrategy::Pooling(pooling_config));
    config.async_support(true);
    Engine::new(&config).unwrap_or_else(|e| {
        warn!("Failed to initialize Wasm Engine with pooling: {}. Falling back to default.", e);
        Engine::default()
    })
});

bitflags! {
    /// Module-level export presence flags. Shared across all pooled instances of a plugin.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    struct HookFlags: u8 {
        const ON_REQUEST_HEADERS      = 1 << 0;
        const ON_REQUEST_BODY         = 1 << 1;
        const ON_RESPONSE_HEADERS     = 1 << 2;
        const ON_RESPONSE_BODY        = 1 << 3;
        const ON_PLUGIN_START         = 1 << 4;
        const ON_PLUGIN_DESTROY       = 1 << 5;
        const ON_TRANSACTION_START    = 1 << 6;
        const ON_TRANSACTION_COMPLETE = 1 << 7;
    }
}

/// Per-instance resolved callback handles. Looked up once at instantiate time.
struct InstanceHooks {
    on_plugin_start: Option<TypedFunc<(), ()>>,
    on_plugin_destroy: Option<TypedFunc<(), ()>>,
    on_transaction_start: Option<TypedFunc<(), ()>>,
    on_transaction_complete: Option<TypedFunc<(), ()>>,
    on_request_headers: Option<TypedFunc<(), i32>>,
    on_request_body: Option<TypedFunc<u32, i32>>,
    on_response_headers: Option<TypedFunc<(), i32>>,
    on_response_body: Option<TypedFunc<u32, i32>>,
}

fn resolve_hook<Params, Results>(
    instance: &Instance,
    store: &mut Store<hostcalls::WasmState>,
    flags: HookFlags,
    flag: HookFlags,
    name: &'static str,
) -> Option<TypedFunc<Params, Results>>
where
    Params: wasmtime::WasmParams,
    Results: wasmtime::WasmResults,
{
    if flags.contains(flag) {
        instance.get_typed_func(store, name).ok()
    } else {
        None
    }
}

impl InstanceHooks {
    /// Resolve every present export into a typed handle. Called once per instance lifetime.
    fn resolve(instance: &Instance, store: &mut Store<hostcalls::WasmState>, flags: HookFlags) -> Self {
        Self {
            on_plugin_start: resolve_hook(instance, store, flags, HookFlags::ON_PLUGIN_START, "on_plugin_start"),
            on_plugin_destroy: resolve_hook(instance, store, flags, HookFlags::ON_PLUGIN_DESTROY, "on_plugin_destroy"),
            on_transaction_start: resolve_hook(
                instance,
                store,
                flags,
                HookFlags::ON_TRANSACTION_START,
                "on_transaction_start",
            ),
            on_transaction_complete: resolve_hook(
                instance,
                store,
                flags,
                HookFlags::ON_TRANSACTION_COMPLETE,
                "on_transaction_complete",
            ),
            on_request_headers: resolve_hook(
                instance,
                store,
                flags,
                HookFlags::ON_REQUEST_HEADERS,
                "on_request_headers",
            ),
            on_request_body: resolve_hook(instance, store, flags, HookFlags::ON_REQUEST_BODY, "on_request_body"),
            on_response_headers: resolve_hook(
                instance,
                store,
                flags,
                HookFlags::ON_RESPONSE_HEADERS,
                "on_response_headers",
            ),
            on_response_body: resolve_hook(instance, store, flags, HookFlags::ON_RESPONSE_BODY, "on_response_body"),
        }
    }
}

// Request-local Wasm execution state
struct WasmFilterState {
    store: Store<hostcalls::WasmState>,
    hooks: InstanceHooks,
}

impl std::fmt::Debug for WasmFilterState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WasmFilterState").finish()
    }
}

impl Drop for WasmFilterState {
    fn drop(&mut self) {
        if let Some(on_destroy) = self.hooks.on_plugin_destroy.clone() {
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                tokio::task::block_in_place(|| {
                    handle.block_on(async {
                        _ = on_destroy.call_async(&mut self.store, ()).await;
                    });
                });
            }
        }
    }
}

#[derive(Clone)]
pub struct WasmFilterInner {
    config: WasmConfig,
    instance_pre: wasmtime::InstancePre<hostcalls::WasmState>,
    instance_pool: Arc<ArrayQueue<WasmFilterState>>,
    hooks: HookFlags,
    shared_memory: Arc<shared::SharedMemory>,
}

impl std::fmt::Debug for WasmFilterInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WasmFilterInner")
            .field("config", &self.config)
            .field("instance_pool", &self.instance_pool)
            .field("hooks", &self.hooks)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub struct WasmFilter {
    inner: Arc<WasmFilterInner>,
    // Mutex is strictly used to satisfy the `Sync` requirement of the compiler.
    // Trick: wWe bypass the lock completely using `.get_mut()` at runtime.
    state: Mutex<Option<WasmFilterState>>,
}

impl Clone for WasmFilter {
    fn clone(&self) -> Self {
        Self { inner: Arc::clone(&self.inner), state: Mutex::new(None) }
    }
}

impl WasmFilter {
    pub fn try_new(config: WasmConfig) -> Result<Self, WasmError> {
        let engine = &*GLOBAL_ENGINE;
        let module = match &config.code {
            DataSource::Path(path) => Module::from_file(engine, path.as_str()).map_err(WasmError::Wasmtime)?,
            DataSource::InlineBytes(bytes) => Module::from_binary(engine, bytes).map_err(WasmError::Wasmtime)?,
            DataSource::InlineString(wat) => Module::new(engine, wat.as_str()).map_err(WasmError::Wasmtime)?,
            DataSource::EnvironmentVariable(env) => {
                return Err(WasmError::UnsupportedDataSource(format!("EnvVar: {env}")));
            },
        };

        let mut linker = Linker::new(engine);
        if let Err(e) = hostcalls::register_hostcalls(&mut linker) {
            return Err(WasmError::InitError(format!("failed to register hostcalls: {e}")));
        }

        let instance_pre = linker.instantiate_pre(&module).map_err(WasmError::Wasmtime)?;

        let mut hooks = HookFlags::empty();
        for export in module.exports() {
            hooks |= match export.name() {
                "on_request_headers" => HookFlags::ON_REQUEST_HEADERS,
                "on_request_body" => HookFlags::ON_REQUEST_BODY,
                "on_response_headers" => HookFlags::ON_RESPONSE_HEADERS,
                "on_response_body" => HookFlags::ON_RESPONSE_BODY,
                "on_plugin_start" => HookFlags::ON_PLUGIN_START,
                "on_plugin_destroy" => HookFlags::ON_PLUGIN_DESTROY,
                "on_transaction_start" => HookFlags::ON_TRANSACTION_START,
                "on_transaction_complete" => HookFlags::ON_TRANSACTION_COMPLETE,
                _ => HookFlags::empty(),
            };
        }

        // Pre-allocate a lock-free queue for hot instances
        let pool = Arc::new(ArrayQueue::new(WASM_INSTANCE_POOL_SIZE));

        Ok(Self {
            inner: Arc::new(WasmFilterInner {
                config,
                instance_pre,
                instance_pool: pool,
                hooks,
                shared_memory: Arc::new(shared::SharedMemory::new(WASM_SHARED_MEMORY_VARIABLES)),
            }),
            state: Mutex::new(None),
        })
    }

    async fn get_state(&mut self) -> Result<&mut WasmFilterState, WasmError> {
        // Use get_mut() to safely access the data bypassing the lock!
        // Zero locking overhead since we have &mut self.
        let state_opt = self.state.get_mut();

        if state_opt.is_none() {
            // Lock-free extraction from the pool
            let state = if let Some(s) = self.inner.instance_pool.pop() {
                s
            } else {
                let engine = &*GLOBAL_ENGINE;
                let plugin_config = self.inner.config.configuration.clone();

                let mut store = Store::new(
                    engine,
                    hostcalls::WasmState {
                        name: self.inner.config.name.to_static_str(),
                        plugin_config,
                        direct_response: None,
                        buffered_request_body: None,
                        buffered_response_body: None,
                        request_trailers: None,
                        response_trailers: None,
                        access_log_operators: Vec::new(),
                        io_deadline: None,
                        shared_memory: Arc::clone(&self.inner.shared_memory),
                        active_request_handle: None,
                        active_response_handle: None,
                        memory: None,
                    },
                );
                // Instantiate the module using the pre-resolved imports
                let instance = match self.inner.instance_pre.instantiate_async(&mut store).await {
                    Ok(inst) => inst,
                    Err(e) => return Err(WasmError::InitError(format!("failed to instantiate module: {e}"))),
                };

                // Cache guest linear memory once for all subsequent hostcalls.
                store.data_mut().memory = instance.get_memory(&mut store, "memory");

                let hooks = InstanceHooks::resolve(&instance, &mut store, self.inner.hooks);

                if let Some(on_start) = &hooks.on_plugin_start {
                    _ = on_start.call_async(&mut store, ()).await;
                }

                WasmFilterState { store, hooks }
            };
            *state_opt = Some(state);
        }

        state_opt.as_mut().ok_or_else(|| WasmError::InitError("Wasm state is uninitialized".to_owned()))
    }

    #[allow(clippy::too_many_lines)]
    pub async fn apply_request(&mut self, req: &mut Request<OrionRequestBody>) -> FilterDecision {
        debug!("WasFilter::apply_request: {:?}", self.inner.config);

        if self.inner.hooks.contains(HookFlags::ON_TRANSACTION_START) {
            let state = match self.get_state().await {
                Ok(s) => s,
                Err(e) => return FilterDecision::internal_server_error(&e.to_string(), req.version()),
            };
            if let Some(on_tx_start) = &state.hooks.on_transaction_start {
                _ = on_tx_start.call_async(&mut state.store, ()).await;
            }
        }

        if !self.inner.hooks.intersects(HookFlags::ON_REQUEST_HEADERS | HookFlags::ON_REQUEST_BODY) {
            return FilterDecision::Continue;
        }

        let req_handle = std::ptr::from_mut::<Request<OrionRequestBody>>(req) as u64;
        if let Ok(s) = self.get_state().await {
            s.store.data_mut().active_response_handle = None;
            s.store.data_mut().active_request_handle = Some(req_handle);
        }

        // PHASE 1: headers evaluation
        let action_code: Result<FilterAction, WasmError> = if self.inner.hooks.contains(HookFlags::ON_REQUEST_HEADERS) {
            let state = match self.get_state().await {
                Ok(s) => s,
                Err(e) => return FilterDecision::internal_server_error(&e.to_string(), req.version()),
            };

            if let Some(on_headers) = &state.hooks.on_request_headers {
                let response = on_headers.call_async(&mut state.store, ()).await;
                response.map_err(WasmError::Wasmtime).and_then(|v| {
                    #[allow(clippy::map_err_ignore)]
                    FilterAction::try_from(v)
                        .map_err(|_| WasmError::InitError(format!("Invalid Wasm FilterAction code: {v}")))
                })
            } else {
                Ok(FilterAction::Continue)
            }
        } else {
            Ok(FilterAction::PauseAndBufferBody) // Implicitly return PauseAndBufferBody if the plugin only implements the body hook
        };

        let decision = match action_code {
            Ok(FilterAction::Continue) => FilterDecision::Continue,
            Ok(FilterAction::PauseAndBufferBody) => {
                // PauseAndBufferBody
                use crate::body::poly_body::PolyBody;
                use crate::body::timeout_body::TimeoutBody;
                use http_body_util::{BodyExt, Full};

                // 1. Buffer the entire body asynchronously
                let Ok(collected) = req.body_mut().collect().await else {
                    return FilterDecision::internal_server_error("Failed to collect body", req.version());
                };
                let trailers = collected.trailers().cloned();
                let full_body_bytes = collected.to_bytes();

                // 2. We consumed the inner stream, we need to swap the inner PolyBody
                let old_body = std::mem::take(req.body_mut());

                *req.body_mut() = old_body.map_inner(|old_timeout_body| {
                    let old_timeout = old_timeout_body.timeout;
                    let mut pb = PolyBody::from(Full::from(full_body_bytes.clone()));
                    if let Some(t) = trailers.clone() {
                        pb =
                            pb.with_trailers(t).unwrap_or_else(|_| PolyBody::from(Full::from(full_body_bytes.clone())));
                    }
                    TimeoutBody::new(old_timeout, pb)
                });

                // 3. Re-enter the Wasm context to invoke on_request_body
                let body_action_code: Result<FilterAction, WasmError> =
                    if self.inner.hooks.contains(HookFlags::ON_REQUEST_BODY) {
                        let state = match self.get_state().await {
                            Ok(s) => s,
                            Err(e) => return FilterDecision::internal_server_error(&e.to_string(), req.version()),
                        };

                        let response = if let Some(on_body) = &state.hooks.on_request_body {
                            state.store.data_mut().buffered_request_body = Some(full_body_bytes.clone());
                            state.store.data_mut().request_trailers = trailers;

                            let response = on_body
                                .call_async(&mut state.store, u32::try_from(full_body_bytes.len()).unwrap_or(0))
                                .await;

                            let mutated_body = state.store.data_mut().buffered_request_body.take();
                            let mutated_trailers = state.store.data_mut().request_trailers.take();

                            if mutated_body.is_some() || mutated_trailers.is_some() {
                                let final_body = mutated_body.unwrap_or_else(|| full_body_bytes.clone());
                                if req.headers().contains_key(http::header::CONTENT_LENGTH) {
                                    req.headers_mut().insert(
                                        http::header::CONTENT_LENGTH,
                                        http::header::HeaderValue::from_str(&final_body.len().to_string()).unwrap(),
                                    );
                                }
                                let old_body = std::mem::take(req.body_mut());
                                *req.body_mut() = old_body.map_inner(|old_timeout_body| {
                                    let old_timeout = old_timeout_body.timeout;
                                    let mut pb = PolyBody::from(Full::from(final_body.clone()));
                                    if let Some(t) = mutated_trailers {
                                        pb = pb
                                            .with_trailers(t)
                                            .unwrap_or_else(|_| PolyBody::from(Full::from(final_body)));
                                    }
                                    TimeoutBody::new(old_timeout, pb)
                                });
                            }

                            response.map_err(WasmError::Wasmtime)
                        } else {
                            Ok(FilterAction::Continue.into())
                        };

                        response.and_then(|v| {
                            #[allow(clippy::map_err_ignore)]
                            FilterAction::try_from(v)
                                .map_err(|_| WasmError::InitError(format!("Invalid body action code: {v}")))
                        })
                    } else {
                        Ok(FilterAction::Continue)
                    };

                match body_action_code {
                    Ok(FilterAction::Continue) => FilterDecision::Continue, // Continue
                    Ok(FilterAction::DirectResponse) => {
                        // DirectResponse
                        let state = match self.get_state().await {
                            Ok(s) => s,
                            Err(e) => return FilterDecision::internal_server_error(&e.to_string(), req.version()),
                        };
                        let direct_resp = state.store.data_mut().direct_response.take();
                        if let Some(mut response) = direct_resp {
                            *response.version_mut() = req.version();
                            FilterDecision::DirectResponse(Box::new(response))
                        } else {
                            warn!(
                                "wasm plugin returned DirectResponse from \
                                 on_request_body without calling \
                                 send_direct_response"
                            );
                            FilterDecision::internal_server_error(
                                "DirectResponse without send_direct_response",
                                req.version(),
                            )
                        }
                    },
                    Ok(action) => FilterDecision::internal_server_error(
                        &format!("Invalid body action code: {action:?}"),
                        req.version(),
                    ),
                    Err(e) => FilterDecision::internal_server_error(&e.to_string(), req.version()),
                }
            },

            Ok(FilterAction::DirectResponse) => {
                // DirectResponse
                let state = match self.get_state().await {
                    Ok(s) => s,
                    Err(e) => return FilterDecision::internal_server_error(&e.to_string(), req.version()),
                };
                let direct_resp = state.store.data_mut().direct_response.take();

                if let Some(mut response) = direct_resp {
                    *response.version_mut() = req.version();
                    FilterDecision::DirectResponse(Box::new(response))
                } else {
                    warn!(
                        "wasm plugin returned DirectResponse from \
                         on_request_headers without calling \
                         send_direct_response"
                    );
                    FilterDecision::internal_server_error("DirectResponse without send_direct_response", req.version())
                }
            },

            Err(e) => FilterDecision::internal_server_error(&e.to_string(), req.version()),
        };

        self.extract_and_apply_access_log_operators(req.extensions());

        decision
    }

    #[allow(unused_variables)]
    fn extract_and_apply_access_log_operators(&mut self, extensions: &http::Extensions) {
        if let Some(state) = self.state.get_mut() {
            let ops = std::mem::take(&mut state.store.data_mut().access_log_operators);
            if !ops.is_empty() {
                #[cfg(all(feature = "access-log", feature = "metrics"))]
                if let Some(ctx) =
                    extensions.get::<std::sync::Arc<crate::listeners::http_connection_manager::TransactionContext>>()
                {
                    let mut kv = orion_metrics::key_value::KeyValueMap::default();
                    for (k, v) in &ops {
                        kv.insert(k.as_str(), v.as_str());
                    }
                    _ = crate::access_log::evaluate_plain_access_log_hook(
                        crate::access_log::AccessLogHook::Wasm,
                        &kv,
                        &mut ctx.trans_state.lock().loggers,
                    );
                }
            }
        }
    }

    #[allow(clippy::too_many_lines)]
    pub async fn apply_response(&mut self, response: &mut Response<OrionResponseBody>) -> FilterDecision {
        debug!("WasFilter::apply_response: {:?}", self.inner.config);
        if !self.inner.hooks.intersects(HookFlags::ON_RESPONSE_HEADERS | HookFlags::ON_RESPONSE_BODY) {
            return FilterDecision::Continue;
        }

        let resp_handle = std::ptr::from_mut::<Response<OrionResponseBody>>(response) as u64;
        if let Ok(s) = self.get_state().await {
            s.store.data_mut().active_request_handle = None;
            s.store.data_mut().active_response_handle = Some(resp_handle);
        }

        // PHASE 1: headers evaluation
        let action_code: Result<FilterAction, WasmError> = if self.inner.hooks.contains(HookFlags::ON_RESPONSE_HEADERS)
        {
            let state = match self.get_state().await {
                Ok(s) => s,
                Err(e) => return FilterDecision::internal_server_error(&e.to_string(), response.version()),
            };

            if let Some(on_response_headers) = &state.hooks.on_response_headers {
                let res_val = on_response_headers.call_async(&mut state.store, ()).await;
                res_val.map_err(WasmError::Wasmtime).and_then(|v| {
                    #[allow(clippy::map_err_ignore)]
                    FilterAction::try_from(v)
                        .map_err(|_| WasmError::InitError(format!("Invalid Wasm response FilterAction code: {v}")))
                })
            } else {
                Ok(FilterAction::Continue)
            }
        } else {
            Ok(FilterAction::PauseAndBufferBody) // Implicitly return PauseAndBufferBody if the plugin only implements the body hook
        };

        let decision = match action_code {
            Ok(FilterAction::Continue) => FilterDecision::Continue,

            Ok(FilterAction::PauseAndBufferBody) => {
                // PauseAndBufferBody for response
                use crate::body::poly_body::PolyBody;
                use http_body_util::{BodyExt, Full};

                // 1. Buffer response body
                let Ok(collected) = response.body_mut().collect().await else {
                    return FilterDecision::internal_server_error(
                        "Failed to collect response body",
                        response.version(),
                    );
                };
                let trailers = collected.trailers().cloned();
                let full_body_bytes = collected.to_bytes();

                // 2. Swap response body
                let old_body = std::mem::take(response.body_mut());
                *response.body_mut() = old_body.map_inner(|_old_poly_body| {
                    let mut pb = PolyBody::from(Full::from(full_body_bytes.clone()));
                    if let Some(t) = trailers.clone() {
                        pb =
                            pb.with_trailers(t).unwrap_or_else(|_| PolyBody::from(Full::from(full_body_bytes.clone())));
                    }
                    pb
                });

                // 3. Invoke on_response_body
                let body_action_code: Result<FilterAction, WasmError> =
                    if self.inner.hooks.contains(HookFlags::ON_RESPONSE_BODY) {
                        let state = match self.get_state().await {
                            Ok(s) => s,
                            Err(e) => return FilterDecision::internal_server_error(&e.to_string(), response.version()),
                        };

                        let res_val = if let Some(on_body) = &state.hooks.on_response_body {
                            state.store.data_mut().buffered_response_body = Some(full_body_bytes.clone());
                            state.store.data_mut().response_trailers = trailers;

                            let res_val = on_body
                                .call_async(&mut state.store, u32::try_from(full_body_bytes.len()).unwrap_or(0))
                                .await;

                            let mutated_body = state.store.data_mut().buffered_response_body.take();
                            let mutated_trailers = state.store.data_mut().response_trailers.take();

                            if mutated_body.is_some() || mutated_trailers.is_some() {
                                let final_body = mutated_body.unwrap_or_else(|| full_body_bytes.clone());
                                if response.headers().contains_key(http::header::CONTENT_LENGTH) {
                                    response.headers_mut().insert(
                                        http::header::CONTENT_LENGTH,
                                        http::header::HeaderValue::from_str(&final_body.len().to_string()).unwrap(),
                                    );
                                }
                                let old_body = std::mem::take(response.body_mut());
                                *response.body_mut() = old_body.map_inner(|_old_poly_body| {
                                    let mut pb = PolyBody::from(Full::from(final_body.clone()));
                                    if let Some(t) = mutated_trailers {
                                        pb = pb
                                            .with_trailers(t)
                                            .unwrap_or_else(|_| PolyBody::from(Full::from(final_body)));
                                    }
                                    pb
                                });
                            }

                            res_val.map_err(WasmError::Wasmtime)
                        } else {
                            Ok(FilterAction::Continue.into())
                        };

                        res_val.and_then(|v| {
                            #[allow(clippy::map_err_ignore)]
                            FilterAction::try_from(v)
                                .map_err(|_| WasmError::InitError(format!("Invalid response body action code: {v}")))
                        })
                    } else {
                        Ok(FilterAction::Continue)
                    };

                match body_action_code {
                    Ok(FilterAction::Continue) => FilterDecision::Continue,
                    Ok(FilterAction::DirectResponse) => {
                        // DirectResponse
                        let state = match self.get_state().await {
                            Ok(s) => s,
                            Err(e) => return FilterDecision::internal_server_error(&e.to_string(), response.version()),
                        };
                        let direct_resp = state.store.data_mut().direct_response.take();
                        if let Some(mut response) = direct_resp {
                            *response.version_mut() = response.version();
                            FilterDecision::DirectResponse(Box::new(response))
                        } else {
                            warn!(
                                "wasm plugin returned DirectResponse from \
                                 on_response_body without calling \
                                 send_direct_response"
                            );
                            FilterDecision::internal_server_error(
                                "DirectResponse without send_direct_response",
                                response.version(),
                            )
                        }
                    },
                    Ok(action) => FilterDecision::internal_server_error(
                        &format!("Invalid response body action code: {action:?}"),
                        response.version(),
                    ),
                    Err(e) => FilterDecision::internal_server_error(&e.to_string(), response.version()),
                }
            },

            Ok(FilterAction::DirectResponse) => {
                // DirectResponse
                let state = match self.get_state().await {
                    Ok(s) => s,
                    Err(e) => return FilterDecision::internal_server_error(&e.to_string(), response.version()),
                };
                let direct_resp = state.store.data_mut().direct_response.take();

                if let Some(mut response) = direct_resp {
                    *response.version_mut() = response.version();
                    FilterDecision::DirectResponse(Box::new(response))
                } else {
                    warn!(
                        "wasm plugin returned DirectResponse from \
                         on_response_headers without calling \
                         send_direct_response"
                    );
                    FilterDecision::internal_server_error(
                        "DirectResponse without send_direct_response",
                        response.version(),
                    )
                }
            },

            Err(e) => FilterDecision::internal_server_error(&e.to_string(), response.version()),
        };

        self.extract_and_apply_access_log_operators(response.extensions());

        decision
    }
}

impl Drop for WasmFilter {
    fn drop(&mut self) {
        if let Some(mut state) = self.state.get_mut().take() {
            if let Some(on_tx_comp) = state.hooks.on_transaction_complete.clone() {
                if let Ok(handle) = tokio::runtime::Handle::try_current() {
                    tokio::task::block_in_place(|| {
                        handle.block_on(async {
                            _ = on_tx_comp.call_async(&mut state.store, ()).await;
                        });
                    });
                }
            }
            let data = state.store.data_mut();
            data.direct_response = None;
            data.buffered_request_body = None;
            data.buffered_response_body = None;
            data.io_deadline = None;
            data.active_request_handle = None;
            data.active_response_handle = None;
            _ = self.inner.instance_pool.push(state);
        }
    }
}

impl FilterFactory for WasmFilter {
    fn new_from(&self) -> Self {
        self.clone()
    }
}
