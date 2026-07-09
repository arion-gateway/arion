use crate::{
    listeners::http_filters::{FilterDecision, FilterFactory},
    OrionRequestBody, OrionResponseBody,
};
use crossbeam_queue::ArrayQueue;
use http::{Request, Response};
use orion_configuration::config::{
    core::DataSource, network_filters::http_connection_manager::http_filters::wasm::WasmConfig,
};
use orion_interner::StringInterner;
use parking_lot::Mutex;
use std::sync::{Arc, LazyLock};
use thiserror::Error;
use tracing::{debug, warn};
use wasmtime::{Engine, Instance, Linker, Module, Store};

mod hostcalls;
mod types;

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
    config.allocation_strategy(wasmtime::InstanceAllocationStrategy::pooling());
    Engine::new(&config).unwrap_or_else(|e| {
        warn!("Failed to initialize Wasm Engine with pooling: {}. Falling back to default.", e);
        Engine::default()
    })
});

// Request-local Wasm execution state
struct WasmFilterState {
    store: Store<hostcalls::WasmState>,
    instance: Instance,
}

impl std::fmt::Debug for WasmFilterState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WasmFilterState").finish()
    }
}

#[derive(Clone)]
pub struct WasmFilterInner {
    config: WasmConfig,
    instance_pre: wasmtime::InstancePre<hostcalls::WasmState>,
    instance_pool: Arc<ArrayQueue<WasmFilterState>>,
    has_on_request_headers: bool,
    has_on_request_body: bool,
    has_on_response_headers: bool,
    has_on_response_body: bool,
}

impl std::fmt::Debug for WasmFilterInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WasmFilterInner").field("config", &self.config).finish()
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
        Self { inner: self.inner.clone(), state: Mutex::new(None) }
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
                return Err(WasmError::UnsupportedDataSource(format!("EnvVar: {}", env)));
            },
        };

        let mut linker = Linker::new(engine);
        if let Err(e) = hostcalls::register_hostcalls(&mut linker) {
            return Err(WasmError::InitError(format!("failed to register hostcalls: {}", e)));
        }

        let instance_pre = linker.instantiate_pre(&module).map_err(WasmError::Wasmtime)?;

        let mut has_on_request_headers = false;
        let mut has_on_request_body = false;
        let mut has_on_response_headers = false;
        let mut has_on_response_body = false;

        for export in module.exports() {
            match export.name() {
                "on_request_headers" => has_on_request_headers = true,
                "on_request_body" => has_on_request_body = true,
                "on_response_headers" => has_on_response_headers = true,
                "on_response_body" => has_on_response_body = true,
                _ => {}
            }
        }

        // Preallocate a lock-free queue for hot instances (max 1024 capacity)
        let pool = Arc::new(ArrayQueue::new(1024));

        Ok(Self {
            inner: Arc::new(WasmFilterInner {
                config,
                instance_pre,
                instance_pool: pool,
                has_on_request_headers,
                has_on_request_body,
                has_on_response_headers,
                has_on_response_body,
            }),
            state: Mutex::new(None)
        })
    }

    fn get_state(&mut self) -> Result<&mut WasmFilterState, WasmError> {
        // Use get_mut() to safely access the data bypassing the lock!
        // Zero locking overhead since we have &mut self.
        let state_opt = self.state.get_mut();

        if state_opt.is_none() {
            // Lock-free extraction from the pool
            let state = match self.inner.instance_pool.pop() {
                Some(s) => s,
                None => {
                    let engine = &*GLOBAL_ENGINE;
                    let mut store = Store::new(
                        engine,
                        hostcalls::WasmState {
                            name: self.inner.config.name.to_static_str(),
                            direct_response: None,
                            buffered_request_body: None,
                            buffered_response_body: None,
                        },
                    );
                    // Instantiate the module using the pre-resolved imports
                    let instance = match self.inner.instance_pre.instantiate(&mut store) {
                        Ok(inst) => inst,
                        Err(e) => return Err(WasmError::InitError(format!("failed to instantiate module: {}", e))),
                    };

                    WasmFilterState { store, instance }
                }
            };
            *state_opt = Some(state);
        }

        state_opt.as_mut().ok_or_else(|| WasmError::InitError("Wasm state is uninitialized".to_string()))
    }

    pub async fn apply_request(&mut self, req: &mut Request<OrionRequestBody>) -> FilterDecision {
        debug!("WasFilter::apply_request: {:?}", self.inner.config);
        if !self.inner.has_on_request_headers && !self.inner.has_on_request_body {
            return FilterDecision::Continue;
        }

        let req_handle = req as *mut Request<OrionRequestBody> as u64;

        // PHASE 1: headers evaluation
        let action_code: Result<i32, WasmError> = if self.inner.has_on_request_headers {
            let state = match self.get_state() {
                Ok(s) => s,
                Err(e) => return FilterDecision::internal_server_error(&e.to_string(), req.version()),
            };

            if let Ok(on_headers) = state.instance.get_typed_func::<u64, i32>(&mut state.store, "on_request_headers") {
                let res = on_headers.call(&mut state.store, req_handle);
                res.map_err(WasmError::Wasmtime)
            } else {
                Ok(types::FilterAction::Continue.into())
            }
        } else {
            Ok(1) // Implicitly return PauseAndBufferBody if the plugin only implements the body hook
        };

        match action_code {
            Ok(0) => FilterDecision::Continue,

            Ok(1) => {
                // PauseAndBufferBody
                use crate::body::poly_body::PolyBody;
                use crate::body::timeout_body::TimeoutBody;
                use http_body_util::{BodyExt, Full};

                // 1. Buffer the entire body asynchronously
                let full_body_bytes = match req.body_mut().collect().await {
                    Ok(collected) => collected.to_bytes(),
                    Err(_) => {
                        return FilterDecision::internal_server_error("Failed to collect body", req.version());
                    },
                };

                // 2. We consumed the inner stream, we need to swap the inner PolyBody
                let old_body = std::mem::take(req.body_mut());

                *req.body_mut() = old_body.map_inner(|old_timeout_body| {
                    let old_timeout = old_timeout_body.timeout;
                    TimeoutBody::new(old_timeout, PolyBody::from(Full::from(full_body_bytes.clone())))
                });

                // 3. Re-enter the Wasm context to invoke on_request_body
                let body_action_code: Result<i32, WasmError> = if self.inner.has_on_request_body {
                    let state = match self.get_state() {
                        Ok(s) => s,
                        Err(e) => return FilterDecision::internal_server_error(&e.to_string(), req.version()),
                    };

                    let res = if let Ok(on_body) =
                        state.instance.get_typed_func::<(u64, u32), i32>(&mut state.store, "on_request_body")
                    {
                        state.store.data_mut().buffered_request_body = Some(full_body_bytes.clone());

                        let res = on_body.call(&mut state.store, (req_handle, full_body_bytes.len() as u32));

                        state.store.data_mut().buffered_request_body = None;
                        res.map_err(WasmError::Wasmtime)
                    } else {
                        Ok(types::FilterAction::Continue.into())
                    };

                    res
                } else {
                    Ok(types::FilterAction::Continue.into())
                };

                match body_action_code {
                    Ok(0) => FilterDecision::Continue, // Continue
                    Ok(2) => {
                        // DirectResponse
                        let state = match self.get_state() {
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
                    Ok(code) => FilterDecision::internal_server_error(
                        &format!("Invalid body action code: {}", code),
                        req.version(),
                    ),
                    Err(e) => FilterDecision::internal_server_error(&e.to_string(), req.version()),
                }
            },

            Ok(2) => {
                // DirectResponse
                let state = match self.get_state() {
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

            Ok(code) => FilterDecision::internal_server_error(
                &format!("Invalid Wasm FilterAction code: {}", code),
                req.version(),
            ),
            Err(e) => FilterDecision::internal_server_error(&e.to_string(), req.version()),
        }
    }

    pub async fn apply_response(&mut self, res: &mut Response<OrionResponseBody>) -> FilterDecision {
        debug!("WasFilter::apply_response: {:?}", self.inner.config);
        if !self.inner.has_on_response_headers && !self.inner.has_on_response_body {
            return FilterDecision::Continue;
        }

        let resp_handle = res as *mut Response<OrionResponseBody> as u64;

        // PHASE 1: headers evaluation
        let action_code: Result<i32, WasmError> = if self.inner.has_on_response_headers {
            let state = match self.get_state() {
                Ok(s) => s,
                Err(e) => return FilterDecision::internal_server_error(&e.to_string(), res.version()),
            };

            if let Ok(on_response_headers) =
                state.instance.get_typed_func::<u64, i32>(&mut state.store, "on_response_headers")
            {
                let res_val = on_response_headers.call(&mut state.store, resp_handle);
                res_val.map_err(WasmError::Wasmtime)
            } else {
                Ok(types::FilterAction::Continue.into())
            }
        } else {
            Ok(1) // Implicitly return PauseAndBufferBody if the plugin only implements the body hook
        };

        match action_code {
            Ok(0) => FilterDecision::Continue,

            Ok(1) => {
                // PauseAndBufferBody for response
                use crate::body::poly_body::PolyBody;
                use http_body_util::{BodyExt, Full};

                // 1. Buffer response body
                let full_body_bytes = match res.body_mut().collect().await {
                    Ok(collected) => collected.to_bytes(),
                    Err(_) => {
                        return FilterDecision::internal_server_error("Failed to collect response body", res.version());
                    },
                };

                // 2. Swap response body
                let old_body = std::mem::take(res.body_mut());
                *res.body_mut() =
                    old_body.map_inner(|_old_poly_body| PolyBody::from(Full::from(full_body_bytes.clone())));

                // 3. Invoke on_response_body
                let body_action_code: Result<i32, WasmError> = if self.inner.has_on_response_body {
                    let state = match self.get_state() {
                        Ok(s) => s,
                        Err(e) => return FilterDecision::internal_server_error(&e.to_string(), res.version()),
                    };

                    let res_val = if let Ok(on_body) =
                        state.instance.get_typed_func::<(u64, u32), i32>(&mut state.store, "on_response_body")
                    {
                        state.store.data_mut().buffered_response_body = Some(full_body_bytes.clone());

                        let res_val = on_body.call(&mut state.store, (resp_handle, full_body_bytes.len() as u32));

                        state.store.data_mut().buffered_response_body = None;
                        res_val.map_err(WasmError::Wasmtime)
                    } else {
                        Ok(types::FilterAction::Continue.into())
                    };

                    res_val
                } else {
                    Ok(types::FilterAction::Continue.into())
                };

                match body_action_code {
                    Ok(0) => FilterDecision::Continue,
                    Ok(2) => {
                        // DirectResponse
                        let state = match self.get_state() {
                            Ok(s) => s,
                            Err(e) => return FilterDecision::internal_server_error(&e.to_string(), res.version()),
                        };
                        let direct_resp = state.store.data_mut().direct_response.take();
                        if let Some(mut response) = direct_resp {
                            *response.version_mut() = res.version();
                            FilterDecision::DirectResponse(Box::new(response))
                        } else {
                            warn!(
                                "wasm plugin returned DirectResponse from \
                                 on_response_body without calling \
                                 send_direct_response"
                            );
                            FilterDecision::internal_server_error(
                                "DirectResponse without send_direct_response",
                                res.version(),
                            )
                        }
                    },
                    Ok(code) => FilterDecision::internal_server_error(
                        &format!("Invalid response body action code: {}", code),
                        res.version(),
                    ),
                    Err(e) => FilterDecision::internal_server_error(&e.to_string(), res.version()),
                }
            },

            Ok(2) => {
                // DirectResponse
                let state = match self.get_state() {
                    Ok(s) => s,
                    Err(e) => return FilterDecision::internal_server_error(&e.to_string(), res.version()),
                };
                let direct_resp = state.store.data_mut().direct_response.take();

                if let Some(mut response) = direct_resp {
                    *response.version_mut() = res.version();
                    FilterDecision::DirectResponse(Box::new(response))
                } else {
                    warn!(
                        "wasm plugin returned DirectResponse from \
                         on_response_headers without calling \
                         send_direct_response"
                    );
                    FilterDecision::internal_server_error("DirectResponse without send_direct_response", res.version())
                }
            },

            Ok(code) => FilterDecision::internal_server_error(
                &format!("Invalid Wasm response FilterAction code: {}", code),
                res.version(),
            ),
            Err(e) => FilterDecision::internal_server_error(&e.to_string(), res.version()),
        }
    }
}

impl Drop for WasmFilter {
    fn drop(&mut self) {
        // Zero locking overhead via get_mut()
        if let Some(mut state) = self.state.get_mut().take() {
            // Reset host state so requests don't pollute each other
            let data = state.store.data_mut();
            data.direct_response = None;
            data.buffered_request_body = None;
            data.buffered_response_body = None;

            // Push back to the global lock-free pool. If it's full (1024), the state is dropped.
            let _ = self.inner.instance_pool.push(state);
        }
    }
}

impl FilterFactory for WasmFilter {
    fn new_from(&self) -> Self {
        self.clone()
    }
}
