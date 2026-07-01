use crate::{
    listeners::http_filters::{FilterDecision, FilterFactory},
    OrionRequestBody, OrionResponseBody,
};
use http::{Request, Response};
use orion_configuration::config::{
    core::DataSource, network_filters::http_connection_manager::http_filters::wasm::WasmConfig,
};
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
pub static GLOBAL_ENGINE: LazyLock<Engine> = LazyLock::new(|| Engine::default());

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

#[derive(Debug, Clone)]
pub struct WasmFilterInner {
    config: WasmConfig,
    module: Module,
}

#[derive(Debug)]
pub struct WasmFilter {
    inner: Arc<WasmFilterInner>,
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

        Ok(Self { inner: Arc::new(WasmFilterInner { config, module }), state: Mutex::new(None) })
    }

    fn instantiate<'a>(
        &self,
        state_lock: &'a mut Option<WasmFilterState>,
    ) -> Result<&'a mut WasmFilterState, WasmError> {
        if state_lock.is_none() {
            let engine = &*GLOBAL_ENGINE;
            let mut store = Store::new(
                engine,
                hostcalls::WasmState {
                    direct_response: None,
                    buffered_request_body: None,
                    buffered_response_body: None,
                },
            );
            let mut linker = Linker::new(engine);

            // register hostcalls...
            if let Err(e) = hostcalls::register_hostcalls(&mut linker) {
                return Err(WasmError::InitError(format!("failed to register hostcalls: {}", e)));
            }

            // Instantiate the module using the globally shared compiled code
            let instance = match linker.instantiate(&mut store, &self.inner.module) {
                Ok(inst) => inst,
                Err(e) => return Err(WasmError::InitError(format!("failed to instantiate module: {}", e))),
            };

            *state_lock = Some(WasmFilterState { store, instance });
        }

        // Return a mutable reference to the Wasm state using a safe Option to Result mapping
        state_lock.as_mut().ok_or_else(|| WasmError::InitError("Wasm state is uninitialized".to_string()))
    }

    pub async fn apply_request(&mut self, req: &mut Request<OrionRequestBody>) -> FilterDecision {
        debug!("WasFilter::apply_request: {:?}", self.inner.config);
        let req_handle = req as *mut Request<OrionRequestBody> as u64;

        // PHASE 1: headers evaluation inside a scoped block to release MutexGuard before await
        let action_code: Result<i32, WasmError> = {
            let mut state_lock = self.state.lock();
            let state = match self.instantiate(&mut state_lock) {
                Ok(s) => s,
                Err(e) => return FilterDecision::internal_server_error(&e.to_string(), req.version()),
            };

            if let Ok(on_headers) = state.instance.get_typed_func::<u64, i32>(&mut state.store, "on_request_headers") {
                let res = on_headers.call(&mut state.store, req_handle);
                res.map_err(WasmError::Wasmtime)
            } else {
                Ok(types::FilterAction::Continue.into())
            }
        };

        match action_code {
            Ok(0) => FilterDecision::Continue,

            Ok(1) => {
                // PauseAndBufferBody
                use crate::body::poly_body::PolyBody;
                use crate::body::timeout_body::TimeoutBody;
                use http_body_util::{BodyExt, Full};

                // 1. Buffer the entire body asynchronously (MutexGuard is not held here!)
                let full_body_bytes = match req.body_mut().collect().await {
                    Ok(collected) => collected.to_bytes(),
                    Err(_) => {
                        return FilterDecision::internal_server_error("Failed to collect body", req.version());
                    },
                };

                // 2. We consumed the inner stream, we need to swap the inner PolyBody
                //    with a new Full body containing the bytes we just collected.
                //    We use `map_inner` to safely replace the inner TimeoutBody<PolyBody>
                //    while preserving the telemetry/instrumentation wrappers!
                let old_body = std::mem::take(req.body_mut());

                // PolyBody implementation provides an `From<Full<Bytes>>`
                *req.body_mut() = old_body.map_inner(|old_timeout_body| {
                    let old_timeout = old_timeout_body.timeout;
                    TimeoutBody::new(old_timeout, PolyBody::from(Full::from(full_body_bytes.clone())))
                });

                // 3. Re-enter the Wasm context inside a scoped block to invoke on_request_body
                let body_action_code: Result<i32, WasmError> = {
                    let mut state_lock = self.state.lock();
                    let state = match self.instantiate(&mut state_lock) {
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
                };

                match body_action_code {
                    Ok(0) => FilterDecision::Continue, // Continue
                    Ok(2) => {
                        // DirectResponse
                        let mut state_lock = self.state.lock();
                        let state = match self.instantiate(&mut state_lock) {
                            Ok(s) => s,
                            Err(e) => return FilterDecision::internal_server_error(&e.to_string(), req.version()),
                        };
                        let direct_resp = state.store.data_mut().direct_response.take();
                        if let Some(mut response) = direct_resp {
                            *response.version_mut() = req.version();
                            FilterDecision::DirectResponse(Box::new(response))
                        } else {
                            // The plugin returned DirectResponse without ever
                            // calling `orion_send_direct_response`
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
                let mut state_lock = self.state.lock();
                let state = match self.instantiate(&mut state_lock) {
                    Ok(s) => s,
                    Err(e) => return FilterDecision::internal_server_error(&e.to_string(), req.version()),
                };
                let direct_resp = state.store.data_mut().direct_response.take();

                if let Some(mut response) = direct_resp {
                    *response.version_mut() = req.version();
                    FilterDecision::DirectResponse(Box::new(response))
                } else {
                    // The plugin returned DirectResponse without ever
                    // calling `orion_send_direct_response`: surface it as a
                    // 500 instead of silently continuing.
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
        let resp_handle = res as *mut Response<OrionResponseBody> as u64;

        // PHASE 1: headers evaluation inside a scoped block to release MutexGuard before await
        let action_code: Result<i32, WasmError> = {
            let mut state_lock = self.state.lock();
            let state = match self.instantiate(&mut state_lock) {
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
        };

        match action_code {
            Ok(0) => FilterDecision::Continue,

            Ok(1) => {
                // PauseAndBufferBody for response
                use crate::body::poly_body::PolyBody;
                use http_body_util::{BodyExt, Full};

                // 1. Buffer response body (MutexGuard is not held here!)
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

                // 3. Invoke on_response_body inside a scoped block
                let body_action_code: Result<i32, WasmError> = {
                    let mut state_lock = self.state.lock();
                    let state = match self.instantiate(&mut state_lock) {
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
                };

                match body_action_code {
                    Ok(0) => FilterDecision::Continue,
                    Ok(code) => FilterDecision::internal_server_error(
                        &format!("Invalid response body action code: {}", code),
                        res.version(),
                    ),
                    Err(e) => FilterDecision::internal_server_error(&e.to_string(), res.version()),
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

impl FilterFactory for WasmFilter {
    fn new_from(&self) -> Self {
        self.clone()
    }
}
