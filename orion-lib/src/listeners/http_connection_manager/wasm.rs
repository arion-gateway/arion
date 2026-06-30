use crate::{
    listeners::http_filters::{FilterDecision, FilterFactory},
    OrionRequestBody, OrionResponseBody,
};
use http::{Request, Response};
use orion_configuration::config::{
    core::DataSource, network_filters::http_connection_manager::http_filters::wasm::WasmConfig,
};
use smol_str::SmolStr;
use std::{
    cell::RefCell,
    collections::HashMap,
    sync::{Arc, LazyLock},
};
use thiserror::Error;
use tracing::{debug, info, warn};
use wasmtime::{Engine, Instance, Linker, Module, Store, TypedFunc};

mod hostcalls;
mod types;

#[derive(Error, Debug)]
pub enum WasmError {
    #[error("Wasmtime compilation or execution error: {0}")]
    Wasmtime(#[from] wasmtime::Error),
    #[error("Wasm Engine is not initialized")]
    EngineNotInitialized,
    #[error("Wasm Registry is not initialized")]
    RegistryNotInitialized,
    #[error("Unsupported data source: {0}")]
    UnsupportedDataSource(String),
    #[error("Initialization error: {0}")]
    InitError(String),
}

// Lazily initialize the global engine and registry on first use
pub static GLOBAL_ENGINE: LazyLock<Engine> = LazyLock::new(|| Engine::default());
pub static WASM_REGISTRY: LazyLock<WasmRegistry> = LazyLock::new(|| WasmRegistry::new());

pub struct WasmRegistry {
    // Papaya's concurrent hash map for lock-free reads
    cache: papaya::HashMap<SmolStr, Module>,
}

impl WasmRegistry {
    pub fn new() -> Self {
        Self { cache: papaya::HashMap::new() }
    }

    pub fn get_or_compile(&self, source: &DataSource) -> Result<Module, WasmError> {
        let key = source.cache_key();
        let cache = self.cache.pin();

        // Fast path: lock-free read using Papaya
        if let Some(module) = cache.get(&key) {
            return Ok(module.clone());
        }

        // Slow path: compile the module based on the data source
        let engine = &*GLOBAL_ENGINE;

        let module = match source {
            DataSource::Path(path) => Module::from_file(engine, path.as_str())?,
            DataSource::InlineBytes(bytes) => Module::from_binary(engine, bytes)?,
            DataSource::InlineString(wat) => Module::new(engine, wat.as_str())?,
            DataSource::EnvironmentVariable(env) => {
                return Err(WasmError::UnsupportedDataSource(format!("EnvVar: {}", env)));
            },
        };

        // Insert into the concurrent map
        cache.insert(key, module.clone());
        Ok(module)
    }
}

// Thread-local execution context
// Holds the Wasmtime Store and Instance for lock-free execution
pub struct ThreadContext {
    pub store: Store<hostcalls::WasmState>,
    pub on_request_headers: Option<TypedFunc<u64, i32>>,
    pub on_request_body: Option<TypedFunc<(u64, u32), i32>>,
    pub on_response_headers: Option<TypedFunc<u64, i32>>,
    pub on_response_body: Option<TypedFunc<(u64, u32), i32>>,
    pub instance: Instance,
}

// Map thread-local storage by cache_key to support multiple distinct plugins
// running on the same thread without overriding each other's Context.
thread_local! {
    static THREAD_CTX: RefCell<HashMap<SmolStr, ThreadContext, ahash::RandomState>> = RefCell::new(std::collections::HashMap::with_hasher(ahash::RandomState::default()));
}

#[derive(Debug, Clone)]
pub struct WasmFilterInner {
    pub config: WasmConfig,
    pub module: Option<Module>,
}

#[derive(Debug, Clone)]
pub struct WasmFilter {
    inner: Arc<WasmFilterInner>,
}

impl WasmFilter {
    pub fn new(config: WasmConfig) -> Self {
        // Fetch or compile. The first Tokio runtime will trigger compilation.
        // Subsequent runtimes will hit the fast path in Papaya.
        let registry = &*WASM_REGISTRY;
        let module = registry.get_or_compile(&config.code).ok();
        Self { inner: Arc::new(WasmFilterInner { config, module }) }
    }

    fn create_thread_context(&self) -> Result<ThreadContext, WasmError> {
        let engine = &*GLOBAL_ENGINE;
        let mut store = Store::new(
            engine,
            hostcalls::WasmState {
                request: None,
                direct_response: None,
                buffered_body: None,
                buffered_response_body: None,
            },
        );
        let mut linker = Linker::new(engine);

        // register hostcalls...
        if let Err(e) = hostcalls::register_hostcalls(&mut linker) {
            return Err(WasmError::InitError(format!("failed to register hostcalls: {}", e)));
        }

        // Instantiate the module using the globally shared compiled code
        let instance = match self.inner.module.as_ref() {
            Some(module) => match linker.instantiate(&mut store, module) {
                Ok(inst) => inst,
                Err(e) => return Err(WasmError::InitError(format!("failed to instantiate module: {}", e))),
            },
            None => return Err(WasmError::InitError("WASM module is not loaded".to_string())),
        };

        let on_request_headers = instance.get_typed_func::<u64, i32>(&mut store, "on_request_headers").ok();
        let on_request_body = instance.get_typed_func::<(u64, u32), i32>(&mut store, "on_request_body").ok();
        let on_response_headers = instance.get_typed_func::<u64, i32>(&mut store, "on_response_headers").ok();
        let on_response_body = instance.get_typed_func::<(u64, u32), i32>(&mut store, "on_response_body").ok();

        Ok(ThreadContext {
            store,
            on_request_headers,
            on_request_body,
            on_response_headers,
            on_response_body,
            instance,
        })
    }

    pub async fn apply_request(&mut self, req: &mut Request<OrionRequestBody>) -> FilterDecision {
        info!("WasFilter::apply_request: {:?}", self.inner.config);
        let cache_key = self.inner.config.code.cache_key();

        // PHASE 1: headers evaluation
        let action_code: Result<i32, WasmError> = THREAD_CTX.with(|ctx| {
            let mut contexts = ctx.borrow_mut();

            if !contexts.contains_key(&cache_key) {
                let thread_ctx = self.create_thread_context()?;
                contexts.insert(cache_key.clone(), thread_ctx);
            }

            let thread_ctx = contexts.get_mut(&cache_key).unwrap();

            if let Some(on_headers) = thread_ctx.on_request_headers.as_ref() {
                let req_handle = req as *mut Request<OrionRequestBody> as u64;
                thread_ctx.store.data_mut().request = Some(req as *mut _);
                let res = on_headers.call(&mut thread_ctx.store, req_handle);
                thread_ctx.store.data_mut().request = None;
                res.map_err(WasmError::Wasmtime)
            } else {
                Ok(types::FilterAction::Continue.into())
            }
        });

        match action_code {
            Ok(0) => FilterDecision::Continue, // Continue

            Ok(1) => {
                // PauseAndBufferBody
                use crate::body::poly_body::PolyBody;
                use crate::body::timeout_body::TimeoutBody;
                use http_body_util::{BodyExt, Full};

                // 1. Buffer the entire body asynchronously
                let full_body_bytes = match req.body_mut().collect().await {
                    Ok(collected) => collected.to_bytes(),
                    Err(_) => return FilterDecision::internal_server_error("Failed to collect body", req.version()),
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

                // 3. Re-enter the Wasm context to invoke on_request_body
                let body_action_code: Result<i32, WasmError> = THREAD_CTX.with(|ctx| {
                    let mut contexts = ctx.borrow_mut();
                    let thread_ctx = contexts.get_mut(&cache_key).unwrap();

                    if let Some(on_body) = thread_ctx.on_request_body.as_ref() {
                        let req_handle = req as *mut Request<OrionRequestBody> as u64;
                        thread_ctx.store.data_mut().request = Some(req as *mut _);
                        thread_ctx.store.data_mut().buffered_body = Some(full_body_bytes.clone());

                        let res = on_body.call(&mut thread_ctx.store, (req_handle, full_body_bytes.len() as u32));

                        thread_ctx.store.data_mut().request = None;
                        thread_ctx.store.data_mut().buffered_body = None;

                        res.map_err(WasmError::Wasmtime)
                    } else {
                        Ok(types::FilterAction::Continue.into())
                    }
                });

                match body_action_code {
                    Ok(0) => FilterDecision::Continue, // Continue
                    Ok(2) => {
                        // DirectResponse
                        let mut direct_resp = None;
                        THREAD_CTX.with(|ctx| {
                            if let Some(thread_ctx) = ctx.borrow_mut().get_mut(&cache_key) {
                                direct_resp = thread_ctx.store.data_mut().direct_response.take();
                            }
                        });

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
                let mut direct_resp = None;
                THREAD_CTX.with(|ctx| {
                    if let Some(thread_ctx) = ctx.borrow_mut().get_mut(&cache_key) {
                        direct_resp = thread_ctx.store.data_mut().direct_response.take();
                    }
                });

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
                    FilterDecision::internal_server_error(
                        "DirectResponse without send_direct_response",
                        req.version(),
                    )
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
        info!("WasFilter::apply_response: {:?}", self.inner.config);
        let cache_key = self.inner.config.code.cache_key();

        let action_code: Result<i32, WasmError> = THREAD_CTX.with(|ctx| {
            let mut contexts = ctx.borrow_mut();

            if !contexts.contains_key(&cache_key) {
                let thread_ctx = self.create_thread_context()?;
                contexts.insert(cache_key.clone(), thread_ctx);
            }

            let thread_ctx = contexts.get_mut(&cache_key).unwrap();

            if let Some(on_response_headers) = thread_ctx.on_response_headers.as_ref() {
                let resp_handle = res as *mut Response<OrionResponseBody> as u64;
                let res_val = on_response_headers.call(&mut thread_ctx.store, resp_handle);
                res_val.map_err(WasmError::Wasmtime)
            } else {
                Ok(types::FilterAction::Continue.into())
            }
        });

        match action_code {
            Ok(0) => FilterDecision::Continue,

            Ok(1) => {
                // PauseAndBufferBody for response
                use crate::body::poly_body::PolyBody;
                use http_body_util::{BodyExt, Full};

                // 1. Buffer response body
                let full_body_bytes = match res.body_mut().collect().await {
                    Ok(collected) => collected.to_bytes(),
                    Err(_) => return FilterDecision::internal_server_error("Failed to collect response body", res.version()),
                };

                // 2. Swap response body
                let old_body = std::mem::take(res.body_mut());
                *res.body_mut() = old_body.map_inner(|_old_poly_body| {
                    PolyBody::from(Full::from(full_body_bytes.clone()))
                });

                // 3. Invoke on_response_body
                let body_action_code: Result<i32, WasmError> = THREAD_CTX.with(|ctx| {
                    let mut contexts = ctx.borrow_mut();
                    let thread_ctx = contexts.get_mut(&cache_key).unwrap();

                    if let Some(on_body) = thread_ctx.on_response_body.as_ref() {
                        let resp_handle = res as *mut Response<OrionResponseBody> as u64;
                        thread_ctx.store.data_mut().buffered_response_body = Some(full_body_bytes.clone());

                        let res_val = on_body.call(&mut thread_ctx.store, (resp_handle, full_body_bytes.len() as u32));

                        thread_ctx.store.data_mut().buffered_response_body = None;
                        res_val.map_err(WasmError::Wasmtime)
                    } else {
                        Ok(types::FilterAction::Continue.into())
                    }
                });

                match body_action_code {
                    Ok(0) => FilterDecision::Continue,
                    Ok(code) => FilterDecision::internal_server_error(
                        &format!("Invalid response body action code: {}", code),
                        res.version(),
                    ),
                    Err(e) => FilterDecision::internal_server_error(&e.to_string(), res.version()),
                }
            }

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
