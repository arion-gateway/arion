use crate::{
    listeners::http_filters::{FilterDecision, FilterFactory},
    OrionRequestBody, OrionResponseBody,
};
use http::{Request, Response};
use orion_configuration::config::{
    core::DataSource, network_filters::http_connection_manager::http_filters::wasm::WasmConfig,
};
use std::{
    cell::RefCell,
    collections::HashMap,
    sync::{Arc, LazyLock},
};
use thiserror::Error;
use tracing::{debug, info};
use smol_str::SmolStr;
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
    pub on_request_headers: TypedFunc<u64, i32>,
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
        let mut store = Store::new(engine, hostcalls::WasmState { request: None, direct_response: None });
        let mut linker = Linker::new(engine);

        // register hostcalls...
        if let Err(e) = hostcalls::register_hostcalls(&mut linker) {
            return Err(WasmError::InitError(format!("failed to register hostcalls: {}", e)));
        }

        // Instantiate the module using the globally shared compiled code
        let instance = match self.inner.module.as_ref() {
            Some(module) => {
                match linker.instantiate(&mut store, module) {
                    Ok(inst) => inst,
                    Err(e) => return Err(WasmError::InitError(format!("failed to instantiate module: {}", e))),
                }
            }
            None => return Err(WasmError::InitError("WASM module is not loaded".to_string())),
        };

        let on_request_headers = instance.get_typed_func::<u64, i32>(&mut store, "on_request_headers")?;

        Ok(ThreadContext { store, on_request_headers, instance })
    }

    pub fn apply_request(&mut self, _req: &mut Request<OrionRequestBody>) -> FilterDecision {
        info!("WasFilter::apply_request: {:?}", self.inner.config);
        let cache_key = self.inner.config.code.cache_key();

        let res: Result<Option<Response<OrionResponseBody>>, WasmError> = THREAD_CTX.with(|ctx| {
            let mut contexts = ctx.borrow_mut();

            // 1. If this thread hasn't instantiated THIS specific module yet, do it now.
            if !contexts.contains_key(&cache_key) {
                let thread_ctx = self.create_thread_context()?;
                contexts.insert(cache_key.clone(), thread_ctx);
            }

            let thread_ctx = contexts.get_mut(&cache_key).unwrap();
            thread_ctx.store.data_mut().request = Some(_req as *mut _);

            // 2. Execute the Wasm logic
            // Pass 0 as a dummy context ID/req_ptr for now, since the actual request
            // is stored in the hostcall WasmState.
            let res = thread_ctx.on_request_headers.call(&mut thread_ctx.store, 0);

            let direct_response = thread_ctx.store.data_mut().direct_response.take();
            thread_ctx.store.data_mut().request = None;

            // Map any Wasm trap/error back to WasmError
            res?;

            Ok(direct_response)
        });

        match res {
            Ok(Some(mut response)) => {
                *response.version_mut() = _req.version();
                FilterDecision::DirectResponse(Box::new(response))
            },
            Ok(None) => FilterDecision::Continue,
            Err(e) => FilterDecision::internal_server_error(&e.to_string(), _req.version()),
        }
    }

    pub fn apply_response(&mut self, _res: &mut Response<OrionResponseBody>) -> FilterDecision {
        info!("WasFilter::apply_response: {:?}", self.inner.config);
        FilterDecision::Continue
    }
}

impl FilterFactory for WasmFilter {
    fn new_from(&self) -> Self {
        self.clone()
    }
}
