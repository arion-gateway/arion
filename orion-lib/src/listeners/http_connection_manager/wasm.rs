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
use wasmtime::{Engine, Instance, Linker, Module, Store};

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
    cache: papaya::HashMap<String, Module>,
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
    pub store: Store<()>,
    pub instance: Option<Instance>,
}

// Map thread-local storage by cache_key to support multiple distinct plugins
// running on the same thread without overriding each other's Context.
thread_local! {
    static THREAD_CTX: RefCell<HashMap<String, ThreadContext, ahash::RandomState>> = RefCell::new(std::collections::HashMap::with_hasher(ahash::RandomState::default()));
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
        // Fetch or compile. The first Tokio runtime will trigger JIT compilation.
        // Subsequent runtimes will hit the fast path in Papaya.
        let registry = &*WASM_REGISTRY;
        let module = registry.get_or_compile(&config.code).ok();
        Self { inner: Arc::new(WasmFilterInner { config, module }) }
    }

    pub fn apply_request(&mut self, _req: &mut Request<OrionRequestBody>) -> FilterDecision {
        info!("WasFilter::apply_request: {:?}", self.inner.config);
        let cache_key = self.inner.config.code.cache_key();

        THREAD_CTX.with(|ctx| {
            let mut contexts = ctx.borrow_mut();

            // 1. If this thread hasn't instantiated THIS specific module yet, do it now.
            let thread_ctx = contexts.entry(cache_key).or_insert_with(|| {
                let engine = &*GLOBAL_ENGINE;
                let mut store = Store::new(engine, ());
                let linker = Linker::new(engine);

                // Note: Host functions (like orion_get_header) would be registered in the linker here.

                // Instantiate the module using the globally shared compiled code
                let instance =
                    self.inner.module.as_ref().map(|module| linker.instantiate(&mut store, &module).ok()).flatten();

                ThreadContext { store, instance }
            });

            // 2. Execute the Wasm logic lock-free
            // Fetch the exported function and call it
            // e.g., let func = thread_ctx.instance.get_typed_func::<u64, i32>(&mut thread_ctx.store, "on_request_headers").unwrap();
            // func.call(&mut thread_ctx.store, req_ptr).unwrap();
        });

        FilterDecision::Continue
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
