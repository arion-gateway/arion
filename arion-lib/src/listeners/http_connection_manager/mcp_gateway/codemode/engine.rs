// Copyright 2026 The arion-gateway Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{
    future::Future,
    sync::{Arc as StdArc, Condvar, LazyLock, Mutex},
    time::{Duration, Instant},
};

use serde_json::Value;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use wasmtime::component::{Component, Linker};
use wasmtime::{
    Engine, InstanceAllocationStrategy, PoolingAllocationConfig, Store, StoreLimits, StoreLimitsBuilder, UpdateDeadline,
};
use wasmtime_wasi::{ResourceTable, WasiCtx, WasiCtxBuilder, WasiCtxView, WasiView};

use super::{
    dispatcher::ToolDispatcher,
    error::{CodeModeError, GuestErrorKind},
    hostcalls, ts_strip,
};

const SANDBOX_WASM: &[u8] = include_bytes!("artifacts/codemode_sandbox.wasm");
const CORE_INSTANCES_PER_COMPONENT: u32 = 4;
const TABLES_PER_COMPONENT: u32 = 2;

const MIB: usize = 1024 * 1024;
const MIN_OUTPUT_BYTES: usize = 1024;

/// Per-execution limits enforced by the engine.
#[derive(Clone, Debug)]
pub struct CodeModeLimits {
    pub execution_timeout: Duration,
    pub cpu_timeout: Duration,
    pub memory_bytes: usize,
    pub max_script_bytes: usize,
    pub max_output_bytes: usize,
    pub max_host_calls: usize,
    pub max_concurrent_executions: usize,
    pub max_concurrent_tool_calls: usize,
}

impl Default for CodeModeLimits {
    fn default() -> Self {
        Self {
            execution_timeout: Duration::from_secs(30),
            cpu_timeout: Duration::from_secs(5),
            memory_bytes: 64 * MIB,
            max_script_bytes: 256 * 1024,
            max_output_bytes: MIB,
            max_host_calls: 100,
            max_concurrent_executions: 32,
            max_concurrent_tool_calls: 8,
        }
    }
}

struct EngineRuntime {
    engine: Engine,
    component: Component,
    ticker: StdArc<EpochTicker>,
    pool_capacity: usize,
    max_memory_bytes: usize,
}

static ENGINE_RUNTIME: LazyLock<Result<EngineRuntime, String>> = LazyLock::new(|| {
    let process_config = crate::RUNTIME_CONFIG.get().map(|runtime| runtime.codemode.clone()).unwrap_or_default();
    let pool_capacity = process_config.pool_capacity.get();
    let total_core_instances = pool_capacity
        .checked_mul(CORE_INSTANCES_PER_COMPONENT)
        .ok_or_else(|| "CodeMode Wasmtime pool capacity is too large".to_owned())?;
    let total_tables = pool_capacity
        .checked_mul(TABLES_PER_COMPONENT)
        .ok_or_else(|| "CodeMode Wasmtime pool capacity is too large".to_owned())?;
    tracing::info!(
        target: "mcp_gateway",
        pool_capacity,
        max_memory_bytes = process_config.max_memory_bytes.get(),
        "Initializing the CodeMode Wasmtime pool"
    );

    let mut config = wasmtime::Config::new();
    config.async_support(true);
    config.epoch_interruption(true);

    let mut pool = PoolingAllocationConfig::default();
    pool.total_component_instances(pool_capacity);
    pool.max_core_instances_per_component(CORE_INSTANCES_PER_COMPONENT);
    pool.max_memories_per_component(1);
    pool.max_tables_per_component(TABLES_PER_COMPONENT);
    pool.total_core_instances(total_core_instances);
    pool.total_stacks(pool_capacity);
    pool.total_memories(pool_capacity);
    pool.total_tables(total_tables);
    pool.max_memory_size(process_config.max_memory_bytes.get());
    pool.table_elements(32_768);
    config.allocation_strategy(InstanceAllocationStrategy::Pooling(pool));

    let engine = Engine::new(&config).map_err(|error| error.to_string())?;
    let component = Component::from_binary(&engine, SANDBOX_WASM).map_err(|error| error.to_string())?;
    let ticker = EpochTicker::start(engine.clone());
    Ok(EngineRuntime {
        engine,
        component,
        ticker,
        pool_capacity: pool_capacity as usize,
        max_memory_bytes: process_config.max_memory_bytes.get(),
    })
});

#[derive(Clone, Debug)]
pub struct ExecuteRequest {
    pub code: String,
    pub timeout: Option<Duration>,
}

impl ExecuteRequest {
    #[cfg(test)]
    pub fn new(code: impl Into<String>) -> Self {
        Self { code: code.into(), timeout: None }
    }
}

/// The value or failure produced by an execution, together with metadata
/// collected before it finished.
#[derive(Debug)]
pub struct ExecOutcome {
    pub result: Result<Value, CodeModeError>,
    pub logs: Vec<String>,
}

impl ExecOutcome {
    fn completed(result: Value, logs: Vec<String>) -> Self {
        Self { result: Ok(result), logs }
    }

    fn failed(error: CodeModeError) -> Self {
        Self::failed_with_logs(error, Vec::new())
    }

    fn failed_with_logs(error: CodeModeError, logs: Vec<String>) -> Self {
        Self { result: Err(error), logs }
    }
}

#[derive(Clone)]
pub struct CodeModeEngine {
    engine: Engine,
    instance_pre: hostcalls::SandboxPre<ExecState>,
    limits: CodeModeLimits,
    exec_permits: StdArc<Semaphore>,
    ticker: StdArc<EpochTicker>,
}

impl CodeModeEngine {
    #[cfg(test)]
    pub fn try_new(limits: CodeModeLimits) -> Result<Self, CodeModeError> {
        Self::try_new_with_artifact(limits, None)
    }

    pub fn try_new_with_artifact(limits: CodeModeLimits, artifact_path: Option<&str>) -> Result<Self, CodeModeError> {
        if limits.execution_timeout.is_zero() {
            return Err(CodeModeError::InvalidLimits("execution_timeout must be greater than zero"));
        }
        if limits.memory_bytes == 0 {
            return Err(CodeModeError::InvalidLimits("memory limit must be greater than zero"));
        }
        if limits.max_output_bytes < MIN_OUTPUT_BYTES {
            return Err(CodeModeError::InvalidLimits("max_output_bytes must be at least 1024"));
        }
        if limits.max_host_calls == 0 {
            return Err(CodeModeError::InvalidLimits("max_host_calls must be greater than zero"));
        }
        if limits.max_concurrent_executions == 0 {
            return Err(CodeModeError::InvalidLimits("max_concurrent_executions must be greater than zero"));
        }
        if limits.max_concurrent_tool_calls == 0 {
            return Err(CodeModeError::InvalidLimits("max_concurrent_tool_calls must be greater than zero"));
        }

        let runtime =
            ENGINE_RUNTIME.as_ref().map_err(|message| CodeModeError::Engine(wasmtime::Error::msg(message.clone())))?;
        if limits.memory_bytes > runtime.max_memory_bytes {
            return Err(CodeModeError::InvalidLimits(
                "memory limit exceeds the process-wide CodeMode Wasmtime maximum",
            ));
        }
        if limits.max_concurrent_executions > runtime.pool_capacity {
            return Err(CodeModeError::InvalidLimits(
                "max_concurrent_executions exceeds the process-wide CodeMode Wasmtime pool capacity",
            ));
        }
        let override_component;
        let component = if let Some(path) = artifact_path {
            let bytes = std::fs::read(path).map_err(CodeModeError::ArtifactRead)?;
            override_component = Component::from_binary(&runtime.engine, &bytes).map_err(CodeModeError::Artifact)?;
            &override_component
        } else {
            &runtime.component
        };
        validate_imports(&runtime.engine, component)?;

        let mut linker = Linker::new(&runtime.engine);
        hostcalls::register(&mut linker).map_err(CodeModeError::Link)?;
        let instance_pre = linker.instantiate_pre(component).map_err(CodeModeError::Link)?;
        let instance_pre = hostcalls::SandboxPre::new(instance_pre).map_err(CodeModeError::Link)?;

        Ok(Self {
            engine: runtime.engine.clone(),
            instance_pre,
            exec_permits: StdArc::new(Semaphore::new(limits.max_concurrent_executions)),
            limits,
            ticker: StdArc::clone(&runtime.ticker),
        })
    }

    pub async fn execute(&self, mut request: ExecuteRequest, dispatcher: StdArc<dyn ToolDispatcher>) -> ExecOutcome {
        if request.code.len() > self.limits.max_script_bytes {
            return ExecOutcome::failed(CodeModeError::ScriptTooLarge);
        }
        request.code = match ts_strip::strip_function_body(&request.code) {
            Ok(code) => code,
            Err(error) => return ExecOutcome::failed(error),
        };

        let Ok(permit) = StdArc::clone(&self.exec_permits).try_acquire_owned() else {
            return ExecOutcome::failed(CodeModeError::ConcurrencyLimit);
        };
        let timeout = request.timeout.unwrap_or(self.limits.execution_timeout).min(self.limits.execution_timeout);
        let execution = self.run(request, dispatcher, permit);

        match pingora_timeout::fast_timeout::fast_timeout(timeout, execution).await {
            Ok(outcome) => outcome,
            Err(_) => ExecOutcome::failed(CodeModeError::Timeout),
        }
    }

    async fn run(
        &self,
        request: ExecuteRequest,
        dispatcher: StdArc<dyn ToolDispatcher>,
        _permit: OwnedSemaphorePermit,
    ) -> ExecOutcome {
        let _active = self.ticker.activate();
        let store_limits =
            StoreLimitsBuilder::new().memory_size(self.limits.memory_bytes).trap_on_grow_failure(false).build();
        let state = ExecState::new(store_limits, self.limits.clone(), dispatcher);
        let mut store = Store::new(&self.engine, state);
        store.limiter(|state| &mut state.store_limits);
        store.set_epoch_deadline(1);
        store.epoch_deadline_callback(|mut context| {
            if context.data_mut().cpu_deadline_reached() {
                Ok(UpdateDeadline::Interrupt)
            } else {
                Ok(UpdateDeadline::Yield(1))
            }
        });

        let instance = match self.instance_pre.instantiate_async(&mut store).await {
            Ok(instance) => instance,
            Err(_error) => {
                return ExecOutcome::failed_with_logs(
                    CodeModeError::Execution,
                    std::mem::take(&mut store.data_mut().logs),
                );
            },
        };
        let response = instance.arion_codemode_runner().call_execute(&mut store, &request.code).await;
        // Cancel and await outstanding dispatches so tool-call records drained
        // by the caller are complete before this execution reports its result.
        store.data_mut().quiesce_host_calls().await;
        let logs = std::mem::take(&mut store.data_mut().logs);
        let response = match response {
            Ok(response) => response,
            Err(_error) => return ExecOutcome::failed_with_logs(execution_error(&store), logs),
        };

        if store.data().host_call_limit_reached {
            return ExecOutcome::failed_with_logs(CodeModeError::HostCallLimit, logs);
        }
        match response {
            Ok(json) => {
                if json.len() > store.data().limits.max_output_bytes {
                    return ExecOutcome::failed_with_logs(CodeModeError::ResultTooLarge, logs);
                }
                match serde_json::from_str(&json) {
                    Ok(result) => ExecOutcome::completed(result, logs),
                    Err(_error) => ExecOutcome::failed_with_logs(CodeModeError::InvalidResult, logs),
                }
            },
            Err(hostcalls::ExecutionError::Javascript(message)) => {
                ExecOutcome::failed_with_logs(CodeModeError::Guest { kind: GuestErrorKind::JavaScript, message }, logs)
            },
            Err(hostcalls::ExecutionError::Fatal(message)) => {
                ExecOutcome::failed_with_logs(CodeModeError::Guest { kind: GuestErrorKind::Fatal, message }, logs)
            },
        }
    }
}

fn execution_error(store: &Store<ExecState>) -> CodeModeError {
    if store.data().host_call_limit_reached {
        CodeModeError::HostCallLimit
    } else if store.data().cpu_exhausted {
        CodeModeError::CpuLimit
    } else {
        CodeModeError::Execution
    }
}

fn validate_imports(engine: &Engine, component: &Component) -> Result<(), CodeModeError> {
    for (name, _item) in component.component_type().imports(engine) {
        if !hostcalls::REQUIRED_COMPONENT_IMPORTS.contains(&name) {
            return Err(CodeModeError::Artifact(wasmtime::Error::msg(format!("unsupported import {name}"))));
        }
    }
    Ok(())
}

pub(super) struct ExecState {
    // Wasmtime and WASI plumbing, accessed mutably by the runtime itself.
    pub store_limits: StoreLimits,
    wasi_ctx: WasiCtx,
    wasi_table: ResourceTable,
    // Fixed for the lifetime of one execution.
    pub(super) limits: CodeModeLimits,
    pub(super) dispatcher: StdArc<dyn ToolDispatcher>,
    pub(super) cancel: CancellationToken,
    pub(super) dispatch_permits: StdArc<Semaphore>,
    // Mutable execution bookkeeping.
    pub logs: Vec<String>,
    host_calls: usize,
    host_call_limit_reached: bool,
    cpu_consumed: Duration,
    last_resume: Instant,
    cpu_exhausted: bool,
    inflight: JoinSet<(u64, Result<String, String>)>,
    next_call_id: u64,
}

const MAX_LOG_ENTRIES: usize = 128;
const MAX_LOG_ENTRY_BYTES: usize = 4096;

impl ExecState {
    pub fn push_log(&mut self, mut line: String) {
        if self.logs.len() >= MAX_LOG_ENTRIES {
            return;
        }
        if line.len() > MAX_LOG_ENTRY_BYTES {
            let mut end = MAX_LOG_ENTRY_BYTES;
            while !line.is_char_boundary(end) {
                end -= 1;
            }
            line.truncate(end);
            line.push('…');
        }
        self.logs.push(line);
    }

    fn new(store_limits: StoreLimits, limits: CodeModeLimits, dispatcher: StdArc<dyn ToolDispatcher>) -> Self {
        let now = Instant::now();
        let wasi_ctx = WasiCtxBuilder::new().build();
        Self {
            store_limits,
            wasi_ctx,
            wasi_table: ResourceTable::new(),
            dispatcher,
            cancel: CancellationToken::new(),
            dispatch_permits: StdArc::new(Semaphore::new(limits.max_concurrent_tool_calls)),
            limits,
            logs: Vec::new(),
            host_calls: 0,
            host_call_limit_reached: false,
            cpu_consumed: Duration::ZERO,
            last_resume: now,
            cpu_exhausted: false,
            inflight: JoinSet::new(),
            // Start at 1 so a call id is never a falsy JavaScript number.
            next_call_id: 1,
        }
    }

    pub fn pause_cpu(&mut self) {
        self.cpu_consumed = self.cpu_consumed.saturating_add(self.last_resume.elapsed());
    }

    pub fn consume_host_call(&mut self) -> bool {
        if self.host_calls >= self.limits.max_host_calls {
            self.host_call_limit_reached = true;
            return false;
        }
        self.host_calls += 1;
        true
    }

    pub fn resume_cpu(&mut self) {
        self.last_resume = Instant::now();
    }

    fn cpu_deadline_reached(&mut self) -> bool {
        self.pause_cpu();
        self.cpu_exhausted = self.cpu_consumed >= self.limits.cpu_timeout;
        self.resume_cpu();
        self.cpu_exhausted
    }

    pub(super) fn next_call_seq(&mut self) -> u64 {
        let id = self.next_call_id;
        self.next_call_id += 1;
        id
    }

    pub(super) fn spawn_dispatch<F>(&mut self, id: u64, dispatch: F)
    where
        F: Future<Output = Result<String, String>> + Send + 'static,
    {
        self.inflight.spawn(async move { (id, dispatch.await) });
    }

    pub(super) async fn wait_for_completion(&mut self) -> Result<Option<(u64, Result<String, String>)>, String> {
        if self.inflight.is_empty() {
            return Ok(None);
        }
        // Host-call wait time is excluded from the guest CPU budget.
        self.pause_cpu();
        let joined = self.inflight.join_next().await;
        self.resume_cpu();
        match joined {
            Some(Ok(completion)) => Ok(Some(completion)),
            // A panicked dispatch task cannot be attributed to its promise;
            // fail the execution rather than mask a host bug as a tool error.
            Some(Err(_join_error)) => Err("a host dispatch task failed".to_owned()),
            None => Ok(None),
        }
    }

    async fn quiesce_host_calls(&mut self) {
        self.cancel.cancel();
        self.inflight.shutdown().await;
    }
}

impl Drop for ExecState {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

impl WasiView for ExecState {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView { ctx: &mut self.wasi_ctx, table: &mut self.wasi_table }
    }
}

struct EpochTicker {
    activity: Mutex<usize>,
    wake: Condvar,
}

impl EpochTicker {
    fn start(engine: Engine) -> StdArc<Self> {
        let ticker = StdArc::new(Self { activity: Mutex::new(0), wake: Condvar::new() });
        let thread_ticker = StdArc::clone(&ticker);
        let _ticker_thread = std::thread::Builder::new()
            .name("codemode-epoch".to_owned())
            .spawn(move || loop {
                let mut active = thread_ticker.activity.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                while *active == 0 {
                    active = thread_ticker.wake.wait(active).unwrap_or_else(std::sync::PoisonError::into_inner);
                }
                drop(active);
                std::thread::sleep(Duration::from_millis(10));
                engine.increment_epoch();
            })
            .unwrap_or_else(|error| panic!("failed to spawn codemode epoch ticker: {error}"));
        ticker
    }

    fn activate(self: &StdArc<Self>) -> ActiveExecution {
        let mut active = self.activity.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        *active += 1;
        if *active == 1 {
            self.wake.notify_one();
        }
        drop(active);
        ActiveExecution(StdArc::clone(self))
    }
}

struct ActiveExecution(StdArc<EpochTicker>);

impl Drop for ActiveExecution {
    fn drop(&mut self) {
        let mut active = self.0.activity.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        *active = active.saturating_sub(1);
    }
}

#[cfg(test)]
mod tests {
    use super::super::dispatcher::{CatalogRequest, DispatchDefect};
    use super::*;
    use futures::FutureExt;

    struct NoTools;

    impl ToolDispatcher for NoTools {
        fn call_tool(
            self: StdArc<Self>,
            _seq: u64,
            _id: crate::listeners::http_connection_manager::mcp_gateway::tools::ToolId,
            _args: Value,
            _cancel: CancellationToken,
        ) -> futures::future::BoxFuture<'static, Result<Value, DispatchDefect>> {
            async { Err(DispatchDefect::InvalidResponse) }.boxed()
        }

        fn catalog(
            self: StdArc<Self>,
            _request: CatalogRequest,
            _cancel: CancellationToken,
        ) -> futures::future::BoxFuture<'static, Result<Value, DispatchDefect>> {
            async { Err(DispatchDefect::InvalidResponse) }.boxed()
        }
    }

    struct SdkDispatcher;

    impl ToolDispatcher for SdkDispatcher {
        fn call_tool(
            self: StdArc<Self>,
            _seq: u64,
            id: crate::listeners::http_connection_manager::mcp_gateway::tools::ToolId,
            args: Value,
            _cancel: CancellationToken,
        ) -> futures::future::BoxFuture<'static, Result<Value, DispatchDefect>> {
            async move { Ok(serde_json::json!({ "ok": true, "data": { "tool": id.qualified_name(), "args": args } })) }
                .boxed()
        }

        fn catalog(
            self: StdArc<Self>,
            request: CatalogRequest,
            _cancel: CancellationToken,
        ) -> futures::future::BoxFuture<'static, Result<Value, DispatchDefect>> {
            async move {
                let operation = match request {
                    CatalogRequest::ToolsSearch { .. } => "tools_search",
                    _ => "other",
                };
                Ok(serde_json::json!({ "ok": true, "data": { "operation": operation } }))
            }
            .boxed()
        }
    }

    struct LargeCatalog;

    impl ToolDispatcher for LargeCatalog {
        fn call_tool(
            self: StdArc<Self>,
            _seq: u64,
            _id: crate::listeners::http_connection_manager::mcp_gateway::tools::ToolId,
            _args: Value,
            _cancel: CancellationToken,
        ) -> futures::future::BoxFuture<'static, Result<Value, DispatchDefect>> {
            async { Ok(serde_json::json!({ "ok": true, "data": "x".repeat(2048) })) }.boxed()
        }

        fn catalog(
            self: StdArc<Self>,
            _request: CatalogRequest,
            _cancel: CancellationToken,
        ) -> futures::future::BoxFuture<'static, Result<Value, DispatchDefect>> {
            async { Ok(serde_json::json!({ "ok": true, "data": "x".repeat(2048) })) }.boxed()
        }
    }

    #[derive(Default)]
    struct Gauge {
        current: std::sync::atomic::AtomicUsize,
        peak: std::sync::atomic::AtomicUsize,
        completed: std::sync::atomic::AtomicUsize,
    }

    impl Gauge {
        async fn settle(&self) {
            for _ in 0..400 {
                if self.current.load(std::sync::atomic::Ordering::SeqCst) == 0 {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            panic!("in-flight dispatch gauge did not settle");
        }
    }

    struct GaugeGuard(StdArc<Gauge>);

    impl GaugeGuard {
        fn enter(gauge: &StdArc<Gauge>) -> Self {
            use std::sync::atomic::Ordering;
            let current = gauge.current.fetch_add(1, Ordering::SeqCst) + 1;
            gauge.peak.fetch_max(current, Ordering::SeqCst);
            Self(StdArc::clone(gauge))
        }
    }

    impl Drop for GaugeGuard {
        fn drop(&mut self) {
            self.0.current.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        }
    }

    /// Delay per tool namespace: `bad` is an immediate infrastructure defect,
    /// `fast` 100ms, `slow` 400ms, `hang` 10s, anything else 300ms.
    struct DelayingDispatcher {
        gauge: StdArc<Gauge>,
    }

    impl DelayingDispatcher {
        fn new() -> (StdArc<Self>, StdArc<Gauge>) {
            let gauge = StdArc::new(Gauge::default());
            (StdArc::new(Self { gauge: StdArc::clone(&gauge) }), gauge)
        }

        fn run(
            gauge: StdArc<Gauge>,
            cancel: CancellationToken,
            delay: Option<Duration>,
            data: Value,
        ) -> futures::future::BoxFuture<'static, Result<Value, DispatchDefect>> {
            async move {
                let _guard = GaugeGuard::enter(&gauge);
                let Some(delay) = delay else {
                    return Err(DispatchDefect::InvalidResponse);
                };
                tokio::select! {
                    () = cancel.cancelled() => Err(DispatchDefect::Cancelled),
                    () = tokio::time::sleep(delay) => {
                        gauge.completed.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        Ok(serde_json::json!({ "ok": true, "data": data }))
                    },
                }
            }
            .boxed()
        }
    }

    impl ToolDispatcher for DelayingDispatcher {
        fn call_tool(
            self: StdArc<Self>,
            _seq: u64,
            id: crate::listeners::http_connection_manager::mcp_gateway::tools::ToolId,
            _args: Value,
            cancel: CancellationToken,
        ) -> futures::future::BoxFuture<'static, Result<Value, DispatchDefect>> {
            let qualified = id.qualified_name();
            let delay = match qualified.split_once('.').map(|(namespace, _)| namespace) {
                Some("bad") => None,
                Some("fast") => Some(Duration::from_millis(100)),
                Some("slow") => Some(Duration::from_millis(400)),
                Some("hang") => Some(Duration::from_secs(10)),
                _ => Some(Duration::from_millis(300)),
            };
            Self::run(StdArc::clone(&self.gauge), cancel, delay, Value::String(qualified))
        }

        fn catalog(
            self: StdArc<Self>,
            _request: CatalogRequest,
            cancel: CancellationToken,
        ) -> futures::future::BoxFuture<'static, Result<Value, DispatchDefect>> {
            Self::run(
                StdArc::clone(&self.gauge),
                cancel,
                Some(Duration::from_millis(300)),
                serde_json::json!({ "operation": "catalog" }),
            )
        }
    }

    #[test]
    fn listener_limits_must_fit_the_process_wasmtime_pool() {
        let too_little_output = CodeModeLimits { max_output_bytes: MIN_OUTPUT_BYTES - 1, ..CodeModeLimits::default() };
        assert!(matches!(
            CodeModeEngine::try_new(too_little_output),
            Err(CodeModeError::InvalidLimits("max_output_bytes must be at least 1024"))
        ));

        let process_config = crate::RUNTIME_CONFIG.get().map(|runtime| runtime.codemode.clone()).unwrap_or_default();
        let too_concurrent = CodeModeLimits {
            max_concurrent_executions: process_config.pool_capacity.get() as usize + 1,
            ..CodeModeLimits::default()
        };
        assert!(matches!(
            CodeModeEngine::try_new(too_concurrent),
            Err(CodeModeError::InvalidLimits(
                "max_concurrent_executions exceeds the process-wide CodeMode Wasmtime pool capacity"
            ))
        ));

        let too_much_memory =
            CodeModeLimits { memory_bytes: process_config.max_memory_bytes.get() + 1, ..CodeModeLimits::default() };
        assert!(matches!(
            CodeModeEngine::try_new(too_much_memory),
            Err(CodeModeError::InvalidLimits("memory limit exceeds the process-wide CodeMode Wasmtime maximum"))
        ));
    }

    #[tokio::test]
    async fn javascript_globals_do_not_leak_between_executions() {
        let engine = CodeModeEngine::try_new(CodeModeLimits::default()).expect("codemode engine");
        let dispatcher: StdArc<dyn ToolDispatcher> = StdArc::new(NoTools);

        let first = engine
            .execute(ExecuteRequest::new("globalThis.secret = 42; return secret"), StdArc::clone(&dispatcher))
            .await;
        let second = engine.execute(ExecuteRequest::new("return typeof secret"), dispatcher).await;

        assert!(first.result.is_ok(), "{first:?}");
        assert!(matches!(second.result, Ok(Value::String(ref value)) if value == "undefined"), "{second:?}");
    }

    #[tokio::test]
    async fn sdk_separates_catalog_discovery_from_qualified_tool_calls() {
        let engine = CodeModeEngine::try_new(CodeModeLimits::default()).expect("codemode engine");
        let source = r#"
            const found = await catalog.tools.search({ query: "customer", namespace: "crm", limit: 3 });
            const called = await tools["crm.find_customer"]({ email: "person@example.com" });
            return { found, called };
        "#;

        let outcome = engine.execute(ExecuteRequest::new(source), StdArc::new(SdkDispatcher)).await;

        let result = outcome.result.expect("sandbox SDK call");
        assert_eq!(result["found"]["data"]["operation"], "tools_search");
        assert_eq!(result["called"]["data"]["tool"], "crm.find_customer");
        assert_eq!(result["called"]["data"]["args"]["email"], "person@example.com");
    }

    #[tokio::test]
    async fn function_property_names_are_valid_tool_path_segments() {
        let engine = CodeModeEngine::try_new(CodeModeLimits::default()).expect("codemode engine");
        let outcome =
            engine.execute(ExecuteRequest::new("return await tools.name.length({})"), StdArc::new(SdkDispatcher)).await;

        let result = outcome.result.expect("function-property path segments must resolve");
        assert_eq!(result["data"]["tool"], "name.length");
    }

    #[tokio::test]
    async fn tools_enumeration_points_to_the_catalog() {
        let engine = CodeModeEngine::try_new(CodeModeLimits::default()).expect("codemode engine");
        let outcome =
            engine.execute(ExecuteRequest::new("return Object.keys(tools)"), StdArc::new(SdkDispatcher)).await;

        assert!(
            matches!(
                outcome.result,
                Err(CodeModeError::Guest {
                    kind: GuestErrorKind::JavaScript,
                    ref message,
                }) if message.contains("catalog.tools.search")
            ),
            "{outcome:?}"
        );
    }

    #[tokio::test]
    async fn oversized_catalog_results_are_branchable() {
        let limits = CodeModeLimits { max_output_bytes: 1024, ..CodeModeLimits::default() };
        let engine = CodeModeEngine::try_new(limits).expect("codemode engine");
        let outcome = engine
            .execute(
                ExecuteRequest::new("return await catalog.toolkits.get({name: 'large'})"),
                StdArc::new(LargeCatalog),
            )
            .await;

        let result = outcome.result.expect("size failure should remain a catalog value");
        assert_eq!(result["ok"], false);
        assert_eq!(result["error"]["code"], "result_too_large");
    }

    #[tokio::test]
    async fn interrupts_runaway_javascript_at_the_cpu_budget() {
        let limits = CodeModeLimits {
            cpu_timeout: Duration::from_millis(50),
            execution_timeout: Duration::from_secs(1),
            ..CodeModeLimits::default()
        };
        let engine = CodeModeEngine::try_new(limits).expect("codemode engine");

        let outcome = engine.execute(ExecuteRequest::new("(() => { while (true) {} })()"), StdArc::new(NoTools)).await;

        assert!(matches!(outcome.result, Err(CodeModeError::CpuLimit)), "{outcome:?}");
    }

    #[tokio::test]
    async fn catalog_and_tool_requests_share_the_host_call_budget() {
        let limits = CodeModeLimits { max_host_calls: 1, ..CodeModeLimits::default() };
        let engine = CodeModeEngine::try_new(limits).expect("codemode engine");
        let source = r#"
            await catalog.tools.search({ query: "customer" });
            return await tools["crm.find_customer"]({});
        "#;

        let outcome = engine.execute(ExecuteRequest::new(source), StdArc::new(SdkDispatcher)).await;

        assert!(matches!(outcome.result, Err(CodeModeError::HostCallLimit)), "{outcome:?}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrency_cap_bounds_inflight_dispatches() {
        let limits = CodeModeLimits { max_concurrent_tool_calls: 2, ..CodeModeLimits::default() };
        let engine = CodeModeEngine::try_new(limits).expect("codemode engine");
        let (dispatcher, gauge) = DelayingDispatcher::new();
        let source = "return await Promise.all([tools.a.x({}), tools.b.x({}), tools.c.x({}), tools.d.x({})])";

        let outcome = engine.execute(ExecuteRequest::new(source), dispatcher).await;

        assert!(outcome.result.is_ok(), "{outcome:?}");
        assert_eq!(gauge.peak.load(std::sync::atomic::Ordering::SeqCst), 2, "cap not respected");
    }

    #[tokio::test]
    async fn budget_exhaustion_mid_parallel_batch_reports_the_limit() {
        let limits = CodeModeLimits { max_host_calls: 2, ..CodeModeLimits::default() };
        let engine = CodeModeEngine::try_new(limits.clone()).expect("codemode engine");
        let (dispatcher, _gauge) = DelayingDispatcher::new();
        let source = "return await Promise.all([tools.fast.x({}), tools.fast.y({}), tools.fast.z({})])";

        let outcome = engine.execute(ExecuteRequest::new(source), dispatcher).await;
        assert!(matches!(outcome.result, Err(CodeModeError::HostCallLimit)), "{outcome:?}");

        // A caught budget exception cannot mask the exceeded limit.
        let engine = CodeModeEngine::try_new(limits).expect("codemode engine");
        let (dispatcher, _gauge) = DelayingDispatcher::new();
        let caught = r#"
            try {
                return await Promise.all([tools.fast.x({}), tools.fast.y({}), tools.fast.z({})]);
            } catch (error) {
                return { caught: String(error) };
            }
        "#;

        let outcome = engine.execute(ExecuteRequest::new(caught), dispatcher).await;
        assert!(matches!(outcome.result, Err(CodeModeError::HostCallLimit)), "{outcome:?}");
    }

    #[tokio::test]
    async fn completion_polling_does_not_consume_the_host_call_budget() {
        let limits = CodeModeLimits { max_host_calls: 3, ..CodeModeLimits::default() };
        let engine = CodeModeEngine::try_new(limits.clone()).expect("codemode engine");
        let (dispatcher, _gauge) = DelayingDispatcher::new();
        let source = r#"
            console.log("go");
            return await Promise.all([tools.fast.x({}), tools.fast.y({})]);
        "#;

        let outcome = engine.execute(ExecuteRequest::new(source), dispatcher).await;
        assert!(outcome.result.is_ok(), "{outcome:?}");

        // The same budget still counts every log and submission.
        let engine = CodeModeEngine::try_new(limits).expect("codemode engine");
        let (dispatcher, _gauge) = DelayingDispatcher::new();
        let over = r#"
            console.log("go");
            console.log("again");
            return await Promise.all([tools.fast.x({}), tools.fast.y({})]);
        "#;

        let outcome = engine.execute(ExecuteRequest::new(over), dispatcher).await;
        assert!(matches!(outcome.result, Err(CodeModeError::HostCallLimit)), "{outcome:?}");
    }

    #[tokio::test]
    async fn stalled_executions_fail_instead_of_riding_out_the_timeout() {
        let engine = CodeModeEngine::try_new(CodeModeLimits::default()).expect("codemode engine");
        let source = "await new Promise(() => {}); return 1";

        let started = Instant::now();
        let outcome = engine.execute(ExecuteRequest::new(source), StdArc::new(NoTools)).await;
        let elapsed = started.elapsed();

        assert!(
            matches!(
                outcome.result,
                Err(CodeModeError::Guest {
                    kind: GuestErrorKind::JavaScript,
                    ref message,
                }) if message.contains("stalled")
            ),
            "{outcome:?}"
        );
        assert!(elapsed < Duration::from_secs(2), "stall took the wall-clock route: {elapsed:?}");
    }

    #[tokio::test]
    async fn infrastructure_defects_reject_the_pending_promise() {
        let engine = CodeModeEngine::try_new(CodeModeLimits::default()).expect("codemode engine");
        let (dispatcher, _gauge) = DelayingDispatcher::new();
        let source = r#"
            try {
                await tools.bad.x({});
                return { caught: false };
            } catch (error) {
                return { caught: String(error) };
            }
        "#;

        let outcome = engine.execute(ExecuteRequest::new(source), dispatcher).await;

        let result = outcome.result.expect("a rejected call must be catchable");
        assert!(result["caught"].as_str().is_some_and(|message| message.contains("invalid response")), "{result:?}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn promise_all_rejection_cancels_inflight_siblings() {
        let engine = CodeModeEngine::try_new(CodeModeLimits::default()).expect("codemode engine");
        let (dispatcher, gauge) = DelayingDispatcher::new();
        let source = "return await Promise.all([tools.bad.x({}), tools.hang.x({})])";

        let outcome = engine.execute(ExecuteRequest::new(source), dispatcher).await;

        assert!(
            matches!(
                outcome.result,
                Err(CodeModeError::Guest { kind: GuestErrorKind::JavaScript, ref message })
                    if message.contains("invalid response")
            ),
            "{outcome:?}"
        );
        gauge.settle().await;
        assert_eq!(gauge.completed.load(std::sync::atomic::Ordering::SeqCst), 0, "sibling ran to completion");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mixed_catalog_and_tool_calls_run_in_parallel() {
        let engine = CodeModeEngine::try_new(CodeModeLimits::default()).expect("codemode engine");
        let (dispatcher, gauge) = DelayingDispatcher::new();
        let source = r#"
            const [found, called] = await Promise.all([
                catalog.tools.search({ query: "x" }),
                tools.crm.find_customer({})
            ]);
            return { found, called };
        "#;

        let outcome = engine.execute(ExecuteRequest::new(source), dispatcher).await;

        let result = outcome.result.expect("mixed parallel calls should succeed");
        assert_eq!(result["found"]["data"]["operation"], "catalog");
        assert_eq!(result["called"]["data"], "crm.find_customer");
        assert_eq!(gauge.peak.load(std::sync::atomic::Ordering::SeqCst), 2, "calls never overlapped");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn log_order_is_preserved_across_parallel_completion() {
        let engine = CodeModeEngine::try_new(CodeModeLimits::default()).expect("codemode engine");
        let (dispatcher, _gauge) = DelayingDispatcher::new();
        let source = r#"
            console.log("before");
            const pa = tools.fast.x({}).then(result => { console.log("a"); return result; });
            const pb = tools.slow.x({}).then(result => { console.log("b"); return result; });
            await Promise.all([pa, pb]);
            console.log("after");
            return 1;
        "#;

        let outcome = engine.execute(ExecuteRequest::new(source), dispatcher).await;

        assert!(outcome.result.is_ok(), "{outcome:?}");
        assert_eq!(outcome.logs, ["before", "a", "b", "after"]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn floating_calls_are_cancelled_when_the_script_settles() {
        let engine = CodeModeEngine::try_new(CodeModeLimits::default()).expect("codemode engine");
        let (dispatcher, gauge) = DelayingDispatcher::new();
        let source = "tools.hang.x({}); return 1";

        let outcome = engine.execute(ExecuteRequest::new(source), dispatcher).await;

        assert!(matches!(outcome.result, Ok(Value::Number(_))), "{outcome:?}");
        gauge.settle().await;
        assert_eq!(gauge.completed.load(std::sync::atomic::Ordering::SeqCst), 0, "floating call ran to completion");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn execution_timeout_aborts_inflight_dispatches() {
        let limits = CodeModeLimits { execution_timeout: Duration::from_millis(300), ..CodeModeLimits::default() };
        let engine = CodeModeEngine::try_new(limits).expect("codemode engine");
        let (dispatcher, gauge) = DelayingDispatcher::new();
        let source = "return await Promise.all([tools.hang.x({}), tools.hang.y({})])";

        let outcome = engine.execute(ExecuteRequest::new(source), dispatcher).await;

        assert!(matches!(outcome.result, Err(CodeModeError::Timeout)), "{outcome:?}");
        gauge.settle().await;
        assert_eq!(gauge.completed.load(std::sync::atomic::Ordering::SeqCst), 0, "aborted call ran to completion");
    }
}
