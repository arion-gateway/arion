use std::sync::atomic::Ordering;
use std::sync::OnceLock;
use std::time::Instant;

use atomicoption::AtomicOption;
use serde::Serialize;

static STARTUP_TIME: OnceLock<Instant> = OnceLock::new();
static PROXY_STATE: AtomicOption<ProxyState> = AtomicOption::none();

#[derive(Clone, Debug, Default, Serialize)]
pub enum ProxyState {
    #[default]
    Initializing,
    Draining,
    PreInitializing,
    Live,
}

pub fn init_startup_time() {
    _ = STARTUP_TIME.set(Instant::now());
}

pub fn get_startup_time() -> Option<&'static Instant> {
    STARTUP_TIME.get()
}

pub fn set_proxy_state(state: ProxyState) {
    PROXY_STATE.store(Ordering::Relaxed, state);
}

pub fn get_proxy_state() -> Option<&'static ProxyState> {
    PROXY_STATE.as_ref(Ordering::Relaxed)
}

/// Return the physical memory allocated by the process.
///
pub fn get_memory_physical_size() -> Option<usize> {
    memory_stats::memory_stats().map(|s| s.physical_mem)
}

/// Return the precise memory allocated on the heap.
///
pub fn get_memory_allocated() -> Option<usize> {
    #[cfg(all(feature = "jemalloc", not(target_env = "msvc")))]
    {
        use tikv_jemalloc_ctl::{epoch, stats};
        epoch::advance().ok()?;
        let allocated_bytes = stats::allocated::mib().ok()?;
        allocated_bytes.read().ok()
    }
    #[cfg(not(all(feature = "jemalloc", not(target_env = "msvc"))))]
    {
        None
    }
}

/// Return the total size of the heap (active pages) managed by the allocator.
///
pub fn get_memory_heap_size() -> Option<usize> {
    #[cfg(all(feature = "jemalloc", not(target_env = "msvc")))]
    {
        use tikv_jemalloc_ctl::{epoch, stats};
        epoch::advance().ok()?;
        let active_bytes = stats::active::mib().ok()?;
        active_bytes.read().ok()
    }
    #[cfg(not(all(feature = "jemalloc", not(target_env = "msvc"))))]
    {
        None
    }
}

pub fn server_uptime() -> u64 {
    let start_up_time = STARTUP_TIME.get().copied().unwrap_or_else(Instant::now);
    start_up_time.elapsed().as_secs()
}
