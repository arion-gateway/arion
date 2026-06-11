use std::sync::OnceLock;
use std::time::Instant;

pub static STARTUP_TIME: OnceLock<Instant> = OnceLock::new();

pub fn init_startup_time() {
    _ = STARTUP_TIME.set(Instant::now());
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
