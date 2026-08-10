use crate::ffi;
use orion_wasm_types::SharedOrdering;
use std::sync::atomic::Ordering;

use super::SharedVarError;

pub struct SharedAtomicU64 {
    id: u32,
}

impl SharedAtomicU64 {
    pub fn try_new(name: &str) -> Result<Self, SharedVarError> {
        let id = unsafe { ffi::ext_shared_resolve(name.as_ptr(), name.len() as u32, 0) };
        if id == u32::MAX {
            Err(SharedVarError::InitFailed)
        } else {
            Ok(Self { id })
        }
    }

    pub fn load(&self, order: Ordering) -> u64 {
        let shared_order: SharedOrdering = order.into();
        unsafe { ffi::ext_shared_u64_load(self.id, shared_order.into()) }
    }

    pub fn store(&self, val: u64, order: Ordering) {
        let shared_order: SharedOrdering = order.into();
        unsafe { ffi::ext_shared_u64_store(self.id, val, shared_order.into()) }
    }

    pub fn swap(&self, val: u64, order: Ordering) -> u64 {
        let shared_order: SharedOrdering = order.into();
        unsafe { ffi::ext_shared_u64_swap(self.id, val, shared_order.into()) }
    }

    pub fn compare_exchange(&self, current: u64, new: u64, success: Ordering, failure: Ordering) -> Result<u64, u64> {
        let succ: SharedOrdering = success.into();
        let fail: SharedOrdering = failure.into();
        let prev = unsafe { ffi::ext_shared_u64_compare_exchange(self.id, current, new, succ.into(), fail.into()) };
        if prev == current {
            Ok(prev)
        } else {
            Err(prev)
        }
    }

    pub fn compare_exchange_weak(
        &self,
        current: u64,
        new: u64,
        success: Ordering,
        failure: Ordering,
    ) -> Result<u64, u64> {
        self.compare_exchange(current, new, success, failure)
    }

    pub fn fetch_add(&self, val: u64, order: Ordering) -> u64 {
        let shared_order: SharedOrdering = order.into();
        unsafe { ffi::ext_shared_u64_fetch_add(self.id, val, shared_order.into()) }
    }

    pub fn fetch_sub(&self, val: u64, order: Ordering) -> u64 {
        let shared_order: SharedOrdering = order.into();
        unsafe { ffi::ext_shared_u64_fetch_sub(self.id, val, shared_order.into()) }
    }

    pub fn fetch_and(&self, val: u64, order: Ordering) -> u64 {
        let shared_order: SharedOrdering = order.into();
        unsafe { ffi::ext_shared_u64_fetch_and(self.id, val, shared_order.into()) }
    }

    pub fn fetch_nand(&self, val: u64, order: Ordering) -> u64 {
        let shared_order: SharedOrdering = order.into();
        unsafe { ffi::ext_shared_u64_fetch_nand(self.id, val, shared_order.into()) }
    }

    pub fn fetch_or(&self, val: u64, order: Ordering) -> u64 {
        let shared_order: SharedOrdering = order.into();
        unsafe { ffi::ext_shared_u64_fetch_or(self.id, val, shared_order.into()) }
    }

    pub fn fetch_xor(&self, val: u64, order: Ordering) -> u64 {
        let shared_order: SharedOrdering = order.into();
        unsafe { ffi::ext_shared_u64_fetch_xor(self.id, val, shared_order.into()) }
    }

    pub fn fetch_max(&self, val: u64, order: Ordering) -> u64 {
        let shared_order: SharedOrdering = order.into();
        unsafe { ffi::ext_shared_u64_fetch_max(self.id, val, shared_order.into()) }
    }

    pub fn fetch_min(&self, val: u64, order: Ordering) -> u64 {
        let shared_order: SharedOrdering = order.into();
        unsafe { ffi::ext_shared_u64_fetch_min(self.id, val, shared_order.into()) }
    }

    pub fn fetch_update<F>(&self, set_order: Ordering, fetch_order: Ordering, mut f: F) -> Result<u64, u64>
    where
        F: FnMut(u64) -> Option<u64>,
    {
        let mut prev = self.load(fetch_order);
        while let Some(next) = f(prev) {
            match self.compare_exchange_weak(prev, next, set_order, fetch_order) {
                Ok(x) => return Ok(x),
                Err(next_prev) => prev = next_prev,
            }
        }
        Err(prev)
    }
}
