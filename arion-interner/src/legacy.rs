// Copyright 2026 The arion-gateway Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Legacy thread-local interner (one leaked `lasso::Rodeo` per thread).
//!
//! Kept verbatim as the baseline for benchmarks: new implementations are
//! compared against this one in `benches/`. Do not optimize it in place.

use std::{cell::RefCell, sync::OnceLock};

use lasso::{Rodeo, ThreadedRodeo};

thread_local! {
    /// Thread-local interner. The `Rodeo` is leaked so interned strings remain
    /// valid after this thread exits (the TLS slot itself is still dropped).
    static THREAD_LOCAL_INTERNER: RefCell<&'static mut Rodeo> = RefCell::new(Box::leak(Box::new(Rodeo::new())));
}

/// Interns `s` in the calling thread's interner and returns a `'static` slice.
#[inline]
pub fn intern_tls(s: &str) -> &'static str {
    THREAD_LOCAL_INTERNER.with_borrow_mut(|interner| {
        let key = &mut interner.get_or_intern(s);
        // SAFETY: `resolve` ties the `&str` to the temporary `RefMut` of the TLS slot.
        // The `Rodeo` is heap-allocated and leaked (`Box::leak`), so it is never dropped
        // when this thread exits — only the `RefCell` (a pointer) is. Interned slices
        // therefore remain valid for the rest of the process, which is the `'static`
        // lifetime we extend to here.
        unsafe { std::mem::transmute::<&str, &'static str>(interner.resolve(key)) }
    })
}

// ---------------------------------------------------------------------------

static GLOBAL_INTERNER: OnceLock<&'static ThreadedRodeo> = OnceLock::new();

/// Interns `s` using a single global `ThreadedRodeo`.
#[inline]
pub fn intern_global(s: &str) -> &'static str {
    let interner = GLOBAL_INTERNER.get_or_init(|| Box::leak(Box::new(ThreadedRodeo::new())));
    let key = interner.get_or_intern(s);
    // SAFETY: The ThreadedRodeo is leaked, so it lives forever.
    // resolve() returns a reference tied to the lifetime of the interner borrow.
    // We transmute to 'static since the interner itself is 'static.
    unsafe { std::mem::transmute::<&str, &'static str>(interner.resolve(&key)) }
}
