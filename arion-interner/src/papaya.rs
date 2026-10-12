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

//! Global, process-wide interner backed by a lock-free [`papaya::HashMap`].
//!
//! # Design
//!
//! - **One map for the whole process**: a string is stored once, regardless of how
//!   many threads intern it, and the returned slice is canonical (same pointer for
//!   equal strings, from any thread).
//! - **`try_insert`, never `insert`**: on an existing key, papaya's `insert` (and
//!   `HashSet::insert`) *replaces* the stored key with the new one. `try_insert`
//!   never replaces, so the first published slice stays canonical forever.
//! - **Resizes**: papaya resizes incrementally by default, so there is no
//!   stop-the-world rehash on the insert path.
//!
//! - **Bounded**: the map holds at most [`max_entries`] strings (default
//!   [`DEFAULT_MAX_ENTRIES`], overridable via [`MAX_ENTRIES_ENV_VAR`]). Once full,
//!   [`intern`] returns `None` for *new* strings; already-interned strings keep
//!   resolving. The bound is soft: threads racing past the check concurrently may
//!   overshoot it by at most one entry each.
//!
//! Like every interner that leaks, memory grows with the number of distinct strings:
//! never intern unbounded, client-controlled input (use [`lookup`] there).

use std::sync::LazyLock;

use papaya::HashMap;

type Map = HashMap<&'static str, (), ahash::RandomState>;

/// Initial capacity: enough for typical configs without early resizes.
const INITIAL_CAPACITY: usize = 1024;

/// Default maximum number of distinct interned strings.
pub const DEFAULT_MAX_ENTRIES: usize = 1 << 20;

/// Environment variable overriding [`DEFAULT_MAX_ENTRIES`]. Read once, on first insert.
pub const MAX_ENTRIES_ENV_VAR: &str = "ARION_INTERNER_MAX_ENTRIES";

static INTERNER: LazyLock<Map> =
    LazyLock::new(|| HashMap::with_capacity_and_hasher(INITIAL_CAPACITY, ahash::RandomState::new()));

static MAX_ENTRIES: LazyLock<usize> = LazyLock::new(|| parse_max_entries(std::env::var(MAX_ENTRIES_ENV_VAR).ok().as_deref()));

/// Maximum number of distinct strings the global interner accepts.
#[inline]
pub fn max_entries() -> usize {
    *MAX_ENTRIES
}

fn parse_max_entries(value: Option<&str>) -> usize {
    let Some(raw) = value else {
        return DEFAULT_MAX_ENTRIES;
    };
    match raw.trim().parse() {
        Ok(max) => max,
        Err(e) => {
            tracing::warn!(
                value = raw, error = %e, default = DEFAULT_MAX_ENTRIES,
                "{MAX_ENTRIES_ENV_VAR} is not a valid number of entries, using the default"
            );
            DEFAULT_MAX_ENTRIES
        },
    }
}

/// Interns `s` and returns the canonical, process-wide `'static` slice,
/// or `None` if `s` is new and the interner already holds [`max_entries`] strings.
#[inline]
#[must_use]
pub fn intern(s: &str) -> Option<&'static str> {
    match lookup(s) {
        Some(interned) => Some(interned),
        None => insert_slow(s, max_entries()),
    }
}

/// Like [`intern`], but with an explicit bound instead of [`max_entries`].
/// Exposed (hidden) for benchmarks and tests.
#[doc(hidden)]
#[inline]
#[must_use]
pub fn intern_bounded(s: &str, max_entries: usize) -> Option<&'static str> {
    match lookup(s) {
        Some(interned) => Some(interned),
        None => insert_slow(s, max_entries),
    }
}

/// Returns the canonical slice for `s` if it has already been interned,
/// **without** interning it. Safe to call with untrusted input.
#[inline]
pub fn lookup(s: &str) -> Option<&'static str> {
    INTERNER.pin().get_key_value(s).map(|(&interned, ())| interned)
}

/// Number of distinct interned strings globally.
pub fn len() -> usize {
    INTERNER.len()
}

#[cold]
#[inline(never)]
fn insert_slow(s: &str, max_entries: usize) -> Option<&'static str> {
    let map = INTERNER.pin();
    if map.len() >= max_entries {
        // Full, but another thread may have published `s` since the caller's lookup.
        return map.get_key_value(s).map(|(&interned, ())| interned);
    }

    let leaked: &'static str = Box::leak(Box::from(s));

    if let Ok(()) = map.try_insert(leaked, ()) {
        Some(leaked)
    } else {
        // Lost the race: another thread published the same string first.
        // Keys are never removed nor replaced, so the winner is guaranteed to be there.
        let winner = map.get_key_value(s).map_or(leaked, |(&interned, ())| interned);

        // If we did not return our newly allocated string, we must free it to avoid
        // a memory leak on races.
        if !std::ptr::eq(winner, leaked) {
            // SAFETY: `leaked` was just allocated by us via `Box::leak(Box::from(s))`.
            // It was rejected by the map (Err), so no other thread has a reference to it.
            // Reconstructing the Box and dropping it is safe and frees the memory.
            drop(unsafe { Box::from_raw(leaked.as_ptr().cast_mut()) });
        }

        Some(winner)
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Barrier, thread};

    use super::*;

    #[test]
    fn interning_is_canonical() {
        let a = intern("global-canonical").unwrap();
        let b = intern(&String::from("global-canonical")).unwrap();
        assert_eq!(a, "global-canonical");
        assert!(std::ptr::eq(a, b));
    }

    #[test]
    fn lookup_does_not_insert() {
        assert_eq!(lookup("global-never-interned"), None);
        assert_eq!(lookup("global-never-interned"), None);
        let s = intern("global-looked-up").unwrap();
        assert!(lookup("global-looked-up").is_some_and(|l| std::ptr::eq(l, s)));
    }

    #[test]
    fn full_interner_rejects_new_strings_but_resolves_existing_ones() {
        let existing = intern("global-bounded-existing").unwrap();
        assert_eq!(intern_bounded("global-bounded-new", 0), None);
        assert_eq!(lookup("global-bounded-new"), None);
        assert!(intern_bounded("global-bounded-existing", 0).is_some_and(|s| std::ptr::eq(s, existing)));
    }

    #[test]
    fn parse_max_entries_falls_back_to_default() {
        assert_eq!(parse_max_entries(None), DEFAULT_MAX_ENTRIES);
        assert_eq!(parse_max_entries(Some(" 42 ")), 42);
        assert_eq!(parse_max_entries(Some("0")), 0);
        assert_eq!(parse_max_entries(Some("lots")), DEFAULT_MAX_ENTRIES);
        assert_eq!(parse_max_entries(Some("-1")), DEFAULT_MAX_ENTRIES);
    }

    #[test]
    fn same_pointer_across_racing_threads() {
        const THREADS: usize = 8;
        let barrier = Barrier::new(THREADS);
        let ptrs: Vec<Vec<&'static str>> = thread::scope(|scope| {
            let handles: Vec<_> = (0..THREADS)
                .map(|_| {
                    scope.spawn(|| {
                        barrier.wait();
                        (0..1000).map(|i| intern(&format!("global-race-{i}")).unwrap()).collect()
                    })
                })
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });
        if let Some(first) = ptrs.first() {
            for other in ptrs.iter().skip(1) {
                for (a, b) in first.iter().zip(other) {
                    assert!(std::ptr::eq(*a, *b), "non-canonical slice for {a}");
                }
            }
        }
    }
}
