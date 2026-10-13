# Arion Interner

`arion-interner` is a small crate that turns strings into process-wide, `'static`, deduplicated slices (*interning*). It is used across the proxy (`arion-configuration`, `arion-lib`, `arion-proxy`, `arion-metrics`, `arion-format`, `arion-tracing`) for strings that are created rarely and read very often: listener, cluster, route and filter names, metric labels, and so on.

Interning a string means storing it once and then handing out a `&'static str` that points to the stored copy. This gives:

- **Cheap copies**: an interned string is just a pointer and a length (`Copy`), so it can be passed around, stored in structs and shared between threads without cloning or reference counting.
- **Deduplication**: equal strings share the same memory, no matter how many times or from how many threads they are interned.
- **`'static` lifetime**: interned strings can be stored anywhere (metrics registries, long-lived tasks, `'static` structs) without lifetime constraints.

The downside is that interned strings are **never freed**: memory grows with the number of distinct strings. Because of this the interner is bounded (see [Capacity limit](#capacity-limit)), and it must not be fed unbounded, client-controlled input.

## Public API

### `StringInterner` trait

The main entry point is the `StringInterner` trait. It is implemented for `str`, `&'static str`, `String`, `SmolStr`, and for the `http` types `Method`, `Version` and `Scheme`.

| Method | Returns | When the interner is full |
|--------|---------|---------------------------|
| `try_to_static_str()` | `Option<&'static str>` | `None` |
| `try_to_interned_str()` | `Option<InternedStr>` | `None` |
| `to_static_str()` | `&'static str` | logs a warning, returns `""` |
| `to_interned_str()` | `InternedStr` | logs a warning, returns `""` |

Use the `try_*` methods when the caller can handle the failure, for example by rejecting a configuration. The infallible methods are convenient where an empty string is an acceptable degraded value.

Some implementations never touch the interner:

- **`Version`** always maps to a constant (`"HTTP/1.1"`, `"HTTP/2"`, ...).
- **`Method`**: the standard methods (`GET`, `POST`, ...) map to constants. Extension methods are interned.
- **`Scheme`**: `http` and `https` map to constants. Other schemes are interned.
- **`&'static str`**: `to_static_str()` returns the slice as is, because it is already `'static`. `to_interned_str()` interns it. If the interner is full, it wraps the original slice instead of returning `""`.

```rust
use arion_interner::{InternedStr, StringInterner};

let name: InternedStr = String::from("listener-1").to_interned_str();
assert_eq!(name.as_str(), "listener-1");

// Fallible variant: let the caller decide what to do when the interner is full.
let Some(cluster) = "cluster-a".to_owned().try_to_static_str() else {
    return Err("too many distinct names");
};
```

### `InternedStr`

`InternedStr` is a thin `Copy` wrapper around `&'static str`. It exists mainly so that interned strings can be used in `serde`-derived structs, where a bare `&'static str` cannot be deserialized. It implements `Deref<Target = str>`, `Borrow<str>`, `Display`, `Hash`, `Eq`, `Ord`, `Default`, `Serialize` and `Deserialize`.

- Equality, ordering and hashing compare **contents**, not pointers. An `InternedStr` that is not canonical (see the `&'static str` fallback above) still behaves correctly.
- **Deserialization fails** with a serde error if the interner is full, instead of silently producing `""`. An empty name in a parsed configuration would be a silent corruption, so the error makes the configuration load (or xDS update) fail with a clear message.
- `InternedStr::lookup(s)` returns the interned value **only if `s` has already been interned**, and never inserts. It is safe to call with untrusted input, for example to look up a header value against a set of configured names.

## Implementation

The backend is a single global, lock-free [`papaya::HashMap`](https://docs.rs/papaya) keyed by the leaked `&'static str`, with the `ahash` hasher.

Key design points:

- **One map for the whole process.** Every thread sees the same map, so the returned slice is *canonical*: equal strings always resolve to the same pointer.
- **Lookup first, insert on the cold path.** The common case (a string that is already interned) is a single lock-free read. The insert path is `#[cold]` and `#[inline(never)]` to keep the fast path small.
- **`try_insert`, never `insert`.** When the key already exists, papaya's `insert` replaces the stored key. `try_insert` never does, so the first published slice stays canonical for the lifetime of the process.
- **Races do not leak.** If two threads insert the same new string at the same time, the loser frees its own copy and returns the winner's slice.
- **Incremental resizes.** papaya grows the table incrementally, so inserts never cause a stop-the-world rehash.

## Capacity limit

The interner holds at most `max_entries` distinct strings:

- The default is `DEFAULT_MAX_ENTRIES` (2^20, i.e. 1,048,576 strings).
- The environment variable **`ARION_INTERNER_MAX_ENTRIES`** overrides the default. It is read once, on first use. An invalid value logs a warning and the default is used. A value of `0` disables interning of new strings.

When the map is full:

- **Already-interned strings keep resolving**, because lookups are not affected by the limit.
- **New strings are rejected**: `try_*` return `None`, `to_static_str()` / `to_interned_str()` return `""`, and `Deserialize` fails.
- **A warning is logged with a rate limit**, on the 1st, 2nd, 4th, 8th, ... rejection. Once the map is full, every new string is rejected, possibly on a request path, and logging each rejection would flood the logs.

The limit is checked before allocating, so a rejected string costs no allocation. The limit is *soft*: threads that pass the check at the same time can each insert one more entry, so the map can briefly exceed `max_entries` by at most the number of concurrent inserters.

## Guidelines

- **Do intern** strings that come from the configuration (bootstrap or xDS): names, labels, identifiers. Their number is bounded and they are read on hot paths.
- **Do intern** request data such as SNI or header values only when it comes from trusted, operator-controlled sources (for example, headers set by an internal gateway or SNI values limited to a known set of hostnames), so that the number of distinct values stays bounded.
- **Do not intern** strings that clients can set freely (paths, query parameters, arbitrary header values or SNI). Use `InternedStr::lookup` to match them against interned values without inserting them, or map them to a fixed value (for example OpenTelemetry's `_OTHER` for unknown HTTP methods).
- **Prefer the `try_*` methods** when a failure can be reported, so a full interner cannot produce empty names silently.

## Benchmarks

The crate includes two benchmarks that compare the papaya backend with the legacy `lasso`-based interners kept in the `legacy` module:

- `intern_hit`: several threads repeatedly re-intern a pre-warmed set of strings (pure lookup throughput).
  `cargo bench -p arion-interner --bench intern_hit`
- `intern_miss`: several threads intern always-new strings for a fixed duration. It measures insert throughput, latency spikes and retained memory per string. It runs without the capacity limit.
  `cargo bench -p arion-interner --bench intern_miss [-- <impl-filter>]` (env: `INTERN_BENCH_SECS`, `INTERN_BENCH_MEM_CAP_MIB`)
