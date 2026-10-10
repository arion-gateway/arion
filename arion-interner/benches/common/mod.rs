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

//! Interner implementations under comparison, shared by all benchmarks.
//! To benchmark a new implementation, add it to [`IMPLS`].

pub type InternFn = fn(&str) -> &'static str;

pub const IMPLS: &[(&str, InternFn)] = &[
    ("legacy-tls", arion_interner::legacy::intern_tls as InternFn),
    ("legacy-glb", arion_interner::legacy::intern_global as InternFn),
    ("papaya-glb", arion_interner::papaya::intern as InternFn),
];

/// Number of concurrent worker threads used by every benchmark.
pub const THREADS: usize = 8;
