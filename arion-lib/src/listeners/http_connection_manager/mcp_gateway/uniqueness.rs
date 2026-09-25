// Copyright 2025-2026 The arion-gateway Authors
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

use papaya::HashSet as PapayaSet;
use smol_str::SmolStr;
use std::sync::OnceLock;

/// Per-runtime claim table for MCP `server_info.name`.
///
/// Prevents two MCP gateway filters on the same runtime from using the
/// same logical server name — which would otherwise let accidental config
/// collisions cross listener scopes when xDS updates fan out.
static SERVER_NAMES: OnceLock<PapayaSet<(usize, SmolStr), ahash::RandomState>> = OnceLock::new();

fn map() -> &'static PapayaSet<(usize, SmolStr), ahash::RandomState> {
    SERVER_NAMES.get_or_init(|| PapayaSet::with_hasher(ahash::RandomState::new()))
}

pub fn claim(runtime_id: usize, server_name: SmolStr) -> Result<(), SmolStr> {
    if map().pin().insert((runtime_id, server_name.clone())) {
        Ok(())
    } else {
        Err(server_name)
    }
}

pub fn release(runtime_id: usize, server_name: &str) {
    map().pin().remove(&(runtime_id, SmolStr::from(server_name)));
}

#[cfg(test)]
mod tests {
    #![allow(clippy::assertions_on_result_states)]
    use super::*;

    #[test]
    fn claim_is_exclusive_per_runtime() {
        let name: SmolStr = "uniqueness_test_server_a".into();
        assert!(claim(9001, name.clone()).is_ok());
        assert_eq!(claim(9001, name.clone()), Err(name.clone()));
        release(9001, &name);
        assert!(claim(9001, name.clone()).is_ok());
        release(9001, &name);
    }

    #[test]
    fn claim_is_independent_across_runtimes() {
        let name: SmolStr = "uniqueness_test_server_b".into();
        assert!(claim(9100, name.clone()).is_ok());
        assert!(claim(9101, name.clone()).is_ok());
        release(9100, &name);
        release(9101, &name);
    }
}
