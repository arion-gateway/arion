// Copyright 2025 The kmesh Authors
// Copyright 2026 The arion-gateway Authors
//
// Modified by arion-gateway Authors.
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

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Build test protobufs
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&["proto/test_service.proto"], &["proto/"])?;

    // 2. Build Wasm filters for E2E testing
    // Cargo sets variables for build.rs that can interfere with a nested cross-compilation cargo build.
    let mut cmd = std::process::Command::new(std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_owned()));
    cmd.args(["build", "--target", "wasm32-unknown-unknown", "--workspace"]).current_dir("../orion-wasm-sdk");

    // Remove conflicting environment variables injected by Cargo into build.rs
    for (key, _) in std::env::vars() {
        if (key.starts_with("CARGO_") && key != "CARGO_HOME") || key == "TARGET" || key == "RUSTFLAGS" {
            cmd.env_remove(&key);
        }
    }

    let status = cmd.status()?;
    if !status.success() {
        println!("cargo:warning=Failed to build Wasm examples in orion-wasm-sdk! Wasm E2E tests may fail.");
    }

    Ok(())
}
