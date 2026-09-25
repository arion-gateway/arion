// Copyright 2025 The kmesh Authors
// Copyright 2026 The arion-gateway Authors
//
// Modified by arion-gateway Authors.
//
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
//
//

#![allow(clippy::print_stdout)]

use arion_configuration::config::Bootstrap;
use arion_error::{Context, Result};
use std::fs::File;

fn main() -> Result<()> {
    let bootstrap = Bootstrap::deserialize_from_envoy(
        File::open("bootstrap.yaml").with_context_msg("failed to open bootstrap.yaml")?,
    )
    .with_context_msg("failed to convert envoy to arion")?;
    let yaml = serde_yaml::to_string(&bootstrap).with_context_msg("failed to serialize arion")?;
    std::fs::write("arion.yaml", yaml.as_bytes())?;
    let bootstrap: Bootstrap =
        serde_yaml::from_reader(File::open("arion.yaml").with_context_msg("failed to open arion.yaml")?)
            .with_context_msg("failed to read yaml from file")?;
    let yaml = serde_yaml::to_string(&bootstrap).with_context_msg("failed to round-trip serialize arion")?;
    println!("{yaml}");
    Ok(())
}
