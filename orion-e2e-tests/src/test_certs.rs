// Copyright 2025 The kmesh Authors
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

use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct TestCerts {
    workspace_root: PathBuf,
}

impl Default for TestCerts {
    fn default() -> Self {
        Self::new()
    }
}

impl TestCerts {
    #[must_use]
    pub fn new() -> Self {
        let workspace_root = Self::find_workspace_root();
        Self { workspace_root }
    }

    fn find_workspace_root() -> PathBuf {
        let mut dir = std::env::current_dir().expect("Failed to get current directory");
        loop {
            if dir.join("test_certs").exists() {
                return dir;
            }
            if !dir.pop() {
                panic!("Could not find workspace root (directory containing test_certs/)");
            }
        }
    }

    fn test_certs_dir(&self) -> PathBuf {
        self.workspace_root.join("test_certs")
    }

    fn beefcake_dir(&self) -> PathBuf {
        self.test_certs_dir().join("beefcakeCA-gathered")
    }

    #[must_use]
    pub fn beefcake_ca_chain(&self) -> PathBuf {
        self.beefcake_dir().join("beefcake.intermediate.ca-chain.cert.pem")
    }

    #[must_use]
    pub fn beefcake_dublin_cert(&self) -> PathBuf {
        self.beefcake_dir().join("beefcake-dublin.cert.pem")
    }

    #[must_use]
    pub fn beefcake_dublin_key(&self) -> PathBuf {
        self.beefcake_dir().join("beefcake-dublin.key.pem")
    }

    #[must_use]
    pub fn beefcake_athlone_cert(&self) -> PathBuf {
        self.beefcake_dir().join("beefcake-athlone.cert.pem")
    }

    #[must_use]
    pub fn beefcake_athlone_key(&self) -> PathBuf {
        self.beefcake_dir().join("beefcake-athlone.key.pem")
    }

    fn deadbeef_dir(&self) -> PathBuf {
        self.test_certs_dir().join("deadbeefCA-gathered")
    }

    #[must_use]
    pub fn deadbeef_ca_chain(&self) -> PathBuf {
        self.deadbeef_dir().join("deadbeef.intermediate.ca-chain.cert.pem")
    }

    #[must_use]
    pub fn deadbeef_dublin_cert(&self) -> PathBuf {
        self.deadbeef_dir().join("deadbeef-dublin.cert.pem")
    }

    #[must_use]
    pub fn deadbeef_dublin_key(&self) -> PathBuf {
        self.deadbeef_dir().join("deadbeef-dublin.key.pem")
    }

    #[must_use]
    pub fn deadbeef_athlone_cert(&self) -> PathBuf {
        self.deadbeef_dir().join("deadbeef-athlone.cert.pem")
    }

    #[must_use]
    pub fn deadbeef_athlone_key(&self) -> PathBuf {
        self.deadbeef_dir().join("deadbeef-athlone.key.pem")
    }

    pub fn load_bytes(&self, path: &PathBuf) -> std::io::Result<Vec<u8>> {
        std::fs::read(path)
    }

    #[must_use]
    pub fn path_to_string(path: &PathBuf) -> String {
        path.to_str().expect("Path is not valid UTF-8").to_string()
    }
}
