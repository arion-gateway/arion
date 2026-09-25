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

use crate::admin::AdminState;
use axum::{extract::State, Json};
use serde_json::{json, Value};

pub async fn memory_handler(State(mut _admin_state): State<AdminState>) -> Json<Value> {
    let memory_physical = orion_stats::get_memory_physical_size().unwrap_or_default() as u64;
    let memory_heap_size = orion_stats::get_memory_heap_size().map_or(memory_physical, |v| v as u64);
    let memory_allocated = orion_stats::get_memory_allocated().map_or(memory_physical, |v| v as u64);
    Json(json!({
        "physical": memory_physical,
        "heap_size": memory_heap_size,
        "allocated": memory_allocated,
    }))
}
