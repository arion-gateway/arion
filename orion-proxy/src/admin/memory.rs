use std::collections::HashMap;

use crate::admin::AdminState;
use axum::{extract::State, Json};
use orion_metrics::metrics::server::{self, update_server_metrics};
use serde_json::{json, Value};
use smallvec::smallvec;

pub async fn get_memory(State(mut _admin_state): State<AdminState>) -> Json<Value> {
    update_server_metrics(&HashMap::new());
    let memory_physical = server::MEMORY_PHYSICAL_SIZE
        .get()
        .and_then(|mem| mem.value.load_all().get(&smallvec![]).map(ToOwned::to_owned))
        .unwrap_or_default();
    let memory_heap_size = server::MEMORY_HEAP_SIZE
        .get()
        .and_then(|mem| mem.value.load_all().get(&smallvec![]).map(ToOwned::to_owned))
        .unwrap_or_default();
    let memory_allocated = server::MEMORY_ALLOCATED
        .get()
        .and_then(|mem| mem.value.load_all().get(&smallvec![]).map(ToOwned::to_owned))
        .unwrap_or_default();
    Json(json!({
        "physical": memory_physical,
        "heap_size": memory_heap_size,
        "allocated": memory_allocated,
    }))
}
