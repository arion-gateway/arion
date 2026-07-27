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
