use crate::admin::AdminState;
use axum::{extract::State, Json};
use serde_json::{json, Value};

pub async fn get_ready(State(mut admin_state): State<AdminState>) -> Json<Value> {
    admin_state.server_info.uptime_all_epochs = Some(admin_state.server_startup.elapsed());
    Json(json!(admin_state.server_info))
}
