use axum::{Json, extract::State};
use serde_json::{Value, json};
use crate::admin::AdminState;


pub async fn get_ready(State(mut admin_state): State<AdminState>) -> Json<Value> {
    admin_state.server_info.uptime_all_epochs = Some(admin_state.server_startup.elapsed());
    Json(json!(admin_state.server_info))
}
