use crate::admin::AdminState;
use axum::extract::State;
use http::StatusCode;
use orion_stats::{get_proxy_state, ProxyState};

pub async fn get_ready(State(mut _admin_state): State<AdminState>) -> Result<String, StatusCode> {
    match get_proxy_state() {
        Some(ProxyState::Live) => Ok("LIVE".into()),
        _ => Err(StatusCode::SERVICE_UNAVAILABLE),
    }
}
