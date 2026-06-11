use crate::admin::AdminState;
use axum::extract::State;

pub async fn get_server_info(State(mut _admin_state): State<AdminState>) -> String {
    "hello world".into()
}
