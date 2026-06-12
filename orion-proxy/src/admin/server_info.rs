use crate::admin::AdminState;
use axum::extract::State;

pub async fn server_info_handler(State(mut _admin_state): State<AdminState>) -> String {
    "hello server_info".into()
}
