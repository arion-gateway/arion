use crate::admin::AdminState;
use axum::extract::State;

pub async fn get_certs(State(mut _admin_state): State<AdminState>) -> String {
    "hello world".into()
}
