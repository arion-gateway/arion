use crate::admin::AdminState;
use axum::extract::State;

pub async fn post_reset_counters(State(mut _admin_state): State<AdminState>) -> String {
    "hello world".into()
}
