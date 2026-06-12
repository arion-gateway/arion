use crate::admin::AdminState;
use axum::extract::State;

pub async fn clusters_handlers(State(mut _admin_state): State<AdminState>) -> String {
    "hello clusters".into()
}
