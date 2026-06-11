use axum::{extract::State};
use crate::admin::AdminState;

pub async fn get_home(State(mut _admin_state): State<AdminState>) -> String {
    "hello world".into()
}
