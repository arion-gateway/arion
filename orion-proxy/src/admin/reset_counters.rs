use crate::admin::AdminState;
use axum::extract::State;
use orion_metrics::metrics::reset_global_metrics;

pub async fn post_reset_counters(State(mut _admin_state): State<AdminState>) -> String {
    reset_global_metrics();
    "OK".into()
}
