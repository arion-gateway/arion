use axum::extract::State;
use crate::admin::AdminState;

#[cfg(feature = "config-dump")]
const CONFIG_DUMP_HELP: &str = "  /config_dump: dump current Orion configs\n";
#[cfg(not(feature = "config-dump"))]
const CONFIG_DUMP_HELP: &str = "";

#[cfg(feature = "metrics")]
const METRICS_HELP: &str = "  /stats: print server stats\n";
#[cfg(not(feature = "metrics"))]
const METRICS_HELP: &str = "";

#[cfg(feature = "prometheus")]
const PROMETHEUS_HELP: &str = "  /stats/prometheus: print server stats in prometheus format\n";
#[cfg(not(feature = "prometheus"))]
const PROMETHEUS_HELP: &str = "";

pub async fn get_help(State(mut _admin_state): State<AdminState>) -> String {
   const_format::concatcp!(
       "admin commands are:\n",
       "  /: admin home page\n",
       CONFIG_DUMP_HELP,
       "  /help: print out list of admin commands\n",
       "  /ready: print server state, return 200 if LIVE, otherwise return 503\n",
       METRICS_HELP,
       PROMETHEUS_HELP,
   ).into()
}
