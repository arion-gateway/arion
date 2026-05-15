#[cfg(feature = "instrumentation")]
use {contatori::counters::{Observable, average::Average, monotone::Monotone}, tracing::info};

#[cfg(feature = "instrumentation")]
pub mod metrics {
    use super::*;
    pub static CONNECTIONS: Monotone = Monotone::new();
    pub static CONNECTION_SETUP_TIME: Average = Average::new();
    pub static LOAD_BALANCING_SRV: Average = Average::new();
    pub static TOTAL_ROUTE_ACTION: Average = Average::new();
    pub static REQUEST_TO_RESPONSE_TIME: Average = Average::new();
    pub static SEND_REQUEST_WAIT_RESPONSE: Average = Average::new();
    pub static SEND_REQUEST: Average = Average::new();
    pub static SEND_REQUEST_WITH_RETRY: Average = Average::new();
    pub static SEND_RLS_REQUEST: Average = Average::new();
}

#[macro_export]
macro_rules! instrument_block {
    ($clock:expr, $callback:expr, $code:block) => {{
        #[cfg(feature = "instrumentation")]
        let start_clock = $clock.raw();

        let result = $code;

        #[cfg(feature = "instrumentation")]
        {
            let nanos = $clock.delta_as_nanos(start_clock, $clock.raw());
            ($callback)(nanos);
        }

        result
    }};
}

#[macro_export]
macro_rules! instrument_function {
    ($clock:expr, $callback:expr) => {
        #[cfg(feature = "instrumentation")]
        let start_clock = $clock.raw();

        #[cfg(feature = "instrumentation")]
        ::scopeguard::defer! {
            let nanos = $clock.delta_as_nanos(start_clock, $clock.raw());
            ($callback)(nanos);
        }
    };
}

#[cfg(feature = "instrumentation")]
pub fn dump_instrumentation_counters() {
    info!("::: instrumentation counters :::");
    info!("connections:");
    info!("   total connections: {}", metrics::CONNECTIONS.value());
    info!("   setup time (ns): {}", metrics::CONNECTION_SETUP_TIME.value());
    info!("routing:");
    info!("   load balancing service time (ns): {}", metrics::LOAD_BALANCING_SRV.value());
    info!("   total route action (ns): {}", metrics::TOTAL_ROUTE_ACTION.value());
    info!("   total request-to-response time (ns): {}", metrics::REQUEST_TO_RESPONSE_TIME.value());
    info!("upstream:");
    info!("   send-request-wait-response time (ns): {}", metrics::SEND_REQUEST_WAIT_RESPONSE.value());
    info!("   send-request time (ns): {}", metrics::SEND_REQUEST.value());
    info!("   send-request with retry time (ns): {}", metrics::SEND_REQUEST_WITH_RETRY.value());
    info!("rate limiting service:");
    info!("   send-request to rate limiting service (ns): {}", metrics::SEND_RLS_REQUEST.value());
}
