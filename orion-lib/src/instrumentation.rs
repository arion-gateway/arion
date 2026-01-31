use contatori::counters::average::Average;
use contatori::counters::monotone::Monotone;
use contatori::counters::Observable;

pub static CONNECTIONS: Monotone = Monotone::new();
pub static CONNECTION_SETUP_TIME: Average = Average::new();
pub static LOAD_BALANCING_SRV: Average = Average::new();
pub static TOTAL_ROUTE_ACTION: Average = Average::new();
pub static REQUEST_TO_RESPONSE_TIME: Average = Average::new();
pub static SEND_REQUEST_WAIT_RESPONSE: Average = Average::new();
pub static SEND_REQUEST: Average = Average::new();
pub static SEND_REQUEST_WITH_RETRY: Average = Average::new();

#[macro_export]
macro_rules! instrument_block {
    ($clock:expr, $callback:expr, $code:block) => {{
        #[cfg(feature = "instrumentation")]
        let start_clock = $clock.raw();

        // Esecuzione del blocco di codice
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
        // Il blocco defer viene eseguito automaticamente alla fine dello scope
        defer! {
            let nanos = $clock.delta_as_nanos(start_clock, $clock.raw());
            ($callback)(nanos);
        }
    };
}

pub fn dump_instrumentation_counters() {
    println!("::: instrumentation counters :::");
    println!("connections:");
    println!("   total connections: {}", CONNECTIONS.value());
    println!("   setup time (ns): {}", CONNECTION_SETUP_TIME.value());
    println!("routing:");
    println!("   load balancing service time (ns): {}", LOAD_BALANCING_SRV.value());
    println!("   total route action (ns): {}", TOTAL_ROUTE_ACTION.value());
    println!("   total request-to-response time (ns): {}", REQUEST_TO_RESPONSE_TIME.value());
    println!("upstream:");
    println!("   send-request-wait-response time (ns): {}", SEND_REQUEST_WAIT_RESPONSE.value());
    println!("   send-request time (ns): {}", SEND_REQUEST.value());
    println!("   send-request with retry time (ns): {}", SEND_REQUEST_WITH_RETRY.value());
}
