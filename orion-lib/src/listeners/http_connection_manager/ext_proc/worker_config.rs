use std::{sync::atomic::Ordering, time::Duration};

use atomic_enum::atomic_enum;
use orion_configuration::config::network_filters::http_connection_manager::http_filters::ext_proc::{
    GrpcServiceSpecifier, HeaderMutationRules, ProcessingMode, RouteCacheAction,
};


#[atomic_enum]
pub enum SendingFlag {
    Empty = 0,
    True,
    False
}

pub struct OverrideSendingFlags {
    pub headers: AtomicSendingFlag,
    pub body: AtomicSendingFlag,
    pub trailers: AtomicSendingFlag,
}

impl Default for OverrideSendingFlags {
    fn default() -> Self {
        Self {
            headers: AtomicSendingFlag::new(SendingFlag::Empty),
            body: AtomicSendingFlag::new(SendingFlag::Empty),
            trailers: AtomicSendingFlag::new(SendingFlag::Empty),
        }
    }
}

impl std::fmt::Debug for OverrideSendingFlags {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OverrideSendingFlags")
            .field(
                "headers",
                &self.headers.load(Ordering::Relaxed),
            )
            .field("body", &self.body.load(Ordering::Relaxed))
            .field(
                "trailers",
                &self.trailers.load(Ordering::Relaxed),
            )
            .finish()
    }
}

#[derive(Debug)]
#[allow(clippy::struct_excessive_bools)]
pub struct ExternalProcessingWorkerConfig {
    pub grpc_service_specifier: GrpcServiceSpecifier,
    pub message_timeout: Duration,
    pub max_message_timeout: Option<Duration>,
    pub observability_mode: bool,
    pub failure_mode_allow: bool,
    pub disable_immediate_response: bool,
    pub mutation_rules: HeaderMutationRules,
    pub processing_mode: ProcessingMode,
    pub allowed_override_modes: Vec<ProcessingMode>,
    pub allow_mode_override: bool,
    pub route_cache_action: RouteCacheAction,
    pub send_body_without_waiting_for_header_response: bool,
    pub override_sending_request: OverrideSendingFlags,
    pub override_sending_response: OverrideSendingFlags,
}
