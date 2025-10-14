use std::time::Duration;

use orion_configuration::config::network_filters::http_connection_manager::http_filters::ext_proc::{
    GrpcServiceSpecifier, HeaderMutationRules, ProcessingMode, RouteCacheAction,
};

#[derive(Debug, Clone)]
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
}
