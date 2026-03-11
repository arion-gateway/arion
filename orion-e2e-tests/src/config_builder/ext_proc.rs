use std::time::Duration;

use orion_data_plane_api::envoy_data_plane_api::{
    envoy::{
        config::core::v3::{
            grpc_service::{EnvoyGrpc, TargetSpecifier},
            GrpcService,
        },
        extensions::filters::http::ext_proc::v3::{
            processing_mode::{BodySendMode, HeaderSendMode},
            ExternalProcessor as EnvoyExternalProcessor, ProcessingMode,
        },
    },
    google::protobuf::Duration as ProtoDuration,
};

#[derive(Debug, Clone)]
pub struct ExtProcBuilder {
    proto: EnvoyExternalProcessor,
}

impl ExtProcBuilder {
    #[must_use]
    pub fn new(cluster_name: impl Into<String>) -> Self {
        Self {
            proto: EnvoyExternalProcessor {
                grpc_service: Some(GrpcService {
                    target_specifier: Some(TargetSpecifier::EnvoyGrpc(EnvoyGrpc {
                        cluster_name: cluster_name.into(),
                        ..Default::default()
                    })),
                    timeout: Some(ProtoDuration { seconds: 5, nanos: 0 }),
                    ..Default::default()
                }),
                ..Default::default()
            },
        }
    }

    #[must_use]
    pub fn request_header_mode(mut self, mode: HeaderSendMode) -> Self {
        self.ensure_processing_mode().request_header_mode = mode.into();
        self
    }

    #[must_use]
    pub fn response_header_mode(mut self, mode: HeaderSendMode) -> Self {
        self.ensure_processing_mode().response_header_mode = mode.into();
        self
    }

    #[must_use]
    pub fn request_body_mode(mut self, mode: BodySendMode) -> Self {
        self.ensure_processing_mode().request_body_mode = mode.into();
        self
    }

    #[must_use]
    pub fn response_body_mode(mut self, mode: BodySendMode) -> Self {
        self.ensure_processing_mode().response_body_mode = mode.into();
        self
    }

    #[must_use]
    pub fn request_only(self) -> Self {
        self.request_header_mode(HeaderSendMode::Send)
            .response_header_mode(HeaderSendMode::Skip)
            .request_body_mode(BodySendMode::None)
            .response_body_mode(BodySendMode::None)
    }

    #[must_use]
    pub fn response_only(self) -> Self {
        self.request_header_mode(HeaderSendMode::Skip)
            .response_header_mode(HeaderSendMode::Send)
            .request_body_mode(BodySendMode::None)
            .response_body_mode(BodySendMode::None)
    }

    #[must_use]
    pub fn headers_only(self) -> Self {
        self.request_header_mode(HeaderSendMode::Send)
            .response_header_mode(HeaderSendMode::Send)
            .request_body_mode(BodySendMode::None)
            .response_body_mode(BodySendMode::None)
    }

    #[must_use]
    pub fn headers_and_body(self) -> Self {
        self.request_header_mode(HeaderSendMode::Send)
            .response_header_mode(HeaderSendMode::Send)
            .request_body_mode(BodySendMode::Buffered)
            .response_body_mode(BodySendMode::Buffered)
    }

    #[must_use]
    pub fn failure_mode_allow(mut self, allow: bool) -> Self {
        self.proto.failure_mode_allow = allow;
        self
    }

    #[must_use]
    pub fn message_timeout(mut self, timeout: Duration) -> Self {
        self.proto.message_timeout =
            Some(ProtoDuration { seconds: timeout.as_secs() as i64, nanos: timeout.subsec_nanos() as i32 });
        self
    }

    #[must_use]
    pub fn observability_mode(mut self, enabled: bool) -> Self {
        self.proto.observability_mode = enabled;
        self
    }

    #[must_use]
    pub fn allow_mode_override(mut self, allow: bool) -> Self {
        self.proto.allow_mode_override = allow;
        self
    }

    #[must_use]
    pub fn build(self) -> EnvoyExternalProcessor {
        self.proto
    }

    fn ensure_processing_mode(&mut self) -> &mut ProcessingMode {
        self.proto.processing_mode.get_or_insert_with(ProcessingMode::default)
    }
}

impl From<ExtProcBuilder> for EnvoyExternalProcessor {
    fn from(builder: ExtProcBuilder) -> Self {
        builder.build()
    }
}
