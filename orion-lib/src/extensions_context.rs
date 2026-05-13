use std::sync::Arc;

use crate::{
    body::response_flags::ResponseFlags, event_error::EventKind, listeners::metadata::DownstreamMetadata,
    utils::instrumented_stream::StreamMetrics,
};

#[derive(Debug, Clone)]
pub struct MetadataContext {
    pub downstream: DownstreamMetadata,
    pub stream_metrics: Arc<StreamMetrics>,
}

#[derive(Debug, Clone)]
pub struct EventContext {
    pub response_flags: ResponseFlags,
    pub event_kind: Option<EventKind>,
}
