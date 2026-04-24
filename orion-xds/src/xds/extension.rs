use std::{future::Future, pin::Pin};

use thiserror::Error;

#[derive(Error, Debug)]
pub enum XdsExtensionError {
    #[error("failed to decode extension resource: {0}")]
    DecodeError(String),
    #[error("extension handler error: {0}")]
    HandlerError(String),
}

pub trait XdsExtensionHandler: Send + Sync {
    fn type_urls(&self) -> &[&str];

    fn handle_update(
        &self,
        type_url: &str,
        resource_id: &str,
        payload: &[u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), XdsExtensionError>> + Send + '_>>;

    fn handle_remove(
        &self,
        type_url: &str,
        resource_id: &str,
    ) -> Pin<Box<dyn Future<Output = Result<(), XdsExtensionError>> + Send + '_>>;
}
