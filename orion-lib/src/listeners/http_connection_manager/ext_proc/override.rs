use crate::listeners::http_connection_manager::ext_proc::{worker_config::ExternalProcessingWorkerConfig};
use crate::listeners::http_connection_manager::ext_proc::kind;

use atomic_enum::atomic_enum;
use orion_configuration::config::network_filters::http_connection_manager::http_filters::ext_proc::{
    BodyProcessingMode, HeaderProcessingMode, TrailerProcessingMode,
};
use std::sync::atomic::Ordering;

#[atomic_enum]
#[derive(Default)]
pub enum OverridableHeaderMode {
    #[default]
    Default,
    Send,
    Skip,
}

#[atomic_enum]
pub enum OverridableBodyMode {
    None,
    Streamed,
    Buffered,
    BufferedPartial,
    FullDuplexStreamed,
}

#[atomic_enum]
#[derive(Default)]
pub enum OverridableTrailerMode {
    #[default]
    Default,
    Send,
    Skip,
}

impl From<HeaderProcessingMode> for OverridableHeaderMode {
    fn from(mode: HeaderProcessingMode) -> Self {
        match mode {
            HeaderProcessingMode::Skip => OverridableHeaderMode::Skip,
            HeaderProcessingMode::Send => OverridableHeaderMode::Send,
            HeaderProcessingMode::Default => OverridableHeaderMode::Default,
        }
    }
}

impl From<BodyProcessingMode> for OverridableBodyMode {
    fn from(mode: BodyProcessingMode) -> Self {
        match mode {
            BodyProcessingMode::None => OverridableBodyMode::None,
            BodyProcessingMode::Streamed => OverridableBodyMode::Streamed,
            BodyProcessingMode::Buffered => OverridableBodyMode::Buffered,
            BodyProcessingMode::BufferedPartial => OverridableBodyMode::BufferedPartial,
            BodyProcessingMode::FullDuplexStreamed => OverridableBodyMode::FullDuplexStreamed,
        }
    }
}

impl From<TrailerProcessingMode> for OverridableTrailerMode {
    fn from(mode: TrailerProcessingMode) -> Self {
        match mode {
            TrailerProcessingMode::Skip => OverridableTrailerMode::Skip,
            TrailerProcessingMode::Send => OverridableTrailerMode::Send,
            TrailerProcessingMode::Default => OverridableTrailerMode::Default,
        }
    }
}

// The following code is inspired by Haskell DataKind/TypeFamilies and TypeApplications
//

pub struct OverridableModes<K: kind::Message> {
    headers_mode: AtomicOverridableHeaderMode,
    body_mode: AtomicOverridableBodyMode,
    trailers_mode: AtomicOverridableTrailerMode,
    _kind: std::marker::PhantomData<K>,
}

impl<K: kind::Message> OverridableModes<K> {
    #[inline]
    pub fn headers_mode(&self) -> OverridableHeaderMode {
        self.headers_mode.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn body_mode(&self) -> OverridableBodyMode {
        self.body_mode.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn trailers_mode(&self) -> OverridableTrailerMode {
        self.trailers_mode.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn set_headers_mode(&self, mode: HeaderProcessingMode) {
        self.headers_mode.store(mode.into(), Ordering::Relaxed);
    }

    #[inline]
    pub fn set_body_mode(&self, mode: BodyProcessingMode) {
        self.body_mode.store(mode.into(), Ordering::Relaxed);
    }

    #[inline]
    pub fn set_trailers_mode(&self, mode: TrailerProcessingMode) {
        self.trailers_mode.store(mode.into(), Ordering::Relaxed);
    }

    pub fn spawn(&self) -> Self {
        Self {
            headers_mode: AtomicOverridableHeaderMode::new(self.headers_mode.load(Ordering::Relaxed)),
            body_mode: AtomicOverridableBodyMode::new(self.body_mode.load(Ordering::Relaxed)),
            trailers_mode: AtomicOverridableTrailerMode::new(self.trailers_mode.load(Ordering::Relaxed)),
            _kind: std::marker::PhantomData,
        }
    }

    pub fn should_process_headers(&self) -> bool {
        match self.headers_mode.load(Ordering::Relaxed) {
            OverridableHeaderMode::Default => true,
            OverridableHeaderMode::Send => true,
            OverridableHeaderMode::Skip => false,
        }
    }

    pub fn should_process_body(&self) -> bool {
        match self.body_mode.load(Ordering::Relaxed) {
            OverridableBodyMode::None => false,
            _ => true,
        }
    }

    pub fn should_process_trailers(&self) -> bool {
        match self.trailers_mode.load(Ordering::Relaxed) {
            OverridableTrailerMode::Default => false,
            OverridableTrailerMode::Send => true,
            OverridableTrailerMode::Skip => false,
        }
    }
}

impl<K: kind::Message> Default for OverridableModes<K> {
    fn default() -> Self {
        Self {
            headers_mode: AtomicOverridableHeaderMode::new(OverridableHeaderMode::Default),
            body_mode: AtomicOverridableBodyMode::new(OverridableBodyMode::None),
            trailers_mode: AtomicOverridableTrailerMode::new(OverridableTrailerMode::Default),
            _kind: std::marker::PhantomData,
        }
    }
}

impl<K: kind::Message> std::fmt::Debug for OverridableModes<K> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(format!("OverridableModes<{}>", std::any::type_name::<K>()).as_str())
            .field("headers_mode", &self.headers_mode)
            .field("body_mode", &self.body_mode)
            .field("trailers_mode", &self.trailers_mode)
            .finish()
    }
}

#[derive(Debug, Default)]
pub struct OverridableGlobalModes {
    pub request: OverridableModes<kind::Request>,
    pub response: OverridableModes<kind::Response>,
}

impl OverridableGlobalModes {
    #[inline]
    pub fn should_process_headers<K: OverridableModeSelector>(&self) -> bool {
        K::get(self).should_process_headers()
    }

    #[inline]
    pub fn should_process_body<K: OverridableModeSelector>(&self) -> bool {
        K::get(self).should_process_body()
    }

    #[inline]
    pub fn should_process_trailers<K: OverridableModeSelector>(&self) -> bool {
        K::get(self).should_process_trailers()
    }

    #[inline]
    pub fn set_headers_mode<K: OverridableModeSelector>(&self, mode: HeaderProcessingMode) {
        K::get(self).set_headers_mode(mode);
    }

    #[inline]
    pub fn set_body_mode<K: OverridableModeSelector>(&self, mode: BodyProcessingMode) {
        K::get(self).set_body_mode(mode);
    }

    #[inline]
    pub fn set_trailers_mode<K: OverridableModeSelector>(&self, mode: TrailerProcessingMode) {
        K::get(self).set_trailers_mode(mode);
    }

    #[inline]
    pub fn headers_mode<K: OverridableModeSelector>(&self) -> OverridableHeaderMode {
        K::get(self).headers_mode()
    }

    #[inline]
    pub fn body_mode<K: OverridableModeSelector>(&self) -> OverridableBodyMode {
        K::get(self).body_mode()
    }

    #[inline]
    pub fn trailers_mode<K: OverridableModeSelector>(&self) -> OverridableTrailerMode {
        K::get(self).trailers_mode()
    }

    pub fn spawn(&self) -> Self {
        Self { request: self.request.spawn(), response: self.response.spawn() }
    }
}

pub trait OverridableModeSelector: Sized + kind::Message {
    fn get<'a>(global_mode: &'a OverridableGlobalModes) -> &'a OverridableModes<Self>;
}

impl OverridableModeSelector for kind::Request {
    fn get(global_mode: &OverridableGlobalModes) -> &OverridableModes<Self> {
        &global_mode.request
    }
}

impl OverridableModeSelector for kind::Response {
    fn get(global_mode: &OverridableGlobalModes) -> &OverridableModes<Self> {
        &global_mode.response
    }
}

impl From<&ExternalProcessingWorkerConfig> for OverridableGlobalModes {
    fn from(config: &ExternalProcessingWorkerConfig) -> Self {
        Self {
            request: OverridableModes {
                headers_mode: AtomicOverridableHeaderMode::new(config.processing_mode.request_header_mode.into()),
                body_mode: AtomicOverridableBodyMode::new(config.processing_mode.request_body_mode.into()),
                trailers_mode: AtomicOverridableTrailerMode::new(config.processing_mode.request_trailer_mode.into()),
                _kind: std::marker::PhantomData,
            },
            response: OverridableModes {
                headers_mode: AtomicOverridableHeaderMode::new(config.processing_mode.response_header_mode.into()),
                body_mode: AtomicOverridableBodyMode::new(config.processing_mode.response_body_mode.into()),
                trailers_mode: AtomicOverridableTrailerMode::new(config.processing_mode.response_trailer_mode.into()),
                _kind: std::marker::PhantomData,
            },
        }
    }
}
