// Copyright 2026 The arion-gateway Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::body::timeout_body::TimeoutBodyError;
use arion_xds::grpc_deps::Status as GrpcError;
use std::sync::Arc;

#[derive(thiserror::Error, Debug, Clone)]
pub enum BodyError {
    #[error(transparent)]
    Hyper(Arc<hyper::Error>),

    #[error(transparent)]
    ExtProc(#[from] crate::listeners::http_connection_manager::ext_proc::ExtProcError),

    #[error(transparent)]
    Grpc(Arc<GrpcError>),

    #[error(transparent)]
    Io(Arc<std::io::Error>),

    #[error("data was not received within the designated timeout")]
    TimedOut,

    #[error("adding trailers is not supported for body type: {0}")]
    TrailersNotSupported(&'static str),
}

impl From<hyper::Error> for BodyError {
    #[cold]
    #[inline(never)]
    fn from(err: hyper::Error) -> Self {
        Self::Hyper(Arc::new(err))
    }
}

impl From<GrpcError> for BodyError {
    #[cold]
    #[inline(never)]
    fn from(err: GrpcError) -> Self {
        Self::Grpc(Arc::new(err))
    }
}

impl From<std::io::Error> for BodyError {
    #[cold]
    #[inline(never)]
    fn from(err: std::io::Error) -> Self {
        Self::Io(Arc::new(err))
    }
}

impl From<std::convert::Infallible> for BodyError {
    #[inline]
    fn from(inf: std::convert::Infallible) -> Self {
        match inf {}
    }
}

impl From<TimeoutBodyError<hyper::Error>> for BodyError {
    #[cold]
    #[inline(never)]
    fn from(value: TimeoutBodyError<hyper::Error>) -> Self {
        match value {
            TimeoutBodyError::TimedOut => Self::TimedOut,
            TimeoutBodyError::BodyError(e) => Self::Hyper(Arc::new(e)),
        }
    }
}
