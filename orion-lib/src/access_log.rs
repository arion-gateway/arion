// Copyright 2025 The kmesh Authors
//
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
//
//

#![allow(unused_macros)]

mod deferred_init;
mod log_writer;
pub mod logger;
mod pool;

use logger::AccessLogger;
use orion_configuration::config::access_log::{AccessLogConf, AccessLogTarget};
use orion_format::FormattedMessage;
use pool::LoggerPool;
use smol_str::SmolStr;
use std::sync::OnceLock;
use tokio::sync::mpsc::error::TrySendError;
use tracing_rolling_file::RollingFrequency;

use std::{fmt::Display, hash::Hash};
use tokio::{sync::mpsc::Sender, task::JoinSet};
use tracing::{error, info};

#[macro_export]
macro_rules! with_access_log {
    ($fmt:expr, $ctx:expr) => {{
        let fmt_val = $fmt;
        if !fmt_val.is_empty() {
            let ctx_val = $ctx;
            for f in fmt_val.iter_mut() {
                f.with_context(&ctx_val);
            }
        }
    }};
}

/// Destination for an access logging event.
///
/// Identifies which logical entity produced a log entry so that loggers can
/// apply the correct per-target configuration.
///
/// - `Listener`: a top-level listener identified by name.
/// - `ListenerFilterChain`: a specific filter-chain within a listener, identified
///   by the listener name and a hash of the filter-chain.
/// - `Admin`: the admin interface.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Target {
    Listener(SmolStr),
    ListenerFilterChain(SmolStr, u64),
    Admin,
}

impl From<AccessLogTarget> for Target {
    fn from(value: AccessLogTarget) -> Self {
        match value {
            AccessLogTarget::Listener(name) => Target::Listener(name),
            AccessLogTarget::ListenerFilterChain(name, hash) => Target::ListenerFilterChain(name, hash),
            AccessLogTarget::Admin => Target::Admin,
        }
    }
}

impl Display for Target {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Target::Listener(name) => write!(f, "Listener({name})"),
            Target::ListenerFilterChain(lister_name, filter_chain_name) => {
                write!(f, "Listener({lister_name}):FilterChain({filter_chain_name})")
            },
            Target::Admin => write!(f, "Admin"),
        }
    }
}

/// Messages exchanged with the background access-logger tasks.
///
/// - `Configure`: replaces the logger configuration for a given [`Target`].
///   Sent once per target during initialiation or on xDS updates.
/// - `Message`: delivers one or more pre-formatted log entries to be written
///   to all sinks configured for the given [`Target`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AccessLogMessage {
    Configure(Target, Vec<AccessLogConf>),
    Message(Target, Vec<FormattedMessage>),
}

#[derive(Debug, thiserror::Error)]
pub enum LoggerError {
    #[error("Failed to initialize logger: {0}")]
    InitializationError(String),
    #[error("Channel closed unexpectedly")]
    SenderError,
}

static SENDER_POOL: OnceLock<LoggerPool<AccessLogMessage>> = OnceLock::new();

/// Sends formatted log entries to the logger for the given target.
///
/// Behaviour depends on the `blocking` flag set during [`start_access_loggers`]:
/// - **blocking**: awaits [`Sender::send`]; the caller is suspended until the
///   channel has capacity.
/// - **non-blocking**: uses [`Sender::try_send`] and logs an error if the
///   buffer is full.
///
/// Logs an error and returns without panicking if no sender is available.
#[allow(clippy::needless_pass_by_value)]
#[inline]
pub async fn log_access(target: Target, vec: Vec<FormattedMessage>) {
    if vec.is_empty() {
        return;
    }
    if let Some(sender) = get_sender() {
        if is_blocking() {
            if let Err(e) = sender.send(AccessLogMessage::Message(target, vec)).await {
                error!("Failed to send access log message: {e}");
            }
        } else if let Err(e) = sender.try_send(AccessLogMessage::Message(target, vec)) {
            error!("Failed to send access log message: {e}");
        }
    } else {
        error!("Failed to send access log message: no available sender.");
    }
}

/// Attempts a non-blocking send of formatted log entries.
///
/// Returns `Ok(())` if the message was enqueued, or a [`TrySendError`]
/// carrying the original `Vec<FormattedMessage>` back to the caller if the
/// channel is full ([`TrySendError::Full`]) or closed
/// ([`TrySendError::Closed`]).
///
/// Does not await or block; use [`log_access`] when back-pressure is acceptable.
#[allow(clippy::needless_pass_by_value)]
pub fn try_log_access(target: Target, vec: Vec<FormattedMessage>) -> Result<(), TrySendError<Vec<FormattedMessage>>> {
    if vec.is_empty() {
        return Ok(());
    }
    if let Some(sender) = get_sender() {
        sender.try_send(AccessLogMessage::Message(target, vec)).map_err(|e| match e {
            TrySendError::Full(AccessLogMessage::Message(_, msg)) => TrySendError::Full(msg),
            TrySendError::Closed(AccessLogMessage::Message(_, msg)) => TrySendError::Closed(msg),
            _ => unreachable!(),
        })
    } else {
        Err(TrySendError::Closed(vec))
    }
}

/// Sends formatted log entries using a pre-reserved permit, with a fallback path.
///
/// First attempts to consume the permit from `permit` via an atomic take
/// (zero-cost if it was already reserved). If the permit has already been
/// taken (or was never set), falls back to [`try_log_access`]. When the
/// `blocking` flag is set and the channel is full, [`tokio::task::block_in_place`]
/// is used to drive a synchronous send without spawning a new task.
///
/// This function is sync and safe to call from non-async contexts (e.g. body
/// completion callbacks registered on [`InstrumentedBody`]).
#[allow(clippy::needless_pass_by_value)]
#[inline]
pub fn log_access_blocking(target: Target, vec: Vec<FormattedMessage>) {
    let target_clone = target.clone();
    if let Err(err) = try_log_access(target, vec) { match err {
        TrySendError::Full(vec) => {
            if is_blocking() {
                tokio::task::block_in_place(move || {
                    if let Some(sender) = get_sender() {
                        let _ = sender.blocking_send(AccessLogMessage::Message(target_clone, vec));
                    }
                });
            }
        },
        TrySendError::Closed(_) => {
            error!("Failed to send access log message: no available sender (channel closed)");
        },
    } }
}

/// Initializes the global sender pool and spawns background logger tasks.
///
/// Creates `num_instances` independent [`AccessLogger`] tasks, each backed by
/// its own bounded MPSC channel of capacity `buffer`. The senders are stored
/// in the process-global [`SENDER_POOL`] (`OnceLock`); calling this function
/// more than once has no effect and logs an error.
///
/// Each logger runs concurrently inside the provided Tokio runtime and
/// processes [`AccessLogMessage`]s asynchronously.
///
/// # Arguments
///
/// * `num_instances` - Number of independent logger tasks to spawn. Using more
///   than one reduces contention on the send side at the cost of out-of-order
///   log entries across instances.
/// * `buffer` - Bounded channel capacity per logger instance.
/// * `frequency` - Optional log-file rolling frequency (from `tracing_rolling_file`).
/// * `max_file_size` - Optional maximum size in bytes for each rolling log file.
/// * `max_log_files` - Maximum number of retained rolling files per target.
/// * `blocking` - When `true`, senders will await channel capacity instead of
///   dropping messages when the buffer is full.
///
/// # Returns
///
/// A [`JoinSet<()>`] containing all spawned logger tasks. Dropping it cancels
/// the loggers; awaiting [`JoinSet::join_all`] waits for them to finish.
#[allow(clippy::needless_pass_by_value)]
pub fn start_access_loggers(
    num_instances: usize,
    buffer: usize,
    frequency: Option<RollingFrequency>,
    max_file_size: Option<u64>,
    max_log_files: usize,
    blocking: bool,
) -> JoinSet<()> {
    let (mut senders, mut receivers) = (Vec::with_capacity(num_instances), Vec::with_capacity(num_instances));
    for _ in 0..num_instances {
        let (sender, receiver) = tokio::sync::mpsc::channel(buffer);
        senders.push(sender);
        receivers.push(receiver);
    }

    info!("Initializing access loggers...");

    if SENDER_POOL.set(LoggerPool { senders, blocking }).is_err() {
        error!("Unable to initialize logger pool!");
        return JoinSet::new(); // Return an empty JoinSet on error
    }

    let mut join_set = JoinSet::new();
    for (i, recv) in receivers.into_iter().enumerate() {
        let frequency = frequency;
        let max_size = max_file_size;
        join_set.spawn(async move {
            let mut logger = AccessLogger::new(i, frequency, max_size, max_log_files);
            logger.run(recv).await
        });
    }
    join_set
}

/// Returns `true` if the global sender pool has been initialized with at least one sender.
#[inline]
pub fn is_access_log_enabled() -> bool {
    SENDER_POOL.get().is_some_and(|pool| !pool.senders.is_empty())
}

/// Returns a reference to a sender selected from the pool by thread-id hash, or `None` if the pool is empty.
#[inline]
fn get_sender() -> Option<&'static Sender<AccessLogMessage>> {
    SENDER_POOL.get().and_then(|pool| pool.get())
}

/// Returns a reference to the sender at `index`, or `None` if out of bounds.
#[inline]
#[allow(unused)]
fn get_sender_at(index: usize) -> Option<&'static Sender<AccessLogMessage>> {
    SENDER_POOL.get().and_then(|pool| pool.get_at(index))
}

/// Returns `true` if the logger pool was started in blocking mode.
///
/// In blocking mode, [`log_access`] awaits channel capacity rather than
/// dropping messages when the buffer is full.
#[inline]
fn is_blocking() -> bool {
    SENDER_POOL.get().map(|pool| pool.blocking).unwrap_or(false)
}

/// Broadcasts a configuration update for `target` to every logger instance.
///
/// Sends an [`AccessLogMessage::Configure`] to all senders in the pool so
/// that every logger applies the new [`AccessLogConf`] list. Returns
/// `Ok(())` if all sends succeed, or [`LoggerError::SenderError`] on the
/// first failure.
pub async fn update_configuration(target: Target, init: Vec<AccessLogConf>) -> Result<(), LoggerError> {
    let pool =
        SENDER_POOL.get().ok_or_else(|| LoggerError::InitializationError("Logger pool not initialized".into()))?;
    for (i, senders) in pool.senders.iter().enumerate() {
        if let Err(e) = senders.send(AccessLogMessage::Configure(target.clone(), init.clone())).await {
            error!("Failed to send logger configuration to sender {i}: {e}");
            return Err(LoggerError::SenderError);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::access_log::start_access_loggers;

    use super::*;
    use orion_format::{
        context::{DownstreamContext, DownstreamResponseContext, FinishContext, InitContext, UpstreamContext},
        types::ResponseFlags,
        LogFormatter, DEFAULT_ACCESS_LOG_FORMAT,
    };
    use tokio::{self, time::timeout};

    fn build_request() -> http::Request<()> {
        http::Request::builder().uri("https://www.rust-lang.org/").header("User-Agent", "awesome/1.0").body(()).unwrap()
    }

    fn build_response() -> http::Response<()> {
        let builder = http::Response::builder().status(http::StatusCode::OK);
        builder.body(()).unwrap()
    }

    #[allow(clippy::disallowed_methods)]
    #[tokio::test]
    async fn test_access_loggers() {
        let req = build_request();
        let resp = build_response();

        let formatter = LogFormatter::try_new(DEFAULT_ACCESS_LOG_FORMAT, false).unwrap();
        let mut fmt = formatter.clone();

        fmt.with_context(&InitContext { start_time: std::time::SystemTime::now() });
        fmt.with_context(&DownstreamContext {
            request: &req,
            trace_id: None,
            request_head_size: 0,
            server_name: None,
            socket_address: Default::default(),
        });
        fmt.with_context(&UpstreamContext {
            authority: Some(req.uri().authority().unwrap()),
            cluster_name: Some("test_cluster"),
            route_name: "test_route",
        });
        fmt.with_context(&DownstreamResponseContext { response: &resp, response_head_size: 0 });
        fmt.with_context(&FinishContext {
            duration: Duration::from_millis(100),
            bytes_received: 128,
            bytes_sent: 256,
            response_flags: ResponseFlags::NO_HEALTHY_UPSTREAM,
            upstream_transport_failure_reason: None,
            response_code_details: None,
            connection_termination_details: None,
        });

        let message = fmt.into_message();

        // initialize the logger pool with one channel for access log messages
        let handles = start_access_loggers(1, 100, None, None, 3, true);

        // send a new configuration for the logger(s)
        update_configuration(
            Target::Listener("test".into()),
            vec![AccessLogConf::File("test-access.log".into()), AccessLogConf::Stderr],
        )
        .await
        .unwrap();

        // log the formatted message to file and stdout...
        log_access(Target::Listener("test".into()), vec![message.clone(), message.clone()]).await;

        // test blocking access as well
        log_access_blocking(Target::Listener("test".into()), vec![message.clone(), message.clone()]);

        _ = timeout(Duration::from_secs(2), handles.join_all()).await;
        std::fs::remove_file("test-access.log").unwrap();
    }
}
