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

use super::{AccessLogMessage, Target};
use flume::Receiver;
use std::collections::HashMap;
use tracing::{error, info};
use tracing_rolling_file::RollingFrequency;

use super::log_writer::LogWriter;

pub(crate) struct AccessLogger {
    id: usize,
    max_log_files: usize,
    max_file_size: Option<u64>,
    frequency: Option<RollingFrequency>,
    map: HashMap<Target, Vec<LogWriter>>,
}

const RECEIVER_BATCH_CAPACITY: usize = 64;

impl AccessLogger {
    pub(crate) fn new(
        id: usize,
        frequency: Option<RollingFrequency>,
        max_file_size: Option<u64>,
        max_log_files: usize,
    ) -> Self {
        AccessLogger { id, max_log_files, frequency, max_file_size, map: HashMap::new() }
    }

    pub(crate) async fn run(&mut self, receiver: Receiver<AccessLogMessage>) {
        let mut buffer: Vec<AccessLogMessage> = Vec::with_capacity(RECEIVER_BATCH_CAPACITY);
        loop {
            match receiver.recv_async().await {
                Ok(msg) => {
                    buffer.push(msg);
                    while buffer.len() < RECEIVER_BATCH_CAPACITY {
                        match receiver.try_recv() {
                            Ok(msg) => buffer.push(msg),
                            Err(_) => break,
                        }
                    }
                },
                Err(_) => {
                    error!("AccessLogger: channel Receiver closed");
                    return;
                },
            }

            for msg in buffer.drain(..) {
                match msg {
                    AccessLogMessage::Configure(target, new_conf) => {
                        let loggers = self.map.get_mut(&target);
                        let update_required = match loggers {
                            None => true,
                            Some(v) => {
                                v.len() != new_conf.len() || v.iter().zip(new_conf.iter()).any(|(l, n)| l.sink() != n)
                            },
                        };
                        if update_required {
                            info!("AccessLogger: updating configuration for target {target}");
                            let mut handlers = vec![];
                            for conf in new_conf {
                                let handler = LogWriter::new(
                                    self.id,
                                    conf,
                                    self.frequency,
                                    self.max_file_size,
                                    self.max_log_files,
                                );
                                handlers.push(handler)
                            }

                            self.map.insert(target.clone(), handlers);
                        }
                    },
                    AccessLogMessage::Message(target, fmt) => {
                        if let Some(loggers) = self.map.get_mut(&target) {
                            for (log, fmt) in loggers.iter_mut().zip(fmt.iter()) {
                                if let Some(logger) = log.get_mut() {
                                    if let Err(e) = fmt.write_to(logger) {
                                        error!("AccessLogger: Failed to write log for target '{target}'. Reason: {e}");
                                    }
                                }
                            }
                        } else {
                            error!("AccessLogger: no loggers for target '{target}'. Message: {fmt:?}");
                        }
                    },
                }
            }
        }
    }
}
