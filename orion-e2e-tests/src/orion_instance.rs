// Copyright 2025 The kmesh Authors
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

use pingora::prelude::fast_timeout::fast_timeout;
use std::io::{BufRead, BufReader};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::oneshot;
use tracing::{debug, error, info, warn};

use crate::{Error, Result};

const DEFAULT_READY_TIMEOUT: Duration = Duration::from_secs(10);
const READY_POLL_INTERVAL: Duration = Duration::from_millis(100);

pub struct OrionInstance {
    process: Option<Child>,
    config_path: PathBuf,
    cleanup_config: bool,
    listener_addr: Option<SocketAddr>,
    admin_addr: Option<SocketAddr>,
    shutdown_requested: Arc<AtomicBool>,
    _output_reader: Option<std::thread::JoinHandle<()>>,
}

impl OrionInstance {
    pub async fn spawn(config_path: impl AsRef<Path>, listener_addr: SocketAddr) -> Result<Self> {
        Self::spawn_with_options(config_path, listener_addr, SpawnOptions::default()).await
    }

    pub async fn spawn_with_options(
        config_path: impl AsRef<Path>,
        listener_addr: SocketAddr,
        options: SpawnOptions,
    ) -> Result<Self> {
        let mut instance = Self::spawn_no_wait(config_path, listener_addr, options.clone()).await?;
        instance.wait_for_listener(options.ready_timeout).await?;
        info!(?listener_addr, "Orion instance is ready");
        Ok(instance)
    }

    #[allow(clippy::unused_async)]
    pub async fn spawn_no_wait(
        config_path: impl AsRef<Path>,
        listener_addr: SocketAddr,
        options: SpawnOptions,
    ) -> Result<Self> {
        Self::spawn_internal(config_path, Some(listener_addr), options).await
    }

    #[allow(clippy::unused_async)]
    pub async fn spawn_no_listener(config_path: impl AsRef<Path>, options: SpawnOptions) -> Result<Self> {
        Self::spawn_internal(config_path, None, options).await
    }

    #[allow(
        clippy::too_many_lines,
        reason = "process spawn logic is inherently sequential and would gain nothing from splitting"
    )]
    pub async fn spawn_auto_port(
        config_path: impl AsRef<Path>,
        listener_name: impl Into<String>,
        options: SpawnOptions,
    ) -> Result<Self> {
        const MAX_CAPTURED_LINES: usize = 75;
        let listener_name = listener_name.into();
        let config_path = config_path.as_ref().to_path_buf();
        let shutdown_requested = Arc::new(AtomicBool::new(false));

        info!(?config_path, ?listener_name, "Spawning Orion instance with auto port discovery");

        let orion_bin = find_orion_binary()?;
        debug!(?orion_bin, "Found Orion binary");

        let mut cmd = Command::new(&orion_bin);
        cmd.arg("--config").arg(&config_path);

        if let Some(cpus) = options.num_cpus {
            cmd.arg("--num-cpus").arg(cpus.to_string());
        }

        if let Some(runtimes) = options.num_runtimes {
            cmd.arg("--num-runtimes").arg(runtimes.to_string());
        }

        let log_level = options.log_level.as_deref().unwrap_or("info");
        cmd.env("RUST_LOG", log_level);

        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::piped());

        let mut process = cmd.spawn().map_err(|e| {
            Error::ProcessStartFailed(format!("Failed to spawn orion binary at {}: {e}", orion_bin.display()))
        })?;

        let stdout = process.stdout.take();
        let stderr = process.stderr.take();

        let (result_tx, result_rx) = oneshot::channel::<std::result::Result<SocketAddr, String>>();
        let verbose = options.verbose_output;
        let name_for_parser = listener_name.clone();

        let captured_lines: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::with_capacity(MAX_CAPTURED_LINES)));

        let stderr_lines = Arc::clone(&captured_lines);
        let stderr_shutdown = Arc::clone(&shutdown_requested);
        let _stderr_reader = stderr.map(|stderr| {
            std::thread::spawn(move || {
                let reader = BufReader::new(stderr);
                for line in reader.lines() {
                    if stderr_shutdown.load(Ordering::SeqCst) {
                        break;
                    }
                    if let Ok(line) = line {
                        if let Ok(mut lines) = stderr_lines.lock() {
                            if lines.len() >= MAX_CAPTURED_LINES {
                                lines.remove(0);
                            }
                            lines.push(line.clone());
                        }
                        if verbose {
                            eprintln!("[ORION-ERR] {line}");
                        }
                        debug!(target: "orion_stderr", "{}", line);
                    }
                }
            })
        });

        let stdout_lines = Arc::clone(&captured_lines);
        let stdout_shutdown = Arc::clone(&shutdown_requested);
        let output_reader = stdout.map(|stdout| {
            std::thread::spawn(move || {
                let reader = BufReader::new(stdout);
                let mut result_tx = Some(result_tx);

                for line in reader.lines() {
                    if stdout_shutdown.load(Ordering::SeqCst) {
                        break;
                    }
                    match line {
                        Ok(line) => {
                            if let Ok(mut lines) = stdout_lines.lock() {
                                if lines.len() >= MAX_CAPTURED_LINES {
                                    lines.remove(0);
                                }
                                lines.push(line.clone());
                            }

                            if let Some(tx) = result_tx.take() {
                                if let Some(addr) = parse_listener_started(&line, &name_for_parser) {
                                    tx.send(Ok(addr)).unwrap_or_else(|e| {
                                        error!("Failed to send listener address: {e:?}");
                                    });
                                } else {
                                    result_tx = Some(tx); // Put it back
                                }
                            }
                            if verbose {
                                eprintln!("[ORION] {line}");
                            }
                            debug!(target: "orion_output", "{}", line);
                        },
                        Err(e) => {
                            warn!("Error reading orion output: {}", e);
                            break;
                        },
                    }
                }

                if let Some(tx) = result_tx.take() {
                    std::thread::sleep(Duration::from_millis(100));
                    let output = stdout_lines.lock().map(|lines| lines.join("\n")).unwrap_or_default();
                    tx.send(Err(output)).unwrap_or_else(|e| {
                        error!("Failed to send listener address: {e:?}");
                    });
                }
            })
        });

        let listener_addr = fast_timeout(options.ready_timeout, result_rx)
            .await
            .map_err(|_e| Error::ReadyTimeout(options.ready_timeout))?
            .map_err(|_e| Error::Config("Channel closed unexpectedly".into()))?
            .map_err(|output| Error::StartupFailed { exit_code: None, output })?;

        info!(?listener_addr, "Discovered Orion listener address");

        Ok(Self {
            process: Some(process),
            config_path,
            cleanup_config: options.cleanup_config,
            listener_addr: Some(listener_addr),
            admin_addr: options.admin_addr,
            shutdown_requested,
            _output_reader: output_reader,
        })
    }

    #[allow(
        clippy::too_many_lines,
        reason = "process spawn logic is inherently sequential and would gain nothing from splitting"
    )]
    pub async fn spawn_with_fixed_port(
        config_path: impl AsRef<Path>,
        listener_name: impl Into<String>,
        port: u16,
        options: SpawnOptions,
    ) -> Result<Self> {
        const MAX_CAPTURED_LINES: usize = 75;
        let listener_name = listener_name.into();
        let config_path = config_path.as_ref().to_path_buf();
        let shutdown_requested = Arc::new(AtomicBool::new(false));

        info!(?config_path, ?listener_name, port, "Spawning Orion instance with fixed port");

        let orion_bin = find_orion_binary()?;
        debug!(?orion_bin, "Found Orion binary");

        let mut cmd = Command::new(&orion_bin);
        cmd.arg("--config").arg(&config_path);

        if let Some(cpus) = options.num_cpus {
            cmd.arg("--num-cpus").arg(cpus.to_string());
        }

        if let Some(runtimes) = options.num_runtimes {
            cmd.arg("--num-runtimes").arg(runtimes.to_string());
        }

        let log_level = options.log_level.as_deref().unwrap_or("info");
        cmd.env("RUST_LOG", log_level);

        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::piped());

        let mut process = cmd.spawn().map_err(|e| {
            Error::ProcessStartFailed(format!("Failed to spawn orion binary at {}: {e}", orion_bin.display()))
        })?;

        let stdout = process.stdout.take();
        let stderr = process.stderr.take();

        let (result_tx, result_rx) = oneshot::channel::<std::result::Result<(), String>>();
        let verbose = options.verbose_output;
        let name_for_parser = listener_name.clone();
        let expected_port = port;

        let captured_lines: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::with_capacity(MAX_CAPTURED_LINES)));

        let stderr_lines = Arc::clone(&captured_lines);
        let stderr_shutdown = Arc::clone(&shutdown_requested);
        let _stderr_reader = stderr.map(|stderr| {
            std::thread::spawn(move || {
                let reader = BufReader::new(stderr);
                for line in reader.lines() {
                    if stderr_shutdown.load(Ordering::SeqCst) {
                        break;
                    }
                    if let Ok(line) = line {
                        if let Ok(mut lines) = stderr_lines.lock() {
                            if lines.len() >= MAX_CAPTURED_LINES {
                                lines.remove(0);
                            }
                            lines.push(line.clone());
                        }
                        if verbose {
                            eprintln!("[ORION-ERR] {line}");
                        }
                        debug!(target: "orion_stderr", "{}", line);
                    }
                }
            })
        });

        let stdout_lines = Arc::clone(&captured_lines);
        let stdout_shutdown = Arc::clone(&shutdown_requested);
        let output_reader = stdout.map(|stdout| {
            std::thread::spawn(move || {
                let reader = BufReader::new(stdout);
                let mut result_tx = Some(result_tx);

                for line in reader.lines() {
                    if stdout_shutdown.load(Ordering::SeqCst) {
                        break;
                    }
                    match line {
                        Ok(line) => {
                            if let Ok(mut lines) = stdout_lines.lock() {
                                if lines.len() >= MAX_CAPTURED_LINES {
                                    lines.remove(0);
                                }
                                lines.push(line.clone());
                            }

                            if let Some(tx) = result_tx.take() {
                                if let Some(addr) = parse_listener_started(&line, &name_for_parser) {
                                    if addr.port() == expected_port {
                                        tx.send(Ok(())).unwrap_or_else(|e| {
                                            error!("Failed to send listener ready signal: {e:?}");
                                        });
                                    } else {
                                        tx.send(Err(format!(
                                            "Listener started on wrong port. Expected: {}, Got: {}",
                                            expected_port,
                                            addr.port()
                                        )))
                                        .unwrap_or_else(|e| {
                                            error!("Failed to send listener ready signal: {e:?}");
                                        });
                                    }
                                } else {
                                    result_tx = Some(tx);
                                }
                            }
                            if verbose {
                                eprintln!("[ORION] {line}");
                            }
                            debug!(target: "orion_output", "{}", line);
                        },
                        Err(e) => {
                            warn!("Error reading orion output: {}", e);
                            break;
                        },
                    }
                }

                if let Some(tx) = result_tx.take() {
                    std::thread::sleep(Duration::from_millis(100));
                    let output = stdout_lines.lock().map(|lines| lines.join("\n")).unwrap_or_default();
                    tx.send(Err(output)).unwrap_or_else(|e| {
                        error!("Failed to send listener ready signal: {e:?}");
                    });
                }
            })
        });

        fast_timeout(options.ready_timeout, result_rx)
            .await
            .map_err(|_e| Error::ReadyTimeout(options.ready_timeout))?
            .map_err(|_e| Error::Config("Channel closed unexpectedly".into()))?
            .map_err(|output| Error::StartupFailed { exit_code: None, output })?;

        let listener_addr = SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), port);
        info!(?listener_addr, "Orion listener ready on fixed port");

        Ok(Self {
            process: Some(process),
            config_path,
            cleanup_config: options.cleanup_config,
            listener_addr: Some(listener_addr),
            admin_addr: options.admin_addr,
            shutdown_requested,
            _output_reader: output_reader,
        })
    }

    #[allow(clippy::unused_async)]
    async fn spawn_internal(
        config_path: impl AsRef<Path>,
        listener_addr: Option<SocketAddr>,
        options: SpawnOptions,
    ) -> Result<Self> {
        let config_path = config_path.as_ref().to_path_buf();
        let shutdown_requested = Arc::new(AtomicBool::new(false));

        info!(?config_path, ?listener_addr, "Spawning Orion instance");

        let orion_bin = find_orion_binary()?;
        debug!(?orion_bin, "Found Orion binary");

        let mut cmd = Command::new(&orion_bin);
        cmd.arg("--config").arg(&config_path);

        if let Some(cpus) = options.num_cpus {
            cmd.arg("--num-cpus").arg(cpus.to_string());
        }

        let log_level = options.log_level.as_deref().unwrap_or("info");
        cmd.env("RUST_LOG", log_level);

        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::piped());

        let mut process = cmd.spawn().map_err(|e| {
            Error::ProcessStartFailed(format!("Failed to spawn orion binary at {}: {e}", orion_bin.display()))
        })?;

        let stderr = process.stderr.take();
        let shutdown_flag = Arc::clone(&shutdown_requested);
        let verbose = options.verbose_output;
        let output_reader = stderr.map(|stderr| {
            std::thread::spawn(move || {
                let reader = BufReader::new(stderr);
                for line in reader.lines() {
                    if shutdown_flag.load(Ordering::SeqCst) {
                        break;
                    }
                    match line {
                        Ok(line) => {
                            if verbose {
                                eprintln!("[ORION] {line}");
                            }
                            debug!(target: "orion_output", "{}", line);
                        },
                        Err(e) => {
                            warn!("Error reading orion output: {}", e);
                            break;
                        },
                    }
                }
            })
        });

        Ok(Self {
            process: Some(process),
            config_path,
            cleanup_config: options.cleanup_config,
            listener_addr,
            admin_addr: options.admin_addr,
            shutdown_requested,
            _output_reader: output_reader,
        })
    }

    /// Polls Orion's admin /ready endpoint until it returns 200 (`ProxyState::Live` is set).
    ///
    /// `spawn_auto_port` returns as soon as the downstream listener port is bound, but
    /// `ProxyState::Live` is set slightly later once all proxy runtimes are fully started.
    /// Calling this after `spawn_auto_port` closes that gap. Backends are always started
    /// before Orion in tests, so by the time this returns, the upstream is also ready.
    ///
    /// Requires the bootstrap config to include an admin section and `SpawnOptions` to be
    /// built with `with_test_admin()`. Returns immediately if no `admin_addr` is configured.
    pub async fn wait_for_upstream_ready(&self, timeout: Duration) -> Result<()> {
        const READY_REQ: &[u8] = b"GET /ready HTTP/1.0\r\nHost: localhost\r\n\r\n";
        const RETRY_INTERVAL: Duration = Duration::from_millis(50);
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let Some(admin_addr) = self.admin_addr else {
            return Ok(());
        };

        let deadline = Instant::now() + timeout;

        loop {
            let outcome = async {
                let mut stream = tokio::net::TcpStream::connect(admin_addr).await?;
                stream.write_all(READY_REQ).await?;
                let mut buf = [0u8; 64];
                let n = stream.read(&mut buf).await?;
                Ok::<Vec<u8>, std::io::Error>(buf.get(..n).unwrap_or_default().to_vec())
            }
            .await;

            match outcome {
                Ok(bytes) => {
                    let status = std::str::from_utf8(&bytes)
                        .ok()
                        .and_then(|s| s.split_whitespace().nth(1))
                        .and_then(|s| s.parse::<u16>().ok());
                    if status == Some(200) {
                        return Ok(());
                    }
                    debug!(?status, "admin /ready not yet 200, retrying");
                },
                Err(e) => debug!(?e, "admin probe failed, retrying"),
            }

            if Instant::now() >= deadline {
                return Err(Error::ReadyTimeout(timeout));
            }
            tokio::time::sleep(RETRY_INTERVAL).await;
        }
    }

    pub async fn wait_for_listener(&mut self, timeout: Duration) -> Result<()> {
        let addr = self.listener_addr.ok_or_else(|| {
            Error::Config("No listener address configured. Use wait_for_listener_at() instead.".into())
        })?;
        self.wait_for_listener_at(addr, timeout).await
    }

    pub async fn wait_for_listener_at(&mut self, addr: SocketAddr, timeout: Duration) -> Result<()> {
        let start = Instant::now();

        loop {
            if let Some(ref mut process) = self.process {
                match process.try_wait() {
                    Ok(Some(status)) => {
                        return Err(Error::ProcessExitedUnexpectedly(status.code()));
                    },
                    Ok(None) => {},
                    Err(e) => {
                        return Err(Error::ProcessStartFailed(format!("Failed to check process status: {e}")));
                    },
                }
            }

            if TcpStream::connect_timeout(&addr, Duration::from_millis(50)).is_ok() {
                return Ok(());
            }

            if start.elapsed() > timeout {
                if let Some(ref mut process) = self.process {
                    process.kill().unwrap_or_else(|e| {
                        error!("Failed to kill orion process after timeout: {e}");
                    });
                }
                return Err(Error::ReadyTimeout(timeout));
            }

            tokio::time::sleep(READY_POLL_INTERVAL).await;
        }
    }

    #[must_use]
    pub fn listener_addr(&self) -> Option<SocketAddr> {
        self.listener_addr
    }

    #[must_use]
    pub fn config_path(&self) -> &Path {
        &self.config_path
    }

    #[must_use]
    pub fn is_running(&mut self) -> bool {
        if let Some(ref mut process) = self.process {
            matches!(process.try_wait(), Ok(None))
        } else {
            false
        }
    }

    pub fn shutdown(mut self) {
        self.shutdown_requested.store(true, Ordering::SeqCst);

        if let Some(mut process) = self.process.take() {
            info!("Shutting down Orion instance");
            if let Err(e) = process.kill() {
                if e.kind() != std::io::ErrorKind::InvalidInput {
                    error!("Failed to kill orion process during shutdown: {e}");
                }
            }
            if let Err(e) = process.wait() {
                error!("Failed to wait for orion process during shutdown: {e}");
            }
        }
    }
}

impl Drop for OrionInstance {
    fn drop(&mut self) {
        self.shutdown_requested.store(true, Ordering::SeqCst);

        if let Some(mut process) = self.process.take() {
            if let Err(e) = process.kill() {
                // InvalidInput means the process already exited — that's expected
                if e.kind() != std::io::ErrorKind::InvalidInput {
                    error!("Failed to send kill signal to orion process: {e}");
                }
            }

            // Bounded wait: SIGKILL should be near-instant; timeout signals a kernel issue.
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                match process.try_wait() {
                    Ok(Some(_)) => break,
                    Ok(None) if Instant::now() < deadline => {
                        std::thread::sleep(Duration::from_millis(50));
                    },
                    Ok(None) => {
                        error!(
                            "Orion process did not exit within 5s after kill — \
                             port may remain bound and affect subsequent tests"
                        );
                        break;
                    },
                    Err(e) => {
                        error!("Failed to wait for orion process exit: {e}");
                        break;
                    },
                }
            }
        }

        if self.cleanup_config {
            if let Err(e) = std::fs::remove_file(&self.config_path) {
                warn!(?e, path = ?self.config_path, "Failed to remove config file");
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct SpawnOptions {
    pub ready_timeout: Duration,
    pub cleanup_config: bool,
    pub num_cpus: Option<usize>,
    pub num_runtimes: Option<usize>,
    pub log_level: Option<String>,
    pub verbose_output: bool,
    pub admin_addr: Option<SocketAddr>,
}

impl Default for SpawnOptions {
    fn default() -> Self {
        Self {
            ready_timeout: DEFAULT_READY_TIMEOUT,
            cleanup_config: false,
            num_cpus: Some(1),
            num_runtimes: None,
            log_level: None,
            verbose_output: false,
            admin_addr: None,
        }
    }
}

impl SpawnOptions {
    #[must_use]
    pub fn with_cleanup(mut self) -> Self {
        self.cleanup_config = true;
        self
    }

    #[must_use]
    pub fn with_ready_timeout(mut self, timeout: Duration) -> Self {
        self.ready_timeout = timeout;
        self
    }

    #[must_use]
    pub fn with_num_cpus(mut self, cpus: usize) -> Self {
        self.num_cpus = Some(cpus);
        self
    }

    #[must_use]
    pub fn with_num_runtimes(mut self, runtimes: usize) -> Self {
        self.num_runtimes = Some(runtimes);
        self
    }

    #[must_use]
    pub fn with_verbose(mut self) -> Self {
        self.verbose_output = true;
        self
    }

    #[must_use]
    pub fn with_log_level(mut self, level: impl Into<String>) -> Self {
        self.log_level = Some(level.into());
        self
    }

    /// Configures the admin address to `TEST_ADMIN_PORT` on localhost. The bootstrap config
    /// must include a matching `.admin("127.0.0.1", TEST_ADMIN_PORT)` call.
    #[must_use]
    pub fn with_test_admin(mut self) -> Self {
        self.admin_addr = Some(SocketAddr::from(([127, 0, 0, 1], crate::TEST_ADMIN_PORT)));
        self
    }
}

fn find_orion_binary() -> Result<PathBuf> {
    if let Ok(path) = std::env::var("ORION_BIN") {
        let path = PathBuf::from(path);
        if path.exists() {
            return Ok(path);
        }
    }

    let mut current = std::env::current_dir()?;
    let workspace_root = loop {
        let cargo_toml = current.join("Cargo.toml");
        if cargo_toml.exists() {
            let content = std::fs::read_to_string(&cargo_toml)?;
            if content.contains("[workspace]") {
                break current;
            }
        }
        if !current.pop() {
            return Err(Error::ProcessStartFailed("Could not find workspace root".to_owned()));
        }
    };

    let debug_path = workspace_root.join("target/debug/orion");
    if debug_path.exists() {
        return Ok(debug_path);
    }

    let release_path = workspace_root.join("target/release/orion");
    if release_path.exists() {
        return Ok(release_path);
    }

    Err(Error::ProcessStartFailed("Could not find orion binary. Run `cargo build -p orion-proxy` first.".to_owned()))
}

fn parse_listener_started(line: &str, name: &str) -> Option<SocketAddr> {
    let pattern = format!("listener '{name}' started: ");
    let idx = line.find(&pattern)?;
    let addr_start = idx + pattern.len();
    let suffix = line.get(addr_start..)?;
    let addr_end = suffix.find(' ').map(|i| addr_start + i).unwrap_or(line.len());
    line.get(addr_start..addr_end)?.parse().ok()
}
