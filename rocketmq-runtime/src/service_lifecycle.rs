// Copyright 2026 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Process-level readiness, liveness, and shutdown coordination.
//!
//! The lifecycle is intentionally transport-neutral. Kubernetes probes observe
//! an explicit service state instead of treating an open business port as
//! health, while pre-stop and operating-system signals converge on the first
//! absolute shutdown deadline.

use std::env;
use std::net::SocketAddr;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use parking_lot::Mutex;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::watch;

use crate::wait_for_signal_result;
use crate::RuntimeError;
use crate::RuntimeResult;
use crate::ServiceContext;
use crate::ShutdownDeadline;

pub const HEALTH_BIND_ADDR_ENV: &str = "ROCKETMQ_HEALTH_BIND_ADDR";
pub const SHUTDOWN_TIMEOUT_SECONDS_ENV: &str = "ROCKETMQ_SHUTDOWN_TIMEOUT_SECONDS";
pub const LIVENESS_STALE_SECONDS_ENV: &str = "ROCKETMQ_LIVENESS_STALE_SECONDS";
pub const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(45);
pub const DEFAULT_LIVENESS_STALE_AFTER: Duration = Duration::from_secs(30);
const PROGRESS_INTERVAL: Duration = Duration::from_secs(1);
const PROBE_REQUEST_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_PROBE_REQUEST_BYTES: usize = 2048;

const STATE_STARTING: u8 = 0;
const STATE_READY: u8 = 1;
const STATE_DRAINING: u8 = 2;
const STATE_STOPPED: u8 = 3;
const STATE_FAILED: u8 = 4;

/// Stable process lifecycle states used by readiness and liveness probes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceLifecycleState {
    Starting,
    Ready,
    Draining,
    Stopped,
    Failed,
}

impl ServiceLifecycleState {
    const fn from_u8(value: u8) -> Self {
        match value {
            STATE_READY => Self::Ready,
            STATE_DRAINING => Self::Draining,
            STATE_STOPPED => Self::Stopped,
            STATE_FAILED => Self::Failed,
            _ => Self::Starting,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Ready => "ready",
            Self::Draining => "draining",
            Self::Stopped => "stopped",
            Self::Failed => "failed",
        }
    }
}

/// Source of the first shutdown request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownReason {
    PreStop,
    Signal,
    Internal,
}

impl ShutdownReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PreStop => "pre_stop",
            Self::Signal => "signal",
            Self::Internal => "internal",
        }
    }
}

/// Immutable first shutdown request and its shared absolute deadline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShutdownRequest {
    pub reason: ShutdownReason,
    pub deadline: ShutdownDeadline,
}

/// Versioned runtime configuration for the process lifecycle boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceLifecycleConfig {
    pub service_name: Arc<str>,
    pub probe_bind_addr: Option<SocketAddr>,
    pub shutdown_timeout: Duration,
    pub liveness_stale_after: Duration,
}

impl ServiceLifecycleConfig {
    /// Reads the optional probe address and bounded lifecycle budgets from the environment.
    ///
    /// The probe server is disabled when `ROCKETMQ_HEALTH_BIND_ADDR` is absent, preserving
    /// non-Kubernetes entrypoint behavior. Shutdown coordination remains active.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError::InvalidConfig`] for a malformed address, non-UTF-8 input,
    /// zero timeout, or a liveness window shorter than two progress intervals.
    pub fn from_env(service_name: impl Into<Arc<str>>) -> RuntimeResult<Self> {
        let probe_bind_addr = optional_env(HEALTH_BIND_ADDR_ENV)?
            .map(|raw| parse_socket_addr(HEALTH_BIND_ADDR_ENV, &raw))
            .transpose()?;
        let shutdown_timeout = parse_duration_env(SHUTDOWN_TIMEOUT_SECONDS_ENV, DEFAULT_SHUTDOWN_TIMEOUT, 1, 300)?;
        let liveness_stale_after =
            parse_duration_env(LIVENESS_STALE_SECONDS_ENV, DEFAULT_LIVENESS_STALE_AFTER, 2, 300)?;
        Ok(Self {
            service_name: service_name.into(),
            probe_bind_addr,
            shutdown_timeout,
            liveness_stale_after,
        })
    }
}

fn optional_env(name: &'static str) -> RuntimeResult<Option<String>> {
    let Some(value) = env::var_os(name) else {
        return Ok(None);
    };
    value
        .into_string()
        .map(Some)
        .map_err(|_| RuntimeError::InvalidConfig(format!("{name} must contain valid UTF-8")))
}

fn parse_socket_addr(name: &'static str, raw: &str) -> RuntimeResult<SocketAddr> {
    raw.parse::<SocketAddr>()
        .map_err(|error| RuntimeError::InvalidConfig(format!("{name} must be a socket address: {error}")))
}

fn parse_duration_env(
    name: &'static str,
    default: Duration,
    minimum_seconds: u64,
    maximum_seconds: u64,
) -> RuntimeResult<Duration> {
    let Some(raw) = optional_env(name)? else {
        return Ok(default);
    };
    let seconds = raw.parse::<u64>().map_err(|error| {
        RuntimeError::InvalidConfig(format!("{name} must be an integer number of seconds: {error}"))
    })?;
    if !(minimum_seconds..=maximum_seconds).contains(&seconds) {
        return Err(RuntimeError::InvalidConfig(format!(
            "{name} must be between {minimum_seconds} and {maximum_seconds} seconds"
        )));
    }
    Ok(Duration::from_secs(seconds))
}

#[derive(Debug)]
struct ServiceLifecycleInner {
    config: ServiceLifecycleConfig,
    state: AtomicU8,
    started_at: Instant,
    last_progress_millis: AtomicU64,
    shutdown_request: Mutex<Option<ShutdownRequest>>,
    shutdown_tx: watch::Sender<Option<ShutdownRequest>>,
    started: AtomicBool,
    probe_local_addr: Mutex<Option<SocketAddr>>,
}

/// Shared process lifecycle and health-probe owner.
#[derive(Debug, Clone)]
pub struct ServiceLifecycle {
    inner: Arc<ServiceLifecycleInner>,
}

impl ServiceLifecycle {
    pub fn new(config: ServiceLifecycleConfig) -> Self {
        let (shutdown_tx, _shutdown_rx) = watch::channel(None);
        Self {
            inner: Arc::new(ServiceLifecycleInner {
                config,
                state: AtomicU8::new(STATE_STARTING),
                started_at: Instant::now(),
                last_progress_millis: AtomicU64::new(0),
                shutdown_request: Mutex::new(None),
                shutdown_tx,
                started: AtomicBool::new(false),
                probe_local_addr: Mutex::new(None),
            }),
        }
    }

    pub fn from_env(service_name: impl Into<Arc<str>>) -> RuntimeResult<Self> {
        Ok(Self::new(ServiceLifecycleConfig::from_env(service_name)?))
    }

    pub fn config(&self) -> &ServiceLifecycleConfig {
        &self.inner.config
    }

    /// Starts the progress heartbeat and optional HTTP probe server under `service_context`.
    ///
    /// # Errors
    ///
    /// Returns an error when called twice, when the listener cannot bind, or when either
    /// owned service task cannot be registered.
    pub async fn start(&self, service_context: &ServiceContext) -> RuntimeResult<()> {
        if self.inner.started.swap(true, Ordering::AcqRel) {
            return Err(RuntimeError::LifecycleOperation {
                operation: "start_service_lifecycle",
                message: "service lifecycle is already started".to_string(),
            });
        }

        self.record_progress();
        let heartbeat = self.clone();
        let heartbeat_cancellation = service_context.task_group().cancellation_token();
        service_context.spawn_service("service-lifecycle.progress", async move {
            let mut interval = tokio::time::interval(PROGRESS_INTERVAL);
            loop {
                tokio::select! {
                    _ = heartbeat_cancellation.cancelled() => break,
                    _ = interval.tick() => heartbeat.record_progress(),
                }
            }
        })?;

        let Some(bind_addr) = self.inner.config.probe_bind_addr else {
            return Ok(());
        };
        let listener = TcpListener::bind(bind_addr)
            .await
            .map_err(|error| RuntimeError::LifecycleOperation {
                operation: "bind_service_health_probe",
                message: error.to_string(),
            })?;
        let local_addr = listener
            .local_addr()
            .map_err(|error| RuntimeError::LifecycleOperation {
                operation: "inspect_service_health_probe",
                message: error.to_string(),
            })?;
        *self.inner.probe_local_addr.lock() = Some(local_addr);

        let lifecycle = self.clone();
        let cancellation = service_context.task_group().cancellation_token();
        service_context.spawn_service("service-lifecycle.probe-server", async move {
            lifecycle.serve_probe_requests(listener, cancellation).await;
        })?;
        tracing::info!(
            service = %self.inner.config.service_name,
            bind = %local_addr,
            "service lifecycle probe server listening"
        );
        Ok(())
    }

    pub fn state(&self) -> ServiceLifecycleState {
        ServiceLifecycleState::from_u8(self.inner.state.load(Ordering::Acquire))
    }

    pub fn probe_local_addr(&self) -> Option<SocketAddr> {
        *self.inner.probe_local_addr.lock()
    }

    pub fn is_ready(&self) -> bool {
        self.state() == ServiceLifecycleState::Ready
    }

    pub fn is_live(&self) -> bool {
        if matches!(
            self.state(),
            ServiceLifecycleState::Stopped | ServiceLifecycleState::Failed
        ) {
            return false;
        }
        self.progress_age() <= self.inner.config.liveness_stale_after
    }

    pub fn record_progress(&self) {
        let elapsed = self.inner.started_at.elapsed().as_millis();
        self.inner
            .last_progress_millis
            .store(u64::try_from(elapsed).unwrap_or(u64::MAX), Ordering::Release);
    }

    fn progress_age(&self) -> Duration {
        let last = self.inner.last_progress_millis.load(Ordering::Acquire);
        let now = u64::try_from(self.inner.started_at.elapsed().as_millis()).unwrap_or(u64::MAX);
        Duration::from_millis(now.saturating_sub(last))
    }

    /// Marks startup dependencies ready without allowing a drain request to be reversed.
    ///
    /// # Errors
    ///
    /// Returns an error when shutdown or failure already began.
    pub fn mark_ready(&self) -> RuntimeResult<()> {
        match self
            .inner
            .state
            .compare_exchange(STATE_STARTING, STATE_READY, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => {
                self.record_progress();
                Ok(())
            }
            Err(STATE_READY) => Ok(()),
            Err(state) => Err(RuntimeError::LifecycleOperation {
                operation: "mark_service_ready",
                message: format!(
                    "cannot transition service from {} to ready",
                    ServiceLifecycleState::from_u8(state).as_str()
                ),
            }),
        }
    }

    pub fn mark_failed(&self) {
        self.inner.state.store(STATE_FAILED, Ordering::Release);
        self.record_progress();
    }

    pub fn mark_stopped(&self) {
        let _ = self
            .inner
            .state
            .try_update(Ordering::AcqRel, Ordering::Acquire, |state| {
                (state != STATE_FAILED).then_some(STATE_STOPPED)
            });
        self.record_progress();
    }

    /// Records the first shutdown request and returns its immutable deadline.
    ///
    /// Repeated pre-stop or signal delivery is idempotent and cannot extend the budget.
    pub fn request_shutdown(&self, reason: ShutdownReason) -> ShutdownRequest {
        let mut request = self.inner.shutdown_request.lock();
        if let Some(existing) = *request {
            return existing;
        }
        let first = ShutdownRequest {
            reason,
            deadline: ShutdownDeadline::after(self.inner.config.shutdown_timeout),
        };
        *request = Some(first);
        let state = self.inner.state.load(Ordering::Acquire);
        if state != STATE_FAILED && state != STATE_STOPPED {
            self.inner.state.store(STATE_DRAINING, Ordering::Release);
        }
        self.record_progress();
        self.inner.shutdown_tx.send_replace(Some(first));
        first
    }

    pub fn shutdown_request(&self) -> Option<ShutdownRequest> {
        *self.inner.shutdown_request.lock()
    }

    pub async fn wait_for_shutdown(&self) -> ShutdownRequest {
        if let Some(request) = self.shutdown_request() {
            return request;
        }
        let mut receiver = self.inner.shutdown_tx.subscribe();
        loop {
            if let Some(request) = *receiver.borrow_and_update() {
                return request;
            }
            if receiver.changed().await.is_err() {
                return self.request_shutdown(ShutdownReason::Internal);
            }
        }
    }

    /// Waits for pre-stop or an operating-system termination signal.
    ///
    /// # Errors
    ///
    /// Returns the platform signal registration error without inventing a successful signal.
    pub async fn wait_for_shutdown_signal(&self) -> RuntimeResult<ShutdownRequest> {
        tokio::select! {
            request = self.wait_for_shutdown() => Ok(request),
            signal = wait_for_signal_result() => {
                signal?;
                Ok(self.request_shutdown(ShutdownReason::Signal))
            }
        }
    }

    async fn serve_probe_requests(&self, listener: TcpListener, cancellation: tokio_util::sync::CancellationToken) {
        loop {
            tokio::select! {
                _ = cancellation.cancelled() => break,
                accepted = listener.accept() => {
                    match accepted {
                        Ok((stream, _peer)) => self.handle_probe_connection(stream).await,
                        Err(error) => {
                            tracing::warn!(error = %error, "service lifecycle probe accept failed");
                            self.mark_failed();
                            self.request_shutdown(ShutdownReason::Internal);
                            break;
                        }
                    }
                }
            }
        }
    }

    async fn handle_probe_connection(&self, mut stream: TcpStream) {
        let mut request = vec![0_u8; MAX_PROBE_REQUEST_BYTES];
        let read = tokio::time::timeout(PROBE_REQUEST_TIMEOUT, stream.read(&mut request)).await;
        let response = match read {
            Ok(Ok(0)) => probe_response(400, "bad_request", self.state()),
            Ok(Ok(length)) => self.route_probe_request(&request[..length]),
            Ok(Err(_)) | Err(_) => probe_response(408, "request_timeout", self.state()),
        };
        let _ = stream.write_all(response.as_bytes()).await;
        let _ = stream.shutdown().await;
    }

    fn route_probe_request(&self, request: &[u8]) -> String {
        let first_line = request.split(|byte| *byte == b'\n').next().unwrap_or_default();
        let first_line = String::from_utf8_lossy(first_line);
        let mut fields = first_line.split_ascii_whitespace();
        let method = fields.next().unwrap_or_default();
        let path = fields.next().unwrap_or_default();
        if method != "GET" && method != "POST" {
            return probe_response(405, "method_not_allowed", self.state());
        }
        match path {
            "/readyz" if self.is_ready() => probe_response(200, "ready", self.state()),
            "/readyz" => probe_response(503, "not_ready", self.state()),
            "/livez" if self.is_live() => probe_response(200, "live", self.state()),
            "/livez" => probe_response(503, "not_live", self.state()),
            "/drainz" => {
                self.request_shutdown(ShutdownReason::PreStop);
                probe_response(200, "draining", self.state())
            }
            _ => probe_response(404, "not_found", self.state()),
        }
    }
}

fn probe_response(status: u16, status_text: &'static str, state: ServiceLifecycleState) -> String {
    let reason = match status {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        405 => "Method Not Allowed",
        408 => "Request Timeout",
        _ => "Service Unavailable",
    };
    let body = format!(r#"{{"status":"{status_text}","state":"{}"}}"#, state.as_str());
    format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: \
         close\r\nCache-Control: no-store\r\n\r\n{body}",
        body.len()
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RuntimeContext;

    fn config(probe_bind_addr: Option<SocketAddr>) -> ServiceLifecycleConfig {
        ServiceLifecycleConfig {
            service_name: Arc::from("test-service"),
            probe_bind_addr,
            shutdown_timeout: Duration::from_secs(45),
            liveness_stale_after: Duration::from_secs(30),
        }
    }

    async fn request(addr: SocketAddr, path: &str) -> String {
        let mut stream = TcpStream::connect(addr).await.expect("connect lifecycle probe");
        stream
            .write_all(format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n").as_bytes())
            .await
            .expect("write lifecycle probe request");
        let mut response = Vec::new();
        stream
            .read_to_end(&mut response)
            .await
            .expect("read lifecycle probe response");
        String::from_utf8(response).expect("probe response is UTF-8")
    }

    #[test]
    fn repeated_shutdown_requests_reuse_the_first_deadline() {
        let lifecycle = ServiceLifecycle::new(config(None));
        lifecycle.mark_ready().unwrap();
        let first = lifecycle.request_shutdown(ShutdownReason::PreStop);
        let second = lifecycle.request_shutdown(ShutdownReason::Signal);
        assert_eq!(first, second);
        assert_eq!(second.reason, ShutdownReason::PreStop);
        assert_eq!(lifecycle.state(), ServiceLifecycleState::Draining);
        assert!(
            lifecycle.mark_ready().is_err(),
            "drain must not transition back to ready"
        );
    }

    #[test]
    fn failed_is_not_overwritten_by_normal_stop_completion() {
        let lifecycle = ServiceLifecycle::new(config(None));
        lifecycle.mark_failed();
        lifecycle.mark_stopped();

        assert_eq!(lifecycle.state(), ServiceLifecycleState::Failed);
    }

    #[tokio::test]
    async fn probe_server_reports_state_and_pre_stop_starts_drain() {
        let context = RuntimeContext::from_current("service-lifecycle-probe-test");
        let service = context.service_context("service-lifecycle-probe-test");
        let lifecycle = ServiceLifecycle::new(config(Some(SocketAddr::from(([127, 0, 0, 1], 0)))));
        lifecycle.start(&service).await.unwrap();
        let addr = lifecycle.probe_local_addr().expect("bound probe address");

        let starting = request(addr, "/readyz").await;
        assert!(starting.starts_with("HTTP/1.1 503"), "{starting}");
        assert!(request(addr, "/livez").await.starts_with("HTTP/1.1 200"));

        lifecycle.mark_ready().unwrap();
        assert!(request(addr, "/readyz").await.starts_with("HTTP/1.1 200"));
        assert!(request(addr, "/drainz").await.starts_with("HTTP/1.1 200"));
        assert!(!lifecycle.is_ready());
        assert_eq!(lifecycle.wait_for_shutdown().await.reason, ShutdownReason::PreStop);

        let report = context.shutdown_tasks(Duration::from_secs(1)).await;
        assert!(report.is_healthy(), "{}", report.to_json());
    }

    #[tokio::test]
    async fn liveness_rejects_stale_progress_and_terminal_states() {
        let mut stale_config = config(None);
        stale_config.liveness_stale_after = Duration::from_millis(5);
        let lifecycle = ServiceLifecycle::new(stale_config);
        lifecycle.record_progress();
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!lifecycle.is_live());
        lifecycle.record_progress();
        assert!(lifecycle.is_live());
        lifecycle.mark_failed();
        assert!(!lifecycle.is_live());
    }
}
