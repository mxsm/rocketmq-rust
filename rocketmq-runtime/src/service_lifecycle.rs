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

mod probe;

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
#[cfg(test)]
use tokio::io::AsyncReadExt;
#[cfg(test)]
use tokio::io::AsyncWriteExt;
#[cfg(test)]
use tokio::net::TcpListener;
#[cfg(test)]
use tokio::net::TcpStream;
use tokio::sync::watch;

use crate::critical::CriticalFailure;
use crate::critical::CriticalFailureState;
use crate::task_group::TaskId;
use crate::task_spawner::TaskSpawner;
use crate::wait_for_signal_result;
use crate::ChildServiceContext;
use crate::RuntimeError;
use crate::RuntimeResult;
use crate::ShutdownDeadline;
use crate::TaskGroup;

/// The health bind addr env constant.
pub const HEALTH_BIND_ADDR_ENV: &str = "ROCKETMQ_HEALTH_BIND_ADDR";
/// The shutdown timeout seconds env constant.
pub const SHUTDOWN_TIMEOUT_SECONDS_ENV: &str = "ROCKETMQ_SHUTDOWN_TIMEOUT_SECONDS";
/// The liveness stale seconds env constant.
pub const LIVENESS_STALE_SECONDS_ENV: &str = "ROCKETMQ_LIVENESS_STALE_SECONDS";
/// HTTP methods accepted by `/drainz`: `POST` (the default) or `GET,POST`.
pub const HEALTH_DRAIN_METHODS_ENV: &str = "ROCKETMQ_HEALTH_DRAIN_METHODS";
/// Default process shutdown budget of [`ServiceLifecycleConfig::shutdown_timeout`].
pub const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(45);
/// The default liveness stale after constant.
pub const DEFAULT_LIVENESS_STALE_AFTER: Duration = Duration::from_secs(30);
const PROGRESS_INTERVAL: Duration = Duration::from_secs(1);

const STATE_STARTING: u8 = 0;
const STATE_READY: u8 = 1;
const STATE_DRAINING: u8 = 2;
const STATE_STOPPED: u8 = 3;
const STATE_FAILED: u8 = 4;

/// Aggregate dependency readiness, independent of maintenance and process shutdown.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DependencyReadiness {
    /// Required dependencies currently satisfy the caller's health policy.
    Ready,
    /// Required dependencies are unavailable or their evidence has expired.
    Degraded,
}

/// How a handled critical task failure affects the service.
///
/// The choice belongs to the business: the runtime only guarantees that
/// readiness is revoked and that the failure reaches the handler.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CriticalFailureRecovery {
    /// Revoke dependency readiness and keep serving.
    ///
    /// Use this when the failed work is not required for the process to answer
    /// traffic correctly and the caller handles the consequence elsewhere.
    RevokeReadiness,
    /// Revoke readiness and mark the service failed.
    FailService,
    /// Revoke readiness, mark the service failed, and request an ordered shutdown.
    ///
    /// The first request wins, so this cannot extend or replace a shutdown that
    /// already started.
    FailAndRequestShutdown,
}

/// Stable process lifecycle states used by readiness and liveness probes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceLifecycleState {
    /// Represents the starting case.
    Starting,
    /// Represents the ready case.
    Ready,
    /// Represents the draining case.
    Draining,
    /// Represents the stopped case.
    Stopped,
    /// Represents the failed case.
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

    /// Borrows this value as str.
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
    /// Represents the pre stop case.
    PreStop,
    /// Represents the signal case.
    Signal,
    /// Represents the internal case.
    Internal,
}

impl ShutdownReason {
    /// Borrows this value as str.
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
    /// The reason value.
    pub reason: ShutdownReason,
    /// The deadline value.
    pub deadline: ShutdownDeadline,
}

/// HTTP methods that the `/drainz` probe route accepts.
///
/// A drain request starts the process shutdown, so the default accepts only
/// `POST`. Kubernetes `preStop.httpGet` hooks can send only `GET`; a deployment
/// that drains through such a hook opts in with [`Self::GetOrPost`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum DrainRequestMethods {
    /// Accepts only `POST`. `GET` receives `405 Method Not Allowed`.
    #[default]
    PostOnly,
    /// Accepts `GET` and `POST`.
    GetOrPost,
}

impl DrainRequestMethods {
    /// Parses the value of `ROCKETMQ_HEALTH_DRAIN_METHODS`.
    ///
    /// Accepts a comma-separated method list, ignoring case, blanks around
    /// methods, and order: `POST` or `GET,POST`. Returns `None` for any other
    /// list, including `GET` alone, since draining must stay reachable by `POST`.
    pub fn parse(raw: &str) -> Option<Self> {
        let mut get = false;
        let mut post = false;
        for method in raw.split(',').map(str::trim) {
            if method.eq_ignore_ascii_case("GET") && !get {
                get = true;
            } else if method.eq_ignore_ascii_case("POST") && !post {
                post = true;
            } else {
                return None;
            }
        }
        match (get, post) {
            (false, true) => Some(Self::PostOnly),
            (true, true) => Some(Self::GetOrPost),
            (_, false) => None,
        }
    }

    /// Returns whether `method` may start a drain.
    pub fn allows(self, method: &str) -> bool {
        match self {
            Self::PostOnly => method == "POST",
            Self::GetOrPost => method == "GET" || method == "POST",
        }
    }

    /// Returns the value of the `Allow` header for a rejected drain request.
    pub const fn allow_header(self) -> &'static str {
        match self {
            Self::PostOnly => "POST",
            Self::GetOrPost => "GET, POST",
        }
    }
}

/// Versioned runtime configuration for the process lifecycle boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceLifecycleConfig {
    /// The service name value.
    pub service_name: Arc<str>,
    /// The probe bind addr value.
    pub probe_bind_addr: Option<SocketAddr>,
    /// Process shutdown budget.
    ///
    /// The first shutdown request fixes a `ShutdownDeadline` this far ahead.
    /// That deadline is the single source for component shutdown and the
    /// owner's final shutdown; repeated requests never extend it.
    pub shutdown_timeout: Duration,
    /// The liveness stale after value.
    pub liveness_stale_after: Duration,
    /// HTTP methods that `/drainz` accepts.
    pub drain_request_methods: DrainRequestMethods,
}

impl ServiceLifecycleConfig {
    /// Reads the optional probe address and bounded lifecycle budgets from the environment.
    ///
    /// The probe server is disabled when `ROCKETMQ_HEALTH_BIND_ADDR` is absent, preserving
    /// non-Kubernetes entrypoint behavior. Shutdown coordination remains active.
    ///
    /// # Errors
    ///
    /// Returns a configuration runtime failure for a malformed address, non-UTF-8 input,
    /// zero timeout, a liveness window shorter than two progress intervals, or a
    /// drain method list other than `POST` or `GET,POST`.
    pub fn from_env(service_name: impl Into<Arc<str>>) -> RuntimeResult<Self> {
        let probe_bind_addr = optional_env(HEALTH_BIND_ADDR_ENV)?
            .map(|raw| parse_socket_addr(HEALTH_BIND_ADDR_ENV, &raw))
            .transpose()?;
        let shutdown_timeout = parse_duration_env(SHUTDOWN_TIMEOUT_SECONDS_ENV, DEFAULT_SHUTDOWN_TIMEOUT, 1, 300)?;
        let liveness_stale_after =
            parse_duration_env(LIVENESS_STALE_SECONDS_ENV, DEFAULT_LIVENESS_STALE_AFTER, 2, 300)?;
        let drain_request_methods = match optional_env(HEALTH_DRAIN_METHODS_ENV)? {
            None => DrainRequestMethods::default(),
            Some(raw) => DrainRequestMethods::parse(&raw)
                .ok_or_else(|| RuntimeError::configuration(crate::RuntimeOperation::ServiceLifecycleDrainMethods))?,
        };
        Ok(Self {
            service_name: service_name.into(),
            probe_bind_addr,
            shutdown_timeout,
            liveness_stale_after,
            drain_request_methods,
        })
    }
}

fn optional_env(name: &'static str) -> RuntimeResult<Option<String>> {
    match env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(RuntimeError::configuration_failure(
            crate::RuntimeOperation::ServiceLifecycleEnvironment,
            error,
        )),
    }
}

fn parse_socket_addr(_name: &'static str, raw: &str) -> RuntimeResult<SocketAddr> {
    raw.parse::<SocketAddr>().map_err(|error| {
        RuntimeError::configuration_failure(crate::RuntimeOperation::ServiceLifecycleProbeAddress, error)
    })
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
        RuntimeError::configuration_failure(crate::RuntimeOperation::ServiceLifecycleDuration, error)
    })?;
    if !(minimum_seconds..=maximum_seconds).contains(&seconds) {
        return Err(RuntimeError::configuration(
            crate::RuntimeOperation::ServiceLifecycleDurationRange,
        ));
    }
    Ok(Duration::from_secs(seconds))
}

#[derive(Debug)]
struct ServiceLifecycleInner {
    config: ServiceLifecycleConfig,
    state: AtomicU8,
    maintenance_readiness_suspended: AtomicBool,
    dependencies_ready: AtomicBool,
    started_at: Instant,
    last_progress_millis: AtomicU64,
    shutdown_request: Mutex<Option<ShutdownRequest>>,
    shutdown_tx: watch::Sender<Option<ShutdownRequest>>,
    started: AtomicBool,
    lifecycle_tasks: Mutex<Option<TaskGroup>>,
    probe_local_addr: Mutex<Option<SocketAddr>>,
    observer: Mutex<Option<Arc<dyn ServiceLifecycleObserver>>>,
}

/// One committed process state change. `from = None` is the initial Starting event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServiceLifecycleTransition {
    /// Previous committed state, absent only for initial observer attachment.
    pub from: Option<ServiceLifecycleState>,
    /// State committed by this transition.
    pub to: ServiceLifecycleState,
}

/// Observes committed state changes without owning the lifecycle.
///
/// Callbacks run synchronously outside lifecycle locks. They must be short,
/// nonblocking and panic-free, and must not flush exporters or perform I/O.
/// Concurrent callers may deliver callbacks out of order; each event carries
/// the actual atomic transition and is delivered once, without polling.
pub trait ServiceLifecycleObserver: std::fmt::Debug + Send + Sync {
    /// Records a committed transition without waiting for external work.
    fn on_transition(&self, transition: ServiceLifecycleTransition);
}

struct LifecycleNotification {
    observer: Arc<dyn ServiceLifecycleObserver>,
    transition: ServiceLifecycleTransition,
}

impl LifecycleNotification {
    fn deliver(self) {
        self.observer.on_transition(self.transition);
    }
}

struct ServiceLifecycleStartAttempt<'a> {
    inner: &'a ServiceLifecycleInner,
    cancellation: Option<tokio_util::sync::CancellationToken>,
    committed: bool,
}

impl<'a> ServiceLifecycleStartAttempt<'a> {
    fn new(inner: &'a ServiceLifecycleInner) -> Self {
        Self {
            inner,
            cancellation: None,
            committed: false,
        }
    }

    fn own(&mut self, cancellation: tokio_util::sync::CancellationToken) {
        self.cancellation = Some(cancellation);
    }

    fn commit(mut self) {
        self.committed = true;
    }
}

impl Drop for ServiceLifecycleStartAttempt<'_> {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        if let Some(cancellation) = self.cancellation.take() {
            cancellation.cancel();
        }
        self.inner.probe_local_addr.lock().take();
        self.inner.started.store(false, Ordering::Release);
    }
}

/// Shared process lifecycle and health-probe owner.
#[derive(Debug, Clone)]
pub struct ServiceLifecycle {
    inner: Arc<ServiceLifecycleInner>,
}

impl ServiceLifecycle {
    /// Creates a new `ServiceLifecycle`.
    pub fn new(config: ServiceLifecycleConfig) -> Self {
        let (shutdown_tx, _shutdown_rx) = watch::channel(None);
        Self {
            inner: Arc::new(ServiceLifecycleInner {
                config,
                state: AtomicU8::new(STATE_STARTING),
                maintenance_readiness_suspended: AtomicBool::new(false),
                dependencies_ready: AtomicBool::new(true),
                started_at: Instant::now(),
                last_progress_millis: AtomicU64::new(0),
                shutdown_request: Mutex::new(None),
                shutdown_tx,
                started: AtomicBool::new(false),
                lifecycle_tasks: Mutex::new(None),
                probe_local_addr: Mutex::new(None),
                observer: Mutex::new(None),
            }),
        }
    }

    /// Creates a value from env.
    pub fn from_env(service_name: impl Into<Arc<str>>) -> RuntimeResult<Self> {
        Ok(Self::new(ServiceLifecycleConfig::from_env(service_name)?))
    }

    /// Returns the config.
    pub fn config(&self) -> &ServiceLifecycleConfig {
        &self.inner.config
    }

    /// Binds one observer before the first state transition and emits Starting.
    ///
    /// # Errors
    ///
    /// Returns an error if an observer is already installed or the lifecycle
    /// has left Starting. Installation is serialized with state transitions.
    pub fn set_observer(&self, observer: Arc<dyn ServiceLifecycleObserver>) -> RuntimeResult<()> {
        let mut slot = self.inner.observer.lock();
        if slot.is_some() || self.state() != ServiceLifecycleState::Starting {
            return Err(RuntimeError::context_unavailable(
                crate::RuntimeOperation::StartServiceLifecycle,
            ));
        }
        *slot = Some(Arc::clone(&observer));
        drop(slot);
        observer.on_transition(ServiceLifecycleTransition {
            from: None,
            to: ServiceLifecycleState::Starting,
        });
        Ok(())
    }

    fn transition(&self, target: u8, mut allowed: impl FnMut(u8) -> bool) -> Result<Option<LifecycleNotification>, u8> {
        let observer = self.inner.observer.lock();
        self.inner
            .state
            .try_update(Ordering::AcqRel, Ordering::Acquire, |state| {
                (state != target && allowed(state)).then_some(target)
            })
            .map(|from| {
                observer.as_ref().map(|observer| LifecycleNotification {
                    observer: Arc::clone(observer),
                    transition: ServiceLifecycleTransition {
                        from: Some(ServiceLifecycleState::from_u8(from)),
                        to: ServiceLifecycleState::from_u8(target),
                    },
                })
            })
    }

    /// Starts the progress heartbeat and optional HTTP probe server under a dedicated
    /// component task group of `service_context`.
    ///
    /// # Errors
    ///
    /// Returns an error when called twice, when the listener cannot bind, or when either
    /// owned service task cannot be registered. A failed start cancels and awaits every
    /// task registered by that attempt before making the lifecycle retryable.
    pub async fn start(&self, service_context: &ChildServiceContext) -> RuntimeResult<()> {
        if self.inner.started.swap(true, Ordering::AcqRel) {
            return Err(RuntimeError::internal_failure(
                crate::RuntimeOperation::StartServiceLifecycle,
            ));
        }

        let mut start_attempt = ServiceLifecycleStartAttempt::new(&self.inner);
        let lifecycle_context = service_context.component("service-lifecycle");
        let lifecycle_tasks = lifecycle_context.task_group().clone();
        let lifecycle_state = lifecycle_tasks.lifecycle_state();
        if lifecycle_state != crate::TaskGroupLifecycleState::Open {
            return Err(lifecycle_state.admission_error(crate::RuntimeOperation::ServiceLifecycleTaskGroup));
        }
        start_attempt.own(lifecycle_tasks.cancellation_token());

        let probe = match self.bind_probe_listener().await {
            Ok(probe) => probe,
            Err(error) => return self.rollback_failed_start(lifecycle_tasks, error).await,
        };

        self.record_progress();
        let heartbeat = self.clone();
        let heartbeat_cancellation = lifecycle_tasks.cancellation_token();
        if let Err(error) = lifecycle_tasks.spawn_service("service-lifecycle.progress", async move {
            let mut interval = tokio::time::interval(PROGRESS_INTERVAL);
            loop {
                tokio::select! {
                    _ = heartbeat_cancellation.cancelled() => break,
                    _ = interval.tick() => heartbeat.record_progress(),
                }
            }
        }) {
            return self.rollback_failed_start(lifecycle_tasks, error).await;
        }

        if let Some((listener, local_addr)) = probe {
            let lifecycle = self.clone();
            let connection_tasks = lifecycle_tasks.clone();
            if let Err(error) = lifecycle_tasks.spawn_service("service-lifecycle.probe-server", async move {
                lifecycle.serve_probe_requests(listener, connection_tasks).await;
            }) {
                return self.rollback_failed_start(lifecycle_tasks, error).await;
            }
            *self.inner.probe_local_addr.lock() = Some(local_addr);
            tracing::info!(
                service = %self.inner.config.service_name,
                bind = %local_addr,
                "service lifecycle probe server listening"
            );
        }

        *self.inner.lifecycle_tasks.lock() = Some(lifecycle_tasks);
        start_attempt.commit();
        Ok(())
    }

    async fn rollback_failed_start(&self, lifecycle_tasks: TaskGroup, error: RuntimeError) -> RuntimeResult<()> {
        let deadline = ShutdownDeadline::after(self.inner.config.shutdown_timeout);
        let report = lifecycle_tasks.shutdown_until(deadline).await;
        report.log_if_unhealthy();
        Err(error)
    }

    /// Returns the state.
    pub fn state(&self) -> ServiceLifecycleState {
        ServiceLifecycleState::from_u8(self.inner.state.load(Ordering::Acquire))
    }

    /// Returns the probe local addr.
    pub fn probe_local_addr(&self) -> Option<SocketAddr> {
        *self.inner.probe_local_addr.lock()
    }

    /// Returns whether ready.
    pub fn is_ready(&self) -> bool {
        self.state() == ServiceLifecycleState::Ready
            && !self.inner.maintenance_readiness_suspended.load(Ordering::Acquire)
            && self.inner.dependencies_ready.load(Ordering::Acquire)
    }

    /// Publishes the aggregate dependency decision without changing lifecycle state.
    /// A healthy dependency cannot undo maintenance, drain, failure, or stop.
    pub fn set_dependency_readiness(&self, readiness: DependencyReadiness) {
        self.inner
            .dependencies_ready
            .store(matches!(readiness, DependencyReadiness::Ready), Ordering::Release);
    }

    /// Returns the latest aggregate dependency decision independently of lifecycle state.
    pub fn dependency_readiness(&self) -> DependencyReadiness {
        if self.inner.dependencies_ready.load(Ordering::Acquire) {
            DependencyReadiness::Ready
        } else {
            DependencyReadiness::Degraded
        }
    }

    /// Applies the handling policy for one critical task failure.
    ///
    /// Dependency readiness is revoked first, because a process whose critical
    /// work failed must stop advertising itself as ready even when it keeps
    /// making progress. The remaining effects are the caller's policy: this
    /// method never decides on its own whether the process fails or asks for an
    /// ordered shutdown.
    pub fn handle_critical_failure(&self, failure: &CriticalFailure, recovery: CriticalFailureRecovery) {
        tracing::warn!(
            kind = failure.kind().as_str(),
            task_kind = ?failure.task_kind(),
            sequence = failure.sequence(),
            "critical task failure"
        );
        self.set_dependency_readiness(DependencyReadiness::Degraded);
        match recovery {
            CriticalFailureRecovery::RevokeReadiness => {}
            CriticalFailureRecovery::FailService => self.mark_failed(),
            CriticalFailureRecovery::FailAndRequestShutdown => {
                self.mark_failed();
                self.request_shutdown(ShutdownReason::Internal);
            }
        }
    }

    /// Spawns a monitor that applies `recovery` when a critical failure is pending.
    ///
    /// `owner` must be an owner outside the group of the monitored tasks, because
    /// a poisoned group can neither spawn nor run its own monitor. The monitor
    /// takes each pending failure before handling it, so repeated failures are
    /// handled in sequence rather than replayed.
    ///
    /// # Errors
    ///
    /// Returns an error when `owner` is shutting down or closed.
    pub fn spawn_critical_failure_monitor(
        &self,
        owner: &TaskSpawner,
        failures: &CriticalFailureState,
        recovery: CriticalFailureRecovery,
    ) -> RuntimeResult<TaskId> {
        let lifecycle = self.clone();
        failures.spawn_monitor(owner, "service-lifecycle.critical-failures", move |failure| {
            lifecycle.handle_critical_failure(&failure, recovery);
        })
    }

    /// Returns whether live.
    pub fn is_live(&self) -> bool {
        if matches!(
            self.state(),
            ServiceLifecycleState::Stopped | ServiceLifecycleState::Failed
        ) {
            return false;
        }
        self.progress_age() <= self.inner.config.liveness_stale_after
    }

    /// Records progress.
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
        match self.transition(STATE_READY, |state| state == STATE_STARTING) {
            Ok(notification) => {
                self.record_progress();
                if let Some(notification) = notification {
                    notification.deliver();
                }
                Ok(())
            }
            Err(STATE_READY) => Ok(()),
            Err(_state) => Err(RuntimeError::internal_failure(
                crate::RuntimeOperation::MarkServiceReady,
            )),
        }
    }

    /// Temporarily removes this process from readiness during a reversible
    /// maintenance operation.
    ///
    /// This does not alter the process lifecycle state. A real shutdown request
    /// remains irreversible and prevents a later readiness restore.
    ///
    /// # Errors
    ///
    /// Returns an error unless the process is ready and shutdown has not begun.
    pub fn suspend_readiness_for_maintenance(&self) -> RuntimeResult<()> {
        let shutdown_request = self.inner.shutdown_request.lock();
        if shutdown_request.is_some() || self.state() != ServiceLifecycleState::Ready {
            return Err(RuntimeError::internal_failure(
                crate::RuntimeOperation::SuspendServiceReadiness,
            ));
        }
        self.inner
            .maintenance_readiness_suspended
            .store(true, Ordering::Release);
        drop(shutdown_request);
        self.record_progress();
        Ok(())
    }

    /// Restores readiness after a reversible maintenance operation.
    ///
    /// # Errors
    ///
    /// Returns an error when shutdown, failure, or stop has begun.
    pub fn restore_readiness_after_maintenance(&self) -> RuntimeResult<()> {
        let shutdown_request = self.inner.shutdown_request.lock();
        if shutdown_request.is_some() || self.state() != ServiceLifecycleState::Ready {
            return Err(RuntimeError::internal_failure(
                crate::RuntimeOperation::RestoreServiceReadiness,
            ));
        }
        self.inner
            .maintenance_readiness_suspended
            .store(false, Ordering::Release);
        drop(shutdown_request);
        self.record_progress();
        Ok(())
    }

    /// Executes mark failed.
    pub fn mark_failed(&self) {
        let notification = self.transition(STATE_FAILED, |_| true).ok().flatten();
        self.record_progress();
        if let Some(notification) = notification {
            notification.deliver();
        }
    }

    /// Executes mark stopped.
    pub fn mark_stopped(&self) {
        let notification = self
            .transition(STATE_STOPPED, |state| state != STATE_FAILED)
            .ok()
            .flatten();
        self.record_progress();
        if let Some(notification) = notification {
            notification.deliver();
        }
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
        // Failure and stop can race this request without taking its mutex.
        // Recheck the terminal states on every compare-and-exchange attempt.
        let notification = self
            .transition(STATE_DRAINING, |state| state != STATE_FAILED && state != STATE_STOPPED)
            .ok()
            .flatten();
        self.record_progress();
        self.inner.shutdown_tx.send_replace(Some(first));
        drop(request);
        if let Some(notification) = notification {
            notification.deliver();
        }
        first
    }

    /// Shuts down request.
    pub fn shutdown_request(&self) -> Option<ShutdownRequest> {
        *self.inner.shutdown_request.lock()
    }

    /// Returns the wait for shutdown.
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
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::*;
    use crate::RuntimeContext;

    #[derive(Debug)]
    struct RecordingObserver {
        lifecycle: std::sync::Weak<ServiceLifecycleInner>,
        events: Mutex<Vec<ServiceLifecycleTransition>>,
    }

    impl ServiceLifecycleObserver for RecordingObserver {
        fn on_transition(&self, transition: ServiceLifecycleTransition) {
            let inner = self.lifecycle.upgrade().unwrap();
            assert!(
                inner.shutdown_request.try_lock().is_some(),
                "shutdown lock held by callback"
            );
            assert!(inner.observer.try_lock().is_some(), "observer lock held by callback");
            self.events.lock().push(transition);
        }
    }

    #[test]
    fn observers_receive_only_committed_transitions_outside_lifecycle_locks() {
        let lifecycle = ServiceLifecycle::new(config(None));
        let observer = Arc::new(RecordingObserver {
            lifecycle: Arc::downgrade(&lifecycle.inner),
            events: Mutex::new(Vec::new()),
        });
        lifecycle.set_observer(observer.clone()).unwrap();
        assert!(lifecycle.set_observer(observer.clone()).is_err());
        lifecycle.mark_ready().unwrap();
        lifecycle.mark_ready().unwrap();
        let first = lifecycle.request_shutdown(ShutdownReason::Internal);
        let repeated = lifecycle.request_shutdown(ShutdownReason::Signal);
        assert_eq!(first.deadline, repeated.deadline);
        lifecycle.mark_stopped();
        lifecycle.mark_stopped();
        let states: Vec<_> = observer.events.lock().iter().map(|event| event.to).collect();
        assert_eq!(
            states,
            [
                ServiceLifecycleState::Starting,
                ServiceLifecycleState::Ready,
                ServiceLifecycleState::Draining,
                ServiceLifecycleState::Stopped,
            ]
        );
        assert_eq!(observer.events.lock()[2].from, Some(ServiceLifecycleState::Ready));

        let failed = ServiceLifecycle::new(config(None));
        let observer = Arc::new(RecordingObserver {
            lifecycle: Arc::downgrade(&failed.inner),
            events: Mutex::new(Vec::new()),
        });
        failed.set_observer(observer.clone()).unwrap();
        failed.mark_failed();
        failed.mark_failed();
        failed.request_shutdown(ShutdownReason::Internal);
        failed.mark_stopped();
        assert!(failed.mark_ready().is_err());
        assert_eq!(observer.events.lock().len(), 2);
        assert_eq!(observer.events.lock()[1].to, ServiceLifecycleState::Failed);
    }

    fn config(probe_bind_addr: Option<SocketAddr>) -> ServiceLifecycleConfig {
        ServiceLifecycleConfig {
            service_name: Arc::from("test-service"),
            probe_bind_addr,
            shutdown_timeout: Duration::from_secs(45),
            liveness_stale_after: Duration::from_secs(30),
            drain_request_methods: DrainRequestMethods::PostOnly,
        }
    }

    async fn request(addr: SocketAddr, path: &str) -> String {
        request_with_method(addr, "GET", path).await
    }

    async fn request_with_method(addr: SocketAddr, method: &str, path: &str) -> String {
        let mut stream = TcpStream::connect(addr).await.expect("connect lifecycle probe");
        stream
            .write_all(format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n").as_bytes())
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
    fn maintenance_readiness_is_reversible_until_shutdown_begins() {
        let lifecycle = ServiceLifecycle::new(config(None));
        lifecycle.mark_ready().unwrap();

        lifecycle.suspend_readiness_for_maintenance().unwrap();
        assert_eq!(lifecycle.state(), ServiceLifecycleState::Ready);
        assert!(!lifecycle.is_ready());

        lifecycle.restore_readiness_after_maintenance().unwrap();
        assert!(lifecycle.is_ready());

        lifecycle.suspend_readiness_for_maintenance().unwrap();
        lifecycle.request_shutdown(ShutdownReason::Internal);
        assert!(lifecycle.restore_readiness_after_maintenance().is_err());
        assert!(!lifecycle.is_ready());
        assert_eq!(lifecycle.state(), ServiceLifecycleState::Draining);
    }

    #[test]
    fn dependency_recovery_cannot_restore_maintenance_or_shutdown_readiness() {
        let lifecycle = ServiceLifecycle::new(config(None));
        lifecycle.set_dependency_readiness(DependencyReadiness::Degraded);
        lifecycle.mark_ready().unwrap();
        assert!(!lifecycle.is_ready());
        assert!(lifecycle.is_live());
        lifecycle.set_dependency_readiness(DependencyReadiness::Ready);
        assert!(lifecycle.is_ready());
        lifecycle.suspend_readiness_for_maintenance().unwrap();
        lifecycle.set_dependency_readiness(DependencyReadiness::Degraded);
        lifecycle.set_dependency_readiness(DependencyReadiness::Ready);
        assert!(!lifecycle.is_ready());
        lifecycle.restore_readiness_after_maintenance().unwrap();
        assert!(lifecycle.is_ready());
        lifecycle.request_shutdown(ShutdownReason::Internal);
        lifecycle.set_dependency_readiness(DependencyReadiness::Ready);
        assert!(!lifecycle.is_ready());
    }

    #[test]
    fn failed_is_not_overwritten_by_normal_stop_completion() {
        let lifecycle = ServiceLifecycle::new(config(None));
        lifecycle.mark_failed();
        lifecycle.mark_stopped();

        assert_eq!(lifecycle.state(), ServiceLifecycleState::Failed);
    }

    #[test]
    fn concurrent_shutdown_cannot_overwrite_a_terminal_state() {
        for fail in [false, true] {
            for _ in 0..64 {
                let lifecycle = ServiceLifecycle::new(config(None));
                lifecycle.mark_ready().unwrap();
                let barrier = std::sync::Barrier::new(2);
                std::thread::scope(|scope| {
                    scope.spawn(|| {
                        barrier.wait();
                        lifecycle.request_shutdown(ShutdownReason::Internal);
                    });
                    barrier.wait();
                    if fail {
                        lifecycle.mark_failed();
                    } else {
                        lifecycle.mark_stopped();
                    }
                });
                assert_eq!(
                    lifecycle.state(),
                    if fail {
                        ServiceLifecycleState::Failed
                    } else {
                        ServiceLifecycleState::Stopped
                    }
                );
                let first = lifecycle.shutdown_request().unwrap();
                assert_eq!(lifecycle.request_shutdown(ShutdownReason::Signal), first);
            }
        }
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
        assert!(request_with_method(addr, "POST", "/drainz")
            .await
            .starts_with("HTTP/1.1 200"));
        assert!(!lifecycle.is_ready());
        assert_eq!(lifecycle.wait_for_shutdown().await.reason, ShutdownReason::PreStop);

        let report = context.shutdown_tasks(Duration::from_secs(1)).await;
        assert!(report.is_healthy(), "{}", report.to_json());
    }

    #[tokio::test]
    async fn failed_probe_bind_rolls_back_owned_tasks_and_allows_retry() {
        let context = RuntimeContext::from_current("service-lifecycle-retry-test");
        let service = context.service_context("service-lifecycle-retry-test");
        let occupied_listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .expect("reserve probe address");
        let probe_addr = occupied_listener.local_addr().expect("inspect reserved address");
        let lifecycle = ServiceLifecycle::new(config(Some(probe_addr)));

        let error = lifecycle
            .start(&service)
            .await
            .expect_err("occupied probe address must fail startup");
        assert_eq!(error.operation(), crate::RuntimeOperation::BindServiceHealthProbe);
        assert!(error.source().is_some());
        assert_eq!(lifecycle.state(), ServiceLifecycleState::Starting);
        assert_eq!(lifecycle.probe_local_addr(), None);
        assert!(!lifecycle.inner.started.load(Ordering::Acquire));
        assert_eq!(service.task_group().task_count(), 0);
        assert_eq!(service.task_group().component_count(), 0);

        drop(occupied_listener);
        lifecycle
            .start(&service)
            .await
            .expect("failed startup must be retryable");
        assert_eq!(lifecycle.probe_local_addr(), Some(probe_addr));
        assert!(lifecycle.inner.started.load(Ordering::Acquire));
        assert_eq!(
            lifecycle
                .inner
                .lifecycle_tasks
                .lock()
                .as_ref()
                .expect("successful startup owns a task group")
                .task_count(),
            2
        );
        assert_eq!(service.task_group().component_count(), 1);
        assert!(request(probe_addr, "/livez").await.starts_with("HTTP/1.1 200"));

        let report = context.shutdown_tasks(Duration::from_secs(1)).await;
        assert!(report.is_healthy(), "{}", report.to_json());
        let lifecycle_tasks = lifecycle.inner.lifecycle_tasks.lock();
        let lifecycle_tasks = lifecycle_tasks
            .as_ref()
            .expect("successful startup retains task ownership");
        assert_eq!(lifecycle_tasks.task_count(), 0);
        assert_eq!(
            lifecycle_tasks.lifecycle_state(),
            crate::TaskGroupLifecycleState::ShutdownCompleted
        );
    }

    #[test]
    fn cancelled_start_attempt_resets_admission_and_cancels_owned_tasks() {
        let lifecycle = ServiceLifecycle::new(config(None));
        lifecycle.inner.started.store(true, Ordering::Release);
        *lifecycle.inner.probe_local_addr.lock() = Some(SocketAddr::from(([127, 0, 0, 1], 8088)));
        let cancellation = tokio_util::sync::CancellationToken::new();

        {
            let mut attempt = ServiceLifecycleStartAttempt::new(&lifecycle.inner);
            attempt.own(cancellation.clone());
        }

        assert!(cancellation.is_cancelled());
        assert!(!lifecycle.inner.started.load(Ordering::Acquire));
        assert_eq!(lifecycle.probe_local_addr(), None);
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
