// Copyright 2023 The RocketMQ Rust Authors
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

//! Transactional task ownership for the Tokio transport client.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use rocketmq_error::RocketMQResult;
use rocketmq_runtime::ShutdownDeadline;
use rocketmq_runtime::ShutdownReport;
use rocketmq_runtime::TaskGroup;
use rocketmq_runtime::TaskGroupLifecycleState;
use rocketmq_runtime::TaskId;
use tokio::sync::watch;
use tokio::time;
use tokio_util::sync::CancellationToken;
use tracing::debug;
use tracing::info;
use tracing::warn;

#[cfg(test)]
use std::sync::atomic::AtomicBool;
#[cfg(test)]
use std::sync::atomic::Ordering;
#[cfg(test)]
use tokio::sync::Notify;

use super::ClientShutdownReport;
use super::ClientStartReport;
use super::ConnectionShutdownReport;
use super::TransportClient;
use crate::error_helpers::remote_error;

const START_ROLLBACK_TIMEOUT: Duration = Duration::from_secs(1);

pub(super) struct ClientLifecycle {
    generation: u64,
    worker_epoch: u64,
    worker_admission_open: bool,
    phase: LifecyclePhase,
    unpublished_shutdown: Option<Arc<ShutdownFlight>>,
    worker_task_group: Option<TaskGroup>,
    #[cfg(test)]
    background_spawn_failure_after: Option<usize>,
    #[cfg(test)]
    start_between_background_spawns: Option<LifecycleTestBarrier>,
    #[cfg(test)]
    shutdown_owner_started: Option<LifecycleTestBarrier>,
    #[cfg(test)]
    shutdown_participant_joined: Option<LifecycleTestBarrier>,
    #[cfg(test)]
    shutdown_phase_finished_before_report: Option<LifecycleTestBarrier>,
    #[cfg(test)]
    shutdown_connections_taken: Option<LifecycleTestBarrier>,
}

enum LifecyclePhase {
    Stopped,
    Starting(ClientGeneration),
    Running(ClientGeneration),
    Stopping(StoppingState),
}

#[derive(Clone)]
struct ClientGeneration {
    number: u64,
    cancellation: CancellationToken,
    background_task_group: TaskGroup,
}

struct StoppingState {
    flight: Arc<ShutdownFlight>,
}

#[derive(Clone)]
struct ShutdownTargets {
    cancellation: Option<CancellationToken>,
    background_task_group: Option<TaskGroup>,
    worker_task_group: Option<TaskGroup>,
}

struct ShutdownFlight {
    targets: ShutdownTargets,
    report_tx: watch::Sender<Option<ClientShutdownReport>>,
}

enum ShutdownAcquire {
    Owner(Arc<ShutdownFlight>),
    Participant(Arc<ShutdownFlight>),
}

#[derive(Clone)]
pub(super) struct ConnectionCommitFence {
    worker_epoch: u64,
    worker_cancellation: CancellationToken,
}

#[derive(Clone)]
pub(super) struct WorkerTaskOwner {
    task_group: TaskGroup,
    commit_fence: ConnectionCommitFence,
}

impl WorkerTaskOwner {
    pub(super) fn commit_fence(&self) -> ConnectionCommitFence {
        self.commit_fence.clone()
    }
}

#[cfg(test)]
#[derive(Clone)]
pub(super) struct LifecycleTestBarrier {
    entered: Arc<AtomicBool>,
    entered_signal: Arc<Notify>,
    release: Arc<Notify>,
}

#[cfg(test)]
impl LifecycleTestBarrier {
    pub(super) fn new() -> Self {
        Self {
            entered: Arc::new(AtomicBool::new(false)),
            entered_signal: Arc::new(Notify::new()),
            release: Arc::new(Notify::new()),
        }
    }

    pub(super) async fn wait_until_entered(&self) {
        while !self.entered.load(Ordering::Acquire) {
            self.entered_signal.notified().await;
        }
    }

    pub(super) fn enter(&self) {
        if !self.entered.swap(true, Ordering::AcqRel) {
            self.entered_signal.notify_waiters();
        }
    }

    pub(super) async fn pause(&self) {
        self.enter();
        self.release.notified().await;
    }

    pub(super) fn release(&self) {
        self.release.notify_one();
    }
}

impl ClientLifecycle {
    pub(super) fn new() -> Self {
        Self {
            generation: 0,
            worker_epoch: 0,
            // Keep the historic lazy pre-start worker behavior until the
            // first explicit shutdown. Afterwards, only a successful `start`
            // may admit connection work again.
            worker_admission_open: true,
            phase: LifecyclePhase::Stopped,
            unpublished_shutdown: None,
            worker_task_group: None,
            #[cfg(test)]
            background_spawn_failure_after: None,
            #[cfg(test)]
            start_between_background_spawns: None,
            #[cfg(test)]
            shutdown_owner_started: None,
            #[cfg(test)]
            shutdown_participant_joined: None,
            #[cfg(test)]
            shutdown_phase_finished_before_report: None,
            #[cfg(test)]
            shutdown_connections_taken: None,
        }
    }

    fn begin_start(&mut self, task_group: TaskGroup) -> RocketMQResult<ClientGeneration> {
        self.generation = self
            .generation
            .checked_add(1)
            .ok_or_else(|| remote_error("transport client lifecycle generation overflow"))?;
        let generation = ClientGeneration {
            number: self.generation,
            cancellation: task_group.cancellation_token(),
            background_task_group: task_group,
        };
        self.phase = LifecyclePhase::Starting(generation.clone());
        Ok(generation)
    }

    fn is_stopped(&self) -> bool {
        matches!(self.phase, LifecyclePhase::Stopped) && self.unpublished_shutdown.is_none()
    }

    fn promote_running(&mut self, generation: u64) {
        let LifecyclePhase::Starting(starting) = &self.phase else {
            return;
        };
        if starting.number == generation {
            self.phase = LifecyclePhase::Running(starting.clone());
            self.worker_admission_open = true;
        }
    }

    fn restore_stopped_after_failed_start(&mut self, generation: u64) {
        if matches!(&self.phase, LifecyclePhase::Starting(starting) if starting.number == generation) {
            self.phase = LifecyclePhase::Stopped;
        }
    }

    fn acquire_shutdown(&mut self) -> ShutdownAcquire {
        self.worker_admission_open = false;
        self.worker_epoch = self.worker_epoch.saturating_add(1);
        if let Some(flight) = self.unpublished_shutdown.clone() {
            #[cfg(test)]
            if let Some(hook) = &self.shutdown_participant_joined {
                hook.enter();
            }
            return ShutdownAcquire::Participant(flight);
        }
        let worker_task_group = self.worker_task_group.clone();
        let phase = std::mem::replace(&mut self.phase, LifecyclePhase::Stopped);
        let targets = match phase {
            LifecyclePhase::Stopping(stopping) => {
                #[cfg(test)]
                if let Some(hook) = &self.shutdown_participant_joined {
                    hook.enter();
                }
                let flight = Arc::clone(&stopping.flight);
                self.phase = LifecyclePhase::Stopping(stopping);
                return ShutdownAcquire::Participant(flight);
            }
            LifecyclePhase::Stopped => ShutdownTargets {
                cancellation: None,
                background_task_group: None,
                worker_task_group,
            },
            LifecyclePhase::Starting(generation) | LifecyclePhase::Running(generation) => ShutdownTargets {
                cancellation: Some(generation.cancellation.clone()),
                background_task_group: Some(generation.background_task_group.clone()),
                worker_task_group,
            },
        };
        let (report_tx, _) = watch::channel(None);
        let flight = Arc::new(ShutdownFlight { targets, report_tx });
        self.phase = LifecyclePhase::Stopping(StoppingState {
            flight: Arc::clone(&flight),
        });
        ShutdownAcquire::Owner(flight)
    }

    fn finish_shutdown_owner(&mut self, flight: &Arc<ShutdownFlight>) -> bool {
        match &self.phase {
            LifecyclePhase::Stopping(stopping) if Arc::ptr_eq(&stopping.flight, flight) => {
                self.phase = LifecyclePhase::Stopped;
                self.worker_task_group = None;
                self.unpublished_shutdown = Some(Arc::clone(flight));
                true
            }
            LifecyclePhase::Stopped
                if self
                    .unpublished_shutdown
                    .as_ref()
                    .is_some_and(|pending| Arc::ptr_eq(pending, flight)) =>
            {
                true
            }
            _ => false,
        }
    }

    fn publish_shutdown_owner_report(&mut self, flight: &Arc<ShutdownFlight>, report: ClientShutdownReport) -> bool {
        if !self
            .unpublished_shutdown
            .as_ref()
            .is_some_and(|pending| Arc::ptr_eq(pending, flight))
        {
            return false;
        }
        flight.complete(report);
        self.unpublished_shutdown = None;
        true
    }

    fn is_stopping(&self) -> bool {
        matches!(self.phase, LifecyclePhase::Stopping(_)) || self.unpublished_shutdown.is_some()
    }

    fn worker_task_owner(&mut self, parent: &TaskGroup) -> Option<WorkerTaskOwner> {
        if self.is_stopping() {
            return None;
        }
        let task_group = self.get_or_create_worker_task_group(parent)?;
        Some(WorkerTaskOwner {
            task_group: task_group.clone(),
            commit_fence: ConnectionCommitFence {
                worker_epoch: self.worker_epoch,
                worker_cancellation: task_group.cancellation_token(),
            },
        })
    }

    pub(super) fn matches_connection_commit_fence(&self, fence: &ConnectionCommitFence) -> bool {
        self.worker_admission_open
            && !self.is_stopping()
            && self.worker_epoch == fence.worker_epoch
            && !fence.worker_cancellation.is_cancelled()
    }

    #[cfg(test)]
    fn worker_task_group(&self) -> Option<TaskGroup> {
        self.worker_task_group.clone()
    }

    fn get_or_create_worker_task_group(&mut self, parent: &TaskGroup) -> Option<TaskGroup> {
        if matches!(self.phase, LifecyclePhase::Stopping(_)) {
            return None;
        }

        if !self.worker_admission_open {
            return None;
        }

        if let Some(task_group) = self
            .worker_task_group
            .as_ref()
            .filter(|task_group| task_group.lifecycle_state() == TaskGroupLifecycleState::Open)
        {
            return Some(task_group.clone());
        }

        if matches!(self.phase, LifecyclePhase::Starting(_)) {
            return None;
        }

        match parent.try_child("rocketmq-transport.client.workers") {
            Ok(task_group) => {
                self.worker_task_group = Some(task_group.clone());
                Some(task_group)
            }
            Err(error) => {
                warn!(?error, "failed to create RemotingClient worker task group");
                None
            }
        }
    }

    #[cfg(test)]
    fn fail_background_spawn_after(&mut self, successful_spawns: usize) {
        self.background_spawn_failure_after = Some(successful_spawns);
    }

    #[cfg(test)]
    fn should_fail_background_spawn(&mut self) -> bool {
        let Some(remaining) = self.background_spawn_failure_after.as_mut() else {
            return false;
        };
        if *remaining == 0 {
            self.background_spawn_failure_after = None;
            return true;
        }
        *remaining -= 1;
        false
    }

    #[cfg(test)]
    fn start_between_background_spawns(&self) -> Option<LifecycleTestBarrier> {
        self.start_between_background_spawns.clone()
    }

    #[cfg(test)]
    fn shutdown_owner_started(&self) -> Option<LifecycleTestBarrier> {
        self.shutdown_owner_started.clone()
    }

    #[cfg(test)]
    fn shutdown_phase_finished_before_report(&self) -> Option<LifecycleTestBarrier> {
        self.shutdown_phase_finished_before_report.clone()
    }

    #[cfg(test)]
    fn shutdown_connections_taken(&self) -> Option<LifecycleTestBarrier> {
        self.shutdown_connections_taken.clone()
    }

    #[cfg(not(test))]
    fn should_fail_background_spawn(&mut self) -> bool {
        false
    }

    #[cfg(test)]
    fn phase_name(&self) -> &'static str {
        match self.phase {
            LifecyclePhase::Stopped => "stopped",
            LifecyclePhase::Starting(_) => "starting",
            LifecyclePhase::Running(_) => "running",
            LifecyclePhase::Stopping(_) => "stopping",
        }
    }
}

impl ShutdownFlight {
    fn complete(&self, report: ClientShutdownReport) {
        if self.report_tx.borrow().is_none() {
            self.report_tx.send_replace(Some(report));
        }
    }

    async fn wait_report(&self) -> ClientShutdownReport {
        let mut receiver = self.report_tx.subscribe();
        loop {
            if let Some(report) = receiver.borrow().clone() {
                return report;
            }
            // The sender is owned by this flight until the lifecycle owner
            // publishes one terminal report, so `changed` cannot lose a
            // publication that races this receiver setup.
            let _ = receiver.changed().await;
        }
    }
}

struct ShutdownOwnerGuard<'client, PR>
where
    PR: Send + Sync + Clone + 'static,
{
    client: &'client TransportClient<PR>,
    flight: Arc<ShutdownFlight>,
    armed: bool,
}

impl<'client, PR> ShutdownOwnerGuard<'client, PR>
where
    PR: Send + Sync + Clone + 'static,
{
    fn new(client: &'client TransportClient<PR>, flight: Arc<ShutdownFlight>) -> Self {
        Self {
            client,
            flight,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl<PR> Drop for ShutdownOwnerGuard<'_, PR>
where
    PR: Send + Sync + Clone + 'static,
{
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        // Future cancellation cannot await. Fall back to the same immediate
        // ownership release used by `shutdown_now`, without spawning work or
        // blocking the caller.
        let report = self.client.shutdown_targets_now(&self.flight.targets);
        self.client.finish_shutdown_owner_now(&self.flight, report);
    }
}

impl<PR: Send + Sync + Clone + 'static> TransportClient<PR> {
    pub(crate) fn spawn_worker_task<F>(&self, name: impl Into<Arc<str>>, future: F) -> Option<TaskId>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let owner = self.capture_worker_task_owner()?;
        self.spawn_worker_task_with_owner(&owner, name, future)
    }

    pub(super) fn capture_worker_task_owner(&self) -> Option<WorkerTaskOwner> {
        let mut lifecycle = self.lifecycle.lock();
        lifecycle.worker_task_owner(self.service_context.task_group())
    }

    pub(super) fn spawn_worker_task_with_owner<F>(
        &self,
        owner: &WorkerTaskOwner,
        name: impl Into<Arc<str>>,
        future: F,
    ) -> Option<TaskId>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let name = name.into();
        if !self.matches_connection_commit_fence(&owner.commit_fence) {
            return None;
        }
        #[cfg(test)]
        {
            *self.worker_task_group.lock() = Some(owner.task_group.clone());
        }
        match owner.task_group.spawn_service(name.clone(), future) {
            Ok(task_id) => Some(task_id),
            Err(error) => {
                warn!(
                    ?error,
                    task = %name,
                    "failed to spawn RemotingClient worker task"
                );
                None
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn worker_task_group(&self) -> Option<TaskGroup> {
        self.lifecycle.lock().worker_task_group()
    }

    #[cfg(test)]
    pub(crate) fn fail_background_spawn_after(&self, successful_spawns: usize) {
        self.lifecycle.lock().fail_background_spawn_after(successful_spawns);
    }

    #[cfg(test)]
    pub(crate) fn lifecycle_phase(&self) -> &'static str {
        self.lifecycle.lock().phase_name()
    }

    #[cfg(test)]
    pub(super) fn install_start_between_background_spawns_barrier(&self, barrier: LifecycleTestBarrier) {
        self.lifecycle.lock().start_between_background_spawns = Some(barrier);
    }

    #[cfg(test)]
    pub(super) fn install_shutdown_owner_started_barrier(&self, barrier: LifecycleTestBarrier) {
        self.lifecycle.lock().shutdown_owner_started = Some(barrier);
    }

    #[cfg(test)]
    pub(super) fn install_shutdown_participant_joined_barrier(&self, barrier: LifecycleTestBarrier) {
        self.lifecycle.lock().shutdown_participant_joined = Some(barrier);
    }

    #[cfg(test)]
    pub(super) fn install_shutdown_phase_finished_before_report_barrier(&self, barrier: LifecycleTestBarrier) {
        self.lifecycle.lock().shutdown_phase_finished_before_report = Some(barrier);
    }

    #[cfg(test)]
    pub(super) fn install_shutdown_connections_taken_barrier(&self, barrier: LifecycleTestBarrier) {
        self.lifecycle.lock().shutdown_connections_taken = Some(barrier);
    }

    pub(crate) fn is_stopping(&self) -> bool {
        self.lifecycle.lock().is_stopping()
    }

    pub(super) fn matches_connection_commit_fence(&self, fence: &ConnectionCommitFence) -> bool {
        self.lifecycle.lock().matches_connection_commit_fence(fence)
    }

    /// Starts maintenance tasks as one transactional lifecycle generation.
    ///
    /// A successful call publishes `Running` only after every configured task
    /// is registered. A concurrent start or shutdown does not create another
    /// generation; it returns an `already_running` report instead.
    ///
    /// # Errors
    ///
    /// Returns a typed runtime error when a lifecycle task group cannot be
    /// created or a configured background task cannot be spawned. Failed starts
    /// cancel and await only their uncommitted background generation.
    pub async fn start(self: &Arc<Self>) -> RocketMQResult<ClientStartReport> {
        let start_attempt = {
            let mut lifecycle = self.lifecycle.lock();
            if !lifecycle.is_stopped() {
                debug!("TransportClient lifecycle is already active");
                return Ok(ClientStartReport {
                    background_tasks_started: 0,
                    already_running: true,
                });
            }

            let task_group = self
                .service_context
                .task_group()
                .try_child("rocketmq-transport.client")
                .map_err(|error| remote_error(format!("failed to create client lifecycle task group: {error}")))?;
            let generation = lifecycle.begin_start(task_group.clone())?;
            let token = generation.cancellation.clone();

            let scan_result = if lifecycle.should_fail_background_spawn() {
                Err(remote_error("injected nameserver scan task spawn failure"))
            } else {
                let client = Arc::clone(self);
                let scan_token = token.clone();
                task_group
                    .spawn_service("remoting.client.namesrv-scan", async move {
                        loop {
                            tokio::select! {
                                () = scan_token.cancelled() => break,
                                () = async {
                                    client.scan_available_name_srv().await;
                                    time::sleep(Self::NAMESERVER_SCAN_INTERVAL).await;
                                } => {}
                            }
                        }
                    })
                    .map_err(|error| remote_error(format!("failed to spawn nameserver scan task: {error}")))
            };
            if let Err(error) = scan_result {
                StartAttempt::Failed { generation, error }
            } else if let Some(idle_scan_interval) = self.tokio_client_config.maintenance.idle_scan_interval {
                StartAttempt::Continue {
                    generation,
                    task_group,
                    token,
                    idle_scan_interval,
                }
            } else {
                lifecycle.promote_running(generation.number);
                #[cfg(test)]
                {
                    *self.background_task_group.lock() = Some(task_group);
                }
                StartAttempt::Started(1)
            }
        };

        let start_attempt = match start_attempt {
            StartAttempt::Continue {
                generation,
                task_group,
                token,
                idle_scan_interval,
            } => {
                #[cfg(test)]
                let start_between_background_spawns = { self.lifecycle.lock().start_between_background_spawns() };
                #[cfg(test)]
                if let Some(barrier) = start_between_background_spawns {
                    barrier.pause().await;
                }

                let mut lifecycle = self.lifecycle.lock();
                if !matches!(&lifecycle.phase, LifecyclePhase::Starting(starting) if starting.number == generation.number)
                {
                    StartAttempt::Interrupted
                } else {
                    let idle_result = if lifecycle.should_fail_background_spawn() {
                        Err(remote_error("injected idle connection scan task spawn failure"))
                    } else {
                        let client = Arc::clone(self);
                        task_group
                            .spawn_service("remoting.client.idle-scan", async move {
                                loop {
                                    tokio::select! {
                                        () = token.cancelled() => break,
                                        () = time::sleep(idle_scan_interval) => client.scan_idle_connections(),
                                    }
                                }
                            })
                            .map_err(|error| {
                                remote_error(format!("failed to spawn idle connection scan task: {error}"))
                            })
                    };
                    match idle_result {
                        Ok(_) => {
                            lifecycle.promote_running(generation.number);
                            #[cfg(test)]
                            {
                                *self.background_task_group.lock() = Some(task_group);
                            }
                            StartAttempt::Started(2)
                        }
                        Err(error) => StartAttempt::Failed { generation, error },
                    }
                }
            }
            start_attempt => start_attempt,
        };

        match start_attempt {
            StartAttempt::Started(background_tasks_started) => Ok(ClientStartReport {
                background_tasks_started,
                already_running: false,
            }),
            StartAttempt::Failed { generation, error } => {
                generation.cancellation.cancel();
                let _ = generation
                    .background_task_group
                    .shutdown_until(ShutdownDeadline::after(START_ROLLBACK_TIMEOUT))
                    .await;
                self.lifecycle
                    .lock()
                    .restore_stopped_after_failed_start(generation.number);
                Err(error)
            }
            StartAttempt::Interrupted => Ok(ClientStartReport {
                background_tasks_started: 0,
                already_running: true,
            }),
            StartAttempt::Continue { generation, .. } => {
                generation.cancellation.cancel();
                let _ = generation
                    .background_task_group
                    .shutdown_until(ShutdownDeadline::after(START_ROLLBACK_TIMEOUT))
                    .await;
                self.lifecycle
                    .lock()
                    .restore_stopped_after_failed_start(generation.number);
                Err(remote_error(
                    "transport client lifecycle start continuation was not completed",
                ))
            }
        }
    }

    /// Gracefully cancels every client task generation and closes connections by
    /// one absolute deadline.
    ///
    /// The same deadline is passed to background tasks, worker tasks, and each
    /// connection close operation. This method awaits task and connection drain
    /// attempts; the returned reports identify work that exceeded the deadline.
    pub async fn shutdown_graceful(&self, deadline: ShutdownDeadline) -> ClientShutdownReport {
        let shutdown = { self.lifecycle.lock().acquire_shutdown() };
        match shutdown {
            ShutdownAcquire::Owner(flight) => {
                let mut owner_guard = ShutdownOwnerGuard::new(self, Arc::clone(&flight));
                #[cfg(test)]
                let shutdown_owner_started = { self.lifecycle.lock().shutdown_owner_started() };
                #[cfg(test)]
                if let Some(barrier) = shutdown_owner_started {
                    barrier.pause().await;
                }
                let targets = &flight.targets;
                if let Some(cancellation) = &targets.cancellation {
                    cancellation.cancel();
                }
                // A graceful shutdown must unblock any connection-flight waiter before it
                // waits for the worker group which owns the connection attempts.
                self.connection_registry.clear_flights();

                let background = match &targets.background_task_group {
                    Some(task_group) => Some(task_group.shutdown_until(deadline).await),
                    None => None,
                };
                let workers = match &targets.worker_task_group {
                    Some(task_group) => Some(task_group.shutdown_until(deadline).await),
                    None => None,
                };

                let clients = self.connection_registry.take_all_sessions();
                #[cfg(test)]
                let shutdown_connections_taken = { self.lifecycle.lock().shutdown_connections_taken() };
                #[cfg(test)]
                if let Some(barrier) = shutdown_connections_taken {
                    barrier.pause().await;
                }
                let mut connections = Vec::with_capacity(clients.len());
                for (addr, client) in clients {
                    let started = Instant::now();
                    let report = match time::timeout_at(
                        time::Instant::from_std(deadline.instant()),
                        client.close_with_report(deadline.remaining()),
                    )
                    .await
                    {
                        Ok(report) => report,
                        Err(_) => {
                            let mut report =
                                ShutdownReport::new("rocketmq.transport.client.connection", started.elapsed());
                            report.timed_out = 1;
                            report
                        }
                    };
                    connections.push(ConnectionShutdownReport { addr, report });
                }

                self.nameserver_health
                    .with_mutation_lock(|| self.endpoint_state.replace_topology(Vec::new()));
                let report = ClientShutdownReport {
                    background,
                    workers,
                    connections,
                };
                self.finish_shutdown_owner(&flight, report.clone()).await;
                owner_guard.disarm();
                report
            }
            ShutdownAcquire::Participant(flight) => {
                let report = flight.wait_report().await;
                report
            }
        }
    }

    /// Immediately cancels and aborts client task groups without waiting for
    /// task drains or connection cleanup to finish.
    ///
    /// The returned report describes immediate task-group cancellation only; it
    /// makes no guarantee that background work or connections were drained.
    pub fn shutdown_now(&self) -> ClientShutdownReport {
        let shutdown = { self.lifecycle.lock().acquire_shutdown() };
        let (flight, is_owner) = match shutdown {
            ShutdownAcquire::Owner(flight) => (flight, true),
            ShutdownAcquire::Participant(flight) => (flight, false),
        };
        let report = self.shutdown_targets_now(&flight.targets);
        if is_owner {
            self.finish_shutdown_owner_now(&flight, report.clone());
        }
        report
    }

    fn shutdown_targets_now(&self, targets: &ShutdownTargets) -> ClientShutdownReport {
        if let Some(cancellation) = &targets.cancellation {
            cancellation.cancel();
        }
        let background = targets
            .background_task_group
            .as_ref()
            .map(|task_group| task_group.shutdown_now());
        let workers = targets
            .worker_task_group
            .as_ref()
            .map(|task_group| task_group.shutdown_now());

        self.connection_registry.take_all_sessions();
        self.connection_registry.clear_flights();
        self.nameserver_health
            .with_mutation_lock(|| self.endpoint_state.replace_topology(Vec::new()));

        ClientShutdownReport {
            background,
            workers,
            connections: Vec::new(),
        }
    }

    async fn finish_shutdown_owner(&self, flight: &Arc<ShutdownFlight>, report: ClientShutdownReport) {
        let stopped = self.finish_shutdown_phase(flight);
        #[cfg(test)]
        let shutdown_phase_finished_before_report = { self.lifecycle.lock().shutdown_phase_finished_before_report() };
        #[cfg(test)]
        if stopped {
            if let Some(barrier) = shutdown_phase_finished_before_report {
                barrier.pause().await;
            }
        }
        if stopped {
            let _ = self.lifecycle.lock().publish_shutdown_owner_report(flight, report);
        }
    }

    fn finish_shutdown_owner_now(&self, flight: &Arc<ShutdownFlight>, report: ClientShutdownReport) {
        if self.finish_shutdown_phase(flight) {
            let _ = self.lifecycle.lock().publish_shutdown_owner_report(flight, report);
        }
    }

    fn finish_shutdown_phase(&self, flight: &Arc<ShutdownFlight>) -> bool {
        let stopped = self.lifecycle.lock().finish_shutdown_owner(flight);
        #[cfg(test)]
        if stopped {
            *self.background_task_group.lock() = None;
            *self.worker_task_group.lock() = None;
        }
        stopped
    }

    /// Compatibility wrapper for [`Self::shutdown_graceful`].
    ///
    /// The duration is converted once into an absolute deadline shared by all
    /// graceful client shutdown stages.
    pub async fn shutdown_with_report(&self, timeout: Duration) -> ClientShutdownReport {
        self.shutdown_graceful(ShutdownDeadline::after(timeout)).await
    }

    /// Compatibility wrapper for [`Self::shutdown_now`].
    ///
    /// This method requests immediate cancellation and does not wait for task
    /// drains or connection cleanup.
    pub fn shutdown(&self) {
        let report = self.shutdown_now();
        if report.background.as_ref().is_some_and(|report| !report.is_healthy()) {
            warn!("RemotingClient background task shutdown report is unhealthy");
        }
        if report.workers.as_ref().is_some_and(|report| !report.is_healthy()) {
            warn!("RemotingClient worker task shutdown report is unhealthy");
        }
        info!("RemotingClient shutdown complete");
    }
}

enum StartAttempt {
    Started(usize),
    Continue {
        generation: ClientGeneration,
        task_group: TaskGroup,
        token: CancellationToken,
        idle_scan_interval: Duration,
    },
    Failed {
        generation: ClientGeneration,
        error: rocketmq_error::RocketMQError,
    },
    Interrupted,
}

#[cfg(test)]
#[path = "../../../tests/unit/clients/rocketmq_tokio_client/lifecycle.rs"]
mod tests;
