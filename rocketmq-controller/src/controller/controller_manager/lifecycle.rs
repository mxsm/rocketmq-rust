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

use super::*;
use crate::error::controller_internal;
use crate::error::controller_internal_by;
use crate::error::not_initialized;
use rocketmq_error::Error;
use rocketmq_error::RocketMQError;

#[derive(Debug)]
struct ControllerStartupRollbackFailure {
    startup: Error,
    cleanup: Option<Error>,
    cleanup_timed_out: bool,
}

impl std::fmt::Display for ControllerStartupRollbackFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let _ = (&self.cleanup, self.cleanup_timed_out);
        formatter.write_str("controller startup rollback failed")
    }
}

impl std::error::Error for ControllerStartupRollbackFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.startup)
    }
}

#[derive(Debug)]
enum ControllerShutdownFailure {
    Canonical { phase: &'static str, error: Error },
    Facade { phase: &'static str, error: RocketMQError },
    UnhealthyReport { phase: &'static str },
    TimedOut { phase: &'static str },
}

#[derive(Debug)]
struct ControllerShutdownFailures {
    failures: Vec<ControllerShutdownFailure>,
}

impl std::fmt::Display for ControllerShutdownFailures {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for failure in &self.failures {
            let phase = match failure {
                ControllerShutdownFailure::Canonical { phase, .. }
                | ControllerShutdownFailure::Facade { phase, .. }
                | ControllerShutdownFailure::UnhealthyReport { phase }
                | ControllerShutdownFailure::TimedOut { phase } => phase,
            };
            let _ = phase;
        }
        formatter.write_str("controller shutdown completed with unhealthy phases")
    }
}

impl std::error::Error for ControllerShutdownFailures {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.failures.iter().find_map(|failure| match failure {
            ControllerShutdownFailure::Canonical { error, .. } => Some(error as &(dyn std::error::Error + 'static)),
            ControllerShutdownFailure::Facade { error, .. } => Some(error as &(dyn std::error::Error + 'static)),
            ControllerShutdownFailure::UnhealthyReport { .. } | ControllerShutdownFailure::TimedOut { .. } => None,
        })
    }
}

impl ControllerManager {
    pub(super) fn ensure_manager_task_group(&self) -> Result<TaskGroup> {
        let mut guard = self.manager_task_group.lock();
        if let Some(task_group) = guard.as_ref() {
            return Ok(task_group.clone());
        }

        let task_group = self
            .service_context
            .component("rocketmq-controller.manager")
            .task_group()
            .clone();
        *guard = Some(task_group.clone());
        Ok(task_group)
    }

    pub(super) fn manager_task_group(&self) -> Option<TaskGroup> {
        self.manager_task_group.lock().clone()
    }

    fn start_broker_session_monitor(&self, task_group: &TaskGroup) -> Result<()> {
        let mut events = self.session_registry.subscribe();
        let heartbeat_manager = Arc::clone(&self.heartbeat_manager);
        task_group
            .spawn_cancellable_service("controller.broker-session-monitor", async move {
                loop {
                    match events.recv().await {
                        Ok(SessionEvent::Connected(_)) => {}
                        Ok(SessionEvent::Disconnected(session_id)) => {
                            heartbeat_manager.on_broker_session_close(BrokerSessionId::from(session_id));
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                            warn!(
                                skipped,
                                "Controller broker session monitor lagged; heartbeat scan remains the backstop"
                            );
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    }
                }
            })
            .map(|_| ())
            .map_err(|error| controller_internal_by("start broker session monitor", error))
    }

    async fn shutdown_manager_tasks(&self, deadline: ShutdownDeadline) -> bool {
        self.leadership_watch_tasks.lock().take();
        let task_group = self.manager_task_group.lock().take();
        let Some(task_group) = task_group else {
            return true;
        };

        let report = task_group.shutdown_until(deadline).await;
        if !report.is_healthy() {
            warn!(
                report = %report.to_json(),
                "Controller manager task shutdown report is unhealthy"
            );
        }
        report.is_healthy()
    }

    /// Initializes heartbeat handling and broker lifecycle listeners before startup.
    ///
    /// Returns `Ok(true)` on the first call and `Ok(false)` once initialized.
    pub async fn initialize(self: &Arc<Self>) -> Result<bool> {
        let _lifecycle_guard = self.lifecycle_lock.lock().await;
        if self.initialized.load(Ordering::Acquire) {
            warn!("Controller manager is already initialized");
            return Ok(false);
        }

        info!("Initializing controller manager...");

        {
            self.heartbeat_manager.initialize_shared();
            info!("Heartbeat manager initialized");
        }

        {
            let inactive_listener = Arc::new(BrokerInactiveListener::new(Arc::downgrade(self)));
            self.heartbeat_manager
                .register_broker_lifecycle_listener_shared(inactive_listener.clone());
            self.raft_controller
                .register_broker_lifecycle_listener(inactive_listener);
            info!("Broker inactive listener registered");
        }

        // The server takes exclusive processor ownership during start.
        info!("Controller request processor wiring initialized");

        // Metrics manager is already initialized from the injected telemetry handle in new().
        #[cfg(feature = "metrics")]
        info!("Metrics manager is ready");

        self.initialized.store(true, Ordering::Release);
        info!("Controller manager initialized successfully");
        Ok(true)
    }

    fn init_processors(controller_manager: Arc<ControllerManager>) -> ControllerRequestProcessor {
        ControllerRequestProcessor::new(controller_manager)
    }

    /// Starts the Controller runtime.
    ///
    /// # Errors
    ///
    /// Returns [`rocketmq_error::Error`] if the manager is not initialized, a component
    /// fails to start, or one-shot resources were consumed by shutdown or rollback.
    ///
    /// Repeated calls while running are idempotent. A stopped or rolled-back
    /// manager cannot be restarted.
    pub async fn start(self: &Arc<Self>) -> Result<()> {
        let _lifecycle_guard = self.lifecycle_lock.lock().await;
        if self.running.load(Ordering::Acquire) {
            warn!("Controller manager is already running");
            return Ok(());
        }

        if self.lifecycle_terminated.load(Ordering::Acquire) {
            return Err(controller_internal("restart terminated controller manager"));
        }

        if !self.initialized.load(Ordering::SeqCst) {
            return Err(not_initialized("controller.manager"));
        }

        if self
            .running
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            warn!("Controller manager is already running");
            return Ok(());
        }

        info!("Starting controller manager...");

        // Raft must start before broker-facing services can observe leadership.
        if let Err(e) = self.raft_controller.startup_shared().await {
            self.running.store(false, Ordering::SeqCst);
            return Err(self.cleanup_after_start_failure(e).await);
        }
        info!("Raft controller started");

        {
            self.heartbeat_manager.start_shared();
            info!("Heartbeat manager started");
        }

        let manager_task_group = match self.ensure_manager_task_group() {
            Ok(task_group) => task_group,
            Err(error) => return Err(self.cleanup_after_start_failure(error).await),
        };
        if let Err(error) = self.start_broker_session_monitor(&manager_task_group) {
            return Err(self.cleanup_after_start_failure(error).await);
        }

        let remoting_server = self.remoting_server.lock().take();
        if let Some(pending_server) = remoting_server {
            let request_processor = Self::init_processors(Arc::clone(self));
            let server = pending_server.build(request_processor);
            let (shutdown_tx, shutdown_rx) = oneshot::channel();
            *self.remoting_server_shutdown_tx.lock() = Some(shutdown_tx);
            let (startup_tx, startup_rx) = oneshot::channel();
            if let Err(error) = manager_task_group.spawn_service("controller.remoting-server", async move {
                let report = server
                    .try_run_with_shutdown_report_and_startup(
                        async move {
                            let _ = shutdown_rx.await;
                        },
                        startup_tx,
                    )
                    .await;
                match report.as_ref() {
                    Ok(report) if !report.is_healthy() => {
                        warn!(
                            report = %report.to_json(),
                            "Controller remoting server shutdown report is unhealthy"
                        );
                    }
                    Err(error) => warn!(%error, "Controller remoting server stopped before startup completed"),
                    _ => {}
                }
            }) {
                let error = controller_internal_by("spawn controller remoting server task", error);
                return Err(self.cleanup_after_start_failure(error).await);
            }
            match startup_rx.await {
                Ok(Ok(_address)) => info!("Remoting server started with ControllerRequestProcessor"),
                Ok(Err(error)) => {
                    return Err(self
                        .cleanup_after_start_failure(controller_internal_by("start controller remoting server", error))
                        .await);
                }
                Err(error) => {
                    return Err(self
                        .cleanup_after_start_failure(controller_internal_by(
                            "receive controller remoting server startup acknowledgement",
                            error,
                        ))
                        .await);
                }
            }
        }

        {
            if let Err(error) = self.remoting_client.start().await {
                let error = controller_internal_by("start controller remoting client", error);
                return Err(self.cleanup_after_start_failure(error).await);
            }
            info!("Remoting client started");
        }

        if let Err(error) = self
            .start_broker_role_notifier_and_synchronize(&manager_task_group)
            .await
        {
            return Err(self.cleanup_after_start_failure(error).await);
        }
        if let Err(error) = self.start_leadership_watch_loop().await {
            return Err(self.cleanup_after_start_failure(error).await);
        }

        #[cfg(feature = "metrics")]
        info!("Metrics manager is already running (singleton)");

        info!("Controller manager started successfully");
        Ok(())
    }

    /// Rolls back a partial start while the caller owns `lifecycle_lock`.
    pub(super) async fn cleanup_after_start_failure(&self, start_error: Error) -> Error {
        self.running.store(true, Ordering::Release);
        let deadline = ShutdownDeadline::after(Duration::from_secs(30));
        let cleanup = tokio::time::timeout(deadline.remaining(), self.shutdown_inner(deadline)).await;

        match cleanup {
            Ok(Ok(())) => start_error,
            Ok(Err(cleanup_error)) => controller_internal_by(
                "rollback controller startup",
                ControllerStartupRollbackFailure {
                    startup: start_error,
                    cleanup: Some(cleanup_error),
                    cleanup_timed_out: false,
                },
            ),
            Err(_) => controller_internal_by(
                "rollback controller startup",
                ControllerStartupRollbackFailure {
                    startup: start_error,
                    cleanup: None,
                    cleanup_timed_out: true,
                },
            ),
        }
    }

    /// Shuts down the Controller runtime. Calling it while stopped is a no-op.
    ///
    /// # Errors
    ///
    /// Returns [`rocketmq_error::Error`] when the deadline expires or a shutdown phase fails.
    pub async fn shutdown(&self) -> Result<()> {
        self.shutdown_until(ShutdownDeadline::after(Duration::from_secs(30)))
            .await
    }

    /// Shuts down the Controller without extending the process-level absolute deadline.
    ///
    /// # Errors
    ///
    /// Returns a typed runtime error when the deadline expires or a shutdown phase fails.
    pub async fn shutdown_until(&self, deadline: ShutdownDeadline) -> Result<()> {
        let shutdown = async {
            let _lifecycle_guard = self.lifecycle_lock.lock().await;
            self.shutdown_inner(deadline).await
        };
        match tokio::time::timeout(deadline.remaining(), shutdown).await {
            Ok(result) => result,
            Err(_) => Err(controller_internal("shutdown controller before deadline")),
        }
    }

    async fn shutdown_inner(&self, deadline: ShutdownDeadline) -> Result<()> {
        if self
            .running
            .compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            warn!("Controller manager is not running");
            return Ok(());
        }
        self.lifecycle_terminated.store(true, Ordering::Release);
        info!("Shutting down controller manager...");
        let mut failures = Vec::new();

        if let Err(error) = self.stop_leadership_gate().await {
            warn!("Failed to stop leader-only scheduling during shutdown: {}", error);
            failures.push(ControllerShutdownFailure::Canonical {
                phase: "leadership scheduling",
                error,
            });
        }
        self.broker_role_notifier.close();
        if let Some(shutdown_tx) = self.remoting_server_shutdown_tx.lock().take() {
            let _ = shutdown_tx.send(());
        }
        let heartbeat_report = self.heartbeat_manager.shutdown_gracefully_until(deadline).await;
        if heartbeat_report.is_healthy() {
            info!("Heartbeat manager shut down");
        } else {
            let detail = heartbeat_report.to_json();
            warn!(report = %detail, "Heartbeat manager shutdown was unhealthy");
            failures.push(ControllerShutdownFailure::UnhealthyReport {
                phase: "heartbeat manager",
            });
        }

        if !self.shutdown_manager_tasks(deadline).await {
            failures.push(ControllerShutdownFailure::UnhealthyReport { phase: "manager tasks" });
        }

        if let Some(security) = &self.security {
            match tokio::time::timeout(deadline.remaining(), security.authenticator().shutdown()).await {
                Ok(Ok(())) => info!("Controller security adapter shut down"),
                Ok(Err(error)) => {
                    warn!(%error, "Controller security adapter shutdown failed");
                    failures.push(ControllerShutdownFailure::Facade {
                        phase: "security adapter",
                        error,
                    });
                }
                Err(_) => {
                    warn!("Timed out waiting for Controller security adapter shutdown");
                    failures.push(ControllerShutdownFailure::TimedOut {
                        phase: "security adapter",
                    });
                }
            }
        }

        {
            let report = self.remoting_client.shutdown_with_report(deadline.remaining()).await;
            if report.is_healthy() {
                info!("Remoting client shut down");
            } else {
                let detail = serde_json::to_string(&report)
                    .unwrap_or_else(|_| "controller remoting shutdown report unavailable".to_owned());
                warn!(report = %detail, "Remoting client shutdown was unhealthy");
                failures.push(ControllerShutdownFailure::UnhealthyReport {
                    phase: "remoting client",
                });
            }
        }

        // Raft shuts down last because it coordinates distributed operations.
        match tokio::time::timeout(
            deadline.remaining().min(Duration::from_secs(10)),
            self.raft_controller.shutdown_shared(),
        )
        .await
        {
            Ok(Ok(())) => info!("Raft controller shut down"),
            Ok(Err(e)) => {
                error!("Failed to shutdown Raft: {}", e);
                failures.push(ControllerShutdownFailure::Canonical {
                    phase: "Raft",
                    error: e,
                });
            }
            Err(_) => {
                warn!("Timed out waiting for Raft controller shutdown");
                failures.push(ControllerShutdownFailure::TimedOut { phase: "Raft" });
            }
        }

        #[cfg(feature = "metrics")]
        info!("Metrics manager will be cleaned up automatically");

        if failures.is_empty() {
            info!("Controller manager shut down successfully");
            Ok(())
        } else {
            Err(controller_internal_by(
                "shutdown controller manager",
                ControllerShutdownFailures { failures },
            ))
        }
    }
}

#[cfg(test)]
mod aggregate_tests {
    use super::*;

    #[test]
    fn lifecycle_aggregates_retain_secondary_errors_and_phases_without_formatting_them() {
        let startup = ControllerStartupRollbackFailure {
            startup: controller_internal("simulate startup failure"),
            cleanup: Some(controller_internal("simulate cleanup failure")),
            cleanup_timed_out: false,
        };
        assert!(startup.cleanup.is_some());
        assert!(!startup.cleanup_timed_out);
        assert_eq!(startup.to_string(), "controller startup rollback failed");
        let outer = controller_internal_by("rollback controller startup", startup);
        let retained_startup = std::error::Error::source(&outer)
            .and_then(|source| source.downcast_ref::<ControllerStartupRollbackFailure>())
            .expect("startup aggregate must remain the typed source");
        assert!(retained_startup.cleanup.is_some());

        let shutdown = ControllerShutdownFailures {
            failures: vec![
                ControllerShutdownFailure::Canonical {
                    phase: "leadership",
                    error: controller_internal("simulate leadership failure"),
                },
                ControllerShutdownFailure::TimedOut { phase: "Raft" },
            ],
        };
        assert!(matches!(
            shutdown.failures.as_slice(),
            [
                ControllerShutdownFailure::Canonical {
                    phase: "leadership",
                    ..
                },
                ControllerShutdownFailure::TimedOut { phase: "Raft" }
            ]
        ));
        assert_eq!(
            shutdown.to_string(),
            "controller shutdown completed with unhealthy phases"
        );
        let outer = controller_internal_by("shutdown controller manager", shutdown);
        let retained_shutdown = std::error::Error::source(&outer)
            .and_then(|source| source.downcast_ref::<ControllerShutdownFailures>())
            .expect("shutdown aggregate must remain the typed source");
        assert_eq!(retained_shutdown.failures.len(), 2);
    }
}
