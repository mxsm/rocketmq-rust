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
                        Ok(V2SessionEvent::Connected(_)) => {}
                        Ok(V2SessionEvent::Disconnected(session_id)) => {
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
            .map_err(|error| ControllerError::runtime_error(format!("Failed to start broker session monitor: {error}")))
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

        // The V2 server takes exclusive processor ownership during start.
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
    /// Returns [`ControllerError`] if the manager is not initialized, a component
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
            return Err(ControllerError::runtime_error(
                "Controller manager cannot be restarted after shutdown or a failed startup",
            ));
        }

        if !self.initialized.load(Ordering::SeqCst) {
            return Err(ControllerError::NotInitialized(
                "Controller manager must be initialized before starting".to_string(),
            ));
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
            return Err(self
                .cleanup_after_start_failure(ControllerError::runtime_error(format!(
                    "Failed to start Raft controller: {e}"
                )))
                .await);
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
                let error =
                    ControllerError::runtime_error(format!("Failed to spawn controller remoting server task: {error}"));
                return Err(self.cleanup_after_start_failure(error).await);
            }
            match startup_rx.await {
                Ok(Ok(_address)) => info!("Remoting server started with ControllerRequestProcessor"),
                Ok(Err(error)) => {
                    return Err(self
                        .cleanup_after_start_failure(ControllerError::runtime_error(format!(
                            "Controller remoting server failed to start: {error}"
                        )))
                        .await);
                }
                Err(error) => {
                    return Err(self
                        .cleanup_after_start_failure(ControllerError::runtime_error(format!(
                            "Controller remoting server startup acknowledgement was dropped: {error}"
                        )))
                        .await);
                }
            }
        }

        {
            if let Err(error) = self.remoting_client.start().await {
                let error = ControllerError::runtime_error(format!("Failed to start remoting client: {error}"));
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
    pub(super) async fn cleanup_after_start_failure(&self, start_error: ControllerError) -> ControllerError {
        self.running.store(true, Ordering::Release);
        let deadline = ShutdownDeadline::after(Duration::from_secs(30));
        let cleanup = tokio::time::timeout(deadline.remaining(), self.shutdown_inner(deadline)).await;

        match cleanup {
            Ok(Ok(())) => start_error,
            Ok(Err(cleanup_error)) => ControllerError::runtime_error(format!(
                "Controller startup failed: {start_error}; startup cleanup was unhealthy: {cleanup_error}"
            )),
            Err(_) => ControllerError::runtime_error(format!(
                "Controller startup failed: {start_error}; startup cleanup exhausted its absolute deadline"
            )),
        }
    }

    /// Shuts down the Controller runtime. Calling it while stopped is a no-op.
    ///
    /// # Errors
    ///
    /// Returns [`ControllerError`] when the deadline expires or a shutdown phase fails.
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
            Err(_) => Err(ControllerError::runtime_error(
                "Controller shutdown exhausted its absolute deadline",
            )),
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
            failures.push(format!("leadership scheduling: {error}"));
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
            failures.push(format!("heartbeat manager: {detail}"));
        }

        if !self.shutdown_manager_tasks(deadline).await {
            failures.push("manager tasks did not stop cleanly".to_string());
        }

        if let Some(security) = &self.security {
            match tokio::time::timeout(deadline.remaining(), security.authenticator().shutdown()).await {
                Ok(Ok(())) => info!("Controller security adapter shut down"),
                Ok(Err(error)) => {
                    warn!(%error, "Controller security adapter shutdown failed");
                    failures.push(format!("security adapter: {error}"));
                }
                Err(_) => {
                    warn!("Timed out waiting for Controller security adapter shutdown");
                    failures.push("security adapter shutdown timed out".to_string());
                }
            }
        }

        {
            let report = self.remoting_client.shutdown_with_report(deadline.remaining()).await;
            if report.is_healthy() {
                info!("Remoting client shut down");
            } else {
                let detail = serde_json::to_string(&report)
                    .unwrap_or_else(|error| format!("failed to serialize remoting shutdown report: {error}"));
                warn!(report = %detail, "Remoting client shutdown was unhealthy");
                failures.push(format!("remoting client: {detail}"));
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
                failures.push(format!("Raft: {e}"));
            }
            Err(_) => {
                warn!("Timed out waiting for Raft controller shutdown");
                failures.push("Raft shutdown timed out".to_string());
            }
        }

        #[cfg(feature = "metrics")]
        info!("Metrics manager will be cleaned up automatically");

        if failures.is_empty() {
            info!("Controller manager shut down successfully");
            Ok(())
        } else {
            Err(ControllerError::runtime_error(format!(
                "Controller shutdown completed with unhealthy phases: {}",
                failures.join("; ")
            )))
        }
    }
}
