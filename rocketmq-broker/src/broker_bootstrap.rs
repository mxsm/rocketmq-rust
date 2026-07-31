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

use std::sync::Arc;

use crate::config::validated::ValidatedBrokerConfig;
use rocketmq_observability::TelemetryRuntimeGuard;
use rocketmq_runtime::wait_for_signal;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_runtime::RuntimeError;
use rocketmq_runtime::RuntimeResult;
use rocketmq_runtime::ServiceLifecycle;
use rocketmq_runtime::ShutdownReason;
use tracing::error;
use tracing::info;

use crate::broker_runtime::BrokerRuntime;
use crate::lifecycle::BrokerReadiness;
use crate::lifecycle::BrokerStartupError;
use crate::lifecycle::Configured;
use crate::lifecycle::Initialized;
use crate::lifecycle::Running;

pub struct BrokerBootstrap<State = Configured> {
    broker_runtime: BrokerRuntime,
    release_identity_required: bool,
    state: State,
}

impl BrokerBootstrap<Configured> {
    pub async fn initialize(mut self) -> Result<BrokerBootstrap<Initialized>, BrokerStartupError> {
        if let Err(error) = self.broker_runtime.initialize().await {
            return Err(self.broker_runtime.rollback_startup(error).await);
        }
        Ok(BrokerBootstrap {
            broker_runtime: self.broker_runtime,
            release_identity_required: self.release_identity_required,
            state: Initialized,
        })
    }

    pub async fn boot(self) {
        let initialized = match self.initialize().await {
            Ok(initialized) => initialized,
            Err(error) => {
                error!(%error, "Broker initialization failed");
                return;
            }
        };
        let running = match initialized.start().await {
            Ok(running) => running,
            Err(error) => {
                error!(%error, "Broker startup failed");
                return;
            }
        };

        // Wait for shutdown signal (Ctrl+C or SIGTERM)
        wait_for_signal().await;
        info!("Broker received shutdown signal");

        // Graceful shutdown
        running.shutdown().await;
        info!("Broker shutdown completed");
    }

    /// Boots the broker under the shared process lifecycle and absolute shutdown deadline.
    ///
    /// # Errors
    ///
    /// Returns a lifecycle error when broker initialization, readiness publication, or
    /// platform signal observation fails.
    pub async fn boot_with_lifecycle(self, lifecycle: ServiceLifecycle) -> RuntimeResult<()> {
        record_broker_lifecycle("starting", "success", "startup");
        let initialized = self.initialize().await.map_err(|error| {
            lifecycle.mark_failed();
            lifecycle.request_shutdown(ShutdownReason::Internal);
            record_broker_lifecycle("failed", "failure", "initialization");
            RuntimeError::LifecycleOperation {
                operation: "initialize_broker",
                message: error.to_string(),
            }
        })?;
        let mut running = initialized.start().await.map_err(|error| {
            lifecycle.mark_failed();
            lifecycle.request_shutdown(ShutdownReason::Internal);
            record_broker_lifecycle("failed", "failure", "startup");
            RuntimeError::LifecycleOperation {
                operation: "start_broker",
                message: error.to_string(),
            }
        })?;
        lifecycle.mark_ready()?;
        record_broker_lifecycle("ready", "success", "startup");
        let shutdown_request = match lifecycle.wait_for_shutdown_signal().await {
            Ok(request) => request,
            Err(error) => {
                lifecycle.mark_failed();
                lifecycle.request_shutdown(ShutdownReason::Internal);
                record_broker_lifecycle("failed", "failure", "signal");
                return Err(error);
            }
        };
        record_broker_lifecycle("stopping", "success", "shutdown_request");
        info!(
            reason = shutdown_request.reason.as_str(),
            remaining_ms = shutdown_request.deadline.remaining().as_millis(),
            "Broker received shutdown request"
        );

        let report = running
            .broker_runtime
            .shutdown_basic_service_until(shutdown_request.deadline)
            .await;
        if !report.is_healthy() {
            tracing::warn!(
                unhealthy_components = ?report.unhealthy_component_names(),
                "Broker lifecycle shutdown report is unhealthy"
            );
            lifecycle.mark_failed();
            record_broker_lifecycle("failed", "failure", "shutdown_timeout");
            return Err(RuntimeError::LifecycleOperation {
                operation: "shutdown_broker",
                message: format!(
                    "broker shutdown did not complete before the shared deadline; unhealthy components: {:?}",
                    report.unhealthy_component_names()
                ),
            });
        }
        lifecycle.mark_stopped();
        record_broker_lifecycle("stopped", "success", "shutdown_complete");
        Ok(())
    }
}

fn record_broker_lifecycle(state: &'static str, result: &'static str, reason: &'static str) {
    info!(
        event = rocketmq_observability::semantic::events::BROKER_LIFECYCLE,
        state, result, reason, "Broker lifecycle transition"
    );
}

impl BrokerBootstrap<Initialized> {
    pub async fn start(mut self) -> Result<BrokerBootstrap<Running>, BrokerStartupError> {
        let readiness = self.broker_runtime.start().await?;
        Ok(BrokerBootstrap {
            broker_runtime: self.broker_runtime,
            release_identity_required: self.release_identity_required,
            state: Running::new(readiness),
        })
    }
}

impl BrokerBootstrap<Running> {
    #[must_use]
    pub fn readiness(&self) -> &BrokerReadiness {
        self.state.readiness()
    }

    pub async fn shutdown(mut self) {
        self.broker_runtime.shutdown().await;
    }
}

pub struct Builder {
    validated_config: ValidatedBrokerConfig,
    service_context: ChildServiceContext,
    telemetry_runtime_guard: TelemetryRuntimeGuard,
    release_identity_required: bool,
}

impl Builder {
    #[inline]
    pub fn new(service_context: ChildServiceContext, telemetry_runtime_guard: TelemetryRuntimeGuard) -> Self {
        Builder {
            validated_config: ValidatedBrokerConfig::default(),
            service_context,
            telemetry_runtime_guard,
            release_identity_required: false,
        }
    }

    #[inline]
    pub fn with_validated_config(mut self, validated_config: ValidatedBrokerConfig) -> Self {
        self.validated_config = validated_config;
        self
    }

    #[inline]
    pub fn require_release_identity_registration(mut self, required: bool) -> Self {
        self.release_identity_required = required;
        self
    }

    #[inline]
    pub fn build(self) -> BrokerBootstrap<Configured> {
        let telemetry_handle = self.telemetry_runtime_guard.handle();
        let mut broker_runtime = BrokerRuntime::new_with_validated_config_and_telemetry(
            Arc::new(self.validated_config),
            self.service_context,
            telemetry_handle,
        );
        broker_runtime.set_telemetry_runtime_guard(self.telemetry_runtime_guard);

        BrokerBootstrap {
            broker_runtime,
            release_identity_required: self.release_identity_required,
            state: Configured,
        }
    }
}

#[cfg(all(test, feature = "local_file_store"))]
mod tests {
    use rocketmq_runtime::RuntimeContext;

    use super::*;

    #[tokio::test]
    async fn builder_passes_service_context_to_broker_runtime() {
        let context = RuntimeContext::from_current("broker-bootstrap-context-test");
        let service_context = context.service_context("broker-bootstrap-service");

        let mut bootstrap = Builder::new(service_context.clone(), TelemetryRuntimeGuard::noop()).build();

        let broker_task_group = bootstrap
            .broker_runtime
            .runtime_state_mut()
            .broker_service_task_group()
            .expect("broker service task group should come from service context");

        assert_eq!(broker_task_group.id(), service_context.task_group().id());
    }
}
