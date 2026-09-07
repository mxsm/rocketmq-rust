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

//! Unique application-owned runtime and RocketMQ client lifecycle.

use std::{error::Error as StdError, fmt, sync::Arc, time::Duration};

use rocketmq_admin_core::client_adapter::{ClientRuntime, ClientRuntimeConfig, TelemetryHandle};
use rocketmq_runtime::{
    ChildServiceContext, RuntimeConfig, RuntimeError, RuntimeOperation, RuntimeOwner, ShutdownReport,
};

use super::admin_provider::GpuiAdminProvider;

/// Runtime bootstrap or shutdown failure at the process boundary.
#[derive(Clone)]
pub(crate) struct DesktopRuntimeError {
    inner: Arc<DesktopRuntimeErrorInner>,
}

struct DesktopRuntimeErrorInner {
    condition: DesktopRuntimeCondition,
    source: Arc<dyn StdError + Send + Sync>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DesktopRuntimeCondition {
    Contract,
    Runtime,
    Client,
    Shutdown,
}

impl DesktopRuntimeError {
    fn caused_by(condition: DesktopRuntimeCondition, source: impl StdError + Send + Sync + 'static) -> Self {
        Self {
            inner: Arc::new(DesktopRuntimeErrorInner {
                condition,
                source: Arc::new(source),
            }),
        }
    }
}

impl fmt::Debug for DesktopRuntimeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DesktopRuntimeError")
            .field("condition", &self.inner.condition)
            .finish()
    }
}

impl fmt::Display for DesktopRuntimeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self.inner.condition {
            DesktopRuntimeCondition::Contract => "The dashboard runtime configuration is invalid.",
            DesktopRuntimeCondition::Runtime => "The dashboard runtime operation failed.",
            DesktopRuntimeCondition::Client => "The RocketMQ client runtime could not start.",
            DesktopRuntimeCondition::Shutdown => "The dashboard runtime did not shut down cleanly.",
        })
    }
}

impl StdError for DesktopRuntimeError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(self.inner.source.as_ref())
    }
}

/// The only runtime owner created by the desktop process.
pub struct DesktopClientRuntime {
    owner: RuntimeOwner,
    application_context: ChildServiceContext,
    work_context: ChildServiceContext,
    client_runtime: Arc<ClientRuntime>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ShutdownStage {
    Work,
    Client,
    Owner,
}

struct CleanupReport {
    stage: ShutdownStage,
    report: ShutdownReport,
}

struct RuntimeCleanupFailures {
    primary: Option<Arc<dyn StdError + Send + Sync>>,
    reports: Vec<CleanupReport>,
    owner_error: Option<RuntimeError>,
}

impl RuntimeCleanupFailures {
    fn new() -> Self {
        Self {
            primary: None,
            reports: Vec::new(),
            owner_error: None,
        }
    }

    fn with_primary(source: impl StdError + Send + Sync + 'static) -> Self {
        Self {
            primary: Some(Arc::new(source)),
            reports: Vec::new(),
            owner_error: None,
        }
    }

    fn retain(&mut self, stage: ShutdownStage, report: ShutdownReport) {
        self.reports.push(CleanupReport { stage, report });
    }

    fn retain_if_unhealthy(&mut self, stage: ShutdownStage, report: &ShutdownReport) {
        if !report.is_healthy() {
            self.retain(stage, report.clone());
        }
    }

    fn is_empty(&self) -> bool {
        self.primary.is_none() && self.reports.is_empty() && self.owner_error.is_none()
    }
}

impl fmt::Debug for RuntimeCleanupFailures {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let stages = self.reports.iter().map(|report| report.stage).collect::<Vec<_>>();
        let unhealthy_report_count = self.reports.iter().filter(|report| !report.report.is_healthy()).count();
        formatter
            .debug_struct("RuntimeCleanupFailures")
            .field("primary_available", &self.primary.is_some())
            .field("report_count", &self.reports.len())
            .field("unhealthy_report_count", &unhealthy_report_count)
            .field("stages", &stages)
            .field("owner_error_available", &self.owner_error.is_some())
            .finish()
    }
}

impl fmt::Display for RuntimeCleanupFailures {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Dashboard runtime startup or shutdown cleanup failed.")
    }
}

impl StdError for RuntimeCleanupFailures {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.primary
            .as_deref()
            .map(|source| source as _)
            .or_else(|| self.owner_error.as_ref().map(|source| source as _))
    }
}

fn client_start_failure(owner: RuntimeOwner, source: impl StdError + Send + Sync + 'static) -> DesktopRuntimeError {
    let mut failures = RuntimeCleanupFailures::with_primary(source);
    match owner.shutdown_runtime_blocking() {
        Ok(report) => failures.retain(ShutdownStage::Owner, report),
        Err(error) => failures.owner_error = Some(error),
    }
    let source = RuntimeError::internal(RuntimeOperation::ShutdownRuntimeBlocking, failures);
    DesktopRuntimeError::caused_by(DesktopRuntimeCondition::Client, source)
}

fn finish_shutdown(
    work_report: ShutdownReport,
    client_report: ShutdownReport,
    owner_result: Result<ShutdownReport, RuntimeError>,
) -> Result<ShutdownReport, DesktopRuntimeError> {
    let mut failures = RuntimeCleanupFailures::new();
    failures.retain_if_unhealthy(ShutdownStage::Work, &work_report);
    failures.retain_if_unhealthy(ShutdownStage::Client, &client_report);
    let owner_report = match owner_result {
        Ok(report) => {
            failures.retain_if_unhealthy(ShutdownStage::Owner, &report);
            Some(report)
        }
        Err(error) => {
            failures.owner_error = Some(error);
            None
        }
    };
    if let Some(owner_report) = owner_report
        && failures.is_empty()
    {
        return Ok(owner_report);
    }
    let source = RuntimeError::internal(RuntimeOperation::ShutdownRuntimeBlocking, failures);
    Err(DesktopRuntimeError::caused_by(
        DesktopRuntimeCondition::Shutdown,
        source,
    ))
}

impl DesktopClientRuntime {
    /// Creates one RuntimeOwner, one application child, and one ClientRuntime.
    pub fn new(telemetry: TelemetryHandle) -> Result<Self, DesktopRuntimeError> {
        Self::new_with_client_config(telemetry, ClientRuntimeConfig::default())
    }

    fn new_with_client_config(
        telemetry: TelemetryHandle,
        client_config: ClientRuntimeConfig,
    ) -> Result<Self, DesktopRuntimeError> {
        let owner = RuntimeOwner::plan(RuntimeConfig::for_parallelism(
            "rocketmq-dashboard-gpui",
            std::thread::available_parallelism().map_or(1, |parallelism| parallelism.get()),
        ))
        .map_err(|source| DesktopRuntimeError::caused_by(DesktopRuntimeCondition::Contract, source))?
        .build()
        .map_err(|source| DesktopRuntimeError::caused_by(DesktopRuntimeCondition::Runtime, source))?;
        let application_context = owner.root_context().component("dashboard-gpui");
        let work_context = application_context.component("application-work");
        let client_runtime = match ClientRuntime::try_new(
            application_context.component("client-runtime"),
            client_config,
            telemetry,
        ) {
            Ok(runtime) => runtime,
            Err(source) => {
                drop(work_context);
                drop(application_context);
                return Err(client_start_failure(owner, source));
            }
        };
        Ok(Self {
            owner,
            application_context,
            work_context,
            client_runtime,
        })
    }

    /// Returns a named application-owned child scope.
    pub fn component(&self, name: &'static str) -> ChildServiceContext {
        self.work_context.component(name)
    }

    /// Returns a provider scope that stays alive while application work is cancelled and drained.
    pub fn provider_component(&self, name: &'static str) -> ChildServiceContext {
        self.application_context.component(name)
    }

    /// Returns the single RocketMQ client runtime shared by every Admin session.
    pub fn client_runtime(&self) -> Arc<ClientRuntime> {
        Arc::clone(&self.client_runtime)
    }

    /// Cancels application work, closes provider and client resources, then shuts down the owner.
    /// Telemetry remains the caller's owner and must be shut down after this returns.
    pub fn shutdown(self, provider: Option<Arc<GpuiAdminProvider>>) -> Result<ShutdownReport, DesktopRuntimeError> {
        let Self {
            owner,
            application_context,
            work_context,
            client_runtime,
        } = self;
        let (work_report, client_report) = owner.block_on(async {
            work_context.task_group().cancel();
            let work_report = work_context.task_group().shutdown(Duration::from_secs(5)).await;
            if let Some(provider) = provider.as_ref() {
                provider.shutdown().await;
            }
            let client_report = client_runtime.shutdown().await;
            (work_report, client_report)
        });
        drop(provider);
        drop(client_runtime);
        drop(work_context);
        drop(application_context);
        let owner_result = owner.shutdown_runtime_blocking();
        finish_shutdown(work_report, client_report, owner_result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infrastructure::{admin_provider::GpuiAdminProvider, auth_state::DesktopAuthState};

    #[test]
    fn application_shutdown_closes_client_and_awaits_owned_tasks() {
        let runtime = DesktopClientRuntime::new(TelemetryHandle::noop()).expect("runtime");
        let provider = GpuiAdminProvider::new(
            runtime.provider_component("provider"),
            runtime.client_runtime(),
            DesktopAuthState::from_process_environment(),
        );
        let report = runtime.shutdown(Some(provider)).expect("shutdown");

        assert!(report.is_healthy());
    }

    #[test]
    fn runtime_error_projection_retains_but_does_not_render_source() {
        let error = DesktopRuntimeError::caused_by(
            DesktopRuntimeCondition::Client,
            std::io::Error::other("password=private-password"),
        );

        assert!(std::error::Error::source(&error).is_some());
        assert_eq!(error.to_string(), "The RocketMQ client runtime could not start.");
        assert!(!format!("{error:?}").contains("private-password"));
    }

    #[test]
    fn client_start_failure_explicitly_shuts_down_owner_and_retains_both_results() {
        let config = ClientRuntimeConfig {
            managed_memory_numerator: 0,
            ..ClientRuntimeConfig::default()
        };
        let error = match DesktopClientRuntime::new_with_client_config(TelemetryHandle::noop(), config) {
            Ok(_) => panic!("invalid client configuration must fail"),
            Err(error) => error,
        };
        let runtime = std::error::Error::source(&error)
            .and_then(|source| source.downcast_ref::<RuntimeError>())
            .expect("typed RuntimeError source");
        let failures = std::error::Error::source(runtime)
            .and_then(|source| source.downcast_ref::<RuntimeCleanupFailures>())
            .expect("typed startup and cleanup aggregate");

        assert!(failures.primary.is_some());
        assert!(failures.owner_error.is_none());
        assert_eq!(failures.reports.len(), 1);
        assert_eq!(failures.reports[0].stage, ShutdownStage::Owner);
        assert!(failures.reports[0].report.is_healthy());
    }

    #[test]
    fn every_unhealthy_cleanup_report_becomes_one_typed_shutdown_failure() {
        fn unhealthy(name: &'static str) -> ShutdownReport {
            let mut report = ShutdownReport::new(name, Duration::ZERO);
            report.failed = 1;
            report
        }

        let error = finish_shutdown(
            unhealthy("private-work-name"),
            unhealthy("private-client-name"),
            Ok(unhealthy("private-owner-name")),
        )
        .expect_err("every unhealthy cleanup report must fail shutdown");
        let runtime = std::error::Error::source(&error)
            .and_then(|source| source.downcast_ref::<RuntimeError>())
            .expect("typed RuntimeError source");
        let failures = std::error::Error::source(runtime)
            .and_then(|source| source.downcast_ref::<RuntimeCleanupFailures>())
            .expect("typed aggregate source");

        assert_eq!(
            failures.reports.iter().map(|failure| failure.stage).collect::<Vec<_>>(),
            vec![ShutdownStage::Work, ShutdownStage::Client, ShutdownStage::Owner]
        );
        assert!(failures.reports.iter().all(|failure| !failure.report.is_healthy()));
        assert!(!format!("{error:?}").contains("private-work-name"));
    }
}
