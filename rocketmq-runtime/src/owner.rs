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

use std::future::Future;

use crate::config::RuntimeConfig;
use crate::diagnostics::RuntimeDiagnostics;
use crate::error::RuntimeError;
use crate::error::RuntimeResult;
use crate::handle::RuntimeHandle;
use crate::resource_budget::ProcessMemoryLimit;
use crate::resources::RuntimeResources;
use crate::service_context::RootServiceContext;
use crate::shutdown_deadline::ShutdownDeadline;
use crate::shutdown_report::ShutdownReport;
use crate::task_group::TaskGroup;
use crate::task_group::TaskGroupLifecycleState;

/// Owns one Tokio runtime and its root service context.
///
/// Dropping this value without an explicit shutdown starts Tokio background
/// shutdown and logs any still-tracked work.
pub struct RuntimeOwner {
    config: RuntimeConfig,
    runtime: Option<tokio::runtime::Runtime>,
    resources: RuntimeResources,
    root_context: RootServiceContext,
}

/// A runtime configuration that has passed deterministic validation.
///
/// Construct this value with [`RuntimeOwner::plan`], then invoke
/// [`Self::build`] at the operational startup boundary.
#[derive(Debug)]
pub struct RuntimeOwnerPlan {
    config: RuntimeConfig,
    memory_limit: Option<ProcessMemoryLimit>,
}

impl RuntimeOwnerPlan {
    /// Supplies a container-provided memory limit for this runtime.
    #[must_use]
    pub fn with_memory_limit(mut self, memory_limit: ProcessMemoryLimit) -> Self {
        self.memory_limit = Some(memory_limit);
        self
    }

    /// Builds the validated Tokio runtime owner.
    ///
    /// # Errors
    ///
    /// Returns an operational error when process-memory discovery or Tokio
    /// runtime construction fails.
    pub fn build(self) -> RuntimeResult<RuntimeOwner> {
        RuntimeOwner::build_validated(self.config, || {
            self.memory_limit.map_or_else(ProcessMemoryLimit::detect, Ok)
        })
    }
}

impl RuntimeOwner {
    /// Validates runtime configuration before runtime construction.
    ///
    /// # Errors
    ///
    /// Returns a deterministic contract violation without discovering system
    /// resources or starting a Tokio runtime.
    pub fn plan(config: RuntimeConfig) -> Result<RuntimeOwnerPlan, crate::RuntimeContractViolation> {
        config.validate()?;
        Ok(RuntimeOwnerPlan {
            config,
            memory_limit: None,
        })
    }

    /// Builds an owner from the internally validated default profile.
    ///
    /// # Errors
    ///
    /// Returns an operational error when process-memory discovery or runtime
    /// construction fails. Use [`Self::plan`] when the caller supplies any
    /// runtime configuration or memory-limit value.
    pub fn new() -> RuntimeResult<Self> {
        Self::plan(RuntimeConfig::default())
            .expect("RuntimeConfig::default always satisfies its validation contract")
            .build()
    }

    fn build_validated<F>(config: RuntimeConfig, detector: F) -> RuntimeResult<Self>
    where
        F: FnOnce() -> RuntimeResult<ProcessMemoryLimit>,
    {
        let memory_limit = detector()?;
        let resources = RuntimeResources::from_memory_limit(memory_limit);

        let mut builder = tokio::runtime::Builder::new_multi_thread();
        builder
            .worker_threads(config.worker_threads)
            .max_blocking_threads(config.max_blocking_threads)
            .thread_name(config.thread_name.clone())
            .thread_keep_alive(config.thread_keep_alive);
        if let Some(thread_stack_size) = config.thread_stack_size {
            builder.thread_stack_size(thread_stack_size);
        }
        if config.enable_io {
            builder.enable_io();
        }
        if config.enable_time {
            builder.enable_time();
        }

        let runtime = build_tokio_runtime(&mut builder)?;
        let runtime_handle = RuntimeHandle::new(runtime.handle().clone());
        let root_group = TaskGroup::root(config.thread_name.clone(), runtime_handle.clone());
        let diagnostics = RuntimeDiagnostics::new();
        let root_context = RootServiceContext::new(
            config.thread_name.clone().into(),
            runtime_handle,
            root_group,
            config.blocking_lane_policies.clone(),
            config.max_blocking_threads,
            diagnostics,
            resources.clone(),
        );
        Ok(Self {
            config,
            runtime: Some(runtime),
            resources,
            root_context,
        })
    }

    /// Returns the root context used to derive owned component scopes.
    pub fn root_context(&self) -> &RootServiceContext {
        &self.root_context
    }

    /// Returns the validated runtime configuration.
    pub fn config(&self) -> &RuntimeConfig {
        &self.config
    }

    /// Returns the process-wide resource capabilities.
    pub fn resources(&self) -> &RuntimeResources {
        &self.resources
    }

    /// Blocks the current thread until `future` completes.
    pub fn block_on<F>(&self, future: F) -> F::Output
    where
        F: Future,
    {
        self.runtime
            .as_ref()
            .expect("runtime owner must still own the runtime")
            .block_on(future)
    }

    /// Cancels and awaits tracked tasks until the configured deadline.
    pub async fn shutdown_tasks(&self) -> ShutdownReport {
        self.shutdown_tasks_until(ShutdownDeadline::after(self.config.shutdown_timeout))
            .await
    }

    /// Blocks while shutting down tracked work and the Tokio runtime.
    ///
    /// # Errors
    ///
    /// Returns an error when invoked from inside a Tokio runtime.
    pub fn shutdown_runtime_blocking(self) -> RuntimeResult<ShutdownReport> {
        let timeout = self.config.shutdown_timeout;
        self.shutdown_runtime_blocking_with_timeout(timeout)
    }

    /// Cancels tracked work and asks Tokio to finish runtime shutdown in the background.
    ///
    /// The returned report captures only the immediate task-group shutdown
    /// state; background Tokio work is not awaited by this method.
    pub fn shutdown_background(mut self) -> ShutdownReport {
        let report = self.shutdown_tasks_now();
        report.log_if_unhealthy();
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_background();
        }
        report
    }

    /// Blocks while shutting down tracked work within `timeout`.
    ///
    /// # Errors
    ///
    /// Returns an error when invoked from inside a Tokio runtime.
    pub fn shutdown_runtime_blocking_with_timeout(self, timeout: std::time::Duration) -> RuntimeResult<ShutdownReport> {
        self.shutdown_runtime_blocking_until(ShutdownDeadline::after(timeout))
    }

    /// Shuts down tracked work and the Tokio runtime using an existing absolute deadline.
    ///
    /// This is the process-entrypoint boundary for a deadline already frozen by
    /// [`crate::ServiceLifecycle`]. It never grants a new timeout to the runtime layer.
    ///
    /// # Errors
    ///
    /// Returns an unsupported-operation runtime failure when called from an
    /// asynchronous Tokio context.
    pub fn shutdown_runtime_blocking_until(mut self, deadline: ShutdownDeadline) -> RuntimeResult<ShutdownReport> {
        if tokio::runtime::Handle::try_current().is_ok() {
            return Err(RuntimeError::unsupported(
                crate::RuntimeOperation::ShutdownRuntimeBlocking,
            ));
        }

        let runtime = self.runtime.take().expect("runtime owner must still own the runtime");
        let report = runtime.block_on(self.shutdown_tasks_until(deadline));
        report.log_if_unhealthy();
        runtime.shutdown_timeout(deadline.remaining());
        Ok(report)
    }
}

fn build_tokio_runtime(builder: &mut tokio::runtime::Builder) -> RuntimeResult<tokio::runtime::Runtime> {
    builder
        .build()
        .map_err(|source| RuntimeError::build(crate::RuntimeOperation::BuildTokioRuntime, source))
}

impl Drop for RuntimeOwner {
    fn drop(&mut self) {
        if let Some(runtime) = self.runtime.take() {
            if self.root_context.task_group().lifecycle_state() != TaskGroupLifecycleState::ShutdownCompleted {
                let report = self.shutdown_tasks_now();
                tracing::warn!(
                    report = %report.to_json(),
                    "RuntimeOwner dropped before root TaskGroup shutdown completed"
                );
            }
            runtime.shutdown_background();
        }
    }
}

impl RuntimeOwner {
    /// Cancels and joins all owned tasks using an existing absolute deadline.
    ///
    /// This keeps nested lifecycle owners on the caller's single shutdown
    /// budget without consuming or destroying the owned Tokio runtime.
    pub async fn shutdown_tasks_until(&self, deadline: ShutdownDeadline) -> ShutdownReport {
        let mut report = self.root_context.task_group().shutdown_until(deadline).await;
        for snapshot in self.root_context.blocking_snapshots() {
            report.merge_blocking(snapshot);
        }
        report
    }

    fn shutdown_tasks_now(&self) -> ShutdownReport {
        let mut report = self.root_context.task_group().shutdown_now();
        for snapshot in self.root_context.blocking_snapshots() {
            report.merge_blocking(snapshot);
        }
        report
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_rejects_invalid_configuration_before_resource_discovery() {
        let invalid = RuntimeConfig {
            worker_threads: 0,
            ..RuntimeConfig::default()
        };

        assert!(matches!(
            RuntimeOwner::plan(invalid),
            Err(crate::RuntimeContractViolation::InvalidConfiguration {
                policy: crate::RuntimeContractPolicy::WorkerThreadsPositive,
            })
        ));
    }

    #[test]
    fn validated_plan_builds_with_a_supplied_memory_limit() {
        let owner = RuntimeOwner::plan(RuntimeConfig::default())
            .expect("default runtime configuration is valid")
            .with_memory_limit(ProcessMemoryLimit::configured(4 * 1024 * 1024).expect("test limit is valid"))
            .build()
            .expect("runtime owner");

        assert_eq!(owner.resources().memory_limit().bytes(), 4 * 1024 * 1024);
    }

    #[test]
    fn tokio_builder_uses_the_production_build_helper() {
        let mut builder = tokio::runtime::Builder::new_multi_thread();
        builder.worker_threads(1).max_blocking_threads(1).enable_all();

        let runtime = build_tokio_runtime(&mut builder).expect("valid Tokio builder should start");

        runtime.block_on(async {});
    }
}
