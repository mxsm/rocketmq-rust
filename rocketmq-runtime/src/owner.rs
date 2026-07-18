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
use crate::context::RuntimeContext;
use crate::error::RuntimeError;
use crate::error::RuntimeResult;
use crate::handle::RuntimeHandle;
use crate::shutdown_deadline::ShutdownDeadline;
use crate::shutdown_report::ShutdownReport;
use crate::task_group::TaskGroupLifecycleState;

pub struct RuntimeOwner {
    config: RuntimeConfig,
    runtime: Option<tokio::runtime::Runtime>,
    context: RuntimeContext,
}

impl RuntimeOwner {
    pub fn new(config: RuntimeConfig) -> RuntimeResult<Self> {
        config.validate()?;

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

        let runtime = builder.build()?;
        let context = RuntimeContext::new_with_blocking_policy(
            RuntimeHandle::new(runtime.handle().clone()),
            config.thread_name.clone(),
            config.blocking_pool_policy.clone(),
        )?;
        Ok(Self {
            config,
            runtime: Some(runtime),
            context,
        })
    }

    pub fn context(&self) -> &RuntimeContext {
        &self.context
    }

    pub fn config(&self) -> &RuntimeConfig {
        &self.config
    }

    pub fn block_on<F>(&self, future: F) -> F::Output
    where
        F: Future,
    {
        self.runtime
            .as_ref()
            .expect("runtime owner must still own the runtime")
            .block_on(future)
    }

    pub async fn shutdown_tasks(&self) -> ShutdownReport {
        self.context
            .shutdown_tasks_until(ShutdownDeadline::after(self.config.shutdown_timeout))
            .await
    }

    pub fn shutdown_runtime_blocking(self) -> RuntimeResult<ShutdownReport> {
        let timeout = self.config.shutdown_timeout;
        self.shutdown_runtime_blocking_with_timeout(timeout)
    }

    pub fn shutdown_background(mut self) -> ShutdownReport {
        let report = self.context.shutdown_tasks_now();
        report.log_if_unhealthy();
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_background();
        }
        report
    }

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
    /// Returns [`RuntimeError::InsideTokioRuntime`] when called from an asynchronous
    /// Tokio context.
    pub fn shutdown_runtime_blocking_until(mut self, deadline: ShutdownDeadline) -> RuntimeResult<ShutdownReport> {
        if tokio::runtime::Handle::try_current().is_ok() {
            return Err(RuntimeError::InsideTokioRuntime("shutdown_runtime_blocking"));
        }

        let runtime = self.runtime.take().expect("runtime owner must still own the runtime");
        let report = runtime.block_on(self.context.shutdown_tasks_until(deadline));
        report.log_if_unhealthy();
        runtime.shutdown_timeout(deadline.remaining());
        Ok(report)
    }
}

impl Drop for RuntimeOwner {
    fn drop(&mut self) {
        if let Some(runtime) = self.runtime.take() {
            if self.context.root_group().lifecycle_state() != TaskGroupLifecycleState::ShutdownCompleted {
                let report = self.context.shutdown_tasks_now();
                tracing::warn!(
                    report = %report.to_json(),
                    "RuntimeOwner dropped before root TaskGroup shutdown completed"
                );
            }
            runtime.shutdown_background();
        }
    }
}
