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

use rocketmq_error::RocketMQError;
use rocketmq_runtime::BlockingExecutor;
use rocketmq_runtime::BlockingExecutorSnapshot;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_runtime::ShutdownDeadline;
use rocketmq_runtime::ShutdownReport;
use rocketmq_runtime::TaskGroup;
use rocketmq_runtime::TaskId;
use rocketmq_runtime::TaskKind;

#[derive(Debug, Clone)]
pub struct RocksDbRuntimeScope {
    service_context: ChildServiceContext,
    blocking_executor: BlockingExecutor,
}

impl RocksDbRuntimeScope {
    pub fn new(service_context: ChildServiceContext) -> Self {
        Self {
            service_context: service_context.clone(),
            blocking_executor: service_context.storage_io().clone(),
        }
    }

    pub async fn spawn_io<F, R>(&self, name: &'static str, operation: F) -> Result<R, RocketMQError>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        self.blocking_executor
            .spawn_io(name, operation)
            .await
            .map_err(|error| RocketMQError::storage_write_failed("rocksdb", format!("{name}: {error}")))
    }

    /// Runs a RocksDB I/O operation without admitting or waiting past `deadline`.
    pub async fn spawn_io_until<F, R>(
        &self,
        name: &'static str,
        deadline: ShutdownDeadline,
        operation: F,
    ) -> Result<R, RocketMQError>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        self.blocking_executor
            .spawn_io_until(name, deadline, operation)
            .await
            .map_err(|error| RocketMQError::storage_write_failed("rocksdb", format!("{name}: {error}")))
    }

    pub fn task_group(&self, name: &'static str) -> TaskGroup {
        self.service_context.component(name).task_group().clone()
    }

    pub fn blocking_snapshot(&self) -> BlockingExecutorSnapshot {
        self.blocking_executor.snapshot()
    }

    pub fn spawn_background_io<F>(&self, name: &'static str, operation: F) -> Result<TaskId, RocketMQError>
    where
        F: FnOnce() + Send + 'static,
    {
        let executor = self.blocking_executor.clone();
        let task_group = self.service_context.task_group();
        task_group
            .spawn(name, TaskKind::Worker, async move {
                if let Err(error) = executor.spawn_io(name, operation).await {
                    tracing::warn!(error = %error, task_name = name, "rocksdb background blocking task failed");
                }
            })
            .map_err(|error| RocketMQError::storage_write_failed("rocksdb", format!("{name}: {error}")))
    }
}

pub async fn spawn_io<F, R>(scope: &RocksDbRuntimeScope, name: &'static str, operation: F) -> Result<R, RocketMQError>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    scope.spawn_io(name, operation).await
}

pub async fn spawn_io_until<F, R>(
    scope: &RocksDbRuntimeScope,
    name: &'static str,
    deadline: ShutdownDeadline,
    operation: F,
) -> Result<R, RocketMQError>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    scope.spawn_io_until(name, deadline, operation).await
}

pub fn spawn_background_io<F>(
    scope: &RocksDbRuntimeScope,
    name: &'static str,
    operation: F,
) -> Result<TaskId, RocketMQError>
where
    F: FnOnce() + Send + 'static,
{
    scope.spawn_background_io(name, operation)
}

pub fn task_group(scope: &RocksDbRuntimeScope, name: &'static str) -> TaskGroup {
    scope.task_group(name)
}

pub fn shutdown_report_result(component: &'static str, report: ShutdownReport) -> Result<(), RocketMQError> {
    report
        .assert_no_task_leak()
        .map_err(|error| RocketMQError::storage_write_failed("rocksdb", format!("{component}: {error}")))
}
