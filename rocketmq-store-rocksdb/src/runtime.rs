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

use rocketmq_runtime::BlockingExecutor;
use rocketmq_runtime::BlockingExecutorSnapshot;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_runtime::ShutdownDeadline;
use rocketmq_runtime::ShutdownReport;
use rocketmq_runtime::TaskGroup;
use rocketmq_runtime::TaskId;
use rocketmq_runtime::TaskKind;
use rocketmq_store_api::StoreError;
use rocketmq_store_api::StoreOperation;

use crate::error::runtime_error;

struct ShutdownReportFailure(String);

impl std::fmt::Display for ShutdownReportFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RocksDB runtime shutdown report failed")
    }
}

impl std::fmt::Debug for ShutdownReportFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShutdownReportFailure")
            .field("detail_present", &!self.0.is_empty())
            .finish()
    }
}

impl std::error::Error for ShutdownReportFailure {}

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

    pub async fn spawn_io<F, R>(
        &self,
        name: &'static str,
        owner_operation: StoreOperation,
        operation: F,
    ) -> Result<R, StoreError>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        self.blocking_executor
            .spawn_io(name, operation)
            .await
            .map_err(|source| runtime_error(owner_operation, source))
    }

    /// Runs a RocksDB I/O operation without admitting or waiting past `deadline`.
    pub async fn spawn_io_until<F, R>(
        &self,
        name: &'static str,
        owner_operation: StoreOperation,
        deadline: ShutdownDeadline,
        operation: F,
    ) -> Result<R, StoreError>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        self.blocking_executor
            .spawn_io_until(name, deadline, operation)
            .await
            .map_err(|source| runtime_error(owner_operation, source))
    }

    pub fn task_group(&self, name: &'static str) -> TaskGroup {
        self.service_context.component(name).task_group().clone()
    }

    pub fn blocking_snapshot(&self) -> BlockingExecutorSnapshot {
        self.blocking_executor.snapshot()
    }

    pub fn spawn_background_io<F>(
        &self,
        name: &'static str,
        owner_operation: StoreOperation,
        operation: F,
    ) -> Result<TaskId, StoreError>
    where
        F: FnOnce() + Send + 'static,
    {
        let executor = self.blocking_executor.clone();
        let task_group = self.service_context.task_group();
        task_group
            .spawn(name, TaskKind::Worker, async move {
                if let Err(source) = executor.spawn_io(name, operation).await {
                    let error = runtime_error(owner_operation, source);
                    tracing::warn!(
                        descriptor = ?error.descriptor().code(),
                        operation = ?error.operation(),
                        component = ?error.component(),
                        source_present = std::error::Error::source(&error).is_some(),
                        "rocksdb background blocking task failed"
                    );
                }
            })
            .map_err(|source| runtime_error(owner_operation, source))
    }
}

pub async fn spawn_io<F, R>(
    scope: &RocksDbRuntimeScope,
    name: &'static str,
    owner_operation: StoreOperation,
    operation: F,
) -> Result<R, StoreError>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    scope.spawn_io(name, owner_operation, operation).await
}

pub async fn spawn_io_until<F, R>(
    scope: &RocksDbRuntimeScope,
    name: &'static str,
    owner_operation: StoreOperation,
    deadline: ShutdownDeadline,
    operation: F,
) -> Result<R, StoreError>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    scope.spawn_io_until(name, owner_operation, deadline, operation).await
}

pub fn spawn_background_io<F>(
    scope: &RocksDbRuntimeScope,
    name: &'static str,
    owner_operation: StoreOperation,
    operation: F,
) -> Result<TaskId, StoreError>
where
    F: FnOnce() + Send + 'static,
{
    scope.spawn_background_io(name, owner_operation, operation)
}

pub fn task_group(scope: &RocksDbRuntimeScope, name: &'static str) -> TaskGroup {
    scope.task_group(name)
}

pub fn shutdown_report_result(
    _component: &'static str,
    operation: StoreOperation,
    report: ShutdownReport,
) -> Result<(), StoreError> {
    report.assert_no_task_leak().map_err(|detail| {
        StoreError::new(&rocketmq_error::STORAGE_INTERNAL_FAILURE, operation)
            .in_component(rocketmq_store_api::StoreComponent::RocksDb)
            .with_source(ShutdownReportFailure(detail))
    })
}
