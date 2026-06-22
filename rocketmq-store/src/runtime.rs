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

use std::sync::OnceLock;

use rocketmq_error::RocketMQError;
use rocketmq_runtime::BlockingExecutor;
use rocketmq_runtime::BlockingExecutorSnapshot;
use rocketmq_runtime::BlockingPoolPolicy;
use rocketmq_runtime::RuntimeHandle;
use rocketmq_runtime::ShutdownReport;
use rocketmq_runtime::TaskGroup;
use rocketmq_runtime::TaskKind;

static STORE_BLOCKING: OnceLock<BlockingExecutor> = OnceLock::new();

#[derive(Debug, Clone)]
pub(crate) struct StoreRuntimeScope {
    parent_task_group: TaskGroup,
    blocking_executor: BlockingExecutor,
}

impl StoreRuntimeScope {
    pub(crate) fn new(parent_task_group: TaskGroup) -> Result<Self, RocketMQError> {
        let blocking_group = parent_task_group.child("rocketmq-store.blocking");
        let blocking_executor = BlockingExecutor::new(
            BlockingPoolPolicy {
                name: "rocketmq-store.blocking".to_string(),
                ..BlockingPoolPolicy::default()
            },
            blocking_group.child("rocketmq-store.blocking-reaper"),
        )
        .map_err(|error| RocketMQError::storage_write_failed("store", format!("blocking executor: {error}")))?;

        Ok(Self {
            parent_task_group,
            blocking_executor,
        })
    }

    pub(crate) async fn spawn_io<F, R>(&self, name: &'static str, operation: F) -> Result<R, RocketMQError>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        self.blocking_executor
            .spawn_io(name, operation)
            .await
            .map_err(|error| RocketMQError::storage_write_failed("store", format!("{name}: {error}")))
    }

    pub(crate) fn task_group(&self, name: &'static str) -> TaskGroup {
        self.parent_task_group.child(name)
    }

    pub(crate) fn blocking_snapshot(&self) -> BlockingExecutorSnapshot {
        self.blocking_executor.snapshot()
    }
}

pub(crate) async fn spawn_io<F, R>(name: &'static str, operation: F) -> Result<R, RocketMQError>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    blocking_executor()?
        .spawn_io(name, operation)
        .await
        .map_err(|error| RocketMQError::storage_write_failed("store", format!("{name}: {error}")))
}

pub(crate) fn spawn_detached_io<F>(name: &'static str, operation: F) -> Result<(), RocketMQError>
where
    F: FnOnce() + Send + 'static,
{
    let task_group = task_group(name)?;
    task_group
        .spawn_detached(name, TaskKind::Worker, async move {
            if let Err(error) = spawn_io(name, operation).await {
                tracing::warn!(error = %error, task_name = name, "store detached blocking task failed");
            }
        })
        .map_err(|error| RocketMQError::storage_write_failed("store", format!("{name}: {error}")))?;
    Ok(())
}

pub(crate) fn task_group(name: &'static str) -> Result<TaskGroup, RocketMQError> {
    let handle = tokio::runtime::Handle::try_current()
        .map_err(|error| RocketMQError::storage_write_failed("store", format!("{name}: {error}")))?;
    Ok(TaskGroup::root(name, RuntimeHandle::new(handle)))
}

pub(crate) fn shutdown_report_result(component: &'static str, report: ShutdownReport) -> Result<(), RocketMQError> {
    report
        .assert_no_task_leak()
        .map_err(|error| RocketMQError::storage_write_failed("store", format!("{component}: {error}")))
}

pub(crate) fn blocking_snapshot() -> Result<BlockingExecutorSnapshot, RocketMQError> {
    Ok(blocking_executor()?.snapshot())
}

fn blocking_executor() -> Result<BlockingExecutor, RocketMQError> {
    if let Some(executor) = STORE_BLOCKING.get() {
        return Ok(executor.clone());
    }

    let handle = tokio::runtime::Handle::try_current()
        .map_err(|error| RocketMQError::storage_write_failed("store", format!("blocking executor: {error}")))?;
    let group = TaskGroup::root("rocketmq-store.blocking", RuntimeHandle::new(handle));
    let executor = BlockingExecutor::new(
        BlockingPoolPolicy {
            name: "rocketmq-store.blocking".to_string(),
            ..BlockingPoolPolicy::default()
        },
        group.child("rocketmq-store.blocking-reaper"),
    )
    .map_err(|error| RocketMQError::storage_write_failed("store", format!("blocking executor: {error}")))?;

    if STORE_BLOCKING.set(executor.clone()).is_err() {
        return Ok(STORE_BLOCKING
            .get()
            .expect("store blocking executor must be initialized")
            .clone());
    }
    Ok(executor)
}
