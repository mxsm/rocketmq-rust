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
use rocketmq_runtime::BlockingPoolPolicy;
use rocketmq_runtime::RuntimeHandle;
use rocketmq_runtime::ShutdownReport;
use rocketmq_runtime::TaskGroup;
use rocketmq_runtime::TaskKind;

static ROCKSDB_BLOCKING: OnceLock<BlockingExecutor> = OnceLock::new();

pub(crate) async fn spawn_io<F, R>(name: &'static str, operation: F) -> Result<R, RocketMQError>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    blocking_executor()?
        .spawn_io(name, operation)
        .await
        .map_err(|error| RocketMQError::storage_write_failed("rocksdb", format!("{name}: {error}")))
}

pub(crate) fn spawn_detached_io<F>(name: &'static str, operation: F) -> Result<(), RocketMQError>
where
    F: FnOnce() + Send + 'static,
{
    let handle = tokio::runtime::Handle::try_current()
        .map_err(|error| RocketMQError::storage_write_failed("rocksdb", format!("{name}: {error}")))?;
    let task_group = TaskGroup::root(name, RuntimeHandle::new(handle));
    task_group
        .spawn_detached(name, TaskKind::Worker, async move {
            if let Err(error) = spawn_io(name, operation).await {
                tracing::warn!(error = %error, task_name = name, "rocksdb detached blocking task failed");
            }
        })
        .map_err(|error| RocketMQError::storage_write_failed("rocksdb", format!("{name}: {error}")))?;
    Ok(())
}

pub(crate) fn task_group(name: &'static str) -> Result<TaskGroup, RocketMQError> {
    let handle = tokio::runtime::Handle::try_current()
        .map_err(|error| RocketMQError::storage_write_failed("rocksdb", format!("{name}: {error}")))?;
    Ok(TaskGroup::root(name, RuntimeHandle::new(handle)))
}

pub(crate) fn shutdown_report_result(component: &'static str, report: ShutdownReport) -> Result<(), RocketMQError> {
    report
        .assert_no_task_leak()
        .map_err(|error| RocketMQError::storage_write_failed("rocksdb", format!("{component}: {error}")))
}

fn blocking_executor() -> Result<BlockingExecutor, RocketMQError> {
    if let Some(executor) = ROCKSDB_BLOCKING.get() {
        return Ok(executor.clone());
    }

    let handle = tokio::runtime::Handle::try_current()
        .map_err(|error| RocketMQError::storage_write_failed("rocksdb", format!("blocking executor: {error}")))?;
    let group = TaskGroup::root("rocketmq-store.rocksdb.blocking", RuntimeHandle::new(handle));
    let executor = BlockingExecutor::new(
        BlockingPoolPolicy {
            name: "rocketmq-store.rocksdb.blocking".to_string(),
            ..BlockingPoolPolicy::default()
        },
        group.child("rocketmq-store.rocksdb.blocking-reaper"),
    )
    .map_err(|error| RocketMQError::storage_write_failed("rocksdb", format!("blocking executor: {error}")))?;

    if ROCKSDB_BLOCKING.set(executor.clone()).is_err() {
        return Ok(ROCKSDB_BLOCKING
            .get()
            .expect("rocksdb blocking executor must be initialized")
            .clone());
    }
    Ok(executor)
}
