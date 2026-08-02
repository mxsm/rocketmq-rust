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
use rocketmq_runtime::BudgetLimit;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_runtime::FullPolicy;
use rocketmq_runtime::ResourceBudget;
use rocketmq_runtime::ShutdownDeadline;
use rocketmq_runtime::ShutdownReport;
use rocketmq_runtime::TaskGroup;
use rocketmq_runtime::TaskId;
use rocketmq_runtime::TaskKind;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone)]
pub(crate) struct StoreRuntimeScope {
    service_context: ChildServiceContext,
    blocking_executor: BlockingExecutor,
    resource_budget: ResourceBudget,
    mapped_file_allocation_budget: ResourceBudget,
    group_commit_budget: ResourceBudget,
}

impl StoreRuntimeScope {
    pub(crate) fn new(service_context: ChildServiceContext) -> Self {
        const STORE_ITEM_LIMIT: usize = usize::MAX;
        const MAPPED_FILE_ALLOCATION_ITEM_LIMIT: usize = 1_024;
        const GROUP_COMMIT_ITEM_LIMIT: usize = 1_024;

        let process_budget = service_context.process_budget();
        let managed_bytes = process_budget.limit().capacity.bytes;
        // These static names and limits are constrained to the already
        // validated RuntimeOwner root, so child construction cannot fail.
        let resource_budget = process_budget
            .child(
                "store",
                BudgetLimit::new(STORE_ITEM_LIMIT, managed_bytes, FullPolicy::Reject),
            )
            .expect("Store budget must fit the RuntimeOwner process budget");
        let mapped_file_allocation_budget = resource_budget
            .child(
                "mapped-file-allocation",
                BudgetLimit::new(MAPPED_FILE_ALLOCATION_ITEM_LIMIT, managed_bytes, FullPolicy::Reject),
            )
            .expect("mapped-file allocation budget must fit the Store budget");
        let group_commit_budget = resource_budget
            .child(
                "group-commit",
                BudgetLimit::new(GROUP_COMMIT_ITEM_LIMIT, managed_bytes, FullPolicy::WaitUntilDeadline),
            )
            .expect("group-commit budget must fit the Store budget");
        tracing::info!(
            store_budget_bytes = managed_bytes,
            mapped_file_allocation_items = MAPPED_FILE_ALLOCATION_ITEM_LIMIT,
            group_commit_items = GROUP_COMMIT_ITEM_LIMIT,
            "initialized RuntimeOwner-derived Store queue budgets"
        );
        Self {
            service_context: service_context.clone(),
            blocking_executor: service_context.storage_io().clone(),
            resource_budget,
            mapped_file_allocation_budget,
            group_commit_budget,
        }
    }

    pub(crate) fn resource_budget(&self) -> ResourceBudget {
        self.resource_budget.clone()
    }

    pub(crate) fn mapped_file_allocation_budget(&self) -> ResourceBudget {
        self.mapped_file_allocation_budget.clone()
    }

    pub(crate) fn group_commit_budget(&self) -> ResourceBudget {
        self.group_commit_budget.clone()
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

    pub(crate) async fn spawn_io_until<F, R>(
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
            .map_err(|error| RocketMQError::storage_write_failed("store", format!("{name}: {error}")))
    }

    pub(crate) fn task_group(&self, name: &'static str) -> TaskGroup {
        self.service_context.component(name).task_group().clone()
    }

    pub(crate) fn child_cancellation_token(&self) -> CancellationToken {
        self.service_context.task_group().cancellation_token().child_token()
    }

    pub(crate) fn blocking_snapshot(&self) -> BlockingExecutorSnapshot {
        self.blocking_executor.snapshot()
    }
}

pub(crate) async fn spawn_io<F, R>(
    scope: &StoreRuntimeScope,
    name: &'static str,
    operation: F,
) -> Result<R, RocketMQError>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    scope.spawn_io(name, operation).await
}

pub(crate) fn spawn_background_io<F>(
    scope: &StoreRuntimeScope,
    name: &'static str,
    operation: F,
) -> Result<TaskId, RocketMQError>
where
    F: FnOnce() + Send + 'static,
{
    let executor = scope.blocking_executor.clone();
    let task_group = scope.service_context.task_group();
    task_group
        .spawn(name, TaskKind::Worker, async move {
            if let Err(error) = executor.spawn_io(name, operation).await {
                tracing::warn!(error = %error, task_name = name, "store background blocking task failed");
            }
        })
        .map_err(|error| RocketMQError::storage_write_failed("store", format!("{name}: {error}")))
}

pub(crate) fn task_group(scope: &StoreRuntimeScope, name: &'static str) -> TaskGroup {
    scope.task_group(name)
}

pub(crate) fn shutdown_report_result(component: &'static str, report: ShutdownReport) -> Result<(), RocketMQError> {
    report
        .assert_no_task_leak()
        .map_err(|error| RocketMQError::storage_write_failed("store", format!("{component}: {error}")))
}

pub(crate) fn blocking_snapshot(scope: &StoreRuntimeScope) -> BlockingExecutorSnapshot {
    scope.blocking_snapshot()
}

#[cfg(test)]
pub(crate) fn test_runtime_owner() -> &'static rocketmq_runtime::RuntimeOwner {
    use std::sync::OnceLock;

    use rocketmq_runtime::RuntimeConfig;
    use rocketmq_runtime::RuntimeOwner;

    static OWNER: OnceLock<RuntimeOwner> = OnceLock::new();
    OWNER.get_or_init(|| {
        RuntimeOwner::new(RuntimeConfig::server_default("rocketmq-store-tests"))
            .expect("store test runtime owner should start")
    })
}

#[cfg(test)]
pub(crate) fn test_service_context(name: &'static str) -> ChildServiceContext {
    test_runtime_owner().root_context().component(name)
}

#[cfg(test)]
pub(crate) fn test_scope(name: &'static str) -> StoreRuntimeScope {
    StoreRuntimeScope::new(test_service_context(name))
}

#[cfg(test)]
mod tests {
    use rocketmq_runtime::BudgetClass;

    use super::test_scope;

    #[test]
    fn store_queue_budgets_share_one_runtime_owned_parent() {
        let scope = test_scope("store-budget-tree-test");
        assert!(scope.resource_budget().path().ends_with("/store"));
        assert!(scope
            .mapped_file_allocation_budget()
            .path()
            .ends_with("/store/mapped-file-allocation"));
        assert!(scope.group_commit_budget().path().ends_with("/store/group-commit"));

        let mapped_permit = scope
            .mapped_file_allocation_budget()
            .try_acquire(64, BudgetClass::Data)
            .expect("mapped-file child budget");
        let group_permit = scope
            .group_commit_budget()
            .try_acquire(32, BudgetClass::Data)
            .expect("group-commit child budget");
        let parent = scope.resource_budget().snapshot();
        assert_eq!(parent.current_count, 2);
        assert_eq!(parent.current_bytes, 96);

        drop((mapped_permit, group_permit));
        let released = scope.resource_budget().snapshot();
        assert_eq!(released.current_count, 0);
        assert_eq!(released.current_bytes, 0);
    }
}
