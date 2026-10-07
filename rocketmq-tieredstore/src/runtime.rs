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

use rocketmq_runtime::OperationContext;
use rocketmq_runtime::ShutdownReport;
use rocketmq_runtime::TaskGroup;
use rocketmq_runtime::TaskKind;
use rocketmq_store_api::StoreError;
use rocketmq_store_api::StoreOperation;

#[derive(Debug, Clone)]
pub(crate) struct TaskOperationOwner {
    task_group: TaskGroup,
    operation: OperationContext,
}

impl TaskOperationOwner {
    pub(crate) fn new(task_group: TaskGroup, kind: TaskKind) -> Self {
        Self {
            task_group,
            operation: OperationContext::without_deadline(kind),
        }
    }

    pub(crate) fn task_group(&self) -> &TaskGroup {
        &self.task_group
    }

    pub(crate) fn operation(&self) -> &OperationContext {
        &self.operation
    }

    pub(crate) fn task_count(&self) -> usize {
        self.operation.active_task_count()
    }

    pub(crate) fn cancel(&self) {
        self.operation.cancel();
    }

    pub(crate) async fn shutdown_report(
        &self,
        name: &'static str,
        timeout: std::time::Duration,
    ) -> Result<ShutdownReport, StoreError> {
        let active_before = self.task_count();
        let joined = self
            .operation
            .cancel_and_wait(&self.task_group, timeout)
            .await
            .map_err(|source| crate::error::runtime_error(StoreOperation::Shutdown, source))?;
        let mut report = ShutdownReport::new(name, std::time::Duration::ZERO);
        if joined {
            report.completed = active_before;
        } else {
            report.aborted = active_before;
            report.timed_out = usize::from(active_before > 0);
        }
        Ok(report)
    }
}

pub(crate) fn shutdown_report_result(_component: &'static str, report: ShutdownReport) -> Result<(), StoreError> {
    report
        .assert_no_task_leak()
        .map_err(|_| crate::error::internal_failure(StoreOperation::Shutdown))
}

pub(crate) struct PersistenceOperation {
    owner: TaskOperationOwner,
    unobserved_error: std::sync::Arc<tokio::sync::Mutex<Option<StoreError>>>,
}

impl PersistenceOperation {
    pub(crate) fn new(task_group: TaskGroup) -> Self {
        Self {
            owner: TaskOperationOwner::new(task_group, TaskKind::Service),
            unobserved_error: std::sync::Arc::new(tokio::sync::Mutex::new(None)),
        }
    }

    pub(crate) async fn run<F>(&self, future: F) -> Result<(), StoreError>
    where
        F: Future<Output = Result<(), StoreError>> + Send + 'static,
    {
        // Cancelling the caller must not drop a file replacement halfway through: Tokio's
        // blocking filesystem operations can continue after their async future is dropped.
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let unobserved_error = self.unobserved_error.clone();
        self.owner
            .task_group()
            .spawn_draining_operation(self.owner.operation(), "tieredstore-persist", async move {
                if let Err(Err(error)) = sender.send(future.await) {
                    let mut slot = unobserved_error.lock().await;
                    if slot.is_none() {
                        *slot = Some(error);
                    }
                }
            })
            .map_err(|source| crate::error::runtime_error(StoreOperation::AppendDerived, source))?;
        receiver.await.map_err(|source| {
            crate::error::source_error(
                &rocketmq_error::STORAGE_INTERNAL_FAILURE,
                StoreOperation::AppendDerived,
                source,
            )
        })?
    }

    pub(crate) async fn drain(&self) -> Result<(), StoreError> {
        // Stop producers first, then close admission and await accepted writes without
        // cancelling them. Successful shutdown makes the fixed temporary paths reusable.
        self.owner.operation().close_admission();
        let joined = self
            .owner
            .operation()
            .wait(self.owner.task_group(), std::time::Duration::from_secs(5))
            .await
            .map_err(|source| crate::error::runtime_error(StoreOperation::Shutdown, source))?;
        if !joined {
            return Err(crate::error::contract_error(
                &rocketmq_error::STORAGE_OPERATION_TIMED_OUT,
                StoreOperation::Shutdown,
            ));
        }
        match self.unobserved_error.lock().await.take() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use rocketmq_runtime::RuntimeContext;

    use super::*;

    #[tokio::test]
    async fn cancelled_persistence_caller_preserves_io_failure_for_shutdown() {
        let context = RuntimeContext::from_current("tieredstore-persistence-error-test");
        let persistence = PersistenceOperation::new(context.root_group().clone());
        let (entered_sender, entered_receiver) = tokio::sync::oneshot::channel();
        let (release_sender, release_receiver) = tokio::sync::oneshot::channel();
        let mut write = Box::pin(persistence.run(async move {
            entered_sender.send(()).expect("signal accepted write");
            release_receiver.await.expect("release accepted write");
            Err(crate::error::io_failed(
                StoreOperation::AppendDerived,
                std::io::Error::from(std::io::ErrorKind::PermissionDenied),
            ))
        }));
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            tokio::select! {
                result = &mut write => panic!("write completed before release: {result:?}"),
                result = entered_receiver => result.expect("accepted write signal"),
            }
        })
        .await
        .expect("write should start");

        drop(write);
        release_sender.send(()).expect("finish cancelled caller's write");
        let error = persistence
            .drain()
            .await
            .expect_err("shutdown must surface unobserved I/O failure");
        assert_eq!(error.descriptor(), &rocketmq_error::STORAGE_IO_FAILED);
        assert_eq!(error.operation(), StoreOperation::AppendDerived);
        assert_eq!(
            std::error::Error::source(&error)
                .and_then(|source| source.downcast_ref::<std::io::Error>())
                .map(std::io::Error::kind),
            Some(std::io::ErrorKind::PermissionDenied)
        );
        assert_eq!(context.root_group().task_count(), 0);
    }

    #[tokio::test]
    async fn operation_owner_reuses_the_injected_component_group() {
        let context = RuntimeContext::from_current("tieredstore-runtime-parent-test");
        let service = context.service_context("tieredstore-service");

        let owner = TaskOperationOwner::new(service.task_group().clone(), TaskKind::Service);

        assert_eq!(owner.task_group().id(), service.task_group().id());
        assert_eq!(owner.task_count(), 0);
        let report = service.task_group().shutdown(std::time::Duration::from_secs(1)).await;
        assert!(report.is_healthy(), "{}", report.to_json());
    }

    #[test]
    fn shutdown_report_result_maps_unhealthy_report_to_service_error() {
        let mut report = ShutdownReport::new("tieredstore-runtime-test", std::time::Duration::ZERO);
        report.leaked = 1;

        let error = shutdown_report_result("tieredstore runtime test", report)
            .expect_err("unhealthy shutdown report should fail");

        assert_eq!(error.descriptor(), &rocketmq_error::STORAGE_INTERNAL_FAILURE);
        assert_eq!(error.operation(), StoreOperation::Shutdown);
    }

    #[test]
    fn shutdown_runtime_mapping_preserves_source_without_rendering_detail() {
        let sentinel = "sensitive-shutdown-runtime-canary";
        let error = crate::error::runtime_error(
            StoreOperation::Shutdown,
            rocketmq_runtime::RuntimeError::internal(
                rocketmq_runtime::RuntimeOperation::external("tieredstore-runtime"),
                std::io::Error::other(sentinel),
            ),
        );

        assert_eq!(error.descriptor(), &rocketmq_error::STORAGE_INTERNAL_FAILURE);
        assert_eq!(error.operation(), StoreOperation::Shutdown);
        assert!(std::error::Error::source(&error)
            .and_then(|source| source.downcast_ref::<rocketmq_runtime::RuntimeError>())
            .is_some());
        let rendered = format!("{error} {error:?}");
        assert!(!rendered.contains(sentinel));
    }
}
