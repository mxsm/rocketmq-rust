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
use rocketmq_error::UnifiedServiceError;
use rocketmq_runtime::OperationContext;
use rocketmq_runtime::ShutdownReport;
use rocketmq_runtime::TaskGroup;
use rocketmq_runtime::TaskKind;

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

    pub(crate) async fn shutdown_report(&self, name: &'static str, timeout: std::time::Duration) -> ShutdownReport {
        let active_before = self.task_count();
        let joined = self
            .operation
            .cancel_and_wait(&self.task_group, timeout)
            .await
            .unwrap_or(false);
        let mut report = ShutdownReport::new(name, std::time::Duration::ZERO);
        if joined {
            report.completed = active_before;
        } else {
            report.aborted = active_before;
            report.timed_out = usize::from(active_before > 0);
        }
        report
    }
}

pub(crate) fn shutdown_report_result(component: &'static str, report: ShutdownReport) -> Result<(), RocketMQError> {
    report.assert_no_task_leak().map_err(|error| {
        RocketMQError::Service(UnifiedServiceError::ShutdownFailed(format!(
            "{component} shutdown failed: {error}"
        )))
    })
}

#[cfg(test)]
mod tests {
    use rocketmq_error::ErrorKind;
    use rocketmq_runtime::RuntimeContext;

    use super::*;

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

        assert_eq!(error.kind(), ErrorKind::Service);
        assert!(matches!(
            error,
            RocketMQError::Service(UnifiedServiceError::ShutdownFailed(_))
        ));
    }
}
