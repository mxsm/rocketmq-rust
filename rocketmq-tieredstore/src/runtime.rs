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
use rocketmq_runtime::RuntimeHandle;
use rocketmq_runtime::ShutdownReport;
use rocketmq_runtime::TaskGroup;

pub(crate) fn task_group(name: &'static str) -> Result<TaskGroup, RocketMQError> {
    let handle = tokio::runtime::Handle::try_current().map_err(|error| {
        RocketMQError::Service(UnifiedServiceError::StartupFailed(format!(
            "{name} requires a Tokio runtime: {error}"
        )))
    })?;
    Ok(TaskGroup::root(name, RuntimeHandle::new(handle)))
}

pub(crate) fn task_group_with_parent(name: &'static str, parent_task_group: &TaskGroup) -> TaskGroup {
    parent_task_group.child(name)
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

    #[test]
    fn task_group_without_tokio_runtime_returns_service_error() {
        let error = match task_group("rocketmq-tieredstore.test") {
            Ok(_) => panic!("task group should require an ambient Tokio runtime"),
            Err(error) => error,
        };

        assert_eq!(error.kind(), ErrorKind::Service);
        assert!(matches!(
            error,
            RocketMQError::Service(UnifiedServiceError::StartupFailed(_))
        ));
    }

    #[tokio::test]
    async fn task_group_with_parent_creates_child_group() {
        let context = RuntimeContext::from_current("tieredstore-runtime-parent-test");
        let service = context.service_context("tieredstore-service");

        let task_group = task_group_with_parent("rocketmq-tieredstore.test", service.task_group());

        assert_eq!(task_group.parent_id(), Some(service.task_group().id()));
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
