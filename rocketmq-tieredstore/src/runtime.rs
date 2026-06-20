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
use rocketmq_runtime::RuntimeHandle;
use rocketmq_runtime::ShutdownReport;
use rocketmq_runtime::TaskGroup;

pub(crate) fn task_group(name: &'static str) -> Result<TaskGroup, RocketMQError> {
    let handle = tokio::runtime::Handle::try_current()
        .map_err(|error| RocketMQError::Internal(format!("{name} requires a Tokio runtime: {error}")))?;
    Ok(TaskGroup::root(name, RuntimeHandle::new(handle)))
}

pub(crate) fn shutdown_report_result(component: &'static str, report: ShutdownReport) -> Result<(), RocketMQError> {
    report
        .assert_no_task_leak()
        .map_err(|error| RocketMQError::Internal(format!("{component} shutdown failed: {error}")))
}
