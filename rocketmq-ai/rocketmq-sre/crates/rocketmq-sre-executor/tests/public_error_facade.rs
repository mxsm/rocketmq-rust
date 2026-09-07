// Copyright 2026 The RocketMQ Rust Authors
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

use std::error::Error;

use rocketmq_sre_executor::ExecutionDisabled;
use rocketmq_sre_executor::ExecutorError;
use rocketmq_sre_executor::ExecutorOperationOutcome;
use rocketmq_sre_executor::ExecutorRejection;
use rocketmq_sre_executor::reject_execution;

#[test]
fn executor_exports_one_operational_error_and_closed_rejections() {
    fn requires_error<T: Error>() {}
    fn requires_closed_outcome(_: ExecutorRejection) {}

    requires_error::<ExecutorError>();
    requires_closed_outcome(ExecutorRejection::PreconditionChanged);
    assert_eq!(
        ExecutorOperationOutcome::<()>::rejected(ExecutorRejection::PreconditionChanged),
        ExecutorOperationOutcome::Rejected(ExecutorRejection::PreconditionChanged)
    );
    assert_eq!(
        ExecutorRejection::PreconditionChanged.to_string(),
        "Change Executor operation was rejected"
    );
    assert_eq!(reject_execution(), Err(ExecutionDisabled));
}
