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

use rocketmq_sre_execution_agent::ExecutionAgentError;
use rocketmq_sre_execution_agent::ExecutionAgentOperationOutcome;
use rocketmq_sre_execution_agent::ExecutionAgentRejection;

#[test]
fn execution_agent_exports_one_operational_error_and_closed_rejections() {
    fn requires_error<T: Error>() {}
    fn requires_closed_outcome(_: ExecutionAgentRejection) {}

    requires_error::<ExecutionAgentError>();
    requires_closed_outcome(ExecutionAgentRejection::FenceRejected);
    assert_eq!(
        ExecutionAgentOperationOutcome::<()>::rejected(ExecutionAgentRejection::FenceRejected),
        ExecutionAgentOperationOutcome::Rejected(ExecutionAgentRejection::FenceRejected)
    );
    assert_eq!(
        ExecutionAgentRejection::FenceRejected.to_string(),
        "Execution Agent operation was rejected"
    );
}
