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

//! An execution through the production session compares the plan it names before it writes.

use serde_json::json;

use super::*;
use crate::model::ControlOperation;
use crate::tool_runtime::plan_binding::plan_digest;

/// One call of `operation` on the targets that the backend of these tests serves.
fn request(operation: ControlOperation, dry_run: bool, expected_plan_digest: Option<&str>) -> MutationToolRequest {
    let mut arguments = match operation {
        ControlOperation::ConsumerOffsetReset => json!({
            "topic": "orders",
            "consumer_group": "orders-consumer",
            "timestamp": "2026-08-30T08:00:00+08:00"
        }),
        ControlOperation::BrokerConfigPatch => json!({
            "broker_name": "broker-a",
            "properties": {"traceTopicEnable": "true"}
        }),
        ControlOperation::ConsumerRequestMode => json!({
            "topic": "orders",
            "consumer_group": "orders-consumer",
            "mode": "pop",
            "pop_share_queue_num": 4,
            "timeout_millis": 12_345
        }),
        other => panic!("{other:?} has tests of its own"),
    };
    arguments["schema_version"] = json!(crate::model::MUTATION_ARGUMENTS_SCHEMA_VERSION);
    arguments["cluster"] = json!("cluster-a");
    arguments["dry_run"] = json!(dry_run);
    arguments["confirm"] = json!(!dry_run);
    arguments["reason"] = json!("approved operation");
    arguments["expected_plan_digest"] = json!(expected_plan_digest);
    match operation {
        ControlOperation::ConsumerOffsetReset => {
            MutationToolRequest::ConsumerOffset(serde_json::from_value(arguments).unwrap())
        }
        ControlOperation::BrokerConfigPatch => {
            MutationToolRequest::BrokerConfig(serde_json::from_value(arguments).unwrap())
        }
        _ => MutationToolRequest::ConsumerRequestMode(serde_json::from_value(arguments).unwrap()),
    }
}

#[tokio::test]
async fn every_execution_compares_its_plan_before_the_first_write() {
    let evidence = Arc::new(Evidence::default());
    let mut session = AdminMutationToolSession::new(ProductionPathBackend {
        evidence: Arc::clone(&evidence),
    });
    let writes = |operation| {
        let counter = match operation {
            ControlOperation::ConsumerOffsetReset => &evidence.offset_executes,
            ControlOperation::BrokerConfigPatch => &evidence.broker_executes,
            _ => &evidence.request_mode_executes,
        };
        counter.load(Ordering::SeqCst)
    };

    for operation in [
        ControlOperation::ConsumerOffsetReset,
        ControlOperation::BrokerConfigPatch,
        ControlOperation::ConsumerRequestMode,
    ] {
        let plan = request(operation, true, None);
        let requested = plan.requested_digest().unwrap();
        let planned = session.run(plan).await.unwrap();
        let reviewed = plan_digest(&requested, &planned).unwrap();

        // The plan of another state or request is not the one read now: nothing is written.
        let other = crate::audit::sha256_hex(reviewed.as_bytes());
        let refused = session.run(request(operation, false, Some(&other))).await;
        assert_eq!(
            refused.unwrap_err(),
            ControlError::precondition_conflict(),
            "{operation:?}"
        );
        assert_eq!(writes(operation), 0, "{operation:?}");

        // The plan that the dry run reported is: the execution goes on to write.
        session.run(request(operation, false, Some(&reviewed))).await.unwrap();
        assert_eq!(writes(operation), 1, "{operation:?}");

        // Without a plan an execution runs on what it reads, as it always did.
        session.run(request(operation, false, None)).await.unwrap();
        assert_eq!(writes(operation), 2, "{operation:?}");
    }
}
