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

use rocketmq_controller::ConsensusNode;
use rocketmq_controller::MembershipChange;
use rocketmq_controller::MembershipChangeRequest;

#[test]
fn membership_request_uses_controller_owned_dtos() {
    let request = MembershipChangeRequest::new(
        "membership-add-node-2",
        7,
        MembershipChange::AddLearner {
            node: ConsensusNode::new(2, "127.0.0.1:60112").expect("valid consensus node"),
        },
        "replace failed controller host",
    )
    .expect("valid membership request");

    assert_eq!(request.operation_id(), "membership-add-node-2");
    assert_eq!(request.expected_membership_version(), 7);
}

#[test]
fn membership_request_rejects_unstable_operation_identity() {
    let error = MembershipChangeRequest::new(
        "contains whitespace",
        7,
        MembershipChange::PromoteVoter { node_id: 2 },
        "promote caught-up learner",
    )
    .expect_err("operation id must be canonical");

    assert!(error.to_string().contains("operation id"));
}

#[test]
fn consensus_node_rejects_non_routable_or_escaped_addresses() {
    for rpc_addr in [
        "127.0.0.1:0",
        "0.0.0.0:60112",
        "[::]:60112",
        "224.0.0.1:60112",
        "[ff02::1]:60112",
        "255.255.255.255:60112",
        "127.0.0.1:60112\n",
        "127.0.0.1%0a:60112",
        "not-a-socket-address",
    ] {
        ConsensusNode::new(2, rpc_addr).expect_err("address must be rejected");
    }
}

#[test]
fn membership_request_rejects_unbounded_or_control_character_reason() {
    for reason in ["", "line one\nline two"] {
        MembershipChangeRequest::new(
            "membership-promote-node-2",
            7,
            MembershipChange::PromoteVoter { node_id: 2 },
            reason,
        )
        .expect_err("reason must be bounded and printable");
    }

    MembershipChangeRequest::new(
        "membership-promote-node-2",
        7,
        MembershipChange::PromoteVoter { node_id: 2 },
        "x".repeat(513),
    )
    .expect_err("reason must be bounded");
}
