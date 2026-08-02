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

use std::collections::BTreeSet;
use std::sync::Arc;

use rocketmq_runtime::common::time_utils::current_millis;
use rocketmq_security_api::MaintenanceAuthorizationGrant;
use tokio::sync::Mutex;

use std::sync::Mutex as StdMutex;

use rocketmq_security_api::MaintenanceAuthorizationContext;
use rocketmq_security_api::MaintenanceAuthorizer;
use rocketmq_security_api::MaintenancePolicy;
use rocketmq_security_api::MaintenancePrincipalBinding;
use rocketmq_security_api::MaintenanceRequestClass;
use rocketmq_security_api::MaintenanceResourceBudget;
use rocketmq_security_api::MaintenanceRole;
use rocketmq_security_api::MaintenanceRoleGrant;
use rocketmq_security_api::MAINTENANCE_POLICY_SCHEMA_VERSION;

use super::coordinator::INVALID_REQUEST_REASON_SHA256;
use super::*;

#[derive(Default)]
struct RecordingAuditSink {
    records: StdMutex<Vec<MembershipAuditRecord>>,
}

impl MembershipAuditSink for RecordingAuditSink {
    fn record(&self, record: &MembershipAuditRecord) {
        self.records.lock().expect("audit lock").push(record.clone());
    }
}

struct MockMembershipPort {
    membership: Mutex<ConsensusMembership>,
    mutations: StdMutex<usize>,
    reads: StdMutex<usize>,
    failed_reads: StdMutex<BTreeSet<usize>>,
}

impl MockMembershipPort {
    fn new(caught_up: BTreeSet<u64>) -> Self {
        let nodes = [
            ConsensusNode::new(1, "127.0.0.1:60111").expect("node 1"),
            ConsensusNode::new(2, "127.0.0.1:60112").expect("node 2"),
        ]
        .into_iter()
        .map(|node| (node.node_id(), node))
        .collect();
        Self {
            membership: Mutex::new(ConsensusMembership::new(
                7,
                Some(1),
                BTreeSet::from([1]),
                BTreeSet::from([2]),
                nodes,
                caught_up,
            )),
            mutations: StdMutex::new(0),
            reads: StdMutex::new(0),
            failed_reads: StdMutex::new(BTreeSet::new()),
        }
    }

    fn mutation_count(&self) -> usize {
        *self.mutations.lock().expect("mutation lock")
    }

    fn fail_read(&self, attempt: usize) {
        self.failed_reads.lock().expect("failed reads lock").insert(attempt);
    }
}

impl ConsensusMembershipPort for MockMembershipPort {
    async fn current_membership(&self) -> Result<ConsensusMembership> {
        let attempt = {
            let mut reads = self.reads.lock().expect("read lock");
            *reads += 1;
            *reads
        };
        if self.failed_reads.lock().expect("failed reads lock").contains(&attempt) {
            return Err(ControllerError::StorageError(
                "injected membership read failure".to_string(),
            ));
        }
        Ok(self.membership.lock().await.clone())
    }

    async fn add_caught_up_learner(&self, node: &ConsensusNode) -> Result<()> {
        *self.mutations.lock().expect("mutation lock") += 1;
        let mut membership = self.membership.lock().await;
        membership.version += 1;
        membership.learners.insert(node.node_id);
        membership.nodes.insert(node.node_id, node.clone());
        membership.caught_up.insert(node.node_id);
        Ok(())
    }

    async fn change_voters(&self, voters: BTreeSet<u64>) -> Result<()> {
        *self.mutations.lock().expect("mutation lock") += 1;
        let mut membership = self.membership.lock().await;
        membership.version += 1;
        membership.learners.retain(|node_id| !voters.contains(node_id));
        membership.voters = voters;
        let retained_nodes = membership
            .voters
            .union(&membership.learners)
            .copied()
            .collect::<BTreeSet<_>>();
        membership.nodes.retain(|node_id, _| retained_nodes.contains(node_id));
        Ok(())
    }

    async fn remove_learner(&self, node_id: u64) -> Result<()> {
        *self.mutations.lock().expect("mutation lock") += 1;
        let mut membership = self.membership.lock().await;
        membership.version += 1;
        membership.learners.remove(&node_id);
        membership.nodes.remove(&node_id);
        membership.caught_up.remove(&node_id);
        Ok(())
    }
}

fn authorization() -> MaintenanceAuthorizationGrant {
    let policy = MaintenancePolicy {
        schema_version: MAINTENANCE_POLICY_SCHEMA_VERSION,
        policy_id: "controller.membership-tests".to_string(),
        policy_version: 9,
        require_authentication: true,
        require_authorization: true,
        require_fencing_token: true,
        max_request_lifetime_millis: 30_000,
        resource_budget: MaintenanceResourceBudget {
            max_checkpoint_bytes: 1_024,
            max_store_members: 3,
            max_concurrent_operations: 1,
        },
        principal_bindings: vec![MaintenancePrincipalBinding {
            principal: "release-operator".to_string(),
            roles: BTreeSet::from([MaintenanceRole::ReleaseOperator]),
        }],
        role_grants: vec![MaintenanceRoleGrant {
            role: MaintenanceRole::ReleaseOperator,
            capabilities: BTreeSet::from([MaintenanceCapability::ReleaseCheckpoint]),
        }],
    };
    let authorizer = MaintenanceAuthorizer::new(policy.into_validated().expect("policy"));
    authorizer
        .authorize(
            Some(&MaintenanceAuthorizationContext {
                authentication_enabled: true,
                authorization_enabled: true,
                principal: Some("release-operator".to_string()),
                request_class: MaintenanceRequestClass::PrivilegedMaintenance,
                capability: MaintenanceCapability::ReleaseCheckpoint,
                deadline_unix_millis: current_millis() + 10_000,
                fencing_token: Some(41),
            }),
            current_millis(),
        )
        .expect("grant")
}

fn promote_request(operation_id: &str, node_id: u64) -> MembershipChangeRequest {
    MembershipChangeRequest::new(
        operation_id,
        7,
        MembershipChange::PromoteVoter { node_id },
        "promote caught-up learner",
    )
    .expect("request")
}

#[tokio::test]
async fn repeated_operation_id_replays_without_second_consensus_mutation() {
    let sink = Arc::new(RecordingAuditSink::default());
    let coordinator = MembershipChangeCoordinator::new(sink.clone());
    let port = MockMembershipPort::new(BTreeSet::from([1, 2]));
    let membership_authorization = authorization();
    let request = promote_request("promote-node-2", 2);

    let applied = coordinator
        .apply(&port, &membership_authorization, request.clone())
        .await
        .expect("apply");
    let replayed = coordinator
        .apply(&port, &membership_authorization, request)
        .await
        .expect("replay");

    assert_eq!(applied.disposition(), MembershipChangeDisposition::Applied);
    assert_eq!(replayed.disposition(), MembershipChangeDisposition::Replayed);
    assert_eq!(port.mutation_count(), 1);
    let records = sink.records.lock().expect("audit lock");
    assert_eq!(records.len(), 2);
    assert_eq!(
        records[0].authorization_capability(),
        MaintenanceCapability::ReleaseCheckpoint
    );
    assert_eq!(records[1].outcome(), MembershipAuditOutcome::Replayed);
}

#[tokio::test]
async fn reused_operation_id_with_different_payload_is_rejected_and_audited() {
    let sink = Arc::new(RecordingAuditSink::default());
    let coordinator = MembershipChangeCoordinator::new(sink.clone());
    let port = MockMembershipPort::new(BTreeSet::from([1, 2]));
    let authorization = authorization();
    coordinator
        .apply(&port, &authorization, promote_request("promote-node", 2))
        .await
        .expect("apply");

    let error = coordinator
        .apply(&port, &authorization, promote_request("promote-node", 3))
        .await
        .expect_err("conflicting idempotency payload");

    assert!(error.to_string().contains("different request"));
    assert_eq!(port.mutation_count(), 1);
    let records = sink.records.lock().expect("audit lock");
    assert_eq!(records[1].decision(), "operation_id_conflict");
}

#[tokio::test]
async fn release_checkpoint_grant_is_the_temporary_membership_permission() {
    let sink = Arc::new(RecordingAuditSink::default());
    let coordinator = MembershipChangeCoordinator::new(sink.clone());
    let port = MockMembershipPort::new(BTreeSet::from([1, 2]));

    let outcome = coordinator
        .apply(
            &port,
            &authorization(),
            promote_request("release-authorized-promote", 2),
        )
        .await
        .expect("temporary release authorization");

    assert_eq!(outcome.disposition(), MembershipChangeDisposition::Applied);
    assert_eq!(port.mutation_count(), 1);
    assert_eq!(
        sink.records.lock().expect("audit lock")[0].authorization_capability(),
        MaintenanceCapability::ReleaseCheckpoint
    );
}

#[tokio::test]
async fn learner_must_be_caught_up_before_promotion() {
    let sink = Arc::new(RecordingAuditSink::default());
    let coordinator = MembershipChangeCoordinator::new(sink.clone());
    let port = MockMembershipPort::new(BTreeSet::from([1]));

    let error = coordinator
        .apply(&port, &authorization(), promote_request("premature-promote", 2))
        .await
        .expect_err("learner is behind");

    assert!(error.to_string().contains("committed log frontier"));
    assert_eq!(port.mutation_count(), 0);
    assert_eq!(
        sink.records.lock().expect("audit lock")[0].decision(),
        "learner_not_caught_up"
    );
}

#[tokio::test]
async fn deserialized_request_is_revalidated_before_mutation() {
    let sink = Arc::new(RecordingAuditSink::default());
    let coordinator = MembershipChangeCoordinator::new(sink.clone());
    let port = MockMembershipPort::new(BTreeSet::from([1, 2]));
    let request: MembershipChangeRequest = serde_json::from_value(serde_json::json!({
        "operation_id": "invalid-address",
        "expected_membership_version": 7,
        "change": {
            "add_learner": {
                "node": {
                    "node_id": 3,
                    "rpc_addr": "0.0.0.0:0"
                }
            }
        },
        "reason": "exercise DTO decode path"
    }))
    .expect("serde accepts the DTO shape before domain validation");

    let error = coordinator
        .apply(&port, &authorization(), request)
        .await
        .expect_err("apply must revalidate deserialized DTOs");

    assert!(error.to_string().contains("port must be greater than zero"));
    assert_eq!(port.mutation_count(), 0);
    let records = sink.records.lock().expect("audit lock");
    assert_eq!(records[0].decision(), "invalid_request");
    assert_eq!(records[0].operation_id(), "<invalid>");
    assert_eq!(records[0].reason_sha256, INVALID_REQUEST_REASON_SHA256);
}

#[tokio::test]
async fn invalid_request_audit_does_not_project_unvalidated_strings() {
    let sink = Arc::new(RecordingAuditSink::default());
    let coordinator = MembershipChangeCoordinator::new(sink.clone());
    let port = MockMembershipPort::new(BTreeSet::from([1, 2]));
    let request: MembershipChangeRequest = serde_json::from_value(serde_json::json!({
        "operation_id": "attacker\noperation",
        "expected_membership_version": 7,
        "change": { "promote_voter": { "node_id": 2 } },
        "reason": "x".repeat(16_384)
    }))
    .expect("serde accepts the DTO shape before domain validation");

    coordinator
        .apply(&port, &authorization(), request)
        .await
        .expect_err("invalid operation id must fail before mutation");

    let records = sink.records.lock().expect("audit lock");
    assert_eq!(records[0].operation_id(), "<invalid>");
    assert_eq!(records[0].reason_sha256, INVALID_REQUEST_REASON_SHA256);
    assert_eq!(records[0].decision(), "invalid_request");
    assert_eq!(port.mutation_count(), 0);
}

#[tokio::test]
async fn verification_read_failure_is_pending_and_same_request_reconciles() {
    let sink = Arc::new(RecordingAuditSink::default());
    let coordinator = MembershipChangeCoordinator::new(sink.clone());
    let port = MockMembershipPort::new(BTreeSet::from([1, 2]));
    port.fail_read(2);
    let request = promote_request("uncertain-promote", 2);

    coordinator
        .apply(&port, &authorization(), request.clone())
        .await
        .expect_err("verification read is injected to fail");
    let replayed = coordinator
        .apply(&port, &authorization(), request)
        .await
        .expect("same operation reconciles committed desired state");

    assert_eq!(replayed.disposition(), MembershipChangeDisposition::Replayed);
    assert_eq!(port.mutation_count(), 1);
    let records = sink.records.lock().expect("audit lock");
    assert_eq!(records[0].outcome(), MembershipAuditOutcome::Pending);
    assert_eq!(records[0].decision(), "verification_read_failed");
    assert_eq!(records[1].decision(), "recovered_after_uncertain_commit");
}

#[tokio::test]
async fn initial_membership_read_failure_is_rejected_and_audited() {
    let sink = Arc::new(RecordingAuditSink::default());
    let coordinator = MembershipChangeCoordinator::new(sink.clone());
    let port = MockMembershipPort::new(BTreeSet::from([1, 2]));
    port.fail_read(1);

    coordinator
        .apply(&port, &authorization(), promote_request("read-failure", 2))
        .await
        .expect_err("initial read is injected to fail");

    assert_eq!(port.mutation_count(), 0);
    let records = sink.records.lock().expect("audit lock");
    assert_eq!(records[0].outcome(), MembershipAuditOutcome::Rejected);
    assert_eq!(records[0].decision(), "membership_read_failed");
}
