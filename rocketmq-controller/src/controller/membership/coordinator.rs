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

//! Serialized membership coordinator and state-machine validation.

use std::collections::BTreeSet;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use rocketmq_error::fields;
use rocketmq_error::Error;
use rocketmq_error::ErrorContext;
use rocketmq_error::Result;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_error::CORE_INTERNAL_FAILURE;
use rocketmq_runtime::common::time_utils::current_millis;
use rocketmq_security_api::MaintenanceAuthorizationGrant;
use rocketmq_security_api::MaintenanceCapability;
use sha2::Digest;
use sha2::Sha256;
use tokio::sync::Mutex;

use crate::error::consensus_timed_out;
use crate::error::controller_internal;
use crate::error::controller_internal_by;
use crate::error::request_invalid;

use super::ConsensusMembership;
use super::ConsensusMembershipPort;
use super::ConsensusNode;
use super::MembershipAuditOutcome;
use super::MembershipAuditRecord;
use super::MembershipAuditSink;
use super::MembershipChange;
use super::MembershipChangeDisposition;
use super::MembershipChangeOutcome;
use super::MembershipChangeRequest;

const MAX_COMPLETED_MEMBERSHIP_OPERATIONS: usize = 4_096;
const MEMBERSHIP_VERIFICATION_TIMEOUT: Duration = Duration::from_secs(5);
const MEMBERSHIP_VERIFICATION_POLL_INTERVAL: Duration = Duration::from_millis(25);
pub(super) const INVALID_REQUEST_REASON_SHA256: &str =
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

struct TracingMembershipAuditSink;

impl MembershipAuditSink for TracingMembershipAuditSink {
    fn record(&self, record: &MembershipAuditRecord) {
        tracing::info!(
            event = "controller.consensus.membership_change",
            operation_id = %record.operation_id,
            principal = %record.principal,
            authorization_capability = ?record.authorization_capability,
            policy_version = record.policy_version,
            operation = ?record.operation,
            target_node_id = record.target_node_id,
            expected_membership_version = record.expected_membership_version,
            observed_membership_version = ?record.observed_membership_version,
            resulting_membership_version = ?record.resulting_membership_version,
            reason_sha256 = %record.reason_sha256,
            outcome = ?record.outcome,
            decision = %record.decision,
            "Controller consensus membership decision"
        );
    }
}

#[derive(Clone)]
enum MembershipOperationState {
    Pending {
        fingerprint: [u8; 32],
        desired: DesiredMembership,
    },
    Completed {
        fingerprint: [u8; 32],
        membership: ConsensusMembership,
    },
}

impl MembershipOperationState {
    const fn fingerprint(&self) -> &[u8; 32] {
        match self {
            Self::Pending { fingerprint, .. } | Self::Completed { fingerprint, .. } => fingerprint,
        }
    }
}

#[derive(Clone)]
enum DesiredMembership {
    CaughtUpLearner { node: ConsensusNode },
    Voter { node_id: u64 },
    Absent { node_id: u64 },
}

impl DesiredMembership {
    fn from_change(change: &MembershipChange) -> Self {
        match change {
            MembershipChange::AddLearner { node } => Self::CaughtUpLearner { node: node.clone() },
            MembershipChange::PromoteVoter { node_id } => Self::Voter { node_id: *node_id },
            MembershipChange::RemoveMember { node_id } => Self::Absent { node_id: *node_id },
        }
    }

    fn is_satisfied(&self, membership: &ConsensusMembership) -> bool {
        match self {
            Self::CaughtUpLearner { node } => {
                membership.learners.contains(&node.node_id)
                    && membership.caught_up.contains(&node.node_id)
                    && membership.nodes.get(&node.node_id) == Some(node)
            }
            Self::Voter { node_id } => membership.voters.contains(node_id) && !membership.learners.contains(node_id),
            Self::Absent { node_id } => {
                !membership.voters.contains(node_id)
                    && !membership.learners.contains(node_id)
                    && !membership.nodes.contains_key(node_id)
            }
        }
    }
}

pub(crate) struct MembershipChangeCoordinator {
    operations: Mutex<HashMap<String, MembershipOperationState>>,
    audit_sink: Arc<dyn MembershipAuditSink>,
}

impl Default for MembershipChangeCoordinator {
    fn default() -> Self {
        Self::new(Arc::new(TracingMembershipAuditSink))
    }
}

impl MembershipChangeCoordinator {
    pub(crate) fn new(audit_sink: Arc<dyn MembershipAuditSink>) -> Self {
        Self {
            operations: Mutex::new(HashMap::new()),
            audit_sink,
        }
    }

    #[cfg(test)]
    pub(super) async fn fill_idempotency_journal_for_test(&self, membership: ConsensusMembership) {
        let mut operations = self.operations.lock().await;
        for index in 0..MAX_COMPLETED_MEMBERSHIP_OPERATIONS {
            operations.insert(
                format!("completed-{index}"),
                MembershipOperationState::Completed {
                    fingerprint: [0; 32],
                    membership: membership.clone(),
                },
            );
        }
    }

    pub(crate) async fn apply<P: ConsensusMembershipPort>(
        &self,
        port: &P,
        authorization: &MaintenanceAuthorizationGrant,
        request: MembershipChangeRequest,
    ) -> RocketMQResult<MembershipChangeOutcome> {
        if let Err(error) = request.validate() {
            return self.reject_invalid_request(authorization, &request, RocketMQError::Shared(Arc::new(error)));
        }
        let fingerprint = request_fingerprint(&request).map_err(|error| RocketMQError::Shared(Arc::new(error)))?;
        self.validate_authorization(authorization, &request)?;
        let mut operations = self.operations.lock().await;
        if let Some(previous) = operations.get(request.operation_id()).cloned() {
            if previous.fingerprint() != &fingerprint {
                return self.reject(
                    authorization,
                    &request,
                    None,
                    "operation_id_conflict",
                    RocketMQError::Shared(Arc::new(request_invalid("reuse membership operation id"))),
                );
            }
            match previous {
                MembershipOperationState::Completed { membership, .. } => {
                    let audit = self.audit_record(
                        authorization,
                        &request,
                        Some(membership.version),
                        Some(membership.version),
                        MembershipAuditOutcome::Replayed,
                        "idempotent_replay",
                    );
                    self.audit_sink.record(&audit);
                    return Ok(MembershipChangeOutcome {
                        disposition: MembershipChangeDisposition::Replayed,
                        membership,
                        audit,
                    });
                }
                MembershipOperationState::Pending { desired, .. } => {
                    let membership = match self
                        .with_deadline(
                            authorization,
                            "reconcile pending Controller membership",
                            port.current_membership(),
                        )
                        .await
                    {
                        Ok(membership) => membership,
                        Err(error) => {
                            return self.record_error(
                                authorization,
                                &request,
                                None,
                                MembershipAuditOutcome::Pending,
                                "pending_state_read_failed",
                                RocketMQError::Shared(Arc::new(error)),
                            );
                        }
                    };
                    if desired.is_satisfied(&membership) {
                        operations.insert(
                            request.operation_id.clone(),
                            MembershipOperationState::Completed {
                                fingerprint,
                                membership: membership.clone(),
                            },
                        );
                        let audit = self.audit_record(
                            authorization,
                            &request,
                            Some(membership.version),
                            Some(membership.version),
                            MembershipAuditOutcome::Replayed,
                            "recovered_after_uncertain_commit",
                        );
                        self.audit_sink.record(&audit);
                        return Ok(MembershipChangeOutcome {
                            disposition: MembershipChangeDisposition::Replayed,
                            membership,
                            audit,
                        });
                    }
                    return self.record_error(
                        authorization,
                        &request,
                        Some(membership.version),
                        MembershipAuditOutcome::Pending,
                        "operation_still_pending",
                        RocketMQError::Shared(Arc::new(controller_internal("reconcile pending Controller membership"))),
                    );
                }
            }
        }
        if operations.len() >= MAX_COMPLETED_MEMBERSHIP_OPERATIONS {
            return self.reject(
                authorization,
                &request,
                None,
                "idempotency_journal_full",
                RocketMQError::Shared(Arc::new(
                    Error::new(&CORE_INTERNAL_FAILURE).with_context(
                        ErrorContext::new()
                            .with_text(fields::OPERATION_DIAGNOSTIC, "admit Controller membership operation"),
                    ),
                )),
            );
        }
        let before = match self
            .with_deadline(authorization, "read Controller membership", port.current_membership())
            .await
        {
            Ok(membership) => membership,
            Err(error) => {
                return self.record_error(
                    authorization,
                    &request,
                    None,
                    MembershipAuditOutcome::Rejected,
                    "membership_read_failed",
                    RocketMQError::Shared(Arc::new(error)),
                );
            }
        };
        if before.version != request.expected_membership_version {
            return self.reject(
                authorization,
                &request,
                Some(before.version),
                "stale_membership_version",
                RocketMQError::Shared(Arc::new(request_invalid("validate membership version"))),
            );
        }

        if let Err((decision, error)) = validate_transition(&before, &request.change) {
            return self.reject(
                authorization,
                &request,
                Some(before.version),
                decision,
                RocketMQError::Shared(Arc::new(error)),
            );
        }

        let desired = DesiredMembership::from_change(&request.change);
        operations.insert(
            request.operation_id.clone(),
            MembershipOperationState::Pending { fingerprint, desired },
        );
        let mutation = apply_transition(port, &before, &request.change);
        if let Err(error) = self
            .with_deadline(authorization, "apply Controller membership", mutation)
            .await
        {
            return self.record_error(
                authorization,
                &request,
                Some(before.version),
                MembershipAuditOutcome::Pending,
                "mutation_outcome_unknown",
                RocketMQError::Shared(Arc::new(error)),
            );
        }
        let verification_started = Instant::now();
        let verification_timeout = MEMBERSHIP_VERIFICATION_TIMEOUT.min(Duration::from_millis(
            authorization.deadline_unix_millis().saturating_sub(current_millis()),
        ));
        let after = loop {
            let observed = match self
                .with_deadline(authorization, "verify Controller membership", port.current_membership())
                .await
            {
                Ok(membership) => membership,
                Err(error) => {
                    return self.record_error(
                        authorization,
                        &request,
                        Some(before.version),
                        MembershipAuditOutcome::Pending,
                        "verification_read_failed",
                        RocketMQError::Shared(Arc::new(error)),
                    );
                }
            };
            match verify_transition(&before, &observed, &request.change) {
                Ok(()) => break observed,
                Err(error) if verification_started.elapsed() >= verification_timeout => {
                    return self.record_error(
                        authorization,
                        &request,
                        Some(observed.version),
                        MembershipAuditOutcome::Pending,
                        "verification_pending",
                        RocketMQError::Shared(Arc::new(error)),
                    );
                }
                Err(_) => tokio::time::sleep(MEMBERSHIP_VERIFICATION_POLL_INTERVAL).await,
            }
        };

        let audit = self.audit_record(
            authorization,
            &request,
            Some(before.version),
            Some(after.version),
            MembershipAuditOutcome::Applied,
            "applied",
        );
        self.audit_sink.record(&audit);
        operations.insert(
            request.operation_id.clone(),
            MembershipOperationState::Completed {
                fingerprint,
                membership: after.clone(),
            },
        );
        Ok(MembershipChangeOutcome {
            disposition: MembershipChangeDisposition::Applied,
            membership: after,
            audit,
        })
    }

    fn validate_authorization(
        &self,
        authorization: &MaintenanceAuthorizationGrant,
        request: &MembershipChangeRequest,
    ) -> RocketMQResult<()> {
        if authorization.capability() != MaintenanceCapability::ReleaseCheckpoint {
            return self.reject(
                authorization,
                request,
                None,
                "capability_denied",
                RocketMQError::authentication_failed(
                    "membership changes temporarily require the release-checkpoint maintenance capability",
                ),
            );
        }
        if authorization.deadline_unix_millis() <= current_millis() {
            return self.reject(
                authorization,
                request,
                None,
                "authorization_expired",
                RocketMQError::Shared(Arc::new(consensus_timed_out("authorize Controller membership", 0))),
            );
        }
        Ok(())
    }

    async fn with_deadline<F, T>(
        &self,
        authorization: &MaintenanceAuthorizationGrant,
        operation: &'static str,
        future: F,
    ) -> Result<T>
    where
        F: std::future::Future<Output = Result<T>>,
    {
        let now = current_millis();
        let timeout_ms = authorization.deadline_unix_millis().saturating_sub(now);
        if timeout_ms == 0 {
            return Err(consensus_timed_out(operation, 0));
        }
        tokio::time::timeout(Duration::from_millis(timeout_ms), future)
            .await
            .map_err(|_| consensus_timed_out(operation, timeout_ms))?
    }

    fn reject<T>(
        &self,
        authorization: &MaintenanceAuthorizationGrant,
        request: &MembershipChangeRequest,
        observed_version: Option<u64>,
        decision: &'static str,
        error: RocketMQError,
    ) -> RocketMQResult<T> {
        self.record_error(
            authorization,
            request,
            observed_version,
            MembershipAuditOutcome::Rejected,
            decision,
            error,
        )
    }

    fn reject_invalid_request<T>(
        &self,
        authorization: &MaintenanceAuthorizationGrant,
        request: &MembershipChangeRequest,
        error: RocketMQError,
    ) -> RocketMQResult<T> {
        let audit = MembershipAuditRecord {
            operation_id: "<invalid>".to_string(),
            principal: authorization.principal().to_string(),
            authorization_capability: authorization.capability(),
            policy_version: authorization.policy_version(),
            operation: request.change.operation(),
            target_node_id: request.change.target_node_id(),
            expected_membership_version: request.expected_membership_version,
            observed_membership_version: None,
            resulting_membership_version: None,
            reason_sha256: INVALID_REQUEST_REASON_SHA256.to_string(),
            outcome: MembershipAuditOutcome::Rejected,
            decision: "invalid_request".to_string(),
        };
        self.audit_sink.record(&audit);
        Err(error)
    }

    fn record_error<T>(
        &self,
        authorization: &MaintenanceAuthorizationGrant,
        request: &MembershipChangeRequest,
        observed_version: Option<u64>,
        outcome: MembershipAuditOutcome,
        decision: &'static str,
        error: RocketMQError,
    ) -> RocketMQResult<T> {
        let audit = self.audit_record(authorization, request, observed_version, None, outcome, decision);
        self.audit_sink.record(&audit);
        Err(error)
    }

    fn audit_record(
        &self,
        authorization: &MaintenanceAuthorizationGrant,
        request: &MembershipChangeRequest,
        observed_membership_version: Option<u64>,
        resulting_membership_version: Option<u64>,
        outcome: MembershipAuditOutcome,
        decision: &'static str,
    ) -> MembershipAuditRecord {
        MembershipAuditRecord {
            operation_id: request.operation_id.clone(),
            principal: authorization.principal().to_string(),
            authorization_capability: authorization.capability(),
            policy_version: authorization.policy_version(),
            operation: request.change.operation(),
            target_node_id: request.change.target_node_id(),
            expected_membership_version: request.expected_membership_version,
            observed_membership_version,
            resulting_membership_version,
            reason_sha256: hex::encode(Sha256::digest(request.reason.as_bytes())),
            outcome,
            decision: decision.to_string(),
        }
    }
}

fn validate_transition(
    membership: &ConsensusMembership,
    change: &MembershipChange,
) -> std::result::Result<(), (&'static str, Error)> {
    match change {
        MembershipChange::AddLearner { node } => {
            if membership.nodes.contains_key(&node.node_id)
                || membership.voters.contains(&node.node_id)
                || membership.learners.contains(&node.node_id)
            {
                return Err((
                    "member_already_exists",
                    request_invalid("add existing Controller learner"),
                ));
            }
        }
        MembershipChange::PromoteVoter { node_id } => {
            if membership.voters.contains(node_id) {
                return Err((
                    "member_already_voter",
                    request_invalid("promote existing Controller voter"),
                ));
            }
            if !membership.learners.contains(node_id) {
                return Err(("learner_missing", request_invalid("promote missing Controller learner")));
            }
            if !membership.caught_up.contains(node_id) {
                return Err((
                    "learner_not_caught_up",
                    request_invalid("promote uncaught-up Controller learner"),
                ));
            }
        }
        MembershipChange::RemoveMember { node_id } => {
            if !membership.voters.contains(node_id) && !membership.learners.contains(node_id) {
                return Err(("member_missing", request_invalid("remove missing Controller member")));
            }
            if membership.leader_id == Some(*node_id) {
                return Err((
                    "leader_removal_requires_transfer",
                    request_invalid("remove current Controller leader"),
                ));
            }
            if membership.voters.contains(node_id) {
                let remaining = membership
                    .voters
                    .iter()
                    .copied()
                    .filter(|voter_id| voter_id != node_id)
                    .collect::<BTreeSet<_>>();
                if remaining.is_empty() {
                    return Err(("last_voter_removal", request_invalid("remove last Controller voter")));
                }
                let required_quorum = remaining.len() / 2 + 1;
                let caught_up_remaining = remaining.intersection(&membership.caught_up).count();
                if caught_up_remaining < required_quorum {
                    return Err((
                        "remaining_quorum_unavailable",
                        request_invalid("remove Controller voter without quorum"),
                    ));
                }
            }
        }
    }
    Ok(())
}

async fn apply_transition<P: ConsensusMembershipPort>(
    port: &P,
    membership: &ConsensusMembership,
    change: &MembershipChange,
) -> Result<()> {
    match change {
        MembershipChange::AddLearner { node } => port.add_caught_up_learner(node).await,
        MembershipChange::PromoteVoter { node_id } => {
            let mut voters = membership.voters.clone();
            voters.insert(*node_id);
            port.change_voters(voters).await
        }
        MembershipChange::RemoveMember { node_id } => {
            if membership.voters.contains(node_id) {
                let mut voters = membership.voters.clone();
                voters.remove(node_id);
                port.change_voters(voters).await
            } else {
                port.remove_learner(*node_id).await
            }
        }
    }
}

fn verify_transition(
    before: &ConsensusMembership,
    after: &ConsensusMembership,
    change: &MembershipChange,
) -> Result<()> {
    if after.version <= before.version {
        return Err(request_invalid("verify Controller membership version"));
    }
    let target = change.target_node_id();
    let applied = match change {
        MembershipChange::AddLearner { node } => {
            after.learners.contains(&target)
                && after.caught_up.contains(&target)
                && after.nodes.get(&target) == Some(node)
        }
        MembershipChange::PromoteVoter { .. } => after.voters.contains(&target) && !after.learners.contains(&target),
        MembershipChange::RemoveMember { .. } => !after.voters.contains(&target) && !after.learners.contains(&target),
    };
    if !applied {
        return Err(request_invalid("verify Controller membership postcondition"));
    }
    Ok(())
}

fn request_fingerprint(request: &MembershipChangeRequest) -> Result<[u8; 32]> {
    let encoded = serde_json::to_vec(request)
        .map_err(|error| controller_internal_by("encode membership request fingerprint", error))?;
    Ok(Sha256::digest(encoded).into())
}
