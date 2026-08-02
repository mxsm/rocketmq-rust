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

//! Controller-owned boundary for authorized consensus membership changes.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::net::SocketAddr;

use rocketmq_security_api::MaintenanceCapability;
use serde::Deserialize;
use serde::Serialize;

use crate::error::ControllerError;
use crate::error::Result;

mod coordinator;
mod raft_adapter;
#[cfg(test)]
mod tests;

pub(crate) use coordinator::MembershipChangeCoordinator;

/// A Controller consensus node without any OpenRaft public type.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub struct ConsensusNode {
    node_id: u64,
    rpc_addr: String,
}

impl ConsensusNode {
    /// Creates a validated consensus node.
    ///
    /// # Errors
    ///
    /// Returns an invalid-request error for node zero or a non-unicast, zero-port, or malformed
    /// RPC address.
    pub fn new(node_id: u64, rpc_addr: impl Into<String>) -> Result<Self> {
        let node = Self {
            node_id,
            rpc_addr: rpc_addr.into(),
        };
        node.validate()?;
        Ok(node)
    }

    fn validate(&self) -> Result<()> {
        if self.node_id == 0 {
            return Err(ControllerError::InvalidRequest(
                "consensus node id must be greater than zero".to_string(),
            ));
        }
        let rpc_addr = self.rpc_addr.parse::<SocketAddr>().map_err(|error| {
            ControllerError::invalid_request_source("consensus node RPC address must be a socket address", error)
        })?;
        if rpc_addr.port() == 0 {
            return Err(ControllerError::InvalidRequest(
                "consensus node RPC port must be greater than zero".to_string(),
            ));
        }
        let invalid_ip = match rpc_addr.ip() {
            std::net::IpAddr::V4(ip) => ip.is_unspecified() || ip.is_multicast() || ip.octets() == [255, 255, 255, 255],
            std::net::IpAddr::V6(ip) => ip.is_unspecified() || ip.is_multicast(),
        };
        if invalid_ip {
            return Err(ControllerError::InvalidRequest(
                "consensus node RPC address must be a concrete unicast address".to_string(),
            ));
        }
        Ok(())
    }

    /// Returns the stable node identifier.
    pub const fn node_id(&self) -> u64 {
        self.node_id
    }

    /// Returns the advertised consensus RPC address.
    pub fn rpc_addr(&self) -> &str {
        &self.rpc_addr
    }
}

/// One explicit step in the Controller membership state machine.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub enum MembershipChange {
    /// Add and synchronously catch up a non-voting learner.
    AddLearner { node: ConsensusNode },
    /// Promote an already caught-up learner to voter.
    PromoteVoter { node_id: u64 },
    /// Remove a learner or non-leader voter while preserving quorum.
    RemoveMember { node_id: u64 },
}

impl MembershipChange {
    const fn operation(&self) -> MembershipOperation {
        match self {
            Self::AddLearner { .. } => MembershipOperation::AddLearner,
            Self::PromoteVoter { .. } => MembershipOperation::PromoteVoter,
            Self::RemoveMember { .. } => MembershipOperation::RemoveMember,
        }
    }

    const fn target_node_id(&self) -> u64 {
        match self {
            Self::AddLearner { node } => node.node_id,
            Self::PromoteVoter { node_id } | Self::RemoveMember { node_id } => *node_id,
        }
    }

    fn validate(&self) -> Result<()> {
        match self {
            Self::AddLearner { node } => node.validate(),
            Self::PromoteVoter { node_id } | Self::RemoveMember { node_id } if *node_id == 0 => Err(
                ControllerError::InvalidRequest("membership target node id must be greater than zero".to_string()),
            ),
            Self::PromoteVoter { .. } | Self::RemoveMember { .. } => Ok(()),
        }
    }
}

/// An authenticated membership request with optimistic version fencing.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub struct MembershipChangeRequest {
    operation_id: String,
    expected_membership_version: u64,
    change: MembershipChange,
    reason: String,
}

impl MembershipChangeRequest {
    /// Creates a validated membership request.
    ///
    /// # Errors
    ///
    /// Returns an invalid-request error when the operation identity or reason is not bounded and
    /// canonical, or when the target node id is zero.
    pub fn new(
        operation_id: impl Into<String>,
        expected_membership_version: u64,
        change: MembershipChange,
        reason: impl Into<String>,
    ) -> Result<Self> {
        let request = Self {
            operation_id: operation_id.into(),
            expected_membership_version,
            change,
            reason: reason.into(),
        };
        request.validate()?;
        Ok(request)
    }

    fn validate(&self) -> Result<()> {
        if !is_canonical_operation_id(&self.operation_id) {
            return Err(ControllerError::InvalidRequest(
                "membership operation id must be 1..=128 canonical ASCII characters".to_string(),
            ));
        }
        self.change.validate()?;
        if self.reason.trim().is_empty() || self.reason.len() > 512 || self.reason.chars().any(char::is_control) {
            return Err(ControllerError::InvalidRequest(
                "membership reason must be 1..=512 printable characters".to_string(),
            ));
        }
        Ok(())
    }

    /// Returns the stable idempotency key.
    pub fn operation_id(&self) -> &str {
        &self.operation_id
    }

    /// Returns the membership version observed by the caller.
    pub const fn expected_membership_version(&self) -> u64 {
        self.expected_membership_version
    }

    /// Returns the requested state-machine step.
    pub const fn change(&self) -> &MembershipChange {
        &self.change
    }
}

/// OpenRaft-independent projection of current consensus membership.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub struct ConsensusMembership {
    version: u64,
    leader_id: Option<u64>,
    voters: BTreeSet<u64>,
    learners: BTreeSet<u64>,
    nodes: BTreeMap<u64, ConsensusNode>,
    caught_up: BTreeSet<u64>,
}

impl ConsensusMembership {
    /// Creates a membership projection for adapters and tests.
    pub fn new(
        version: u64,
        leader_id: Option<u64>,
        voters: BTreeSet<u64>,
        learners: BTreeSet<u64>,
        nodes: BTreeMap<u64, ConsensusNode>,
        caught_up: BTreeSet<u64>,
    ) -> Self {
        Self {
            version,
            leader_id,
            voters,
            learners,
            nodes,
            caught_up,
        }
    }

    /// Returns the log-backed membership version.
    pub const fn version(&self) -> u64 {
        self.version
    }

    /// Returns the currently observed leader.
    pub const fn leader_id(&self) -> Option<u64> {
        self.leader_id
    }

    /// Returns the current voters.
    pub const fn voters(&self) -> &BTreeSet<u64> {
        &self.voters
    }

    /// Returns the current learners.
    pub const fn learners(&self) -> &BTreeSet<u64> {
        &self.learners
    }

    /// Returns the known node descriptors.
    pub const fn nodes(&self) -> &BTreeMap<u64, ConsensusNode> {
        &self.nodes
    }

    /// Returns nodes whose replication position has reached the committed log frontier.
    pub const fn caught_up(&self) -> &BTreeSet<u64> {
        &self.caught_up
    }
}

/// Stable membership operation label used by audit records.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MembershipOperation {
    /// Add a learner.
    AddLearner,
    /// Promote a voter.
    PromoteVoter,
    /// Remove a member.
    RemoveMember,
}

/// Stable audit outcome for one membership attempt.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MembershipAuditOutcome {
    /// Consensus accepted and verified the requested change.
    Applied,
    /// A completed operation was returned without a second consensus mutation.
    Replayed,
    /// Consensus may have committed, but the resulting membership is not verified yet.
    Pending,
    /// Validation, authorization, fencing, or consensus rejected the attempt.
    Rejected,
}

/// Auditable, credential-free facts for one membership attempt.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub struct MembershipAuditRecord {
    operation_id: String,
    principal: String,
    authorization_capability: MaintenanceCapability,
    policy_version: u64,
    operation: MembershipOperation,
    target_node_id: u64,
    expected_membership_version: u64,
    observed_membership_version: Option<u64>,
    resulting_membership_version: Option<u64>,
    reason_sha256: String,
    outcome: MembershipAuditOutcome,
    decision: String,
}

impl MembershipAuditRecord {
    /// Returns the idempotency key bound to this audit fact.
    pub fn operation_id(&self) -> &str {
        &self.operation_id
    }

    /// Returns the authenticated operator identity.
    pub fn principal(&self) -> &str {
        &self.principal
    }

    /// Returns the release-maintenance capability temporarily reused by this boundary.
    ///
    /// A future versioned maintenance policy should split membership administration into a
    /// dedicated capability.
    pub const fn authorization_capability(&self) -> MaintenanceCapability {
        self.authorization_capability
    }

    /// Returns the authorization policy version.
    pub const fn policy_version(&self) -> u64 {
        self.policy_version
    }

    /// Returns the stable outcome.
    pub const fn outcome(&self) -> MembershipAuditOutcome {
        self.outcome
    }

    /// Returns the bounded decision code.
    pub fn decision(&self) -> &str {
        &self.decision
    }
}

/// Whether an authorized request mutated consensus or replayed a prior result.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MembershipChangeDisposition {
    /// Consensus applied the mutation.
    Applied,
    /// The same operation id and payload had already completed.
    Replayed,
}

/// Verified result of one authorized membership operation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub struct MembershipChangeOutcome {
    disposition: MembershipChangeDisposition,
    membership: ConsensusMembership,
    audit: MembershipAuditRecord,
}

impl MembershipChangeOutcome {
    /// Returns whether the operation was applied or replayed.
    pub const fn disposition(&self) -> MembershipChangeDisposition {
        self.disposition
    }

    /// Returns the verified membership after the operation.
    pub const fn membership(&self) -> &ConsensusMembership {
        &self.membership
    }

    /// Returns the audit facts emitted for this attempt.
    pub const fn audit(&self) -> &MembershipAuditRecord {
        &self.audit
    }
}

/// Project-owned port implemented by a consensus adapter.
#[allow(async_fn_in_trait)]
pub(crate) trait ConsensusMembershipPort: Send + Sync {
    /// Reads current membership and replication readiness.
    async fn current_membership(&self) -> Result<ConsensusMembership>;

    /// Adds a learner and waits for it to reach the committed frontier.
    async fn add_caught_up_learner(&self, node: &ConsensusNode) -> Result<()>;

    /// Changes the voter set without retaining removed voters as learners.
    async fn change_voters(&self, voters: BTreeSet<u64>) -> Result<()>;

    /// Removes one non-voting learner.
    async fn remove_learner(&self, node_id: u64) -> Result<()>;
}

/// Sink for security-relevant membership audit facts.
pub trait MembershipAuditSink: Send + Sync {
    /// Records one bounded audit fact.
    fn record(&self, record: &MembershipAuditRecord);
}

fn is_canonical_operation_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b':' | b'/'))
}
