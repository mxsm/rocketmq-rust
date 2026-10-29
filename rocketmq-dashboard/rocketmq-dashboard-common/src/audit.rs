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

//! Local desktop audit contracts, independent of HTTP and the storage backend.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::{EnvironmentId, OperationActor, OperationContext};

/// A reviewed operation category. Read-only queries do not produce these events.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditAction {
    /// The local configuration committed durably; installing its connection has separate feedback.
    ConfigurationSaved,
    MonitorSaved,
    MonitorDeleted,
    TopicCreated,
    TopicUpdated,
    TopicDeleted,
    TopicBrokerDeleted,
    TopicMessageSent,
    ConsumerOffsetReset,
    ConsumerBacklogSkipped,
    ConsumerCreated,
    ConsumerUpdated,
    ConsumerDeleted,
    BrokerUpdated,
    MessageConsumed,
    DlqResent,
    AclUserCreated,
    AclUserUpdated,
    AclUserDeleted,
    AclPolicyCreated,
    AclPolicyUpdated,
    AclPolicyDeleted,
    LocalLogin,
    LocalLogout,
}

/// The business result, independent of a following refresh or audit persistence failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditOutcome {
    Succeeded,
    /// Validation, authorization, or a version conflict prevented the operation.
    Rejected,
    /// At least one target applied and at least one did not confirm success.
    Partial,
    /// The operation has a confirmed negative business result.
    Failed,
    /// Execution may have reached the remote system; automatic replay is unsafe.
    Unknown,
}

/// Safe, fixed error categories. Raw error messages never enter an event.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditFailureCode {
    Validation,
    Authentication,
    Configuration,
    Connection,
    CapabilityUnavailable,
    Conflict,
    Unknown,
}

/// A reviewed Topic field name, without its previous or requested configuration value.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditTopicField {
    ReadQueueCount,
    WriteQueueCount,
    Permission,
    Ordered,
    MessageType,
}

/// Bounded facts that cannot contain message bodies, diagnostics, secrets, or configuration values.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct AuditFacts {
    pub applied_targets: u32,
    pub unconfirmed_targets: u32,
    pub refresh_failed: bool,
    pub failure_code: Option<AuditFailureCode>,
    /// Identity returned by successful local authentication, independently of the actor that
    /// initiated the login. The authentication service supplies this fact after verification.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authenticated_actor: Option<OperationActor>,
}

impl AuditFacts {
    /// Checks the bounded identity produced by successful local authentication.
    ///
    /// # Errors
    /// Returns an error for an invalid identity or a non-operator actor.
    pub fn validate(&self) -> Result<(), AuditContractError> {
        if let Some(actor) = &self.authenticated_actor {
            match actor {
                OperationActor::LocalOperator { username } if valid_identity(username) => {}
                _ => return Err(AuditContractError::InvalidIdentity),
            }
        }
        Ok(())
    }
}

/// Durable evidence written before an external mutation starts.
///
/// A record without a terminal event after restart requires authoritative verification. It never
/// authorizes resubmitting the original mutation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct AuditAcceptedOperation {
    pub context: OperationContext,
    pub action: AuditAction,
    pub accepted_at_epoch_ms: u64,
    /// Requested field intent survives pending-operation recovery and terminal-write retries.
    /// The closed enum bounds this set to five names; older records deserialize as an empty set.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub topic_fields: BTreeSet<AuditTopicField>,
}

/// One immutable terminal event per accepted request ID in the retained local journal.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct AuditEvent {
    /// Monotonic identity assigned by the local writer at terminal commit time.
    pub event_id: u64,
    pub operation: AuditAcceptedOperation,
    pub outcome: AuditOutcome,
    pub timestamp_epoch_ms: u64,
    pub facts: AuditFacts,
}

/// Immutable filter carried in the cursor so a page cannot silently change its query.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct AuditFilter {
    /// Inclusive UTC boundary.
    pub start_epoch_ms: u64,
    /// Exclusive UTC boundary; the maximum window is 31 days.
    pub end_epoch_ms: u64,
    pub actor: Option<OperationActor>,
    pub action: Option<AuditAction>,
    pub outcome: Option<AuditOutcome>,
    pub environment_id: Option<EnvironmentId>,
}

impl AuditFilter {
    pub const MAX_WINDOW_MS: u64 = 31 * 24 * 60 * 60 * 1_000;

    /// Creates the default most recent 24-hour window.
    #[must_use]
    pub fn recent(now_epoch_ms: u64) -> Self {
        Self {
            start_epoch_ms: now_epoch_ms.saturating_sub(24 * 60 * 60 * 1_000),
            end_epoch_ms: now_epoch_ms.saturating_add(1),
            actor: None,
            action: None,
            outcome: None,
            environment_id: None,
        }
    }

    /// Rejects empty, inverted, and oversized query windows.
    pub fn validate(&self) -> Result<(), AuditContractError> {
        if self.start_epoch_ms >= self.end_epoch_ms || self.end_epoch_ms - self.start_epoch_ms > Self::MAX_WINDOW_MS {
            return Err(AuditContractError::InvalidWindow);
        }
        Ok(())
    }

    #[must_use]
    pub fn matches(&self, event: &AuditEvent) -> bool {
        event.timestamp_epoch_ms >= self.start_epoch_ms
            && event.timestamp_epoch_ms < self.end_epoch_ms
            && self
                .actor
                .as_ref()
                .is_none_or(|actor| actor == &event.operation.context.actor)
            && self.action.is_none_or(|action| action == event.operation.action)
            && self.outcome.is_none_or(|outcome| outcome == event.outcome)
            && self
                .environment_id
                .as_ref()
                .is_none_or(|id| id == &event.operation.context.environment_id)
    }
}

/// Stable descending pagination within the first page's terminal-event high watermark.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct AuditCursor {
    pub filter: AuditFilter,
    pub high_watermark: u64,
    pub before_event_id: u64,
    /// Journal retention generation captured on the first page. Any later eviction expires the
    /// cursor, including eviction of only some unread rows.
    pub retention_generation: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuditQuery {
    pub filter: AuditFilter,
    /// Between 1 and 200 rows.
    pub limit: usize,
    pub cursor: Option<AuditCursor>,
}

impl AuditQuery {
    pub fn validate(&self) -> Result<(), AuditContractError> {
        self.filter.validate()?;
        if !(1..=200).contains(&self.limit) {
            return Err(AuditContractError::InvalidLimit);
        }
        if self.cursor.as_ref().is_some_and(|cursor| {
            cursor.filter != self.filter
                || cursor.before_event_id == 0
                || cursor.before_event_id > cursor.high_watermark
        }) {
            return Err(AuditContractError::InvalidCursor);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuditPage {
    pub events: Vec<AuditEvent>,
    pub next_cursor: Option<AuditCursor>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum AuditContractError {
    #[error("Audit query windows must be nonempty and at most 31 days.")]
    InvalidWindow,
    #[error("Audit page size must be between 1 and 200.")]
    InvalidLimit,
    #[error("The audit cursor does not belong to this query.")]
    InvalidCursor,
    #[error("Audit attribution contains an invalid or oversized identifier.")]
    InvalidIdentity,
}

impl AuditAcceptedOperation {
    /// Validates the controlled identity fields before persistence; facts have no free-text fields.
    pub fn validate(&self) -> Result<(), AuditContractError> {
        use crate::OperationResource;

        let resource = match &self.context.resource {
            OperationResource::Configuration | OperationResource::LocalSession => None,
            OperationResource::Monitor(value)
            | OperationResource::Topic(value)
            | OperationResource::ConsumerGroup(value)
            | OperationResource::Broker(value)
            | OperationResource::Message(value)
            | OperationResource::AclSubject(value) => Some(value),
        };
        if !valid_identity(&self.context.request_id)
            || resource.is_some_and(|value| !valid_identity(value))
            || matches!(&self.context.actor, OperationActor::LocalOperator { username } if !valid_identity(username))
        {
            return Err(AuditContractError::InvalidIdentity);
        }
        Ok(())
    }
}

fn valid_identity(value: &str) -> bool {
    !value.trim().is_empty() && value.len() <= 512 && !value.chars().any(char::is_control)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_audit_records_round_trip_and_topic_intent_accepts_only_field_names() {
        let legacy = serde_json::json!({
            "eventId": 41,
            "operation": {
                "context": {
                    "environmentId": "env-legacy",
                    "configurationRevision": 7,
                    "requestId": "request-legacy",
                    "actor": {"kind": "local_desktop"},
                    "resource": {"kind": "topic", "identity": "orders"}
                },
                "action": "topic_updated",
                "acceptedAtEpochMs": 100
            },
            "outcome": "succeeded",
            "timestampEpochMs": 101,
            "facts": {"appliedTargets": 1, "unconfirmedTargets": 0, "refreshFailed": false, "failureCode": null}
        });
        let mut event: AuditEvent = serde_json::from_value(legacy.clone()).unwrap();
        assert!(event.operation.topic_fields.is_empty());
        assert_eq!(serde_json::to_value(&event).unwrap(), legacy);
        event.operation.topic_fields = BTreeSet::from([AuditTopicField::ReadQueueCount, AuditTopicField::Ordered]);
        let mut encoded = serde_json::to_value(&event).unwrap();
        assert_eq!(
            encoded["operation"]["topicFields"],
            serde_json::json!(["read_queue_count", "ordered"])
        );
        assert_eq!(serde_json::from_value::<AuditEvent>(encoded.clone()).unwrap(), event);
        encoded["operation"]["topicFields"] = serde_json::json!(["message_body"]);
        assert!(serde_json::from_value::<AuditEvent>(encoded).is_err());
    }
}
