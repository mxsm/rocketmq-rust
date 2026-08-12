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

//! Controller failover qualification primitives.
//!
//! These types record evidence; they do not manufacture a production SLO. A failover
//! harness records the ordered T0-T5 milestones, messages that received `PutOk`, and
//! observed `confirmOffset` bounds. The resulting report is suitable for machine-readable
//! benchmark and fault-injection artifacts.

use std::collections::BTreeMap;
use std::fmt;
use std::time::Duration;
use std::time::Instant;

use serde::Deserialize;
use serde::Serialize;

const DEFAULT_MAX_PUT_OK_MESSAGES: usize = 1_000_000;
const DEFAULT_FAILURE_SAMPLE_LIMIT: usize = 64;

/// An ordered failover milestone, from fault injection through producer recovery.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailoverMilestone {
    FaultInjected,
    ControllerLeaderElected,
    BrokerMasterElected,
    StoreWriteAuthorityGranted,
    RouteConverged,
    ProducerRecovered,
}

impl FailoverMilestone {
    const ORDER: [Self; 6] = [
        Self::FaultInjected,
        Self::ControllerLeaderElected,
        Self::BrokerMasterElected,
        Self::StoreWriteAuthorityGranted,
        Self::RouteConverged,
        Self::ProducerRecovered,
    ];

    fn at(index: usize) -> Option<Self> {
        Self::ORDER.get(index).copied()
    }
}

/// One milestone measured relative to T0.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct FailoverMilestoneRecord {
    pub milestone: FailoverMilestone,
    pub elapsed_micros: u64,
}

/// A duration between two adjacent failover milestones.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct FailoverSegment {
    pub from: FailoverMilestone,
    pub to: FailoverMilestone,
    pub duration_micros: u64,
}

/// Serializable output from one failover attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FailoverTimelineSnapshot {
    pub complete: bool,
    pub total_rto_micros: Option<u64>,
    pub milestones: Vec<FailoverMilestoneRecord>,
    pub segments: Vec<FailoverSegment>,
}

/// Records one failover timeline while enforcing T0-T5 order.
#[derive(Debug)]
pub struct FailoverTimeline {
    started_at: Instant,
    records: Vec<FailoverMilestoneRecord>,
}

impl Default for FailoverTimeline {
    fn default() -> Self {
        Self::new()
    }
}

impl FailoverTimeline {
    /// Starts a timeline at T0 (`fault_injected`).
    #[must_use]
    pub fn new() -> Self {
        Self {
            started_at: Instant::now(),
            records: vec![FailoverMilestoneRecord {
                milestone: FailoverMilestone::FaultInjected,
                elapsed_micros: 0,
            }],
        }
    }

    /// Records the next milestone using the monotonic clock captured at T0.
    ///
    /// # Errors
    ///
    /// Returns [`QualificationError::UnexpectedMilestone`] if a milestone is missing,
    /// duplicated, or recorded out of order.
    pub fn record_now(&mut self, milestone: FailoverMilestone) -> Result<(), QualificationError> {
        self.record_elapsed(milestone, self.started_at.elapsed())
    }

    /// Records the next milestone with an explicitly supplied T0-relative duration.
    ///
    /// This deterministic entry point is intended for virtual-time tests and imported
    /// fault-injection observations.
    ///
    /// # Errors
    ///
    /// Returns an error when the milestone is out of order, the elapsed time regresses,
    /// or the duration cannot be represented as microseconds.
    pub fn record_elapsed(
        &mut self,
        milestone: FailoverMilestone,
        elapsed: Duration,
    ) -> Result<(), QualificationError> {
        let expected = FailoverMilestone::at(self.records.len()).ok_or(QualificationError::TimelineComplete)?;
        if milestone != expected {
            return Err(QualificationError::UnexpectedMilestone {
                expected,
                actual: milestone,
            });
        }
        let elapsed_micros = u64::try_from(elapsed.as_micros()).map_err(|_| QualificationError::DurationOverflow)?;
        let previous_micros = self.records.last().map_or(0, |record| record.elapsed_micros);
        if elapsed_micros < previous_micros {
            return Err(QualificationError::ElapsedTimeRegression {
                previous_micros,
                actual_micros: elapsed_micros,
            });
        }
        self.records.push(FailoverMilestoneRecord {
            milestone,
            elapsed_micros,
        });
        Ok(())
    }

    /// Returns a serializable snapshot without changing the timeline.
    #[must_use]
    pub fn snapshot(&self) -> FailoverTimelineSnapshot {
        let segments = self
            .records
            .windows(2)
            .map(|window| FailoverSegment {
                from: window[0].milestone,
                to: window[1].milestone,
                duration_micros: window[1].elapsed_micros.saturating_sub(window[0].elapsed_micros),
            })
            .collect();
        let complete = self.records.len() == FailoverMilestone::ORDER.len();
        FailoverTimelineSnapshot {
            complete,
            total_rto_micros: complete.then(|| self.records.last().map_or(0, |record| record.elapsed_micros)),
            milestones: self.records.clone(),
            segments,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RecoveredMessage {
    expected_offset: u64,
    recovered_offset: Option<u64>,
    observations: u32,
}

/// Bounded audit of messages for which the producer received `PutOk`.
#[derive(Debug)]
pub struct PutOkMessageAudit {
    messages: BTreeMap<String, RecoveredMessage>,
    unexpected_count: usize,
    unexpected_samples: Vec<String>,
    max_messages: usize,
    sample_limit: usize,
}

impl Default for PutOkMessageAudit {
    fn default() -> Self {
        Self::with_limits(DEFAULT_MAX_PUT_OK_MESSAGES, DEFAULT_FAILURE_SAMPLE_LIMIT)
    }
}

impl PutOkMessageAudit {
    /// Creates an audit with explicit memory and failure-sample limits.
    #[must_use]
    pub fn with_limits(max_messages: usize, sample_limit: usize) -> Self {
        Self {
            messages: BTreeMap::new(),
            unexpected_count: 0,
            unexpected_samples: Vec::new(),
            max_messages,
            sample_limit,
        }
    }

    /// Records one message only after the client receives `PutOk`.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty or duplicate message ID, or when the configured
    /// audit capacity is exhausted.
    pub fn record_put_ok(&mut self, message_id: impl Into<String>, end_offset: u64) -> Result<(), QualificationError> {
        let message_id = message_id.into();
        if message_id.trim().is_empty() {
            return Err(QualificationError::EmptyMessageId);
        }
        if self.messages.contains_key(&message_id) {
            return Err(QualificationError::DuplicatePutOkMessageId(message_id));
        }
        if self.messages.len() >= self.max_messages {
            return Err(QualificationError::AuditCapacityExceeded {
                capacity: self.max_messages,
            });
        }
        self.messages.insert(
            message_id,
            RecoveredMessage {
                expected_offset: end_offset,
                recovered_offset: None,
                observations: 0,
            },
        );
        Ok(())
    }

    /// Records a message found after failover.
    pub fn observe_recovered(&mut self, message_id: impl Into<String>, end_offset: u64) {
        let message_id = message_id.into();
        if let Some(message) = self.messages.get_mut(&message_id) {
            message.observations = message.observations.saturating_add(1);
            message.recovered_offset.get_or_insert(end_offset);
        } else {
            self.unexpected_count = self.unexpected_count.saturating_add(1);
            push_sample(&mut self.unexpected_samples, message_id, self.sample_limit);
        }
    }

    /// Produces the current RPO and duplicate-delivery evidence.
    #[must_use]
    pub fn report(&self) -> PutOkMessageAuditReport {
        let mut missing_samples = Vec::new();
        let mut duplicate_samples = Vec::new();
        let mut offset_mismatch_samples = Vec::new();
        let mut recovered_once_count = 0;
        let mut missing_count = 0;
        let mut duplicate_count = 0;
        let mut offset_mismatch_count = 0;

        for (message_id, message) in &self.messages {
            if message.observations == 0 {
                missing_count += 1;
                push_sample(&mut missing_samples, message_id.clone(), self.sample_limit);
                continue;
            }
            recovered_once_count += 1;
            if message.observations > 1 {
                duplicate_count += usize::try_from(message.observations - 1).unwrap_or(usize::MAX);
                push_sample(&mut duplicate_samples, message_id.clone(), self.sample_limit);
            }
            if message.recovered_offset != Some(message.expected_offset) {
                offset_mismatch_count += 1;
                push_sample(&mut offset_mismatch_samples, message_id.clone(), self.sample_limit);
            }
        }

        PutOkMessageAuditReport {
            put_ok_count: self.messages.len(),
            recovered_once_count,
            missing_count,
            duplicate_count,
            unexpected_count: self.unexpected_count,
            offset_mismatch_count,
            rpo_zero: missing_count == 0,
            exact_recovery: missing_count == 0
                && duplicate_count == 0
                && self.unexpected_count == 0
                && offset_mismatch_count == 0,
            missing_samples,
            duplicate_samples,
            unexpected_samples: self.unexpected_samples.clone(),
            offset_mismatch_samples,
        }
    }
}

/// Serializable recovery evidence for messages that received `PutOk`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PutOkMessageAuditReport {
    pub put_ok_count: usize,
    pub recovered_once_count: usize,
    pub missing_count: usize,
    pub duplicate_count: usize,
    pub unexpected_count: usize,
    pub offset_mismatch_count: usize,
    pub rpo_zero: bool,
    pub exact_recovery: bool,
    pub missing_samples: Vec<String>,
    pub duplicate_samples: Vec<String>,
    pub unexpected_samples: Vec<String>,
    pub offset_mismatch_samples: Vec<String>,
}

/// The reason an observed confirm offset violates the HA boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConfirmOffsetViolationKind {
    AuthorityEpochRegression,
    ConfirmOffsetRegression,
    ExceedsInSyncAck,
}

/// One bounded diagnostic sample for a confirm-offset violation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfirmOffsetViolation {
    pub kind: ConfirmOffsetViolationKind,
    pub authority_epoch: u64,
    pub confirm_offset: u64,
    pub legal_in_sync_ack: u64,
}

/// Audits confirm-offset monotonicity and the current in-sync acknowledgement bound.
#[derive(Debug)]
pub struct ConfirmOffsetAudit {
    last_authority_epoch: Option<u64>,
    last_confirm_offset: Option<u64>,
    observations: usize,
    violation_count: usize,
    violations: Vec<ConfirmOffsetViolation>,
    sample_limit: usize,
}

impl Default for ConfirmOffsetAudit {
    fn default() -> Self {
        Self::with_sample_limit(DEFAULT_FAILURE_SAMPLE_LIMIT)
    }
}

impl ConfirmOffsetAudit {
    /// Creates an audit that keeps at most `sample_limit` violation details.
    #[must_use]
    pub fn with_sample_limit(sample_limit: usize) -> Self {
        Self {
            last_authority_epoch: None,
            last_confirm_offset: None,
            observations: 0,
            violation_count: 0,
            violations: Vec::new(),
            sample_limit,
        }
    }

    /// Records one authority/confirm/in-sync watermark observation.
    pub fn observe(&mut self, authority_epoch: u64, confirm_offset: u64, legal_in_sync_ack: u64) {
        self.observations = self.observations.saturating_add(1);
        if self
            .last_authority_epoch
            .is_some_and(|previous| authority_epoch < previous)
        {
            self.record_violation(
                ConfirmOffsetViolationKind::AuthorityEpochRegression,
                authority_epoch,
                confirm_offset,
                legal_in_sync_ack,
            );
        }
        if self
            .last_confirm_offset
            .is_some_and(|previous| confirm_offset < previous)
        {
            self.record_violation(
                ConfirmOffsetViolationKind::ConfirmOffsetRegression,
                authority_epoch,
                confirm_offset,
                legal_in_sync_ack,
            );
        }
        if confirm_offset > legal_in_sync_ack {
            self.record_violation(
                ConfirmOffsetViolationKind::ExceedsInSyncAck,
                authority_epoch,
                confirm_offset,
                legal_in_sync_ack,
            );
        }
        self.last_authority_epoch = Some(
            self.last_authority_epoch
                .map_or(authority_epoch, |value| value.max(authority_epoch)),
        );
        self.last_confirm_offset = Some(
            self.last_confirm_offset
                .map_or(confirm_offset, |value| value.max(confirm_offset)),
        );
    }

    fn record_violation(
        &mut self,
        kind: ConfirmOffsetViolationKind,
        authority_epoch: u64,
        confirm_offset: u64,
        legal_in_sync_ack: u64,
    ) {
        self.violation_count = self.violation_count.saturating_add(1);
        if self.violations.len() < self.sample_limit {
            self.violations.push(ConfirmOffsetViolation {
                kind,
                authority_epoch,
                confirm_offset,
                legal_in_sync_ack,
            });
        }
    }

    /// Produces the current confirm-offset evidence.
    #[must_use]
    pub fn report(&self) -> ConfirmOffsetAuditReport {
        ConfirmOffsetAuditReport {
            observations: self.observations,
            violation_count: self.violation_count,
            valid: self.violation_count == 0,
            last_authority_epoch: self.last_authority_epoch,
            last_confirm_offset: self.last_confirm_offset,
            violations: self.violations.clone(),
        }
    }
}

/// Serializable confirm-offset evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfirmOffsetAuditReport {
    pub observations: usize,
    pub violation_count: usize,
    pub valid: bool,
    pub last_authority_epoch: Option<u64>,
    pub last_confirm_offset: Option<u64>,
    pub violations: Vec<ConfirmOffsetViolation>,
}

/// Preconditions required before one run can support a strict payload RPO=0 claim.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurabilityEvidence {
    pub synchronous_local_flush: bool,
    pub required_replica_acks: bool,
    pub clean_election: bool,
}

impl DurabilityEvidence {
    /// Returns whether the run was configured to make a strict RPO=0 claim meaningful.
    #[must_use]
    pub const fn supports_strict_rpo_zero(self) -> bool {
        self.synchronous_local_flush && self.required_replica_acks && self.clean_election
    }
}

/// Machine-readable qualification evidence for one controller/broker failover run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FailoverQualificationReport {
    pub schema_version: u32,
    pub artifact_kind: String,
    pub scenario: String,
    pub durability: DurabilityEvidence,
    pub timeline: FailoverTimelineSnapshot,
    pub put_ok_messages: PutOkMessageAuditReport,
    pub confirm_offset: ConfirmOffsetAuditReport,
    pub strict_qualification_passed: bool,
    pub rejection_reasons: Vec<String>,
}

impl FailoverQualificationReport {
    /// Builds one evidence report. This method never upgrades a partial observation into an SLO.
    #[must_use]
    pub fn new(
        scenario: impl Into<String>,
        durability: DurabilityEvidence,
        timeline: FailoverTimelineSnapshot,
        put_ok_messages: PutOkMessageAuditReport,
        confirm_offset: ConfirmOffsetAuditReport,
    ) -> Self {
        let mut rejection_reasons = Vec::new();
        if !timeline.complete {
            rejection_reasons.push("failover timeline is incomplete".to_string());
        }
        if !durability.supports_strict_rpo_zero() {
            rejection_reasons.push("strict durability preconditions were not all enabled".to_string());
        }
        if put_ok_messages.put_ok_count == 0 {
            rejection_reasons.push("no PutOk messages were recorded for recovery audit".to_string());
        }
        if !put_ok_messages.rpo_zero {
            rejection_reasons.push("one or more PutOk messages were missing after failover".to_string());
        }
        if !put_ok_messages.exact_recovery {
            rejection_reasons.push("recovered message set was not an exact match".to_string());
        }
        if !confirm_offset.valid {
            rejection_reasons.push("confirmOffset violated monotonicity or its in-sync bound".to_string());
        }
        if confirm_offset.observations == 0 {
            rejection_reasons.push("no confirmOffset observations were recorded".to_string());
        }
        let strict_qualification_passed = rejection_reasons.is_empty();
        Self {
            schema_version: 1,
            artifact_kind: "controller_failover_qualification_evidence".to_string(),
            scenario: scenario.into(),
            durability,
            timeline,
            put_ok_messages,
            confirm_offset,
            strict_qualification_passed,
            rejection_reasons,
        }
    }

    /// Serializes the report for benchmark and fault-injection artifacts.
    ///
    /// # Errors
    ///
    /// Returns a JSON serialization error if the report cannot be encoded.
    pub fn to_pretty_json(&self) -> serde_json::Result<String> {
        serde_json::to_string_pretty(self)
    }
}

/// Errors caused by invalid qualification input or event ordering.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QualificationError {
    UnexpectedMilestone {
        expected: FailoverMilestone,
        actual: FailoverMilestone,
    },
    TimelineComplete,
    ElapsedTimeRegression {
        previous_micros: u64,
        actual_micros: u64,
    },
    DurationOverflow,
    EmptyMessageId,
    DuplicatePutOkMessageId(String),
    AuditCapacityExceeded {
        capacity: usize,
    },
}

impl fmt::Display for QualificationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnexpectedMilestone { expected, actual } => {
                write!(formatter, "expected milestone {expected:?}, received {actual:?}")
            }
            Self::TimelineComplete => formatter.write_str("failover timeline is already complete"),
            Self::ElapsedTimeRegression {
                previous_micros,
                actual_micros,
            } => write!(
                formatter,
                "failover elapsed time regressed from {previous_micros}us to {actual_micros}us"
            ),
            Self::DurationOverflow => formatter.write_str("failover duration exceeds the supported microsecond range"),
            Self::EmptyMessageId => formatter.write_str("PutOk message ID must not be empty"),
            Self::DuplicatePutOkMessageId(message_id) => {
                write!(formatter, "PutOk message ID was recorded twice: {message_id}")
            }
            Self::AuditCapacityExceeded { capacity } => {
                write!(formatter, "PutOk audit capacity of {capacity} messages was exceeded")
            }
        }
    }
}

impl std::error::Error for QualificationError {}

fn push_sample(samples: &mut Vec<String>, value: String, sample_limit: usize) {
    if samples.len() < sample_limit {
        samples.push(value);
    }
}
