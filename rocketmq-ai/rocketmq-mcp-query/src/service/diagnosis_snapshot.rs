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

//! The parts of an evidence snapshot that every diagnosis builds the same way.

use serde::Serialize;
use serde_json::Value;
use sha2::Digest;
use sha2::Sha256;

use crate::model::contract::observed_at;
use crate::model::diagnosis::EvidenceItem;
use crate::model::diagnosis::EvidenceKind;
use crate::model::diagnosis::EvidencePayload;
use crate::model::diagnosis::EvidenceSnapshot;
use crate::model::diagnosis::EvidenceStatus;
use crate::service::diagnosis_evidence::TopicActivity;
use crate::tools::executor::ToolFailure;
use crate::tools::executor::ToolRejection;

/// Identifies what a snapshot was collected for: the same `parts` always hash the same.
pub(crate) fn query_hash(parts: &[&str]) -> String {
    let mut digest = Sha256::new();
    for (index, part) in parts.iter().enumerate() {
        if index > 0 {
            digest.update([0]);
        }
        digest.update(part.as_bytes());
    }
    digest.finalize().iter().map(|byte| format!("{byte:02x}")).collect()
}

/// The snapshot of one diagnosis.
pub(crate) fn snapshot(
    evidence_version: &str,
    query_hash: String,
    cluster: &str,
    target: Value,
    items: Vec<EvidenceItem>,
) -> EvidenceSnapshot {
    EvidenceSnapshot {
        evidence_version: evidence_version.to_string(),
        query_hash,
        cluster: cluster.to_string(),
        target,
        observed_at: observed_at(),
        items,
    }
}

/// The item of one evidence read: what it returned, or why it returned nothing.
pub(crate) fn item<T>(
    id: &str,
    source_tool: &str,
    kind: EvidenceKind,
    query_hash: &str,
    result: &Result<T, ToolFailure>,
) -> EvidenceItem
where
    T: Serialize,
{
    match result {
        Ok(value) => EvidenceItem {
            id: id.to_string(),
            kind,
            source_tool: source_tool.to_string(),
            observed_at: observed_at(),
            freshness_ms: 0,
            status: EvidenceStatus::Present,
            query_hash: query_hash.to_string(),
            payload: Some(payload(kind, value)),
            error_code: None,
            summary: format!("{source_tool} returned evidence."),
        },
        Err(error) => EvidenceItem {
            id: id.to_string(),
            kind,
            source_tool: source_tool.to_string(),
            observed_at: observed_at(),
            freshness_ms: 0,
            status: error_status(error),
            query_hash: query_hash.to_string(),
            payload: None,
            error_code: Some(error.code().to_string()),
            summary: error.to_string(),
        },
    }
}

/// The item of evidence that was not asked for, because an earlier result left nothing to ask by.
pub(crate) fn not_collected(
    id: &str,
    source_tool: &str,
    kind: EvidenceKind,
    query_hash: &str,
    reason: &str,
) -> EvidenceItem {
    EvidenceItem {
        id: id.to_string(),
        kind,
        source_tool: source_tool.to_string(),
        observed_at: observed_at(),
        freshness_ms: 0,
        status: EvidenceStatus::Missing,
        query_hash: query_hash.to_string(),
        payload: None,
        error_code: None,
        summary: reason.to_string(),
    }
}

/// The item of a retry or dead-letter Topic. One that does not exist is evidence, not a gap: the
/// lookup completed and established that nothing was ever written to it.
pub(crate) fn system_topic_item(
    id: &str,
    query_hash: &str,
    result: &Result<TopicActivity, ToolFailure>,
) -> EvidenceItem {
    let mut item = item(
        id,
        "rocketmq_get_topic_stats",
        EvidenceKind::TopicStats,
        query_hash,
        result,
    );
    if result.as_ref().is_ok_and(|activity| !activity.exists) {
        item.summary = "The Topic does not exist, so nothing was written to it.".to_string();
    }
    item
}

fn payload<T>(kind: EvidenceKind, value: &T) -> EvidencePayload
where
    T: Serialize,
{
    let value = serde_json::to_value(value).unwrap_or(Value::Null);
    match kind {
        EvidenceKind::ConsumerLag => EvidencePayload::ConsumerLag(value),
        EvidenceKind::TopicRoute => EvidencePayload::TopicRoute(value),
        EvidenceKind::BrokerSummary => EvidencePayload::BrokerSummary(value),
        EvidenceKind::ConsumerGroup => EvidencePayload::ConsumerGroup(value),
        EvidenceKind::TopicConfig => EvidencePayload::TopicConfig(value),
        EvidenceKind::TopicStats => EvidencePayload::TopicStats(value),
        EvidenceKind::ConsumerProgress => EvidencePayload::ConsumerProgress(value),
        EvidenceKind::BrokerDiagnostics => EvidencePayload::BrokerDiagnostics(value),
        EvidenceKind::BrokerConfig => EvidencePayload::BrokerConfig(value),
        EvidenceKind::HaStatus => EvidencePayload::HaStatus(value),
        EvidenceKind::BrokerInventory => EvidencePayload::BrokerInventory(value),
        EvidenceKind::MessageMetadata => EvidencePayload::MessageMetadata(value),
    }
}

fn error_status(error: &ToolFailure) -> EvidenceStatus {
    match error {
        ToolFailure::Rejected(ToolRejection::TimedOut { .. }) => EvidenceStatus::Timeout,
        ToolFailure::Rejected(ToolRejection::PermissionDenied) => EvidenceStatus::Unauthorized,
        ToolFailure::Rejected(ToolRejection::InvalidArguments { .. }) => EvidenceStatus::Invalid,
        // The lookup completed and established that the target does not exist.
        ToolFailure::Rejected(ToolRejection::NotFound { .. }) => EvidenceStatus::Missing,
        _ => EvidenceStatus::Unavailable,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tools::executor::NotFoundEntity;
    use crate::tools::executor::ToolExecutionError;

    #[test]
    fn the_query_hash_separates_its_parts() {
        assert_eq!(query_hash(&["a", "bc"]), query_hash(&["a", "bc"]));
        assert_ne!(query_hash(&["a", "bc"]), query_hash(&["ab", "c"]));
        assert_ne!(query_hash(&["a"]), query_hash(&["a", ""]));
        assert_eq!(query_hash(&["a"]).len(), 64);
    }

    #[test]
    fn an_item_records_what_a_read_returned_or_why_it_returned_nothing() {
        let read = |result: Result<u32, ToolFailure>| item("id", "tool", EvidenceKind::TopicStats, "hash", &result);

        let present = read(Ok(7));
        assert_eq!(present.status, EvidenceStatus::Present);
        assert_eq!(present.payload, Some(EvidencePayload::TopicStats(Value::from(7))));
        assert_eq!(present.error_code, None);

        for (failure, status, code) in [
            (
                ToolFailure::Rejected(ToolRejection::TimedOut { timeout_ms: 5 }),
                EvidenceStatus::Timeout,
                "backend_timeout",
            ),
            (
                ToolFailure::not_found(NotFoundEntity::Topic),
                EvidenceStatus::Missing,
                "not_found",
            ),
            (
                ToolFailure::Rejected(ToolRejection::PermissionDenied),
                EvidenceStatus::Unauthorized,
                "permission_denied",
            ),
            (
                ToolFailure::Operational(ToolExecutionError::SourceUnavailable(None)),
                EvidenceStatus::Unavailable,
                "source_unavailable",
            ),
        ] {
            let missing = read(Err(failure));
            assert_eq!(missing.status, status);
            assert_eq!(missing.payload, None);
            assert_eq!(missing.error_code.as_deref(), Some(code));
        }

        let skipped = not_collected("id", "tool", EvidenceKind::BrokerSummary, "hash", "nothing to ask by");
        assert_eq!(skipped.status, EvidenceStatus::Missing);
        assert_eq!(skipped.error_code, None);
        assert_eq!(skipped.summary, "nothing to ask by");
    }
}
