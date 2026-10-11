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

//! What the trail remembers about recent invocations, so a caller can ask how one ended.
//!
//! The durable records are the evidence; this index is a bounded, in-memory view of the newest
//! ones. It is filled as records are appended and rebuilt from the records the sink still holds
//! when a trail resumes, so an answer never says more than a persisted record does.

use std::collections::BTreeMap;

use super::AuditEvent;
use super::AuditInvocationId;
use super::AuditMode;
use super::AuditRecord;
use super::AuditResult;
use super::AuditTarget;
use super::AuditTargetResults;
use crate::error::ControlErrorCode;
use crate::model::ClusterName;
use crate::model::ControlOperation;

/// Invocations the index remembers. Older ones are forgotten first, finished ones before any that
/// is still running.
pub(super) const RESULT_INDEX_CAPACITY: usize = 2_048;

/// Selects one invocation of a caller: by its identifier, by the digest of its request key, or by
/// both, in which case both have to match.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvocationQuery {
    pub invocation_id: Option<u64>,
    pub request_key_digest: Option<String>,
}

/// What is known about one invocation. It never carries the operator or the reason.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordedInvocation {
    pub invocation_id: AuditInvocationId,
    pub operation: ControlOperation,
    pub mode: AuditMode,
    /// The logical object of the mutation; absent on records older than version 3.
    pub target: Option<AuditTarget>,
    pub state: InvocationState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InvocationState {
    /// The `started` record is persisted and this process has not written the terminal record yet.
    InProgress,
    /// An earlier process persisted the `started` record and stopped before the terminal one, so
    /// nothing records whether the cluster changed.
    Interrupted,
    Finished(InvocationOutcome),
}

/// The terminal record of one invocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvocationOutcome {
    pub result: AuditResult,
    pub error_code: Option<ControlErrorCode>,
    pub duration_millis: Option<u64>,
    pub changed: Option<bool>,
    pub target_results: Option<AuditTargetResults>,
}

struct Entry {
    /// Who started the invocation. It decides who may read the entry and is never returned.
    operator: Option<String>,
    cluster: ClusterName,
    request_key_digest: Option<String>,
    invocation: RecordedInvocation,
}

pub(super) struct ResultIndex {
    entries: BTreeMap<AuditInvocationId, Entry>,
    capacity: usize,
}

impl Default for ResultIndex {
    fn default() -> Self {
        Self::with_capacity(RESULT_INDEX_CAPACITY)
    }
}

impl ResultIndex {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: BTreeMap::new(),
            capacity: capacity.max(1),
        }
    }

    /// Rebuilds the index from the records a sink still holds, oldest first.
    ///
    /// An invocation whose `started` record has no terminal record was left by the process that
    /// wrote these records, so it is interrupted: no running task can still finish it. A terminal
    /// record whose `started` record is no longer held is skipped.
    pub(super) fn recovered(records: &[AuditRecord]) -> Self {
        let mut index = Self::default();
        for record in records {
            match record.event {
                AuditEvent::Started => index.started(record, InvocationState::Interrupted),
                AuditEvent::Completed | AuditEvent::Failed => index.finished(record),
            }
        }
        index
    }

    /// Remembers an invocation whose `started` record was just persisted.
    pub(super) fn started(&mut self, record: &AuditRecord, state: InvocationState) {
        if self.entries.len() >= self.capacity {
            self.forget_one();
        }
        self.entries.insert(
            record.invocation_id,
            Entry {
                operator: record.operator.clone(),
                cluster: record.cluster.clone(),
                request_key_digest: record.request_key_digest.clone(),
                invocation: RecordedInvocation {
                    invocation_id: record.invocation_id,
                    operation: record.operation,
                    mode: record.mode,
                    target: record.target.clone(),
                    state,
                },
            },
        );
    }

    /// Remembers the terminal record of an invocation, if the index still holds the invocation.
    pub(super) fn finished(&mut self, record: &AuditRecord) {
        if let Some(entry) = self.entries.get_mut(&record.invocation_id) {
            entry.invocation.state = InvocationState::Finished(InvocationOutcome {
                result: record.result,
                error_code: record.error_code,
                duration_millis: record.duration_millis,
                changed: record.changed,
                target_results: record.target_results,
            });
        }
    }

    /// Finds the invocation `query` selects among those `operator` started on `cluster`.
    ///
    /// A request key selects the newest invocation that carried it. An invocation of another
    /// operator or cluster is not found, exactly like one that never existed.
    pub(super) fn find(
        &self,
        operator: &str,
        cluster: &ClusterName,
        query: &InvocationQuery,
    ) -> Option<RecordedInvocation> {
        let readable = |entry: &&Entry| {
            entry.operator.as_deref() == Some(operator)
                && entry.cluster == *cluster
                && query
                    .request_key_digest
                    .as_ref()
                    .is_none_or(|digest| entry.request_key_digest.as_ref() == Some(digest))
        };
        let entry = match (query.invocation_id, &query.request_key_digest) {
            (Some(invocation_id), _) => self.entries.get(&AuditInvocationId(invocation_id)).filter(readable),
            (None, Some(_)) => self.entries.values().rev().find(readable),
            (None, None) => None,
        };
        entry.map(|entry| entry.invocation.clone())
    }

    /// Makes room for one entry: the oldest that is not running, or the oldest of all.
    fn forget_one(&mut self) {
        let settled = self
            .entries
            .iter()
            .find(|(_, entry)| entry.invocation.state != InvocationState::InProgress)
            .map(|(invocation_id, _)| *invocation_id);
        match settled {
            Some(invocation_id) => {
                self.entries.remove(&invocation_id);
            }
            None => {
                self.entries.pop_first();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::audit::AuditSchemaVersion;

    fn record(sequence: u64, invocation: u64, event: AuditEvent, operator: &str, key: Option<&str>) -> AuditRecord {
        let finished = event != AuditEvent::Started;
        AuditRecord {
            schema_version: AuditSchemaVersion::V3,
            sequence,
            invocation_id: AuditInvocationId(invocation),
            timestamp_unix_millis: 1,
            event,
            operation: ControlOperation::TopicUpsert,
            cluster: ClusterName::try_new("cluster-a").unwrap(),
            operator: Some(operator.to_owned()),
            reason: Some("ticket 42".to_owned()),
            mode: AuditMode::Execute,
            result: match event {
                AuditEvent::Started => AuditResult::Started,
                AuditEvent::Completed => AuditResult::Applied,
                AuditEvent::Failed => AuditResult::Failed,
            },
            error_code: (event == AuditEvent::Failed).then_some(ControlErrorCode::Timeout),
            duration_millis: finished.then_some(7),
            target: Some(AuditTarget {
                topic: Some("orders".to_owned()),
                ..AuditTarget::default()
            }),
            requested_digest: Some("0".repeat(64)),
            request_key_digest: key.map(|key| crate::audit::sha256_hex(key.as_bytes())),
            before_digest: None,
            changed: finished.then_some(event == AuditEvent::Completed),
            target_results: None,
        }
    }

    fn cluster() -> ClusterName {
        ClusterName::try_new("cluster-a").unwrap()
    }

    fn by_id(invocation_id: u64) -> InvocationQuery {
        InvocationQuery {
            invocation_id: Some(invocation_id),
            request_key_digest: None,
        }
    }

    fn by_key(key: &str) -> InvocationQuery {
        InvocationQuery {
            invocation_id: None,
            request_key_digest: Some(crate::audit::sha256_hex(key.as_bytes())),
        }
    }

    #[test]
    fn an_entry_follows_its_records_and_is_read_only_by_its_operator_on_its_cluster() {
        let mut index = ResultIndex::default();
        index.started(
            &record(1, 1, AuditEvent::Started, "alice", Some("key-0001")),
            InvocationState::InProgress,
        );
        let running = index.find("alice", &cluster(), &by_id(1)).unwrap();
        assert_eq!(running.state, InvocationState::InProgress);
        assert_eq!(running.target.unwrap().topic.as_deref(), Some("orders"));

        index.finished(&record(2, 1, AuditEvent::Completed, "alice", Some("key-0001")));
        let finished = index.find("alice", &cluster(), &by_id(1)).unwrap();
        assert_eq!(
            finished.state,
            InvocationState::Finished(InvocationOutcome {
                result: AuditResult::Applied,
                error_code: None,
                duration_millis: Some(7),
                changed: Some(true),
                target_results: None,
            })
        );

        assert_eq!(index.find("bob", &cluster(), &by_id(1)), None);
        let other_cluster = ClusterName::try_new("cluster-b").unwrap();
        assert_eq!(index.find("alice", &other_cluster, &by_id(1)), None);
        assert_eq!(index.find("alice", &cluster(), &by_id(2)), None);
        assert_eq!(
            index.find(
                "alice",
                &cluster(),
                &InvocationQuery {
                    invocation_id: None,
                    request_key_digest: None
                }
            ),
            None
        );
    }

    #[test]
    fn a_request_key_selects_the_newest_invocation_of_the_caller_that_carried_it() {
        let mut index = ResultIndex::default();
        for (sequence, operator, key) in [
            (1, "alice", Some("key-0001")),
            (2, "bob", Some("key-0001")),
            (3, "alice", Some("key-0001")),
            (4, "alice", Some("key-0002")),
            (5, "alice", None),
        ] {
            index.started(
                &record(sequence, sequence, AuditEvent::Started, operator, key),
                InvocationState::InProgress,
            );
        }
        assert_eq!(
            index
                .find("alice", &cluster(), &by_key("key-0001"))
                .unwrap()
                .invocation_id
                .get(),
            3
        );
        assert_eq!(
            index
                .find("bob", &cluster(), &by_key("key-0001"))
                .unwrap()
                .invocation_id
                .get(),
            2
        );
        assert_eq!(index.find("alice", &cluster(), &by_key("key-0003")), None);

        // With both selectors, the identified invocation has to carry the key as well.
        let both = |invocation_id, key: &str| InvocationQuery {
            invocation_id: Some(invocation_id),
            ..by_key(key)
        };
        assert!(index.find("alice", &cluster(), &both(1, "key-0001")).is_some());
        assert_eq!(index.find("alice", &cluster(), &both(4, "key-0001")), None);
        assert_eq!(index.find("alice", &cluster(), &both(5, "key-0001")), None);
    }

    #[test]
    fn recovery_marks_unfinished_invocations_interrupted_and_skips_orphan_terminals() {
        let records = [
            // The `started` record of invocation 1 is in a segment the sink no longer holds.
            record(3, 1, AuditEvent::Completed, "alice", None),
            record(4, 4, AuditEvent::Started, "alice", None),
            record(5, 5, AuditEvent::Started, "alice", None),
            record(6, 4, AuditEvent::Failed, "alice", None),
        ];
        let index = ResultIndex::recovered(&records);
        assert_eq!(index.find("alice", &cluster(), &by_id(1)), None);
        let InvocationState::Finished(outcome) = index.find("alice", &cluster(), &by_id(4)).unwrap().state else {
            panic!("invocation 4 has a terminal record");
        };
        assert_eq!(outcome.result, AuditResult::Failed);
        assert_eq!(outcome.error_code, Some(ControlErrorCode::Timeout));
        assert_eq!(
            index.find("alice", &cluster(), &by_id(5)).unwrap().state,
            InvocationState::Interrupted
        );
    }

    #[test]
    fn the_index_is_bounded_and_forgets_settled_invocations_before_running_ones() {
        let mut index = ResultIndex::with_capacity(3);
        index.started(
            &record(1, 1, AuditEvent::Started, "alice", None),
            InvocationState::InProgress,
        );
        index.started(
            &record(2, 2, AuditEvent::Started, "alice", None),
            InvocationState::InProgress,
        );
        index.finished(&record(3, 2, AuditEvent::Completed, "alice", None));
        index.started(
            &record(4, 4, AuditEvent::Started, "alice", None),
            InvocationState::InProgress,
        );

        // Full: the finished invocation 2 goes before the older, still running invocation 1.
        index.started(
            &record(5, 5, AuditEvent::Started, "alice", None),
            InvocationState::InProgress,
        );
        assert_eq!(index.entries.len(), 3);
        assert!(index.find("alice", &cluster(), &by_id(1)).is_some());
        assert_eq!(index.find("alice", &cluster(), &by_id(2)), None);

        // Nothing is settled, so the oldest goes; its terminal record then finds no entry.
        index.started(
            &record(6, 6, AuditEvent::Started, "alice", None),
            InvocationState::InProgress,
        );
        assert_eq!(index.entries.len(), 3);
        assert_eq!(index.find("alice", &cluster(), &by_id(1)), None);
        index.finished(&record(7, 1, AuditEvent::Completed, "alice", None));
        assert_eq!(index.entries.len(), 3);
        assert_eq!(index.find("alice", &cluster(), &by_id(1)), None);
    }
}
