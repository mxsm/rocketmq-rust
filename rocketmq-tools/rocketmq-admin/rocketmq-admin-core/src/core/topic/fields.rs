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

//! Exact-target, changed-field Topic edits over the supervised state CAS protocol.

#[cfg(any(feature = "mutation-client-adapter", test))]
use std::collections::BTreeMap;
use std::collections::BTreeSet;

use rocketmq_model::common::topic::TopicValidator;

#[cfg(any(feature = "mutation-client-adapter", test))]
use crate::core::supervised_mutation::MutationPersistenceState;
use crate::core::supervised_mutation::{
    MutationFailureCode, TopicMessageType, TopicReplacement, MAX_METADATA_MUTATION_TARGETS,
};
use crate::core::{AdminError, AdminFuture, AdminResult};

/// Only present fields are changed; all other fields come from each target's
/// version-checked state, never from another Broker's configuration.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TopicConfigFieldsPatch {
    pub read_queue_nums: Option<u32>,
    pub write_queue_nums: Option<u32>,
    pub perm: Option<u32>,
    pub order: Option<bool>,
    pub message_type: Option<TopicMessageType>,
}

impl TopicConfigFieldsPatch {
    pub const fn is_empty(self) -> bool {
        self.read_queue_nums.is_none()
            && self.write_queue_nums.is_none()
            && self.perm.is_none()
            && self.order.is_none()
            && self.message_type.is_none()
    }

    #[cfg(any(feature = "mutation-client-adapter", test))]
    fn apply(self, current: &TopicReplacement) -> TopicReplacement {
        TopicReplacement {
            read_queue_nums: self.read_queue_nums.unwrap_or(current.read_queue_nums),
            write_queue_nums: self.write_queue_nums.unwrap_or(current.write_queue_nums),
            perm: self.perm.unwrap_or(current.perm),
            order: self.order.unwrap_or(current.order),
            message_type: self.message_type.unwrap_or(current.message_type),
        }
    }
}

/// The exact master identity and version reviewed before confirmation.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct TopicConfigFieldsTarget {
    pub cluster_name: String,
    pub broker_name: String,
    pub broker_address: String,
    pub expected_version: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TopicConfigFieldsPatchRequest {
    topic: String,
    targets: Vec<TopicConfigFieldsTarget>,
    patch: TopicConfigFieldsPatch,
}

impl TopicConfigFieldsPatchRequest {
    /// Freezes a bounded, unambiguous Broker set and validates the changed fields.
    ///
    /// # Errors
    /// Returns an invalid-argument error for a protected Topic, missing or
    /// ambiguous targets, an empty patch, invalid queue counts, or permissions
    /// that grant neither read nor write access.
    pub fn try_new(
        topic: impl Into<String>,
        targets: impl IntoIterator<Item = TopicConfigFieldsTarget>,
        patch: TopicConfigFieldsPatch,
    ) -> AdminResult<Self> {
        let topic = topic.into().trim().to_owned();
        if !TopicValidator::validate_topic(&topic).valid() || TopicValidator::is_system_topic(&topic) {
            return Err(AdminError::invalid_argument(
                "topic",
                "must be an eligible application Topic",
            ));
        }
        if patch.is_empty() {
            return Err(AdminError::invalid_argument("patch", "must contain a changed field"));
        }
        for (field, value) in [
            ("readQueueNums", patch.read_queue_nums),
            ("writeQueueNums", patch.write_queue_nums),
        ] {
            if value.is_some_and(|value| !(1..=128).contains(&value)) {
                return Err(AdminError::invalid_argument(field, "must be between 1 and 128"));
            }
        }
        if patch.perm.is_some_and(|perm| !(1..=7).contains(&perm) || perm & 6 == 0) {
            return Err(AdminError::invalid_argument(
                "perm",
                "must grant read or write permission",
            ));
        }
        let mut targets = targets.into_iter().collect::<Vec<_>>();
        targets.sort();
        targets.dedup();
        if targets.is_empty() || targets.len() > MAX_METADATA_MUTATION_TARGETS {
            return Err(AdminError::invalid_argument(
                "targets",
                "select between 1 and 64 exact Brokers",
            ));
        }
        let mut names = BTreeSet::new();
        for target in &targets {
            if target.cluster_name.trim().is_empty()
                || target.broker_address.trim().is_empty()
                || target.broker_name.is_empty()
                || target
                    .broker_name
                    .chars()
                    .any(|value| value.is_control() || matches!(value, ':' | ';'))
                || !names.insert(target.broker_name.as_str())
            {
                return Err(AdminError::invalid_argument(
                    "targets",
                    "Broker identities must be exact and unambiguous",
                ));
            }
        }
        Ok(Self { topic, targets, patch })
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }
    pub fn targets(&self) -> &[TopicConfigFieldsTarget] {
        &self.targets
    }
    pub const fn patch(&self) -> TopicConfigFieldsPatch {
        self.patch
    }
}

/// Absent configurations retain absence rather than inventing a version.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TopicConfigFieldsState {
    pub version: Option<u64>,
    pub configuration: Option<TopicReplacement>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TopicConfigFieldsTargetOutcome {
    pub target: TopicConfigFieldsTarget,
    pub applied: bool,
    pub failure: Option<MutationFailureCode>,
    pub retryable: bool,
    pub outcome_unknown: bool,
    pub latest: Option<TopicConfigFieldsState>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TopicConfigFieldsPatchReport {
    pub targets: Vec<TopicConfigFieldsTargetOutcome>,
    /// NameServer order configuration has a preflight comparison, not a CAS.
    /// `None` means this edit did not require an order-map write.
    pub order_reconciled: Option<bool>,
}

pub trait TopicConfigFieldsMutationAdmin: Send {
    fn patch_topic_config_fields<'a>(
        &'a mut self,
        request: &'a TopicConfigFieldsPatchRequest,
    ) -> AdminFuture<'a, TopicConfigFieldsPatchReport>;
}

#[derive(Clone, Debug)]
#[cfg(any(feature = "mutation-client-adapter", test))]
pub(crate) struct TopicFieldsWriteResult {
    pub applied: bool,
    pub version: Option<u64>,
    pub persistence: MutationPersistenceState,
}

#[cfg(any(feature = "mutation-client-adapter", test))]
pub(crate) trait TopicFieldsBackend {
    fn validate_targets<'a>(&'a mut self, targets: &'a [TopicConfigFieldsTarget]) -> AdminFuture<'a, ()>;
    fn state<'a>(
        &'a mut self,
        topic: &'a str,
        target: &'a TopicConfigFieldsTarget,
    ) -> AdminFuture<'a, TopicConfigFieldsState>;
    fn replace<'a>(
        &'a mut self,
        topic: &'a str,
        target: &'a TopicConfigFieldsTarget,
        replacement: &'a TopicReplacement,
    ) -> AdminFuture<'a, TopicFieldsWriteResult>;
    fn order_config<'a>(&'a mut self, topic: &'a str) -> AdminFuture<'a, Option<String>>;
    fn write_order_config<'a>(&'a mut self, topic: &'a str, value: Option<String>) -> AdminFuture<'a, ()>;
}

/// Brokers apply one closed replacement atomically with their reviewed version.
/// Order-map entries for failed and unselected Brokers are preserved. A later
/// NameServer failure is reported separately and never erases Broker success.
#[cfg(any(feature = "mutation-client-adapter", test))]
pub(crate) async fn execute_fields_patch(
    backend: &mut impl TopicFieldsBackend,
    request: &TopicConfigFieldsPatchRequest,
) -> AdminResult<TopicConfigFieldsPatchReport> {
    backend.validate_targets(request.targets()).await?;
    let mut targets = Vec::new();
    let mut replacements = Vec::new();
    for target in request.targets() {
        let mut outcome = TopicConfigFieldsTargetOutcome {
            target: target.clone(),
            applied: false,
            failure: None,
            retryable: false,
            outcome_unknown: false,
            latest: None,
        };
        let replacement = match backend.state(request.topic(), target).await {
            Ok(state) if state.version == Some(target.expected_version) && state.configuration.is_some() => {
                state.configuration.as_ref().map(|config| request.patch().apply(config))
            }
            Ok(state) => {
                outcome.failure = Some(MutationFailureCode::Conflict);
                outcome.latest = Some(state);
                None
            }
            Err(_) => {
                outcome.failure = Some(MutationFailureCode::Unavailable);
                None
            }
        };
        targets.push(outcome);
        replacements.push(replacement);
    }
    let changes_order = replacements.iter().any(Option::is_some)
        && (request.patch().order.is_some()
            || request.patch().write_queue_nums.is_some() && replacements.iter().flatten().any(|config| config.order));
    let original_order = if changes_order {
        Some(parse_order(backend.order_config(request.topic()).await?)?)
    } else {
        None
    };
    for (outcome, replacement) in targets.iter_mut().zip(&replacements) {
        let Some(replacement) = replacement else { continue };
        match backend.replace(request.topic(), &outcome.target, replacement).await {
            Ok(result) => {
                outcome.applied = result.applied;
                outcome.outcome_unknown = result.persistence == MutationPersistenceState::Unconfirmed;
                // Dirty persistence rejects writes at the same version. Only
                // a changed state without a persistence failure can be rebased.
                outcome.failure = if matches!(
                    result.persistence,
                    MutationPersistenceState::Failed | MutationPersistenceState::Unconfirmed
                ) {
                    Some(MutationFailureCode::PersistenceFailed)
                } else if !result.applied {
                    Some(
                        if result.persistence == MutationPersistenceState::NotRequired
                            && result.version != Some(outcome.target.expected_version)
                        {
                            MutationFailureCode::Conflict
                        } else {
                            MutationFailureCode::Unavailable
                        },
                    )
                } else {
                    None
                };
                if outcome.failure == Some(MutationFailureCode::Conflict) {
                    outcome.latest = backend.state(request.topic(), &outcome.target).await.ok();
                } else if result.applied {
                    outcome.latest = Some(TopicConfigFieldsState {
                        version: result.version,
                        configuration: Some(replacement.clone()),
                    });
                }
            }
            Err(error) => {
                use rocketmq_error::CanonicalCondition;
                outcome.failure = Some(MutationFailureCode::Unavailable);
                outcome.outcome_unknown = match error.condition() {
                    CanonicalCondition::InvalidArgument
                    | CanonicalCondition::NotFound
                    | CanonicalCondition::AlreadyExists
                    | CanonicalCondition::Unauthenticated
                    | CanonicalCondition::PermissionDenied
                    | CanonicalCondition::FailedPrecondition
                    | CanonicalCondition::Unimplemented => false,
                    CanonicalCondition::ResourceExhausted
                    | CanonicalCondition::Aborted
                    | CanonicalCondition::Unavailable
                    | CanonicalCondition::DeadlineExceeded
                    | CanonicalCondition::DataLoss
                    | CanonicalCondition::Cancelled
                    | CanonicalCondition::Internal => true,
                };
            }
        }
    }
    let order_reconciled = if let Some(original_order) = original_order {
        let mut desired = original_order.clone();
        let mut changed = false;
        for (outcome, replacement) in targets.iter().zip(&replacements) {
            if outcome.applied && outcome.failure.is_none() {
                if let Some(replacement) = replacement {
                    if replacement.order {
                        desired.insert(outcome.target.broker_name.clone(), replacement.write_queue_nums);
                    } else {
                        desired.remove(&outcome.target.broker_name);
                    }
                    changed = true;
                }
            }
        }
        if changed {
            let still_current = backend
                .order_config(request.topic())
                .await
                .ok()
                .and_then(|value| parse_order(value).ok())
                == Some(original_order);
            Some(
                still_current
                    && backend
                        .write_order_config(request.topic(), encode_order(desired))
                        .await
                        .is_ok(),
            )
        } else {
            None
        }
    } else {
        None
    };
    Ok(TopicConfigFieldsPatchReport {
        targets,
        order_reconciled,
    })
}

#[cfg(any(feature = "mutation-client-adapter", test))]
fn parse_order(value: Option<String>) -> AdminResult<BTreeMap<String, u32>> {
    let mut entries = BTreeMap::new();
    let Some(value) = value else { return Ok(entries) };
    for entry in value.split(';') {
        let (name, queues) = entry
            .split_once(':')
            .ok_or_else(|| AdminError::invalid_argument("orderConfig", "malformed Broker entry"))?;
        let queues = queues.parse::<u32>().ok().filter(|value| *value > 0);
        if name.is_empty() || queues.is_none() || entries.insert(name.to_owned(), queues.unwrap_or_default()).is_some()
        {
            return Err(AdminError::invalid_argument(
                "orderConfig",
                "invalid or ambiguous Broker entry",
            ));
        }
    }
    Ok(entries)
}

#[cfg(any(feature = "mutation-client-adapter", test))]
fn encode_order(entries: BTreeMap<String, u32>) -> Option<String> {
    (!entries.is_empty()).then(|| {
        entries
            .into_iter()
            .map(|(name, queues)| format!("{name}:{queues}"))
            .collect::<Vec<_>>()
            .join(";")
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Fake {
        states: BTreeMap<String, TopicConfigFieldsState>,
        writes: Vec<(String, TopicReplacement)>,
        fail_broker: Option<String>,
        race_broker: Option<String>,
        order: Option<String>,
        order_writes: Vec<Option<String>>,
        order_reads: usize,
        race_order: bool,
        fail_order: bool,
        identities_changed: bool,
        write_results: BTreeMap<String, TopicFieldsWriteResult>,
    }

    fn config(read: u32, permission: u32) -> TopicReplacement {
        TopicReplacement {
            read_queue_nums: read,
            write_queue_nums: 8,
            perm: permission,
            order: false,
            message_type: TopicMessageType::Normal,
        }
    }

    fn target(name: &str, version: u64) -> TopicConfigFieldsTarget {
        TopicConfigFieldsTarget {
            cluster_name: "cluster-a".into(),
            broker_name: name.into(),
            broker_address: format!("{name}:10911"),
            expected_version: version,
        }
    }

    fn fake() -> Fake {
        Fake {
            states: [
                (
                    "broker-a".into(),
                    TopicConfigFieldsState {
                        version: Some(7),
                        configuration: Some(config(8, 6)),
                    },
                ),
                (
                    "broker-b".into(),
                    TopicConfigFieldsState {
                        version: Some(9),
                        configuration: Some(config(12, 4)),
                    },
                ),
            ]
            .into(),
            writes: Vec::new(),
            fail_broker: None,
            race_broker: None,
            order: Some("outside:16".into()),
            order_writes: Vec::new(),
            order_reads: 0,
            race_order: false,
            fail_order: false,
            identities_changed: false,
            write_results: BTreeMap::new(),
        }
    }

    impl TopicFieldsBackend for Fake {
        fn validate_targets<'a>(&'a mut self, _: &'a [TopicConfigFieldsTarget]) -> AdminFuture<'a, ()> {
            Box::pin(async move {
                if self.identities_changed {
                    Err(AdminError::invalid_argument("targets", "identity changed"))
                } else {
                    Ok(())
                }
            })
        }
        fn state<'a>(
            &'a mut self,
            _: &'a str,
            target: &'a TopicConfigFieldsTarget,
        ) -> AdminFuture<'a, TopicConfigFieldsState> {
            Box::pin(async move { Ok(self.states[&target.broker_name].clone()) })
        }
        fn replace<'a>(
            &'a mut self,
            _: &'a str,
            target: &'a TopicConfigFieldsTarget,
            replacement: &'a TopicReplacement,
        ) -> AdminFuture<'a, TopicFieldsWriteResult> {
            Box::pin(async move {
                if let Some(result) = self.write_results.remove(&target.broker_name) {
                    if result.applied {
                        self.writes.push((target.broker_name.clone(), replacement.clone()));
                    }
                    return Ok(result);
                }
                if self.fail_broker.as_deref() == Some(&target.broker_name) {
                    return Err(AdminError::operation_failed("replace", "unavailable"));
                }
                if self.race_broker.as_deref() == Some(&target.broker_name) {
                    self.states.get_mut(&target.broker_name).unwrap().version = Some(target.expected_version + 2);
                    return Ok(TopicFieldsWriteResult {
                        applied: false,
                        version: Some(target.expected_version + 2),
                        persistence: MutationPersistenceState::NotRequired,
                    });
                }
                self.writes.push((target.broker_name.clone(), replacement.clone()));
                Ok(TopicFieldsWriteResult {
                    applied: true,
                    version: Some(target.expected_version + 1),
                    persistence: MutationPersistenceState::Persisted,
                })
            })
        }
        fn order_config<'a>(&'a mut self, _: &'a str) -> AdminFuture<'a, Option<String>> {
            Box::pin(async move {
                self.order_reads += 1;
                if self.race_order && self.order_reads > 1 {
                    Ok(Some("concurrent:8".into()))
                } else {
                    Ok(self.order.clone())
                }
            })
        }
        fn write_order_config<'a>(&'a mut self, _: &'a str, value: Option<String>) -> AdminFuture<'a, ()> {
            Box::pin(async move {
                self.order_writes.push(value);
                if self.fail_order {
                    Err(AdminError::operation_failed("order", "unavailable"))
                } else {
                    Ok(())
                }
            })
        }
    }

    #[tokio::test]
    async fn field_patch_preserves_each_brokers_unchanged_fields() {
        let mut backend = fake();
        let request = TopicConfigFieldsPatchRequest::try_new(
            "orders",
            [target("broker-a", 7), target("broker-b", 9)],
            TopicConfigFieldsPatch {
                message_type: Some(TopicMessageType::Transaction),
                ..Default::default()
            },
        )
        .unwrap();
        let report = execute_fields_patch(&mut backend, &request).await.unwrap();
        assert!(report
            .targets
            .iter()
            .all(|target| target.applied && target.failure.is_none()));
        assert_eq!(
            backend.writes[0].1,
            TopicReplacement {
                message_type: TopicMessageType::Transaction,
                ..config(8, 6)
            }
        );
        assert_eq!(
            backend.writes[1].1,
            TopicReplacement {
                message_type: TopicMessageType::Transaction,
                ..config(12, 4)
            }
        );
        assert_eq!(backend.order_reads, 0);
        assert!(backend.order_writes.is_empty());
    }

    #[tokio::test]
    async fn multi_field_patch_retains_partial_results_and_unselected_order_entries() {
        let mut backend = fake();
        backend.fail_broker = Some("broker-b".into());
        backend.order = Some("broker-b:8;outside:16".into());
        let request = TopicConfigFieldsPatchRequest::try_new(
            "orders",
            [target("broker-a", 7), target("broker-b", 9)],
            TopicConfigFieldsPatch {
                read_queue_nums: Some(6),
                write_queue_nums: Some(6),
                perm: Some(7),
                order: Some(true),
                message_type: Some(TopicMessageType::Fifo),
            },
        )
        .unwrap();
        let report = execute_fields_patch(&mut backend, &request).await.unwrap();
        assert!(report.targets[0].applied);
        assert_eq!(report.targets[1].failure, Some(MutationFailureCode::Unavailable));
        assert!(report.targets[1].outcome_unknown);
        assert_eq!(
            backend.writes,
            vec![(
                "broker-a".into(),
                TopicReplacement {
                    read_queue_nums: 6,
                    write_queue_nums: 6,
                    perm: 7,
                    order: true,
                    message_type: TopicMessageType::Fifo
                }
            )]
        );
        assert_eq!(
            backend.order_writes,
            vec![Some("broker-a:6;broker-b:8;outside:16".into())]
        );
        assert_eq!(report.order_reconciled, Some(true));
    }

    #[tokio::test]
    async fn unordered_patch_removes_only_the_successful_brokers_order_entry() {
        let mut backend = fake();
        backend
            .states
            .get_mut("broker-a")
            .unwrap()
            .configuration
            .as_mut()
            .unwrap()
            .order = true;
        backend
            .states
            .get_mut("broker-b")
            .unwrap()
            .configuration
            .as_mut()
            .unwrap()
            .order = true;
        backend.order = Some("broker-a:8;broker-b:8;outside:256".into());
        backend.fail_broker = Some("broker-b".into());
        let request = TopicConfigFieldsPatchRequest::try_new(
            "orders",
            [target("broker-a", 7), target("broker-b", 9)],
            TopicConfigFieldsPatch {
                order: Some(false),
                ..Default::default()
            },
        )
        .unwrap();
        let report = execute_fields_patch(&mut backend, &request).await.unwrap();
        assert_eq!(report.order_reconciled, Some(true));
        assert_eq!(backend.order_writes, [Some("broker-b:8;outside:256".into())]);
    }

    #[tokio::test]
    async fn stale_versions_and_cas_races_return_full_authoritative_fields() {
        for race in [false, true] {
            let mut backend = fake();
            if race {
                backend.race_broker = Some("broker-a".into());
            }
            let request = TopicConfigFieldsPatchRequest::try_new(
                "orders",
                [target("broker-a", if race { 7 } else { 6 })],
                TopicConfigFieldsPatch {
                    perm: Some(2),
                    order: Some(true),
                    message_type: Some(TopicMessageType::Delay),
                    ..Default::default()
                },
            )
            .unwrap();
            let report = execute_fields_patch(&mut backend, &request).await.unwrap();
            let result = &report.targets[0];
            assert!(!result.applied);
            assert_eq!(result.failure, Some(MutationFailureCode::Conflict));
            let latest = result.latest.as_ref().unwrap();
            assert_eq!(latest.version, Some(if race { 9 } else { 7 }));
            assert_eq!(latest.configuration, Some(config(8, 6)));
            assert!(backend.writes.is_empty());
            assert!(backend.order_writes.is_empty());
        }
    }

    #[tokio::test]
    async fn same_version_persistence_failures_are_terminal_and_preserve_other_target_success() {
        for (applied, persistence, unknown) in [
            (false, MutationPersistenceState::Failed, false),
            (false, MutationPersistenceState::Unconfirmed, true),
            (true, MutationPersistenceState::Failed, false),
            (true, MutationPersistenceState::Unconfirmed, true),
        ] {
            let mut backend = fake();
            backend.write_results.insert(
                "broker-b".into(),
                TopicFieldsWriteResult {
                    applied,
                    version: Some(9),
                    persistence,
                },
            );
            let request = TopicConfigFieldsPatchRequest::try_new(
                "orders",
                [target("broker-a", 7), target("broker-b", 9)],
                TopicConfigFieldsPatch {
                    order: Some(true),
                    ..Default::default()
                },
            )
            .unwrap();
            let report = execute_fields_patch(&mut backend, &request).await.unwrap();
            assert!(report.targets[0].applied);
            assert_eq!(report.targets[0].failure, None);
            let failed = &report.targets[1];
            assert_eq!(failed.applied, applied);
            assert_eq!(failed.failure, Some(MutationFailureCode::PersistenceFailed));
            assert_eq!(failed.outcome_unknown, unknown);
            assert!(!failed.retryable);
            if !applied {
                assert!(
                    failed.latest.is_none(),
                    "persistence-dirty state is not a version rebase candidate"
                );
            }
            assert_eq!(report.order_reconciled, Some(true));
            assert_eq!(backend.order_writes, [Some("broker-a:8;outside:16".into())]);
        }
    }

    #[tokio::test]
    async fn rejected_same_version_without_a_state_conflict_is_not_rebased() {
        let mut backend = fake();
        backend.write_results.insert(
            "broker-a".into(),
            TopicFieldsWriteResult {
                applied: false,
                version: Some(7),
                persistence: MutationPersistenceState::NotRequired,
            },
        );
        let request = TopicConfigFieldsPatchRequest::try_new(
            "orders",
            [target("broker-a", 7)],
            TopicConfigFieldsPatch {
                perm: Some(4),
                ..Default::default()
            },
        )
        .unwrap();
        let report = execute_fields_patch(&mut backend, &request).await.unwrap();
        assert_eq!(report.targets[0].failure, Some(MutationFailureCode::Unavailable));
        assert!(!report.targets[0].outcome_unknown);
        assert!(report.targets[0].latest.is_none());
        assert!(backend.writes.is_empty());
    }

    #[tokio::test]
    async fn order_failure_or_concurrent_change_does_not_erase_applied_broker_success() {
        for concurrent in [false, true] {
            let mut backend = fake();
            backend.fail_order = !concurrent;
            backend.race_order = concurrent;
            let request = TopicConfigFieldsPatchRequest::try_new(
                "orders",
                [target("broker-a", 7)],
                TopicConfigFieldsPatch {
                    order: Some(true),
                    ..Default::default()
                },
            )
            .unwrap();
            let report = execute_fields_patch(&mut backend, &request).await.unwrap();
            assert!(report.targets[0].applied);
            assert_eq!(report.order_reconciled, Some(false));
            assert_eq!(backend.order_writes.len(), usize::from(!concurrent));
        }
    }

    #[tokio::test]
    async fn changed_exact_identity_rejects_before_any_broker_write() {
        let mut backend = fake();
        backend.identities_changed = true;
        let request = TopicConfigFieldsPatchRequest::try_new(
            "orders",
            [target("broker-a", 7)],
            TopicConfigFieldsPatch {
                perm: Some(4),
                ..Default::default()
            },
        )
        .unwrap();
        assert!(execute_fields_patch(&mut backend, &request).await.is_err());
        assert!(backend.writes.is_empty());
    }

    #[test]
    fn field_patch_rejects_invalid_combinations_and_ambiguous_targets() {
        for patch in [
            TopicConfigFieldsPatch::default(),
            TopicConfigFieldsPatch {
                read_queue_nums: Some(0),
                ..Default::default()
            },
            TopicConfigFieldsPatch {
                write_queue_nums: Some(129),
                perm: Some(6),
                ..Default::default()
            },
            TopicConfigFieldsPatch {
                perm: Some(1),
                message_type: Some(TopicMessageType::Fifo),
                ..Default::default()
            },
        ] {
            assert!(TopicConfigFieldsPatchRequest::try_new("orders", [target("broker-a", 7)], patch).is_err());
        }
        assert!(TopicConfigFieldsPatchRequest::try_new(
            "orders",
            [target("broker-a", 7), target("broker-a", 8)],
            TopicConfigFieldsPatch {
                perm: Some(4),
                ..Default::default()
            }
        )
        .is_err());
    }
}
