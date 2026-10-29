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

use super::*;
use rocketmq_dashboard_common::{
    AuditConsumerField, ConsumerConfigIdentity, ConsumerConfigPatch, ConsumerConfigPatchCommand,
    ConsumerConfigPatchOutcome, ConsumerConfigSnapshot,
};
use std::collections::BTreeSet;

#[test]
fn audit_consumer_broker_targets_and_cleanup_steps_keep_distinct_terminal_facts_without_replay() {
    use rocketmq_dashboard_common::{
        AuditConsumerCleanupFacts, ConsumerCapabilities, ConsumerFailureCode, ConsumerFailureStage, ConsumerInventory,
        ConsumerMutationGuarantee, ConsumerMutationKind, ConsumerObservationState, ConsumerPartialOutcome,
        ConsumerTargetOutcome,
    };
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Case {
        AllCleanupApplied,
        CleanupRejected,
        CleanupUnconfirmed,
        PreflightRejected,
        MutationUnconfirmed,
    }
    for case in [
        Case::AllCleanupApplied,
        Case::CleanupRejected,
        Case::CleanupUnconfirmed,
        Case::PreflightRejected,
        Case::MutationUnconfirmed,
    ] {
        let mut fixture = Fixture::new();
        let backend = Arc::new(FakeConsumerBackend::default());
        let target = ConsumerTargetIdentity::parse("cluster-a", "broker-a", "a:10911").unwrap();
        let step = |name: &str, stage, applied, failure| ConsumerTargetOutcome {
            target: name.into(),
            stage,
            applied,
            failure,
            retryable: !applied,
        };
        let mut outcome = ConsumerPartialOutcome {
            group: ConsumerIdentity::parse("orders").unwrap(),
            kind: ConsumerMutationKind::Delete,
            guarantee: ConsumerMutationGuarantee::PreflightBestEffort,
            targets: vec![
                step("broker-a", ConsumerFailureStage::Mutation, true, None),
                step("retry-topic", ConsumerFailureStage::Cleanup, true, None),
                step("dlq-topic", ConsumerFailureStage::Cleanup, true, None),
            ],
            reload_failed: false,
        };
        let (expected, applied, unconfirmed, cleanup) = match case {
            Case::AllCleanupApplied => (
                AuditOutcome::Succeeded,
                1,
                0,
                Some(AuditConsumerCleanupFacts {
                    applied_steps: 2,
                    ..Default::default()
                }),
            ),
            Case::CleanupRejected => {
                outcome.targets[2].applied = false;
                outcome.targets[2].failure = Some(ConsumerFailureCode::NotApplied);
                (
                    AuditOutcome::Partial,
                    1,
                    0,
                    Some(AuditConsumerCleanupFacts {
                        applied_steps: 1,
                        failed_steps: 1,
                        unconfirmed_steps: 0,
                    }),
                )
            }
            Case::CleanupUnconfirmed => {
                outcome.targets[2].applied = false;
                outcome.targets[2].failure = Some(ConsumerFailureCode::Unavailable);
                (
                    AuditOutcome::Partial,
                    1,
                    0,
                    Some(AuditConsumerCleanupFacts {
                        applied_steps: 1,
                        failed_steps: 0,
                        unconfirmed_steps: 1,
                    }),
                )
            }
            Case::PreflightRejected => {
                outcome.targets = vec![step(
                    "broker-a",
                    ConsumerFailureStage::ConnectionObservation,
                    false,
                    Some(ConsumerFailureCode::Conflict),
                )];
                (AuditOutcome::Rejected, 0, 0, None)
            }
            Case::MutationUnconfirmed => {
                outcome.targets = vec![step(
                    "broker-a",
                    ConsumerFailureStage::Mutation,
                    false,
                    Some(ConsumerFailureCode::Unavailable),
                )];
                (AuditOutcome::Unknown, 0, 1, None)
            }
        };
        backend.queue_delete(Ok(outcome.clone()));
        if outcome.has_applied_effects() {
            backend.queue_inventory(Ok(ConsumerInventory {
                groups: Vec::new(),
                targets: vec![target.clone()],
                target_observation: ConsumerObservationState::Complete,
                group_observation: ConsumerObservationState::Complete,
                observation: ConsumerObservationState::Complete,
                failures: Vec::new(),
                capabilities: ConsumerCapabilities::for_scope(rocketmq_dashboard_common::ConnectionScope::NameServer),
            }));
        }
        fixture.services = fixture.services.with_consumer_backend(backend.clone());
        fixture.runtime.block_on(async {
            fixture
                .services
                .delete_consumer(
                    ConsumerRequestScope {
                        revision: 0,
                        epoch: 1,
                        query_scope: Default::default(),
                    },
                    rocketmq_dashboard_common::ConsumerDeleteCommand {
                        group: outcome.group.clone(),
                        selected_targets: vec![target.clone()],
                        authoritative_targets: vec![target],
                        authorization: ConsumerAclClassification::Authorized,
                    },
                )
                .await
                .unwrap();
            fixture.services.retry_audit_writes().await.unwrap();
        });
        let event = fixture.events().pop().unwrap();
        assert_eq!(event.outcome, expected, "case {case:?}");
        assert_eq!(event.facts.applied_targets, applied, "case {case:?}");
        assert_eq!(event.facts.unconfirmed_targets, unconfirmed, "case {case:?}");
        assert_eq!(event.facts.consumer_cleanup, cleanup, "case {case:?}");
        if case == Case::PreflightRejected {
            assert_eq!(event.facts.failure_code, Some(AuditFailureCode::Conflict));
        }
        if case == Case::MutationUnconfirmed {
            assert_eq!(event.facts.failure_code, Some(AuditFailureCode::Unknown));
        }
        assert_eq!(backend.calls().delete.len(), 1);
        fixture.runtime.shutdown_runtime_blocking().unwrap();
    }
}

#[test]
fn audit_consumer_edit_dropped_waiter_keeps_reviewed_intent_and_never_persists_config_or_jstack() {
    let mut fixture = Fixture::new();
    fixture
        .runtime
        .block_on(fixture.services.authenticate("operator", "private-password"))
        .unwrap();
    let backend = Arc::new(FakeConsumerBackend::default());
    let started = backend.observe_next_patch();
    let release = backend.queue_pending_patch();
    let group = ConsumerIdentity::parse("orders-consumer").unwrap();
    let target = ConsumerTargetIdentity::parse("cluster-a", "broker-a", "a:10911").unwrap();
    let command = ConsumerConfigPatchCommand {
        snapshot: ConsumerConfigSnapshot {
            identity: ConsumerConfigIdentity {
                group: group.clone(),
                target,
            },
            generation: 7,
            entries: ConsumerConfigEntries::default(),
            read_only: rocketmq_dashboard_common::ConsumerConfigReadOnly {
                attributes: vec![("private-config-key".into(), "private-config-value".into())],
                ..Default::default()
            },
        },
        patch: ConsumerConfigPatch {
            retry_max_times: Some(3),
            consume_enable: Some(false),
            broker_id: Some(9),
            ..Default::default()
        },
        authorization: ConsumerAclClassification::Authorized,
    };
    backend.queue_configuration(Err(UiError::new(
        "private-refresh-diagnostic",
        UiErrorCode::Connection,
        true,
    )));
    fixture.services = fixture.services.with_consumer_backend(backend.clone());
    let expected = BTreeSet::from([
        AuditConsumerField::RetryMaxTimes,
        AuditConsumerField::ConsumeEnable,
        AuditConsumerField::BrokerId,
    ]);
    fixture.runtime.block_on(async {
        let mut waiter=Box::pin(fixture.services.patch_consumer_config(ConsumerRequestScope {revision:0,epoch:1,query_scope:Default::default()},command));
        tokio::select! {result=&mut waiter=>panic!("edit completed before release: {}",result.is_ok()),result=started=>result.unwrap(),}
        let pending=fixture.services.audit_pending().unwrap().pop().unwrap();assert_eq!(pending.consumer_fields,expected);
        drop(waiter);fixture.services.sign_out().await.unwrap();
        let mut changes=fixture.services.audit_changes().unwrap();changes.borrow_and_update();
        assert!(release.send(Ok(ConsumerConfigPatchOutcome::Applied {previous_generation:7,generation:8})).is_ok());
        changes.changed().await.unwrap();fixture.services.retry_audit_writes().await.unwrap();
        assert!(fixture.mutations.task_group().shutdown(Duration::from_secs(5)).await.is_healthy());
    });
    let event = fixture
        .events()
        .into_iter()
        .find(|event| event.operation.action == AuditAction::ConsumerUpdated)
        .unwrap();
    assert_eq!(event.operation.consumer_fields, expected);
    assert!(event.operation.topic_fields.is_empty());
    assert_eq!(event.outcome, AuditOutcome::Succeeded);
    assert!(event.facts.refresh_failed);
    assert_eq!(event.facts.applied_targets, 1);
    assert_eq!(
        event.operation.context.actor,
        OperationActor::LocalOperator {
            username: "operator".into()
        }
    );
    let calls = backend.calls();
    assert_eq!(calls.patch.len(), 1);
    assert_eq!(calls.patch[0].1.patch.retry_max_times, Some(3));
    let persisted = std::fs::read_to_string(fixture.store.path()).unwrap();
    for private in [
        "private-config-key",
        "private-config-value",
        "private-refresh-diagnostic",
        "private-password",
        "jstack",
    ] {
        assert!(!persisted.contains(private));
    }
    fixture.runtime.shutdown_runtime_blocking().unwrap();
}

#[test]
fn audit_consumer_conflict_is_rejected_with_only_the_requested_field_intent() {
    let mut fixture = Fixture::new();
    let backend = Arc::new(FakeConsumerBackend::default());
    backend.queue_patch(Ok(ConsumerConfigPatchOutcome::GenerationConflict {
        expected_generation: 7,
        actual_generation: 8,
    }));
    fixture.services = fixture.services.with_consumer_backend(backend.clone());
    let command = ConsumerConfigPatchCommand {
        snapshot: ConsumerConfigSnapshot {
            identity: ConsumerConfigIdentity {
                group: ConsumerIdentity::parse("orders").unwrap(),
                target: ConsumerTargetIdentity::parse("cluster", "a", "a:10911").unwrap(),
            },
            generation: 7,
            entries: Default::default(),
            read_only: Default::default(),
        },
        patch: ConsumerConfigPatch {
            retry_queue_nums: Some(0),
            ..Default::default()
        },
        authorization: ConsumerAclClassification::Authorized,
    };
    fixture
        .runtime
        .block_on(fixture.services.patch_consumer_config(
            ConsumerRequestScope {
                revision: 0,
                epoch: 1,
                query_scope: Default::default(),
            },
            command,
        ))
        .unwrap();
    let event = fixture.events().pop().unwrap();
    assert_eq!(event.outcome, AuditOutcome::Rejected);
    assert_eq!(event.facts.failure_code, Some(AuditFailureCode::Conflict));
    assert_eq!(
        event.operation.consumer_fields,
        BTreeSet::from([AuditConsumerField::RetryQueueNums])
    );
    assert_eq!(backend.calls().patch.len(), 1);
    fixture.runtime.shutdown_runtime_blocking().unwrap();
}

#[test]
fn audit_consumer_persistence_failure_and_uncertainty_keep_distinct_terminal_facts_without_replay() {
    use rocketmq_dashboard_common::ConsumerConfigPersistence;
    for (applied, persistence, expected, unconfirmed) in [
        (false, ConsumerConfigPersistence::Failed, AuditOutcome::Failed, 0),
        (true, ConsumerConfigPersistence::Failed, AuditOutcome::Partial, 0),
        (true, ConsumerConfigPersistence::Unconfirmed, AuditOutcome::Partial, 1),
    ] {
        let mut fixture = Fixture::new();
        let backend = Arc::new(FakeConsumerBackend::default());
        backend.queue_patch(Ok(ConsumerConfigPatchOutcome::PersistenceIncomplete {
            applied,
            previous_generation: 7,
            generation: 7 + u64::from(applied),
            persistence,
        }));
        fixture.services = fixture.services.with_consumer_backend(backend.clone());
        let command = ConsumerConfigPatchCommand {
            snapshot: ConsumerConfigSnapshot {
                identity: ConsumerConfigIdentity {
                    group: ConsumerIdentity::parse("orders").unwrap(),
                    target: ConsumerTargetIdentity::parse("cluster", "a", "a:10911").unwrap(),
                },
                generation: 7,
                entries: Default::default(),
                read_only: Default::default(),
            },
            patch: ConsumerConfigPatch {
                retry_max_times: Some(3),
                ..Default::default()
            },
            authorization: ConsumerAclClassification::Authorized,
        };
        fixture.runtime.block_on(async {
            let result = fixture
                .services
                .patch_consumer_config(
                    ConsumerRequestScope {
                        revision: 0,
                        epoch: 1,
                        query_scope: Default::default(),
                    },
                    command,
                )
                .await
                .unwrap();
            assert!(matches!(
                result,
                crate::services::consumers::ConsumerConfigMutationResult::PersistenceIncomplete { .. }
            ));
            fixture.services.retry_audit_writes().await.unwrap();
        });
        let event = fixture.events().pop().unwrap();
        assert_eq!(event.outcome, expected);
        assert_eq!(event.facts.unconfirmed_targets, unconfirmed);
        assert_eq!(event.facts.applied_targets, u32::from(applied));
        assert_eq!(backend.calls().patch.len(), 1);
        assert!(backend.calls().configuration.is_empty());
        fixture.runtime.shutdown_runtime_blocking().unwrap();
    }
}
