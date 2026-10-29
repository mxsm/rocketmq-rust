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

use std::collections::BTreeSet;

use rocketmq_dashboard_common::{
    AuditTopicField, TopicCompleteness, TopicConfigEditOutcome, TopicConfigFieldsPatch, TopicConfigTargetView,
    TopicConfigView, TopicInventory, TopicMessageType, TopicPermission, TopicTargetIdentity,
};

use super::*;
use crate::services::{ConfigMutation, topics::TopicFieldsPatchCommand};

fn command(patch: TopicConfigFieldsPatch) -> TopicFieldsPatchCommand {
    TopicFieldsPatchCommand {
        topic: TopicIdentity::parse("orders").unwrap(),
        targets: ["broker-a", "broker-b"]
            .into_iter()
            .map(|broker| TopicConfigTargetView {
                target: TopicTargetIdentity::parse("cluster-a", broker, format!("{broker}:10911")).unwrap(),
                version: 7,
                read_queue_count: 8,
                write_queue_count: 8,
                permission: TopicPermission::parse(6).ok(),
                ordered: true,
                message_type: TopicMessageType::Normal,
            })
            .collect(),
        patch,
    }
}

fn edited(states: &[(bool, Option<TopicFailureCode>)]) -> TopicConfigEditOutcome {
    TopicConfigEditOutcome {
        outcome: TopicPartialOutcome {
            topic: TopicIdentity::parse("orders").unwrap(),
            kind: TopicMutationKind::Edit,
            guarantee: TopicMutationGuarantee::VersionCas,
            targets: states
                .iter()
                .enumerate()
                .map(|(index, (applied, failure))| TopicTargetOutcome {
                    target: format!("broker-{index}"),
                    stage: TopicFailureStage::Mutation,
                    applied: *applied,
                    failure: *failure,
                    retryable: false,
                })
                .collect(),
            reload_failed: false,
        },
        conflicts: Vec::new(),
    }
}

fn queue_reload(backend: &FakeTopicBackend, failed: bool) {
    backend.queue_config(if failed {
        Err(UiError::new(
            "private-fields-refresh-diagnostic",
            UiErrorCode::Connection,
            true,
        ))
    } else {
        Ok(TopicConfigView {
            topic: TopicIdentity::parse("orders").unwrap(),
            targets: Vec::new(),
            inconsistent_fields: Vec::new(),
            completeness: TopicCompleteness::Complete,
            failures: Vec::new(),
        })
    });
    backend.queue_inventory(Ok(TopicInventory {
        items: Vec::new(),
        targets: Vec::new(),
        completeness: TopicCompleteness::Complete,
        failures: Vec::new(),
    }));
}

#[test]
fn audit_topic_fields_waiter_drop_preserves_intent_actor_environment_and_partial_unknown() {
    let mut fixture = Fixture::new();
    fixture
        .runtime
        .block_on(fixture.services.authenticate("operator", "private-password"))
        .unwrap();
    let backend = Arc::new(FakeTopicBackend::default());
    let started = backend.observe_next_fields();
    let release = backend.queue_pending_fields();
    queue_reload(&backend, true);
    fixture.services = fixture.services.with_topic_backend(backend.clone());
    let original_environment = fixture.store.environments().unwrap().active_id;
    let expected_fields = BTreeSet::from([
        AuditTopicField::ReadQueueCount,
        AuditTopicField::Permission,
        AuditTopicField::Ordered,
        AuditTopicField::MessageType,
    ]);
    let patch = TopicConfigFieldsPatch {
        read_queue_count: Some(12),
        permission: TopicPermission::parse(4).ok(),
        ordered: Some(false),
        message_type: Some(TopicMessageType::Delay),
        ..Default::default()
    };
    fixture.runtime.block_on(async {
        let mut waiter = Box::pin(
            fixture
                .services
                .patch_topic_fields(TopicRequestScope { revision: 0, epoch: 1 }, command(patch)),
        );
        tokio::select! {
            result = &mut waiter => panic!("field edit completed before release: {result:?}"),
            result = started => result.unwrap(),
        }
        let accepted = fixture.services.audit_pending().unwrap().pop().unwrap();
        assert_eq!(accepted.topic_fields, expected_fields);
        assert_eq!(accepted.context.environment_id, original_environment);
        drop(waiter);
        fixture.services.sign_out().await.unwrap();
        let (progress, _updates) = tokio::sync::mpsc::unbounded_channel();
        fixture
            .services
            .mutate_with_progress(ConfigMutation::CreateEnvironment("Staging".into()), progress)
            .await
            .unwrap();
        assert_ne!(fixture.store.environments().unwrap().active_id, original_environment);
        let mut changes = fixture.services.audit_changes().unwrap();
        changes.borrow_and_update();
        assert!(
            release
                .send(Ok(edited(&[
                    (true, None),
                    (false, Some(TopicFailureCode::Unconfirmed))
                ])))
                .is_ok()
        );
        changes.changed().await.unwrap();
        fixture.services.retry_audit_writes().await.unwrap();
        let report = fixture.mutations.task_group().shutdown(Duration::from_secs(5)).await;
        assert!(report.is_healthy());
    });
    let events = fixture.events();
    let event = events
        .iter()
        .find(|event| event.operation.action == AuditAction::TopicUpdated)
        .unwrap();
    assert_eq!(
        event.operation.context.actor,
        OperationActor::LocalOperator {
            username: "operator".into()
        }
    );
    assert_eq!(event.operation.context.environment_id, original_environment);
    assert_eq!(event.operation.context.configuration_revision, 0);
    assert_eq!(
        event.operation.context.resource,
        OperationResource::Topic("orders".into())
    );
    assert_eq!(event.operation.topic_fields, expected_fields);
    assert_eq!(event.outcome, AuditOutcome::Partial);
    assert_eq!(event.facts.applied_targets, 1);
    assert_eq!(event.facts.unconfirmed_targets, 1);
    assert!(event.facts.refresh_failed);
    assert!(fixture.services.audit_pending().unwrap().is_empty());
    let calls = backend.calls();
    assert_eq!(calls.fields.len(), 1);
    assert_eq!(calls.fields[0].patch, patch);
    assert_eq!(calls.fields[0].targets[0].version, 7);
    assert_eq!(calls.config.len(), 1);
    assert_eq!(calls.inventory.len(), 1);
    let file = std::fs::read_to_string(fixture.store.path()).unwrap();
    for excluded in [
        "private-password",
        "private-fields-refresh-diagnostic",
        "readQueueCount",
        "writeQueueCount",
    ] {
        assert!(!file.contains(excluded));
    }
    fixture.runtime.shutdown_runtime_blocking().unwrap();
}

#[test]
fn audit_topic_fields_distinguishes_known_persistence_failure_conflict_unknown_and_refresh() {
    let mut fixture = Fixture::new();
    let backend = Arc::new(FakeTopicBackend::default());
    fixture.services = fixture.services.with_topic_backend(backend.clone());
    use TopicFailureCode::{Conflict, Unavailable, Unconfirmed};
    let cases = [
        (
            vec![(false, Some(Conflict))],
            AuditOutcome::Rejected,
            0,
            0,
            false,
            Some(AuditFailureCode::Conflict),
        ),
        (
            vec![(false, Some(Unavailable))],
            AuditOutcome::Failed,
            0,
            0,
            false,
            Some(AuditFailureCode::Connection),
        ),
        (
            vec![(true, Some(Unavailable))],
            AuditOutcome::Partial,
            1,
            0,
            false,
            Some(AuditFailureCode::Connection),
        ),
        (
            vec![(false, Some(Unconfirmed))],
            AuditOutcome::Unknown,
            0,
            1,
            false,
            Some(AuditFailureCode::Unknown),
        ),
        (
            vec![(true, None), (false, Some(Conflict))],
            AuditOutcome::Partial,
            1,
            0,
            false,
            Some(AuditFailureCode::Conflict),
        ),
        (
            vec![(true, Some(Unconfirmed))],
            AuditOutcome::Partial,
            1,
            1,
            false,
            Some(AuditFailureCode::Unknown),
        ),
        (vec![(true, None)], AuditOutcome::Succeeded, 1, 0, true, None),
    ];
    for (states, expected, applied, unconfirmed, refresh_failed, failure) in cases {
        backend.queue_fields(Ok(edited(&states)));
        if applied > 0 || unconfirmed > 0 {
            queue_reload(&backend, refresh_failed);
        }
        fixture
            .runtime
            .block_on(fixture.services.patch_topic_fields(
                TopicRequestScope { revision: 0, epoch: 1 },
                command(TopicConfigFieldsPatch {
                    read_queue_count: Some(12),
                    ..Default::default()
                }),
            ))
            .unwrap();
        let event = fixture.events().remove(0);
        assert_eq!(event.outcome, expected, "{states:?}");
        assert_eq!(event.facts.applied_targets, applied);
        assert_eq!(event.facts.unconfirmed_targets, unconfirmed);
        assert_eq!(event.facts.refresh_failed, refresh_failed);
        assert_eq!(event.facts.failure_code, failure);
        assert_eq!(
            event.operation.topic_fields,
            BTreeSet::from([AuditTopicField::ReadQueueCount])
        );
    }
    assert_eq!(backend.calls().fields.len(), 7);
    fixture.runtime.shutdown_runtime_blocking().unwrap();
}

#[test]
fn audit_topic_fields_rejected_backend_retains_intent_and_terminal_retry_never_replays() {
    let mut fixture = Fixture::new();
    let backend = Arc::new(FakeTopicBackend::default());
    let started = backend.observe_next_fields();
    let release = backend.queue_pending_fields();
    fixture.services = fixture.services.with_topic_backend(backend.clone());
    let accepted = fixture.runtime.block_on(async {
        let mut waiter = Box::pin(fixture.services.patch_topic_fields(
            TopicRequestScope { revision: 0, epoch: 1 },
            command(TopicConfigFieldsPatch {
                ordered: Some(false),
                ..Default::default()
            }),
        ));
        tokio::select! {
            result = &mut waiter => panic!("field edit completed before release: {result:?}"),
            result = started => result.unwrap(),
        }
        let accepted = fixture.services.audit_pending().unwrap().pop().unwrap();
        let backup = fixture.directory.path().join("saved-config.json");
        std::fs::rename(fixture.store.path(), &backup).unwrap();
        std::fs::create_dir(fixture.store.path()).unwrap();
        assert!(
            release
                .send(Err(UiError::new(
                    "private-field-validation-diagnostic",
                    UiErrorCode::Validation,
                    false
                )))
                .is_ok()
        );
        assert!(waiter.await.is_err());
        assert!(fixture.services.audit_write_warning());
        std::fs::remove_dir(fixture.store.path()).unwrap();
        std::fs::rename(backup, fixture.store.path()).unwrap();
        fixture.services.retry_audit_writes().await.unwrap();
        fixture.services.retry_audit_writes().await.unwrap();
        accepted
    });
    let events = fixture.events();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].operation, accepted);
    assert_eq!(
        events[0].operation.topic_fields,
        BTreeSet::from([AuditTopicField::Ordered])
    );
    assert_eq!(events[0].outcome, AuditOutcome::Rejected);
    assert_eq!(events[0].facts.failure_code, Some(AuditFailureCode::Validation));
    assert_eq!(backend.calls().fields.len(), 1);
    assert!(backend.calls().config.is_empty());
    assert!(fixture.services.audit_pending().unwrap().is_empty());
    assert!(!fixture.services.audit_write_warning());
    assert!(
        !std::fs::read_to_string(fixture.store.path())
            .unwrap()
            .contains("private-field-validation-diagnostic")
    );
    fixture.runtime.shutdown_runtime_blocking().unwrap();
}
