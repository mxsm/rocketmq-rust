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

use rocketmq_dashboard_common::{
    AuditAction, AuditFilter, EnvironmentId, OperationActor, OperationContext, OperationResource,
};
use rocketmq_runtime::{ProcessMemoryLimit, RuntimeConfig, RuntimeOwner};

use super::*;

fn operation(index: usize) -> AuditAcceptedOperation {
    AuditAcceptedOperation {
        context: OperationContext {
            environment_id: EnvironmentId::parse("env-a").unwrap(),
            configuration_revision: 3,
            request_id: format!("request-{index}"),
            actor: OperationActor::LocalOperator {
                username: "operator".into(),
            },
            resource: OperationResource::Topic("orders".into()),
        },
        action: AuditAction::TopicCreated,
        accepted_at_epoch_ms: 100,
        topic_fields: Default::default(),
        consumer_fields: Default::default(),
    }
}

fn query(limit: usize) -> AuditQuery {
    AuditQuery {
        filter: AuditFilter {
            start_epoch_ms: 0,
            end_epoch_ms: 200,
            actor: None,
            action: None,
            outcome: None,
            environment_id: None,
        },
        limit,
        cursor: None,
    }
}

fn runtime() -> RuntimeOwner {
    RuntimeOwner::plan(RuntimeConfig::for_parallelism("audit-store-test", 1))
        .unwrap()
        .with_memory_limit(ProcessMemoryLimit::configured(256 * 1024 * 1024).unwrap())
        .build()
        .unwrap()
}

#[test]
fn audit_cursor_excludes_later_commits_and_rejects_changed_filters() {
    let mut journal = AuditJournal::default();
    for index in 0..5 {
        journal.accept(operation(index)).unwrap();
        journal
            .finish(operation(index), AuditOutcome::Succeeded, AuditFacts::default(), 100)
            .unwrap();
    }
    let mut request = query(2);
    let first = journal.query(request.clone()).unwrap();
    assert_eq!(
        first.events.iter().map(|event| event.event_id).collect::<Vec<_>>(),
        [5, 4]
    );
    journal
        .finish(operation(9), AuditOutcome::Succeeded, AuditFacts::default(), 100)
        .unwrap();
    request.cursor = first.next_cursor;
    let second = journal.query(request.clone()).unwrap();
    assert_eq!(
        second.events.iter().map(|event| event.event_id).collect::<Vec<_>>(),
        [3, 2]
    );
    request.cursor = second.next_cursor;
    let third = journal.query(request.clone()).unwrap();
    assert_eq!(third.events.iter().map(|event| event.event_id).collect::<Vec<_>>(), [1]);
    assert!(third.next_cursor.is_none());
    request.filter.action = Some(AuditAction::TopicDeleted);
    assert!(journal.query(request).is_err());
    let mut invalid = query(2);
    invalid.filter.end_epoch_ms = AuditFilter::MAX_WINDOW_MS + 1;
    assert!(journal.query(invalid).is_err());
}

#[test]
fn audit_cursor_expires_when_retention_removes_only_part_of_its_unread_rows() {
    let mut journal = AuditJournal {
        events: (1..=MAX_EVENTS)
            .map(|index| AuditEvent {
                event_id: u64::try_from(index).unwrap(),
                operation: operation(index),
                outcome: AuditOutcome::Succeeded,
                timestamp_epoch_ms: 100,
                facts: AuditFacts::default(),
            })
            .collect(),
        next_event_id: u64::try_from(MAX_EVENTS).unwrap() + 1,
        ..AuditJournal::default()
    };
    journal.validate().unwrap();
    let mut request = query(2);
    let first = journal.query(request.clone()).unwrap();
    assert_eq!(
        first.events.iter().map(|event| event.event_id).collect::<Vec<_>>(),
        [10_000, 9_999]
    );
    request.cursor = first.next_cursor;
    for index in MAX_EVENTS + 1..=MAX_EVENTS + 200 {
        journal
            .finish(operation(index), AuditOutcome::Succeeded, AuditFacts::default(), 100)
            .unwrap();
    }
    assert_eq!(journal.events.len(), MAX_EVENTS);
    assert_eq!(journal.events.first().unwrap().event_id, 201);
    assert!(
        journal.query(request).is_err(),
        "continuation must not silently omit the 200 evicted unread rows"
    );
}

#[test]
fn audit_has_one_terminal_per_request_and_checks_attribution_and_bounds() {
    let mut journal = AuditJournal::default();
    let accepted = operation(0);
    journal.accept(accepted.clone()).unwrap();
    assert!(
        journal.accept(accepted.clone()).is_err(),
        "duplicate acceptance must never authorize replay"
    );
    journal
        .finish(
            accepted.clone(),
            AuditOutcome::Partial,
            AuditFacts {
                applied_targets: 1,
                unconfirmed_targets: 1,
                ..AuditFacts::default()
            },
            110,
        )
        .unwrap();
    journal
        .finish(accepted.clone(), AuditOutcome::Succeeded, AuditFacts::default(), 120)
        .unwrap();
    assert_eq!(journal.events.len(), 1);
    assert_eq!(journal.events[0].outcome, AuditOutcome::Partial);
    let mut changed = accepted;
    changed.context.actor = OperationActor::System;
    assert!(
        journal
            .finish(changed, AuditOutcome::Succeeded, AuditFacts::default(), 120)
            .is_err()
    );
    let mut oversized = operation(1);
    oversized.context.resource = OperationResource::Topic("x".repeat(513));
    assert!(journal.accept(oversized).is_err());
    journal.validate().unwrap();
}

#[test]
fn audit_independent_requests_for_one_resource_do_not_lock_or_resolve_each_other() {
    let mut journal = AuditJournal::default();
    let unresolved = operation(1);
    let confirmed_new_request = operation(2);
    assert_eq!(unresolved.context.resource, confirmed_new_request.context.resource);
    journal.accept(unresolved.clone()).unwrap();
    journal.accept(confirmed_new_request.clone()).unwrap();
    journal
        .finish(
            confirmed_new_request.clone(),
            AuditOutcome::Succeeded,
            AuditFacts::default(),
            110,
        )
        .unwrap();
    assert_eq!(journal.pending, [unresolved]);
    assert_eq!(journal.events.len(), 1);
    assert_eq!(journal.events[0].operation, confirmed_new_request);
}

#[test]
fn audit_retention_expires_cursors_but_keeps_unresolved_requests_and_limits_new_admission() {
    let mut journal = AuditJournal::default();
    journal.accept(operation(0)).unwrap();
    journal
        .finish(operation(1), AuditOutcome::Succeeded, AuditFacts::default(), 100)
        .unwrap();
    journal
        .finish(operation(2), AuditOutcome::Succeeded, AuditFacts::default(), 101)
        .unwrap();
    let mut request = query(1);
    request.cursor = journal.query(request.clone()).unwrap().next_cursor;
    journal
        .finish(
            operation(3),
            AuditOutcome::Succeeded,
            AuditFacts::default(),
            AuditFilter::MAX_WINDOW_MS + 200,
        )
        .unwrap();
    assert!(journal.query(request).is_err());
    assert_eq!(journal.pending, [operation(0)]);
    assert_eq!(journal.events.len(), 1);
    for index in 1..MAX_PENDING {
        let mut pending = operation(index + 100);
        pending.context.resource = OperationResource::Topic(format!("pending-{index}"));
        journal.accept(pending).unwrap();
    }
    let mut overflow = operation(10_000);
    overflow.context.resource = OperationResource::Topic("overflow".into());
    assert!(journal.accept(overflow).is_err());
    assert_eq!(journal.pending[0], operation(0));
    journal.validate().unwrap();
}

#[test]
fn audit_restart_retains_unfinished_requests_without_inventing_an_outcome() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("config.json");
    let runtime = runtime();
    let config = DesktopConfigStore::new(path.clone(), runtime.root_context().component("first-store"));
    runtime.block_on(config.load()).unwrap();
    let audit = AuditStore::new(Arc::clone(&config));
    runtime.block_on(audit.accept(operation(0))).unwrap();
    drop(audit);
    drop(config);
    let config = DesktopConfigStore::new(path, runtime.root_context().component("reopened-store"));
    runtime.block_on(config.load()).unwrap();
    let audit = AuditStore::new(config);
    assert_eq!(audit.pending().unwrap(), [operation(0)]);
    assert!(audit.query(query(20)).unwrap().events.is_empty());
    runtime.shutdown_runtime_blocking().unwrap();
}

#[test]
fn audit_schema_two_migrates_committed_local_records_and_requires_journal_in_schema_three() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("config.json");
    let runtime = runtime();
    let config = DesktopConfigStore::new(path.clone(), runtime.root_context().component("config"));
    runtime.block_on(async {
        let mut settings = config.load().await.unwrap();
        settings.transport.use_tls = true;
        config.save_next(settings).await.unwrap();
    });
    let mut value: serde_json::Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    value["schemaVersion"] = 2.into();
    value.as_object_mut().unwrap().remove("audit");
    std::fs::write(&path, serde_json::to_vec(&value).unwrap()).unwrap();
    assert!(runtime.block_on(config.load()).unwrap().transport.use_tls);
    let events = AuditStore::new(Arc::clone(&config))
        .query(AuditQuery {
            filter: AuditFilter::recent(crate::services::audit::now_epoch_ms().unwrap()),
            limit: 20,
            cursor: None,
        })
        .unwrap()
        .events;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].operation.action, AuditAction::ConfigurationSaved);
    value["schemaVersion"] = 3.into();
    let missing_journal = serde_json::to_vec(&value).unwrap();
    std::fs::write(&path, &missing_journal).unwrap();
    assert!(runtime.block_on(config.load()).is_err());
    assert_eq!(std::fs::read(path).unwrap(), missing_journal);
    runtime.shutdown_runtime_blocking().unwrap();
}

#[cfg(windows)]
#[test]
fn audit_local_config_and_monitor_commit_failure_cannot_publish_state_or_event_separately() {
    use std::os::windows::fs::OpenOptionsExt as _;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("config.json");
    let runtime = runtime();
    let config = DesktopConfigStore::new(path.clone(), runtime.root_context().component("config"));
    let mut settings = runtime.block_on(config.load()).unwrap();
    let original = std::fs::read(&path).unwrap();
    let open_original = std::fs::OpenOptions::new()
        .read(true)
        .share_mode(1)
        .open(&path)
        .unwrap();
    settings.transport.use_tls = true;
    assert!(runtime.block_on(config.save_next(settings.clone())).is_err());
    let monitors = crate::infrastructure::monitor_store::MonitorStore::with_config_store(Arc::clone(&config));
    let rule = crate::infrastructure::monitor_store::MonitorRule {
        id: "orders".into(),
        consumer_group: "orders".into(),
        enabled: true,
    };
    assert!(runtime.block_on(monitors.upsert(rule.clone())).is_err());
    assert_eq!(std::fs::read(&path).unwrap(), original);
    assert!(runtime.block_on(monitors.list()).unwrap().is_empty());
    let audit = AuditStore::new(Arc::clone(&config));
    assert!(audit.query(query(20)).unwrap().events.is_empty());
    drop(open_original);
    runtime.block_on(config.save_next(settings)).unwrap();
    runtime.block_on(monitors.upsert(rule)).unwrap();
    let value: serde_json::Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    assert_eq!(value["profiles"][0]["transport"]["useTLS"], true);
    assert_eq!(value["audit"]["events"].as_array().unwrap().len(), 2);
    assert_eq!(value["monitors"]["rules"][0]["rule"]["consumerGroup"], "orders");
    assert_eq!(value["audit"]["events"][1]["operation"]["action"], "monitor_saved");
    runtime.shutdown_runtime_blocking().unwrap();
}
