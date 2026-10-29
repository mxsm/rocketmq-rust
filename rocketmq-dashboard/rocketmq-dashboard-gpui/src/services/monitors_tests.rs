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

use std::{path::PathBuf, sync::Arc, time::Duration};

use rocketmq_dashboard_common::{
    AuditEvent, AuditFilter, AuditQuery, EnvironmentId, MonitorRuleStatus, MonitorRuleView, MonitorThresholds,
    OperationActor,
};
use rocketmq_runtime::{ChildServiceContext, ProcessMemoryLimit, RuntimeConfig, RuntimeOwner};

use super::*;
use crate::{
    infrastructure::{
        auth_state::{DesktopAuthState, LOGIN_PASSWORD_ENV, LOGIN_USERNAME_ENV, MapEnvironment},
        config_store::{CONFIG_SCHEMA_VERSION, DesktopConfigStore},
    },
    services::ConfigMutation,
};

pub(crate) struct Fixture {
    pub runtime: RuntimeOwner,
    pub store: Arc<DesktopConfigStore>,
    pub services: AppServices,
    pub mutations: ChildServiceContext,
    directory: Option<tempfile::TempDir>,
}

impl Fixture {
    pub fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let mut fixture = Self::at(directory.path().join("config.json"));
        fixture.directory = Some(directory);
        fixture
    }

    fn at(path: PathBuf) -> Self {
        let runtime = RuntimeOwner::plan(RuntimeConfig::for_parallelism("monitor-rules-test", 1))
            .unwrap()
            .with_memory_limit(ProcessMemoryLimit::configured(256 * 1024 * 1024).unwrap())
            .build()
            .unwrap();
        let store = DesktopConfigStore::new(path, runtime.root_context().component("storage"));
        let mutations = runtime.root_context().component("mutations");
        let auth = DesktopAuthState::new(Arc::new(MapEnvironment::new([
            (LOGIN_USERNAME_ENV, "operator"),
            (LOGIN_PASSWORD_ENV, "private-password"),
        ])));
        let services = AppServices::desktop_with_fake_provider(
            Arc::clone(&store),
            auth,
            mutations.clone(),
            runtime.root_context().component("history"),
            runtime.root_context().component("monitor"),
        );
        runtime.block_on(services.bootstrap()).unwrap();
        Self {
            runtime,
            store,
            services,
            mutations,
            directory: None,
        }
    }

    pub fn environment(&self) -> EnvironmentId {
        self.store.environment_snapshot().unwrap().environment_id
    }
    pub fn revision(&self) -> u64 {
        self.store.environment_snapshot().unwrap().connection.revision
    }

    pub fn request(
        &self,
        group: &str,
        expected_revision: u64,
        min_count: u32,
        max_diff_total: u64,
    ) -> MonitorRuleMutation {
        MonitorRuleMutation {
            environment_id: self.environment(),
            consumer_group: group.into(),
            expected_revision,
            change: MonitorRuleChange::Save {
                thresholds: MonitorThresholds {
                    min_count,
                    max_diff_total,
                },
                enabled: true,
                adopt_legacy_id: None,
            },
        }
    }

    pub fn events(&self) -> Vec<AuditEvent> {
        self.services
            .query_audit(AuditQuery {
                filter: AuditFilter::recent(crate::services::audit::now_epoch_ms().unwrap()),
                limit: 200,
                cursor: None,
            })
            .unwrap()
            .events
    }

    pub(crate) fn hold_next_commit(&self) -> (tokio::sync::oneshot::Receiver<()>, tokio::sync::oneshot::Sender<()>) {
        self.services.monitors.as_ref().unwrap().hold_next_commit()
    }

    pub(crate) fn schema_three(&self) -> Vec<u8> {
        let mut document: serde_json::Value =
            serde_json::from_slice(&std::fs::read(self.store.path()).unwrap()).unwrap();
        document["schemaVersion"] = 3.into();
        document["monitors"]["schemaVersion"] = 1.into();
        document["monitors"].as_object_mut().unwrap().remove("revisionCounter");
        document["monitors"]["rules"] = serde_json::json!([
            {"environmentId": self.environment(), "rule": {"id":"scoped-old-id", "consumerGroup":"orders", "enabled":true}, "migrationState":"needs_thresholds"},
            {"environmentId": null, "rule": {"id":"unassigned-old-id", "consumerGroup":"legacy", "enabled":true}, "migrationState":"needs_environment_and_thresholds"}
        ]);
        let bytes = serde_json::to_vec(&document).unwrap();
        std::fs::write(self.store.path(), &bytes).unwrap();
        bytes
    }
}

pub(crate) fn saved(outcome: MonitorMutationOutcome) -> MonitorRuleView {
    match outcome {
        MonitorMutationOutcome::Saved(rule) => rule,
        other => panic!("Expected saved rule: {other:?}"),
    }
}

#[test]
fn monitor_rules_cas_is_per_environment_and_rejects_delete_recreate_and_stale_configuration() {
    let fixture = Fixture::new();
    fixture.runtime.block_on(async {
        let first_environment = fixture.environment();
        let first = saved(fixture.services.mutate_monitor_rule(0, fixture.request("orders", 0, 0, 0)).await.unwrap());
        assert!(first.updated_at_epoch_ms.is_some());
        assert_eq!(first.status, MonitorRuleStatus::Ready);
        assert!(matches!(fixture.services.mutate_monitor_rule(0, fixture.request("orders", 0, 9, 9)).await.unwrap(), MonitorMutationOutcome::Conflict { latest: Some(rule) } if rule == first));
        let (one, two) = tokio::join!(
            fixture.services.mutate_monitor_rule(0, fixture.request("orders", first.revision, 2, 20)),
            fixture.services.mutate_monitor_rule(0, fixture.request("orders", first.revision, 3, 30)),
        );
        let current = match (one.unwrap(), two.unwrap()) {
            (MonitorMutationOutcome::Saved(rule), MonitorMutationOutcome::Conflict { latest: Some(latest) })
            | (MonitorMutationOutcome::Conflict { latest: Some(latest) }, MonitorMutationOutcome::Saved(rule)) => { assert_eq!(rule, latest); rule },
            other => panic!("Exactly one concurrent edit must commit: {other:?}"),
        };
        let delete = MonitorRuleMutation { change: MonitorRuleChange::Delete, ..fixture.request("orders", current.revision, 0, 0) };
        assert_eq!(fixture.services.mutate_monitor_rule(0, delete.clone()).await.unwrap(), MonitorMutationOutcome::Deleted);
        assert_eq!(fixture.services.mutate_monitor_rule(0, delete).await.unwrap(), MonitorMutationOutcome::Conflict { latest: None });
        let recreated = saved(fixture.services.mutate_monitor_rule(0, fixture.request("orders", 0, 4, 40)).await.unwrap());
        assert!(recreated.revision > current.revision);
        assert!(matches!(fixture.services.mutate_monitor_rule(0, fixture.request("orders", current.revision, 99, 99)).await.unwrap(), MonitorMutationOutcome::Conflict { latest: Some(rule) } if rule == recreated));
        let (progress, _) = tokio::sync::mpsc::unbounded_channel();
        fixture.services.mutate_with_progress(ConfigMutation::CreateEnvironment("Staging".into()), progress).await.unwrap();
        assert_ne!(fixture.environment(), first_environment);
        assert!(fixture.services.monitor_rules(fixture.revision()).await.unwrap().rules.is_empty());
        saved(fixture.services.mutate_monitor_rule(fixture.revision(), fixture.request("orders", 0, 7, 70)).await.unwrap());
        let (progress, _) = tokio::sync::mpsc::unbounded_channel();
        fixture.services.mutate_with_progress(ConfigMutation::SwitchEnvironment(first_environment), progress).await.unwrap();
        assert_eq!(fixture.services.monitor_rules(fixture.revision()).await.unwrap().rules, vec![recreated]);
        assert!(fixture.services.mutate_monitor_rule(0, fixture.request("orders", 0, 1, 1)).await.is_err());
    });
    fixture.runtime.shutdown_runtime_blocking().unwrap();
}

#[test]
fn monitor_schema_three_migration_preserves_pending_thresholds_audit_and_explicit_assignment() {
    let fixture = Fixture::new();
    fixture
        .runtime
        .block_on(fixture.services.authenticate("operator", "private-password"))
        .unwrap();
    let events = fixture.events();
    fixture.schema_three();
    fixture.runtime.block_on(async {
        fixture.store.load().await.unwrap();
        let snapshot = fixture.services.monitor_rules(0).await.unwrap();
        assert_eq!(snapshot.rules[0].legacy_id, "scoped-old-id");
        assert_eq!(snapshot.rules[0].status, MonitorRuleStatus::NeedsThresholds);
        assert_eq!(
            snapshot.unassigned[0].status,
            MonitorRuleStatus::NeedsEnvironmentAndThresholds
        );
        for rule in snapshot.rules.iter().chain(&snapshot.unassigned) {
            assert!(rule.enabled);
            assert!(rule.thresholds.is_none());
            assert!(rule.updated_at_epoch_ms.is_none());
            assert!(rule.revision > 0);
        }
        assert_eq!(fixture.events(), events);
        let mut assign = fixture.request("legacy", snapshot.unassigned[0].revision, 1, 100);
        if let MonitorRuleChange::Save { adopt_legacy_id, .. } = &mut assign.change {
            *adopt_legacy_id = Some("unassigned-old-id".into());
        }
        let assigned = saved(fixture.services.mutate_monitor_rule(0, assign).await.unwrap());
        assert_eq!(assigned.environment_id, Some(fixture.environment()));
        assert_eq!(assigned.status, MonitorRuleStatus::Ready);
        assert!(fixture.services.monitor_rules(0).await.unwrap().unassigned.is_empty());
    });
    let document: serde_json::Value = serde_json::from_slice(&std::fs::read(fixture.store.path()).unwrap()).unwrap();
    assert_eq!(document["schemaVersion"], 4);
    assert_eq!(document["monitors"]["schemaVersion"], 2);
    fixture.runtime.shutdown_runtime_blocking().unwrap();
}

#[test]
fn monitor_restart_retains_rule_revision_thresholds_and_write_time() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("config.json");
    let expected;
    {
        let fixture = Fixture::at(path.clone());
        expected = saved(
            fixture
                .runtime
                .block_on(
                    fixture
                        .services
                        .mutate_monitor_rule(0, fixture.request("orders", 0, 2, 300)),
                )
                .unwrap(),
        );
        fixture.runtime.shutdown_runtime_blocking().unwrap();
    }
    let fixture = Fixture::at(path);
    let snapshot = fixture.runtime.block_on(fixture.services.monitor_rules(0)).unwrap();
    assert_eq!(snapshot.rules, vec![expected]);
    assert_eq!(
        fixture
            .events()
            .iter()
            .filter(|event| event.operation.action == AuditAction::MonitorSaved)
            .count(),
        1
    );
    fixture.runtime.shutdown_runtime_blocking().unwrap();
}

#[test]
fn monitor_owned_waiter_drop_keeps_trusted_actor_and_atomic_terminal_without_replay() {
    let fixture = Fixture::new();
    fixture.runtime.block_on(async {
        fixture.services.authenticate("operator", "private-password").await.unwrap();
        let store = fixture.services.monitors.as_ref().unwrap();
        let (started, release) = store.hold_next_commit();
        let mut waiter = Box::pin(fixture.services.mutate_monitor_rule(0, fixture.request("orders", 0, 2, 300)));
        tokio::select! { result = &mut waiter => panic!("Commit finished before release: {result:?}"), result = started => result.unwrap() }
        let pending = fixture.services.audit_pending().unwrap().pop().unwrap();
        drop(waiter);
        fixture.services.sign_out().await.unwrap();
        release.send(()).unwrap();
        assert!(fixture.mutations.task_group().shutdown(Duration::from_secs(5)).await.is_healthy());
        let rule = fixture.store.monitor_rules().await.unwrap().pop().unwrap();
        let event = fixture.events().into_iter().find(|event| event.operation.action == AuditAction::MonitorSaved).unwrap();
        assert_eq!(event.operation, pending);
        assert_eq!(event.operation.context.actor, OperationActor::LocalOperator { username: "operator".into() });
        assert_eq!(event.outcome, AuditOutcome::Succeeded);
        assert_eq!(rule.thresholds.unwrap().max_diff_total, 300);
        assert!(fixture.services.audit_pending().unwrap().is_empty());
        assert!(!serde_json::to_string(&event).unwrap().contains("maxDiffTotal"));
    });
    fixture.runtime.shutdown_runtime_blocking().unwrap();
}

#[test]
fn monitor_future_schema_is_protected_and_invalid_thresholds_never_start_an_operation() {
    let fixture = Fixture::new();
    fixture.runtime.block_on(async {
        let mut invalid = fixture.request("orders", 0, 1, 2);
        if let MonitorRuleChange::Save { thresholds, .. } = &mut invalid.change {
            thresholds.min_count = u32::MAX;
        }
        assert!(fixture.services.mutate_monitor_rule(0, invalid).await.is_err());
        assert!(fixture.events().is_empty());
        let mut document: serde_json::Value =
            serde_json::from_slice(&std::fs::read(fixture.store.path()).unwrap()).unwrap();
        document["schemaVersion"] = (CONFIG_SCHEMA_VERSION + 1).into();
        document["futureData"] = serde_json::json!({"preserve": true});
        let original = serde_json::to_vec(&document).unwrap();
        std::fs::write(fixture.store.path(), &original).unwrap();
        assert!(fixture.store.load().await.is_err());
        assert!(
            fixture
                .services
                .mutate_monitor_rule(0, fixture.request("orders", 0, 1, 2))
                .await
                .is_err()
        );
        assert_eq!(std::fs::read(fixture.store.path()).unwrap(), original);
    });
    fixture.runtime.shutdown_runtime_blocking().unwrap();
}

#[cfg(windows)]
#[test]
fn monitor_interrupted_migration_and_rule_replacement_preserve_state_and_audit_together() {
    use std::os::windows::fs::OpenOptionsExt as _;
    let fixture = Fixture::new();
    let legacy = fixture.schema_three();
    let lock = std::fs::OpenOptions::new()
        .read(true)
        .share_mode(1)
        .open(fixture.store.path())
        .unwrap();
    assert!(fixture.runtime.block_on(fixture.store.load()).is_err());
    assert_eq!(std::fs::read(fixture.store.path()).unwrap(), legacy);
    drop(lock);
    fixture.runtime.block_on(fixture.store.load()).unwrap();
    let before = std::fs::read(fixture.store.path()).unwrap();
    let lock = std::fs::OpenOptions::new()
        .read(true)
        .share_mode(1)
        .open(fixture.store.path())
        .unwrap();
    let request = fixture.request("new-rule", 0, 1, 20);
    let context = fixture
        .store
        .operation_context(
            0,
            OperationActor::LocalDesktop,
            OperationResource::Monitor("new-rule".into()),
        )
        .unwrap();
    assert!(
        fixture
            .runtime
            .block_on(fixture.store.commit_monitor_rule(request, context))
            .is_err()
    );
    assert_eq!(std::fs::read(fixture.store.path()).unwrap(), before);
    drop(lock);
    fixture.runtime.shutdown_runtime_blocking().unwrap();
}
