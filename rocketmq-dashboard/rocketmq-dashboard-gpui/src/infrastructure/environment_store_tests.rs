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

use std::sync::Arc;

use rocketmq_dashboard_common::{OperationActor, OperationContext, OperationResource};
use rocketmq_runtime::{ProcessMemoryLimit, RuntimeConfig, RuntimeOwner};

use super::*;
use crate::infrastructure::{config_store::DesktopConfigStore, monitor_store::MonitorStore};

fn runtime() -> RuntimeOwner {
    RuntimeOwner::plan(RuntimeConfig::for_parallelism("environment-test", 1))
        .unwrap()
        .with_memory_limit(ProcessMemoryLimit::configured(256 * 1024 * 1024).unwrap())
        .build()
        .unwrap()
}

fn operation(store: &DesktopConfigStore) -> OperationContext {
    let snapshot = store.environment_snapshot().unwrap();
    store
        .operation_context(
            snapshot.connection.revision,
            OperationActor::LocalDesktop,
            OperationResource::Configuration,
        )
        .unwrap()
}

#[test]
fn legacy_config_and_rules_migrate_without_attributing_the_rules() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("config.json");
    let mut legacy = DesktopConfig::default();
    legacy.schema_version = 1;
    legacy.revision = 12;
    legacy.nameservers = vec!["one:9876".into()];
    legacy.current_nameserver = Some("one:9876".into());
    std::fs::write(&path, serde_json::to_vec(&legacy).unwrap()).unwrap();
    let monitor_source = br#"[{"id":"old-id","consumerGroup":"orders","enabled":true}]"#;
    std::fs::write(directory.path().join("monitors.json"), monitor_source).unwrap();
    let runtime = runtime();
    let store = DesktopConfigStore::new(path.clone(), runtime.root_context().component("configuration"));
    let monitors = MonitorStore::with_config_store(Arc::clone(&store));
    runtime.block_on(async {
        let config = store.load().await.unwrap();
        assert_eq!(config.revision, 12);
        assert_eq!(config.nameservers, legacy.nameservers);
        assert!(monitors.list().await.unwrap().is_empty());
        let pending = monitors.unassigned().await.unwrap();
        assert_eq!(pending[0].rule.id, "old-id");
        assert!(pending[0].rule.enabled);
        assert_eq!(
            pending[0].migration_state,
            MonitorMigrationState::NeedsEnvironmentAndThresholds
        );
        assert_eq!(store.load().await.unwrap(), config);
    });
    assert_eq!(
        std::fs::read(directory.path().join("monitors.json")).unwrap(),
        monitor_source
    );
    let document: DesktopDocument = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    assert_eq!(document.schema_version, CONFIG_SCHEMA_VERSION);
    assert_eq!(document.monitors.schema_version, 1);
    assert!(document.monitors.rules[0].environment_id.is_none());
    runtime.shutdown_runtime_blocking().unwrap();
}

#[test]
fn transport_and_endpoint_edits_preserve_identity_and_explicit_profiles_restore_settings() {
    let directory = tempfile::tempdir().unwrap();
    let runtime = runtime();
    let store = DesktopConfigStore::new(
        directory.path().join("config.json"),
        runtime.root_context().component("configuration"),
    );
    runtime.block_on(async {
        let mut config = store.load().await.unwrap();
        let first = store.environments().unwrap().active_id;
        config.transport.use_tls = true;
        config.nameservers = vec!["first:9876".into()];
        config.current_nameserver = Some("first:9876".into());
        store.save_next(config).await.unwrap();
        assert_eq!(store.environments().unwrap().active_id, first);
        let second = store
            .change_environment(EnvironmentChange::Create { name: "Staging".into() }, operation(&store))
            .await
            .unwrap();
        assert!(second.nameservers.is_empty());
        assert_ne!(store.environments().unwrap().active_id, first);
        let restored = store
            .change_environment(EnvironmentChange::Select { id: first.clone() }, operation(&store))
            .await
            .unwrap();
        assert!(restored.transport.use_tls);
        assert_eq!(restored.current_nameserver.as_deref(), Some("first:9876"));
        let id = store.environment_snapshot().unwrap().environment_id;
        store.load().await.unwrap();
        assert_eq!(store.environment_snapshot().unwrap().environment_id, id);
    });
    runtime.shutdown_runtime_blocking().unwrap();
}

#[test]
fn configuration_and_monitor_records_commit_once_with_their_state() {
    let directory = tempfile::tempdir().unwrap();
    let runtime = runtime();
    let path = directory.path().join("config.json");
    let store = DesktopConfigStore::new(path.clone(), runtime.root_context().component("configuration"));
    runtime.block_on(async {
        let mut config = store.load().await.unwrap();
        config.transport.use_vip_channel = true;
        let context = operation(&store);
        let saved = store
            .save_next_with_context(config.clone(), context.clone())
            .await
            .unwrap();
        assert_eq!(store.save_next_with_context(config, context).await.unwrap(), saved);
        let monitors = MonitorStore::with_config_store(Arc::clone(&store));
        monitors
            .upsert(MonitorRule {
                id: "orders".into(),
                consumer_group: "orders".into(),
                enabled: true,
            })
            .await
            .unwrap();
        let document: DesktopDocument = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert!(document.config().unwrap().transport.use_vip_channel);
        assert_eq!(document.monitors.rules.len(), 1);
        assert_eq!(document.local_operations.len(), 2);
        assert_eq!(document.local_operations[1].kind, LocalOperationKind::MonitorSaved);
        assert_eq!(
            document.monitors.rules[0].environment_id.as_ref(),
            Some(&document.active_environment_id)
        );
        assert_eq!(
            document.monitors.rules[0].migration_state,
            MonitorMigrationState::NeedsThresholds
        );
    });
    runtime.shutdown_runtime_blocking().unwrap();
}

#[test]
fn identical_monitor_ids_in_two_profiles_do_not_replace_or_delete_each_other() {
    let directory = tempfile::tempdir().unwrap();
    let runtime = runtime();
    let store = DesktopConfigStore::new(
        directory.path().join("config.json"),
        runtime.root_context().component("configuration"),
    );
    let monitors = MonitorStore::with_config_store(Arc::clone(&store));
    runtime.block_on(async {
        store.load().await.unwrap();
        let first = store.environments().unwrap().active_id;
        monitors
            .upsert(MonitorRule {
                id: "orders".into(),
                consumer_group: "orders".into(),
                enabled: true,
            })
            .await
            .unwrap();
        store
            .change_environment(EnvironmentChange::Create { name: "Staging".into() }, operation(&store))
            .await
            .unwrap();
        assert!(monitors.list().await.unwrap().is_empty());
        monitors
            .upsert(MonitorRule {
                id: "orders".into(),
                consumer_group: "orders".into(),
                enabled: false,
            })
            .await
            .unwrap();
        assert!(!monitors.list().await.unwrap()[0].enabled);
        assert!(monitors.delete("orders".into()).await.unwrap());
        store
            .change_environment(EnvironmentChange::Select { id: first }, operation(&store))
            .await
            .unwrap();
        assert!(monitors.list().await.unwrap()[0].enabled);
    });
    runtime.shutdown_runtime_blocking().unwrap();
}

#[test]
fn invalid_monitor_source_cannot_partially_replace_the_legacy_configuration() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("config.json");
    let source = br#"{"schemaVersion":1,"revision":9}"#;
    std::fs::write(&path, source).unwrap();
    std::fs::write(directory.path().join("monitors.json"), b"[broken").unwrap();
    let runtime = runtime();
    let store = DesktopConfigStore::new(path.clone(), runtime.root_context().component("configuration"));
    assert!(runtime.block_on(store.load()).is_err());
    assert_eq!(std::fs::read(path).unwrap(), source);
    runtime.shutdown_runtime_blocking().unwrap();
}

#[cfg(windows)]
#[test]
fn blocked_migration_replacement_preserves_source_and_retry_recovers() {
    use std::os::windows::fs::OpenOptionsExt as _;

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("config.json");
    let source = br#"{"schemaVersion":1,"revision":9}"#;
    std::fs::write(&path, source).unwrap();
    let open_original = std::fs::OpenOptions::new()
        .read(true)
        .share_mode(1)
        .open(&path)
        .unwrap();
    let runtime = runtime();
    let store = DesktopConfigStore::new(path.clone(), runtime.root_context().component("configuration"));
    assert!(runtime.block_on(store.load()).is_err());
    assert_eq!(std::fs::read(&path).unwrap(), source);
    drop(open_original);
    assert_eq!(runtime.block_on(store.load()).unwrap().revision, 9);
    runtime.shutdown_runtime_blocking().unwrap();
}

#[cfg(windows)]
#[test]
fn failed_local_commit_does_not_separate_configuration_from_its_record() {
    use std::os::windows::fs::OpenOptionsExt as _;

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("config.json");
    let runtime = runtime();
    let store = DesktopConfigStore::new(path.clone(), runtime.root_context().component("configuration"));
    runtime.block_on(async {
        let mut config = store.load().await.unwrap();
        let before = std::fs::read(&path).unwrap();
        let context = operation(&store);
        config.transport.use_tls = true;
        let original = std::fs::OpenOptions::new()
            .read(true)
            .share_mode(1)
            .open(&path)
            .unwrap();
        assert!(
            store
                .save_next_with_context(config.clone(), context.clone())
                .await
                .is_err()
        );
        assert_eq!(std::fs::read(&path).unwrap(), before);
        drop(original);
        store.save_next_with_context(config, context.clone()).await.unwrap();
        let document: DesktopDocument = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert!(document.config().unwrap().transport.use_tls);
        assert_eq!(document.local_operations.len(), 1);
        assert_eq!(document.local_operations[0].context.request_id, context.request_id);
    });
    runtime.shutdown_runtime_blocking().unwrap();
}
