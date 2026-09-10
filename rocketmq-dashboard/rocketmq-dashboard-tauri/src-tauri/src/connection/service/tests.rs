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
use crate::audit::types::{AuditAction, AuditContext};
use rocketmq_runtime::{RuntimeConfig, RuntimeOwner};
use std::time::Duration;
use uuid::Uuid;

struct Fixture {
    owner: Option<RuntimeOwner>,
    storage: StorageManager,
    manager: ConnectionManager,
    directory: std::path::PathBuf,
}
impl Fixture {
    fn new() -> Self {
        let owner = RuntimeOwner::plan(RuntimeConfig::server_default("connection-test"))
            .unwrap()
            .build()
            .unwrap();
        let directory = std::env::temp_dir().join(format!("tauri-connection-{}", Uuid::new_v4()));
        let storage = StorageManager::new(
            directory.join("dashboard.db"),
            owner.root_context().component("storage"),
        );
        let manager = owner.block_on(async {
            storage.initialize().await.unwrap();
            let path = storage.path().to_path_buf();
            let snapshot = storage
                .run("bootstrap", move |connection| {
                    crate::nameserver::NameServerDb::from_path(path).init()?;
                    Ok(crate::nameserver::db::load_snapshot_from_connection(connection)?)
                })
                .await
                .unwrap();
            let runtime = Arc::new(NameServerRuntimeState::new(
                snapshot,
                crate::nameserver::runtime::test_client_runtime(),
            ));
            ConnectionManager::initialize(storage.clone(), runtime).await.unwrap()
        });
        Self {
            owner: Some(owner),
            storage,
            manager,
            directory,
        }
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        if let Some(owner) = self.owner.take() {
            assert!(owner.block_on(self.storage.shutdown(Duration::from_secs(5))));
            let _ = owner.shutdown_runtime_blocking();
        }
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}
fn audit() -> AuditContext {
    AuditContext {
        actor: Some("admin".into()),
        environment: Arc::new(Mutex::new(None)),
        event_id: Uuid::new_v4().to_string(),
        request_id: Uuid::new_v4().to_string(),
        action: AuditAction::ReplaceNameServers,
        resource_name: None,
    }
}

#[test]
fn concurrent_saves_with_the_same_revision_have_one_winner() {
    let fixture = Fixture::new();
    fixture.owner.as_ref().unwrap().block_on(async {
        let (first, second) = tokio::join!(
            fixture.manager.change(0, ConnectionChange::Vip(false), audit()),
            fixture.manager.change(0, ConnectionChange::Tls(true), audit())
        );
        assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
        let loser = first.err().or_else(|| second.err()).unwrap();
        assert!(matches!(loser, DashboardError::ConfigurationConflict));
        assert_eq!(fixture.manager.snapshot().unwrap().revision, 1);
        let count = fixture
            .storage
            .run("audit-count", |connection| {
                Ok(connection.query_row::<i64, _, _>("SELECT COUNT(*) FROM audit_events", [], |row| row.get(0))?)
            })
            .await
            .unwrap();
        assert_eq!(count, 1);
    });
}

#[test]
fn invalid_replacement_rolls_back_and_valid_new_selection_is_atomic() {
    let fixture = Fixture::new();
    fixture.owner.as_ref().unwrap().block_on(async {
        let before = fixture.manager.snapshot().unwrap();
        let invalid = ConnectionChange::Replace {
            addresses: vec!["127.0.0.1:9876".into(), "invalid".into()],
            current_endpoint: before
                .current_nameserver_id
                .clone()
                .map(NameServerSelection::ExistingId),
        };
        assert!(fixture.manager.change(0, invalid, audit()).await.is_err());
        assert_eq!(fixture.manager.snapshot().unwrap(), before);
        let result = fixture
            .manager
            .change(
                0,
                ConnectionChange::Replace {
                    addresses: vec![" 127.0.0.2:9876 ".into(), "".into(), "127.0.0.2:9876".into()],
                    current_endpoint: Some(NameServerSelection::Address("127.0.0.2:9876".into())),
                },
                audit(),
            )
            .await
            .unwrap();
        assert_eq!(result.settings.nameserver.namesrv_addr_list, ["127.0.0.2:9876"]);
        assert_ne!(result.settings.environment_id, before.environment_id);
        assert_eq!(fixture.manager.runtime.snapshot(), result.settings.nameserver);
    });
}

#[test]
fn endpoint_and_environment_ids_survive_settings_changes_and_readdition() {
    let fixture = Fixture::new();
    fixture.owner.as_ref().unwrap().block_on(async {
        let initial = fixture.manager.snapshot().unwrap();
        let settings = fixture
            .manager
            .change(0, ConnectionChange::Vip(false), audit())
            .await
            .unwrap()
            .settings;
        assert_eq!(settings.environment_id, initial.environment_id);
        assert_eq!(settings.current_nameserver_id, initial.current_nameserver_id);
        fixture
            .manager
            .change(
                1,
                ConnectionChange::Delete {
                    kind: EndpointKind::NameServer,
                    address: "127.0.0.1:9876".into(),
                },
                audit(),
            )
            .await
            .unwrap();
        let path = fixture.storage.path().to_path_buf();
        fixture
            .storage
            .run("reopen", move |_| {
                crate::nameserver::NameServerDb::from_path(path).init()
            })
            .await
            .unwrap();
        let stored = fixture
            .storage
            .run("read", |connection| db::load(connection))
            .await
            .unwrap();
        assert!(stored.nameserver.namesrv_addr_list.is_empty());
        let restored = fixture
            .manager
            .change(
                2,
                ConnectionChange::Add {
                    kind: EndpointKind::NameServer,
                    address: "127.0.0.1:9876".into(),
                },
                audit(),
            )
            .await
            .unwrap()
            .settings;
        assert_eq!(restored.current_nameserver_id, initial.current_nameserver_id);
        assert_eq!(restored.environment_id, initial.environment_id);
        let proxy = fixture
            .manager
            .change(
                3,
                ConnectionChange::Add {
                    kind: EndpointKind::Proxy,
                    address: "127.0.0.1:8080".into(),
                },
                audit(),
            )
            .await
            .unwrap()
            .settings;
        assert_eq!(proxy.environment_id, initial.environment_id);
        assert!(proxy.current_proxy_id.is_some());
    });
}

#[test]
fn configuration_switch_waits_for_accepted_remote_mutations_and_rejects_stale_ones() {
    let fixture = Fixture::new();
    fixture.owner.as_ref().unwrap().block_on(async {
        let lease = fixture.manager.mutation_lease(0, &audit()).await.unwrap();
        let mut switch = Box::pin(fixture.manager.change(0, ConnectionChange::Vip(false), audit()));
        std::future::poll_fn(|context| {
            assert!(switch.as_mut().poll(context).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        drop(lease);
        switch.await.unwrap();
        assert!(matches!(
            fixture.manager.mutation_lease(0, &audit()).await,
            Err(DashboardError::ConfigurationConflict)
        ));
    });
}

#[test]
fn audit_failure_prevents_config_commit_and_runtime_publication() {
    let fixture = Fixture::new();
    fixture.owner.as_ref().unwrap().block_on(async {
        let before = fixture.manager.snapshot().unwrap();
        fixture
            .storage
            .run("break-audit", |connection| {
                connection.execute("DROP TABLE audit_events", [])?;
                Ok(())
            })
            .await
            .unwrap();
        assert!(
            fixture
                .manager
                .change(0, ConnectionChange::Vip(false), audit())
                .await
                .is_err()
        );
        assert_eq!(fixture.manager.snapshot().unwrap(), before);
        assert_eq!(
            fixture
                .storage
                .run("read", |connection| db::load(connection))
                .await
                .unwrap(),
            before
        );
        assert_eq!(fixture.manager.runtime.generation(), 0);
    });
}
