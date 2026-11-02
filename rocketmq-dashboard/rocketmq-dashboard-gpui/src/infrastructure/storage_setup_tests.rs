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
use rocketmq_runtime::{ProcessMemoryLimit, RuntimeConfig, RuntimeOwner};

fn runtime() -> RuntimeOwner {
    RuntimeOwner::plan(RuntimeConfig::for_parallelism("storage-setup-test", 2))
        .unwrap()
        .with_memory_limit(ProcessMemoryLimit::configured(256 * 1024 * 1024).unwrap())
        .build()
        .unwrap()
}

fn selected_store(
    path: PathBuf,
    context: ChildServiceContext,
    override_engine: Option<StorageEngine>,
) -> Arc<DesktopConfigStore> {
    let mut store = DesktopConfigStore::fields(path, context, false, None);
    store.from_environment = true;
    store.engine_override = Ok(override_engine);
    Arc::new(store)
}

#[test]
fn saved_engine_and_invalid_selection_stay_frozen_until_restart_without_file_fallback() {
    let runtime = runtime();
    let directory = tempfile::tempdir().unwrap();
    runtime.block_on(async {
        let path = directory.path().join("config.json");
        let context = runtime.root_context().component("selection");
        let invalid = br#"{"engine":"postgres","databaseUrl":"PRIVATE-URL"}"#;
        let store = selected_store(path.clone(), context.clone(), None);
        std::fs::write(store.selection_path(), invalid).unwrap();
        let before = store.storage_setup_snapshot().await;
        assert!(before.current_engine.is_err());
        assert!(before.status.is_err());
        assert_eq!(std::fs::read(store.selection_path()).unwrap(), invalid);
        assert!(!path.exists());
        store.save_storage_engine(StorageEngine::File).await.unwrap();
        assert!(
            store.storage_setup_snapshot().await.current_engine.is_err(),
            "save cannot change the failed opening target"
        );
        assert!(!path.exists());
        let reopened = selected_store(path.clone(), context.clone(), None);
        assert_eq!(
            reopened.storage_setup_snapshot().await.current_engine.unwrap(),
            StorageEngine::File
        );
        reopened.close().await.unwrap();
        StorageSelection {
            engine: StorageEngine::Postgres,
        }
        .save(store.selection_path(), context.clone())
        .await
        .unwrap();
        let sql = selected_store(path.clone(), context.clone(), None);
        assert_eq!(
            sql.frozen_selection().await.engine.as_ref().unwrap(),
            &StorageEngine::Postgres
        );
        #[cfg(not(feature = "storage-postgres"))]
        assert!(
            sql.native_storage().await.is_err(),
            "an uncompiled SQL opening cannot become file"
        );
        sql.save_storage_engine(StorageEngine::File).await.unwrap();
        assert_eq!(
            sql.frozen_selection().await.engine.as_ref().unwrap(),
            &StorageEngine::Postgres
        );
        assert!(sql.native.get().is_none());
    });
}

#[test]
fn external_engine_has_priority_and_cannot_be_replaced_by_a_local_preference() {
    let runtime = runtime();
    let directory = tempfile::tempdir().unwrap();
    runtime.block_on(async {
        let path = directory.path().join("config.json");
        let context = runtime.root_context().component("selection");
        let store = selected_store(path, context.clone(), Some(StorageEngine::File));
        StorageSelection {
            engine: StorageEngine::Postgres,
        }
        .save(store.selection_path(), context.clone())
        .await
        .unwrap();
        let status = store.storage_setup_snapshot().await;
        assert_eq!(status.current_engine.unwrap(), StorageEngine::File);
        assert_eq!(status.source, SelectionSource::Environment);
        assert!(store.save_storage_engine(StorageEngine::File).await.is_err());
        assert_eq!(
            StorageSelection::load(store.selection_path(), context)
                .await
                .unwrap()
                .unwrap()
                .engine,
            StorageEngine::Postgres
        );
        assert!(
            store
                .complete_workspace_setup(WorkspaceSetupAction::Initialize)
                .await
                .is_err()
        );
        store.close().await.unwrap();
    });
}

#[cfg(feature = "storage-sqlite")]
#[test]
fn explicit_import_uses_existing_authority_preserves_sources_and_never_overwrites_workspace() {
    use crate::{
        infrastructure::auth_state::{DesktopAuthState, LOGIN_PASSWORD_ENV, LOGIN_USERNAME_ENV, MapEnvironment},
        services::AppServices,
    };
    let runtime = runtime();
    let directory = tempfile::tempdir().unwrap();
    runtime.block_on(async {
        let context = runtime.root_context().component("workspace");
        let source = directory.path().join("config.json");
        let bytes = serde_json::to_vec(&DesktopConfig {
            schema_version: 1,
            revision: 42,
            auth: rocketmq_dashboard_storage::AuthConfig {
                enabled: true,
                ..Default::default()
            },
            ..Default::default()
        })
        .unwrap();
        std::fs::write(&source, &bytes).unwrap();
        let native = Arc::new(
            DashboardStore::open(
                StorageOptions::Database {
                    engine: StorageEngine::Sqlite,
                    connection: DatabaseConnection::new("sqlite::memory:".into()),
                },
                context.clone(),
            )
            .await
            .unwrap(),
        );
        let store = DesktopConfigStore::with_native_storage(source.clone(), context, native.clone());
        assert!(!store.storage_setup_snapshot().await.status.unwrap().initialized);
        assert!(store.environments().is_none());
        store
            .complete_workspace_setup(WorkspaceSetupAction::ImportLegacy {
                config_path: source.clone(),
            })
            .await
            .unwrap();
        assert!(Arc::ptr_eq(&native, &store.native_storage().await.unwrap()));
        assert_eq!(store.load().await.unwrap().revision, 42);
        assert!(store.load().await.unwrap().auth.enabled);
        assert_eq!(std::fs::read(&source).unwrap(), bytes);
        let original = native.load_desktop().await.unwrap().unwrap();
        assert!(
            store
                .complete_workspace_setup(WorkspaceSetupAction::Initialize)
                .await
                .is_err()
        );
        assert_eq!(native.load_desktop().await.unwrap().unwrap(), original);
        let services = AppServices::desktop_with_fake_provider(
            store.clone(),
            DesktopAuthState::new(Arc::new(MapEnvironment::new([
                (LOGIN_USERNAME_ENV, "workspace-operator"),
                (LOGIN_PASSWORD_ENV, "setup-test-password"),
            ]))),
            runtime.root_context().component("services"),
            runtime.root_context().component("history"),
            runtime.root_context().component("monitor"),
        );
        let boot = services.bootstrap().await.unwrap();
        assert!(
            boot.login_required && !boot.has_valid_session,
            "imported authentication is applied by normal bootstrap"
        );
        store.close().await.unwrap();
    });
}

#[cfg(any(feature = "storage-mysql", feature = "storage-postgres"))]
fn existing_sql_workspace_rejects_setup_and_preserves_current_authority(engine: StorageEngine, url: String) {
    let runtime = runtime();
    let directory = tempfile::tempdir().unwrap();
    runtime.block_on(async {
        let context = runtime.root_context().component("workspace-server-test");
        let native = Arc::new(
            DashboardStore::open(
                StorageOptions::Database {
                    engine,
                    connection: DatabaseConnection::new(url),
                },
                context.clone(),
            )
            .await
            .unwrap(),
        );
        let original = native
            .load_desktop()
            .await
            .unwrap()
            .expect("dedicated fixture must retain its existing native workspace");
        let source = directory.path().join("config.json");
        let bytes = b"unread legacy source remains unchanged";
        std::fs::write(&source, bytes).unwrap();
        let store = DesktopConfigStore::with_native_storage(source.clone(), context.clone(), native.clone());
        let observed = store.storage_setup_snapshot().await;
        assert_eq!(observed.current_engine.unwrap(), engine);
        let status = observed.status.unwrap();
        assert_eq!(status.engine, engine);
        assert!(status.initialized && status.pool_connections.unwrap() > 0);
        assert_eq!(status.document_schema_version, Some(CONFIG_SCHEMA_VERSION));
        assert!(status.last_successful_write_epoch_ms.is_some());
        assert!(
            store
                .complete_workspace_setup(WorkspaceSetupAction::Initialize)
                .await
                .is_err()
        );
        assert!(
            store
                .complete_workspace_setup(WorkspaceSetupAction::ImportLegacy {
                    config_path: source.clone()
                })
                .await
                .is_err()
        );
        assert_eq!(native.load_desktop().await.unwrap().unwrap(), original);
        assert_eq!(std::fs::read(&source).unwrap(), bytes);
        store.save_storage_engine(StorageEngine::File).await.unwrap();
        assert!(Arc::ptr_eq(&native, &store.native_storage().await.unwrap()));
        assert_eq!(store.storage_setup_snapshot().await.current_engine.unwrap(), engine);
        assert_eq!(
            StorageSelection::load(store.selection_path(), context)
                .await
                .unwrap()
                .unwrap()
                .engine,
            StorageEngine::File
        );
        assert_eq!(native.load_desktop().await.unwrap().unwrap(), original);
        store.close().await.unwrap();
    });
    runtime.shutdown_runtime_blocking().unwrap();
}

#[cfg(feature = "storage-mysql")]
#[test]
#[ignore = "requires the dedicated native MySQL fixture"]
fn native_workspace_setup_mysql_preserves_existing_target_and_next_launch_choice() {
    existing_sql_workspace_rejects_setup_and_preserves_current_authority(
        StorageEngine::MySql,
        std::env::var("ROCKETMQ_DASHBOARD_NATIVE_MYSQL_TEST_URL").expect("dedicated native MySQL fixture required"),
    );
}

#[cfg(feature = "storage-postgres")]
#[test]
#[ignore = "requires the dedicated native PostgreSQL fixture"]
fn native_workspace_setup_postgres_preserves_existing_target_and_next_launch_choice() {
    existing_sql_workspace_rejects_setup_and_preserves_current_authority(
        StorageEngine::Postgres,
        std::env::var("ROCKETMQ_DASHBOARD_NATIVE_POSTGRES_TEST_URL")
            .expect("dedicated native PostgreSQL fixture required"),
    );
}
