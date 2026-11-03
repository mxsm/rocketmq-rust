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

use crate::{
    contracts_tests::{exercise_contract, runtime},
    DashboardStore, DesktopConfig, ImportSnapshot, StorageOptions,
};

use super::*;

async fn execute(store: &SqlStore, statement: &str) {
    with_pool!(store, pool, {
        sqlx::query(statement).execute(pool).await.unwrap();
    });
}

fn exercise_sql(engine: StorageEngine, url: String) {
    let runtime = runtime();
    runtime.block_on(async {
        let context = runtime.root_context().component("sql-contract");
        // The opt-in server fixture variables must target a disposable dedicated database.
        // Only this native crate's tables are reset; Web and RocketMQ data are never touched.
        let helper = SqlStore::connect(engine, DatabaseConnection::new(url.clone()), context.clone()).await.unwrap();
        execute(&helper, "DELETE FROM native_dashboard_history_lease").await;
        execute(&helper, "DELETE FROM native_dashboard_document").await;
        let options = || StorageOptions::Database { engine, connection: DatabaseConnection::new(url.clone()) };
        let store = DashboardStore::open(options(), context.clone()).await.unwrap();
        assert!(store.load_desktop().await.unwrap().is_none());
        assert!(!store.status().await.unwrap().initialized);
        assert_eq!(store.history().await.unwrap_err().kind(), StorageErrorKind::Uninitialized);
        let document = DesktopDocument::from_legacy(DesktopConfig::default(), Vec::new()).unwrap();
        store.initialize(document).await.unwrap();
        let peer = DashboardStore::open(options(), context.clone()).await.unwrap();
        exercise_contract(&store, &peer).await;
        let committed = store.load_desktop().await.unwrap().unwrap();
        assert_eq!(peer.load_desktop().await.unwrap().unwrap(), committed);
        let status = store.status().await.unwrap();
        assert_eq!(status.engine, engine);
        assert!(status.pool_connections.is_some());
        store.close().await.unwrap();
        peer.close().await.unwrap();
        assert_eq!(store.status().await.unwrap_err().kind(), StorageErrorKind::Closed);
        let reopened = DashboardStore::open(options(), context.clone()).await.unwrap();
        assert_eq!(reopened.load_desktop().await.unwrap().unwrap(), committed);
        assert_eq!(reopened.sessions(SessionQuery::default()).await.unwrap().sessions.len(), 4);

        let source = tempfile::tempdir().unwrap();
        let path = source.path().join("config.json");
        let legacy = DesktopConfig {
            schema_version: 1,
            revision: 42,
            ..Default::default()
        };
        let original = serde_json::to_vec(&legacy).unwrap();
        std::fs::write(&path, &original).unwrap();
        let history_bytes = serde_json::to_vec(&serde_json::json!({ "schemaVersion": 1, "points": [{ "metric": "topic_messages", "seriesIdentity": "legacy-orders", "timestampEpochMs": 100, "value": 3.5, "sourceRevision": 42 }] })).unwrap();
        std::fs::write(path.with_file_name("history.json"), &history_bytes).unwrap();
        let candidate = ImportSnapshot::read_files(path.clone(), context.clone()).await.unwrap();
        execute(&helper, "DELETE FROM native_dashboard_document").await;
        // Fail on the second insert after the canonical document insert has succeeded.
        execute(&helper, "INSERT INTO native_dashboard_document (document_name, storage_revision, payload, written_at_ms) VALUES ('history', 0, '{}', 1)").await;
        assert_eq!(reopened.import(candidate.clone()).await.unwrap_err().kind(), StorageErrorKind::AlreadyInitialized);
        assert!(reopened.load_desktop().await.unwrap().is_none(), "a failed multi-domain import must roll back the first insert");
        assert!(helper.read_payload("sessions").await.unwrap().is_none());
        assert_eq!(std::fs::read(&path).unwrap(), original);
        execute(&helper, "DELETE FROM native_dashboard_document").await;
        let imported = reopened.import(candidate.clone()).await.unwrap();
        assert_eq!(imported.document, candidate.document);
        assert_eq!(reopened.history().await.unwrap(), candidate.history);
        assert!(reopened.history().await.unwrap().points[0].environment_id.is_none());
        assert_eq!(reopened.import(candidate).await.unwrap_err().kind(), StorageErrorKind::AlreadyInitialized);
        assert_eq!(std::fs::read(&path).unwrap(), original);
        assert_eq!(std::fs::read(path.with_file_name("history.json")).unwrap(), history_bytes);
        reopened.close().await.unwrap();

        // Restart after interrupted schema setup recreates the absent auxiliary table and
        // keeps committed native data. A future schema is rejected without replacing data.
        execute(&helper, "DROP TABLE native_dashboard_history_lease").await;
        let resumed = DashboardStore::open(options(), context.clone()).await.unwrap();
        assert_eq!(resumed.load_desktop().await.unwrap().unwrap(), imported);
        execute(&helper, "UPDATE native_dashboard_schema SET schema_version = 999 WHERE singleton = 1").await;
        let error = match DashboardStore::open(options(), context.clone()).await { Ok(_) => panic!("future schema must fail"), Err(error) => error };
        assert_eq!(error.kind(), StorageErrorKind::UnsupportedSchema { found: 999, supported: 1 });
        assert_eq!(resumed.status().await.unwrap_err().kind(), error.kind());
        execute(&helper, "UPDATE native_dashboard_schema SET schema_version = 1 WHERE singleton = 1").await;
        assert_eq!(resumed.load_desktop().await.unwrap().unwrap(), imported);
        resumed.close().await.unwrap();
        helper.close().await.unwrap();
    });
}

#[cfg(feature = "sqlite")]
#[test]
fn sqlite_native_contract() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("native.sqlite3");
    let url = format!("sqlite://{}", path.to_string_lossy().replace('\\', "/"));
    exercise_sql(StorageEngine::Sqlite, url);
}

#[cfg(feature = "mysql")]
#[test]
#[ignore = "requires an isolated disposable database in ROCKETMQ_DASHBOARD_NATIVE_MYSQL_TEST_URL"]
fn mysql_native_contract() {
    let url = std::env::var("ROCKETMQ_DASHBOARD_NATIVE_MYSQL_TEST_URL")
        .expect("isolated native MySQL fixture URL is required");
    exercise_sql(StorageEngine::MySql, url);
}

#[cfg(feature = "postgres")]
#[test]
#[ignore = "requires an isolated disposable database in ROCKETMQ_DASHBOARD_NATIVE_POSTGRES_TEST_URL"]
fn postgres_native_contract() {
    let url = std::env::var("ROCKETMQ_DASHBOARD_NATIVE_POSTGRES_TEST_URL")
        .expect("isolated native PostgreSQL fixture URL is required");
    exercise_sql(StorageEngine::Postgres, url);
}
