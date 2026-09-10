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
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;
use std::future::poll_fn;
use std::task::Poll;
use uuid::Uuid;

struct Fixture {
    directory: PathBuf,
    owner: Option<RuntimeOwner>,
    storage: StorageManager,
}

impl Fixture {
    fn new() -> Self {
        let directory = std::env::temp_dir().join(format!("tauri-storage-{}", Uuid::new_v4()));
        let owner = RuntimeOwner::plan(RuntimeConfig::server_default("storage-test"))
            .unwrap()
            .build()
            .unwrap();
        let storage = StorageManager::new(
            directory.join("dashboard.db"),
            owner.root_context().component("storage"),
        );
        Self {
            directory,
            owner: Some(owner),
            storage,
        }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        if let Some(owner) = self.owner.take() {
            let _ = owner.shutdown_runtime_blocking();
        }
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

#[test]
fn data_directory_override_is_explicit_and_does_not_resolve_default() {
    assert_eq!(
        data_path(Some(OsString::from("isolated")), || panic!(
            "default should not be resolved"
        ))
        .unwrap(),
        PathBuf::from("isolated").join("dashboard.db"),
    );
    assert!(data_path(Some(OsString::new()), || panic!("empty override is invalid")).is_err());
    assert_eq!(
        data_path(None, || Ok(PathBuf::from("default"))).unwrap(),
        PathBuf::from("default/data/dashboard.db")
    );
}

#[test]
fn file_initialization_reopens_and_rejects_operations_after_shutdown() {
    let fixture = Fixture::new();
    fixture.owner.as_ref().unwrap().block_on(async {
        fixture.storage.initialize().await.unwrap();
        fixture.storage.initialize().await.unwrap();
        assert!(fixture.storage.path().is_file());
        assert_eq!(fixture.storage.health().completed, 2);
        assert!(fixture.storage.shutdown(Duration::from_secs(5)).await);
        assert!(matches!(
            fixture.storage.initialize().await,
            Err(DashboardError::StorageClosed)
        ));
    });
}

#[test]
fn dropping_waiter_preserves_accepted_transaction_and_shutdown_waits() {
    let fixture = Fixture::new();
    fixture.owner.as_ref().unwrap().block_on(async {
        fixture.storage.initialize().await.unwrap();
        let (entered, entered_rx) = oneshot::channel();
        let (release, release_rx) = std::sync::mpsc::channel();
        let mut operation = Box::pin(fixture.storage.run("accepted-write", move |connection| {
            let _ = entered.send(());
            release_rx.recv().unwrap();
            connection.execute("INSERT INTO users (username, password_hash, created_at, updated_at) VALUES ('accepted', 'hash', 'now', 'now')", [])?;
            Ok(())
        }));
        tokio::select! {
            result = &mut operation => panic!("operation completed before release: {result:?}"),
            _ = entered_rx => {}
        }
        drop(operation);
        assert_eq!(fixture.storage.health().active, 1);
        let mut shutdown = Box::pin(fixture.storage.shutdown(Duration::from_secs(5)));
        poll_fn(|context| {
            assert!(shutdown.as_mut().poll(context).is_pending());
            Poll::Ready(())
        }).await;
        release.send(()).unwrap();
        assert!(shutdown.await);
        assert_eq!(fixture.storage.health().active, 0);
    });
    let connection = open_connection(fixture.storage.path()).unwrap();
    let username: String = connection
        .query_row("SELECT username FROM users", [], |row| row.get(0))
        .unwrap();
    assert_eq!(username, "accepted");
}

#[test]
fn shutdown_waits_for_owned_background_cleanup() {
    let fixture = Fixture::new();
    fixture.owner.as_ref().unwrap().block_on(async {
        let cancellation = fixture.storage.background.task_group().cancellation_token();
        let (cancelled, cancelled_rx) = oneshot::channel();
        let (release, release_rx) = oneshot::channel();
        fixture
            .storage
            .background
            .spawn_service("cleanup", async move {
                cancellation.cancelled().await;
                let _ = cancelled.send(());
                let _ = release_rx.await;
            })
            .unwrap();
        let mut shutdown = Box::pin(fixture.storage.shutdown(Duration::from_secs(5)));
        tokio::select! {
            _ = &mut shutdown => panic!("shutdown did not await background cleanup"),
            _ = cancelled_rx => {}
        }
        poll_fn(|context| {
            assert!(shutdown.as_mut().poll(context).is_pending());
            Poll::Ready(())
        })
        .await;
        release.send(()).unwrap();
        assert!(shutdown.await);
    });
}
