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

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use rocketmq_error::RocketMQError;
use rocketmq_store_api::DerivedRecordId;
use rocketmq_tieredstore::DefaultTieredDispatcher;
use rocketmq_tieredstore::FileSegmentType;
use rocketmq_tieredstore::PosixProvider;
use rocketmq_tieredstore::TieredDispatchReadiness;
use rocketmq_tieredstore::TieredDispatchRequest;
use rocketmq_tieredstore::TieredDispatcher;
use rocketmq_tieredstore::TieredFileSegment;
use rocketmq_tieredstore::TieredLifecycle;
use rocketmq_tieredstore::TieredMessageFetcher;
use rocketmq_tieredstore::TieredStorageLevel;
use rocketmq_tieredstore::TieredStore;
use rocketmq_tieredstore::TieredStoreConfig;
use rocketmq_tieredstore::TieredStoreProvider;

#[derive(Clone, Default)]
struct FailureControl {
    fail_writes: Arc<AtomicUsize>,
    partial_writes: Arc<AtomicUsize>,
}

impl FailureControl {
    fn fail_next(&self, count: usize) {
        self.fail_writes.store(count, Ordering::Release);
    }

    fn partial_next(&self, count: usize) {
        self.partial_writes.store(count, Ordering::Release);
    }

    fn clear(&self) {
        self.fail_writes.store(0, Ordering::Release);
        self.partial_writes.store(0, Ordering::Release);
    }

    fn take(counter: &AtomicUsize) -> bool {
        counter
            .try_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                (current > 0).then(|| current - 1)
            })
            .is_ok()
    }
}

#[derive(Clone)]
struct ControlledPosixProvider {
    inner: PosixProvider,
    control: FailureControl,
}

impl ControlledPosixProvider {
    fn new(root: std::path::PathBuf, control: FailureControl) -> Self {
        Self {
            inner: PosixProvider::new(root),
            control,
        }
    }
}

impl TieredStoreProvider for ControlledPosixProvider {
    async fn create_segment(
        &self,
        path: String,
        segment_type: FileSegmentType,
        base_offset: u64,
        max_size: u64,
    ) -> Result<TieredFileSegment<Self>, RocketMQError> {
        let metadata = rocketmq_tieredstore::FileSegmentMetadata::new(path.clone(), segment_type, base_offset);
        Ok(TieredFileSegment::new(
            path,
            segment_type,
            base_offset,
            max_size,
            metadata,
            self.clone(),
        ))
    }

    async fn segment_size(&self, path: String) -> Result<u64, RocketMQError> {
        self.inner.segment_size(path).await
    }

    async fn read(&self, path: String, position: u64, length: usize) -> Result<Bytes, RocketMQError> {
        self.inner.read(path, position, length).await
    }

    async fn write(&self, path: String, position: u64, data: Bytes) -> Result<usize, RocketMQError> {
        if FailureControl::take(&self.control.fail_writes) {
            return Err(RocketMQError::storage_write_failed(path, "injected provider timeout"));
        }
        if FailureControl::take(&self.control.partial_writes) {
            let partial = (data.len() / 2).max(1).min(data.len());
            return self.inner.write(path, position, data.slice(..partial)).await;
        }
        self.inner.write(path, position, data).await
    }

    async fn delete(&self, path: String) -> Result<(), RocketMQError> {
        self.inner.delete(path).await
    }

    async fn sync(&self, path: String) -> Result<(), RocketMQError> {
        self.inner.sync(path).await
    }

    async fn rename(&self, source: String, destination: String) -> Result<(), RocketMQError> {
        self.inner.rename(source, destination).await
    }

    async fn list(&self, prefix: String) -> Result<Vec<String>, RocketMQError> {
        self.inner.list(prefix).await
    }

    async fn delete_prefix(&self, prefix: String) -> Result<(), RocketMQError> {
        self.inner.delete_prefix(prefix).await
    }

    async fn atomic_write(&self, path: String, data: Bytes) -> Result<(), RocketMQError> {
        self.inner.atomic_write(path, data).await
    }
}

fn test_config(root: std::path::PathBuf) -> TieredStoreConfig {
    TieredStoreConfig {
        storage_level: TieredStorageLevel::Force,
        store_path_root_dir: root,
        backend_provider: "controlled-posix".to_owned(),
        source_epoch: 7,
        max_pending_tasks: 4,
        max_pending_bytes: 1024,
        retry_ledger_max_entries: 4,
        retry_ledger_max_bytes: 1024,
        retry_ledger_max_age: Duration::from_secs(60),
        retry_backoff_initial: Duration::from_millis(5),
        retry_backoff_max: Duration::from_millis(20),
        source_wal_segment_size: 16,
        commit_log_segment_size: 1024,
        consume_queue_segment_size: 1024,
        delete_file_interval: Duration::from_secs(3600),
        ..TieredStoreConfig::default()
    }
}

fn request(offset: u64, queue_offset: i64, body: Bytes) -> TieredDispatchRequest {
    TieredDispatchRequest {
        topic: "RetryTopic".to_owned(),
        queue_id: 0,
        queue_offset,
        commit_log_offset: offset as i64,
        message_size: body.len() as i32,
        tags_code: queue_offset,
        store_timestamp: 100 + queue_offset,
        keys: Some(format!("key-{queue_offset}")),
        uniq_key: None,
        offset_id: None,
        sys_flag: 0,
        body: Some(body),
    }
}

fn record(config: &TieredStoreConfig, offset: u64, length: usize) -> DerivedRecordId {
    DerivedRecordId::try_new(config.source_epoch, offset, length as u32).expect("valid test record")
}

async fn wait_for_health<P>(
    dispatcher: &DefaultTieredDispatcher<P>,
    predicate: impl Fn(&rocketmq_tieredstore::TieredDispatchHealth) -> bool,
) where
    P: TieredStoreProvider,
{
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let health = dispatcher.health();
            if predicate(&health) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("tiered dispatch health should reach expected state");
}

#[tokio::test]
async fn timeout_ledger_survives_restart_without_payload_and_releases_wal_pin() -> Result<(), RocketMQError> {
    let temp_dir = tempfile::tempdir().map_err(|error| RocketMQError::Internal(error.to_string()))?;
    let root = temp_dir.path().join("tiered");
    let provider_root = temp_dir.path().join("provider");
    let config = test_config(root.clone());
    let body = Bytes::from_static(b"timeout-source-payload");
    let source_record = record(&config, 32, body.len());
    let control = FailureControl::default();
    control.fail_next(1);

    let store = TieredStore::with_provider(
        config.clone(),
        ControlledPosixProvider::new(provider_root.clone(), control),
    )?;
    store.load().await?;
    store.start().await?;
    store
        .dispatcher()
        .dispatch_derived(source_record, request(32, 0, body.clone()))
        .await?;
    wait_for_health(store.dispatcher().as_ref(), |health| health.retry_count() == 1).await;

    let failed_health = store.dispatcher().health();
    assert_eq!(
        failed_health.cursor().map(|cursor| cursor.next_offset()),
        Some(32 + body.len() as u64)
    );
    assert_eq!(failed_health.minimum_pinned_offset(), Some(32));
    assert_eq!(failed_health.minimum_pinned_wal_segment(), Some(32));
    let progress_bytes = tokio::fs::read(root.join("config").join("tieredDispatchProgress.bin"))
        .await
        .map_err(|error| RocketMQError::Internal(error.to_string()))?;
    assert!(!progress_bytes.windows(body.len()).any(|window| window == body.as_ref()));
    store.shutdown().await?;

    let restarted = TieredStore::with_provider(
        config.clone(),
        ControlledPosixProvider::new(provider_root, FailureControl::default()),
    )?;
    restarted.load().await?;
    assert_eq!(restarted.dispatcher().health().retry_count(), 1);
    let retry_body = body.clone();
    restarted
        .dispatcher()
        .set_retry_payload_resolver(Arc::new(move |offset, length| {
            (offset == 32 && length as usize == retry_body.len()).then(|| retry_body.clone())
        }));
    restarted.start().await?;
    wait_for_health(restarted.dispatcher().as_ref(), |health| {
        health.retry_count() == 0 && health.minimum_pinned_offset().is_none()
    })
    .await;
    let fetched = restarted
        .fetcher()
        .get_message("RetryTopic".to_owned(), 0, 0, 1)
        .await?;
    assert_eq!(fetched.messages, vec![body]);
    restarted.shutdown().await
}

#[tokio::test]
async fn partial_provider_write_resumes_and_duplicate_commit_is_idempotent() -> Result<(), RocketMQError> {
    let temp_dir = tempfile::tempdir().map_err(|error| RocketMQError::Internal(error.to_string()))?;
    let config = test_config(temp_dir.path().join("tiered"));
    let body = Bytes::from_static(b"partial-write-body");
    let source_record = record(&config, 0, body.len());
    let control = FailureControl::default();
    control.partial_next(1);
    let store = TieredStore::with_provider(
        config.clone(),
        ControlledPosixProvider::new(temp_dir.path().join("provider"), control.clone()),
    )?;
    store.load().await?;
    let retry_body = body.clone();
    store
        .dispatcher()
        .set_retry_payload_resolver(Arc::new(move |offset, length| {
            (offset == 0 && length as usize == retry_body.len()).then(|| retry_body.clone())
        }));
    store.start().await?;
    store
        .dispatcher()
        .dispatch_derived(source_record, request(0, 0, body.clone()))
        .await?;
    wait_for_health(store.dispatcher().as_ref(), |health| {
        health.cursor().map(|cursor| cursor.next_offset()) == Some(body.len() as u64) && health.retry_count() == 0
    })
    .await;
    assert_eq!(control.partial_writes.load(Ordering::Acquire), 0);

    store
        .dispatcher()
        .dispatch_derived(source_record, request(0, 0, body.clone()))
        .await?;
    tokio::time::sleep(Duration::from_millis(25)).await;
    let fetched = store.fetcher().get_message("RetryTopic".to_owned(), 0, 0, 2).await?;
    assert_eq!(fetched.messages, vec![body]);
    assert_eq!(store.dispatcher().health().retry_count(), 0);
    assert_eq!(store.dispatcher().health().minimum_pinned_offset(), None);
    store.shutdown().await
}

#[tokio::test]
async fn failed_partition_is_isolated_after_retry_is_durable() -> Result<(), RocketMQError> {
    let temp_dir = tempfile::tempdir().map_err(|error| RocketMQError::Internal(error.to_string()))?;
    let mut config = test_config(temp_dir.path().join("tiered"));
    config.retry_backoff_initial = Duration::from_secs(10);
    config.retry_backoff_max = Duration::from_secs(10);
    let control = FailureControl::default();
    control.fail_next(1);
    let store = TieredStore::with_provider(
        config.clone(),
        ControlledPosixProvider::new(temp_dir.path().join("provider"), control),
    )?;
    store.load().await?;
    store.start().await?;
    let body = Bytes::from_static(b"four");
    store
        .dispatcher()
        .dispatch_derived(record(&config, 0, 4), request(0, 0, body.clone()))
        .await?;
    wait_for_health(store.dispatcher().as_ref(), |health| health.retry_count() == 1).await;

    let mut isolated = request(4, 0, body.clone());
    isolated.topic = "HealthyTopic".to_owned();
    isolated.queue_id = 1;
    store
        .dispatcher()
        .dispatch_derived(record(&config, 4, 4), isolated)
        .await?;
    wait_for_health(store.dispatcher().as_ref(), |health| {
        health.cursor().map(|cursor| cursor.next_offset()) == Some(8) && health.retry_count() == 1
    })
    .await;
    let fetched = store.fetcher().get_message("HealthyTopic".to_owned(), 1, 0, 1).await?;
    assert_eq!(fetched.messages, vec![body]);
    assert_eq!(store.dispatcher().health().minimum_pinned_offset(), Some(0));
    store.shutdown().await
}

#[tokio::test]
async fn full_retry_ledger_holds_cursor_and_applies_byte_backpressure() -> Result<(), RocketMQError> {
    let temp_dir = tempfile::tempdir().map_err(|error| RocketMQError::Internal(error.to_string()))?;
    let mut config = test_config(temp_dir.path().join("tiered"));
    config.max_pending_tasks = 1;
    config.max_pending_bytes = 4;
    config.retry_ledger_max_entries = 1;
    config.retry_backoff_initial = Duration::from_secs(10);
    config.retry_backoff_max = Duration::from_secs(10);
    let control = FailureControl::default();
    control.fail_next(100);
    let store = TieredStore::with_provider(
        config.clone(),
        ControlledPosixProvider::new(temp_dir.path().join("provider"), control.clone()),
    )?;
    store.load().await?;
    store.start().await?;
    let body = Bytes::from_static(b"four");
    store
        .dispatcher()
        .dispatch_derived(record(&config, 0, 4), request(0, 0, body.clone()))
        .await?;
    wait_for_health(store.dispatcher().as_ref(), |health| health.retry_count() == 1).await;
    store
        .dispatcher()
        .dispatch_derived(record(&config, 4, 4), request(4, 1, body.clone()))
        .await?;
    wait_for_health(store.dispatcher().as_ref(), |health| {
        health.readiness() == TieredDispatchReadiness::RetryLedgerFull
    })
    .await;

    assert_eq!(
        store.dispatcher().health().cursor().map(|cursor| cursor.next_offset()),
        Some(4)
    );
    assert_eq!(store.dispatcher().health().retry_count(), 1);
    let blocked = tokio::time::timeout(
        Duration::from_millis(100),
        store
            .dispatcher()
            .dispatch_derived(record(&config, 8, 4), request(8, 2, body)),
    )
    .await;
    assert!(blocked.is_err(), "third record must wait for bounded byte capacity");

    control.clear();
    let _ = store.dispatcher().shutdown().await;
    store.shutdown().await
}

#[tokio::test]
async fn retry_source_byte_limit_stops_cursor_before_unrecorded_failure() -> Result<(), RocketMQError> {
    let temp_dir = tempfile::tempdir().map_err(|error| RocketMQError::Internal(error.to_string()))?;
    let mut config = test_config(temp_dir.path().join("tiered"));
    config.retry_ledger_max_entries = 4;
    config.retry_ledger_max_bytes = 4;
    config.retry_backoff_initial = Duration::from_secs(10);
    config.retry_backoff_max = Duration::from_secs(10);
    let control = FailureControl::default();
    control.fail_next(100);
    let store = TieredStore::with_provider(
        config.clone(),
        ControlledPosixProvider::new(temp_dir.path().join("provider"), control.clone()),
    )?;
    store.load().await?;
    store.start().await?;
    let body = Bytes::from_static(b"four");
    store
        .dispatcher()
        .dispatch_derived(record(&config, 0, 4), request(0, 0, body.clone()))
        .await?;
    wait_for_health(store.dispatcher().as_ref(), |health| health.retry_count() == 1).await;
    store
        .dispatcher()
        .dispatch_derived(record(&config, 4, 4), request(4, 1, body))
        .await?;
    wait_for_health(store.dispatcher().as_ref(), |health| {
        health.readiness() == TieredDispatchReadiness::RetryLedgerBytesExceeded
    })
    .await;

    let health = store.dispatcher().health();
    assert_eq!(health.cursor().map(|cursor| cursor.next_offset()), Some(4));
    assert_eq!(health.retry_source_bytes(), 4);
    assert_eq!(health.minimum_pinned_offset(), Some(0));
    control.clear();
    let _ = store.dispatcher().shutdown().await;
    store.shutdown().await
}

#[tokio::test]
async fn retry_age_limit_fails_readiness_without_losing_durable_entry() -> Result<(), RocketMQError> {
    let temp_dir = tempfile::tempdir().map_err(|error| RocketMQError::Internal(error.to_string()))?;
    let mut config = test_config(temp_dir.path().join("tiered"));
    config.retry_ledger_max_age = Duration::from_millis(10);
    config.retry_backoff_initial = Duration::from_millis(5);
    config.retry_backoff_max = Duration::from_millis(5);
    let control = FailureControl::default();
    control.fail_next(1);
    let store = TieredStore::with_provider(
        config.clone(),
        ControlledPosixProvider::new(temp_dir.path().join("provider"), control),
    )?;
    store.load().await?;
    store.start().await?;
    let body = Bytes::from_static(b"aged");
    store
        .dispatcher()
        .dispatch_derived(record(&config, 0, body.len()), request(0, 0, body))
        .await?;
    wait_for_health(store.dispatcher().as_ref(), |health| {
        health.readiness() == TieredDispatchReadiness::RetryLedgerExpired
    })
    .await;
    assert_eq!(store.dispatcher().health().retry_count(), 1);
    assert!(store.dispatcher().health().oldest_retry_age() >= Duration::from_millis(10));
    store.shutdown().await
}

#[tokio::test]
async fn corrupted_progress_snapshot_fails_restart_closed() -> Result<(), RocketMQError> {
    let temp_dir = tempfile::tempdir().map_err(|error| RocketMQError::Internal(error.to_string()))?;
    let root = temp_dir.path().join("tiered");
    let provider_root = temp_dir.path().join("provider");
    let config = test_config(root.clone());
    let store = TieredStore::with_provider(
        config.clone(),
        ControlledPosixProvider::new(provider_root.clone(), FailureControl::default()),
    )?;
    store.load().await?;
    store.start().await?;
    let body = Bytes::from_static(b"valid");
    store
        .dispatcher()
        .dispatch_derived(record(&config, 0, body.len()), request(0, 0, body))
        .await?;
    wait_for_health(store.dispatcher().as_ref(), |health| health.cursor().is_some()).await;
    store.shutdown().await?;

    let progress_path = root.join("config").join("tieredDispatchProgress.bin");
    let mut encoded = tokio::fs::read(&progress_path)
        .await
        .map_err(|error| RocketMQError::Internal(error.to_string()))?;
    let last = encoded
        .last_mut()
        .ok_or_else(|| RocketMQError::Internal("empty progress snapshot".to_owned()))?;
    *last ^= 0xFF;
    tokio::fs::write(&progress_path, encoded)
        .await
        .map_err(|error| RocketMQError::Internal(error.to_string()))?;

    let restarted = TieredStore::with_provider(
        config,
        ControlledPosixProvider::new(provider_root, FailureControl::default()),
    )?;
    let error = restarted
        .load()
        .await
        .expect_err("corrupted progress must fail readiness");
    assert!(
        error.to_string().contains("tieredDispatchProgress.bin"),
        "unexpected restart error: {error}"
    );
    Ok(())
}
