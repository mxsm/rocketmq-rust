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
use std::time::Duration;

use bytes::Bytes;
use rocketmq_runtime::RuntimeContext;
use rocketmq_store_api::DerivedRecordId;
use rocketmq_store_api::StoreComponent;
use rocketmq_store_api::StoreError;
use rocketmq_store_api::StoreOperation;
use tokio_util::sync::CancellationToken;

use super::*;
use crate::config::TieredStoreConfig;
use crate::file::TieredFlatFileStore;
use crate::metadata::JsonMetadataStore;
use crate::provider::MemoryProvider;

fn valid_dispatch_request(queue_offset: i64) -> TieredDispatchRequest {
    TieredDispatchRequest {
        topic: "TopicA".to_owned(),
        queue_id: 0,
        queue_offset,
        commit_log_offset: 1024,
        message_size: 4,
        tags_code: 7,
        store_timestamp: 100,
        keys: None,
        uniq_key: None,
        offset_id: None,
        sys_flag: 0,
        body: Some(Bytes::from_static(b"body")),
    }
}

#[test]
fn dispatcher_task_result_preserves_original_error_kind() {
    let original = StoreError::new(&rocketmq_error::STORAGE_REQUEST_INVALID, StoreOperation::AppendDerived);
    let error = dispatcher_task_result(Some(original)).expect_err("dispatcher worker error should be propagated");

    assert_eq!(error.descriptor(), &rocketmq_error::STORAGE_REQUEST_INVALID);
    assert_eq!(error.operation(), StoreOperation::AppendDerived);
}

#[test]
fn dispatcher_startup_failed_uses_service_error_kind() {
    let error = dispatcher_startup_failed(
        "spawn test worker",
        rocketmq_runtime::RuntimeError::InsideTokioRuntime("task group closed"),
    );

    assert_eq!(error.descriptor(), &rocketmq_error::STORAGE_INTERNAL_FAILURE);
    assert!(std::error::Error::source(&error)
        .and_then(|source| source.downcast_ref::<rocketmq_runtime::RuntimeError>())
        .is_some());
}

#[tokio::test]
async fn dispatch_writes_commit_log_and_consume_queue_unit() -> Result<(), StoreError> {
    let temp_dir = tempfile::tempdir().map_err(|source| {
        crate::error::source_error(
            &rocketmq_error::STORAGE_INTERNAL_FAILURE,
            rocketmq_store_api::StoreOperation::Load,
            source,
        )
    })?;
    let config = Arc::new(TieredStoreConfig {
        store_path_root_dir: temp_dir.path().to_path_buf(),
        backend_provider: "memory".to_owned(),
        max_pending_tasks: 4,
        ..TieredStoreConfig::default()
    });
    let flat_file_store = Arc::new(TieredFlatFileStore::new(
        config.clone(),
        Arc::new(JsonMetadataStore::new(config.clone())),
        MemoryProvider::default(),
    ));
    let context = RuntimeContext::from_current("tieredstore-dispatch-write-test");
    let dispatcher = DefaultTieredDispatcher::new(
        config,
        flat_file_store.clone(),
        CancellationToken::new(),
        context.root_group().clone(),
    );

    dispatcher.start().await?;
    dispatcher
        .dispatch(TieredDispatchRequest {
            topic: "TopicA".to_owned(),
            queue_id: 0,
            queue_offset: 3,
            commit_log_offset: 1024,
            message_size: 4,
            tags_code: 7,
            store_timestamp: 100,
            keys: None,
            uniq_key: None,
            offset_id: None,
            sys_flag: 0,
            body: Some(Bytes::from_static(b"body")),
        })
        .await?;
    dispatcher.shutdown().await?;

    let flat_file = flat_file_store
        .get("TopicA", 0)
        .ok_or_else(|| crate::error::internal_failure(rocketmq_store_api::StoreOperation::Load))?;
    let cq_unit = flat_file
        .read_consume_queue_unit(3)
        .await?
        .ok_or_else(|| crate::error::internal_failure(rocketmq_store_api::StoreOperation::Load))?;

    assert_eq!(cq_unit.commit_log_offset, 0);
    assert_eq!(cq_unit.size, 4);
    assert_eq!(cq_unit.tags_code, 7);
    assert_eq!(
        flat_file.read_message_by_queue_offset(3).await?,
        Some(Bytes::from_static(b"body"))
    );
    Ok(())
}

#[tokio::test]
async fn new_with_task_group_parents_dispatcher_task() -> Result<(), StoreError> {
    let temp_dir = tempfile::tempdir().map_err(|source| {
        crate::error::source_error(
            &rocketmq_error::STORAGE_INTERNAL_FAILURE,
            rocketmq_store_api::StoreOperation::Load,
            source,
        )
    })?;
    let config = Arc::new(TieredStoreConfig {
        store_path_root_dir: temp_dir.path().to_path_buf(),
        backend_provider: "memory".to_owned(),
        max_pending_tasks: 4,
        ..TieredStoreConfig::default()
    });
    let flat_file_store = Arc::new(TieredFlatFileStore::new(
        config.clone(),
        Arc::new(JsonMetadataStore::new(config.clone())),
        MemoryProvider::default(),
    ));
    let context = RuntimeContext::from_current("tieredstore-dispatcher-parent-test");
    let service = context.service_context("tieredstore-dispatcher");
    let dispatcher = DefaultTieredDispatcher::new(
        config,
        flat_file_store,
        CancellationToken::new(),
        service.task_group().clone(),
    );

    dispatcher.start().await?;
    assert!(service.task_group().task_count() >= 2);
    dispatcher.shutdown().await?;

    let report = service.task_group().shutdown(Duration::from_secs(1)).await;
    assert!(report.is_healthy(), "{}", report.to_json());
    assert!(report.completed >= 2, "{}", report.to_json());
    assert!(report.children.is_empty(), "{}", report.to_json());
    Ok(())
}

#[tokio::test]
async fn cancellation_releases_a_sender_waiting_for_byte_capacity() -> Result<(), StoreError> {
    let temp_dir = tempfile::tempdir().map_err(|source| {
        crate::error::source_error(
            &rocketmq_error::STORAGE_INTERNAL_FAILURE,
            rocketmq_store_api::StoreOperation::Load,
            source,
        )
    })?;
    let config = Arc::new(TieredStoreConfig {
        store_path_root_dir: temp_dir.path().to_path_buf(),
        backend_provider: "memory".to_owned(),
        max_pending_tasks: 1,
        max_pending_bytes: 4,
        ..TieredStoreConfig::default()
    });
    let flat_file_store = Arc::new(TieredFlatFileStore::new(
        config.clone(),
        Arc::new(JsonMetadataStore::new(config.clone())),
        MemoryProvider::default(),
    ));
    let shutdown = CancellationToken::new();
    let context = RuntimeContext::from_current("tieredstore-dispatch-cancellation-test");
    let dispatcher =
        DefaultTieredDispatcher::new(config, flat_file_store, shutdown.clone(), context.root_group().clone());
    let request = || TieredDispatchRequest {
        topic: "TopicA".to_owned(),
        queue_id: 0,
        queue_offset: 0,
        commit_log_offset: 0,
        message_size: 4,
        tags_code: 0,
        store_timestamp: 0,
        keys: None,
        uniq_key: None,
        offset_id: None,
        sys_flag: 0,
        body: Some(Bytes::from_static(b"body")),
    };

    dispatcher.dispatch(request()).await?;
    let blocked = dispatcher.dispatch(request());
    tokio::pin!(blocked);
    assert!(tokio::time::timeout(Duration::from_millis(25), &mut blocked)
        .await
        .is_err());

    shutdown.cancel();
    let error = tokio::time::timeout(Duration::from_secs(1), blocked)
        .await
        .map_err(|source| {
            crate::error::source_error(
                &rocketmq_error::STORAGE_INTERNAL_FAILURE,
                rocketmq_store_api::StoreOperation::Load,
                source,
            )
        })?
        .expect_err("cancelled sender must return an interruption");
    assert_eq!(error.descriptor(), &rocketmq_error::STORAGE_BACKEND_UNAVAILABLE);
    assert!(dispatcher.is_shutdown());
    Ok(())
}

#[tokio::test]
async fn try_dispatch_distinguishes_full_capacity_from_closed_backend() -> Result<(), StoreError> {
    let temp_dir = tempfile::tempdir().map_err(|source| {
        crate::error::source_error(&rocketmq_error::STORAGE_INTERNAL_FAILURE, StoreOperation::Load, source)
    })?;
    let config = Arc::new(TieredStoreConfig {
        store_path_root_dir: temp_dir.path().to_path_buf(),
        backend_provider: "memory".to_owned(),
        max_pending_tasks: 1,
        ..TieredStoreConfig::default()
    });
    let flat_file_store = Arc::new(TieredFlatFileStore::new(
        config.clone(),
        Arc::new(JsonMetadataStore::new(config.clone())),
        MemoryProvider::default(),
    ));
    let context = RuntimeContext::from_current("tieredstore-try-dispatch-error-test");
    let dispatcher = DefaultTieredDispatcher::new(
        config.clone(),
        flat_file_store.clone(),
        CancellationToken::new(),
        context.root_group().clone(),
    );

    dispatcher.try_dispatch(valid_dispatch_request(0))?;
    let full = dispatcher
        .try_dispatch(valid_dispatch_request(1))
        .expect_err("a full dispatch channel must report capacity exhaustion");
    assert_eq!(full.descriptor(), &rocketmq_error::STORAGE_CAPACITY_EXHAUSTED);
    assert_eq!(full.recovery_hint(), rocketmq_error::RecoveryHint::OperatorAction);
    assert_eq!(full.operation(), StoreOperation::AppendDerived);
    assert_eq!(full.component(), StoreComponent::TieredStore);

    let closed_dispatcher = DefaultTieredDispatcher::new(
        config.clone(),
        flat_file_store,
        CancellationToken::new(),
        context.root_group().clone(),
    );
    drop(closed_dispatcher.receiver.lock().await.take());
    let record =
        DerivedRecordId::try_new(config.source_epoch, 1024, 4).expect("the derived record fixture must be valid");
    let closed = closed_dispatcher
        .try_dispatch_derived(record, valid_dispatch_request(0))
        .expect_err("a closed dispatch channel must report backend unavailability");
    assert_eq!(closed.descriptor(), &rocketmq_error::STORAGE_BACKEND_UNAVAILABLE);
    assert_eq!(closed.recovery_hint(), rocketmq_error::RecoveryHint::Backoff);
    assert_eq!(closed.operation(), StoreOperation::AppendDerived);
    assert_eq!(closed.component(), StoreComponent::TieredStore);
    Ok(())
}
