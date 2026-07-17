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
use rocketmq_error::ErrorKind;
use rocketmq_error::RocketMQError;
use rocketmq_runtime::RuntimeContext;
use tokio_util::sync::CancellationToken;

use super::*;
use crate::config::TieredStoreConfig;
use crate::file::TieredFlatFileStore;
use crate::metadata::JsonMetadataStore;
use crate::provider::MemoryProvider;

#[test]
fn dispatcher_task_result_preserves_original_error_kind() {
    let error = dispatcher_task_result(Some(RocketMQError::illegal_argument("dispatch offset gap")))
        .expect_err("dispatcher worker error should be propagated");

    assert_eq!(error.kind(), ErrorKind::IllegalArgument);
    assert!(error.to_string().contains("dispatch offset gap"));
}

#[test]
fn dispatcher_startup_failed_uses_service_error_kind() {
    let error = dispatcher_startup_failed("spawn test worker", "task group closed");

    assert_eq!(error.kind(), ErrorKind::Service);
    assert!(error.to_string().contains("tieredstore dispatcher spawn test worker"));
}

#[tokio::test]
async fn dispatch_writes_commit_log_and_consume_queue_unit() -> Result<(), RocketMQError> {
    let temp_dir = tempfile::tempdir().map_err(|err| RocketMQError::Internal(err.to_string()))?;
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
    let dispatcher = DefaultTieredDispatcher::new(config, flat_file_store.clone(), CancellationToken::new());

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
        .ok_or_else(|| RocketMQError::Internal("missing dispatched flat file".to_owned()))?;
    let cq_unit = flat_file
        .read_consume_queue_unit(3)
        .await?
        .ok_or_else(|| RocketMQError::Internal("missing dispatched consume queue unit".to_owned()))?;

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
async fn new_with_task_group_parents_dispatcher_task() -> Result<(), RocketMQError> {
    let temp_dir = tempfile::tempdir().map_err(|err| RocketMQError::Internal(err.to_string()))?;
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
    let dispatcher = DefaultTieredDispatcher::new_with_task_group(
        config,
        flat_file_store,
        CancellationToken::new(),
        service.task_group().clone(),
    );

    dispatcher.start().await?;
    dispatcher.shutdown().await?;

    let report = service.task_group().shutdown(Duration::from_secs(1)).await;
    assert!(
        report
            .children
            .iter()
            .any(|child| child.name == "rocketmq-tieredstore.dispatcher"),
        "{}",
        report.to_json()
    );
    assert!(report.is_healthy(), "{}", report.to_json());
    Ok(())
}

#[tokio::test]
async fn cancellation_releases_a_sender_waiting_for_byte_capacity() -> Result<(), RocketMQError> {
    let temp_dir = tempfile::tempdir().map_err(|error| RocketMQError::Internal(error.to_string()))?;
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
    let dispatcher = DefaultTieredDispatcher::new(config, flat_file_store, shutdown.clone());
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
        .map_err(|error| RocketMQError::Internal(error.to_string()))?
        .expect_err("cancelled sender must return an interruption");
    assert_eq!(error.kind(), ErrorKind::Service);
    assert!(dispatcher.is_shutdown());
    Ok(())
}
