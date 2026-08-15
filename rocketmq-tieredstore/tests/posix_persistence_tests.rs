// Copyright 2023 The RocketMQ Rust Authors
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

use bytes::Bytes;
use bytes::BytesMut;
use rocketmq_error::RocketMQError;
use rocketmq_model::boundary_type::BoundaryType;
use rocketmq_runtime::RuntimeContext;
use rocketmq_tieredstore::fetcher::TieredGetMessageStatus;
use rocketmq_tieredstore::TieredDispatchRequest;
use rocketmq_tieredstore::TieredDispatcher;
use rocketmq_tieredstore::TieredLifecycle;
use rocketmq_tieredstore::TieredMessageFetcher;
use rocketmq_tieredstore::TieredStorageLevel;
use rocketmq_tieredstore::TieredStore;
use rocketmq_tieredstore::TieredStoreConfig;

const MESSAGE_STORE_TIMESTAMP_POSITION: usize = 56;

#[tokio::test]
async fn posix_store_recovers_dispatched_messages_and_index_after_restart() -> Result<(), RocketMQError> {
    let temp_dir =
        tempfile::tempdir().map_err(|source| RocketMQError::internal("create temporary directory", source))?;
    let config = TieredStoreConfig {
        storage_level: TieredStorageLevel::Force,
        backend_provider: "posix".to_owned(),
        store_path_root_dir: temp_dir.path().join("tieredstore"),
        commit_log_segment_size: 128,
        consume_queue_segment_size: 80,
        index_file_max_hash_slot_num: 16,
        index_file_max_index_num: 64,
        max_pending_tasks: 8,
        ..TieredStoreConfig::default()
    };
    let topic = "TopicA".to_owned();
    let queue_id = 0;
    let queue_offset = 0;
    let store_timestamp = 1_700_000_i64;
    let message = encoded_message(store_timestamp, b"persisted-posix-message");

    let context = RuntimeContext::from_current("tiered-posix-persistence-test");
    let store = TieredStore::new(config.clone(), context.root_group().clone())?;
    store.load().await?;
    store.start().await?;
    store
        .dispatcher()
        .dispatch(TieredDispatchRequest {
            topic: topic.clone(),
            queue_id,
            queue_offset,
            commit_log_offset: 0,
            message_size: message.len() as i32,
            tags_code: 0,
            store_timestamp,
            keys: Some("keyA".to_owned()),
            uniq_key: Some("uniqA".to_owned()),
            offset_id: None,
            sys_flag: 0,
            body: Some(message.clone()),
        })
        .await?;
    store.shutdown().await?;

    let provider_before_restart = provider_file_snapshot(&config.store_path_root_dir)?;
    let metadata: serde_json::Value = serde_json::from_slice(
        &std::fs::read(
            config
                .store_path_root_dir
                .join("config")
                .join("tieredStoreMetadata.json"),
        )
        .map_err(|source| RocketMQError::internal("read tiered metadata", source))?,
    )
    .map_err(|source| RocketMQError::internal("decode tiered metadata", source))?;
    assert_eq!(metadata["format"], "rocketmq-tiered-metadata");
    assert_eq!(metadata["version"], 1);
    assert_eq!(metadata["provider"]["id"], "posix");
    assert_eq!(metadata["provider"]["format"], "rocketmq-tiered-posix");
    assert_eq!(metadata["provider"]["version"], 1);

    let reloaded = TieredStore::new(config.clone(), context.root_group().clone())?;
    reloaded.load().await?;
    assert_eq!(
        provider_file_snapshot(&config.store_path_root_dir)?,
        provider_before_restart
    );

    let fetched = reloaded
        .fetcher()
        .get_message(topic.clone(), queue_id, queue_offset, 1)
        .await?;
    assert_eq!(fetched.status, TieredGetMessageStatus::Found);
    assert_eq!(fetched.messages, vec![message.clone()]);
    assert_eq!(
        reloaded
            .fetcher()
            .get_message_timestamp(topic.clone(), queue_id, queue_offset)
            .await?,
        store_timestamp
    );
    assert_eq!(
        reloaded
            .fetcher()
            .get_offset_by_time(topic.clone(), queue_id, store_timestamp - 1)
            .await?,
        queue_offset
    );
    assert_eq!(
        reloaded
            .fetcher()
            .get_offset_by_time_with_boundary(topic.clone(), queue_id, store_timestamp + 1, BoundaryType::Upper)
            .await?,
        queue_offset
    );

    let query_result = reloaded
        .fetcher()
        .query_message(topic.clone(), "keyA".to_owned(), 10, 0, i64::MAX)
        .await?;
    assert_eq!(query_result.values, vec![message.clone()]);
    let uniq_query_result = reloaded
        .fetcher()
        .query_message(topic, "uniqA".to_owned(), 10, 0, i64::MAX)
        .await?;
    assert_eq!(uniq_query_result.values, vec![message]);

    Ok(())
}

fn provider_file_snapshot(root: &std::path::Path) -> Result<Vec<(String, Vec<u8>)>, RocketMQError> {
    let mut snapshot = Vec::new();
    collect_provider_files(root, root, &mut snapshot)?;
    snapshot.sort_by(|left, right| left.0.cmp(&right.0));
    Ok(snapshot)
}

fn collect_provider_files(
    root: &std::path::Path,
    current: &std::path::Path,
    snapshot: &mut Vec<(String, Vec<u8>)>,
) -> Result<(), RocketMQError> {
    for entry in std::fs::read_dir(current).map_err(|source| RocketMQError::internal("list provider files", source))? {
        let entry = entry.map_err(|source| RocketMQError::internal("read provider entry", source))?;
        let path = entry.path();
        let relative = path
            .strip_prefix(root)
            .map_err(|source| RocketMQError::internal("resolve provider path", source))?;
        if relative
            .components()
            .next()
            .is_some_and(|component| component.as_os_str() == "config")
        {
            continue;
        }
        if path.is_dir() {
            collect_provider_files(root, &path, snapshot)?;
        } else {
            snapshot.push((
                relative.to_string_lossy().replace('\\', "/"),
                std::fs::read(&path).map_err(|source| RocketMQError::internal("read provider file", source))?,
            ));
        }
    }
    Ok(())
}

fn encoded_message(store_timestamp: i64, body: &[u8]) -> Bytes {
    let mut bytes = BytesMut::zeroed(MESSAGE_STORE_TIMESTAMP_POSITION + std::mem::size_of::<i64>());
    bytes[MESSAGE_STORE_TIMESTAMP_POSITION..MESSAGE_STORE_TIMESTAMP_POSITION + std::mem::size_of::<i64>()]
        .copy_from_slice(&store_timestamp.to_be_bytes());
    bytes.extend_from_slice(body);
    bytes.freeze()
}
