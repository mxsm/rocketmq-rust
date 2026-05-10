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
use rocketmq_error::RocketMQError;
use rocketmq_tieredstore::TieredDispatchRequest;
use rocketmq_tieredstore::TieredDispatcher;
use rocketmq_tieredstore::TieredLifecycle;
use rocketmq_tieredstore::TieredMessageFetcher;
use rocketmq_tieredstore::TieredStorageLevel;
use rocketmq_tieredstore::TieredStore;
use rocketmq_tieredstore::TieredStoreConfig;

#[tokio::main]
async fn main() -> Result<(), RocketMQError> {
    let temp_dir = tempfile::tempdir().map_err(|error| RocketMQError::Internal(error.to_string()))?;
    let store = TieredStore::new(TieredStoreConfig {
        storage_level: TieredStorageLevel::Force,
        backend_provider: "memory".to_owned(),
        store_path_root_dir: temp_dir.path().join("tieredstore"),
        max_pending_tasks: 16,
        ..TieredStoreConfig::default()
    })?;

    store.load().await?;
    store.start().await?;

    let body = Bytes::from_static(b"hello-tieredstore");
    store
        .dispatcher()
        .dispatch(TieredDispatchRequest {
            topic: "ExampleTopic".to_owned(),
            queue_id: 0,
            queue_offset: 0,
            commit_log_offset: 0,
            message_size: body.len() as i32,
            tags_code: 0,
            store_timestamp: 1_700_000_000,
            keys: Some("example-key".to_owned()),
            uniq_key: Some("example-uniq".to_owned()),
            offset_id: None,
            sys_flag: 0,
            body: Some(body),
        })
        .await?;
    store.shutdown().await?;

    let fetched = store.fetcher().get_message("ExampleTopic".to_owned(), 0, 0, 1).await?;
    println!(
        "tieredstore fetched status={:?}, message_count={}",
        fetched.status,
        fetched.messages.len()
    );
    Ok(())
}
