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

use std::path::PathBuf;
use std::sync::Arc;

use rocketmq_error::RocketMQError;

use crate::rocksdb::store::RocksDbStore;

pub async fn create_checkpoint(store: Arc<RocksDbStore>, target_dir: PathBuf) -> Result<(), RocketMQError> {
    tokio::task::spawn_blocking(move || {
        let db = store.db();
        let checkpoint = ::rocksdb::checkpoint::Checkpoint::new(&db)
            .map_err(|error| RocketMQError::storage_write_failed("rocksdb", error.to_string()))?;
        checkpoint
            .create_checkpoint(target_dir)
            .map_err(|error| RocketMQError::storage_write_failed("rocksdb", error.to_string()))
    })
    .await
    .map_err(|error| RocketMQError::storage_write_failed("rocksdb", format!("checkpoint task failed: {error}")))?
}
