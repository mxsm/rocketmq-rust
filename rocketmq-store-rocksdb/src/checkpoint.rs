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

use crate::store::RocksDbStore;

pub async fn create_checkpoint(store: Arc<RocksDbStore>, target_dir: PathBuf) -> Result<(), RocketMQError> {
    store.create_checkpoint(target_dir).await
}

pub async fn create_backup(store: Arc<RocksDbStore>, backup_dir: PathBuf) -> Result<(), RocketMQError> {
    store.create_backup(backup_dir).await
}

pub async fn restore_latest_backup(
    backup_dir: PathBuf,
    db_dir: PathBuf,
    wal_dir: Option<PathBuf>,
) -> Result<(), RocketMQError> {
    RocksDbStore::restore_latest_backup(backup_dir, db_dir, wal_dir).await
}
