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

use crate::config::message_store_config::MessageStoreConfig;

pub use rocketmq_store_rocksdb::config::RocksDbBlockBasedIndexType;
pub use rocketmq_store_rocksdb::config::RocksDbColumnFamilyConfig;
pub use rocketmq_store_rocksdb::config::RocksDbCompactionStyle;
pub use rocketmq_store_rocksdb::config::RocksDbCompressionType;
pub use rocketmq_store_rocksdb::config::RocksDbConfig;
pub use rocketmq_store_rocksdb::config::RocksDbConfigSource;
pub use rocketmq_store_rocksdb::config::RocksDbDataBlockIndexType;
pub use rocketmq_store_rocksdb::config::RocksDbWalRecoveryMode;

impl RocksDbConfigSource for MessageStoreConfig {
    fn store_path_root_dir(&self) -> &str {
        self.store_path_root_dir.as_str()
    }

    fn rocksdb_store_enabled(&self) -> bool {
        self.is_enable_rocksdb_store()
    }

    fn use_separate_store_path_for_rocksdb_cq(&self) -> bool {
        self.use_separate_store_path_for_rocksdb_cq
    }

    fn mem_table_flush_interval_ms(&self) -> usize {
        self.mem_table_flush_interval_ms
    }

    fn clean_rocksdb_dirty_cq_interval_min(&self) -> usize {
        self.clean_rocksdb_dirty_cq_interval_min
    }

    fn rocksdb_checkpoint_interval_ms(&self) -> usize {
        self.rocksdb_checkpoint_interval_ms
    }

    fn rocksdb_backup_interval_ms(&self) -> usize {
        self.rocksdb_backup_interval_ms
    }

    fn rocksdb_backup_dir(&self) -> Option<&str> {
        self.rocksdb_backup_dir.as_ref().map(|path| path.as_str())
    }
}
