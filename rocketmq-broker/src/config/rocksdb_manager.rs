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

use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use rocketmq_error::RocketMQError;
use rocketmq_remoting::protocol::data_version_facade::DataVersionExt;
use rocketmq_remoting::protocol::DataVersion;
use rocketmq_store::rocksdb::batch::RocksDbWriteBatch;
use rocketmq_store::rocksdb::config::RocksDbColumnFamilyConfig;
use rocketmq_store::rocksdb::config::RocksDbConfig;
use rocketmq_store::rocksdb::config::RocksDbWalRecoveryMode;
use rocketmq_store::rocksdb::error::codec_error;
use rocketmq_store::rocksdb::iterator::RocksDbRangeScanOptions;
use rocketmq_store::rocksdb::store::KeyValueStore;
use rocketmq_store::rocksdb::store::RocksDbStore;

const DEFAULT_COLUMN_FAMILY: &str = "default";
const KV_DATA_VERSION_COLUMN_FAMILY: &str = "kvDataVersion";
const KV_DATA_VERSION_KEY: &[u8] = b"kvDataVersionKey";
const MB: usize = 1024 * 1024;
const MB_U64: u64 = 1024 * 1024;

type ConfigRecords = Vec<(Vec<u8>, Vec<u8>)>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RocksDbBrokerConfigManagerConfig {
    path: PathBuf,
    default_cf: String,
    version_cf: String,
    extra_cfs: Vec<String>,
    block_cache_size: usize,
    write_buffer_size: usize,
}

impl RocksDbBrokerConfigManagerConfig {
    pub(crate) fn topic(path: PathBuf) -> Self {
        Self {
            path,
            default_cf: "topic".to_string(),
            version_cf: "topicVersion".to_string(),
            extra_cfs: Vec::new(),
            block_cache_size: 256 * 1024 * 1024,
            write_buffer_size: 64 * 1024 * 1024,
        }
    }

    pub(crate) fn consumer_offset(path: PathBuf) -> Self {
        Self {
            path,
            default_cf: "consumerOffset".to_string(),
            version_cf: "consumerOffsetVersion".to_string(),
            extra_cfs: Vec::new(),
            block_cache_size: 256 * 1024 * 1024,
            write_buffer_size: 64 * 1024 * 1024,
        }
    }

    pub(crate) fn subscription_group(path: PathBuf) -> Self {
        Self {
            path,
            default_cf: "subscriptionGroup".to_string(),
            version_cf: "subscriptionGroupVersion".to_string(),
            extra_cfs: vec!["forbidden".to_string()],
            block_cache_size: 256 * 1024 * 1024,
            write_buffer_size: 64 * 1024 * 1024,
        }
    }

    pub(crate) fn generic(path: PathBuf) -> Self {
        Self {
            path,
            default_cf: DEFAULT_COLUMN_FAMILY.to_string(),
            version_cf: KV_DATA_VERSION_COLUMN_FAMILY.to_string(),
            extra_cfs: Vec::new(),
            block_cache_size: 256 * 1024 * 1024,
            write_buffer_size: 64 * 1024 * 1024,
        }
    }
}

pub(crate) struct RocksDbBrokerConfigStorageLayout;

impl RocksDbBrokerConfigStorageLayout {
    pub(crate) fn topic_path(root: &Path, use_single_rocksdb_for_all_configs: bool) -> PathBuf {
        Self::config_path(root, use_single_rocksdb_for_all_configs, "metadata", "topics")
    }

    pub(crate) fn consumer_offset_path(root: &Path, use_single_rocksdb_for_all_configs: bool) -> PathBuf {
        Self::config_path(root, use_single_rocksdb_for_all_configs, "metadata", "consumerOffsets")
    }

    pub(crate) fn subscription_group_path(root: &Path, use_single_rocksdb_for_all_configs: bool) -> PathBuf {
        Self::config_path(
            root,
            use_single_rocksdb_for_all_configs,
            "metadata",
            "subscriptionGroups",
        )
    }

    fn config_path(
        root: &Path,
        use_single_rocksdb_for_all_configs: bool,
        single_name: &str,
        separate_name: &str,
    ) -> PathBuf {
        root.join("config").join(if use_single_rocksdb_for_all_configs {
            single_name
        } else {
            separate_name
        })
    }
}

struct RocksDbBrokerConfigStoreCore {
    rocksdb_config: RocksDbConfig,
    store: RocksDbStore,
}

pub(crate) struct RocksDbBrokerConfigManager {
    default_cf: String,
    version_cf: String,
    block_cache_size: usize,
    write_buffer_size: usize,
    kv_data_version: parking_lot::Mutex<DataVersion>,
    core: Arc<RocksDbBrokerConfigStoreCore>,
}

impl RocksDbBrokerConfigManager {
    pub(crate) fn open(config: RocksDbBrokerConfigManagerConfig) -> Result<Self, RocketMQError> {
        let rocksdb_config = build_rocksdb_config(&config);
        let store = RocksDbStore::open_with_existing_column_families(rocksdb_config.clone())?;
        let core = Arc::new(RocksDbBrokerConfigStoreCore { rocksdb_config, store });
        Ok(Self::with_core(config, core))
    }

    pub(crate) fn open_shared(configs: Vec<RocksDbBrokerConfigManagerConfig>) -> Result<Vec<Arc<Self>>, RocketMQError> {
        if configs.is_empty() {
            return Ok(Vec::new());
        }
        let rocksdb_config = build_shared_rocksdb_config(&configs)?;
        let store = RocksDbStore::open_with_existing_column_families(rocksdb_config.clone())?;
        let core = Arc::new(RocksDbBrokerConfigStoreCore { rocksdb_config, store });
        configs
            .into_iter()
            .map(|config| Ok(Arc::new(Self::with_core(config, Arc::clone(&core)))))
            .collect()
    }

    fn with_core(config: RocksDbBrokerConfigManagerConfig, core: Arc<RocksDbBrokerConfigStoreCore>) -> Self {
        Self {
            default_cf: config.default_cf,
            version_cf: config.version_cf,
            block_cache_size: config.block_cache_size,
            write_buffer_size: config.write_buffer_size,
            kv_data_version: parking_lot::Mutex::new(
                rocketmq_remoting::protocol::data_version_facade::new_data_version(),
            ),
            core,
        }
    }

    pub(crate) fn rocksdb_config(&self) -> &RocksDbConfig {
        &self.core.rocksdb_config
    }

    pub(crate) fn path(&self) -> &Path {
        self.core.rocksdb_config.path.as_path()
    }

    pub(crate) fn default_cf(&self) -> &str {
        &self.default_cf
    }

    pub(crate) fn version_cf(&self) -> &str {
        &self.version_cf
    }

    pub(crate) fn put_string(&self, key: &str, value: &str) -> Result<(), RocketMQError> {
        self.put_cf_string(&self.default_cf, key, value)
    }

    pub(crate) fn put_cf_string(&self, cf: &str, key: &str, value: &str) -> Result<(), RocketMQError> {
        self.put_cf_bytes(cf, key.as_bytes(), value.as_bytes())
    }

    pub(crate) fn put_cf_bytes(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<(), RocketMQError> {
        self.create_cf_if_missing(cf)?;
        self.core.store.put_cf(cf, key, value)
    }

    pub(crate) fn get_string(&self, key: &str) -> Result<Option<String>, RocketMQError> {
        self.get_cf_string(&self.default_cf, key)
    }

    pub(crate) fn get_cf_string(&self, cf: &str, key: &str) -> Result<Option<String>, RocketMQError> {
        self.get_cf_bytes(cf, key.as_bytes())?
            .map(|bytes| {
                String::from_utf8(bytes.to_vec())
                    .map_err(|error| codec_error(format!("config utf8 decode failed: {error}")))
            })
            .transpose()
    }

    pub(crate) fn get_cf_bytes(&self, cf: &str, key: &[u8]) -> Result<Option<bytes::Bytes>, RocketMQError> {
        self.core.store.get_cf(cf, key)
    }

    pub(crate) fn delete(&self, key: &str) -> Result<(), RocketMQError> {
        self.delete_cf(&self.default_cf, key)
    }

    pub(crate) fn delete_cf(&self, cf: &str, key: &str) -> Result<(), RocketMQError> {
        self.core.store.delete_cf(cf, key.as_bytes())
    }

    pub(crate) fn load_data(&self) -> Result<ConfigRecords, RocketMQError> {
        self.load_cf_data(&self.default_cf)
    }

    pub(crate) fn default_cf_is_empty(&self) -> Result<bool, RocketMQError> {
        Ok(self.load_data()?.is_empty())
    }

    pub(crate) fn load_cf_data(&self, cf: &str) -> Result<ConfigRecords, RocketMQError> {
        self.core
            .store
            .range_scan(&RocksDbRangeScanOptions::new(cf, Vec::new(), Vec::new(), 0))?
            .into_iter()
            .map(|item| Ok((item.key.to_vec(), item.value.to_vec())))
            .collect()
    }

    pub(crate) fn load_data_version(&self) -> Result<Option<DataVersion>, RocketMQError> {
        let Some(body) = self.core.store.get_cf(&self.version_cf, KV_DATA_VERSION_KEY)? else {
            return Ok(None);
        };
        let data_version = serde_json::from_slice::<DataVersion>(&body)
            .map_err(|error| codec_error(format!("config data version decode failed: {error}")))?;
        self.kv_data_version.lock().assign_new_one(&data_version);
        Ok(Some(data_version))
    }

    pub(crate) fn update_kv_data_version(&self) -> Result<DataVersion, RocketMQError> {
        let mut data_version = self.kv_data_version.lock();
        data_version.next_version();
        let body = serde_json::to_vec(&*data_version)
            .map_err(|error| codec_error(format!("config data version encode failed: {error}")))?;
        self.put_cf_bytes(&self.version_cf, KV_DATA_VERSION_KEY, &body)?;
        Ok(data_version.clone())
    }

    pub(crate) fn set_kv_data_version(&self, data_version: DataVersion) -> Result<(), RocketMQError> {
        let body = serde_json::to_vec(&data_version)
            .map_err(|error| codec_error(format!("config data version encode failed: {error}")))?;
        self.put_cf_bytes(&self.version_cf, KV_DATA_VERSION_KEY, &body)?;
        self.kv_data_version.lock().assign_new_one(&data_version);
        Ok(())
    }

    pub(crate) fn batch_put_with_wal(&self, records: &[(Vec<u8>, Vec<u8>)]) -> Result<(), RocketMQError> {
        let mut batch = RocksDbWriteBatch::with_capacity(records.len());
        for (key, value) in records {
            batch.put_cf(self.default_cf.clone(), key.clone(), value.clone());
        }
        self.core.store.write_batch(&batch)
    }

    pub(crate) fn flush_wal(&self) -> Result<(), RocketMQError> {
        self.core.store.flush_wal(false)
    }

    pub(crate) fn flush_memtable(&self) -> Result<(), RocketMQError> {
        self.core.store.flush()
    }

    pub(crate) fn close(&self) {
        self.core.store.close();
    }

    fn create_cf_if_missing(&self, cf: &str) -> Result<(), RocketMQError> {
        self.core
            .store
            .create_cf_if_missing(config_column_family(cf, self.block_cache_size, self.write_buffer_size))
    }
}

fn build_rocksdb_config(config: &RocksDbBrokerConfigManagerConfig) -> RocksDbConfig {
    let mut column_family_names = vec![
        DEFAULT_COLUMN_FAMILY.to_string(),
        config.default_cf.clone(),
        config.version_cf.clone(),
    ];
    column_family_names.extend(config.extra_cfs.iter().cloned());
    column_family_names.sort();
    column_family_names.dedup();

    RocksDbConfig {
        enabled: true,
        path: config.path.clone(),
        wal_enabled: true,
        sync_write: false,
        write_buffer_size: config.write_buffer_size,
        block_cache_size: config.block_cache_size,
        db_write_buffer_size: Some(128 * MB),
        wal_bytes_per_sync: Some(MB_U64),
        wal_recovery_mode: RocksDbWalRecoveryMode::SkipAnyCorruptedRecord,
        stats_dump_period_sec: 600,
        max_subcompactions: 4,
        use_direct_io_for_flush_and_compaction: true,
        use_direct_reads: true,
        column_families: column_family_names
            .into_iter()
            .map(|name| config_column_family(&name, config.block_cache_size, config.write_buffer_size))
            .collect(),
        ..RocksDbConfig::default()
    }
}

fn build_shared_rocksdb_config(configs: &[RocksDbBrokerConfigManagerConfig]) -> Result<RocksDbConfig, RocketMQError> {
    let Some(first) = configs.first() else {
        return Err(RocketMQError::illegal_argument(
            "rocksdb broker config manager requires at least one config",
        ));
    };
    if configs.iter().any(|config| config.path != first.path) {
        return Err(RocketMQError::illegal_argument(
            "shared rocksdb broker config managers must use the same path",
        ));
    }

    let mut column_family_names = vec![DEFAULT_COLUMN_FAMILY.to_string()];
    for config in configs {
        column_family_names.push(config.default_cf.clone());
        column_family_names.push(config.version_cf.clone());
        column_family_names.extend(config.extra_cfs.iter().cloned());
    }
    column_family_names.sort();
    column_family_names.dedup();

    Ok(RocksDbConfig {
        enabled: true,
        path: first.path.clone(),
        wal_enabled: true,
        sync_write: false,
        write_buffer_size: first.write_buffer_size,
        block_cache_size: first.block_cache_size,
        db_write_buffer_size: Some(128 * MB),
        wal_bytes_per_sync: Some(MB_U64),
        wal_recovery_mode: RocksDbWalRecoveryMode::SkipAnyCorruptedRecord,
        stats_dump_period_sec: 600,
        max_subcompactions: 4,
        use_direct_io_for_flush_and_compaction: true,
        use_direct_reads: true,
        column_families: column_family_names
            .into_iter()
            .map(|name| config_column_family(&name, first.block_cache_size, first.write_buffer_size))
            .collect(),
        ..RocksDbConfig::default()
    })
}

fn config_column_family(name: &str, block_cache_size: usize, write_buffer_size: usize) -> RocksDbColumnFamilyConfig {
    RocksDbColumnFamilyConfig::broker_config(name, block_cache_size, write_buffer_size)
}
