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

use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::bail;
use anyhow::Result;
use config::Config;
use rocketmq_controller::ControllerConfig;
use rocketmq_controller::RaftPeer;
use rocketmq_controller::StorageEngineType;
use rocketmq_namesrv::NamesrvConfig;
use rocketmq_runtime::common::parse_config_file;
use serde::Deserialize;

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ControllerConfigOverrides {
    rocketmq_home: Option<String>,
    config_store_path: Option<PathBuf>,
    controller_type: Option<String>,
    scan_not_active_broker_interval: Option<u64>,
    controller_thread_pool_nums: Option<usize>,
    controller_request_thread_pool_queue_capacity: Option<usize>,
    mapped_file_size: Option<usize>,
    controller_store_path: Option<String>,
    elect_master_max_retry_count: Option<u32>,
    enable_elect_unclean_master: Option<bool>,
    is_process_read_event: Option<bool>,
    notify_broker_role_changed: Option<bool>,
    scan_inactive_master_interval: Option<u64>,
    config_black_list: Option<String>,
    node_id: Option<u64>,
    listen_addr: Option<SocketAddr>,
    raft_peers: Option<String>,
    controller_peers: Option<String>,
    election_timeout_ms: Option<u64>,
    heartbeat_interval_ms: Option<u64>,
    storage_path: Option<String>,
    #[serde(rename = "storageEngineType")]
    storage_engine_type: Option<String>,
    enable_elect_unclean_master_local: Option<bool>,
}

pub(super) fn load_controller_config(
    config_file: Option<PathBuf>,
    namesrv_config: &NamesrvConfig,
) -> Result<ControllerConfig> {
    let mut controller_config = ControllerConfig::default().with_rocketmq_home(namesrv_config.rocketmq_home.clone());

    if let Some(config_file) = config_file {
        let cfg = Config::builder()
            .add_source(config::File::from(config_file.as_path()))
            .build()
            .map_err(|error| {
                anyhow::anyhow!(
                    "failed to build embedded Controller configuration: {}",
                    parse_config_file::render_safe_config_error(&error)
                )
            })?;
        // Unknown fields are allowed in this shared config, so reject the removed key explicitly.
        if cfg.get::<config::Value>("storageBackend").is_ok() {
            anyhow::bail!("storageBackend has been removed; use storageEngineType");
        }
        let overrides = cfg.try_deserialize::<ControllerConfigOverrides>().map_err(|error| {
            anyhow::anyhow!(
                "failed to deserialize embedded Controller configuration: {}",
                parse_config_file::render_safe_config_error(&error)
            )
        })?;
        apply_controller_config_overrides(&mut controller_config, overrides)?;
    }

    Ok(controller_config)
}

fn apply_controller_config_overrides(
    controller_config: &mut ControllerConfig,
    overrides: ControllerConfigOverrides,
) -> Result<()> {
    if let Some(rocketmq_home) = overrides.rocketmq_home {
        controller_config.rocketmq_home = rocketmq_home;
    }
    if let Some(config_store_path) = overrides.config_store_path {
        controller_config.config_store_path = config_store_path;
    }
    if let Some(controller_type) = overrides.controller_type {
        controller_config.controller_type = controller_type;
    }
    if let Some(scan_not_active_broker_interval) = overrides.scan_not_active_broker_interval {
        controller_config.scan_not_active_broker_interval = scan_not_active_broker_interval;
    }
    if let Some(controller_thread_pool_nums) = overrides.controller_thread_pool_nums {
        controller_config.controller_thread_pool_nums = controller_thread_pool_nums;
    }
    if let Some(controller_request_thread_pool_queue_capacity) = overrides.controller_request_thread_pool_queue_capacity
    {
        controller_config.controller_request_thread_pool_queue_capacity = controller_request_thread_pool_queue_capacity;
    }
    if let Some(mapped_file_size) = overrides.mapped_file_size {
        controller_config.mapped_file_size = mapped_file_size;
    }
    if let Some(controller_store_path) = overrides.controller_store_path {
        controller_config.controller_store_path = controller_store_path;
    }
    if let Some(elect_master_max_retry_count) = overrides.elect_master_max_retry_count {
        controller_config.elect_master_max_retry_count = elect_master_max_retry_count;
    }
    if let Some(enable_elect_unclean_master) = overrides.enable_elect_unclean_master {
        controller_config.enable_elect_unclean_master = enable_elect_unclean_master;
    }
    if let Some(is_process_read_event) = overrides.is_process_read_event {
        controller_config.is_process_read_event = is_process_read_event;
    }
    if let Some(notify_broker_role_changed) = overrides.notify_broker_role_changed {
        controller_config.notify_broker_role_changed = notify_broker_role_changed;
    }
    if let Some(scan_inactive_master_interval) = overrides.scan_inactive_master_interval {
        controller_config.scan_inactive_master_interval = scan_inactive_master_interval;
    }
    if let Some(config_black_list) = overrides.config_black_list {
        controller_config.config_black_list = config_black_list;
    }
    if let Some(node_id) = overrides.node_id {
        controller_config.node_id = node_id;
    }
    if let Some(listen_addr) = overrides.listen_addr {
        controller_config.listen_addr = listen_addr;
    }
    if let Some(raft_peers) = overrides.raft_peers {
        controller_config.raft_peers = parse_raft_peers(&raft_peers)?;
    }
    if let Some(controller_peers) = overrides.controller_peers {
        controller_config.controller_peers = parse_raft_peers(&controller_peers)?;
    }
    if let Some(election_timeout_ms) = overrides.election_timeout_ms {
        controller_config.election_timeout_ms = election_timeout_ms;
    }
    if let Some(heartbeat_interval_ms) = overrides.heartbeat_interval_ms {
        controller_config.heartbeat_interval_ms = heartbeat_interval_ms;
    }
    if let Some(storage_path) = overrides.storage_path {
        controller_config.storage_path = storage_path;
    }
    if let Some(storage_engine_type) = overrides.storage_engine_type {
        controller_config.storage_engine_type = match storage_engine_type.to_ascii_lowercase().as_str() {
            "rocks_db" | "rocksdb" => StorageEngineType::RocksDB,
            "file" => StorageEngineType::File,
            "memory" => StorageEngineType::Memory,
            _ => bail!("invalid storageEngineType: {}", storage_engine_type),
        };
    }
    if let Some(enable_elect_unclean_master_local) = overrides.enable_elect_unclean_master_local {
        controller_config.enable_elect_unclean_master_local = enable_elect_unclean_master_local;
    }

    Ok(())
}

fn parse_raft_peers(value: &str) -> Result<Vec<RaftPeer>> {
    if value.trim().is_empty() {
        return Ok(Vec::new());
    }

    value
        .split(';')
        .filter(|entry| !entry.trim().is_empty())
        .map(|entry| {
            let (id, addr) = entry
                .split_once('-')
                .ok_or_else(|| anyhow::anyhow!("invalid raft peer entry: {}", entry))?;
            Ok(RaftPeer {
                id: id.parse()?,
                addr: addr.parse()?,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn controller_storage_engine_override_accepts_only_canonical_key() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("namesrv.toml");
        std::fs::write(&path, "storageEngineType = 'memory'\nunrelatedField = true").unwrap();
        let config = load_controller_config(Some(path), &NamesrvConfig::default()).unwrap();
        assert_eq!(config.storage_engine_type, StorageEngineType::Memory);
    }

    #[test]
    fn controller_storage_engine_override_rejects_removed_key() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("namesrv.toml");
        for source in [
            "storageBackend = 'Memory'",
            "storageEngineType = 'RocksDB'\nstorageBackend = 'Memory'",
            "storageBackend = 'private-value'",
            "storageEngineType = 'Memory'\nstorageBackend = 'private-value'",
        ] {
            std::fs::write(&path, source).unwrap();
            let error = load_controller_config(Some(path.clone()), &NamesrvConfig::default()).unwrap_err();
            assert_eq!(
                error.to_string(),
                "storageBackend has been removed; use storageEngineType"
            );
            assert!(!format!("{error:?}").contains("private-value"));
        }
    }

    #[test]
    fn controller_storage_engine_override_rejects_invalid_canonical_value() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("namesrv.toml");
        std::fs::write(&path, "storageEngineType = 'unknown_engine'").unwrap();
        let error = load_controller_config(Some(path), &NamesrvConfig::default()).unwrap_err();
        assert!(error.to_string().contains("invalid storageEngineType"));
    }
}
