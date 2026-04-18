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

use std::sync::Arc;

use clap::Parser;
use rocketmq_admin_core::core::export_data::ExportMetadataInRocksDbRequest;
use rocketmq_admin_core::core::export_data::ExportMetadataInRocksDbResult;
use rocketmq_admin_core::core::export_data::ExportService;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;

#[derive(Debug, Clone, Parser)]
pub struct ExportMetadataInRocksDBSubCommand {
    #[arg(
        short = 'p',
        long = "path",
        required = true,
        help = "Absolute path for the metadata directory"
    )]
    path: String,

    #[arg(
        short = 't',
        long = "configType",
        required = true,
        help = "Name of kv config, e.g. topics/subscriptionGroups"
    )]
    config_type: String,

    #[arg(
        short = 'j',
        long = "jsonEnable",
        required = false,
        help = "Json format enable, Default: false"
    )]
    json_enable: bool,
}

impl ExportMetadataInRocksDBSubCommand {
    fn request(&self) -> ExportMetadataInRocksDbRequest {
        ExportMetadataInRocksDbRequest::new(self.path.clone(), self.config_type.clone(), self.json_enable)
    }

    fn print_result(result: &ExportMetadataInRocksDbResult) -> RocketMQResult<()> {
        match result {
            ExportMetadataInRocksDbResult::InvalidPath => {
                println!("RocksDB path is invalid.");
            }
            ExportMetadataInRocksDbResult::InvalidConfigType { config_type } => {
                println!(
                    "Invalid config type={}, Options: topics,subscriptionGroups",
                    config_type
                );
            }
            ExportMetadataInRocksDbResult::Data {
                config_type,
                json_enable,
                entries,
            } if *json_enable => {
                let mut config_table = serde_json::Map::new();

                for entry in entries {
                    match serde_json::from_str::<serde_json::Value>(&entry.value) {
                        Ok(json_object) => {
                            config_table.insert(entry.key.clone(), json_object);
                        }
                        Err(_) => {
                            config_table.insert(entry.key.clone(), serde_json::Value::String(entry.value.clone()));
                        }
                    }
                }

                let mut json_config = serde_json::Map::new();
                json_config.insert(
                    config_type.table_key().to_string(),
                    serde_json::Value::Object(config_table),
                );

                let json_config_str = serde_json::to_string_pretty(&json_config)
                    .map_err(|e| RocketMQError::Internal(format!("Failed to serialize JSON: {}", e)))?;
                println!("{}", json_config_str);
            }
            ExportMetadataInRocksDbResult::Data { entries, .. } => {
                for (index, entry) in entries.iter().enumerate() {
                    println!("{}, Key: {}, Value: {}", index + 1, entry.key, entry.value);
                }
            }
        }
        Ok(())
    }
}

impl CommandExecute for ExportMetadataInRocksDBSubCommand {
    async fn execute(&self, _rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let result = ExportService::export_metadata_in_rocksdb_by_request(&self.request())?;
        Self::print_result(&result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocketmq_admin_core::core::export_data::ExportMetadataInRocksDbConfigType;

    #[test]
    fn export_metadata_in_rocksdb_sub_command_builds_core_request() {
        let command = ExportMetadataInRocksDBSubCommand {
            path: " /tmp/metadata ".to_string(),
            config_type: " topics ".to_string(),
            json_enable: true,
        };

        let request = command.request();

        assert_eq!(request.path().to_string_lossy(), "/tmp/metadata");
        assert_eq!(request.config_type(), "topics");
        assert!(request.json_enable());
        assert_eq!(
            request.normalized_config_type(),
            Some(ExportMetadataInRocksDbConfigType::Topics)
        );
    }
}
