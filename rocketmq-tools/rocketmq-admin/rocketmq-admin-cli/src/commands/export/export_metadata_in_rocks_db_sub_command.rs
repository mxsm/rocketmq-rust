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

use clap::Parser;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;
use rocksdb::DB;
use rocksdb::Options;

use crate::commands::CommandExecute;

const TOPICS_JSON_CONFIG: &str = "topics";
const SUBSCRIPTION_GROUP_JSON_CONFIG: &str = "subscriptionGroups";

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
    fn handle_export_metadata(db: &DB, config_type: &str, json_enable: bool) -> RocketMQResult<()> {
        if json_enable {
            let mut config_table = serde_json::Map::new();

            Self::iterate_kv_store(db, |key, value| {
                let config_key = String::from_utf8_lossy(key).to_string();
                let config_value = String::from_utf8_lossy(value).to_string();
                match serde_json::from_str::<serde_json::Value>(&config_value) {
                    Ok(json_object) => {
                        config_table.insert(config_key, json_object);
                    }
                    Err(_) => {
                        config_table.insert(config_key, serde_json::Value::String(config_value));
                    }
                }
            })?;

            let table_key = if config_type.eq_ignore_ascii_case(TOPICS_JSON_CONFIG) {
                "topicConfigTable"
            } else {
                "subscriptionGroupTable"
            };

            let mut json_config = serde_json::Map::new();
            json_config.insert(table_key.to_string(), serde_json::Value::Object(config_table));

            let json_config_str = serde_json::to_string_pretty(&json_config)
                .map_err(|e| RocketMQError::Internal(format!("Failed to serialize JSON: {}", e)))?;
            println!("{}", json_config_str);
        } else {
            let mut count: u64 = 0;
            Self::iterate_kv_store(db, |key, value| {
                count += 1;
                let config_key = String::from_utf8_lossy(key);
                let config_value = String::from_utf8_lossy(value);
                println!("{}, Key: {}, Value: {}", count, config_key, config_value);
            })?;
        }
        Ok(())
    }

    fn iterate_kv_store<F>(db: &DB, mut consumer: F) -> RocketMQResult<()>
    where
        F: FnMut(&[u8], &[u8]),
    {
        let iter = db.iterator(rocksdb::IteratorMode::Start);
        for item in iter {
            match item {
                Ok((key, value)) => {
                    consumer(&key, &value);
                }
                Err(e) => {
                    return Err(RocketMQError::Internal(format!("RocksDB iterator error: {}", e)));
                }
            }
        }
        Ok(())
    }
}

impl CommandExecute for ExportMetadataInRocksDBSubCommand {
    async fn execute(&self, _rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let path = self.path.trim();
        if path.is_empty() || !std::path::Path::new(path).exists() {
            println!("RocksDB path is invalid.");
            return Ok(());
        }

        let config_type = self.config_type.trim();
        let full_path = PathBuf::from(path).join(config_type);

        if !config_type.eq_ignore_ascii_case(TOPICS_JSON_CONFIG)
            && !config_type.eq_ignore_ascii_case(SUBSCRIPTION_GROUP_JSON_CONFIG)
        {
            println!(
                "Invalid config type={}, Options: topics,subscriptionGroups",
                config_type
            );
            return Ok(());
        }

        let mut opts = Options::default();
        opts.create_if_missing(false);

        let db = DB::open_for_read_only(&opts, &full_path, false).map_err(|e| {
            println!("RocksDB load error, path={}", full_path.display());
            RocketMQError::Internal(format!("Failed to open RocksDB: {}", e))
        })?;

        let result = Self::handle_export_metadata(&db, config_type, self.json_enable);
        drop(db);
        result
    }
}
