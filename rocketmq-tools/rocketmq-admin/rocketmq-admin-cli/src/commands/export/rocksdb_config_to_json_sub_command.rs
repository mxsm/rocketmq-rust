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

use clap::Parser;
use rocketmq_admin_core::core::export_data::ExportFileOverwritePolicy;
use rocketmq_admin_core::core::export_data::ExportFileWriteRequest;
use rocketmq_admin_core::core::export_data::ExportMetadataInRocksDbConfigType;
use rocketmq_admin_core::core::export_data::ExportMetadataInRocksDbRequest;
use rocketmq_admin_core::core::export_data::ExportMetadataInRocksDbResult;
use rocketmq_admin_core::core::export_data::ExportRocksDbConfigRpcRequest;
use rocketmq_admin_core::core::export_data::ExportRocksDbConfigRpcResult;
use rocketmq_admin_core::core::export_data::ExportService;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_error::ToolsError;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;

const ALL_ROCKSDB_CONFIG_TYPES: &str = "topics;subscriptionGroups;consumerOffsets";

#[derive(Debug, Clone, Parser)]
pub struct RocksDBConfigToJsonSubCommand {
    #[arg(
        short = 't',
        long = "configType",
        required = false,
        help = "Name of kv config, e.g. topics/subscriptionGroups/consumerOffsets. Required in local mode and default \
                all in rpc mode."
    )]
    config_type: Option<String>,

    #[arg(
        short = 'p',
        long = "configPath",
        required = false,
        help = "[local mode] Absolute path to the metadata config directory"
    )]
    config_path: Option<String>,

    #[arg(
        short = 'e',
        long = "exportFile",
        required = false,
        help = "[local mode] Absolute file path for exporting. If provided, jsonEnable is ignored."
    )]
    export_file: Option<String>,

    #[arg(
        short = 'j',
        long = "jsonEnable",
        required = false,
        default_value_t = true,
        action = clap::ArgAction::Set,
        help = "[local mode] Json format enable, Default: true"
    )]
    json_enable: bool,

    #[arg(
        short = 'n',
        long = "nameserverAddr",
        visible_alias = "namesrvAddr",
        required = false,
        help = "[rpc mode] nameserverAddr. If nameserverAddr and cluster are provided, cluster mode is used."
    )]
    nameserver_addr: Option<String>,

    #[arg(short = 'c', long = "cluster", required = false, help = "[rpc mode] Cluster name")]
    cluster: Option<String>,

    #[arg(
        short = 'b',
        long = "brokerAddr",
        required = false,
        help = "[rpc mode] Broker address"
    )]
    broker_addr: Option<String>,
}

#[derive(Debug, Clone)]
enum RocksDBConfigToJsonMode {
    Local {
        request: ExportMetadataInRocksDbRequest,
        export_file: Option<String>,
    },
    Rpc(ExportRocksDbConfigRpcRequest),
}

impl CommandExecute for RocksDBConfigToJsonSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        match self.mode()? {
            RocksDBConfigToJsonMode::Local { request, export_file } => {
                println!("Use [local mode] load rocksdb to print or export file");
                let result = ExportService::export_metadata_in_rocksdb_by_request(&request)?;
                Self::handle_local_result(&result, export_file.as_deref())
            }
            RocksDBConfigToJsonMode::Rpc(request) => {
                println!("Use [rpc mode] call broker(s) to export to json file");
                let result =
                    ExportService::export_rocksdb_config_rpc_by_request_with_rpc_hook(request, rpc_hook).await?;
                Self::print_rpc_result(&result);
                Ok(())
            }
        }
    }
}

impl RocksDBConfigToJsonSubCommand {
    fn mode(&self) -> RocketMQResult<RocksDBConfigToJsonMode> {
        if let Some(nameserver_addr) = trim_optional(&self.nameserver_addr) {
            let cluster = trim_optional(&self.cluster).ok_or_else(|| {
                ToolsError::validation_error("cluster", "rpc mode with nameserverAddr requires cluster")
            })?;
            let request = ExportRocksDbConfigRpcRequest::try_new(Some(cluster), None, self.rpc_config_types(), None)?
                .with_optional_namesrv_addr(Some(nameserver_addr));
            return Ok(RocksDBConfigToJsonMode::Rpc(request));
        }

        if let Some(broker_addr) = trim_optional(&self.broker_addr) {
            let request =
                ExportRocksDbConfigRpcRequest::try_new(None, Some(broker_addr), self.rpc_config_types(), None)?;
            return Ok(RocksDBConfigToJsonMode::Rpc(request));
        }

        if let Some(config_path) = trim_optional(&self.config_path) {
            let config_type = trim_optional(&self.config_type)
                .ok_or_else(|| ToolsError::validation_error("configType", "local mode requires configType"))?;
            return Ok(RocksDBConfigToJsonMode::Local {
                request: ExportMetadataInRocksDbRequest::new(config_path, config_type, self.json_enable),
                export_file: trim_optional(&self.export_file),
            });
        }

        Err(ToolsError::validation_error("mode", "provide nameserverAddr+cluster, brokerAddr, or configPath").into())
    }

    fn rpc_config_types(&self) -> String {
        trim_optional(&self.config_type).unwrap_or_else(|| ALL_ROCKSDB_CONFIG_TYPES.to_string())
    }

    fn handle_local_result(result: &ExportMetadataInRocksDbResult, export_file: Option<&str>) -> RocketMQResult<()> {
        if let Some(export_file) = export_file
            && let Some(json_value) = Self::local_result_json_value(result)?
        {
            let request = ExportFileWriteRequest::try_new(export_file, ExportFileOverwritePolicy::Overwrite)?;
            let write_result = ExportService::write_json_export_file(&request, &json_value)?;
            println!("export {} success", write_result.output_path().display());
            return Ok(());
        }

        Self::print_local_result(result)
    }

    fn print_local_result(result: &ExportMetadataInRocksDbResult) -> RocketMQResult<()> {
        match result {
            ExportMetadataInRocksDbResult::InvalidPath => {
                println!("Rocksdb path is invalid.");
            }
            ExportMetadataInRocksDbResult::InvalidConfigType { config_type } => {
                println!(
                    "Invalid configType: {} please input topics/subscriptionGroups/consumerOffsets",
                    config_type
                );
            }
            ExportMetadataInRocksDbResult::Data {
                config_type,
                json_enable,
                entries,
            } if *json_enable => {
                let json_value = Self::entries_json_value(*config_type, entries)?;
                let json = serde_json::to_string_pretty(&json_value)
                    .map_err(|error| RocketMQError::Internal(format!("RocksDBConfigToJsonSubCommand: {error}")))?;
                println!("{json}");
            }
            ExportMetadataInRocksDbResult::Data {
                config_type, entries, ..
            } => {
                println!("type: {}", config_type.type_name());
                for (index, entry) in entries.iter().enumerate() {
                    println!("{}, Key: {}, Value: {}", index + 1, entry.key, entry.value);
                }
            }
        }
        Ok(())
    }

    fn local_result_json_value(result: &ExportMetadataInRocksDbResult) -> RocketMQResult<Option<serde_json::Value>> {
        match result {
            ExportMetadataInRocksDbResult::Data {
                config_type, entries, ..
            } => Self::entries_json_value(*config_type, entries).map(Some),
            ExportMetadataInRocksDbResult::InvalidPath | ExportMetadataInRocksDbResult::InvalidConfigType { .. } => {
                Self::print_local_result(result)?;
                Ok(None)
            }
        }
    }

    fn entries_json_value(
        config_type: ExportMetadataInRocksDbConfigType,
        entries: &[rocketmq_admin_core::core::export_data::ExportMetadataInRocksDbEntry],
    ) -> RocketMQResult<serde_json::Value> {
        let mut config_table = serde_json::Map::new();
        for entry in entries {
            let value = serde_json::from_str::<serde_json::Value>(&entry.value)
                .unwrap_or_else(|_| serde_json::Value::String(entry.value.clone()));
            config_table.insert(entry.key.clone(), value);
        }

        let mut root = serde_json::Map::new();
        root.insert(
            config_type.table_key().to_string(),
            serde_json::Value::Object(config_table),
        );
        Ok(serde_json::Value::Object(root))
    }

    fn print_rpc_result(result: &ExportRocksDbConfigRpcResult) {
        for target in &result.targets {
            if let Some(error) = &target.error {
                eprintln!(
                    "{} export error: {}",
                    if target.broker_name.is_empty() {
                        target.broker_addr.as_str()
                    } else {
                        target.broker_name.as_str()
                    },
                    error
                );
            } else {
                println!(
                    "{} export done.",
                    if target.broker_name.is_empty() {
                        target.broker_addr.as_str()
                    } else {
                        target.broker_name.as_str()
                    }
                );
            }
        }
        println!("broker export done.");
    }
}

fn trim_optional(value: &Option<String>) -> Option<String> {
    value
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocketmq_admin_core::core::export_data::ExportRocksDbConfigRpcTarget;

    #[test]
    fn rocksdb_config_to_json_builds_local_request_with_export_file() {
        let cmd = RocksDBConfigToJsonSubCommand::try_parse_from([
            "rocksDBConfigToJson",
            "-p",
            " /tmp/metadata ",
            "-t",
            " topics ",
            "-e",
            " /tmp/topics.json ",
            "-j",
            "false",
        ])
        .unwrap();

        match cmd.mode().unwrap() {
            RocksDBConfigToJsonMode::Local { request, export_file } => {
                assert_eq!(request.path().to_string_lossy(), "/tmp/metadata");
                assert_eq!(request.config_type(), "topics");
                assert!(!request.json_enable());
                assert_eq!(export_file.as_deref(), Some("/tmp/topics.json"));
            }
            RocksDBConfigToJsonMode::Rpc(_) => panic!("expected local mode"),
        }
    }

    #[test]
    fn rocksdb_config_to_json_local_mode_requires_config_type() {
        let cmd =
            RocksDBConfigToJsonSubCommand::try_parse_from(["rocksDBConfigToJson", "-p", "/tmp/metadata"]).unwrap();

        assert!(cmd.mode().is_err());
    }

    #[test]
    fn rocksdb_config_to_json_builds_rpc_cluster_request_and_defaults_all_types() {
        let cmd = RocksDBConfigToJsonSubCommand::try_parse_from([
            "rocksDBConfigToJson",
            "-n",
            " 127.0.0.1:9876 ",
            "-c",
            " DefaultCluster ",
            "-b",
            " 127.0.0.1:10911 ",
            "-p",
            " /tmp/metadata ",
        ])
        .unwrap();

        match cmd.mode().unwrap() {
            RocksDBConfigToJsonMode::Rpc(request) => {
                assert_eq!(request.namesrv_addr(), Some("127.0.0.1:9876"));
                assert_eq!(
                    request.target(),
                    &ExportRocksDbConfigRpcTarget::Cluster("DefaultCluster".into())
                );
                assert_eq!(
                    request.config_types(),
                    &[
                        ExportMetadataInRocksDbConfigType::Topics,
                        ExportMetadataInRocksDbConfigType::SubscriptionGroups,
                        ExportMetadataInRocksDbConfigType::ConsumerOffsets,
                    ]
                );
            }
            RocksDBConfigToJsonMode::Local { .. } => panic!("expected rpc mode"),
        }
    }

    #[test]
    fn rocksdb_config_to_json_builds_rpc_broker_request() {
        let cmd = RocksDBConfigToJsonSubCommand::try_parse_from([
            "rocksDBConfigToJson",
            "-b",
            " 127.0.0.1:10911 ",
            "-t",
            " topics;consumerOffsets ",
        ])
        .unwrap();

        match cmd.mode().unwrap() {
            RocksDBConfigToJsonMode::Rpc(request) => {
                assert_eq!(
                    request.target(),
                    &ExportRocksDbConfigRpcTarget::Broker("127.0.0.1:10911".into())
                );
                assert_eq!(
                    request.config_types(),
                    &[
                        ExportMetadataInRocksDbConfigType::Topics,
                        ExportMetadataInRocksDbConfigType::ConsumerOffsets,
                    ]
                );
            }
            RocksDBConfigToJsonMode::Local { .. } => panic!("expected rpc mode"),
        }
    }

    #[test]
    fn rocksdb_config_to_json_rejects_missing_mode() {
        let cmd = RocksDBConfigToJsonSubCommand::try_parse_from(["rocksDBConfigToJson"]).unwrap();

        assert!(cmd.mode().is_err());
    }
}
