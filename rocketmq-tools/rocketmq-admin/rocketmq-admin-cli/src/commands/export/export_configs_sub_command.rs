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
use rocketmq_admin_core::core::export_data::ExportConfigsRequest;
use rocketmq_admin_core::core::export_data::ExportConfigsResult;
use rocketmq_admin_core::core::export_data::ExportService;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

const DEFAULT_FILE_PATH: &str = "/tmp/rocketmq/export";

use crate::commands::CommandExecute;
use crate::commands::CommonArgs;

#[derive(Debug, Clone, Parser)]
pub struct ExportConfigsSubCommand {
    #[command(flatten)]
    common_args: CommonArgs,

    #[arg(
        short = 'c',
        long = "clusterName",
        required = true,
        help = "choose a cluster to export"
    )]
    cluster_name: String,

    #[arg(
        short = 'f',
        long = "filePath",
        required = false,
        default_value = DEFAULT_FILE_PATH,
        help = "export configs.json path | default /tmp/rocketmq/export"
    )]
    file_path: String,
}

impl ExportConfigsSubCommand {
    fn request(&self) -> RocketMQResult<ExportConfigsRequest> {
        ExportConfigsRequest::try_new(self.cluster_name.clone())
            .map(|request| request.with_optional_namesrv_addr(self.common_args.namesrv_addr.clone()))
    }
}

impl CommandExecute for ExportConfigsSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let file_path = self.file_path.trim();
        let result = ExportService::export_configs_by_request_with_rpc_hook(self.request()?, rpc_hook).await?;

        Self::write_result(&result, file_path)
    }
}

impl ExportConfigsSubCommand {
    fn write_result(export_result: &ExportConfigsResult, file_path: &str) -> RocketMQResult<()> {
        let mut result = serde_json::Map::new();

        let mut cluster_scale_map = serde_json::Map::new();
        cluster_scale_map.insert(
            "namesrvSize".to_string(),
            serde_json::Value::Number(serde_json::Number::from(export_result.name_servers.len() as i64)),
        );
        cluster_scale_map.insert(
            "masterBrokerSize".to_string(),
            serde_json::Value::Number(serde_json::Number::from(export_result.master_broker_size)),
        );
        cluster_scale_map.insert(
            "slaveBrokerSize".to_string(),
            serde_json::Value::Number(serde_json::Number::from(export_result.slave_broker_size)),
        );

        result.insert(
            "brokerConfigs".to_string(),
            serde_json::to_value(&export_result.broker_configs).map_err(|e| {
                RocketMQError::Internal(format!(
                    "ExportConfigsSubCommand: Failed to serialize broker configs: {}",
                    e
                ))
            })?,
        );
        result.insert("clusterScale".to_string(), serde_json::Value::Object(cluster_scale_map));

        let path = format!("{}/configs.json", file_path);
        let json_content = serde_json::to_string_pretty(&result).map_err(|e| {
            RocketMQError::Internal(format!(
                "ExportConfigsSubCommand: Failed to serialize export result: {}",
                e
            ))
        })?;

        rocketmq_common::FileUtils::string_to_file(&json_content, &path)?;
        println!("export {} success", path);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn export_configs_sub_command_builds_core_request() {
        let cmd = ExportConfigsSubCommand::try_parse_from([
            "exportConfigs",
            "-c",
            " DefaultCluster ",
            "-n",
            " 127.0.0.1:9876 ",
            "-f",
            " C:/tmp/export ",
        ])
        .unwrap();

        let request = cmd.request().unwrap();

        assert_eq!(request.cluster_name().as_str(), "DefaultCluster");
        assert_eq!(request.namesrv_addr(), Some("127.0.0.1:9876"));
        assert_eq!(cmd.file_path.trim(), "C:/tmp/export");
    }
}
