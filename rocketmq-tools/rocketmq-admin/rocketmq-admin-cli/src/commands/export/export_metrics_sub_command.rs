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

use std::path::Path;
use std::sync::Arc;

use clap::Parser;
use rocketmq_admin_core::core::export_data::ExportMetricsRequest;
use rocketmq_admin_core::core::export_data::ExportService;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use crate::commands::CommonArgs;

const DEFAULT_FILE_PATH: &str = "/tmp/rocketmq/export";

#[derive(Debug, Clone, Parser)]
pub struct ExportMetricsSubCommand {
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
        help = "export metrics.json path | default /tmp/rocketmq/export"
    )]
    file_path: String,
}

impl CommandExecute for ExportMetricsSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let result = ExportService::export_metrics_by_request_with_rpc_hook(self.request()?, rpc_hook).await?;
        let output_path = self.output_path();
        let json_content = serde_json::to_string_pretty(&result)
            .map_err(|error| RocketMQError::Internal(format!("ExportMetricsSubCommand: {error}")))?;
        rocketmq_common::FileUtils::string_to_file(&json_content, &output_path)?;
        println!("export {} success", output_path);
        Ok(())
    }
}

impl ExportMetricsSubCommand {
    fn request(&self) -> RocketMQResult<ExportMetricsRequest> {
        ExportMetricsRequest::try_new(self.cluster_name.clone())
            .map(|request| request.with_optional_namesrv_addr(self.common_args.namesrv_addr.clone()))
    }

    fn output_path(&self) -> String {
        Path::new(self.file_path.trim())
            .join("metrics.json")
            .to_string_lossy()
            .to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn export_metrics_sub_command_builds_core_request() {
        let cmd = ExportMetricsSubCommand::try_parse_from([
            "exportMetrics",
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
        assert!(
            cmd.output_path()
                .replace('\\', "/")
                .ends_with("C:/tmp/export/metrics.json")
        );
    }

    #[test]
    fn export_metrics_sub_command_uses_java_default_path() {
        let cmd = ExportMetricsSubCommand::try_parse_from(["exportMetrics", "-c", "DefaultCluster"]).unwrap();

        assert_eq!(
            cmd.output_path().replace('\\', "/"),
            "/tmp/rocketmq/export/metrics.json"
        );
    }
}
