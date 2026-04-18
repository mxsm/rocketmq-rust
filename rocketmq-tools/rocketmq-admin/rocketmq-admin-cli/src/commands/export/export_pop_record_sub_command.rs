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
use rocketmq_admin_core::core::export_data::ExportPopRecordRequest;
use rocketmq_admin_core::core::export_data::ExportPopRecordResult;
use rocketmq_admin_core::core::export_data::ExportService;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use crate::commands::CommonArgs;

#[derive(Debug, Clone, Parser)]
pub struct ExportPopRecordSubCommand {
    #[command(flatten)]
    common_args: CommonArgs,

    #[arg(
        short = 'c',
        long = "clusterName",
        required = false,
        conflicts_with = "broker_addr",
        help = "choose one cluster to export"
    )]
    cluster_name: Option<String>,

    #[arg(
        short = 'b',
        long = "brokerAddr",
        required = false,
        conflicts_with = "cluster_name",
        help = "choose one broker to export"
    )]
    broker_addr: Option<String>,

    #[arg(
        short = 'd',
        long = "dryRun",
        required = false,
        default_value = "false",
        help = "no actual changes will be made"
    )]
    dry_run: bool,
}

impl ExportPopRecordSubCommand {
    fn request(&self) -> RocketMQResult<ExportPopRecordRequest> {
        ExportPopRecordRequest::try_new(self.cluster_name.clone(), self.broker_addr.clone(), self.dry_run)
            .map(|request| request.with_optional_namesrv_addr(self.common_args.namesrv_addr.clone()))
    }

    fn print_result(result: &ExportPopRecordResult) {
        for target in &result.targets {
            if let Some(error) = &target.error {
                eprintln!(
                    "Export broker records error, brokerName={}, brokerAddr={}, dryRun={}\n{}",
                    target.broker_name, target.broker_addr, target.dry_run, error
                );
            } else {
                println!(
                    "Export broker records, brokerName={}, brokerAddr={}, dryRun={}",
                    target.broker_name, target.broker_addr, target.dry_run
                );
            }
        }
    }
}

impl CommandExecute for ExportPopRecordSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let result = ExportService::export_pop_records_by_request_with_rpc_hook(self.request()?, rpc_hook).await?;
        Self::print_result(&result);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocketmq_admin_core::core::export_data::ExportPopRecordTarget;

    #[test]
    fn export_pop_record_sub_command_builds_broker_request() {
        let cmd = ExportPopRecordSubCommand::try_parse_from([
            "exportPopRecord",
            "-b",
            " 127.0.0.1:10911 ",
            "-n",
            " 127.0.0.1:9876 ",
            "-d",
        ])
        .unwrap();

        let request = cmd.request().unwrap();

        assert_eq!(
            request.target(),
            &ExportPopRecordTarget::Broker("127.0.0.1:10911".into())
        );
        assert!(request.dry_run());
        assert_eq!(request.namesrv_addr(), Some("127.0.0.1:9876"));
    }

    #[test]
    fn export_pop_record_sub_command_builds_cluster_request() {
        let cmd = ExportPopRecordSubCommand::try_parse_from(["exportPopRecord", "-c", " DefaultCluster "]).unwrap();

        let request = cmd.request().unwrap();

        assert_eq!(
            request.target(),
            &ExportPopRecordTarget::Cluster("DefaultCluster".into())
        );
        assert!(!request.dry_run());
    }
}
