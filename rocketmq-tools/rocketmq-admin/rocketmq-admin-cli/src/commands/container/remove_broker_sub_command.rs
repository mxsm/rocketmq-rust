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
use rocketmq_admin_core::core::container::ContainerRemoveBrokerRequest;
use rocketmq_admin_core::core::container::ContainerService;
use rocketmq_error::RocketMQResult;
use rocketmq_error::ToolsError;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use crate::commands::CommonArgs;

#[derive(Debug, Clone, Parser)]
pub struct RemoveBrokerSubCommand {
    #[command(flatten)]
    common_args: CommonArgs,

    #[arg(
        short = 'c',
        long = "brokerContainerAddr",
        required = true,
        help = "Broker container address"
    )]
    broker_container_addr: String,

    #[arg(
        short = 'b',
        long = "brokerIdentity",
        required = true,
        help = "Information to identify a broker: clusterName:brokerName:brokerId"
    )]
    broker_identity: String,
}

impl CommandExecute for RemoveBrokerSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let result = ContainerService::remove_broker_by_request_with_rpc_hook(self.request()?, rpc_hook).await?;
        println!("remove broker from {} success", result.broker_container_addr);
        Ok(())
    }
}

impl RemoveBrokerSubCommand {
    fn request(&self) -> RocketMQResult<ContainerRemoveBrokerRequest> {
        let (cluster_name, broker_name, broker_id) = parse_broker_identity(&self.broker_identity)?;
        ContainerRemoveBrokerRequest::try_new(self.broker_container_addr.clone(), cluster_name, broker_name, broker_id)
            .map(|request| request.with_optional_namesrv_addr(self.common_args.namesrv_addr.clone()))
    }
}

fn parse_broker_identity(value: &str) -> RocketMQResult<(String, String, i64)> {
    let parts = value.split(':').map(str::trim).collect::<Vec<_>>();
    if parts.len() != 3 || parts.iter().any(|part| part.is_empty()) {
        return Err(ToolsError::validation_error(
            "brokerIdentity",
            "brokerIdentity must be formatted as clusterName:brokerName:brokerId",
        )
        .into());
    }

    let broker_id = parts[2].parse::<i64>().map_err(|_| {
        ToolsError::validation_error(
            "brokerIdentity",
            format!("brokerIdentity contains invalid brokerId: {}", parts[2]),
        )
    })?;

    Ok((parts[0].to_string(), parts[1].to_string(), broker_id))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn remove_broker_sub_command_builds_core_request() {
        let cmd = RemoveBrokerSubCommand::try_parse_from([
            "removeBroker",
            "-c",
            " 127.0.0.1:10911 ",
            "-b",
            " DefaultCluster : broker-a : 1 ",
            "-n",
            " 127.0.0.1:9876 ",
        ])
        .unwrap();

        let request = cmd.request().unwrap();

        assert_eq!(request.broker_container_addr().as_str(), "127.0.0.1:10911");
        assert_eq!(request.cluster_name().as_str(), "DefaultCluster");
        assert_eq!(request.broker_name().as_str(), "broker-a");
        assert_eq!(request.broker_id(), 1);
        assert_eq!(request.namesrv_addr(), Some("127.0.0.1:9876"));
    }

    #[test]
    fn remove_broker_sub_command_rejects_invalid_identity() {
        let cmd =
            RemoveBrokerSubCommand::try_parse_from(["removeBroker", "-c", "127.0.0.1:10911", "-b", "cluster:broker"])
                .unwrap();
        assert!(cmd.request().is_err());

        let cmd = RemoveBrokerSubCommand::try_parse_from([
            "removeBroker",
            "-c",
            "127.0.0.1:10911",
            "-b",
            "cluster:broker:bad",
        ])
        .unwrap();
        assert!(cmd.request().is_err());
    }

    #[test]
    fn remove_broker_sub_command_rejects_negative_broker_id() {
        let cmd = RemoveBrokerSubCommand::try_parse_from([
            "removeBroker",
            "-c",
            "127.0.0.1:10911",
            "-b",
            "cluster:broker:-1",
        ])
        .unwrap();
        assert!(cmd.request().is_err());
    }
}
