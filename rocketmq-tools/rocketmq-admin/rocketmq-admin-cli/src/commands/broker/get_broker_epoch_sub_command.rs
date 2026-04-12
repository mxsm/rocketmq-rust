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

use clap::ArgGroup;
use clap::Parser;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use rocketmq_admin_core::core::broker::BrokerEpochQueryRequest;
use rocketmq_admin_core::core::broker::BrokerEpochQueryResult;
use rocketmq_admin_core::core::broker::BrokerService;

#[derive(Debug, Clone, Parser)]
#[command(group(
    ArgGroup::new("target")
        .required(true)
        .args(&["broker_name", "cluster_name"])
))]
pub struct GetBrokerEpochSubCommand {
    #[arg(short = 'b', long = "brokerName", help = "which broker to get epoch")]
    broker_name: Option<String>,

    #[arg(short = 'c', long = "clusterName", help = "which cluster to get epoch")]
    cluster_name: Option<String>,

    #[arg(
        short = 'i',
        long = "interval",
        required = false,
        help = "the interval(second) of get info"
    )]
    interval: Option<u64>,
}

impl GetBrokerEpochSubCommand {
    fn request(&self) -> RocketMQResult<BrokerEpochQueryRequest> {
        BrokerEpochQueryRequest::try_new(self.broker_name.clone(), self.cluster_name.clone())
    }

    fn print_result(result: BrokerEpochQueryResult) {
        for section in result.sections {
            println!(
                "\n#clusterName\t{}\n#brokerName\t{}\n#brokerAddr\t{}\n#brokerId\t{}",
                section.cluster_name, section.broker_name, section.broker_addr, section.broker_id
            );

            for epoch in section.epochs {
                println!(
                    "#Epoch: EpochEntry{{epoch={}, startOffset={}, endOffset={}}}",
                    epoch.epoch, epoch.start_offset, epoch.end_offset
                );
            }
        }
    }
}

impl CommandExecute for GetBrokerEpochSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let request = self.request()?;
        if let Some(interval) = self.interval {
            let flush_second = if interval > 0 { interval } else { 3 };
            loop {
                match BrokerService::query_broker_epoch_by_request_with_rpc_hook(request.clone(), rpc_hook.clone())
                    .await
                {
                    Ok(result) => Self::print_result(result),
                    Err(error) => eprintln!("GetBrokerEpochSubCommand: error: {}", error),
                }
                tokio::time::sleep(tokio::time::Duration::from_secs(flush_second)).await;
            }
        } else {
            let result = BrokerService::query_broker_epoch_by_request_with_rpc_hook(request, rpc_hook).await?;
            Self::print_result(result);
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;
    use rocketmq_admin_core::core::broker::BrokerEpochQueryTarget;

    #[test]
    fn parses_get_broker_epoch_request() {
        let command = GetBrokerEpochSubCommand::try_parse_from(["getBrokerEpoch", "-b", " broker-a "]).unwrap();
        let request = command.request().unwrap();

        assert!(matches!(
            request.target(),
            BrokerEpochQueryTarget::BrokerName(broker_name) if broker_name.as_str() == "broker-a"
        ));
    }
}
