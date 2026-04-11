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
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use rocketmq_admin_core::core::broker::BrokerRuntimeStatsQueryRequest;
use rocketmq_admin_core::core::broker::BrokerRuntimeStatsResult;
use rocketmq_admin_core::core::broker::BrokerService;
use rocketmq_admin_core::core::broker::BrokerTarget;

#[derive(Debug, Clone, Parser)]
#[command(group(
    clap::ArgGroup::new("target")
        .required(true)
        .args(&["broker_addr", "cluster_name"])
))]
pub struct BrokerStatusSubCommand {
    #[arg(short = 'b', long = "brokerAddr", help = "Broker address")]
    broker_addr: Option<String>,

    #[arg(short = 'c', long = "clusterName", help = "which cluster")]
    cluster_name: Option<String>,
}

impl BrokerStatusSubCommand {
    fn request(&self) -> RocketMQResult<BrokerRuntimeStatsQueryRequest> {
        BrokerRuntimeStatsQueryRequest::try_new(self.broker_addr.clone(), self.cluster_name.clone())
    }
}

impl CommandExecute for BrokerStatusSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let request = self.request()?;
        let print_broker = matches!(request.target(), BrokerTarget::ClusterName(_));
        let result = BrokerService::query_broker_runtime_stats_by_request_with_rpc_hook(request, rpc_hook).await?;
        print_runtime_stats_result(&result, print_broker);
        Ok(())
    }
}

fn print_runtime_stats_result(result: &BrokerRuntimeStatsResult, print_broker: bool) {
    for section in &result.sections {
        for entry in &section.entries {
            if print_broker {
                println!("{:<24} {:<32}: {}", section.broker_addr, entry.key, entry.value);
            } else {
                println!("{:<32}: {}", entry.key, entry.value);
            }
        }
    }

    for failure in &result.failures {
        eprintln!(
            "BrokerStatusSubCommand: Failed to fetch runtime stats from {}: {}",
            failure.broker_addr, failure.error
        );
    }
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;

    use super::*;

    #[test]
    fn parses_broker_addr_into_runtime_stats_request() {
        let cmd = BrokerStatusSubCommand::try_parse_from(["brokerStatus", "-b", " 127.0.0.1:10911 "]).unwrap();

        let request = cmd.request().unwrap();

        assert_eq!(
            request.target(),
            &BrokerTarget::BrokerAddr(CheetahString::from("127.0.0.1:10911"))
        );
    }

    #[test]
    fn parses_cluster_name_into_runtime_stats_request() {
        let cmd = BrokerStatusSubCommand::try_parse_from(["brokerStatus", "-c", " DefaultCluster "]).unwrap();

        let request = cmd.request().unwrap();

        assert_eq!(
            request.target(),
            &BrokerTarget::ClusterName(CheetahString::from("DefaultCluster"))
        );
    }

    #[test]
    fn rejects_missing_or_ambiguous_runtime_stats_target() {
        assert!(BrokerStatusSubCommand::try_parse_from(["brokerStatus"]).is_err());
        assert!(
            BrokerStatusSubCommand::try_parse_from(["brokerStatus", "-b", "127.0.0.1:10911", "-c", "DefaultCluster"])
                .is_err()
        );
    }
}
