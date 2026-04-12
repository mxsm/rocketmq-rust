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
use rocketmq_admin_core::core::broker::BrokerBooleanOperationResult;
use rocketmq_admin_core::core::broker::BrokerOptionalTarget;
use rocketmq_admin_core::core::broker::BrokerService;

#[derive(Debug, Clone, Parser)]
#[command(group(ArgGroup::new("target")
    .required(false)
    .args(&["broker_addr", "cluster_name"]))
)]
pub struct CleanUnusedTopicSubCommand {
    #[arg(short = 'b', long = "brokerAddr", required = false, help = "Broker address")]
    broker_addr: Option<String>,

    #[arg(short = 'c', long = "cluster", required = false, help = "Cluster name")]
    cluster_name: Option<String>,
}

impl CleanUnusedTopicSubCommand {
    fn request(&self) -> RocketMQResult<BrokerOptionalTarget> {
        BrokerOptionalTarget::new(self.broker_addr.clone(), self.cluster_name.clone())
    }
}

impl CommandExecute for CleanUnusedTopicSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let result = BrokerService::clean_unused_topic_by_request_with_rpc_hook(self.request()?, rpc_hook).await?;
        print_result(&result);
        Ok(())
    }
}

fn print_result(result: &BrokerBooleanOperationResult) {
    if result.success {
        println!("success");
    } else {
        println!("false");
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;

    #[test]
    fn parses_clean_unused_topic_optional_target() {
        let cmd = CleanUnusedTopicSubCommand::try_parse_from(["cleanUnusedTopic", "-c", " DefaultCluster "]).unwrap();
        let request = cmd.request().unwrap();

        assert!(request.broker_addr().is_none());
        assert_eq!(request.cluster_name().unwrap().as_str(), "DefaultCluster");
    }
}
