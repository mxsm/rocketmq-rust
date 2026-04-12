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
use rocketmq_remoting::protocol::body::get_broker_lite_info_response_body::GetBrokerLiteInfoResponseBody;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use rocketmq_admin_core::core::lite::BrokerLiteInfoQueryRequest;
use rocketmq_admin_core::core::lite::BrokerLiteInfoQueryResult;
use rocketmq_admin_core::core::lite::LiteService;

#[derive(Debug, Clone, Parser)]
#[command(group(
    ArgGroup::new("target")
        .required(true)
        .args(&["broker_addr", "cluster_name"])
))]
pub struct GetBrokerLiteInfoSubCommand {
    #[arg(short = 'b', long = "brokerAddr", help = "Broker address")]
    broker_addr: Option<String>,

    #[arg(short = 'c', long = "clusterName", help = "Cluster name")]
    cluster_name: Option<String>,

    #[arg(short = 'd', long = "showDetail", help = "Show topic and group detail info")]
    show_detail: bool,
}

impl CommandExecute for GetBrokerLiteInfoSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let result = LiteService::query_broker_lite_info_by_request_with_rpc_hook(self.request()?, rpc_hook).await?;
        Self::print_result(&result, self.show_detail);
        Ok(())
    }
}

impl GetBrokerLiteInfoSubCommand {
    fn request(&self) -> RocketMQResult<BrokerLiteInfoQueryRequest> {
        BrokerLiteInfoQueryRequest::try_new(self.broker_addr.clone(), self.cluster_name.clone())
    }

    fn print_result(result: &BrokerLiteInfoQueryResult, show_detail: bool) {
        Self::print_header();
        for entry in &result.entries {
            if let Some(body) = &entry.body {
                Self::print_row(body, entry.broker_addr.as_str(), show_detail);
            } else {
                println!("[{}] error.", entry.broker_addr);
            }
        }
    }

    fn print_header() {
        println!(
            "{:<30} {:<17} {:<10} {:<14} {:<20} {:<17} {:<15} {:<18} {:<15}",
            "#Broker",
            "#Store Type",
            "#Max LMQ",
            "#Current LMQ",
            "#SubscriptionCount",
            "#OrderInfoCount",
            "#CQTableSize",
            "#OffsetTableSize",
            "#eventMapSize"
        );
    }

    fn print_row(response_body: &GetBrokerLiteInfoResponseBody, broker_addr: &str, show_detail: bool) {
        let store_type = response_body.get_store_type().map(|s| s.as_str()).unwrap_or("");

        println!(
            "{:<30} {:<17} {:<10} {:<14} {:<20} {:<17} {:<15} {:<18} {:<15}",
            broker_addr,
            store_type,
            response_body.get_max_lmq_num(),
            response_body.get_current_lmq_num(),
            response_body.get_lite_subscription_count(),
            response_body.get_order_info_count(),
            response_body.get_cq_table_size(),
            response_body.get_offset_table_size(),
            response_body.get_event_map_size()
        );

        // If show_detail enabled, print Topic Meta and Group Meta on new lines
        if show_detail {
            println!(
                "Topic Meta: {}",
                serde_json::to_string(response_body.get_topic_meta()).unwrap_or_default()
            );
            println!(
                "Group Meta: {}\n",
                serde_json::to_string(response_body.get_group_meta()).unwrap_or_default()
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocketmq_admin_core::core::lite::BrokerLiteInfoTarget;

    #[test]
    fn get_broker_lite_info_sub_command_builds_broker_request() {
        let cmd =
            GetBrokerLiteInfoSubCommand::try_parse_from(["getBrokerLiteInfo", "-b", " 127.0.0.1:10911 "]).unwrap();

        assert_eq!(
            cmd.request().unwrap().target(),
            &BrokerLiteInfoTarget::Broker("127.0.0.1:10911".into())
        );
    }

    #[test]
    fn get_broker_lite_info_sub_command_builds_cluster_request() {
        let cmd =
            GetBrokerLiteInfoSubCommand::try_parse_from(["getBrokerLiteInfo", "-c", " DefaultCluster ", "-d"]).unwrap();

        assert!(cmd.show_detail);
        assert_eq!(
            cmd.request().unwrap().target(),
            &BrokerLiteInfoTarget::Cluster("DefaultCluster".into())
        );
    }
}
