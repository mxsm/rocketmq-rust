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

use cheetah_string::CheetahString;
use clap::ArgGroup;
use clap::Parser;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::body::get_broker_lite_info_response_body::GetBrokerLiteInfoResponseBody;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use crate::commands::command_util::CommandUtil;
use rocketmq_admin_core::admin::default_mq_admin_ext::DefaultMQAdminExt;

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
        let mut default_mqadmin_ext = if let Some(rpc_hook) = rpc_hook {
            DefaultMQAdminExt::with_rpc_hook(rpc_hook)
        } else {
            DefaultMQAdminExt::new()
        };

        default_mqadmin_ext
            .client_config_mut()
            .set_instance_name(current_millis().to_string().into());

        let operation_result = async {
            MQAdminExt::start(&mut default_mqadmin_ext).await.map_err(|e| {
                RocketMQError::Internal(format!(
                    "GetBrokerLiteInfoSubCommand: Failed to start MQAdminExt: {}",
                    e
                ))
            })?;

            let show_detail = self.show_detail;
            Self::print_header();

            if let Some(broker_addr) = &self.broker_addr {
                let broker_addr = broker_addr.trim();
                let response_body = default_mqadmin_ext
                    .get_broker_lite_info(CheetahString::from(broker_addr))
                    .await?;
                Self::print_row(&response_body, broker_addr, show_detail);
            } else if let Some(cluster_name) = &self.cluster_name {
                let cluster_name = cluster_name.trim();
                let cluster_info = default_mqadmin_ext.examine_broker_cluster_info().await.map_err(|e| {
                    RocketMQError::Internal(format!(
                        "GetBrokerLiteInfoSubCommand: Failed to examine broker cluster info: {}",
                        e
                    ))
                })?;

                let master_set = CommandUtil::fetch_master_addr_by_cluster_name(&cluster_info, cluster_name)?;

                for broker_addr in &master_set {
                    match default_mqadmin_ext
                        .get_broker_lite_info(CheetahString::from(broker_addr.as_str()))
                        .await
                    {
                        Ok(response_body) => {
                            Self::print_row(&response_body, broker_addr, show_detail);
                        }
                        Err(_) => {
                            println!("[{}] error.", broker_addr);
                        }
                    }
                }
            }

            Ok(())
        }
        .await;

        MQAdminExt::shutdown(&mut default_mqadmin_ext).await;
        operation_result
    }
}

impl GetBrokerLiteInfoSubCommand {
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
