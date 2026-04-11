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
use clap::Parser;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_common::common::mq_version::RocketMqVersion;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use rocketmq_admin_core::admin::default_mq_admin_ext::DefaultMQAdminExt;

#[derive(Debug, Clone, Parser)]
pub struct ConsumerConnectionSubCommand {
    #[arg(short = 'g', long = "consumerGroup", required = true, help = "consumer group name")]
    consumer_group: String,

    #[arg(short = 'b', long = "brokerAddr", required = false, help = "broker address")]
    broker_addr: Option<String>,
}

impl CommandExecute for ConsumerConnectionSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> rocketmq_error::RocketMQResult<()> {
        let mut default_mq_admin_ext = if let Some(rpc_hook) = rpc_hook {
            DefaultMQAdminExt::with_rpc_hook(rpc_hook)
        } else {
            DefaultMQAdminExt::new()
        };

        default_mq_admin_ext
            .client_config_mut()
            .set_instance_name(current_millis().to_string().into());

        default_mq_admin_ext.start().await?;

        let group = self.consumer_group.trim();
        let broker_addr = self
            .broker_addr
            .as_ref()
            .map(|s| CheetahString::from_string(s.trim().to_string()));

        let cc = default_mq_admin_ext
            .examine_consumer_connection_info(group.into(), broker_addr)
            .await?;

        println!("{:<36} {:<22} {:<10} #Version", "#ClientId", "#ClientAddr", "#Language");

        let mut connections: Vec<_> = cc.get_connection_set().iter().collect();
        connections.sort_by_key(|a| a.get_client_id());
        for conn in &connections {
            let version_desc = RocketMqVersion::from_ordinal(conn.get_version() as u32).name();
            println!(
                "{:<36} {:<22} {:<10} {}",
                conn.get_client_id(),
                conn.get_client_addr(),
                conn.get_language(),
                version_desc
            );
        }

        println!("\nBelow is subscription:");
        println!("{:<20} #SubExpression", "#Topic");
        for sd in cc.get_subscription_table().values() {
            println!("{:<20} {}", sd.topic, sd.sub_string);
        }

        println!();
        if let Some(consume_type) = cc.get_consume_type() {
            let consume_type_str = match consume_type {
                rocketmq_remoting::protocol::heartbeat::consume_type::ConsumeType::ConsumeActively => {
                    "CONSUME_ACTIVELY"
                }
                rocketmq_remoting::protocol::heartbeat::consume_type::ConsumeType::ConsumePassively => {
                    "CONSUME_PASSIVELY"
                }
                rocketmq_remoting::protocol::heartbeat::consume_type::ConsumeType::ConsumePop => "CONSUME_POP",
            };
            println!("ConsumeType: {}", consume_type_str);
        }
        if let Some(message_model) = cc.get_message_model() {
            println!("MessageModel: {}", message_model);
        }
        if let Some(consume_from_where) = cc.get_consume_from_where() {
            let consume_from_where_str = serde_json::to_string(&consume_from_where)
                .unwrap_or_default()
                .trim_matches('"')
                .to_string();
            println!("ConsumeFromWhere: {}", consume_from_where_str);
        }

        default_mq_admin_ext.shutdown().await;
        Ok(())
    }
}
