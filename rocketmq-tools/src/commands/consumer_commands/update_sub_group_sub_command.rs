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

use std::collections::HashMap;
use std::sync::Arc;

use cheetah_string::CheetahString;
use clap::Parser;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_common::common::attribute::attribute_parser::AttributeParser;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::subscription::group_retry_policy::GroupRetryPolicy;
use rocketmq_remoting::runtime::RPCHook;

use crate::admin::default_mq_admin_ext::DefaultMQAdminExt;
use crate::commands::command_util::CommandUtil;
use crate::commands::CommandExecute;

#[derive(Debug, Clone, Parser)]
pub struct UpdateSubGroupSubCommand {
    #[arg(
        short = 'b',
        long = "brokerAddr",
        required = false,
        help = "create subscription group to which broker"
    )]
    broker_addr: Option<String>,
    #[arg(
        short = 'c',
        long = "clusterName",
        required = false,
        help = "create subscription group to which cluster"
    )]
    cluster_name: Option<String>,
    #[arg(short = 'g', long = "groupName", required = true, help = "consumer group name")]
    group_name: String,

    #[arg(short = 's', long = "consumeEnable", required = false, help = "consume enable")]
    consume_enable: Option<bool>,

    #[arg(
        short = 'm',
        long = "consumeFromMinEnable",
        required = false,
        help = "from min offset"
    )]
    consume_from_min_enable: Option<bool>,

    #[arg(short = 'd', long = "consumeBroadcastEnable", required = false, help = "broadcast")]
    consume_broadcast_enable: Option<bool>,

    #[arg(
        short = 'o',
        long = "consumeMessageOrderly",
        required = false,
        help = "consume message orderly"
    )]
    consume_message_orderly: Option<bool>,

    #[arg(short = 'q', long = "retryQueueNums", required = false, help = "retry queue nums")]
    retry_queue_nums: Option<i32>,

    #[arg(short = 'r', long = "retryMaxTimes", required = false, help = "retry max times")]
    retry_max_times: Option<i32>,

    #[arg(
        short = 'p',
        long = "groupRetryPolicy",
        required = false,
        help = "a json string for retry policy ( e.g. \
                {\"type\":\"EXPONENTIAL\",\"exponentialRetryPolicy\":{\"initial\":5000,\"max\":7200000,\"multiplier\":\
                2}} or {\"type\":\"CUSTOMIZED\",\"customizedRetryPolicy\":{\"next\":[1000,5000,10000]}} )"
    )]
    group_retry_policy: Option<String>,

    #[arg(
        short = 'i',
        long = "brokerId",
        required = false,
        help = "consumer from which broker id"
    )]
    broker_id: Option<u64>,

    #[arg(
        short = 'w',
        long = "whichBrokerWhenConsumeSlowly",
        required = false,
        help = "which broker id when consume slowly"
    )]
    which_broker_when_consume_slowly: Option<u64>,

    #[arg(
        short = 'a',
        long = "notifyConsumerIdsChanged",
        required = false,
        help = "notify consumerId changed"
    )]
    notify_consumer_ids_changed: Option<bool>,

    #[arg(long = "attributes", required = false, help = "attribute(+a=b,+c=d,-e)")]
    attributes: Option<String>,
}

impl CommandExecute for UpdateSubGroupSubCommand {
    async fn execute(&self, _rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        if (self.broker_addr.is_none() && self.cluster_name.is_none())
            || (self.broker_addr.is_some() && self.cluster_name.is_some())
        {
            return Err(RocketMQError::IllegalArgument(
                "UpdateSubGroupSubCommand: Invalid arguments: specify exactly one of --brokerAddr (-b) or \
                 --clusterName (-c)."
                    .into(),
            ));
        }

        let mut default_mqadmin_ext = DefaultMQAdminExt::new();
        default_mqadmin_ext
            .client_config_mut()
            .set_instance_name(get_current_millis().to_string().into());

        let operation_result = async {
            use rocketmq_remoting::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;

            let mut subscription_group_config = SubscriptionGroupConfig::new(self.group_name.as_str().into());

            if let Some(consume_enable) = self.consume_enable {
                subscription_group_config.set_consume_enable(consume_enable);
            }
            if let Some(consume_from_min_enable) = self.consume_from_min_enable {
                subscription_group_config.set_consume_from_min_enable(consume_from_min_enable);
            }
            if let Some(consume_broadcast_enable) = self.consume_broadcast_enable {
                subscription_group_config.set_consume_broadcast_enable(consume_broadcast_enable);
            }
            if let Some(consume_message_orderly) = self.consume_message_orderly {
                subscription_group_config.set_consume_message_orderly(consume_message_orderly);
            }

            if let Some(retry_queue_nums) = self.retry_queue_nums {
                subscription_group_config.set_retry_queue_nums(retry_queue_nums);
            }
            if let Some(retry_max_times) = self.retry_max_times {
                subscription_group_config.set_retry_max_times(retry_max_times);
            }
            if let Some(ref group_retry_policy) = self.group_retry_policy {
                match serde_json::from_str::<GroupRetryPolicy>(group_retry_policy.as_str()) {
                    Ok(value) => subscription_group_config.set_group_retry_policy(value),
                    Err(e) => {
                        return Err(RocketMQError::Internal(format!(
                            "UpdateSubGroupSubCommand: Failed to parse groupRetryPolicy: {}",
                            e
                        )))
                    }
                }
            }

            if let Some(broker_id) = self.broker_id {
                subscription_group_config.set_broker_id(broker_id);
            }
            if let Some(which_broker_when_consume_slowly) = self.which_broker_when_consume_slowly {
                subscription_group_config.set_which_broker_when_consume_slowly(which_broker_when_consume_slowly);
            }

            if let Some(notify_consumer_ids_changed) = self.notify_consumer_ids_changed {
                subscription_group_config.set_notify_consumer_ids_changed_enable(notify_consumer_ids_changed);
            }

            if let Some(ref attributes) = self.attributes {
                match AttributeParser::parse_to_map(attributes.as_str()) {
                    Ok(attributes) => {
                        let attributes_modification = attributes
                            .into_iter()
                            .map(|(k, v)| (k.into(), v.into()))
                            .collect::<HashMap<CheetahString, CheetahString>>();
                        subscription_group_config.set_attributes(attributes_modification);
                    }
                    Err(e) => {
                        return Err(RocketMQError::Internal(format!(
                            "UpdateSubGroupSubCommand: Failed to parse attributes: {}: {}",
                            attributes, e
                        )))
                    }
                }
            }

            if let Some(ref broker_addr) = self.broker_addr {
                MQAdminExt::start(&mut default_mqadmin_ext).await.map_err(|e| {
                    RocketMQError::Internal(format!("UpdateSubGroupSubCommand: Failed to start MQAdminExt: {}", e))
                })?;

                match default_mqadmin_ext
                    .create_and_update_subscription_group_config(
                        broker_addr.as_str().into(),
                        subscription_group_config.clone(),
                    )
                    .await
                {
                    Ok(_) => {
                        println!("Create subscription group to {} success.", broker_addr);
                        println!("{:#?}", subscription_group_config);
                    }
                    Err(e) => {
                        return Err(RocketMQError::Internal(format!(
                            "UpdateSubGroupSubCommand: Failed to create or update subscription group config: {}",
                            e
                        )))
                    }
                }
            } else if let Some(ref cluster_name) = self.cluster_name {
                MQAdminExt::start(&mut default_mqadmin_ext).await.map_err(|e| {
                    RocketMQError::Internal(format!("UpdateSubGroupSubCommand: Failed to start MQAdminExt: {}", e))
                })?;

                let cluster_info = default_mqadmin_ext.examine_broker_cluster_info().await?;
                match CommandUtil::fetch_master_addr_by_cluster_name(&cluster_info, cluster_name.as_str()) {
                    Ok(addresses) => {
                        for addr in addresses {
                            if let Err(e) = default_mqadmin_ext
                                .create_and_update_subscription_group_config(
                                    addr.clone(),
                                    subscription_group_config.clone(),
                                )
                                .await
                            {
                                eprintln!(
                                    "UpdateSubGroupSubCommand: Failed to create or update subscription group config \
                                     for broker with address {}: {}",
                                    addr, e
                                )
                            } else {
                                println!("Create subscription group to {} success.", addr);
                                println!("{:#?}", subscription_group_config);
                            }
                        }
                    }
                    Err(e) => {
                        return Err(RocketMQError::Internal(format!(
                            "UpdateSubGroupSubCommand: Failed to create or update subscription group config: {}",
                            e
                        )));
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

#[cfg(test)]
mod tests {
    use crate::commands::consumer_commands::update_sub_group_sub_command::UpdateSubGroupSubCommand;

    use clap::Parser;

    #[test]
    fn test_create_subscription_configuration_with_short_commands() {
        let args = vec![
            vec![""],
            vec!["-b", "127.0.0.1:3434"],
            vec!["-c", "some-cluster-name"],
            vec!["-g", "some-group-name"],
            vec!["-s", "true"],
            vec!["-m", "true"],
            vec!["-d", "true"],
            vec!["-o", "true"],
            vec!["-q", "10"],
            vec!["-r", "20"],
            vec![
                "-p",
                "{\"type\":\"CUSTOMIZED\",\"customizedRetryPolicy\":{\"next\":[1000,5000,10000]}}",
            ],
            vec!["-i", "2"],
            vec!["-w", "3"],
            vec!["-a", "true"],
            vec!["--attributes", "+a=b,+c=d,-e"],
        ];

        let args = args.concat();

        let cmd = UpdateSubGroupSubCommand::try_parse_from(args).unwrap();
        assert_eq!(Some("127.0.0.1:3434"), cmd.broker_addr.as_deref());
        assert_eq!(Some("some-cluster-name"), cmd.cluster_name.as_deref());
        assert_eq!("some-group-name", cmd.group_name.as_str());
        assert_eq!(Some(true), cmd.consume_enable);
        assert_eq!(Some(true), cmd.consume_from_min_enable);
        assert_eq!(Some(true), cmd.consume_broadcast_enable);
        assert_eq!(Some(true), cmd.consume_message_orderly);
        assert_eq!(Some(10), cmd.retry_queue_nums);
        assert_eq!(Some(20), cmd.retry_max_times);
        assert_eq!(
            Some("{\"type\":\"CUSTOMIZED\",\"customizedRetryPolicy\":{\"next\":[1000,5000,10000]}}"),
            cmd.group_retry_policy.as_deref()
        );
        assert_eq!(Some(2), cmd.broker_id);
        assert_eq!(Some(3), cmd.which_broker_when_consume_slowly);
        assert_eq!(Some(true), cmd.notify_consumer_ids_changed);
        assert_eq!(Some("+a=b,+c=d,-e"), cmd.attributes.as_deref());
    }

    #[test]
    fn test_create_subscription_configuration_with_long_commands() {
        let args = vec![
            vec![""],
            vec!["--brokerAddr", "127.0.0.1:3455"],
            vec!["--clusterName", "some-cluster-name-2"],
            vec!["--groupName", "some-group-name-2"],
            vec!["--consumeEnable", "false"],
            vec!["--consumeFromMinEnable", "false"],
            vec!["--consumeBroadcastEnable", "false"],
            vec!["--consumeMessageOrderly", "false"],
            vec!["--retryQueueNums", "10"],
            vec!["--retryMaxTimes", "20"],
            vec![
                "--groupRetryPolicy",
                "{\"type\":\"CUSTOMIZED\",\"customizedRetryPolicy\":{\"next\":[1000,5000,10000]}}",
            ],
            vec!["--brokerId", "2"],
            vec!["--whichBrokerWhenConsumeSlowly", "3"],
            vec!["--notifyConsumerIdsChanged", "true"],
            vec!["--attributes", "+a=b,+c=d,-e"],
        ];

        let args = args.concat();

        let cmd = UpdateSubGroupSubCommand::try_parse_from(args).unwrap();
        assert_eq!(Some("127.0.0.1:3455"), cmd.broker_addr.as_deref());
        assert_eq!(Some("some-cluster-name-2"), cmd.cluster_name.as_deref());
        assert_eq!("some-group-name-2", cmd.group_name.as_str());
        assert_eq!(Some(false), cmd.consume_enable);
        assert_eq!(Some(false), cmd.consume_from_min_enable);
        assert_eq!(Some(false), cmd.consume_broadcast_enable);
        assert_eq!(Some(false), cmd.consume_message_orderly);
        assert_eq!(Some(10), cmd.retry_queue_nums);
        assert_eq!(Some(20), cmd.retry_max_times);
        assert_eq!(
            Some("{\"type\":\"CUSTOMIZED\",\"customizedRetryPolicy\":{\"next\":[1000,5000,10000]}}"),
            cmd.group_retry_policy.as_deref()
        );
        assert_eq!(Some(2), cmd.broker_id);
        assert_eq!(Some(3), cmd.which_broker_when_consume_slowly);
        assert_eq!(Some(true), cmd.notify_consumer_ids_changed);
        assert_eq!(Some("+a=b,+c=d,-e"), cmd.attributes.as_deref());
    }
}
