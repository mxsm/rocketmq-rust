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
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_common::common::message::message_enum::MessageRequestMode;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::admin::default_mq_admin_ext::DefaultMQAdminExt;
use crate::commands::command_util::CommandUtil;
use crate::commands::CommandExecute;
use crate::commands::CommonArgs;

#[derive(Debug, Clone, Parser)]
pub struct SetConsumeModeSubCommand {
    #[command(flatten)]
    common_args: CommonArgs,

    #[arg(
        short = 'b',
        long = "brokerAddr",
        required = false,
        conflicts_with = "cluster_name",
        help = "Set consume mode on which broker"
    )]
    broker_addr: Option<String>,

    #[arg(
        short = 'c',
        long = "clusterName",
        required = false,
        conflicts_with = "broker_addr",
        help = "Set consume mode on which cluster"
    )]
    cluster_name: Option<String>,

    #[arg(short = 't', long = "topicName", required = true, help = "Topic name")]
    topic_name: String,

    #[arg(short = 'g', long = "groupName", required = true, help = "Consumer group name")]
    group_name: String,

    #[arg(
        short = 'm',
        long = "mode",
        required = true,
        value_parser = parse_consume_mode,
        help = "Consume mode: PULL or POP"
    )]
    mode: MessageRequestMode,

    #[arg(
        short = 'q',
        long = "popShareQueueNum",
        required = false,
        help = "Number of queue which share in pop mode"
    )]
    pop_share_queue_num: Option<i32>,
}

fn parse_consume_mode(s: &str) -> Result<MessageRequestMode, String> {
    match s.trim().to_uppercase().as_str() {
        "PULL" => Ok(MessageRequestMode::Pull),
        "POP" => Ok(MessageRequestMode::Pop),
        _ => Err(format!("Invalid consume mode: {}. Must be PULL or POP", s.trim())),
    }
}

fn validate_cluster_consume_mode_success(success_count: usize) -> RocketMQResult<()> {
    if success_count == 0 {
        Err(RocketMQError::Internal(
            "SetConsumeModeSubCommand: Failed to set consume mode on all brokers in cluster".into(),
        ))
    } else {
        Ok(())
    }
}

impl CommandExecute for SetConsumeModeSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let topic_name = self.topic_name.trim();
        let group_name = self.group_name.trim();
        let mode = self.mode;
        let pop_share_queue_num = self.pop_share_queue_num.unwrap_or(0);

        let mut admin_ext = if let Some(rpc_hook) = rpc_hook {
            DefaultMQAdminExt::with_rpc_hook(rpc_hook)
        } else {
            DefaultMQAdminExt::new()
        };
        admin_ext
            .client_config_mut()
            .set_instance_name(get_current_millis().to_string().into());

        if let Some(addr) = &self.common_args.namesrv_addr {
            admin_ext.set_namesrv_addr(addr.trim());
        }

        admin_ext.start().await.map_err(|e| {
            RocketMQError::Internal(format!("SetConsumeModeSubCommand: Failed to start MQAdminExt: {}", e))
        })?;

        let result = if let Some(broker_addr) = &self.broker_addr {
            let addr = broker_addr.trim();
            admin_ext
                .set_message_request_mode(
                    addr.into(),
                    topic_name.into(),
                    group_name.into(),
                    mode,
                    pop_share_queue_num,
                    5000,
                )
                .await?;
            println!("set consume mode to {} success.", addr);
            println!(
                "topic[{}] group[{}] consume mode[{}] popShareQueueNum[{}]",
                topic_name,
                group_name,
                mode.get_name(),
                pop_share_queue_num
            );
            Ok(())
        } else if let Some(cluster_name) = &self.cluster_name {
            let cluster_name = cluster_name.trim();
            let cluster_info = admin_ext.examine_broker_cluster_info().await?;
            let master_set = CommandUtil::fetch_master_addr_by_cluster_name(&cluster_info, cluster_name)?;

            let mut success_count = 0;
            for addr in &master_set {
                match admin_ext
                    .set_message_request_mode(
                        addr.clone(),
                        topic_name.into(),
                        group_name.into(),
                        mode,
                        pop_share_queue_num,
                        5000,
                    )
                    .await
                {
                    Ok(_) => {
                        println!("set consume mode to {} success.", addr);
                        success_count += 1;
                    }
                    Err(e) => {
                        eprintln!("{}", e);
                        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                    }
                }
            }
            validate_cluster_consume_mode_success(success_count)?;
            println!(
                "topic[{}] group[{}] consume mode[{}] popShareQueueNum[{}]",
                topic_name,
                group_name,
                mode.get_name(),
                pop_share_queue_num
            );
            Ok(())
        } else {
            Err(RocketMQError::IllegalArgument(
                "SetConsumeModeSubCommand: Either brokerAddr (-b) or clusterName (-c) must be provided".into(),
            ))
        };

        admin_ext.shutdown().await;
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_request_mode_get_name() {
        assert_eq!(MessageRequestMode::Pull.get_name(), "PULL");
        assert_eq!(MessageRequestMode::Pop.get_name(), "POP");
    }

    #[test]
    fn test_parse_consume_mode_pull_uppercase() {
        let result = parse_consume_mode("PULL");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), MessageRequestMode::Pull);
    }

    #[test]
    fn test_parse_consume_mode_pull_lowercase() {
        let result = parse_consume_mode("pull");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), MessageRequestMode::Pull);
    }

    #[test]
    fn test_parse_consume_mode_pull_mixed_case() {
        let result = parse_consume_mode("PuLl");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), MessageRequestMode::Pull);
    }

    #[test]
    fn test_parse_consume_mode_pop_uppercase() {
        let result = parse_consume_mode("POP");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), MessageRequestMode::Pop);
    }

    #[test]
    fn test_parse_consume_mode_pop_lowercase() {
        let result = parse_consume_mode("pop");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), MessageRequestMode::Pop);
    }

    #[test]
    fn test_parse_consume_mode_pop_mixed_case() {
        let result = parse_consume_mode("PoP");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), MessageRequestMode::Pop);
    }

    #[test]
    fn test_parse_consume_mode_invalid() {
        let result = parse_consume_mode("INVALID");
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            "Invalid consume mode: INVALID. Must be PULL or POP"
        );
    }

    #[test]
    fn test_parse_consume_mode_empty() {
        let result = parse_consume_mode("");
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Invalid consume mode: . Must be PULL or POP");
    }

    #[test]
    fn test_parse_consume_mode_numeric() {
        let result = parse_consume_mode("123");
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Invalid consume mode: 123. Must be PULL or POP");
    }

    #[test]
    fn test_parse_consume_mode_with_whitespace() {
        let result = parse_consume_mode(" PULL ");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), MessageRequestMode::Pull);
    }

    #[test]
    fn test_parse_consume_mode_special_chars() {
        let result = parse_consume_mode("PUL@L");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_consume_mode_push_mode_not_supported() {
        let result = parse_consume_mode("PUSH");
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Invalid consume mode: PUSH. Must be PULL or POP");
    }

    #[test]
    fn test_validate_cluster_consume_mode_success_when_all_brokers_fail() {
        let result = validate_cluster_consume_mode_success(0);
        assert!(result.is_err());
        match result {
            Err(RocketMQError::Internal(message)) => {
                assert_eq!(
                    message,
                    "SetConsumeModeSubCommand: Failed to set consume mode on all brokers in cluster"
                );
            }
            _ => panic!("Expected RocketMQError::Internal"),
        }
    }

    #[test]
    fn test_validate_cluster_consume_mode_success_when_at_least_one_broker_succeeds() {
        assert!(validate_cluster_consume_mode_success(1).is_ok());
        assert!(validate_cluster_consume_mode_success(3).is_ok());
    }
}
