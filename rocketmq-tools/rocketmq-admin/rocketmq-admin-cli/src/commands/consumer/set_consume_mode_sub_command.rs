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
use rocketmq_common::common::message::message_enum::MessageRequestMode;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use crate::commands::CommonArgs;
use rocketmq_admin_core::core::consumer::ConsumerOperationResult;
use rocketmq_admin_core::core::consumer::ConsumerService;
use rocketmq_admin_core::core::consumer::SetConsumeModeRequest;

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

impl SetConsumeModeSubCommand {
    fn request(&self) -> RocketMQResult<SetConsumeModeRequest> {
        SetConsumeModeRequest::try_new(
            self.broker_addr.clone(),
            self.cluster_name.clone(),
            self.topic_name.clone(),
            self.group_name.clone(),
            self.mode,
            self.pop_share_queue_num,
        )
        .map(|request| request.with_optional_namesrv_addr(self.common_args.namesrv_addr.clone()))
    }

    fn print_result(request: &SetConsumeModeRequest, result: ConsumerOperationResult) -> RocketMQResult<()> {
        for broker_addr in &result.broker_addrs {
            println!("set consume mode to {} success.", broker_addr);
        }
        for failure in &result.failures {
            eprintln!("{}", failure.error);
        }
        validate_cluster_consume_mode_success(result.broker_addrs.len())?;
        println!(
            "topic[{}] group[{}] consume mode[{}] popShareQueueNum[{}]",
            request.topic_name(),
            request.group_name(),
            request.mode().get_name(),
            request.pop_share_queue_num()
        );
        Ok(())
    }
}

impl CommandExecute for SetConsumeModeSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let request = self.request()?;
        let result = ConsumerService::set_consume_mode_by_request_with_rpc_hook(request.clone(), rpc_hook).await?;
        Self::print_result(&request, result)
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
    fn test_parse_consume_mode_pop_lowercase() {
        let result = parse_consume_mode("pop");
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
    fn test_validate_cluster_consume_mode_success_when_all_brokers_fail() {
        let result = validate_cluster_consume_mode_success(0);
        assert!(result.is_err());
    }

    #[test]
    fn parses_set_consume_mode_request() {
        let command = SetConsumeModeSubCommand::try_parse_from([
            "setConsumeMode",
            "-b",
            " 127.0.0.1:10911 ",
            "-t",
            " TestTopic ",
            "-g",
            " GroupA ",
            "-m",
            "POP",
            "-q",
            "8",
            "-n",
            " 127.0.0.1:9876 ",
        ])
        .unwrap();
        let request = command.request().unwrap();

        assert_eq!(request.topic_name().as_str(), "TestTopic");
        assert_eq!(request.group_name().as_str(), "GroupA");
        assert_eq!(request.mode(), MessageRequestMode::Pop);
        assert_eq!(request.pop_share_queue_num(), 8);
    }
}
