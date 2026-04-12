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
use rocketmq_remoting::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use rocketmq_admin_core::core::consumer::ConsumerConfigQueryRequest;
use rocketmq_admin_core::core::consumer::ConsumerService;

#[derive(Debug, Clone, Parser)]
pub struct GetConsumerConfigSubCommand {
    #[arg(short = 'g', long = "groupName", required = true, help = "subscription group name")]
    group_name: String,
}

impl GetConsumerConfigSubCommand {
    fn request(&self) -> RocketMQResult<ConsumerConfigQueryRequest> {
        ConsumerConfigQueryRequest::try_new(self.group_name.clone())
    }
}

impl CommandExecute for GetConsumerConfigSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let result = ConsumerService::query_consumer_config_by_request_with_rpc_hook(self.request()?, rpc_hook).await?;
        for info in &result.entries {
            println!(
                "============================={}:{}=============================",
                info.cluster_name, info.broker_name
            );
            print_config(&info.subscription_group_config);
        }
        Ok(())
    }
}

fn print_config(config: &SubscriptionGroupConfig) {
    print_field("groupName", config.group_name());
    print_field("consumeEnable", &config.consume_enable());
    print_field("consumeFromMinEnable", &config.consume_from_min_enable());
    print_field("consumeMessageOrderly", &config.consume_message_orderly());
    print_field("consumeBroadcastEnable", &config.consume_broadcast_enable());
    print_field("retryQueueNums", &config.retry_queue_nums());
    print_field("retryMaxTimes", &config.retry_max_times());
    print_field("brokerId", &config.broker_id());
    print_field(
        "whichBrokerWhenConsumeSlowly",
        &config.which_broker_when_consume_slowly(),
    );
    print_field(
        "notifyConsumerIdsChangedEnable",
        &config.notify_consumer_ids_changed_enable(),
    );
    print_field("groupSysFlag", &config.group_sys_flag());
    print_field("consumeTimeoutMinute", &config.consume_timeout_minute());
    println!();
}

fn print_field(name: &str, value: &dyn std::fmt::Display) {
    println!("  {:<40}=  {}", name, value);
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;

    #[test]
    fn parses_get_consumer_config_request() {
        let command = GetConsumerConfigSubCommand::try_parse_from(["getConsumerConfig", "-g", " GroupA "]).unwrap();
        let request = command.request().unwrap();

        assert_eq!(request.group_name().as_str(), "GroupA");
    }
}
