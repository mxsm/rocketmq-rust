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

use std::cmp::Ordering;
use std::collections::HashMap;

use cheetah_string::CheetahString;
use clap::Parser;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::RemotingSerializable;
use rocketmq_remoting::protocol::route::topic_route_data::TopicRouteData;

use crate::commands::CommandExecute;
use crate::commands::CommonArgs;
use rocketmq_admin_core::core::ToolsError;
use rocketmq_admin_core::core::topic::TopicRouteQueryRequest;
use rocketmq_admin_core::core::topic::TopicService;

#[derive(Debug, Clone, Parser)]
pub struct TopicRouteSubCommand {
    #[command(flatten)]
    common_args: CommonArgs,

    #[arg(short = 't', long = "topic", required = true, help = "topic name")]
    topic: String,

    #[arg(short = 'l', long = "list format", required = false, help = "list format")]
    list_format: Option<bool>,
}
impl TopicRouteSubCommand {
    fn broker_name_compare(a: &CheetahString, b: &CheetahString) -> Ordering {
        if a > b {
            return std::cmp::Ordering::Greater;
        } else if a < b {
            return std::cmp::Ordering::Less;
        }
        std::cmp::Ordering::Equal
    }
    fn print_data(&self, topic_route_data: &TopicRouteData, use_list_format: bool) -> RocketMQResult<()> {
        if !use_list_format {
            println!("{}", topic_route_data.serialize_json()?);
            return Ok(());
        }

        let (mut total_read_queue, mut total_write_queue) = (0, 0);
        let mut queue_data_list = topic_route_data.queue_datas.clone();
        let mut map = HashMap::new();
        for queue_data in &queue_data_list {
            map.insert(queue_data.broker_name().clone(), queue_data.clone())
                .unwrap();
        }
        queue_data_list.sort_by(|a, b| TopicRouteSubCommand::broker_name_compare(a.broker_name(), b.broker_name()));

        let mut broker_data_list = topic_route_data.broker_datas.clone();
        broker_data_list.sort_by(|a, b| TopicRouteSubCommand::broker_name_compare(a.broker_name(), b.broker_name()));

        println!("#ClusterName #BrokerName #BrokerAddrs #ReadQueue #WriteQueue #Perm");

        for broker_data in &broker_data_list {
            let broker_name = broker_data.broker_name();
            let queue_data = map.get(broker_name).unwrap();
            total_read_queue += queue_data.read_queue_nums();
            total_write_queue += queue_data.write_queue_nums();
            println!(
                "{} {} {:?} {} {} {}",
                broker_data.cluster(),
                broker_name,
                broker_data.broker_addrs(),
                queue_data.read_queue_nums(),
                queue_data.write_queue_nums(),
                queue_data.perm()
            );
        }

        for _i in 0..158 {
            print!("-");
        }
        println!("Total: {} {} {}", map.len(), total_read_queue, total_write_queue,);
        Ok(())
    }
}
impl CommandExecute for TopicRouteSubCommand {
    async fn execute(
        &self,
        _rpc_hook: Option<std::sync::Arc<dyn rocketmq_remoting::runtime::RPCHook>>,
    ) -> rocketmq_error::RocketMQResult<()> {
        let request = TopicRouteQueryRequest::try_new(self.topic.clone())?
            .with_optional_namesrv_addr(self.common_args.namesrv_addr.clone());
        let topic_route_data = TopicService::query_topic_route(request)
            .await?
            .ok_or_else(|| ToolsError::topic_not_found(self.topic.trim()))?;
        self.print_data(&topic_route_data, self.list_format.is_some())?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topic_route_sub_command_parse() {
        let cmd =
            TopicRouteSubCommand::try_parse_from(["topicRoute", "-t", "TestTopic", "-n", "127.0.0.1:9876"]).unwrap();

        assert_eq!(cmd.topic, "TestTopic");
        assert_eq!(cmd.common_args.namesrv_addr, Some("127.0.0.1:9876".to_string()));
    }

    #[test]
    fn topic_route_sub_command_missing_topic() {
        let cmd = TopicRouteSubCommand::try_parse_from(["topicRoute"]);

        assert!(cmd.is_err());
    }
}
