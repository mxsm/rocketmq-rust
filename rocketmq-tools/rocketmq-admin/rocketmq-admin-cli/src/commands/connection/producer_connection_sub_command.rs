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
use rocketmq_admin_core::core::connection::ConnectionService;
use rocketmq_admin_core::core::connection::ProducerConnectionQueryRequest;
use rocketmq_admin_core::core::connection::ProducerConnectionQueryResult;
use rocketmq_common::common::mq_version::RocketMqVersion;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::body::producer_connection::ProducerConnection;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;

#[derive(Debug, Clone, Parser)]
pub struct ProducerConnectionSubCommand {
    #[arg(short = 'g', long = "producerGroup", required = true, help = "producer group name")]
    producer_group: String,

    #[arg(short = 't', long = "topic", required = true, help = "topic name")]
    topic: String,
}

impl CommandExecute for ProducerConnectionSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let request = ProducerConnectionQueryRequest::try_new(self.producer_group.clone(), self.topic.clone())?;
        let result = ConnectionService::query_producer_connection_by_request_with_rpc_hook(request, rpc_hook).await?;
        render_producer_connection_result(result);
        Ok(())
    }
}

fn render_producer_connection_result(result: ProducerConnectionQueryResult) {
    let pc = result.connection;
    print_producer_connection(&pc);
}

fn print_producer_connection(pc: &ProducerConnection) {
    let mut connections: Vec<_> = pc.connection_set().iter().collect();
    connections.sort_by_key(|a| a.get_client_id());
    for (idx, conn) in connections.iter().enumerate() {
        let version_desc = RocketMqVersion::from_ordinal(conn.get_version() as u32).name();
        println!(
            "{:04}  {:<32} {:<22} {:<8} {}",
            idx + 1,
            conn.get_client_id(),
            conn.get_client_addr(),
            conn.get_language(),
            version_desc
        );
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use crate::commands::connection::producer_connection_sub_command::ProducerConnectionSubCommand;

    #[test]
    fn producer_connection_sub_command_parse() {
        let command =
            ProducerConnectionSubCommand::try_parse_from(["", "-g", "producer-a", "-t", "TopicTest"]).unwrap();

        assert_eq!(command.producer_group, "producer-a");
        assert_eq!(command.topic, "TopicTest");
    }
}
