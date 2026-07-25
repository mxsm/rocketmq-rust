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

use clap::Parser;
use rocketmq_error::RocketMQResult;

use crate::commands::CommandExecute;
use rocketmq_admin_core::client_adapter::services::offset::ConsumerStatusQueryRequest;
use rocketmq_admin_core::client_adapter::services::offset::ConsumerStatusResult;
use rocketmq_admin_core::client_adapter::services::offset::OffsetService;

#[derive(Debug, Clone, Parser)]
pub struct GetConsumerStatusSubCommand {
    #[arg(short = 'g', long = "group", required = true, help = "set the consumer group")]
    group: String,

    #[arg(short = 't', long = "topic", required = true, help = "set the topic")]
    topic: String,

    #[arg(
        short = 'i',
        long = "originClientId",
        required = false,
        help = "set the consumer clientId"
    )]
    origin_client_id: Option<String>,
}

impl GetConsumerStatusSubCommand {
    fn request(&self) -> RocketMQResult<ConsumerStatusQueryRequest> {
        ConsumerStatusQueryRequest::try_new(self.group.clone(), self.topic.clone(), self.origin_client_id.clone())
    }
}

impl CommandExecute for GetConsumerStatusSubCommand {
    async fn execute(
        &self,
        credentials: Option<rocketmq_admin_core::core::security::AdminCredentials>,
        client_runtime: std::sync::Arc<rocketmq_admin_core::client_adapter::ClientRuntime>,
    ) -> RocketMQResult<()> {
        let request = self.request()?;
        let result = OffsetService::query_consumer_status_by_request_with_credentials(
            request.clone(),
            credentials,
            client_runtime.clone(),
        )
        .await?;
        print_consumer_status_result(&request, &result);
        Ok(())
    }
}

fn print_consumer_status_result(request: &ConsumerStatusQueryRequest, result: &ConsumerStatusResult) {
    println!(
        "get consumer status from client. group={}, topic={}, originClientId={}",
        request.group(),
        request.topic(),
        request.origin_client_id()
    );

    println!(
        "{:<50}  {:<15}  {:<15}  {:<20}",
        "#clientId", "#brokerName", "#queueId", "#offset"
    );

    for row in &result.rows {
        println!(
            "{:<50}  {:<15}  {:<15}  {:<20}",
            row.client_id, row.broker_name, row.queue_id, row.offset,
        );
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;

    #[test]
    fn parses_get_consumer_status_request() {
        let cmd = GetConsumerStatusSubCommand::try_parse_from([
            "getConsumerStatus",
            "-g",
            " TestGroup ",
            "-t",
            " TestTopic ",
            "-i",
            " client-a ",
        ])
        .unwrap();
        let request = cmd.request().unwrap();

        assert_eq!(request.group().as_str(), "TestGroup");
        assert_eq!(request.topic().as_str(), "TestTopic");
        assert_eq!(request.origin_client_id().as_str(), "client-a");
    }
}
