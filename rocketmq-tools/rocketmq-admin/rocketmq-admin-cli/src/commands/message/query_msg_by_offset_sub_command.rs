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
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use rocketmq_admin_core::core::message::MessageService;
use rocketmq_admin_core::core::message::QueryMessageByOffsetRequest;

#[derive(Debug, Clone, Parser)]
pub struct QueryMsgByOffsetSubCommand {
    #[arg(short = 't', long = "topic", required = true, help = "Topic name")]
    topic: String,

    #[arg(short = 'b', long = "brokerName", required = true, help = "Broker name")]
    broker_name: String,

    #[arg(short = 'i', long = "queueId", required = true, help = "Queue Id")]
    queue_id: i32,

    #[arg(short = 'o', long = "offset", required = true, help = "Queue Offset")]
    offset: i64,

    #[arg(
        short = 'f',
        long = "bodyFormat",
        required = false,
        help = "print message body by the specified format"
    )]
    body_format: Option<String>,

    #[arg(
        short = 'r',
        long = "routeTopic",
        required = false,
        help = "the topic which is used to find route info"
    )]
    route_topic: Option<String>,
}

impl CommandExecute for QueryMsgByOffsetSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let request = QueryMessageByOffsetRequest::try_new(
            self.topic.clone(),
            self.broker_name.clone(),
            self.queue_id,
            self.offset,
            self.route_topic.clone(),
        )?;
        let result = MessageService::query_message_by_offset_by_request_with_rpc_hook(request, rpc_hook).await?;

        if let Some(msg) = result.message {
            let msg: &MessageExt = &msg;
            let body_format = self.body_format.as_deref().map(str::trim).unwrap_or("UTF-8");
            let body = match msg.body() {
                Some(bytes) => {
                    if body_format.eq_ignore_ascii_case("UTF-8") || body_format.eq_ignore_ascii_case("UTF8") {
                        String::from_utf8(bytes.to_vec()).unwrap_or_else(|_| "BINARY".to_string())
                    } else if body_format.eq_ignore_ascii_case("ISO-8859-1")
                        || body_format.eq_ignore_ascii_case("LATIN1")
                        || body_format.eq_ignore_ascii_case("LATIN-1")
                    {
                        bytes.iter().map(|&b| b as char).collect()
                    } else if body_format.eq_ignore_ascii_case("ASCII") {
                        if bytes.is_ascii() {
                            String::from_utf8(bytes.to_vec()).unwrap_or_else(|_| "BINARY".to_string())
                        } else {
                            return Err(RocketMQError::IllegalArgument(format!(
                                "Unsupported body bytes for ASCII bodyFormat: {}",
                                body_format
                            )));
                        }
                    } else {
                        return Err(RocketMQError::IllegalArgument(format!(
                            "Unsupported bodyFormat: {}",
                            body_format
                        )));
                    }
                }
                None => "EMPTY".to_string(),
            };
            println!("MSGID: {} {} BODY: {}", msg.msg_id(), msg, body);
        }

        Ok(())
    }
}
