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

use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::utils::util_all::time_millis_to_human_string2;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use rocketmq_admin_core::core::message::MessageService;
use rocketmq_admin_core::core::message::QueryMessageByUniqueKeyRequest;
use rocketmq_admin_core::core::message::QueryMessageByUniqueKeyResult;
use rocketmq_admin_core::core::message::UniqueKeyDirectStatus;

#[derive(Debug, Clone, Parser)]
pub struct QueryMsgByUniqueKeySubCommand {
    #[arg(short = 'i', long = "msgId", required = true, help = "Message Id")]
    msg_id: String,

    #[arg(short = 'g', long = "consumerGroup", required = false, help = "consumer group name")]
    consumer_group: Option<String>,

    #[arg(short = 'd', long = "clientId", required = false, help = "The consumer's client id")]
    client_id: Option<String>,

    #[arg(short = 't', long = "topic", required = true, help = "The topic of msg")]
    topic: String,

    #[arg(short = 'a', long = "showAll", required = false, default_value_t = false, action = clap::ArgAction::SetTrue, help = "Print all message, the limit is 32")]
    show_all: bool,

    #[arg(
        short = 'c',
        long = "cluster",
        required = false,
        help = "Cluster name or lmq parent topic, lmq is used to find the route."
    )]
    cluster: Option<String>,

    #[arg(short = 's', long = "startTime", required = false, help = "startTime")]
    start_time: Option<String>,

    #[arg(short = 'e', long = "endTime", required = false, help = "endTime")]
    end_time: Option<String>,
}

impl QueryMsgByUniqueKeySubCommand {
    fn show_message(msg: &MessageExt, index: usize) -> RocketMQResult<()> {
        let body_tmp_file_path = Self::create_body_file(msg, index)?;
        println!("{:<20} {}", "Topic:", msg.topic());
        println!("{:<20} [{}]", "Tags:", msg.get_tags().unwrap_or_default());
        if let Some(keys) = msg.message_inner().keys() {
            println!("{:<20} [{}]", "Keys:", keys.join(" "));
        } else {
            println!("{:<20} []", "Keys:");
        }
        println!("{:<20} {}", "Queue ID:", msg.queue_id());
        println!("{:<20} {}", "Queue Offset:", msg.queue_offset());
        println!("{:<20} {}", "CommitLog Offset:", msg.commit_log_offset());
        println!("{:<20} {}", "Reconsume Times:", msg.reconsume_times());
        println!(
            "{:<20} {}",
            "Born Timestamp:",
            time_millis_to_human_string2(msg.born_timestamp())
        );
        println!(
            "{:<20} {}",
            "Store Timestamp:",
            time_millis_to_human_string2(msg.store_timestamp())
        );
        println!("{:<20} {}", "Born Host:", msg.born_host());
        println!("{:<20} {}", "Store Host:", msg.store_host());
        println!("{:<20} {}", "System Flag:", msg.sys_flag());
        println!("{:<20} {:?}", "Properties:", msg.properties());
        println!("{:<20} {}", "Message Body Path:", body_tmp_file_path.display());

        println!();
        println!("WARN: messageTrackDetail is not implemented yet in rocketmq-rust");
        println!();

        Ok(())
    }

    fn create_body_file(msg: &MessageExt, index: usize) -> RocketMQResult<PathBuf> {
        let mut body_tmp_file_path = PathBuf::from("/tmp/rocketmq/msgbodys");
        fs::create_dir_all(&body_tmp_file_path)
            .map_err(|e| RocketMQError::Internal(format!("Failed to create body temp directory: {}", e)))?;

        let mut filename = msg.msg_id().to_string();
        if index > 0 {
            filename.push_str(&format!("_{}", index));
        }
        body_tmp_file_path.push(filename);

        let body = msg.body().map(|b| b.to_vec()).unwrap_or_default();
        fs::write(&body_tmp_file_path, &body)
            .map_err(|e| RocketMQError::Internal(format!("Failed to write body file: {}", e)))?;

        Ok(body_tmp_file_path)
    }
}

impl CommandExecute for QueryMsgByUniqueKeySubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let start_time = self
            .start_time
            .as_deref()
            .map(str::trim)
            .map(|value| {
                value
                    .parse::<i64>()
                    .map_err(|e| RocketMQError::IllegalArgument(format!("Invalid startTime '{}': {}", value, e)))
            })
            .transpose()?;
        let end_time = self
            .end_time
            .as_deref()
            .map(str::trim)
            .map(|value| {
                value
                    .parse::<i64>()
                    .map_err(|e| RocketMQError::IllegalArgument(format!("Invalid endTime '{}': {}", value, e)))
            })
            .transpose()?;

        let request = QueryMessageByUniqueKeyRequest::try_new(
            self.msg_id.clone(),
            self.consumer_group.clone(),
            self.client_id.clone(),
            self.topic.clone(),
            self.show_all,
            self.cluster.clone(),
            start_time,
            end_time,
        )?;

        match MessageService::query_message_by_unique_key_by_request_with_rpc_hook(request, rpc_hook).await? {
            QueryMessageByUniqueKeyResult::Messages(messages) => {
                for (index, msg) in messages.iter().enumerate() {
                    Self::show_message(msg, index)?;
                }
            }
            QueryMessageByUniqueKeyResult::DirectStatus(UniqueKeyDirectStatus::PushConsumerUnsupported {
                client_id,
            }) => {
                println!(
                    "consumeMessageDirectly path is not implemented in rocketmq-rust yet, skip direct push for client \
                     {}",
                    client_id
                );
            }
            QueryMessageByUniqueKeyResult::DirectStatus(UniqueKeyDirectStatus::NotPushConsumer { client_id }) => {
                println!(
                    "get consumer info failed or this {} client is not push consumer, not support direct push",
                    client_id
                );
            }
            QueryMessageByUniqueKeyResult::DirectStatus(UniqueKeyDirectStatus::RunningInfoFailed { client_id }) => {
                println!("get consumer runtime info for {} client failed", client_id);
            }
        }

        Ok(())
    }
}
