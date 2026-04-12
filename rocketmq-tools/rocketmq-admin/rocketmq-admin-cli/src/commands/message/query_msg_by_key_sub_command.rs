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
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use rocketmq_admin_core::core::message::MessageService;
use rocketmq_admin_core::core::message::QueryMessageByKeyRequest;

#[derive(Debug, Clone, Parser)]
pub struct QueryMsgByKeySubCommand {
    #[arg(short = 't', long = "topic", required = true, help = "Topic name")]
    topic: String,

    #[arg(short = 'k', long = "msgKey", required = true, help = "Message Key")]
    msg_key: String,

    #[arg(
        short = 'b',
        long = "beginTimestamp",
        required = false,
        help = "Begin timestamp(ms). default:0, eg:1676730526212"
    )]
    begin_timestamp: Option<i64>,

    #[arg(
        short = 'e',
        long = "endTimestamp",
        required = false,
        help = "End timestamp(ms). default:Long.MAX_VALUE, eg:1676730526212"
    )]
    end_timestamp: Option<i64>,

    #[arg(
        short = 'm',
        long = "maxNum",
        required = false,
        default_value = "64",
        help = "The maximum number of messages returned by the query, default:64"
    )]
    max_num: i32,

    #[arg(
        short = 'c',
        long = "cluster",
        required = false,
        help = "Cluster name or lmq parent topic, lmq is used to find the route."
    )]
    cluster: Option<String>,

    #[arg(
        short = 'p',
        long = "keyType",
        required = false,
        help = "Index key type, default index key type is K, you can use K for keys OR T for tags"
    )]
    key_type: Option<String>,

    #[arg(short = 'l', long = "lastKey", required = false, help = "Last Key")]
    last_key: Option<String>,
}

impl CommandExecute for QueryMsgByKeySubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let request = QueryMessageByKeyRequest::try_new(
            self.topic.clone(),
            self.msg_key.clone(),
            self.begin_timestamp,
            self.end_timestamp,
            self.max_num,
            self.cluster.clone(),
            self.key_type.clone(),
            self.last_key.clone(),
        )?;

        let query_result = MessageService::query_message_by_key_by_request_with_rpc_hook(request, rpc_hook)
            .await
            .map_err(|e| RocketMQError::Internal(format!("Failed to query message by key: {}", e)))?;

        println!(
            "{:<50} {:>4} {:>40} {:<200}",
            "#Message ID", "#QID", "#Offset", "#IndexKey"
        );
        for row in query_result.rows {
            if let Some(index_key) = row.index_key {
                println!(
                    "{:<50} {:>4} {:>40} {:<200}",
                    row.message_id, row.queue_id, row.queue_offset, index_key
                );
            } else {
                println!("{:<50} {:>4} {:>40}", row.message_id, row.queue_id, row.queue_offset);
            }
        }

        Ok(())
    }
}
