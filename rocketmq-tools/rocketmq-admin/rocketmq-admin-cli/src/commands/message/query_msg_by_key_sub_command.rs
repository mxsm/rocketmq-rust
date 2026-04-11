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

use cheetah_string::CheetahString;
use clap::Parser;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_common::common::message::MessageConst;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use rocketmq_admin_core::admin::default_mq_admin_ext::DefaultMQAdminExt;

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

fn deal_time_to_hour_stamps(timestamp: i64) -> i64 {
    const HOUR_MS: i64 = 1000 * 60 * 60;
    timestamp / HOUR_MS * HOUR_MS
}

impl CommandExecute for QueryMsgByKeySubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let mut default_mqadmin_ext = if let Some(rpc_hook) = rpc_hook {
            DefaultMQAdminExt::with_rpc_hook(rpc_hook)
        } else {
            DefaultMQAdminExt::new()
        };
        default_mqadmin_ext
            .client_config_mut()
            .set_instance_name(current_millis().to_string().into());

        let operation_result = async {
            MQAdminExt::start(&mut default_mqadmin_ext).await.map_err(|e| {
                RocketMQError::Internal(format!("QueryMsgByKeySubCommand: Failed to start MQAdminExt: {}", e))
            })?;

            let topic = self.topic.trim();
            let key = self.msg_key.trim();

            let mut key_type = CheetahString::from_static_str(MessageConst::INDEX_KEY_TYPE);
            if let Some(ref kt) = self.key_type {
                let kt = kt.trim();
                if kt != MessageConst::INDEX_KEY_TYPE && kt != MessageConst::INDEX_TAG_TYPE {
                    println!("index type error, just support K for keys or T for tags");
                    return Ok(());
                }
                key_type = CheetahString::from(kt);
            }

            let begin_timestamp = self.begin_timestamp.unwrap_or(0);
            let end_timestamp = self.end_timestamp.unwrap_or(i64::MAX);
            let max_num = self.max_num;
            let cluster_name = self.cluster.as_deref().map(|s| CheetahString::from(s.trim()));
            let last_key = self.last_key.as_deref().map(|s| CheetahString::from(s.trim()));

            let query_result = default_mqadmin_ext
                .query_message_by_key(
                    cluster_name,
                    CheetahString::from(topic),
                    CheetahString::from(key),
                    max_num,
                    begin_timestamp,
                    end_timestamp,
                    key_type.clone(),
                    last_key,
                )
                .await
                .map_err(|e| RocketMQError::Internal(format!("Failed to query message by key: {}", e)))?;

            println!(
                "{:<50} {:>4} {:>40} {:<200}",
                "#Message ID", "#QID", "#Offset", "#IndexKey"
            );
            for msg in query_result.message_list() {
                if !key_type.is_empty() {
                    let store_timestamp = deal_time_to_hour_stamps(msg.store_timestamp());
                    let index_last_key = format!(
                        "{}@{}@{}@{}@{}@{}",
                        store_timestamp,
                        topic,
                        key_type,
                        key,
                        msg.msg_id(),
                        msg.commit_log_offset()
                    );
                    println!(
                        "{:<50} {:>4} {:>40} {:<200}",
                        msg.msg_id(),
                        msg.queue_id(),
                        msg.queue_offset(),
                        index_last_key
                    );
                } else {
                    println!("{:<50} {:>4} {:>40}", msg.msg_id(), msg.queue_id(), msg.queue_offset(),);
                }
            }

            Ok(())
        }
        .await;

        MQAdminExt::shutdown(&mut default_mqadmin_ext).await;
        operation_result
    }
}
