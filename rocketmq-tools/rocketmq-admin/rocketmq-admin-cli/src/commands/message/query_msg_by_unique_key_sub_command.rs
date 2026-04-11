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

use cheetah_string::CheetahString;
use clap::Parser;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::utils::util_all::time_millis_to_human_string2;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use rocketmq_admin_core::admin::default_mq_admin_ext::DefaultMQAdminExt;

const QUERY_MAX_NUM: i32 = 32;
const DEFAULT_QUERY_WINDOW_MS: i64 = 36 * 60 * 60 * 1000;

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
    async fn query_by_id(
        admin: &DefaultMQAdminExt,
        cluster_name: Option<CheetahString>,
        topic: &str,
        msg_id: &str,
        show_all: bool,
        start_time: Option<&str>,
        end_time: Option<&str>,
    ) -> RocketMQResult<()> {
        let query_result = if let (Some(start_time), Some(end_time)) = (start_time, end_time) {
            let start_time_long = start_time
                .trim()
                .parse::<i64>()
                .map_err(|e| RocketMQError::IllegalArgument(format!("Invalid startTime '{}': {}", start_time, e)))?;
            let end_time_long = end_time
                .trim()
                .parse::<i64>()
                .map_err(|e| RocketMQError::IllegalArgument(format!("Invalid endTime '{}': {}", end_time, e)))?;

            admin
                .query_message_by_unique_key(
                    cluster_name,
                    CheetahString::from(topic),
                    CheetahString::from(msg_id),
                    QUERY_MAX_NUM,
                    start_time_long,
                    end_time_long,
                )
                .await?
        } else {
            let now = current_millis() as i64;
            admin
                .query_message_by_unique_key(
                    cluster_name,
                    CheetahString::from(topic),
                    CheetahString::from(msg_id),
                    QUERY_MAX_NUM,
                    now - DEFAULT_QUERY_WINDOW_MS,
                    now + DEFAULT_QUERY_WINDOW_MS,
                )
                .await?
        };

        let mut list: Vec<&MessageExt> = query_result.message_list().iter().collect();
        if list.is_empty() {
            return Ok(());
        }

        list.sort_by_key(|msg| msg.store_timestamp());
        let upper = if show_all { list.len() } else { 1 };
        for (index, msg) in list.iter().take(upper).enumerate() {
            Self::show_message(msg, index)?;
        }

        Ok(())
    }

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
        let mut default_mqadmin_ext = if let Some(rpc_hook) = rpc_hook {
            DefaultMQAdminExt::with_rpc_hook(rpc_hook)
        } else {
            DefaultMQAdminExt::new()
        };
        default_mqadmin_ext
            .client_config_mut()
            .set_instance_name(current_millis().to_string().into());

        let operation_result = async {
            MQAdminExt::start(&mut default_mqadmin_ext)
                .await
                .map_err(|e| RocketMQError::Internal(format!("Failed to start MQAdminExt: {}", e)))?;

            let msg_id = self.msg_id.trim();
            let topic = self.topic.trim();
            let cluster_name = self.cluster.as_deref().map(|s| CheetahString::from(s.trim()));
            let start_time = self.start_time.as_deref().map(str::trim);
            let end_time = self.end_time.as_deref().map(str::trim);

            if let (Some(consumer_group), Some(client_id)) = (&self.consumer_group, &self.client_id) {
                let consumer_group = CheetahString::from(consumer_group.trim());
                let client_id = CheetahString::from(client_id.trim());

                let consumer_running_info = default_mqadmin_ext
                    .get_consumer_running_info(consumer_group.clone(), client_id.clone(), false, Some(false))
                    .await;

                match consumer_running_info {
                    Ok(info) if info.is_push_type() => {
                        println!(
                            "consumeMessageDirectly path is not implemented in rocketmq-rust yet, skip direct push \
                             for client {}",
                            client_id
                        );
                    }
                    Ok(_) => {
                        println!(
                            "get consumer info failed or this {} client is not push consumer, not support direct push",
                            client_id
                        );
                    }
                    Err(_) => {
                        println!("get consumer runtime info for {} client failed", client_id);
                    }
                }
            } else {
                Self::query_by_id(
                    &default_mqadmin_ext,
                    cluster_name,
                    topic,
                    msg_id,
                    self.show_all,
                    start_time,
                    end_time,
                )
                .await?;
            }

            Ok(())
        }
        .await;

        MQAdminExt::shutdown(&mut default_mqadmin_ext).await;
        operation_result
    }
}
