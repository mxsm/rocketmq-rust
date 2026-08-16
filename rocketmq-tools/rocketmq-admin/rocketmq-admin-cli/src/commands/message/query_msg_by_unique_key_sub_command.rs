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

use clap::Parser;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_model::common::message::message_ext::MessageExt;
use rocketmq_runtime::common::util_all::time_millis_to_human_string2;

use crate::commands::CommandExecute;
use rocketmq_admin_core::client_adapter::services::message::DirectConsumeMessageResult;
use rocketmq_admin_core::client_adapter::services::message::DirectConsumeMessageStatus;
use rocketmq_admin_core::client_adapter::services::message::MessageService;
use rocketmq_admin_core::client_adapter::services::message::QueryMessageByUniqueKeyEntry;
use rocketmq_admin_core::client_adapter::services::message::QueryMessageByUniqueKeyRequest;
use rocketmq_admin_core::client_adapter::services::message::QueryMessageByUniqueKeyResult;

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
    fn show_message(entry: &QueryMessageByUniqueKeyEntry, index: usize) -> RocketMQResult<()> {
        let msg = &entry.message;
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

        if entry.tracks.is_empty() {
            println!("{:<20} []", "Consumer Track:");
        } else {
            println!("Consumer Track:");
            for track in &entry.tracks {
                println!(
                    "  group={} status={} exception={}",
                    track.consumer_group,
                    track.track_type.as_deref().unwrap_or("UNKNOWN"),
                    track.exception_desc
                );
            }
        }
        if let Some(error) = &entry.track_error {
            eprintln!("WARN: message track query failed: {error}");
        }
        println!();

        Ok(())
    }

    fn create_body_file(msg: &MessageExt, index: usize) -> RocketMQResult<PathBuf> {
        let mut body_tmp_file_path = Self::body_directory();
        fs::create_dir_all(&body_tmp_file_path).map_err(RocketMQError::IO)?;

        let mut filename = msg.msg_id().to_string();
        if index > 0 {
            filename.push_str(&format!("_{}", index));
        }
        body_tmp_file_path.push(filename);

        let body = msg.body().map(|b| b.to_vec()).unwrap_or_default();
        fs::write(&body_tmp_file_path, &body).map_err(RocketMQError::IO)?;

        Ok(body_tmp_file_path)
    }

    fn body_directory() -> PathBuf {
        std::env::temp_dir().join("rocketmq").join("msgbodys")
    }

    fn direct_consume_lines(result: &DirectConsumeMessageResult) -> Vec<String> {
        let target = format!(
            "topic={} msg_id={} group={} client={}",
            result.topic, result.msg_id, result.consumer_group, result.client_id
        );
        match &result.status {
            DirectConsumeMessageStatus::Consumed(detail) => vec![
                format!("direct consume succeeded: {target}"),
                format!(
                    "order={} auto_commit={} consume_result={} remark={} spent_time_millis={}",
                    detail.order,
                    detail.auto_commit,
                    detail.consume_result.as_deref().unwrap_or("UNKNOWN"),
                    detail.remark.as_deref().unwrap_or_default(),
                    detail.spent_time_millis
                ),
            ],
            DirectConsumeMessageStatus::NotPushConsumer => {
                vec![format!(
                    "direct consume unavailable: {target}; client is not a push consumer"
                )]
            }
            DirectConsumeMessageStatus::RunningInfoFailed { error } => {
                vec![format!(
                    "direct consume unavailable: {target}; running info failed: {error}"
                )]
            }
            DirectConsumeMessageStatus::Failed { error } => {
                vec![format!("direct consume failed: {target}; {error}")]
            }
        }
    }
}

impl CommandExecute for QueryMsgByUniqueKeySubCommand {
    async fn execute(
        &self,
        credentials: Option<rocketmq_admin_core::core::security::AdminCredentials>,
        client_runtime: std::sync::Arc<rocketmq_admin_core::client_adapter::ClientRuntime>,
    ) -> RocketMQResult<()> {
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

        match MessageService::query_message_by_unique_key_by_request_with_credentials(
            request,
            credentials,
            client_runtime.clone(),
        )
        .await?
        {
            QueryMessageByUniqueKeyResult::Messages(entries) => {
                for (index, entry) in entries.iter().enumerate() {
                    Self::show_message(entry, index)?;
                }
            }
            QueryMessageByUniqueKeyResult::Direct(result) => {
                for line in Self::direct_consume_lines(&result) {
                    println!("{line}");
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use rocketmq_admin_core::client_adapter::services::message::DirectConsumeMessageResult;
    use rocketmq_admin_core::client_adapter::services::message::DirectConsumeMessageResultDetail;
    use rocketmq_admin_core::client_adapter::services::message::DirectConsumeMessageStatus;

    use super::*;

    #[test]
    fn message_body_directory_is_platform_portable() {
        let directory = QueryMsgByUniqueKeySubCommand::body_directory();
        assert!(directory.starts_with(std::env::temp_dir()));
        assert!(directory.ends_with(PathBuf::from("rocketmq").join("msgbodys")));
    }

    #[test]
    fn direct_consume_lines_preserve_backend_result() {
        let result = DirectConsumeMessageResult {
            topic: "TopicA".into(),
            msg_id: "MSGID".into(),
            consumer_group: "GroupA".into(),
            client_id: "ClientA".into(),
            status: DirectConsumeMessageStatus::Consumed(DirectConsumeMessageResultDetail {
                order: true,
                auto_commit: false,
                consume_result: Some("CR_SUCCESS".to_string()),
                remark: Some("consumed".to_string()),
                spent_time_millis: 7,
            }),
        };

        let lines = QueryMsgByUniqueKeySubCommand::direct_consume_lines(&result);
        assert!(lines.iter().any(|line| line.contains("CR_SUCCESS")));
        assert!(lines.iter().any(|line| line.contains("spent_time_millis=7")));
        assert!(!lines.iter().any(|line| line.contains("not implemented")));
    }
}
