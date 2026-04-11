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

use std::collections::HashMap;
use std::sync::Arc;

use cheetah_string::CheetahString;
use chrono::Local;
use chrono::TimeZone;
use clap::Parser;
use rocketmq_client_rust::TraceDataEncoder;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_common::UtilAll::YYYY_MM_DD_HH_MM_SS_SSS;
use rocketmq_common::common::topic::TopicValidator;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use crate::commands::CommonArgs;
use rocketmq_admin_core::admin::default_mq_admin_ext::DefaultMQAdminExt;

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct MessageTraceView {
    pub msg_type: String,
    pub group_name: String,
    pub client_host: String,
    pub time_stamp: i64,
    pub cost_time: i32,
    pub status: String,
    pub topic: Option<String>,
    pub tags: Option<String>,
    pub keys: Option<String>,
    pub store_host: Option<String>,
}

impl MessageTraceView {
    fn format_timestamp(timestamp: i64) -> String {
        if timestamp <= 0 {
            return "N/A".to_string();
        }
        let dt = Local.timestamp_millis_opt(timestamp);
        match dt {
            chrono::LocalResult::Single(dt) => dt.format(YYYY_MM_DD_HH_MM_SS_SSS).to_string(),
            _ => "N/A".to_string(),
        }
    }

    fn status_str(is_success: bool) -> String {
        if is_success {
            "success".to_string()
        } else {
            "failed".to_string()
        }
    }
}

#[derive(Debug, Clone, Parser)]
pub struct QueryMsgTraceByIdSubCommand {
    #[arg(short = 'i', long = "msgId", required = true, help = "Message ID to query")]
    msg_id: String,

    #[arg(
        short = 't',
        long = "traceTopic",
        required = false,
        help = "The name value of message trace topic"
    )]
    trace_topic: Option<String>,

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
        short = 'c',
        long = "maxNum",
        required = false,
        default_value = "64",
        help = "The maximum number of messages returned by the query, default:64"
    )]
    max_num: i32,

    #[command(flatten)]
    common_args: CommonArgs,
}

impl QueryMsgTraceByIdSubCommand {
    async fn query_trace_by_msg_id(
        &self,
        admin: &mut DefaultMQAdminExt,
        msg_id: &str,
        trace_topic: &str,
        max_num: i32,
        begin_timestamp: i64,
        end_timestamp: i64,
    ) -> RocketMQResult<Vec<MessageTraceView>> {
        let query_result = admin
            .query_message_by_key(
                None,
                CheetahString::from(trace_topic),
                CheetahString::from(msg_id),
                max_num,
                begin_timestamp,
                end_timestamp,
                CheetahString::from_static_str(""),
                None,
            )
            .await?;

        let mut trace_views = Vec::new();

        for msg in query_result.message_list() {
            if let Some(body) = msg.body() {
                let body_str = String::from_utf8_lossy(body.as_ref());
                if body_str.is_empty() {
                    continue;
                }

                let trace_contexts = TraceDataEncoder::decoder_from_trace_data_string(&body_str);

                for context in trace_contexts {
                    if let Some(trace_type) = context.trace_type {
                        if let Some(trace_beans) = &context.trace_beans {
                            for bean in trace_beans {
                                if bean.msg_id.as_str() != msg_id {
                                    continue;
                                }

                                let client_host = if !bean.client_host.is_empty() {
                                    bean.client_host.to_string()
                                } else {
                                    msg.born_host().to_string()
                                };

                                let trace_view = MessageTraceView {
                                    msg_type: trace_type.to_string(),
                                    group_name: context.group_name.to_string(),
                                    client_host,
                                    time_stamp: context.time_stamp as i64,
                                    cost_time: context.cost_time,
                                    status: MessageTraceView::status_str(context.is_success),
                                    topic: Some(bean.topic.to_string()),
                                    tags: Some(bean.tags.to_string()),
                                    keys: Some(bean.keys.to_string()),
                                    store_host: Some(bean.store_host.to_string()),
                                };
                                trace_views.push(trace_view);
                            }
                        }
                    }
                }
            }
        }

        Ok(trace_views)
    }

    fn print_message_trace(trace_views: Vec<MessageTraceView>) {
        let mut pub_traces: Vec<&MessageTraceView> = Vec::new();
        let mut consumer_trace_map: HashMap<String, Vec<&MessageTraceView>> = HashMap::new();

        for trace in &trace_views {
            if trace.msg_type == "Pub" {
                pub_traces.push(trace);
            } else {
                consumer_trace_map
                    .entry(trace.group_name.clone())
                    .or_default()
                    .push(trace);
            }
        }

        pub_traces.sort_by_key(|t| t.time_stamp);

        if !pub_traces.is_empty() {
            println!(
                "{:<10} {:<20} {:<20} {:<20} {:<12} {:<10}",
                "#Type", "#ProducerGroup", "#ClientHost", "#SendTime", "#CostTime", "#Status"
            );
            println!("{}", "-".repeat(100));

            for trace in pub_traces {
                println!(
                    "{:<10} {:<20} {:<20} {:<20} {:<12} {:<10}",
                    trace.msg_type,
                    trace.group_name,
                    trace.client_host,
                    MessageTraceView::format_timestamp(trace.time_stamp),
                    format!("{}ms", trace.cost_time),
                    trace.status
                );
            }
            println!();
        }

        let mut sorted_groups: Vec<_> = consumer_trace_map.into_iter().collect();
        sorted_groups.sort_by(|(a, _), (b, _)| a.cmp(b));

        for (consumer_group, mut traces) in sorted_groups {
            traces.sort_by_key(|t| t.time_stamp);

            println!(
                "{:<10} {:<20} {:<20} {:<20} {:<12} {:<10}",
                "#Type", "#ConsumerGroup", "#ClientHost", "#ConsumeTime", "#CostTime", "#Status"
            );
            println!("{}", "-".repeat(100));

            for trace in traces {
                println!(
                    "{:<10} {:<20} {:<20} {:<20} {:<12} {:<10}",
                    trace.msg_type,
                    consumer_group,
                    trace.client_host,
                    MessageTraceView::format_timestamp(trace.time_stamp),
                    format!("{}ms", trace.cost_time),
                    trace.status
                );
            }
            println!();
        }

        if trace_views.is_empty() {
            println!("No trace information found for message ID.");
        }
    }
}

impl CommandExecute for QueryMsgTraceByIdSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let mut default_mqadmin_ext = if let Some(rpc_hook) = rpc_hook {
            DefaultMQAdminExt::with_rpc_hook(rpc_hook)
        } else {
            DefaultMQAdminExt::new()
        };
        default_mqadmin_ext
            .client_config_mut()
            .set_instance_name(current_millis().to_string().into());

        if let Some(ref namesrv_addr) = self.common_args.namesrv_addr {
            default_mqadmin_ext.set_namesrv_addr(namesrv_addr);
        }

        let operation_result = async {
            MQAdminExt::start(&mut default_mqadmin_ext)
                .await
                .map_err(|e| RocketMQError::Internal(format!("Failed to start MQAdminExt: {}", e)))?;

            let msg_id = self.msg_id.trim();
            let trace_topic = self
                .trace_topic
                .as_ref()
                .map(|t| t.trim().to_string())
                .unwrap_or_else(|| TopicValidator::RMQ_SYS_TRACE_TOPIC.to_string());

            let begin_timestamp = self.begin_timestamp.unwrap_or(0);
            let end_timestamp = self.end_timestamp.unwrap_or(i64::MAX);
            let max_num = self.max_num;

            let trace_views = self
                .query_trace_by_msg_id(
                    &mut default_mqadmin_ext,
                    msg_id,
                    &trace_topic,
                    max_num,
                    begin_timestamp,
                    end_timestamp,
                )
                .await?;

            Self::print_message_trace(trace_views);

            Ok(())
        }
        .await;

        MQAdminExt::shutdown(&mut default_mqadmin_ext).await;
        operation_result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_trace_view_creation() {
        let trace = MessageTraceView {
            msg_type: "Pub".to_string(),
            group_name: "test-group".to_string(),
            client_host: "192.168.1.100".to_string(),
            time_stamp: 1708337445123,
            cost_time: 100,
            status: "success".to_string(),
            topic: Some("test-topic".to_string()),
            tags: Some("tag1".to_string()),
            keys: Some("key1".to_string()),
            store_host: Some("192.168.1.10:10911".to_string()),
        };
        assert_eq!(trace.msg_type, "Pub");
        assert_eq!(trace.group_name, "test-group");
        assert_eq!(trace.cost_time, 100);
    }

    #[test]
    fn test_message_trace_view_debug() {
        let trace = MessageTraceView {
            msg_type: "Pub".to_string(),
            group_name: "producer-group".to_string(),
            client_host: "192.168.1.100".to_string(),
            time_stamp: 1708337445123,
            cost_time: 100,
            status: "success".to_string(),
            topic: None,
            tags: None,
            keys: None,
            store_host: None,
        };
        let debug_str = format!("{:?}", trace);
        assert!(debug_str.contains("MessageTraceView"));
        assert!(debug_str.contains("Pub"));
        assert!(debug_str.contains("producer-group"));
    }

    #[test]
    fn test_print_message_trace_empty() {
        let traces: Vec<MessageTraceView> = Vec::new();
        QueryMsgTraceByIdSubCommand::print_message_trace(traces);
    }

    #[test]
    fn test_print_message_trace_pub_only() {
        let traces = vec![
            MessageTraceView {
                msg_type: "Pub".to_string(),
                group_name: "producer-group-1".to_string(),
                client_host: "192.168.1.100".to_string(),
                time_stamp: 1708337445123,
                cost_time: 5,
                status: "success".to_string(),
                topic: Some("test-topic".to_string()),
                tags: Some("tag1".to_string()),
                keys: Some("key1".to_string()),
                store_host: Some("192.168.1.10:10911".to_string()),
            },
            MessageTraceView {
                msg_type: "Pub".to_string(),
                group_name: "producer-group-2".to_string(),
                client_host: "192.168.1.101".to_string(),
                time_stamp: 1708337446123,
                cost_time: 3,
                status: "success".to_string(),
                topic: Some("test-topic".to_string()),
                tags: Some("tag2".to_string()),
                keys: Some("key2".to_string()),
                store_host: Some("192.168.1.11:10911".to_string()),
            },
        ];
        QueryMsgTraceByIdSubCommand::print_message_trace(traces);
    }

    #[test]
    fn test_print_message_trace_mixed() {
        let traces = vec![
            MessageTraceView {
                msg_type: "Pub".to_string(),
                group_name: "producer-group".to_string(),
                client_host: "192.168.1.100".to_string(),
                time_stamp: 1708337445123,
                cost_time: 5,
                status: "success".to_string(),
                topic: Some("test-topic".to_string()),
                tags: Some("tag1".to_string()),
                keys: Some("key1".to_string()),
                store_host: Some("192.168.1.10:10911".to_string()),
            },
            MessageTraceView {
                msg_type: "SubBefore".to_string(),
                group_name: "consumer-group-1".to_string(),
                client_host: "192.168.1.200".to_string(),
                time_stamp: 1708337448123,
                cost_time: 0,
                status: "success".to_string(),
                topic: Some("test-topic".to_string()),
                tags: Some("tag1".to_string()),
                keys: Some("key1".to_string()),
                store_host: Some("192.168.1.10:10911".to_string()),
            },
            MessageTraceView {
                msg_type: "SubAfter".to_string(),
                group_name: "consumer-group-1".to_string(),
                client_host: "192.168.1.200".to_string(),
                time_stamp: 1708337448223,
                cost_time: 100,
                status: "success".to_string(),
                topic: Some("test-topic".to_string()),
                tags: Some("tag1".to_string()),
                keys: Some("key1".to_string()),
                store_host: Some("192.168.1.10:10911".to_string()),
            },
        ];
        QueryMsgTraceByIdSubCommand::print_message_trace(traces);
    }

    #[test]
    fn test_print_message_trace_multiple_consumer_groups() {
        let traces = vec![
            MessageTraceView {
                msg_type: "Pub".to_string(),
                group_name: "producer-group".to_string(),
                client_host: "192.168.1.100".to_string(),
                time_stamp: 1708337445123,
                cost_time: 5,
                status: "success".to_string(),
                topic: Some("test-topic".to_string()),
                tags: Some("tag1".to_string()),
                keys: Some("key1".to_string()),
                store_host: Some("192.168.1.10:10911".to_string()),
            },
            MessageTraceView {
                msg_type: "SubBefore".to_string(),
                group_name: "consumer-group-a".to_string(),
                client_host: "192.168.1.201".to_string(),
                time_stamp: 1708337448123,
                cost_time: 0,
                status: "success".to_string(),
                topic: Some("test-topic".to_string()),
                tags: Some("tag1".to_string()),
                keys: Some("key1".to_string()),
                store_host: Some("192.168.1.10:10911".to_string()),
            },
            MessageTraceView {
                msg_type: "SubBefore".to_string(),
                group_name: "consumer-group-b".to_string(),
                client_host: "192.168.1.202".to_string(),
                time_stamp: 1708337448223,
                cost_time: 0,
                status: "success".to_string(),
                topic: Some("test-topic".to_string()),
                tags: Some("tag1".to_string()),
                keys: Some("key1".to_string()),
                store_host: Some("192.168.1.10:10911".to_string()),
            },
        ];
        QueryMsgTraceByIdSubCommand::print_message_trace(traces);
    }

    #[test]
    fn test_print_message_trace_failed_status() {
        let traces = vec![
            MessageTraceView {
                msg_type: "Pub".to_string(),
                group_name: "producer-group".to_string(),
                client_host: "192.168.1.100".to_string(),
                time_stamp: 1708337445123,
                cost_time: 5,
                status: "failed".to_string(),
                topic: Some("test-topic".to_string()),
                tags: Some("tag1".to_string()),
                keys: Some("key1".to_string()),
                store_host: Some("192.168.1.10:10911".to_string()),
            },
            MessageTraceView {
                msg_type: "SubBefore".to_string(),
                group_name: "consumer-group".to_string(),
                client_host: "192.168.1.200".to_string(),
                time_stamp: 1708337448123,
                cost_time: 0,
                status: "failed".to_string(),
                topic: Some("test-topic".to_string()),
                tags: Some("tag1".to_string()),
                keys: Some("key1".to_string()),
                store_host: Some("192.168.1.10:10911".to_string()),
            },
        ];
        QueryMsgTraceByIdSubCommand::print_message_trace(traces);
    }
}
