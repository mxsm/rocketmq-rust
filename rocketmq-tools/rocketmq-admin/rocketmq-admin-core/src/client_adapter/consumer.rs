// Copyright 2026 The RocketMQ Rust Authors
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

use cheetah_string::CheetahString;
use rocketmq_client_rust::MQAdminExt;
use rocketmq_model::message::MessageQueue;
use rocketmq_model::topic::RETRY_GROUP_TOPIC_PREFIX;
use rocketmq_protocol::common::key_builder::KeyBuilder;
use rocketmq_protocol::protocol::heartbeat::consume_type::ConsumeType;
use rocketmq_protocol::protocol::heartbeat::message_model::MessageModel;

use crate::client_adapter::lifecycle::AdminSession;
use crate::core::consumer::ConsumerAdmin;
use crate::core::consumer::ConsumerGroupSummary;
use crate::core::consumer::ConsumerLagRow;
use crate::core::consumer::ListConsumerGroupsRequest;
use crate::core::consumer::ListConsumerGroupsResult;
use crate::core::consumer::QueryConsumerLagRequest;
use crate::core::consumer::QueryConsumerLagResult;
use crate::core::AdminError;
use crate::core::AdminFuture;

impl ConsumerAdmin for AdminSession {
    fn list_consumer_groups<'a>(
        &'a mut self,
        _request: &'a ListConsumerGroupsRequest,
    ) -> AdminFuture<'a, ListConsumerGroupsResult> {
        Box::pin(async move {
            self.ensure_open()?;
            let topic_list = self
                .inner
                .fetch_all_topic_list()
                .await
                .map_err(|error| AdminError::backend("fetch_all_topic_list", error.to_string()))?;
            let mut groups = Vec::new();
            for retry_topic in topic_list
                .topic_list
                .into_iter()
                .filter(|topic| topic.starts_with(RETRY_GROUP_TOPIC_PREFIX))
            {
                let group = KeyBuilder::parse_group(retry_topic.as_str());
                let mut summary = ConsumerGroupSummary {
                    group: group.clone(),
                    version: 0,
                    client_count: 0,
                    consume_type: format!("{:?}", ConsumeType::ConsumePassively),
                    message_model: format!("{:?}", MessageModel::Clustering),
                    consume_tps: 0.0,
                    diff_total: 0,
                };

                if let Ok(stats) = self
                    .inner
                    .examine_consume_stats(CheetahString::from(group.as_str()), None, None, None, None)
                    .await
                {
                    summary.consume_tps = stats.get_consume_tps();
                    summary.diff_total = stats.compute_total_diff();
                }
                if let Ok(connection) = self
                    .inner
                    .examine_consumer_connection_info(CheetahString::from(group.as_str()), None)
                    .await
                {
                    summary.client_count = connection.get_connection_set().len() as i32;
                    summary.consume_type = format!(
                        "{:?}",
                        connection.get_consume_type().unwrap_or(ConsumeType::ConsumePassively)
                    );
                    summary.message_model = format!(
                        "{:?}",
                        connection.get_message_model().unwrap_or(MessageModel::Clustering)
                    );
                    summary.version = connection.compute_min_version();
                }
                groups.push(summary);
            }
            groups.sort_by(|left, right| left.group.cmp(&right.group));
            Ok(ListConsumerGroupsResult { groups })
        })
    }

    fn query_consumer_lag<'a>(
        &'a mut self,
        request: &'a QueryConsumerLagRequest,
    ) -> AdminFuture<'a, QueryConsumerLagResult> {
        Box::pin(async move {
            self.ensure_open()?;
            let stats = self
                .inner
                .examine_consume_stats(
                    CheetahString::from(request.consumer_group.as_str()),
                    Some(CheetahString::from(request.topic.as_str())),
                    None,
                    None,
                    None,
                )
                .await
                .map_err(|error| AdminError::backend("examine_consume_stats", error.to_string()))?;
            let allocation = if request.include_client_ip {
                message_queue_allocation(&self.inner, request.consumer_group.as_str()).await
            } else {
                HashMap::new()
            };

            let mut queues = stats.get_offset_table().keys().cloned().collect::<Vec<_>>();
            queues.sort();
            let mut result = QueryConsumerLagResult {
                consume_tps: stats.get_consume_tps(),
                ..QueryConsumerLagResult::default()
            };
            for queue in queues {
                let Some(offset) = stats.get_offset_table().get(&queue) else {
                    continue;
                };
                let lag = offset.get_broker_offset() - offset.get_consumer_offset();
                let inflight = offset.get_pull_offset() - offset.get_consumer_offset();
                result.total_lag += lag;
                result.inflight_total += inflight;
                result.rows.push(ConsumerLagRow {
                    topic: queue.topic().to_string(),
                    broker_name: queue.broker_name().to_string(),
                    queue_id: queue.queue_id(),
                    broker_offset: offset.get_broker_offset(),
                    consumer_offset: offset.get_consumer_offset(),
                    lag,
                    inflight,
                    last_timestamp: offset.get_last_timestamp(),
                    client_ip: allocation.get(&queue).cloned(),
                });
            }
            Ok(result)
        })
    }
}

async fn message_queue_allocation(
    admin: &rocketmq_client_rust::DefaultMQAdminExt,
    group: &str,
) -> HashMap<MessageQueue, String> {
    let mut allocation = HashMap::new();
    let Ok(connection) = admin
        .examine_consumer_connection_info(CheetahString::from(group), None)
        .await
    else {
        return allocation;
    };
    for client in connection.get_connection_set() {
        let client_id = client.get_client_id().clone();
        let Ok(running_info) = admin
            .get_consumer_running_info(CheetahString::from(group), client_id.clone(), false, None)
            .await
        else {
            continue;
        };
        let client_ip = client_id.split('@').next().unwrap_or_default().to_string();
        for queue in running_info.mq_table.keys() {
            allocation.insert(queue.clone(), client_ip.clone());
        }
    }
    allocation
}
