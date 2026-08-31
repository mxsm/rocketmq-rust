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

use rocketmq_admin_core::core::consumer_observation as admin;
use rocketmq_admin_core::core::consumer_observation::ConsumerObservationQueryAdmin;

use super::map_logical_admin_error;
use super::AdminCoreSession;
use super::SessionConsumerProgress;
use crate::model::contract::observed_at;
use crate::model::contract::observed_at_from_millis;
use crate::model::contract::QueryPayload;
use crate::tools::consumer_tools as tool;
use crate::tools::executor::ToolExecutionError;

impl AdminCoreSession {
    pub(super) async fn query_consumer_group_details_observation(
        &mut self,
        consumer_group: &str,
    ) -> Result<QueryPayload<tool::GetConsumerGroupDetailsOutput>, ToolExecutionError> {
        let request = admin::QueryConsumerGroupDetailsRequest::try_new(
            self.cluster.rocketmq_cluster_name.clone(),
            consumer_group,
        )
        .map_err(|_| ToolExecutionError::InvalidArguments("invalid consumer group selector".to_string()))?;
        let result = self
            .admin_mut()?
            .query_consumer_group_details(&request)
            .await
            .map_err(map_logical_admin_error)?;
        let cluster = self.cluster.name.clone();
        Ok(
            QueryPayload::from_admin(result).map(|result| tool::GetConsumerGroupDetailsOutput {
                cluster,
                consumer_group: result.consumer_group,
                total_connection_count: result.total_connection_count,
                brokers: result
                    .brokers
                    .into_iter()
                    .map(|row| tool::ConsumerGroupDetailsBrokerRow {
                        broker_name: row.broker_name,
                        config_state: match row.config_state {
                            admin::ConsumerGroupConfigState::Present => tool::ConsumerGroupConfigPresence::Present,
                            admin::ConsumerGroupConfigState::Absent => tool::ConsumerGroupConfigPresence::Absent,
                        },
                        config_version: row.config_version,
                        consume_enable: row.consume_enable,
                        consume_from_min_enable: row.consume_from_min_enable,
                        consume_broadcast_enable: row.consume_broadcast_enable,
                        consume_message_orderly: row.consume_message_orderly,
                        retry_queue_nums: row.retry_queue_nums,
                        retry_max_times: row.retry_max_times,
                        notify_consumer_ids_changed_enable: row.notify_consumer_ids_changed_enable,
                        consume_timeout_minutes: row.consume_timeout_minutes,
                        connection_state: row.connection_state.map(|state| match state {
                            admin::ConsumerConnectionState::Online => tool::ConsumerConnectionState::Online,
                            admin::ConsumerConnectionState::Offline => tool::ConsumerConnectionState::Offline,
                        }),
                        connection_count: row.connection_count,
                        consume_type: row.consume_type.map(|value| match value {
                            admin::ConsumerConsumeType::Pull => tool::ConsumerConsumeType::Pull,
                            admin::ConsumerConsumeType::Push => tool::ConsumerConsumeType::Push,
                            admin::ConsumerConsumeType::Pop => tool::ConsumerConsumeType::Pop,
                            admin::ConsumerConsumeType::Unknown => tool::ConsumerConsumeType::Unknown,
                        }),
                        message_model: row.message_model.map(|value| match value {
                            admin::ConsumerMessageModel::Broadcasting => tool::ConsumerMessageModel::Broadcasting,
                            admin::ConsumerMessageModel::Clustering => tool::ConsumerMessageModel::Clustering,
                            admin::ConsumerMessageModel::Unknown => tool::ConsumerMessageModel::Unknown,
                        }),
                        consume_from_where: row.consume_from_where.map(|value| match value {
                            admin::ConsumerConsumeFromWhere::LastOffset => tool::ConsumerConsumeFromWhere::LastOffset,
                            admin::ConsumerConsumeFromWhere::LastOffsetAndMinFirst => {
                                tool::ConsumerConsumeFromWhere::LastOffsetAndMinFirst
                            }
                            admin::ConsumerConsumeFromWhere::MinOffset => tool::ConsumerConsumeFromWhere::MinOffset,
                            admin::ConsumerConsumeFromWhere::MaxOffset => tool::ConsumerConsumeFromWhere::MaxOffset,
                            admin::ConsumerConsumeFromWhere::FirstOffset => tool::ConsumerConsumeFromWhere::FirstOffset,
                            admin::ConsumerConsumeFromWhere::Timestamp => tool::ConsumerConsumeFromWhere::Timestamp,
                            admin::ConsumerConsumeFromWhere::Unknown => tool::ConsumerConsumeFromWhere::Unknown,
                        }),
                    })
                    .collect(),
                generated_at: observed_at(),
            }),
        )
    }

    pub(super) async fn query_consumer_progress_observation(
        &mut self,
        consumer_group: &str,
    ) -> Result<QueryPayload<SessionConsumerProgress>, ToolExecutionError> {
        let request = admin::QueryConsumerProgressRequest::try_new(
            self.cluster.rocketmq_cluster_name.clone(),
            consumer_group,
            admin::MAX_CONSUMER_PROGRESS_ROWS,
        )
        .map_err(|_| ToolExecutionError::InvalidArguments("invalid consumer progress selector".to_string()))?;
        let result = self
            .admin_mut()?
            .query_consumer_progress(&request)
            .await
            .map_err(map_logical_admin_error)?;
        Ok(QueryPayload::from_admin(result).map(|result| SessionConsumerProgress {
            state: match result.state {
                admin::ConsumerProgressState::NoConsumption => tool::ConsumerProgressState::NoConsumption,
                admin::ConsumerProgressState::Observed => tool::ConsumerProgressState::Observed,
            },
            topic_count: result.topic_count,
            queue_count: result.queue_count,
            total_lag: result.total_lag,
            max_queue_lag: result.max_queue_lag,
            total_inflight: result.total_inflight,
            consume_tps: result.consume_tps,
            queues: result
                .queues
                .into_iter()
                .map(|row| tool::ConsumerProgressQueueRow {
                    topic: row.topic,
                    broker_name: row.broker_name,
                    queue_id: row.queue_id,
                    broker_offset: row.broker_offset,
                    consumer_offset: row.consumer_offset,
                    pull_offset: row.pull_offset,
                    lag: row.lag,
                    inflight: row.inflight,
                    last_observed_at: observed_at_from_millis(row.last_timestamp),
                })
                .collect(),
            truncated: result.truncated,
        }))
    }
}
