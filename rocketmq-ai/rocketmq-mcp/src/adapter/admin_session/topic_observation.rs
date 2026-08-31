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

use rocketmq_admin_core::core::topic_observation::QueryTopicConfigRequest;
use rocketmq_admin_core::core::topic_observation::QueryTopicStatsRequest;
use rocketmq_admin_core::core::topic_observation::TopicConfigDifferenceField as AdminDifference;
use rocketmq_admin_core::core::topic_observation::TopicObservationQueryAdmin;
use rocketmq_admin_core::core::topic_observation::MAX_TOPIC_STATS_ROWS;

use super::map_logical_admin_error;
use super::AdminCoreSession;
use super::SessionTopicStats;
use crate::model::contract::observed_at;
use crate::model::contract::observed_at_from_millis;
use crate::model::contract::QueryPayload;
use crate::tools::config_tools::GetTopicConfigOutput;
use crate::tools::config_tools::TopicConfigDifferenceField;
use crate::tools::config_tools::TopicConfigObservationRow;
use crate::tools::executor::ToolExecutionError;
use crate::tools::topic_tools::TopicStatsQueueRow;

impl AdminCoreSession {
    pub(super) async fn query_topic_stats_observation(
        &mut self,
        topic: &str,
    ) -> Result<QueryPayload<SessionTopicStats>, ToolExecutionError> {
        let request =
            QueryTopicStatsRequest::try_new(self.cluster.rocketmq_cluster_name.clone(), topic, MAX_TOPIC_STATS_ROWS)
                .map_err(|_| ToolExecutionError::InvalidArguments("invalid Topic statistics selector".to_string()))?;
        let result = self
            .admin_mut()?
            .query_topic_stats(&request)
            .await
            .map_err(map_logical_admin_error)?;
        Ok(QueryPayload::from_admin(result).map(|result| SessionTopicStats {
            total_message_count: result.total_message_count,
            queue_count: result.queue_count,
            queues: result
                .queues
                .into_iter()
                .map(|row| TopicStatsQueueRow {
                    broker_name: row.broker_name,
                    queue_id: row.queue_id,
                    min_offset: row.min_offset,
                    max_offset: row.max_offset,
                    message_count: row.message_count,
                    last_update_at: observed_at_from_millis(row.last_update_timestamp),
                })
                .collect(),
            truncated: result.truncated,
        }))
    }

    pub(super) async fn query_topic_config_observation(
        &mut self,
        topic: &str,
    ) -> Result<QueryPayload<GetTopicConfigOutput>, ToolExecutionError> {
        let request = QueryTopicConfigRequest::try_new(self.cluster.rocketmq_cluster_name.clone(), topic)
            .map_err(|_| ToolExecutionError::InvalidArguments("invalid Topic configuration selector".to_string()))?;
        let result = self
            .admin_mut()?
            .query_topic_config(&request)
            .await
            .map_err(map_logical_admin_error)?;
        let cluster = self.cluster.name.clone();
        Ok(QueryPayload::from_admin(result).map(|result| GetTopicConfigOutput {
            cluster,
            topic: result.topic,
            brokers: result
                .brokers
                .into_iter()
                .map(|row| TopicConfigObservationRow {
                    broker_name: row.broker_name,
                    version: row.version,
                    read_queue_nums: row.read_queue_nums,
                    write_queue_nums: row.write_queue_nums,
                    perm: row.perm,
                    order: row.order,
                    message_type: row.message_type,
                })
                .collect(),
            inconsistent_fields: result
                .inconsistent_fields
                .into_iter()
                .map(|field| match field {
                    AdminDifference::ReadQueueNums => TopicConfigDifferenceField::ReadQueueNums,
                    AdminDifference::WriteQueueNums => TopicConfigDifferenceField::WriteQueueNums,
                    AdminDifference::Perm => TopicConfigDifferenceField::Perm,
                    AdminDifference::Order => TopicConfigDifferenceField::Order,
                    AdminDifference::MessageType => TopicConfigDifferenceField::MessageType,
                })
                .collect(),
            generated_at: observed_at(),
        }))
    }
}
