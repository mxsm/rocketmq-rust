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

//! What one Broker holds for Lite Topics.

use rocketmq_admin_core::core::lite::LiteBrokerQueryAdmin;
use rocketmq_admin_core::core::lite::LiteBrokerQueryRequest;

use super::map_admin_error;
use super::AdminCoreSession;
use crate::model::contract::observed_at;
use crate::tools::broker_tools::GetLiteBrokerInfoOutput;
use crate::tools::broker_tools::LiteTopicSummary;
use crate::tools::executor::NotFoundEntity;
use crate::tools::executor::ToolFailure;
use crate::tools::executor::ToolRejection;

impl AdminCoreSession {
    pub(super) async fn query_lite_broker_observation(
        &mut self,
        broker_name: &str,
    ) -> Result<GetLiteBrokerInfoOutput, ToolFailure> {
        let request = LiteBrokerQueryRequest::try_new(self.cluster.rocketmq_cluster_name.clone(), broker_name)
            .map_err(|_| ToolFailure::Rejected(ToolRejection::InvalidArguments { _source: None }))?;
        let observed = self
            .admin_mut()?
            .query_lite_broker_info(&request)
            .await
            .map_err(map_admin_error(NotFoundEntity::Broker))?;
        Ok(GetLiteBrokerInfoOutput {
            cluster: self.cluster.name.clone(),
            broker_name: observed.broker_name,
            store_type: observed.store_type,
            max_lmq_count: observed.max_lmq_num,
            current_lmq_count: observed.current_lmq_num,
            lite_subscription_count: observed.lite_subscription_count,
            order_info_count: observed.order_info_count,
            consume_queue_count: observed.consume_queue_count,
            offset_count: observed.offset_count,
            event_count: observed.event_count,
            lite_topic_count: observed.lite_topic_count,
            lite_topics: observed
                .lite_topics
                .into_iter()
                .map(|topic| LiteTopicSummary {
                    topic: topic.topic,
                    expiration_minutes: topic.expiration_minutes,
                    bound_group_count: topic.bound_group_count,
                    bound_groups: topic.bound_groups,
                })
                .collect(),
            truncated: observed.truncated,
            generated_at: observed_at(),
        })
    }
}
