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

use rocketmq_admin_core::core::AdminError;
use rocketmq_admin_core::core::topic;
use rocketmq_admin_core::core::topic::TopicBatchDeleteAdmin;
use rocketmq_admin_core::core::topic::TopicBatchDeleteRequest;

use super::DashboardError;
use super::TopicListView;
use super::TopicOperationResult;
use super::TopicTargetResult;
use super::authoritative_topic;
use super::build_operation_result;
use super::ensure_topic_operation_allowed;

pub(super) trait TopicBatchDeleteExecutor {
    async fn delete_batch(
        &mut self,
        request: &TopicBatchDeleteRequest,
    ) -> Result<topic::TopicBatchDeleteOutcome, AdminError>;
}

impl<T> TopicBatchDeleteExecutor for T
where
    T: TopicBatchDeleteAdmin,
{
    async fn delete_batch(
        &mut self,
        request: &TopicBatchDeleteRequest,
    ) -> Result<topic::TopicBatchDeleteOutcome, AdminError> {
        self.delete_topic_batch(request).await
    }
}

pub(super) async fn run_topic_batch_delete<E>(
    catalog: TopicListView,
    topic: String,
    executor: &mut E,
) -> Result<TopicOperationResult, DashboardError>
where
    E: TopicBatchDeleteExecutor,
{
    let item = authoritative_topic(&catalog, &topic)?;
    ensure_topic_operation_allowed(item, "DELETE_TOPIC")?;
    let request = TopicBatchDeleteRequest::try_new(topic.clone(), item.clusters.clone())?;
    let outcome = executor.delete_batch(&request).await?;
    let mut targets = outcome
        .targets
        .into_iter()
        .map(|target| {
            if target.success {
                TopicTargetResult::success(target.broker_name, target.message)
            } else {
                TopicTargetResult::failure(target.broker_name, target.message)
            }
        })
        .collect::<Vec<_>>();
    if let Some(order_config) = outcome.order_config.filter(|result| !result.success) {
        targets.push(TopicTargetResult::failure("ORDER_TOPIC_CONFIG", order_config.message));
    }
    Ok(build_operation_result("DELETE_TOPIC", topic, targets))
}
