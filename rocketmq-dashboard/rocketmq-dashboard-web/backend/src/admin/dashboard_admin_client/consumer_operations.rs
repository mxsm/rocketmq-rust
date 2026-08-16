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

use rocketmq_admin_core::core::AdminError;
use rocketmq_admin_core::core::consumer as core_consumer;
use rocketmq_admin_core::core::consumer::ConsumerBatchMutationAdmin;
use rocketmq_admin_core::core::consumer::ConsumerQueryAdmin;

use super::*;
use crate::model::*;

impl DashboardAdminClient {
    pub async fn create_consumer_group(
        &self,
        group: &str,
        request: ConsumerUpsertView,
    ) -> Result<ConsumerOperationResult, DashboardError> {
        let mutation_guard = self.acquire_consumer_mutation_lock().await;
        self.upsert_consumer_group_with_guard(group, request, "CREATE", mutation_guard)
            .await
    }

    pub async fn update_consumer_group(
        &self,
        group: &str,
        request: ConsumerUpsertView,
    ) -> Result<ConsumerOperationResult, DashboardError> {
        let mutation_guard = self.acquire_consumer_mutation_lock().await;
        self.upsert_consumer_group_with_guard(group, request, "UPDATE", mutation_guard)
            .await
    }

    pub async fn delete_consumer_group(
        &self,
        group: &str,
        request: ConsumerDeleteView,
    ) -> Result<ConsumerOperationResult, DashboardError> {
        let group = validate_consumer_mutation_group(group)?;
        let selected_broker_names = canonicalize_consumer_broker_names(request.broker_names)?;
        let mutation_guard = self.acquire_consumer_mutation_lock().await;
        let operation_group = group.clone();
        let selected_for_call = selected_broker_names.clone();

        let batch_result = run_consumer_admin_rpc!(self, Some(mutation_guard), |admin| async move {
            let list_request = core_consumer::DashboardConsumerGroupListRequest {
                skip_sys_group: false,
                address: None,
            };
            let list = admin.query_dashboard_consumer_groups(&list_request).await?;
            let item = list
                .items
                .into_iter()
                .find(|item| item.raw_group_name == operation_group || item.display_group_name == operation_group)
                .ok_or_else(|| AdminError::not_found("consumerGroup", operation_group.clone()))?;
            let delete_request = core_consumer::ConsumerBatchDeleteRequest::try_new(
                operation_group,
                selected_for_call,
                item.broker_names,
            )?;
            admin.delete_consumer_group_batch(&delete_request).await
        })?;

        Ok(map_consumer_batch_result(batch_result, "DELETE", &group))
    }

    async fn upsert_consumer_group_with_guard(
        &self,
        group: &str,
        request: ConsumerUpsertView,
        operation: &str,
        mutation_guard: tokio::sync::OwnedMutexGuard<()>,
    ) -> Result<ConsumerOperationResult, DashboardError> {
        let group = validate_consumer_mutation_group(group)?;
        let core_request = core_consumer::DashboardConsumerUpsertRequest {
            cluster_name_list: request.cluster_name_list,
            broker_name_list: request.broker_name_list,
            consumer_group: group.clone(),
            consume_enable: request.consume_enable,
            consume_from_min_enable: request.consume_from_min_enable,
            consume_broadcast_enable: request.consume_broadcast_enable,
            consume_message_orderly: request.consume_message_orderly,
            retry_queue_nums: request.retry_queue_nums,
            retry_max_times: request.retry_max_times,
            broker_id: request.broker_id,
            which_broker_when_consume_slowly: request.which_broker_when_consume_slowly,
            notify_consumer_ids_changed_enable: request.notify_consumer_ids_changed_enable,
            group_sys_flag: request.group_sys_flag,
            consume_timeout_minute: request.consume_timeout_minute,
        };
        let batch_request = core_consumer::ConsumerBatchUpsertRequest::try_new(core_request)?;
        let batch_result = run_consumer_admin_rpc!(self, Some(mutation_guard), |admin| async move {
            admin.upsert_consumer_group_batch(&batch_request).await
        })?;

        Ok(map_consumer_batch_result(batch_result, operation, &group))
    }
}

fn validate_consumer_mutation_group(group: &str) -> Result<String, DashboardError> {
    let group = group.trim();
    if group.is_empty() {
        return Err(DashboardError::Validation("Consumer group is required".to_string()));
    }
    if group.starts_with("%SYS%") || local_is_system_consumer_group(group) {
        return Err(DashboardError::Validation(
            "System consumer groups cannot be mutated".to_string(),
        ));
    }
    Ok(group.to_string())
}

fn canonicalize_consumer_broker_names(names: Vec<String>) -> Result<Vec<String>, DashboardError> {
    let mut names = names
        .into_iter()
        .map(|name| name.trim().to_string())
        .filter(|name| !name.is_empty())
        .collect::<Vec<_>>();
    names.sort();
    names.dedup();
    if names.is_empty() {
        return Err(DashboardError::Validation(
            "Select at least one broker before deleting the consumer group".to_string(),
        ));
    }
    Ok(names)
}

fn map_consumer_batch_result(
    result: core_consumer::DashboardConsumerBatchResult,
    operation: &str,
    group: &str,
) -> ConsumerOperationResult {
    let targets = result
        .targets
        .into_iter()
        .map(|target| ConsumerTargetResult {
            target: target.target,
            kind: target.kind,
            success: target.success,
            message: target.message,
        })
        .collect::<Vec<_>>();
    let success = result.success && targets.iter().all(|target| target.success);
    let target_count = targets.len();
    ConsumerOperationResult {
        operation: operation.to_string(),
        consumer_group: group.to_string(),
        success,
        target_count,
        message: if success {
            format!("Consumer group `{group}` operation completed")
        } else {
            format!("Consumer group `{group}` operation completed with failed targets")
        },
        targets,
    }
}

fn local_is_system_consumer_group(group: &str) -> bool {
    group.starts_with("CID_RMQ_SYS_")
        || matches!(
            group,
            "TOOLS_CONSUMER"
                | "FILTERSRV_CONSUMER"
                | "SELF_TEST_C_GROUP"
                | "CID_ONS-HTTP-PROXY"
                | "CID_ONSAPI_PULL"
                | "CID_ONSAPI_PERMISSION"
                | "CID_ONSAPI_OWNER"
                | "CID_RMQ_SYS_TRANS"
                | "CID_DefaultHeartBeatSyncerTopic"
        )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn batch_result(targets: Vec<(&str, bool)>) -> core_consumer::DashboardConsumerBatchResult {
        core_consumer::DashboardConsumerBatchResult {
            consumer_group: "orders-consumer".to_string(),
            success: targets.iter().all(|(_, success)| *success),
            targets: targets
                .into_iter()
                .map(|(target, success)| core_consumer::DashboardConsumerTargetOutcome {
                    target: target.to_string(),
                    kind: "BROKER".to_string(),
                    success,
                    message: if success {
                        "ok".to_string()
                    } else {
                        "failed".to_string()
                    },
                })
                .collect(),
        }
    }

    #[test]
    fn operation_result_keeps_failed_targets_and_never_flattens_partial_success() {
        let view = map_consumer_batch_result(
            batch_result(vec![("broker-a", true), ("broker-b", false)]),
            "UPDATE",
            "orders-consumer",
        );
        assert_eq!(view.operation, "UPDATE");
        assert!(!view.success);
        assert_eq!(view.target_count, 2);
        assert_eq!(view.targets[1].target, "broker-b");
        assert!(!view.targets[1].success);
    }
}
