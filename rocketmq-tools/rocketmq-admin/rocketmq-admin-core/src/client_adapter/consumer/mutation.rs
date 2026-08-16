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

use std::collections::HashSet;

use cheetah_string::CheetahString;
use rocketmq_client_rust::{ConsumerAdmin as _, RouteAdmin as _, TopicAdmin as _};
use rocketmq_model::topic::DLQ_GROUP_TOPIC_PREFIX;
use rocketmq_model::topic::RETRY_GROUP_TOPIC_PREFIX;
use rocketmq_protocol::common::wire_constants::MASTER_ID;
use rocketmq_protocol::protocol::body::broker_body::cluster_info::ClusterInfo;
use rocketmq_protocol::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;

use crate::client_adapter::lifecycle::AdminSession;
use crate::core::consumer;
use crate::core::consumer::ConsumerBatchMutationAdmin;
use crate::core::stable_error_message;
use crate::core::AdminError;
use crate::core::AdminFuture;
use crate::core::AdminResult;

use super::backend_error;

struct AdminSessionBatchExecutor<'a> {
    session: &'a mut AdminSession,
    cluster_info: ClusterInfo,
}

impl ConsumerBatchExecutor for AdminSessionBatchExecutor<'_> {
    fn resolve_upsert_targets<'a>(
        &'a mut self,
        request: &'a consumer::DashboardConsumerUpsertRequest,
    ) -> AdminFuture<'a, Vec<String>> {
        Box::pin(async move {
            let broker_names = resolve_consumer_target_broker_names(
                &self.cluster_info,
                &request.cluster_name_list,
                &request.broker_name_list,
            )?;
            for broker_name in &broker_names {
                if resolve_master_broker_addr(&self.cluster_info, broker_name).is_none() {
                    return Err(AdminError::invalid_argument(
                        "brokerNameList",
                        format!("Broker `{broker_name}` does not have a reachable master address."),
                    ));
                }
            }
            Ok(broker_names)
        })
    }

    fn validate_delete_targets<'a>(&'a mut self, targets: &'a [String]) -> AdminFuture<'a, ()> {
        Box::pin(async move {
            for broker_name in targets {
                if resolve_master_broker_addr(&self.cluster_info, broker_name).is_none() {
                    return Err(AdminError::invalid_argument(
                        "allBrokerNames",
                        format!("Broker `{broker_name}` does not have a reachable master address."),
                    ));
                }
            }
            Ok(())
        })
    }

    fn upsert_target<'a>(
        &'a mut self,
        target: &'a str,
        request: &'a consumer::DashboardConsumerUpsertRequest,
    ) -> AdminFuture<'a, ()> {
        Box::pin(async move {
            let broker_addr = resolve_master_broker_addr(&self.cluster_info, target).ok_or_else(|| {
                AdminError::invalid_argument(
                    "brokerNameList",
                    format!("Broker `{target}` does not have a reachable master address."),
                )
            })?;
            let mut config = SubscriptionGroupConfig::default();
            config.set_group_name(CheetahString::from(request.consumer_group.as_str()));
            config.set_consume_enable(request.consume_enable);
            config.set_consume_from_min_enable(request.consume_from_min_enable);
            config.set_consume_broadcast_enable(request.consume_broadcast_enable);
            config.set_consume_message_orderly(request.consume_message_orderly);
            config.set_retry_queue_nums(request.retry_queue_nums);
            config.set_retry_max_times(request.retry_max_times);
            config.set_broker_id(request.broker_id);
            config.set_which_broker_when_consume_slowly(request.which_broker_when_consume_slowly);
            config.set_notify_consumer_ids_changed_enable(request.notify_consumer_ids_changed_enable);
            config.set_group_sys_flag(request.group_sys_flag);
            config.set_consume_timeout_minute(request.consume_timeout_minute);
            self.session
                .inner
                .create_and_update_subscription_group_config(broker_addr, config)
                .await
                .map_err(|error| backend_error("create_and_update_subscription_group_config", error))
        })
    }

    fn delete_target<'a>(&'a mut self, target: &'a str, group: &'a str) -> AdminFuture<'a, ()> {
        Box::pin(async move {
            let broker_addr = resolve_master_broker_addr(&self.cluster_info, target).ok_or_else(|| {
                AdminError::invalid_argument(
                    "selectedBrokerNames",
                    format!("Broker `{target}` does not have a reachable master address."),
                )
            })?;
            self.session
                .inner
                .delete_subscription_group(broker_addr, CheetahString::from(group), Some(true))
                .await
                .map_err(|error| backend_error("delete_subscription_group", error))
        })
    }

    fn cleanup_internal_topic<'a>(&'a mut self, target: &'a str, broker_names: &'a [String]) -> AdminFuture<'a, ()> {
        Box::pin(async move {
            let mut broker_addresses = HashSet::with_capacity(broker_names.len());
            for broker_name in broker_names {
                let broker_addr = resolve_master_broker_addr(&self.cluster_info, broker_name).ok_or_else(|| {
                    AdminError::invalid_argument(
                        "allBrokerNames",
                        format!("Broker `{broker_name}` does not have a reachable master address."),
                    )
                })?;
                broker_addresses.insert(broker_addr);
            }
            self.session
                .inner
                .delete_topic_in_broker(broker_addresses, CheetahString::from(target))
                .await
                .map_err(|error| backend_error("delete_topic_in_broker", error))?;
            let namesrv_targets = self
                .session
                .inner
                .get_name_server_address_list()
                .await
                .into_iter()
                .collect::<HashSet<_>>();
            self.session
                .inner
                .delete_topic_in_name_server(namesrv_targets, None, CheetahString::from(target))
                .await
                .map_err(|error| backend_error("delete_topic_in_name_server", error))
        })
    }
}

impl ConsumerBatchMutationAdmin for AdminSession {
    fn upsert_consumer_group_batch<'a>(
        &'a mut self,
        request: &'a consumer::ConsumerBatchUpsertRequest,
    ) -> AdminFuture<'a, consumer::DashboardConsumerBatchResult> {
        Box::pin(async move {
            self.ensure_open()?;
            let request = consumer::ConsumerBatchUpsertRequest::try_new(request.inner().clone())?;
            let cluster_info = self
                .inner
                .examine_broker_cluster_info()
                .await
                .map_err(|error| backend_error("examine_broker_cluster_info", error))?;
            let mut executor = AdminSessionBatchExecutor {
                session: self,
                cluster_info,
            };
            run_consumer_upsert_batch(request, &mut executor).await
        })
    }

    fn delete_consumer_group_batch<'a>(
        &'a mut self,
        request: &'a consumer::ConsumerBatchDeleteRequest,
    ) -> AdminFuture<'a, consumer::DashboardConsumerBatchResult> {
        Box::pin(async move {
            self.ensure_open()?;
            let request = consumer::ConsumerBatchDeleteRequest::try_new(
                request.consumer_group(),
                request.selected_broker_names().to_vec(),
                request.all_broker_names().to_vec(),
            )?;
            let cluster_info = self
                .inner
                .examine_broker_cluster_info()
                .await
                .map_err(|error| backend_error("examine_broker_cluster_info", error))?;
            let mut executor = AdminSessionBatchExecutor {
                session: self,
                cluster_info,
            };
            run_consumer_delete_batch(request, &mut executor).await
        })
    }
}

pub(super) trait ConsumerBatchExecutor {
    fn resolve_upsert_targets<'a>(
        &'a mut self,
        request: &'a consumer::DashboardConsumerUpsertRequest,
    ) -> AdminFuture<'a, Vec<String>>;

    fn validate_delete_targets<'a>(&'a mut self, targets: &'a [String]) -> AdminFuture<'a, ()>;

    fn upsert_target<'a>(
        &'a mut self,
        target: &'a str,
        request: &'a consumer::DashboardConsumerUpsertRequest,
    ) -> AdminFuture<'a, ()>;

    fn delete_target<'a>(&'a mut self, target: &'a str, group: &'a str) -> AdminFuture<'a, ()>;

    fn cleanup_internal_topic<'a>(&'a mut self, target: &'a str, broker_names: &'a [String]) -> AdminFuture<'a, ()>;
}

pub(super) async fn run_consumer_upsert_batch<Executor>(
    request: consumer::ConsumerBatchUpsertRequest,
    executor: &mut Executor,
) -> AdminResult<consumer::DashboardConsumerBatchResult>
where
    Executor: ConsumerBatchExecutor + ?Sized,
{
    let request = consumer::ConsumerBatchUpsertRequest::try_new(request.inner().clone())?;
    let mut targets = executor.resolve_upsert_targets(request.inner()).await?;
    for target in &mut targets {
        *target = target.trim().to_string();
    }
    targets.retain(|target| !target.is_empty());
    targets.sort();
    targets.dedup();
    if targets.is_empty() {
        return Err(AdminError::invalid_argument(
            "brokerNameList",
            "Select at least one cluster or broker before saving the consumer group.",
        ));
    }

    let mut outcomes = Vec::with_capacity(targets.len());
    for target in targets {
        let result = executor.upsert_target(&target, request.inner()).await;
        outcomes.push(target_outcome(target, "BROKER", "Consumer group updated.", result));
    }
    let success = outcomes.iter().all(|outcome| outcome.success);
    Ok(consumer::DashboardConsumerBatchResult {
        consumer_group: request.inner().consumer_group.clone(),
        success,
        targets: outcomes,
    })
}

pub(super) async fn run_consumer_delete_batch<Executor>(
    request: consumer::ConsumerBatchDeleteRequest,
    executor: &mut Executor,
) -> AdminResult<consumer::DashboardConsumerBatchResult>
where
    Executor: ConsumerBatchExecutor + ?Sized,
{
    let request = consumer::ConsumerBatchDeleteRequest::try_new(
        request.consumer_group(),
        request.selected_broker_names().to_vec(),
        request.all_broker_names().to_vec(),
    )?;
    executor.validate_delete_targets(request.all_broker_names()).await?;

    let mut outcomes = Vec::with_capacity(request.selected_broker_names().len() + 2);
    for target in request.selected_broker_names() {
        let result = executor.delete_target(target, request.consumer_group()).await;
        outcomes.push(target_outcome(
            target.clone(),
            "BROKER",
            "Consumer group deleted.",
            result,
        ));
    }

    let all_brokers_succeeded = outcomes.iter().all(|outcome| outcome.success);
    let all_real_targets_selected = request.selected_broker_names() == request.all_broker_names();
    if all_real_targets_selected && all_brokers_succeeded {
        for topic in consumer_internal_topics(request.consumer_group()) {
            let result = executor
                .cleanup_internal_topic(&topic, request.all_broker_names())
                .await;
            outcomes.push(target_outcome(
                topic,
                "INTERNAL_TOPIC_CLEANUP",
                "Internal consumer topic deleted.",
                result,
            ));
        }
    }

    let success = outcomes.iter().all(|outcome| outcome.success);
    Ok(consumer::DashboardConsumerBatchResult {
        consumer_group: request.consumer_group().to_string(),
        success,
        targets: outcomes,
    })
}

fn target_outcome(
    target: String,
    kind: &str,
    success_message: &str,
    result: AdminResult<()>,
) -> consumer::DashboardConsumerTargetOutcome {
    match result {
        Ok(()) => consumer::DashboardConsumerTargetOutcome {
            target,
            kind: kind.to_string(),
            success: true,
            message: success_message.to_string(),
        },
        Err(error) => consumer::DashboardConsumerTargetOutcome {
            target,
            kind: kind.to_string(),
            success: false,
            message: stable_error_message(&error),
        },
    }
}

pub(super) fn resolve_master_broker_addr(cluster_info: &ClusterInfo, broker_name: &str) -> Option<CheetahString> {
    cluster_info
        .broker_addr_table
        .as_ref()
        .and_then(|table| table.get(broker_name))
        .and_then(|broker| broker.broker_addrs().get(&MASTER_ID).cloned())
}

pub(super) fn resolve_consumer_target_broker_names(
    cluster_info: &ClusterInfo,
    cluster_names: &[String],
    broker_names: &[String],
) -> AdminResult<Vec<String>> {
    let mut targets = HashSet::new();
    if let Some(cluster_table) = cluster_info.cluster_addr_table.as_ref() {
        for cluster_name in cluster_names {
            let cluster_name = cluster_name.trim();
            if cluster_name.is_empty() {
                continue;
            }
            let brokers = cluster_table.get(cluster_name).ok_or_else(|| {
                AdminError::invalid_argument(
                    "clusterNameList",
                    format!("Cluster `{cluster_name}` was not found in the current cluster view."),
                )
            })?;
            targets.extend(brokers.iter().map(ToString::to_string));
        }
    }
    for broker_name in broker_names {
        let broker_name = broker_name.trim();
        if broker_name.is_empty() {
            continue;
        }
        if resolve_master_broker_addr(cluster_info, broker_name).is_none() {
            return Err(AdminError::invalid_argument(
                "brokerNameList",
                format!("Broker `{broker_name}` was not found in the current cluster view."),
            ));
        }
        targets.insert(broker_name.to_string());
    }
    if targets.is_empty() {
        return Err(AdminError::invalid_argument(
            "brokerNameList",
            "Select at least one cluster or broker before saving the consumer group.",
        ));
    }
    let mut values = targets.into_iter().collect::<Vec<_>>();
    values.sort();
    Ok(values)
}

pub(super) fn validate_consumer_limits(request: &consumer::DashboardConsumerUpsertRequest) -> AdminResult<()> {
    if request.retry_queue_nums < 0 {
        return Err(AdminError::invalid_argument(
            "retryQueueNums",
            "Retry queues must be zero or greater.",
        ));
    }
    if request.retry_max_times < -1 {
        return Err(AdminError::invalid_argument(
            "retryMaxTimes",
            "Max retries must be -1 or greater.",
        ));
    }
    if request.consume_timeout_minute <= 0 {
        return Err(AdminError::invalid_argument(
            "consumeTimeoutMinute",
            "Consume timeout must be greater than zero.",
        ));
    }
    Ok(())
}

pub(super) fn consumer_internal_topics(group: &str) -> [String; 2] {
    [
        format!("{RETRY_GROUP_TOPIC_PREFIX}{group}"),
        format!("{DLQ_GROUP_TOPIC_PREFIX}{group}"),
    ]
}
