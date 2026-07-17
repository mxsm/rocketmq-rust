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
use rocketmq_model::topic::DLQ_GROUP_TOPIC_PREFIX;
use rocketmq_model::topic::RETRY_GROUP_TOPIC_PREFIX;
use rocketmq_protocol::common::wire_constants::MASTER_ID;
use rocketmq_protocol::protocol::body::broker_body::cluster_info::ClusterInfo;

use crate::core::consumer;
use crate::core::AdminError;
use crate::core::AdminResult;

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
