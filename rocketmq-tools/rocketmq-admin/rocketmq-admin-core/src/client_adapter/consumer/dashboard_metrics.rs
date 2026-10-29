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

//! Complete Consumer backlog evidence for Dashboard aggregation.

use super::{promote_to_admin_error, workspace::discover_groups};
use crate::client_adapter::lifecycle::AdminSession;
use rocketmq_protocol::common::wire_constants::MASTER_ID;

pub(crate) async fn dashboard_consumer_summary(
    session: &AdminSession,
) -> crate::core::AdminResult<crate::core::dashboard_metrics::DashboardConsumerSummary> {
    session.ensure_open()?;
    let cluster = rocketmq_client_rust::MQAdminReadExt::examine_broker_cluster_info(&session.inner)
        .await
        .map_err(|source| promote_to_admin_error("examine_broker_cluster_info", source))?;
    let discovery = discover_groups(&session.inner, &cluster).await;
    // Missing master addresses are absent from the discovery helper's query list.
    let missing_masters = cluster
        .broker_addr_table
        .as_ref()
        .into_iter()
        .flat_map(|table| table.values())
        .filter(|broker| !broker.broker_addrs().contains_key(&MASTER_ID))
        .count();
    let discovery_complete =
        cluster.broker_addr_table.is_some() && discovery.failures.is_empty() && missing_masters == 0;
    let mut failed_targets =
        discovery.failures.len() + missing_masters + usize::from(cluster.broker_addr_table.is_none());
    let mut complete_backlog = discovery_complete.then_some(0_i64);
    for (group, meta) in &discovery.groups {
        if meta.broker_addresses.is_empty() {
            failed_targets += 1;
            complete_backlog = None;
        }
        for address in &meta.broker_addresses {
            let observation = rocketmq_client_rust::MQAdminReadExt::examine_consume_stats_with_evidence(
                &session.inner,
                group.as_str().into(),
                None,
                Some(address.as_str().into()),
                None,
            )
            .await;
            match observation {
                Ok(observation) => match observed_target_backlog(&observation) {
                    Some(value) => {
                        complete_backlog = complete_backlog.and_then(|total| total.checked_add(value));
                    }
                    None => {
                        failed_targets += observation.failures.len().max(1);
                        complete_backlog = None;
                    }
                },
                Err(_) => {
                    failed_targets += 1;
                    complete_backlog = None;
                }
            }
        }
    }
    Ok(crate::core::dashboard_metrics::DashboardConsumerSummary {
        group_count: discovery_complete.then_some(discovery.groups.len()),
        complete_backlog,
        failed_targets,
    })
}

fn observed_target_backlog(observation: &rocketmq_client_rust::ConsumeStatsReadResult) -> Option<i64> {
    if observation.attempted_brokers != 1 || observation.successful_brokers != 1 || !observation.failures.is_empty() {
        return None;
    }
    observation
        .stats
        .get_offset_table()
        .values()
        .try_fold(0_i64, |total, offset| {
            offset
                .get_broker_offset()
                .checked_sub(offset.get_consumer_offset())
                .and_then(|lag| total.checked_add(lag))
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn dashboard_backlog_requires_complete_targets_and_preserves_zero_and_negative_lag() {
        use rocketmq_client_rust::ConsumeStatsReadResult;
        use rocketmq_model::message::MessageQueue;
        use rocketmq_protocol::protocol::admin::{consume_stats::ConsumeStats, offset_wrapper::OffsetWrapper};
        let mut stats = ConsumeStats::new();
        let mut offset = OffsetWrapper::new();
        offset.set_broker_offset(5);
        offset.set_consumer_offset(5);
        stats
            .get_offset_table_mut()
            .insert(MessageQueue::from_parts("orders", "broker", 0), offset.clone());
        let mut evidence = ConsumeStatsReadResult {
            stats,
            attempted_brokers: 1,
            successful_brokers: 1,
            failures: Vec::new(),
        };
        assert_eq!(observed_target_backlog(&evidence), Some(0));
        offset.set_consumer_offset(7);
        evidence
            .stats
            .get_offset_table_mut()
            .insert(MessageQueue::from_parts("orders", "broker", 0), offset);
        assert_eq!(observed_target_backlog(&evidence), Some(-2));
        evidence.attempted_brokers = 2;
        assert_eq!(observed_target_backlog(&evidence), None);
        evidence.attempted_brokers = 0;
        evidence.successful_brokers = 0;
        assert_eq!(observed_target_backlog(&evidence), None);
    }
}
