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

use rocketmq_admin_core::core::dashboard::DashboardAdmin;
use rocketmq_admin_core::core::topic;
use rocketmq_admin_core::core::topic::GetTopicConfigRequest;
use rocketmq_admin_core::core::topic::TopicAdmin;
use rocketmq_admin_core::core::topic::TopicCatalogRequest;

use super::*;
use crate::model::TopicConfigView;
use crate::model::TopicConsumerView;
use crate::model::TopicConsumersView;
use crate::model::TopicQueueOffsetView;
use crate::model::TopicTargetOptionView;

impl DashboardAdminClient {
    pub async fn list_topics(&self) -> Result<TopicListView, DashboardError> {
        let catalog = run_topic_admin_rpc!(self, |admin| admin.get_topic_catalog(&TopicCatalogRequest {
            skip_system_topics: false,
            skip_retry_and_dlq_topics: false,
        }))?;
        Ok(map_topic_catalog(catalog))
    }

    pub async fn get_topic(&self, topic: &str) -> Result<TopicInfo, DashboardError> {
        validate_name(topic, "Topic")?;
        let catalog = self.list_topics().await?;
        topic_info_from_catalog(&catalog, topic).ok_or_else(|| {
            DashboardError::NotFound(format!(
                "Topic `{topic}` was not found in the authoritative topic catalog"
            ))
        })
    }

    pub async fn topic_route(&self, topic: &str) -> Result<TopicRouteInfo, DashboardError> {
        validate_name(topic, "Topic")?;
        let route = run_admin_rpc!(self, |admin| admin.dashboard_topic_route(topic))?;
        Ok(map_topic_route(route))
    }

    pub async fn topic_stats(&self, topic: &str) -> Result<TopicStatsInfo, DashboardError> {
        validate_name(topic, "Topic")?;
        let topic = topic.to_string();
        let stats = run_topic_admin_rpc!(self, |admin| admin.get_topic_stats(&topic))?;
        Ok(map_topic_stats(stats))
    }

    pub async fn topic_config(
        &self,
        topic: &str,
        broker_name: Option<&str>,
    ) -> Result<TopicConfigView, DashboardError> {
        let request = GetTopicConfigRequest::try_new(topic, broker_name.map(str::to_string))?;
        let config = run_topic_admin_rpc!(self, |admin| admin.get_topic_config(&request))?;
        Ok(map_topic_config(config))
    }

    pub async fn topic_consumers(&self, topic: &str) -> Result<TopicConsumersView, DashboardError> {
        validate_name(topic, "Topic")?;
        let topic = topic.to_string();
        let consumers = run_topic_admin_rpc!(self, |admin| admin.get_topic_consumers(&topic))?;
        Ok(map_topic_consumers(consumers))
    }

    pub async fn create_or_update_topic(
        &self,
        request: TopicMutationRequest,
    ) -> Result<MutationResult, DashboardError> {
        validate_name(&request.topic, "Topic")?;
        if request.cluster_name_list.is_empty() && request.broker_name_list.is_empty() {
            return Err(DashboardError::Validation(
                "Select at least one cluster or broker before saving the topic".to_string(),
            ));
        }
        let request = core::DashboardTopicMutationRequest {
            topic: request.topic,
            read_queue_count: request.read_queue_count,
            write_queue_count: request.write_queue_count,
            perm: request.perm,
            broker_name_list: request.broker_name_list,
            cluster_name_list: request.cluster_name_list,
            order: request.order.unwrap_or(false),
            message_type: request.message_type,
        };
        let result = run_admin_rpc!(self, |admin| admin.dashboard_upsert_topic(&request))?;
        Ok(MutationResult {
            message: result.message,
        })
    }

    pub async fn delete_topic(&self, topic: &str) -> Result<MutationResult, DashboardError> {
        validate_name(topic, "Topic")?;
        let result = run_admin_rpc!(self, |admin| admin.dashboard_delete_topic(topic))?;
        Ok(MutationResult {
            message: result.message,
        })
    }
}

fn map_topic_catalog(catalog: topic::TopicCatalog) -> TopicListView {
    let mut items = catalog
        .items
        .into_iter()
        .map(|item| {
            let mut brokers = item.brokers;
            let mut clusters = item.clusters;
            brokers.sort_unstable();
            clusters.sort_unstable();
            TopicInfo {
                topic: item.topic,
                broker_name: brokers.first().cloned(),
                brokers,
                clusters,
                read_queue_count: item.read_queue_count,
                write_queue_count: item.write_queue_count,
                perm: item.perm.max(0) as u32,
                category: item.category,
                message_type: item.message_type,
                order: item.order,
                system_topic: item.system_topic,
            }
        })
        .collect::<Vec<_>>();
    let mut targets = catalog
        .targets
        .into_iter()
        .map(|target| {
            let mut broker_names = target.broker_names;
            broker_names.sort_unstable();
            TopicTargetOptionView {
                cluster_name: target.cluster_name,
                broker_names,
            }
        })
        .collect::<Vec<_>>();
    items.sort_unstable_by(|left, right| left.topic.cmp(&right.topic));
    targets.sort_unstable_by(|left, right| left.cluster_name.cmp(&right.cluster_name));
    TopicListView {
        total: items.len(),
        items,
        targets,
    }
}

fn map_topic_route(route: core::DashboardTopicRoute) -> TopicRouteInfo {
    TopicRouteInfo {
        topic: route.topic,
        brokers: route
            .brokers
            .into_iter()
            .map(|broker| TopicRouteBroker {
                broker_name: broker.broker_name,
                broker_addrs: broker.broker_addrs,
            })
            .collect(),
        queues: route
            .queues
            .into_iter()
            .map(|queue| TopicRouteQueue {
                broker_name: queue.broker_name,
                read_queue_nums: queue.read_queue_nums,
                write_queue_nums: queue.write_queue_nums,
                perm: queue.perm,
            })
            .collect(),
    }
}

fn topic_info_from_catalog(catalog: &TopicListView, topic: &str) -> Option<TopicInfo> {
    catalog.items.iter().find(|item| item.topic == topic).cloned()
}

fn map_topic_stats(stats: topic::TopicStats) -> TopicStatsInfo {
    let mut offsets = stats
        .offsets
        .into_iter()
        .map(|offset| TopicQueueOffsetView {
            broker_name: offset.broker_name,
            queue_id: offset.queue_id,
            min_offset: offset.min_offset,
            max_offset: offset.max_offset,
            last_update_timestamp: offset.last_update_timestamp,
        })
        .collect::<Vec<_>>();
    offsets.sort_unstable_by(|left, right| {
        left.broker_name
            .cmp(&right.broker_name)
            .then(left.queue_id.cmp(&right.queue_id))
    });
    let total_min_offset = offsets.iter().map(|offset| offset.min_offset).sum();
    let total_max_offset = offsets.iter().map(|offset| offset.max_offset).sum();
    TopicStatsInfo {
        topic: stats.topic,
        queue_count: stats.queue_count,
        total_message_count: stats.total_message_count,
        total_min_offset,
        total_max_offset,
        offsets,
    }
}

fn map_topic_config(config: topic::TopicConfigDetail) -> TopicConfigView {
    let mut broker_name_list = config.broker_name_list;
    let mut cluster_name_list = config.cluster_name_list;
    broker_name_list.sort_unstable();
    cluster_name_list.sort_unstable();
    TopicConfigView {
        topic_name: config.topic_name,
        broker_name: config.broker_name,
        cluster_name: config.cluster_name,
        broker_name_list,
        cluster_name_list,
        read_queue_nums: config.read_queue_nums,
        write_queue_nums: config.write_queue_nums,
        perm: config.perm,
        order: config.order,
        message_type: config.message_type,
        attributes: config.attributes,
        inconsistent_fields: config.inconsistent_fields,
    }
}

fn map_topic_consumers(consumers: topic::TopicConsumers) -> TopicConsumersView {
    let mut items = consumers
        .items
        .into_iter()
        .map(|consumer| TopicConsumerView {
            consumer_group: consumer.consumer_group,
            total_diff: consumer.total_diff,
            inflight_diff: consumer.inflight_diff,
            consume_tps: consumer.consume_tps,
        })
        .collect::<Vec<_>>();
    items.sort_unstable_by(|left, right| left.consumer_group.cmp(&right.consumer_group));
    TopicConsumersView { items }
}

#[cfg(test)]
mod tests {
    use rocketmq_admin_core::core::topic as core_topic;

    use super::map_topic_catalog;
    use super::map_topic_stats;
    use super::topic_info_from_catalog;

    #[test]
    fn maps_core_stats_without_losing_queue_identity() {
        let view = map_topic_stats(core_topic::TopicStats {
            topic: "orders".into(),
            total_message_count: 9,
            queue_count: 1,
            offsets: vec![core_topic::TopicQueueOffset {
                broker_name: "broker-a".into(),
                queue_id: 2,
                min_offset: 3,
                max_offset: 12,
                last_update_timestamp: 1_700_000_000_000,
            }],
        });
        assert_eq!(view.total_message_count, 9);
        assert_eq!(view.offsets[0].queue_id, 2);
    }

    #[test]
    fn topic_detail_reuses_authoritative_catalog_metadata() {
        let catalog = map_topic_catalog(core_topic::TopicCatalog {
            items: vec![core_topic::TopicCatalogItem {
                topic: "orders".to_string(),
                category: "NORMAL".to_string(),
                message_type: "MIXED".to_string(),
                clusters: vec!["DefaultCluster".to_string()],
                brokers: vec!["broker-a".to_string()],
                read_queue_count: 8,
                write_queue_count: 12,
                perm: 6,
                order: true,
                system_topic: false,
            }],
            targets: Vec::new(),
        });

        let detail = topic_info_from_catalog(&catalog, "orders").expect("topic catalog entry");

        assert_eq!(detail.clusters, ["DefaultCluster"]);
        assert_eq!(detail.message_type, "MIXED");
        assert!(detail.order);
        assert_eq!(detail.category, "NORMAL");
    }
}
