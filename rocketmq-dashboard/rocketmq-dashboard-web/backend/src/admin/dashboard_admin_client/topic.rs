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
use rocketmq_admin_core::core::dashboard::DashboardAdmin;
use rocketmq_admin_core::core::stable_error_message;
use rocketmq_admin_core::core::topic;
use rocketmq_admin_core::core::topic::GetTopicConfigRequest;
use rocketmq_admin_core::core::topic::OrderTopicConfigRequest;
use rocketmq_admin_core::core::topic::TopicAdmin;
use rocketmq_admin_core::core::topic::TopicCatalogRequest;

use super::*;
use crate::model::TopicConfigView;
use crate::model::TopicConsumerView;
use crate::model::TopicConsumersView;
use crate::model::TopicOperationResult;
use crate::model::TopicQueueOffsetView;
use crate::model::TopicTargetOptionView;
use crate::model::TopicTargetResult;
use crate::model::build_operation_result;

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
    ) -> Result<TopicOperationResult, DashboardError> {
        self.upsert_topic(request, TopicMutationKind::Update).await
    }

    pub async fn create_topic(&self, request: TopicMutationRequest) -> Result<TopicOperationResult, DashboardError> {
        self.upsert_topic(request, TopicMutationKind::Create).await
    }

    pub async fn delete_topic(&self, topic: &str) -> Result<MutationResult, DashboardError> {
        validate_name(topic, "Topic")?;
        let result = run_admin_rpc!(self, |admin| admin.dashboard_delete_topic(topic))?;
        Ok(MutationResult {
            message: result.message,
        })
    }
}

#[derive(Clone, Copy)]
enum TopicMutationKind {
    Create,
    Update,
}

impl DashboardAdminClient {
    async fn upsert_topic(
        &self,
        request: TopicMutationRequest,
        kind: TopicMutationKind,
    ) -> Result<TopicOperationResult, DashboardError> {
        let operation = match kind {
            TopicMutationKind::Create => "CREATE",
            TopicMutationKind::Update => "UPDATE",
        };
        run_topic_admin_rpc!(self, |admin| async {
            let catalog = map_topic_catalog(
                admin
                    .get_topic_catalog(&TopicCatalogRequest {
                        skip_system_topics: false,
                        skip_retry_and_dlq_topics: false,
                    })
                    .await?,
            );
            let mut executor = TopicAdminUpsertExecutor { admin };
            run_topic_upserts(catalog, request, kind, operation, &mut executor).await
        })
    }
}

trait TopicUpsertExecutor {
    async fn upsert(&mut self, request: &topic::UpsertTopicRequest) -> Result<String, AdminError>;

    async fn reconcile_order_config(&mut self, request: &OrderTopicConfigRequest) -> Result<String, AdminError>;
}

struct TopicAdminUpsertExecutor<'a, T> {
    admin: &'a mut T,
}

impl<T> TopicUpsertExecutor for TopicAdminUpsertExecutor<'_, T>
where
    T: TopicAdmin,
{
    async fn upsert(&mut self, request: &topic::UpsertTopicRequest) -> Result<String, AdminError> {
        self.admin
            .upsert_topic_without_order_reconcile(request)
            .await
            .map(|result| result.message)
    }

    async fn reconcile_order_config(&mut self, request: &OrderTopicConfigRequest) -> Result<String, AdminError> {
        self.admin
            .reconcile_order_topic_config(request)
            .await
            .map(|_| "Order topic configuration reconciled".to_string())
    }
}

async fn run_topic_upserts<E>(
    catalog: TopicListView,
    request: TopicMutationRequest,
    kind: TopicMutationKind,
    operation: &str,
    executor: &mut E,
) -> Result<TopicOperationResult, DashboardError>
where
    E: TopicUpsertExecutor,
{
    match kind {
        TopicMutationKind::Create => ensure_topic_does_not_exist(&catalog, &request.topic)?,
        TopicMutationKind::Update => require_mutable_topic(&catalog, &request.topic)?,
    }
    let broker_names = resolve_topic_targets(&catalog.targets, &request.cluster_name_list, &request.broker_name_list)?;
    let mut targets = Vec::with_capacity(broker_names.len());
    for broker_name in broker_names {
        let upsert_request = topic::UpsertTopicRequest {
            cluster_names: Vec::new(),
            broker_names: vec![broker_name.clone()],
            topic: request.topic.clone(),
            write_queue_nums: request.write_queue_count,
            read_queue_nums: request.read_queue_count,
            perm: request.perm,
            order: request.order.unwrap_or(false),
            message_type: request.message_type.clone(),
        };
        match executor.upsert(&upsert_request).await {
            Ok(message) => targets.push(TopicTargetResult::success(broker_name, message)),
            Err(error) => targets.push(TopicTargetResult::failure(broker_name, stable_error_message(&error))),
        }
    }
    let successful_brokers = targets
        .iter()
        .filter(|target| target.success)
        .map(|target| target.target.clone())
        .collect::<Vec<_>>();
    if !successful_brokers.is_empty() {
        let order_request = OrderTopicConfigRequest::try_new(
            request.topic.clone(),
            successful_brokers,
            request.write_queue_count,
            request.order.unwrap_or(false),
        )
        .map_err(DashboardError::from)?;
        match executor.reconcile_order_config(&order_request).await {
            Ok(_) => {}
            Err(error) => targets.push(TopicTargetResult::failure(
                "ORDER_TOPIC_CONFIG",
                stable_error_message(&error),
            )),
        }
    }
    Ok(build_operation_result(operation, request.topic, targets))
}

pub(crate) fn resolve_topic_targets(
    targets: &[TopicTargetOptionView],
    cluster_names: &[String],
    broker_names: &[String],
) -> Result<Vec<String>, DashboardError> {
    let mut resolved = std::collections::BTreeSet::new();
    for cluster_name in cluster_names {
        let cluster_name = cluster_name.trim();
        let target = targets
            .iter()
            .find(|target| target.cluster_name == cluster_name)
            .ok_or_else(|| DashboardError::Validation(format!("Unknown cluster target `{cluster_name}`")))?;
        resolved.extend(
            target
                .broker_names
                .iter()
                .map(|broker_name| broker_name.trim())
                .filter(|broker_name| !broker_name.is_empty())
                .map(str::to_string),
        );
    }
    for broker_name in broker_names {
        let broker_name = broker_name.trim();
        let canonical_broker_name = targets
            .iter()
            .flat_map(|target| target.broker_names.iter())
            .map(|candidate| candidate.trim())
            .find(|candidate| *candidate == broker_name && !candidate.is_empty())
            .ok_or_else(|| DashboardError::Validation(format!("Unknown broker target `{broker_name}`")))?;
        resolved.insert(canonical_broker_name.to_string());
    }
    if resolved.is_empty() {
        return Err(DashboardError::Validation(
            "Select at least one cluster or broker before saving the topic".to_string(),
        ));
    }
    Ok(resolved.into_iter().collect())
}

fn ensure_topic_does_not_exist(catalog: &TopicListView, topic: &str) -> Result<(), DashboardError> {
    if catalog.items.iter().any(|item| item.topic == topic) {
        return Err(DashboardError::Validation(format!("Topic `{topic}` already exists")));
    }
    Ok(())
}

fn require_mutable_topic(catalog: &TopicListView, topic: &str) -> Result<(), DashboardError> {
    let item = catalog.items.iter().find(|item| item.topic == topic).ok_or_else(|| {
        DashboardError::NotFound(format!(
            "Topic `{topic}` was not found in the authoritative topic catalog"
        ))
    })?;
    if item.system_topic {
        return Err(DashboardError::Validation(format!(
            "System topic `{}` cannot be modified",
            item.topic
        )));
    }
    Ok(())
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
    let mut target_brokers_by_cluster = std::collections::BTreeMap::<String, std::collections::BTreeSet<String>>::new();
    for target in catalog.targets {
        let cluster_name = target.cluster_name.trim();
        if cluster_name.is_empty() {
            continue;
        }
        target_brokers_by_cluster
            .entry(cluster_name.to_string())
            .or_default()
            .extend(
                target
                    .broker_names
                    .into_iter()
                    .map(|broker_name| broker_name.trim().to_string())
                    .filter(|broker_name| !broker_name.is_empty()),
            );
    }
    let targets = target_brokers_by_cluster
        .into_iter()
        .map(|(cluster_name, broker_names)| TopicTargetOptionView {
            cluster_name,
            broker_names: broker_names.into_iter().collect(),
        })
        .collect::<Vec<_>>();
    items.sort_unstable_by(|left, right| left.topic.cmp(&right.topic));
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
    use super::OrderTopicConfigRequest;
    use super::TopicMutationKind;
    use super::TopicUpsertExecutor;
    use super::map_topic_catalog;
    use super::map_topic_stats;
    use super::resolve_topic_targets;
    use super::run_topic_upserts;
    use super::topic_info_from_catalog;
    use crate::model::TopicListView;
    use crate::model::TopicMutationRequest;
    use crate::model::TopicTargetOptionView;
    use rocketmq_admin_core::core::AdminError;
    use rocketmq_admin_core::core::topic as core_topic;

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

    #[test]
    fn resolves_clusters_and_brokers_to_unique_canonical_brokers() {
        let targets = vec![TopicTargetOptionView {
            cluster_name: "DefaultCluster".into(),
            broker_names: vec!["broker-a".into(), "broker-b".into()],
        }];

        assert_eq!(
            resolve_topic_targets(&targets, &[" DefaultCluster ".into()], &["broker-a".into()]).unwrap(),
            vec!["broker-a", "broker-b"]
        );
    }

    #[test]
    fn canonicalizes_catalog_broker_names_when_resolving_targets() {
        let targets = vec![TopicTargetOptionView {
            cluster_name: "DefaultCluster".into(),
            broker_names: vec![" broker-a ".into(), "broker-a".into(), " broker-b".into()],
        }];

        assert_eq!(
            resolve_topic_targets(&targets, &["DefaultCluster".into()], &[]).unwrap(),
            vec!["broker-a", "broker-b"]
        );
    }

    #[test]
    fn catalog_mapping_merges_equivalent_cluster_target_entries() {
        let catalog = map_topic_catalog(core_topic::TopicCatalog {
            items: Vec::new(),
            targets: vec![
                core_topic::TopicTargetOption {
                    cluster_name: " DefaultCluster ".into(),
                    broker_names: vec![" broker-b ".into(), "broker-a".into()],
                },
                core_topic::TopicTargetOption {
                    cluster_name: "DefaultCluster".into(),
                    broker_names: vec!["broker-b".into()],
                },
            ],
        });

        assert_eq!(
            catalog.targets,
            vec![TopicTargetOptionView {
                cluster_name: "DefaultCluster".into(),
                broker_names: vec!["broker-a".into(), "broker-b".into()],
            }]
        );
    }

    #[tokio::test]
    async fn create_catalog_collision_runs_no_target_upserts() {
        let catalog = map_topic_catalog(core_topic::TopicCatalog {
            items: vec![core_topic::TopicCatalogItem {
                topic: "orders".into(),
                category: "NORMAL".into(),
                message_type: "NORMAL".into(),
                clusters: vec!["DefaultCluster".into()],
                brokers: vec!["broker-a".into()],
                read_queue_count: 8,
                write_queue_count: 8,
                perm: 6,
                order: false,
                system_topic: false,
            }],
            targets: vec![core_topic::TopicTargetOption {
                cluster_name: "DefaultCluster".into(),
                broker_names: vec!["broker-a".into()],
            }],
        });
        let mut executor = RecordingExecutor::default();

        let result = run_topic_upserts(
            catalog,
            mutation_request(vec!["broker-a".into()]),
            TopicMutationKind::Create,
            "CREATE",
            &mut executor,
        )
        .await;

        assert!(result.is_err());
        assert!(executor.calls.is_empty());
    }

    #[tokio::test]
    async fn target_upserts_continue_after_a_failure_in_canonical_order() {
        let catalog = map_topic_catalog(core_topic::TopicCatalog {
            items: vec![core_topic::TopicCatalogItem {
                topic: "orders".into(),
                category: "NORMAL".into(),
                message_type: "NORMAL".into(),
                clusters: vec!["DefaultCluster".into()],
                brokers: vec!["broker-a".into(), "broker-b".into()],
                read_queue_count: 8,
                write_queue_count: 8,
                perm: 6,
                order: false,
                system_topic: false,
            }],
            targets: vec![core_topic::TopicTargetOption {
                cluster_name: "DefaultCluster".into(),
                broker_names: vec!["broker-b".into(), "broker-a".into()],
            }],
        });
        let mut executor = RecordingExecutor {
            failing_target: Some("broker-a".into()),
            ..RecordingExecutor::default()
        };

        let result = run_topic_upserts(
            catalog,
            mutation_request(vec!["broker-b".into(), "broker-a".into(), "broker-a".into()]),
            TopicMutationKind::Update,
            "UPDATE",
            &mut executor,
        )
        .await
        .expect("per-target failures return a structured result");

        assert_eq!(executor.calls, vec!["broker-a", "broker-b"]);
        assert!(!result.success);
        assert_eq!(result.target_count, 2);
        assert_eq!(result.targets[0].target, "broker-a");
        assert!(!result.targets[0].success);
        assert_eq!(result.targets[1].target, "broker-b");
        assert!(result.targets[1].success);
    }

    #[tokio::test]
    async fn system_topic_update_runs_no_target_upserts() {
        let catalog = map_topic_catalog(core_topic::TopicCatalog {
            items: vec![core_topic::TopicCatalogItem {
                topic: "TBW102".into(),
                category: "SYSTEM".into(),
                message_type: "NORMAL".into(),
                clusters: vec!["DefaultCluster".into()],
                brokers: vec!["broker-a".into()],
                read_queue_count: 8,
                write_queue_count: 8,
                perm: 6,
                order: false,
                system_topic: true,
            }],
            targets: vec![core_topic::TopicTargetOption {
                cluster_name: "DefaultCluster".into(),
                broker_names: vec!["broker-a".into()],
            }],
        });
        let mut request = mutation_request(vec!["broker-a".into()]);
        request.topic = "TBW102".into();
        let mut executor = RecordingExecutor::default();

        let result = run_topic_upserts(catalog, request, TopicMutationKind::Update, "UPDATE", &mut executor).await;

        assert!(result.is_err());
        assert!(executor.calls.is_empty());
    }

    #[tokio::test]
    async fn ordered_topic_reconciles_all_successful_brokers_once() {
        let catalog = catalog_with_targets(&["broker-b", "broker-a"]);
        let mut request = mutation_request(vec!["broker-a".into(), "broker-b".into()]);
        request.order = Some(true);
        let mut executor = RecordingExecutor::default();

        let result = run_topic_upserts(catalog, request, TopicMutationKind::Update, "UPDATE", &mut executor)
            .await
            .expect("ordered topic mutation");

        assert!(result.success);
        assert_eq!(executor.order_reconciliations.len(), 1);
        assert_eq!(
            executor.order_reconciliations[0].broker_names,
            vec!["broker-a", "broker-b"]
        );
        assert!(executor.order_reconciliations[0].order);
    }

    #[tokio::test]
    async fn unordered_topic_removes_the_global_order_configuration() {
        let catalog = catalog_with_targets(&["broker-a"]);
        let mut executor = RecordingExecutor::default();

        let result = run_topic_upserts(
            catalog,
            mutation_request(vec!["broker-a".into()]),
            TopicMutationKind::Update,
            "UPDATE",
            &mut executor,
        )
        .await
        .expect("unordered topic mutation");

        assert!(result.success);
        assert_eq!(executor.order_reconciliations.len(), 1);
        assert!(!executor.order_reconciliations[0].order);
    }

    #[tokio::test]
    async fn order_reconciliation_failure_is_a_structured_global_failure() {
        let catalog = catalog_with_targets(&["broker-a"]);
        let mut executor = RecordingExecutor {
            reconcile_error: Some(AdminError::backend("order_reconcile", "unavailable")),
            ..RecordingExecutor::default()
        };

        let result = run_topic_upserts(
            catalog,
            mutation_request(vec!["broker-a".into()]),
            TopicMutationKind::Update,
            "UPDATE",
            &mut executor,
        )
        .await
        .expect("broker mutation remains structured");

        assert!(!result.success);
        assert!(
            result.targets.iter().any(|target| {
                target.target == "ORDER_TOPIC_CONFIG" && !target.success && !target.message.is_empty()
            })
        );
    }

    fn mutation_request(broker_name_list: Vec<String>) -> TopicMutationRequest {
        TopicMutationRequest {
            topic: "orders".into(),
            read_queue_count: 8,
            write_queue_count: 8,
            perm: 6,
            broker_name_list,
            cluster_name_list: Vec::new(),
            order: Some(false),
            message_type: Some("NORMAL".into()),
        }
    }

    fn catalog_with_targets(broker_names: &[&str]) -> TopicListView {
        map_topic_catalog(core_topic::TopicCatalog {
            items: vec![core_topic::TopicCatalogItem {
                topic: "orders".into(),
                category: "NORMAL".into(),
                message_type: "NORMAL".into(),
                clusters: vec!["DefaultCluster".into()],
                brokers: broker_names.iter().map(|broker| (*broker).to_string()).collect(),
                read_queue_count: 8,
                write_queue_count: 8,
                perm: 6,
                order: false,
                system_topic: false,
            }],
            targets: vec![core_topic::TopicTargetOption {
                cluster_name: "DefaultCluster".into(),
                broker_names: broker_names.iter().map(|broker| (*broker).to_string()).collect(),
            }],
        })
    }

    #[derive(Default)]
    struct RecordingExecutor {
        calls: Vec<String>,
        failing_target: Option<String>,
        order_reconciliations: Vec<OrderTopicConfigRequest>,
        reconcile_error: Option<AdminError>,
    }

    impl TopicUpsertExecutor for RecordingExecutor {
        async fn upsert(&mut self, request: &core_topic::UpsertTopicRequest) -> Result<String, AdminError> {
            let broker_name = request.broker_names.first().cloned().expect("one broker target");
            self.calls.push(broker_name.clone());
            if self.failing_target.as_deref() == Some(broker_name.as_str()) {
                Err(AdminError::backend("upsert_topic", "unavailable"))
            } else {
                Ok("saved".to_string())
            }
        }

        async fn reconcile_order_config(&mut self, request: &OrderTopicConfigRequest) -> Result<String, AdminError> {
            self.order_reconciliations.push(request.clone());
            match self.reconcile_error.take() {
                Some(error) => Err(error),
                None => Ok("reconciled".to_string()),
            }
        }
    }
}
