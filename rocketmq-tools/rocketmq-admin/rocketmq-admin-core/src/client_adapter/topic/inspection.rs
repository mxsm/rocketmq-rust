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

use super::*;
use crate::core::topic::{
    DetailedTopicCatalog, DetailedTopicCatalogItem, DetailedTopicConfig, DetailedTopicConfigTarget,
    DetailedTopicConsumers, DetailedTopicStats, TopicBrokerTarget, TopicInspectionAdmin, TopicInspectionCompleteness,
    TopicInspectionFailure, TopicInspectionFailureCode, TopicInspectionStage,
};
use rocketmq_client_rust::DefaultMQAdminExt;

impl TopicInspectionAdmin for AdminSession {
    fn inspect_topic_catalog<'a>(&'a self, request: &'a TopicCatalogRequest) -> AdminFuture<'a, DetailedTopicCatalog> {
        Box::pin(async move {
            self.ensure_open()?;
            let topic_list = rocketmq_client_rust::MQAdminReadExt::fetch_all_topic_list(&self.inner)
                .await
                .map_err(|error| backend_error("fetch_all_topic_list", error))?;
            let cluster_info = rocketmq_client_rust::MQAdminReadExt::examine_broker_cluster_info(&self.inner)
                .await
                .map_err(|error| backend_error("examine_broker_cluster_info", error))?;
            let targets = cluster_targets_from_cluster_info(&cluster_info);
            let broker_targets = broker_targets_from_cluster_info(&cluster_info);
            let mut failures = Vec::new();
            let mut successful_target_count = 0usize;
            let mut topic_configs = HashMap::<String, Vec<TopicBrokerConfigSnapshot>>::new();

            for target in &broker_targets {
                match self
                    .inner
                    .get_all_topic_config(CheetahString::from(target.broker_addr.as_str()), SEND_TIMEOUT_MILLIS)
                    .await
                {
                    Ok(wrapper) => {
                        successful_target_count += 1;
                        if let Some(config_table) = wrapper.topic_config_table() {
                            for (topic, config) in config_table {
                                topic_configs
                                    .entry(topic.to_string())
                                    .or_default()
                                    .push(TopicBrokerConfigSnapshot {
                                        broker_name: target.broker_name.clone(),
                                        cluster_name: Some(target.cluster_name.clone()),
                                        config: config.clone(),
                                    });
                            }
                        }
                    }
                    Err(error) => failures.push(rocketmq_failure(
                        target.broker_name.clone(),
                        TopicInspectionStage::CatalogConfig,
                        &error,
                    )),
                }
            }
            for configs in topic_configs.values_mut() {
                configs.sort_by(|left, right| left.broker_name.cmp(&right.broker_name));
            }
            let config_complete = failures.is_empty();

            let mut topics = topic_list
                .topic_list
                .into_iter()
                .map(|topic| topic.to_string())
                .collect::<Vec<_>>();
            topics.sort();
            let mut items = Vec::with_capacity(topics.len());
            for topic in topics {
                let configs = topic_configs.get(&topic).map(Vec::as_slice);
                let (category, system_topic) = safe_topic_category(&topic);
                if request.skip_system_topics && system_topic {
                    continue;
                }
                if request.skip_retry_and_dlq_topics && matches!(category, "RETRY" | "DLQ") {
                    continue;
                }
                let route_summary = match rocketmq_client_rust::MQAdminReadExt::examine_topic_route_info(
                    &self.inner,
                    CheetahString::from(topic.as_str()),
                )
                .await
                {
                    Ok(Some(route)) => {
                        successful_target_count += 1;
                        Some(summarize_route(&route))
                    }
                    Ok(None) => {
                        failures.push(stable_failure(
                            topic.clone(),
                            TopicInspectionStage::CatalogRoute,
                            TopicInspectionFailureCode::NotFound,
                            false,
                        ));
                        None
                    }
                    Err(error) => {
                        failures.push(rocketmq_failure(
                            topic.clone(),
                            TopicInspectionStage::CatalogRoute,
                            &error,
                        ));
                        None
                    }
                };
                let (clusters, brokers, read_queue_count, write_queue_count, perm) = route_summary
                    .map(|(clusters, brokers, read, write, perm)| {
                        (clusters, brokers, Some(read), Some(write), Some(perm))
                    })
                    .unwrap_or_default();
                let message_type = if config_complete {
                    configs.and_then(summarize_message_type)
                } else {
                    None
                };
                let order = if config_complete && configs.is_some() {
                    Some(summarize_order(configs))
                } else {
                    None
                };
                items.push(DetailedTopicCatalogItem {
                    topic,
                    category: category.to_string(),
                    message_type,
                    clusters,
                    brokers,
                    read_queue_count,
                    write_queue_count,
                    perm,
                    order,
                    system_topic,
                });
            }

            ensure_some_detailed_success("inspect_topic_catalog", successful_target_count, &failures)?;
            Ok(DetailedTopicCatalog {
                items,
                targets,
                broker_targets,
                completeness: completeness(successful_target_count, failures.len()),
                failures,
            })
        })
    }

    fn inspect_topic_route<'a>(&'a self, request: &'a GetTopicRouteRequest) -> AdminFuture<'a, Option<TopicRoute>> {
        Box::pin(async move {
            self.ensure_open()?;
            rocketmq_client_rust::MQAdminReadExt::examine_topic_route_info(
                &self.inner,
                CheetahString::from(request.topic.as_str()),
            )
            .await
            .map(|route| route.map(map_topic_route))
            .map_err(|error| backend_error("examine_topic_route_info", error))
        })
    }

    fn inspect_topic_stats<'a>(&'a self, topic: &'a str) -> AdminFuture<'a, DetailedTopicStats> {
        Box::pin(async move {
            self.ensure_open()?;
            let route = require_topic_route(&self.inner, topic).await?;
            let mut stats = TopicStatsTable::new();
            let mut failures = Vec::new();
            let mut successful_target_count = 0usize;
            for broker in &route.broker_datas {
                let Some(master_addr) = broker.broker_addrs().get(&MASTER_ID) else {
                    failures.push(stable_failure(
                        broker.broker_name().to_string(),
                        TopicInspectionStage::Stats,
                        TopicInspectionFailureCode::InvalidData,
                        false,
                    ));
                    continue;
                };
                match <DefaultMQAdminExt as rocketmq_client_rust::TopicAdmin>::examine_topic_stats(
                    &self.inner,
                    CheetahString::from(topic),
                    Some(master_addr.clone()),
                )
                .await
                {
                    Ok(broker_stats) => {
                        stats.get_offset_table_mut().extend(broker_stats.into_offset_table());
                        successful_target_count += 1;
                    }
                    Err(error) => failures.push(rocketmq_failure(
                        broker.broker_name().to_string(),
                        TopicInspectionStage::Stats,
                        &error,
                    )),
                }
            }
            finish_detailed_topic_stats(topic, &stats, successful_target_count, failures)
        })
    }

    fn inspect_topic_config<'a>(&'a self, topic: &'a str) -> AdminFuture<'a, DetailedTopicConfig> {
        Box::pin(async move {
            self.ensure_open()?;
            let route = require_topic_route(&self.inner, topic).await?;
            let mut targets = Vec::new();
            let mut failures = Vec::new();
            for broker in &route.broker_datas {
                let Some(master_addr) = broker.broker_addrs().get(&MASTER_ID) else {
                    failures.push(stable_failure(
                        broker.broker_name().to_string(),
                        TopicInspectionStage::Configuration,
                        TopicInspectionFailureCode::InvalidData,
                        false,
                    ));
                    continue;
                };
                match rocketmq_client_rust::MQAdminReadExt::topic_config_with_version(
                    &self.inner,
                    master_addr.clone(),
                    CheetahString::from(topic),
                )
                .await
                {
                    Ok(snapshot) => targets.push(DetailedTopicConfigTarget {
                        topic_name: topic.to_string(),
                        broker_name: broker.broker_name().to_string(),
                        broker_addr: master_addr.to_string(),
                        cluster_name: broker.cluster().to_string(),
                        version: snapshot.version,
                        read_queue_nums: snapshot.config.read_queue_nums,
                        write_queue_nums: snapshot.config.write_queue_nums,
                        perm: snapshot.config.perm,
                        order: snapshot.config.order,
                        message_type: snapshot.config.get_topic_message_type().to_string(),
                    }),
                    Err(error) => failures.push(rocketmq_failure(
                        broker.broker_name().to_string(),
                        TopicInspectionStage::Configuration,
                        &error,
                    )),
                }
            }
            targets.sort_by(|left, right| left.broker_name.cmp(&right.broker_name));
            ensure_some_detailed_success("inspect_topic_config", targets.len(), &failures)?;
            Ok(DetailedTopicConfig {
                topic: topic.to_string(),
                inconsistent_fields: detailed_inconsistent_fields(&targets),
                completeness: completeness(targets.len(), failures.len()),
                targets,
                failures,
            })
        })
    }

    fn inspect_topic_consumers<'a>(&'a self, topic: &'a str) -> AdminFuture<'a, DetailedTopicConsumers> {
        Box::pin(async move {
            self.ensure_open()?;
            let groups = rocketmq_client_rust::MQAdminReadExt::query_topic_consume_by_who(
                &self.inner,
                CheetahString::from(topic),
            )
            .await
            .map_err(|error| backend_error("query_topic_consume_by_who", error))?;
            let mut group_names = groups
                .group_list
                .into_iter()
                .map(|group| group.to_string())
                .collect::<Vec<_>>();
            group_names.sort();
            let expected_target_count = group_names.len();
            let mut items = Vec::with_capacity(expected_target_count);
            let mut failures = Vec::new();
            for consumer_group in group_names {
                match rocketmq_client_rust::MQAdminReadExt::examine_consume_stats(
                    &self.inner,
                    CheetahString::from(consumer_group.as_str()),
                    Some(CheetahString::from(topic)),
                    None,
                    None,
                    None,
                )
                .await
                {
                    Ok(stats) => items.push(TopicConsumerInfo {
                        consumer_group,
                        total_diff: stats.compute_total_diff(),
                        inflight_diff: stats.compute_inflight_total_diff(),
                        consume_tps: stats.get_consume_tps(),
                    }),
                    Err(error) => {
                        failures.push(rocketmq_failure(consumer_group, TopicInspectionStage::Consumer, &error))
                    }
                }
            }
            if expected_target_count != 0 {
                ensure_some_detailed_success("inspect_topic_consumers", items.len(), &failures)?;
            }
            Ok(DetailedTopicConsumers {
                topic: topic.to_string(),
                completeness: completeness(items.len(), failures.len()),
                items,
                failures,
            })
        })
    }
}

fn broker_targets_from_cluster_info(cluster_info: &ClusterInfo) -> Vec<TopicBrokerTarget> {
    collect_master_broker_targets(cluster_info)
        .into_iter()
        .map(|(broker_name, broker_addr)| TopicBrokerTarget {
            cluster_name: find_cluster_name_by_broker_name(cluster_info, &broker_name).unwrap_or_default(),
            broker_name,
            broker_addr: broker_addr.to_string(),
        })
        .collect()
}

fn safe_topic_category(topic: &str) -> (&'static str, bool) {
    if topic.starts_with(RETRY_GROUP_TOPIC_PREFIX) {
        ("RETRY", false)
    } else if topic.starts_with(DLQ_GROUP_TOPIC_PREFIX) {
        ("DLQ", false)
    } else if is_dashboard_system_topic(topic) {
        ("SYSTEM", true)
    } else {
        ("APPLICATION", false)
    }
}

fn detailed_inconsistent_fields(targets: &[DetailedTopicConfigTarget]) -> Vec<String> {
    let Some(baseline) = targets.first() else {
        return Vec::new();
    };
    let mut fields = Vec::new();
    if targets
        .iter()
        .any(|target| target.read_queue_nums != baseline.read_queue_nums)
    {
        fields.push("readQueueNums".to_string());
    }
    if targets
        .iter()
        .any(|target| target.write_queue_nums != baseline.write_queue_nums)
    {
        fields.push("writeQueueNums".to_string());
    }
    if targets.iter().any(|target| target.perm != baseline.perm) {
        fields.push("perm".to_string());
    }
    if targets.iter().any(|target| target.order != baseline.order) {
        fields.push("order".to_string());
    }
    if targets
        .iter()
        .any(|target| target.message_type != baseline.message_type)
    {
        fields.push("messageType".to_string());
    }
    fields
}

fn completeness(successful_target_count: usize, failed_target_count: usize) -> TopicInspectionCompleteness {
    if failed_target_count == 0 {
        TopicInspectionCompleteness::Complete
    } else {
        TopicInspectionCompleteness::Partial {
            successful_target_count,
            failed_target_count,
        }
    }
}

fn finish_detailed_topic_stats(
    topic: &str,
    stats: &TopicStatsTable,
    successful_target_count: usize,
    failures: Vec<TopicInspectionFailure>,
) -> AdminResult<DetailedTopicStats> {
    ensure_some_detailed_success("inspect_topic_stats", successful_target_count, &failures)?;
    Ok(DetailedTopicStats {
        stats: map_topic_stats(topic, stats),
        completeness: completeness(successful_target_count, failures.len()),
        failures,
    })
}

fn ensure_some_detailed_success(
    operation: &'static str,
    successful_target_count: usize,
    failures: &[TopicInspectionFailure],
) -> AdminResult<()> {
    if successful_target_count == 0 && !failures.is_empty() {
        Err(AdminError::backend(
            operation,
            "every authoritative Topic target failed",
        ))
    } else {
        Ok(())
    }
}

fn stable_failure(
    target: String,
    stage: TopicInspectionStage,
    code: TopicInspectionFailureCode,
    retryable: bool,
) -> TopicInspectionFailure {
    TopicInspectionFailure {
        target,
        stage,
        code,
        retryable,
    }
}

fn rocketmq_failure(target: String, stage: TopicInspectionStage, error: &RocketMQError) -> TopicInspectionFailure {
    let view = error.boundary_view();
    let code = if view.http().status.as_u16() == 404 {
        TopicInspectionFailureCode::NotFound
    } else {
        TopicInspectionFailureCode::Unavailable
    };
    stable_failure(target, stage, code, view.is_retryable())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detailed_inconsistency_is_allowlisted_and_stable() {
        let baseline = DetailedTopicConfigTarget {
            topic_name: "orders".into(),
            broker_name: "broker-a".into(),
            broker_addr: "127.0.0.1:10911".into(),
            cluster_name: "DefaultCluster".into(),
            version: 7,
            read_queue_nums: 8,
            write_queue_nums: 8,
            perm: 6,
            order: false,
            message_type: "NORMAL".into(),
        };
        let changed = DetailedTopicConfigTarget {
            broker_name: "broker-b".into(),
            write_queue_nums: 16,
            perm: 4,
            order: true,
            message_type: "FIFO".into(),
            ..baseline.clone()
        };

        assert_eq!(
            detailed_inconsistent_fields(&[baseline, changed]),
            ["writeQueueNums", "perm", "order", "messageType"]
        );
    }

    #[test]
    fn all_target_failure_is_not_a_partial_success() {
        let failures = [stable_failure(
            "broker-a".into(),
            TopicInspectionStage::Stats,
            TopicInspectionFailureCode::Unavailable,
            true,
        )];
        assert!(ensure_some_detailed_success("inspect", 0, &failures).is_err());
        assert!(ensure_some_detailed_success("inspect", 1, &failures).is_ok());
    }

    #[test]
    fn successful_authoritative_targets_may_return_complete_empty_stats() {
        let detailed = finish_detailed_topic_stats("orders", &TopicStatsTable::new(), 2, Vec::new())
            .expect("successful empty authoritative stats remain a valid observation");

        assert!(detailed.stats.offsets.is_empty());
        assert_eq!(detailed.stats.total_message_count, 0);
        assert_eq!(detailed.completeness, TopicInspectionCompleteness::Complete);
        assert!(detailed.failures.is_empty());
    }

    #[test]
    fn empty_stats_are_an_error_only_when_every_authoritative_target_failed() {
        let failure = stable_failure(
            "broker-a".into(),
            TopicInspectionStage::Stats,
            TopicInspectionFailureCode::Unavailable,
            true,
        );

        assert!(finish_detailed_topic_stats("orders", &TopicStatsTable::new(), 0, vec![failure]).is_err());
    }
}
