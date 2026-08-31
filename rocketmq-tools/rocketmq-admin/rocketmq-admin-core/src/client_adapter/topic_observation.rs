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

//! Read-client implementation of address-free Topic observations.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use cheetah_string::CheetahString;
use rocketmq_client_rust::DefaultMQAdminExt;
use rocketmq_client_rust::MQAdminReadExt;
use rocketmq_client_rust::MQAdminTopicStatsReadExt;
use rocketmq_client_rust::TopicConfigVersioned;
use rocketmq_error::RocketMQError;
use rocketmq_model::common::mix_all;
use rocketmq_protocol::protocol::admin::topic_stats_table::TopicStatsTable;
use rocketmq_protocol::protocol::body::broker_body::cluster_info::ClusterInfo;
use rocketmq_protocol::protocol::route::topic_route_data::TopicRouteData;

use crate::core::broker::is_valid_remoting_endpoint;
use crate::core::query::AdminQueryFailureCode;
use crate::core::query::AdminQueryResult;
use crate::core::query::AdminQuerySource;
use crate::core::query::AdminSourceFailure;
use crate::core::topic_observation::QueryTopicConfigRequest;
use crate::core::topic_observation::QueryTopicConfigResult;
use crate::core::topic_observation::QueryTopicStatsRequest;
use crate::core::topic_observation::QueryTopicStatsResult;
use crate::core::topic_observation::TopicConfigDifferenceField;
use crate::core::topic_observation::TopicConfigObservationRow;
use crate::core::topic_observation::TopicStatsQueueRow;
use crate::core::topic_observation::MAX_TOPIC_OBSERVATION_TARGETS;
use crate::core::topic_observation::TOPIC_STATS_TOTAL_SATURATED_WARNING;
use crate::core::topic_observation::TOPIC_STATS_TRUNCATED_WARNING;
use crate::core::AdminError;
use crate::core::AdminResult;

type BrokerTarget = (String, CheetahString);

#[allow(async_fn_in_trait)]
trait TopicObservationSource: Send {
    async fn cluster_info(&self) -> Result<ClusterInfo, RocketMQError>;
    async fn topic_route(&self, topic: &str) -> Result<Option<TopicRouteData>, RocketMQError>;
    async fn topic_stats(&self, broker_addr: CheetahString, topic: &str) -> Result<TopicStatsTable, RocketMQError>;
    async fn topic_config(
        &self,
        broker_addr: CheetahString,
        topic: &str,
    ) -> Result<TopicConfigVersioned, RocketMQError>;
}

impl TopicObservationSource for DefaultMQAdminExt {
    async fn cluster_info(&self) -> Result<ClusterInfo, RocketMQError> {
        MQAdminReadExt::examine_broker_cluster_info(self).await
    }

    async fn topic_route(&self, topic: &str) -> Result<Option<TopicRouteData>, RocketMQError> {
        MQAdminReadExt::examine_topic_route_info(self, CheetahString::from(topic)).await
    }

    async fn topic_stats(&self, broker_addr: CheetahString, topic: &str) -> Result<TopicStatsTable, RocketMQError> {
        MQAdminTopicStatsReadExt::topic_stats_at(self, broker_addr, CheetahString::from(topic)).await
    }

    async fn topic_config(
        &self,
        broker_addr: CheetahString,
        topic: &str,
    ) -> Result<TopicConfigVersioned, RocketMQError> {
        MQAdminReadExt::topic_config_with_version(self, broker_addr, CheetahString::from(topic)).await
    }
}

pub(crate) async fn query_topic_stats(
    admin: &DefaultMQAdminExt,
    request: &QueryTopicStatsRequest,
) -> AdminResult<AdminQueryResult<QueryTopicStatsResult>> {
    query_topic_stats_from(admin, request).await
}

pub(crate) async fn query_topic_config(
    admin: &DefaultMQAdminExt,
    request: &QueryTopicConfigRequest,
) -> AdminResult<AdminQueryResult<QueryTopicConfigResult>> {
    query_topic_config_from(admin, request).await
}

async fn query_topic_stats_from<S: TopicObservationSource>(
    source: &S,
    request: &QueryTopicStatsRequest,
) -> AdminResult<AdminQueryResult<QueryTopicStatsResult>> {
    let request = QueryTopicStatsRequest::try_new(&request.cluster, &request.topic, request.max_rows)?;
    let (targets, mut failures) =
        resolve_topic_targets(source, &request.cluster, &request.topic, AdminQuerySource::TopicStats).await?;
    let mut retained = BTreeMap::<(String, i32), TopicStatsQueueRow>::new();
    let mut successful_sources = 0usize;
    let mut queue_count = 0usize;
    let mut total_message_count = 0u128;
    let mut truncated = false;

    for (broker_name, broker_addr) in targets {
        match source.topic_stats(broker_addr, &request.topic).await {
            Ok(stats) => {
                let (rows, invalid_response) = normalize_topic_stats(&broker_name, &request.topic, stats);
                if !rows.is_empty() || !invalid_response {
                    successful_sources += 1;
                }
                if invalid_response {
                    failures.push(AdminSourceFailure::new(
                        AdminQuerySource::TopicStats,
                        AdminQueryFailureCode::InvalidResponse,
                        false,
                        &broker_name,
                    ));
                }
                queue_count = queue_count.saturating_add(rows.len());
                for row in rows {
                    total_message_count = total_message_count.saturating_add(u128::from(row.message_count));
                    retained.insert((row.broker_name.clone(), row.queue_id), row);
                    if retained.len() > request.max_rows {
                        retained.pop_last();
                        truncated = true;
                    }
                }
            }
            Err(error) => failures.push(source_failure(AdminQuerySource::TopicStats, &broker_name, &error)),
        }
    }

    let saturated = total_message_count > u128::from(u64::MAX);
    let result = QueryTopicStatsResult {
        topic: request.topic,
        total_message_count: total_message_count.min(u128::from(u64::MAX)) as u64,
        queue_count,
        queues: retained.into_values().collect(),
        truncated,
    };
    let mut result = AdminQueryResult::from_sources(result, successful_sources, failures)?;
    if truncated {
        result.partial = true;
        result.warnings.push(TOPIC_STATS_TRUNCATED_WARNING.to_string());
    }
    if saturated {
        result.partial = true;
        result.warnings.push(TOPIC_STATS_TOTAL_SATURATED_WARNING.to_string());
    }
    result.warnings.sort();
    result.warnings.dedup();
    Ok(result)
}

async fn query_topic_config_from<S: TopicObservationSource>(
    source: &S,
    request: &QueryTopicConfigRequest,
) -> AdminResult<AdminQueryResult<QueryTopicConfigResult>> {
    let request = QueryTopicConfigRequest::try_new(&request.cluster, &request.topic)?;
    let (targets, mut failures) =
        resolve_topic_targets(source, &request.cluster, &request.topic, AdminQuerySource::TopicConfig).await?;
    let mut brokers = Vec::new();
    let mut successful_sources = 0usize;
    for (broker_name, broker_addr) in targets {
        match source.topic_config(broker_addr, &request.topic).await {
            Ok(snapshot) => {
                successful_sources += 1;
                brokers.push(config_row(broker_name, snapshot));
            }
            Err(error) => failures.push(source_failure(AdminQuerySource::TopicConfig, &broker_name, &error)),
        }
    }
    brokers.sort_by(|left, right| left.broker_name.cmp(&right.broker_name));
    let inconsistent_fields = topic_config_differences(&brokers);
    AdminQueryResult::from_sources(
        QueryTopicConfigResult {
            topic: request.topic,
            brokers,
            inconsistent_fields,
        },
        successful_sources,
        failures,
    )
}

async fn resolve_topic_targets<S: TopicObservationSource>(
    source: &S,
    cluster: &str,
    topic: &str,
    failure_source: AdminQuerySource,
) -> AdminResult<(Vec<BrokerTarget>, Vec<AdminSourceFailure>)> {
    let cluster_info = source
        .cluster_info()
        .await
        .map_err(|error| backend_error("examine_broker_cluster_info", error))?;
    let route = source
        .topic_route(topic)
        .await
        .map_err(|error| backend_error("examine_topic_route_info", error))?
        .ok_or_else(|| AdminError::not_found("topic", topic))?;
    selected_cluster_route_masters(&cluster_info, &route, cluster, failure_source)
}

fn selected_cluster_route_masters(
    cluster_info: &ClusterInfo,
    route: &TopicRouteData,
    cluster: &str,
    source: AdminQuerySource,
) -> AdminResult<(Vec<BrokerTarget>, Vec<AdminSourceFailure>)> {
    let cluster_brokers = cluster_info
        .cluster_addr_table
        .as_ref()
        .and_then(|table| table.get(cluster))
        .ok_or_else(|| AdminError::not_found("cluster", cluster))?;
    let mut route_brokers = BTreeSet::new();
    let mut failures = Vec::new();
    for broker in &route.broker_datas {
        let broker_name = broker.broker_name().as_str();
        let listed_in_selected_cluster = cluster_brokers.contains(broker_name);
        if broker.cluster() != cluster {
            if listed_in_selected_cluster {
                failures.push(invalid_response_failure(source, broker_name));
            }
            continue;
        }
        if !listed_in_selected_cluster || !safe_broker_name(broker_name) {
            failures.push(invalid_response_failure(source, broker_name));
            continue;
        }
        route_brokers.insert(broker_name.to_string());
    }
    if route_brokers.is_empty() {
        if !failures.is_empty() {
            return Ok((Vec::new(), failures));
        }
        return Err(AdminError::not_found("topic route in selected cluster", cluster));
    }
    if route_brokers.len() > MAX_TOPIC_OBSERVATION_TARGETS {
        return Err(AdminError::backend_view(
            "resolve_topic_observation_targets",
            "TOPIC_OBSERVATION_TARGET_LIMIT_EXCEEDED",
            "Topic route has too many selected-cluster Broker targets",
            None,
            422,
            false,
        ));
    }

    let broker_table = cluster_info.broker_addr_table.as_ref();
    let mut targets = Vec::new();
    for broker_name in route_brokers {
        let Some(broker) = broker_table.and_then(|table| table.get(broker_name.as_str())) else {
            failures.push(invalid_response_failure(source, &broker_name));
            continue;
        };
        if broker.cluster() != cluster || broker.broker_name().as_str() != broker_name {
            failures.push(invalid_response_failure(source, &broker_name));
            continue;
        }
        let master = broker
            .broker_addrs()
            .get(&mix_all::MASTER_ID)
            .filter(|address| is_valid_remoting_endpoint(address.as_str()));
        match master {
            Some(address) => targets.push((broker_name, address.clone())),
            None => failures.push(invalid_response_failure(source, &broker_name)),
        }
    }
    Ok((targets, failures))
}

fn normalize_topic_stats(broker_name: &str, topic: &str, stats: TopicStatsTable) -> (Vec<TopicStatsQueueRow>, bool) {
    let mut rows = BTreeMap::<i32, TopicStatsQueueRow>::new();
    let mut invalid = false;
    for (queue, offset) in stats.into_offset_table() {
        let min_offset = offset.get_min_offset();
        let max_offset = offset.get_max_offset();
        let last_update_timestamp = offset.get_last_update_timestamp();
        let Some(message_count) = max_offset
            .checked_sub(min_offset)
            .and_then(|count| u64::try_from(count).ok())
        else {
            invalid = true;
            continue;
        };
        if queue.topic_str() != topic
            || queue.broker_name().as_str() != broker_name
            || queue.queue_id() < 0
            || min_offset < 0
            || last_update_timestamp < 0
        {
            invalid = true;
            continue;
        }
        let row = TopicStatsQueueRow {
            broker_name: broker_name.to_string(),
            queue_id: queue.queue_id(),
            min_offset,
            max_offset,
            message_count,
            last_update_timestamp,
        };
        match rows.entry(row.queue_id) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(row);
            }
            std::collections::btree_map::Entry::Occupied(mut entry) => {
                invalid = true;
                if stats_row_value(&row) < stats_row_value(entry.get()) {
                    entry.insert(row);
                }
            }
        }
    }
    (rows.into_values().collect(), invalid)
}

fn stats_row_value(row: &TopicStatsQueueRow) -> (i64, i64, u64, i64) {
    (
        row.min_offset,
        row.max_offset,
        row.message_count,
        row.last_update_timestamp,
    )
}

fn config_row(broker_name: String, snapshot: TopicConfigVersioned) -> TopicConfigObservationRow {
    let config = snapshot.config;
    TopicConfigObservationRow {
        broker_name,
        version: snapshot.version,
        read_queue_nums: config.read_queue_nums,
        write_queue_nums: config.write_queue_nums,
        perm: config.perm,
        order: config.order,
        message_type: config.get_topic_message_type().to_string(),
    }
}

fn topic_config_differences(rows: &[TopicConfigObservationRow]) -> Vec<TopicConfigDifferenceField> {
    let Some(baseline) = rows.first() else {
        return Vec::new();
    };
    let mut differences = Vec::new();
    if rows.iter().any(|row| row.read_queue_nums != baseline.read_queue_nums) {
        differences.push(TopicConfigDifferenceField::ReadQueueNums);
    }
    if rows.iter().any(|row| row.write_queue_nums != baseline.write_queue_nums) {
        differences.push(TopicConfigDifferenceField::WriteQueueNums);
    }
    if rows.iter().any(|row| row.perm != baseline.perm) {
        differences.push(TopicConfigDifferenceField::Perm);
    }
    if rows.iter().any(|row| row.order != baseline.order) {
        differences.push(TopicConfigDifferenceField::Order);
    }
    if rows.iter().any(|row| row.message_type != baseline.message_type) {
        differences.push(TopicConfigDifferenceField::MessageType);
    }
    differences
}

fn safe_broker_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 128
        && name.is_ascii()
        && name
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.'))
}

fn invalid_response_failure(source: AdminQuerySource, broker_name: &str) -> AdminSourceFailure {
    AdminSourceFailure::new(source, AdminQueryFailureCode::InvalidResponse, false, broker_name)
}

fn source_failure(source: AdminQuerySource, broker_name: &str, error: &RocketMQError) -> AdminSourceFailure {
    let view = error.boundary_view();
    let code = match view.http().status.as_u16() {
        401 | 403 => AdminQueryFailureCode::PermissionDenied,
        404 => AdminQueryFailureCode::NotFound,
        408 | 504 => AdminQueryFailureCode::Timeout,
        429 => AdminQueryFailureCode::RateLimited,
        400 | 413 | 422 => AdminQueryFailureCode::InvalidResponse,
        _ => AdminQueryFailureCode::SourceUnavailable,
    };
    AdminSourceFailure::new(source, code, view.is_retryable(), broker_name)
}

fn backend_error(operation: &'static str, error: RocketMQError) -> AdminError {
    let view = error.boundary_view();
    AdminError::backend_view(
        operation,
        view.code().as_str(),
        view.message(),
        (!view.context().is_empty()).then(|| view.context().to_string()),
        view.http().status.as_u16(),
        view.is_retryable(),
    )
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::collections::HashSet;
    use std::sync::Mutex;

    use rocketmq_model::common::config::TopicConfig;
    use rocketmq_model::message::MessageQueue;
    use rocketmq_protocol::protocol::admin::topic_offset::TopicOffset;
    use rocketmq_protocol::protocol::route::route_data_view::BrokerData;

    use super::*;
    use crate::core::topic_observation::MAX_TOPIC_STATS_ROWS;

    const ADDRESS_A: &str = "127.0.0.1:10911";
    const ADDRESS_B: &str = "127.0.0.2:10911";
    const ADDRESS_C: &str = "127.0.0.3:10911";

    #[derive(Default)]
    struct FakeSource {
        cluster_info: ClusterInfo,
        route: Option<TopicRouteData>,
        stats: BTreeMap<String, TestResult<TopicStatsTable>>,
        configs: BTreeMap<String, TestResult<TopicConfigVersioned>>,
        stats_calls: Mutex<Vec<String>>,
        config_calls: Mutex<Vec<String>>,
    }

    #[derive(Clone)]
    enum TestResult<T> {
        Value(T),
        Failure(&'static str),
    }

    impl TopicObservationSource for FakeSource {
        async fn cluster_info(&self) -> Result<ClusterInfo, RocketMQError> {
            Ok(self.cluster_info.clone())
        }

        async fn topic_route(&self, _topic: &str) -> Result<Option<TopicRouteData>, RocketMQError> {
            Ok(self.route.clone())
        }

        async fn topic_stats(
            &self,
            broker_addr: CheetahString,
            _topic: &str,
        ) -> Result<TopicStatsTable, RocketMQError> {
            self.stats_calls.lock().unwrap().push(broker_addr.to_string());
            match self.stats.get(broker_addr.as_str()) {
                Some(TestResult::Value(stats)) => Ok(stats.clone()),
                Some(TestResult::Failure(reason)) => Err(test_error(reason)),
                None => Ok(TopicStatsTable::new()),
            }
        }

        async fn topic_config(
            &self,
            broker_addr: CheetahString,
            _topic: &str,
        ) -> Result<TopicConfigVersioned, RocketMQError> {
            self.config_calls.lock().unwrap().push(broker_addr.to_string());
            match self.configs.get(broker_addr.as_str()) {
                Some(TestResult::Value(config)) => Ok(config.clone()),
                Some(TestResult::Failure(reason)) => Err(test_error(reason)),
                None => Ok(TopicConfigVersioned {
                    version: 1,
                    config: TopicConfig::new("orders"),
                }),
            }
        }
    }

    fn topology(brokers: &[(&str, &str, &str)]) -> (ClusterInfo, TopicRouteData) {
        let mut broker_table = HashMap::new();
        let mut cluster_table = HashMap::<CheetahString, HashSet<CheetahString>>::new();
        let mut route_brokers = Vec::new();
        for (cluster, broker, address) in brokers {
            let data = BrokerData::new(
                (*cluster).into(),
                (*broker).into(),
                HashMap::from([(mix_all::MASTER_ID, (*address).into())]),
                None,
            );
            broker_table.insert((*broker).into(), data.clone());
            cluster_table
                .entry((*cluster).into())
                .or_default()
                .insert((*broker).into());
            route_brokers.push(data);
        }
        (
            ClusterInfo::new(Some(broker_table), Some(cluster_table)),
            TopicRouteData {
                broker_datas: route_brokers,
                ..Default::default()
            },
        )
    }

    fn stats(broker_name: &str, rows: &[(i32, i64, i64, i64)]) -> TopicStatsTable {
        let mut table = TopicStatsTable::new();
        for (queue_id, min, max, timestamp) in rows {
            insert_stats_row(&mut table, "orders", broker_name, *queue_id, *min, *max, *timestamp);
        }
        table
    }

    fn insert_stats_row(
        table: &mut TopicStatsTable,
        topic: &str,
        broker_name: &str,
        queue_id: i32,
        min_offset: i64,
        max_offset: i64,
        last_update_timestamp: i64,
    ) {
        let mut offset = TopicOffset::new();
        offset.set_min_offset(min_offset);
        offset.set_max_offset(max_offset);
        offset.set_last_update_timestamp(last_update_timestamp);
        table
            .get_offset_table_mut()
            .insert(MessageQueue::from_parts(topic, broker_name, queue_id), offset);
    }

    fn source_with_two_clusters() -> FakeSource {
        let (cluster_info, route) = topology(&[
            ("cluster-a", "broker-b", ADDRESS_B),
            ("cluster-a", "broker-a", ADDRESS_A),
            ("cluster-b", "broker-c", ADDRESS_C),
        ]);
        FakeSource {
            cluster_info,
            route: Some(route),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn statistics_select_cluster_masters_sort_bound_and_aggregate_safely() {
        let mut source = source_with_two_clusters();
        source.stats.insert(
            ADDRESS_A.to_string(),
            TestResult::Value(stats("broker-a", &[(2, 5, 9, 20), (0, 1, 4, 10)])),
        );
        source.stats.insert(
            ADDRESS_B.to_string(),
            TestResult::Value(stats("broker-b", &[(1, 0, 8, 30)])),
        );
        let result = query_topic_stats_from(
            &source,
            &QueryTopicStatsRequest::try_new("cluster-a", "orders", 2).unwrap(),
        )
        .await
        .unwrap();

        assert!(result.partial);
        assert_eq!(result.warnings, [TOPIC_STATS_TRUNCATED_WARNING]);
        assert_eq!(result.data.total_message_count, 15);
        assert_eq!(result.data.queue_count, 3);
        assert!(result.data.truncated);
        assert_eq!(
            result
                .data
                .queues
                .iter()
                .map(|row| (row.broker_name.as_str(), row.queue_id))
                .collect::<Vec<_>>(),
            [("broker-a", 0), ("broker-a", 2)]
        );
    }

    #[tokio::test]
    async fn statistics_preserve_partial_and_total_failure_without_backend_text() {
        let mut source = source_with_two_clusters();
        source.stats.insert(
            ADDRESS_A.to_string(),
            TestResult::Value(stats("broker-a", &[(0, 0, 5, 10)])),
        );
        source
            .stats
            .insert(ADDRESS_B.to_string(), TestResult::Failure("secret-internal-b"));
        let request = QueryTopicStatsRequest::try_new("cluster-a", "orders", 10).unwrap();
        let partial = query_topic_stats_from(&source, &request).await.unwrap();
        assert!(partial.partial);
        assert_eq!(partial.source_failures.len(), 1);
        assert_eq!(partial.source_failures[0].logical_target(), "broker-b");
        assert!(!serde_json::to_string(&partial).unwrap().contains("secret-internal-b"));

        source
            .stats
            .insert(ADDRESS_A.to_string(), TestResult::Failure("secret-internal-a"));
        let error = query_topic_stats_from(&source, &request).await.unwrap_err();
        assert_eq!(error.code(), Some("ADMIN_QUERY_ALL_SOURCES_FAILED"));
        assert!(!error.to_string().contains("secret-internal"));
    }

    #[tokio::test]
    async fn malformed_statistics_are_partial_and_never_underflow() {
        let mut source = source_with_two_clusters();
        source.stats.insert(
            ADDRESS_A.to_string(),
            TestResult::Value(stats("broker-a", &[(0, 9, 3, 10), (1, 1, 4, 10)])),
        );
        source
            .stats
            .insert(ADDRESS_B.to_string(), TestResult::Value(TopicStatsTable::new()));
        let result = query_topic_stats_from(
            &source,
            &QueryTopicStatsRequest::try_new("cluster-a", "orders", 10).unwrap(),
        )
        .await
        .unwrap();
        assert!(result.partial);
        assert_eq!(result.data.total_message_count, 3);
        assert_eq!(result.data.queues.len(), 1);
        assert_eq!(result.source_failures[0].code(), AdminQueryFailureCode::InvalidResponse);
    }

    #[test]
    fn statistics_reject_negative_and_checked_sub_overflow_boundaries() {
        let mut malformed = TopicStatsTable::new();
        insert_stats_row(&mut malformed, "orders", "broker-a", 0, -1, 1, 1);
        insert_stats_row(&mut malformed, "orders", "broker-a", 1, 0, -1, 1);
        insert_stats_row(&mut malformed, "orders", "broker-a", 2, 0, 1, -1);
        insert_stats_row(&mut malformed, "orders", "broker-a", 3, i64::MIN, i64::MAX, 1);
        insert_stats_row(&mut malformed, "orders", "broker-a", 4, 0, 1, 1);

        let (rows, invalid) = normalize_topic_stats("broker-a", "orders", malformed);

        assert!(invalid);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].queue_id, 4);
        assert_eq!(rows[0].message_count, 1);
    }

    #[test]
    fn statistics_require_exact_topic_broker_and_nonnegative_queue_identity() {
        let mut single_mismatch = TopicStatsTable::new();
        insert_stats_row(&mut single_mismatch, "other-topic", "broker-a", 0, 0, 1, 1);
        let (rows, invalid) = normalize_topic_stats("broker-a", "orders", single_mismatch);
        assert!(invalid);
        assert!(rows.is_empty());

        let mut multiple_mismatches = TopicStatsTable::new();
        insert_stats_row(&mut multiple_mismatches, "orders", "broker-forged", 0, 0, 1, 1);
        insert_stats_row(&mut multiple_mismatches, "orders", "broker-a", -1, 0, 1, 1);
        let (rows, invalid) = normalize_topic_stats("broker-a", "orders", multiple_mismatches);
        assert!(invalid);
        assert!(rows.is_empty());
    }

    #[tokio::test]
    async fn valid_and_mismatched_rows_are_partial_without_rewriting_the_owner() {
        let (cluster_info, route) = topology(&[("cluster-a", "broker-a", ADDRESS_A)]);
        let mut mixed = TopicStatsTable::new();
        insert_stats_row(&mut mixed, "orders", "broker-a", 0, 0, 7, 1);
        insert_stats_row(&mut mixed, "orders", "broker-forged", 1, 0, 100, 1);
        let source = FakeSource {
            cluster_info,
            route: Some(route),
            stats: BTreeMap::from([(ADDRESS_A.to_string(), TestResult::Value(mixed))]),
            ..Default::default()
        };

        let result = query_topic_stats_from(
            &source,
            &QueryTopicStatsRequest::try_new("cluster-a", "orders", 10).unwrap(),
        )
        .await
        .unwrap();

        assert!(result.partial);
        assert_eq!(result.data.queue_count, 1);
        assert_eq!(result.data.total_message_count, 7);
        assert_eq!(result.data.queues[0].broker_name, "broker-a");
        assert_eq!(result.source_failures[0].code(), AdminQueryFailureCode::InvalidResponse);
    }

    #[tokio::test]
    async fn only_identity_mismatches_are_a_total_source_failure() {
        let (cluster_info, route) = topology(&[("cluster-a", "broker-a", ADDRESS_A)]);
        let mut mismatched = TopicStatsTable::new();
        insert_stats_row(&mut mismatched, "orders", "broker-forged", 0, 0, 1, 1);
        insert_stats_row(&mut mismatched, "other-topic", "broker-a", 1, 0, 1, 1);
        let source = FakeSource {
            cluster_info,
            route: Some(route),
            stats: BTreeMap::from([(ADDRESS_A.to_string(), TestResult::Value(mismatched))]),
            ..Default::default()
        };

        let error = query_topic_stats_from(
            &source,
            &QueryTopicStatsRequest::try_new("cluster-a", "orders", 10).unwrap(),
        )
        .await
        .unwrap_err();

        assert_eq!(error.code(), Some("ADMIN_QUERY_ALL_SOURCES_FAILED"));
        assert_eq!(source.stats_calls.lock().unwrap().as_slice(), [ADDRESS_A]);
    }

    #[tokio::test]
    async fn the_same_queue_id_on_distinct_exact_brokers_is_valid() {
        let mut source = source_with_two_clusters();
        source.stats.insert(
            ADDRESS_A.to_string(),
            TestResult::Value(stats("broker-a", &[(0, 0, 3, 1)])),
        );
        source.stats.insert(
            ADDRESS_B.to_string(),
            TestResult::Value(stats("broker-b", &[(0, 0, 5, 1)])),
        );

        let result = query_topic_stats_from(
            &source,
            &QueryTopicStatsRequest::try_new("cluster-a", "orders", 10).unwrap(),
        )
        .await
        .unwrap();

        assert!(!result.partial);
        assert_eq!(result.data.queue_count, 2);
        assert_eq!(result.data.total_message_count, 8);
        assert_eq!(
            result
                .data
                .queues
                .iter()
                .map(|row| (row.broker_name.as_str(), row.queue_id))
                .collect::<Vec<_>>(),
            [("broker-a", 0), ("broker-b", 0)]
        );
    }

    #[tokio::test]
    async fn malicious_owner_duplicate_cannot_replace_or_double_count_the_exact_source_row() {
        let mut source = source_with_two_clusters();
        let mut duplicate_stats = TopicStatsTable::new();
        insert_stats_row(&mut duplicate_stats, "orders", "broker-a", 0, 5, 15, 10);
        insert_stats_row(&mut duplicate_stats, "orders", "forged-broker", 0, 1, 4, 10);
        source
            .stats
            .insert(ADDRESS_A.to_string(), TestResult::Value(duplicate_stats));
        source
            .stats
            .insert(ADDRESS_B.to_string(), TestResult::Value(TopicStatsTable::new()));

        let result = query_topic_stats_from(
            &source,
            &QueryTopicStatsRequest::try_new("cluster-a", "orders", 10).unwrap(),
        )
        .await
        .unwrap();

        assert!(result.partial);
        assert_eq!(result.data.queue_count, 1);
        assert_eq!(result.data.total_message_count, 10);
        assert_eq!(result.data.queues.len(), 1);
        assert_eq!(result.data.queues[0].min_offset, 5);
        assert_eq!(result.source_failures[0].code(), AdminQueryFailureCode::InvalidResponse);
    }

    #[tokio::test]
    async fn statistics_saturate_the_public_total_with_one_stable_warning() {
        let addresses = [ADDRESS_A, ADDRESS_B, ADDRESS_C];
        let brokers = ["broker-a", "broker-b", "broker-c"];
        let (cluster_info, route) = topology(&[
            ("cluster-a", brokers[0], addresses[0]),
            ("cluster-a", brokers[1], addresses[1]),
            ("cluster-a", brokers[2], addresses[2]),
        ]);
        let mut source = FakeSource {
            cluster_info,
            route: Some(route),
            ..Default::default()
        };
        for (broker, address) in brokers.into_iter().zip(addresses) {
            source.stats.insert(
                address.to_string(),
                TestResult::Value(stats(broker, &[(0, 0, i64::MAX, 1)])),
            );
        }

        let result = query_topic_stats_from(
            &source,
            &QueryTopicStatsRequest::try_new("cluster-a", "orders", 10).unwrap(),
        )
        .await
        .unwrap();

        assert!(result.partial);
        assert_eq!(result.data.queue_count, 3);
        assert_eq!(result.data.total_message_count, u64::MAX);
        assert_eq!(result.warnings, [TOPIC_STATS_TOTAL_SATURATED_WARNING]);
        assert!(result.source_failures.is_empty());
    }

    #[tokio::test]
    async fn retained_row_boundary_is_exact_stable_and_aggregates_all_unique_rows() {
        let (cluster_info, route) = topology(&[("cluster-a", "broker-a", ADDRESS_A)]);
        let mut exact = TopicStatsTable::new();
        for queue_id in (0..MAX_TOPIC_STATS_ROWS as i32).rev() {
            insert_stats_row(&mut exact, "orders", "broker-a", queue_id, 0, 1, 1);
        }
        let exact_source = FakeSource {
            cluster_info: cluster_info.clone(),
            route: Some(route.clone()),
            stats: BTreeMap::from([(ADDRESS_A.to_string(), TestResult::Value(exact))]),
            ..Default::default()
        };
        let request = QueryTopicStatsRequest::try_new("cluster-a", "orders", MAX_TOPIC_STATS_ROWS).unwrap();
        let exact_result = query_topic_stats_from(&exact_source, &request).await.unwrap();
        assert!(!exact_result.partial);
        assert!(!exact_result.data.truncated);
        assert_eq!(exact_result.data.queue_count, MAX_TOPIC_STATS_ROWS);
        assert_eq!(exact_result.data.total_message_count, MAX_TOPIC_STATS_ROWS as u64);
        assert_eq!(exact_result.data.queues.first().unwrap().queue_id, 0);
        assert_eq!(
            exact_result.data.queues.last().unwrap().queue_id,
            MAX_TOPIC_STATS_ROWS as i32 - 1
        );

        let mut over = TopicStatsTable::new();
        for queue_id in (0..=MAX_TOPIC_STATS_ROWS as i32).rev() {
            insert_stats_row(&mut over, "orders", "broker-a", queue_id, 0, 1, 1);
        }
        let over_source = FakeSource {
            cluster_info,
            route: Some(route),
            stats: BTreeMap::from([(ADDRESS_A.to_string(), TestResult::Value(over))]),
            ..Default::default()
        };
        let over_result = query_topic_stats_from(&over_source, &request).await.unwrap();
        assert!(over_result.partial);
        assert!(over_result.data.truncated);
        assert_eq!(over_result.data.queue_count, MAX_TOPIC_STATS_ROWS + 1);
        assert_eq!(over_result.data.total_message_count, MAX_TOPIC_STATS_ROWS as u64 + 1);
        assert_eq!(over_result.data.queues.len(), MAX_TOPIC_STATS_ROWS);
        assert_eq!(over_result.data.queues.first().unwrap().queue_id, 0);
        assert_eq!(
            over_result.data.queues.last().unwrap().queue_id,
            MAX_TOPIC_STATS_ROWS as i32 - 1
        );
        assert_eq!(over_result.warnings, [TOPIC_STATS_TRUNCATED_WARNING]);
    }

    #[tokio::test]
    async fn configuration_is_fixed_sorted_and_versions_do_not_create_differences() {
        let mut source = source_with_two_clusters();
        let config = |version, read_queue_nums, message_type: &str| {
            let mut config = TopicConfig::with_queues("orders", read_queue_nums, 8);
            config.perm = 6;
            config.attributes.insert("message.type".into(), message_type.into());
            config
                .attributes
                .insert("secret.attribute".into(), "must-not-escape".into());
            TopicConfigVersioned { version, config }
        };
        source
            .configs
            .insert(ADDRESS_A.to_string(), TestResult::Value(config(10, 8, "NORMAL")));
        source
            .configs
            .insert(ADDRESS_B.to_string(), TestResult::Value(config(99, 16, "FIFO")));
        let result = query_topic_config_from(
            &source,
            &QueryTopicConfigRequest::try_new("cluster-a", "orders").unwrap(),
        )
        .await
        .unwrap();

        assert!(!result.partial);
        assert_eq!(
            result.data.inconsistent_fields,
            [
                TopicConfigDifferenceField::ReadQueueNums,
                TopicConfigDifferenceField::MessageType,
            ]
        );
        assert_eq!(result.data.brokers[0].broker_name, "broker-a");
        assert_eq!(result.data.brokers[0].version, 10);
        let json = serde_json::to_string(&result).unwrap();
        assert!(!json.contains("internal-a"));
        assert!(!json.contains("secret.attribute"));
        assert!(!json.contains("must-not-escape"));
    }

    #[tokio::test]
    async fn configuration_version_only_changes_are_observation_evidence_not_differences() {
        let mut source = source_with_two_clusters();
        let mut config = TopicConfig::with_queues("orders", 8, 16);
        config.perm = 6;
        config.order = true;
        config.attributes.insert("message.type".into(), "FIFO".into());
        source.configs.insert(
            ADDRESS_A.to_string(),
            TestResult::Value(TopicConfigVersioned {
                version: 10,
                config: config.clone(),
            }),
        );
        source.configs.insert(
            ADDRESS_B.to_string(),
            TestResult::Value(TopicConfigVersioned { version: 99, config }),
        );

        let result = query_topic_config_from(
            &source,
            &QueryTopicConfigRequest::try_new("cluster-a", "orders").unwrap(),
        )
        .await
        .unwrap();

        assert!(!result.partial);
        assert_eq!(result.data.brokers[0].version, 10);
        assert_eq!(result.data.brokers[1].version, 99);
        assert!(result.data.inconsistent_fields.is_empty());
    }

    #[tokio::test]
    async fn configuration_reports_all_closed_semantic_differences_in_stable_order() {
        let mut source = source_with_two_clusters();
        let mut baseline = TopicConfig::with_queues("orders", 8, 8);
        baseline.perm = 6;
        baseline.order = false;
        baseline.attributes.insert("message.type".into(), "NORMAL".into());
        let mut different = TopicConfig::with_queues("orders", 16, 32);
        different.perm = 4;
        different.order = true;
        different.attributes.insert("message.type".into(), "FIFO".into());
        source.configs.insert(
            ADDRESS_A.to_string(),
            TestResult::Value(TopicConfigVersioned {
                version: 1,
                config: baseline,
            }),
        );
        source.configs.insert(
            ADDRESS_B.to_string(),
            TestResult::Value(TopicConfigVersioned {
                version: 1,
                config: different,
            }),
        );

        let result = query_topic_config_from(
            &source,
            &QueryTopicConfigRequest::try_new("cluster-a", "orders").unwrap(),
        )
        .await
        .unwrap();

        assert_eq!(
            result.data.inconsistent_fields,
            [
                TopicConfigDifferenceField::ReadQueueNums,
                TopicConfigDifferenceField::WriteQueueNums,
                TopicConfigDifferenceField::Perm,
                TopicConfigDifferenceField::Order,
                TopicConfigDifferenceField::MessageType,
            ]
        );
    }

    #[tokio::test]
    async fn configuration_preserves_partial_and_total_failure_semantics() {
        let mut source = source_with_two_clusters();
        source.configs.insert(
            ADDRESS_A.to_string(),
            TestResult::Value(TopicConfigVersioned {
                version: 7,
                config: TopicConfig::new("orders"),
            }),
        );
        source
            .configs
            .insert(ADDRESS_B.to_string(), TestResult::Failure("secret-internal-b"));
        let request = QueryTopicConfigRequest::try_new("cluster-a", "orders").unwrap();
        let partial = query_topic_config_from(&source, &request).await.unwrap();
        assert!(partial.partial);
        assert_eq!(partial.data.brokers.len(), 1);
        assert_eq!(partial.source_failures[0].logical_target(), "broker-b");
        assert!(!serde_json::to_string(&partial).unwrap().contains("secret-internal-b"));

        source
            .configs
            .insert(ADDRESS_A.to_string(), TestResult::Failure("secret-internal-a"));
        let error = query_topic_config_from(&source, &request).await.unwrap_err();
        assert_eq!(error.code(), Some("ADMIN_QUERY_ALL_SOURCES_FAILED"));
        assert!(!error.to_string().contains("secret-internal"));
    }

    #[tokio::test]
    async fn same_name_from_another_cluster_is_invalid_and_never_queried() {
        let (cluster_info, mut route) = topology(&[("cluster-a", "broker-a", ADDRESS_A)]);
        route.broker_datas[0].set_cluster("cluster-b".into());
        let source = FakeSource {
            cluster_info,
            route: Some(route),
            ..Default::default()
        };

        let error = query_topic_stats_from(
            &source,
            &QueryTopicStatsRequest::try_new("cluster-a", "orders", 10).unwrap(),
        )
        .await
        .unwrap_err();

        assert_eq!(error.code(), Some("ADMIN_QUERY_ALL_SOURCES_FAILED"));
        assert!(source.stats_calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn table_records_must_match_the_selected_cluster_and_lookup_name_without_rpc() {
        for corruption in ["cluster", "name"] {
            let (mut cluster_info, route) = topology(&[("cluster-a", "broker-a", ADDRESS_A)]);
            let record = cluster_info
                .broker_addr_table
                .as_mut()
                .unwrap()
                .get_mut("broker-a")
                .unwrap();
            match corruption {
                "cluster" => record.set_cluster("cluster-b".into()),
                "name" => record.set_broker_name("broker-forged".into()),
                _ => unreachable!(),
            }

            let (targets, failures) =
                selected_cluster_route_masters(&cluster_info, &route, "cluster-a", AdminQuerySource::TopicStats)
                    .unwrap();
            assert!(targets.is_empty(), "corruption={corruption}");
            assert_eq!(failures.len(), 1, "corruption={corruption}");
            assert_eq!(failures[0].code(), AdminQueryFailureCode::InvalidResponse);
            assert_eq!(failures[0].logical_target(), "broker-a");

            let source = FakeSource {
                cluster_info,
                route: Some(route),
                ..Default::default()
            };
            let error = query_topic_stats_from(
                &source,
                &QueryTopicStatsRequest::try_new("cluster-a", "orders", 10).unwrap(),
            )
            .await
            .unwrap_err();
            assert_eq!(error.code(), Some("ADMIN_QUERY_ALL_SOURCES_FAILED"));
            assert!(source.stats_calls.lock().unwrap().is_empty());
        }
    }

    #[tokio::test]
    async fn master_endpoint_must_exist_and_pass_the_shared_remoting_validator_without_rpc() {
        for invalid_master in [
            None,
            Some("https://secret.invalid:10911/path?token=value"),
            Some("127.0.0.1:0"),
        ] {
            let (mut cluster_info, route) = topology(&[("cluster-a", "broker-a", ADDRESS_A)]);
            let record = cluster_info
                .broker_addr_table
                .as_mut()
                .unwrap()
                .get_mut("broker-a")
                .unwrap();
            record.broker_addrs_mut().remove(&mix_all::MASTER_ID);
            if let Some(invalid_master) = invalid_master {
                record
                    .broker_addrs_mut()
                    .insert(mix_all::MASTER_ID, invalid_master.into());
            }

            let (targets, failures) =
                selected_cluster_route_masters(&cluster_info, &route, "cluster-a", AdminQuerySource::TopicStats)
                    .unwrap();
            assert!(targets.is_empty());
            assert_eq!(failures.len(), 1);
            assert_eq!(failures[0].code(), AdminQueryFailureCode::InvalidResponse);
            assert_eq!(failures[0].logical_target(), "broker-a");
            assert!(!serde_json::to_string(&failures).unwrap().contains("secret.invalid"));

            let source = FakeSource {
                cluster_info,
                route: Some(route),
                ..Default::default()
            };
            let error = query_topic_stats_from(
                &source,
                &QueryTopicStatsRequest::try_new("cluster-a", "orders", 10).unwrap(),
            )
            .await
            .unwrap_err();
            assert_eq!(error.code(), Some("ADMIN_QUERY_ALL_SOURCES_FAILED"));
            assert!(source.stats_calls.lock().unwrap().is_empty());
        }
    }

    #[tokio::test]
    async fn invalid_topology_is_partial_beside_a_valid_target_and_never_receives_an_rpc() {
        let mut source = source_with_two_clusters();
        source
            .cluster_info
            .broker_addr_table
            .as_mut()
            .unwrap()
            .get_mut("broker-b")
            .unwrap()
            .broker_addrs_mut()
            .insert(mix_all::MASTER_ID, "invalid-master".into());
        source.stats.insert(
            ADDRESS_A.to_string(),
            TestResult::Value(stats("broker-a", &[(0, 0, 5, 10)])),
        );

        let result = query_topic_stats_from(
            &source,
            &QueryTopicStatsRequest::try_new("cluster-a", "orders", 10).unwrap(),
        )
        .await
        .unwrap();

        assert!(result.partial);
        assert_eq!(result.data.total_message_count, 5);
        assert_eq!(result.source_failures.len(), 1);
        assert_eq!(result.source_failures[0].code(), AdminQueryFailureCode::InvalidResponse);
        assert_eq!(result.source_failures[0].logical_target(), "broker-b");
        assert_eq!(source.stats_calls.lock().unwrap().as_slice(), [ADDRESS_A]);
    }

    #[test]
    fn target_resolution_accepts_exactly_the_target_limit() {
        let mut broker_table = HashMap::new();
        let mut cluster_brokers = HashSet::new();
        let mut route_brokers = Vec::new();
        for index in 0..MAX_TOPIC_OBSERVATION_TARGETS {
            let broker_name = format!("broker-{index:02}");
            let address = format!("127.0.0.1:{}", 10_000 + index);
            let data = BrokerData::new(
                "cluster-a".into(),
                broker_name.clone().into(),
                HashMap::from([(mix_all::MASTER_ID, address.into())]),
                None,
            );
            broker_table.insert(broker_name.clone().into(), data.clone());
            cluster_brokers.insert(broker_name.into());
            route_brokers.push(data);
        }
        let cluster_info = ClusterInfo::new(
            Some(broker_table),
            Some(HashMap::from([("cluster-a".into(), cluster_brokers)])),
        );
        let route = TopicRouteData {
            broker_datas: route_brokers,
            ..Default::default()
        };

        let (targets, failures) =
            selected_cluster_route_masters(&cluster_info, &route, "cluster-a", AdminQuerySource::TopicStats).unwrap();
        assert_eq!(targets.len(), MAX_TOPIC_OBSERVATION_TARGETS);
        assert!(failures.is_empty());
    }

    #[test]
    fn target_resolution_rejects_oversized_selected_cluster_routes() {
        let mut broker_table = HashMap::new();
        let mut cluster_brokers = HashSet::new();
        let mut route_brokers = Vec::new();
        for index in 0..=MAX_TOPIC_OBSERVATION_TARGETS {
            let broker_name = format!("broker-{index:02}");
            let data = BrokerData::new(
                "cluster-a".into(),
                broker_name.clone().into(),
                HashMap::from([(mix_all::MASTER_ID, format!("internal-{index:02}").into())]),
                None,
            );
            broker_table.insert(broker_name.clone().into(), data.clone());
            cluster_brokers.insert(broker_name.into());
            route_brokers.push(data);
        }
        let cluster_info = ClusterInfo::new(
            Some(broker_table),
            Some(HashMap::from([("cluster-a".into(), cluster_brokers)])),
        );
        let route = TopicRouteData {
            broker_datas: route_brokers,
            ..Default::default()
        };

        let error = selected_cluster_route_masters(&cluster_info, &route, "cluster-a", AdminQuerySource::TopicStats)
            .unwrap_err();
        assert_eq!(error.code(), Some("TOPIC_OBSERVATION_TARGET_LIMIT_EXCEEDED"));
    }

    #[tokio::test]
    async fn missing_topic_and_cross_cluster_route_fail_closed() {
        let mut source = source_with_two_clusters();
        source.route = None;
        assert!(matches!(
            query_topic_config_from(
                &source,
                &QueryTopicConfigRequest::try_new("cluster-a", "orders").unwrap()
            )
            .await,
            Err(AdminError::NotFound { .. })
        ));

        let (cluster_info, route) = topology(&[("cluster-b", "broker-c", ADDRESS_C)]);
        source.cluster_info = cluster_info;
        source.route = Some(route);
        assert!(query_topic_stats_from(
            &source,
            &QueryTopicStatsRequest::try_new("cluster-a", "orders", 10).unwrap()
        )
        .await
        .is_err());
    }

    fn test_error(reason: &str) -> RocketMQError {
        RocketMQError::ResponseProcessFailed {
            operation: "topic_observation_test",
            reason: reason.to_string(),
        }
    }
}
