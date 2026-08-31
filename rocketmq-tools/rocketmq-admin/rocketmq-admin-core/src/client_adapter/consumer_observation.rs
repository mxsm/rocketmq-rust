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

//! Read-client implementation of address-free Consumer observations.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use cheetah_string::CheetahString;
use rocketmq_client_rust::ConsumerConnectionRead;
use rocketmq_client_rust::ConsumerGroupConfigRead;
use rocketmq_client_rust::ConsumerProgressRead;
use rocketmq_client_rust::DefaultMQAdminExt;
use rocketmq_client_rust::MQAdminConsumerObservationReadExt;
use rocketmq_client_rust::MQAdminReadExt;
use rocketmq_client_rust::SubscriptionGroupConfigVersioned;
use rocketmq_error::RocketMQError;
use rocketmq_model::common::consumer::consume_from_where::ConsumeFromWhere;
use rocketmq_model::common::mix_all;
use rocketmq_protocol::protocol::admin::consume_stats::ConsumeStats;
use rocketmq_protocol::protocol::body::broker_body::cluster_info::ClusterInfo;
use rocketmq_protocol::protocol::heartbeat::consume_type::ConsumeType;
use rocketmq_protocol::protocol::heartbeat::message_model::MessageModel;
use rocketmq_protocol::protocol::route::topic_route_data::TopicRouteData;

use crate::core::broker::is_valid_remoting_endpoint;
use crate::core::consumer_observation::*;
use crate::core::query::AdminQueryFailureCode;
use crate::core::query::AdminQueryResult;
use crate::core::query::AdminQuerySource;
use crate::core::query::AdminSourceFailure;
use crate::core::AdminError;
use crate::core::AdminResult;

type BrokerTarget = (String, CheetahString);
const MAX_CONSUMER_PROGRESS_SOURCE_ROWS: usize = 50_000;
const MAX_CONSUMER_PROGRESS_QUERY_ROWS: usize = 50_000;

#[allow(async_fn_in_trait)]
trait ConsumerObservationSource: Send {
    async fn cluster_info(&self) -> Result<ClusterInfo, RocketMQError>;
    async fn consumer_route(&self, consumer_group: &str) -> Result<Option<TopicRouteData>, RocketMQError>;
    async fn group_config(
        &self,
        broker_addr: CheetahString,
        consumer_group: &str,
    ) -> Result<ConsumerGroupConfigRead, RocketMQError>;
    async fn connection(
        &self,
        broker_addr: CheetahString,
        consumer_group: &str,
    ) -> Result<ConsumerConnectionRead, RocketMQError>;
    async fn progress(
        &self,
        broker_addr: CheetahString,
        consumer_group: &str,
    ) -> Result<ConsumerProgressRead, RocketMQError>;
}

impl ConsumerObservationSource for DefaultMQAdminExt {
    async fn cluster_info(&self) -> Result<ClusterInfo, RocketMQError> {
        MQAdminReadExt::examine_broker_cluster_info(self).await
    }

    async fn consumer_route(&self, consumer_group: &str) -> Result<Option<TopicRouteData>, RocketMQError> {
        MQAdminReadExt::examine_topic_route_info(self, CheetahString::from(mix_all::get_retry_topic(consumer_group)))
            .await
    }

    async fn group_config(
        &self,
        broker_addr: CheetahString,
        consumer_group: &str,
    ) -> Result<ConsumerGroupConfigRead, RocketMQError> {
        MQAdminConsumerObservationReadExt::consumer_group_config_at(
            self,
            broker_addr,
            CheetahString::from(consumer_group),
        )
        .await
    }

    async fn connection(
        &self,
        broker_addr: CheetahString,
        consumer_group: &str,
    ) -> Result<ConsumerConnectionRead, RocketMQError> {
        MQAdminConsumerObservationReadExt::consumer_connection_at(
            self,
            broker_addr,
            CheetahString::from(consumer_group),
        )
        .await
    }

    async fn progress(
        &self,
        broker_addr: CheetahString,
        consumer_group: &str,
    ) -> Result<ConsumerProgressRead, RocketMQError> {
        MQAdminConsumerObservationReadExt::consumer_progress_at(self, broker_addr, CheetahString::from(consumer_group))
            .await
    }
}

pub(crate) async fn query_consumer_group_details(
    admin: &DefaultMQAdminExt,
    request: &QueryConsumerGroupDetailsRequest,
) -> AdminResult<AdminQueryResult<QueryConsumerGroupDetailsResult>> {
    query_consumer_group_details_from(admin, request).await
}

pub(crate) async fn query_consumer_progress(
    admin: &DefaultMQAdminExt,
    request: &QueryConsumerProgressRequest,
) -> AdminResult<AdminQueryResult<QueryConsumerProgressResult>> {
    query_consumer_progress_from(admin, request).await
}

async fn query_consumer_group_details_from<S: ConsumerObservationSource>(
    source: &S,
    request: &QueryConsumerGroupDetailsRequest,
) -> AdminResult<AdminQueryResult<QueryConsumerGroupDetailsResult>> {
    let request = QueryConsumerGroupDetailsRequest::try_new(&request.cluster, &request.consumer_group)?;
    let (targets, mut failures) = resolve_consumer_targets(
        source,
        &request.cluster,
        &request.consumer_group,
        AdminQuerySource::ConsumerGroupConfig,
    )
    .await?;
    let mut rows = Vec::new();
    let mut configured = Vec::new();
    let mut absent_count = 0usize;

    for (broker_name, broker_addr) in targets {
        match source.group_config(broker_addr.clone(), &request.consumer_group).await {
            Ok(ConsumerGroupConfigRead::Present(snapshot)) => {
                rows.push(config_row(&broker_name, &snapshot));
                configured.push((broker_name, broker_addr, rows.len() - 1));
            }
            Ok(ConsumerGroupConfigRead::Absent) => {
                absent_count += 1;
                rows.push(absent_config_row(broker_name));
            }
            Err(error) => failures.push(source_failure(
                AdminQuerySource::ConsumerGroupConfig,
                &broker_name,
                &error,
            )),
        }
    }

    if configured.is_empty() {
        if !failures.is_empty() {
            return AdminQueryResult::from_sources(QueryConsumerGroupDetailsResult::default(), 0, failures);
        }
        if absent_count > 0 {
            return Err(AdminError::not_found("consumer group", request.consumer_group));
        }
    }

    let successful_configs = configured.len();
    let mut total_connection_count = 0u128;
    for (broker_name, broker_addr, row_index) in configured {
        match source.connection(broker_addr, &request.consumer_group).await {
            Ok(ConsumerConnectionRead::Offline) => {
                rows[row_index].connection_state = Some(ConsumerConnectionState::Offline);
            }
            Ok(ConsumerConnectionRead::Online(connection)) => {
                let count = connection.get_connection_set().len() as u128;
                total_connection_count = total_connection_count.saturating_add(count);
                let row = &mut rows[row_index];
                row.connection_state = Some(ConsumerConnectionState::Online);
                row.connection_count = count.min(u128::from(u64::MAX)) as u64;
                row.consume_type = Some(map_consume_type(connection.get_consume_type()));
                row.message_model = Some(map_message_model(connection.get_message_model()));
                row.consume_from_where = Some(map_consume_from_where(connection.get_consume_from_where()));
            }
            Err(error) => failures.push(source_failure(
                AdminQuerySource::ConsumerConnection,
                &broker_name,
                &error,
            )),
        }
    }

    rows.sort_by(|left, right| left.broker_name.cmp(&right.broker_name));
    let saturated = total_connection_count > u128::from(u64::MAX);
    let mut result = AdminQueryResult::from_sources(
        QueryConsumerGroupDetailsResult {
            consumer_group: request.consumer_group,
            total_connection_count: total_connection_count.min(u128::from(u64::MAX)) as u64,
            brokers: rows,
        },
        successful_configs,
        failures,
    )?;
    if saturated {
        result.partial = true;
        result
            .warnings
            .push(CONSUMER_DETAILS_TOTAL_SATURATED_WARNING.to_string());
        result.warnings.sort();
        result.warnings.dedup();
    }
    Ok(result)
}

async fn query_consumer_progress_from<S: ConsumerObservationSource>(
    source: &S,
    request: &QueryConsumerProgressRequest,
) -> AdminResult<AdminQueryResult<QueryConsumerProgressResult>> {
    let request = QueryConsumerProgressRequest::try_new(&request.cluster, &request.consumer_group, request.max_rows)?;
    let (targets, mut failures) = resolve_consumer_targets(
        source,
        &request.cluster,
        &request.consumer_group,
        AdminQuerySource::ConsumerGroupConfig,
    )
    .await?;
    let mut configured = Vec::new();
    let mut absent_count = 0usize;
    for (broker_name, broker_addr) in targets {
        match source.group_config(broker_addr.clone(), &request.consumer_group).await {
            Ok(ConsumerGroupConfigRead::Present(_)) => configured.push((broker_name, broker_addr)),
            Ok(ConsumerGroupConfigRead::Absent) => absent_count += 1,
            Err(error) => failures.push(source_failure(
                AdminQuerySource::ConsumerGroupConfig,
                &broker_name,
                &error,
            )),
        }
    }
    if configured.is_empty() {
        if !failures.is_empty() {
            return AdminQueryResult::from_sources(empty_progress_result(&request.consumer_group), 0, failures);
        }
        if absent_count > 0 {
            return Err(AdminError::not_found("consumer group", request.consumer_group));
        }
    }

    let mut collector = BoundedProgressCollector::new(request.max_rows);
    let mut successful_statistics = 0usize;
    let mut observed_nonempty = false;
    let mut consume_tps = 0.0f64;
    let mut invalid_tps = false;
    let mut tps_saturated = false;
    for (broker_name, broker_addr) in configured {
        match source.progress(broker_addr, &request.consumer_group).await {
            Ok(ConsumerProgressRead::Absent) => failures.push(AdminSourceFailure::new(
                AdminQuerySource::ConsumerStatistics,
                AdminQueryFailureCode::NotFound,
                false,
                &broker_name,
            )),
            Ok(ConsumerProgressRead::Observed(stats)) => {
                let tps = stats.get_consume_tps();
                let source_invalid_tps = !tps.is_finite() || tps < 0.0;
                let observation = collector.observe_source(&broker_name, stats);
                let source_succeeded = !observation.had_offsets || observation.valid_rows > 0;
                if source_succeeded {
                    successful_statistics += 1;
                    observed_nonempty |= observation.had_offsets;
                    if source_invalid_tps {
                        invalid_tps = true;
                    } else {
                        let next = consume_tps + tps;
                        if next.is_finite() {
                            consume_tps = next;
                        } else {
                            consume_tps = f64::MAX;
                            tps_saturated = true;
                        }
                    }
                }
                if observation.invalid || source_invalid_tps {
                    failures.push(invalid_response_failure(
                        AdminQuerySource::ConsumerStatistics,
                        &broker_name,
                    ));
                }
            }
            Err(error) => failures.push(source_failure(
                AdminQuerySource::ConsumerStatistics,
                &broker_name,
                &error,
            )),
        }
    }

    let truncated = collector.queue_count > request.max_rows;
    let saturated = collector.aggregates_saturated
        || collector.total_lag > u128::from(u64::MAX)
        || collector.total_inflight > u128::from(u64::MAX)
        || tps_saturated;
    let result_topic_count = collector.topics.len();
    let result_queue_count = collector.queue_count;
    let result_total_lag = collector.total_lag.min(u128::from(u64::MAX)) as u64;
    let result_max_queue_lag = collector.max_queue_lag;
    let result_total_inflight = collector.total_inflight.min(u128::from(u64::MAX)) as u64;
    let queues = collector.into_rows();
    let result = QueryConsumerProgressResult {
        consumer_group: request.consumer_group,
        state: if observed_nonempty {
            ConsumerProgressState::Observed
        } else {
            ConsumerProgressState::NoConsumption
        },
        topic_count: result_topic_count,
        queue_count: result_queue_count,
        total_lag: result_total_lag,
        max_queue_lag: result_max_queue_lag,
        total_inflight: result_total_inflight,
        consume_tps,
        queues,
        truncated,
    };
    let mut result = AdminQueryResult::from_sources(result, successful_statistics, failures)?;
    if truncated {
        result.partial = true;
        result.warnings.push(CONSUMER_PROGRESS_TRUNCATED_WARNING.to_string());
    }
    if saturated {
        result.partial = true;
        result
            .warnings
            .push(CONSUMER_PROGRESS_TOTAL_SATURATED_WARNING.to_string());
    }
    if invalid_tps {
        result.partial = true;
        result.warnings.push(CONSUMER_PROGRESS_INVALID_TPS_WARNING.to_string());
    }
    result.warnings.sort();
    result.warnings.dedup();
    Ok(result)
}

async fn resolve_consumer_targets<S: ConsumerObservationSource>(
    source: &S,
    cluster: &str,
    consumer_group: &str,
    failure_source: AdminQuerySource,
) -> AdminResult<(Vec<BrokerTarget>, Vec<AdminSourceFailure>)> {
    let cluster_info = source
        .cluster_info()
        .await
        .map_err(|error| backend_error("examine_broker_cluster_info", error))?;
    let route = source
        .consumer_route(consumer_group)
        .await
        .map_err(|error| backend_error("examine_consumer_route_info", error))?
        .ok_or_else(|| AdminError::not_found("consumer group", consumer_group))?;
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
        let listed = cluster_brokers.contains(broker_name);
        if broker.cluster() != cluster {
            if listed {
                failures.push(invalid_response_failure(source, broker_name));
            }
            continue;
        }
        if !listed || !safe_logical_name(broker_name) {
            failures.push(invalid_response_failure(source, broker_name));
            continue;
        }
        route_brokers.insert(broker_name.to_string());
    }
    if route_brokers.is_empty() {
        if !failures.is_empty() {
            return Ok((Vec::new(), failures));
        }
        return Err(AdminError::not_found("consumer route in selected cluster", cluster));
    }
    if route_brokers.len() > MAX_CONSUMER_OBSERVATION_TARGETS {
        return Err(AdminError::backend_view(
            "resolve_consumer_observation_targets",
            "CONSUMER_OBSERVATION_TARGET_LIMIT_EXCEEDED",
            "Consumer route has too many selected-cluster Broker targets",
            None,
            422,
            false,
        ));
    }
    let broker_table = cluster_info.broker_addr_table.as_ref();
    // Consuming the BTreeSet fixes Broker query order by logical name. Query-wide progress budget
    // ownership is therefore deterministic when a later source would cross the remaining bound.
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
        match broker
            .broker_addrs()
            .get(&mix_all::MASTER_ID)
            .filter(|address| is_valid_remoting_endpoint(address.as_str()))
        {
            Some(address) => targets.push((broker_name, address.clone())),
            None => failures.push(invalid_response_failure(source, &broker_name)),
        }
    }
    Ok((targets, failures))
}

fn config_row(broker_name: &str, snapshot: &SubscriptionGroupConfigVersioned) -> ConsumerGroupDetailsBrokerRow {
    let config = &snapshot.config;
    ConsumerGroupDetailsBrokerRow {
        broker_name: broker_name.to_string(),
        config_state: ConsumerGroupConfigState::Present,
        config_version: Some(snapshot.version),
        consume_enable: Some(config.consume_enable()),
        consume_from_min_enable: Some(config.consume_from_min_enable()),
        consume_broadcast_enable: Some(config.consume_broadcast_enable()),
        consume_message_orderly: Some(config.consume_message_orderly()),
        retry_queue_nums: Some(config.retry_queue_nums()),
        retry_max_times: Some(config.retry_max_times()),
        notify_consumer_ids_changed_enable: Some(config.notify_consumer_ids_changed_enable()),
        consume_timeout_minutes: Some(config.consume_timeout_minute()),
        connection_state: None,
        connection_count: 0,
        consume_type: None,
        message_model: None,
        consume_from_where: None,
    }
}

fn absent_config_row(broker_name: String) -> ConsumerGroupDetailsBrokerRow {
    ConsumerGroupDetailsBrokerRow {
        broker_name,
        config_state: ConsumerGroupConfigState::Absent,
        config_version: None,
        consume_enable: None,
        consume_from_min_enable: None,
        consume_broadcast_enable: None,
        consume_message_orderly: None,
        retry_queue_nums: None,
        retry_max_times: None,
        notify_consumer_ids_changed_enable: None,
        consume_timeout_minutes: None,
        connection_state: None,
        connection_count: 0,
        consume_type: None,
        message_model: None,
        consume_from_where: None,
    }
}

type ProgressRowKey = (String, String, i32);

struct SourceProgressObservation {
    had_offsets: bool,
    valid_rows: usize,
    invalid: bool,
}

/// Retains only the lexicographically smallest requested queue rows while computing complete
/// aggregates. The decoded response table remains bounded by the remoting transport body limit;
/// this collector never creates a second full row table or vector.
struct BoundedProgressCollector {
    limit: usize,
    rows: BTreeMap<ProgressRowKey, ConsumerProgressQueueRow>,
    // Exact topic counting requires one name per distinct topic, bounded by the fixed per-source
    // row cap and the 64-target topology cap, rather than one full queue-row copy.
    topics: BTreeSet<String>,
    // Broker targets are unique by topology resolution and capped at 64. Keeping the names here
    // makes a future accidental repeated source fail closed instead of double-counting aggregates.
    observed_brokers: BTreeSet<String>,
    decoded_rows: usize,
    queue_count: usize,
    total_lag: u128,
    total_inflight: u128,
    max_queue_lag: u64,
    aggregates_saturated: bool,
    #[cfg(test)]
    max_retained_len: usize,
}

impl BoundedProgressCollector {
    fn new(limit: usize) -> Self {
        Self {
            limit,
            rows: BTreeMap::new(),
            topics: BTreeSet::new(),
            observed_brokers: BTreeSet::new(),
            decoded_rows: 0,
            queue_count: 0,
            total_lag: 0,
            total_inflight: 0,
            max_queue_lag: 0,
            aggregates_saturated: false,
            #[cfg(test)]
            max_retained_len: 0,
        }
    }

    fn observe_source(&mut self, broker_name: &str, stats: ConsumeStats) -> SourceProgressObservation {
        let had_offsets = !stats.offset_table.is_empty();
        // Decoding is already bounded by the remoting transport body limit. Reject before cloning
        // any row or topic when a decoded source still exceeds this fixed observation-layer cap.
        if stats.offset_table.len() > MAX_CONSUMER_PROGRESS_SOURCE_ROWS {
            return SourceProgressObservation {
                had_offsets,
                valid_rows: 0,
                invalid: true,
            };
        }
        let Some(reserved_rows) = self.decoded_rows.checked_add(stats.offset_table.len()) else {
            return SourceProgressObservation {
                had_offsets,
                valid_rows: 0,
                invalid: true,
            };
        };
        if reserved_rows > MAX_CONSUMER_PROGRESS_QUERY_ROWS {
            return SourceProgressObservation {
                had_offsets,
                valid_rows: 0,
                invalid: true,
            };
        }
        if self.observed_brokers.contains(broker_name) {
            return SourceProgressObservation {
                had_offsets,
                valid_rows: 0,
                invalid: true,
            };
        }
        // Reserve the whole source before any topic/row clone, broker-set insertion, or aggregate
        // update. A rejected source is never partially admitted and consumes no query budget.
        self.decoded_rows = reserved_rows;
        self.observed_brokers.insert(broker_name.to_string());

        let mut valid_rows = 0usize;
        let mut invalid = false;
        // `offset_table` is a HashMap keyed by the exact wire topic, broker name, and queue id.
        // The checks below do not trim or normalize any key component, so one decoded table cannot
        // produce a normalized-key collision. Distinct Broker sources also have distinct exact
        // broker names, as enforced above and by topology resolution.
        for (queue, offset) in stats.offset_table {
            let topic = queue.topic_str();
            let broker_offset = offset.get_broker_offset();
            let consumer_offset = offset.get_consumer_offset();
            let pull_offset = offset.get_pull_offset();
            let last_timestamp = offset.get_last_timestamp();
            let valid = safe_topic_name(topic)
                && queue.broker_name().as_str() == broker_name
                && queue.queue_id() >= 0
                && broker_offset >= 0
                && consumer_offset >= 0
                && pull_offset >= 0
                && last_timestamp >= 0;
            let Some(lag) = broker_offset
                .checked_sub(consumer_offset)
                .and_then(|value| u64::try_from(value).ok())
            else {
                invalid = true;
                continue;
            };
            let Some(inflight) = pull_offset
                .checked_sub(consumer_offset)
                .and_then(|value| u64::try_from(value).ok())
            else {
                invalid = true;
                continue;
            };
            if !valid {
                invalid = true;
                continue;
            }

            valid_rows = valid_rows.saturating_add(1);
            let row = ConsumerProgressQueueRow {
                topic: topic.to_string(),
                broker_name: broker_name.to_string(),
                queue_id: queue.queue_id(),
                broker_offset,
                consumer_offset,
                pull_offset,
                lag,
                inflight,
                last_timestamp,
            };
            self.observe_unique_row(row);
        }
        debug_assert!(self.decoded_rows <= MAX_CONSUMER_PROGRESS_QUERY_ROWS);
        debug_assert!(self.topics.len() <= self.decoded_rows);
        debug_assert!(self.rows.len() <= self.limit);
        SourceProgressObservation {
            had_offsets,
            valid_rows,
            invalid,
        }
    }

    fn observe_unique_row(&mut self, row: ConsumerProgressQueueRow) {
        self.topics.insert(row.topic.clone());
        let Some(queue_count) = self.queue_count.checked_add(1) else {
            self.queue_count = usize::MAX;
            self.aggregates_saturated = true;
            return;
        };
        self.queue_count = queue_count;
        self.total_lag = self.total_lag.checked_add(u128::from(row.lag)).unwrap_or_else(|| {
            self.aggregates_saturated = true;
            u128::MAX
        });
        self.total_inflight = self
            .total_inflight
            .checked_add(u128::from(row.inflight))
            .unwrap_or_else(|| {
                self.aggregates_saturated = true;
                u128::MAX
            });
        self.max_queue_lag = self.max_queue_lag.max(row.lag);

        let key = (row.topic.clone(), row.broker_name.clone(), row.queue_id);
        if self.rows.len() < self.limit {
            self.rows.insert(key, row);
        } else if self.rows.last_key_value().is_some_and(|(last, _)| key < *last) {
            self.rows.pop_last();
            self.rows.insert(key, row);
        }
        #[cfg(test)]
        {
            self.max_retained_len = self.max_retained_len.max(self.rows.len());
        }
    }

    fn into_rows(self) -> Vec<ConsumerProgressQueueRow> {
        self.rows.into_values().collect()
    }
}

fn empty_progress_result(consumer_group: &str) -> QueryConsumerProgressResult {
    QueryConsumerProgressResult {
        consumer_group: consumer_group.to_string(),
        state: ConsumerProgressState::NoConsumption,
        topic_count: 0,
        queue_count: 0,
        total_lag: 0,
        max_queue_lag: 0,
        total_inflight: 0,
        consume_tps: 0.0,
        queues: Vec::new(),
        truncated: false,
    }
}

fn map_consume_type(value: Option<ConsumeType>) -> ConsumerConsumeType {
    match value {
        Some(ConsumeType::ConsumeActively) => ConsumerConsumeType::Pull,
        Some(ConsumeType::ConsumePassively) => ConsumerConsumeType::Push,
        Some(ConsumeType::ConsumePop) => ConsumerConsumeType::Pop,
        None => ConsumerConsumeType::Unknown,
    }
}

fn map_message_model(value: Option<MessageModel>) -> ConsumerMessageModel {
    match value {
        Some(MessageModel::Broadcasting) => ConsumerMessageModel::Broadcasting,
        Some(MessageModel::Clustering) => ConsumerMessageModel::Clustering,
        None => ConsumerMessageModel::Unknown,
    }
}

#[allow(deprecated)]
fn map_consume_from_where(value: Option<ConsumeFromWhere>) -> ConsumerConsumeFromWhere {
    match value {
        Some(ConsumeFromWhere::ConsumeFromLastOffset) => ConsumerConsumeFromWhere::LastOffset,
        Some(ConsumeFromWhere::ConsumeFromLastOffsetAndFromMinWhenBootFirst) => {
            ConsumerConsumeFromWhere::LastOffsetAndMinFirst
        }
        Some(ConsumeFromWhere::ConsumeFromMinOffset) => ConsumerConsumeFromWhere::MinOffset,
        Some(ConsumeFromWhere::ConsumeFromMaxOffset) => ConsumerConsumeFromWhere::MaxOffset,
        Some(ConsumeFromWhere::ConsumeFromFirstOffset) => ConsumerConsumeFromWhere::FirstOffset,
        Some(ConsumeFromWhere::ConsumeFromTimestamp) => ConsumerConsumeFromWhere::Timestamp,
        None => ConsumerConsumeFromWhere::Unknown,
    }
}

fn safe_logical_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 128
        && name.is_ascii()
        && name
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.'))
}

fn safe_topic_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 255
        && name.is_ascii()
        && name
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '%' | '-' | '_' | '|'))
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
#[path = "consumer_observation_tests.rs"]
mod tests;
