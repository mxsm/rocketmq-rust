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

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use chrono::DateTime;
use chrono::Utc;
use serde::Deserialize;
use serde_json::Value;
use serde_json::json;

use super::InventoryAccumulator;
use super::asset;
use super::edge;
use super::key;
use super::recoverable_gap;
use super::schema_mismatch;
use super::validate_inventory_name;
use crate::ConnectorError;
use crate::ConnectorErrorCode;
use crate::EvidenceOperation;
use crate::mcp::McpGateway;
use crate::read_gateway::ConnectorReadGateway;
use crate::read_gateway::ReadSession;
use crate::sources::common::SourceOutput;

const INVENTORY_PAGE_LIMIT: usize = 200;
const INVENTORY_DETAIL_QUERY_LIMIT: usize = 32;

struct ObservedOutput {
    output: SourceOutput,
    is_mcp: bool,
}

#[derive(Deserialize)]
struct ClusterOverviewWire {
    cluster: String,
    brokers: Vec<BrokerWire>,
    topic_count: usize,
    consumer_group_count: usize,
}

#[derive(Deserialize)]
struct AdminBrokerListWire {
    brokers: Vec<BrokerWire>,
}

#[derive(Clone, Deserialize)]
struct BrokerWire {
    cluster: Option<String>,
    broker_name: String,
    broker_id: u64,
    version: String,
    broker_active: bool,
    #[serde(default)]
    timer_progress: Option<String>,
    #[serde(default)]
    page_cache_lock_time_millis: Option<String>,
    #[serde(default)]
    space: Option<String>,
}

#[derive(Deserialize)]
struct TopicPageWire {
    items: Vec<TopicWire>,
    total_count: usize,
    has_more: bool,
}

#[derive(Deserialize)]
struct AdminTopicListWire {
    topics: Vec<TopicWire>,
}

#[derive(Clone, Deserialize)]
struct TopicWire {
    topic: String,
    #[serde(default)]
    consumer_group: Option<String>,
}

#[derive(Clone, Eq, Ord, PartialEq, PartialOrd)]
struct ConsumerTopic {
    group: String,
    topic: String,
}

struct TopicDiscovery {
    topics: Vec<String>,
    consumer_topics: Vec<ConsumerTopic>,
    partial: bool,
}

#[derive(Deserialize)]
struct ConsumerGroupPageWire {
    items: Vec<ConsumerGroupWire>,
    total_count: usize,
    has_more: bool,
}

#[derive(Deserialize)]
struct AdminConsumerGroupListWire {
    groups: Vec<ConsumerGroupWire>,
}

#[derive(Clone, Deserialize)]
struct ConsumerGroupWire {
    group: String,
    version: i32,
    client_count: i32,
    consume_type: String,
    message_model: String,
    consume_tps: f64,
    diff_total: i64,
}

#[derive(Deserialize)]
struct TopicDescriptionWire {
    topic: String,
    #[serde(default)]
    broker_names: Vec<String>,
    read_queue_count: u32,
    write_queue_count: u32,
    #[serde(default)]
    brokers: Vec<RouteBrokerWire>,
    items: Vec<RouteQueueWire>,
    total_count: usize,
    has_more: bool,
}

#[derive(Deserialize)]
struct AdminTopicRouteWire {
    #[serde(default)]
    brokers: Vec<RouteBrokerWire>,
    #[serde(default)]
    queues: Vec<RouteQueueWire>,
}

#[derive(Deserialize)]
struct RouteBrokerWire {
    broker_name: String,
    #[serde(default)]
    zone_name: Option<String>,
    #[serde(default)]
    enable_acting_master: bool,
}

#[derive(Clone, Deserialize)]
struct RouteQueueWire {
    broker_name: String,
    read_queue_nums: u32,
    write_queue_nums: u32,
    perm: u32,
    #[serde(default)]
    topic_sys_flag: u32,
}

#[derive(Deserialize)]
struct ConsumerLagWire {
    topic: String,
    consumer_group: String,
    items: Vec<QueueLagWire>,
    total_count: usize,
    has_more: bool,
}

#[derive(Deserialize)]
struct AdminConsumerLagWire {
    rows: Vec<QueueLagWire>,
}

#[derive(Clone, Deserialize)]
struct QueueLagWire {
    topic: String,
    broker_name: String,
    queue_id: i32,
}

pub(super) async fn collect<G>(
    inventory: &mut InventoryAccumulator,
    read_gateway: &ConnectorReadGateway<G>,
    session: &ReadSession<'_, '_>,
    max_rows: usize,
) -> Result<Vec<String>, ConnectorError>
where
    G: McpGateway,
{
    collect_overview(inventory, read_gateway, session)
        .await
        .inspect_err(|error| log_stage_error("overview", error))?;
    let topics = collect_topics(inventory, read_gateway, session, max_rows)
        .await
        .inspect_err(|error| log_stage_error("topics", error))?;
    let discovered_consumers = collect_consumers(inventory, read_gateway, session, max_rows)
        .await
        .inspect_err(|error| log_stage_error("consumers", error))?;

    let mut remaining_detail_queries = INVENTORY_DETAIL_QUERY_LIMIT;
    for topic in topics.topics.iter().take(remaining_detail_queries) {
        remaining_detail_queries = remaining_detail_queries.saturating_sub(1);
        match query_preferred(
            read_gateway,
            session,
            &EvidenceOperation::TopicDescribe {
                topic: topic.clone(),
                limit: Some(page_limit(max_rows)),
                cursor: None,
            },
            &format!("admin/topic-route/{topic}"),
        )
        .await
        {
            Ok(observed) if observed.is_mcp => {
                add_topic_description(inventory, observed.output, max_rows)
                    .inspect_err(|error| log_stage_error("topic_detail", error))?;
            }
            Ok(observed) => {
                add_admin_topic_route(inventory, topic, observed.output, max_rows)
                    .inspect_err(|error| log_stage_error("topic_detail", error))?;
            }
            Err(error) if recoverable_gap(&error) => {
                inventory.mark_gap("queue", "topic_route_source_unavailable");
                inventory.mark_partial("topic_route_query_incomplete");
                if error.code == ConnectorErrorCode::DeadlineExceeded {
                    break;
                }
            }
            Err(error) => return Err(error),
        }
    }
    if topics.topics.len() > INVENTORY_DETAIL_QUERY_LIMIT {
        inventory.mark_source("queue", true);
        inventory.mark_partial("topic_detail_query_budget_applied");
    } else if topics.topics.is_empty() && !topics.partial {
        inventory.mark_source("queue", false);
    }

    let consumer_query_budget = remaining_detail_queries;
    for consumer_topic in topics.consumer_topics.iter().take(consumer_query_budget) {
        match query_preferred(
            read_gateway,
            session,
            &EvidenceOperation::ConsumerLag {
                topic: consumer_topic.topic.clone(),
                consumer_group: consumer_topic.group.clone(),
                limit: Some(page_limit(max_rows)),
                cursor: None,
            },
            &format!("admin/consumer-lag/{}/{}", consumer_topic.group, consumer_topic.topic),
        )
        .await
        {
            Ok(observed) if observed.is_mcp => {
                add_consumer_lag(inventory, observed.output, max_rows)
                    .inspect_err(|error| log_stage_error("consumer_lag", error))?;
            }
            Ok(observed) => {
                add_admin_consumer_lag(
                    inventory,
                    &consumer_topic.group,
                    &consumer_topic.topic,
                    observed.output,
                    max_rows,
                )
                .inspect_err(|error| log_stage_error("consumer_lag", error))?;
            }
            Err(error) if recoverable_gap(&error) => {
                inventory.mark_partial("consumer_queue_relation_query_incomplete");
                if error.code == ConnectorErrorCode::DeadlineExceeded {
                    break;
                }
            }
            Err(error) => return Err(error),
        }
    }
    if topics.consumer_topics.len() > consumer_query_budget {
        inventory.mark_partial("consumer_queue_query_budget_applied");
    }
    let mut consumer_groups = discovered_consumers.into_iter().collect::<BTreeSet<_>>();
    consumer_groups.extend(topics.consumer_topics.into_iter().map(|association| association.group));
    Ok(consumer_groups.into_iter().collect())
}

fn log_stage_error(stage: &'static str, error: &ConnectorError) {
    tracing::warn!(
        source = "rocketmq",
        stage,
        code = error.code.as_str(),
        retryable = error.retryable,
        "read-only RocketMQ inventory stage failed"
    );
}

async fn collect_overview<G>(
    inventory: &mut InventoryAccumulator,
    read_gateway: &ConnectorReadGateway<G>,
    session: &ReadSession<'_, '_>,
) -> Result<(), ConnectorError>
where
    G: McpGateway,
{
    let context = session.context();
    let observed = query_preferred(
        read_gateway,
        session,
        &EvidenceOperation::ClusterOverview,
        "admin/brokers",
    )
    .await;
    match observed {
        Ok(observed) if observed.is_mcp => add_cluster_overview(inventory, context.external_cluster, observed.output)
            .inspect_err(|error| log_stage_error("overview_projection", error)),
        Ok(observed) => add_admin_brokers(inventory, context.external_cluster, observed.output)
            .inspect_err(|error| log_stage_error("overview_projection", error)),
        Err(error) if recoverable_gap(&error) => {
            inventory.mark_gap("broker", "broker_inventory_source_unavailable");
            inventory.mark_gap("store", "broker_store_inventory_source_unavailable");
            inventory.mark_partial("cluster_overview_query_incomplete");
            Ok(())
        }
        Err(error) => {
            log_stage_error("overview_query", &error);
            Err(error)
        }
    }
}

async fn collect_topics<G>(
    inventory: &mut InventoryAccumulator,
    read_gateway: &ConnectorReadGateway<G>,
    session: &ReadSession<'_, '_>,
    max_rows: usize,
) -> Result<TopicDiscovery, ConnectorError>
where
    G: McpGateway,
{
    match query_preferred(
        read_gateway,
        session,
        &EvidenceOperation::TopicList {
            filter: None,
            limit: Some(page_limit(max_rows)),
            cursor: None,
        },
        "admin/topics",
    )
    .await
    {
        Ok(observed) if observed.is_mcp => add_topics(inventory, observed.output),
        Ok(observed) => add_admin_topics(inventory, observed.output, max_rows),
        Err(error) if recoverable_gap(&error) => {
            inventory.mark_gap("topic", "topic_inventory_source_unavailable");
            inventory.mark_gap("queue", "topic_route_source_unavailable");
            inventory.mark_gap("consumer", "topic_consumer_relation_source_unavailable");
            inventory.mark_partial("topic_inventory_query_incomplete");
            Ok(TopicDiscovery {
                topics: Vec::new(),
                consumer_topics: Vec::new(),
                partial: true,
            })
        }
        Err(error) => Err(error),
    }
}

async fn collect_consumers<G>(
    inventory: &mut InventoryAccumulator,
    read_gateway: &ConnectorReadGateway<G>,
    session: &ReadSession<'_, '_>,
    max_rows: usize,
) -> Result<Vec<String>, ConnectorError>
where
    G: McpGateway,
{
    match query_preferred(
        read_gateway,
        session,
        &EvidenceOperation::ConsumerGroupList {
            filter: None,
            limit: Some(page_limit(max_rows)),
            cursor: None,
        },
        "admin/consumer-groups",
    )
    .await
    {
        Ok(observed) if observed.is_mcp => add_consumer_groups(inventory, observed.output),
        Ok(observed) => add_admin_consumer_groups(inventory, observed.output, max_rows),
        Err(error) if recoverable_gap(&error) => {
            inventory.mark_gap("consumer", "consumer_group_inventory_source_unavailable");
            inventory.mark_partial("consumer_group_query_incomplete");
            Ok(Vec::new())
        }
        Err(error) => Err(error),
    }
}

async fn query_preferred<G>(
    read_gateway: &ConnectorReadGateway<G>,
    session: &ReadSession<'_, '_>,
    operation: &EvidenceOperation,
    admin_resource: &str,
) -> Result<ObservedOutput, ConnectorError>
where
    G: McpGateway,
{
    match read_gateway.mcp_query(session, operation).await {
        Ok(output) => Ok(ObservedOutput { output, is_mcp: true }),
        Err(error) if recoverable_gap(&error) && read_gateway.admin_configured() => read_gateway
            .admin_query(session, admin_resource)
            .await
            .map(|output| ObservedOutput { output, is_mcp: false }),
        Err(error) => Err(error),
    }
}

fn add_cluster_overview(
    inventory: &mut InventoryAccumulator,
    expected_cluster: &str,
    output: SourceOutput,
) -> Result<(), ConnectorError> {
    let overview: ClusterOverviewWire =
        decode(output.content).inspect_err(|error| log_stage_error("overview_decode", error))?;
    if overview.cluster != expected_cluster {
        log_stage_error("overview_cluster_boundary", &schema_mismatch());
        return Err(schema_mismatch());
    }
    inventory.merge_cluster_attributes(json!({
        "topic_count": overview.topic_count,
        "consumer_group_count": overview.consumer_group_count,
        "broker_row_count": overview.brokers.len(),
    }));
    add_brokers(
        inventory,
        overview.brokers,
        "mcp",
        output.observed_at,
        output.freshness_seconds,
        output.partial,
    )
    .inspect_err(|error| log_stage_error("overview_brokers", error))
}

fn add_admin_brokers(
    inventory: &mut InventoryAccumulator,
    _expected_cluster: &str,
    output: SourceOutput,
) -> Result<(), ConnectorError> {
    let brokers: AdminBrokerListWire = decode(output.content)?;
    add_brokers(
        inventory,
        brokers.brokers,
        "admin",
        output.observed_at,
        output.freshness_seconds,
        output.partial,
    )
}

#[allow(
    clippy::too_many_arguments,
    reason = "inventory provenance and freshness remain explicit at the projection boundary"
)]
fn add_brokers(
    inventory: &mut InventoryAccumulator,
    brokers: Vec<BrokerWire>,
    source: &'static str,
    observed_at: DateTime<Utc>,
    freshness_seconds: u64,
    partial: bool,
) -> Result<(), ConnectorError> {
    let mut grouped = BTreeMap::<String, Vec<BrokerWire>>::new();
    for broker in brokers {
        validate_inventory_name(&broker.broker_name)?;
        if let Some(reported_cluster) = broker
            .cluster
            .as_deref()
            .filter(|reported_cluster| !reported_cluster.trim().is_empty())
        {
            validate_inventory_name(reported_cluster)?;
        }
        grouped.entry(broker.broker_name.clone()).or_default().push(broker);
    }
    inventory.mark_source("broker", partial);
    inventory.mark_source("store", partial);
    for (broker_name, mut members) in grouped {
        members.sort_by_key(|broker| broker.broker_id);
        let broker_ids = members.iter().map(|broker| broker.broker_id).collect::<Vec<_>>();
        let versions = members
            .iter()
            .map(|broker| broker.version.as_str())
            .collect::<BTreeSet<_>>();
        let reported_clusters = members
            .iter()
            .filter_map(|broker| broker.cluster.as_deref())
            .filter(|reported_cluster| !reported_cluster.trim().is_empty())
            .collect::<BTreeSet<_>>();
        let active_member_count = members.iter().filter(|broker| broker.broker_active).count();
        let broker_key = inventory.insert_asset(asset(
            "broker",
            &broker_name,
            &broker_name,
            source,
            json!({
                "broker_name": broker_name,
                "broker_ids": broker_ids,
                "versions": versions,
                "reported_clusters": reported_clusters,
                "member_count": members.len(),
                "active_member_count": active_member_count,
            }),
            observed_at,
            freshness_seconds,
            partial,
        ));
        inventory.insert_edge(edge(
            inventory.cluster_key().clone(),
            broker_key.clone(),
            "contains",
            source,
            observed_at,
            freshness_seconds,
            partial,
        ));

        let runtime_members = members
            .iter()
            .map(|broker| {
                json!({
                    "broker_id": broker.broker_id,
                    "space": broker.space,
                    "timer_progress": broker.timer_progress,
                    "page_cache_lock_time_millis": broker.page_cache_lock_time_millis,
                })
            })
            .collect::<Vec<_>>();
        let store_partial = partial
            || members.iter().any(|broker| {
                broker.space.is_none()
                    && broker.timer_progress.is_none()
                    && broker.page_cache_lock_time_millis.is_none()
            });
        inventory.mark_source("store", store_partial);
        let store_key = inventory.insert_asset(asset(
            "store",
            &format!("broker/{broker_name}"),
            &format!("{broker_name} store"),
            source,
            json!({
                "broker_name": broker_name,
                "implementation": "broker_embedded_store",
                "runtime_members": runtime_members,
            }),
            observed_at,
            freshness_seconds,
            store_partial,
        ));
        inventory.insert_edge(edge(
            broker_key,
            store_key,
            "stores_on",
            source,
            observed_at,
            freshness_seconds,
            store_partial,
        ));
    }
    Ok(())
}

fn add_topics(inventory: &mut InventoryAccumulator, output: SourceOutput) -> Result<TopicDiscovery, ConnectorError> {
    let topics: TopicPageWire = decode(output.content)?;
    validate_page(topics.items.len(), topics.total_count, topics.has_more)?;
    let partial = output.partial || topics.has_more || topics.items.len() < topics.total_count;
    add_topic_rows(
        inventory,
        topics.items,
        "mcp",
        output.observed_at,
        output.freshness_seconds,
        partial,
    )
}

fn add_admin_topics(
    inventory: &mut InventoryAccumulator,
    output: SourceOutput,
    max_rows: usize,
) -> Result<TopicDiscovery, ConnectorError> {
    let mut topics: AdminTopicListWire = decode(output.content)?;
    let truncated = topics.topics.len() > max_rows;
    topics.topics.truncate(max_rows);
    add_topic_rows(
        inventory,
        topics.topics,
        "admin",
        output.observed_at,
        output.freshness_seconds,
        output.partial || truncated,
    )
}

fn add_topic_rows(
    inventory: &mut InventoryAccumulator,
    rows: Vec<TopicWire>,
    source: &'static str,
    observed_at: DateTime<Utc>,
    freshness_seconds: u64,
    partial: bool,
) -> Result<TopicDiscovery, ConnectorError> {
    let mut topics = BTreeSet::new();
    let mut consumer_topics = BTreeSet::new();
    for row in rows {
        validate_inventory_name(&row.topic)?;
        let topic_key = key("topic", &row.topic);
        if topics.insert(row.topic.clone()) {
            inventory.insert_asset(asset(
                "topic",
                &row.topic,
                &row.topic,
                source,
                json!({}),
                observed_at,
                freshness_seconds,
                partial,
            ));
            inventory.insert_edge(edge(
                inventory.cluster_key().clone(),
                topic_key.clone(),
                "contains",
                source,
                observed_at,
                freshness_seconds,
                partial,
            ));
        }
        if let Some(group) = row.consumer_group.filter(|group| !group.is_empty()) {
            validate_inventory_name(&group)?;
            let consumer_key = inventory.insert_asset(asset(
                "consumer",
                &group,
                &group,
                source,
                json!({
                    "consumer_group": group,
                    "observed_via": "topic_consumer_association",
                }),
                observed_at,
                freshness_seconds,
                true,
            ));
            inventory.insert_edge(edge(
                consumer_key,
                topic_key,
                "consumes_from",
                source,
                observed_at,
                freshness_seconds,
                partial,
            ));
            consumer_topics.insert(ConsumerTopic {
                group,
                topic: row.topic,
            });
        }
    }
    inventory.mark_source("topic", partial);
    if !consumer_topics.is_empty() {
        inventory.mark_source("consumer", true);
    }
    Ok(TopicDiscovery {
        topics: topics.into_iter().collect(),
        consumer_topics: consumer_topics.into_iter().collect(),
        partial,
    })
}

fn add_consumer_groups(
    inventory: &mut InventoryAccumulator,
    output: SourceOutput,
) -> Result<Vec<String>, ConnectorError> {
    let groups: ConsumerGroupPageWire = decode(output.content)?;
    validate_page(groups.items.len(), groups.total_count, groups.has_more)?;
    let partial = output.partial || groups.has_more || groups.items.len() < groups.total_count;
    add_consumer_group_rows(
        inventory,
        groups.items,
        "mcp",
        output.observed_at,
        output.freshness_seconds,
        partial,
    )
}

fn add_admin_consumer_groups(
    inventory: &mut InventoryAccumulator,
    output: SourceOutput,
    max_rows: usize,
) -> Result<Vec<String>, ConnectorError> {
    let mut groups: AdminConsumerGroupListWire = decode(output.content)?;
    let truncated = groups.groups.len() > max_rows;
    groups.groups.truncate(max_rows);
    add_consumer_group_rows(
        inventory,
        groups.groups,
        "admin",
        output.observed_at,
        output.freshness_seconds,
        output.partial || truncated,
    )
}

fn add_consumer_group_rows(
    inventory: &mut InventoryAccumulator,
    groups: Vec<ConsumerGroupWire>,
    source: &'static str,
    observed_at: DateTime<Utc>,
    freshness_seconds: u64,
    partial: bool,
) -> Result<Vec<String>, ConnectorError> {
    let mut identities = BTreeSet::new();
    for group in groups {
        validate_inventory_name(&group.group)?;
        if !identities.insert(group.group.clone()) || group.client_count < 0 {
            return Err(schema_mismatch());
        }
        let group_name = group.group;
        let consumer_key = inventory.insert_asset(asset(
            "consumer",
            &group_name,
            &group_name,
            source,
            json!({
                "consumer_group": group_name,
                "version": group.version,
                "client_count": group.client_count,
                "consume_type": group.consume_type,
                "message_model": group.message_model,
                "consume_tps": group.consume_tps,
                "diff_total": group.diff_total,
            }),
            observed_at,
            freshness_seconds,
            partial,
        ));
        inventory.insert_edge(edge(
            inventory.cluster_key().clone(),
            consumer_key,
            "contains",
            source,
            observed_at,
            freshness_seconds,
            partial,
        ));
    }
    inventory.mark_source("consumer", partial);
    Ok(identities.into_iter().collect())
}

fn add_topic_description(
    inventory: &mut InventoryAccumulator,
    output: SourceOutput,
    max_rows: usize,
) -> Result<(), ConnectorError> {
    let description: TopicDescriptionWire = decode(output.content)?;
    validate_inventory_name(&description.topic)?;
    validate_page(description.items.len(), description.total_count, description.has_more)?;
    let route_partial = output.partial || description.has_more || description.items.len() < description.total_count;
    let known_brokers = description
        .broker_names
        .iter()
        .chain(description.brokers.iter().map(|broker| &broker.broker_name))
        .cloned()
        .collect::<BTreeSet<_>>();
    for broker in &description.brokers {
        validate_inventory_name(&broker.broker_name)?;
        ensure_route_broker(
            inventory,
            &broker.broker_name,
            "mcp",
            output.observed_at,
            output.freshness_seconds,
            route_partial,
            json!({
                "zone_name": broker.zone_name,
                "enable_acting_master": broker.enable_acting_master,
                "observed_via": "topic_route",
            }),
        );
    }
    for broker_name in known_brokers {
        validate_inventory_name(&broker_name)?;
        ensure_route_broker(
            inventory,
            &broker_name,
            "mcp",
            output.observed_at,
            output.freshness_seconds,
            true,
            json!({"observed_via": "topic_route"}),
        );
    }
    add_route_queues(
        inventory,
        &description.topic,
        description.items,
        description.read_queue_count,
        description.write_queue_count,
        "mcp",
        output.observed_at,
        output.freshness_seconds,
        route_partial,
        max_rows,
    )
}

fn add_admin_topic_route(
    inventory: &mut InventoryAccumulator,
    topic: &str,
    output: SourceOutput,
    max_rows: usize,
) -> Result<(), ConnectorError> {
    let route: Option<AdminTopicRouteWire> = decode(output.content)?;
    let Some(route) = route else {
        inventory.mark_source("queue", true);
        inventory.mark_gap("queue", "topic_route_not_found");
        return Ok(());
    };
    for broker in route.brokers {
        validate_inventory_name(&broker.broker_name)?;
        ensure_route_broker(
            inventory,
            &broker.broker_name,
            "admin",
            output.observed_at,
            output.freshness_seconds,
            output.partial,
            json!({
                "zone_name": broker.zone_name,
                "enable_acting_master": broker.enable_acting_master,
                "observed_via": "topic_route",
            }),
        );
    }
    let read_total = route.queues.iter().map(|queue| queue.read_queue_nums).sum();
    let write_total = route.queues.iter().map(|queue| queue.write_queue_nums).sum();
    add_route_queues(
        inventory,
        topic,
        route.queues,
        read_total,
        write_total,
        "admin",
        output.observed_at,
        output.freshness_seconds,
        output.partial,
        max_rows,
    )
}

#[allow(
    clippy::too_many_arguments,
    reason = "queue route observations retain explicit source and freshness metadata"
)]
fn add_route_queues(
    inventory: &mut InventoryAccumulator,
    topic: &str,
    routes: Vec<RouteQueueWire>,
    read_queue_count: u32,
    write_queue_count: u32,
    source: &'static str,
    observed_at: DateTime<Utc>,
    freshness_seconds: u64,
    mut partial: bool,
    max_rows: usize,
) -> Result<(), ConnectorError> {
    let topic_key = key("topic", topic);
    let mut remaining = inventory.remaining_asset_capacity(max_rows);
    if remaining == 0 && !routes.is_empty() {
        inventory.mark_source("queue", true);
        inventory.mark_partial("inventory_asset_budget_applied");
        return Ok(());
    }
    for route in routes {
        validate_inventory_name(&route.broker_name)?;
        let broker_key = ensure_route_broker(
            inventory,
            &route.broker_name,
            source,
            observed_at,
            freshness_seconds,
            true,
            json!({"observed_via": "topic_route"}),
        );
        let queue_count = route.read_queue_nums.max(route.write_queue_nums) as usize;
        let take = queue_count.min(remaining);
        partial |= take < queue_count;
        for queue_id in 0..take {
            let external_key = format!("{topic}/{}/{queue_id}", route.broker_name);
            let queue_key = inventory.insert_asset(asset(
                "queue",
                &external_key,
                &format!("{topic}/{}:{queue_id}", route.broker_name),
                source,
                json!({
                    "topic": topic,
                    "broker_name": route.broker_name,
                    "queue_id": queue_id,
                    "read_enabled": queue_id < route.read_queue_nums as usize,
                    "write_enabled": queue_id < route.write_queue_nums as usize,
                    "perm": route.perm,
                    "topic_sys_flag": route.topic_sys_flag,
                    "identity_basis": "rocketmq_route_queue_count",
                }),
                observed_at,
                freshness_seconds,
                partial,
            ));
            inventory.insert_edge(edge(
                topic_key.clone(),
                queue_key.clone(),
                "contains",
                source,
                observed_at,
                freshness_seconds,
                partial,
            ));
            inventory.insert_edge(edge(
                queue_key,
                broker_key.clone(),
                "routes_to",
                source,
                observed_at,
                freshness_seconds,
                partial,
            ));
        }
        remaining = remaining.saturating_sub(take);
        if remaining == 0 {
            break;
        }
    }
    inventory.mark_source("queue", partial);
    inventory.merge_asset_attributes(
        &topic_key,
        json!({
            "read_queue_count": read_queue_count,
            "write_queue_count": write_queue_count,
        }),
    );
    Ok(())
}

fn add_consumer_lag(
    inventory: &mut InventoryAccumulator,
    output: SourceOutput,
    max_rows: usize,
) -> Result<(), ConnectorError> {
    let lag: ConsumerLagWire = decode(output.content)?;
    validate_page(lag.items.len(), lag.total_count, lag.has_more)?;
    if lag.items.iter().any(|row| row.topic != lag.topic) {
        return Err(schema_mismatch());
    }
    let partial = output.partial || lag.has_more || lag.items.len() < lag.total_count;
    add_consumer_queue_rows(
        inventory,
        &lag.consumer_group,
        &lag.topic,
        lag.items,
        "mcp",
        output.observed_at,
        output.freshness_seconds,
        partial,
        max_rows,
    )
}

fn add_admin_consumer_lag(
    inventory: &mut InventoryAccumulator,
    group: &str,
    topic: &str,
    output: SourceOutput,
    max_rows: usize,
) -> Result<(), ConnectorError> {
    let mut lag: AdminConsumerLagWire = decode(output.content)?;
    let truncated = lag.rows.len() > max_rows;
    lag.rows.truncate(max_rows);
    if lag.rows.iter().any(|row| row.topic != topic) {
        return Err(schema_mismatch());
    }
    add_consumer_queue_rows(
        inventory,
        group,
        topic,
        lag.rows,
        "admin",
        output.observed_at,
        output.freshness_seconds,
        output.partial || truncated,
        max_rows,
    )
}

#[allow(
    clippy::too_many_arguments,
    reason = "consumer queue relationships retain explicit source and freshness metadata"
)]
fn add_consumer_queue_rows(
    inventory: &mut InventoryAccumulator,
    group: &str,
    topic: &str,
    rows: Vec<QueueLagWire>,
    source: &'static str,
    observed_at: DateTime<Utc>,
    freshness_seconds: u64,
    partial: bool,
    max_rows: usize,
) -> Result<(), ConnectorError> {
    validate_inventory_name(group)?;
    validate_inventory_name(topic)?;
    let consumer_key = key("consumer", group);
    let topic_key = key("topic", topic);
    for row in rows {
        validate_inventory_name(&row.broker_name)?;
        if row.queue_id < 0 {
            return Err(schema_mismatch());
        }
        let broker_key = ensure_route_broker(
            inventory,
            &row.broker_name,
            source,
            observed_at,
            freshness_seconds,
            true,
            json!({"observed_via": "consumer_lag"}),
        );
        let external_key = format!("{topic}/{}/{}", row.broker_name, row.queue_id);
        let queue_identity = key("queue", &external_key);
        if !inventory.contains_asset(&queue_identity) && inventory.remaining_asset_capacity(max_rows) == 0 {
            inventory.mark_source("queue", true);
            inventory.mark_partial("inventory_asset_budget_applied");
            continue;
        }
        let queue_key = inventory.insert_asset(asset(
            "queue",
            &external_key,
            &format!("{topic}/{}:{}", row.broker_name, row.queue_id),
            source,
            json!({
                "topic": topic,
                "broker_name": row.broker_name,
                "queue_id": row.queue_id,
                "observed_via": "consumer_lag",
            }),
            observed_at,
            freshness_seconds,
            true,
        ));
        inventory.insert_edge(edge(
            topic_key.clone(),
            queue_key.clone(),
            "contains",
            source,
            observed_at,
            freshness_seconds,
            partial,
        ));
        inventory.insert_edge(edge(
            queue_key.clone(),
            broker_key,
            "routes_to",
            source,
            observed_at,
            freshness_seconds,
            partial,
        ));
        if inventory.contains_asset(&consumer_key) {
            inventory.insert_edge(edge(
                consumer_key.clone(),
                queue_key,
                "consumes_from",
                source,
                observed_at,
                freshness_seconds,
                partial,
            ));
        }
    }
    Ok(())
}

#[allow(
    clippy::too_many_arguments,
    reason = "route broker placeholders keep source and freshness provenance explicit"
)]
pub(super) fn ensure_route_broker(
    inventory: &mut InventoryAccumulator,
    broker_name: &str,
    source: &'static str,
    observed_at: DateTime<Utc>,
    freshness_seconds: u64,
    partial: bool,
    attributes: Value,
) -> super::AssetKey {
    let broker_key = key("broker", broker_name);
    if !inventory.contains_asset(&broker_key) {
        inventory.insert_asset(asset(
            "broker",
            broker_name,
            broker_name,
            source,
            attributes,
            observed_at,
            freshness_seconds,
            partial,
        ));
        inventory.insert_edge(edge(
            inventory.cluster_key().clone(),
            broker_key.clone(),
            "contains",
            source,
            observed_at,
            freshness_seconds,
            partial,
        ));
        inventory.mark_source("broker", true);
    }
    broker_key
}

fn validate_page(item_count: usize, total_count: usize, has_more: bool) -> Result<(), ConnectorError> {
    if item_count > total_count || (has_more && item_count >= total_count) {
        return Err(schema_mismatch());
    }
    Ok(())
}

fn page_limit(max_rows: usize) -> u32 {
    u32::try_from(max_rows.clamp(1, INVENTORY_PAGE_LIMIT)).unwrap_or(INVENTORY_PAGE_LIMIT as u32)
}

fn decode<T: for<'de> Deserialize<'de>>(value: Value) -> Result<T, ConnectorError> {
    serde_json::from_value(value).map_err(|_| schema_mismatch())
}

#[cfg(test)]
pub(super) fn fixture_add_overview(
    inventory: &mut InventoryAccumulator,
    expected_cluster: &str,
    output: SourceOutput,
) -> Result<(), ConnectorError> {
    add_cluster_overview(inventory, expected_cluster, output)
}

#[cfg(test)]
pub(super) fn fixture_add_topics(
    inventory: &mut InventoryAccumulator,
    output: SourceOutput,
) -> Result<(), ConnectorError> {
    add_topics(inventory, output).map(|_| ())
}

#[cfg(test)]
pub(super) fn fixture_add_topic_description(
    inventory: &mut InventoryAccumulator,
    output: SourceOutput,
    max_rows: usize,
) -> Result<(), ConnectorError> {
    add_topic_description(inventory, output, max_rows)
}
