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

//! Bounded native topology collection through exact advertised targets.

#[path = "topology_replication.rs"]
mod replication;

use crate::topic_observation::promote_to_admin_error;
use crate::{
    client_adapter::lifecycle::AdminSession,
    core::{topology::*, AdminFuture, AdminResult},
};
use rocketmq_client_rust::{
    ConsumerConnectionRead, ConsumerProgressRead, DefaultMQAdminExt, MQAdminConsumerObservationReadExt as _,
    MQAdminReadExt as _,
};
use rocketmq_protocol::protocol::{
    body::broker_body::cluster_info::ClusterInfo, route::topic_route_data::TopicRouteData,
};
use std::collections::{BTreeMap, BTreeSet};

const GROUP_CATALOG_LIMIT: usize = 4096;

trait TopologySource: Send + Sync {
    async fn cluster(&self) -> AdminResult<ClusterInfo>;
    async fn names(&self) -> AdminResult<Vec<String>>;
    async fn runtime(&self, target: &TopologyBrokerKey) -> AdminResult<BTreeMap<String, String>>;
    async fn route(&self, topic: &str) -> AdminResult<Option<TopicRouteData>>;
    async fn producers(&self, target: &TopologyBrokerKey) -> AdminResult<Vec<(String, Vec<String>)>>;
    async fn groups(&self, target: &TopologyBrokerKey) -> AdminResult<Vec<String>>;
    async fn connections(&self, target: &TopologyBrokerKey, group: &str) -> AdminResult<Vec<String>>;
    async fn progress(&self, target: &TopologyBrokerKey, group: &str) -> AdminResult<Option<(i64, f64)>>;
    async fn config(&self, target: &TopologyBrokerKey) -> AdminResult<BTreeMap<String, String>>;
}
impl TopologySource for DefaultMQAdminExt {
    async fn cluster(&self) -> AdminResult<ClusterInfo> {
        self.examine_broker_cluster_info()
            .await
            .map_err(|error| promote_to_admin_error("topology_cluster", error))
    }
    async fn names(&self) -> AdminResult<Vec<String>> {
        self.fetch_all_topic_list()
            .await
            .map(|list| list.topic_list.into_iter().map(|topic| topic.to_string()).collect())
            .map_err(|error| promote_to_admin_error("topology_topics", error))
    }
    async fn runtime(&self, target: &TopologyBrokerKey) -> AdminResult<BTreeMap<String, String>> {
        self.fetch_broker_runtime_stats(target.address.as_str().into())
            .await
            .map(|table| {
                table
                    .table
                    .into_iter()
                    .map(|(key, value)| (key.to_string(), value.to_string()))
                    .collect()
            })
            .map_err(|error| promote_to_admin_error("topology_runtime", error))
    }
    async fn route(&self, topic: &str) -> AdminResult<Option<TopicRouteData>> {
        self.examine_topic_route_info(topic.into())
            .await
            .map_err(|error| promote_to_admin_error("topology_route", error))
    }
    async fn producers(&self, target: &TopologyBrokerKey) -> AdminResult<Vec<(String, Vec<String>)>> {
        self.get_all_producer_info(target.address.as_str().into())
            .await
            .map(|table| {
                table
                    .data()
                    .iter()
                    .map(|(group, rows)| {
                        (
                            group.clone(),
                            rows.iter().map(|row| row.client_id().to_string()).collect(),
                        )
                    })
                    .collect()
            })
            .map_err(|error| promote_to_admin_error("topology_producers", error))
    }
    async fn groups(&self, target: &TopologyBrokerKey) -> AdminResult<Vec<String>> {
        rocketmq_client_rust::ConsumerAdmin::get_all_subscription_group(self, target.address.as_str().into(), 3000)
            .await
            .map(|wrapper| {
                wrapper
                    .get_subscription_group_table()
                    .keys()
                    .map(ToString::to_string)
                    .collect()
            })
            .map_err(|error| promote_to_admin_error("topology_groups", error))
    }
    async fn connections(&self, target: &TopologyBrokerKey, group: &str) -> AdminResult<Vec<String>> {
        self.consumer_connection_at(target.address.as_str().into(), group.into())
            .await
            .map(|reading| match reading {
                ConsumerConnectionRead::Online(connection) => connection
                    .get_connection_set()
                    .iter()
                    .map(|client| client.get_client_id().to_string())
                    .collect(),
                ConsumerConnectionRead::Offline => Vec::new(),
            })
            .map_err(|error| promote_to_admin_error("topology_connections", error))
    }
    async fn progress(&self, target: &TopologyBrokerKey, group: &str) -> AdminResult<Option<(i64, f64)>> {
        self.consumer_progress_at(target.address.as_str().into(), group.into())
            .await
            .map(|reading| match reading {
                ConsumerProgressRead::Observed(stats) => Some((stats.compute_total_diff(), stats.consume_tps)),
                ConsumerProgressRead::Absent => None,
            })
            .map_err(|error| promote_to_admin_error("topology_progress", error))
    }
    async fn config(&self, target: &TopologyBrokerKey) -> AdminResult<BTreeMap<String, String>> {
        rocketmq_client_rust::BrokerAdmin::get_broker_config(self, target.address.as_str().into())
            .await
            .map(|entries| {
                entries
                    .into_iter()
                    .map(|(key, value)| (key.to_string(), value.to_string()))
                    .collect()
            })
            .map_err(|error| promote_to_admin_error("topology_config", error))
    }
}

impl TopologyReadAdmin for AdminSession {
    fn topology_brokers(&self) -> AdminFuture<'_, TopologyRead<TopologyBrokerRow>> {
        Box::pin(async move {
            self.ensure_open()?;
            brokers(&self.inner).await
        })
    }
    fn topology_topics(&self) -> AdminFuture<'_, TopologyRead<TopologyTopicRow>> {
        Box::pin(async move {
            self.ensure_open()?;
            topics(&self.inner).await
        })
    }
    fn topology_producers(&self) -> AdminFuture<'_, TopologyRead<TopologyGroupRow>> {
        Box::pin(async move {
            self.ensure_open()?;
            producers(&self.inner).await
        })
    }
    fn topology_consumers(&self) -> AdminFuture<'_, TopologyRead<TopologyGroupRow>> {
        Box::pin(async move {
            self.ensure_open()?;
            consumers(&self.inner).await
        })
    }
    fn topology_replication(&self) -> AdminFuture<'_, TopologyReplicationRead> {
        Box::pin(async move {
            self.ensure_open()?;
            replication::observe(&self.inner).await
        })
    }
}

fn targets(cluster: &ClusterInfo) -> (Vec<TopologyBrokerKey>, usize) {
    let mut result = BTreeSet::new();
    let mut invalid = 0;
    if let (Some(clusters), Some(brokers)) = (&cluster.cluster_addr_table, &cluster.broker_addr_table) {
        for (name, data) in brokers {
            if name != data.broker_name()
                || !clusters
                    .get(data.cluster())
                    .is_some_and(|members| members.contains(name))
            {
                invalid += 1;
                continue;
            }
            for (id, address) in data.broker_addrs() {
                if crate::core::broker::canonical_remoting_endpoint(address).is_none() {
                    invalid += 1;
                    continue;
                }
                result.insert(TopologyBrokerKey {
                    cluster: data.cluster().to_string(),
                    broker_name: name.to_string(),
                    broker_id: *id,
                    address: address.to_string(),
                });
            }
        }
        invalid += clusters
            .values()
            .flat_map(|members| members.iter())
            .filter(|name| !brokers.contains_key(*name))
            .count();
    } else {
        invalid = 1;
    }
    (result.into_iter().collect(), invalid)
}
async fn masters<S: TopologySource>(source: &S) -> AdminResult<(Vec<TopologyBrokerKey>, TopologyReadQuality)> {
    let (all, invalid) = targets(&source.cluster().await?);
    let all = all
        .into_iter()
        .filter(|target| target.broker_id == 0)
        .collect::<Vec<_>>();
    let mut quality = TopologyReadQuality::complete(0);
    quality.truncated = all.len() > TOPOLOGY_MASTER_TARGETS;
    quality.inventory_complete = !quality.truncated && invalid == 0;
    quality.total_complete = quality.inventory_complete;
    quality.failures = invalid;
    Ok((all.into_iter().take(TOPOLOGY_MASTER_TARGETS).collect(), quality))
}

fn rate(entries: &BTreeMap<String, String>, key: &str) -> Option<f64> {
    entries
        .get(key)?
        .split_whitespace()
        .next()?
        .parse::<f64>()
        .ok()
        .filter(|rate| rate.is_finite() && *rate >= 0.)
}

async fn brokers<S: TopologySource>(source: &S) -> AdminResult<TopologyRead<TopologyBrokerRow>> {
    let (all, invalid) = targets(&source.cluster().await?);
    let mut quality = TopologyReadQuality::complete(all.len());
    quality.truncated = all.len() > TOPOLOGY_BROKER_TARGETS;
    quality.inventory_complete = !quality.truncated && invalid == 0;
    quality.total_complete = invalid == 0;
    quality.failures = invalid;
    let mut items = Vec::new();
    for identity in all.into_iter().take(TOPOLOGY_BROKER_TARGETS) {
        let entries = match source.runtime(&identity).await {
            Ok(entries) => {
                quality.successful_reads += 1;
                Some(entries)
            }
            Err(_) => {
                quality.failures += 1;
                None
            }
        };
        items.push(TopologyBrokerRow {
            identity,
            version: entries
                .as_ref()
                .and_then(|entries| entries.get("brokerVersionDesc").cloned()),
            produce_tps: entries.as_ref().and_then(|entries| rate(entries, "putTps")),
            consume_tps: entries
                .as_ref()
                .and_then(|entries| rate(entries, "getTransferredTps").or_else(|| rate(entries, "getTransferedTps"))),
            runtime_available: entries.is_some(),
        });
    }
    Ok(TopologyRead { items, quality })
}

async fn topics<S: TopologySource>(source: &S) -> AdminResult<TopologyRead<TopologyTopicRow>> {
    let names = source.names().await?.into_iter().collect::<BTreeSet<_>>();
    let mut quality = TopologyReadQuality::complete(names.len());
    quality.successful_reads = 1;
    quality.truncated = names.len() > TOPOLOGY_TOPIC_ROUTES;
    quality.inventory_complete = !quality.truncated;
    let mut items = Vec::new();
    for topic in names.into_iter().take(TOPOLOGY_TOPIC_ROUTES) {
        let route = match source.route(&topic).await {
            Ok(Some(route)) => {
                quality.successful_reads += 1;
                Some(route)
            }
            _ => {
                quality.failures += 1;
                quality.inventory_complete = false;
                None
            }
        };
        let route_observed = route.is_some();
        let mut brokers = BTreeSet::new();
        if let Some(route) = route {
            for data in &route.broker_datas {
                for (id, address) in data.broker_addrs() {
                    brokers.insert(TopologyBrokerKey {
                        cluster: data.cluster().to_string(),
                        broker_name: data.broker_name().to_string(),
                        broker_id: *id,
                        address: address.to_string(),
                    });
                }
            }
        }
        if brokers.len() > TOPOLOGY_BROKER_TARGETS {
            quality.truncated = true;
        }
        items.push(TopologyTopicRow {
            topic,
            brokers: brokers.into_iter().take(TOPOLOGY_BROKER_TARGETS).collect(),
            route_observed,
        });
    }
    Ok(TopologyRead { items, quality })
}

async fn producers<S: TopologySource>(source: &S) -> AdminResult<TopologyRead<TopologyGroupRow>> {
    let (targets, mut quality) = masters(source).await?;
    let mut groups = BTreeMap::<String, (BTreeSet<String>, BTreeSet<TopologyBrokerKey>)>::new();
    for target in targets {
        match source.producers(&target).await {
            Ok(rows) => {
                quality.successful_reads += 1;
                for (group, clients) in rows {
                    if matches!(
                        group.as_str(),
                        "CLIENT_INNER_PRODUCER" | "DEFAULT_PRODUCER" | "SELF_TEST_P_GROUP"
                    ) {
                        continue;
                    }
                    if !groups.contains_key(&group) && groups.len() == GROUP_CATALOG_LIMIT {
                        quality.truncated = true;
                        quality.total_complete = false;
                        continue;
                    }
                    let (ids, brokers) = groups.entry(group).or_default();
                    ids.extend(clients);
                    brokers.insert(target.clone());
                }
            }
            Err(_) => {
                quality.failures += 1;
                quality.inventory_complete = false;
                quality.total_complete = false;
            }
        }
    }
    quality.total = groups.len();
    let complete = quality.total_complete;
    let mut items = groups
        .into_iter()
        .map(|(group, (clients, brokers))| TopologyGroupRow {
            group,
            brokers: brokers.into_iter().collect(),
            connections: complete.then_some(clients.len()),
            backlog_messages: None,
            consume_tps: None,
        })
        .collect::<Vec<_>>();
    items.sort_by(|a, b| b.connections.cmp(&a.connections).then(a.group.cmp(&b.group)));
    quality.truncated |= items.len() > TOPOLOGY_GROUPS;
    if quality.truncated {
        quality.inventory_complete = false;
    }
    items.truncate(TOPOLOGY_GROUPS);
    Ok(TopologyRead { items, quality })
}

async fn consumers<S: TopologySource>(source: &S) -> AdminResult<TopologyRead<TopologyGroupRow>> {
    let (targets, mut quality) = masters(source).await?;
    let mut membership = BTreeMap::<String, BTreeSet<TopologyBrokerKey>>::new();
    for target in targets {
        match source.groups(&target).await {
            Ok(groups) => {
                quality.successful_reads += 1;
                for group in groups {
                    if crate::core::consumer::is_protected_consumer_group(&group) {
                        continue;
                    }
                    if !membership.contains_key(&group) && membership.len() == GROUP_CATALOG_LIMIT {
                        quality.truncated = true;
                        quality.total_complete = false;
                        continue;
                    }
                    membership.entry(group).or_default().insert(target.clone());
                }
            }
            Err(_) => {
                quality.failures += 1;
                quality.inventory_complete = false;
                quality.total_complete = false;
            }
        }
    }
    quality.total = membership.len();
    quality.truncated |= membership.len() > TOPOLOGY_GROUPS;
    let mut remaining = TOPOLOGY_CONSUMER_OBSERVATIONS;
    let mut items = Vec::new();
    for (group, brokers) in membership.into_iter().take(TOPOLOGY_GROUPS) {
        let mut clients = BTreeSet::new();
        let mut backlog = 0i64;
        let mut tps = 0.;
        let mut connections_complete = quality.total_complete;
        let mut progress_complete = quality.total_complete;
        for broker in &brokers {
            if remaining == 0 {
                quality.truncated = true;
                connections_complete = false;
                progress_complete = false;
                break;
            }
            remaining -= 1;
            match source.connections(broker, &group).await {
                Ok(ids) => clients.extend(ids),
                Err(_) => {
                    quality.failures += 1;
                    connections_complete = false;
                }
            }
            match source.progress(broker, &group).await {
                Ok(Some((lag, rate))) if rate.is_finite() && rate >= 0. => match backlog.checked_add(lag) {
                    Some(sum) => {
                        backlog = sum;
                        tps += rate;
                    }
                    None => {
                        progress_complete = false;
                        quality.failures += 1;
                    }
                },
                _ => {
                    progress_complete = false;
                    quality.failures += 1;
                }
            }
        }
        items.push(TopologyGroupRow {
            group,
            brokers: brokers.into_iter().collect(),
            connections: connections_complete.then_some(clients.len()),
            backlog_messages: progress_complete.then_some(backlog),
            consume_tps: (progress_complete && tps.is_finite()).then_some(tps),
        });
    }
    if quality.truncated {
        quality.inventory_complete = false;
    }
    Ok(TopologyRead { items, quality })
}

#[cfg(test)]
#[path = "topology_tests.rs"]
mod tests;
