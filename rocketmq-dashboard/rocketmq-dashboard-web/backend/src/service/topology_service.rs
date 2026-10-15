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
use crate::error::DashboardError;
use crate::model::BrokerInfo;
use crate::model::ConsumerGroupListItem;
use crate::model::ConsumerQuery;
use crate::model::DashboardConfigView;
use crate::model::NameserverAvailabilityStatus;
use crate::model::NameserverAvailabilityView;
use crate::model::ProducerInfo;
use crate::model::TopicInfo;
use crate::model::TopologyBrokerGroup;
use crate::model::TopologyCluster;
use crate::model::TopologyConsumer;
use crate::model::TopologyNameServer;
use crate::model::TopologyNameServerStatus;
use crate::model::TopologyProducer;
use crate::model::TopologySource;
use crate::model::TopologySourceState;
use crate::model::TopologySourceStatus;
use crate::model::TopologyTotals;
use crate::model::TopologyView;
use crate::service::broker_service;
use crate::service::config_service;
use crate::service::consumer_service;
use crate::service::producer_service;
use crate::service::topic_service;
use crate::state::AppState;
use chrono::Utc;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::time::Instant;

/// Most producer and consumer groups in a topology view, busiest first.
pub const TOPOLOGY_GROUP_LIMIT: usize = 50;

/// Producer groups the RocketMQ client and tools register on their own.
const SYSTEM_PRODUCER_GROUPS: [&str; 3] = ["CLIENT_INNER_PRODUCER", "DEFAULT_PRODUCER", "SELF_TEST_P_GROUP"];

/// Reads every layer of the topology map at once.
///
/// The layer queries are the existing list services, run concurrently. A failed layer is left empty
/// and reported in `sources`, so one slow or failing query does not hide the rest of the map.
/// With `skipSystem`, system producer and consumer groups are left out of the groups and totals.
///
/// # Errors
///
/// Returns the first layer error when every layer query fails.
pub async fn topology(state: &AppState, query: ConsumerQuery) -> Result<TopologyView, DashboardError> {
    let skip_system = query.skip_system.unwrap_or(false);
    let (config, nameservers, brokers, topics, producers, consumers) = tokio::join!(
        timed(TopologySource::Config, config_service::get_config(state)),
        timed(
            TopologySource::Nameservers,
            config_service::get_nameserver_availability(state)
        ),
        timed(TopologySource::Brokers, async {
            broker_service::list_brokers(state).await.map(|view| view.items)
        }),
        timed(TopologySource::Topics, async {
            topic_service::list_topics(state).await.map(|view| view.items)
        }),
        timed(TopologySource::Producers, async {
            producer_service::list_producers(state)
                .await
                .map(|rows| without_system_producers(rows, skip_system))
        }),
        timed(TopologySource::Consumers, async {
            consumer_service::list_consumers(state, query)
                .await
                .map(|view| without_system_consumers(view.items, skip_system))
        }),
    );
    assemble(
        LayerResults {
            config,
            nameservers,
            brokers,
            topics,
            producers,
            consumers,
        },
        Utc::now().timestamp_millis(),
    )
}

struct Timed<T> {
    status: TopologySourceStatus,
    result: Result<T, DashboardError>,
}

async fn timed<T>(source: TopologySource, query: impl Future<Output = Result<T, DashboardError>>) -> Timed<T> {
    let started = Instant::now();
    let result = query.await;
    let duration_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    let (status, code) = match &result {
        Ok(_) => (TopologySourceState::Ok, None),
        Err(error) => {
            tracing::warn!(source = ?source, code = %error.code(), error = %error, "Topology layer query failed");
            (TopologySourceState::Failed, Some(error.code().into_owned()))
        }
    };
    Timed {
        status: TopologySourceStatus {
            source,
            status,
            duration_ms,
            code,
        },
        result,
    }
}

/// The consumer listing marks system groups with a category; `skipSystem` there only changes their
/// display name, so the topology drops them itself, as the consumer page does by default.
fn without_system_consumers(items: Vec<ConsumerGroupListItem>, skip_system: bool) -> Vec<ConsumerGroupListItem> {
    if !skip_system {
        return items;
    }
    items
        .into_iter()
        .filter(|item| !item.category.eq_ignore_ascii_case("SYSTEM"))
        .collect()
}

/// Producer rows carry only a group name, so system producers are recognized by name.
fn without_system_producers(rows: Vec<ProducerInfo>, skip_system: bool) -> Vec<ProducerInfo> {
    if !skip_system {
        return rows;
    }
    rows.into_iter()
        .filter(|row| !is_system_producer_group(&row.producer_group))
        .collect()
}

fn is_system_producer_group(group: &str) -> bool {
    group.starts_with("%SYS%") || SYSTEM_PRODUCER_GROUPS.contains(&group)
}

struct LayerResults {
    config: Timed<DashboardConfigView>,
    nameservers: Timed<NameserverAvailabilityView>,
    brokers: Timed<Vec<BrokerInfo>>,
    topics: Timed<Vec<TopicInfo>>,
    producers: Timed<Vec<ProducerInfo>>,
    consumers: Timed<Vec<ConsumerGroupListItem>>,
}

/// Responses of the layer queries; `None` marks a layer whose query failed.
#[derive(Default)]
struct TopologyLayers {
    config: Option<DashboardConfigView>,
    nameservers: Option<NameserverAvailabilityView>,
    brokers: Option<Vec<BrokerInfo>>,
    topics: Option<Vec<TopicInfo>>,
    producers: Option<Vec<ProducerInfo>>,
    consumers: Option<Vec<ConsumerGroupListItem>>,
}

fn keep<T>(
    layer: Timed<T>,
    sources: &mut Vec<TopologySourceStatus>,
    first_error: &mut Option<DashboardError>,
) -> Option<T> {
    sources.push(layer.status);
    match layer.result {
        Ok(value) => Some(value),
        Err(error) => {
            first_error.get_or_insert(error);
            None
        }
    }
}

fn assemble(results: LayerResults, generated_at: i64) -> Result<TopologyView, DashboardError> {
    let mut sources = Vec::with_capacity(6);
    let mut first_error = None;
    let layers = TopologyLayers {
        config: keep(results.config, &mut sources, &mut first_error),
        nameservers: keep(results.nameservers, &mut sources, &mut first_error),
        brokers: keep(results.brokers, &mut sources, &mut first_error),
        topics: keep(results.topics, &mut sources, &mut first_error),
        producers: keep(results.producers, &mut sources, &mut first_error),
        consumers: keep(results.consumers, &mut sources, &mut first_error),
    };
    if sources
        .iter()
        .all(|source| source.status == TopologySourceState::Failed)
        && let Some(error) = first_error
    {
        return Err(error);
    }
    Ok(build_topology(layers, sources, generated_at))
}

fn build_topology(layers: TopologyLayers, sources: Vec<TopologySourceStatus>, generated_at: i64) -> TopologyView {
    let mut producers = layers
        .producers
        .as_deref()
        .map(|rows| aggregate_producers(rows, layers.topics.as_deref()))
        .unwrap_or_default();
    let mut consumers = layers.consumers.as_deref().map(map_consumers).unwrap_or_default();
    let totals = TopologyTotals {
        producer_groups: producers.len(),
        consumer_groups: consumers.len(),
        message_backlog: consumers
            .iter()
            .fold(0_i64, |total, consumer| total.saturating_add(consumer.diff_total)),
    };
    producers.truncate(TOPOLOGY_GROUP_LIMIT);
    consumers.truncate(TOPOLOGY_GROUP_LIMIT);

    TopologyView {
        generated_at,
        sources,
        name_servers: name_servers(layers.config.as_ref(), layers.nameservers.as_ref()),
        clusters: layers.brokers.as_deref().map(group_brokers).unwrap_or_default(),
        producers,
        consumers,
        totals,
    }
}

/// Configured NameServers in configuration order, with the latest availability check of each.
fn name_servers(
    config: Option<&DashboardConfigView>,
    availability: Option<&NameserverAvailabilityView>,
) -> Vec<TopologyNameServer> {
    let checks = availability
        .map(|view| {
            view.endpoints
                .iter()
                .map(|endpoint| (endpoint.address.as_str(), endpoint))
                .collect::<HashMap<_, _>>()
        })
        .unwrap_or_default();
    let configured = config.into_iter().flat_map(|config| config.namesrv_addr_list.iter());
    let checked = availability
        .into_iter()
        .flat_map(|view| view.endpoints.iter().map(|endpoint| &endpoint.address));
    let mut addresses: Vec<&str> = Vec::new();
    for address in configured.chain(checked) {
        if !addresses.contains(&address.as_str()) {
            addresses.push(address);
        }
    }
    let current = config.and_then(|config| config.current_namesrv.as_deref());

    addresses
        .into_iter()
        .map(|address| {
            let check = checks.get(address);
            TopologyNameServer {
                address: address.to_string(),
                current: current == Some(address),
                status: check.map_or(TopologyNameServerStatus::Unknown, |check| match check.status {
                    NameserverAvailabilityStatus::Available => TopologyNameServerStatus::Available,
                    NameserverAvailabilityStatus::Unavailable => TopologyNameServerStatus::Unavailable,
                }),
                checked_at: check.map(|check| check.checked_at),
            }
        })
        .collect()
}

fn round_tps(value: f64) -> f64 {
    (value * 100.0).round() / 100.0
}

fn finite_sum(values: impl Iterator<Item = f64>) -> f64 {
    values.filter(|value| value.is_finite()).sum()
}

/// Groups broker instances by cluster and broker name; broker id 0 is the master.
fn group_brokers(brokers: &[BrokerInfo]) -> Vec<TopologyCluster> {
    let mut clusters: BTreeMap<&str, BTreeMap<&str, Vec<&BrokerInfo>>> = BTreeMap::new();
    for broker in brokers {
        clusters
            .entry(broker.cluster_name.as_str())
            .or_default()
            .entry(broker.broker_name.as_str())
            .or_default()
            .push(broker);
    }

    clusters
        .into_iter()
        .map(|(cluster_name, groups)| {
            let brokers = groups
                .into_iter()
                .map(|(broker_name, members)| broker_group(broker_name, &members))
                .collect::<Vec<_>>();
            TopologyCluster {
                cluster_name: cluster_name.to_string(),
                produce_tps: round_tps(finite_sum(brokers.iter().map(|group| group.produce_tps))),
                consume_tps: round_tps(finite_sum(brokers.iter().map(|group| group.consume_tps))),
                brokers,
            }
        })
        .collect()
}

fn broker_group(broker_name: &str, members: &[&BrokerInfo]) -> TopologyBrokerGroup {
    let master = members.iter().find(|member| member.broker_id == 0);
    let mut slave_addresses = members
        .iter()
        .filter(|member| member.broker_id != 0)
        .map(|member| member.address.clone())
        .collect::<Vec<_>>();
    slave_addresses.sort();
    TopologyBrokerGroup {
        broker_name: broker_name.to_string(),
        master_address: master.map(|master| master.address.clone()),
        slave_addresses,
        version: master
            .or_else(|| members.first())
            .map(|member| member.version.clone())
            .unwrap_or_default(),
        // Same reading as the broker list: every instance of the group reports its own traffic.
        produce_tps: round_tps(finite_sum(members.iter().map(|member| member.produce_tps))),
        consume_tps: round_tps(finite_sum(members.iter().map(|member| member.consume_tps))),
    }
}

/// Folds the producer rows into one entry per group. Rows of one group that name different topics
/// usually come from the same clients, so the largest row gives the connection count; adding the
/// rows up would count those clients once per topic. A group links to the brokers its rows report
/// and to the brokers of its topics.
fn aggregate_producers(rows: &[ProducerInfo], topics: Option<&[TopicInfo]>) -> Vec<TopologyProducer> {
    let topic_brokers = topics
        .unwrap_or_default()
        .iter()
        .map(|topic| (topic.topic.as_str(), topic.brokers.as_slice()))
        .collect::<HashMap<_, _>>();
    let mut groups: BTreeMap<&str, (usize, BTreeSet<&str>, BTreeSet<&str>)> = BTreeMap::new();
    for row in rows.iter().filter(|row| !row.producer_group.is_empty()) {
        let (connections, group_topics, broker_names) = groups.entry(row.producer_group.as_str()).or_default();
        *connections = (*connections).max(row.connection_count);
        if !row.topic.is_empty() {
            group_topics.insert(row.topic.as_str());
        }
        broker_names.extend(row.broker_names.iter().map(String::as_str));
    }

    let mut producers = groups
        .into_iter()
        .map(|(producer_group, (connection_count, group_topics, mut broker_names))| {
            broker_names.extend(
                group_topics
                    .iter()
                    .flat_map(|topic| topic_brokers.get(topic).copied().unwrap_or_default())
                    .map(String::as_str),
            );
            TopologyProducer {
                producer_group: producer_group.to_string(),
                connection_count,
                topics: group_topics.into_iter().map(str::to_string).collect(),
                broker_names: broker_names.into_iter().map(str::to_string).collect(),
            }
        })
        .collect::<Vec<_>>();
    producers.sort_by(|left, right| {
        right
            .connection_count
            .cmp(&left.connection_count)
            .then_with(|| left.producer_group.cmp(&right.producer_group))
    });
    producers
}

/// Consumer groups ranked by connections.
fn map_consumers(items: &[ConsumerGroupListItem]) -> Vec<TopologyConsumer> {
    let mut consumers = items
        .iter()
        .map(|item| TopologyConsumer {
            group: item.raw_group_name.clone(),
            display_name: if item.display_group_name.is_empty() {
                item.raw_group_name.clone()
            } else {
                item.display_group_name.clone()
            },
            connection_count: item.connection_count,
            consume_tps: item.consume_tps,
            diff_total: item.diff_total,
            broker_names: item
                .broker_names
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect(),
        })
        .collect::<Vec<_>>();
    consumers.sort_by(|left, right| {
        right
            .connection_count
            .cmp(&left.connection_count)
            .then_with(|| left.group.cmp(&right.group))
    });
    consumers
}

#[cfg(test)]
mod tests {
    use super::LayerResults;
    use super::TOPOLOGY_GROUP_LIMIT;
    use super::Timed;
    use super::aggregate_producers;
    use super::assemble;
    use super::without_system_consumers;
    use super::without_system_producers;
    use crate::error::DashboardError;
    use crate::model::BrokerInfo;
    use crate::model::ConsumerGroupListItem;
    use crate::model::DashboardConfigView;
    use crate::model::NameserverAvailabilityStatus;
    use crate::model::NameserverAvailabilityView;
    use crate::model::NameserverEndpointAvailability;
    use crate::model::ProducerInfo;
    use crate::model::TopicInfo;
    use crate::model::TopologyNameServerStatus;
    use crate::model::TopologySource;
    use crate::model::TopologySourceState;
    use crate::model::TopologySourceStatus;

    fn broker(cluster_name: &str, broker_name: &str, broker_id: u64, address: &str, tps: (f64, f64)) -> BrokerInfo {
        BrokerInfo {
            cluster_name: cluster_name.to_string(),
            broker_name: broker_name.to_string(),
            broker_id,
            address: address.to_string(),
            role: if broker_id == 0 { "MASTER" } else { "SLAVE" }.to_string(),
            version: "V5_3_1".to_string(),
            produce_tps: tps.0,
            consume_tps: tps.1,
        }
    }

    fn topic(name: &str, brokers: &[&str]) -> TopicInfo {
        TopicInfo {
            topic: name.to_string(),
            broker_name: brokers.first().map(|broker| (*broker).to_string()),
            brokers: brokers.iter().map(|broker| (*broker).to_string()).collect(),
            clusters: Vec::new(),
            read_queue_count: 8,
            write_queue_count: 8,
            perm: 6,
            category: "APPLICATION".to_string(),
            message_type: "NORMAL".to_string(),
            order: false,
            system_topic: false,
        }
    }

    fn producer(topic: &str, producer_group: &str, connection_count: usize) -> ProducerInfo {
        ProducerInfo {
            topic: topic.to_string(),
            producer_group: producer_group.to_string(),
            connection_count,
            broker_names: Vec::new(),
        }
    }

    fn consumer(group: &str, connection_count: usize, diff_total: i64, broker_names: &[&str]) -> ConsumerGroupListItem {
        ConsumerGroupListItem {
            display_group_name: group.to_string(),
            raw_group_name: group.to_string(),
            category: "NORMAL".to_string(),
            connection_count,
            consume_tps: 120,
            diff_total,
            message_model: "CLUSTERING".to_string(),
            consume_type: "CONSUME_PASSIVELY".to_string(),
            version: None,
            version_desc: String::new(),
            broker_names: broker_names.iter().map(|broker| (*broker).to_string()).collect(),
            broker_addresses: Vec::new(),
            update_timestamp: 0,
        }
    }

    fn config() -> DashboardConfigView {
        DashboardConfigView {
            namesrv_addr_list: vec!["127.0.0.1:9876".to_string(), "10.0.4.12:9876".to_string()],
            current_namesrv: Some("127.0.0.1:9876".to_string()),
            ..DashboardConfigView::default()
        }
    }

    fn availability() -> NameserverAvailabilityView {
        NameserverAvailabilityView {
            endpoints: vec![
                NameserverEndpointAvailability {
                    address: "127.0.0.1:9876".to_string(),
                    status: NameserverAvailabilityStatus::Available,
                    checked_at: 1_000,
                },
                NameserverEndpointAvailability {
                    address: "10.0.4.12:9876".to_string(),
                    status: NameserverAvailabilityStatus::Unavailable,
                    checked_at: 1_000,
                },
            ],
        }
    }

    fn ok<T>(source: TopologySource, value: T) -> Timed<T> {
        Timed {
            status: TopologySourceStatus {
                source,
                status: TopologySourceState::Ok,
                duration_ms: 12,
                code: None,
            },
            result: Ok(value),
        }
    }

    fn failed<T>(source: TopologySource) -> Timed<T> {
        let error = DashboardError::Internal("layer query failed".to_string());
        Timed {
            status: TopologySourceStatus {
                source,
                status: TopologySourceState::Failed,
                duration_ms: 30,
                code: Some(error.code().into_owned()),
            },
            result: Err(error),
        }
    }

    fn results() -> LayerResults {
        LayerResults {
            config: ok(TopologySource::Config, config()),
            nameservers: ok(TopologySource::Nameservers, availability()),
            brokers: ok(
                TopologySource::Brokers,
                vec![
                    broker("DefaultCluster", "broker-a", 0, "10.0.4.21:10911", (1204.36, 1198.02)),
                    broker("DefaultCluster", "broker-a", 1, "10.0.4.121:10911", (0.0, 3.5)),
                    broker("cluster-east", "broker-c", 0, "10.0.5.21:10911", (420.3, 412.8)),
                ],
            ),
            topics: ok(
                TopologySource::Topics,
                vec![
                    topic("order-events", &["broker-a"]),
                    topic("notification-push", &["broker-c"]),
                ],
            ),
            producers: ok(
                TopologySource::Producers,
                vec![
                    producer("order-events", "order-service-producer", 6),
                    producer("notification-push", "order-service-producer", 4),
                    producer("notification-push", "notification-producer", 2),
                ],
            ),
            consumers: ok(
                TopologySource::Consumers,
                vec![
                    consumer("audit-archiver", 0, 0, &["broker-c"]),
                    consumer("order-consumer", 12, 8_420, &["broker-a", "broker-a"]),
                ],
            ),
        }
    }

    #[test]
    fn assembles_every_layer_into_one_view() {
        let view = assemble(results(), 42).expect("topology view");

        assert_eq!(view.generated_at, 42);
        assert!(
            view.sources
                .iter()
                .all(|source| source.status == TopologySourceState::Ok)
        );
        assert_eq!(
            view.sources.iter().map(|source| source.source).collect::<Vec<_>>(),
            vec![
                TopologySource::Config,
                TopologySource::Nameservers,
                TopologySource::Brokers,
                TopologySource::Topics,
                TopologySource::Producers,
                TopologySource::Consumers,
            ]
        );

        let names = view
            .name_servers
            .iter()
            .map(|name_server| (name_server.address.as_str(), name_server.current, name_server.status))
            .collect::<Vec<_>>();
        assert_eq!(
            names,
            vec![
                ("127.0.0.1:9876", true, TopologyNameServerStatus::Available),
                ("10.0.4.12:9876", false, TopologyNameServerStatus::Unavailable),
            ]
        );

        let default_cluster = &view.clusters[0];
        assert_eq!(default_cluster.cluster_name, "DefaultCluster");
        assert_eq!(default_cluster.produce_tps, 1204.36);
        assert_eq!(default_cluster.consume_tps, 1201.52);
        let broker_a = &default_cluster.brokers[0];
        assert_eq!(broker_a.master_address.as_deref(), Some("10.0.4.21:10911"));
        assert_eq!(broker_a.slave_addresses, vec!["10.0.4.121:10911".to_string()]);

        let order_service = &view.producers[0];
        assert_eq!(order_service.producer_group, "order-service-producer");
        assert_eq!(order_service.connection_count, 6);
        assert_eq!(order_service.topics, vec!["notification-push", "order-events"]);
        assert_eq!(order_service.broker_names, vec!["broker-a", "broker-c"]);

        assert_eq!(view.consumers[0].group, "order-consumer");
        assert_eq!(view.consumers[0].broker_names, vec!["broker-a"]);
        assert_eq!(view.totals.producer_groups, 2);
        assert_eq!(view.totals.consumer_groups, 2);
        assert_eq!(view.totals.message_backlog, 8_420);
    }

    #[test]
    fn keeps_the_other_layers_when_one_query_fails() {
        let mut results = results();
        results.brokers = failed(TopologySource::Brokers);

        let view = assemble(results, 42).expect("partial topology view");

        assert!(view.clusters.is_empty());
        assert_eq!(view.producers.len(), 2);
        assert_eq!(view.name_servers.len(), 2);
        let brokers = &view.sources[2];
        assert_eq!(brokers.status, TopologySourceState::Failed);
        assert_eq!(brokers.code.as_deref(), Some("INTERNAL_ERROR"));
    }

    #[test]
    fn marks_availability_unknown_when_the_check_fails() {
        let mut results = results();
        results.nameservers = failed(TopologySource::Nameservers);

        let view = assemble(results, 42).expect("topology view");

        assert!(
            view.name_servers
                .iter()
                .all(|name_server| name_server.status == TopologyNameServerStatus::Unknown
                    && name_server.checked_at.is_none())
        );
    }

    #[test]
    fn returns_an_error_only_when_every_layer_fails() {
        let results = LayerResults {
            config: failed(TopologySource::Config),
            nameservers: failed(TopologySource::Nameservers),
            brokers: failed(TopologySource::Brokers),
            topics: failed(TopologySource::Topics),
            producers: failed(TopologySource::Producers),
            consumers: failed(TopologySource::Consumers),
        };

        let error = assemble(results, 42).expect_err("every layer failed");

        assert_eq!(error.code(), "INTERNAL_ERROR");
    }

    #[test]
    fn reports_an_empty_cluster_without_failing() {
        let mut results = results();
        results.brokers = ok(TopologySource::Brokers, Vec::new());

        let view = assemble(results, 42).expect("topology view");

        assert!(view.clusters.is_empty());
        assert!(
            view.sources
                .iter()
                .all(|source| source.status == TopologySourceState::Ok)
        );
    }

    #[test]
    fn leaves_a_broker_group_without_slave_with_an_empty_slave_list() {
        let view = assemble(results(), 42).expect("topology view");

        let broker_c = &view.clusters[1].brokers[0];
        assert_eq!(broker_c.broker_name, "broker-c");
        assert_eq!(broker_c.master_address.as_deref(), Some("10.0.5.21:10911"));
        assert!(broker_c.slave_addresses.is_empty());
    }

    #[test]
    fn lists_the_busiest_groups_and_counts_all_of_them() {
        let mut results = results();
        results.consumers = ok(
            TopologySource::Consumers,
            (0..TOPOLOGY_GROUP_LIMIT + 10)
                .map(|index| consumer(&format!("group-{index:03}"), index, 1, &["broker-a"]))
                .collect(),
        );

        let view = assemble(results, 42).expect("topology view");

        assert_eq!(view.consumers.len(), TOPOLOGY_GROUP_LIMIT);
        assert_eq!(view.consumers[0].group, "group-059");
        assert_eq!(view.totals.consumer_groups, TOPOLOGY_GROUP_LIMIT + 10);
        assert_eq!(view.totals.message_backlog, 60);
    }

    #[test]
    fn producer_groups_link_to_their_registered_brokers_and_their_topics_brokers() {
        let rows = vec![
            ProducerInfo {
                broker_names: vec!["broker-b".to_string()],
                ..producer("", "order-service-producer", 2)
            },
            producer("order-events", "order-service-producer", 2),
        ];
        let topics = vec![topic("order-events", &["broker-a"])];

        let with_topics = aggregate_producers(&rows, Some(&topics));
        // Without the topic list, the brokers the rows report still link the group.
        let without_topics = aggregate_producers(&rows, None);

        assert_eq!(with_topics.len(), 1);
        assert_eq!(with_topics[0].connection_count, 2);
        assert_eq!(with_topics[0].broker_names, vec!["broker-a", "broker-b"]);
        assert_eq!(without_topics[0].broker_names, vec!["broker-b"]);
    }

    #[test]
    fn serializes_the_field_names_the_frontend_reads() {
        let mut results = results();
        results.topics = failed(TopologySource::Topics);
        let view = assemble(results, 42).expect("topology view");

        let json = serde_json::to_value(&view).expect("topology JSON");

        assert_eq!(json["generatedAt"], 42);
        assert_eq!(json["nameServers"][0]["checkedAt"], 1_000);
        assert_eq!(json["nameServers"][1]["status"], "unavailable");
        assert_eq!(json["sources"][1]["source"], "nameservers");
        assert_eq!(json["sources"][1]["durationMs"], 12);
        assert!(json["sources"][1].get("code").is_none());
        assert_eq!(json["sources"][3]["status"], "failed");
        assert_eq!(json["sources"][3]["code"], "INTERNAL_ERROR");
        assert_eq!(
            json["clusters"][0]["brokers"][0]["slaveAddresses"][0],
            "10.0.4.121:10911"
        );
        assert_eq!(json["producers"][0]["brokerNames"], serde_json::json!([]));
        assert_eq!(json["consumers"][0]["displayName"], "order-consumer");
        assert_eq!(json["totals"]["messageBacklog"], 8_420);
    }

    #[test]
    fn skipping_system_groups_drops_system_consumers_and_internal_producers() {
        let mut tools = consumer("TOOLS_CONSUMER", 0, 0, &["broker-a"]);
        tools.category = "SYSTEM".to_string();
        let consumers = vec![tools, consumer("order-consumer", 2, 9, &["broker-a"])];
        let producers = vec![
            producer("", "CLIENT_INNER_PRODUCER", 14),
            producer("", "%SYS%SELF_TEST_P_GROUP", 1),
            producer("", "order-service-producer", 4),
        ];

        let kept_consumers = without_system_consumers(consumers.clone(), true);
        let kept_producers = without_system_producers(producers.clone(), true);

        assert_eq!(
            kept_consumers
                .iter()
                .map(|item| item.raw_group_name.as_str())
                .collect::<Vec<_>>(),
            vec!["order-consumer"]
        );
        assert_eq!(
            kept_producers
                .iter()
                .map(|row| row.producer_group.as_str())
                .collect::<Vec<_>>(),
            vec!["order-service-producer"]
        );
        assert_eq!(without_system_consumers(consumers, false).len(), 2);
        assert_eq!(without_system_producers(producers, false).len(), 3);
    }
}
