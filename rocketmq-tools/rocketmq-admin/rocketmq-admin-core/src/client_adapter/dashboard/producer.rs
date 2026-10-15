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
use rocketmq_protocol::protocol::body::producer_table_info::ProducerTableInfo;

/// Folds the producer tables of individual brokers, keyed by broker name, into one entry per group.
///
/// A client registers with every broker it sends heartbeats to, so it is counted once by client ID,
/// and the group lists the brokers its clients are registered with. Entries are sorted by group.
pub(super) fn aggregate_producer_tables(
    tables: &[(String, ProducerTableInfo)],
) -> Vec<dashboard::DashboardProducerInfo> {
    let mut groups: BTreeMap<&str, (BTreeSet<&str>, BTreeSet<&str>)> = BTreeMap::new();
    for (broker_name, table) in tables {
        for (group, producers) in table.data() {
            let (clients, brokers) = groups.entry(group.as_str()).or_default();
            if !producers.is_empty() {
                brokers.insert(broker_name.as_str());
            }
            clients.extend(producers.iter().map(|producer| producer.client_id()));
        }
    }
    groups
        .into_iter()
        .map(
            |(producer_group, (clients, brokers))| dashboard::DashboardProducerInfo {
                topic: String::new(),
                producer_group: producer_group.to_string(),
                connection_count: clients.len(),
                broker_names: brokers.into_iter().map(str::to_string).collect(),
            },
        )
        .collect()
}

pub(super) fn map_producer_connection(
    topic: &str,
    producer_group: &str,
    connection: &ProducerConnection,
) -> dashboard::DashboardProducerConnections {
    let mut connections = connection
        .connection_set()
        .iter()
        .map(|item| dashboard::DashboardProducerConnection {
            client_id: item.get_client_id().to_string(),
            client_addr: item.get_client_addr().to_string(),
            language: item.get_language().to_string(),
            version: item.get_version(),
            version_desc: value2version(item.get_version().max(0) as u32).name().to_string(),
        })
        .collect::<Vec<_>>();
    connections.sort_by(|left, right| left.client_id.cmp(&right.client_id));
    dashboard::DashboardProducerConnections {
        topic: topic.to_string(),
        producer_group: producer_group.to_string(),
        connections,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocketmq_protocol::protocol::body::producer_info::ProducerInfo;
    use rocketmq_protocol::protocol::LanguageCode;

    fn table(groups: &[(&str, &[&str])]) -> ProducerTableInfo {
        ProducerTableInfo::new(
            groups
                .iter()
                .map(|(group, clients)| {
                    let producers = clients
                        .iter()
                        .map(|client| ProducerInfo::new(*client, "10.0.1.1", LanguageCode::JAVA, 0, 0))
                        .collect();
                    (group.to_string(), producers)
                })
                .collect(),
        )
    }

    #[test]
    fn producer_groups_count_each_client_once_and_list_their_brokers() {
        let tables = vec![
            (
                "broker-a".to_string(),
                table(&[("orders", &["client-1", "client-2"]), ("idle", &[])]),
            ),
            // A client that also consumes sends heartbeats to slaves, which report it again.
            ("broker-a".to_string(), table(&[("orders", &["client-1"])])),
            (
                "broker-b".to_string(),
                table(&[("orders", &["client-1", "client-2"]), ("audit", &["client-3"])]),
            ),
        ];

        let producers = aggregate_producer_tables(&tables);

        let summary = producers
            .iter()
            .map(|item| {
                (
                    item.producer_group.as_str(),
                    item.connection_count,
                    item.broker_names.clone(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            summary,
            vec![
                ("audit", 1, vec!["broker-b".to_string()]),
                ("idle", 0, Vec::new()),
                ("orders", 2, vec!["broker-a".to_string(), "broker-b".to_string()]),
            ]
        );
        assert!(producers.iter().all(|item| item.topic.is_empty()));
    }
}
