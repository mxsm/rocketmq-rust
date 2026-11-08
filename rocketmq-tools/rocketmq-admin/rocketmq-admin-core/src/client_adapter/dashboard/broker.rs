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

pub(super) async fn broker_addresses(
    admin: &rocketmq_client_rust::DefaultMQAdminExt,
) -> Result<Vec<String>, AdminError> {
    let cluster_info = admin
        .examine_broker_cluster_info()
        .await
        .map_err(|source| promote_to_admin_error("examine_broker_cluster_info", source))?;
    Ok(broker_addresses_from_cluster(&cluster_info))
}

pub(super) fn broker_addresses_from_cluster(cluster_info: &ClusterInfo) -> Vec<String> {
    let mut addresses = BTreeSet::new();
    if let Some(table) = cluster_info.broker_addr_table.as_ref() {
        for broker_data in table.values() {
            addresses.extend(broker_data.broker_addrs().values().map(ToString::to_string));
        }
    }
    addresses.into_iter().collect()
}

/// Every broker address in the cluster, masters and slaves, with the name of its broker group.
pub(super) fn broker_names_by_address(cluster_info: &ClusterInfo) -> BTreeMap<String, String> {
    let mut names = BTreeMap::new();
    if let Some(table) = cluster_info.broker_addr_table.as_ref() {
        for broker_data in table.values() {
            for address in broker_data.broker_addrs().values() {
                names.insert(address.to_string(), broker_data.broker_name().to_string());
            }
        }
    }
    names
}

pub(super) async fn resolve_broker_address(
    admin: &rocketmq_client_rust::DefaultMQAdminExt,
    broker_name: &str,
) -> Result<String, AdminError> {
    if broker_name.contains(':') {
        return Ok(broker_name.to_string());
    }
    let cluster_info = admin
        .examine_broker_cluster_info()
        .await
        .map_err(|source| promote_to_admin_error("examine_broker_cluster_info", source))?;
    cluster_info
        .broker_addr_table
        .as_ref()
        .and_then(|table| table.get(broker_name))
        .and_then(|broker| {
            broker
                .broker_addrs()
                .get(&MASTER_ID)
                .or_else(|| broker.broker_addrs().values().next())
        })
        .map(|address| address.to_string())
        .ok_or_else(|| AdminError::broker_not_found(broker_name))
}

pub(super) fn kv_table_to_map(table: &KVTable) -> BTreeMap<String, String> {
    table
        .table
        .iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect()
}

pub(super) fn parse_rate_value(value: Option<&String>) -> f64 {
    value
        .and_then(|value| value.split_whitespace().next())
        .and_then(|value| value.parse().ok())
        .unwrap_or_default()
}

pub(super) fn select_consume_tps_value(entries: &BTreeMap<String, String>) -> Option<&String> {
    match entries.get("getTransferedTps") {
        Some(value) if !value.trim().is_empty() => Some(value),
        _ => entries.get("getTransferredTps"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocketmq_protocol::protocol::route::route_data_view::BrokerData;

    #[test]
    fn broker_names_cover_master_and_slave_addresses() {
        let broker = |name: &str, addresses: &[(u64, &str)]| {
            BrokerData::new(
                CheetahString::from("cluster-a"),
                CheetahString::from(name),
                addresses
                    .iter()
                    .map(|(id, address)| (*id, CheetahString::from(*address)))
                    .collect(),
                None,
            )
        };
        let table = HashMap::from([
            (
                CheetahString::from("broker-a"),
                broker("broker-a", &[(MASTER_ID, "10.0.0.1:10911"), (1, "10.0.0.2:10911")]),
            ),
            (
                CheetahString::from("broker-b"),
                broker("broker-b", &[(MASTER_ID, "10.0.0.3:10911")]),
            ),
        ]);

        let names = broker_names_by_address(&ClusterInfo::new(Some(table), None));

        assert_eq!(
            names,
            BTreeMap::from([
                ("10.0.0.1:10911".to_string(), "broker-a".to_string()),
                ("10.0.0.2:10911".to_string(), "broker-a".to_string()),
                ("10.0.0.3:10911".to_string(), "broker-b".to_string()),
            ])
        );
    }

    #[test]
    fn corrected_consume_tps_key_is_used_when_legacy_value_is_blank() {
        let entries = BTreeMap::from([
            ("getTransferedTps".to_string(), "   ".to_string()),
            ("getTransferredTps".to_string(), "8.25 1min".to_string()),
        ]);

        assert_eq!(
            select_consume_tps_value(&entries).map(String::as_str),
            Some("8.25 1min")
        );
    }
}
