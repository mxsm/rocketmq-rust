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

use chrono::DateTime;
use chrono::Utc;
use serde::Deserialize;
use serde_json::Map;
use serde_json::Value;
use serde_json::json;

use super::InventoryAccumulator;
use super::asset;
use super::edge;
use super::key;
use super::recoverable_gap;
use super::rocketmq::ensure_route_broker;
use super::schema_mismatch;
use super::validate_inventory_name;
use crate::ConnectorError;
use crate::ConnectorErrorCode;
use crate::mcp::McpGateway;
use crate::read_gateway::ConnectorReadGateway;
use crate::read_gateway::ReadSession;
use crate::sources::common::SourceOutput;
use crate::sources::common::pseudonymize_identifier;

const CLIENT_CONNECTION_QUERY_LIMIT: usize = 200;
const CONSUMER_CONNECTION_GROUP_LIMIT: usize = 32;

#[derive(Deserialize)]
struct ProducerConnectionsWire {
    connections: Vec<ProducerConnectionWire>,
    queried_broker_count: usize,
    failed_brokers: Vec<String>,
    truncated: bool,
}

#[derive(Deserialize)]
struct ProducerConnectionWire {
    producer_group: String,
    connection: ClientConnectionWire,
}

#[derive(Deserialize)]
struct ConsumerConnectionsWire {
    consumer_group: String,
    connections: Vec<ClientConnectionWire>,
    queried_broker_count: usize,
    failed_brokers: Vec<String>,
    truncated: bool,
}

#[derive(Deserialize)]
struct ClientConnectionWire {
    broker_name: String,
    client_id: String,
    client_addr: String,
    language: String,
    version: i32,
}

#[allow(
    clippy::too_many_arguments,
    reason = "connection collection keeps scope, identity, and resource bounds explicit"
)]
pub(super) async fn collect<G>(
    inventory: &mut InventoryAccumulator,
    read_gateway: &ConnectorReadGateway<G>,
    session: &ReadSession<'_, '_>,
    consumer_groups: &[String],
    max_rows: usize,
    pseudonymization_key: &[u8],
) -> Result<(), ConnectorError>
where
    G: McpGateway,
{
    if !read_gateway.admin_configured() {
        inventory.mark_gap("producer", "admin_read_connection_source_not_configured");
        inventory.mark_gap("connection", "admin_read_connection_source_not_configured");
        inventory.mark_partial("client_connection_query_not_configured");
        return Ok(());
    }

    let row_limit = max_rows.clamp(1, CLIENT_CONNECTION_QUERY_LIMIT);
    match read_gateway.admin_producer_connections(session, row_limit).await {
        Ok(output) => {
            add_producer_connections(inventory, output, row_limit, pseudonymization_key)?;
        }
        Err(error) if recoverable_gap(&error) => {
            inventory.mark_gap("producer", "producer_connection_query_unavailable");
            inventory.mark_gap("connection", "producer_connection_query_unavailable");
            inventory.mark_partial("producer_connection_query_incomplete");
        }
        Err(error) => return Err(error),
    }

    let mut remaining_rows = row_limit;
    for consumer_group in consumer_groups.iter().take(CONSUMER_CONNECTION_GROUP_LIMIT) {
        if remaining_rows == 0 {
            inventory.mark_source("connection", true);
            inventory.mark_partial("client_connection_row_budget_applied");
            break;
        }
        match read_gateway
            .admin_consumer_connections(session, consumer_group, remaining_rows)
            .await
        {
            Ok(output) => {
                let observed = add_consumer_connections(inventory, output, remaining_rows, pseudonymization_key)?;
                remaining_rows = remaining_rows.saturating_sub(observed);
            }
            Err(error) if recoverable_gap(&error) => {
                inventory.mark_gap("connection", "consumer_connection_query_unavailable");
                inventory.mark_source("connection", true);
                inventory.mark_partial("consumer_connection_query_incomplete");
                if error.code == ConnectorErrorCode::DeadlineExceeded {
                    break;
                }
            }
            Err(error) => return Err(error),
        }
    }
    if consumer_groups.len() > CONSUMER_CONNECTION_GROUP_LIMIT {
        inventory.mark_source("connection", true);
        inventory.mark_partial("consumer_connection_group_budget_applied");
    }
    Ok(())
}

fn add_producer_connections(
    inventory: &mut InventoryAccumulator,
    output: SourceOutput,
    max_rows: usize,
    pseudonymization_key: &[u8],
) -> Result<usize, ConnectorError> {
    let mut observed: ProducerConnectionsWire =
        serde_json::from_value(output.content).map_err(|_| schema_mismatch())?;
    validate_query_summary(
        observed.queried_broker_count,
        &observed.failed_brokers,
        observed.connections.len(),
    )?;
    observed.connections.sort_by(|left, right| {
        left.producer_group
            .cmp(&right.producer_group)
            .then(connection_order(&left.connection).cmp(&connection_order(&right.connection)))
    });
    let bounded = observed.connections.len() > max_rows;
    observed.connections.truncate(max_rows);
    let observed_count = observed.connections.len();
    let partial = output.partial || observed.truncated || bounded || !observed.failed_brokers.is_empty();
    let mut group_counts = BTreeMap::<String, usize>::new();
    for row in observed.connections {
        validate_inventory_name(&row.producer_group)?;
        let producer_key = ensure_client_group(
            inventory,
            "producer",
            &row.producer_group,
            "producer_group",
            output.observed_at,
            output.freshness_seconds,
            partial,
        );
        let (connection_key, broker_key) = add_connection(
            inventory,
            row.connection,
            pseudonymization_key,
            output.observed_at,
            output.freshness_seconds,
            partial,
        )?;
        inventory.insert_edge(edge(
            producer_key,
            connection_key.clone(),
            "connected_to",
            "admin-read",
            output.observed_at,
            output.freshness_seconds,
            partial,
        ));
        inventory.insert_edge(edge(
            connection_key,
            broker_key,
            "connected_to",
            "admin-read",
            output.observed_at,
            output.freshness_seconds,
            partial,
        ));
        *group_counts.entry(row.producer_group).or_default() += 1;
    }
    for (group, count) in group_counts {
        inventory.merge_asset_attributes(&key("producer", &group), json!({"observed_connection_count": count}));
    }
    inventory.mark_source("producer", partial);
    inventory.mark_source("connection", partial);
    if bounded {
        inventory.mark_partial("producer_connection_row_budget_applied");
    }
    Ok(observed_count)
}

fn add_consumer_connections(
    inventory: &mut InventoryAccumulator,
    output: SourceOutput,
    max_rows: usize,
    pseudonymization_key: &[u8],
) -> Result<usize, ConnectorError> {
    let mut observed: ConsumerConnectionsWire =
        serde_json::from_value(output.content).map_err(|_| schema_mismatch())?;
    validate_inventory_name(&observed.consumer_group)?;
    validate_query_summary(
        observed.queried_broker_count,
        &observed.failed_brokers,
        observed.connections.len(),
    )?;
    observed
        .connections
        .sort_by(|left, right| connection_order(left).cmp(&connection_order(right)));
    let bounded = observed.connections.len() > max_rows;
    observed.connections.truncate(max_rows);
    let observed_count = observed.connections.len();
    let partial = output.partial || observed.truncated || bounded || !observed.failed_brokers.is_empty();
    let consumer_key = ensure_client_group(
        inventory,
        "consumer",
        &observed.consumer_group,
        "consumer_group",
        output.observed_at,
        output.freshness_seconds,
        partial,
    );
    for connection in observed.connections {
        let (connection_key, broker_key) = add_connection(
            inventory,
            connection,
            pseudonymization_key,
            output.observed_at,
            output.freshness_seconds,
            partial,
        )?;
        inventory.insert_edge(edge(
            consumer_key.clone(),
            connection_key.clone(),
            "connected_to",
            "admin-read",
            output.observed_at,
            output.freshness_seconds,
            partial,
        ));
        inventory.insert_edge(edge(
            connection_key,
            broker_key,
            "connected_to",
            "admin-read",
            output.observed_at,
            output.freshness_seconds,
            partial,
        ));
    }
    inventory.merge_asset_attributes(&consumer_key, json!({"observed_connection_count": observed_count}));
    inventory.mark_source("connection", partial);
    if bounded {
        inventory.mark_partial("consumer_connection_row_budget_applied");
    }
    Ok(observed_count)
}

#[allow(
    clippy::too_many_arguments,
    reason = "connection observations retain explicit source and freshness provenance"
)]
fn add_connection(
    inventory: &mut InventoryAccumulator,
    connection: ClientConnectionWire,
    pseudonymization_key: &[u8],
    observed_at: DateTime<Utc>,
    freshness_seconds: u64,
    partial: bool,
) -> Result<(super::AssetKey, super::AssetKey), ConnectorError> {
    for value in [
        connection.broker_name.as_str(),
        connection.client_id.as_str(),
        connection.client_addr.as_str(),
        connection.language.as_str(),
    ] {
        validate_inventory_name(value)?;
    }
    let identity_material = format!(
        "rocketmq-client-connection.v1\0{}\0{}\0{}",
        connection.broker_name, connection.client_id, connection.client_addr
    );
    let identity = pseudonymize_identifier(&identity_material, pseudonymization_key);
    let display_suffix = identity
        .strip_prefix("sha256:")
        .and_then(|digest| digest.get(..12))
        .ok_or_else(schema_mismatch)?;
    let connection_key = inventory.insert_asset(asset(
        "connection",
        &identity,
        &format!("connection-{display_suffix}"),
        "admin-read",
        json!({
            "identity": identity.clone(),
            "language": connection.language,
            "version": connection.version,
            "identity_class": "tenant_cluster_pseudonym",
        }),
        observed_at,
        freshness_seconds,
        partial,
    ));
    let broker_key = ensure_route_broker(
        inventory,
        &connection.broker_name,
        "admin-read",
        observed_at,
        freshness_seconds,
        partial,
        json!({"observed_via": "client_connection_query"}),
    );
    Ok((connection_key, broker_key))
}

#[allow(
    clippy::too_many_arguments,
    reason = "client group observations retain explicit source and freshness provenance"
)]
fn ensure_client_group(
    inventory: &mut InventoryAccumulator,
    kind: &'static str,
    group: &str,
    attribute_name: &'static str,
    observed_at: DateTime<Utc>,
    freshness_seconds: u64,
    partial: bool,
) -> super::AssetKey {
    let group_key = key(kind, group);
    if !inventory.contains_asset(&group_key) {
        let mut attributes = Map::new();
        attributes.insert(attribute_name.to_owned(), Value::String(group.to_owned()));
        attributes.insert(
            "observed_via".to_owned(),
            Value::String("client_connection_query".to_owned()),
        );
        inventory.insert_asset(asset(
            kind,
            group,
            group,
            "admin-read",
            Value::Object(attributes),
            observed_at,
            freshness_seconds,
            partial,
        ));
        inventory.insert_edge(edge(
            inventory.cluster_key().clone(),
            group_key.clone(),
            "contains",
            "admin-read",
            observed_at,
            freshness_seconds,
            partial,
        ));
    }
    inventory.mark_source(kind, partial);
    group_key
}

fn validate_query_summary(
    queried_broker_count: usize,
    failed_brokers: &[String],
    connection_count: usize,
) -> Result<(), ConnectorError> {
    if failed_brokers.len() > queried_broker_count
        || (queried_broker_count == 0 && connection_count > 0)
        || failed_brokers
            .iter()
            .any(|broker| validate_inventory_name(broker).is_err())
    {
        return Err(schema_mismatch());
    }
    Ok(())
}

fn connection_order(connection: &ClientConnectionWire) -> (&str, &str, &str, &str, i32) {
    (
        connection.broker_name.as_str(),
        connection.client_id.as_str(),
        connection.client_addr.as_str(),
        connection.language.as_str(),
        connection.version,
    )
}

#[cfg(test)]
pub(super) fn fixture_add_producer_connections(
    inventory: &mut InventoryAccumulator,
    output: SourceOutput,
    max_rows: usize,
    pseudonymization_key: &[u8],
) -> Result<usize, ConnectorError> {
    add_producer_connections(inventory, output, max_rows, pseudonymization_key)
}

#[cfg(test)]
pub(super) fn fixture_add_consumer_connections(
    inventory: &mut InventoryAccumulator,
    output: SourceOutput,
    max_rows: usize,
    pseudonymization_key: &[u8],
) -> Result<usize, ConnectorError> {
    add_consumer_connections(inventory, output, max_rows, pseudonymization_key)
}
