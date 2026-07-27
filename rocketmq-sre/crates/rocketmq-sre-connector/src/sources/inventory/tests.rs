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

use chrono::TimeZone;
use rocketmq_sre_contracts::CoverageStatus;
use rocketmq_sre_contracts::Sensitivity;
use serde_json::Value;
use serde_json::json;

use super::InventoryAccumulator;
use super::asset;
use super::client_connections;
use super::k8s;
use super::rocketmq;
use crate::sources::common::SourceOutput;

fn at() -> chrono::DateTime<chrono::Utc> {
    chrono::Utc
        .with_ymd_and_hms(2026, 7, 27, 8, 0, 0)
        .single()
        .expect("time")
}

fn output(content: Value) -> SourceOutput {
    SourceOutput {
        observed_at: at(),
        freshness_seconds: 1,
        partial: false,
        warnings: Vec::new(),
        sensitivity: Sensitivity::Internal,
        coverage: CoverageStatus::Available,
        content,
    }
}

#[test]
fn overview_keeps_external_alias_boundary_separate_from_reported_cluster_name() {
    let mut inventory = InventoryAccumulator::new(rocketmq_sre_contracts::ClusterId::new(), "rocketmq-rust", at());
    rocketmq::fixture_add_overview(
        &mut inventory,
        "rocketmq-rust",
        output(json!({
            "cluster": "rocketmq-rust",
            "brokers": [{
                "cluster": "RocketmqRust",
                "broker_name": "broker-a",
                "broker_id": 0,
                "version": "5.3.2",
                "broker_active": true
            }],
            "topic_count": 1,
            "consumer_group_count": 1
        })),
    )
    .expect("the MCP alias scopes the response independently of RocketMQ's reported cluster name");

    let upload = inventory.finish(100, 512 * 1024).expect("inventory");
    let broker = upload
        .assets
        .iter()
        .find(|asset| asset.kind == "broker")
        .expect("broker asset");
    assert_eq!(broker.attributes["reported_clusters"], json!(["RocketmqRust"]));
}

#[test]
fn overview_treats_blank_reported_cluster_as_absent() {
    let mut inventory = InventoryAccumulator::new(rocketmq_sre_contracts::ClusterId::new(), "rocketmq-rust", at());
    rocketmq::fixture_add_overview(
        &mut inventory,
        "rocketmq-rust",
        output(json!({
            "cluster": "rocketmq-rust",
            "brokers": [{
                "cluster": "",
                "broker_name": "broker-a",
                "broker_id": 0,
                "version": "5.3.2",
                "broker_active": true
            }],
            "topic_count": 1,
            "consumer_group_count": 1
        })),
    )
    .expect("a blank optional physical cluster must not invalidate the verified MCP alias");

    let upload = inventory.finish(100, 512 * 1024).expect("inventory");
    let broker = upload
        .assets
        .iter()
        .find(|asset| asset.kind == "broker")
        .expect("broker asset");
    assert_eq!(broker.attributes["reported_clusters"], json!([]));
}

#[test]
fn fixture_forms_only_verified_topic_storage_path_and_reports_client_gap() {
    let mut inventory = InventoryAccumulator::new(rocketmq_sre_contracts::ClusterId::new(), "local", at());
    rocketmq::fixture_add_overview(
        &mut inventory,
        "local",
        output(json!({
            "cluster": "local",
            "brokers": [{
                "cluster": "local",
                "broker_name": "broker-a",
                "broker_id": 0,
                "version": "5.3.2",
                "broker_active": true,
                "space": "0.42",
                "timer_progress": "0",
                "page_cache_lock_time_millis": "0"
            }],
            "topic_count": 1,
            "consumer_group_count": 1
        })),
    )
    .expect("overview");
    rocketmq::fixture_add_topics(
        &mut inventory,
        output(json!({
            "items": [{"topic": "orders", "consumer_group": "billing"}],
            "total_count": 1,
            "has_more": false
        })),
    )
    .expect("topics");
    rocketmq::fixture_add_topic_description(
        &mut inventory,
        output(json!({
            "topic": "orders",
            "broker_names": ["broker-a"],
            "read_queue_count": 2,
            "write_queue_count": 2,
            "brokers": [{
                "broker_name": "broker-a",
                "zone_name": "zone-a",
                "enable_acting_master": false
            }],
            "items": [{
                "broker_name": "broker-a",
                "read_queue_nums": 2,
                "write_queue_nums": 2,
                "perm": 6,
                "topic_sys_flag": 0
            }],
            "total_count": 1,
            "has_more": false
        })),
        100,
    )
    .expect("topic route");
    k8s::fixture_add_pods(
        &mut inventory,
        output(json!({
            "kind": "pods",
            "namespace": "rocketmq",
            "items": [{
                "name": "broker-0",
                "namespace": "rocketmq",
                "labels": {
                    "rocketmq.apache.org/cluster": "local",
                    "rocketmq.apache.org/service": "broker",
                    "rocketmq.apache.org/broker-name": "broker-a"
                },
                "phase": "Running",
                "node_name": "worker-a.internal",
                "persistent_volume_claims": ["broker-data-0"],
                "containers": [{"ready": true, "restart_count": 0}]
            }]
        })),
        b"tenant-cluster-key",
    )
    .expect("pods");
    k8s::fixture_add_nodes(
        &mut inventory,
        output(json!({
            "kind": "nodes",
            "namespace": "rocketmq",
            "items": [{
                "name": "worker-a.internal",
                "labels": {"rocketmq.apache.org/cluster": "local"},
                "unschedulable": false,
                "pod_capacity": "110",
                "pod_allocatable": "110",
                "conditions": [{"type": "Ready", "status": "True"}]
            }]
        })),
        b"tenant-cluster-key",
    )
    .expect("nodes");
    k8s::fixture_add_pvcs(
        &mut inventory,
        output(json!({
            "kind": "persistent_volume_claims",
            "namespace": "rocketmq",
            "items": [{
                "name": "broker-data-0",
                "namespace": "rocketmq",
                "labels": {"rocketmq.apache.org/cluster": "local"},
                "phase": "Bound",
                "storage_class": "rocketmq-retain",
                "access_modes": ["ReadWriteOnce"],
                "requested_storage": "100Gi",
                "capacity_storage": "100Gi"
            }]
        })),
    )
    .expect("claims");

    let upload = inventory.finish(100, 512 * 1024).expect("inventory");
    let wire = serde_json::to_value(&upload).expect("wire");
    assert_eq!(
        wire.pointer("/assets/0/attributes/inventory_coverage/paths/topic_queue_broker_store_kubernetes/status"),
        Some(&json!("available"))
    );
    assert_eq!(
        wire.pointer("/assets/0/attributes/inventory_coverage/assets/producer/status"),
        Some(&json!("not_production_verified"))
    );
    assert_eq!(
        wire.pointer("/assets/0/attributes/inventory_coverage/assets/connection/status"),
        Some(&json!("not_production_verified"))
    );
    assert!(
        upload.partial,
        "missing producer/connection source keeps the snapshot partial"
    );

    let edge_ids = upload
        .edges
        .iter()
        .map(|edge| {
            format!(
                "{}:{}-{}->{}:{}",
                edge.from.kind, edge.from.external_key, edge.relation, edge.to.kind, edge.to.external_key
            )
        })
        .collect::<Vec<_>>();
    assert!(
        edge_ids
            .iter()
            .any(|edge| edge == "topic:orders-contains->queue:orders/broker-a/0")
    );
    assert!(
        edge_ids
            .iter()
            .any(|edge| edge == "queue:orders/broker-a/0-routes_to->broker:broker-a")
    );
    assert!(
        edge_ids
            .iter()
            .any(|edge| edge == "broker:broker-a-stores_on->store:broker/broker-a")
    );
    assert!(
        edge_ids
            .iter()
            .any(|edge| edge == "store:broker/broker-a-runs_on->pod:rocketmq/broker-0")
    );
    assert!(
        edge_ids
            .iter()
            .any(|edge| { edge == "store:broker/broker-a-stores_on->persistent_volume_claim:rocketmq/broker-data-0" })
    );

    let encoded = serde_json::to_string(&upload).expect("encoded");
    for forbidden in [
        "worker-a.internal",
        "10.0.0.",
        "secret",
        "access_key",
        "message_body",
        "payload",
    ] {
        assert!(!encoded.contains(forbidden), "{forbidden} leaked into inventory");
    }
}

#[test]
fn bounding_keeps_cluster_and_drops_edges_with_removed_endpoints() {
    let mut inventory = InventoryAccumulator::new(rocketmq_sre_contracts::ClusterId::new(), "local", at());
    for index in 0..20 {
        inventory.insert_asset(asset(
            "topic",
            &format!("topic-{index:02}"),
            &format!("topic-{index:02}"),
            "mcp",
            json!({}),
            at(),
            0,
            false,
        ));
    }
    inventory.mark_source("topic", false);
    let upload = inventory.finish(5, 64 * 1024).expect("bounded inventory");
    assert_eq!(upload.assets.len(), 5);
    assert_eq!(upload.assets[0].kind, "cluster");
    assert!(upload.partial);
    let retained = upload
        .assets
        .iter()
        .map(|asset| (asset.kind, asset.external_key.as_str()))
        .collect::<std::collections::BTreeSet<_>>();
    assert!(upload.edges.iter().all(|edge| {
        retained.contains(&(edge.from.kind, edge.from.external_key.as_str()))
            && retained.contains(&(edge.to.kind, edge.to.external_key.as_str()))
    }));
}

#[test]
fn connection_fixtures_form_real_pseudonymous_client_broker_edges() {
    let mut inventory = InventoryAccumulator::new(rocketmq_sre_contracts::ClusterId::new(), "local", at());
    rocketmq::fixture_add_overview(
        &mut inventory,
        "local",
        output(json!({
            "cluster": "local",
            "brokers": [{
                "cluster": "local",
                "broker_name": "broker-a",
                "broker_id": 0,
                "version": "5.3.2",
                "broker_active": true
            }],
            "topic_count": 1,
            "consumer_group_count": 1
        })),
    )
    .expect("overview");
    let producer_fixture = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../tests/fixtures/inventory/producer-connections.json"
    )))
    .expect("producer fixture");
    let consumer_fixture = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../tests/fixtures/inventory/consumer-connections.json"
    )))
    .expect("consumer fixture");
    let pseudonymization_key = b"tenant-cluster-stable-key";

    client_connections::fixture_add_producer_connections(
        &mut inventory,
        output(producer_fixture),
        100,
        pseudonymization_key,
    )
    .expect("producer connections");
    client_connections::fixture_add_consumer_connections(
        &mut inventory,
        output(consumer_fixture),
        100,
        pseudonymization_key,
    )
    .expect("consumer connections");

    let upload = inventory.finish(100, 512 * 1024).expect("inventory");
    let connections = upload
        .assets
        .iter()
        .filter(|asset| asset.kind == "connection")
        .collect::<Vec<_>>();
    assert_eq!(
        connections.len(),
        1,
        "the same observed client/broker connection must share one stable pseudonym"
    );
    assert!(connections[0].external_key.starts_with("sha256:"));
    assert_eq!(connections[0].attributes["identity_class"], "tenant_cluster_pseudonym");
    let edge_ids = upload
        .edges
        .iter()
        .map(|edge| {
            format!(
                "{}:{}-{}->{}:{}",
                edge.from.kind, edge.from.external_key, edge.relation, edge.to.kind, edge.to.external_key
            )
        })
        .collect::<Vec<_>>();
    let connection_key = &connections[0].external_key;
    assert!(
        edge_ids
            .iter()
            .any(|edge| { edge == &format!("producer:orders-producer-connected_to->connection:{connection_key}") })
    );
    assert!(
        edge_ids
            .iter()
            .any(|edge| { edge == &format!("consumer:orders-consumer-connected_to->connection:{connection_key}") })
    );
    assert!(
        edge_ids
            .iter()
            .any(|edge| { edge == &format!("connection:{connection_key}-connected_to->broker:broker-a") })
    );

    let wire = serde_json::to_value(&upload).expect("wire");
    assert_eq!(
        wire.pointer("/assets/0/attributes/inventory_coverage/assets/producer/status"),
        Some(&json!("available"))
    );
    assert_eq!(
        wire.pointer("/assets/0/attributes/inventory_coverage/assets/connection/status"),
        Some(&json!("available"))
    );
    assert_eq!(
        wire.pointer("/assets/0/attributes/inventory_coverage/paths/client_connection_broker/status"),
        Some(&json!("available"))
    );
    let encoded = serde_json::to_string(&upload).expect("encoded");
    for forbidden in ["orders-client@10.0.0.8", "10.0.0.8", "41200"] {
        assert!(!encoded.contains(forbidden), "{forbidden} leaked into inventory");
    }
}
