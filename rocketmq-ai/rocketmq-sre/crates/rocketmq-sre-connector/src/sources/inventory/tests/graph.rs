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

use serde_json::json;

use super::super::InventoryAccumulator;
use super::super::asset;
use super::super::edge;
use super::super::key;
use super::at;

fn graph(queues: usize) -> InventoryAccumulator {
    let mut inventory = InventoryAccumulator::new(rocketmq_sre_contracts::ClusterId::new(), "local", at());
    for (kind, name) in [
        ("topic", "orders"),
        ("broker", "a"),
        ("broker", "b"),
        ("store", "a0"),
        ("store", "a1"),
        ("store", "b0"),
        ("pod", "a0"),
        ("pod", "a1"),
        ("persistent_volume_claim", "b0"),
    ] {
        inventory.insert_asset(asset(kind, name, name, "mcp", json!({}), at(), 0, false));
        inventory.mark_source(kind, false);
    }
    for (broker, store, tail_kind) in [
        ("a", "a0", "pod"),
        ("a", "a1", "pod"),
        ("b", "b0", "persistent_volume_claim"),
    ] {
        inventory.insert_edge(edge(
            key("broker", broker),
            key("store", store),
            "stores_on",
            "mcp",
            at(),
            0,
            false,
        ));
        inventory.insert_edge(edge(
            key("store", store),
            key(tail_kind, store),
            if tail_kind == "pod" { "runs_on" } else { "stores_on" },
            "mcp",
            at(),
            0,
            false,
        ));
    }
    for index in 0..queues {
        let queue = format!("orders/{index:04}");
        inventory.insert_asset(asset("queue", &queue, &queue, "mcp", json!({}), at(), 0, false));
        inventory.insert_edge(edge(
            key("topic", "orders"),
            key("queue", &queue),
            "contains",
            "mcp",
            at(),
            0,
            false,
        ));
        inventory.insert_edge(edge(
            key("queue", &queue),
            key("broker", if index % 2 == 0 { "a" } else { "b" }),
            "routes_to",
            "mcp",
            at(),
            0,
            false,
        ));
    }
    inventory.mark_source("queue", false);
    inventory
}

fn path(inventory: &InventoryAccumulator) -> String {
    let assets = inventory.assets.values().cloned().collect::<Vec<_>>();
    let edges = inventory.edges.values().cloned().collect::<Vec<_>>();
    let (coverage, _) = inventory.coverage.render(&assets, &edges, false, Default::default());
    coverage["paths"]["topic_queue_broker_store_kubernetes"]["status"]
        .as_str()
        .unwrap()
        .to_owned()
}

#[test]
fn large_graph_requires_every_routed_brokers_own_store_tail() {
    let mut inventory = graph(4990);
    assert_eq!(inventory.assets.len(), 5000);
    assert_eq!(path(&inventory), "available");

    // Other Stores' Pod/PVC tails cannot make this Broker's missing tail whole.
    inventory
        .edges
        .remove(&(key("store", "a1"), key("pod", "a1"), "runs_on"));
    assert_eq!(path(&inventory), "partial");
    inventory.insert_edge(edge(
        key("pod", "a1"),
        key("store", "a1"),
        "runs_on",
        "mcp",
        at(),
        0,
        false,
    ));
    assert_eq!(path(&inventory), "partial", "the reverse direction is not proof");
    inventory.insert_edge(edge(
        key("store", "a1"),
        key("pod", "missing"),
        "runs_on",
        "mcp",
        at(),
        0,
        false,
    ));
    assert_eq!(path(&inventory), "partial", "an unobserved endpoint is not proof");
}

#[test]
fn an_extra_unresolved_route_or_store_does_not_certify_a_complete_subgraph() {
    for (from, to, relation) in [
        (key("queue", "orders/0000"), key("broker", "missing"), "routes_to"),
        (key("broker", "a"), key("store", "missing"), "stores_on"),
    ] {
        let mut inventory = graph(2);
        add_client(&mut inventory);
        inventory.insert_edge(edge(from, to, relation, "mcp", at(), 0, false));
        assert_eq!(path(&inventory), "partial");
        let upload = inventory.finish(100, 512 * 1024).unwrap();
        assert!(upload.partial);
        assert_eq!(
            upload.assets[0].attributes["inventory_coverage"]["paths"]["topic_queue_broker_store_kubernetes"]["status"],
            "partial"
        );
        assert_eq!(
            upload.assets[0].attributes["inventory_coverage"]["paths"]["client_connection_broker"]["status"],
            "available"
        );
        assert!(upload.edges.iter().all(|edge| edge.to.external_key != "missing"));
    }
}

fn add_client(inventory: &mut InventoryAccumulator) {
    for (kind, name) in [("producer", "client"), ("connection", "connection")] {
        inventory.insert_asset(asset(kind, name, name, "mcp", json!({}), at(), 0, false));
        inventory.mark_source(kind, false);
    }
    inventory.insert_edge(edge(
        key("producer", "client"),
        key("connection", "connection"),
        "connected_to",
        "mcp",
        at(),
        0,
        false,
    ));
    inventory.insert_edge(edge(
        key("connection", "connection"),
        key("broker", "a"),
        "connected_to",
        "mcp",
        at(),
        0,
        false,
    ));
}

#[test]
fn trimmed_participants_keep_their_path_gap_without_damaging_an_unrelated_path() {
    let mut inventory = graph(2);
    add_client(&mut inventory);
    let upload = inventory.finish(13, 512 * 1024).unwrap();
    assert!(
        upload
            .assets
            .iter()
            .all(|asset| asset.kind != "persistent_volume_claim")
    );
    let paths = &upload.assets[0].attributes["inventory_coverage"]["paths"];
    assert_eq!(paths["topic_queue_broker_store_kubernetes"]["status"], "partial");
    assert_eq!(paths["client_connection_broker"]["status"], "available");

    let mut inventory = graph(2);
    add_client(&mut inventory);
    let upload = inventory.finish(1, 512 * 1024).unwrap();
    let paths = &upload.assets[0].attributes["inventory_coverage"]["paths"];
    assert_eq!(paths["topic_queue_broker_store_kubernetes"]["status"], "partial");
    assert_eq!(paths["client_connection_broker"]["status"], "partial");
}

#[test]
fn byte_trimming_measures_changed_coverage_and_retains_only_observed_endpoints() {
    // These sizes cross count digit boundaries and path-completeness changes.
    // No monotonic encoded-size assumption or exact maximum retention is needed.
    for queues in [9, 10, 99, 100, 4990] {
        for limit in [4096, 8192, 32768] {
            let upload = graph(queues).finish(5000, limit).expect("bounded upload");
            assert!(serde_json::to_vec(&upload).unwrap().len() <= limit);
            assert_eq!(upload.assets[0].kind, "cluster");
            assert!(upload.partial);
            let retained = upload
                .assets
                .iter()
                .map(|asset| (asset.kind, asset.external_key.as_str()))
                .collect::<std::collections::BTreeSet<_>>();
            assert!(upload.edges.iter().all(
                |edge| retained.contains(&(edge.from.kind, edge.from.external_key.as_str()))
                    && retained.contains(&(edge.to.kind, edge.to.external_key.as_str()))
            ));
            if upload.assets.len() < queues + 10 || upload.edges.len() < queues * 2 + 6 {
                assert_eq!(upload.assets[0].attributes["inventory_coverage"]["bounded"], true);
            }
        }
    }
    assert_eq!(
        graph(10).finish(5000, 1).unwrap_err().failure(),
        crate::ConnectorFailure::OutputTooLarge
    );
}
