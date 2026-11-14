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

use serde_json::Map;
use serde_json::Value;
use serde_json::json;

use super::AssetObservation;
use super::TopologyObservation;

const REQUIRED_KINDS: [&str; 14] = [
    "name_server",
    "controller",
    "broker",
    "proxy",
    "store",
    "pod",
    "node",
    "persistent_volume_claim",
    "pod_disruption_budget",
    "topic",
    "queue",
    "producer",
    "consumer",
    "connection",
];

#[derive(Default)]
struct CoverageObservation {
    queried: bool,
    partial: bool,
    reasons: BTreeSet<&'static str>,
}

pub(super) struct InventoryCoverage {
    observations: BTreeMap<&'static str, CoverageObservation>,
    inventory_reasons: BTreeSet<&'static str>,
}

/// Remember observed associations/participants lost from the bounded wire
/// representation. A remaining complete subgraph cannot erase that gap.
#[derive(Clone, Copy, Default)]
pub(super) struct PathLoss {
    topic_storage: bool,
    client_connection: bool,
}

impl PathLoss {
    pub(super) fn asset(&mut self, asset: &AssetObservation) {
        self.topic_storage |= matches!(
            asset.kind,
            "topic" | "queue" | "broker" | "store" | "pod" | "persistent_volume_claim"
        );
        self.client_connection |= matches!(asset.kind, "producer" | "consumer" | "connection" | "broker");
    }

    pub(super) fn edge(&mut self, edge: &TopologyObservation) {
        self.topic_storage |= matches!(
            (edge.from.kind, edge.relation, edge.to.kind),
            ("topic", "contains", "queue")
                | ("queue", "routes_to", "broker")
                | ("broker", "stores_on", "store")
                | ("store", "runs_on", "pod")
                | ("store", "stores_on", "persistent_volume_claim")
        );
        self.client_connection |= matches!(
            (edge.from.kind, edge.relation, edge.to.kind),
            ("producer" | "consumer", "connected_to", "connection") | ("connection", "connected_to", "broker")
        );
    }
}

impl InventoryCoverage {
    pub(super) fn new() -> Self {
        Self {
            observations: REQUIRED_KINDS
                .into_iter()
                .map(|kind| (kind, CoverageObservation::default()))
                .collect(),
            inventory_reasons: BTreeSet::new(),
        }
    }

    pub(super) fn mark_source(&mut self, kind: &'static str, partial: bool) {
        let observation = self.observations.entry(kind).or_default();
        observation.queried = true;
        observation.partial |= partial;
    }

    pub(super) fn mark_gap(&mut self, kind: &'static str, reason: &'static str) {
        self.observations.entry(kind).or_default().reasons.insert(reason);
    }

    pub(super) fn mark_gap_if_unqueried(&mut self, kind: &'static str, reason: &'static str) {
        let observation = self.observations.entry(kind).or_default();
        if !observation.queried {
            observation.reasons.insert(reason);
        }
    }

    pub(super) fn mark_inventory_gap(&mut self, reason: &'static str) {
        self.inventory_reasons.insert(reason);
    }

    pub(super) fn render(
        &self,
        assets: &[AssetObservation],
        edges: &[TopologyObservation],
        bounded: bool,
        loss: PathLoss,
    ) -> (Value, bool) {
        let counts = assets.iter().fold(BTreeMap::<&str, usize>::new(), |mut counts, asset| {
            *counts.entry(asset.kind).or_default() += 1;
            counts
        });
        let mut kinds = Map::new();
        let mut has_gap = false;
        for kind in REQUIRED_KINDS {
            let observation = self.observations.get(kind);
            let observed_count = counts.get(kind).copied().unwrap_or_default();
            let status = match observation {
                Some(observation) if observation.queried && !observation.partial && !bounded => "available",
                Some(observation) if observation.queried => "partial",
                _ => "not_production_verified",
            };
            has_gap |= status != "available";
            let reasons = observation
                .into_iter()
                .flat_map(|observation| observation.reasons.iter().copied())
                .take(8)
                .collect::<Vec<_>>();
            kinds.insert(
                kind.to_owned(),
                json!({
                    "status": status,
                    "observed_count": observed_count,
                    "reason_codes": reasons,
                }),
            );
        }

        let graph = RetainedGraph::new(assets, edges);
        let topic_path = if loss.topic_storage {
            "partial"
        } else {
            topic_storage_path_status(&graph)
        };
        let client_path = if loss.client_connection {
            "partial"
        } else {
            client_connection_path_status(&graph)
        };
        let client_path_reason = (client_path == "not_production_verified").then(|| {
            self.observations
                .get("connection")
                .and_then(|observation| observation.reasons.iter().next().copied())
                .unwrap_or("no_observed_client_connection")
        });
        has_gap |= topic_path != "available" || client_path != "available";
        (
            json!({
                "schema_version": "rocketmq-sre.inventory-coverage.v1",
                "bounded": bounded,
                "inventory_reason_codes": self.inventory_reasons.iter().copied().take(8).collect::<Vec<_>>(),
                "assets": kinds,
                "paths": {
                    "topic_queue_broker_store_kubernetes": {
                        "status": topic_path,
                        "required_path": "Topic -> Queue -> Broker -> Store -> Pod/Node/PVC"
                    },
                    "client_connection_broker": {
                        "status": client_path,
                        "required_path": "Producer/Consumer -> Connection -> Broker",
                        "reason_code": client_path_reason
                    }
                }
            }),
            has_gap,
        )
    }
}

type Node<'a> = (&'a str, &'a str);
type Adjacency<'a> = BTreeMap<(Node<'a>, &'a str), BTreeSet<Node<'a>>>;

/// Only retained endpoints participate in a path. Borrowed typed keys preserve
/// direction and identity without allocating a canonical String per comparison.
struct RetainedGraph<'a> {
    nodes: BTreeSet<Node<'a>>,
    outgoing: Adjacency<'a>,
    incoming: Adjacency<'a>,
    dangling_outgoing: BTreeSet<(Node<'a>, &'a str)>,
    dangling_incoming: BTreeSet<(Node<'a>, &'a str)>,
}

impl<'a> RetainedGraph<'a> {
    fn new(assets: &'a [AssetObservation], edges: &'a [TopologyObservation]) -> Self {
        let nodes = assets
            .iter()
            .map(|asset| (asset.kind, asset.external_key.as_str()))
            .collect::<BTreeSet<_>>();
        let mut outgoing = Adjacency::new();
        let mut incoming = Adjacency::new();
        let mut dangling_outgoing = BTreeSet::new();
        let mut dangling_incoming = BTreeSet::new();
        for edge in edges {
            let from = (edge.from.kind, edge.from.external_key.as_str());
            let to = (edge.to.kind, edge.to.external_key.as_str());
            if nodes.contains(&from) && nodes.contains(&to) {
                outgoing.entry((from, edge.relation)).or_default().insert(to);
                incoming.entry((to, edge.relation)).or_default().insert(from);
            } else {
                if nodes.contains(&from) {
                    dangling_outgoing.insert((from, edge.relation));
                }
                if nodes.contains(&to) {
                    dangling_incoming.insert((to, edge.relation));
                }
            }
        }
        Self {
            nodes,
            outgoing,
            incoming,
            dangling_outgoing,
            dangling_incoming,
        }
    }

    fn of_kind(&self, kind: &str) -> BTreeSet<Node<'a>> {
        self.nodes.iter().copied().filter(|node| node.0 == kind).collect()
    }

    fn outgoing(&self, node: Node<'a>, relation: &'a str) -> impl Iterator<Item = Node<'a>> + '_ {
        self.outgoing.get(&(node, relation)).into_iter().flatten().copied()
    }

    fn incoming(&self, node: Node<'a>, relation: &'a str) -> impl Iterator<Item = Node<'a>> + '_ {
        self.incoming.get(&(node, relation)).into_iter().flatten().copied()
    }
}

fn topic_storage_path_status(graph: &RetainedGraph<'_>) -> &'static str {
    let topics = graph.of_kind("topic");
    let queues = graph.of_kind("queue");
    if topics.is_empty() || queues.is_empty() {
        return "not_production_verified";
    }
    // Preserve the existing Pod-or-PVC tail: a Node is additional context, not
    // evidence required to replace either observed tail. Every Store belonging
    // to every routed Broker must have its own tail; other Brokers cannot fill it.
    let stores_with_tail = graph
        .of_kind("store")
        .into_iter()
        .filter(|store| {
            !graph.dangling_outgoing.contains(&(*store, "runs_on"))
                && !graph.dangling_outgoing.contains(&(*store, "stores_on"))
                && (graph.outgoing(*store, "runs_on").any(|node| node.0 == "pod")
                    || graph
                        .outgoing(*store, "stores_on")
                        .any(|node| node.0 == "persistent_volume_claim"))
        })
        .collect::<BTreeSet<_>>();
    let complete_brokers = graph
        .of_kind("broker")
        .into_iter()
        .filter(|broker| {
            let mut stores = graph
                .outgoing(*broker, "stores_on")
                .filter(|node| node.0 == "store")
                .peekable();
            !graph.dangling_outgoing.contains(&(*broker, "stores_on"))
                && stores.peek().is_some()
                && stores.all(|store| stores_with_tail.contains(&store))
        })
        .collect::<BTreeSet<_>>();
    let complete = topics.iter().all(|topic| {
        !graph.dangling_outgoing.contains(&(*topic, "contains"))
            && graph.outgoing(*topic, "contains").any(|node| queues.contains(&node))
    }) && queues.iter().all(|queue| {
        let has_topic = graph.incoming(*queue, "contains").any(|node| topics.contains(&node));
        let mut brokers = graph
            .outgoing(*queue, "routes_to")
            .filter(|node| node.0 == "broker")
            .peekable();
        !graph.dangling_incoming.contains(&(*queue, "contains"))
            && !graph.dangling_outgoing.contains(&(*queue, "routes_to"))
            && has_topic
            && brokers.peek().is_some()
            && brokers.all(|broker| complete_brokers.contains(&broker))
    });
    if complete { "available" } else { "partial" }
}

fn client_connection_path_status(graph: &RetainedGraph<'_>) -> &'static str {
    let clients = graph
        .nodes
        .iter()
        .copied()
        .filter(|node| matches!(node.0, "producer" | "consumer"))
        .collect::<BTreeSet<_>>();
    let connections = graph.of_kind("connection");
    if clients.is_empty() || connections.is_empty() {
        return "not_production_verified";
    }
    let connected = connections
        .into_iter()
        .filter(|connection| {
            !graph.dangling_outgoing.contains(&(*connection, "connected_to"))
                && graph
                    .outgoing(*connection, "connected_to")
                    .any(|node| node.0 == "broker")
        })
        .collect::<BTreeSet<_>>();
    let complete = clients.iter().all(|client| {
        !graph.dangling_outgoing.contains(&(*client, "connected_to"))
            && graph
                .outgoing(*client, "connected_to")
                .any(|node| connected.contains(&node))
    });
    if complete { "available" } else { "partial" }
}
