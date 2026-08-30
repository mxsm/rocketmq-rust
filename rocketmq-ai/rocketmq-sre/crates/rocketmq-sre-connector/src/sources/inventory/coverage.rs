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

        let topic_path = topic_storage_path_status(assets, edges);
        let client_path = client_connection_path_status(assets, edges);
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

fn topic_storage_path_status(assets: &[AssetObservation], edges: &[TopologyObservation]) -> &'static str {
    let topics = keys_of_kind(assets, "topic");
    let queues = keys_of_kind(assets, "queue");
    if topics.is_empty() || queues.is_empty() {
        return "not_production_verified";
    }
    let brokers = keys_of_kind(assets, "broker");
    let stores = keys_of_kind(assets, "store");
    let pods = keys_of_kind(assets, "pod");
    let nodes = keys_of_kind(assets, "node");
    let claims = keys_of_kind(assets, "persistent_volume_claim");
    let mut complete = true;
    let mut any_kubernetes_tail = false;
    for topic in &topics {
        if !edges.iter().any(|edge| {
            edge.relation == "contains" && canonical(&edge.from) == *topic && queues.contains(&canonical(&edge.to))
        }) {
            complete = false;
        }
    }
    for queue in &queues {
        let has_topic = edges.iter().any(|edge| {
            edge.relation == "contains" && topics.contains(&canonical(&edge.from)) && canonical(&edge.to) == *queue
        });
        let routed_brokers = edges
            .iter()
            .filter(|edge| {
                edge.relation == "routes_to"
                    && canonical(&edge.from) == *queue
                    && brokers.contains(&canonical(&edge.to))
            })
            .map(|edge| canonical(&edge.to))
            .collect::<Vec<_>>();
        if !has_topic || routed_brokers.is_empty() {
            complete = false;
            continue;
        }
        for broker in routed_brokers {
            let broker_stores = edges
                .iter()
                .filter(|edge| {
                    edge.relation == "stores_on"
                        && canonical(&edge.from) == broker
                        && stores.contains(&canonical(&edge.to))
                })
                .map(|edge| canonical(&edge.to))
                .collect::<Vec<_>>();
            if broker_stores.is_empty() {
                complete = false;
                continue;
            }
            for store in broker_stores {
                let has_tail = edges.iter().any(|edge| {
                    canonical(&edge.from) == store
                        && ((edge.relation == "runs_on" && pods.contains(&canonical(&edge.to)))
                            || (edge.relation == "stores_on" && claims.contains(&canonical(&edge.to))))
                }) || edges.iter().any(|store_pod| {
                    canonical(&store_pod.from) == store
                        && store_pod.relation == "runs_on"
                        && pods.contains(&canonical(&store_pod.to))
                        && edges.iter().any(|pod_node| {
                            pod_node.relation == "runs_on"
                                && pod_node.from == store_pod.to
                                && nodes.contains(&canonical(&pod_node.to))
                        })
                });
                any_kubernetes_tail |= has_tail;
                complete &= has_tail;
            }
        }
    }
    if complete && any_kubernetes_tail {
        "available"
    } else {
        "partial"
    }
}

fn client_connection_path_status(assets: &[AssetObservation], edges: &[TopologyObservation]) -> &'static str {
    let clients = assets
        .iter()
        .filter(|asset| matches!(asset.kind, "producer" | "consumer"))
        .map(|asset| format!("{}:{}", asset.kind, asset.external_key))
        .collect::<BTreeSet<_>>();
    let connections = keys_of_kind(assets, "connection");
    let brokers = keys_of_kind(assets, "broker");
    if clients.is_empty() || connections.is_empty() {
        return "not_production_verified";
    }
    let complete = clients.iter().all(|client| {
        edges
            .iter()
            .filter(|edge| edge.relation == "connected_to" && canonical(&edge.from) == *client)
            .any(|client_edge| {
                let connection = canonical(&client_edge.to);
                connections.contains(&connection)
                    && edges.iter().any(|broker_edge| {
                        broker_edge.relation == "connected_to"
                            && broker_edge.from == client_edge.to
                            && brokers.contains(&canonical(&broker_edge.to))
                    })
            })
    });
    if complete { "available" } else { "partial" }
}

fn keys_of_kind(assets: &[AssetObservation], kind: &str) -> BTreeSet<String> {
    assets
        .iter()
        .filter(|asset| asset.kind == kind)
        .map(|asset| format!("{}:{}", asset.kind, asset.external_key))
        .collect()
}

fn canonical(key: &super::AssetKey) -> String {
    format!("{}:{}", key.kind, key.external_key)
}
