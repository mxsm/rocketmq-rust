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

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TopologyDiff {
    pub added: Vec<TopologyNodeId>,
    /// Nodes with changed projected identity labels, membership, availability, version or traffic.
    pub changed: Vec<TopologyNodeId>,
    /// Absence is reported only from a complete, untruncated inventory in the same context.
    pub removed: Vec<TopologyNodeId>,
    pub unconfirmed_missing: usize,
}

#[derive(Default)]
pub struct TopologyCache {
    snapshot: Option<TopologySnapshot>,
}
impl TopologyCache {
    pub fn clear(&mut self) {
        self.snapshot = None;
    }
    pub fn snapshot(&self) -> Option<&TopologySnapshot> {
        self.snapshot.as_ref()
    }
    /// Expires retained observations before a paused page becomes visible again, without
    /// presenting elapsed time as a new observation or confirmed inventory removal.
    pub fn expire(&mut self, now: u64) -> TopologyDiff {
        let Some(snapshot) = self.snapshot.as_mut() else {
            return TopologyDiff::default();
        };
        let previous = snapshot.clone();
        for layer in snapshot.layers.values_mut() {
            expire_layer(layer, now);
        }
        diff(Some(&previous), snapshot)
    }
    /// Merges independent source observations, retaining partial/failed-source data until expiry.
    pub fn apply(&mut self, context: TopologyContext, layers: Vec<TopologyLayer>, now: u64) -> TopologyDiff {
        if self
            .snapshot
            .as_ref()
            .is_some_and(|snapshot| snapshot.context != context)
        {
            self.clear();
        }
        let old = self.snapshot.as_ref();
        let mut incoming = layers
            .into_iter()
            .map(|layer| (layer.source, layer))
            .collect::<BTreeMap<_, _>>();
        let mut merged = BTreeMap::new();
        for source in TopologySource::ALL {
            let next = incoming
                .remove(&source)
                .unwrap_or_else(|| TopologyLayer::failed(source, now, TopologyFailureCode::Unavailable));
            merged.insert(
                source,
                merge(old.and_then(|snapshot| snapshot.layers.get(&source)), next, now),
            );
        }
        let next = TopologySnapshot {
            context,
            generated_at_ms: now,
            layers: merged,
        };
        let diff = diff(old, &next);
        self.snapshot = Some(next);
        diff
    }
}

fn merge(previous: Option<&TopologyLayer>, mut next: TopologyLayer, now: u64) -> TopologyLayer {
    next.total_edges = next.total_edges.max(next.edges.len());
    if !next.inventory_complete || next.truncated {
        if let Some(previous) = previous {
            let mut nodes = previous
                .nodes
                .iter()
                .cloned()
                .map(|node| (node.id.clone(), node))
                .collect::<BTreeMap<_, _>>();
            nodes.extend(next.nodes.into_iter().map(|node| (node.id.clone(), node)));
            next.nodes = nodes.into_values().collect();
            let mut edges = previous
                .edges
                .iter()
                .cloned()
                .map(|edge| (edge.id.clone(), edge))
                .collect::<BTreeMap<_, _>>();
            edges.extend(next.edges.into_iter().map(|edge| (edge.id.clone(), edge)));
            next.edges = edges.into_values().collect();
            let mut replication = previous
                .replication
                .iter()
                .cloned()
                .map(|group| ((group.cluster.clone(), group.broker_name.clone()), group))
                .collect::<BTreeMap<_, _>>();
            replication.extend(
                next.replication
                    .into_iter()
                    .map(|group| ((group.cluster.clone(), group.broker_name.clone()), group)),
            );
            next.replication = replication.into_values().collect();
            let mut controllers = previous
                .controllers
                .iter()
                .cloned()
                .map(|controller| (controller.address.clone(), controller))
                .collect::<BTreeMap<_, _>>();
            for controller in next.controllers {
                if controller.observed_at_ms.is_some() || !controllers.contains_key(&controller.address) {
                    controllers.insert(controller.address.clone(), controller);
                }
            }
            next.controllers = controllers.into_values().collect();
            if next.state == TopologySourceState::Failed {
                next.observed_at_ms = previous.observed_at_ms;
                next.total = previous.total;
                next.total_edges = previous.total_edges;
                next.total_complete = previous.total_complete;
            }
        }
    }
    expire_layer(&mut next, now);
    if next.nodes.len() > TOPOLOGY_NODE_LIMIT
        || next.edges.len() > TOPOLOGY_EDGE_LIMIT
        || next.replication.len() > TOPOLOGY_NODE_LIMIT
    {
        next.truncated = true;
        next.inventory_complete = false;
        next.nodes
            .sort_by(|a, b| b.observed_at_ms.cmp(&a.observed_at_ms).then(a.id.cmp(&b.id)));
        next.nodes.truncate(TOPOLOGY_NODE_LIMIT);
        next.edges
            .sort_by(|a, b| b.observed_at_ms.cmp(&a.observed_at_ms).then(a.id.cmp(&b.id)));
        next.edges.truncate(TOPOLOGY_EDGE_LIMIT);
        next.replication.sort_by(|a, b| {
            b.observed_at_ms
                .cmp(&a.observed_at_ms)
                .then(a.cluster.cmp(&b.cluster))
                .then(a.broker_name.cmp(&b.broker_name))
        });
        next.replication.truncate(TOPOLOGY_NODE_LIMIT);
    }
    next.nodes.sort_by(|a, b| a.id.cmp(&b.id));
    next.edges.sort_by(|a, b| a.id.cmp(&b.id));
    retain_controller_identities(&mut next);
    next
}

fn expire_layer(layer: &mut TopologyLayer, now: u64) {
    layer
        .nodes
        .retain(|node| now.saturating_sub(node.observed_at_ms) <= TOPOLOGY_CACHE_TTL_MS);
    layer
        .edges
        .retain(|edge| now.saturating_sub(edge.observed_at_ms) <= TOPOLOGY_CACHE_TTL_MS);
    layer.expired = layer.observed_at_ms.is_some_and(|observed| expired(observed, now));
    for controller in &mut layer.controllers {
        if controller.observed_at_ms.is_some_and(|observed| expired(observed, now)) {
            controller.observed_at_ms = None;
            controller.group = None;
            controller.leader_id = None;
            controller.is_leader = None;
            controller.peer_count = None;
            controller.last_log_index = None;
            controller.committed_log_index = None;
            controller.applied_log_index = None;
        }
    }
    layer.replication.retain_mut(|group| {
        if expired(group.observed_at_ms, now) {
            return false;
        }
        if group
            .config_observed_at_ms
            .is_some_and(|observed| expired(observed, now))
        {
            group.config_observed_at_ms = None;
            group.election = TopologyElection::Unknown;
            group.controllers.clear();
        }
        if group.ha_observed_at_ms.is_some_and(|observed| expired(observed, now)) {
            group.ha_observed_at_ms = None;
            group.master_commit_log_max_offset = None;
            group.in_sync_slave_count = None;
            for replica in &mut group.replicas {
                replica.lag_bytes = None;
                replica.in_sync = None;
                replica.bytes_per_second = None;
            }
        }
        if group.sync_observed_at_ms.is_some_and(|observed| expired(observed, now)) {
            group.sync_observed_at_ms = None;
            group.sync_state = None;
        }
        true
    });
    retain_controller_identities(layer);
}

fn expired(observed: u64, now: u64) -> bool {
    now.saturating_sub(observed) > TOPOLOGY_CACHE_TTL_MS
}

fn retain_controller_identities(layer: &mut TopologyLayer) {
    let addresses = layer
        .nodes
        .iter()
        .filter_map(|node| match &node.id {
            TopologyNodeId::Controller(address) => Some(address.as_str()),
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    layer
        .controllers
        .retain(|controller| addresses.contains(controller.address.as_str()));
}

fn diff(old: Option<&TopologySnapshot>, next: &TopologySnapshot) -> TopologyDiff {
    let mut result = TopologyDiff::default();
    let previous = old
        .map(TopologySnapshot::nodes)
        .unwrap_or_default()
        .into_iter()
        .map(|node| (node.id.clone(), node))
        .collect::<BTreeMap<_, _>>();
    let current = next
        .nodes()
        .into_iter()
        .map(|node| (node.id.clone(), node))
        .collect::<BTreeMap<_, _>>();
    for (id, node) in &current {
        match previous.get(id) {
            None => result.added.push(id.clone()),
            Some(before)
                if before.label != node.label
                    || before.clusters != node.clusters
                    || before.availability != node.availability
                    || before.broker_version != node.broker_version
                    || before.traffic != node.traffic =>
            {
                result.changed.push(id.clone())
            }
            Some(_) => {}
        }
    }
    for (id, node) in previous {
        if current.contains_key(&id) {
            continue;
        }
        if next
            .layers
            .get(&node.source)
            .is_some_and(|layer| layer.inventory_complete && !layer.truncated && !layer.expired)
        {
            result.removed.push(id);
        } else {
            result.unconfirmed_missing += 1;
        }
    }
    result
}
