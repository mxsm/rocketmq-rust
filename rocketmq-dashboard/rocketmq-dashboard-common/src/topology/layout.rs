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

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct TopologyBox {
    pub x: f32,
    pub y: f32,
    pub width: f32,
    pub height: f32,
}
#[derive(Clone, Debug, PartialEq)]
pub struct PlacedTopologyNode {
    pub node: TopologyNode,
    pub bounds: TopologyBox,
    pub stale: bool,
}
#[derive(Clone, Debug, Default, PartialEq)]
pub struct TopologyLayout {
    pub nodes: Vec<PlacedTopologyNode>,
    pub edges: Vec<TopologyEdge>,
    pub paths: Vec<(TopologyBox, TopologyBox)>,
    pub width: f32,
    pub height: f32,
    pub omitted_nodes: usize,
    pub omitted_edges: usize,
}

/// Deterministic columns and structural edge clipping, computed before a renderer paints.
pub fn layout_topology(snapshot: &TopologySnapshot, cluster: Option<&str>, scale: f32) -> TopologyLayout {
    let mut nodes = snapshot
        .nodes()
        .into_iter()
        .filter(|node| {
            cluster.is_none_or(|cluster| {
                matches!(
                    node.id,
                    TopologyNodeId::Configuration | TopologyNodeId::NameServer(_) | TopologyNodeId::Controller(_)
                ) || node.clusters.contains(cluster)
            })
        })
        .collect::<Vec<_>>();
    nodes.sort_by(|a, b| match (&a.id, &b.id) {
        (TopologyNodeId::Producer(_), TopologyNodeId::Producer(_))
        | (TopologyNodeId::Consumer(_), TopologyNodeId::Consumer(_)) => {
            b.traffic.connections.cmp(&a.traffic.connections).then(a.id.cmp(&b.id))
        }
        _ => a.id.cmp(&b.id),
    });
    let mut producer_count = 0;
    let mut consumer_count = 0;
    let before = nodes.len();
    nodes.retain(|node| match node.id {
        TopologyNodeId::Producer(_) => {
            producer_count += 1;
            producer_count <= TOPOLOGY_GROUP_DISPLAY_LIMIT
        }
        TopologyNodeId::Consumer(_) => {
            consumer_count += 1;
            consumer_count <= TOPOLOGY_GROUP_DISPLAY_LIMIT
        }
        _ => true,
    });
    let mut layout = TopologyLayout {
        width: 1560. * scale,
        ..Default::default()
    };
    layout.omitted_nodes = before.saturating_sub(nodes.len()) + nodes.len().saturating_sub(TOPOLOGY_NODE_LIMIT);
    let mut rows = [0usize; 7];
    for node in nodes.into_iter().take(TOPOLOGY_NODE_LIMIT) {
        let column = match &node.id {
            TopologyNodeId::Configuration | TopologyNodeId::Producer(_) => 0,
            TopologyNodeId::NameServer(_) => 1,
            TopologyNodeId::Cluster(_) | TopologyNodeId::Controller(_) => 2,
            TopologyNodeId::Broker(_) => 3,
            TopologyNodeId::Topic(_) => 4,
            TopologyNodeId::Consumer(_) => 5,
        };
        let row = rows[column];
        rows[column] += 1;
        let stale = snapshot.layers.get(&node.source).is_some_and(|layer| {
            layer.state == TopologySourceState::Failed || layer.expired || node.observed_at_ms < layer.attempted_at_ms
        });
        layout.nodes.push(PlacedTopologyNode {
            node,
            bounds: TopologyBox {
                x: (12. + column as f32 * 260.) * scale,
                y: (40. + row as f32 * 90.) * scale,
                width: 230. * scale,
                height: 72. * scale,
            },
            stale,
        });
    }
    layout.height = (60. + rows.into_iter().max().unwrap_or_default() as f32 * 90.) * scale;
    let visible = layout
        .nodes
        .iter()
        .map(|node| node.node.id.clone())
        .collect::<BTreeSet<_>>();
    let mut edges = BTreeMap::new();
    for layer in snapshot.layers.values() {
        for edge in &layer.edges {
            if visible.contains(&edge.id.from) && visible.contains(&edge.id.to) {
                edges.insert(edge.id.clone(), edge.clone());
            }
        }
    }
    layout.omitted_edges = edges.len().saturating_sub(TOPOLOGY_EDGE_LIMIT);
    layout.edges = edges.into_values().take(TOPOLOGY_EDGE_LIMIT).collect();
    let positions = layout
        .nodes
        .iter()
        .map(|node| (&node.node.id, node.bounds))
        .collect::<BTreeMap<_, _>>();
    layout.paths = layout
        .edges
        .iter()
        .filter_map(|edge| Some((*positions.get(&edge.id.from)?, *positions.get(&edge.id.to)?)))
        .collect();
    layout
}
