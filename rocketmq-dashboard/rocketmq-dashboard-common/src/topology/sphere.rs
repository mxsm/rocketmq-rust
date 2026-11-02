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

//! Static, native-renderer-independent spherical presentation of the bounded 2D view.
//!
//! Geometry carries the same typed identities, observations and edges as `TopologyLayout`.
//! Camera changes never query a source or alter the captured environment or Cluster filter.

use std::{collections::BTreeMap, f32::consts::TAU};

use super::{
    PlacedTopologyNode, TopologyEdge, TopologyLayout, TopologyNodeId, TOPOLOGY_EDGE_LIMIT, TOPOLOGY_NODE_LIMIT,
};

#[path = "sphere_projection.rs"]
mod projection;
pub use projection::*;

pub const SPHERE_LABEL_LIMIT: usize = 32;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum SphereGlobe {
    Brokers,
    NameServers,
    Controllers,
}
impl SphereGlobe {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Brokers => "Brokers and client routes",
            Self::NameServers => "NameServers",
            Self::Controllers => "Controllers",
        }
    }
    fn radius(self) -> f32 {
        match self {
            Self::Brokers => 1.,
            Self::NameServers | Self::Controllers => 0.36,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct SphereNode {
    pub placed: PlacedTopologyNode,
    pub globe: SphereGlobe,
    /// Local coordinates, relative to this globe's centre.
    pub position: [f32; 3],
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct SphereScene {
    pub nodes: Vec<SphereNode>,
    pub edges: Vec<TopologyEdge>,
    pub omitted_nodes: usize,
    pub omitted_edges: usize,
}

/// Reuses the 2D view's membership and budgets, including its busiest-Group selection.
pub fn sphere_scene(layout: &TopologyLayout) -> SphereScene {
    let mut nodes = layout
        .nodes
        .iter()
        .take(TOPOLOGY_NODE_LIMIT)
        .cloned()
        .collect::<Vec<_>>();
    nodes.sort_by(|a, b| a.node.id.cmp(&b.node.id));
    let mut rows = BTreeMap::<(SphereGlobe, String, u8), Vec<PlacedTopologyNode>>::new();
    for placed in nodes {
        let (globe, band) = match &placed.node.id {
            TopologyNodeId::NameServer(_) => (SphereGlobe::NameServers, 0),
            TopologyNodeId::Controller(_) => (SphereGlobe::Controllers, 0),
            TopologyNodeId::Configuration => (SphereGlobe::Brokers, 0),
            TopologyNodeId::Cluster(_) => (SphereGlobe::Brokers, 1),
            TopologyNodeId::Producer(_) => (SphereGlobe::Brokers, 2),
            TopologyNodeId::Broker(_) => (SphereGlobe::Brokers, 3),
            TopologyNodeId::Topic(_) => (SphereGlobe::Brokers, 4),
            TopologyNodeId::Consumer(_) => (SphereGlobe::Brokers, 5),
        };
        // Multi-Cluster nodes remain one selectable observation. Their complete membership
        // is retained on the node; only their presentation sector uses the first Cluster.
        let cluster = if globe == SphereGlobe::Brokers {
            placed.node.clusters.first().cloned().unwrap_or_default()
        } else {
            String::new()
        };
        rows.entry((globe, cluster, band)).or_default().push(placed);
    }
    let clusters = rows
        .keys()
        .filter(|(globe, _, _)| *globe == SphereGlobe::Brokers)
        .map(|(_, cluster, _)| cluster.clone())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let mut scene = SphereScene {
        omitted_nodes: layout
            .omitted_nodes
            .saturating_add(layout.nodes.len().saturating_sub(TOPOLOGY_NODE_LIMIT)),
        omitted_edges: layout.omitted_edges,
        ..Default::default()
    };
    for ((globe, cluster, band), row) in rows {
        let count = row.len();
        let sector = clusters
            .iter()
            .position(|candidate| candidate == &cluster)
            .unwrap_or_default();
        let arc = TAU / clusters.len().max(1) as f32;
        for (index, placed) in row.into_iter().enumerate() {
            let position = if globe == SphereGlobe::Brokers {
                let latitude = [66_f32, 50., 30., 5., -20., -45.][band as usize].to_radians();
                let middle = sector as f32 * arc;
                let longitude = middle + ((index as f32 + 0.5) / count as f32 - 0.5) * arc * 0.88;
                surface(latitude, longitude, 1.02)
            } else {
                // Small-globe nodes form a front-facing cap so configured endpoints stay legible.
                let angle: f32 = if count == 1 { 0. } else { 0.72 };
                let bearing = std::f32::consts::FRAC_PI_2 + index as f32 * TAU / count as f32;
                let radius = globe.radius() * 1.04;
                [
                    radius * angle.sin() * bearing.cos(),
                    radius * angle.sin() * bearing.sin(),
                    radius * angle.cos(),
                ]
            };
            scene.nodes.push(SphereNode {
                placed,
                globe,
                position,
            });
        }
    }
    let visible = scene
        .nodes
        .iter()
        .map(|node| &node.placed.node.id)
        .collect::<std::collections::BTreeSet<_>>();
    let edges = layout
        .edges
        .iter()
        .filter(|edge| visible.contains(&edge.id.from) && visible.contains(&edge.id.to))
        .collect::<Vec<_>>();
    scene.edges = edges.into_iter().take(TOPOLOGY_EDGE_LIMIT).cloned().collect();
    scene.omitted_edges = scene
        .omitted_edges
        .saturating_add(layout.edges.len().saturating_sub(scene.edges.len()));
    scene
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SphereTurn {
    Left,
    Right,
    Up,
    Down,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SphereZoom {
    In,
    Out,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SphereCamera {
    yaw: f32,
    pitch: f32,
    zoom: f32,
}
impl Default for SphereCamera {
    fn default() -> Self {
        Self {
            yaw: 0.,
            pitch: 14_f32.to_radians(),
            zoom: 1.,
        }
    }
}
impl SphereCamera {
    pub fn turn(&mut self, direction: SphereTurn) {
        let (yaw, pitch) = match direction {
            SphereTurn::Left => (-15., 0.),
            SphereTurn::Right => (15., 0.),
            SphereTurn::Up => (0., 15.),
            SphereTurn::Down => (0., -15.),
        };
        self.rotate(yaw, pitch);
    }
    /// Manual drag deltas in degrees. There is no autonomous motion or animation clock.
    pub fn rotate(&mut self, yaw: f32, pitch: f32) {
        if yaw.is_finite() && pitch.is_finite() {
            self.yaw = (self.yaw + yaw.to_radians()).rem_euclid(TAU);
            self.pitch = (self.pitch + pitch.to_radians()).clamp(-1.35, 1.35);
        }
    }
    pub fn zoom(&mut self, direction: SphereZoom) {
        self.zoom = match direction {
            SphereZoom::In => (self.zoom + 0.15).min(1.45),
            SphereZoom::Out => (self.zoom - 0.15).max(0.7),
        };
    }
    /// Moves the selected observation to its own globe's front without changing selection.
    pub fn focus(&mut self, scene: &SphereScene, selected: &TopologyNodeId) {
        if let Some(node) = scene.nodes.iter().find(|node| &node.placed.node.id == selected) {
            let [x, y, z] = node.position;
            if node.position.into_iter().all(f32::is_finite) {
                self.yaw = -x.atan2(z);
                self.pitch = y.atan2(x.hypot(z)).clamp(-1.35, 1.35);
            }
        }
    }
    pub fn scale(self) -> f32 {
        self.zoom
    }
}

fn surface(latitude: f32, longitude: f32, radius: f32) -> [f32; 3] {
    [
        radius * latitude.cos() * longitude.sin(),
        radius * latitude.sin(),
        radius * latitude.cos() * longitude.cos(),
    ]
}
fn rotate([x, y, z]: [f32; 3], camera: SphereCamera) -> [f32; 3] {
    let (sin_yaw, cos_yaw) = camera.yaw.sin_cos();
    let (sin_pitch, cos_pitch) = camera.pitch.sin_cos();
    let x1 = cos_yaw * x + sin_yaw * z;
    let z1 = -sin_yaw * x + cos_yaw * z;
    [x1, cos_pitch * y - sin_pitch * z1, sin_pitch * y + cos_pitch * z1]
}

#[cfg(test)]
#[path = "sphere_tests.rs"]
mod tests;
