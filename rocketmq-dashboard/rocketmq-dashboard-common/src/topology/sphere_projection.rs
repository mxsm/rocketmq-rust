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
use crate::TopologyBox;

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct SphereViewport {
    pub width: f32,
    pub height: f32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SphereUnavailable {
    InvalidViewport,
    SmallViewport,
    InvalidGeometry,
}
impl SphereUnavailable {
    pub const fn message(self) -> &'static str {
        match self {
            Self::InvalidViewport => "Sphere is waiting for a usable canvas. The same observations are shown in the 2D map.",
            Self::SmallViewport => "The canvas is too small for Sphere. The same observations are shown in the 2D map; widen the window to restore Sphere.",
            Self::InvalidGeometry => "Sphere could not prepare finite geometry. The same observations are shown in the 2D map.",
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ProjectedSphereNode {
    pub placed: PlacedTopologyNode,
    pub center: [f32; 2],
    pub depth: f32,
    pub front: bool,
}
#[derive(Clone, Debug, PartialEq)]
pub struct ProjectedSphereEdge {
    pub edge: TopologyEdge,
    pub from: [f32; 2],
    pub to: [f32; 2],
}
#[derive(Clone, Debug, PartialEq)]
pub struct ProjectedSphereGlobe {
    pub globe: SphereGlobe,
    pub center: [f32; 2],
    pub radius: f32,
    pub grid: Vec<([f32; 2], [f32; 2])>,
}
#[derive(Clone, Debug, PartialEq)]
pub struct SphereLabel {
    pub id: TopologyNodeId,
    pub text: String,
    pub bounds: TopologyBox,
}
#[derive(Clone, Debug, PartialEq)]
pub struct SphereProjection {
    pub viewport: SphereViewport,
    /// Back-to-front ordering is deterministic. Back-facing nodes remain selectable.
    pub nodes: Vec<ProjectedSphereNode>,
    pub edges: Vec<ProjectedSphereEdge>,
    /// A globe exists only when at least one actual observation belongs to it.
    pub globes: Vec<ProjectedSphereGlobe>,
    pub labels: Vec<SphereLabel>,
}

/// Orthographic geometry and label placement, prepared before the native render path.
///
/// # Errors
/// Returns a fallback reason for non-finite geometry or a viewport below 480 by 260 pixels.
pub fn project_sphere(
    scene: &SphereScene,
    camera: SphereCamera,
    viewport: SphereViewport,
    selected: Option<&TopologyNodeId>,
) -> Result<SphereProjection, SphereUnavailable> {
    if !viewport.width.is_finite() || !viewport.height.is_finite() || viewport.width <= 0. || viewport.height <= 0. {
        return Err(SphereUnavailable::InvalidViewport);
    }
    if viewport.width < 480. || viewport.height < 260. {
        return Err(SphereUnavailable::SmallViewport);
    }
    if scene.nodes.iter().any(|node| {
        node.position
            .iter()
            .any(|value| !value.is_finite() || value.abs() > 1.1)
    }) {
        return Err(SphereUnavailable::InvalidGeometry);
    }
    let radius = (viewport.width * 0.18).min(viewport.height * 0.28) * camera.scale();
    let mut nodes = scene
        .nodes
        .iter()
        .take(TOPOLOGY_NODE_LIMIT)
        .map(|node| {
            let [x, y, z] = rotate(node.position, camera);
            let [center_x, center_y] = center(node.globe, viewport);
            ProjectedSphereNode {
                placed: node.placed.clone(),
                center: [center_x + x * radius, center_y - y * radius],
                depth: z,
                front: z >= 0.,
            }
        })
        .collect::<Vec<_>>();
    nodes.sort_by(|a, b| {
        a.depth
            .total_cmp(&b.depth)
            .then(a.placed.node.id.cmp(&b.placed.node.id))
    });
    let positions = nodes
        .iter()
        .map(|node| (&node.placed.node.id, node.center))
        .collect::<BTreeMap<_, _>>();
    let edges = scene
        .edges
        .iter()
        .take(TOPOLOGY_EDGE_LIMIT)
        .filter_map(|edge| {
            Some(ProjectedSphereEdge {
                edge: edge.clone(),
                from: *positions.get(&edge.id.from)?,
                to: *positions.get(&edge.id.to)?,
            })
        })
        .collect();
    let globes = scene
        .nodes
        .iter()
        .map(|node| node.globe)
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .map(|globe| {
            let center = center(globe, viewport);
            let globe_radius = globe.radius() * radius;
            ProjectedSphereGlobe {
                globe,
                center,
                radius: globe_radius,
                grid: grid(globe_radius, center, camera),
            }
        })
        .collect();
    let labels = labels(&nodes, selected, viewport);
    Ok(SphereProjection {
        viewport,
        nodes,
        edges,
        globes,
        labels,
    })
}

fn center(globe: SphereGlobe, viewport: SphereViewport) -> [f32; 2] {
    let horizontal = match globe {
        SphereGlobe::Brokers => 0.5,
        SphereGlobe::NameServers => 0.13,
        SphereGlobe::Controllers => 0.87,
    };
    [viewport.width * horizontal, viewport.height * 0.52]
}

fn grid(radius: f32, [center_x, center_y]: [f32; 2], camera: SphereCamera) -> Vec<([f32; 2], [f32; 2])> {
    let mut lines = Vec::new();
    let project = |[x, y, _]: [f32; 3]| [center_x + x * radius, center_y - y * radius];
    for index in 0..64 {
        let a = index as f32 * TAU / 64.;
        let b = (index + 1) as f32 * TAU / 64.;
        lines.push((project([a.cos(), a.sin(), 0.]), project([b.cos(), b.sin(), 0.])));
    }
    for latitude in [-45_f32, 0., 45.] {
        for index in 0..64 {
            let a = rotate(surface(latitude.to_radians(), index as f32 * TAU / 64., 1.), camera);
            let b = rotate(
                surface(latitude.to_radians(), (index + 1) as f32 * TAU / 64., 1.),
                camera,
            );
            if a[2] >= 0. && b[2] >= 0. {
                lines.push((project(a), project(b)));
            }
        }
    }
    for longitude in [-60_f32, 0., 60.] {
        for index in 0..32 {
            let a = rotate(
                surface(
                    (-90. + index as f32 * 180. / 32.).to_radians(),
                    longitude.to_radians(),
                    1.,
                ),
                camera,
            );
            let b = rotate(
                surface(
                    (-90. + (index + 1) as f32 * 180. / 32.).to_radians(),
                    longitude.to_radians(),
                    1.,
                ),
                camera,
            );
            if a[2] >= 0. && b[2] >= 0. {
                lines.push((project(a), project(b)));
            }
        }
    }
    lines
}

fn labels(
    nodes: &[ProjectedSphereNode],
    selected: Option<&TopologyNodeId>,
    viewport: SphereViewport,
) -> Vec<SphereLabel> {
    let mut candidates = nodes
        .iter()
        .filter(|node| node.front || selected == Some(&node.placed.node.id))
        .collect::<Vec<_>>();
    candidates.sort_by(|a, b| {
        (selected == Some(&b.placed.node.id))
            .cmp(&(selected == Some(&a.placed.node.id)))
            .then(b.depth.total_cmp(&a.depth))
            .then(a.placed.node.id.cmp(&b.placed.node.id))
    });
    let mut labels = Vec::<SphereLabel>::new();
    for node in candidates {
        if labels.len() == SPHERE_LABEL_LIMIT {
            break;
        }
        let name = node.placed.node.label.chars().take(28).collect::<String>();
        let text = format!(
            "{} · {}{}{}",
            node.placed.node.id.kind_label(),
            name,
            if node.placed.node.label.chars().count() > 28 {
                "…"
            } else {
                ""
            },
            if node.placed.stale { " · cached" } else { "" }
        );
        let width = (text.chars().count() as f32 * 6.5 + 8.).min(210.);
        let [x, y] = node.center;
        for (left, top) in [
            (x + 12., y - 10.),
            (x + 12., y + 12.),
            (x - width - 12., y - 10.),
            (x - width / 2., y - 32.),
        ] {
            let bounds = TopologyBox {
                x: left.clamp(8., viewport.width - width - 8.),
                y: top.clamp(8., viewport.height - 28.),
                width,
                height: 20.,
            };
            if labels.iter().all(|label| !overlaps(label.bounds, bounds)) {
                labels.push(SphereLabel {
                    id: node.placed.node.id.clone(),
                    text,
                    bounds,
                });
                break;
            }
        }
    }
    labels
}

fn overlaps(a: TopologyBox, b: TopologyBox) -> bool {
    a.x < b.x + b.width + 3. && b.x < a.x + a.width + 3. && a.y < b.y + b.height + 3. && b.y < a.y + a.height + 3.
}
