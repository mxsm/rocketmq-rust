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
use crate::*;

fn topic(index: usize, cluster: &str, now: u64) -> TopologyNode {
    let mut node = TopologyNode::new(
        TopologyNodeId::Topic(format!("topic-{index:04}")),
        format!("topic-{index:04}"),
        TopologySource::Topics,
        now,
    );
    node.clusters.insert(cluster.into());
    node
}
fn context() -> TopologyContext {
    TopologyContext {
        environment: None,
        revision: 1,
        scope: ConnectionScope::NameServer,
    }
}
fn observed(nodes: Vec<TopologyNode>, now: u64) -> TopologyLayer {
    TopologyLayer {
        total: nodes.len(),
        nodes,
        ..TopologyLayer::empty(TopologySource::Topics, now)
    }
}
fn viewport() -> SphereViewport {
    SphereViewport {
        width: 960.,
        height: 400.,
    }
}

#[test]
fn sphere_uses_the_bounded_filtered_map_membership_edges_and_observation_quality() {
    let nodes = (0..400)
        .map(|index| topic(index, if index % 2 == 0 { "a" } else { "b" }, 100))
        .collect();
    let mut layer = observed(nodes, 100);
    for index in 1..400 {
        layer.edges.push(TopologyEdge::new(
            topic(0, "a", 100).id,
            topic(index, "a", 100).id,
            TopologyEdgeKind::TopicRoute,
            TopologySource::Topics,
            100,
        ));
    }
    let mut cache = TopologyCache::default();
    cache.apply(context(), vec![layer], 100);
    cache.apply(
        context(),
        vec![TopologyLayer::failed(
            TopologySource::Topics,
            200,
            TopologyFailureCode::Connection,
        )],
        200,
    );
    let layout = layout_topology(cache.snapshot().unwrap(), Some("a"), 1.);
    let scene = sphere_scene(&layout);
    assert_eq!(scene.nodes.len(), layout.nodes.len());
    assert_eq!(scene.edges, layout.edges);
    assert_eq!(scene.omitted_nodes, layout.omitted_nodes);
    assert_eq!(scene.omitted_edges, layout.omitted_edges);
    for node in &scene.nodes {
        assert_eq!(
            &node.placed,
            layout
                .nodes
                .iter()
                .find(|placed| placed.node.id == node.placed.node.id)
                .unwrap()
        );
        assert!(node.placed.stale && node.placed.node.observed_at_ms == 100);
        assert_eq!(node.placed.node.availability, EndpointAvailability::Unknown);
        assert_eq!(
            node.placed.node.clusters,
            std::collections::BTreeSet::from(["a".into()])
        );
    }
    let projection = project_sphere(&scene, SphereCamera::default(), viewport(), None).unwrap();
    assert!(projection.nodes.len() <= TOPOLOGY_NODE_LIMIT && projection.edges.len() <= TOPOLOGY_EDGE_LIMIT);
    assert!(projection.labels.len() <= SPHERE_LABEL_LIMIT);
    assert_eq!(
        projection.globes.iter().map(|globe| globe.globe).collect::<Vec<_>>(),
        vec![SphereGlobe::Brokers]
    );
    assert!(projection.nodes.iter().all(|node| node.placed.stale));
    let mut reversed = layout.clone();
    reversed.nodes.reverse();
    assert_eq!(scene, sphere_scene(&reversed));
    cache.expire(100 + TOPOLOGY_CACHE_TTL_MS + 1);
    let expired = sphere_scene(&layout_topology(cache.snapshot().unwrap(), Some("a"), 1.));
    assert!(expired.nodes.is_empty() && expired.edges.is_empty());
}

#[test]
fn sphere_cameras_stay_finite_focus_every_globe_and_reset_without_altering_observations() {
    let mut layer = observed(vec![topic(0, "a", 100), topic(1, "b", 100)], 100);
    layer.nodes.extend([
        TopologyNode::new(
            TopologyNodeId::NameServer("ns:9876".into()),
            "ns:9876".into(),
            TopologySource::Nameservers,
            100,
        ),
        TopologyNode::new(
            TopologyNodeId::Controller("controller:9878".into()),
            "controller:9878".into(),
            TopologySource::Replication,
            100,
        ),
    ]);
    let snapshot = TopologySnapshot {
        context: context(),
        generated_at_ms: 100,
        layers: std::collections::BTreeMap::from([(TopologySource::Topics, layer)]),
    };
    let scene = sphere_scene(&layout_topology(&snapshot, None, 1.));
    let original = scene.clone();
    let mut camera = SphereCamera::default();
    camera.rotate(f32::NAN, f32::INFINITY);
    assert_eq!(camera, SphereCamera::default());
    for _ in 0..1000 {
        camera.turn(SphereTurn::Up);
        camera.turn(SphereTurn::Right);
        camera.zoom(SphereZoom::In);
    }
    let projection = project_sphere(&scene, camera, viewport(), None).unwrap();
    assert_eq!(projection.globes.len(), 3);
    for node in &scene.nodes {
        camera.focus(&scene, &node.placed.node.id);
        let projection = project_sphere(&scene, camera, viewport(), Some(&node.placed.node.id)).unwrap();
        let point = projection
            .nodes
            .iter()
            .find(|point| point.placed.node.id == node.placed.node.id)
            .unwrap();
        let globe = projection
            .globes
            .iter()
            .find(|globe| globe.globe == node.globe)
            .unwrap();
        assert!(point.front);
        assert!((point.center[0] - globe.center[0]).abs() < 0.001 && (point.center[1] - globe.center[1]).abs() < 0.001);
        assert!(projection.labels.iter().any(|label| label.id == node.placed.node.id));
    }
    for _ in 0..1000 {
        camera.zoom(SphereZoom::Out);
        camera.turn(SphereTurn::Down);
    }
    for node in project_sphere(&scene, camera, viewport(), None).unwrap().nodes {
        assert!(node.center.into_iter().all(f32::is_finite));
        assert!(
            node.center[0] >= 0.
                && node.center[0] <= viewport().width
                && node.center[1] >= 0.
                && node.center[1] <= viewport().height
        );
    }
    assert_eq!(scene, original);
    assert_eq!(
        project_sphere(&scene, SphereCamera::default(), viewport(), None),
        project_sphere(&original, SphereCamera::default(), viewport(), None)
    );
}

#[test]
fn sphere_labels_have_a_fixed_budget_no_overlap_and_selected_node_priority() {
    let snapshot = TopologySnapshot {
        context: context(),
        generated_at_ms: 100,
        layers: std::collections::BTreeMap::from([(
            TopologySource::Topics,
            observed((0..320).map(|index| topic(index, "a", 100)).collect(), 100),
        )]),
    };
    let scene = sphere_scene(&layout_topology(&snapshot, None, 1.));
    let selected = scene.nodes.last().unwrap().placed.node.id.clone();
    let projection = project_sphere(&scene, SphereCamera::default(), viewport(), Some(&selected)).unwrap();
    assert!(!projection.labels.is_empty() && projection.labels.len() <= SPHERE_LABEL_LIMIT);
    assert_eq!(projection.labels[0].id, selected);
    for (index, label) in projection.labels.iter().enumerate() {
        let bounds = label.bounds;
        assert!(
            bounds.x >= 0.
                && bounds.y >= 0.
                && bounds.x + bounds.width <= viewport().width
                && bounds.y + bounds.height <= viewport().height
        );
        for other in &projection.labels[index + 1..] {
            let other = other.bounds;
            assert!(
                bounds.x + bounds.width <= other.x
                    || other.x + other.width <= bounds.x
                    || bounds.y + bounds.height <= other.y
                    || other.y + other.height <= bounds.y
            );
        }
    }
}

#[test]
fn sphere_rejects_invalid_or_tiny_viewports_and_malformed_geometry() {
    let scene = SphereScene::default();
    for viewport in [
        SphereViewport::default(),
        SphereViewport {
            width: f32::NAN,
            height: 400.,
        },
        SphereViewport {
            width: 960.,
            height: f32::INFINITY,
        },
    ] {
        assert_eq!(
            project_sphere(&scene, SphereCamera::default(), viewport, None),
            Err(SphereUnavailable::InvalidViewport)
        );
    }
    assert_eq!(
        project_sphere(
            &scene,
            SphereCamera::default(),
            SphereViewport {
                width: 479.,
                height: 400.
            },
            None
        ),
        Err(SphereUnavailable::SmallViewport)
    );
    let mut scene = sphere_scene(&TopologyLayout {
        nodes: vec![PlacedTopologyNode {
            node: topic(0, "a", 100),
            bounds: TopologyBox {
                x: 0.,
                y: 0.,
                width: 1.,
                height: 1.,
            },
            stale: false,
        }],
        ..Default::default()
    });
    scene.nodes[0].position[0] = f32::NAN;
    let mut camera = SphereCamera::default();
    camera.focus(&scene, &scene.nodes[0].placed.node.id);
    assert_eq!(camera, SphereCamera::default());
    assert_eq!(
        project_sphere(&scene, camera, viewport(), None),
        Err(SphereUnavailable::InvalidGeometry)
    );
}
