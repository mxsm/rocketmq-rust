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

use gpui::{AppContext as _, Modifiers, point, px, size};
use gpui_component::Root;
use rocketmq_dashboard_common::{TopologyNode, TopologyNodeId, TopologySource};
use std::{cell::RefCell, rc::Rc, sync::Arc};

use super::*;
use crate::services::{AppServices, topology::test_support::FakeTopologyBackend};

#[gpui::test]
fn topology_header_retains_readable_width_and_separates_controls_after_window_resize(cx: &mut gpui::TestAppContext) {
    cx.update(gpui_component::init);
    let backend = Arc::new(FakeTopologyBackend::default());
    let services = AppServices::default().with_topology_backend(backend.clone());
    let capture = Rc::new(RefCell::new(None));
    let captured = capture.clone();
    let (root, cx) = cx.add_window_view(move |window, cx| {
        let dashboard = cx.new(|cx| RocketmqDashboard::with_services(window, services, cx));
        captured.replace(Some(dashboard.clone()));
        Root::new(dashboard, window, cx)
    });
    cx.run_until_parked();
    let dashboard = capture.borrow_mut().take().unwrap();
    cx.deactivate_window();
    cx.update(|window, app| {
        window.activate_window();
        dashboard.update(app, |dashboard, cx| dashboard.navigate(AppRoute::Topology, window, cx));
    });
    cx.run_until_parked();
    let topology = cx.read(|app| dashboard.read(app).legacy_pages.topology.clone());
    cx.update(|_, app| topology.update(app, |view, cx| {
        let mut snapshot = view.store.cache.snapshot().unwrap().clone();
        let layer = snapshot.layers.get_mut(&TopologySource::Topics).unwrap();
        for index in 0..16 {
            let label = format!("DLQ_native_fixture_orders_failed_messages_with_a_long_complete_identity_{index:02}_production_cluster");
            layer.nodes.push(TopologyNode::new(TopologyNodeId::Topic(label.clone()), label, TopologySource::Topics, snapshot.generated_at_ms));
        }
        layer.total = layer.nodes.len();
        view.store.apply(snapshot);
        cx.notify();
    }));
    let ids = cx.read(|app| {
        topology
            .read(app)
            .store
            .layout
            .nodes
            .iter()
            .map(|node| node.node.id.clone())
            .collect::<Vec<_>>()
    });
    assert_eq!(ids.len(), 26);
    let selected = ids[6].clone();
    let TopologyNodeId::Topic(full_identity) = &selected else {
        panic!("first long fixture Topic")
    };
    assert!(full_identity.starts_with("DLQ_native_fixture_"));
    let calls = backend.calls().len();
    for (width, height) in [(2560., 1440.), (1444., 931.), (960., 640.), (1444., 931.)] {
        let viewport = size(px(width), px(height));
        cx.simulate_resize(viewport);
        for _ in 0..3 {
            cx.draw(point(px(0.), px(0.)), viewport, |_, _| root.clone());
            cx.run_until_parked();
        }
        let content = cx.debug_bounds("page-content").unwrap();
        let description = cx.debug_bounds("page-description").unwrap();
        let refresh = cx.debug_bounds("topology-refresh").unwrap();
        assert!(
            description.size.width >= content.size.width - px(60.),
            "subtitle collapsed at {width}: {description:?}; content {content:?}"
        );
        assert!(
            description.size.height <= px(80.),
            "subtitle must wrap as readable lines at {width}: {description:?}"
        );
        assert!(
            refresh.top() >= description.bottom(),
            "page actions must follow the complete heading"
        );
        assert!(refresh.right() <= px(width));
        for selector in [
            "topology-cluster",
            "topology-all-clusters",
            "topology-sphere",
            "topology-node-picker",
        ] {
            let control = cx.debug_bounds(selector).unwrap();
            assert!(
                control.left() >= content.left() && control.right() <= content.right(),
                "responsive action escaped the page at {width}: {selector} {control:?}"
            );
            assert!(control.top() >= description.bottom());
        }
        let node = cx.debug_bounds("topology-node-6").unwrap();
        let label = cx.debug_bounds("topology-node-label-6").unwrap();
        assert!(
            label.left() >= node.left() && label.right() <= node.right(),
            "long label escapes its fixed node: {label:?}; {node:?}"
        );
        assert!(label.top() >= node.top() && label.bottom() <= node.bottom());
        if width == 2560. {
            cx.simulate_click(node.center(), Modifiers::default());
            cx.run_until_parked();
            assert!(cx.read(|app| {
                topology
                    .read(app)
                    .store
                    .detail
                    .iter()
                    .any(|line| line.contains(full_identity))
            }));
        }
    }
    assert_eq!(backend.calls().len(), calls, "resizing does not start another query");
    assert_eq!(
        cx.read(|app| topology
            .read(app)
            .store
            .layout
            .nodes
            .iter()
            .map(|node| node.node.id.clone())
            .collect::<Vec<_>>()),
        ids
    );
    assert_eq!(
        cx.read(|app| topology.read(app).store.selection.as_ref().unwrap().node.clone()),
        selected
    );
}
