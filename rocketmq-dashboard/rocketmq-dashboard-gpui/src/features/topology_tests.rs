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
use crate::services::topology::test_support::FakeTopologyBackend;
use gpui::{Modifiers, point, size};
use gpui_component::Root;
use std::{
    cell::RefCell,
    rc::Rc,
    sync::{Arc, mpsc},
    time::Duration,
};
struct Harness {
    view: Entity<TopologyView>,
}
impl Render for Harness {
    fn render(&mut self, _: &mut Window, _: &mut Context<Self>) -> impl IntoElement {
        div().size_full().child(self.view.clone())
    }
}
fn open(
    cx: &mut gpui::TestAppContext,
    services: AppServices,
) -> (Entity<Root>, Entity<TopologyView>, &mut gpui::VisualTestContext) {
    cx.update(gpui_component::init);
    let capture = Rc::new(RefCell::new(None));
    let captured = capture.clone();
    let (root, cx) = cx.add_window_view(move |window, cx| {
        let view = cx.new(|cx| TopologyView::new(window, services, cx));
        view.update(cx, |view, cx| view.set_visible(true, window, cx));
        captured.replace(Some(view.clone()));
        let harness = cx.new(|_| Harness { view });
        Root::new(harness, window, cx)
    });
    let view = capture.borrow_mut().take().unwrap();
    cx.simulate_resize(size(px(1440.), px(900.)));
    cx.deactivate_window();
    cx.update(|window, _| window.activate_window());
    cx.run_until_parked();
    (root, view, cx)
}
fn draw(root: &Entity<Root>, cx: &mut gpui::VisualTestContext) {
    cx.run_until_parked();
    for _ in 0..2 {
        cx.draw(point(px(0.), px(0.)), size(px(1440.), px(900.)), |_, _| root.clone());
    }
}
fn click(cx: &mut gpui::VisualTestContext, selector: &'static str) {
    let bounds = cx
        .debug_bounds(selector)
        .unwrap_or_else(|| panic!("missing actual topology control {selector}"));
    cx.simulate_click(bounds.center(), Modifiers::default());
    cx.run_until_parked();
}
fn choose(cx: &mut gpui::VisualTestContext, selector: &'static str, search: &str) {
    click(cx, selector);
    cx.simulate_input(search);
    cx.run_until_parked();
    cx.simulate_keystrokes("down enter");
    cx.run_until_parked();
}
#[gpui::test]
fn topology_real_map_list_search_cluster_zoom_refresh_and_narrow_viewport_share_stable_selection(
    cx: &mut gpui::TestAppContext,
) {
    let backend = Arc::new(FakeTopologyBackend::default());
    let (root, view, cx) = open(cx, AppServices::default().with_topology_backend(backend.clone()));
    draw(&root, cx);
    assert_eq!(backend.calls().len(), 6);
    assert!(!cx.read(|app| view.read(app).store.layout.nodes.is_empty()));
    choose(cx, "topology-node-picker", "orders");
    let selected = cx.read(|app| view.read(app).store.selection.clone().unwrap());
    assert_eq!(selected.node, TopologyNodeId::Topic("orders".into()));
    let old_width = cx.read(|app| view.read(app).store.layout.width);
    click(cx, "topology-zoom-in");
    draw(&root, cx);
    assert!(cx.read(|app| view.read(app).store.layout.width) > old_width);
    click(cx, "topology-list");
    draw(&root, cx);
    assert!(cx.read(|app| view.read(app).presentation == Presentation::List));
    assert_eq!(
        cx.read(|app| view.read(app).store.selection.clone()),
        Some(selected.clone())
    );
    click(cx, "topology-refresh");
    draw(&root, cx);
    assert_eq!(cx.read(|app| view.read(app).store.selection.clone()), Some(selected));
    assert_eq!(backend.calls().len(), 12);
    choose(cx, "topology-cluster", "b");
    draw(&root, cx);
    cx.read(|app| {
        let store = &view.read(app).store;
        assert_eq!(store.cluster.as_deref(), Some("b"));
        assert!(store.selection.is_none());
        assert!(
            store
                .layout
                .nodes
                .iter()
                .all(|placed| !placed.node.clusters.contains("a"))
        );
        let visible = store
            .layout
            .nodes
            .iter()
            .map(|placed| &placed.node.id)
            .collect::<std::collections::BTreeSet<_>>();
        assert!(
            store
                .layout
                .edges
                .iter()
                .all(|edge| visible.contains(&edge.id.from) && visible.contains(&edge.id.to))
        );
    });
    click(cx, "topology-all-clusters");
    draw(&root, cx);
    assert!(cx.read(|app| {
        view.read(app)
            .store
            .layout
            .nodes
            .iter()
            .any(|placed| placed.node.id == TopologyNodeId::Topic("orders".into()))
    }));
    click(cx, "topology-map");
    draw(&root, cx);
    for (width, height) in [(930., 620.), (960., 640.)] {
        let viewport = size(px(width), px(height));
        cx.simulate_resize(viewport);
        for _ in 0..2 {
            cx.draw(point(px(0.), px(0.)), viewport, |_, _| root.clone());
        }
        let map = cx.debug_bounds("topology-viewport").unwrap();
        assert_eq!(map.size.height, px(400.));
        assert!(cx.debug_bounds("topology-node-picker").unwrap().size.width >= px(240.));
        assert!(cx.debug_bounds("topology-refresh").unwrap().bottom() <= px(height));
    }
}
fn loop_finished(completed: &mpsc::Receiver<&'static str>) {
    loop {
        if completed.recv_timeout(Duration::from_secs(5)).unwrap() == "gpui-topology-loop" {
            break;
        }
    }
}

#[gpui::test]
fn topology_sphere_shortcuts_follow_mode_and_empty_canvas_click_from_outside_focus(cx: &mut gpui::TestAppContext) {
    use rocketmq_dashboard_common::topology::sphere::SphereCamera;
    let backend = Arc::new(FakeTopologyBackend::default());
    let (root, view, cx) = open(cx, AppServices::default().with_topology_backend(backend.clone()));
    draw(&root, cx);
    let outside = cx.update(|_, app| app.focus_handle());
    cx.update(|window, _| outside.focus(window));
    click(cx, "topology-sphere");
    draw_sphere_size(&root, cx, 1440., 900.);
    cx.simulate_keystrokes("alt-right");
    cx.run_until_parked();
    let turned = cx.read(|app| view.read(app).store.sphere.camera);
    assert_ne!(
        turned,
        SphereCamera::default(),
        "choosing Sphere must provide the advertised shortcut focus"
    );
    cx.update(|window, _| outside.focus(window));
    let viewport = cx.debug_bounds("topology-viewport").unwrap();
    cx.simulate_click(viewport.origin + point(px(4.), px(4.)), Modifiers::default());
    cx.simulate_keystrokes("alt-left");
    cx.run_until_parked();
    assert_ne!(
        cx.read(|app| view.read(app).store.sphere.camera),
        turned,
        "clicking the canvas must restore camera shortcut focus"
    );
    assert_eq!(backend.calls().len(), 6);
}

fn draw_sphere_size(root: &Entity<Root>, cx: &mut gpui::VisualTestContext, width: f32, height: f32) {
    let viewport = size(px(width), px(height));
    cx.simulate_resize(viewport);
    for _ in 0..3 {
        cx.draw(point(px(0.), px(0.)), viewport, |_, _| root.clone());
        cx.run_until_parked();
    }
}

#[gpui::test]
fn topology_sphere_actual_camera_controls_node_click_and_keyboard_picker_share_the_map_selection(
    cx: &mut gpui::TestAppContext,
) {
    use rocketmq_dashboard_common::topology::sphere::SphereCamera;
    let backend = Arc::new(FakeTopologyBackend::default());
    let (root, view, cx) = open(cx, AppServices::default().with_topology_backend(backend.clone()));
    draw(&root, cx);
    choose(cx, "topology-node-picker", "orders");
    let selected = cx.read(|app| view.read(app).store.selection.clone().unwrap());
    let calls = backend.calls().len();
    click(cx, "topology-sphere");
    draw_sphere_size(&root, cx, 1440., 900.);
    let initial = cx.read(|app| view.read(app).store.sphere.projection.as_ref().unwrap().clone());
    assert_eq!(
        initial.nodes.len(),
        cx.read(|app| view.read(app).store.layout.nodes.len())
    );
    cx.simulate_keystrokes("alt-left");
    draw(&root, cx);
    assert_ne!(
        cx.read(|app| view.read(app).store.sphere.camera),
        SphereCamera::default(),
        "the active Sphere workspace must provide the equivalent camera keyboard path"
    );
    cx.simulate_keystrokes("alt-r");
    draw(&root, cx);
    assert_eq!(
        cx.read(|app| view.read(app).store.sphere.camera),
        SphereCamera::default()
    );
    click(cx, "topology-sphere-turn-1");
    draw(&root, cx);
    assert_ne!(
        initial,
        cx.read(|app| view.read(app).store.sphere.projection.as_ref().unwrap().clone())
    );
    let before_zoom = cx.read(|app| view.read(app).store.sphere.camera.scale());
    click(cx, "topology-zoom-in");
    assert!(cx.read(|app| view.read(app).store.sphere.camera.scale()) > before_zoom);
    click(cx, "topology-sphere-focus");
    draw(&root, cx);
    cx.read(|app| {
        let projection = view.read(app).store.sphere.projection.as_ref().unwrap();
        let selected_node = projection
            .nodes
            .iter()
            .find(|node| node.placed.node.id == selected.node)
            .unwrap();
        assert!(selected_node.front);
        assert_eq!(projection.labels[0].id, selected.node);
    });
    click(cx, "topology-sphere-reset");
    draw(&root, cx);
    assert_eq!(
        cx.read(|app| view.read(app).store.sphere.camera),
        SphereCamera::default()
    );
    assert_eq!(
        backend.calls().len(),
        calls,
        "camera controls must not collect another snapshot"
    );
    let clicked = cx.read(|app| {
        view.read(app).store.sphere.projection.as_ref().unwrap().nodes[0]
            .placed
            .node
            .id
            .clone()
    });
    click(cx, "topology-sphere-node-0");
    draw(&root, cx);
    assert_eq!(
        cx.read(|app| view.read(app).store.selection.as_ref().unwrap().node.clone()),
        clicked
    );
    click(cx, "topology-list");
    draw(&root, cx);
    assert_eq!(
        cx.read(|app| view.read(app).store.selection.as_ref().unwrap().node.clone()),
        clicked
    );
    choose(cx, "topology-node-picker", "orders");
    assert_eq!(cx.read(|app| view.read(app).store.selection.clone()), Some(selected));
    assert_eq!(backend.calls().len(), calls);
}

#[gpui::test]
fn topology_sphere_uses_actual_canvas_resize_and_recovers_from_map_fallback_without_changing_filter_or_selection(
    cx: &mut gpui::TestAppContext,
) {
    use rocketmq_dashboard_common::topology::sphere::SphereUnavailable;
    let backend = Arc::new(FakeTopologyBackend::default());
    let (root, view, cx) = open(cx, AppServices::default().with_topology_backend(backend.clone()));
    draw(&root, cx);
    choose(cx, "topology-cluster", "a");
    draw(&root, cx);
    choose(cx, "topology-node-picker", "orders");
    let selected = cx.read(|app| view.read(app).store.selection.clone().unwrap());
    let calls = backend.calls().len();
    click(cx, "topology-sphere");
    draw_sphere_size(&root, cx, 960., 640.);
    let wide = cx.read(|app| view.read(app).store.sphere.viewport);
    assert_eq!(
        wide.width,
        f32::from(cx.debug_bounds("topology-viewport").unwrap().size.width)
    );
    assert_eq!(wide.height, 400.);
    assert!(cx.read(|app| view.read(app).store.sphere.projection.is_ok()));
    draw_sphere_size(&root, cx, 440., 640.);
    cx.read(|app| {
        let view = view.read(app);
        assert_eq!(view.presentation, Presentation::Sphere);
        assert_eq!(
            view.store.sphere.projection.as_ref().unwrap_err(),
            &SphereUnavailable::SmallViewport
        );
        assert_eq!(view.store.selection.as_ref(), Some(&selected));
        assert_eq!(view.store.cluster.as_deref(), Some("a"));
    });
    assert!(cx.debug_bounds("topology-sphere-fallback").is_some());
    assert!(
        cx.debug_bounds("topology-node-0").is_some(),
        "fallback must render the actual 2D map"
    );
    let map_width = cx.read(|app| view.read(app).store.layout.width);
    let camera = cx.read(|app| view.read(app).store.sphere.camera);
    click(cx, "topology-zoom-in");
    draw_sphere_size(&root, cx, 440., 640.);
    assert!(cx.read(|app| view.read(app).store.layout.width) > map_width);
    assert_eq!(cx.read(|app| view.read(app).store.sphere.camera), camera);
    draw_sphere_size(&root, cx, 930., 620.);
    cx.read(|app| {
        let view = view.read(app);
        let projection = view.store.sphere.projection.as_ref().unwrap();
        assert_ne!(projection.viewport, wide);
        assert_eq!(view.store.selection.as_ref(), Some(&selected));
        assert_eq!(view.store.cluster.as_deref(), Some("a"));
        let map_ids = view
            .store
            .layout
            .nodes
            .iter()
            .map(|node| &node.node.id)
            .collect::<std::collections::BTreeSet<_>>();
        let sphere_ids = projection
            .nodes
            .iter()
            .map(|node| &node.placed.node.id)
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(map_ids, sphere_ids);
    });
    assert!(cx.debug_bounds("topology-sphere-surface").is_some());
    assert_eq!(
        backend.calls().len(),
        calls,
        "resize and fallback must not query another source"
    );
}

#[gpui::test]
fn topology_actual_page_leave_activation_and_context_clear_cancel_one_owned_refresh_loop(
    cx: &mut gpui::TestAppContext,
) {
    use rocketmq_runtime::{ProcessMemoryLimit, RuntimeConfig, RuntimeOwner};
    let runtime = RuntimeOwner::plan(RuntimeConfig::for_parallelism("topology-controls", 1))
        .unwrap()
        .with_memory_limit(ProcessMemoryLimit::configured(256 * 1024 * 1024).unwrap())
        .build()
        .unwrap();
    let backend = Arc::new(FakeTopologyBackend::default());
    let (started, _release, finished) = backend.hold(TopologySource::Brokers);
    let (completion, completed) = mpsc::channel();
    let services = AppServices::default()
        .with_topology_backend(backend.clone())
        .with_topology_runtime(runtime.root_context().component("application"))
        .with_runtime_completion(completion);
    let (root, view, cx) = open(cx, services);
    runtime.block_on(started).unwrap();
    cx.update(|window, app| view.update(app, |view, cx| view.set_visible(false, window, cx)));
    cx.run_until_parked();
    runtime.block_on(finished).unwrap();
    loop_finished(&completed);
    assert!(cx.read(|app| view.read(app).store.cache.snapshot().is_none()));
    cx.update(|window, app| view.update(app, |view, cx| view.set_visible(true, window, cx)));
    cx.run_until_parked();
    loop_finished(&completed);
    draw(&root, cx);
    assert!(cx.read(|app| view.read(app).store.cache.snapshot().is_some()));
    click(cx, "topology-sphere");
    draw_sphere_size(&root, cx, 1440., 900.);
    assert!(cx.read(|app| view.read(app).store.sphere.projection.is_ok()));
    let (started, _release, finished) = backend.hold(TopologySource::Brokers);
    click(cx, "topology-interval-1");
    runtime.block_on(started).unwrap();
    cx.deactivate_window();
    cx.run_until_parked();
    runtime.block_on(finished).unwrap();
    loop_finished(&completed);
    let before = backend
        .calls()
        .iter()
        .filter(|request| request.source == TopologySource::Brokers)
        .count();
    click(cx, "topology-interval-2");
    assert!(cx.read(|app| view.read(app).refresh == TopologyRefresh::TenSeconds));
    assert_eq!(
        backend
            .calls()
            .iter()
            .filter(|request| request.source == TopologySource::Brokers)
            .count(),
        before
    );
    let (started, _release, finished) = backend.hold(TopologySource::Brokers);
    cx.update(|window, _| window.activate_window());
    cx.run_until_parked();
    runtime.block_on(started).unwrap();
    assert_eq!(
        backend
            .calls()
            .iter()
            .filter(|request| request.source == TopologySource::Brokers)
            .count(),
        before + 1
    );
    cx.update(|window, app| view.update(app, |view, cx| view.clear_for_context(window, cx)));
    cx.run_until_parked();
    runtime.block_on(finished).unwrap();
    loop_finished(&completed);
    cx.read(|app| {
        let store = &view.read(app).store;
        assert!(store.cache.snapshot().is_none() && store.layout.edges.is_empty() && store.selection.is_none());
        assert!(store.sphere.scene.nodes.is_empty() && store.sphere.scene.edges.is_empty());
    });
    assert!(runtime.shutdown_runtime_blocking().unwrap().is_healthy());
}
