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
    });
    assert!(runtime.shutdown_runtime_blocking().unwrap().is_healthy());
}
