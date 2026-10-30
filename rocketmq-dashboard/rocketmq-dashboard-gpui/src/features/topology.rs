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

use crate::{
    components::{topology_map, topology_panels},
    features::topology_store::TopologyStore,
    route::AppRoute,
    services::{AppServices, topology::now_ms},
    state::UiError,
};
use gpui::{
    AppContext as _, Context, Entity, EventEmitter, InteractiveElement as _, IntoElement, ParentElement as _, Render,
    SharedString, Styled as _, Subscription, Task, Window, div, prelude::FluentBuilder as _, px,
};
use gpui_component::{
    ActiveTheme as _, Disableable as _,
    button::{Button, ButtonVariants as _},
    scroll::ScrollableElement as _,
    select::{SearchableVec, Select, SelectEvent, SelectItem, SelectState},
};
use rocketmq_dashboard_common::*;

#[derive(Clone)]
pub(crate) struct NodeChoice {
    id: TopologyNodeId,
    title: String,
}
impl SelectItem for NodeChoice {
    type Value = TopologyNodeId;
    fn title(&self) -> SharedString {
        self.title.clone().into()
    }
    fn value(&self) -> &Self::Value {
        &self.id
    }
}
#[derive(Clone)]
pub(crate) enum TopologyIntent {
    Navigate(AppRoute),
    Producer(String),
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum Presentation {
    Map,
    List,
}

pub(crate) struct TopologyView {
    services: AppServices,
    pub store: TopologyStore,
    cluster_picker: Entity<SelectState<SearchableVec<String>>>,
    node_picker: Entity<SelectState<SearchableVec<NodeChoice>>>,
    _subscriptions: Vec<Subscription>,
    loop_task: Option<Task<()>>,
    epoch: u64,
    visible: bool,
    active: bool,
    loading: bool,
    refresh: TopologyRefresh,
    presentation: Presentation,
    error: Option<UiError>,
}
impl EventEmitter<TopologyIntent> for TopologyView {}
impl TopologyView {
    pub fn new(window: &mut Window, services: AppServices, cx: &mut Context<Self>) -> Self {
        let cluster_picker =
            cx.new(|cx| SelectState::new(SearchableVec::new(Vec::<String>::new()), None, window, cx).searchable(true));
        let node_picker = cx.new(|cx| {
            SelectState::new(SearchableVec::new(Vec::<NodeChoice>::new()), None, window, cx).searchable(true)
        });
        let subscriptions = vec![
            cx.subscribe_in(&cluster_picker, window, |view, _, event, window, cx| {
                let SelectEvent::Confirm(cluster) = event;
                view.store.set_cluster(cluster.clone());
                view.sync_node_picker(window, cx);
                cx.notify();
            }),
            cx.subscribe_in(&node_picker, window, |view, _, event, _, cx| {
                if let SelectEvent::Confirm(Some(id)) = event {
                    view.store.select(id.clone());
                    cx.notify();
                }
            }),
            cx.observe_window_activation(window, |view, window, cx| {
                view.set_active(window.is_window_active(), window, cx);
            }),
        ];
        Self {
            services,
            store: TopologyStore::default(),
            cluster_picker,
            node_picker,
            _subscriptions: subscriptions,
            loop_task: None,
            epoch: 0,
            visible: false,
            active: window.is_window_active(),
            loading: false,
            refresh: TopologyRefresh::Manual,
            presentation: Presentation::Map,
            error: None,
        }
    }
    pub fn set_visible(&mut self, visible: bool, window: &mut Window, cx: &mut Context<Self>) {
        if self.visible == visible {
            if visible && self.active && self.loop_task.is_none() {
                self.restart(window, cx);
            }
            return;
        }
        self.visible = visible;
        self.restart(window, cx);
    }
    fn set_active(&mut self, active: bool, window: &mut Window, cx: &mut Context<Self>) {
        if self.active == active {
            return;
        }
        self.active = active;
        self.restart(window, cx);
    }
    pub fn clear_for_context(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.stop();
        self.visible = false;
        self.store.clear();
        self.error = None;
        self.cluster_picker.update(cx, |picker, cx| {
            picker.set_items(SearchableVec::new(Vec::new()), window, cx);
            picker.set_selected_index(None, window, cx);
        });
        self.sync_node_picker(window, cx);
        cx.notify();
    }
    fn stop(&mut self) {
        self.epoch = self.epoch.saturating_add(1);
        self.loop_task = None;
        self.loading = false;
    }
    fn restart(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.stop();
        if !self.visible || !self.active {
            cx.notify();
            return;
        }
        if let Ok(now) = now_ms() {
            self.store.expire(now);
        }
        self.loading = true;
        self.error = None;
        let epoch = self.epoch;
        let services = self.services.clone();
        let context = services.topology_context();
        let refresh = self.refresh;
        self.loop_task = Some(cx.spawn_in(window, async move |this, cx| {
            let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
            let receive = async {
                while let Some(result) = receiver.recv().await {
                    let accepted = this
                        .update_in(cx, |view, window, cx| {
                            if view.epoch != epoch
                                || !view.visible
                                || !view.active
                                || view.services.topology_context() != context
                            {
                                return false;
                            }
                            view.loading = false;
                            match result {
                                Ok(snapshot) => {
                                    view.store.apply(snapshot);
                                    view.error = None;
                                }
                                Err(error) => {
                                    if let Ok(now) = now_ms() {
                                        view.store.apply(TopologySnapshot {
                                            context: context.clone(),
                                            generated_at_ms: now,
                                            layers: TopologySource::ALL
                                                .into_iter()
                                                .map(|source| {
                                                    (
                                                        source,
                                                        TopologyLayer::failed(
                                                            source,
                                                            now,
                                                            TopologyFailureCode::Unavailable,
                                                        ),
                                                    )
                                                })
                                                .collect(),
                                        });
                                    }
                                    view.error = Some(error);
                                }
                            }
                            let items = view.store.clusters.clone();
                            let selected = view.store.cluster.clone();
                            view.cluster_picker.update(cx, |picker, cx| {
                                picker.set_items(SearchableVec::new(items), window, cx);
                                if let Some(selected) = selected {
                                    picker.set_selected_value(&selected, window, cx);
                                } else {
                                    picker.set_selected_index(None, window, cx);
                                }
                            });
                            view.sync_node_picker(window, cx);
                            cx.notify();
                            true
                        })
                        .unwrap_or(false);
                    if !accepted {
                        receiver.close();
                        break;
                    }
                }
            };
            let watch = services.watch_topology(context.clone(), refresh, sender);
            let (result, _) = tokio::join!(watch, receive);
            if let Err(error) = result {
                let _ = this.update(cx, |view, cx| {
                    if view.epoch == epoch {
                        view.loading = false;
                        view.error = Some(error);
                        cx.notify();
                    }
                });
            }
        }));
        cx.notify();
    }
    fn sync_node_picker(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let items: Vec<_> = self
            .store
            .layout
            .nodes
            .iter()
            .map(|node| NodeChoice {
                id: node.node.id.clone(),
                title: format!("{} · {}", node.node.id.kind_label(), node.node.label),
            })
            .collect();
        let selected = self.store.selection.as_ref().map(|selection| selection.node.clone());
        self.node_picker.update(cx, |picker, cx| {
            picker.set_items(SearchableVec::new(items), window, cx);
            if let Some(selected) = selected {
                picker.set_selected_value(&selected, window, cx);
            } else {
                picker.set_selected_index(None, window, cx);
            }
        });
    }
    fn select(&mut self, id: TopologyNodeId, window: &mut Window, cx: &mut Context<Self>) {
        self.store.select(id);
        self.sync_node_picker(window, cx);
        cx.notify();
    }
    fn open_selected(&mut self, cx: &mut Context<Self>) {
        if let Some(TopologySelection {
            node: TopologyNodeId::Producer(group),
            ..
        }) = &self.store.selection
        {
            cx.emit(TopologyIntent::Producer(group.clone()));
        } else if let Some(route) = &self.store.route {
            cx.emit(TopologyIntent::Navigate(route.clone()));
        }
    }
}

impl Render for TopologyView {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let unavailable = self
            .store
            .cache
            .snapshot()
            .is_some_and(TopologySnapshot::all_remote_failed);
        let layout = &self.store.layout;
        let map = topology_map::board(layout.width, layout.height)
            .child(topology_map::links(layout, cx.theme().border))
            .children(layout.nodes.iter().enumerate().map(|(index, placed)| {
                let id = placed.node.id.clone();
                let selected = self
                    .store
                    .selection
                    .as_ref()
                    .is_some_and(|selection| selection.node == id);
                div()
                    .absolute()
                    .left(px(placed.bounds.x))
                    .top(px(placed.bounds.y))
                    .w(px(placed.bounds.width))
                    .h(px(placed.bounds.height))
                    .child(
                        Button::new(SharedString::from(format!("topology-map:{id:?}")))
                            .label(format!(
                                "{} · {}{}",
                                id.kind_label(),
                                placed.node.label,
                                if placed.stale { " · cached" } else { "" }
                            ))
                            .tooltip(placed.node.label.clone())
                            .outline()
                            .when(selected, |button| button.primary())
                            .w_full()
                            .h_full()
                            .debug_selector(move || format!("topology-node-{index}"))
                            .on_click(cx.listener(move |view, _, window, cx| view.select(id.clone(), window, cx))),
                    )
            }));
        let list = div()
            .w_full()
            .flex()
            .flex_col()
            .gap_2()
            .children(layout.nodes.iter().enumerate().map(|(index, placed)| {
                let id = placed.node.id.clone();
                let selected = self
                    .store
                    .selection
                    .as_ref()
                    .is_some_and(|selection| selection.node == id);
                Button::new(SharedString::from(format!("topology-list:{id:?}")))
                    .label(format!(
                        "{} · {}{}",
                        id.kind_label(),
                        placed.node.label,
                        if placed.stale { " · cached" } else { "" }
                    ))
                    .tooltip(placed.node.label.clone())
                    .outline()
                    .when(selected, |button| button.primary())
                    .w_full()
                    .debug_selector(move || format!("topology-list-node-{index}"))
                    .on_click(cx.listener(move |view, _, window, cx| view.select(id.clone(), window, cx)))
            }));
        div().id("topology-workspace").debug_selector(|| "topology-workspace".into()).size_full().overflow_y_scrollbar().child(div().w_full().p_4().flex().flex_col().gap_3()
            .child(div().flex().flex_wrap().gap_2().items_center()
                .child(Button::new("topology-refresh").label(if self.loading { "Refresh again" } else { "Refresh" }).outline().debug_selector(|| "topology-refresh".into()).on_click(cx.listener(|view, _, window, cx| view.restart(window, cx))))
                .children([(TopologyRefresh::Manual, "Manual"), (TopologyRefresh::FiveSeconds, "5s"), (TopologyRefresh::TenSeconds, "10s"), (TopologyRefresh::ThirtySeconds, "30s")].into_iter().enumerate().map(|(index, (mode, label))| Button::new(("topology-interval", index)).label(label).outline().when(self.refresh == mode, |button| button.primary()).debug_selector(move || format!("topology-interval-{index}")).on_click(cx.listener(move |view, _, window, cx| { view.refresh = mode; view.restart(window, cx); }))))
                .child(div().w(px(240.)).debug_selector(|| "topology-cluster".into()).child(Select::new(&self.cluster_picker).placeholder("All clusters")))
                .child(Button::new("topology-all-clusters").label("All clusters").outline().debug_selector(|| "topology-all-clusters".into()).on_click(cx.listener(|view, _, window, cx| { view.store.set_cluster(None); view.cluster_picker.update(cx, |picker, cx| picker.set_selected_index(None, window, cx)); view.sync_node_picker(window, cx); cx.notify(); }))))
            .child(div().text_xs().child(if !self.visible || !self.active { "Paused while the page or application is inactive." } else if self.refresh == TopologyRefresh::Manual { "Manual refresh · Read-only observations · Unknown values remain unknown." } else { "Automatic refresh waits for the previous read to finish. The loop pauses off-page and while inactive." }))
            .when(unavailable, |this| this.child(div().text_color(cx.theme().danger).child("Operational topology sources are unavailable. Retained observations keep their original time; no healthy empty topology is inferred.")))
            .child(div().text_xs().child("Cluster filters the display. Counts and traffic describe the captured environment-wide reads."))
            .children(self.error.as_ref().map(|error| div().text_color(cx.theme().danger).child(error.summary().to_string())))
            .child(div().flex().flex_wrap().gap_2()
                .child(Button::new("topology-map").label("2D map").outline().debug_selector(|| "topology-map".into()).on_click(cx.listener(|view, _, _, cx| { view.presentation = Presentation::Map; cx.notify(); })))
                .child(Button::new("topology-list").label("Keyboard list").outline().debug_selector(|| "topology-list".into()).on_click(cx.listener(|view, _, _, cx| { view.presentation = Presentation::List; cx.notify(); })))
                .child(Button::new("topology-zoom-out").label("Zoom −").outline().debug_selector(|| "topology-zoom-out".into()).on_click(cx.listener(|view, _, _, cx| { view.store.zoom(false); cx.notify(); })))
                .child(Button::new("topology-zoom-in").label("Zoom +").outline().debug_selector(|| "topology-zoom-in".into()).on_click(cx.listener(|view, _, _, cx| { view.store.zoom(true); cx.notify(); })))
                .child(div().w(px(300.)).debug_selector(|| "topology-node-picker".into()).child(Select::new(&self.node_picker).placeholder("Search and select a node"))))
            .child(div().w_full().h(px(400.)).flex_shrink_0().debug_selector(|| "topology-viewport".into()).child(div().size_full().overflow_scrollbar().child(if self.presentation == Presentation::Map { map.into_any_element() } else { list.into_any_element() })))
            .child(topology_panels::lines("Selected observation", &self.store.detail))
            .child(Button::new("topology-open-entity").label("Open entity").outline().disabled(self.store.route.is_none()).debug_selector(|| "topology-open-entity".into()).on_click(cx.listener(|view, _, _, cx| view.open_selected(cx))))
            .child(topology_panels::lines("Sources and observation times", &self.store.sources))
            .child(topology_panels::lines("Node changes", &self.store.changes))
            .child(topology_panels::lines("Observed traffic (msg/s, messages)", &self.store.traffic))
        )
    }
}

#[cfg(test)]
#[path = "topology_tests.rs"]
mod tests;
