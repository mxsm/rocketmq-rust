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

//! Real Broker inventory Table and typed Sheet navigation.

use gpui::{
    App, AppContext as _, Context, Entity, EventEmitter, Focusable as _, InteractiveElement as _, IntoElement,
    KeyBinding, ParentElement as _, Render, Styled as _, Subscription, Task, WeakEntity, Window, div, px,
};
use gpui_component::{
    ActiveTheme as _, Disableable as _, Sizable as _, WindowExt as _,
    button::Button,
    input::{Input, InputEvent, InputState},
    table::{Column, Table, TableDelegate, TableEvent, TableState},
};

gpui::actions!(brokers, [OpenSelectedBroker]);

pub fn init(cx: &mut App) {
    cx.bind_keys([KeyBinding::new("enter", OpenSelectedBroker, Some("BrokersTable"))]);
}
use rocketmq_dashboard_common::{BrokerInventoryItem, BrokerInventorySort, BrokerRole, EndpointAvailability, Observed};

use crate::{
    features::{
        broker_inspector::{BrokerInspector, BrokerInspectorIntent},
        brokers_store::{BrokerOpenIntent, BrokersStore},
    },
    route::{AppRoute, BrokerTab},
    services::{AppServices, brokers::BrokerCacheInvalidation},
    state::Loadable,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BrokersIntent {
    Navigate(AppRoute),
    ReplaceRoute(AppRoute),
    SheetClosed,
    ConfigApplied(Vec<BrokerCacheInvalidation>),
}

struct BrokerTableDelegate {
    columns: Vec<Column>,
    rows: Vec<BrokerInventoryItem>,
    loading: bool,
    owner: Option<WeakEntity<BrokersView>>,
}

impl BrokerTableDelegate {
    fn new() -> Self {
        Self {
            columns: vec![
                Column::new("cluster", "Cluster"),
                Column::new("broker", "Broker"),
                Column::new("id", "ID"),
                Column::new("address", "Address"),
                Column::new("role", "Role"),
                Column::new("version", "Version"),
                Column::new("availability", "Availability"),
                Column::new("produce-tps", "Produce TPS"),
                Column::new("consume-tps", "Consume TPS"),
                Column::new("actions", "Actions"),
            ],
            rows: Vec::new(),
            loading: true,
            owner: None,
        }
    }
}

impl TableDelegate for BrokerTableDelegate {
    fn columns_count(&self, _: &App) -> usize {
        self.columns.len()
    }

    fn rows_count(&self, _: &App) -> usize {
        self.rows.len()
    }

    fn column(&self, col_ix: usize, _: &App) -> &Column {
        &self.columns[col_ix]
    }

    fn loading(&self, _: &App) -> bool {
        self.loading
    }

    fn render_td(
        &mut self,
        row_ix: usize,
        col_ix: usize,
        _: &mut Window,
        _: &mut Context<TableState<Self>>,
    ) -> impl IntoElement {
        let item = &self.rows[row_ix];
        let value = match col_ix {
            0 => item.identity.cluster.clone(),
            1 => item.identity.broker_name.clone(),
            2 => item.identity.broker_id.to_string(),
            3 => item.identity.address.clone(),
            4 => match item.role {
                BrokerRole::Master => "Master".into(),
                BrokerRole::Slave => "Slave".into(),
                BrokerRole::Unknown => "Unknown".into(),
            },
            5 => observed_string(&item.version),
            6 => match item.availability {
                EndpointAvailability::Available => "Available".into(),
                EndpointAvailability::Unavailable => "Unavailable".into(),
                EndpointAvailability::Unknown => "Unknown".into(),
            },
            7 => observed_number(&item.produce_tps),
            8 => observed_number(&item.consume_tps),
            _ => {
                return div().px_2().child(
                    Button::new(("open-broker-row", row_ix))
                        .label("Open")
                        .small()
                        .outline()
                        .on_click({
                            let owner = self.owner.clone();
                            move |_, window, cx| {
                                if let Some(owner) = &owner {
                                    let _ = owner.update(cx, |view, cx| {
                                        view.open_row(row_ix, BrokerTab::Overview, window, cx);
                                    });
                                }
                            }
                        }),
                );
            }
        };
        div().px_2().text_sm().child(value)
    }

    fn render_empty(&mut self, _: &mut Window, cx: &mut Context<TableState<Self>>) -> impl IntoElement {
        div()
            .size_full()
            .flex()
            .items_center()
            .justify_center()
            .text_sm()
            .text_color(cx.theme().muted_foreground)
            .child("No Brokers match the current inventory filters.")
    }
}

pub struct BrokersView {
    services: AppServices,
    revision: u64,
    store: BrokersStore,
    keyword: Entity<InputState>,
    table: Entity<TableState<BrokerTableDelegate>>,
    inspector: Option<Entity<BrokerInspector>>,
    _subscriptions: [Subscription; 2],
    inspector_subscription: Option<Subscription>,
    pending_route: Option<(String, BrokerTab)>,
    suppress_sheet_closed: bool,
    inventory_task: Option<Task<()>>,
}

impl EventEmitter<BrokersIntent> for BrokersView {}

impl BrokersView {
    pub fn new(window: &mut Window, services: AppServices, revision: u64, cx: &mut Context<Self>) -> Self {
        let keyword = cx.new(|cx| InputState::new(window, cx).placeholder("Search cluster, Broker, or address"));
        let table = cx.new(|cx| {
            TableState::new(BrokerTableDelegate::new(), window, cx)
                .col_movable(false)
                .col_resizable(true)
                .sortable(false)
                .col_selectable(false)
                .row_selectable(true)
        });
        let subscriptions = [
            cx.subscribe_in(&keyword, window, |view, input, event: &InputEvent, _, cx| {
                if matches!(event, InputEvent::Change) {
                    view.store.filter.keyword = input.read(cx).value().to_string();
                    view.store.page = 0;
                    view.sync_table(cx);
                }
            }),
            cx.subscribe_in(&table, window, |view, _, event: &TableEvent, window, cx| {
                if let TableEvent::DoubleClickedRow(row) = event {
                    view.open_row(*row, BrokerTab::Overview, window, cx);
                }
            }),
        ];
        let mut view = Self {
            services,
            revision,
            store: BrokersStore::default(),
            keyword,
            table,
            inspector: None,
            _subscriptions: subscriptions,
            inspector_subscription: None,
            pending_route: None,
            suppress_sheet_closed: false,
            inventory_task: None,
        };
        let owner = cx.entity().downgrade();
        view.table
            .update(cx, |table, _| table.delegate_mut().owner = Some(owner));
        view.refresh_in(window, cx);
        view
    }

    pub fn set_revision(&mut self, revision: u64, cx: &mut Context<Self>) {
        let changed = revision != self.revision;
        self.revision = revision;
        if changed {
            self.store.invalidate();
            if let Some(inspector) = &self.inspector {
                inspector.update(cx, |inspector, cx| inspector.set_revision(revision, cx));
            }
        }
        self.refresh(cx);
    }

    pub fn refresh(&mut self, cx: &mut Context<Self>) {
        let Some(request) = self.store.begin_refresh(self.revision) else {
            return;
        };
        self.sync_table(cx);
        let services = self.services.clone();
        let revision = self.revision;
        self.inventory_task = Some(cx.spawn(async move |this, cx| {
            let result = services.broker_inventory(revision).await;
            let _ = this.update(cx, |view, cx| {
                view.store.finish_refresh(request, view.revision, result);
                view.sync_table(cx);
                if let (Some(selected), Some(inspector)) = (&view.store.selected, &view.inspector) {
                    inspector.update(cx, |inspector, cx| inspector.set_stale(selected.stale, cx));
                }
                cx.notify();
            });
        }));
    }

    fn refresh_in(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(request) = self.store.begin_refresh(self.revision) else {
            return;
        };
        self.sync_table(cx);
        let services = self.services.clone();
        let revision = self.revision;
        self.inventory_task = Some(cx.spawn_in(window, async move |this, cx| {
            let result = services.broker_inventory(revision).await;
            let _ = this.update_in(cx, |view, window, cx| {
                view.store.finish_refresh(request, view.revision, result);
                view.sync_table(cx);
                if let (Some(selected), Some(inspector)) = (&view.store.selected, &view.inspector) {
                    inspector.update(cx, |inspector, cx| {
                        inspector.set_inventory_stale(selected.stale, window, cx);
                    });
                }
                view.open_pending_route(window, cx);
                cx.notify();
            });
        }));
    }

    pub fn open_route(&mut self, address: &str, tab: BrokerTab, window: &mut Window, cx: &mut Context<Self>) {
        if let Some(item) = self.store.find_by_address(address) {
            self.open_identity(item, tab, false, window, cx);
        } else if matches!(
            self.store.inventory.state,
            Loadable::Idle | Loadable::InitialLoading | Loadable::Refreshing(_)
        ) {
            self.pending_route = Some((address.to_owned(), tab));
            self.refresh_in(window, cx);
        }
    }

    pub fn close_detail(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.pending_route = None;
        if window.has_active_sheet(cx) {
            self.suppress_sheet_closed = true;
            window.close_sheet(cx);
        }
    }

    fn open_pending_route(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some((address, tab)) = self.pending_route.take() else {
            return;
        };
        if let Some(item) = self.store.find_by_address(&address) {
            self.open_identity(item, tab, false, window, cx);
        }
    }

    fn sync_table(&mut self, cx: &mut Context<Self>) {
        let rows = self.store.visible_page();
        let loading = matches!(self.store.inventory.state, Loadable::InitialLoading);
        self.table.update(cx, |table, cx| {
            table.delegate_mut().rows = rows;
            table.delegate_mut().loading = loading;
            table.refresh(cx);
            cx.notify();
        });
    }

    fn open_selected(&mut self, tab: BrokerTab, window: &mut Window, cx: &mut Context<Self>) {
        let row = self.table.read(cx).selected_row().unwrap_or(0);
        self.open_row(row, tab, window, cx);
    }

    fn open_row(&mut self, row: usize, tab: BrokerTab, window: &mut Window, cx: &mut Context<Self>) {
        self.open_row_with_route(row, tab, true, window, cx);
    }

    fn open_row_with_route(
        &mut self,
        row: usize,
        tab: BrokerTab,
        emit_route: bool,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(item) = self.store.visible_page().get(row).cloned() else {
            return;
        };
        self.open_identity(item, tab, emit_route, window, cx);
    }

    fn open_identity(
        &mut self,
        item: BrokerInventoryItem,
        tab: BrokerTab,
        emit_route: bool,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Ok(intent) = self.store.select(item.identity, tab) else {
            return;
        };
        self.open_sheet(intent, emit_route, window, cx);
    }

    fn open_sheet(&mut self, intent: BrokerOpenIntent, emit_route: bool, window: &mut Window, cx: &mut Context<Self>) {
        self.table.focus_handle(cx).focus(window);
        if window.has_active_sheet(cx) {
            self.suppress_sheet_closed = true;
            window.close_sheet(cx);
        }
        let inspector = cx.new(|cx| {
            BrokerInspector::new(
                window,
                self.services.clone(),
                self.revision,
                intent.identity.clone(),
                intent.tab,
                cx,
            )
        });
        self.inspector_subscription = Some(cx.subscribe_in(
            &inspector,
            window,
            |view, _, event: &BrokerInspectorIntent, window, cx| match event {
                BrokerInspectorIntent::ConfigApplied(invalidations) => {
                    cx.emit(BrokersIntent::ConfigApplied(invalidations.clone()));
                    if invalidations.contains(&BrokerCacheInvalidation::BrokerInventory) {
                        view.refresh_in(window, cx);
                    }
                    if let Some(inspector) = &view.inspector {
                        inspector.update(cx, |inspector, cx| inspector.consume_invalidations(invalidations, cx));
                    }
                }
                BrokerInspectorIntent::NavigateTab(route) => cx.emit(BrokersIntent::ReplaceRoute(route.clone())),
            },
        ));
        self.inspector = Some(inspector.clone());
        let view = cx.entity().downgrade();
        let title = format!("{} · {}", intent.identity.broker_name, intent.identity.address);
        window.open_sheet(cx, move |sheet, _, _| {
            let view = view.clone();
            sheet
                .title(title.clone())
                .size(px(720.))
                .on_close(move |_, window, cx| {
                    let _ = view.update(cx, |view, cx| {
                        view.store.close_sheet();
                        view.inspector = None;
                        view.inspector_subscription = None;
                        if view.store.take_focus_restore() {
                            view.table.focus_handle(cx).focus(window);
                        }
                        let emit_close = !view.suppress_sheet_closed;
                        view.suppress_sheet_closed = false;
                        if emit_close {
                            cx.emit(BrokersIntent::SheetClosed);
                        }
                    });
                })
                .child(inspector.clone())
        });
        if emit_route {
            cx.emit(BrokersIntent::Navigate(intent.route));
        }
    }

    fn cycle_cluster(&mut self, cx: &mut Context<Self>) {
        let clusters = self
            .store
            .inventory
            .state
            .value()
            .into_iter()
            .flat_map(|items| items.iter().map(|item| item.identity.cluster.clone()))
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        self.store.filter.cluster = match &self.store.filter.cluster {
            None => clusters.first().cloned(),
            Some(current) => clusters
                .iter()
                .position(|cluster| cluster == current)
                .and_then(|index| clusters.get(index + 1).cloned()),
        };
        self.store.page = 0;
        self.sync_table(cx);
    }

    fn cycle_role(&mut self, cx: &mut Context<Self>) {
        self.store.filter.role = match self.store.filter.role {
            None => Some(BrokerRole::Master),
            Some(BrokerRole::Master) => Some(BrokerRole::Slave),
            Some(BrokerRole::Slave) | Some(BrokerRole::Unknown) => None,
        };
        self.store.page = 0;
        self.sync_table(cx);
    }

    fn cycle_sort(&mut self, cx: &mut Context<Self>) {
        self.store.sort = match self.store.sort {
            BrokerInventorySort::Identity => BrokerInventorySort::BrokerName,
            BrokerInventorySort::BrokerName => BrokerInventorySort::Role,
            BrokerInventorySort::Role => BrokerInventorySort::Availability,
            BrokerInventorySort::Availability => BrokerInventorySort::Identity,
        };
        self.sync_table(cx);
    }
}

impl Render for BrokersView {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let cluster = self
            .store
            .filter
            .cluster
            .clone()
            .unwrap_or_else(|| "All clusters".into());
        let role = match self.store.filter.role {
            None => "All roles",
            Some(BrokerRole::Master) => "Master",
            Some(BrokerRole::Slave) => "Slave",
            Some(BrokerRole::Unknown) => "Unknown",
        };
        let refreshing = matches!(self.store.inventory.state, Loadable::Refreshing(_));
        let initial_failure = match &self.store.inventory.state {
            Loadable::Failed { previous: None, error } => Some(error.summary().to_owned()),
            _ => None,
        };
        let inventory_body = if let Some(error) = initial_failure {
            crate::components::states::error_state(
                "Broker inventory unavailable",
                &error,
                cx.theme().foreground,
                cx.theme().muted_foreground,
                Some(cx.listener(|view, _, window, cx| view.refresh_in(window, cx))),
                None::<fn(&gpui::ClickEvent, &mut Window, &mut App)>,
            )
        } else {
            div()
                .flex_1()
                .min_h_0()
                .child(Table::new(&self.table).bordered(true).small())
        };
        div()
            .id("brokers-page")
            .key_context("BrokersTable")
            .on_action(cx.listener(|view, _: &OpenSelectedBroker, window, cx| {
                view.open_selected(BrokerTab::Overview, window, cx);
            }))
            .size_full()
            .px_6()
            .pb_6()
            .flex()
            .flex_col()
            .gap_3()
            .child(
                div()
                    .flex()
                    .flex_wrap()
                    .gap_2()
                    .child(div().flex_1().min_w(px(260.)).child(Input::new(&self.keyword)))
                    .child(
                        Button::new("filter-broker-cluster")
                            .label(cluster)
                            .outline()
                            .on_click(cx.listener(|view, _, _, cx| view.cycle_cluster(cx))),
                    )
                    .child(
                        Button::new("filter-broker-role")
                            .label(role)
                            .outline()
                            .on_click(cx.listener(|view, _, _, cx| view.cycle_role(cx))),
                    )
                    .child(
                        Button::new("sort-brokers")
                            .label("Sort")
                            .outline()
                            .on_click(cx.listener(|view, _, _, cx| view.cycle_sort(cx))),
                    )
                    .child(
                        Button::new("refresh-brokers")
                            .label(if refreshing { "Refreshing…" } else { "Refresh" })
                            .outline()
                            .disabled(refreshing)
                            .on_click(cx.listener(|view, _, window, cx| view.refresh_in(window, cx))),
                    ),
            )
            .child(inventory_body)
            .child(
                div()
                    .flex()
                    .justify_between()
                    .items_center()
                    .child(format!("Page {} of {}", self.store.page + 1, self.store.page_count()))
                    .child(
                        div()
                            .flex()
                            .gap_2()
                            .child(
                                Button::new("brokers-previous-page")
                                    .label("Previous")
                                    .outline()
                                    .disabled(self.store.page == 0)
                                    .on_click(cx.listener(|view, _, _, cx| {
                                        view.store.page = view.store.page.saturating_sub(1);
                                        view.sync_table(cx);
                                    })),
                            )
                            .child(
                                Button::new("brokers-next-page")
                                    .label("Next")
                                    .outline()
                                    .disabled(!self.store.can_advance_page())
                                    .on_click(cx.listener(|view, _, _, cx| {
                                        view.store.page = view.store.page.saturating_add(1);
                                        view.sync_table(cx);
                                    })),
                            ),
                    ),
            )
    }
}

fn observed_string(value: &Observed<String>) -> String {
    match value {
        Observed::Observed(value) => value.clone(),
        Observed::Unknown => "Unknown".into(),
    }
}

fn observed_number(value: &Observed<f64>) -> String {
    match value {
        Observed::Observed(value) => format!("{value:.2}"),
        Observed::Unknown => "—".into(),
    }
}

#[cfg(test)]
#[path = "brokers_tests.rs"]
mod tests;
