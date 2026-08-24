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

//! Truthful Consumer inventory table and one bounded route-driven Sheet.

use gpui::prelude::FluentBuilder as _;
use gpui::{
    App, AppContext as _, Context, Entity, EventEmitter, Focusable as _, InteractiveElement as _, IntoElement,
    ParentElement as _, Render, Styled as _, Subscription, Task, WeakEntity, Window, div, px,
};
use gpui_component::{
    ActiveTheme as _, Disableable as _, Sizable as _, WindowExt as _,
    button::Button,
    input::{Input, InputEvent, InputState},
    table::{Column, Table, TableDelegate, TableEvent, TableState},
};
use rocketmq_dashboard_common::{
    CapabilityAvailability, ConsumerAclClassification, ConsumerCategory, ConsumerConfigEntries,
    ConsumerConnectionFilter, ConsumerConnectionState, ConsumerCreateCommand, ConsumerGroupObservation,
    ConsumerIdentity, ConsumerObservation, ConsumerObservationState, ConsumerPartialOutcome, ConsumerSortKey,
};

use crate::{
    features::{
        consumer_detail::{ConsumerDetail, ConsumerDetailIntent},
        consumers_store::ConsumersStore,
    },
    route::{AppRoute, ConsumerTab, RouteKey},
    services::{
        AppServices,
        consumers::{ConsumerMutationResult, ConsumerRequestScope},
    },
    state::Loadable,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConsumersIntent {
    Navigate(AppRoute),
    ReplaceRoute(AppRoute),
    SheetClosed,
}

struct ConsumerTableDelegate {
    columns: Vec<Column>,
    rows: Vec<ConsumerGroupObservation>,
    loading: bool,
    owner: Option<WeakEntity<ConsumersView>>,
}

impl ConsumerTableDelegate {
    fn new() -> Self {
        Self {
            columns: vec![
                Column::new("group", "Consumer group"),
                Column::new("category", "Category"),
                Column::new("connection", "Connection"),
                Column::new("clients", "Clients"),
                Column::new("lag", "Lag"),
                Column::new("consume-type", "Consume type"),
                Column::new("message-model", "Message model"),
                Column::new("actions", "Actions"),
            ],
            rows: Vec::new(),
            loading: true,
            owner: None,
        }
    }
}

impl TableDelegate for ConsumerTableDelegate {
    fn columns_count(&self, _: &App) -> usize {
        self.columns.len()
    }

    fn rows_count(&self, _: &App) -> usize {
        self.rows.len()
    }

    fn column(&self, index: usize, _: &App) -> &Column {
        &self.columns[index]
    }

    fn loading(&self, _: &App) -> bool {
        self.loading
    }

    fn render_td(
        &mut self,
        row: usize,
        column: usize,
        _: &mut Window,
        _: &mut Context<TableState<Self>>,
    ) -> impl IntoElement {
        let item = &self.rows[row];
        let value = match column {
            0 => item.identity.as_str().to_owned(),
            1 => category_label(item.category).into(),
            2 => connection_label(&item.connection_state),
            3 => observed_value(&item.client_count),
            4 => observed_value(&item.lag),
            5 => observed_value(&item.consume_type),
            6 => observed_value(&item.message_model),
            _ => {
                return div().px_2().child(
                    Button::new(("open-consumer-row", row))
                        .label("Open")
                        .small()
                        .outline()
                        .on_click({
                            let owner = self.owner.clone();
                            move |_, window, cx| {
                                if let Some(owner) = &owner {
                                    let _ = owner.update(cx, |view, cx| {
                                        view.open_row(row, ConsumerTab::Overview, true, window, cx);
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
            .child("No Consumer groups match the applied filters.")
    }
}

pub struct ConsumersView {
    services: AppServices,
    revision: u64,
    pub store: ConsumersStore,
    keyword: Entity<InputState>,
    create_group: Entity<InputState>,
    table: Entity<TableState<ConsumerTableDelegate>>,
    detail: Option<Entity<ConsumerDetail>>,
    detail_subscription: Option<Subscription>,
    _subscriptions: Vec<Subscription>,
    inventory_task: Option<Task<()>>,
    mutation_task: Option<Task<()>>,
    mutation_epoch: u64,
    mutation_status: Option<String>,
    mutation_outcome: Option<ConsumerPartialOutcome>,
    mutation_replay_blocked: bool,
    pending_route: Option<(String, ConsumerTab)>,
    suppress_sheet_closed: bool,
}

impl EventEmitter<ConsumersIntent> for ConsumersView {}

impl ConsumersView {
    pub fn new(window: &mut Window, services: AppServices, revision: u64, cx: &mut Context<Self>) -> Self {
        let keyword = cx.new(|cx| InputState::new(window, cx).placeholder("Search Consumer group"));
        let create_group = cx.new(|cx| InputState::new(window, cx).placeholder("New Consumer group"));
        let table = cx.new(|cx| {
            TableState::new(ConsumerTableDelegate::new(), window, cx)
                .col_movable(false)
                .col_resizable(true)
                .sortable(false)
                .col_selectable(false)
                .row_selectable(true)
        });
        let subscriptions = vec![
            cx.subscribe_in(&keyword, window, |view, input, event: &InputEvent, _, cx| {
                if matches!(event, InputEvent::Change) {
                    view.store.draft_filter.keyword = input.read(cx).value().to_string();
                    cx.notify();
                }
            }),
            cx.subscribe_in(&table, window, |view, _, event: &TableEvent, window, cx| {
                if let TableEvent::DoubleClickedRow(row) = event {
                    view.open_row(*row, ConsumerTab::Overview, true, window, cx);
                }
            }),
            cx.subscribe_in(&create_group, window, |view, _, event: &InputEvent, _, cx| {
                if matches!(event, InputEvent::Change) {
                    view.mutation_replay_blocked = false;
                    view.mutation_outcome = None;
                    cx.notify();
                }
            }),
        ];
        let view = Self {
            services,
            revision,
            store: ConsumersStore::default(),
            keyword,
            create_group,
            table,
            detail: None,
            detail_subscription: None,
            _subscriptions: subscriptions,
            inventory_task: None,
            mutation_task: None,
            mutation_epoch: 0,
            mutation_status: None,
            mutation_outcome: None,
            mutation_replay_blocked: false,
            pending_route: None,
            suppress_sheet_closed: false,
        };
        let owner = cx.entity().downgrade();
        view.table
            .update(cx, |table, _| table.delegate_mut().owner = Some(owner));
        view
    }

    #[cfg(test)]
    pub(crate) fn create_group_for_test(&self) -> Entity<InputState> {
        self.create_group.clone()
    }

    #[cfg(test)]
    pub(crate) fn detail_for_test(&self) -> Option<Entity<ConsumerDetail>> {
        self.detail.clone()
    }

    #[cfg(test)]
    pub(crate) fn mutation_status_for_test(&self) -> Option<&str> {
        self.mutation_status.as_deref()
    }

    pub fn set_revision(&mut self, revision: u64, cx: &mut Context<Self>) {
        let loaded = !matches!(self.store.inventory.state, Loadable::Idle);
        if revision != self.revision {
            self.revision = revision;
            self.store.clear_for_revision();
            if let Some(detail) = &self.detail {
                detail.update(cx, |detail, cx| detail.set_revision(revision, cx));
            }
        }
        if loaded {
            self.refresh(cx);
        }
    }

    pub fn ensure_loaded(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if matches!(self.store.inventory.state, Loadable::Idle) {
            self.refresh_in(window, cx);
        }
    }

    pub fn refresh(&mut self, cx: &mut Context<Self>) {
        let Some(request) = self.store.begin_inventory(self.revision) else {
            return;
        };
        self.sync_table(cx);
        let services = self.services.clone();
        self.inventory_task = Some(cx.spawn(async move |this, cx| {
            let result = services.consumer_inventory(request.scope).await;
            let _ = this.update(cx, |view, cx| {
                view.store.finish_inventory(request, view.revision, result);
                view.sync_table(cx);
                cx.notify();
            });
        }));
    }

    fn refresh_in(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(request) = self.store.begin_inventory(self.revision) else {
            return;
        };
        self.sync_table(cx);
        let services = self.services.clone();
        self.inventory_task = Some(cx.spawn_in(window, async move |this, cx| {
            let result = services.consumer_inventory(request.scope).await;
            let _ = this.update_in(cx, |view, window, cx| {
                view.store.finish_inventory(request, view.revision, result);
                view.sync_table(cx);
                view.open_pending_route(window, cx);
                cx.notify();
            });
        }));
    }

    pub fn open_route(&mut self, group: &str, tab: ConsumerTab, window: &mut Window, cx: &mut Context<Self>) {
        let Ok(identity) = ConsumerIdentity::parse(group.to_owned()) else {
            return;
        };
        if self.find(&identity).is_some() {
            self.open_group(identity, tab, false, window, cx);
        } else {
            self.pending_route = Some((group.to_owned(), tab));
            if matches!(self.store.inventory.state, Loadable::Idle | Loadable::InitialLoading) {
                self.refresh_in(window, cx);
            }
        }
    }

    pub fn close_detail(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.pending_route = None;
        if self.detail.is_some() && window.has_active_sheet(cx) {
            self.suppress_sheet_closed = true;
            window.close_sheet(cx);
        } else if self.detail.take().is_some() {
            self.detail_subscription = None;
            self.store.close_detail();
            if self.store.take_focus_restore() {
                self.table.focus_handle(cx).focus(window);
            }
            cx.notify();
        }
    }

    fn open_pending_route(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some((group, tab)) = self.pending_route.take() else {
            return;
        };
        let Ok(identity) = ConsumerIdentity::parse(group) else {
            return;
        };
        if self.find(&identity).is_some() {
            self.open_group(identity, tab, false, window, cx);
        }
    }

    fn find(&self, identity: &ConsumerIdentity) -> Option<&ConsumerGroupObservation> {
        self.store
            .inventory
            .state
            .value()
            .and_then(|inventory| inventory.groups.iter().find(|item| &item.identity == identity))
    }

    fn open_row(
        &mut self,
        row: usize,
        tab: ConsumerTab,
        emit_route: bool,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(group) = self.store.page().items.get(row).map(|item| item.identity.clone()) else {
            return;
        };
        self.open_group(group, tab, emit_route, window, cx);
    }

    fn open_group(
        &mut self,
        group: ConsumerIdentity,
        tab: ConsumerTab,
        emit_route: bool,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(group_observation) = self.find(&group) else {
            return;
        };
        let targets = group_observation.targets.clone();
        let Some(capabilities) = self
            .store
            .inventory
            .state
            .value()
            .map(|inventory| inventory.capabilities)
        else {
            return;
        };
        self.store.open_route(group.clone(), tab);
        self.table.focus_handle(cx).focus(window);
        if window.has_active_sheet(cx) {
            self.suppress_sheet_closed = true;
            window.close_sheet(cx);
        }
        let detail = cx.new(|cx| {
            ConsumerDetail::new(
                self.services.clone(),
                self.revision,
                group.clone(),
                tab,
                targets,
                capabilities,
                cx,
            )
        });
        self.detail_subscription = Some(cx.subscribe_in(
            &detail,
            window,
            |view, _, event: &ConsumerDetailIntent, window, cx| match event {
                ConsumerDetailIntent::ReplaceRoute(route) => cx.emit(ConsumersIntent::ReplaceRoute(route.clone())),
                ConsumerDetailIntent::InventoryReloaded(inventory) => {
                    view.store.inventory.replace(inventory.clone());
                    view.sync_table(cx);
                    cx.notify();
                }
                ConsumerDetailIntent::Deleted(inventory) => {
                    view.store.inventory.replace(inventory.clone());
                    view.sync_table(cx);
                    window.close_sheet(cx);
                }
            },
        ));
        self.detail = Some(detail.clone());
        let owner = cx.entity().downgrade();
        let title = group.as_str().to_owned();
        window.open_sheet(cx, move |sheet, _, _| {
            let owner = owner.clone();
            sheet
                .title(title.clone())
                .size(px(800.))
                .on_close(move |_, window, cx| {
                    let _ = owner.update(cx, |view, cx| {
                        view.store.close_detail();
                        view.detail = None;
                        view.detail_subscription = None;
                        if view.store.take_focus_restore() {
                            view.table.focus_handle(cx).focus(window);
                        }
                        let emit = !view.suppress_sheet_closed;
                        view.suppress_sheet_closed = false;
                        if emit {
                            cx.emit(ConsumersIntent::SheetClosed);
                        }
                    });
                })
                .child(detail.clone())
        });
        if emit_route && let Ok(group) = RouteKey::parse(group.as_str()) {
            cx.emit(ConsumersIntent::Navigate(AppRoute::ConsumerDetail { group, tab }));
        }
    }

    fn create_ready(&self) -> bool {
        self.store.inventory.state.value().is_some_and(|inventory| {
            inventory.capabilities.create == CapabilityAvailability::Available
                && inventory.observation == ConsumerObservationState::Complete
                && inventory.failures.is_empty()
                && !inventory.targets.is_empty()
        }) && self.mutation_task.is_none()
            && !self.mutation_replay_blocked
    }

    fn submit_create(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if !self.create_ready() {
            self.mutation_status =
                Some("Create requires a complete authoritative target inventory and Direct capability.".into());
            cx.notify();
            return;
        }
        let value = self.create_group.read(cx).value().to_string();
        let Ok(group) = ConsumerIdentity::parse(value) else {
            self.mutation_status = Some("Enter a valid non-empty Consumer group.".into());
            cx.notify();
            return;
        };
        let Some(targets) = self
            .store
            .inventory
            .state
            .value()
            .map(|inventory| inventory.targets.clone())
        else {
            return;
        };
        self.mutation_epoch = self.mutation_epoch.saturating_add(1);
        let scope = ConsumerRequestScope {
            revision: self.revision,
            epoch: self.mutation_epoch,
        };
        let command = ConsumerCreateCommand {
            group,
            targets,
            entries: ConsumerConfigEntries {
                retry_max_times: 16,
                retry_queue_nums: 1,
                consume_timeout_minutes: 15,
            },
            authorization: ConsumerAclClassification::Authorized,
        };
        let services = self.services.clone();
        self.mutation_status = Some("Verifying all-target absence before any write…".into());
        self.mutation_outcome = None;
        self.mutation_task = Some(cx.spawn_in(window, async move |this, cx| {
            let result = services.create_consumer(scope, command).await;
            let _ = this.update_in(cx, |view, window, cx| {
                view.mutation_task = None;
                match result {
                    Ok(ConsumerMutationResult::Rejected(outcome)) => {
                        view.mutation_replay_blocked = true;
                        view.mutation_outcome = Some(outcome.clone());
                        view.mutation_status = Some(format!(
                            "Create rejected with zero accepted writes (Partial: {}/{} applied).",
                            outcome.applied_count(),
                            outcome.targets.len()
                        ));
                    }
                    Ok(ConsumerMutationResult::Applied {
                        outcome,
                        inventory,
                        invalidations,
                    }) => {
                        view.mutation_replay_blocked = false;
                        view.mutation_outcome = Some(outcome.clone());
                        view.observe_invalidations(&invalidations);
                        view.store.inventory.replace(inventory);
                        view.create_group
                            .update(cx, |input, cx| input.set_value(String::new(), window, cx));
                        view.mutation_status = Some(format!(
                            "Created on {} target(s); authoritative inventory reloaded.",
                            outcome.applied_count()
                        ));
                        view.sync_table(cx);
                    }
                    Ok(ConsumerMutationResult::AppliedReloadFailed {
                        outcome,
                        invalidations,
                        error,
                    }) => {
                        view.mutation_replay_blocked = true;
                        view.mutation_outcome = Some(outcome.clone());
                        view.observe_invalidations(&invalidations);
                        view.create_group
                            .update(cx, |input, cx| input.set_value(String::new(), window, cx));
                        view.mutation_status = Some(format!(
                            "Create applied on {} target(s), but authoritative reload failed: {} Draft cleared; refresh and do not replay blindly.",
                            outcome.applied_count(),
                            error.summary()
                        ));
                    }
                    Err(error) => {
                        view.mutation_replay_blocked = true;
                        view.mutation_status = Some(format!(
                            "{} Command state is unknown; change the draft before a new command.",
                            error.summary()
                        ));
                    }
                }
                cx.notify();
            });
        }));
    }

    fn observe_invalidations(&mut self, invalidations: &[crate::services::consumers::ConsumerCacheInvalidation]) {
        for invalidation in invalidations {
            match invalidation {
                crate::services::consumers::ConsumerCacheInvalidation::Inventory
                | crate::services::consumers::ConsumerCacheInvalidation::Dashboard
                | crate::services::consumers::ConsumerCacheInvalidation::TopicConsumers => {}
                crate::services::consumers::ConsumerCacheInvalidation::Overview(group)
                | crate::services::consumers::ConsumerCacheInvalidation::Progress(group) => {
                    if self.store.detail.as_ref().is_some_and(|detail| &detail.group == group) {
                        self.store.detail = None;
                    }
                }
            }
        }
    }

    fn sync_table(&mut self, cx: &mut Context<Self>) {
        let page = self.store.page();
        let loading = matches!(self.store.inventory.state, Loadable::InitialLoading);
        self.table.update(cx, |table, cx| {
            table.delegate_mut().rows = page.items;
            table.delegate_mut().loading = loading;
            table.refresh(cx);
            cx.notify();
        });
    }

    fn apply_filter(&mut self, cx: &mut Context<Self>) {
        self.store.apply_filter();
        self.sync_table(cx);
        cx.notify();
    }

    fn cycle_connection_filter(&mut self, cx: &mut Context<Self>) {
        self.store.draft_filter.connection = match self.store.draft_filter.connection {
            None => Some(ConsumerConnectionFilter::Connected),
            Some(ConsumerConnectionFilter::Connected) => Some(ConsumerConnectionFilter::Disconnected),
            Some(ConsumerConnectionFilter::Disconnected) => Some(ConsumerConnectionFilter::Unknown),
            Some(ConsumerConnectionFilter::Unknown) => None,
        };
        cx.notify();
    }

    fn cycle_sort(&mut self, cx: &mut Context<Self>) {
        self.store.sort.key = match self.store.sort.key {
            ConsumerSortKey::Group => ConsumerSortKey::Clients,
            ConsumerSortKey::Clients => ConsumerSortKey::Lag,
            ConsumerSortKey::Lag => ConsumerSortKey::ConsumeType,
            ConsumerSortKey::ConsumeType => ConsumerSortKey::Group,
        };
        self.sync_table(cx);
        cx.notify();
    }
}

impl Render for ConsumersView {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let page = self.store.page();
        let partial = self
            .store
            .inventory
            .state
            .value()
            .is_some_and(|inventory| inventory.observation == ConsumerObservationState::Partial);
        let initial_error = match &self.store.inventory.state {
            Loadable::Failed { previous: None, error } => Some(error.summary().to_owned()),
            _ => None,
        };
        let connection = match self.store.draft_filter.connection {
            None => "All connections",
            Some(ConsumerConnectionFilter::Connected) => "Connected",
            Some(ConsumerConnectionFilter::Disconnected) => "Disconnected",
            Some(ConsumerConnectionFilter::Unknown) => "Unknown",
        };
        div()
            .id("consumers-page")
            .size_full()
            .px_6()
            .pb_6()
            .flex()
            .flex_col()
            .gap_3()
            .when(partial, |this| {
                this.child(
                    div()
                        .p_3()
                        .rounded_md()
                        .bg(cx.theme().warning.opacity(0.12))
                        .child("Partial Consumer inventory — failed targets are not inferred as zero or offline."),
                )
            })
            .when_some(self.mutation_status.clone(), |this, status| {
                this.child(div().p_3().rounded_md().bg(cx.theme().muted).child(status))
            })
            .when_some(self.mutation_outcome.clone(), |this, outcome| {
                this.child(render_consumer_outcome(&outcome, cx))
            })
            .when(
                self.store
                    .inventory
                    .state
                    .value()
                    .is_some_and(|inventory| inventory.capabilities.create == CapabilityAvailability::Available),
                |this| {
                    this.child(
                        div()
                            .flex()
                            .gap_2()
                            .child(Input::new(&self.create_group).w(px(280.)))
                            .child(
                                Button::new("consumer-create")
                                    .label("Create on all targets")
                                    .disabled(!self.create_ready())
                                    .debug_selector(|| "consumer-create".to_owned())
                                    .on_click(cx.listener(|view, _, window, cx| view.submit_create(window, cx))),
                            ),
                    )
                },
            )
            .child(
                div()
                    .flex()
                    .gap_2()
                    .child(Input::new(&self.keyword).w(px(280.)))
                    .child(
                        Button::new("consumer-apply-filter")
                            .label("Apply")
                            .on_click(cx.listener(|view, _, _, cx| view.apply_filter(cx))),
                    )
                    .child(
                        Button::new("consumer-connection-filter")
                            .label(connection)
                            .outline()
                            .on_click(cx.listener(|view, _, _, cx| view.cycle_connection_filter(cx))),
                    )
                    .child(
                        Button::new("consumer-sort")
                            .label(format!("Sort: {:?}", self.store.sort.key))
                            .outline()
                            .on_click(cx.listener(|view, _, _, cx| view.cycle_sort(cx))),
                    )
                    .child(
                        Button::new("consumer-refresh")
                            .label("Refresh")
                            .on_click(cx.listener(|view, _, _, cx| view.refresh(cx))),
                    ),
            )
            .when_some(initial_error, |this, error| {
                this.child(
                    div().p_3().child(error).child(
                        Button::new("consumer-retry-inventory")
                            .label("Retry")
                            .on_click(cx.listener(|view, _, _, cx| view.refresh(cx))),
                    ),
                )
            })
            .child(div().text_sm().child(format!("{} Consumer groups", page.total)))
            .child(
                div()
                    .flex_1()
                    .min_h_0()
                    .child(Table::new(&self.table).bordered(true).small()),
            )
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_2()
                    .child(
                        Button::new("consumer-prev-page")
                            .label("Previous")
                            .disabled(page.page <= 1)
                            .on_click(cx.listener(|view, _, _, cx| {
                                view.store.set_page(view.store.page.saturating_sub(1));
                                view.sync_table(cx);
                            })),
                    )
                    .child(format!("Page {} of {}", page.page, page.page_count))
                    .child(
                        Button::new("consumer-next-page")
                            .label("Next")
                            .disabled(page.page >= page.page_count)
                            .on_click(cx.listener(|view, _, _, cx| {
                                view.store.set_page(view.store.page.saturating_add(1));
                                view.sync_table(cx);
                            })),
                    ),
            )
    }
}

fn render_consumer_outcome(outcome: &ConsumerPartialOutcome, cx: &gpui::App) -> impl IntoElement {
    div()
        .p_3()
        .rounded_md()
        .bg(cx.theme().muted)
        .flex()
        .flex_col()
        .gap_1()
        .child(format!(
            "{}: {}/{} target outcomes applied",
            if outcome.is_complete_success() {
                "Complete"
            } else {
                "Partial"
            },
            outcome.applied_count(),
            outcome.targets.len()
        ))
        .children(outcome.targets.iter().map(|target| {
            div().text_sm().child(format!(
                "{} · stage={:?} · code={} · retryable={} · applied={}",
                target.target,
                target.stage,
                target
                    .failure
                    .map_or_else(|| "None".to_owned(), |failure| format!("{failure:?}")),
                target.retryable,
                target.applied
            ))
        }))
}

fn category_label(category: ConsumerCategory) -> &'static str {
    match category {
        ConsumerCategory::Application => "Application",
        ConsumerCategory::System => "System",
        ConsumerCategory::Unknown => "Unknown",
    }
}

fn connection_label(observation: &ConsumerObservation<ConsumerConnectionState>) -> String {
    match observation {
        ConsumerObservation::Complete(ConsumerConnectionState::Connected) => "Connected".into(),
        ConsumerObservation::Complete(ConsumerConnectionState::Disconnected) => "Disconnected".into(),
        ConsumerObservation::Partial { .. } => "Partial".into(),
        ConsumerObservation::Unknown { .. } => "Unknown".into(),
    }
}

fn observed_value<T: std::fmt::Display>(observation: &ConsumerObservation<T>) -> String {
    match observation {
        ConsumerObservation::Complete(value) => value.to_string(),
        ConsumerObservation::Partial { value, .. } => format!("Partial ({value})"),
        ConsumerObservation::Unknown { .. } => "Unknown".into(),
    }
}
