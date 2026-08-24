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

//! Real Topic inventory Table, applied filters, and bounded typed Sheet.

use gpui::prelude::FluentBuilder as _;
use gpui::{
    App, AppContext as _, Context, Entity, EventEmitter, Focusable as _, InteractiveElement as _, IntoElement,
    ParentElement as _, Render, Styled as _, Subscription, Task, WeakEntity, Window, div, px,
};
use gpui_component::{
    ActiveTheme as _, Disableable as _, Sizable as _, WindowExt as _,
    button::{Button, ButtonVariants as _},
    input::{Input, InputEvent, InputState},
    table::{Column, Table, TableDelegate, TableEvent, TableState},
};
use rocketmq_dashboard_common::{
    TopicCategory, TopicCompleteness, TopicFailureCode, TopicFailureStage, TopicInventoryItem, TopicMessageType,
    TopicPermission, TopicSortKey,
};

use crate::{
    features::{
        topic_detail::{TopicDetail, TopicDetailIntent},
        topic_dialogs::{TopicDialogForm, TopicDialogKind, TopicEditDraft, TopicSendDraft},
        topics_store::{TopicCountEvidence, TopicsStore},
    },
    route::{AppRoute, TopicTab},
    services::AppServices,
    state::Loadable,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TopicsIntent {
    Navigate(AppRoute),
    ReplaceRoute(AppRoute),
    SheetClosed,
}

struct TopicTableDelegate {
    columns: Vec<Column>,
    rows: Vec<TopicInventoryItem>,
    loading: bool,
    owner: Option<WeakEntity<TopicsView>>,
}

impl TopicTableDelegate {
    fn new() -> Self {
        Self {
            columns: vec![
                Column::new("topic", "Topic"),
                Column::new("category", "Category"),
                Column::new("message-type", "Message type"),
                Column::new("clusters", "Clusters"),
                Column::new("brokers", "Brokers"),
                Column::new("read-queues", "Read queues"),
                Column::new("write-queues", "Write queues"),
                Column::new("permission", "Permission"),
                Column::new("ordered", "Ordered"),
                Column::new("actions", "Actions"),
            ],
            rows: Vec::new(),
            loading: true,
            owner: None,
        }
    }
}

impl TableDelegate for TopicTableDelegate {
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
            2 => message_type_label(item.message_type).into(),
            3 => compact_names(&item.clusters),
            4 => compact_names(&item.brokers),
            5 => optional_number(item.read_queue_count),
            6 => optional_number(item.write_queue_count),
            7 => permission_label(item.permission),
            8 => item.ordered.map_or_else(|| "Unknown".into(), |value| value.to_string()),
            _ => {
                return div().px_2().child(
                    Button::new(("open-topic-row", row))
                        .label("Open")
                        .small()
                        .outline()
                        .on_click({
                            let owner = self.owner.clone();
                            move |_, window, cx| {
                                if let Some(owner) = &owner {
                                    let _ = owner.update(cx, |view, cx| {
                                        view.open_row(row, TopicTab::Overview, true, window, cx);
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
            .child("No Topics match the applied filters.")
    }
}

pub struct TopicsView {
    pub(super) services: AppServices,
    pub(super) revision: u64,
    pub store: TopicsStore,
    keyword: Entity<InputState>,
    table: Entity<TableState<TopicTableDelegate>>,
    pub(super) detail: Option<Entity<TopicDetail>>,
    detail_subscription: Option<Subscription>,
    _subscriptions: [Subscription; 2],
    inventory_task: Option<Task<()>>,
    pub(super) mutation_task: Option<Task<()>>,
    pub(super) dialog_form: Option<Entity<TopicDialogForm>>,
    pending_route: Option<(String, TopicTab)>,
    suppress_sheet_closed: bool,
}

impl EventEmitter<TopicsIntent> for TopicsView {}

impl TopicsView {
    pub fn new(window: &mut Window, services: AppServices, revision: u64, cx: &mut Context<Self>) -> Self {
        let keyword = cx.new(|cx| InputState::new(window, cx).placeholder("Search Topic name"));
        let table = cx.new(|cx| {
            TableState::new(TopicTableDelegate::new(), window, cx)
                .col_movable(false)
                .col_resizable(true)
                .sortable(false)
                .col_selectable(false)
                .row_selectable(true)
        });
        let subscriptions = [
            cx.subscribe_in(&keyword, window, |view, input, event: &InputEvent, _, cx| {
                if matches!(event, InputEvent::Change) {
                    view.store.draft_filter.keyword = input.read(cx).value().to_string();
                    cx.notify();
                }
            }),
            cx.subscribe_in(&table, window, |view, _, event: &TableEvent, window, cx| {
                if let TableEvent::DoubleClickedRow(row) = event {
                    view.open_row(*row, TopicTab::Overview, true, window, cx);
                }
            }),
        ];
        let view = Self {
            services,
            revision,
            store: TopicsStore::default(),
            keyword,
            table,
            detail: None,
            detail_subscription: None,
            _subscriptions: subscriptions,
            inventory_task: None,
            mutation_task: None,
            dialog_form: None,
            pending_route: None,
            suppress_sheet_closed: false,
        };
        let owner = cx.entity().downgrade();
        view.table
            .update(cx, |table, _| table.delegate_mut().owner = Some(owner));
        view
    }

    pub fn set_revision(&mut self, revision: u64, cx: &mut Context<Self>) {
        let loaded = !matches!(self.store.inventory.state, Loadable::Idle);
        if revision != self.revision {
            self.revision = revision;
            self.store.invalidate();
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
            let result = services.topic_inventory(request.scope).await;
            let _ = this.update(cx, |view, cx| {
                view.store.finish_inventory(request, view.revision, result);
                view.sync_table(cx);
                view.sync_detail_stale(cx);
                cx.notify();
            });
        }));
    }

    pub(super) fn refresh_in(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(request) = self.store.begin_inventory(self.revision) else {
            return;
        };
        self.sync_table(cx);
        let services = self.services.clone();
        self.inventory_task = Some(cx.spawn_in(window, async move |this, cx| {
            let result = services.topic_inventory(request.scope).await;
            let _ = this.update_in(cx, |view, window, cx| {
                view.store.finish_inventory(request, view.revision, result);
                view.sync_table(cx);
                view.sync_detail_stale(cx);
                view.open_pending_route(window, cx);
                cx.notify();
            });
        }));
    }

    pub fn open_route(&mut self, topic: &str, tab: TopicTab, window: &mut Window, cx: &mut Context<Self>) {
        let Ok(identity) = rocketmq_dashboard_common::TopicIdentity::parse(topic.to_owned()) else {
            return;
        };
        if let Some(item) = self.store.find(&identity) {
            self.open_item(item, tab, false, window, cx);
        } else if matches!(
            self.store.inventory.state,
            Loadable::Idle | Loadable::InitialLoading | Loadable::Refreshing(_)
        ) {
            self.pending_route = Some((topic.to_owned(), tab));
            self.refresh_in(window, cx);
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

    #[cfg(test)]
    pub fn row_focus_is_focused(&self, window: &Window, cx: &App) -> bool {
        self.table.focus_handle(cx).is_focused(window)
    }

    #[cfg(test)]
    pub(crate) fn dialog_form_for_test(&self) -> Option<Entity<TopicDialogForm>> {
        self.dialog_form.clone()
    }

    fn open_pending_route(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some((topic, tab)) = self.pending_route.take() else {
            return;
        };
        let Ok(identity) = rocketmq_dashboard_common::TopicIdentity::parse(topic) else {
            return;
        };
        if let Some(item) = self.store.find(&identity) {
            self.open_item(item, tab, false, window, cx);
        }
    }

    fn open_row(&mut self, row: usize, tab: TopicTab, emit_route: bool, window: &mut Window, cx: &mut Context<Self>) {
        let Some(item) = self.store.visible_page().items.get(row).cloned() else {
            return;
        };
        self.open_item(item, tab, emit_route, window, cx);
    }

    fn open_item(
        &mut self,
        item: TopicInventoryItem,
        tab: TopicTab,
        emit_route: bool,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Ok(route) = self.store.open(item.clone(), tab) else {
            return;
        };
        let Some(selected) = self.store.detail.as_ref().map(|detail| detail.selected.clone()) else {
            return;
        };
        self.table.focus_handle(cx).focus(window);
        if window.has_active_sheet(cx) {
            self.suppress_sheet_closed = true;
            window.close_sheet(cx);
        }
        let detail =
            cx.new(|cx| TopicDetail::new_with_selection(self.services.clone(), self.revision, selected, tab, cx));
        let detail_topic = item.identity.clone();
        self.detail_subscription = Some(cx.subscribe_in(
            &detail,
            window,
            move |view, _, event: &TopicDetailIntent, window, cx| match event {
                TopicDetailIntent::SelectionEvidenceUpdated {
                    revision,
                    topic,
                    selected,
                } => {
                    if *revision == view.revision
                        && topic == &detail_topic
                        && view.store.sync_detail_selection_evidence(topic, selected.clone())
                    {
                        cx.notify();
                    }
                }
                TopicDetailIntent::ReplaceRoute(route) => cx.emit(TopicsIntent::ReplaceRoute(route.clone())),
                TopicDetailIntent::NavigateConsumer(route) => cx.emit(TopicsIntent::Navigate(route.clone())),
                TopicDetailIntent::EditTarget {
                    target,
                    expected_version,
                    read_queue_count,
                    write_queue_count,
                } => {
                    let topic = detail_topic.clone();
                    view.open_topic_dialog(
                        TopicDialogKind::Edit(TopicEditDraft {
                            topic,
                            target: target.clone(),
                            expected_version: *expected_version,
                            read_queue_count: *read_queue_count,
                            write_queue_count: *write_queue_count,
                        }),
                        window,
                        cx,
                    );
                }
                TopicDetailIntent::Send(topic) => {
                    view.open_topic_dialog(TopicDialogKind::Send(TopicSendDraft::new(topic.clone())), window, cx)
                }
                TopicDetailIntent::DeleteTopic { topic, clusters } => view.open_topic_dialog(
                    TopicDialogKind::DeleteTopic {
                        topic: topic.clone(),
                        clusters: clusters.clone(),
                    },
                    window,
                    cx,
                ),
                TopicDetailIntent::DeleteBroker { topic, target } => view.open_topic_dialog(
                    TopicDialogKind::DeleteBroker {
                        topic: topic.clone(),
                        target: target.clone(),
                    },
                    window,
                    cx,
                ),
                TopicDetailIntent::ResetOffset {
                    topic,
                    consumer_group,
                    clusters,
                } => view.open_topic_dialog(
                    TopicDialogKind::ResetOffset {
                        topic: topic.clone(),
                        consumer_group: consumer_group.clone(),
                        clusters: clusters.clone(),
                        timestamp: 0,
                        force: false,
                    },
                    window,
                    cx,
                ),
                TopicDetailIntent::SkipAccumulated {
                    topic,
                    consumer_group,
                    clusters,
                } => view.open_topic_dialog(
                    TopicDialogKind::SkipAccumulated {
                        topic: topic.clone(),
                        consumer_group: consumer_group.clone(),
                        clusters: clusters.clone(),
                        force: false,
                    },
                    window,
                    cx,
                ),
            },
        ));
        self.detail = Some(detail.clone());
        let owner = cx.entity().downgrade();
        let title = item.identity.as_str().to_owned();
        window.open_sheet(cx, move |sheet, _, _| {
            let owner = owner.clone();
            sheet
                .title(title.clone())
                .size(px(760.))
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
                            cx.emit(TopicsIntent::SheetClosed);
                        }
                    });
                })
                .child(detail.clone())
        });
        if emit_route {
            cx.emit(TopicsIntent::Navigate(route));
        }
    }

    pub(super) fn sync_detail_stale(&mut self, cx: &mut Context<Self>) {
        let selection = self.store.detail.as_ref().map(|detail| detail.selected.clone());
        if let (Some(detail), Some(selected)) = (&self.detail, selection) {
            detail.update(cx, |detail, cx| detail.sync_inventory_selection(selected, cx));
        }
    }

    pub(super) fn sync_table(&mut self, cx: &mut Context<Self>) {
        let page = self.store.visible_page();
        let loading = matches!(self.store.inventory.state, Loadable::InitialLoading);
        self.table.update(cx, |table, cx| {
            table.delegate_mut().rows = page.items;
            table.delegate_mut().loading = loading;
            table.refresh(cx);
            cx.notify();
        });
    }

    fn search(&mut self, cx: &mut Context<Self>) {
        self.store.search();
        self.sync_table(cx);
        cx.notify();
    }

    fn reset(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.store.reset_filters();
        self.keyword
            .update(cx, |input, cx| input.set_value(String::new(), window, cx));
        self.sync_table(cx);
        cx.notify();
    }

    fn cycle_category(&mut self, cx: &mut Context<Self>) {
        self.store.draft_filter.category = match self.store.draft_filter.category {
            None => Some(TopicCategory::Application),
            Some(TopicCategory::Application) => Some(TopicCategory::Retry),
            Some(TopicCategory::Retry) => Some(TopicCategory::Dlq),
            Some(TopicCategory::Dlq) => Some(TopicCategory::System),
            Some(TopicCategory::System) | Some(TopicCategory::Unknown) => None,
        };
        cx.notify();
    }

    fn cycle_message_type(&mut self, cx: &mut Context<Self>) {
        self.store.draft_filter.message_type = match self.store.draft_filter.message_type {
            None => Some(TopicMessageType::Normal),
            Some(TopicMessageType::Normal) => Some(TopicMessageType::Delay),
            Some(TopicMessageType::Delay) => Some(TopicMessageType::Fifo),
            Some(TopicMessageType::Fifo) => Some(TopicMessageType::Transaction),
            Some(_) => None,
        };
        cx.notify();
    }

    fn cycle_sort(&mut self, cx: &mut Context<Self>) {
        self.store.sort.key = match self.store.sort.key {
            TopicSortKey::Name => TopicSortKey::Category,
            TopicSortKey::Category => TopicSortKey::MessageType,
            TopicSortKey::MessageType => TopicSortKey::ReadQueues,
            TopicSortKey::ReadQueues => TopicSortKey::WriteQueues,
            TopicSortKey::WriteQueues => TopicSortKey::Name,
        };
        self.sync_table(cx);
        cx.notify();
    }
}

impl Render for TopicsView {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let page = self.store.visible_page();
        let refreshing = matches!(self.store.inventory.state, Loadable::Refreshing(_));
        let initial_error = match &self.store.inventory.state {
            Loadable::Failed { previous: None, error } => Some(error.summary().to_owned()),
            _ => None,
        };
        let partial = self
            .store
            .inventory
            .state
            .value()
            .is_some_and(|inventory| matches!(inventory.completeness, TopicCompleteness::Partial { .. }));
        let partial_failures = self
            .store
            .inventory
            .state
            .value()
            .map(|inventory| {
                inventory
                    .failures
                    .iter()
                    .map(|failure| {
                        format!(
                            "{} · stage={} · code={} · retryable={}",
                            failure.target,
                            topic_failure_stage_label(failure.stage),
                            topic_failure_code_label(failure.code),
                            failure.retryable
                        )
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let category = self
            .store
            .draft_filter
            .category
            .map_or("All categories", category_label);
        let message_type = self
            .store
            .draft_filter
            .message_type
            .map_or("All message types", message_type_label);
        let create_enabled = self.store.inventory.state.value().is_some_and(|inventory| {
            matches!(inventory.completeness, TopicCompleteness::Complete) && !inventory.targets.is_empty()
        });
        let body = if let Some(error) = initial_error {
            crate::components::states::error_state(
                "Topic inventory unavailable",
                &error,
                cx.theme().foreground,
                cx.theme().muted_foreground,
                Some(cx.listener(|view, _, _, cx| view.refresh(cx))),
                None::<fn(&gpui::ClickEvent, &mut Window, &mut App)>,
            )
        } else {
            div()
                .flex_1()
                .min_h_0()
                .child(Table::new(&self.table).bordered(true).small())
        };
        div()
            .id("topics-page")
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
                        .child("Partial inventory — counts are lower bounds and missing targets are not assumed empty.")
                        .children(partial_failures.clone()),
                )
            })
            .child(render_counts(&self.store, cx))
            .child(
                div()
                    .flex()
                    .flex_wrap()
                    .gap_2()
                    .child(
                        Button::new("topic-create")
                            .label("Create Topic")
                            .primary()
                            .disabled(!create_enabled)
                            .debug_selector(|| "topic-create".to_owned())
                            .on_click(cx.listener(|view, _, window, cx| view.open_create_dialog(window, cx))),
                    )
                    .child(div().flex_1().min_w(px(240.)).child(Input::new(&self.keyword)))
                    .child(
                        Button::new("topic-filter-category")
                            .label(category)
                            .outline()
                            .on_click(cx.listener(|view, _, _, cx| view.cycle_category(cx))),
                    )
                    .child(
                        Button::new("topic-filter-message-type")
                            .label(message_type)
                            .outline()
                            .on_click(cx.listener(|view, _, _, cx| view.cycle_message_type(cx))),
                    )
                    .child(
                        Button::new("topic-sort")
                            .label("Sort")
                            .outline()
                            .on_click(cx.listener(|view, _, _, cx| view.cycle_sort(cx))),
                    )
                    .child(
                        Button::new("topic-search")
                            .label("Search")
                            .primary()
                            .on_click(cx.listener(|view, _, _, cx| view.search(cx))),
                    )
                    .child(
                        Button::new("topic-reset")
                            .label("Reset")
                            .outline()
                            .on_click(cx.listener(|view, _, window, cx| view.reset(window, cx))),
                    )
                    .child(
                        Button::new("topic-refresh")
                            .label(if refreshing { "Refreshing…" } else { "Refresh" })
                            .outline()
                            .disabled(refreshing)
                            .on_click(cx.listener(|view, _, _, cx| view.refresh(cx))),
                    ),
            )
            .child(body)
            .child(
                div()
                    .flex()
                    .justify_between()
                    .items_center()
                    .child(format!(
                        "Page {} of {} · {} Topics",
                        page.page, page.page_count, page.total
                    ))
                    .child(
                        div()
                            .flex()
                            .gap_2()
                            .child(
                                Button::new("topics-previous")
                                    .label("Previous")
                                    .outline()
                                    .disabled(page.page == 1)
                                    .on_click(cx.listener(|view, _, _, cx| {
                                        view.store.page = view.store.page.saturating_sub(1).max(1);
                                        view.sync_table(cx);
                                    })),
                            )
                            .child(
                                Button::new("topics-next")
                                    .label("Next")
                                    .outline()
                                    .disabled(page.page >= page.page_count)
                                    .on_click(cx.listener(|view, _, _, cx| {
                                        view.store.page = view.store.page.saturating_add(1);
                                        view.sync_table(cx);
                                    })),
                            ),
                    ),
            )
    }
}

fn render_counts(store: &TopicsStore, cx: &mut Context<TopicsView>) -> gpui::Div {
    let counts = store.category_counts();
    div().flex().flex_wrap().gap_3().children(
        [
            (TopicCategory::Application, "Application"),
            (TopicCategory::Retry, "Retry"),
            (TopicCategory::Dlq, "DLQ"),
            (TopicCategory::System, "System"),
        ]
        .into_iter()
        .map(|(category, label)| {
            let value = match counts.get(&category).copied().unwrap_or(TopicCountEvidence::Unknown) {
                TopicCountEvidence::Exact(value) => value.to_string(),
                TopicCountEvidence::LowerBound(value) => format!("≥ {value}"),
                TopicCountEvidence::Unknown => "Unknown".into(),
            };
            div()
                .p_3()
                .min_w(px(140.))
                .border_1()
                .border_color(cx.theme().border)
                .rounded_md()
                .child(format!("{label}: {value}"))
        }),
    )
}

fn compact_names(values: &[String]) -> String {
    match values {
        [] => "Unknown".into(),
        [only] => only.clone(),
        [first, ..] => format!("{first} +{}", values.len() - 1),
    }
}

fn optional_number(value: Option<u32>) -> String {
    value.map_or_else(|| "Unknown".into(), |value| value.to_string())
}

fn permission_label(permission: Option<TopicPermission>) -> String {
    permission.map_or_else(
        || "Unknown".into(),
        |permission| {
            format!(
                "{}{}{}",
                if permission.can_read() { "R" } else { "-" },
                if permission.can_write() { "W" } else { "-" },
                if permission.inherits() { "I" } else { "-" }
            )
        },
    )
}

fn category_label(category: TopicCategory) -> &'static str {
    match category {
        TopicCategory::Application => "Application",
        TopicCategory::Retry => "Retry",
        TopicCategory::Dlq => "DLQ",
        TopicCategory::System => "System",
        TopicCategory::Unknown => "Unknown",
    }
}

fn message_type_label(message_type: TopicMessageType) -> &'static str {
    match message_type {
        TopicMessageType::Normal => "Normal",
        TopicMessageType::Delay => "Delay",
        TopicMessageType::Fifo => "FIFO",
        TopicMessageType::Transaction => "Transaction",
        TopicMessageType::Retry => "Retry",
        TopicMessageType::Dlq => "DLQ",
        TopicMessageType::System => "System",
        TopicMessageType::Unspecified => "Unspecified",
        TopicMessageType::Unknown => "Unknown",
    }
}

fn topic_failure_stage_label(stage: TopicFailureStage) -> &'static str {
    match stage {
        TopicFailureStage::CatalogConfig => "catalog_config",
        TopicFailureStage::CatalogRoute => "catalog_route",
        TopicFailureStage::Stats => "stats",
        TopicFailureStage::Configuration => "configuration",
        TopicFailureStage::Consumer => "consumer",
        TopicFailureStage::Mutation => "mutation",
        TopicFailureStage::Reload => "reload",
    }
}

fn topic_failure_code_label(code: TopicFailureCode) -> &'static str {
    match code {
        TopicFailureCode::NotFound => "not_found",
        TopicFailureCode::InvalidData => "invalid_data",
        TopicFailureCode::Unavailable => "unavailable",
        TopicFailureCode::Conflict => "conflict",
    }
}
