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

//! Group-scoped DLQ pages with body-free selection and a shared bounded inspector.

#[path = "dlq_actions.rs"]
mod actions;
#[path = "dlq_render.rs"]
mod render;

use crate::{
    features::{
        dashboard_store::ResourceSlot,
        dlq_dialogs::DlqConfirmation,
        dlq_store::{DlqRequest, DlqStore},
        message_detail::{MessageDetail, format_time},
    },
    services::{AppServices, consumers::ConsumerRequestScope, messages::MessageRequestScope},
    state::{Loadable, UiError, UiErrorCode},
};
use gpui::{
    App, AppContext as _, Context, Entity, InteractiveElement as _, IntoElement, ParentElement as _, Render,
    Styled as _, Subscription, Task, WeakEntity, Window, div, prelude::FluentBuilder as _, px,
};
use gpui_component::{
    ActiveTheme as _, Disableable as _, Sizable as _, WindowExt as _,
    button::Button,
    checkbox::Checkbox,
    dialog::{Dialog, DialogButtonProps},
    input::{Input, InputEvent, InputState},
    scroll::ScrollableElement as _,
    select::{SearchableVec, Select, SelectEvent, SelectState},
    table::{Column, Table, TableDelegate, TableState},
};
use rocketmq_dashboard_common::{
    ConnectionScope, ConsumerInventory, ConsumerObservationState, DlqQuery, DlqRow, MessageIdentity, MessagePageExtent,
};
use std::collections::BTreeSet;

struct DlqTable {
    columns: Vec<Column>,
    rows: Vec<DlqRow>,
    selected: BTreeSet<MessageIdentity>,
    selectable: BTreeSet<MessageIdentity>,
    loading: bool,
    owner: WeakEntity<DlqView>,
}
impl TableDelegate for DlqTable {
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
        if column == 0 {
            let owner = self.owner.clone();
            let identity = item.metadata.identity.clone();
            return div()
                .child(
                    Checkbox::new(("dlq-select", row))
                        .checked(self.selected.contains(&identity))
                        .disabled(self.loading || !self.selectable.contains(&identity))
                        .debug_selector(move || format!("dlq-select-{row}"))
                        .on_click(move |checked, _, cx| {
                            let _ = owner.update(cx, |view, cx| {
                                view.store.toggle(identity.clone(), *checked);
                                view.sync_table(cx);
                                cx.notify();
                            });
                        }),
                )
                .into_any_element();
        }
        if column == 1 {
            let owner = self.owner.clone();
            return div()
                .child(
                    Button::new(("dlq-open", row))
                        .label("Open")
                        .outline()
                        .small()
                        .disabled(self.loading)
                        .debug_selector(move || format!("dlq-open-{row}"))
                        .on_click(move |_, window, cx| {
                            let _ = owner.update(cx, |view, cx| view.open_row(row, window, cx));
                        }),
                )
                .into_any_element();
        }
        let value = match column {
            2 => item.metadata.identity.message_id.clone(),
            3 => item.target.as_ref().map_or_else(
                |error| error.summary().to_string(),
                |target| target.original().topic.clone(),
            ),
            4 => item
                .metadata
                .tags
                .as_ref()
                .map_or_else(|| "-".into(), |value| value.display()),
            5 => item
                .metadata
                .keys
                .as_ref()
                .map_or_else(|| "-".into(), |value| value.display()),
            6 => item.metadata.reconsume_times.to_string(),
            _ => format_time(item.metadata.store_timestamp),
        };
        div()
            .text_sm()
            .px_2()
            .overflow_hidden()
            .text_ellipsis()
            .child(value)
            .into_any_element()
    }
    fn render_empty(&mut self, _: &mut Window, _: &mut Context<TableState<Self>>) -> impl IntoElement {
        div()
            .p_4()
            .child("No dead-letter messages returned for the applied page.")
    }
}

pub(crate) struct DlqView {
    services: AppServices,
    revision: u64,
    store: DlqStore,
    group: Entity<InputState>,
    group_picker: Entity<SelectState<SearchableVec<String>>>,
    groups: ResourceSlot<ConsumerInventory>,
    begin: Entity<InputState>,
    end: Entity<InputState>,
    message_id: Entity<InputState>,
    page_size: Entity<InputState>,
    table: Entity<TableState<DlqTable>>,
    detail: Option<Entity<MessageDetail>>,
    confirmation: Option<Entity<DlqConfirmation>>,
    query_task: Option<Task<()>>,
    group_task: Option<Task<()>>,
    batch_task: Option<Task<()>>,
    export_task: Option<Task<()>>,
    export_epoch: u64,
    status: Option<String>,
    _subscriptions: Vec<Subscription>,
}
impl DlqView {
    #[cfg(test)]
    pub(crate) fn query_is_idle(&self) -> bool {
        matches!(self.store.rows, Loadable::Idle)
    }
    pub fn new(window: &mut Window, services: AppServices, revision: u64, cx: &mut Context<Self>) -> Self {
        let now = chrono::Utc::now();
        let group = cx.new(|cx| InputState::new(window, cx).placeholder("Consumer Group (manual fallback)"));
        let group_picker =
            cx.new(|cx| SelectState::new(SearchableVec::new(Vec::<String>::new()), None, window, cx).searchable(true));
        let begin = cx.new(|cx| {
            InputState::new(window, cx).default_value(
                (now - chrono::TimeDelta::hours(3))
                    .format("%Y-%m-%d %H:%M:%S")
                    .to_string(),
            )
        });
        let end = cx.new(|cx| InputState::new(window, cx).default_value(now.format("%Y-%m-%d %H:%M:%S").to_string()));
        let message_id =
            cx.new(|cx| InputState::new(window, cx).placeholder("Optional physical or producer Message ID"));
        let page_size = cx.new(|cx| InputState::new(window, cx).default_value("20"));
        let owner = cx.entity().downgrade();
        let table = cx.new(|cx| {
            TableState::new(
                DlqTable {
                    columns: vec![
                        Column::new("select", "Select").width(px(50.)),
                        Column::new("actions", "Actions").width(px(65.)),
                        Column::new("id", "DLQ Message ID").width(px(210.)),
                        Column::new("topic", "Original Topic").width(px(180.)),
                        Column::new("tags", "Tags").width(px(100.)),
                        Column::new("keys", "Keys").width(px(120.)),
                        Column::new("retries", "Retries").width(px(65.)),
                        Column::new("stored", "Stored UTC").width(px(165.)),
                    ],
                    rows: Vec::new(),
                    selected: BTreeSet::new(),
                    selectable: BTreeSet::new(),
                    loading: false,
                    owner,
                },
                window,
                cx,
            )
        });
        let mut subscriptions = [&group, &begin, &end, &message_id, &page_size]
            .into_iter()
            .map(|input| {
                cx.subscribe_in(input, window, |view, _, event, window, cx| {
                    if matches!(event, InputEvent::Change) {
                        view.edit(window, cx);
                    }
                })
            })
            .collect::<Vec<_>>();
        subscriptions.push(cx.subscribe_in(&group_picker, window, |view, _, event, window, cx| {
            if let SelectEvent::Confirm(Some(group)) = event {
                view.group
                    .update(cx, |input, cx| input.set_value(group.clone(), window, cx));
                view.edit(window, cx);
            }
        }));
        Self {
            services,
            revision,
            store: DlqStore::default(),
            group,
            group_picker,
            groups: ResourceSlot::default(),
            begin,
            end,
            message_id,
            page_size,
            table,
            detail: None,
            confirmation: None,
            query_task: None,
            group_task: None,
            batch_task: None,
            export_task: None,
            export_epoch: 0,
            status: None,
            _subscriptions: subscriptions,
        }
    }
    pub fn set_revision(&mut self, revision: u64, window: &mut Window, cx: &mut Context<Self>) {
        if self.revision == revision {
            return;
        }
        self.revision = revision;
        self.leave(window, cx);
        self.store.clear_context();
        for input in [&self.group, &self.message_id] {
            input.update(cx, |input, cx| input.set_value("", window, cx));
        }
        self.sync_table(cx);
        cx.notify();
    }
    pub fn leave(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.query_task = None;
        self.group_task = None;
        self.groups.clear();
        self.set_groups(Vec::new(), window, cx);
        self.store.clear_reads();
        self.cancel_export();
        self.close_sensitive(window, cx);
        self.status = None;
        self.sync_table(cx);
        cx.notify();
    }
    fn edit(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.query_task = None;
        self.store.edit();
        self.cancel_export();
        self.close_sensitive(window, cx);
        self.status = None;
        self.sync_table(cx);
        cx.notify();
    }
    fn cancel_export(&mut self) {
        self.export_epoch = self.export_epoch.saturating_add(1);
        self.export_task = None;
    }
    fn close_sensitive(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if let Some(detail) = self.detail.take() {
            detail.update(cx, |detail, cx| detail.clear_sensitive(cx));
            window.close_sheet(cx);
        }
        if self.confirmation.take().is_some() {
            window.close_all_dialogs(cx);
        }
    }
    pub fn ensure_groups(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.require_group_scope(window, cx) && matches!(self.groups.state, Loadable::Idle) {
            self.load_groups(window, cx);
        }
    }
    fn require_group_scope(&mut self, window: &mut Window, cx: &mut Context<Self>) -> bool {
        if self.services.connection_state().config.scope == ConnectionScope::NameServer {
            return true;
        }
        self.group_task = None;
        self.groups.clear_with_error(UiError::new(
            "DLQ Group discovery requires NameServer Direct scope.",
            UiErrorCode::CapabilityUnavailable,
            false,
        ));
        self.set_groups(Vec::new(), window, cx);
        cx.notify();
        false
    }
    fn set_groups(&self, mut groups: Vec<String>, window: &mut Window, cx: &mut Context<Self>) {
        groups.sort();
        groups.dedup();
        self.group_picker.update(cx, |picker, cx| {
            picker.set_items(SearchableVec::new(groups), window, cx);
            picker.set_selected_index(None, window, cx);
        });
    }
    fn load_groups(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if !self.require_group_scope(window, cx) {
            return;
        }
        let Some(request) = self.groups.begin(self.revision) else {
            return;
        };
        let services = self.services.clone();
        self.group_task = Some(cx.spawn_in(window, async move |this, cx| {
            let result = services
                .dlq_group_inventory(ConsumerRequestScope {
                    revision: request.revision(),
                    epoch: request.epoch(),
                    query_scope: Default::default(),
                })
                .await
                .map(Some);
            let _ = this.update_in(cx, |view, window, cx| {
                if request.revision() != view.revision || !view.require_group_scope(window, cx) {
                    return;
                }
                if !view.groups.finish(request, view.revision, result) {
                    return;
                }
                let groups = view.groups.state.value().map_or_else(Vec::new, |inventory| {
                    inventory
                        .groups
                        .iter()
                        .map(|group| group.identity.as_str().to_string())
                        .filter(|group| !group.starts_with("CID_RMQ_SYS_"))
                        .collect()
                });
                view.set_groups(groups, window, cx);
                cx.notify();
            });
        }));
        cx.notify();
    }
    fn draft(&self, cx: &App) -> Result<DlqQuery, UiError> {
        let id = self.message_id.read(cx).value().trim().to_string();
        let query = DlqQuery {
            consumer_group: self.group.read(cx).value().trim().to_string(),
            begin: super::messages::parse_time(&self.begin.read(cx).value())?,
            end: super::messages::parse_time(&self.end.read(cx).value())?,
            message_id: (!id.is_empty()).then_some(id),
            page: 1,
            page_size: self
                .page_size
                .read(cx)
                .value()
                .trim()
                .parse()
                .map_err(|_| UiError::new("Rows per page must be from 1 to 50.", UiErrorCode::Validation, false))?,
        };
        query.validate().map_err(crate::services::dlq::validation)?;
        Ok(query)
    }
    fn search(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        match self.draft(cx) {
            Ok(query) => {
                self.close_sensitive(window, cx);
                self.cancel_export();
                if let Some(request) = self.store.apply(query, self.revision) {
                    self.start_query(request, window, cx);
                }
            }
            Err(error) => {
                self.status = Some(error.summary().into());
                cx.notify();
            }
        }
    }
    fn refresh(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.close_sensitive(window, cx);
        self.cancel_export();
        if let Some(request) = self.store.begin(self.revision) {
            self.start_query(request, window, cx);
        }
    }
    fn page(&mut self, next: bool, window: &mut Window, cx: &mut Context<Self>) {
        let Some(query) = self.store.applied.as_ref() else {
            return;
        };
        let Some(number) = (if next {
            query.page.checked_add(1)
        } else {
            query.page.checked_sub(1)
        }) else {
            return;
        };
        let Ok(query) = query.with_page(number) else {
            return;
        };
        self.close_sensitive(window, cx);
        self.cancel_export();
        if let Some(request) = self.store.apply(query, self.revision) {
            self.start_query(request, window, cx);
        }
    }
    fn start_query(&mut self, request: DlqRequest, window: &mut Window, cx: &mut Context<Self>) {
        self.status = None;
        self.sync_table(cx);
        let services = self.services.clone();
        self.query_task = Some(cx.spawn_in(window, async move |this, cx| {
            let result = services.query_dlq(request.scope, request.query.clone()).await;
            let _ = this.update_in(cx, |view, _, cx| {
                if view.store.finish(&request, view.revision, result) {
                    view.sync_table(cx);
                    cx.notify();
                }
            });
        }));
        cx.notify();
    }
    fn sync_table(&self, cx: &mut Context<Self>) {
        let rows = self.store.rows.value().map_or_else(Vec::new, |page| page.rows.clone());
        let selectable = rows
            .iter()
            .filter(|row| self.store.selectable(&row.metadata.identity))
            .map(|row| row.metadata.identity.clone())
            .collect();
        self.table.update(cx, |table, cx| {
            table.delegate_mut().rows = rows;
            table.delegate_mut().selected = self.store.selected.clone();
            table.delegate_mut().selectable = selectable;
            table.delegate_mut().loading = self.batch_task.is_some()
                || matches!(self.store.rows, Loadable::InitialLoading | Loadable::Refreshing(_));
            cx.notify();
        });
    }
    fn open_row(&mut self, index: usize, window: &mut Window, cx: &mut Context<Self>) {
        let Some(row) = self.store.rows.value().and_then(|page| page.rows.get(index)).cloned() else {
            return;
        };
        self.close_sensitive(window, cx);
        let reason = row.target.as_ref().map_or_else(
            |error| error.summary(),
            |_| "Use Review selected on the DLQ page to recover the verified original Topic, Group and Message ID.",
        );
        let detail = cx.new(|_| MessageDetail::read_only(self.services.clone(), self.revision, row.metadata, reason));
        self.detail = Some(detail.clone());
        let owner = cx.entity().downgrade();
        window.open_sheet(cx, move |sheet, _, _| {
            let owner = owner.clone();
            sheet
                .title("Dead-letter message details")
                .size(px(680.))
                .child(detail.clone())
                .on_close(move |_, _, cx| {
                    let _ = owner.update(cx, |view, cx| {
                        if let Some(detail) = view.detail.take() {
                            detail.update(cx, |detail, cx| detail.clear_sensitive(cx));
                        }
                        cx.notify();
                    });
                })
        });
        cx.notify();
    }
}

#[cfg(test)]
#[path = "dlq_tests.rs"]
mod tests;
