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

//! Real Message search, lazy inspector, and one explicitly confirmed consume attempt.

use crate::{
    components::topic_picker,
    features::dashboard_store::ResourceSlot,
    features::{
        message_detail::{MessageDetail, MessageDetailIntent, format_time},
        message_dialogs::{MessageConsumeForm, receipt_text},
        messages_store::{MessageQueryRequest, MessageSearchMode, MessagesStore},
    },
    services::{AppServices, messages::MessageRequestScope},
    state::{Loadable, UiError, UiErrorCode},
};
use gpui::{
    App, AppContext as _, Context, Entity, InteractiveElement as _, IntoElement, ParentElement as _, Render,
    Styled as _, Subscription, Task, WeakEntity, Window, div, prelude::FluentBuilder as _, px,
};
use gpui_component::{
    ActiveTheme as _, Disableable as _, Sizable as _, WindowExt as _,
    button::Button,
    dialog::{Dialog, DialogButtonProps},
    input::{Input, InputEvent, InputState},
    select::SelectEvent,
    table::{Column, Table, TableDelegate, TableState},
};
use rocketmq_dashboard_common::{
    ConnectionScope, MessageMetadataRow, MessagePageExtent, MessageSearchQuery, TopicInventory,
};

struct MessageTable {
    columns: Vec<Column>,
    rows: Vec<MessageMetadataRow>,
    loading: bool,
    owner: WeakEntity<MessagesView>,
}
impl TableDelegate for MessageTable {
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
            return div().child(
                Button::new(("message-open-row", row))
                    .label("Open")
                    .outline()
                    .small()
                    .disabled(self.loading)
                    .debug_selector(move || format!("message-open-row-{row}"))
                    .on_click(move |_, window, cx| {
                        let _ = owner.update(cx, |view, cx| view.open_row(row, window, cx));
                    }),
            );
        }
        let value = match column {
            1 => item.identity.message_id.clone(),
            2 => item.identity.topic.clone(),
            3 => format_time(item.store_timestamp),
            4 => format!("{} / {}", item.queue_id, item.queue_offset),
            5 => item.tags.as_ref().map_or_else(|| "-".into(), |value| value.display()),
            _ => item.keys.as_ref().map_or_else(|| "-".into(), |value| value.display()),
        };
        div().text_sm().px_2().overflow_hidden().text_ellipsis().child(value)
    }
    fn render_empty(&mut self, _: &mut Window, _: &mut Context<TableState<Self>>) -> impl IntoElement {
        div().p_4().child("No messages returned for the applied query.")
    }
}

pub struct MessagesView {
    services: AppServices,
    revision: u64,
    pub(super) store: MessagesStore,
    mode: MessageSearchMode,
    topic: Entity<InputState>,
    topic_picker: Entity<topic_picker::TopicPickerState>,
    topic_candidates: ResourceSlot<TopicInventory>,
    topic_task: Option<Task<()>>,
    key: Entity<InputState>,
    message_id: Entity<InputState>,
    begin: Entity<InputState>,
    end: Entity<InputState>,
    page_size: Entity<InputState>,
    table: Entity<TableState<MessageTable>>,
    detail: Option<Entity<MessageDetail>>,
    detail_subscription: Option<Subscription>,
    consume_form: Option<Entity<MessageConsumeForm>>,
    _subscriptions: Vec<Subscription>,
    query_task: Option<Task<()>>,
    // The receiver remains owned by this page when its inspector closes. P11 also owns admitted
    // execution and audit completion if sign-out destroys this page and drops the receiver.
    mutation_task: Option<Task<()>>,
    mutation_epoch: u64,
    status: Option<String>,
}

impl MessagesView {
    #[cfg(test)]
    pub(crate) fn search_is_idle(&self) -> bool {
        matches!(self.store.rows, Loadable::Idle)
    }
    #[cfg(test)]
    pub(crate) fn topic_input(&self) -> Entity<InputState> {
        self.topic.clone()
    }
    #[cfg(test)]
    pub(crate) fn detail_for_test(&self) -> Option<Entity<MessageDetail>> {
        self.detail.clone()
    }

    pub fn new(window: &mut Window, services: AppServices, revision: u64, cx: &mut Context<Self>) -> Self {
        let now = chrono::Utc::now();
        let topic = cx.new(|cx| InputState::new(window, cx).placeholder("Topic (required in every mode)"));
        let topic_picker = topic_picker::state(window, cx);
        let key = cx.new(|cx| InputState::new(window, cx).placeholder("Message Key"));
        let message_id = cx.new(|cx| InputState::new(window, cx).placeholder("Physical or producer Message ID"));
        let begin = cx.new(|cx| {
            InputState::new(window, cx).default_value(
                (now - chrono::TimeDelta::hours(1))
                    .format("%Y-%m-%d %H:%M:%S")
                    .to_string(),
            )
        });
        let end = cx.new(|cx| InputState::new(window, cx).default_value(now.format("%Y-%m-%d %H:%M:%S").to_string()));
        let page_size = cx.new(|cx| InputState::new(window, cx).default_value("25"));
        let owner = cx.entity().downgrade();
        let table = cx.new(|cx| {
            TableState::new(
                MessageTable {
                    columns: vec![
                        Column::new("actions", "Actions").width(px(70.)),
                        Column::new("id", "Message ID").width(px(210.)),
                        Column::new("topic", "Topic").width(px(130.)),
                        Column::new("stored", "Stored (UTC)").width(px(160.)),
                        Column::new("queue", "Queue / Offset").width(px(90.)),
                        Column::new("tags", "Tags").width(px(90.)),
                        Column::new("keys", "Keys").width(px(110.)),
                    ],
                    rows: Vec::new(),
                    loading: false,
                    owner,
                },
                window,
                cx,
            )
        });
        let mut subscriptions: Vec<Subscription> = [&topic, &key, &message_id, &begin, &end, &page_size]
            .into_iter()
            .map(|input| {
                cx.subscribe_in(input, window, |view, _, event, window, cx| {
                    if matches!(event, InputEvent::Change) {
                        view.draft_changed(window, cx);
                    }
                })
            })
            .collect();
        subscriptions.push(cx.subscribe_in(&topic_picker, window, |view, _, event, window, cx| {
            if let SelectEvent::Confirm(Some(topic)) = event {
                view.topic
                    .update(cx, |input, cx| input.set_value(topic.clone(), window, cx));
                view.draft_changed(window, cx);
            }
        }));
        Self {
            services,
            revision,
            store: MessagesStore::default(),
            mode: MessageSearchMode::TopicRange,
            topic,
            topic_picker,
            topic_candidates: ResourceSlot::default(),
            topic_task: None,
            key,
            message_id,
            begin,
            end,
            page_size,
            table,
            detail: None,
            detail_subscription: None,
            consume_form: None,
            _subscriptions: subscriptions,
            query_task: None,
            mutation_task: None,
            mutation_epoch: 0,
            status: None,
        }
    }
    pub fn set_revision(&mut self, revision: u64, window: &mut Window, cx: &mut Context<Self>) {
        if self.revision == revision {
            return;
        }
        self.revision = revision;
        self.clear_topics(window, cx);
        self.query_task = None;
        self.store.clear_context();
        self.close_sensitive(window, cx);
        self.status = None;
        for input in [&self.topic, &self.key, &self.message_id] {
            input.update(cx, |input, cx| input.set_value("", window, cx));
        }
        self.sync_table(cx);
        cx.notify();
    }
    pub fn leave(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.clear_topics(window, cx);
        self.query_task = None;
        self.store.cancel_reads();
        self.close_sensitive(window, cx);
        self.sync_table(cx);
        cx.notify();
    }
    pub fn ensure_topics(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if matches!(self.topic_candidates.state, Loadable::Idle) {
            self.load_topics(window, cx);
        }
    }
    fn clear_topics(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.topic_task = None;
        self.topic_candidates.clear();
        topic_picker::set_topics(&self.topic_picker, Vec::new(), window, cx);
    }
    fn load_topics(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.services.connection_state().config.scope != ConnectionScope::NameServer {
            return;
        }
        let Some(request) = self.topic_candidates.begin(self.revision) else {
            return;
        };
        let services = self.services.clone();
        self.topic_task = Some(cx.spawn_in(window, async move |this, cx| {
            let result = services
                .topic_inventory(crate::services::topics::TopicRequestScope {
                    revision: request.revision(),
                    epoch: request.epoch(),
                })
                .await
                .map(Some);
            let _ = this.update_in(cx, |view, window, cx| {
                if !view.topic_candidates.finish(request, view.revision, result) {
                    return;
                }
                let topics = view.topic_candidates.state.value().map_or_else(Vec::new, |inventory| {
                    inventory
                        .items
                        .iter()
                        .map(|item| item.identity.as_str().to_string())
                        .collect()
                });
                topic_picker::set_topics(&view.topic_picker, topics, window, cx);
                cx.notify();
            });
        }));
        cx.notify();
    }
    fn draft_changed(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.query_task = None;
        self.store.edit_draft();
        self.close_sensitive(window, cx);
        self.status = None;
        self.sync_table(cx);
        cx.notify();
    }
    fn close_sensitive(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if let Some(detail) = self.detail.take() {
            detail.update(cx, |detail, cx| detail.clear_sensitive(cx));
            window.close_sheet(cx);
        }
        self.detail_subscription = None;
        if let Some(form) = self.consume_form.take() {
            form.update(cx, |form, cx| form.clear(window, cx));
            window.close_all_dialogs(cx);
        }
        self.store.selected = None;
    }
    fn draft_query(&self, cx: &Context<Self>) -> Result<MessageSearchQuery, UiError> {
        let topic = self.topic.read(cx).value().trim().to_string();
        let query = match self.mode {
            MessageSearchMode::TopicRange => MessageSearchQuery::TopicRange {
                topic,
                begin: parse_time(self.begin.read(cx).value().as_str())?,
                end: parse_time(self.end.read(cx).value().as_str())?,
                page: 1,
                page_size: self
                    .page_size
                    .read(cx)
                    .value()
                    .parse()
                    .map_err(|_| validation("Page size must be an integer from 1 to 50."))?,
            },
            MessageSearchMode::Key => MessageSearchQuery::Key {
                topic,
                key: self.key.read(cx).value().trim().to_string(),
            },
            MessageSearchMode::MessageId => MessageSearchQuery::MessageId {
                topic,
                message_id: self.message_id.read(cx).value().trim().to_string(),
            },
        };
        query.validate().map_err(crate::services::messages::validation_error)?;
        Ok(query)
    }
    fn search(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let query = match self.draft_query(cx) {
            Ok(query) => query,
            Err(error) => {
                self.status = Some(error.summary().into());
                cx.notify();
                return;
            }
        };
        self.close_sensitive(window, cx);
        if let Some(request) = self.store.apply(query, self.revision) {
            self.start_query(request, window, cx);
        }
    }
    fn refresh(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.close_sensitive(window, cx);
        if let Some(request) = self.store.begin(self.revision) {
            self.start_query(request, window, cx);
        }
    }
    fn page(&mut self, next: bool, window: &mut Window, cx: &mut Context<Self>) {
        let Some(query) = self.store.applied.as_ref() else {
            return;
        };
        let Some(number) = (if next {
            query.page().checked_add(1)
        } else {
            query.page().checked_sub(1)
        }) else {
            return;
        };
        let Ok(query) = query.with_page(number) else {
            return;
        };
        self.close_sensitive(window, cx);
        if let Some(request) = self.store.apply(query, self.revision) {
            self.start_query(request, window, cx);
        }
    }
    fn start_query(&mut self, request: MessageQueryRequest, window: &mut Window, cx: &mut Context<Self>) {
        let services = self.services.clone();
        self.status = None;
        self.sync_table(cx);
        self.query_task = Some(cx.spawn_in(window, async move |this, cx| {
            let result = services.query_messages(request.scope, request.query.clone()).await;
            let _ = this.update_in(cx, |view, _, cx| {
                if view.store.finish(&request, view.revision, result) {
                    view.sync_table(cx);
                    cx.notify();
                }
            });
        }));
    }
    fn sync_table(&self, cx: &mut Context<Self>) {
        let rows = self.store.rows.value().map_or_else(Vec::new, |page| page.rows.clone());
        let loading = matches!(self.store.rows, Loadable::InitialLoading | Loadable::Refreshing(_));
        self.table.update(cx, |table, cx| {
            table.delegate_mut().rows = rows;
            table.delegate_mut().loading = loading;
            cx.notify();
        });
    }
    fn open_row(&mut self, index: usize, window: &mut Window, cx: &mut Context<Self>) {
        if matches!(self.store.rows, Loadable::InitialLoading | Loadable::Refreshing(_)) {
            return;
        }
        let Some(row) = self.store.rows.value().and_then(|page| page.rows.get(index)).cloned() else {
            return;
        };
        self.close_sensitive(window, cx);
        if self.store.select(&row.identity).is_none() {
            return;
        }
        let detail = cx.new(|_| MessageDetail::new(self.services.clone(), self.revision, row));
        self.detail_subscription = Some(
            cx.subscribe_in(&detail, window, |view, _, event, window, cx| match event {
                MessageDetailIntent::Consume => view.open_consume(window, cx),
            }),
        );
        self.detail = Some(detail.clone());
        let owner = cx.entity().downgrade();
        window.open_sheet(cx, move |sheet, _, _| {
            let owner = owner.clone();
            sheet
                .title("Message details")
                .size(px(680.))
                .child(detail.clone())
                .on_close(move |_, window, cx| {
                    let _ = owner.update(cx, |view, cx| {
                        if let Some(detail) = view.detail.take() {
                            detail.update(cx, |detail, cx| detail.clear_sensitive(cx));
                        }
                        view.detail_subscription = None;
                        view.store.selected = None;
                        if let Some(form) = view.consume_form.take() {
                            form.update(cx, |form, cx| form.clear(window, cx));
                            window.close_all_dialogs(cx);
                        }
                        cx.notify();
                    });
                })
        });
        cx.notify();
    }
    fn open_consume(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.consume_form.is_some() || self.mutation_task.is_some() {
            return;
        }
        let Some(row) = self.store.selected.as_ref() else {
            return;
        };
        let state = self.services.connection_state();
        let scope = state.config.scope;
        let environment = state
            .environments
            .as_ref()
            .and_then(|catalog| catalog.profiles.iter().find(|profile| profile.id == catalog.active_id))
            .map_or("Local desktop", |profile| profile.name.as_str());
        let connection_label = format!(
            "Environment: {environment} · NameServer: {} · revision {}",
            state.config.current_nameserver.as_deref().unwrap_or("not configured"),
            self.revision
        );
        let form = cx.new(|cx| MessageConsumeForm::new(row.identity.clone(), scope, connection_label, window, cx));
        self.consume_form = Some(form.clone());
        let owner = cx.entity().downgrade();
        window.open_dialog(cx, move |dialog: Dialog, _, _| {
            let submit = owner.clone();
            let close = owner.clone();
            let footer = form.clone();
            let clear = form.clone();
            dialog
                .title("Confirm direct consume")
                .confirm()
                .button_props(
                    DialogButtonProps::default()
                        .ok_text("Consume once")
                        .cancel_text("Close"),
                )
                .child(form.clone())
                .footer(move |ok, cancel, window, cx| {
                    vec![
                        div()
                            .debug_selector(|| "message-consume-cancel".into())
                            .child(cancel(window, cx)),
                        div()
                            .debug_selector(|| "message-consume-confirm".into())
                            .child(if footer.read(cx).submitted {
                                Button::new("message-consume-submitted")
                                    .label("Submitted")
                                    .disabled(true)
                                    .into_any_element()
                            } else {
                                ok(window, cx)
                            }),
                    ]
                })
                .on_ok(move |_, window, cx| {
                    let _ = submit.update(cx, |view, cx| view.submit_consume(window, cx));
                    false
                })
                .on_close(move |_, window, cx| {
                    clear.update(cx, |form, cx| form.clear(window, cx));
                    let _ = close.update(cx, |view, cx| {
                        view.consume_form = None;
                        cx.notify();
                    });
                })
        });
        cx.notify();
    }
    fn submit_consume(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.mutation_task.is_some() {
            return;
        }
        let Some(form) = self.consume_form.clone() else {
            return;
        };
        if form.read(cx).submitted {
            return;
        }
        let command = match form.read(cx).command(cx) {
            Ok(command) => command,
            Err(error) => {
                form.update(cx, |form, cx| {
                    form.status = Some(error.summary().into());
                    cx.notify();
                });
                return;
            }
        };
        self.mutation_epoch = self.mutation_epoch.saturating_add(1);
        let scope = MessageRequestScope {
            revision: self.revision,
            epoch: self.mutation_epoch,
        };
        form.update(cx, |form, cx| {
            form.submitted = true;
            form.status = Some("Direct consume is executing…".into());
            cx.notify();
        });
        let services = self.services.clone();
        self.mutation_task = Some(cx.spawn_in(window, async move |this, cx| {
            let result = services.consume_message(scope, command).await;
            let _ = this.update_in(cx, |view, _, cx| {
                view.mutation_task = None;
                cx.notify();
                if view.revision != scope.revision {
                    return;
                }
                let status = match result {
                    Ok(receipt) => receipt_text(receipt),
                    Err(error) => error.summary().into(),
                };
                view.status = Some(status.clone());
                if view
                    .consume_form
                    .as_ref()
                    .is_some_and(|current| current.entity_id() == form.entity_id())
                {
                    form.update(cx, |form, cx| {
                        form.status = Some(status);
                        cx.notify();
                    });
                }
            });
        }));
        cx.notify();
    }
}

impl Render for MessagesView {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let direct = self.services.connection_state().config.scope == ConnectionScope::NameServer;
        let page = self.store.applied.as_ref().map_or(1, MessageSearchQuery::page);
        let is_range = matches!(self.store.applied, Some(MessageSearchQuery::TopicRange { .. }));
        let next = self
            .store
            .rows
            .value()
            .is_some_and(|page| page.extent == MessagePageExtent::MoreMayExist);
        let loading = matches!(self.store.rows, Loadable::InitialLoading | Loadable::Refreshing(_));
        let modes = div().flex().flex_wrap().gap_2().children(
            [
                ("message-mode-range", "Topic / Time", MessageSearchMode::TopicRange),
                ("message-mode-key", "Key", MessageSearchMode::Key),
                ("message-mode-id", "Message ID", MessageSearchMode::MessageId),
            ]
            .into_iter()
            .map(|(id, label, mode)| {
                Button::new(id)
                    .label(label)
                    .outline()
                    .disabled(self.mode == mode)
                    .debug_selector(move || id.into())
                    .on_click(cx.listener(move |view, _, window, cx| {
                        view.mode = mode;
                        view.draft_changed(window, cx);
                    }))
            }),
        );
        let range = div()
            .w_full()
            .flex_shrink_0()
            .flex()
            .flex_wrap()
            .gap_2()
            .child(
                div()
                    .debug_selector(|| "message-begin-field".into())
                    .flex_1()
                    .min_w(px(240.))
                    .child("Begin UTC (YYYY-MM-DD HH:MM:SS)")
                    .child(Input::new(&self.begin)),
            )
            .child(
                div()
                    .debug_selector(|| "message-end-field".into())
                    .flex_1()
                    .min_w(px(240.))
                    .child("End UTC (inclusive)")
                    .child(Input::new(&self.end)),
            )
            .child(
                div()
                    .debug_selector(|| "message-page-size-field".into())
                    .w(px(100.))
                    .flex_shrink_0()
                    .child("Rows / page")
                    .child(Input::new(&self.page_size)),
            );
        let controls = div()
            .flex()
            .flex_wrap()
            .items_center()
            .gap_2()
            .child(
                Button::new("message-search")
                    .label("Search")
                    .outline()
                    .disabled(!direct)
                    .debug_selector(|| "message-search".into())
                    .on_click(cx.listener(|view, _, window, cx| view.search(window, cx))),
            )
            .child(
                Button::new("message-refresh")
                    .label("Refresh applied query")
                    .outline()
                    .disabled(self.store.applied.is_none() || !direct)
                    .debug_selector(|| "message-refresh".into())
                    .on_click(cx.listener(|view, _, window, cx| view.refresh(window, cx))),
            )
            .child(
                Button::new("message-previous-page")
                    .label("Previous")
                    .outline()
                    .disabled(!is_range || page <= 1 || loading)
                    .debug_selector(|| "message-previous-page".into())
                    .on_click(cx.listener(|view, _, window, cx| view.page(false, window, cx))),
            )
            .child(format!("Page {page}"))
            .child(
                Button::new("message-next-page")
                    .label("Next (if available)")
                    .outline()
                    .disabled(!next || loading)
                    .debug_selector(|| "message-next-page".into())
                    .on_click(cx.listener(|view, _, window, cx| view.page(true, window, cx))),
            );
        let error = match &self.store.rows {
            Loadable::Failed { error, .. } => Some(
                div()
                    .text_sm()
                    .text_color(cx.theme().danger)
                    .child(error.summary().to_string()),
            ),
            Loadable::Idle => Some(div().child("Choose a query mode, enter a Topic and Search.")),
            _ => None,
        };
        let limit = self.store.rows.value().and_then(|page| match page.extent {
            MessagePageExtent::KeyLimitReached => {
                Some("Key results reached the 64-match limit; additional matches may exist.")
            }
            MessagePageExtent::End | MessagePageExtent::MoreMayExist => None,
        });
        div().id("messages-workspace").debug_selector(|| "messages-workspace".into())
            .size_full().p_4().flex().flex_col().gap_3()
            .child(modes)
            .child(div().flex().flex_wrap().gap_2().items_center()
                .child(div().id("message-topic-picker").debug_selector(||"message-topic-picker".into()).w(px(280.)).child(topic_picker::render(&self.topic_picker)))
                .child(div().flex_1().min_w(px(180.)).child(Input::new(&self.topic)))
                .child(Button::new("message-topics-refresh").label("Refresh Topics").outline().disabled(!direct)
                    .debug_selector(||"message-topics-refresh".into()).on_click(cx.listener(|view,_,window,cx|view.load_topics(window,cx)))))
            .child(div().text_xs().child(match &self.topic_candidates.state {
                Loadable::InitialLoading|Loadable::Refreshing(_)=>"Loading Topic candidates… Manual Topic entry is available.",
                Loadable::Failed{..}=>"Topic discovery failed. Enter a valid Topic manually or refresh the candidates.",
                Loadable::Ready(inventory) if !inventory.completeness.is_complete()=>"Topic discovery is partial. Additional Topics can be entered manually.",
                Loadable::Ready(_)|Loadable::Empty=>"Choose a discovered Topic or enter one manually.",
                Loadable::Idle=>"Refresh Topics to discover candidates, or enter a Topic manually.",
            }))
            .when(self.mode == MessageSearchMode::Key, |this| this.child(
                div().debug_selector(|| "message-key-field".into()).w_full().min_w_0().flex_shrink_0()
                    .child(Input::new(&self.key).w_full())))
            .when(self.mode == MessageSearchMode::MessageId, |this| this.child(
                div().debug_selector(|| "message-id-field".into()).w_full().min_w_0().flex_shrink_0()
                    .child(Input::new(&self.message_id).w_full())))
            .when(self.mode == MessageSearchMode::TopicRange, |this| this.child(range))
            .child(controls)
            .child(div().text_xs().text_color(cx.theme().muted_foreground).child(if direct {
                "NameServer Direct · Range pages scan Broker queues; only each returned page is sorted by time. No exact total is inferred. Key queries return at most 64 matches."
            } else {
                "Messages require NameServer Direct scope; Proxy forwarding is unavailable."
            }))
            .when(self.store.draft_changed, |this| this.child("Draft changed. Search applies it; Refresh replays the previous applied query."))
            .children(self.status.as_ref().map(|status| div().text_sm().child(status.clone())))
            .children(error)
            .children(limit.map(|text| div().text_sm().child(text)))
            .child(div().flex_1().min_h_0().child(Table::new(&self.table)))
    }
}

fn validation(summary: &'static str) -> UiError {
    UiError::new(summary, UiErrorCode::Validation, false)
}
pub(super) fn parse_time(value: &str) -> Result<i64, UiError> {
    chrono::NaiveDateTime::parse_from_str(value.trim(), "%Y-%m-%d %H:%M:%S")
        .map(|time| time.and_utc().timestamp_millis())
        .map_err(|_| validation("Use YYYY-MM-DD HH:MM:SS for UTC timestamps."))
}
#[cfg(test)]
#[path = "messages_tests.rs"]
mod tests;
