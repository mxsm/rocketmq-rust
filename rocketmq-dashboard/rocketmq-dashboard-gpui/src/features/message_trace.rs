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

//! Candidate discovery and real Trace observations in a native read-only workspace.

use crate::{
    components::{topic_picker, trace_timeline},
    features::{
        dashboard_store::ResourceSlot,
        message_detail::format_time,
        message_trace_store::{MessageTraceStore, TraceRequest},
        messages_store::MessageQueryRequest,
    },
    services::{AppServices, topics::TopicRequestScope},
    state::{Loadable, UiError, UiErrorCode},
};
use gpui::{
    App, AppContext as _, Context, Entity, InteractiveElement as _, IntoElement, ParentElement as _, Render,
    Styled as _, Subscription, Task, WeakEntity, Window, div, px,
};
use gpui_component::{
    ActiveTheme as _, Disableable as _, Sizable as _,
    button::Button,
    input::{Input, InputEvent, InputState},
    scroll::ScrollableElement as _,
    select::SelectEvent,
    table::{Column, Table, TableDelegate, TableState},
};
use rocketmq_dashboard_common::{
    ConnectionScope, DEFAULT_TRACE_TOPIC, MessageMetadataRow, MessagePageExtent, MessageSearchQuery, TopicIdentity,
    TopicInventory,
};

#[derive(Clone, Copy, PartialEq, Eq, Default)]
enum CandidateMode {
    #[default]
    MessageId,
    Key,
}

struct CandidateTable {
    columns: Vec<Column>,
    rows: Vec<MessageMetadataRow>,
    owner: WeakEntity<MessageTraceView>,
}
impl TableDelegate for CandidateTable {
    fn columns_count(&self, _: &App) -> usize {
        self.columns.len()
    }
    fn rows_count(&self, _: &App) -> usize {
        self.rows.len()
    }
    fn column(&self, index: usize, _: &App) -> &Column {
        &self.columns[index]
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
                Button::new(("trace-select-row", row))
                    .label("Load Trace")
                    .outline()
                    .small()
                    .debug_selector(move || format!("trace-select-row-{row}"))
                    .on_click(move |_, window, cx| {
                        let _ = owner.update(cx, |view, cx| view.select(row, window, cx));
                    }),
            );
        }
        let value = match column {
            1 => item.identity.message_id.clone(),
            2 => item.identity.topic.clone(),
            3 => format_time(item.store_timestamp),
            _ => item.keys.as_ref().map_or_else(|| "-".into(), |key| key.display()),
        };
        div().px_2().text_sm().overflow_hidden().text_ellipsis().child(value)
    }
    fn render_empty(&mut self, _: &mut Window, _: &mut Context<TableState<Self>>) -> impl IntoElement {
        div().p_3().child("No candidate messages returned.")
    }
}

pub(crate) struct MessageTraceView {
    services: AppServices,
    revision: u64,
    store: MessageTraceStore,
    mode: CandidateMode,
    topic: Entity<InputState>,
    identifier: Entity<InputState>,
    trace_topic: Entity<InputState>,
    topic_picker: Entity<topic_picker::TopicPickerState>,
    topics: ResourceSlot<TopicInventory>,
    table: Entity<TableState<CandidateTable>>,
    candidate_task: Option<Task<()>>,
    trace_task: Option<Task<()>>,
    topic_task: Option<Task<()>>,
    _subscriptions: Vec<Subscription>,
    validation: Option<String>,
}

impl MessageTraceView {
    #[cfg(test)]
    pub(crate) fn query_is_idle(&self) -> bool {
        matches!(self.store.candidates.rows, Loadable::Idle) && matches!(self.store.trace.state, Loadable::Idle)
    }
    pub fn new(window: &mut Window, services: AppServices, revision: u64, cx: &mut Context<Self>) -> Self {
        let topic = cx.new(|cx| InputState::new(window, cx).placeholder("Message Topic"));
        let identifier = cx.new(|cx| InputState::new(window, cx).placeholder("Physical or producer Message ID"));
        let trace_topic = cx.new(|cx| InputState::new(window, cx).default_value(DEFAULT_TRACE_TOPIC));
        let topic_picker = topic_picker::state(window, cx);
        let owner = cx.entity().downgrade();
        let table = cx.new(|cx| {
            TableState::new(
                CandidateTable {
                    columns: vec![
                        Column::new("select", "Action").width(px(110.)),
                        Column::new("id", "Physical Message ID").width(px(300.)),
                        Column::new("topic", "Topic").width(px(160.)),
                        Column::new("stored", "Stored (UTC)").width(px(170.)),
                        Column::new("keys", "Keys").width(px(180.)),
                    ],
                    rows: Vec::new(),
                    owner,
                },
                window,
                cx,
            )
        });
        let mut subscriptions = [&topic, &identifier]
            .into_iter()
            .map(|input| {
                cx.subscribe_in(input, window, |view, _, event, _, cx| {
                    if matches!(event, InputEvent::Change) {
                        view.invalidate_candidates(cx)
                    }
                })
            })
            .collect::<Vec<_>>();
        subscriptions.push(cx.subscribe(&trace_topic, |view, _, event, cx| {
            if matches!(event, InputEvent::Change) {
                view.trace_task = None;
                view.store.trace.clear();
                view.validation = None;
                cx.notify();
            }
        }));
        subscriptions.push(cx.subscribe_in(&topic_picker, window, |view, _, event, window, cx| {
            if let SelectEvent::Confirm(Some(topic)) = event {
                view.topic
                    .update(cx, |input, cx| input.set_value(topic.clone(), window, cx));
                view.invalidate_candidates(cx);
            }
        }));
        Self {
            services,
            revision,
            store: MessageTraceStore::default(),
            mode: CandidateMode::MessageId,
            topic,
            identifier,
            trace_topic,
            topic_picker,
            topics: ResourceSlot::default(),
            table,
            candidate_task: None,
            trace_task: None,
            topic_task: None,
            _subscriptions: subscriptions,
            validation: None,
        }
    }
    pub fn set_revision(&mut self, revision: u64, window: &mut Window, cx: &mut Context<Self>) {
        if self.revision == revision {
            return;
        }
        self.revision = revision;
        self.leave(window, cx);
        self.store.clear_context();
        for input in [&self.topic, &self.identifier] {
            input.update(cx, |input, cx| input.set_value("", window, cx));
        }
        self.trace_topic
            .update(cx, |input, cx| input.set_value(DEFAULT_TRACE_TOPIC, window, cx));
        self.validation = None;
    }
    pub fn leave(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.candidate_task = None;
        self.trace_task = None;
        self.topic_task = None;
        self.store.leave();
        self.topics.clear();
        topic_picker::set_topics(&self.topic_picker, Vec::new(), window, cx);
        self.sync_table(cx);
        cx.notify();
    }
    pub fn ensure_topics(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if matches!(self.topics.state, Loadable::Idle) {
            self.load_topics(window, cx)
        }
    }
    fn load_topics(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.services.connection_state().config.scope != ConnectionScope::NameServer {
            return;
        }
        let Some(request) = self.topics.begin(self.revision) else {
            return;
        };
        let services = self.services.clone();
        self.topic_task = Some(cx.spawn_in(window, async move |this, cx| {
            let result = services
                .topic_inventory(TopicRequestScope {
                    revision: request.revision(),
                    epoch: request.epoch(),
                })
                .await
                .map(Some);
            let _ = this.update_in(cx, |view, window, cx| {
                if !view.topics.finish(request, view.revision, result) {
                    return;
                }
                let topics = view.topics.state.value().map_or_else(Vec::new, |inventory| {
                    inventory
                        .items
                        .iter()
                        .map(|item| item.identity.as_str().to_owned())
                        .collect()
                });
                topic_picker::set_topics(&view.topic_picker, topics, window, cx);
                cx.notify();
            });
        }));
        cx.notify();
    }
    fn invalidate_candidates(&mut self, cx: &mut Context<Self>) {
        self.candidate_task = None;
        self.trace_task = None;
        self.store.edit_candidates();
        self.validation = None;
        self.sync_table(cx);
        cx.notify();
    }
    fn query(&self, cx: &Context<Self>) -> Result<MessageSearchQuery, UiError> {
        let topic = self.topic.read(cx).value().trim().to_owned();
        let identifier = self.identifier.read(cx).value().trim().to_owned();
        let query = match self.mode {
            CandidateMode::MessageId => MessageSearchQuery::MessageId {
                topic,
                message_id: identifier,
            },
            CandidateMode::Key => MessageSearchQuery::Key { topic, key: identifier },
        };
        query.validate().map_err(crate::services::messages::validation_error)?;
        TopicIdentity::parse(self.trace_topic.read(cx).value().trim())
            .map_err(|_| UiError::new("A valid Trace Topic is required.", UiErrorCode::Validation, false))?;
        Ok(query)
    }
    fn find(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        match self.query(cx) {
            Ok(query) => {
                self.trace_task = None;
                self.validation = None;
                if let Some(request) = self.store.apply(query, self.revision) {
                    self.start_candidates(request, window, cx);
                }
            }
            Err(error) => {
                self.validation = Some(error.summary().into());
                cx.notify();
            }
        }
    }
    fn start_candidates(&mut self, request: MessageQueryRequest, window: &mut Window, cx: &mut Context<Self>) {
        let services = self.services.clone();
        self.candidate_task = Some(cx.spawn_in(window, async move |this, cx| {
            let result = services.query_messages(request.scope, request.query.clone()).await;
            let _ = this.update_in(cx, |view, _, cx| {
                if view.store.finish_candidates(&request, view.revision, result) {
                    view.candidate_task = None;
                    view.sync_table(cx);
                    cx.notify();
                }
            });
        }));
        self.sync_table(cx);
        cx.notify();
    }
    fn select(&mut self, index: usize, window: &mut Window, cx: &mut Context<Self>) {
        let Some(row) = self.store.candidates.rows.value().and_then(|page| page.rows.get(index)) else {
            return;
        };
        let identity = row.identity.clone();
        let topic = self.trace_topic.read(cx).value().trim().to_owned();
        if let Some(request) = self.store.select(&identity, topic, self.revision) {
            self.start_trace(request, window, cx);
        }
    }
    fn reload_trace(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(identity) = self.store.candidates.selected.as_ref().map(|row| row.identity.clone()) else {
            return;
        };
        let topic = self.trace_topic.read(cx).value().trim().to_owned();
        if let Some(request) = self.store.trace.begin(identity, topic, self.revision) {
            self.start_trace(request, window, cx);
        }
    }
    fn start_trace(&mut self, request: TraceRequest, window: &mut Window, cx: &mut Context<Self>) {
        let services = self.services.clone();
        self.trace_task = Some(cx.spawn_in(window, async move |this, cx| {
            let result = services.message_trace(request.scope, request.query.clone()).await;
            let _ = this.update_in(cx, |view, _, cx| {
                if view.store.trace.finish(&request, view.revision, result) {
                    view.trace_task = None;
                    cx.notify();
                }
            });
        }));
        cx.notify();
    }
    fn sync_table(&self, cx: &mut Context<Self>) {
        let rows = self
            .store
            .candidates
            .rows
            .value()
            .map_or_else(Vec::new, |page| page.rows.clone());
        self.table.update(cx, |table, cx| {
            table.delegate_mut().rows = rows;
            cx.notify();
        });
    }
}

impl Render for MessageTraceView {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let direct = self.services.connection_state().config.scope == ConnectionScope::NameServer;
        let loading = matches!(
            self.store.candidates.rows,
            Loadable::InitialLoading | Loadable::Refreshing(_)
        );
        let mut body = div().id("message-trace-workspace").debug_selector(|| "message-trace-workspace".into())
            .size_full().min_w_0().flex().flex_col().gap_3().p_4().overflow_y_scrollbar()
            .text_color(cx.theme().foreground)
            .child(div().text_xl().child("Message Trace"))
            .child(div().w_full().min_w_0().flex_shrink_0().whitespace_normal()
                .child("Find a real message, then select it to read client-reported Trace records. NameServer Direct reads only."))
            .child(div().w_full().flex_shrink_0().flex().flex_wrap().gap_2().children([
                ("trace-mode-id", "By Message ID", CandidateMode::MessageId),
                ("trace-mode-key", "By Key", CandidateMode::Key),
            ].into_iter().map(|(id, label, mode)| Button::new(id).label(label).outline()
                .disabled(self.mode == mode).debug_selector(move || id.into())
                .on_click(cx.listener(move |view, _, _, cx| { view.mode = mode; view.invalidate_candidates(cx); })))))
            .child(div().w_full().min_w_0().flex_shrink_0().flex().flex_wrap().gap_2()
                .child(div().flex_1().min_w(px(240.)).child("Message Topic").child(Input::new(&self.topic).disabled(!direct))
                    .debug_selector(|| "trace-message-topic-field".into()))
                .child(div().flex_1().min_w(px(240.)).child("Discovered Topics").child(topic_picker::render(&self.topic_picker))))
            .child(div().w_full().flex_shrink_0().child(if self.mode == CandidateMode::Key { "Message Key" } else { "Message ID" })
                .child(Input::new(&self.identifier).disabled(!direct)).debug_selector(|| "trace-identifier-field".into()))
            .child(div().w_full().flex_shrink_0().child("Trace Topic").child(Input::new(&self.trace_topic).disabled(!direct))
                .debug_selector(|| "trace-topic-field".into()))
            .child(div().w_full().flex_shrink_0().flex().flex_wrap().gap_2()
                .child(Button::new("trace-find").label("Find candidates").outline().disabled(!direct || loading)
                    .debug_selector(|| "trace-find".into()).on_click(cx.listener(|view, _, window, cx| view.find(window, cx))))
                .child(Button::new("trace-reload").label("Reload selected Trace").outline()
                    .disabled(!direct || self.store.candidates.selected.is_none())
                    .debug_selector(|| "trace-reload".into()).on_click(cx.listener(|view, _, window, cx| view.reload_trace(window, cx)))));
        if !direct {
            body = body.child("Trace is unavailable in Proxy scope; no forwarding capability is configured.");
        }
        if let Loadable::Failed { error, .. } = &self.topics.state {
            body = body.child(
                div()
                    .flex_shrink_0()
                    .flex()
                    .flex_col()
                    .gap_2()
                    .child(format!(
                        "Topic discovery failed: {} Enter an explicit Topic above.",
                        error.summary()
                    ))
                    .child(
                        Button::new("trace-topics-retry")
                            .label("Retry Topic discovery")
                            .outline()
                            .on_click(cx.listener(|view, _, window, cx| view.load_topics(window, cx))),
                    ),
            );
        }
        if let Some(error) = &self.validation {
            body = body.child(div().child(error.clone()));
        }
        match &self.store.candidates.rows {
            Loadable::InitialLoading | Loadable::Refreshing(_) => body = body.child("Loading candidate messages…"),
            Loadable::Failed { error, .. } => {
                body = body.child(
                    div()
                        .debug_selector(|| "trace-candidates-error".into())
                        .child(error.summary().to_owned()),
                )
            }
            _ => {}
        }
        if self
            .store
            .candidates
            .rows
            .value()
            .is_some_and(|page| page.extent != MessagePageExtent::End)
        {
            body = body.child(
                div()
                    .debug_selector(|| "trace-candidates-truncated".into())
                    .child("Candidate limit reached (Key: 64, ID: 1). Additional matches may exist."),
            );
        }
        body.child(
            div()
                .w_full()
                .h(px(230.))
                .flex_shrink_0()
                .child(Table::new(&self.table)),
        )
        .child(
            div()
                .w_full()
                .min_w_0()
                .flex_shrink_0()
                .child(trace_timeline::render(&self.store.trace.state, cx)),
        )
    }
}

#[cfg(test)]
#[path = "message_trace_tests.rs"]
mod tests;
