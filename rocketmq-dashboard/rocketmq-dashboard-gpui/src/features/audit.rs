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

//! Read-only local Audit filters, stable pages and bounded record details.

use gpui::{
    AppContext as _, Context, Entity, InteractiveElement as _, IntoElement, ParentElement as _, Render, SharedString,
    StatefulInteractiveElement as _, Styled as _, Subscription, Task, Window, div, prelude::FluentBuilder as _, px,
};
use gpui_component::{
    ActiveTheme as _, Disableable as _, WindowExt as _,
    button::Button,
    dialog::{Dialog, DialogButtonProps},
    input::{Input, InputEvent, InputState},
    select::{Select, SelectEvent, SelectItem, SelectState},
};
use rocketmq_dashboard_common::{AuditAction, AuditFilter, AuditOutcome, AuditQuery, EnvironmentId, OperationActor};

use super::{
    audit_detail::{AuditDetail, AuditRecord},
    audit_presentation::{ACTIONS, OUTCOMES, action_label, actor_label, outcome_label, resource_label, timestamp},
    audit_query_store::AuditQueryStore,
};
use crate::{
    services::{AppServices, audit::AuditReadAccess},
    state::{RequestEpoch, UiError, UiErrorCode},
};

#[derive(Clone)]
struct Choice<T: Clone> {
    label: SharedString,
    value: T,
}
impl<T: Clone> SelectItem for Choice<T> {
    type Value = T;
    fn title(&self) -> SharedString {
        self.label.clone()
    }
    fn value(&self) -> &T {
        &self.value
    }
}
type Picker<T> = SelectState<Vec<Choice<T>>>;

#[derive(Clone, Copy, PartialEq, Eq)]
enum ActorKind {
    Any,
    Desktop,
    System,
    Operator,
}

fn choice<T: Clone>(label: impl Into<SharedString>, value: T) -> Choice<T> {
    Choice {
        label: label.into(),
        value,
    }
}
fn picker<T: Clone + PartialEq + 'static>(
    items: Vec<Choice<T>>,
    value: &T,
    window: &mut Window,
    cx: &mut Context<AuditView>,
) -> Entity<Picker<T>> {
    cx.new(|cx| {
        let mut picker = SelectState::new(items, None, window, cx);
        picker.set_selected_value(value, window, cx);
        picker
    })
}

pub(crate) struct AuditView {
    services: AppServices,
    store: AuditQueryStore,
    start: Entity<InputState>,
    end: Entity<InputState>,
    username: Entity<InputState>,
    action: Entity<Picker<Option<AuditAction>>>,
    outcome: Entity<Picker<Option<AuditOutcome>>>,
    actor: Entity<Picker<ActorKind>>,
    environment: Entity<Picker<Option<EnvironmentId>>>,
    draft_changed: bool,
    validation: Option<UiError>,
    active: bool,
    epoch: RequestEpoch,
    query_task: Option<Task<()>>,
    read_access: Option<AuditReadAccess>,
    #[cfg(test)]
    query_gate: Option<(tokio::sync::oneshot::Sender<()>, tokio::sync::oneshot::Receiver<()>)>,
    detail: Option<Entity<AuditDetail>>,
    _subscriptions: Vec<Subscription>,
}

impl AuditView {
    pub fn new(window: &mut Window, services: AppServices, cx: &mut Context<Self>) -> Self {
        let filter = AuditFilter::recent(now());
        let start = cx.new(|cx| InputState::new(window, cx).default_value(input_time(filter.start_epoch_ms)));
        let end = cx.new(|cx| InputState::new(window, cx).default_value(input_time(filter.end_epoch_ms)));
        let username = cx.new(|cx| InputState::new(window, cx).placeholder("Exact local operator username"));
        let mut actions = vec![choice("Any action", None)];
        actions.extend(
            ACTIONS
                .into_iter()
                .map(|action| choice(action_label(action), Some(action))),
        );
        let action = picker(actions, &None, window, cx);
        let mut outcomes = vec![choice("Any outcome", None)];
        outcomes.extend(
            OUTCOMES
                .into_iter()
                .map(|outcome| choice(outcome_label(outcome), Some(outcome))),
        );
        let outcome = picker(outcomes, &None, window, cx);
        let actor = picker(
            vec![
                choice("Any actor", ActorKind::Any),
                choice("Local desktop", ActorKind::Desktop),
                choice("System", ActorKind::System),
                choice("Local operator", ActorKind::Operator),
            ],
            &ActorKind::Any,
            window,
            cx,
        );
        let (environments, selected) = environment_choices(&services);
        let environment = picker(environments, &selected, window, cx);
        let mut subscriptions = [&start, &end, &username]
            .into_iter()
            .map(|input| {
                cx.subscribe_in(input, window, |view, _, event, window, cx| {
                    if matches!(event, InputEvent::Change) {
                        view.edit_draft(window, cx);
                    }
                    if matches!(event, InputEvent::PressEnter { .. }) {
                        view.apply(window, cx);
                    }
                })
            })
            .collect::<Vec<_>>();
        subscriptions.push(cx.subscribe_in(
            &action,
            window,
            |view, _, _: &SelectEvent<Vec<Choice<Option<AuditAction>>>>, window, cx| view.edit_draft(window, cx),
        ));
        subscriptions.push(cx.subscribe_in(
            &outcome,
            window,
            |view, _, _: &SelectEvent<Vec<Choice<Option<AuditOutcome>>>>, window, cx| view.edit_draft(window, cx),
        ));
        subscriptions.push(cx.subscribe_in(
            &actor,
            window,
            |view, _, _: &SelectEvent<Vec<Choice<ActorKind>>>, window, cx| view.edit_draft(window, cx),
        ));
        subscriptions.push(cx.subscribe_in(
            &environment,
            window,
            |view, _, _: &SelectEvent<Vec<Choice<Option<EnvironmentId>>>>, window, cx| view.edit_draft(window, cx),
        ));
        Self {
            services,
            store: AuditQueryStore::default(),
            start,
            end,
            username,
            action,
            outcome,
            actor,
            environment,
            draft_changed: false,
            validation: None,
            active: false,
            epoch: RequestEpoch::initial(),
            query_task: None,
            read_access: None,
            #[cfg(test)]
            query_gate: None,
            detail: None,
            _subscriptions: subscriptions,
        }
    }

    pub fn set_active(&mut self, active: bool, window: &mut Window, cx: &mut Context<Self>) {
        if self.active == active {
            return;
        }
        self.active = active;
        if active && self.store.applied.is_none() {
            self.set_recent_times(window, cx);
            let (items, selected) = environment_choices(&self.services);
            self.environment.update(cx, |picker, cx| {
                picker.set_items(items, window, cx);
                picker.set_selected_value(&selected, window, cx);
            });
            self.apply(window, cx);
        }
        if !active {
            self.close_detail(window, cx);
        }
    }

    pub fn clear_context(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.clear_records(window, cx);
        self.validation = None;
        self.active = false;
        self.reset_inputs(window, cx);
        cx.notify();
    }

    fn clear_records(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.query_task = None;
        let _ = self.epoch.advance();
        self.store.clear();
        self.read_access = None;
        self.close_detail(window, cx);
    }

    fn read_access(&mut self, window: &mut Window, cx: &mut Context<Self>) -> Option<AuditReadAccess> {
        let live = self.services.audit_read_access();
        let changed = self
            .read_access
            .as_ref()
            .is_some_and(|previous| !live.as_ref().is_ok_and(|current| current == previous));
        match live {
            Ok(access) if !changed => Some(access),
            result => {
                if self.read_access.is_some() || self.store.applied.is_some() || self.query_task.is_some() {
                    self.clear_records(window, cx);
                }
                self.store.error = Some(result.err().unwrap_or_else(|| {
                    UiError::new(
                        "Local audit access changed. Apply the query again for the current session.",
                        UiErrorCode::Authentication,
                        false,
                    )
                }));
                None
            }
        }
    }

    fn reset_inputs(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.set_recent_times(window, cx);
        self.username.update(cx, |input, cx| input.set_value("", window, cx));
        self.action
            .update(cx, |picker, cx| picker.set_selected_value(&None, window, cx));
        self.outcome
            .update(cx, |picker, cx| picker.set_selected_value(&None, window, cx));
        self.actor
            .update(cx, |picker, cx| picker.set_selected_value(&ActorKind::Any, window, cx));
        let (items, selected) = environment_choices(&self.services);
        self.environment.update(cx, |picker, cx| {
            picker.set_items(items, window, cx);
            picker.set_selected_value(&selected, window, cx);
        });
        self.draft_changed = false;
    }

    fn set_recent_times(&self, window: &mut Window, cx: &mut Context<Self>) {
        let filter = AuditFilter::recent(now());
        self.start.update(cx, |input, cx| {
            input.set_value(input_time(filter.start_epoch_ms), window, cx)
        });
        self.end.update(cx, |input, cx| {
            input.set_value(input_time(filter.end_epoch_ms), window, cx)
        });
    }

    fn edit_draft(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.draft_changed = self
            .draft(cx)
            .map_or(true, |filter| self.store.applied.as_ref() != Some(&filter));
        self.validation = None;
        self.close_detail(window, cx);
        cx.notify();
    }

    fn draft(&self, cx: &Context<Self>) -> Result<AuditFilter, UiError> {
        let actor = match self.actor.read(cx).selected_value().copied().unwrap_or(ActorKind::Any) {
            ActorKind::Any => None,
            ActorKind::Desktop => Some(OperationActor::LocalDesktop),
            ActorKind::System => Some(OperationActor::System),
            ActorKind::Operator => {
                let username = self.username.read(cx).value().trim().to_owned();
                if username.is_empty() || username.len() > 512 || username.chars().any(char::is_control) {
                    return Err(validation("Enter an exact local operator username (1–512 characters)."));
                }
                Some(OperationActor::LocalOperator { username })
            }
        };
        let filter = AuditFilter {
            start_epoch_ms: parse_time(self.start.read(cx).value().as_str())?,
            end_epoch_ms: parse_time(self.end.read(cx).value().as_str())?,
            actor,
            action: self.action.read(cx).selected_value().copied().flatten(),
            outcome: self.outcome.read(cx).selected_value().copied().flatten(),
            environment_id: self.environment.read(cx).selected_value().cloned().flatten(),
        };
        filter
            .validate()
            .map_err(|_| validation("Choose a nonempty UTC window no longer than 31 days; the end is exclusive."))?;
        Ok(filter)
    }

    fn apply(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.read_access(window, cx).is_none() {
            cx.notify();
            return;
        }
        match self.draft(cx) {
            Ok(filter) => {
                self.validation = None;
                self.draft_changed = false;
                self.close_detail(window, cx);
                let query = self.store.apply(filter);
                self.query(query, window, cx);
            }
            Err(error) => {
                self.validation = Some(error);
                cx.notify();
            }
        }
    }

    fn restart(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.read_access(window, cx).is_none() {
            cx.notify();
            return;
        }
        if let Some(filter) = self.store.applied.clone() {
            self.close_detail(window, cx);
            let query = self.store.apply(filter);
            self.query(query, window, cx);
        }
    }

    fn next(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.read_access(window, cx).is_none() {
            cx.notify();
            return;
        }
        self.close_detail(window, cx);
        if self.store.page_index + 1 < self.store.pages.len() {
            self.store.page_index += 1;
            cx.notify();
        } else if let Some(query) = self.store.next() {
            self.query(query, window, cx);
        }
    }

    fn previous(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.read_access(window, cx).is_some() {
            self.close_detail(window, cx);
            self.store.page_index = self.store.page_index.saturating_sub(1);
        }
        cx.notify();
    }

    fn query(&mut self, query: AuditQuery, window: &mut Window, cx: &mut Context<Self>) {
        let Some(access) = self.read_access(window, cx) else {
            cx.notify();
            return;
        };
        self.read_access = Some(access.clone());
        self.query_task = None;
        let Ok(epoch) = self.epoch.advance() else {
            return;
        };
        self.store.loading = true;
        self.store.error = None;
        let services = self.services.clone();
        #[cfg(test)]
        let gate = self.query_gate.take();
        self.query_task = Some(cx.spawn_in(window, async move |this, cx| {
            // The GPUI entity owns both the waiter and this bounded read of the existing local journal.
            let (query, result) = cx
                .background_executor()
                .spawn(async move {
                    let result = services
                        .query_audit(query.clone())
                        .and_then(|page| services.audit_pending().map(|pending| (page, pending)));
                    (query, result)
                })
                .await;
            #[cfg(test)]
            if let Some((started, release)) = gate {
                let _ = started.send(());
                let _ = release.await;
            }
            let _ = this.update_in(cx, |view, window, cx| {
                if !view.epoch.accepts(epoch) {
                    return;
                }
                if !view.read_access(window, cx).is_some_and(|current| current == access) {
                    cx.notify();
                    return;
                }
                view.query_task = None;
                if result
                    .as_ref()
                    .is_err_and(|error| error.code() == UiErrorCode::Authentication)
                {
                    view.store.clear();
                    view.close_detail(window, cx);
                }
                view.store.finish(&query, result);
                cx.notify();
            });
        }));
        cx.notify();
    }

    fn close_detail(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if let Some(detail) = self.detail.take() {
            detail.update(cx, |detail, cx| {
                detail.record = None;
                cx.notify();
            });
            window.close_dialog(cx);
        }
    }

    fn open_detail(&mut self, record: AuditRecord, epoch: RequestEpoch, window: &mut Window, cx: &mut Context<Self>) {
        let Some(access) = self.read_access(window, cx) else {
            cx.notify();
            return;
        };
        if !self.epoch.accepts(epoch) || self.read_access.is_none() {
            return;
        }
        self.close_detail(window, cx);
        let detail = cx.new(|_| AuditDetail::new(record, self.services.clone(), access));
        self.detail = Some(detail.clone());
        let owner = cx.entity().downgrade();
        window.open_dialog(cx, move |dialog: Dialog, window, _| {
            let owner = owner.clone();
            let closing_detail = detail.clone();
            dialog
                .title("Local Audit record")
                .w(window.viewport_size().width.min(px(640.)) - px(32.))
                .margin_top(px(16.))
                .max_h(window.viewport_size().height - px(32.))
                .alert()
                .button_props(DialogButtonProps::default().ok_text("Close"))
                .footer(|ok, _, window, cx| {
                    vec![
                        div()
                            .debug_selector(|| "audit-detail-close".into())
                            .child(ok(window, cx)),
                    ]
                })
                .child(detail.clone())
                .on_close(move |_, _, cx| {
                    closing_detail.update(cx, |detail, cx| {
                        detail.record = None;
                        cx.notify();
                    });
                    let _ = owner.update(cx, |view, cx| {
                        view.detail = None;
                        cx.notify();
                    });
                })
        });
        cx.notify();
    }
}

#[cfg(test)]
impl AuditView {
    pub(crate) fn hold_next_query(&mut self) -> (tokio::sync::oneshot::Receiver<()>, tokio::sync::oneshot::Sender<()>) {
        let (started, observed) = tokio::sync::oneshot::channel();
        let (release, wait) = tokio::sync::oneshot::channel();
        self.query_gate = Some((started, wait));
        (observed, release)
    }
    pub(crate) fn retained_record_count(&self) -> usize {
        self.store.pages.iter().map(|page| page.events.len()).sum::<usize>() + self.store.pending.len()
    }
    pub(crate) fn has_detail(&self) -> bool {
        self.detail.is_some()
    }
}

#[cfg(test)]
#[path = "audit_tests.rs"]
pub(crate) mod tests;

impl Render for AuditView {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let _ = self.read_access(window, cx);
        let epoch = self.epoch;
        let operator = self.actor.read(cx).selected_value() == Some(&ActorKind::Operator);
        let error = self.validation.as_ref().or(self.store.error.as_ref());
        let rows = self.store.page().map(|page| page.events.clone()).unwrap_or_default();
        let pending = self.store.pending.clone();
        let controls = div()
            .flex()
            .flex_wrap()
            .gap_2()
            .child(
                Button::new("audit-apply")
                    .debug_selector(|| "audit-apply".into())
                    .label("Apply filters")
                    .disabled(self.store.loading)
                    .on_click(cx.listener(|view, _, window, cx| view.apply(window, cx))),
            )
            .child(
                Button::new("audit-recent")
                    .debug_selector(|| "audit-recent".into())
                    .label("Recent 24h")
                    .outline()
                    .disabled(self.store.loading)
                    .on_click(cx.listener(|view, _, window, cx| {
                        view.set_recent_times(window, cx);
                        view.apply(window, cx);
                    })),
            )
            .child(
                Button::new("audit-reset")
                    .debug_selector(|| "audit-reset".into())
                    .label("Reset filters")
                    .outline()
                    .disabled(self.store.loading)
                    .on_click(cx.listener(|view, _, window, cx| {
                        view.reset_inputs(window, cx);
                        view.apply(window, cx);
                    })),
            )
            .child(
                Button::new("audit-restart")
                    .debug_selector(|| "audit-restart".into())
                    .label("Restart applied query")
                    .outline()
                    .disabled(self.store.loading || self.store.applied.is_none())
                    .on_click(cx.listener(|view, _, window, cx| view.restart(window, cx))),
            );
        let filters = div()
            .w_full()
            .flex()
            .flex_wrap()
            .gap_3()
            .child(
                div()
                    .flex_1()
                    .min_w(px(240.))
                    .child("Start UTC (inclusive)")
                    .child(Input::new(&self.start)),
            )
            .child(
                div()
                    .flex_1()
                    .min_w(px(240.))
                    .child("End UTC (exclusive; maximum 31 days)")
                    .child(Input::new(&self.end)),
            )
            .child(
                div()
                    .flex_1()
                    .min_w(px(190.))
                    .debug_selector(|| "audit-action-filter".into())
                    .child("Action")
                    .child(Select::new(&self.action)),
            )
            .child(
                div()
                    .flex_1()
                    .min_w(px(190.))
                    .debug_selector(|| "audit-outcome-filter".into())
                    .child("Outcome")
                    .child(Select::new(&self.outcome)),
            )
            .child(
                div()
                    .flex_1()
                    .min_w(px(190.))
                    .debug_selector(|| "audit-actor-filter".into())
                    .child("Actor")
                    .child(Select::new(&self.actor)),
            )
            .child(
                div()
                    .flex_1()
                    .min_w(px(190.))
                    .debug_selector(|| "audit-environment-filter".into())
                    .child("Environment")
                    .child(Select::new(&self.environment)),
            )
            .when(operator, |body| {
                body.child(
                    div()
                        .w_full()
                        .child("Exact local operator username")
                        .child(Input::new(&self.username)),
                )
            });
        div().debug_selector(|| "audit-workspace".into()).size_full().min_w_0().p_4().flex().flex_col().gap_3()
            .child(div().flex_shrink_0().text_sm().child(format!("Operations recorded by this desktop. Local authentication is {}.", if self.services.connection_state().config.auth.enabled { "required" } else { "disabled" })))
            .child(div().id("audit-filters").debug_selector(|| "audit-filters".into()).w_full().max_h(window.viewport_size().height / 3.).flex_shrink_0().overflow_y_scroll().child(filters))
            .child(controls)
            .when(self.draft_changed, |body| body.child(div().text_sm().child("Draft changed. Apply filters starts a new cursor window; paging uses the applied query.")))
            .children(self.store.applied.as_ref().map(|filter| div().text_xs().child(format!("Applied: {} → {} (exclusive)", timestamp(filter.start_epoch_ms), timestamp(filter.end_epoch_ms)))))
            .children(error.map(|error| div().debug_selector(|| "audit-query-error".into()).text_sm().text_color(cx.theme().danger).child(error.summary().to_owned()).when(self.store.next().is_some(), |body| body.child("The retained page may have expired. Restart the applied query; draft inputs are preserved."))))
            .when(self.store.loading, |body| body.child("Reading local Audit…"))
            .child(div().id("audit-results").debug_selector(|| "audit-results".into()).flex_1().min_h_0().min_w_0().overflow_y_scroll().child(
                div().w_full().min_w_0().flex().flex_col().gap_3()
                    .when(!pending.is_empty(), |body| body.child(format!("{} unfinished accepted requests match these filters. No terminal outcome is inferred; showing up to 25.", pending.len())))
                    .children(pending.into_iter().take(25).enumerate().map(|(index, operation)| {
                        let label = format!("Unfinished · {} · {} · {}", action_label(operation.action), timestamp(operation.accepted_at_epoch_ms), operation.context.request_id);
                        div().id(("audit-pending-row", index)).w_full().min_w_0().child(Button::new(("audit-pending", index)).debug_selector(move || format!("audit-pending-{index}")).label("Unfinished request · Details").outline().on_click(cx.listener(move |view, _, window, cx| view.open_detail(AuditRecord::Pending(operation.clone()), epoch, window, cx)))).child(row_text("audit-pending-summary", label))
                    }))
                    .when(rows.is_empty() && !self.store.loading && self.store.applied.is_some() && self.store.error.is_none(), |body| body.child("No terminal records match the applied filters."))
                    .children(rows.into_iter().enumerate().map(|(index, event)| {
                        let selected = event.clone();
                        div().id(("audit-row", event.event_id)).w_full().min_w_0().p_3().border_1().border_color(cx.theme().border).rounded_md().flex().flex_col().gap_2()
                            .child(Button::new(("audit-open", index)).debug_selector(move || format!("audit-open-{index}")).label(format!("{} · {} · Details", action_label(event.operation.action), outcome_label(event.outcome))).outline().on_click(cx.listener(move |view, _, window, cx| view.open_detail(AuditRecord::Terminal(selected.clone()), epoch, window, cx))))
                            .child(row_text("audit-record-time", timestamp(event.timestamp_epoch_ms)))
                            .child(row_text("audit-record-actor", actor_label(&event.operation.context.actor)))
                            .child(row_text("audit-record-resource", resource_label(&event.operation.context.resource)))
                            .child(row_text("audit-record-environment", format!("Environment: {}", event.operation.context.environment_id.as_str())))
                            .child(row_text("audit-record-request", format!("Request ID: {}", event.operation.context.request_id)))
                    }))
            ))
            .child(div().flex().flex_wrap().gap_2().items_center()
                .child(Button::new("audit-previous").debug_selector(|| "audit-previous".into()).label("Previous").outline().disabled(self.store.loading || self.store.page_index == 0).on_click(cx.listener(|view, _, window, cx| view.previous(window, cx))))
                .child(format!("Page {} · up to 25 terminal records", self.store.page_index + 1))
                .child(Button::new("audit-next").debug_selector(|| "audit-next".into()).label("Next").outline().disabled(self.store.loading || self.store.next().is_none()).on_click(cx.listener(|view, _, window, cx| view.next(window, cx)))))
    }
}

fn row_text(id: &'static str, value: String) -> gpui::Stateful<gpui::Div> {
    div()
        .id(id)
        .debug_selector(move || id.into())
        .w_full()
        .min_w_0()
        .overflow_x_scroll()
        .child(div().whitespace_nowrap().child(value))
}
fn environment_choices(services: &AppServices) -> (Vec<Choice<Option<EnvironmentId>>>, Option<EnvironmentId>) {
    let mut items = vec![choice("All environments", None)];
    let active = services.connection_state().environments.map(|catalog| {
        items.extend(
            catalog
                .profiles
                .into_iter()
                .map(|profile| choice(format!("{} ({})", profile.name, profile.id.as_str()), Some(profile.id))),
        );
        catalog.active_id
    });
    (items, active)
}
fn now() -> u64 {
    chrono::Utc::now().timestamp_millis().max(0) as u64
}
fn input_time(value: u64) -> String {
    timestamp(value).trim_end_matches(" UTC").to_owned()
}
fn validation(summary: &'static str) -> UiError {
    UiError::new(summary, UiErrorCode::Validation, false)
}
fn parse_time(value: &str) -> Result<u64, UiError> {
    chrono::NaiveDateTime::parse_from_str(value.trim(), "%Y-%m-%d %H:%M:%S%.f")
        .ok()
        .and_then(|time| u64::try_from(time.and_utc().timestamp_millis()).ok())
        .ok_or_else(|| validation("Use YYYY-MM-DD HH:MM:SS[.mmm] for UTC times on or after 1970-01-01."))
}
