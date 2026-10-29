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

//! Five-tab Consumer Sheet with lazy resources and explicit diagnostics.

#[path = "consumer_detail_mutations.rs"]
mod mutations;

#[path = "consumer_detail_render.rs"]
mod render;

use gpui::prelude::FluentBuilder as _;
use gpui::{
    ClipboardItem, Context, Entity, EventEmitter, InteractiveElement as _, IntoElement, ParentElement as _, Render,
    Styled as _, Task, Window, div,
};
use gpui_component::{
    ActiveTheme as _, Sizable as _,
    button::{Button, ButtonVariants as _},
    scroll::ScrollableElement as _,
};
use rocketmq_dashboard_common::{
    CONSUMER_DIAGNOSTIC_MAX_BYTES, ConsumerCapabilities, ConsumerConfigPatchCommand, ConsumerDiagnosticKind,
    ConsumerDiagnosticRequest, ConsumerIdentity, ConsumerInventory, ConsumerObservation, ConsumerPartialOutcome,
    ConsumerTargetIdentity,
};

use crate::{
    features::{consumers_store::ConsumerDetailStore, topic_dialogs::TopicDialogForm},
    route::{AppRoute, ConsumerTab, RouteKey},
    services::AppServices,
    state::Loadable,
};

pub(super) const CONSUMER_SHEET_WIDTH: gpui::Pixels = gpui::px(800.);
// The pinned Sheet has a 26px small close button and 8px padding above and below it.
const SHEET_TITLE_ROW_HEIGHT: gpui::Pixels = gpui::px(42.);

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConsumerDetailIntent {
    ReplaceRoute(AppRoute),
    QueryScopeChanged(rocketmq_dashboard_common::ConsumerQueryScope),
    InventoryReloaded(ConsumerInventory),
    Deleted(ConsumerInventory),
}

pub struct ConsumerDetail {
    services: AppServices,
    revision: u64,
    pub store: ConsumerDetailStore,
    resource_task: Option<Task<()>>,
    diagnostic_task: Option<Task<()>>,
    mutation_task: Option<Task<()>>,
    targets: Vec<ConsumerTargetIdentity>,
    capabilities: ConsumerCapabilities,
    edit_draft: Option<ConsumerConfigPatchCommand>,
    mutation_epoch: u64,
    mutation_status: Option<String>,
    mutation_outcome: Option<ConsumerPartialOutcome>,
    mutation_replay_blocked: bool,
    offset_dialog: Option<Entity<TopicDialogForm>>,
    mutation_dialog: Option<Entity<crate::features::consumer_dialogs::ConsumerDialogForm>>,
    query_scope: rocketmq_dashboard_common::ConsumerQueryScope,
}

impl EventEmitter<ConsumerDetailIntent> for ConsumerDetail {}

impl ConsumerDetail {
    pub fn new(
        services: AppServices,
        revision: u64,
        group: ConsumerIdentity,
        tab: ConsumerTab,
        targets: Vec<ConsumerTargetIdentity>,
        capabilities: ConsumerCapabilities,
        query_scope: rocketmq_dashboard_common::ConsumerQueryScope,
        cx: &mut Context<Self>,
    ) -> Self {
        let mut detail = Self {
            services,
            revision,
            store: ConsumerDetailStore::new(group, tab),
            resource_task: None,
            diagnostic_task: None,
            mutation_task: None,
            targets,
            capabilities,
            edit_draft: None,
            mutation_epoch: 0,
            mutation_status: None,
            mutation_outcome: None,
            mutation_replay_blocked: false,
            offset_dialog: None,
            mutation_dialog: None,
            query_scope,
        };
        detail.refresh_active(cx);
        detail
    }

    #[cfg(test)]
    pub(crate) fn mutation_status_for_test(&self) -> Option<&str> {
        self.mutation_status.as_deref()
    }

    #[cfg(test)]
    pub(crate) fn mutation_dialog_for_test(
        &self,
    ) -> Option<Entity<crate::features::consumer_dialogs::ConsumerDialogForm>> {
        self.mutation_dialog.clone()
    }

    #[cfg(test)]
    pub(crate) fn offset_dialog_for_test(&self) -> Option<Entity<TopicDialogForm>> {
        self.offset_dialog.clone()
    }

    #[cfg(test)]
    pub(crate) fn offset_blockers_for_test(&self) -> (bool, bool, bool) {
        (
            self.mutation_task.is_some(),
            self.offset_dialog.is_some(),
            self.mutation_replay_blocked,
        )
    }

    #[cfg(test)]
    pub(crate) fn load_diagnostic_for_test(
        &mut self,
        client: rocketmq_dashboard_common::ConsumerClientIdentity,
        kind: ConsumerDiagnosticKind,
        cx: &mut Context<Self>,
    ) {
        self.load_diagnostic(client, kind, cx);
    }

    pub fn set_revision(&mut self, revision: u64, cx: &mut Context<Self>) {
        if revision != self.revision {
            self.revision = revision;
            self.store.clear_diagnostic();
            self.store.overview.clear();
            self.store.clients.clear();
            self.store.progress.clear();
            self.store.configuration.clear();
            self.store.offset_actions.clear();
            self.refresh_active(cx);
            cx.notify();
        }
    }

    pub(super) fn has_pending_mutation(&self) -> bool {
        self.mutation_task.is_some()
    }

    #[cfg(test)]
    pub(super) fn set_mutation_task_for_test(&mut self, task: Option<Task<()>>) {
        self.mutation_task = task;
    }

    pub(super) fn clear_for_environment(&mut self, revision: u64, window: &mut gpui::Window, cx: &mut Context<Self>) {
        self.revision = revision;
        self.resource_task = None;
        self.mutation_dialog = None;
        self.diagnostic_task = None;
        self.store.clear_diagnostic();
        self.store.overview.clear();
        self.store.clients.clear();
        self.store.progress.clear();
        self.store.configuration.clear();
        self.store.offset_actions.clear();
        self.targets.clear();
        self.edit_draft = None;
        self.mutation_status = None;
        self.mutation_outcome = None;
        if let Some(form) = self.offset_dialog.take() {
            form.update(cx, |form, cx| form.clear_sensitive(window, cx));
        }
        cx.notify();
    }

    pub(super) fn discard_queries(&mut self, cx: &mut Context<Self>) {
        self.resource_task = None;
        self.diagnostic_task = None;
        self.store.clear_diagnostic();
        cx.notify();
    }

    pub fn set_tab(&mut self, tab: ConsumerTab, cx: &mut Context<Self>) {
        if tab != self.store.active_tab {
            self.diagnostic_task = None;
            self.store.clear_diagnostic();
            self.store.set_tab(tab);
            self.refresh_active(cx);
            if let Ok(group) = RouteKey::parse(self.store.group.as_str()) {
                cx.emit(ConsumerDetailIntent::ReplaceRoute(AppRoute::ConsumerDetail {
                    group,
                    tab,
                }));
            }
            cx.notify();
        }
    }

    pub fn retry_active(&mut self, cx: &mut Context<Self>) {
        match self.store.active_tab {
            ConsumerTab::Overview => self.store.overview.clear(),
            ConsumerTab::Clients => self.store.clients.clear(),
            ConsumerTab::Progress => self.store.progress.clear(),
            ConsumerTab::Configuration => self.store.configuration.clear(),
            ConsumerTab::OffsetActions => self.store.offset_actions.clear(),
        }
        self.refresh_active(cx);
    }

    fn refresh_active(&mut self, cx: &mut Context<Self>) {
        if !self.store.active_is_idle() {
            return;
        }
        let Some(mut request) = self.store.begin_active(self.revision) else {
            return;
        };
        request.scope.query_scope = self.query_scope;
        let services = self.services.clone();
        let group = self.store.group.clone();
        let tab = self.store.active_tab;
        self.resource_task = Some(cx.spawn(async move |this, cx| match tab {
            ConsumerTab::Overview => {
                let result = services.consumer_inventory(request.scope).await;
                let _ = this.update(cx, |detail, cx| {
                    detail.store.finish_overview(request, detail.revision, result);
                    cx.notify();
                });
            }
            ConsumerTab::Clients => {
                let result = services.consumer_clients(request.scope, group).await;
                let _ = this.update(cx, |detail, cx| {
                    detail.store.finish_clients(request, detail.revision, result);
                    cx.notify();
                });
            }
            ConsumerTab::Progress => {
                let result = services.consumer_progress(request.scope, group).await;
                let _ = this.update(cx, |detail, cx| {
                    detail.store.finish_progress(request, detail.revision, result);
                    cx.notify();
                });
            }
            ConsumerTab::Configuration => {
                let result = services.consumer_configuration(request.scope, group).await;
                let _ = this.update(cx, |detail, cx| {
                    detail.store.finish_configuration(request, detail.revision, result);
                    cx.notify();
                });
            }
            ConsumerTab::OffsetActions => {
                let result = services.consumer_progress(request.scope, group).await;
                let _ = this.update(cx, |detail, cx| {
                    detail.store.finish_offset_actions(request, detail.revision, result);
                    cx.notify();
                });
            }
        }));
    }

    fn load_diagnostic(
        &mut self,
        client: rocketmq_dashboard_common::ConsumerClientIdentity,
        kind: ConsumerDiagnosticKind,
        cx: &mut Context<Self>,
    ) {
        if self.capabilities.diagnostics != rocketmq_dashboard_common::CapabilityAvailability::Available {
            return;
        }
        let Some(mut load) = self.store.begin_diagnostic(self.revision, client.clone(), kind) else {
            return;
        };
        load.scope.query_scope = self.query_scope;
        let request = ConsumerDiagnosticRequest {
            group: self.store.group.clone(),
            client,
            kind,
            max_output_bytes: CONSUMER_DIAGNOSTIC_MAX_BYTES,
        };
        let services = self.services.clone();
        self.diagnostic_task = Some(cx.spawn(async move |this, cx| {
            let result = services.consumer_diagnostic(load.scope, request).await;
            let _ = this.update(cx, |detail, cx| {
                detail.store.finish_diagnostic(load, detail.revision, result);
                cx.notify();
            });
        }));
    }

    fn render_active(&self, window: &mut Window, cx: &mut Context<Self>) -> gpui::Div {
        let failed = match self.store.active_tab {
            ConsumerTab::Overview => failed_summary(&self.store.overview.state),
            ConsumerTab::Clients => failed_summary(&self.store.clients.state),
            ConsumerTab::Progress => failed_summary(&self.store.progress.state),
            ConsumerTab::Configuration => failed_summary(&self.store.configuration.state),
            ConsumerTab::OffsetActions => failed_summary(&self.store.offset_actions.state),
        };
        if let Some(summary) = failed {
            let retryable = match self.store.active_tab {
                ConsumerTab::Overview => failed_retryable(&self.store.overview.state),
                ConsumerTab::Clients => failed_retryable(&self.store.clients.state),
                ConsumerTab::Progress | ConsumerTab::OffsetActions => failed_retryable(&self.store.progress.state),
                ConsumerTab::Configuration => failed_retryable(&self.store.configuration.state),
            };
            return div().flex().flex_col().gap_3().child(summary).when(retryable, |this| {
                this.child(
                    Button::new("retry-consumer-tab")
                        .label("Retry")
                        .on_click(cx.listener(|detail, _, _, cx| detail.retry_active(cx))),
                )
            });
        }
        let body = match self.store.active_tab {
            ConsumerTab::Overview => self.render_overview(window, cx),
            ConsumerTab::Clients => self.render_clients(cx),
            ConsumerTab::Progress => self.render_progress(false, cx),
            ConsumerTab::Configuration => self.render_configuration(window, cx),
            ConsumerTab::OffsetActions => self.render_progress(true, cx),
        };
        let refresh_failure = match self.store.active_tab {
            ConsumerTab::Overview => retained_failure(&self.store.overview.state),
            ConsumerTab::Clients => retained_failure(&self.store.clients.state),
            ConsumerTab::Progress | ConsumerTab::OffsetActions => retained_failure(&self.store.progress.state),
            ConsumerTab::Configuration => retained_failure(&self.store.configuration.state),
        };
        div()
            .w_full()
            .min_w_0()
            .whitespace_normal()
            .flex()
            .flex_col()
            .gap_3()
            .when_some(refresh_failure, |this, (summary, retryable)| {
                this.child(
                    div()
                        .p_3()
                        .rounded_md()
                        .bg(cx.theme().warning.opacity(0.12))
                        .child(summary)
                        .when(retryable, |this| {
                            this.child(
                                Button::new("retry-consumer-tab-refresh")
                                    .label("Retry")
                                    .on_click(cx.listener(|detail, _, _, cx| detail.retry_active(cx))),
                            )
                        }),
                )
            })
            .child(body)
    }

    fn render_overview(&self, _window: &mut Window, cx: &mut Context<Self>) -> gpui::Div {
        match self.store.overview.state.value() {
            Some(group) => div()
                .flex()
                .flex_col()
                .gap_2()
                .child(format!(
                    "Connection: {}",
                    connection_observation_label(&group.connection_state)
                ))
                .child(format!("Clients: {}", observation_label(&group.client_count)))
                .child(format!("Lag: {}", observation_label(&group.lag)))
                .child(format!("Consume type: {}", observation_label(&group.consume_type)))
                .child(format!("Message model: {}", observation_label(&group.message_model)))
                .when(
                    self.capabilities.delete == rocketmq_dashboard_common::CapabilityAvailability::Available
                        && !self.targets.is_empty(),
                    |this| {
                        this.child(
                            Button::new("consumer-delete")
                                .label("Delete Consumer group")
                                .danger()
                                .debug_selector(|| "consumer-delete".to_owned())
                                .on_click(cx.listener(|detail, _, window, cx| detail.request_delete(window, cx))),
                        )
                    },
                ),
            None => loading_or_empty(&self.store.overview.state),
        }
    }

    fn exact_cluster_for_broker(&self, broker_name: &str) -> Option<String> {
        let mut matches = self
            .targets
            .iter()
            .filter(|target| target.broker_name() == broker_name)
            .map(|target| target.cluster_name())
            .collect::<Vec<_>>();
        matches.sort_unstable();
        matches.dedup();
        (matches.len() == 1).then(|| matches[0].to_owned())
    }
}

impl Render for ConsumerDetail {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let tabs = [
            (ConsumerTab::Overview, "Overview"),
            (ConsumerTab::Clients, "Clients"),
            (ConsumerTab::Progress, "Progress"),
            (ConsumerTab::Configuration, "Configuration"),
            (ConsumerTab::OffsetActions, "Offset Actions"),
        ];
        let copy_group = self.store.group.as_str().to_owned();
        let window_padding = gpui_component::window_paddings(window);
        let content = div()
            .w_full()
            .min_w_0()
            .whitespace_normal()
            .p_4()
            .flex()
            .flex_col()
            .gap_4()
            .child(
                div()
                    .w_full()
                    .min_w_0()
                    .whitespace_normal()
                    .debug_selector(|| "consumer-detail-group".to_owned())
                    .child(format!("Consumer group: {}", self.store.group.as_str())),
            )
            .child(
                Button::new("consumer-copy-group")
                    .debug_selector(|| "consumer-copy-group".to_owned())
                    .label("Copy group")
                    .small()
                    .outline()
                    .on_click(move |_, _, cx| cx.write_to_clipboard(ClipboardItem::new_string(copy_group.clone()))),
            )
            .child(div().w_full().min_w_0().whitespace_normal().child(format!(
                    "Consumer reads: {}",
                    self.query_scope
                        .description(&self.services.connection_state().config.connection_snapshot())
                )))
            .child(
                Button::new("consumer-detail-query-scope")
                    .debug_selector(|| "consumer-detail-query-scope".to_owned())
                    .label("Switch query scope")
                    .on_click(cx.listener(|detail, _, _, cx| {
                        detail.diagnostic_task = None;
                        detail.store.clear_diagnostic();
                        let scope = match detail.query_scope {
                            rocketmq_dashboard_common::ConsumerQueryScope::Configured => {
                                rocketmq_dashboard_common::ConsumerQueryScope::BrokerDirect
                            }
                            rocketmq_dashboard_common::ConsumerQueryScope::BrokerDirect => {
                                rocketmq_dashboard_common::ConsumerQueryScope::Configured
                            }
                        };
                        cx.emit(ConsumerDetailIntent::QueryScopeChanged(scope));
                    })),
            )
            .child(
                div()
                    .flex()
                    .flex_wrap()
                    .gap_2()
                    .children(tabs.into_iter().enumerate().map(|(index, (tab, label))| {
                        Button::new(("consumer-tab", index))
                            .label(label)
                            .small()
                            .when(tab == self.store.active_tab, |button| button.primary())
                            .on_click(cx.listener(move |detail, _, _, cx| detail.set_tab(tab, cx)))
                    })),
            )
            .when_some(self.mutation_status.clone(), |this, status| {
                this.child(div().p_2().rounded_md().bg(cx.theme().muted).child(status))
            })
            .when_some(self.mutation_outcome.clone(), |this, outcome| {
                this.child(render_consumer_outcome(&outcome, cx))
            })
            .child(self.render_active(window, cx));
        div()
            .id("consumer-detail")
            .debug_selector(|| "consumer-detail".to_owned())
            .size_full()
            // Sheet's scroll layer measures long diagnostic lines at their intrinsic width.
            // Keep the content inside the configured Sheet width and its one-pixel left border.
            .max_w(window.viewport_size().width.min(CONSUMER_SHEET_WIDTH) - gpui::px(1.))
            .max_h(
                window.viewport_size().height
                    - window_padding.top
                    - window_padding.bottom
                    - gpui_component::TITLE_BAR_HEIGHT
                    - SHEET_TITLE_ROW_HEIGHT,
            )
            .min_w_0()
            .min_h_0()
            .child(
                div()
                    .id("consumer-detail-scroll")
                    .debug_selector(|| "consumer-detail-scroll".to_owned())
                    .size_full()
                    .min_w_0()
                    .min_h_0()
                    .overflow_y_scrollbar()
                    .child(content),
            )
    }
}

fn render_consumer_outcome(outcome: &ConsumerPartialOutcome, cx: &gpui::App) -> impl IntoElement {
    let cleanup = outcome.counts().cleanup_steps;
    div()
        .p_2()
        .rounded_md()
        .bg(cx.theme().muted)
        .flex()
        .flex_col()
        .gap_1()
        .child(format!(
            "{}: {}/{} Broker targets applied",
            if outcome.is_complete_success() {
                "Complete"
            } else if !outcome.has_applied_effects() && outcome.has_unconfirmed_effects() {
                "Unconfirmed"
            } else if !outcome.has_applied_effects() {
                "Rejected"
            } else {
                "Partial"
            },
            outcome.applied_count(),
            outcome.target_count()
        ))
        .when(cleanup.total() > 0, |this| {
            this.child(format!(
                "Cleanup steps: {} applied, {} not applied, {} unconfirmed",
                cleanup.applied, cleanup.not_applied, cleanup.unconfirmed
            ))
        })
        .children(outcome.targets.iter().enumerate().map(|(index, target)| {
            let applied = target.applied;
            div()
                .debug_selector(move || {
                    if applied {
                        format!("consumer-mutation-applied-{index}")
                    } else {
                        format!("consumer-mutation-failed-{index}")
                    }
                })
                .text_sm()
                .child(format!(
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

fn observation_label<T: std::fmt::Display>(observation: &ConsumerObservation<T>) -> String {
    match observation {
        ConsumerObservation::Complete(value) => value.to_string(),
        ConsumerObservation::Partial { value, .. } => format!("Partial ({value})"),
        ConsumerObservation::Unknown { .. } => "Unknown".into(),
    }
}

fn connection_observation_label(
    observation: &ConsumerObservation<rocketmq_dashboard_common::ConsumerConnectionState>,
) -> &'static str {
    match observation {
        ConsumerObservation::Complete(rocketmq_dashboard_common::ConsumerConnectionState::Connected) => "Connected",
        ConsumerObservation::Complete(rocketmq_dashboard_common::ConsumerConnectionState::Disconnected) => {
            "Disconnected"
        }
        ConsumerObservation::Partial { .. } => "Partial",
        ConsumerObservation::Unknown { .. } => "Unknown",
    }
}

fn observation_state_label<T>(observation: &ConsumerObservation<T>) -> &'static str {
    match observation {
        ConsumerObservation::Complete(_) => "Complete observation",
        ConsumerObservation::Partial { .. } => "Partial observation — missing targets are not assumed empty.",
        ConsumerObservation::Unknown { .. } => "Unknown — no zero or offline value is inferred.",
    }
}

fn failed_summary<T>(state: &Loadable<T>) -> Option<String> {
    match state {
        Loadable::Failed { previous: None, error } => Some(error.summary().to_owned()),
        _ => None,
    }
}

fn retained_failure<T>(state: &Loadable<T>) -> Option<(String, bool)> {
    match state {
        Loadable::Failed {
            previous: Some(_),
            error,
        } => Some((error.summary().to_owned(), error.is_retryable())),
        _ => None,
    }
}

fn failed_retryable<T>(state: &Loadable<T>) -> bool {
    matches!(state, Loadable::Failed { error, .. } if error.is_retryable())
}

fn loading_or_empty<T>(state: &Loadable<T>) -> gpui::Div {
    match state {
        Loadable::Empty => div().child("No authoritative data was returned."),
        _ => div().child("Loading…"),
    }
}

#[cfg(test)]
mod recovery_tests {
    use crate::state::{UiError, UiErrorCode};

    use super::*;

    #[test]
    fn refresh_failure_keeps_data_visible_and_exposes_recovery_separately() {
        let state = Loadable::ready("retained").fail(UiError::new("Refresh failed.", UiErrorCode::Connection, true));

        assert_eq!(state.value(), Some(&"retained"));
        assert_eq!(failed_summary(&state), None);
        assert_eq!(retained_failure(&state), Some(("Refresh failed.".to_owned(), true)));
    }

    #[test]
    fn initial_failure_has_no_recovery_button_when_retry_is_not_allowed() {
        let state: Loadable<()> =
            Loadable::InitialLoading.fail(UiError::new("Invalid request.", UiErrorCode::Validation, false));

        assert_eq!(failed_summary(&state), Some("Invalid request.".to_owned()));
        assert!(!failed_retryable(&state));
        assert_eq!(retained_failure(&state), None);
    }
}
