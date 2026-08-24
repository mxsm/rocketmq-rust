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

use gpui::prelude::FluentBuilder as _;
use gpui::{
    Context, Entity, EventEmitter, InteractiveElement as _, IntoElement, ParentElement as _, Render, Styled as _, Task,
    Window, div,
};
use gpui_component::{
    ActiveTheme as _, Sizable as _,
    button::{Button, ButtonVariants as _},
};
use rocketmq_dashboard_common::{
    CONSUMER_DIAGNOSTIC_MAX_BYTES, ConsumerCapabilities, ConsumerConfigPatch, ConsumerConfigPatchCommand,
    ConsumerDiagnosticKind, ConsumerDiagnosticRequest, ConsumerIdentity, ConsumerInventory, ConsumerObservation,
    ConsumerPartialOutcome, ConsumerTargetIdentity,
};

use crate::{
    features::{consumers_store::ConsumerDetailStore, topic_dialogs::TopicDialogForm},
    route::{AppRoute, ConsumerTab, RouteKey},
    services::AppServices,
    state::Loadable,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConsumerDetailIntent {
    ReplaceRoute(AppRoute),
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
        };
        detail.refresh_active(cx);
        detail
    }

    #[cfg(test)]
    pub(crate) fn mutation_status_for_test(&self) -> Option<&str> {
        self.mutation_status.as_deref()
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

    pub fn set_tab(&mut self, tab: ConsumerTab, cx: &mut Context<Self>) {
        if tab != self.store.active_tab {
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
        let Some(request) = self.store.begin_active(self.revision) else {
            return;
        };
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
        let Some(load) = self.store.begin_diagnostic(self.revision, client.clone(), kind) else {
            return;
        };
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
            return div().flex().flex_col().gap_3().child(summary).child(
                Button::new("retry-consumer-tab")
                    .label("Retry")
                    .on_click(cx.listener(|detail, _, _, cx| detail.retry_active(cx))),
            );
        }
        match self.store.active_tab {
            ConsumerTab::Overview => self.render_overview(window, cx),
            ConsumerTab::Clients => self.render_clients(cx),
            ConsumerTab::Progress => self.render_progress(false, cx),
            ConsumerTab::Configuration => self.render_configuration(cx),
            ConsumerTab::OffsetActions => self.render_progress(true, cx),
        }
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

    fn render_clients(&self, cx: &mut Context<Self>) -> gpui::Div {
        let mut body = div().flex().flex_col().gap_2();
        if let Some(observation) = self.store.clients.state.value() {
            body = body.child(observation_state_label(observation));
            if let Some(clients) = observation.value() {
                for (index, client) in clients.clients.iter().enumerate() {
                    let running_client = client.identity.clone();
                    let jstack_client = client.identity.clone();
                    body = body.child(
                        div()
                            .flex()
                            .items_center()
                            .gap_2()
                            .child(format!(
                                "{} · {} · {}",
                                client.identity.as_str(),
                                client.language,
                                client.version_description
                            ))
                            .child(
                                Button::new(("running-info", index))
                                    .label("Running Info")
                                    .small()
                                    .outline()
                                    .on_click(cx.listener(move |detail, _, _, cx| {
                                        detail.load_diagnostic(
                                            running_client.clone(),
                                            ConsumerDiagnosticKind::RunningInfo,
                                            cx,
                                        );
                                    })),
                            )
                            .child(
                                Button::new(("jstack", index))
                                    .label("JStack")
                                    .small()
                                    .outline()
                                    .on_click(cx.listener(move |detail, _, _, cx| {
                                        detail.load_diagnostic(
                                            jstack_client.clone(),
                                            ConsumerDiagnosticKind::Jstack,
                                            cx,
                                        );
                                    })),
                            ),
                    );
                }
            }
        } else {
            body = body.child("Loading clients…");
        }
        if let Some(payload) = self.store.diagnostic.state.value() {
            body = body.child(
                div()
                    .id("consumer-diagnostic")
                    .mt_3()
                    .p_3()
                    .rounded_md()
                    .bg(cx.theme().muted)
                    .child(if payload.truncated() {
                        "Diagnostic output (truncated)"
                    } else {
                        "Diagnostic output"
                    })
                    .children(
                        payload
                            .properties()
                            .iter()
                            .map(|(key, value)| div().child(format!("{key}: {value}"))),
                    )
                    .when_some(payload.text(), |this, text| this.child(div().child(text.to_owned()))),
            );
        }
        body
    }

    fn render_progress(&self, actions: bool, cx: &mut Context<Self>) -> gpui::Div {
        let observation = if actions {
            self.store.offset_actions.state.value()
        } else {
            self.store.progress.state.value()
        };
        match observation {
            Some(observation) => {
                let mut body = div()
                    .flex()
                    .flex_col()
                    .gap_2()
                    .child(observation_state_label(observation));
                if let Some(progress) = observation.value() {
                    body = body.child(format!("Signed total delta: {}", progress.total_delta));
                    for (index, row) in progress.rows.iter().enumerate() {
                        let mut item = div().flex().flex_col().gap_1().child(format!(
                            "{} / {} / queue {} · broker={} consumer={} delta={}",
                            row.topic, row.broker_name, row.queue_id, row.broker_offset, row.consumer_offset, row.delta
                        ));
                        if actions
                            && self.capabilities.offset_actions
                                == rocketmq_dashboard_common::CapabilityAvailability::Available
                            && let Some(cluster_name) = self.exact_cluster_for_broker(&row.broker_name)
                        {
                            let reset_topic = row.topic.clone();
                            let reset_cluster = cluster_name.clone();
                            let skip_topic = row.topic.clone();
                            item = item.child(
                                div()
                                    .flex()
                                    .flex_wrap()
                                    .items_center()
                                    .gap_2()
                                    .child(
                                        Button::new(("consumer-reset-offset", index))
                                            .debug_selector(move || format!("consumer-reset-offset-{index}"))
                                            .label("Reset offset")
                                            .small()
                                            .outline()
                                            .on_click(cx.listener(move |detail, _, window, cx| {
                                                detail.request_offset_action(
                                                    reset_topic.clone(),
                                                    reset_cluster.clone(),
                                                    false,
                                                    window,
                                                    cx,
                                                );
                                            })),
                                    )
                                    .child(
                                        Button::new(("consumer-skip-offset", index))
                                            .debug_selector(move || format!("consumer-skip-offset-{index}"))
                                            .label("Skip accumulated")
                                            .small()
                                            .outline()
                                            .on_click(cx.listener(move |detail, _, window, cx| {
                                                detail.request_offset_action(
                                                    skip_topic.clone(),
                                                    cluster_name.clone(),
                                                    true,
                                                    window,
                                                    cx,
                                                );
                                            })),
                                    ),
                            );
                        }
                        body = body.child(item);
                    }
                }
                if actions {
                    body = body.child(
                        "Each action reuses the D4 exact Topic/group/one-cluster coordinator; detailed broker/queue outcomes remain partial and are never broadly replayed.",
                    );
                }
                body
            }
            None => div().child("Loading progress…"),
        }
    }

    fn render_configuration(&self, cx: &mut Context<Self>) -> gpui::Div {
        match self.store.configuration.state.value() {
            Some(configuration) => {
                let mut body = div()
                    .flex()
                    .flex_col()
                    .gap_2()
                    .child(format!("Observation: {:?}", configuration.observation));
                for (index, snapshot) in configuration.snapshots.iter().enumerate() {
                    let retry_snapshot = snapshot.clone();
                    let queue_snapshot = snapshot.clone();
                    let timeout_snapshot = snapshot.clone();
                    body = body.child(
                        div()
                            .flex()
                            .flex_col()
                            .gap_2()
                            .child(format!(
                                "{} / {} · version={} · retry_max_times={} · retry_queue_nums={} · consume_timeout_minutes={}",
                                snapshot.identity.target.cluster_name(),
                                snapshot.identity.target.broker_name(),
                                snapshot.generation,
                                snapshot.entries.retry_max_times,
                                snapshot.entries.retry_queue_nums,
                                snapshot.entries.consume_timeout_minutes
                            ))
                            .when(
                                self.capabilities.edit
                                    == rocketmq_dashboard_common::CapabilityAvailability::Available,
                                |this| {
                                    this.child(
                                        div()
                                            .flex()
                                            .gap_2()
                                            .child(
                                                Button::new(("draft-retry-max", index))
                                                    .debug_selector(move || format!("draft-retry-max-{index}"))
                                                    .label("Draft retry max +1")
                                                    .small()
                                                    .outline()
                                                    .on_click(cx.listener(move |detail, _, _, cx| {
                                                        detail.draft_config_patch(
                                                            retry_snapshot.clone(),
                                                            ConsumerConfigPatch {
                                                                retry_max_times: Some(
                                                                    retry_snapshot
                                                                        .entries
                                                                        .retry_max_times
                                                                        .saturating_add(1)
                                                                        .min(16),
                                                                ),
                                                                ..ConsumerConfigPatch::default()
                                                            },
                                                            cx,
                                                        );
                                                    })),
                                            )
                                            .child(
                                                Button::new(("draft-retry-queues", index))
                                                    .label("Draft retry queues +1")
                                                    .small()
                                                    .outline()
                                                    .on_click(cx.listener(move |detail, _, _, cx| {
                                                        detail.draft_config_patch(
                                                            queue_snapshot.clone(),
                                                            ConsumerConfigPatch {
                                                                retry_queue_nums: Some(
                                                                    queue_snapshot
                                                                        .entries
                                                                        .retry_queue_nums
                                                                        .saturating_add(1)
                                                                        .min(8),
                                                                ),
                                                                ..ConsumerConfigPatch::default()
                                                            },
                                                            cx,
                                                        );
                                                    })),
                                            )
                                            .child(
                                                Button::new(("draft-consume-timeout", index))
                                                    .label("Draft timeout +1")
                                                    .small()
                                                    .outline()
                                                    .on_click(cx.listener(move |detail, _, _, cx| {
                                                        detail.draft_config_patch(
                                                            timeout_snapshot.clone(),
                                                            ConsumerConfigPatch {
                                                                consume_timeout_minutes: Some(
                                                                    timeout_snapshot
                                                                        .entries
                                                                        .consume_timeout_minutes
                                                                        .saturating_add(1)
                                                                        .min(1_440),
                                                                ),
                                                                ..ConsumerConfigPatch::default()
                                                            },
                                                            cx,
                                                        );
                                                    })),
                                            ),
                                    )
                                },
                            ),
                    );
                }
                if let Some(draft) = &self.edit_draft {
                    body = body
                        .child(format!(
                            "Draft target: {} / {} · expected generation {} · only retry_max_times, retry_queue_nums, and consume_timeout_minutes can change.",
                            draft.snapshot.identity.target.cluster_name(),
                            draft.snapshot.identity.target.broker_name(),
                            draft.snapshot.generation
                        ))
                        .child(
                            Button::new("apply-consumer-config-draft")
                                .debug_selector(|| "apply-consumer-config-draft".to_owned())
                                .label("Apply exact-target CAS")
                                .primary()
                                .on_click(cx.listener(|detail, _, _, cx| detail.submit_config_patch(cx))),
                        );
                }
                body
            }
            None => loading_or_empty(&self.store.configuration.state),
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
        div()
            .id("consumer-detail")
            .size_full()
            .p_4()
            .flex()
            .flex_col()
            .gap_4()
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
            .child(self.render_active(window, cx))
    }
}

fn render_consumer_outcome(outcome: &ConsumerPartialOutcome, cx: &gpui::App) -> impl IntoElement {
    div()
        .p_2()
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
        Loadable::Failed { error, .. } => Some(error.summary().to_owned()),
        _ => None,
    }
}

fn loading_or_empty<T>(state: &Loadable<T>) -> gpui::Div {
    match state {
        Loadable::Empty => div().child("No authoritative data was returned."),
        _ => div().child("Loading…"),
    }
}
