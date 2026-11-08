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

//! Explicit Consumer create/edit/delete drafts and exact Broker selection.

use gpui::prelude::FluentBuilder as _;
use gpui::{
    AppContext as _, Context, Entity, InteractiveElement as _, IntoElement, ParentElement as _, Render,
    StatefulInteractiveElement as _, Styled as _, Window, div, px,
};
use gpui_component::{
    ActiveTheme as _, Disableable as _,
    button::Button,
    checkbox::Checkbox,
    input::{Input, InputState},
};
use rocketmq_dashboard_common::{
    ConsumerAclClassification, ConsumerConfigEntries, ConsumerConfigPatch, ConsumerConfigPatchCommand,
    ConsumerConfigSnapshot, ConsumerCreateCommand, ConsumerDeleteCommand, ConsumerIdentity, ConsumerTargetIdentity,
};

#[derive(Clone)]
pub enum ConsumerDialogKind {
    Create {
        targets: Vec<ConsumerTargetIdentity>,
    },
    Edit(ConsumerConfigSnapshot),
    Delete {
        group: ConsumerIdentity,
        targets: Vec<ConsumerTargetIdentity>,
    },
}

pub enum PreparedConsumerCommand {
    Create(ConsumerCreateCommand),
    Edit(Box<ConsumerConfigPatchCommand>),
    Delete(ConsumerDeleteCommand),
}

#[derive(Clone, Copy)]
pub enum ConsumerNumberField {
    RetryMax,
    RetryQueues,
    Timeout,
    BrokerId,
    SlowBrokerId,
}
impl ConsumerNumberField {
    fn all() -> [Self; 5] {
        [
            Self::RetryMax,
            Self::RetryQueues,
            Self::Timeout,
            Self::BrokerId,
            Self::SlowBrokerId,
        ]
    }
    pub fn index(self) -> usize {
        match self {
            Self::RetryMax => 0,
            Self::RetryQueues => 1,
            Self::Timeout => 2,
            Self::BrokerId => 3,
            Self::SlowBrokerId => 4,
        }
    }
    fn label(self) -> &'static str {
        match self {
            Self::RetryMax => "Maximum retries (-1 or greater)",
            Self::RetryQueues => "Retry queues (0 or greater)",
            Self::Timeout => "Consume timeout (minutes)",
            Self::BrokerId => "Broker ID",
            Self::SlowBrokerId => "Slow consumer Broker ID",
        }
    }
    pub fn selector(self) -> &'static str {
        match self {
            Self::RetryMax => "consumer-field-retry-max",
            Self::RetryQueues => "consumer-field-retry-queues",
            Self::Timeout => "consumer-field-timeout",
            Self::BrokerId => "consumer-field-broker-id",
            Self::SlowBrokerId => "consumer-field-slow-broker-id",
        }
    }
}

pub struct ConsumerDialogForm {
    pub kind: ConsumerDialogKind,
    group: Entity<InputState>,
    numbers: Vec<Entity<InputState>>,
    flags: [bool; 4],
    selected_targets: Vec<bool>,
    busy: bool,
    submitted: bool,
    status: Option<String>,
}

impl ConsumerDialogForm {
    pub fn new(
        kind: ConsumerDialogKind,
        group: Entity<InputState>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let entries = match &kind {
            ConsumerDialogKind::Edit(snapshot) => snapshot.entries,
            _ => ConsumerConfigEntries::default(),
        };
        let values = [
            entries.retry_max_times.to_string(),
            entries.retry_queue_nums.to_string(),
            entries.consume_timeout_minutes.to_string(),
            entries.broker_id.to_string(),
            entries.which_broker_when_consume_slowly.to_string(),
        ];
        let numbers = values
            .into_iter()
            .map(|value| cx.new(|cx| InputState::new(window, cx).default_value(value)))
            .collect();
        let target_count = match &kind {
            ConsumerDialogKind::Create { targets } | ConsumerDialogKind::Delete { targets, .. } => targets.len(),
            ConsumerDialogKind::Edit(_) => 0,
        };
        Self {
            kind,
            group,
            numbers,
            flags: [
                entries.consume_enable,
                entries.consume_from_min_enable,
                entries.consume_broadcast_enable,
                entries.consume_message_orderly,
            ],
            selected_targets: vec![false; target_count],
            busy: false,
            submitted: false,
            status: None,
        }
    }
    pub fn title(&self) -> &'static str {
        match self.kind {
            ConsumerDialogKind::Create { .. } => "Create Consumer group",
            ConsumerDialogKind::Edit(_) => "Edit Consumer configuration",
            ConsumerDialogKind::Delete { .. } => "Delete Consumer targets",
        }
    }
    pub fn can_cancel(&self) -> bool {
        !self.busy
    }
    pub fn finish(&mut self, status: String, cx: &mut Context<Self>) {
        self.busy = false;
        self.status = Some(status);
        cx.notify();
    }
    pub fn error(&mut self, status: impl Into<String>, cx: &mut Context<Self>) {
        self.status = Some(status.into());
        cx.notify();
    }
    pub fn begin(&mut self, cx: &mut Context<Self>) -> Result<PreparedConsumerCommand, &'static str> {
        if self.busy || self.submitted {
            return Err("This command has already been submitted. Close and reload before a new command.");
        }
        let command = self.prepare(cx)?;
        self.busy = true;
        self.submitted = true;
        self.status = Some("Submitting the reviewed exact targets…".into());
        cx.notify();
        Ok(command)
    }
    pub fn prepare(&self, cx: &gpui::App) -> Result<PreparedConsumerCommand, &'static str> {
        let selected = |targets: &[ConsumerTargetIdentity]| {
            targets
                .iter()
                .zip(&self.selected_targets)
                .filter(|(_, selected)| **selected)
                .map(|(target, _)| target.clone())
                .collect::<Vec<_>>()
        };
        let authorization = ConsumerAclClassification::Authorized;
        if let ConsumerDialogKind::Delete { group, targets } = &self.kind {
            return ConsumerDeleteCommand {
                group: group.clone(),
                selected_targets: selected(targets),
                authoritative_targets: targets.clone(),
                authorization,
            }
            .validate()
            .map(PreparedConsumerCommand::Delete)
            .map_err(|_| "Select at least one authoritative Broker target.");
        }
        let value = |field: ConsumerNumberField| self.numbers[field.index()].read(cx).value().trim().to_owned();
        let mut entries = match &self.kind {
            ConsumerDialogKind::Edit(snapshot) => snapshot.entries,
            _ => ConsumerConfigEntries::default(),
        };
        entries.retry_max_times = value(ConsumerNumberField::RetryMax)
            .parse()
            .map_err(|_| "Maximum retries must be an integer (-1 or greater).")?;
        entries.retry_queue_nums = value(ConsumerNumberField::RetryQueues)
            .parse()
            .map_err(|_| "Retry queues must be a non-negative integer.")?;
        entries.consume_timeout_minutes = value(ConsumerNumberField::Timeout)
            .parse()
            .map_err(|_| "Consume timeout must be a positive integer.")?;
        entries.broker_id = value(ConsumerNumberField::BrokerId)
            .parse()
            .map_err(|_| "Broker ID must be a non-negative integer.")?;
        entries.which_broker_when_consume_slowly = value(ConsumerNumberField::SlowBrokerId)
            .parse()
            .map_err(|_| "Slow consumer Broker ID must be a non-negative integer.")?;
        entries.consume_enable = self.flags[0];
        entries.consume_from_min_enable = self.flags[1];
        entries.consume_broadcast_enable = self.flags[2];
        entries.consume_message_orderly = self.flags[3];
        entries
            .validate()
            .map_err(|_| "Retry limits or consume timeout are outside the Broker's legal range.")?;
        match &self.kind {
            ConsumerDialogKind::Create { targets } => {
                let group = ConsumerIdentity::parse(self.group.read(cx).value().to_string())
                    .map_err(|_| "Enter a valid Consumer group.")?;
                ConsumerCreateCommand {
                    group,
                    targets: selected(targets),
                    entries,
                    authorization,
                }
                .validate()
                .map(PreparedConsumerCommand::Create)
                .map_err(|_| "Select at least one Broker target.")
            }
            ConsumerDialogKind::Edit(snapshot) => ConsumerConfigPatchCommand {
                snapshot: snapshot.clone(),
                patch: ConsumerConfigPatch::between(snapshot.entries, entries),
                authorization,
            }
            .validate()
            .map(|command| PreparedConsumerCommand::Edit(Box::new(command)))
            .map_err(|_| "Change at least one editable field before Apply."),
            ConsumerDialogKind::Delete { .. } => Err("The delete form has no editable configuration."),
        }
    }
    #[cfg(test)]
    pub(crate) fn number_input(&self, field: ConsumerNumberField) -> Entity<InputState> {
        self.numbers[field.index()].clone()
    }
}

impl Render for ConsumerDialogForm {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let locked = self.busy || self.submitted;
        let targets = match &self.kind {
            ConsumerDialogKind::Create { targets } | ConsumerDialogKind::Delete { targets, .. } => targets.as_slice(),
            ConsumerDialogKind::Edit(_) => &[],
        };
        let mut clusters = targets
            .iter()
            .map(|target| target.cluster_name().to_owned())
            .collect::<Vec<_>>();
        clusters.sort();
        clusters.dedup();
        let cluster_controls = clusters
            .into_iter()
            .enumerate()
            .map(|(index, cluster)| {
                let members = targets
                    .iter()
                    .enumerate()
                    .filter(|(_, t)| t.cluster_name() == cluster)
                    .map(|(index, _)| index)
                    .collect::<Vec<_>>();
                let all_selected = members.iter().all(|i| self.selected_targets[*i]);
                Button::new(("consumer-select-cluster", index))
                    .debug_selector(move || format!("consumer-select-cluster-{index}"))
                    .label(format!("{} {cluster}", if all_selected { "Clear" } else { "Select" }))
                    .disabled(locked)
                    .on_click(cx.listener(move |form, _, _, cx| {
                        if !form.busy && !form.submitted {
                            for i in &members {
                                form.selected_targets[*i] = !all_selected;
                            }
                            cx.notify();
                        }
                    }))
            })
            .collect::<Vec<_>>();
        let target_controls = targets
            .iter()
            .enumerate()
            .map(|(index, target)| {
                Checkbox::new(("consumer-target", index))
                    .debug_selector(move || format!("consumer-target-{index}"))
                    .label(format!(
                        "{} / {} / {}",
                        target.cluster_name(),
                        target.broker_name(),
                        target.broker_address()
                    ))
                    .checked(self.selected_targets[index])
                    .disabled(locked)
                    .on_click(cx.listener(move |form, checked, _, cx| {
                        if !form.busy && !form.submitted {
                            form.selected_targets[index] = *checked;
                            cx.notify();
                        }
                    }))
            })
            .collect::<Vec<_>>();
        let is_create = matches!(self.kind, ConsumerDialogKind::Create { .. });
        let is_delete = matches!(self.kind, ConsumerDialogKind::Delete { .. });
        let numeric_controls = ConsumerNumberField::all()
            .into_iter()
            .map(|field| {
                div().min_w_0().flex().flex_col().gap_1().child(field.label()).child(
                    div()
                        .debug_selector(move || field.selector().to_owned())
                        .child(Input::new(&self.numbers[field.index()]).disabled(locked)),
                )
            })
            .collect::<Vec<_>>();
        let flag_controls = [
            "Consume enabled",
            "Consume from minimum",
            "Broadcast consume",
            "Orderly consume",
        ]
        .into_iter()
        .enumerate()
        .map(|(index, label)| {
            Checkbox::new(("consumer-flag", index))
                .debug_selector(move || format!("consumer-flag-{index}"))
                .label(label)
                .checked(self.flags[index])
                .disabled(locked)
                .on_click(cx.listener(move |form, checked, _, cx| {
                    if !form.busy && !form.submitted {
                        form.flags[index] = *checked;
                        cx.notify();
                    }
                }))
        })
        .collect::<Vec<_>>();
        let body = div().w_full().min_w_0().flex().flex_col().gap_3()
            .when(is_create,|body| body.child("Consumer group").child(Input::new(&self.group).disabled(locked)))
            .when(!targets.is_empty(),|body| body.child("Choose exact Broker targets; cluster buttons select the displayed members.")
                .child(div().flex().flex_wrap().gap_2().children(cluster_controls)).children(target_controls))
            .when_some(match &self.kind {ConsumerDialogKind::Edit(snapshot)=>Some(format!("{} / {} · expected version {}",snapshot.identity.target.cluster_name(),snapshot.identity.target.broker_name(),snapshot.generation)),_=>None},|body,summary| body.child(summary))
            .when(!is_delete,|body| body.child(div().grid().grid_cols(2).gap_3().children(numeric_controls))
                .child(div().flex().flex_wrap().gap_3().children(flag_controls))
                .child("Notifications, system flag, retry policy, subscriptions and attributes retain the Broker's existing values during edits."))
            .when(is_delete,|body| body.child("A fresh authoritative inventory and connection check must prove every existing target disconnected. Internal Topic cleanup occurs only when every target is selected and deleted."))
            .when_some(self.status.clone(),|body,status| body.child(div().p_2().rounded_md().bg(cx.theme().muted).child(status)));
        // Reserve the dialog title, footer, padding and window margins outside the scroll body.
        div()
            .id("consumer-mutation-form")
            .debug_selector(|| "consumer-mutation-form".to_owned())
            .w_full()
            .max_h((window.viewport_size().height - px(200.)).max(px(120.)))
            .overflow_y_scroll()
            .child(body)
    }
}
