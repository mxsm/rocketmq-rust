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

//! Full-field edit drafts and explicit conflict rebasing.

use super::*;
use gpui_component::{input::Input, radio::Radio};
use rocketmq_dashboard_common::{TopicConfigTargetView, TopicConfigValues};

pub struct TopicEditDraft {
    pub topic: TopicIdentity,
    pub targets: Vec<TopicConfigTargetView>,
    pub primary_target: TopicTargetIdentity,
    pub baseline: TopicConfigValues,
    /// Fields explicitly retained after a conflict stay intentional even when
    /// the first Broker already has their requested values.
    kept_fields: TopicConfigFieldsPatch,
}

impl TopicEditDraft {
    pub fn new(
        topic: TopicIdentity,
        primary: &TopicConfigTargetView,
        targets: Vec<TopicConfigTargetView>,
    ) -> Option<Self> {
        Some(Self {
            topic,
            baseline: primary.editable_values()?,
            primary_target: primary.target.clone(),
            targets,
            kept_fields: TopicConfigFieldsPatch::default(),
        })
    }
}

impl fmt::Debug for TopicEditDraft {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TopicEditDraft")
            .field("target_count", &self.targets.len())
            .field("baseline", &self.baseline)
            .finish_non_exhaustive()
    }
}

pub(super) struct TopicEditInputs {
    pub(super) read_queues: Entity<InputState>,
    pub(super) write_queues: Entity<InputState>,
    pub(super) permission: TopicPermission,
    pub(super) ordered: bool,
    pub(super) message_type: TopicMessageType,
    pub(super) selected_targets: Vec<bool>,
}

impl TopicEditInputs {
    pub(super) fn new(draft: &TopicEditDraft, window: &mut Window, cx: &mut Context<TopicDialogForm>) -> Self {
        Self {
            read_queues: number_input(draft.baseline.read_queue_count, "Read queues", window, cx),
            write_queues: number_input(draft.baseline.write_queue_count, "Write queues", window, cx),
            permission: draft.baseline.permission,
            ordered: draft.baseline.ordered,
            message_type: draft.baseline.message_type,
            selected_targets: draft
                .targets
                .iter()
                .map(|target| target.target == draft.primary_target)
                .collect(),
        }
    }

    fn values(&self, cx: &gpui::App) -> Result<TopicConfigValues, UiError> {
        Ok(TopicConfigValues {
            read_queue_count: queue_count(&self.read_queues, cx)?,
            write_queue_count: queue_count(&self.write_queues, cx)?,
            permission: self.permission,
            ordered: self.ordered,
            message_type: self.message_type,
        })
    }
}

pub(super) fn prepare(
    draft: &TopicEditDraft,
    inputs: &TopicEditInputs,
    cx: &gpui::App,
) -> Result<TopicFieldsPatchCommand, UiError> {
    let values = inputs.values(cx)?;
    let changed = values.changes_from(draft.baseline);
    let kept = draft.kept_fields;
    let patch = TopicConfigFieldsPatch {
        read_queue_count: (kept.read_queue_count.is_some() || changed.read_queue_count.is_some())
            .then_some(values.read_queue_count),
        write_queue_count: (kept.write_queue_count.is_some() || changed.write_queue_count.is_some())
            .then_some(values.write_queue_count),
        permission: (kept.permission.is_some() || changed.permission.is_some()).then_some(values.permission),
        ordered: (kept.ordered.is_some() || changed.ordered.is_some()).then_some(values.ordered),
        message_type: (kept.message_type.is_some() || changed.message_type.is_some()).then_some(values.message_type),
    };
    if patch.is_empty() {
        return Err(validation("change at least one Topic field"));
    }
    let targets = draft
        .targets
        .iter()
        .zip(&inputs.selected_targets)
        .filter(|(_, selected)| **selected)
        .map(|(target, _)| target.clone())
        .collect::<Vec<_>>();
    if targets.is_empty() || targets.iter().any(|target| target.editable_values().is_none()) {
        return Err(validation(
            "select exact Brokers with a supported, complete configuration",
        ));
    }
    Ok(TopicFieldsPatchCommand {
        topic: draft.topic.clone(),
        targets,
        patch,
    })
}

pub(super) fn conflict_comparison(targets: &[TopicConfigTargetView], patch: TopicConfigFieldsPatch) -> String {
    targets.iter().filter_map(|target| {
        let current = target.editable_values()?;
        let draft = current.with_patch(patch);
        Some(format!(
            "{} / {} version {}: authoritative → kept draft: read {} → {}, write {} → {}, permission {} → {}, ordered {} → {}, message type {:?} → {:?}. Unchanged fields retain each Broker's current values.",
            target.target.cluster_name(), target.target.broker_name(), target.version,
            current.read_queue_count, draft.read_queue_count, current.write_queue_count, draft.write_queue_count,
            current.permission.bits(), draft.permission.bits(), current.ordered, draft.ordered, current.message_type, draft.message_type,
        ))
    }).collect::<Vec<_>>().join(" ")
}

impl TopicDialogForm {
    pub(super) fn resolve_conflict(&mut self, adopt_authoritative: bool, window: &mut Window, cx: &mut Context<Self>) {
        let TopicSubmissionState::FieldsCompleted {
            result,
            submitted_patch,
        } = &self.state.submission
        else {
            return;
        };
        let Some(primary) = result.edit.conflicts.first() else {
            return;
        };
        let Some(baseline) = primary.editable_values() else {
            return;
        };
        let desired = if adopt_authoritative {
            baseline
        } else {
            baseline.with_patch(*submitted_patch)
        };
        let kept_fields = if adopt_authoritative {
            TopicConfigFieldsPatch::default()
        } else {
            *submitted_patch
        };
        let previous_outcome = render::format_outcome("Previous attempt", &result.edit.outcome);
        let targets = result.edit.conflicts.clone();
        let primary_target = primary.target.clone();
        if let TopicDialogKind::Edit(draft) = &mut self.state.kind {
            draft.targets = targets;
            draft.primary_target = primary_target;
            draft.baseline = baseline;
            draft.kept_fields = kept_fields;
        }
        if let TopicDialogInputs::Edit(inputs) = &mut self.inputs {
            inputs.read_queues.update(cx, |input, cx| {
                input.set_value(desired.read_queue_count.to_string(), window, cx)
            });
            inputs.write_queues.update(cx, |input, cx| {
                input.set_value(desired.write_queue_count.to_string(), window, cx)
            });
            inputs.permission = desired.permission;
            inputs.ordered = desired.ordered;
            inputs.message_type = desired.message_type;
            inputs.selected_targets = if let TopicDialogKind::Edit(draft) = &self.state.kind {
                vec![true; draft.targets.len()]
            } else {
                Vec::new()
            };
        }
        self.state.submission = TopicSubmissionState::Idle;
        self.status = Some(format!(
            "{previous_outcome}. {}. Only conflicted Brokers remain selected; review every field and confirm again.",
            if adopt_authoritative {
                "Adopted authoritative values"
            } else {
                "Kept changed draft fields on the authoritative versions"
            }
        ));
        cx.notify();
    }

    #[cfg(test)]
    pub(crate) fn keep_submitted_after_conflict(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.resolve_conflict(false, window, cx);
    }

    pub(super) fn render_edit_fields(&self, inputs: &TopicEditInputs, cx: &mut Context<Self>) -> gpui::Div {
        let permissions = [2, 3, 4, 5, 6, 7].into_iter().filter_map(|bits| {
            let permission = TopicPermission::parse(bits).ok()?;
            let owner = cx.entity().downgrade();
            Some(
                div().id(("topic-edit-permission-wrap", bits as usize)).child(
                    Radio::new(("topic-edit-permission", bits as usize))
                        .debug_selector(move || format!("topic-edit-permission-{bits}"))
                        .label(format!(
                            "{}{}{}",
                            if permission.can_read() { "Read " } else { "" },
                            if permission.can_write() { "Write " } else { "" },
                            if permission.inherits() { "+ Inherit" } else { "" }
                        ))
                        .checked(inputs.permission == permission)
                        .disabled(!matches!(self.state.submission, TopicSubmissionState::Idle))
                        .on_click(move |checked, _, cx| {
                            if *checked {
                                let _ = owner.update(cx, |form, cx| {
                                    if let TopicDialogInputs::Edit(inputs) = &mut form.inputs {
                                        inputs.permission = permission;
                                        cx.notify();
                                    }
                                });
                            }
                        }),
                ),
            )
        });
        let message_types = [
            TopicMessageType::Normal,
            TopicMessageType::Fifo,
            TopicMessageType::Delay,
            TopicMessageType::Transaction,
            TopicMessageType::Unspecified,
        ]
        .into_iter()
        .enumerate()
        .map(|(index, option)| {
            let owner = cx.entity().downgrade();
            div().id(("topic-edit-type-wrap", index)).child(
                Radio::new(("topic-edit-message-type", index))
                    .debug_selector(move || format!("topic-edit-message-type-{index}"))
                    .label(format!("{option:?}"))
                    .checked(inputs.message_type == option)
                    .disabled(!matches!(self.state.submission, TopicSubmissionState::Idle))
                    .on_click(move |checked, _, cx| {
                        if *checked {
                            let _ = owner.update(cx, |form, cx| {
                                if let TopicDialogInputs::Edit(inputs) = &mut form.inputs {
                                    inputs.message_type = option;
                                    cx.notify();
                                }
                            });
                        }
                    }),
            )
        });
        let mut fields = div()
            .flex()
            .flex_col()
            .gap_2()
            .child("Read queues (1–128)")
            .child(div().id("topic-edit-read-queues").child(
                Input::new(&inputs.read_queues).disabled(!matches!(self.state.submission, TopicSubmissionState::Idle)),
            ))
            .child("Write queues (1–128)")
            .child(div().id("topic-edit-write-queues").child(
                Input::new(&inputs.write_queues).disabled(!matches!(self.state.submission, TopicSubmissionState::Idle)),
            ))
            .child("Permission")
            .child(div().flex().flex_wrap().gap_2().children(permissions))
            .child("Message type")
            .child(div().flex().flex_wrap().gap_2().children(message_types));
        for ordered in [false, true] {
            let owner = cx.entity().downgrade();
            fields = fields.child(
                div().id(("topic-edit-order-wrap", usize::from(ordered))).child(
                    Radio::new(("topic-edit-order", usize::from(ordered)))
                        .debug_selector(move || format!("topic-edit-ordered-{ordered}"))
                        .label(if ordered { "Ordered" } else { "Unordered" })
                        .checked(inputs.ordered == ordered)
                        .disabled(!matches!(self.state.submission, TopicSubmissionState::Idle))
                        .on_click(move |checked, _, cx| {
                            if *checked {
                                let _ = owner.update(cx, |form, cx| {
                                    if let TopicDialogInputs::Edit(inputs) = &mut form.inputs {
                                        inputs.ordered = ordered;
                                        cx.notify();
                                    }
                                });
                            }
                        }),
                ),
            );
        }
        if let TopicDialogKind::Edit(draft) = &self.state.kind {
            fields = fields.child(self.render_target_choices(
                draft.targets.iter().map(|target| target.target.clone()).collect(),
                &inputs.selected_targets,
                true,
                cx,
            ));
        }
        fields
    }
}
