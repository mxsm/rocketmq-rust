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

//! Official Topic dialog controls and redacted outcome rendering.

use gpui::prelude::FluentBuilder as _;
use gpui::{Context, InteractiveElement as _, IntoElement, ParentElement as _, Render, Styled as _, Window, div};
use gpui_component::{ActiveTheme as _, button::Button, checkbox::Checkbox, input::Input, radio::Radio};

use super::*;

impl Render for TopicDialogForm {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let fields = match &self.inputs {
            TopicDialogInputs::Create {
                topic,
                read_queues,
                write_queues,
                selected_targets,
                permission,
                message_type,
                ordered,
            } => {
                let target_controls = if let TopicDialogKind::Create(draft) = &self.state.kind {
                    draft
                        .targets
                        .iter()
                        .enumerate()
                        .map(|(index, target)| {
                            let owner = cx.entity().downgrade();
                            Checkbox::new(("topic-create-target", index))
                                .label(format!(
                                    "{} / {} / {}",
                                    target.cluster_name(),
                                    target.broker_name(),
                                    target.broker_address()
                                ))
                                .checked(selected_targets.get(index).copied().unwrap_or(false))
                                .on_click(move |checked, _, cx| {
                                    let _ = owner.update(cx, |form, cx| {
                                        if let TopicDialogInputs::Create { selected_targets, .. } = &mut form.inputs
                                            && let Some(selected) = selected_targets.get_mut(index)
                                        {
                                            *selected = *checked;
                                            cx.notify();
                                        }
                                    });
                                })
                        })
                        .collect::<Vec<_>>()
                } else {
                    Vec::new()
                };
                let permission_controls = [
                    (4, "Read only"),
                    (2, "Write only"),
                    (6, "Read + write"),
                    (7, "Read + write + inherit"),
                ]
                .into_iter()
                .map(|(bits, label)| {
                    let owner = cx.entity().downgrade();
                    let option = TopicPermission::parse(bits).ok();
                    Radio::new(("topic-create-permission", bits as usize))
                        .label(label)
                        .checked(*permission == option)
                        .on_click(move |checked, _, cx| {
                            if *checked {
                                let _ = owner.update(cx, |form, cx| {
                                    if let TopicDialogInputs::Create { permission, .. } = &mut form.inputs {
                                        *permission = option;
                                        cx.notify();
                                    }
                                });
                            }
                        })
                })
                .collect::<Vec<_>>();
                let message_type_controls = [
                    (TopicMessageType::Normal, "Normal"),
                    (TopicMessageType::Fifo, "FIFO"),
                    (TopicMessageType::Delay, "Delay"),
                    (TopicMessageType::Transaction, "Transaction"),
                ]
                .into_iter()
                .enumerate()
                .map(|(index, (option, label))| {
                    let owner = cx.entity().downgrade();
                    Radio::new(("topic-create-message-type", index))
                        .label(label)
                        .checked(*message_type == Some(option))
                        .on_click(move |checked, _, cx| {
                            if *checked {
                                let _ = owner.update(cx, |form, cx| {
                                    if let TopicDialogInputs::Create { message_type, .. } = &mut form.inputs {
                                        *message_type = Some(option);
                                        cx.notify();
                                    }
                                });
                            }
                        })
                })
                .collect::<Vec<_>>();
                let unordered = cx.entity().downgrade();
                let ordered_owner = cx.entity().downgrade();
                div()
                    .flex()
                    .flex_col()
                    .gap_2()
                    .child(div().id("topic-create-name").child(Input::new(topic)))
                    .child(div().id("topic-create-read-queues").child(Input::new(read_queues)))
                    .child(div().id("topic-create-write-queues").child(Input::new(write_queues)))
                    .child(div().text_sm().child("Exact Broker targets"))
                    .child(div().id("topic-create-targets").children(target_controls))
                    .child(div().text_sm().child("Permission"))
                    .child(div().id("topic-create-permissions").children(permission_controls))
                    .child(div().text_sm().child("Message type"))
                    .child(div().id("topic-create-message-types").children(message_type_controls))
                    .child(div().text_sm().child("Ordering"))
                    .child(
                        Radio::new("topic-create-unordered")
                            .label("Unordered")
                            .checked(*ordered == Some(false))
                            .on_click(move |checked, _, cx| {
                                if *checked {
                                    let _ = unordered.update(cx, |form, cx| {
                                        if let TopicDialogInputs::Create { ordered, .. } = &mut form.inputs {
                                            *ordered = Some(false);
                                            cx.notify();
                                        }
                                    });
                                }
                            }),
                    )
                    .child(
                        Radio::new("topic-create-ordered")
                            .label("Ordered")
                            .checked(*ordered == Some(true))
                            .on_click(move |checked, _, cx| {
                                if *checked {
                                    let _ = ordered_owner.update(cx, |form, cx| {
                                        if let TopicDialogInputs::Create { ordered, .. } = &mut form.inputs {
                                            *ordered = Some(true);
                                            cx.notify();
                                        }
                                    });
                                }
                            }),
                    )
            }
            TopicDialogInputs::Edit {
                read_queues,
                write_queues,
            } => div()
                .flex()
                .flex_col()
                .gap_2()
                .child(div().id("topic-edit-read-queues").child(Input::new(read_queues)))
                .child(div().id("topic-edit-write-queues").child(Input::new(write_queues))),
            TopicDialogInputs::Send { key, tag, body } => div()
                .flex()
                .flex_col()
                .gap_2()
                .child(div().id("topic-send-key").child(Input::new(key)))
                .child(div().id("topic-send-tag").child(Input::new(tag)))
                .child(div().id("topic-send-body").child(Input::new(body))),
            TopicDialogInputs::Reset {
                timestamp,
                selected_cluster,
            } => div()
                .flex()
                .flex_col()
                .gap_2()
                .child(div().id("topic-reset-timestamp").child(Input::new(timestamp)))
                .child(self.render_cluster_choices(selected_cluster.as_deref(), cx)),
            TopicDialogInputs::DeleteTopic { confirmation } => {
                div().child(div().id("topic-delete-confirmation").child(Input::new(confirmation)))
            }
            TopicDialogInputs::OffsetConfirmation { selected_cluster } => {
                self.render_cluster_choices(selected_cluster.as_deref(), cx)
            }
            TopicDialogInputs::Confirmation => div(),
        };
        let submission_summary = self.submission_summary();
        let conflict_actions = matches!(self.state.submission, TopicSubmissionState::Conflict { .. }).then(|| {
            let adopt = cx.entity().downgrade();
            let keep = cx.entity().downgrade();
            div()
                .flex()
                .gap_2()
                .child(
                    Button::new("topic-edit-adopt-authoritative")
                        .label("Adopt authoritative")
                        .outline()
                        .on_click(move |_, window, cx| {
                            let _ = adopt.update(cx, |form, cx| form.resolve_conflict(true, window, cx));
                        }),
                )
                .child(
                    Button::new("topic-edit-keep-submitted")
                        .label("Keep submitted values")
                        .outline()
                        .on_click(move |_, window, cx| {
                            let _ = keep.update(cx, |form, cx| form.resolve_conflict(false, window, cx));
                        }),
                )
        });
        div()
            .flex()
            .flex_col()
            .gap_3()
            .child(
                div()
                    .text_sm()
                    .text_color(cx.theme().muted_foreground)
                    .child(self.description()),
            )
            .child(fields)
            .when_some(submission_summary, |this, summary| {
                this.child(div().text_sm().child(summary))
            })
            .when_some(conflict_actions, |this, actions| this.child(actions))
            .when_some(self.status.clone(), |this, status| {
                this.child(div().text_sm().child(status))
            })
    }
}

pub(super) fn format_outcome(prefix: &str, outcome: &TopicPartialOutcome) -> String {
    let targets = outcome
        .targets
        .iter()
        .map(|target| {
            format!(
                "{}: stage={}, code={}, retryable={}",
                target.target,
                failure_stage_label(target.stage),
                target.failure.map_or("applied", failure_code_label),
                target.retryable
            )
        })
        .collect::<Vec<_>>()
        .join("; ");
    format!(
        "{prefix}: {} applied, {} failed. {targets}",
        outcome.applied_count(),
        outcome.failed_count()
    )
}

fn failure_stage_label(stage: TopicFailureStage) -> &'static str {
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

fn failure_code_label(code: TopicFailureCode) -> &'static str {
    match code {
        TopicFailureCode::NotFound => "not_found",
        TopicFailureCode::InvalidData => "invalid_data",
        TopicFailureCode::Unavailable => "unavailable",
        TopicFailureCode::Conflict => "conflict",
    }
}
