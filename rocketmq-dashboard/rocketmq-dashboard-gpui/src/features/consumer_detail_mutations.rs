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

//! Exact-target Consumer mutation and D4 offset-action coordination.

use std::time::{SystemTime, UNIX_EPOCH};

use gpui::{AppContext as _, Context, InteractiveElement as _, ParentElement as _, Styled as _, Window, px};
use gpui_component::{
    WindowExt as _,
    dialog::{Dialog, DialogButtonProps},
};
use rocketmq_dashboard_common::{ConsumerDeleteCommand, ConsumerObservation, TopicIdentity};

use super::{ConsumerDetail, ConsumerDetailIntent};
use crate::{
    features::consumer_dialogs::{ConsumerDialogForm, ConsumerDialogKind, PreparedConsumerCommand},
    features::topic_dialogs::{PreparedTopicCommand, TopicDialogForm, TopicDialogKind, TopicSubmissionState},
    services::{
        consumers::{
            ConsumerCacheInvalidation, ConsumerConfigMutationResult, ConsumerMutationResult, ConsumerRequestScope,
        },
        topics::{TopicMutationResult, TopicRequestScope},
    },
    state::UiError,
};

impl ConsumerDetail {
    fn next_consumer_scope(&mut self) -> ConsumerRequestScope {
        self.mutation_epoch = self.mutation_epoch.saturating_add(1);
        ConsumerRequestScope {
            revision: self.revision,
            epoch: self.mutation_epoch,
            query_scope: Default::default(),
        }
    }

    pub(super) fn request_delete(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.targets.is_empty() {
            return;
        }
        self.open_consumer_dialog(
            ConsumerDialogKind::Delete {
                group: self.store.group.clone(),
                targets: self.targets.clone(),
            },
            window,
            cx,
        );
    }

    pub(super) fn open_config_dialog(
        &mut self,
        snapshot: rocketmq_dashboard_common::ConsumerConfigSnapshot,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.open_consumer_dialog(ConsumerDialogKind::Edit(snapshot), window, cx);
    }

    fn open_consumer_dialog(&mut self, kind: ConsumerDialogKind, window: &mut Window, cx: &mut Context<Self>) {
        if self.mutation_task.is_some() || self.mutation_dialog.is_some() || self.offset_dialog.is_some() {
            return;
        }
        self.mutation_replay_blocked = false;
        let group = cx.new(|cx| {
            gpui_component::input::InputState::new(window, cx).default_value(self.store.group.as_str().to_owned())
        });
        let form = cx.new(|cx| ConsumerDialogForm::new(kind, group, window, cx));
        let title = form.read(cx).title();
        self.mutation_dialog = Some(form.clone());
        let owner = cx.entity().downgrade();
        window.open_dialog(cx, move |dialog: Dialog, window, _| {
            let submit = owner.clone();
            let close = owner.clone();
            let cancel_form = form.clone();
            dialog
                .title(title)
                .margin_top(px(16.))
                .max_h(window.viewport_size().height - px(32.))
                .confirm()
                .child(form.clone())
                .button_props(
                    DialogButtonProps::default()
                        .ok_text("Apply reviewed command")
                        .cancel_text("Close"),
                )
                .footer(|ok, cancel, window, cx| {
                    vec![
                        gpui::div()
                            .debug_selector(|| "consumer-dialog-close".to_owned())
                            .child(cancel(window, cx)),
                        gpui::div()
                            .debug_selector(|| "consumer-dialog-confirm".to_owned())
                            .child(ok(window, cx)),
                    ]
                })
                .on_ok(move |_, _, cx| {
                    let _ = submit.update(cx, |detail, cx| detail.submit_consumer_dialog(cx));
                    false
                })
                .on_cancel(move |_, _, cx| cancel_form.read(cx).can_cancel())
                .on_close(move |_, _, cx| {
                    let _ = close.update(cx, |detail, cx| {
                        detail.mutation_dialog = None;
                        detail.edit_draft = None;
                        cx.notify();
                    });
                })
        });
        cx.notify();
    }

    fn submit_consumer_dialog(&mut self, cx: &mut Context<Self>) {
        if self.mutation_task.is_some() || self.mutation_replay_blocked {
            return;
        }
        let Some(form) = self.mutation_dialog.clone() else {
            return;
        };
        match form.update(cx, |form, cx| form.begin(cx)) {
            Ok(PreparedConsumerCommand::Edit(command)) => {
                self.edit_draft = Some(*command);
                self.submit_config_patch(cx);
            }
            Ok(PreparedConsumerCommand::Delete(command)) => self.submit_delete(command, cx),
            Ok(PreparedConsumerCommand::Create(_)) => {}
            Err(error) => form.update(cx, |form, cx| form.error(error, cx)),
        }
    }

    fn submit_delete(&mut self, command: ConsumerDeleteCommand, cx: &mut Context<Self>) {
        if self.targets.is_empty() || self.mutation_task.is_some() || self.mutation_replay_blocked {
            return;
        }
        let services = self.services.clone();
        let scope = self.next_consumer_scope();
        self.mutation_status = Some("Checking exact targets and live connections…".into());
        self.mutation_outcome = None;
        self.mutation_task = Some(cx.spawn(async move |this, cx| {
            let result = services.delete_consumer(scope, command).await;
            let _ = this.update(cx, |detail, cx| {
                detail.mutation_task = None;
                cx.notify();
                if scope.revision != detail.revision {
                    return;
                }
                match result {
                    Ok(ConsumerMutationResult::Rejected(outcome)) => {
                        detail.mutation_replay_blocked = true;
                        detail.mutation_outcome = Some(outcome.clone());
                        detail.mutation_status = Some(format!(
                            "{} ({}/{} Broker targets applied). Close this Sheet before a new command.",
                            if outcome.has_unconfirmed_effects() {"Delete result is unconfirmed; the command cannot be replayed"} else {"Delete rejected before execution"},
                            outcome.applied_count(),
                            outcome.target_count()
                        ));
                    }
                    Ok(ConsumerMutationResult::Applied {
                        outcome,
                        inventory,
                        invalidations,
                    }) => {
                        detail.mutation_outcome = Some(outcome.clone());
                        detail.targets=inventory.groups.iter().find(|item| item.identity==detail.store.group).map(|item| item.targets.clone()).unwrap_or_default();
                        detail.store.configuration.clear();
                        detail.apply_invalidations(&invalidations);
                        if outcome.is_complete_success() {
                            detail.mutation_status = Some(format!(
                                "Deleted on {} Broker target(s); authoritative inventory reloaded.",
                                outcome.applied_count()
                            ));
                            if inventory.groups.iter().any(|item| item.identity == detail.store.group) {
                                cx.emit(ConsumerDetailIntent::InventoryReloaded(inventory));
                            } else { cx.emit(ConsumerDetailIntent::Deleted(inventory)); }
                        } else {
                            detail.mutation_replay_blocked = true;
                            detail.mutation_status = Some(format!(
                                "Delete was only Partial ({}/{} Broker targets applied; cleanup step outcomes are retained below). Authoritative inventory reloaded; this Sheet remains open and the command cannot be replayed.",
                                outcome.applied_count(),
                                outcome.target_count()
                            ));
                            cx.emit(ConsumerDetailIntent::InventoryReloaded(inventory));
                        }
                    }
                    Ok(ConsumerMutationResult::AppliedReloadFailed {
                        outcome,
                        invalidations,
                        error,
                    }) => {
                        detail.mutation_replay_blocked = true;
                        detail.mutation_outcome = Some(outcome.clone());
                        detail.apply_invalidations(&invalidations);
                        detail.mutation_status = Some(format!(
                            "Delete applied on {} Broker target(s), but authoritative reload failed: {} Do not replay blindly; refresh first.",
                            outcome.applied_count(),
                            error.summary()
                        ));
                    }
                    Err(error) => {
                        detail.mutation_replay_blocked = true;
                        detail.mutation_status = Some(format!(
                            "{} Command state is unknown; close this Sheet before a new command.",
                            error.summary()
                        ));
                    }
                }
                if let Some(form)=&detail.mutation_dialog { let status=detail.mutation_status.clone().unwrap_or_default(); form.update(cx,|form,cx| form.finish(status,cx)); }
                cx.notify();
            });
        }));
    }

    pub(super) fn submit_config_patch(&mut self, cx: &mut Context<Self>) {
        if self.mutation_task.is_some() || self.mutation_replay_blocked {
            return;
        }
        let Some(command) = self.edit_draft.clone() else {
            return;
        };
        let services = self.services.clone();
        let scope = self.next_consumer_scope();
        self.mutation_status = Some("Applying exact-target version CAS…".into());
        self.mutation_task = Some(cx.spawn(async move |this, cx| {
            let result = services.patch_consumer_config(scope, command).await;
            let _ = this.update(cx, |detail, cx| {
                detail.mutation_task = None;
                cx.notify();
                if scope.revision != detail.revision {
                    return;
                }
                match result {
                    Ok(ConsumerConfigMutationResult::PersistenceIncomplete {applied,previous_generation,generation,persistence,invalidations}) => {
                        detail.mutation_replay_blocked=true;detail.apply_invalidations(&invalidations);detail.store.configuration.clear();
                        detail.mutation_status=Some(if applied {
                            format!("Broker applied configuration in memory ({previous_generation} → {generation}), but persistence is {persistence:?}. This is not a durable success; this command cannot be replayed. Close and refresh before a new reviewed command.")
                        } else {format!("Broker did not apply this command: persistence is {persistence:?} at version {generation}. Resolve Broker persistence before a new reviewed command.")});
                    },
                    Ok(ConsumerConfigMutationResult::Applied {
                        previous_generation,
                        generation,
                        configuration,
                        inventory,
                        invalidations,
                    }) => {
                        detail.apply_invalidations(&invalidations);
                        detail.store.configuration.replace(configuration);
                        detail.edit_draft = None;
                        detail.mutation_status = Some(format!(
                            "Configuration CAS applied ({previous_generation} → {generation}) and authoritatively reloaded."
                        ));
                        cx.emit(ConsumerDetailIntent::InventoryReloaded(inventory));
                    }
                    Ok(ConsumerConfigMutationResult::GenerationConflict {
                        expected_generation,
                        actual_generation,
                    }) => {
                        detail.mutation_replay_blocked = true;
                        detail.mutation_status = Some(format!(
                            "Generation conflict: expected {expected_generation}, actual {actual_generation}. Draft retained; reload before retry."
                        ));
                    }
                    Ok(ConsumerConfigMutationResult::AppliedReloadFailed {
                        previous_generation,
                        generation,
                        invalidations,
                        error,
                    }) => {
                        detail.mutation_replay_blocked = true;
                        detail.apply_invalidations(&invalidations);
                        detail.mutation_status = Some(format!(
                            "Configuration CAS applied ({previous_generation} → {generation}), but reload failed: {} Draft retained for inspection; this command cannot be resubmitted.",
                            error.summary()
                        ));
                    }
                    Err(error) => {
                        detail.mutation_replay_blocked = true;
                        detail.mutation_status = Some(format!(
                            "{} Draft retained; this command cannot be resubmitted until a new draft is created.",
                            error.summary()
                        ));
                    }
                }
                if let Some(form)=&detail.mutation_dialog { let status=detail.mutation_status.clone().unwrap_or_default(); form.update(cx,|form,cx| form.finish(status,cx)); }
                cx.notify();
            });
        }));
    }

    pub(super) fn request_offset_action(
        &mut self,
        topic: String,
        cluster_name: String,
        skip: bool,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.mutation_task.is_some() || self.offset_dialog.is_some() || self.mutation_replay_blocked {
            return;
        }
        let Ok(topic) = TopicIdentity::parse(topic) else {
            self.mutation_status = Some("The exact Topic identity is invalid.".into());
            cx.notify();
            return;
        };
        let kind = if skip {
            TopicDialogKind::SkipAccumulated {
                topic,
                consumer_group: self.store.group.as_str().to_owned(),
                clusters: vec![cluster_name],
                force: false,
            }
        } else {
            TopicDialogKind::ResetOffset {
                topic,
                consumer_group: self.store.group.as_str().to_owned(),
                clusters: vec![cluster_name],
                timestamp: current_epoch_millis(),
                force: false,
            }
        };
        let form = cx.new(|cx| TopicDialogForm::new(kind, window, cx));
        let title = form.read(cx).title();
        let confirm_label = form.read(cx).confirm_label();
        self.offset_dialog = Some(form.clone());
        let owner = cx.entity().downgrade();
        let on_ok = owner.clone();
        let on_close = owner;
        window.open_dialog(cx, move |dialog: Dialog, _, _| {
            let on_ok = on_ok.clone();
            let on_close = on_close.clone();
            let cancel_form = form.clone();
            let close_form = form.clone();
            dialog
                .title(title)
                .confirm()
                .button_props(DialogButtonProps::default().ok_text(confirm_label).cancel_text("Close"))
                .child(form.clone())
                .footer(|ok, cancel, window, cx| {
                    vec![
                        gpui::div()
                            .debug_selector(|| "consumer-offset-dialog-close".to_owned())
                            .child(cancel(window, cx)),
                        gpui::div()
                            .debug_selector(|| "consumer-offset-dialog-confirm".to_owned())
                            .child(ok(window, cx)),
                    ]
                })
                .on_ok(move |_, window, cx| {
                    let _ = on_ok.update(cx, |detail, cx| detail.submit_offset_dialog(window, cx));
                    false
                })
                .on_cancel(move |_, _, cx| cancel_form.read(cx).can_cancel())
                .on_close(move |_, window, cx| {
                    close_form.update(cx, |form, cx| form.clear_sensitive(window, cx));
                    let _ = on_close.update(cx, |detail, cx| {
                        detail.offset_dialog = None;
                        detail.mutation_replay_blocked = false;
                        cx.notify();
                    });
                })
        });
        cx.notify();
    }

    fn submit_offset_dialog(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.mutation_task.is_some() || self.mutation_replay_blocked {
            return;
        }
        let Some(form) = self.offset_dialog.clone() else {
            return;
        };
        let command = match form.read(cx).prepare(cx) {
            Ok(PreparedTopicCommand::Reset(command)) => (command, false),
            Ok(PreparedTopicCommand::Skip(command)) => (command, true),
            Ok(_) => {
                form.update(cx, |form, cx| form.set_status("Unexpected offset command type.", cx));
                return;
            }
            Err(error) => {
                form.update(cx, |form, cx| form.set_status(error.summary().to_owned(), cx));
                return;
            }
        };
        let Some(token) = form.update(cx, |form, _| form.state.begin_submit(self.revision)) else {
            return;
        };
        self.mutation_epoch = self.mutation_epoch.saturating_add(1);
        let topic_scope = TopicRequestScope {
            revision: self.revision,
            epoch: self.mutation_epoch,
        };
        let consumer_scope = ConsumerRequestScope {
            revision: self.revision,
            epoch: self.mutation_epoch,
            query_scope: Default::default(),
        };
        let group = self.store.group.clone();
        let (command, skip) = command;
        let services = self.services.clone();
        self.mutation_status = Some(if skip {
            "Skipping accumulated messages for one exact Topic/group/cluster…".into()
        } else {
            "Resetting offset for one exact Topic/group/cluster…".into()
        });
        self.mutation_task = Some(cx.spawn_in(window, async move |this, cx| {
            let result = if skip {
                services.skip_topic_accumulated(topic_scope, command).await
            } else {
                services.reset_topic_offset(topic_scope, command).await
            };
            let reload = if matches!(
                result,
                Ok(TopicMutationResult::Applied { .. } | TopicMutationResult::AppliedReloadFailed { .. })
            ) {
                Some(services.consumer_progress(consumer_scope, group).await)
            } else {
                None
            };
            let _ = this.update_in(cx, |detail, _, cx| {
                detail.mutation_task = None;
                cx.notify();
                if consumer_scope.revision != detail.revision {
                    return;
                }
                detail.finish_offset_action(&result, reload);
                let blocked = !matches!(
                    &result,
                    Ok(TopicMutationResult::Applied { outcome, .. }) if outcome.is_complete_success()
                );
                detail.mutation_replay_blocked = blocked;
                let partial_applied = matches!(
                    &result,
                    Ok(TopicMutationResult::Applied { outcome, .. }) if !outcome.is_complete_success()
                );
                form.update(cx, |form, cx| {
                    if !form.state.accepts(token, detail.revision) {
                        return;
                    }
                    form.state.submission = match result {
                        Ok(result @ TopicMutationResult::Rejected(_)) => TopicSubmissionState::Rejected(result),
                        Ok(result @ TopicMutationResult::Applied { .. }) if partial_applied => {
                            TopicSubmissionState::PartiallySucceeded(result)
                        }
                        Ok(TopicMutationResult::Applied { .. }) => TopicSubmissionState::Succeeded,
                        Ok(result @ TopicMutationResult::AppliedReloadFailed { .. }) => {
                            TopicSubmissionState::AppliedReloadFailed(result)
                        }
                        Err(error) => TopicSubmissionState::Failed(error),
                    };
                    cx.notify();
                });
                cx.notify();
            });
        }));
    }

    fn finish_offset_action(
        &mut self,
        result: &Result<TopicMutationResult, UiError>,
        reload: Option<Result<ConsumerObservation<rocketmq_dashboard_common::ConsumerProgress>, UiError>>,
    ) {
        self.mutation_status = Some(match result {
            Ok(TopicMutationResult::Rejected(outcome)) => format!(
                "Offset action rejected ({} target failures); it was not replayed.",
                outcome.failed_count()
            ),
            Ok(TopicMutationResult::Applied {
                outcome,
                inventory,
                consumers,
                invalidations,
            }) => {
                let _reload_metadata = (inventory.is_some(), consumers.is_some(), invalidations.len());
                format!("Offset action applied on {} queue target(s).", outcome.applied_count())
            }
            Ok(TopicMutationResult::AppliedReloadFailed {
                outcome,
                invalidations,
                error,
            }) => {
                let _invalidation_count = invalidations.len();
                format!(
                    "Offset action applied on {} queue target(s), but Topic reload failed: {} The mutation was not replayed.",
                    outcome.applied_count(),
                    error.summary()
                )
            }
            Err(error) => error.summary().to_owned(),
        });
        if let Some(reload) = reload {
            match reload {
                Ok(progress) if matches!(progress, ConsumerObservation::Complete(_)) => {
                    self.store.progress.replace(progress.clone());
                    self.store.offset_actions.replace(progress);
                }
                Ok(_) => {
                    self.mutation_replay_blocked = true;
                    self.mutation_status = Some(
                        "Offset action applied, but Consumer progress reload was Partial or Unknown. The previous authoritative progress remains visible; close this dialog before any new command."
                            .into(),
                    );
                }
                Err(error) => {
                    self.store.progress.clear_with_error(error.clone());
                    self.store.offset_actions.clear_with_error(error);
                    self.mutation_status = Some(
                        "Offset action may have applied, but Consumer progress reload failed. Refresh before any retry; do not replay blindly."
                            .into(),
                    );
                }
            }
        }
    }

    fn apply_invalidations(&mut self, invalidations: &[ConsumerCacheInvalidation]) {
        let detail_resources_changed = invalidations.iter().any(|invalidation| {
            matches!(
                invalidation,
                ConsumerCacheInvalidation::Overview(group) | ConsumerCacheInvalidation::Progress(group)
                    if group == &self.store.group
            )
        });
        if detail_resources_changed {
            self.store.invalidate_overview_and_progress();
        }
    }
}

fn current_epoch_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| u64::try_from(duration.as_millis()).ok())
        .unwrap_or(946_684_800_000)
        .clamp(946_684_800_000, 4_102_444_800_000)
}
