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

//! Bounded Topic mutation coordinator and authoritative store reconciliation.

use gpui::{AppContext as _, Context, Entity, ParentElement as _, Window};
use gpui_component::{
    WindowExt as _,
    dialog::{Dialog, DialogButtonProps},
};
use rocketmq_dashboard_common::{TopicCompleteness, TopicMutationKind};

use crate::{
    features::{
        topic_dialogs::{
            PreparedTopicCommand, TopicCreateDraft, TopicDialogForm, TopicDialogKind, TopicSubmissionState,
            TopicSubmitToken,
        },
        topics::TopicsView,
    },
    services::topics::{TopicCacheInvalidation, TopicFieldsPatchResult, TopicMutationResult, TopicRequestScope},
    state::UiError,
};

enum TopicCommandCompletion {
    Mutation(Result<TopicMutationResult, UiError>),
    Patch {
        result: Result<TopicFieldsPatchResult, UiError>,
        submitted_patch: rocketmq_dashboard_common::TopicConfigFieldsPatch,
    },
    Send(Result<(), UiError>),
}

impl TopicsView {
    pub(super) fn open_create_dialog(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(inventory) = self.store.inventory.state.value() else {
            return;
        };
        if !matches!(inventory.completeness, TopicCompleteness::Complete) || inventory.targets.is_empty() {
            return;
        }
        self.open_topic_dialog(
            TopicDialogKind::Create(TopicCreateDraft {
                topic_name: String::new(),
                targets: inventory.targets.clone(),
                read_queue_count: 8,
                write_queue_count: 8,
                permission: None,
                ordered: None,
                message_type: None,
                catalog_epoch: self.store.inventory_generation,
            }),
            window,
            cx,
        );
    }

    pub(super) fn open_topic_dialog(&mut self, kind: TopicDialogKind, window: &mut Window, cx: &mut Context<Self>) {
        let inventory_verified = self
            .store
            .inventory
            .state
            .value()
            .is_some_and(|inventory| inventory.completeness.is_complete() && inventory.failures.is_empty());
        let selection_verified = self
            .store
            .detail
            .as_ref()
            .is_none_or(|detail| detail.selected.inventory_verified());
        if self.dialog_form.is_some() || !inventory_verified || !selection_verified {
            return;
        }
        let form = cx.new(|cx| TopicDialogForm::new(kind, window, cx));
        let title = form.read(cx).title();
        let confirm_label = form.read(cx).confirm_label();
        self.dialog_form = Some(form.clone());
        let owner = cx.entity().downgrade();
        let on_ok = owner.clone();
        let on_close = owner.clone();
        window.open_dialog(cx, move |dialog: Dialog, _, _| {
            let on_ok = on_ok.clone();
            let on_close = on_close.clone();
            let form_for_cancel = form.clone();
            let form_for_close = form.clone();
            dialog
                .title(title)
                .confirm()
                .button_props(DialogButtonProps::default().ok_text(confirm_label).cancel_text("Close"))
                .child(form.clone())
                .on_ok(move |_, window, cx| {
                    let _ = on_ok.update(cx, |view, cx| view.submit_topic_dialog(window, cx));
                    false
                })
                .on_cancel(move |_, _, cx| form_for_cancel.read(cx).can_cancel())
                .on_close(move |_, window, cx| {
                    form_for_close.update(cx, |form, cx| form.clear_sensitive(window, cx));
                    let _ = on_close.update(cx, |view, cx| {
                        view.dialog_form = None;
                        cx.notify();
                    });
                })
        });
        cx.notify();
    }

    fn submit_topic_dialog(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.mutation_task.is_some() {
            return;
        }
        let Some(form) = self.dialog_form.clone() else {
            return;
        };
        let command = match form.read(cx).prepare(cx) {
            Ok(command) => command,
            Err(error) => {
                form.update(cx, |form, cx| form.set_status(error.summary().to_owned(), cx));
                return;
            }
        };
        let Some(token) = form.update(cx, |form, _| form.state.begin_submit(self.revision)) else {
            return;
        };
        let services = self.services.clone();
        let scope = TopicRequestScope {
            revision: self.revision,
            epoch: self.store.inventory_generation,
        };
        self.mutation_task = Some(cx.spawn_in(window, async move |this, cx| {
            let completion = match command {
                PreparedTopicCommand::Create(command) => {
                    TopicCommandCompletion::Mutation(services.create_topic(scope, command).await)
                }
                PreparedTopicCommand::Edit(command) => {
                    let submitted_patch = command.patch;
                    TopicCommandCompletion::Patch {
                        result: services.patch_topic_fields(scope, command).await,
                        submitted_patch,
                    }
                }
                PreparedTopicCommand::DeleteTopic(command) => {
                    TopicCommandCompletion::Mutation(services.delete_topic(scope, command).await)
                }
                PreparedTopicCommand::DeleteBroker(command) => {
                    TopicCommandCompletion::Mutation(services.delete_topic_from_broker(scope, command).await)
                }
                PreparedTopicCommand::Send(command) => {
                    TopicCommandCompletion::Send(services.send_topic_message(scope, command).await)
                }
                PreparedTopicCommand::Reset(command) => {
                    TopicCommandCompletion::Mutation(services.reset_topic_offset(scope, command).await)
                }
                PreparedTopicCommand::Skip(command) => {
                    TopicCommandCompletion::Mutation(services.skip_topic_accumulated(scope, command).await)
                }
            };
            let _ = this.update_in(cx, |view, window, cx| {
                view.mutation_task = None;
                view.finish_topic_command(&form, token, completion, window, cx);
            });
        }));
    }

    fn finish_topic_command(
        &mut self,
        form: &Entity<TopicDialogForm>,
        token: TopicSubmitToken,
        completion: TopicCommandCompletion,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if !form.read(cx).state.accepts(token, self.revision)
            || self
                .dialog_form
                .as_ref()
                .is_none_or(|active| active.entity_id() != form.entity_id())
        {
            return;
        }
        match completion {
            TopicCommandCompletion::Mutation(Err(error)) | TopicCommandCompletion::Send(Err(error)) => {
                let summary = error.summary().to_owned();
                form.update(cx, |form, cx| {
                    let _ = form.state.fail(token, self.revision, error);
                    form.set_status(summary, cx);
                });
            }
            TopicCommandCompletion::Send(Ok(())) => {
                form.update(cx, |form, cx| {
                    form.state.submission = TopicSubmissionState::Succeeded;
                    form.clear_sensitive(window, cx);
                    form.set_status("Message acknowledged. The body was cleared; close this dialog.", cx);
                });
            }
            TopicCommandCompletion::Mutation(Ok(result)) => self.finish_mutation_result(form, result, window, cx),
            TopicCommandCompletion::Patch { result: Err(error), .. } => {
                let summary = error.summary().to_owned();
                form.update(cx, |form, cx| {
                    let _ = form.state.fail(token, self.revision, error);
                    form.set_status(summary, cx);
                });
            }
            TopicCommandCompletion::Patch {
                result: Ok(mut result),
                submitted_patch,
            } => {
                if let Some(inventory) = result.inventory.take() {
                    self.store.replace_inventory(inventory);
                    self.sync_table(cx);
                    self.sync_detail_stale(cx);
                }
                if let Some(detail) = &self.detail {
                    detail.update(cx, |detail, cx| {
                        detail.apply_mutation_reload(&result.invalidations, result.configuration.take(), None, None, cx)
                    });
                }
                if let Some(error) = &result.reload_error {
                    self.apply_failed_invalidations(&result.invalidations, error, cx);
                }
                let summary = format!(
                    "{} Broker/stage(s) applied, {} failed. Conflicts preserve all changed draft fields. Applied or unknown targets cannot be automatically retried.",
                    result.edit.outcome.applied_count(),
                    result.edit.outcome.failed_count()
                );
                form.update(cx, |form, cx| {
                    form.state.submission = TopicSubmissionState::FieldsCompleted {
                        result,
                        submitted_patch,
                    };
                    form.set_status(summary, cx);
                });
            }
        }
        cx.notify();
    }

    fn finish_mutation_result(
        &mut self,
        form: &Entity<TopicDialogForm>,
        result: TopicMutationResult,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        match result {
            TopicMutationResult::Rejected(result) => {
                let applied = result.applied_count();
                let failed = result.failed_count();
                let wrapped = TopicMutationResult::Rejected(result);
                form.update(cx, |form, cx| {
                    form.state.submission = TopicSubmissionState::Rejected(wrapped);
                    form.set_status(
                        format!("No target applied (applied {applied}, failed {failed}). Review outcomes; this command cannot be blindly retried."),
                        cx,
                    );
                });
            }
            TopicMutationResult::Applied {
                outcome,
                inventory,
                consumers,
                invalidations,
            } => {
                let applied = outcome.applied_count();
                let failed = outcome.failed_count();
                let delete_topic = outcome.kind == TopicMutationKind::DeleteTopic;
                if let Some(inventory) = inventory.as_ref() {
                    self.store.replace_inventory(inventory.clone());
                    self.sync_table(cx);
                    self.sync_detail_stale(cx);
                }
                if let Some(detail) = &self.detail {
                    detail.update(cx, |detail, cx| {
                        detail.apply_mutation_reload(&invalidations, None, consumers.clone(), None, cx)
                    });
                }
                if delete_topic {
                    self.close_detail(window, cx);
                }
                let complete = outcome.is_complete_success();
                let wrapped = TopicMutationResult::Applied {
                    outcome,
                    inventory,
                    consumers,
                    invalidations,
                };
                form.update(cx, |form, cx| {
                    form.state.submission = if complete {
                        TopicSubmissionState::Succeeded
                    } else {
                        TopicSubmissionState::PartiallySucceeded(wrapped)
                    };
                    form.set_status(
                        if complete {
                            format!("Applied to {applied} target(s); authoritative affected resource loaded.")
                        } else {
                            format!("Partial result: applied {applied}, failed {failed}; authoritative affected resource loaded. No automatic retry.")
                        },
                        cx,
                    );
                });
            }
            TopicMutationResult::AppliedReloadFailed {
                outcome,
                invalidations,
                error,
            } => {
                let applied = outcome.applied_count();
                let failed = outcome.failed_count();
                let delete_topic = outcome.kind == TopicMutationKind::DeleteTopic;
                let summary = error.summary().to_owned();
                self.apply_failed_invalidations(&invalidations, &error, cx);
                if delete_topic {
                    self.close_detail(window, cx);
                }
                let wrapped = TopicMutationResult::AppliedReloadFailed {
                    outcome,
                    invalidations,
                    error,
                };
                form.update(cx, |form, cx| {
                    form.state.submission = TopicSubmissionState::AppliedReloadFailed(wrapped);
                    form.set_status(
                        format!("Applied {applied}, failed {failed}, but authoritative reload failed: {summary}. This command cannot be resubmitted."),
                        cx,
                    );
                });
            }
        }
    }

    fn apply_failed_invalidations(
        &mut self,
        invalidations: &[TopicCacheInvalidation],
        error: &UiError,
        cx: &mut Context<Self>,
    ) {
        if invalidations
            .iter()
            .any(|invalidation| matches!(invalidation, TopicCacheInvalidation::Inventory))
        {
            self.store.inventory.clear_with_error(error.clone());
            self.sync_table(cx);
        }
        if let Some(detail) = &self.detail {
            detail.update(cx, |detail, cx| {
                detail.apply_mutation_reload(invalidations, None, None, Some(error), cx)
            });
        }
    }
}
