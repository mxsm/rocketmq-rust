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

//! Reviewed Consumer creation with selected Broker targets and app-owned mutation results.
use super::ConsumersView;
use crate::{
    features::consumer_dialogs::{ConsumerDialogForm, ConsumerDialogKind, PreparedConsumerCommand},
    services::consumers::{ConsumerMutationResult, ConsumerRequestScope},
};
use gpui::{AppContext as _, Context, InteractiveElement as _, ParentElement as _, Styled as _, Window, div, px};
use gpui_component::{
    WindowExt as _,
    dialog::{Dialog, DialogButtonProps},
};
use rocketmq_dashboard_common::{CapabilityAvailability, ConsumerObservationState};
impl ConsumersView {
    pub(super) fn open_create_dialog(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if !self.create_ready() || self.create_dialog.is_some() {
            return;
        }
        let Some(inventory) = self.store.inventory.state.value() else {
            return;
        };
        let kind = ConsumerDialogKind::Create {
            targets: inventory.targets.clone(),
        };
        let form = cx.new(|cx| ConsumerDialogForm::new(kind, self.create_group.clone(), window, cx));
        self.create_dialog = Some(form.clone());
        let owner = cx.entity().downgrade();
        window.open_dialog(cx, move |dialog: Dialog, window, _| {
            let submit_owner = owner.clone();
            let close_owner = owner.clone();
            let cancel_form = form.clone();
            dialog
                .title("Create Consumer group")
                .margin_top(px(16.))
                .max_h(window.viewport_size().height - px(32.))
                .confirm()
                .child(form.clone())
                .button_props(
                    DialogButtonProps::default()
                        .ok_text("Create selected targets")
                        .cancel_text("Close"),
                )
                .footer(|ok, cancel, window, cx| {
                    vec![
                        div()
                            .debug_selector(|| "consumer-dialog-close".to_owned())
                            .child(cancel(window, cx)),
                        div()
                            .debug_selector(|| "consumer-dialog-confirm".to_owned())
                            .child(ok(window, cx)),
                    ]
                })
                .on_ok(move |_, window, cx| {
                    let _ = submit_owner.update(cx, |view, cx| view.submit_create(window, cx));
                    false
                })
                .on_cancel(move |_, _, cx| cancel_form.read(cx).can_cancel())
                .on_close(move |_, _, cx| {
                    let _ = close_owner.update(cx, |view, cx| {
                        view.create_dialog = None;
                        view.mutation_replay_blocked = false;
                        cx.notify();
                    });
                })
        });
        cx.notify();
    }
    pub(super) fn create_ready(&self) -> bool {
        self.store.inventory.state.value().is_some_and(|inventory| {
            inventory.capabilities.create == CapabilityAvailability::Available
                && inventory.target_observation == ConsumerObservationState::Complete
                && !inventory.targets.is_empty()
        }) && self.mutation_task.is_none()
            && !self.mutation_replay_blocked
    }

    fn submit_create(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if !self.create_ready() {
            self.mutation_status =
                Some("Create requires a complete authoritative target inventory and Direct capability.".into());
            cx.notify();
            return;
        }
        let Some(form) = self.create_dialog.clone() else {
            return;
        };
        let command = match form.update(cx, |form, cx| form.begin(cx)) {
            Ok(PreparedConsumerCommand::Create(command)) => command,
            Ok(_) => return,
            Err(error) => {
                form.update(cx, |form, cx| form.error(error, cx));
                return;
            }
        };
        self.mutation_epoch = self.mutation_epoch.saturating_add(1);
        let scope = ConsumerRequestScope {
            revision: self.revision,
            epoch: self.mutation_epoch,
            query_scope: Default::default(),
        };
        let services = self.services.clone();
        self.mutation_status = Some("Verifying all-target absence before any write…".into());
        self.mutation_outcome = None;
        self.mutation_task = Some(cx.spawn_in(window, async move |this, cx| {
            let result = services.create_consumer(scope, command).await;
            let _ = this.update_in(cx, |view, _, cx| {
                view.mutation_task = None;
                cx.notify();
                if scope.revision != view.revision {
                    return;
                }
                match result {
                    Ok(ConsumerMutationResult::Rejected(outcome)) => {
                        view.mutation_replay_blocked = true;
                        view.mutation_outcome = Some(outcome.clone());
                        view.mutation_status = Some(format!(
                            "{} ({}/{} Broker targets applied). Close this form before a new command.",
                            if outcome.has_unconfirmed_effects() {"Create result is unconfirmed; the command cannot be replayed"} else {"Create rejected before execution"},
                            outcome.applied_count(),
                            outcome.target_count()
                        ));
                    }
                    Ok(ConsumerMutationResult::Applied {
                        outcome,
                        inventory,
                        invalidations,
                    }) => {
                        view.mutation_replay_blocked = !outcome.is_complete_success();
                        view.mutation_outcome = Some(outcome.clone());
                        view.observe_invalidations(&invalidations);
                        view.store.inventory.replace(inventory);
                        view.mutation_status = Some(format!(
                            "Create applied on {}/{} Broker target(s); authoritative inventory reloaded. Close this form before any new command.",
                            outcome.applied_count(), outcome.target_count()
                        ));
                        view.sync_table(cx);
                    }
                    Ok(ConsumerMutationResult::AppliedReloadFailed {
                        outcome,
                        invalidations,
                        error,
                    }) => {
                        view.mutation_replay_blocked = true;
                        view.mutation_outcome = Some(outcome.clone());
                        view.observe_invalidations(&invalidations);
                        view.mutation_status = Some(format!(
                            "Create applied on {} Broker target(s), but authoritative reload failed: {} Draft retained for inspection; refresh and do not replay blindly.",
                            outcome.applied_count(),
                            error.summary()
                        ));
                    }
                    Err(error) => {
                        view.mutation_replay_blocked = true;
                        view.mutation_status = Some(format!(
                            "{} Command state is unknown; change the draft before a new command.",
                            error.summary()
                        ));
                    }
                }
                if let Some(form) = &view.create_dialog { let status = view.mutation_status.clone().unwrap_or_default(); form.update(cx,|form,cx| form.finish(status,cx)); }
                cx.notify();
            });
        }));
    }
}
