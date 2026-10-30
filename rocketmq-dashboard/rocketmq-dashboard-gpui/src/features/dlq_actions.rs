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

use super::*;

impl DlqView {
    pub(super) fn review_recovery(&mut self, retry_failed: bool, window: &mut Window, cx: &mut Context<Self>) {
        if self.batch_task.is_some()
            || self.confirmation.is_some()
            || matches!(self.store.rows, Loadable::InitialLoading | Loadable::Refreshing(_))
        {
            return;
        }
        let targets = if retry_failed {
            self.store
                .results
                .as_ref()
                .map_or_else(Vec::new, |result| result.retryable_targets())
        } else {
            self.store.targets()
        };
        if targets.is_empty() {
            return;
        }
        let state = self.services.connection_state();
        let environment = state
            .environments
            .as_ref()
            .and_then(|catalog| catalog.profiles.iter().find(|profile| profile.id == catalog.active_id))
            .map_or("Local desktop", |profile| profile.name.as_str());
        let connection = format!(
            "Environment: {environment} · NameServer: {} · revision {}",
            state.config.current_nameserver.as_deref().unwrap_or("not configured"),
            self.revision
        );
        let confirmation = cx.new(|_| DlqConfirmation {
            targets,
            submitted: false,
            connection,
        });
        self.confirmation = Some(confirmation.clone());
        let owner = cx.entity().downgrade();
        window.open_dialog(cx, move |dialog: Dialog, _, _| {
            let submit = owner.clone();
            let close = owner.clone();
            let footer = confirmation.clone();
            dialog
                .title(if retry_failed {
                    "Review failed DLQ entries"
                } else {
                    "Confirm DLQ recovery"
                })
                .confirm()
                .button_props(
                    DialogButtonProps::default()
                        .ok_text("Recover once")
                        .cancel_text("Close"),
                )
                .child(confirmation.clone())
                .footer(move |ok, cancel, window, cx| {
                    vec![
                        div()
                            .debug_selector(|| "dlq-confirm-cancel".into())
                            .child(cancel(window, cx)),
                        div()
                            .debug_selector(|| "dlq-confirm-submit".into())
                            .child(if footer.read(cx).submitted {
                                Button::new("dlq-submitted")
                                    .label("Submitted")
                                    .disabled(true)
                                    .into_any_element()
                            } else {
                                ok(window, cx)
                            }),
                    ]
                })
                .on_ok(move |_, window, cx| {
                    let _ = submit.update(cx, |view, cx| view.submit_recovery(window, cx));
                    false
                })
                .on_close(move |_, _, cx| {
                    let _ = close.update(cx, |view, cx| {
                        view.confirmation = None;
                        cx.notify();
                    });
                })
        });
        cx.notify();
    }
    fn submit_recovery(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.batch_task.is_some() {
            return;
        }
        let Some(confirmation) = self.confirmation.clone() else {
            return;
        };
        if confirmation.read(cx).submitted {
            return;
        }
        let targets = confirmation.read(cx).targets.clone();
        confirmation.update(cx, |form, cx| {
            form.submitted = true;
            cx.notify();
        });
        let scope = MessageRequestScope {
            revision: self.revision,
            epoch: self.store.epoch(),
        };
        let services = self.services.clone();
        self.store.selected.clear();
        self.status = Some("DLQ batch is executing sequentially…".into());
        self.batch_task = Some(cx.spawn_in(window, async move |this, cx| {
            let result = services.recover_dlq(scope, targets).await;
            let _ = this.update_in(cx, |view, _, cx| {
                view.batch_task = None;
                view.sync_table(cx);
                cx.notify();
                if view.revision != scope.revision || view.store.epoch() != scope.epoch {
                    return;
                }
                match result {
                    Ok(result) => {
                        let count = result.entries.len();
                        view.store.merge_results(result);
                        view.status = Some(format!("Completed {count} batch entries. Review each outcome below."));
                    }
                    Err(error) => view.status = Some(error.summary().into()),
                }
                view.sync_table(cx);
                cx.notify();
            });
        }));
        self.sync_table(cx);
        cx.notify();
    }
    pub(super) fn export_page(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.export_task.is_some() || !matches!(self.store.rows, Loadable::Ready(_)) {
            return;
        }
        let Some(query) = self.store.applied.clone() else {
            return;
        };
        let expected = self.store.rows.value().map_or_else(Vec::new, |page| {
            page.rows
                .iter()
                .map(|row| row.metadata.identity.clone())
                .collect::<Vec<_>>()
        });
        let directory = dirs::download_dir().or_else(dirs::home_dir).unwrap_or_default();
        let chooser = cx.prompt_for_new_path(&directory, Some(&format!("dlq-page-{}.csv", query.page)));
        self.export_epoch = self.export_epoch.saturating_add(1);
        let epoch = self.export_epoch;
        let scope = MessageRequestScope {
            revision: self.revision,
            epoch: self.store.epoch(),
        };
        let services = self.services.clone();
        self.status = Some("Choose a destination for the applied DLQ page. Cancel writes nothing.".into());
        self.export_task = Some(cx.spawn_in(window, async move |this, cx| {
            let destination = chooser.await;
            let current = this
                .update_in(cx, |view, _, _| {
                    view.revision == scope.revision
                        && view.store.epoch() == scope.epoch
                        && view.export_epoch == epoch
                        && view.store.applied.as_ref() == Some(&query)
                })
                .unwrap_or(false);
            if !current {
                return;
            }
            let result = match destination {
                Ok(Ok(Some(path))) => services.export_dlq_page(scope, query, expected, path).await.map(Some),
                Ok(Ok(None)) => Ok(None),
                _ => Err(UiError::new(
                    "Unable to open the native Save File chooser.",
                    UiErrorCode::Configuration,
                    true,
                )),
            };
            let _ = this.update_in(cx, |view, _, cx| {
                if view.revision != scope.revision || view.store.epoch() != scope.epoch || view.export_epoch != epoch {
                    return;
                }
                view.export_task = None;
                view.status = Some(match result {
                    Ok(Some(bytes)) => format!("Saved {bytes} full CSV bytes for the applied page."),
                    Ok(None) => "Export cancelled; no file was written.".into(),
                    Err(error) => error.summary().into(),
                });
                cx.notify();
            });
        }));
        cx.notify();
    }
}
