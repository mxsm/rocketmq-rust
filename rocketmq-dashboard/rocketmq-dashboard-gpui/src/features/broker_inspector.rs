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

//! Broker Sheet inspector with independently loaded Overview, Runtime, and Config tabs.

use std::collections::BTreeMap;

use gpui::{
    AppContext as _, ClipboardItem, Context, Entity, EventEmitter, InteractiveElement as _, IntoElement,
    ParentElement as _, Render, Styled as _, Subscription, Task, Window, div, prelude::FluentBuilder as _, px,
};
use gpui_component::{
    ActiveTheme as _, Disableable as _, StyledExt as _, WindowExt as _,
    button::{Button, ButtonVariants as _},
    dialog::{Dialog, DialogButtonProps},
    input::{Input, InputEvent, InputState},
    scroll::ScrollableElement as _,
    tab::TabBar,
};
use rocketmq_dashboard_common::{BrokerIdentity, RuntimeEntry, filter_runtime_entries, is_sensitive_key};

use crate::{
    components::{dialog, key_value, toast},
    features::inspector_store::{ConfigSubmissionState, InspectorStore},
    route::{AppRoute, BrokerTab, RouteKey},
    services::{AppServices, brokers::BrokerCacheInvalidation},
    state::Loadable,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BrokerInspectorIntent {
    ConfigApplied(Vec<BrokerCacheInvalidation>),
    NavigateTab(crate::route::AppRoute),
}

struct ConfigEditor {
    inputs: BTreeMap<String, Entity<InputState>>,
    pending_draft: Option<BTreeMap<String, String>>,
}

impl ConfigEditor {
    fn new(draft: BTreeMap<String, String>) -> Self {
        Self {
            inputs: BTreeMap::new(),
            pending_draft: Some(draft),
        }
    }

    fn reconcile(&mut self, draft: BTreeMap<String, String>, cx: &mut Context<Self>) {
        self.inputs.retain(|key, _| draft.contains_key(key));
        self.pending_draft = Some(draft);
        cx.notify();
    }

    fn values(&self, cx: &gpui::App) -> BTreeMap<String, String> {
        let mut values = self.pending_draft.clone().unwrap_or_default();
        values.extend(
            self.inputs
                .iter()
                .map(|(key, input)| (key.clone(), input.read(cx).value().to_string())),
        );
        values
    }

    fn apply_pending(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(draft) = self.pending_draft.take() else {
            return;
        };
        self.inputs.retain(|key, _| draft.contains_key(key));
        for (key, value) in draft {
            if let Some(input) = self.inputs.get(&key) {
                input.update(cx, |input, cx| input.set_value(value.clone(), window, cx));
            } else {
                self.inputs.insert(
                    key,
                    cx.new(|cx| InputState::new(window, cx).multi_line(true).default_value(value)),
                );
            }
        }
    }
}

impl Render for ConfigEditor {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        self.apply_pending(window, cx);
        div()
            .id("broker-config-editor")
            .mt_3()
            .max_h(px(480.))
            .overflow_y_scrollbar()
            .flex()
            .flex_col()
            .gap_3()
            .children(self.inputs.iter().map(|(key, input)| {
                div()
                    .flex()
                    .flex_col()
                    .gap_2()
                    .child(div().text_sm().font_medium().child(key.clone()))
                    .child(Input::new(input).h(px(76.)))
            }))
    }
}

pub struct BrokerInspector {
    services: AppServices,
    revision: u64,
    active_tab: BrokerTab,
    store: InspectorStore,
    runtime_filter: Entity<InputState>,
    config_editor: Option<Entity<ConfigEditor>>,
    _runtime_subscription: Subscription,
    config_dialog_open: bool,
    overview_task: Option<Task<()>>,
    runtime_task: Option<Task<()>>,
    config_task: Option<Task<()>>,
    mutation_task: Option<Task<()>>,
}

impl EventEmitter<BrokerInspectorIntent> for BrokerInspector {}

impl BrokerInspector {
    pub fn new(
        window: &mut Window,
        services: AppServices,
        revision: u64,
        identity: BrokerIdentity,
        tab: BrokerTab,
        cx: &mut Context<Self>,
    ) -> Self {
        let runtime_filter = cx.new(|cx| InputState::new(window, cx).placeholder("Filter runtime keys"));
        let runtime_subscription =
            cx.subscribe_in(&runtime_filter, window, |this, input, event: &InputEvent, _, cx| {
                if matches!(event, InputEvent::Change) {
                    this.store.runtime_filter = input.read(cx).value().to_string();
                    cx.notify();
                }
            });
        let mut inspector = Self {
            services,
            revision,
            active_tab: tab,
            store: InspectorStore::new(identity, revision),
            runtime_filter,
            config_editor: None,
            _runtime_subscription: runtime_subscription,
            config_dialog_open: false,
            overview_task: None,
            runtime_task: None,
            config_task: None,
            mutation_task: None,
        };
        inspector.refresh_overview(cx);
        inspector.refresh_active(cx);
        inspector
    }

    pub fn set_stale(&mut self, stale: bool, cx: &mut Context<Self>) {
        if stale {
            self.store.mark_stale();
            self.config_editor = None;
        } else {
            self.store.confirm_identity_revision(self.revision);
            self.refresh_overview(cx);
            self.refresh_active(cx);
        }
        cx.notify();
    }

    pub fn set_inventory_stale(&mut self, stale: bool, window: &mut Window, cx: &mut Context<Self>) {
        if stale {
            self.config_dialog_open = false;
            window.close_all_dialogs(cx);
        }
        self.set_stale(stale, cx);
    }

    pub fn set_revision(&mut self, revision: u64, cx: &mut Context<Self>) {
        if revision != self.revision {
            self.revision = revision;
            self.store.mark_stale();
            self.config_dialog_open = false;
            self.config_editor = None;
            cx.notify();
        }
    }

    pub fn consume_invalidations(&mut self, invalidations: &[BrokerCacheInvalidation], cx: &mut Context<Self>) {
        if invalidations.contains(&BrokerCacheInvalidation::BrokerRuntime(self.store.identity.clone())) {
            self.store.runtime.invalidate();
            self.refresh_runtime(cx);
        }
    }

    fn refresh_active(&mut self, cx: &mut Context<Self>) {
        match self.active_tab {
            BrokerTab::Overview if matches!(self.store.overview.state, Loadable::Idle) => self.refresh_overview(cx),
            BrokerTab::Runtime if matches!(self.store.runtime.state, Loadable::Idle) => self.refresh_runtime(cx),
            BrokerTab::Configuration if matches!(self.store.config.state, Loadable::Idle) => {
                self.refresh_config(false, cx);
            }
            BrokerTab::Overview | BrokerTab::Runtime | BrokerTab::Configuration => {}
        }
    }

    fn refresh_overview(&mut self, cx: &mut Context<Self>) {
        let Some(request) = self.store.begin_overview(self.revision) else {
            return;
        };
        let services = self.services.clone();
        let revision = self.revision;
        let identity = self.store.identity.clone();
        self.overview_task = Some(cx.spawn(async move |this, cx| {
            let result = services
                .broker_inventory(revision)
                .await
                .map(|items| items.into_iter().find(|item| item.identity == identity));
            let _ = this.update(cx, |inspector, cx| {
                inspector.store.finish_overview(request, inspector.revision, result);
                cx.notify();
            });
        }));
    }

    fn refresh_runtime(&mut self, cx: &mut Context<Self>) {
        let Some(request) = self.store.begin_runtime(self.revision) else {
            return;
        };
        let services = self.services.clone();
        let revision = self.revision;
        let identity = self.store.identity.clone();
        self.runtime_task = Some(cx.spawn(async move |this, cx| {
            let result = services.broker_runtime(revision, identity).await;
            let _ = this.update(cx, |inspector, cx| {
                inspector.store.finish_runtime(request, inspector.revision, result);
                cx.notify();
            });
        }));
    }

    fn refresh_config(&mut self, preserve_draft: bool, cx: &mut Context<Self>) {
        if preserve_draft {
            self.sync_draft_from_inputs(cx);
        }
        let Some(request) = self.store.begin_config(self.revision, preserve_draft) else {
            return;
        };
        let services = self.services.clone();
        let revision = self.revision;
        let identity = self.store.identity.clone();
        self.config_task = Some(cx.spawn(async move |this, cx| {
            let result = services.broker_config(revision, identity).await;
            let _ = this.update(cx, |inspector, cx| {
                if inspector.store.finish_config(request, inspector.revision, result)
                    && let Some(editor) = &inspector.config_editor
                {
                    let draft = inspector.store.draft().clone();
                    editor.update(cx, |editor, cx| editor.reconcile(draft, cx));
                }
                cx.notify();
            });
        }));
    }

    fn sync_draft_from_inputs(&mut self, cx: &mut Context<Self>) {
        let values = self
            .config_editor
            .as_ref()
            .map(|editor| editor.read(cx).values(cx))
            .unwrap_or_default();
        for (key, value) in values {
            let _ = self.store.set_draft_value(&key, value);
        }
    }

    fn request_save(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.sync_draft_from_inputs(cx);
        let submission = match self.store.prepare_submission() {
            Ok(Some(submission)) => submission,
            Ok(None) => return,
            Err(error) => {
                toast::ToastHost::error(error.summary().to_owned(), window, cx);
                return;
            }
        };
        let confirmation = submission.confirmation;
        let description = format!(
            "Broker: {}\nAddress: {}\nChanges: {}\nKeys: {}",
            confirmation.broker_name,
            confirmation.address,
            confirmation.change_count(),
            confirmation.changed_keys.join(", ")
        );
        let inspector = cx.entity().downgrade();
        dialog::open_confirm(
            "Apply Broker configuration?",
            description,
            "Apply",
            move |_, window, cx| {
                let _ = inspector.update(cx, |inspector, cx| inspector.submit_config(window, cx));
                true
            },
            window,
            cx,
        );
    }

    fn open_config_editor(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.config_dialog_open || !self.store.write_ready() {
            return;
        }
        let editor = cx.new(|_| ConfigEditor::new(self.store.draft().clone()));
        editor.update(cx, |editor, cx| editor.apply_pending(window, cx));
        self.config_editor = Some(editor.clone());
        let inspector = cx.entity().downgrade();
        let on_ok = inspector.clone();
        let on_cancel = inspector.clone();
        let on_close = inspector.clone();
        self.config_dialog_open = true;
        window.open_dialog(cx, move |dialog: Dialog, _, cx| {
            let theme = cx.theme();
            let on_ok = on_ok.clone();
            let on_cancel = on_cancel.clone();
            let on_close = on_close.clone();
            dialog
                .title("Edit Broker configuration")
                .w(px(700.))
                .max_w(px(760.))
                .confirm()
                .button_props(
                    DialogButtonProps::default()
                        .ok_text("Review changes")
                        .cancel_text("Cancel"),
                )
                .footer(|ok, cancel, window, cx| {
                    vec![
                        div()
                            .debug_selector(|| "broker-config-editor-cancel".to_owned())
                            .child(cancel(window, cx)),
                        div()
                            .debug_selector(|| "broker-config-editor-review".to_owned())
                            .child(ok(window, cx)),
                    ]
                })
                .child(
                    div()
                        .text_sm()
                        .text_color(theme.muted_foreground)
                        .child("Changes are applied directly to this Broker using generation compare-and-set. Sensitive keys remain read-only and are never included."),
                )
                .child(editor.clone())
                .on_ok(move |_, window, cx| {
                    let _ = on_ok.update(cx, |inspector, cx| inspector.request_save(window, cx));
                    false
                })
                .on_cancel(move |_, _, cx| {
                    let _ = on_cancel.update(cx, |inspector, cx| {
                        inspector.store.cancel_draft();
                        inspector.config_editor = None;
                        cx.notify();
                    });
                    true
                })
                .on_close(move |_, _, cx| {
                    let _ = on_close.update(cx, |inspector, cx| {
                        inspector.config_dialog_open = false;
                        inspector.config_editor = None;
                        cx.notify();
                    });
                })
        });
        cx.notify();
    }

    fn submit_config(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let submission = match self.store.begin_submit(self.revision) {
            Ok(Some(submission)) => submission,
            Ok(None) => return,
            Err(error) => {
                toast::ToastHost::error(error.summary().to_owned(), window, cx);
                return;
            }
        };
        let services = self.services.clone();
        let revision = self.revision;
        self.mutation_task = Some(cx.spawn_in(window, async move |this, cx| {
            let result = services.patch_broker_config(revision, submission.patch).await;
            let _ = this.update_in(cx, |inspector, window, cx| {
                let applied_invalidations = result.as_ref().ok().and_then(|result| match result {
                    crate::services::brokers::BrokerConfigMutationResult::Applied { invalidations, .. } => {
                        Some(invalidations.clone())
                    }
                    crate::services::brokers::BrokerConfigMutationResult::AppliedReloadFailed {
                        invalidations, ..
                    } => Some(invalidations.clone()),
                    crate::services::brokers::BrokerConfigMutationResult::GenerationConflict { .. } => None,
                });
                if !inspector.store.finish_submit(inspector.revision, result) {
                    return;
                }
                if matches!(inspector.store.submission, ConfigSubmissionState::Succeeded { .. })
                    && let Some(invalidations) = applied_invalidations.clone()
                {
                    window.close_all_dialogs(cx);
                    toast::ToastHost::success("Broker configuration updated", window, cx);
                    cx.emit(BrokerInspectorIntent::ConfigApplied(invalidations));
                } else if !matches!(inspector.store.submission, ConfigSubmissionState::Succeeded { .. }) {
                    if let Some(invalidations) = applied_invalidations {
                        cx.emit(BrokerInspectorIntent::ConfigApplied(invalidations));
                    }
                    inspector.open_submission_recovery(window, cx);
                }
                cx.notify();
            });
        }));
    }

    fn open_submission_recovery(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let inspector = cx.entity().downgrade();
        match self.store.submission {
            ConfigSubmissionState::GenerationConflict { .. } => dialog::open_confirm(
                "Broker configuration changed",
                "The draft is retained. Reload the latest generation and review the same draft again?",
                "Reload",
                move |_, _window, cx| {
                    let _ = inspector.update(cx, |inspector, cx| inspector.refresh_config(true, cx));
                    true
                },
                window,
                cx,
            ),
            ConfigSubmissionState::Failed(_) => dialog::open_confirm(
                "Broker configuration update failed",
                "The draft is retained. Retry the same reviewed changes?",
                "Retry",
                move |_, window, cx| {
                    let _ = inspector.update(cx, |inspector, cx| inspector.submit_config(window, cx));
                    true
                },
                window,
                cx,
            ),
            ConfigSubmissionState::AppliedReloadFailed { .. } => dialog::open_confirm(
                "Broker configuration reload failed",
                "The update was accepted, but authoritative configuration could not be reloaded. The draft is retained; reload before making another change.",
                "Reload",
                move |_, _window, cx| {
                    let _ = inspector.update(cx, |inspector, cx| inspector.refresh_config(true, cx));
                    true
                },
                window,
                cx,
            ),
            ConfigSubmissionState::Idle
            | ConfigSubmissionState::Submitting
            | ConfigSubmissionState::Succeeded { .. } => {}
        }
    }

    fn render_overview(&self, cx: &mut Context<Self>) -> gpui::Div {
        let theme = cx.theme();
        let Some(item) = self.store.overview.state.value() else {
            return loadable_message(&self.store.overview.state, theme.muted_foreground);
        };
        div()
            .flex()
            .flex_col()
            .child(key_value::render(
                "Cluster",
                item.identity.cluster.clone(),
                theme.foreground,
                theme.muted_foreground,
                theme.border,
            ))
            .child(key_value::render(
                "Broker ID",
                item.identity.broker_id.to_string(),
                theme.foreground,
                theme.muted_foreground,
                theme.border,
            ))
            .child(key_value::render(
                "Address",
                item.identity.address.clone(),
                theme.foreground,
                theme.muted_foreground,
                theme.border,
            ))
    }

    fn render_runtime(&self, cx: &mut Context<Self>) -> gpui::Div {
        let theme = cx.theme();
        let Some(entries) = self.store.runtime.state.value() else {
            return div()
                .flex()
                .flex_col()
                .gap_3()
                .child(loadable_message(&self.store.runtime.state, theme.muted_foreground))
                .child(
                    Button::new("refresh-broker-runtime-empty")
                        .label("Retry")
                        .outline()
                        .on_click(cx.listener(|inspector, _, _, cx| inspector.refresh_runtime(cx))),
                );
        };
        div()
            .flex()
            .flex_col()
            .gap_3()
            .child(
                div()
                    .flex()
                    .gap_2()
                    .child(div().flex_1().child(Input::new(&self.runtime_filter)))
                    .child(
                        Button::new("refresh-broker-runtime")
                            .label("Refresh")
                            .outline()
                            .disabled(matches!(self.store.runtime.state, Loadable::Refreshing(_)))
                            .on_click(cx.listener(|inspector, _, _, cx| inspector.refresh_runtime(cx))),
                    ),
            )
            .children(
                filter_runtime_entries(entries, &self.store.runtime_filter)
                    .into_iter()
                    .enumerate()
                    .map(|(index, entry)| self.render_runtime_entry(index, entry, cx)),
            )
    }

    fn render_runtime_entry(&self, index: usize, entry: &RuntimeEntry, cx: &mut Context<Self>) -> gpui::Div {
        let theme = cx.theme();
        let copy_value = entry.copy_value().map(str::to_owned);
        div()
            .flex()
            .items_start()
            .gap_2()
            .child(div().flex_1().child(key_value::render(
                entry.key.clone(),
                entry.display_value().to_owned(),
                theme.foreground,
                theme.muted_foreground,
                theme.border,
            )))
            .when_some(copy_value, |this, value| {
                this.child(
                    Button::new(("copy-runtime", index))
                        .label("Copy")
                        .ghost()
                        .on_click(move |_, _, cx| {
                            cx.write_to_clipboard(ClipboardItem::new_string(value.clone()));
                        }),
                )
            })
    }

    fn render_config(&self, cx: &mut Context<Self>) -> gpui::Div {
        let theme = cx.theme();
        let Some(snapshot) = self.store.config.state.value() else {
            return div()
                .flex()
                .flex_col()
                .gap_3()
                .child(loadable_message(&self.store.config.state, theme.muted_foreground))
                .child(
                    Button::new("refresh-broker-config-empty")
                        .label("Retry")
                        .outline()
                        .disabled(self.store.stale)
                        .on_click(cx.listener(|inspector, _, _, cx| {
                            inspector.refresh_config(false, cx);
                        })),
                );
        };
        let busy = matches!(self.store.submission, ConfigSubmissionState::Submitting);
        div()
            .flex()
            .flex_col()
            .gap_3()
            .children(snapshot.entries().iter().map(|(key, value)| {
                div().child(key_value::render(
                    key.clone(),
                    if is_sensitive_key(key) {
                        "<redacted>".into()
                    } else {
                        value.clone()
                    },
                    theme.foreground,
                    theme.muted_foreground,
                    theme.border,
                ))
            }))
            .child(
                div()
                    .pt_3()
                    .flex()
                    .justify_end()
                    .gap_2()
                    .child(
                        Button::new("refresh-broker-config")
                            .label("Refresh")
                            .outline()
                            .disabled(
                                busy || self.store.stale || matches!(self.store.config.state, Loadable::Refreshing(_)),
                            )
                            .on_click(cx.listener(|inspector, _, _, cx| {
                                inspector.refresh_config(false, cx);
                            })),
                    )
                    .child(
                        Button::new("edit-broker-config")
                            .label(if busy { "Applying…" } else { "Edit" })
                            .primary()
                            .disabled(busy || self.store.stale || !self.store.write_ready() || self.config_dialog_open)
                            .on_click(cx.listener(|inspector, _, window, cx| {
                                inspector.open_config_editor(window, cx);
                            })),
                    ),
            )
    }
}

impl Render for BrokerInspector {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let selected_index = match self.active_tab {
            BrokerTab::Overview => 0,
            BrokerTab::Runtime => 1,
            BrokerTab::Configuration => 2,
        };
        let body = match self.active_tab {
            BrokerTab::Overview => self.render_overview(cx),
            BrokerTab::Runtime => self.render_runtime(cx),
            BrokerTab::Configuration => self.render_config(cx),
        };
        div()
            .size_full()
            .flex()
            .flex_col()
            .gap_3()
            .child(div().text_sm().when(self.store.stale, |this| {
                this.child("Stale — this Broker is no longer in current inventory")
            }))
            .child(
                TabBar::new("broker-inspector-tabs")
                    .selected_index(selected_index)
                    .children(["Overview", "Runtime", "Config"])
                    .on_click(cx.listener(|inspector, index, _window, cx| {
                        let tab = match *index {
                            0 => BrokerTab::Overview,
                            1 => BrokerTab::Runtime,
                            _ => BrokerTab::Configuration,
                        };
                        inspector.active_tab = tab;
                        inspector.refresh_active(cx);
                        if let Ok(broker) = RouteKey::parse(inspector.store.identity.address.clone()) {
                            cx.emit(BrokerInspectorIntent::NavigateTab(AppRoute::BrokerDetail {
                                broker,
                                tab,
                            }));
                        }
                        cx.notify();
                    })),
            )
            .child(div().flex_1().min_h_0().overflow_y_scrollbar().child(body))
    }
}

fn loadable_message<T>(state: &Loadable<T>, color: gpui::Hsla) -> gpui::Div {
    let message = match state {
        Loadable::Idle | Loadable::InitialLoading => "Loading…",
        Loadable::Refreshing(_) => "Refreshing…",
        Loadable::Empty => "No entries returned.",
        Loadable::Failed { error, .. } => error.summary(),
        Loadable::Ready(_) => "",
    };
    div().p_4().text_sm().text_color(color).child(message.to_owned())
}

#[cfg(test)]
#[path = "broker_inspector_tests.rs"]
mod tests;
