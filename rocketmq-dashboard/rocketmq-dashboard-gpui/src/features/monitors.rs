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

//! Environment-scoped rule configuration, with independent query and accepted-write ownership.

use gpui::prelude::FluentBuilder as _;
use gpui::{
    App, AppContext as _, Context, Entity, InteractiveElement as _, IntoElement, ParentElement as _, Render,
    Styled as _, Task, WeakEntity, Window, div,
};
use gpui_component::{
    ActiveTheme as _, Disableable as _, Sizable as _, WindowExt as _,
    button::{Button, ButtonVariants as _},
    dialog::{Dialog, DialogButtonProps},
    input::{Input, InputState},
    table::{Column, Table, TableDelegate, TableState},
};
use rocketmq_dashboard_common::{MonitorMutationOutcome, MonitorRuleStatus, MonitorRuleView};

use super::{monitor_dialog::MonitorDialog, monitors_store::MonitorsStore};
use crate::{services::AppServices, state::Loadable};

struct MonitorTable {
    columns: Vec<Column>,
    rows: Vec<MonitorRuleView>,
    owner: Option<WeakEntity<MonitorsView>>,
    enabled: bool,
    loading: bool,
}

impl TableDelegate for MonitorTable {
    fn columns_count(&self, _: &App) -> usize {
        self.columns.len()
    }
    fn rows_count(&self, _: &App) -> usize {
        self.rows.len()
    }
    fn column(&self, index: usize, _: &App) -> &Column {
        &self.columns[index]
    }
    fn loading(&self, _: &App) -> bool {
        self.loading
    }
    fn render_td(
        &mut self,
        row: usize,
        column: usize,
        _: &mut Window,
        _: &mut Context<TableState<Self>>,
    ) -> impl IntoElement {
        let rule = &self.rows[row];
        let value = match column {
            0 => rule.consumer_group.clone(),
            1 => rule.thresholds.map_or_else(
                || "Not configured".into(),
                |thresholds| thresholds.min_count.to_string(),
            ),
            2 => rule.thresholds.map_or_else(
                || "Not configured".into(),
                |thresholds| thresholds.max_diff_total.to_string(),
            ),
            3 => match rule.status {
                MonitorRuleStatus::Ready if rule.enabled => "Configured · enabled".into(),
                MonitorRuleStatus::Ready => "Configured · disabled".into(),
                MonitorRuleStatus::NeedsThresholds => "Needs thresholds".into(),
                MonitorRuleStatus::NeedsEnvironmentAndThresholds => "Unassigned · needs thresholds".into(),
            },
            4 => format!(
                "{} · {}",
                rule.revision,
                rule.updated_at_epoch_ms
                    .and_then(|time| i64::try_from(time).ok())
                    .and_then(chrono::DateTime::from_timestamp_millis)
                    .map_or_else(
                        || "unknown legacy write time".into(),
                        |time| time.format("%Y-%m-%d %H:%M:%S UTC").to_string()
                    )
            ),
            _ => {
                let edit_owner = self.owner.clone();
                let edit_rule = rule.clone();
                let delete_owner = self.owner.clone();
                let delete_rule = rule.clone();
                return div()
                    .flex()
                    .gap_2()
                    .child(
                        Button::new(("monitor-edit", row))
                            .debug_selector(move || format!("monitor-edit-{row}"))
                            .small()
                            .outline()
                            .label(if rule.environment_id.is_none() {
                                "Assign / complete"
                            } else {
                                "Edit"
                            })
                            .disabled(!self.enabled)
                            .on_click(move |_, window, cx| {
                                if let Some(owner) = &edit_owner {
                                    let _ = owner.update(cx, |view, cx| {
                                        view.open_editor(Some(edit_rule.clone()), false, window, cx)
                                    });
                                }
                            }),
                    )
                    .child(
                        Button::new(("monitor-delete", row))
                            .debug_selector(move || format!("monitor-delete-{row}"))
                            .small()
                            .outline()
                            .label("Delete")
                            .disabled(!self.enabled || rule.environment_id.is_none())
                            .on_click(move |_, window, cx| {
                                if let Some(owner) = &delete_owner {
                                    let _ = owner.update(cx, |view, cx| {
                                        view.open_editor(Some(delete_rule.clone()), true, window, cx)
                                    });
                                }
                            }),
                    );
            }
        };
        div().px_2().text_sm().child(value)
    }
    fn render_empty(&mut self, _: &mut Window, _: &mut Context<TableState<Self>>) -> impl IntoElement {
        div().p_4().child("No rule configurations match the Group search.")
    }
}

pub struct MonitorsView {
    services: AppServices,
    revision: u64,
    active: bool,
    store: MonitorsStore,
    keyword: Entity<InputState>,
    table: Entity<TableState<MonitorTable>>,
    form: Option<Entity<MonitorDialog>>,
    query_task: Option<Task<()>>,
    mutation_task: Option<Task<()>>,
}

impl MonitorsView {
    pub fn new(window: &mut Window, services: AppServices, revision: u64, cx: &mut Context<Self>) -> Self {
        let keyword = cx.new(|cx| InputState::new(window, cx).placeholder("Search Consumer Group"));
        let owner = cx.entity().downgrade();
        let table = cx.new(|cx| {
            TableState::new(
                MonitorTable {
                    columns: vec![
                        Column::new("group", "Consumer Group")
                            .width(gpui::px(180.))
                            .fixed_left(),
                        Column::new("min", "Min Count").width(gpui::px(100.)),
                        Column::new("max", "Max Diff Total").width(gpui::px(140.)),
                        Column::new("state", "Configuration").width(gpui::px(210.)),
                        Column::new("revision", "Revision / write time").width(gpui::px(250.)),
                        Column::new("actions", "Actions").width(gpui::px(170.)),
                    ],
                    rows: Vec::new(),
                    owner: Some(owner),
                    enabled: false,
                    loading: false,
                },
                window,
                cx,
            )
            .col_movable(false)
            .col_resizable(true)
        });
        Self {
            services,
            revision,
            active: false,
            store: MonitorsStore::default(),
            keyword,
            table,
            form: None,
            query_task: None,
            mutation_task: None,
        }
    }

    pub fn set_active(&mut self, active: bool, window: &mut Window, cx: &mut Context<Self>) {
        if active == self.active {
            return;
        }
        self.active = active;
        if active {
            self.refresh(cx);
        } else {
            self.query_task = None;
            self.store.rules.invalidate();
            if self.form.take().is_some() {
                window.close_all_dialogs(cx);
            }
        }
    }

    pub fn set_revision(&mut self, revision: u64, cx: &mut Context<Self>) {
        if revision == self.revision {
            return;
        }
        self.revision = revision;
        self.store.rules.invalidate();
        self.sync_table(cx);
        if self.active {
            self.refresh(cx);
        }
    }

    pub fn clear_for_environment(&mut self, revision: u64, window: &mut Window, cx: &mut Context<Self>) {
        self.revision = revision;
        self.query_task = None;
        self.form = None;
        self.store.clear();
        self.keyword.update(cx, |input, cx| input.set_value("", window, cx));
        self.sync_table(cx);
        if self.active {
            self.refresh(cx);
        }
    }

    fn verified(&self) -> bool {
        self.store
            .rules
            .state
            .value()
            .is_some_and(|snapshot| snapshot.configuration_revision == self.revision)
    }

    fn sync_table(&mut self, cx: &mut Context<Self>) {
        self.store.rebuild();
        let enabled = self.verified() && self.mutation_task.is_none();
        self.table.update(cx, |table, cx| {
            table.delegate_mut().rows = self.store.visible.clone();
            table.delegate_mut().enabled = enabled;
            table.delegate_mut().loading = matches!(self.store.rules.state, Loadable::InitialLoading);
            cx.notify();
        });
        cx.notify();
    }

    fn refresh(&mut self, cx: &mut Context<Self>) {
        if !self.active {
            return;
        }
        let Some(request) = self.store.rules.begin(self.revision) else {
            return;
        };
        let services = self.services.clone();
        self.query_task = Some(cx.spawn(async move |this, cx| {
            let result = services.monitor_rules(request.revision()).await;
            let _ = this.update(cx, |view, cx| {
                if view.store.rules.finish(request, view.revision, result.map(Some)) {
                    view.sync_table(cx);
                }
            });
        }));
        self.sync_table(cx);
    }

    fn open_editor(
        &mut self,
        rule: Option<MonitorRuleView>,
        deleting: bool,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.form.is_some() || self.mutation_task.is_some() || !self.verified() {
            return;
        }
        let Some(snapshot) = self.store.rules.state.value() else {
            return;
        };
        let environment = snapshot.environment_id.clone();
        let form = cx.new(|cx| MonitorDialog::new(environment, self.revision, rule, deleting, window, cx));
        self.form = Some(form.clone());
        let owner = cx.entity().downgrade();
        let close_owner = owner.clone();
        window.open_dialog(cx, move |dialog: Dialog, window, _| {
            let owner = owner.clone();
            let close_owner = close_owner.clone();
            dialog
                .title(if deleting {
                    "Delete Monitor rule"
                } else {
                    "Configure Monitor rule"
                })
                .margin_top(gpui::px(16.))
                .max_h(window.viewport_size().height - gpui::px(32.))
                .footer(|ok, cancel, window, cx| {
                    vec![
                        div()
                            .debug_selector(|| "monitor-dialog-close".into())
                            .child(cancel(window, cx))
                            .into_any_element(),
                        div()
                            .debug_selector(|| "monitor-dialog-save".into())
                            .child(ok(window, cx))
                            .into_any_element(),
                    ]
                })
                .button_props(
                    DialogButtonProps::default()
                        .ok_text(if deleting { "Delete rule" } else { "Save rule" })
                        .cancel_text("Close"),
                )
                .child(form.clone())
                .on_ok(move |_, window, cx| {
                    let _ = owner.update(cx, |view, cx| view.submit(window, cx));
                    false
                })
                .on_close(move |_, _, cx| {
                    let _ = close_owner.update(cx, |view, cx| {
                        view.form = None;
                        cx.notify();
                    });
                })
        });
        cx.notify();
    }

    fn submit(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.mutation_task.is_some() {
            return;
        }
        let Some(form) = self.form.clone() else {
            return;
        };
        let revision = form.read(cx).configuration_revision;
        let request = match form.read(cx).prepare(cx) {
            Ok(request) if revision == self.revision => request,
            Ok(_) => {
                form.update(cx, |form, cx| {
                    form.error = Some("The connection changed. Close and reopen the rule after refreshing.".into());
                    cx.notify();
                });
                return;
            }
            Err(error) => {
                form.update(cx, |form, cx| {
                    form.error = Some(error.summary().into());
                    cx.notify();
                });
                return;
            }
        };
        form.update(cx, |form, cx| {
            form.busy = true;
            form.error = None;
            cx.notify();
        });
        let services = self.services.clone();
        let form_id = form.entity_id();
        self.mutation_task = Some(cx.spawn_in(window, async move |this, cx| {
            let result = services.mutate_monitor_rule(revision, request).await;
            let _ = this.update_in(cx, |view, window, cx| {
                view.mutation_task = None;
                view.sync_table(cx);
                if view.revision != revision { return; }
                if result.is_ok() { view.refresh(cx); }
                if view.form.as_ref().map(Entity::entity_id) != Some(form_id) { return; }
                match result {
                    Ok(MonitorMutationOutcome::Conflict { latest }) => { form.update(cx, |form, cx| { form.busy = false; form.conflict = Some(latest); form.error = Some("The saved rule changed. Your draft is preserved; review the authoritative state below.".into()); cx.notify(); }); },
                    Ok(MonitorMutationOutcome::Saved(_) | MonitorMutationOutcome::Deleted) => { view.form = None; window.close_all_dialogs(cx); }
                    Err(error) => form.update(cx, |form, cx| { form.busy = false; form.error = Some(error.summary().into()); cx.notify(); }),
                }
                cx.notify();
            });
        }));
        self.sync_table(cx);
    }
}

impl Render for MonitorsView {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let failure = match &self.store.rules.state {
            Loadable::Failed { error, .. } => Some(error.summary().to_owned()),
            _ => None,
        };
        let loading = matches!(
            self.store.rules.state,
            Loadable::InitialLoading | Loadable::Refreshing(_)
        );
        div().size_full().flex().flex_col().gap_3().p_4().debug_selector(|| "monitor-rules-page".into())
            .child("Rule configuration only · Min Count is a consumer count; Max Diff Total is a backlog message count. No evaluator, live alerts, or notifications.")
            .child(div().flex().items_center().gap_2()
                .child(div().flex_1().debug_selector(|| "monitor-search-input".into()).child(Input::new(&self.keyword)))
                .child(Button::new("monitor-search").debug_selector(|| "monitor-search".into()).label("Search").on_click(cx.listener(|view, _, _, cx| { view.store.search(&view.keyword.read(cx).value()); view.sync_table(cx); })))
                .child(Button::new("monitor-refresh").debug_selector(|| "monitor-refresh".into()).label("Refresh").on_click(cx.listener(|view, _, _, cx| view.refresh(cx))))
                .child(Button::new("monitor-create").debug_selector(|| "monitor-create".into()).primary().label("Create rule").disabled(!self.verified() || self.mutation_task.is_some()).on_click(cx.listener(|view, _, window, cx| view.open_editor(None, false, window, cx)))))
            .when(loading, |view| view.child("Loading rule configurations…"))
            .when_some(failure, |view, message| view.child(div().text_color(cx.theme().danger).child(message)))
            .child(div().flex_1().min_h_0().child(Table::new(&self.table).bordered(true).small()))
    }
}

#[cfg(test)]
#[path = "monitors_tests.rs"]
mod tests;
