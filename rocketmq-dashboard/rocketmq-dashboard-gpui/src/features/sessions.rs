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

//! Persistent native session governance. The UI never receives session capabilities.

use gpui::{
    App, AppContext as _, Context, Entity, InteractiveElement as _, IntoElement, ParentElement as _, Render,
    Styled as _, Task, WeakEntity, Window, div, prelude::FluentBuilder as _, px,
};
use gpui_component::{
    ActiveTheme as _, Disableable as _, Sizable as _, WindowExt as _,
    button::Button,
    dialog::DialogButtonProps,
    input::{Enter, Input, InputState},
    table::{Column, Table, TableDelegate, TableState},
};
use rocketmq_dashboard_storage::StorageEngine;

use super::{
    audit_presentation::timestamp,
    sessions_store::{SessionPageRequest, SessionsStore},
};
use crate::{
    services::{
        AppServices, SessionState,
        sessions::{RevokeAllRequest, SessionRow, validate_username},
    },
    state::{RequestEpoch, UiError},
};

struct SessionTable {
    columns: Vec<Column>,
    rows: Vec<SessionRow>,
    observed_at: u64,
    loading: bool,
    owner: WeakEntity<SessionsView>,
}

impl TableDelegate for SessionTable {
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
        let session = &self.rows[row];
        let value = match column {
            0 => session.username.clone(),
            1 => timestamp(session.created_at_epoch_ms),
            2 => timestamp(session.expires_at_epoch_ms),
            3 => timestamp(session.last_seen_at_epoch_ms),
            4 => if session.revoked_at_epoch_ms.is_some() {
                "Revoked"
            } else if session.expires_at_epoch_ms <= self.observed_at {
                "Expired"
            } else if session.current {
                "Current session"
            } else {
                "Active"
            }
            .to_owned(),
            _ => {
                let owner = self.owner.clone();
                let username = session.username.clone();
                return div().px_2().child(
                    Button::new(("session-revoke", row))
                        .debug_selector(move || format!("session-revoke-{row}"))
                        .small()
                        .outline()
                        .label("Revoke all for user")
                        .disabled(self.loading)
                        .on_click(move |_, window, cx| {
                            let _ = owner.update(cx, |view, cx| view.open_revoke_all(username.clone(), window, cx));
                        }),
                );
            }
        };
        div().px_2().text_sm().child(value)
    }
    fn render_empty(&mut self, _: &mut Window, _: &mut Context<TableState<Self>>) -> impl IntoElement {
        div()
            .p_4()
            .child("No persistent sessions match the applied username filter.")
    }
}

struct RevokeDialog {
    username: String,
    confirmation: Entity<InputState>,
    busy: bool,
    error: Option<String>,
}

impl RevokeDialog {
    fn new(username: String, window: &mut Window, cx: &mut Context<Self>) -> Self {
        let confirmation = cx.new(|cx| {
            InputState::new(window, cx)
                .multi_line(false)
                .placeholder("Type the exact username")
        });
        Self {
            username,
            confirmation,
            busy: false,
            error: None,
        }
    }
    fn request(&self, cx: &App) -> Result<RevokeAllRequest, UiError> {
        let request = RevokeAllRequest {
            username: self.username.clone(),
            confirmation: self.confirmation.read(cx).value().to_string(),
        };
        request.validate()?;
        Ok(request)
    }
    fn clear(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.confirmation
            .update(cx, |input, cx| *input = InputState::new(window, cx).multi_line(false));
        self.error = None;
        cx.notify();
    }
}

impl Render for RevokeDialog {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        div().w_full().min_w_0().flex().flex_col().gap_3()
            .child(format!("Revoke every active session for {} in the selected desktop storage?", self.username))
            .child("This includes this desktop when the username matches. Other desktops lose access on their next protected action or validation poll.")
            .child(div().debug_selector(|| "sessions-revoke-confirmation".into()).child(Input::new(&self.confirmation).disabled(self.busy)))
            .when(self.busy, |view| view.child("Revoking sessions…"))
            .when_some(self.error.clone(), |view, error| view.child(div().text_color(cx.theme().danger).child(error)))
    }
}

pub(crate) struct SessionsView {
    services: AppServices,
    store: SessionsStore,
    username: Entity<InputState>,
    table: Entity<TableState<SessionTable>>,
    form: Option<Entity<RevokeDialog>>,
    active: bool,
    owner: Option<SessionState>,
    epoch: RequestEpoch,
    query_task: Option<Task<()>>,
    mutation_task: Option<Task<()>>,
}

impl SessionsView {
    pub fn new(window: &mut Window, services: AppServices, cx: &mut Context<Self>) -> Self {
        let username = cx.new(|cx| {
            InputState::new(window, cx)
                .multi_line(false)
                .placeholder("Exact username; empty shows all users")
        });
        let owner = cx.entity().downgrade();
        let table = cx.new(|cx| {
            TableState::new(
                SessionTable {
                    columns: vec![
                        Column::new("username", "Username").width(px(160.)).fixed_left(),
                        Column::new("created", "Created UTC").width(px(210.)),
                        Column::new("expires", "Expires UTC").width(px(210.)),
                        Column::new("accessed", "Last access UTC").width(px(210.)),
                        Column::new("state", "State when read").width(px(160.)),
                        Column::new("actions", "Actions").width(px(180.)),
                    ],
                    rows: Vec::new(),
                    observed_at: 0,
                    loading: false,
                    owner,
                },
                window,
                cx,
            )
            .col_movable(false)
            .col_resizable(true)
        });
        Self {
            services,
            store: SessionsStore::default(),
            username,
            table,
            form: None,
            active: false,
            owner: None,
            epoch: RequestEpoch::initial(),
            query_task: None,
            mutation_task: None,
        }
    }

    pub fn set_active(&mut self, active: bool, window: &mut Window, cx: &mut Context<Self>) {
        if self.active == active {
            return;
        }
        self.active = active;
        if active && self.store.applied.is_none() {
            self.apply(window, cx);
        }
        if !active {
            self.query_task = None;
            self.store.loading = false;
            let _ = self.epoch.advance();
            self.close_form(window, cx);
            self.sync_table(cx);
        }
    }

    pub fn clear_context(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.query_task = None;
        self.mutation_task = None;
        let _ = self.epoch.advance();
        self.owner = None;
        self.store.clear();
        self.close_form(window, cx);
        self.username.update(cx, |input, cx| {
            *input = InputState::new(window, cx)
                .multi_line(false)
                .placeholder("Exact username; empty shows all users")
        });
        self.sync_table(cx);
        cx.notify();
    }

    fn local_access_is_current(&self) -> bool {
        self.owner
            .as_ref()
            .is_some_and(|owner| owner.is_authenticated() && self.services.current_session().as_ref() == Some(owner))
    }

    fn apply(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let username = self.username.read(cx).value().to_string();
        let username = if username.is_empty() {
            None
        } else {
            if let Err(error) = validate_username(&username) {
                self.store.error = Some(error);
                cx.notify();
                return;
            }
            Some(username)
        };
        self.close_form(window, cx);
        let request = self.store.apply(username);
        self.query(request, window, cx);
    }

    fn query(&mut self, request: SessionPageRequest, window: &mut Window, cx: &mut Context<Self>) {
        self.query_task = None;
        let Ok(epoch) = self.epoch.advance() else {
            return;
        };
        let owner = self.services.current_session();
        self.owner = owner.clone();
        self.store.loading = true;
        self.store.error = None;
        self.sync_table(cx);
        let services = self.services.clone();
        self.query_task = Some(cx.spawn_in(window, async move |this, cx| {
            let result = match request.cached.clone() {
                Some(page) => services.revalidate_session_page(page).await,
                None => services.query_sessions(request.query.clone()).await,
            };
            let _ = this.update_in(cx, |view, window, cx| {
                if !view.epoch.accepts(epoch) || !view.active {
                    return;
                }
                if view.services.current_session() != owner {
                    view.clear_context(window, cx);
                    return;
                }
                view.query_task = None;
                view.store.finish(request, result);
                view.sync_table(cx);
                cx.notify();
            });
        }));
        cx.notify();
    }

    fn sync_table(&self, cx: &mut Context<Self>) {
        self.table.update(cx, |table, cx| {
            table.delegate_mut().rows = self.store.page.as_ref().map_or_else(Vec::new, |page| page.rows.clone());
            table.delegate_mut().observed_at = self.store.page.as_ref().map_or(0, |page| page.observed_at_epoch_ms);
            table.delegate_mut().loading = self.store.loading || self.mutation_task.is_some();
            cx.notify();
        });
    }

    fn open_revoke_all(&mut self, username: String, window: &mut Window, cx: &mut Context<Self>) {
        if !self.local_access_is_current() || self.mutation_task.is_some() {
            self.clear_context(window, cx);
            return;
        }
        self.close_form(window, cx);
        let form = cx.new(|cx| RevokeDialog::new(username, window, cx));
        self.form = Some(form.clone());
        let owner = cx.entity().downgrade();
        let closing_owner = owner.clone();
        let closing_form = form.clone();
        let form_id = form.entity_id();
        window.open_dialog(cx, move |dialog, window, _| {
            let owner = owner.clone();
            let closing_owner = closing_owner.clone();
            let closing_form = closing_form.clone();
            dialog
                .title("Revoke persistent sessions")
                .w(window.viewport_size().width.min(px(620.)) - px(32.))
                .margin_top(px(16.))
                .max_h(window.viewport_size().height - px(32.))
                .confirm()
                .child(form.clone())
                .button_props(
                    DialogButtonProps::default()
                        .ok_text("Revoke all sessions")
                        .cancel_text("Cancel"),
                )
                .footer(|ok, cancel, window, cx| {
                    vec![
                        div()
                            .debug_selector(|| "sessions-revoke-cancel".into())
                            .child(cancel(window, cx)),
                        div()
                            .debug_selector(|| "sessions-revoke-submit".into())
                            .child(ok(window, cx)),
                    ]
                })
                .on_ok(move |_, window, cx| {
                    let _ = owner.update(cx, |view, cx| {
                        if view.form.as_ref().map(Entity::entity_id) == Some(form_id) {
                            view.submit_revoke(window, cx);
                        }
                    });
                    false
                })
                .on_close(move |_, window, cx| {
                    closing_form.update(cx, |form, cx| form.clear(window, cx));
                    let _ = closing_owner.update(cx, |view, cx| {
                        if view.form.as_ref().map(Entity::entity_id) == Some(form_id) {
                            view.form = None;
                            cx.notify();
                        }
                    });
                })
        });
        cx.notify();
    }

    fn submit_revoke(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.mutation_task.is_some() {
            return;
        }
        if !self.local_access_is_current() {
            self.clear_context(window, cx);
            return;
        }
        let Some(form) = self.form.clone() else {
            return;
        };
        let request = match form.read(cx).request(cx) {
            Ok(request) => request,
            Err(error) => {
                form.update(cx, |form, cx| {
                    form.error = Some(error.summary().to_owned());
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
        let form_id = form.entity_id();
        let services = self.services.clone();
        let owner = self.owner.clone();
        self.mutation_task = Some(cx.spawn_in(window, async move |this, cx| {
            let result = services.revoke_all_sessions(request).await;
            let _ = this.update_in(cx, |view, window, cx| {
                view.mutation_task = None;
                if result.as_ref().is_ok_and(|outcome| outcome.current_ended)
                    || view.services.current_session() != owner
                {
                    view.clear_context(window, cx);
                    return;
                }
                if view.form.as_ref().map(Entity::entity_id) != Some(form_id) {
                    return;
                }
                match result {
                    Ok(_) => {
                        view.close_form(window, cx);
                        view.apply(window, cx);
                    }
                    Err(error) => form.update(cx, |form, cx| {
                        form.busy = false;
                        form.error = Some(error.summary().to_owned());
                        cx.notify();
                    }),
                }
                view.sync_table(cx);
                cx.notify();
            });
        }));
        self.sync_table(cx);
        cx.notify();
    }

    fn close_form(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if let Some(form) = self.form.take() {
            form.update(cx, |form, cx| form.clear(window, cx));
            window.close_dialog(cx);
        }
    }
}

impl Render for SessionsView {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let storage = self.store.page.as_ref().map(|page| {
            format!(
                "Selected desktop storage: {} · observed {}",
                storage_name(page.engine),
                timestamp(page.observed_at_epoch_ms)
            )
        });
        div().size_full().min_w_0().flex().flex_col().gap_3().p_4().debug_selector(|| "sessions-page".into())
            .child("Persistent sessions use the local environment-configured operator. Each login expires after eight hours; access does not extend expiry.")
            .child("Session records survive restart. Sign in again on each launch; credentials and session capabilities are never saved in desktop configuration.")
            .when_some(storage, |view, text| view.child(text))
            .when_some(self.store.applied.as_ref(), |view, filter| view.child(filter.username.as_ref().map_or_else(|| "Applied filter: all usernames".to_owned(), |username| format!("Applied exact username: {username}"))))
            .child(div().w_full().flex().flex_wrap().items_center().gap_2()
                .child(div().flex_1().min_w(px(220.)).debug_selector(|| "sessions-username-filter".into())
                    .on_action(cx.listener(|view, _: &Enter, window, cx| view.apply(window, cx)))
                    .child(Input::new(&self.username)))
                .child(Button::new("sessions-apply").debug_selector(|| "sessions-apply".into()).label("Apply username filter").disabled(self.store.loading).on_click(cx.listener(|view, _, window, cx| view.apply(window, cx)))))
            .when_some(self.store.error.as_ref(), |view, error| view.child(div().text_color(cx.theme().danger).child(error.summary().to_owned())))
            .child(div().flex_1().min_h_0().child(Table::new(&self.table).bordered(true).small()))
            .child(div().flex().flex_wrap().items_center().gap_2()
                .child(Button::new("sessions-previous").debug_selector(|| "sessions-previous".into()).label("Previous page").outline().disabled(self.store.loading || self.store.previous().is_none()).on_click(cx.listener(|view, _, window, cx| { if let Some(request) = view.store.previous() { view.query(request, window, cx); } })))
                .child(format!("Page {} · up to 25 records", self.store.page_index + 1))
                .child(Button::new("sessions-next").debug_selector(|| "sessions-next".into()).label("Next page").outline().disabled(self.store.loading || self.store.next().is_none()).on_click(cx.listener(|view, _, window, cx| { if let Some(request) = view.store.next() { view.query(request, window, cx); } }))))
    }
}

fn storage_name(engine: StorageEngine) -> &'static str {
    match engine {
        StorageEngine::File => "File",
        StorageEngine::Sqlite => "SQLite",
        StorageEngine::MySql => "MySQL",
        StorageEngine::Postgres => "PostgreSQL",
    }
}

#[cfg(test)]
impl SessionsView {
    pub(crate) fn confirmation_input_for_test(&self, cx: &App) -> Option<Entity<InputState>> {
        self.form.as_ref().map(|form| form.read(cx).confirmation.clone())
    }
}

#[cfg(test)]
#[path = "sessions_tests.rs"]
mod tests;
