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

//! Confirmed-scope ACL workspace. Queries follow the page; admitted writes follow the application.

use super::{
    acl_policy_dialog::AclPolicyDialog,
    acl_store::{AclRow, AclStore, AclTab},
    acl_user_dialog::AclUserDialog,
};
use crate::{
    components::topic_picker,
    services::{AppServices, acl::AclRequest},
    state::Loadable,
};
use gpui::{
    App, AppContext as _, Context, Entity, EntityId, InteractiveElement as _, IntoElement, ParentElement as _, Render,
    StatefulInteractiveElement as _, Styled as _, Subscription, Task, Window, div, px,
};
use gpui_component::{
    ActiveTheme as _, Disableable as _, Selectable as _, WindowExt as _,
    button::{Button, ButtonVariants as _},
    dialog::{Dialog, DialogButtonProps},
    input::{Input, InputState},
    scroll::ScrollableElement as _,
    select::{Select, SelectEvent},
};
use rocketmq_dashboard_common::*;

#[derive(Clone)]
enum AclForm {
    User(Entity<AclUserDialog>),
    Policy(Entity<AclPolicyDialog>),
}
impl AclForm {
    fn id(&self) -> EntityId {
        match self {
            Self::User(form) => form.entity_id(),
            Self::Policy(form) => form.entity_id(),
        }
    }
    fn prepare(&self, cx: &App) -> Result<AclCommand, AclValidationError> {
        match self {
            Self::User(form) => form.read(cx).prepare(cx),
            Self::Policy(form) => form.read(cx).prepare(cx),
        }
    }
    fn clear(&self, window: &mut Window, cx: &mut App) {
        if let Self::User(form) = self {
            form.update(cx, |form, cx| form.clear_secret(window, cx));
        }
    }
    fn attempted(&self, window: &mut Window, cx: &mut App) {
        match self {
            Self::User(form) => form.update(cx, |form, cx| {
                form.clear_secret(window, cx);
                form.attempted = true;
                form.busy = true;
                form.error = None;
                cx.notify();
            }),
            Self::Policy(form) => form.update(cx, |form, cx| {
                form.attempted = true;
                form.busy = true;
                form.error = None;
                cx.notify();
            }),
        }
    }
    fn error(&self, message: String, cx: &mut App) {
        match self {
            Self::User(form) => form.update(cx, |form, cx| {
                form.error = Some(message);
                form.busy = false;
                cx.notify();
            }),
            Self::Policy(form) => form.update(cx, |form, cx| {
                form.error = Some(message);
                form.busy = false;
                cx.notify();
            }),
        }
    }
}

pub struct AclView {
    services: AppServices,
    revision: u64,
    active: bool,
    pub(super) store: AclStore,
    cluster: Entity<topic_picker::TopicPickerState>,
    broker: Entity<topic_picker::TopicPickerState>,
    pub(super) users_search: Entity<InputState>,
    pub(super) policies_search: Entity<InputState>,
    form: Option<AclForm>,
    form_revision: u64,
    show_passwords: bool,
    status: Option<String>,
    write_result: Option<AclWriteResult>,
    inventory_task: Option<Task<()>>,
    users_task: Option<Task<()>>,
    policies_task: Option<Task<()>>,
    mutation_task: Option<Task<()>>,
    _subscriptions: Vec<Subscription>,
}
impl AclView {
    pub fn new(window: &mut Window, services: AppServices, revision: u64, cx: &mut Context<Self>) -> Self {
        let cluster = topic_picker::state(window, cx);
        let broker = topic_picker::state(window, cx);
        let users_search = cx.new(|cx| InputState::new(window, cx).placeholder("Search Users"));
        let policies_search = cx.new(|cx| InputState::new(window, cx).placeholder("Search Policies"));
        let subscriptions = vec![
            cx.subscribe_in(&cluster, window, |view, _, event, window, cx| {
                let SelectEvent::Confirm(value) = event;
                view.store.draft_cluster = value.clone().unwrap_or_default();
                view.store.draft_broker = None;
                view.draft_changed(window, cx);
                let brokers = view
                    .store
                    .identities()
                    .into_iter()
                    .filter(|target| target.cluster == view.store.draft_cluster)
                    .map(|target| target.broker_name)
                    .collect();
                topic_picker::set_topics(&view.broker, brokers, window, cx);
            }),
            cx.subscribe_in(&broker, window, |view, _, event, window, cx| {
                let SelectEvent::Confirm(value) = event;
                view.store.draft_broker = value.clone();
                view.draft_changed(window, cx);
            }),
        ];
        Self {
            services,
            revision,
            active: false,
            store: AclStore::default(),
            cluster,
            broker,
            users_search,
            policies_search,
            form: None,
            form_revision: revision,
            show_passwords: false,
            status: None,
            write_result: None,
            inventory_task: None,
            users_task: None,
            policies_task: None,
            mutation_task: None,
            _subscriptions: subscriptions,
        }
    }
    pub fn set_active(&mut self, active: bool, window: &mut Window, cx: &mut Context<Self>) {
        if self.active == active {
            return;
        }
        self.active = active;
        if active {
            self.load_inventory(window, cx);
        } else {
            self.leave(window, cx);
        }
    }
    pub fn leave(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.active = false;
        self.inventory_task = None;
        self.draft_changed(window, cx);
    }
    pub fn set_revision(&mut self, revision: u64, window: &mut Window, cx: &mut Context<Self>) {
        if revision == self.revision {
            return;
        }
        self.revision = revision;
        self.inventory_task = None;
        self.store.inventory.clear();
        self.store.draft_cluster.clear();
        self.store.draft_broker = None;
        self.draft_changed(window, cx);
        topic_picker::set_topics(&self.cluster, Vec::new(), window, cx);
        topic_picker::set_topics(&self.broker, Vec::new(), window, cx);
        if self.active {
            self.load_inventory(window, cx);
        }
    }
    fn close_form(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.show_passwords = false;
        self.store.clear_secrets();
        if let Some(form) = self.form.take() {
            form.clear(window, cx);
            window.close_all_dialogs(cx);
        }
    }
    fn draft_changed(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.users_task = None;
        self.policies_task = None;
        self.close_form(window, cx);
        self.store.invalidate_scope();
        self.show_passwords = false;
        self.status = None;
        self.write_result = None;
        cx.notify();
    }
    fn load_inventory(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if !self.active {
            return;
        }
        let Some(request) = self.store.inventory.begin(self.revision) else {
            return;
        };
        let services = self.services.clone();
        self.inventory_task = Some(cx.spawn_in(window, async move |this, cx| {
            let result = services.broker_inventory(request.revision()).await;
            let _ = this.update_in(cx, |view, window, cx| {
                if view.store.inventory.finish(request, view.revision, result.map(Some)) {
                    let clusters = view
                        .store
                        .identities()
                        .into_iter()
                        .map(|target| target.cluster)
                        .collect();
                    topic_picker::set_topics(&view.cluster, clusters, window, cx);
                    cx.notify();
                }
            });
        }));
        cx.notify();
    }
    fn confirm(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.close_form(window, cx);
        match self.store.confirm() {
            Ok(()) => {
                self.show_passwords = false;
                self.status = None;
                self.write_result = None;
                self.refresh(cx);
            }
            Err(error) => {
                self.status = Some(error.to_string());
                cx.notify();
            }
        }
    }
    fn change_tab(&mut self, tab: AclTab, window: &mut Window, cx: &mut Context<Self>) {
        if tab == self.store.tab {
            return;
        }
        self.close_form(window, cx);
        self.users_task = None;
        self.policies_task = None;
        self.show_passwords = false;
        self.status = None;
        self.write_result = None;
        self.store.change_tab(tab);
        self.refresh(cx);
        cx.notify();
    }
    fn refresh(&mut self, cx: &mut Context<Self>) {
        if !self.active {
            return;
        }
        let Some(scope) = self.store.confirmed.clone() else {
            return;
        };
        let services = self.services.clone();
        match self.store.tab {
            AclTab::Users => {
                let Some(token) = self.store.users.begin(self.revision) else {
                    return;
                };
                let request = AclRequest {
                    revision: self.revision,
                    epoch: token.epoch(),
                    scope: scope.clone(),
                };
                self.users_task = Some(cx.spawn(async move |this, cx| {
                    let result = services.acl_users(request).await;
                    let _ = this.update(cx, |view, cx| {
                        if view.store.confirmed.as_ref() == Some(&scope)
                            && view.store.users.finish(token, view.revision, result.map(Some))
                        {
                            view.store.rebuild();
                            cx.notify();
                        }
                    });
                }));
            }
            AclTab::Policies => {
                let Some(token) = self.store.policies.begin(self.revision) else {
                    return;
                };
                let request = AclRequest {
                    revision: self.revision,
                    epoch: token.epoch(),
                    scope: scope.clone(),
                };
                self.policies_task = Some(cx.spawn(async move |this, cx| {
                    let result = services.acl_policies(request).await;
                    let _ = this.update(cx, |view, cx| {
                        if view.store.confirmed.as_ref() == Some(&scope)
                            && view.store.policies.finish(token, view.revision, result.map(Some))
                        {
                            view.store.rebuild();
                            cx.notify();
                        }
                    });
                }));
            }
        }
        cx.notify();
    }
    fn open_user(&mut self, user: Option<AclUser>, deleting: bool, window: &mut Window, cx: &mut Context<Self>) {
        if self.mutation_task.is_some() || self.form.is_some() || user.as_ref().is_some_and(|user| user.read_only) {
            return;
        }
        let Some(scope) = self.store.confirmed.clone() else {
            return;
        };
        let form = cx.new(|cx| AclUserDialog::new(scope, user, deleting, window, cx));
        self.open_form(AclForm::User(form), window, cx);
    }
    fn open_policy(
        &mut self,
        policy: Option<(AclPolicy, usize)>,
        deleting: bool,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.mutation_task.is_some()
            || self.form.is_some()
            || policy.as_ref().is_some_and(|(policy, _)| policy.read_only)
        {
            return;
        }
        let Some(scope) = self.store.confirmed.clone() else {
            return;
        };
        let form = cx.new(|cx| AclPolicyDialog::new(scope, policy, deleting, window, cx));
        self.open_form(AclForm::Policy(form), window, cx);
    }
    fn open_form(&mut self, form: AclForm, window: &mut Window, cx: &mut Context<Self>) {
        self.show_passwords = false;
        self.store.clear_secrets();
        self.form = Some(form.clone());
        self.form_revision = self.revision;
        let owner = cx.entity().downgrade();
        let closer = owner.clone();
        window.open_dialog(cx, move |dialog: Dialog, window, _| {
            let owner = owner.clone();
            let closer = closer.clone();
            let form = form.clone();
            let child = match &form {
                AclForm::User(form) => form.clone().into_any_element(),
                AclForm::Policy(form) => form.clone().into_any_element(),
            };
            dialog
                .title("Confirm ACL operation")
                .margin_top(px(16.))
                .max_h(window.viewport_size().height - px(32.))
                .button_props(
                    DialogButtonProps::default()
                        .ok_text("Confirm once")
                        .cancel_text("Close"),
                )
                .child(child)
                .on_ok(move |_, window, cx| {
                    let _ = owner.update(cx, |view, cx| view.submit(window, cx));
                    false
                })
                .on_close(move |_, window, cx| {
                    let _ = closer.update(cx, |view, cx| {
                        view.show_passwords = false;
                        view.store.clear_secrets();
                        if let Some(form) = view.form.take() {
                            form.clear(window, cx);
                        }
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
        let command = match form.prepare(cx) {
            Ok(command)
                if self.form_revision == self.revision && self.store.confirmed.as_ref() == Some(&command.scope) =>
            {
                command
            }
            Ok(_) => {
                form.error("Scope changed. Close and confirm it again.".into(), cx);
                return;
            }
            Err(error) => {
                form.error(error.to_string(), cx);
                return;
            }
        };
        let scope = command.scope.clone();
        let revision = self.revision;
        let form_id = form.id();
        form.attempted(window, cx);
        let services = self.services.clone();
        self.mutation_task=Some(cx.spawn_in(window,async move|this,cx|{let result=services.mutate_acl(revision,command).await;let _=this.update_in(cx,|view,window,cx|{
            view.mutation_task=None;
            if view.revision!=revision||view.store.confirmed.as_ref()!=Some(&scope){cx.notify();return;}
            match result{
                Ok(result)=>{let all=result.targets.iter().all(|target|target.state==AclWriteState::Applied);view.status=Some(if all{"Every selected target confirmed the operation.".into()}else{"Some targets did not confirm success. Verify the per-target results; this attempt cannot be replayed.".into()});view.write_result=Some(result);view.show_passwords=false;view.refresh(cx);if view.form.as_ref().map(AclForm::id)==Some(form_id){if all{view.close_form(window,cx);}else{form.error("Attempt complete. Verify each target, then close this form.".into(),cx);}}},
                Err(error)=>{view.status=Some(error.summary().into());if view.form.as_ref().map(AclForm::id)==Some(form_id){form.error(error.summary().into(),cx);}}
            }cx.notify();
        });}));
        cx.notify();
    }
    fn render_row(&self, row: &AclRow, index: usize, cx: &mut Context<Self>) -> gpui::Div {
        let disabled = self.mutation_task.is_some();
        let mut element = div()
            .p_3()
            .border_b_1()
            .border_color(cx.theme().border)
            .flex()
            .flex_col()
            .gap_2();
        match row {
            AclRow::User(user) => {
                let password = match &user.password {
                    Some(password) if self.show_passwords => password.expose().to_owned(),
                    Some(_) => "••••••••".into(),
                    None => "No retained password".into(),
                };
                element = element
                    .child(format!(
                        "{} · {} · {} · {} @ {}",
                        user.username,
                        user.user_type.as_deref().unwrap_or("Unknown type"),
                        user.status.as_deref().unwrap_or("Unknown status"),
                        user.target.broker_name,
                        user.target.address
                    ))
                    .child(
                        div()
                            .debug_selector(move || format!("acl-user-secret-{index}"))
                            .child(password),
                    );
                let mut edit = user.clone();
                edit.clear_secret();
                let delete = edit.clone();
                element = element.child(
                    div()
                        .flex()
                        .flex_wrap()
                        .gap_2()
                        .child(
                            Button::new(("acl-user-edit", index))
                                .debug_selector(move || format!("acl-user-edit-{index}"))
                                .label("Edit user")
                                .disabled(disabled || user.read_only)
                                .on_click(cx.listener(move |view, _, window, cx| {
                                    view.open_user(Some(edit.clone()), false, window, cx)
                                })),
                        )
                        .child(
                            Button::new(("acl-user-delete", index))
                                .debug_selector(move || format!("acl-user-delete-{index}"))
                                .label("Delete user")
                                .disabled(disabled || user.read_only)
                                .on_click(cx.listener(move |view, _, window, cx| {
                                    view.open_user(Some(delete.clone()), true, window, cx)
                                })),
                        ),
                );
                if user.read_only {
                    element = element.child("Read-only: unknown or unsupported user fields.");
                }
            }
            AclRow::Policy(policy, entry_index) => {
                element = element.child(format!(
                    "{} · {} · {} @ {}",
                    policy.subject, policy.policy_type, policy.target.broker_name, policy.target.address
                ));
                if let Some(entry) = policy.entries.get(*entry_index) {
                    element = element.child(format!(
                        "{} · {} · {} · Source IPs: {}",
                        entry.resource,
                        entry.actions.join(", "),
                        entry.decision,
                        entry.source_ips.join(", ")
                    ));
                }
                let edit = (policy.clone(), *entry_index);
                let delete = edit.clone();
                element = element.child(
                    div()
                        .flex()
                        .flex_wrap()
                        .gap_2()
                        .child(
                            Button::new(("acl-policy-edit", index))
                                .debug_selector(move || format!("acl-policy-edit-{index}"))
                                .label("Edit entry")
                                .disabled(disabled || policy.read_only)
                                .on_click(cx.listener(move |view, _, window, cx| {
                                    view.open_policy(Some(edit.clone()), false, window, cx)
                                })),
                        )
                        .child(
                            Button::new(("acl-policy-delete", index))
                                .debug_selector(move || format!("acl-policy-delete-{index}"))
                                .label("Delete entry")
                                .disabled(disabled || policy.read_only)
                                .on_click(cx.listener(move |view, _, window, cx| {
                                    view.open_policy(Some(delete.clone()), true, window, cx)
                                })),
                        ),
                );
                if policy.read_only {
                    element =
                        element.child("Read-only: unknown fields or enum values. The Broker document is preserved.");
                }
            }
        }
        element
    }
}
impl Render for AclView {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let confirmed = self.store.confirmed.is_some();
        let busy = self.mutation_task.is_some();
        let rows = self.store.visible.clone();
        let (error, loading, failures, truncated) = match self.store.tab {
            AclTab::Users => read_state(&self.store.users.state),
            AclTab::Policies => read_state(&self.store.policies.state),
        };
        let search = match self.store.tab {
            AclTab::Users => self.users_search.clone(),
            AclTab::Policies => self.policies_search.clone(),
        };
        let mut content = div()
            .size_full()
            .flex()
            .flex_col()
            .gap_3()
            .p_4()
            .min_w_0()
            .debug_selector(|| "acl-workspace".into())
            .child(
                "Select real Broker inventory and explicitly confirm scope. ACL reads start only after confirmation.",
            )
            .child(
                div()
                    .flex()
                    .flex_wrap()
                    .gap_2()
                    .child(
                        div()
                            .w(px(220.))
                            .child(Select::new(&self.cluster).placeholder("Cluster")),
                    )
                    .child(
                        div()
                            .w(px(240.))
                            .child(Select::new(&self.broker).placeholder("All Brokers in Cluster")),
                    )
                    .child(
                        Button::new("acl-entire-cluster")
                            .debug_selector(|| "acl-entire-cluster".into())
                            .label("Use entire Cluster")
                            .on_click(cx.listener(|view, _, window, cx| {
                                view.store.draft_broker = None;
                                view.broker
                                    .update(cx, |picker, cx| picker.set_selected_index(None, window, cx));
                                view.draft_changed(window, cx);
                            })),
                    )
                    .child(
                        Button::new("acl-inventory")
                            .label("Reload inventory")
                            .on_click(cx.listener(|view, _, window, cx| {
                                view.draft_changed(window, cx);
                                view.load_inventory(window, cx);
                            })),
                    )
                    .child(
                        Button::new("acl-confirm")
                            .debug_selector(|| "acl-confirm".into())
                            .primary()
                            .label("Confirm scope")
                            .on_click(cx.listener(|view, _, window, cx| view.confirm(window, cx))),
                    ),
            )
            .child(
                div()
                    .flex()
                    .flex_wrap()
                    .gap_2()
                    .child(
                        Button::new("acl-users")
                            .debug_selector(|| "acl-users".into())
                            .label("Users")
                            .selected(self.store.tab == AclTab::Users)
                            .on_click(cx.listener(|view, _, window, cx| view.change_tab(AclTab::Users, window, cx))),
                    )
                    .child(
                        Button::new("acl-policies")
                            .debug_selector(|| "acl-policies".into())
                            .label("Policies")
                            .selected(self.store.tab == AclTab::Policies)
                            .on_click(cx.listener(|view, _, window, cx| view.change_tab(AclTab::Policies, window, cx))),
                    ),
            )
            .child(
                div()
                    .flex()
                    .flex_wrap()
                    .gap_2()
                    .child(div().w(px(240.)).child(Input::new(&search)))
                    .child(
                        Button::new("acl-search")
                            .debug_selector(|| "acl-search".into())
                            .label("Search")
                            .on_click(cx.listener(|view, _, _, cx| {
                                let value = match view.store.tab {
                                    AclTab::Users => view.users_search.read(cx).value(),
                                    AclTab::Policies => view.policies_search.read(cx).value(),
                                };
                                view.store.search(&value);
                                cx.notify();
                            })),
                    )
                    .child(
                        Button::new("acl-refresh")
                            .debug_selector(|| "acl-refresh".into())
                            .label("Refresh")
                            .disabled(!confirmed)
                            .on_click(cx.listener(|view, _, _, cx| view.refresh(cx))),
                    )
                    .child(
                        Button::new("acl-create")
                            .debug_selector(|| "acl-create".into())
                            .primary()
                            .label(if self.store.tab == AclTab::Users {
                                "Create user"
                            } else {
                                "Create policy entry"
                            })
                            .disabled(!confirmed || busy)
                            .on_click(cx.listener(|view, _, window, cx| match view.store.tab {
                                AclTab::Users => view.open_user(None, false, window, cx),
                                AclTab::Policies => view.open_policy(None, false, window, cx),
                            })),
                    ),
            );
        if let Loadable::Failed { error, .. } = &self.store.inventory.state {
            content = content.child(error.summary().to_owned());
        }
        if !confirmed {
            content = content.child("ACL scope is not confirmed; no records are loaded.");
        }
        if loading {
            content = content.child("Loading ACL records…");
        }
        if let Some(error) = error {
            content = content.child(error);
        }
        if truncated {
            content = content.child("Results are truncated. Broadened writes still require exact target confirmation.");
        }
        if let Some(status) = &self.status {
            content = content.child(status.clone());
        }
        if !failures.is_empty() || self.write_result.is_some() {
            let mut notices = div().flex().flex_col().gap_1().pb_1();
            for (index, failure) in failures.into_iter().enumerate() {
                notices = notices.child(div().debug_selector(move || format!("acl-read-failure-{index}")).child(
                    format!(
                        "{} @ {}: {}",
                        failure.target.broker_name,
                        failure.target.address,
                        failure.failure.summary()
                    ),
                ));
            }
            if let Some(result) = &self.write_result {
                for (index, target) in result.targets.iter().enumerate() {
                    notices = notices.child(div().debug_selector(move || format!("acl-write-target-{index}")).child(
                        format!(
                            "{} @ {}: {}",
                            target.target.broker_name,
                            target.target.address,
                            match target.state {
                                AclWriteState::Applied => "Applied",
                                AclWriteState::Rejected(failure) => failure.summary(),
                                AclWriteState::Unknown => AclFailure::Unknown.summary(),
                            }
                        ),
                    ));
                }
            }
            content = content.child(
                div()
                    .id("acl-result-notices")
                    .debug_selector(|| "acl-result-notices".into())
                    .w_full()
                    .min_w_0()
                    .flex_shrink_0()
                    .max_h(px(112.))
                    .overflow_y_scroll()
                    .child(notices),
            );
        }
        if self.store.tab == AclTab::Users {
            content = content.child(
                Button::new("acl-show-passwords")
                    .debug_selector(|| "acl-show-passwords".into())
                    .label(if self.show_passwords {
                        "Hide returned passwords"
                    } else {
                        "Show backend-returned passwords"
                    })
                    .disabled(!confirmed)
                    .on_click(cx.listener(|view, _, _, cx| {
                        view.show_passwords = !view.show_passwords;
                        cx.notify();
                    })),
            );
        }
        let mut list = div().id("acl-records").flex_1().min_h_0().overflow_y_scrollbar();
        for (index, row) in rows.iter().enumerate() {
            list = list.child(self.render_row(row, index, cx));
        }
        if rows.is_empty() && confirmed && !loading {
            list = list.child("No matching returned records. Target failures, if any, are shown above.");
        }
        content
            .child(
                div()
                    .debug_selector(|| "acl-records".into())
                    .flex_1()
                    .min_h_0()
                    .child(list),
            )
            .child(
                div()
                    .debug_selector(|| "acl-pagination".into())
                    .flex_shrink_0()
                    .flex()
                    .gap_2()
                    .child(
                        Button::new("acl-prev")
                            .debug_selector(|| "acl-prev".into())
                            .label("Previous")
                            .disabled(self.store.page() == 0)
                            .on_click(cx.listener(|view, _, _, cx| {
                                view.store.move_page(false);
                                cx.notify();
                            })),
                    )
                    .child(format!(
                        "Page {} · {} matches",
                        self.store.page() + 1,
                        self.store.filtered_count
                    ))
                    .child(
                        Button::new("acl-next")
                            .debug_selector(|| "acl-next".into())
                            .label("Next")
                            .disabled((self.store.page() + 1) * ACL_PAGE_SIZE >= self.store.filtered_count)
                            .on_click(cx.listener(|view, _, _, cx| {
                                view.store.move_page(true);
                                cx.notify();
                            })),
                    ),
            )
    }
}
fn read_state<T>(state: &Loadable<AclRead<T>>) -> (Option<String>, bool, Vec<AclTargetFailure>, bool) {
    let error = match state {
        Loadable::Failed { error, .. } => Some(error.summary().into()),
        _ => None,
    };
    let loading = matches!(state, Loadable::InitialLoading | Loadable::Refreshing(_));
    (
        error,
        loading,
        state.value().map_or(Vec::new(), |value| value.failures.clone()),
        state.value().is_some_and(|value| value.truncated),
    )
}

#[cfg(test)]
#[path = "acl_tests.rs"]
mod tests;
