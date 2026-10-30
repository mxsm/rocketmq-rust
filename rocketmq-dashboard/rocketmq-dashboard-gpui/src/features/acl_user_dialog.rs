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

use gpui::{
    App, AppContext as _, Context, Entity, InteractiveElement as _, IntoElement, ParentElement as _, Render,
    Styled as _, Window, div, px,
};
use gpui_component::{
    ActiveTheme as _, Disableable as _, Selectable as _,
    button::Button,
    checkbox::Checkbox,
    input::{Input, InputState},
    scroll::ScrollableElement as _,
};
use rocketmq_dashboard_common::*;

pub struct AclUserDialog {
    pub username: Entity<InputState>,
    pub password: Entity<InputState>,
    pub user_type: String,
    pub status: String,
    pub replace_password: bool,
    pub deleting: bool,
    pub editing: bool,
    pub busy: bool,
    pub attempted: bool,
    pub error: Option<String>,
    pub scope: AclScope,
    pub targets: Vec<BrokerIdentity>,
}
impl AclUserDialog {
    pub fn new(
        scope: AclScope,
        user: Option<AclUser>,
        deleting: bool,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let username = cx.new(|cx| {
            InputState::new(window, cx)
                .default_value(user.as_ref().map_or_else(String::new, |user| user.username.clone()))
        });
        let password = cx.new(|cx| InputState::new(window, cx).masked(true));
        let targets = user
            .as_ref()
            .map_or_else(|| scope.targets().to_vec(), |user| vec![user.target.clone()]);
        Self {
            username,
            password,
            user_type: user
                .as_ref()
                .and_then(|user| user.user_type.clone())
                .unwrap_or_else(|| "Normal".into()),
            status: user
                .as_ref()
                .and_then(|user| user.status.clone())
                .unwrap_or_else(|| "enable".into()),
            replace_password: user.is_none(),
            editing: user.is_some(),
            deleting,
            busy: false,
            attempted: false,
            error: None,
            scope,
            targets,
        }
    }
    /// Replace the old state as well as its Entity, so retained handles cannot undo a cleared secret.
    pub fn clear_secret(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.password
            .update(cx, |input, cx| *input = InputState::new(window, cx).masked(true));
        self.password = cx.new(|cx| InputState::new(window, cx).masked(true));
    }
    pub fn prepare(&self, cx: &App) -> Result<AclCommand, AclValidationError> {
        if self.attempted || self.busy {
            return Err(AclValidationError::Scope);
        }
        let username = self.username.read(cx).value().trim().to_owned();
        let change = if self.deleting {
            AclChange::DeleteUser { username }
        } else {
            AclChange::SaveUser {
                create: !self.editing,
                username,
                password: self
                    .replace_password
                    .then(|| AclSecret::new(self.password.read(cx).value().to_string())),
                user_type: self.user_type.clone(),
                status: self.status.clone(),
            }
        };
        let command = AclCommand {
            scope: self.scope.clone(),
            targets: self.targets.clone(),
            change,
        };
        command.validate()?;
        Ok(command)
    }
}
impl Render for AclUserDialog {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let mut form = div().flex().flex_col().gap_3().min_w_0().child(format!(
            "Confirmed Cluster {} · {} exact Broker target(s)",
            self.scope.cluster(),
            self.targets.len()
        ));
        for target in &self.targets {
            form = form.child(format!(
                "{} / {} / {}",
                target.broker_name, target.broker_id, target.address
            ));
        }
        form = form.child("Username").child(
            div()
                .debug_selector(|| "acl-username".into())
                .child(Input::new(&self.username).disabled(self.editing || self.busy || self.attempted)),
        );
        if self.deleting {
            form = form.child(
                "Delete this user from the listed targets? Authentication and access for this identity are affected.",
            );
        } else {
            if self.editing {
                form = form.child(
                    Checkbox::new("acl-change-password")
                        .debug_selector(|| "acl-change-password".into())
                        .label("Change password")
                        .checked(self.replace_password)
                        .disabled(self.busy || self.attempted)
                        .on_click(cx.listener(|form, checked, window, cx| {
                            form.replace_password = *checked;
                            form.clear_secret(window, cx);
                            cx.notify();
                        })),
                );
            }
            form = form
                .child(if self.replace_password {
                    "Explicit password"
                } else {
                    "Password unchanged"
                })
                .child(
                    div().debug_selector(|| "acl-password".into()).child(
                        Input::new(&self.password)
                            .mask_toggle()
                            .disabled(!self.replace_password || self.busy || self.attempted),
                    ),
                );
            form = form.child("User type");
            let mut types = div().flex().flex_wrap().gap_2();
            for (index, value) in ACL_USER_TYPES.iter().enumerate() {
                types = types.child(
                    Button::new(("acl-user-type", index))
                        .debug_selector(move || format!("acl-user-type-{index}"))
                        .label(*value)
                        .selected(self.user_type == *value)
                        .disabled(self.busy || self.attempted)
                        .on_click(cx.listener(move |form, _, _, cx| {
                            form.user_type = (*value).into();
                            cx.notify();
                        })),
                );
            }
            form = form.child(types).child("User status");
            let mut statuses = div().flex().flex_wrap().gap_2();
            for (index, value) in ACL_USER_STATUSES.iter().enumerate() {
                statuses = statuses.child(
                    Button::new(("acl-user-status", index))
                        .debug_selector(move || format!("acl-user-status-{index}"))
                        .label(*value)
                        .selected(self.status == *value)
                        .disabled(self.busy || self.attempted)
                        .on_click(cx.listener(move |form, _, _, cx| {
                            form.status = (*value).into();
                            cx.notify();
                        })),
                );
            }
            form = form
                .child(statuses)
                .child("Passwords are never prefilled from a user record. No secret is generated by this form.");
        }
        if let Some(error) = &self.error {
            form = form.child(div().text_color(cx.theme().danger).child(error.clone()));
        }
        if self.attempted {
            form = form.child(
                "This attempt cannot be replayed. Close, verify the refreshed Broker state, and open a new form.",
            );
        }
        div()
            .id("acl-user-form")
            .max_h((window.viewport_size().height - px(220.)).max(px(120.)))
            .overflow_y_scrollbar()
            .child(form)
    }
}
