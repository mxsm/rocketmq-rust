// Copyright 2025 The RocketMQ Rust Authors
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

//! Login form state that owns stable GPUI input entities.

use std::fmt;

use gpui::{
    AppContext as _, Context, Entity, IntoElement, ParentElement as _, Styled as _, Window, div,
    prelude::FluentBuilder as _,
};
use gpui_component::{
    Disableable as _,
    button::{Button, ButtonVariants as _},
    form::{Field, Form},
    input::{Input, InputState},
};

use crate::state::UiError;

/// Credentials held only for the duration of an authentication call.
///
/// This type never exposes sensitive contents through `Debug`.
pub struct LoginCredentials {
    username: String,
    password: String,
}

/// The invariant-preserving actions for a rejected Login submission.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct LoginFailurePlan {
    clear_password: bool,
    focus_password: bool,
}

impl LoginFailurePlan {
    const fn rejected_authentication() -> Self {
        Self {
            clear_password: true,
            focus_password: true,
        }
    }
}

impl LoginCredentials {
    fn new(username: String, password: String) -> Self {
        Self { username, password }
    }

    /// Returns the username for the injected authentication service.
    pub fn username(&self) -> &str {
        &self.username
    }

    /// Returns the password only for the injected authentication service call.
    pub fn password(&self) -> &str {
        &self.password
    }
}

impl fmt::Debug for LoginCredentials {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LoginCredentials")
            .field("username_available", &!self.username.is_empty())
            .field("password_available", &!self.password.is_empty())
            .finish()
    }
}

/// Stable input state for the Login route.
///
/// The form intentionally does not derive `Debug`: it owns a password editor and must not create
/// an accidental credential logging path.
pub struct LoginForm {
    username: Entity<InputState>,
    password: Entity<InputState>,
    error: Option<UiError>,
    submitting: bool,
}

impl LoginForm {
    /// Creates the two input entities once per Login route entry.
    pub fn new<T: 'static>(window: &mut Window, cx: &mut Context<T>) -> Self {
        let username = cx.new(|cx| InputState::new(window, cx).multi_line(false).placeholder("Username"));
        let password = cx.new(|cx| {
            InputState::new(window, cx)
                .multi_line(false)
                .placeholder("Password")
                .masked(true)
        });

        Self {
            username,
            password,
            error: None,
            submitting: false,
        }
    }

    /// Returns credentials only while a submit handler is executing.
    pub fn credentials(&self, cx: &gpui::App) -> LoginCredentials {
        let username = self.username.read(cx).value();
        let password = self.password.read(cx).value();
        LoginCredentials::new(
            username.trim_end_matches(['\r', '\n']).to_owned(),
            password.trim_end_matches(['\r', '\n']).to_owned(),
        )
    }

    /// Clears the password and focuses it after an authentication failure.
    pub fn recover_from_failure<T: 'static>(&mut self, error: UiError, window: &mut Window, cx: &mut Context<T>) {
        let plan = LoginFailurePlan::rejected_authentication();
        self.password.update(cx, |password, cx| {
            if plan.clear_password {
                password.set_value("", window, cx);
            }
            if plan.focus_password {
                password.focus(window, cx);
            }
        });
        self.error = Some(error);
        self.submitting = false;
        cx.notify();
    }

    /// Removes the password when the user leaves the Login route or signs out.
    pub fn clear_sensitive<T: 'static>(&mut self, window: &mut Window, cx: &mut Context<T>) {
        self.password
            .update(cx, |password, cx| password.set_value("", window, cx));
        self.error = None;
        self.submitting = false;
        cx.notify();
    }

    /// Marks the form busy and clears an older error.
    pub fn begin_submit<T: 'static>(&mut self, cx: &mut Context<T>) -> bool {
        if self.submitting {
            return false;
        }
        self.submitting = true;
        self.error = None;
        cx.notify();
        true
    }

    /// Returns whether duplicate submission must remain disabled.
    pub const fn is_submitting(&self) -> bool {
        self.submitting
    }

    /// Returns the stable password editor used for Enter-key submission wiring.
    pub fn password_input(&self) -> Entity<InputState> {
        self.password.clone()
    }

    /// Cancels visible submission state after the configuration supersedes a request.
    pub fn cancel_submission<T: 'static>(&mut self, window: &mut Window, cx: &mut Context<T>) {
        self.password
            .update(cx, |password, cx| password.set_value("", window, cx));
        self.submitting = false;
        cx.notify();
    }

    #[cfg(test)]
    pub fn values(&self, cx: &gpui::App) -> (String, String) {
        (
            self.username.read(cx).value().to_string(),
            self.password.read(cx).value().to_string(),
        )
    }

    #[cfg(test)]
    pub fn password_entity_id(&self) -> gpui::EntityId {
        self.password.entity_id()
    }

    #[cfg(test)]
    pub fn error_summary(&self) -> Option<&str> {
        self.error.as_ref().map(UiError::summary)
    }

    #[cfg(test)]
    pub fn set_values<T: 'static>(
        &mut self,
        username: &'static str,
        password: &'static str,
        window: &mut Window,
        cx: &mut Context<T>,
    ) {
        self.username
            .update(cx, |input, cx| input.set_value(username, window, cx));
        self.password
            .update(cx, |input, cx| input.set_value(password, window, cx));
    }

    #[cfg(test)]
    pub fn focus_password<T: 'static>(&self, window: &mut Window, cx: &mut Context<T>) {
        self.password.update(cx, |password, cx| password.focus(window, cx));
    }

    /// Renders the official component form around the two stable editor entities.
    pub fn render(
        &self,
        error_color: gpui::Hsla,
        submit: impl Fn(&gpui::ClickEvent, &mut Window, &mut gpui::App) + 'static,
    ) -> impl IntoElement {
        div()
            .w_full()
            .flex()
            .flex_col()
            .gap_4()
            .child(
                Form::vertical()
                    .child(
                        Field::new()
                            .label("Username")
                            .required(true)
                            .child(Input::new(&self.username)),
                    )
                    .child(
                        Field::new()
                            .label("Password")
                            .required(true)
                            .child(Input::new(&self.password).mask_toggle()),
                    ),
            )
            .when_some(self.error.as_ref(), |this, error| {
                this.child(
                    div()
                        .text_sm()
                        .text_color(error_color)
                        .child(error.summary().to_owned()),
                )
            })
            .child(
                Button::new("login-submit")
                    .label(if self.is_submitting() {
                        "Signing in…"
                    } else {
                        "Sign in"
                    })
                    .primary()
                    .w_full()
                    .disabled(self.is_submitting())
                    .on_click(submit),
            )
    }
}

#[cfg(test)]
mod tests {
    use super::LoginCredentials;

    #[test]
    fn credentials_debug_redacts_username_and_password() {
        let credentials = LoginCredentials::new("operator@example".to_owned(), "secret-password".to_owned());
        let debug = format!("{credentials:?}");

        assert!(!debug.contains("operator@example"));
        assert!(!debug.contains("secret-password"));
        assert!(debug.contains("username_available: true"));
        assert!(debug.contains("password_available: true"));
    }

    #[test]
    fn login_failure_clears_the_password_and_refocuses_its_input() {
        let plan = super::LoginFailurePlan::rejected_authentication();

        assert!(plan.clear_password);
        assert!(plan.focus_password);
    }
}
