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

//! Root ownership of native session changes and focus-time validation.

use super::*;

impl RocketmqDashboard {
    pub(super) fn apply_config_auth_transition(
        &mut self,
        event: ConfigUpdated,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if event.route_transition == ConfigRouteTransition::None
            || !matches!(
                event.phase,
                ConfigUpdatePhase::Persisted
                    | ConfigUpdatePhase::Invalidated
                    | ConfigUpdatePhase::Completed
                    | ConfigUpdatePhase::RolledBack
            )
            || self.applied_auth_transition == Some((event.revision, event.route_transition))
        {
            return;
        }
        // Enforce the persisted policy before provider replacement/reconciliation can wait or
        // fail. Later phase notifications must not discard a login established under it.
        self.applied_auth_transition = Some((event.revision, event.route_transition));
        self.finish_sign_out(window, cx);
        match event.route_transition {
            ConfigRouteTransition::None | ConfigRouteTransition::AuthenticationEnabled => {}
            ConfigRouteTransition::AuthenticationDisabled => {
                self.history.reset(AppRoute::Dashboard);
                self.startup_state = StartupState::Ready(ReadyScreen::MainShell);
            }
        }
        self.sync_message_route(window, cx);
    }

    pub(super) fn session_focus_subscription(window: &mut Window, cx: &mut Context<Self>) -> Subscription {
        cx.observe_window_activation(window, |dashboard, window, cx| {
            if window.is_window_active() {
                dashboard.validate_session_on_focus(window, cx);
            }
        })
    }

    pub(super) fn subscribe_sessions(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(mut changes) = self.services.session_changes() else {
            return;
        };
        let services = self.services.clone();
        self.session_watch_task = Some(cx.spawn_in(window, async move |this, cx| {
            while changes.changed().await.is_ok() {
                let Some(current) = services.current_session() else {
                    break;
                };
                if this
                    .update_in(cx, |dashboard, window, cx| {
                        dashboard.apply_session_change(current, window, cx)
                    })
                    .is_err()
                {
                    break;
                }
            }
        }));
    }

    fn validate_session_on_focus(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if !matches!(self.startup_state, StartupState::Ready(_))
            || self.services.current_session().is_none()
            || self.session_validation_task.is_some()
        {
            return;
        }
        let services = self.services.clone();
        let expected_session = services.current_session();
        self.session_validation_task = Some(cx.spawn_in(window, async move |this, cx| {
            let result = services.protected_access_for(expected_session.clone()).await;
            let current = services.current_session();
            let _ = this.update_in(cx, |dashboard, window, cx| {
                dashboard.session_validation_task = None;
                if dashboard.services.current_session() != expected_session {
                    return;
                }
                if let Some(current) = current {
                    dashboard.apply_session_change(current, window, cx);
                }
                if let Err(error) = result {
                    dashboard.last_service_error = Some(error);
                }
                cx.notify();
            });
        }));
    }

    fn apply_session_change(&mut self, current: SessionState, window: &mut Window, cx: &mut Context<Self>) {
        if self.services.current_session().as_ref() != Some(&current) {
            return;
        }
        if current == self.session {
            return;
        }
        if !current.is_authenticated() && !self.session.is_authenticated() {
            self.session = current;
            return;
        }
        // The login result owns entry into the shell, including its Audit terminal write. A
        // successful service notification must not cancel that same in-flight UI request.
        if current.is_authenticated() && (!self.session.is_authenticated() || self.login.is_submitting()) {
            return;
        }
        let ended = !current.is_authenticated();
        self.finish_sign_out(window, cx);
        self.session = current;
        if ended {
            self.login.recover_from_failure(
                UiError::new(
                    "The session ended or could not be verified. Sign in again.",
                    UiErrorCode::Authentication,
                    true,
                ),
                window,
                cx,
            );
        } else {
            self.history.reset(AppRoute::Dashboard);
            self.startup_state = StartupState::Ready(ReadyScreen::MainShell);
            self.legacy_pages
                .ops
                .update(cx, |view, cx| view.sync_local_session(true, cx));
        }
        cx.notify();
    }
}

#[cfg(test)]
#[path = "sessions_tests.rs"]
mod tests;
