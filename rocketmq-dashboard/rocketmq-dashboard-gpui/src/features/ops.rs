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

//! OPS Connection, Security, and Storage product workflows.

use std::collections::BTreeMap;

use gpui::{
    AppContext as _, Context, Entity, EventEmitter, IntoElement, ParentElement as _, Render, Styled as _, Task, Window,
    div, prelude::FluentBuilder as _, px,
};
use gpui_component::{
    ActiveTheme as _, Disableable as _, Selectable as _, StyledExt as _,
    button::{Button, ButtonVariants as _},
    input::{Input, InputState},
    scroll::ScrollableElement as _,
};
use rocketmq_dashboard_common::{CredentialSourceKind, EndpointAvailability, EndpointHealth, TransportSettings};

use crate::{
    components::{dialog, states, status_badge, toast},
    infrastructure::{
        auth_state::{
            ADMIN_ACCESS_KEY_ENV, ADMIN_SECRET_KEY_ENV, ADMIN_SECURITY_TOKEN_ENV, LOGIN_PASSWORD_ENV,
            LOGIN_USERNAME_ENV,
        },
        config_store::DesktopConfig,
    },
    services::{AppServices, ConfigMutation, ConfigUpdated},
    state::UiError,
};

/// OPS section selected by the operator.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum OpsSection {
    /// Endpoint and transport management.
    #[default]
    Connection,
    /// Authentication and credential source status.
    Security,
    /// Read-only persistence information.
    Storage,
}

/// Session-flow intents emitted by Security and handled by the root session owner.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OpsIntent {
    /// Reuse the Login flow.
    SignIn,
    /// Clear the in-memory session through the root service flow.
    SignOut,
}

/// Entity-owned OPS state with stable input and async task ownership.
pub struct OpsView {
    services: AppServices,
    endpoint_input: Entity<InputState>,
    section: OpsSection,
    config: DesktopConfig,
    transport_draft: TransportSettings,
    transport_dirty: bool,
    security_only: bool,
    local_authenticated: bool,
    health: BTreeMap<String, EndpointHealth>,
    loaded: bool,
    busy: bool,
    error: Option<UiError>,
    task: Option<Task<()>>,
}

impl EventEmitter<ConfigUpdated> for OpsView {}
impl EventEmitter<OpsIntent> for OpsView {}

impl OpsView {
    /// Creates the page and its stable endpoint editor.
    pub fn new(window: &mut Window, services: AppServices, cx: &mut Context<Self>) -> Self {
        let endpoint_input = cx.new(|cx| InputState::new(window, cx).placeholder("nameserver.example:9876"));
        let config = services.connection_state().config;
        Self {
            services,
            endpoint_input,
            section: OpsSection::Connection,
            transport_draft: config.transport,
            config,
            transport_dirty: false,
            security_only: false,
            local_authenticated: false,
            health: BTreeMap::new(),
            loaded: false,
            busy: false,
            error: None,
            task: None,
        }
    }

    /// Synchronizes after startup or a sibling page update without discarding a dirty transport draft.
    pub fn sync_from_services(&mut self, cx: &mut Context<Self>) {
        let config = self.services.connection_state().config;
        if self.config.revision != config.revision {
            self.health.clear();
        }
        self.config = config;
        if !self.transport_dirty {
            self.transport_draft = self.config.transport;
        }
        self.loaded = true;
        cx.notify();
    }

    /// Synchronizes the root-owned local session marker for Security actions.
    pub fn sync_local_session(&mut self, authenticated: bool, cx: &mut Context<Self>) {
        self.local_authenticated = authenticated;
        cx.notify();
    }

    /// Restricts the view to Security for unauthenticated recovery.
    pub fn show_security_recovery(&mut self, enabled: bool, cx: &mut Context<Self>) {
        self.security_only = enabled;
        if enabled {
            self.section = OpsSection::Security;
        }
        cx.notify();
    }

    /// Clears an obsolete recoverable operation error after a later configuration succeeds.
    pub fn clear_recoverable_error(&mut self, cx: &mut Context<Self>) {
        if self.error.take().is_some() {
            cx.notify();
        }
    }

    fn set_section(&mut self, section: OpsSection, cx: &mut Context<Self>) {
        self.section = section;
        cx.notify();
    }

    fn request_section(&mut self, section: OpsSection, window: &mut Window, cx: &mut Context<Self>) {
        if self.section == section {
            return;
        }
        if !self.transport_dirty || self.section != OpsSection::Connection {
            self.set_section(section, cx);
            return;
        }
        let view = cx.entity().downgrade();
        dialog::open_confirm(
            "Discard transport draft?",
            "TLS/VIP changes have not been saved. Discard them before changing sections?",
            "Discard",
            move |_, _window, cx| {
                let _ = view.update(cx, |this, cx| {
                    this.discard_transport(cx);
                    this.set_section(section, cx);
                });
                true
            },
            window,
            cx,
        );
    }

    /// Returns whether closing or navigating away must ask before discarding transport edits.
    pub const fn has_unsaved_transport(&self) -> bool {
        self.transport_dirty
    }

    /// Discards an unsaved transport draft after an explicit confirmation.
    pub fn discard_unsaved_transport(&mut self, cx: &mut Context<Self>) {
        self.discard_transport(cx);
    }

    #[cfg(test)]
    pub fn mark_transport_dirty(&mut self, cx: &mut Context<Self>) {
        self.transport_draft.use_tls = !self.config.transport.use_tls;
        self.transport_dirty = true;
        cx.notify();
    }

    #[cfg(test)]
    pub fn add_nameserver_for_test(&mut self, address: &str, window: &mut Window, cx: &mut Context<Self>) {
        self.run_mutation(ConfigMutation::AddNameServer(address.into()), false, window, cx);
    }

    #[cfg(test)]
    pub fn save_transport_for_test(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.transport_draft.use_tls = !self.config.transport.use_tls;
        self.transport_dirty = true;
        self.save_transport(window, cx);
    }

    #[cfg(test)]
    pub fn set_recoverable_error_for_test(&mut self, error: UiError, cx: &mut Context<Self>) {
        self.error = Some(error);
        cx.notify();
    }

    #[cfg(test)]
    pub fn has_recoverable_error(&self) -> bool {
        self.error.is_some()
    }

    fn add_nameserver(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let address = self.endpoint_input.read(cx).value().to_string();
        self.run_mutation(ConfigMutation::AddNameServer(address), true, window, cx);
    }

    fn request_switch(&mut self, address: String, window: &mut Window, cx: &mut Context<Self>) {
        let old = self
            .config
            .current_nameserver
            .as_deref()
            .unwrap_or("Not configured")
            .to_owned();
        let description = format!(
            "Switch NameServer from {old} to {address}. In-flight reads become stale; writes are never replayed."
        );
        let view = cx.entity().downgrade();
        dialog::open_confirm(
            "Switch NameServer",
            description,
            "Switch",
            move |_, window, cx| {
                let address = address.clone();
                let _ = view.update(cx, |this, cx| {
                    this.run_mutation(ConfigMutation::SwitchNameServer(address), false, window, cx);
                });
                true
            },
            window,
            cx,
        );
    }

    fn request_remove(
        &mut self,
        address: String,
        replacement: Option<String>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let active = self.config.scope == rocketmq_dashboard_common::ConnectionScope::NameServer
            && self.config.current_nameserver.as_deref() == Some(address.as_str());
        if active && replacement.is_none() {
            self.error = Some(UiError::new(
                "Choose a specific replacement before removing the active NameServer.",
                crate::state::UiErrorCode::Validation,
                false,
            ));
            cx.notify();
            return;
        }
        let impact = replacement.as_deref().map_or_else(
            || format!("Remove NameServer {address}."),
            |next| format!("Remove active NameServer {address} and explicitly replace it with {next}."),
        );
        let view = cx.entity().downgrade();
        dialog::open_confirm(
            "Remove NameServer",
            impact,
            "Remove",
            move |_, window, cx| {
                let address = address.clone();
                let replacement = replacement.clone();
                let _ = view.update(cx, |this, cx| {
                    this.run_mutation(
                        ConfigMutation::RemoveNameServer { address, replacement },
                        false,
                        window,
                        cx,
                    );
                });
                true
            },
            window,
            cx,
        );
    }

    fn toggle_tls(&mut self, cx: &mut Context<Self>) {
        self.transport_draft.use_tls = !self.transport_draft.use_tls;
        self.transport_dirty = true;
        cx.notify();
    }

    fn toggle_vip(&mut self, cx: &mut Context<Self>) {
        self.transport_draft.use_vip_channel = !self.transport_draft.use_vip_channel;
        self.transport_dirty = true;
        cx.notify();
    }

    fn discard_transport(&mut self, cx: &mut Context<Self>) {
        self.transport_draft = self.config.transport;
        self.transport_dirty = false;
        self.error = None;
        cx.notify();
    }

    fn save_transport(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.run_mutation(ConfigMutation::SaveTransport(self.transport_draft), false, window, cx);
    }

    fn reload(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.run_mutation(ConfigMutation::Reload, false, window, cx);
    }

    fn set_auth_enabled(&mut self, enabled: bool, window: &mut Window, cx: &mut Context<Self>) {
        self.run_mutation(ConfigMutation::SetAuthEnabled(enabled), false, window, cx);
    }

    fn set_credential_source(&mut self, source: CredentialSourceKind, window: &mut Window, cx: &mut Context<Self>) {
        self.run_mutation(ConfigMutation::SetCredentialSource(source), false, window, cx);
    }

    fn request_sign_in(&mut self, cx: &mut Context<Self>) {
        cx.emit(OpsIntent::SignIn);
    }

    fn request_sign_out(&mut self, cx: &mut Context<Self>) {
        cx.emit(OpsIntent::SignOut);
    }

    fn run_mutation(
        &mut self,
        mutation: ConfigMutation,
        clear_input: bool,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.busy {
            return;
        }
        self.busy = true;
        self.error = None;
        let services = self.services.clone();
        let request_revision = self.config.revision;
        let transport_save = matches!(mutation, ConfigMutation::SaveTransport(_));
        self.task = Some(cx.spawn_in(window, async move |this, cx| {
            let (progress, mut updates) = tokio::sync::mpsc::unbounded_channel();
            let mut operation = Box::pin(services.mutate_with_progress(mutation, progress));
            let result = loop {
                tokio::select! {
                    update = updates.recv() => {
                        if let Some(update) = update {
                            let _ = this.update_in(cx, |_this, _window, cx| {
                                cx.emit(update);
                                cx.notify();
                            });
                        }
                    }
                    result = &mut operation => break result,
                }
            };
            while let Ok(update) = updates.try_recv() {
                let _ = this.update_in(cx, |_this, _window, cx| {
                    cx.emit(update);
                    cx.notify();
                });
            }
            let current_revision = services.connection_state().config.revision;
            let _ = this.update_in(cx, |this, window, cx| {
                this.busy = false;
                let stale = match &result {
                    Ok(update) => update.config.revision != current_revision,
                    Err(_) => current_revision != request_revision && !transport_save,
                };
                if stale {
                    this.config = this.services.connection_state().config;
                    cx.notify();
                    return;
                }
                match result {
                    Ok(update) => {
                        this.config = update.config;
                        this.transport_draft = this.config.transport;
                        this.transport_dirty = false;
                        this.loaded = true;
                        this.error = update.connection_warning;
                        if clear_input {
                            this.endpoint_input
                                .update(cx, |input, cx| input.set_value("", window, cx));
                        }
                        toast::ToastHost::success("Configuration updated", window, cx);
                    }
                    Err(error) => {
                        // The persisted state may have rolled back, but the attempted draft remains available
                        // for correction and an explicit retry.
                        if transport_save {
                            this.config = this.services.connection_state().config;
                            this.transport_dirty = true;
                        }
                        this.error = Some(error);
                        this.endpoint_input.update(cx, |input, cx| input.focus(window, cx));
                    }
                }
                cx.notify();
            });
        }));
        cx.notify();
    }

    fn check_all(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.busy {
            return;
        }
        self.busy = true;
        self.error = None;
        let services = self.services.clone();
        let request_revision = self.config.revision;
        self.task = Some(cx.spawn_in(window, async move |this, cx| {
            let result = services.check_all_nameservers().await;
            let current_revision = services.connection_state().config.revision;
            let _ = this.update_in(cx, |this, _window, cx| {
                this.busy = false;
                if current_revision != request_revision {
                    cx.notify();
                    return;
                }
                match result {
                    Ok(results) => {
                        this.health = results
                            .into_iter()
                            .map(|health| (health.endpoint.clone(), health))
                            .collect();
                    }
                    Err(error) => this.error = Some(error),
                }
                cx.notify();
            });
        }));
        cx.notify();
    }

    fn render_connection(&self, cx: &mut Context<Self>) -> gpui::Div {
        let theme = cx.theme();
        let rows = self
            .config
            .nameservers
            .iter()
            .cloned()
            .enumerate()
            .map(|(row_id, address)| {
                let selected = self.config.current_nameserver.as_deref() == Some(address.as_str());
                let active = selected;
                let availability = self
                    .health
                    .get(&address)
                    .map(|health| health.availability)
                    .or_else(|| {
                        self.services
                            .connection_state()
                            .health
                            .filter(|health| health.endpoint == address)
                            .map(|health| health.availability)
                    })
                    .unwrap_or_default();
                let status = availability_label(availability);
                let switch_address = address.clone();
                let remove_address = address.clone();
                let mut row = div()
                    .min_h(px(44.))
                    .px_3()
                    .flex()
                    .items_center()
                    .gap_3()
                    .border_b_1()
                    .border_color(theme.border)
                    .child(div().flex_1().min_w_0().child(address.clone()))
                    .when(selected, |row| {
                        row.child(status_badge::render("Current", theme.primary, theme.primary_foreground))
                    })
                    .child(status_badge::render(status, theme.muted, theme.muted_foreground))
                    .when(!active, |row| {
                        row.child(
                            Button::new(("switch-nameserver", row_id))
                                .label("Set current")
                                .ghost()
                                .disabled(self.busy)
                                .on_click(cx.listener(move |this, _, window, cx| {
                                    this.request_switch(switch_address.clone(), window, cx);
                                })),
                        )
                    });
                if active {
                    for (replacement_id, replacement) in self
                        .config
                        .nameservers
                        .iter()
                        .filter(|candidate| candidate.as_str() != address)
                        .cloned()
                        .enumerate()
                    {
                        let address = address.clone();
                        let selected_replacement = replacement.clone();
                        row = row.child(
                            Button::new(gpui::SharedString::from(format!(
                                "replace-remove-nameserver-{row_id}-{replacement_id}"
                            )))
                            .label(format!("Use {replacement} & remove"))
                            .danger()
                            .disabled(self.busy)
                            .on_click(cx.listener(move |this, _, window, cx| {
                                this.request_remove(address.clone(), Some(selected_replacement.clone()), window, cx);
                            })),
                        );
                    }
                    if self.config.nameservers.len() == 1 {
                        row = row.child(
                            Button::new(("remove-last-nameserver", row_id))
                                .label("Replacement required")
                                .danger()
                                .disabled(true),
                        );
                    }
                } else {
                    row = row.child(
                        Button::new(("remove-nameserver", row_id))
                            .label("Remove")
                            .danger()
                            .disabled(self.busy)
                            .on_click(cx.listener(move |this, _, window, cx| {
                                this.request_remove(remove_address.clone(), None, window, cx);
                            })),
                    );
                }
                row
            });
        div()
            .flex()
            .flex_col()
            .gap_4()
            .child(
                div()
                    .flex()
                    .gap_2()
                    .child(div().flex_1().child(Input::new(&self.endpoint_input)))
                    .child(
                        Button::new("add-nameserver")
                            .label("Save endpoint")
                            .primary()
                            .disabled(self.busy)
                            .on_click(cx.listener(|this, _, window, cx| this.add_nameserver(window, cx))),
                    )
                    .child(
                        Button::new("reset-nameserver-draft")
                            .label("Reset")
                            .disabled(self.busy || self.endpoint_input.read(cx).value().is_empty())
                            .on_click(cx.listener(|this, _, window, cx| {
                                this.endpoint_input
                                    .update(cx, |input, cx| input.set_value("", window, cx));
                            })),
                    )
                    .child(
                        Button::new("check-all-nameservers")
                            .label(if self.busy { "Working…" } else { "Check all" })
                            .disabled(self.busy || self.config.nameservers.is_empty())
                            .on_click(cx.listener(|this, _, window, cx| this.check_all(window, cx))),
                    ),
            )
            .when(self.config.nameservers.is_empty(), |view| {
                view.child(states::empty_state(
                    "No NameServer configured",
                    "Add a host:port endpoint to create a real read-only Admin session.",
                    theme.foreground,
                    theme.muted_foreground,
                ))
            })
            .when(!self.config.nameservers.is_empty(), |view| {
                view.child(
                    div()
                        .rounded(theme.radius)
                        .border_1()
                        .border_color(theme.border)
                        .children(rows),
                )
            })
            .child(
                div()
                    .p_4()
                    .rounded(theme.radius)
                    .border_1()
                    .border_color(theme.border)
                    .flex()
                    .flex_col()
                    .gap_3()
                    .child(div().font_semibold().child("Transport draft"))
                    .child(
                        div()
                            .flex()
                            .gap_2()
                            .child(
                                Button::new("toggle-tls")
                                    .label(if self.transport_draft.use_tls {
                                        "TLS on"
                                    } else {
                                        "TLS off"
                                    })
                                    .disabled(self.busy)
                                    .on_click(cx.listener(|this, _, _, cx| this.toggle_tls(cx))),
                            )
                            .child(
                                Button::new("toggle-vip")
                                    .label(if self.transport_draft.use_vip_channel {
                                        "VIP on"
                                    } else {
                                        "VIP off"
                                    })
                                    .disabled(self.busy)
                                    .on_click(cx.listener(|this, _, _, cx| this.toggle_vip(cx))),
                            )
                            .child(
                                Button::new("save-transport")
                                    .label("Save")
                                    .primary()
                                    .disabled(self.busy || !self.transport_dirty)
                                    .on_click(cx.listener(|this, _, window, cx| this.save_transport(window, cx))),
                            )
                            .child(
                                Button::new("discard-transport")
                                    .label("Discard draft")
                                    .ghost()
                                    .disabled(self.busy || !self.transport_dirty)
                                    .on_click(cx.listener(|this, _, _, cx| this.discard_transport(cx))),
                            ),
                    ),
            )
    }

    fn render_security(&self, cx: &mut Context<Self>) -> gpui::Div {
        let theme = cx.theme();
        let enable_auth = !self.config.auth.enabled;
        let next_source = if self.config.auth.credential_source == CredentialSourceKind::Environment {
            CredentialSourceKind::None
        } else {
            CredentialSourceKind::Environment
        };
        div()
            .flex()
            .flex_col()
            .gap_3()
            .child(key_values(
                [
                    (
                        "Auth",
                        if self.config.auth.enabled {
                            "enabled".into()
                        } else {
                            "disabled".into()
                        },
                    ),
                    (
                        "Session",
                        if self.local_authenticated {
                            "authenticated".into()
                        } else {
                            "signed out".into()
                        },
                    ),
                    (
                        "Credential source",
                        credential_label(self.config.auth.credential_source).into(),
                    ),
                    ("Secret persistence", "Never stored".into()),
                    (
                        "Local auth environment",
                        format!("{LOGIN_USERNAME_ENV} / {LOGIN_PASSWORD_ENV}"),
                    ),
                    (
                        "Admin environment",
                        format!(
                            "{ADMIN_ACCESS_KEY_ENV} / {ADMIN_SECRET_KEY_ENV} / {ADMIN_SECURITY_TOKEN_ENV} (optional)"
                        ),
                    ),
                ],
                theme.border,
                theme.muted_foreground,
            ))
            .child(
                div()
                    .flex()
                    .gap_2()
                    .when(self.config.auth.enabled && !self.local_authenticated, |actions| {
                        actions.child(
                            Button::new("security-sign-in")
                                .label("Sign in")
                                .primary()
                                .disabled(self.busy)
                                .on_click(cx.listener(|this, _, _, cx| this.request_sign_in(cx))),
                        )
                    })
                    .when(self.config.auth.enabled && self.local_authenticated, |actions| {
                        actions.child(
                            Button::new("security-sign-out")
                                .label("Sign out")
                                .danger()
                                .disabled(self.busy)
                                .on_click(cx.listener(|this, _, _, cx| this.request_sign_out(cx))),
                        )
                    })
                    .child(
                        Button::new("toggle-local-auth")
                            .label(if enable_auth {
                                "Enable sign-in"
                            } else {
                                "Disable sign-in"
                            })
                            .disabled(self.busy)
                            .on_click(cx.listener(move |this, _, window, cx| {
                                this.set_auth_enabled(enable_auth, window, cx);
                            })),
                    )
                    .child(
                        Button::new("toggle-admin-credential-source")
                            .label(match next_source {
                                CredentialSourceKind::None => "Use no Admin credential",
                                CredentialSourceKind::Environment => "Use environment credentials",
                            })
                            .disabled(self.busy)
                            .on_click(cx.listener(move |this, _, window, cx| {
                                this.set_credential_source(next_source, window, cx);
                            })),
                    ),
            )
    }

    fn render_storage(&self, cx: &mut Context<Self>) -> gpui::Div {
        let theme = cx.theme();
        let path = self
            .services
            .config_path()
            .map_or_else(|| "Unavailable".into(), |path| path.display().to_string());
        div()
            .flex()
            .flex_col()
            .gap_3()
            .child(key_values(
                [
                    ("Configuration file", path),
                    ("Schema version", self.config.schema_version.to_string()),
                    ("Revision", self.config.revision.to_string()),
                    ("History store", "Local JSON foundation (no collection)".into()),
                    ("Monitor store", "Local JSON foundation (no evaluation)".into()),
                    ("Configuration mode", "Versioned atomic file".into()),
                ],
                theme.border,
                theme.muted_foreground,
            ))
            .child(
                Button::new("reload-storage")
                    .label("Reload")
                    .disabled(self.busy)
                    .on_click(cx.listener(|this, _, window, cx| this.reload(window, cx))),
            )
    }
}

impl Render for OpsView {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        if !self.loaded {
            return div()
                .size_full()
                .child(states::loading_state(theme.foreground, theme.muted_foreground));
        }
        div().size_full().child(
            div()
                .size_full()
                .overflow_y_scrollbar()
                .px_6()
                .pb_6()
                .flex()
                .flex_col()
                .gap_4()
                .when(!self.security_only, |view| {
                    view.child(
                        div()
                            .flex()
                            .gap_2()
                            .child(section_button(
                                "ops-connection",
                                "Connection",
                                self.section == OpsSection::Connection,
                                cx.listener(|this, _, window, cx| {
                                    this.request_section(OpsSection::Connection, window, cx);
                                }),
                            ))
                            .child(section_button(
                                "ops-security",
                                "Security",
                                self.section == OpsSection::Security,
                                cx.listener(|this, _, window, cx| {
                                    this.request_section(OpsSection::Security, window, cx);
                                }),
                            ))
                            .child(section_button(
                                "ops-storage",
                                "Storage",
                                self.section == OpsSection::Storage,
                                cx.listener(|this, _, window, cx| {
                                    this.request_section(OpsSection::Storage, window, cx);
                                }),
                            )),
                    )
                })
                .when_some(self.error.as_ref(), |view, error| {
                    view.child(states::error_state(
                        "Operation needs attention",
                        error.summary(),
                        theme.foreground,
                        theme.muted_foreground,
                        error
                            .is_retryable()
                            .then(|| cx.listener(|this, _, window, cx| this.reload(window, cx))),
                        None::<fn(&gpui::ClickEvent, &mut Window, &mut gpui::App)>,
                    ))
                })
                .child(match self.section {
                    OpsSection::Connection => self.render_connection(cx),
                    OpsSection::Security => self.render_security(cx),
                    OpsSection::Storage => self.render_storage(cx),
                }),
        )
    }
}

fn section_button(
    id: &'static str,
    label: &'static str,
    active: bool,
    listener: impl Fn(&gpui::ClickEvent, &mut Window, &mut gpui::App) + 'static,
) -> Button {
    Button::new(id).label(label).selected(active).on_click(listener)
}

fn key_values<const N: usize>(values: [(&'static str, String); N], border: gpui::Hsla, muted: gpui::Hsla) -> gpui::Div {
    div()
        .rounded(px(8.))
        .border_1()
        .border_color(border)
        .children(values.into_iter().map(|(label, value)| {
            div()
                .min_h(px(44.))
                .px_3()
                .flex()
                .items_center()
                .border_b_1()
                .border_color(border)
                .child(div().w(px(180.)).text_color(muted).child(label))
                .child(div().flex_1().min_w_0().child(value))
        }))
}

fn availability_label(availability: EndpointAvailability) -> &'static str {
    match availability {
        EndpointAvailability::Unknown => "Unknown",
        EndpointAvailability::Available => "Available",
        EndpointAvailability::Unavailable => "Unavailable",
    }
}

fn credential_label(kind: CredentialSourceKind) -> &'static str {
    match kind {
        CredentialSourceKind::None => "None",
        CredentialSourceKind::Environment => "Environment",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn labels_do_not_infer_proxy_health_or_secret_presence() {
        assert_eq!(availability_label(EndpointAvailability::Unknown), "Unknown");
        assert_eq!(credential_label(CredentialSourceKind::Environment), "Environment");
    }
}
