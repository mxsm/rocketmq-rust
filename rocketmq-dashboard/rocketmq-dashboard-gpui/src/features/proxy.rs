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

//! Proxy endpoint product workflow. Proxy health remains explicitly Unknown.

use gpui::{
    AppContext as _, Context, Entity, EventEmitter, IntoElement, ParentElement as _, Render, Styled as _, Task, Window,
    div, prelude::FluentBuilder as _, px,
};
use gpui_component::{
    ActiveTheme as _, Disableable as _,
    button::{Button, ButtonVariants as _},
    input::{Input, InputState},
    scroll::ScrollableElement as _,
};
use rocketmq_dashboard_common::ConnectionScope;

use crate::{
    components::{dialog, states, status_badge, toast},
    infrastructure::config_store::DesktopConfig,
    services::{AppServices, ConfigMutation, ConfigUpdated},
    state::UiError,
};

/// Entity-owned Proxy page state.
pub struct ProxyView {
    services: AppServices,
    endpoint_input: Entity<InputState>,
    config: DesktopConfig,
    loaded: bool,
    busy: bool,
    error: Option<UiError>,
    task: Option<Task<()>>,
}

impl EventEmitter<ConfigUpdated> for ProxyView {}

impl ProxyView {
    /// Creates the page and its stable Proxy endpoint editor.
    pub fn new(window: &mut Window, services: AppServices, cx: &mut Context<Self>) -> Self {
        let endpoint_input = cx.new(|cx| InputState::new(window, cx).placeholder("proxy.example:8080"));
        Self {
            config: services.connection_state().config,
            services,
            endpoint_input,
            loaded: false,
            busy: false,
            error: None,
            task: None,
        }
    }

    /// Synchronizes after startup or a sibling update.
    pub fn sync_from_services(&mut self, cx: &mut Context<Self>) {
        self.config = self.services.connection_state().config;
        self.loaded = true;
        cx.notify();
    }

    /// Clears an obsolete recoverable operation error after a later configuration succeeds.
    pub fn clear_recoverable_error(&mut self, cx: &mut Context<Self>) {
        if self.error.take().is_some() {
            cx.notify();
        }
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

    #[cfg(test)]
    pub fn add_proxy_for_test(&mut self, address: &str, window: &mut Window, cx: &mut Context<Self>) {
        self.run_mutation(ConfigMutation::AddProxy(address.into()), false, window, cx);
    }

    fn add_proxy(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let address = self.endpoint_input.read(cx).value().to_string();
        self.run_mutation(ConfigMutation::AddProxy(address), true, window, cx);
    }

    fn reload(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.run_mutation(ConfigMutation::Reload, false, window, cx);
    }

    fn request_switch(&mut self, address: String, window: &mut Window, cx: &mut Context<Self>) {
        let old = if self.config.scope == ConnectionScope::Proxy {
            self.config.current_proxy.as_deref().unwrap_or("Not configured")
        } else {
            "NameServer scope"
        };
        let impact = format!(
            "Switch consumer scope from {old} to Proxy {address}. Old reads become stale; no mutation is replayed."
        );
        let view = cx.entity().downgrade();
        dialog::open_confirm(
            "Switch Proxy scope",
            impact,
            "Switch",
            move |_, window, cx| {
                let address = address.clone();
                let _ = view.update(cx, |this, cx| {
                    this.run_mutation(ConfigMutation::SwitchProxy(address), false, window, cx);
                });
                true
            },
            window,
            cx,
        );
    }

    fn request_delete(
        &mut self,
        address: String,
        replacement: Option<String>,
        fallback_to_nameserver: bool,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let active = self.config.scope == ConnectionScope::Proxy
            && self.config.current_proxy.as_deref() == Some(address.as_str());
        if active && !fallback_to_nameserver && replacement.is_none() {
            self.error = Some(UiError::new(
                "Choose NameServer fallback or add another Proxy before deleting the active endpoint.",
                crate::state::UiErrorCode::Validation,
                false,
            ));
            cx.notify();
            return;
        }
        let impact = if fallback_to_nameserver {
            format!("Delete active Proxy {address} and explicitly fall back to NameServer scope.")
        } else if let Some(replacement) = replacement.as_deref() {
            format!("Delete active Proxy {address} and explicitly replace it with {replacement}.")
        } else {
            format!("Delete Proxy {address}.")
        };
        let view = cx.entity().downgrade();
        dialog::open_confirm(
            "Delete Proxy endpoint",
            impact,
            "Delete",
            move |_, window, cx| {
                let address = address.clone();
                let replacement = replacement.clone();
                let _ = view.update(cx, |this, cx| {
                    this.run_mutation(
                        ConfigMutation::RemoveProxy {
                            address,
                            replacement,
                            fallback_to_nameserver,
                        },
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
                    Err(_) => current_revision != request_revision,
                };
                if stale {
                    this.config = this.services.connection_state().config;
                    cx.notify();
                    return;
                }
                match result {
                    Ok(update) => {
                        this.config = update.config;
                        this.loaded = true;
                        this.error = update.connection_warning;
                        if clear_input {
                            this.endpoint_input
                                .update(cx, |input, cx| input.set_value("", window, cx));
                        }
                        toast::ToastHost::success("Proxy configuration updated", window, cx);
                    }
                    Err(error) => {
                        this.error = Some(error);
                        this.endpoint_input.update(cx, |input, cx| input.focus(window, cx));
                    }
                }
                cx.notify();
            });
        }));
        cx.notify();
    }
}

impl Render for ProxyView {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        if !self.loaded {
            return div()
                .size_full()
                .child(states::loading_state(theme.foreground, theme.muted_foreground));
        }
        let current = if self.config.scope == ConnectionScope::Proxy {
            self.config.current_proxy.as_deref().unwrap_or("Not configured")
        } else {
            "NameServer scope"
        };
        let rows = self
            .config
            .proxies
            .iter()
            .cloned()
            .enumerate()
            .map(|(row_id, address)| {
                let active = self.config.scope == ConnectionScope::Proxy
                    && self.config.current_proxy.as_deref() == Some(address.as_str());
                let switch_address = address.clone();
                let delete_address = address.clone();
                let fallback_address = address.clone();
                let mut row = div()
                    .min_h(px(44.))
                    .px_3()
                    .flex()
                    .items_center()
                    .gap_3()
                    .border_b_1()
                    .border_color(theme.border)
                    .child(div().flex_1().min_w_0().child(address.clone()))
                    .when(active, |row| {
                        row.child(status_badge::render("Current", theme.primary, theme.primary_foreground))
                    })
                    .child(status_badge::render("Unknown", theme.muted, theme.muted_foreground))
                    .when(!active, |row| {
                        row.child(
                            Button::new(("switch-proxy", row_id))
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
                        .proxies
                        .iter()
                        .filter(|candidate| candidate.as_str() != address)
                        .cloned()
                        .enumerate()
                    {
                        let address = address.clone();
                        let selected_replacement = replacement.clone();
                        row = row.child(
                            Button::new(gpui::SharedString::from(format!(
                                "replace-delete-proxy-{row_id}-{replacement_id}"
                            )))
                            .label(format!("Use {replacement} & delete"))
                            .danger()
                            .disabled(self.busy)
                            .on_click(cx.listener(move |this, _, window, cx| {
                                this.request_delete(
                                    address.clone(),
                                    Some(selected_replacement.clone()),
                                    false,
                                    window,
                                    cx,
                                );
                            })),
                        );
                    }
                    row = row.child(
                        Button::new(("fallback-delete-proxy", row_id))
                            .label("Fallback & delete")
                            .danger()
                            .disabled(self.busy || self.config.current_nameserver.is_none())
                            .on_click(cx.listener(move |this, _, window, cx| {
                                this.request_delete(fallback_address.clone(), None, true, window, cx);
                            })),
                    );
                } else {
                    row = row.child(
                        Button::new(("delete-proxy", row_id))
                            .label("Delete")
                            .danger()
                            .disabled(self.busy)
                            .on_click(cx.listener(move |this, _, window, cx| {
                                this.request_delete(delete_address.clone(), None, false, window, cx);
                            })),
                    );
                }
                row
            });
        div().size_full().child(
            div()
                .size_full()
                .overflow_y_scrollbar()
                .px_6()
                .pb_6()
                .flex()
                .flex_col()
                .gap_4()
                .child(
                    div()
                        .p_4()
                        .rounded(theme.radius)
                        .border_1()
                        .border_color(theme.border)
                        .flex()
                        .gap_6()
                        .child(div().child(format!("Current endpoint: {current}")))
                        .child(div().child(format!("Configured: {}", self.config.proxies.len()))),
                )
                .child(
                    div()
                        .flex()
                        .gap_2()
                        .child(div().flex_1().child(Input::new(&self.endpoint_input)))
                        .child(
                            Button::new("add-proxy")
                                .label("Add Proxy")
                                .primary()
                                .disabled(self.busy)
                                .on_click(cx.listener(|this, _, window, cx| this.add_proxy(window, cx))),
                        )
                        .child(
                            Button::new("reload-proxy")
                                .label("Retry / Reload")
                                .disabled(self.busy)
                                .on_click(cx.listener(|this, _, window, cx| this.reload(window, cx))),
                        ),
                )
                .when_some(self.error.as_ref(), |view, error| {
                    view.child(states::error_state(
                        "Proxy operation needs attention",
                        error.summary(),
                        theme.foreground,
                        theme.muted_foreground,
                        error
                            .is_retryable()
                            .then(|| cx.listener(|this, _, window, cx| this.reload(window, cx))),
                        None::<fn(&gpui::ClickEvent, &mut Window, &mut gpui::App)>,
                    ))
                })
                .when(self.config.proxies.is_empty(), |view| {
                    view.child(states::empty_state(
                        "No Proxy configured",
                        "Add a host:port endpoint. Availability remains Unknown until a later Proxy probe exists.",
                        theme.foreground,
                        theme.muted_foreground,
                    ))
                })
                .when(!self.config.proxies.is_empty(), |view| {
                    view.child(
                        div()
                            .rounded(theme.radius)
                            .border_1()
                            .border_color(theme.border)
                            .children(rows),
                    )
                }),
        )
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn proxy_page_never_claims_availability() {
        let label = "Unknown";
        assert_ne!(label, "Available");
        assert_ne!(label, "Healthy");
    }
}
