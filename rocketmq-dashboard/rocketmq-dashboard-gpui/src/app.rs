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

//! Root application entity, startup lifecycle, shell composition, and page cache ownership.

use gpui::{
    AppContext as _, Context, Entity, FocusHandle, InteractiveElement as _, IntoElement, ParentElement as _, Render,
    StatefulInteractiveElement as _, Styled as _, Subscription, Task, WeakEntity, Window, div,
    prelude::FluentBuilder as _, px,
};
use gpui_component::{
    ActiveTheme as _, Disableable as _, IconName, Root, StyledExt as _, WindowExt as _,
    button::{Button, ButtonVariants as _},
    input::InputEvent,
    sidebar::{Sidebar, SidebarGroup as GpuiSidebarGroup, SidebarMenu, SidebarMenuItem},
};

use crate::{
    components::{
        app_shell::uses_fixed_sidebar,
        data_table, dialog, page_header, query_toolbar,
        sidebar::{SidebarGroup, SidebarItem, is_active, navigation_groups},
        states, status_badge, toast,
        topbar::{ConnectionSummary, TOPBAR_HEIGHT},
    },
    features::{
        login::LoginForm,
        ops::{OpsIntent, OpsView},
        proxy::ProxyView,
    },
    route::{AppRoute, NavigationHistory},
    services::{
        AppServices, ConfigMutation, ConfigRouteTransition, ConfigUpdatePhase, ConfigUpdated, SessionState,
        StartupSnapshot,
    },
    state::{RequestEpoch, UiError, UiErrorCode},
    ui::{
        cluster_view::ClusterView, consumer_view::ConsumerView, dashboard_view::DashboardView,
        message_view::MessageView, producer_view::ProducerView, topic_view::TopicView,
    },
};

/// The visible startup state. Ready retains the safe destination selected by local startup data.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StartupState {
    /// Startup has an entity-owned task in progress.
    Booting,
    /// Startup selected Login or the protected main shell.
    Ready(ReadyScreen),
    /// Startup has a safe user-facing failure with recovery actions.
    Failed(UiError),
}

impl StartupState {
    /// Returns whether retry is permitted without restarting the application.
    pub const fn can_retry(&self) -> bool {
        matches!(self, Self::Failed(error) if error.is_retryable())
    }
}

/// The two routes allowed immediately after startup.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadyScreen {
    /// A user must authenticate before using the shell.
    Login,
    /// An existing valid session or configuration permits the shell.
    MainShell,
}

impl ReadyScreen {
    const fn from_route(route: &AppRoute) -> Self {
        if matches!(route, AppRoute::Login) {
            Self::Login
        } else {
            Self::MainShell
        }
    }
}

/// A user action delegated through the narrow service boundary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ServiceIntent {
    OpenConfigLocation,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NavigationAction {
    Navigate,
    Back,
    Forward,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum PendingDiscardAction {
    Navigate(AppRoute),
    Back,
    Forward,
    CloseWindow,
}

/// The source of visible page content for the active route.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PageTarget {
    Legacy,
    Placeholder,
}

/// A marker for state that must be dropped on sign out. It never stores feature data in Delivery 01.
#[derive(Default)]
struct SensitiveFeatureCache {
    entries: usize,
}

impl SensitiveFeatureCache {
    fn clear(&mut self) {
        self.entries = 0;
    }

    #[cfg(test)]
    fn with_entries(entries: usize) -> Self {
        Self { entries }
    }
}

/// Legacy content entities constructed once and reused on route changes.
struct LegacyPageCache {
    dashboard: Entity<DashboardView>,
    brokers: Entity<ClusterView>,
    topics: Entity<TopicView>,
    consumers: Entity<ConsumerView>,
    producers: Entity<ProducerView>,
    messages: Entity<MessageView>,
    ops: Entity<OpsView>,
    proxy: Entity<ProxyView>,
    unavailable_table: Entity<gpui_component::table::TableState<data_table::UnavailableTable>>,
}

impl LegacyPageCache {
    fn new(window: &mut Window, services: &AppServices, cx: &mut Context<RocketmqDashboard>) -> Self {
        Self {
            dashboard: cx.new(|_| DashboardView::new()),
            brokers: cx.new(|_| ClusterView::new()),
            topics: cx.new(|_| TopicView::new()),
            consumers: cx.new(|_| ConsumerView::new()),
            producers: cx.new(|_| ProducerView::new()),
            messages: cx.new(|_| MessageView::new()),
            ops: cx.new(|cx| OpsView::new(window, services.clone(), cx)),
            proxy: cx.new(|cx| ProxyView::new(window, services.clone(), cx)),
            unavailable_table: data_table::unavailable_state(
                "No data capability is available for this route yet.",
                window,
                cx,
            ),
        }
    }

    const fn accepts_route(route: &AppRoute) -> bool {
        matches!(
            route,
            AppRoute::Dashboard
                | AppRoute::Brokers
                | AppRoute::Topics
                | AppRoute::Consumers
                | AppRoute::Producers
                | AppRoute::Messages
                | AppRoute::OpsSettings
                | AppRoute::Proxy
        )
    }

    #[cfg(test)]
    fn entity_ids(&self) -> [gpui::EntityId; 9] {
        [
            self.dashboard.entity_id(),
            self.brokers.entity_id(),
            self.topics.entity_id(),
            self.consumers.entity_id(),
            self.producers.entity_id(),
            self.messages.entity_id(),
            self.ops.entity_id(),
            self.proxy.entity_id(),
            self.unavailable_table.entity_id(),
        ]
    }
}

/// A startup attempt captures both freshness dimensions before asynchronous work begins.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct StartupRequest {
    epoch: RequestEpoch,
    configuration_revision: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct LoginRequest {
    epoch: RequestEpoch,
    configuration_revision: u64,
}

/// Root dashboard application state.
///
/// GPUI tasks are retained by this entity; runtime/network/storage work is delegated to the
/// application-owned Delivery 02 services and their child contexts.
pub struct RocketmqDashboard {
    services: AppServices,
    startup_state: StartupState,
    startup_task: Option<Task<()>>,
    session_task: Option<Task<()>>,
    intent_task: Option<Task<()>>,
    subscriptions: Vec<Subscription>,
    startup_epoch: RequestEpoch,
    login_epoch: RequestEpoch,
    configuration_revision: u64,
    history: NavigationHistory,
    session: SessionState,
    login: LoginForm,
    login_security_recovery: bool,
    navigation_trigger_focus: FocusHandle,
    dirty_confirmation_focus: FocusHandle,
    pending_discard: Option<PendingDiscardAction>,
    legacy_pages: LegacyPageCache,
    sensitive_feature_cache: SensitiveFeatureCache,
    last_intent: Option<ServiceIntent>,
    last_service_error: Option<UiError>,
}

impl RocketmqDashboard {
    /// Creates the root in Booting state and starts its owned startup task.
    #[cfg(test)]
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        Self::with_services(window, AppServices::default(), cx)
    }

    /// Creates the root with injectable service seams for focused tests and host integration.
    pub fn with_services(window: &mut Window, services: AppServices, cx: &mut Context<Self>) -> Self {
        let legacy_pages = LegacyPageCache::new(window, &services, cx);
        let login = LoginForm::new(window, cx);
        let password_input = login.password_input();
        let subscriptions = vec![
            cx.subscribe_in(
                &legacy_pages.ops,
                window,
                |this, _, event: &ConfigUpdated, window, cx| {
                    this.handle_config_updated(*event, window, cx);
                },
            ),
            cx.subscribe_in(
                &legacy_pages.proxy,
                window,
                |this, _, event: &ConfigUpdated, window, cx| {
                    this.handle_config_updated(*event, window, cx);
                },
            ),
            cx.subscribe_in(&legacy_pages.ops, window, |this, _, event: &OpsIntent, window, cx| {
                this.handle_ops_intent(*event, window, cx)
            }),
            cx.subscribe_in(&password_input, window, |this, _, event: &InputEvent, window, cx| {
                if matches!(event, InputEvent::PressEnter { .. }) {
                    this.submit_login(window, cx);
                }
            }),
        ];
        let mut dashboard = Self {
            services,
            startup_state: StartupState::Booting,
            startup_task: None,
            session_task: None,
            intent_task: None,
            subscriptions,
            startup_epoch: RequestEpoch::initial(),
            login_epoch: RequestEpoch::initial(),
            configuration_revision: 0,
            history: NavigationHistory::new(AppRoute::Login),
            session: SessionState::signed_out(),
            login,
            login_security_recovery: false,
            navigation_trigger_focus: cx.focus_handle().tab_stop(true),
            dirty_confirmation_focus: cx.focus_handle().tab_stop(false),
            pending_discard: None,
            legacy_pages,
            sensitive_feature_cache: SensitiveFeatureCache::default(),
            last_intent: None,
            last_service_error: None,
        };
        dashboard.start_bootstrap(cx);
        let dashboard_entity = cx.entity().downgrade();
        window.on_window_should_close(cx, move |window, cx| {
            let Some(dashboard) = dashboard_entity.upgrade() else {
                return true;
            };
            let needs_confirmation = {
                let dashboard = dashboard.read(cx);
                dashboard.history.current() == &AppRoute::OpsSettings
                    && dashboard.legacy_pages.ops.read(cx).has_unsaved_transport()
            };
            if !needs_confirmation {
                return true;
            }
            if !window.has_active_dialog(cx) {
                dashboard.update(cx, |dashboard, cx| {
                    dashboard.request_discard_confirmation(PendingDiscardAction::CloseWindow, window, cx);
                });
            }
            false
        });
        dashboard
    }

    fn start_bootstrap(&mut self, cx: &mut Context<Self>) {
        let epoch = match self.startup_epoch.advance() {
            Ok(request) => request,
            Err(_) => {
                self.startup_state = StartupState::Failed(UiError::new(
                    "The dashboard can no longer schedule startup work.",
                    UiErrorCode::Unknown,
                    false,
                ));
                cx.notify();
                return;
            }
        };
        let request = StartupRequest {
            epoch,
            configuration_revision: self.configuration_revision,
        };

        self.startup_state = StartupState::Booting;
        self.last_service_error = None;
        let services = self.services.clone();
        self.startup_task = Some(cx.spawn(async move |this, cx| {
            let result = services.bootstrap().await;
            let _ = this.update(cx, move |dashboard, cx| {
                dashboard.finish_bootstrap(request, result, cx);
            });
        }));
        cx.notify();
    }

    fn finish_bootstrap(
        &mut self,
        request: StartupRequest,
        result: Result<StartupSnapshot, UiError>,
        cx: &mut Context<Self>,
    ) {
        if !accepts_startup_attempt(self.startup_epoch, request, self.configuration_revision) {
            return;
        }

        match result {
            Ok(snapshot) if snapshot.configuration_revision < self.configuration_revision => return,
            Ok(snapshot) => {
                self.configuration_revision = snapshot.configuration_revision;
                self.session = if snapshot.has_valid_session {
                    SessionState::authenticated()
                } else {
                    SessionState::signed_out()
                };
                let route = snapshot.destination();
                self.history.replace(route.clone());
                self.startup_state = StartupState::Ready(ReadyScreen::from_route(&route));
                self.legacy_pages.ops.update(cx, |view, cx| view.sync_from_services(cx));
                self.legacy_pages.ops.update(cx, |view, cx| {
                    view.sync_local_session(self.session.is_authenticated(), cx);
                });
                self.legacy_pages
                    .proxy
                    .update(cx, |view, cx| view.sync_from_services(cx));
            }
            Err(error) => self.startup_state = StartupState::Failed(error),
        }
        cx.notify();
        if std::env::var_os(crate::SMOKE_EXIT_ENV).is_some() {
            cx.quit();
        }
    }

    fn retry_startup(&mut self, cx: &mut Context<Self>) {
        if self.startup_state.can_retry() {
            self.start_bootstrap(cx);
        }
    }

    fn open_config_location(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.last_intent = Some(ServiceIntent::OpenConfigLocation);
        self.last_service_error = None;
        let services = self.services.clone();
        self.intent_task = Some(cx.spawn_in(window, async move |this, cx| {
            let result = services.open_config_location().await;
            let _ = this.update_in(cx, |dashboard, window, cx| {
                dashboard.last_service_error = result.err();
                if let Some(error) = &dashboard.last_service_error {
                    toast::ToastHost::error(error.summary().to_owned(), window, cx);
                }
                cx.notify();
            });
        }));
        cx.notify();
    }

    fn navigate(&mut self, route: AppRoute, window: &mut Window, cx: &mut Context<Self>) {
        if self.request_discard_before_leaving_ops(route.clone(), NavigationAction::Navigate, window, cx) {
            return;
        }
        self.navigate_now(route, window, cx);
    }

    fn navigate_now(&mut self, route: AppRoute, window: &mut Window, cx: &mut Context<Self>) {
        self.history.navigate(route);
        if window.has_active_sheet(cx) {
            window.close_sheet(cx);
        }
        cx.notify();
    }

    fn back(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(target) = self.history.back_target().cloned() else {
            return;
        };
        if self.request_discard_before_leaving_ops(target, NavigationAction::Back, window, cx) {
            return;
        }
        if self.history.back().is_some() {
            cx.notify();
        }
    }

    fn forward(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(target) = self.history.forward_target().cloned() else {
            return;
        };
        if self.request_discard_before_leaving_ops(target, NavigationAction::Forward, window, cx) {
            return;
        }
        if self.history.forward().is_some() {
            cx.notify();
        }
    }

    fn request_discard_before_leaving_ops(
        &mut self,
        target: AppRoute,
        action: NavigationAction,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        if self.history.current() != &AppRoute::OpsSettings
            || target == AppRoute::OpsSettings
            || !self.legacy_pages.ops.read(cx).has_unsaved_transport()
        {
            return false;
        }
        let pending = match action {
            NavigationAction::Navigate => PendingDiscardAction::Navigate(target),
            NavigationAction::Back => PendingDiscardAction::Back,
            NavigationAction::Forward => PendingDiscardAction::Forward,
        };
        self.request_discard_confirmation(pending, window, cx);
        true
    }

    fn request_discard_confirmation(
        &mut self,
        pending: PendingDiscardAction,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.pending_discard = Some(pending);
        self.dirty_confirmation_focus.focus(window);
        self.open_pending_discard_confirmation(window, cx);
    }

    fn open_pending_discard_confirmation(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if window.has_active_dialog(cx) {
            return;
        }
        if !self.legacy_pages.ops.read(cx).has_unsaved_transport() {
            self.pending_discard = None;
            return;
        }
        let Some(pending) = self.pending_discard.clone() else {
            return;
        };
        let close_window = pending == PendingDiscardAction::CloseWindow;
        let dashboard = cx.entity().downgrade();
        dialog::open_confirm(
            "Discard transport draft?",
            if close_window {
                "TLS/VIP changes have not been saved. Discard them and close the dashboard?"
            } else {
                "TLS/VIP changes have not been saved. Discard them before leaving Operations Settings?"
            },
            if close_window { "Discard & close" } else { "Discard" },
            move |_, window, cx| {
                let _ = dashboard.update(cx, |dashboard, cx| {
                    let Some(pending) = dashboard.pending_discard.take() else {
                        return;
                    };
                    dashboard
                        .legacy_pages
                        .ops
                        .update(cx, |ops, cx| ops.discard_unsaved_transport(cx));
                    match pending {
                        PendingDiscardAction::Navigate(target) => dashboard.navigate_now(target, window, cx),
                        PendingDiscardAction::Back => {
                            let _ = dashboard.history.back();
                            cx.notify();
                        }
                        PendingDiscardAction::Forward => {
                            let _ = dashboard.history.forward();
                            cx.notify();
                        }
                        PendingDiscardAction::CloseWindow => window.remove_window(),
                    }
                });
                true
            },
            window,
            cx,
        );
    }

    fn submit_login(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if !self.login.begin_submit(cx) {
            return;
        }
        let epoch = match self.login_epoch.advance() {
            Ok(epoch) => epoch,
            Err(_) => {
                self.login.recover_from_failure(
                    UiError::new(
                        "No further sign-in attempts can be scheduled.",
                        UiErrorCode::Unknown,
                        false,
                    ),
                    window,
                    cx,
                );
                return;
            }
        };
        let request = LoginRequest {
            epoch,
            configuration_revision: self.configuration_revision,
        };
        let credentials = self.login.credentials(cx);
        let username = credentials.username().to_owned();
        let password = credentials.password().to_owned();
        drop(credentials);
        let services = self.services.clone();
        self.session_task = Some(cx.spawn_in(window, async move |this, cx| {
            let result = services.authenticate(&username, &password).await;
            drop(password);
            let _ = this.update_in(cx, |dashboard, window, cx| {
                dashboard.finish_login(request, result, window, cx);
            });
        }));
        cx.notify();
    }

    fn handle_config_updated(&mut self, event: ConfigUpdated, window: &mut Window, cx: &mut Context<Self>) {
        if event.revision < self.configuration_revision {
            return;
        }
        if event.revision > self.configuration_revision {
            let _ = self.login_epoch.advance();
            if self.login.is_submitting() {
                self.login.cancel_submission(window, cx);
            }
        }
        self.configuration_revision = event.revision;
        if event.phase == ConfigUpdatePhase::Invalidated {
            let _ = self.startup_epoch.advance();
            self.sensitive_feature_cache.clear();
            self.pending_discard = None;
            self.login.clear_sensitive(window, cx);
            self.last_service_error = None;
            if window.has_active_sheet(cx) {
                window.close_sheet(cx);
            }
            window.close_all_dialogs(cx);
            window.clear_notifications(cx);
        }
        if matches!(
            event.phase,
            ConfigUpdatePhase::Completed | ConfigUpdatePhase::RolledBack
        ) {
            self.last_service_error = None;
            self.legacy_pages
                .ops
                .update(cx, |view, cx| view.clear_recoverable_error(cx));
            self.legacy_pages
                .proxy
                .update(cx, |view, cx| view.clear_recoverable_error(cx));
            match event.route_transition {
                ConfigRouteTransition::None => {}
                ConfigRouteTransition::AuthenticationDisabled => {
                    self.session.clear();
                    self.login_security_recovery = false;
                    self.legacy_pages
                        .ops
                        .update(cx, |view, cx| view.show_security_recovery(false, cx));
                    self.history.reset(AppRoute::Dashboard);
                    self.startup_state = StartupState::Ready(ReadyScreen::MainShell);
                }
                ConfigRouteTransition::AuthenticationEnabled => {
                    self.session.clear();
                    self.login_security_recovery = false;
                    self.history.reset(AppRoute::Login);
                    self.startup_state = StartupState::Ready(ReadyScreen::Login);
                }
            }
        }
        self.legacy_pages.ops.update(cx, |view, cx| view.sync_from_services(cx));
        self.legacy_pages
            .proxy
            .update(cx, |view, cx| view.sync_from_services(cx));
        cx.notify();
    }

    fn finish_login(
        &mut self,
        request: LoginRequest,
        result: Result<SessionState, UiError>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if !accepts_login_attempt(self.login_epoch, request, self.configuration_revision) {
            return;
        }
        match result {
            Ok(session) if session.is_authenticated() => {
                self.login.clear_sensitive(window, cx);
                self.session = session;
                self.history.replace(AppRoute::Dashboard);
                self.startup_state = StartupState::Ready(ReadyScreen::MainShell);
                self.legacy_pages
                    .ops
                    .update(cx, |view, cx| view.sync_local_session(true, cx));
                cx.notify();
            }
            Ok(_) => self.login.recover_from_failure(
                UiError::new(
                    "Authentication did not create a usable session.",
                    UiErrorCode::Authentication,
                    false,
                ),
                window,
                cx,
            ),
            Err(error) => self.login.recover_from_failure(error, window, cx),
        }
    }

    fn finish_sign_out(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.session.clear();
        self.login.clear_sensitive(window, cx);
        self.sensitive_feature_cache.clear();
        self.rebuild_pages(window, cx);
        self.history.reset(AppRoute::Login);
        self.startup_state = StartupState::Ready(ReadyScreen::Login);
        self.login_security_recovery = false;
        self.last_intent = None;
        self.last_service_error = None;
        cx.notify();
    }

    fn rebuild_pages(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let pages = LegacyPageCache::new(window, &self.services, cx);
        let password_input = self.login.password_input();
        self.subscriptions = vec![
            cx.subscribe_in(&pages.ops, window, |this, _, event: &ConfigUpdated, window, cx| {
                this.handle_config_updated(*event, window, cx);
            }),
            cx.subscribe_in(&pages.proxy, window, |this, _, event: &ConfigUpdated, window, cx| {
                this.handle_config_updated(*event, window, cx);
            }),
            cx.subscribe_in(&pages.ops, window, |this, _, event: &OpsIntent, window, cx| {
                this.handle_ops_intent(*event, window, cx)
            }),
            cx.subscribe_in(&password_input, window, |this, _, event: &InputEvent, window, cx| {
                if matches!(event, InputEvent::PressEnter { .. }) {
                    this.submit_login(window, cx);
                }
            }),
        ];
        self.legacy_pages = pages;
    }

    fn handle_ops_intent(&mut self, event: OpsIntent, window: &mut Window, cx: &mut Context<Self>) {
        match event {
            OpsIntent::SignIn => self.return_to_login(window, cx),
            OpsIntent::SignOut => self.request_sign_out(window, cx),
        }
    }

    fn return_to_login(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let _ = self.login_epoch.advance();
        self.login.clear_sensitive(window, cx);
        self.login_security_recovery = false;
        self.legacy_pages
            .ops
            .update(cx, |view, cx| view.show_security_recovery(false, cx));
        self.history.reset(AppRoute::Login);
        self.startup_state = StartupState::Ready(ReadyScreen::Login);
        cx.notify();
    }

    fn open_login_security(&mut self, cx: &mut Context<Self>) {
        self.login_security_recovery = true;
        self.legacy_pages
            .ops
            .update(cx, |view, cx| view.show_security_recovery(true, cx));
        cx.notify();
    }

    fn close_login_security(&mut self, cx: &mut Context<Self>) {
        self.login_security_recovery = false;
        self.legacy_pages
            .ops
            .update(cx, |view, cx| view.show_security_recovery(false, cx));
        cx.notify();
    }

    fn disable_auth_from_login(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let services = self.services.clone();
        self.session_task = Some(cx.spawn_in(window, async move |this, cx| {
            let (progress, mut updates) = tokio::sync::mpsc::unbounded_channel();
            let mut operation =
                Box::pin(services.mutate_with_progress(ConfigMutation::SetAuthEnabled(false), progress));
            let result = loop {
                tokio::select! {
                    update = updates.recv() => {
                        if let Some(update) = update {
                            let _ = this.update_in(cx, |dashboard, window, cx| {
                                dashboard.handle_config_updated(update, window, cx);
                            });
                        }
                    }
                    result = &mut operation => break result,
                }
            };
            while let Ok(update) = updates.try_recv() {
                let _ = this.update_in(cx, |dashboard, window, cx| {
                    dashboard.handle_config_updated(update, window, cx);
                });
            }
            if let Err(error) = result {
                let _ = this.update_in(cx, |dashboard, window, cx| {
                    dashboard.login.recover_from_failure(error, window, cx);
                });
            }
        }));
        cx.notify();
    }

    fn begin_sign_out(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let services = self.services.clone();
        self.session_task = Some(cx.spawn_in(window, async move |this, cx| {
            let result = services.sign_out().await;
            let _ = this.update_in(cx, |dashboard, window, cx| match result {
                Ok(()) => dashboard.finish_sign_out(window, cx),
                Err(error) => toast::ToastHost::error(error.summary().to_owned(), window, cx),
            });
        }));
        cx.notify();
    }

    #[cfg(test)]
    fn sign_out(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.finish_sign_out(window, cx);
    }

    fn request_sign_out(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let view = cx.entity().downgrade();
        dialog::open_confirm(
            "Sign out",
            "This clears the current session and all local sensitive dashboard state.",
            "Sign out",
            move |_, window, cx| {
                let _ = view.update(cx, |dashboard, cx| dashboard.begin_sign_out(window, cx));
                true
            },
            window,
            cx,
        );
    }

    fn can_sign_out(&self) -> bool {
        self.session.is_authenticated()
    }

    fn current_page_target(&self) -> PageTarget {
        page_target(self.history.current())
    }

    fn open_navigation_drawer(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        // Buttons intentionally avoid taking mouse focus. Set the stable trigger focus first so
        // Root can restore it after the Sheet closes.
        self.navigation_trigger_focus.focus(window);
        let current = self.history.current().clone();
        let view = cx.entity().downgrade();
        window.open_sheet(cx, move |sheet, _window, _cx| {
            sheet
                .title("Navigation")
                .size(px(320.))
                .child(drawer_sidebar(current.clone(), view.clone()))
        });
    }

    fn render_startup_loading(&self, cx: &mut Context<Self>) -> gpui::Div {
        let theme = cx.theme();
        div()
            .size_full()
            .flex()
            .items_center()
            .justify_center()
            .bg(theme.background)
            .child(states::loading_state(theme.foreground, theme.muted_foreground))
    }

    fn render_startup_failure(&self, cx: &mut Context<Self>) -> gpui::Div {
        let theme = cx.theme();
        let error = match &self.startup_state {
            StartupState::Failed(error) => error,
            StartupState::Booting | StartupState::Ready(_) => return self.render_startup_loading(cx),
        };
        let retryable = error.is_retryable();
        let detail = self.last_service_error.as_ref().map_or_else(
            || error.summary().to_owned(),
            |service_error| service_error.summary().to_owned(),
        );

        div()
            .size_full()
            .flex()
            .items_center()
            .justify_center()
            .bg(theme.background)
            .child(states::error_state(
                "Dashboard startup needs attention",
                &detail,
                theme.foreground,
                theme.muted_foreground,
                retryable.then(|| cx.listener(|this, _, _, cx| this.retry_startup(cx))),
                Some(cx.listener(|this, _, window, cx| this.open_config_location(window, cx))),
            ))
    }

    fn render_login(&self, cx: &mut Context<Self>) -> gpui::Div {
        let theme = cx.theme();
        div()
            .size_full()
            .flex()
            .items_center()
            .justify_center()
            .bg(theme.background)
            .child(
                div()
                    .w(px(440.))
                    .max_w_full()
                    .p_6()
                    .rounded(theme.radius_lg)
                    .bg(theme.popover)
                    .border_1()
                    .border_color(theme.border)
                    .flex()
                    .flex_col()
                    .gap_5()
                    .child(page_header::render(
                        "Sign in",
                        "Use the configured local authentication service.",
                        theme.foreground,
                        theme.muted_foreground,
                    ))
                    .child(self.login.render(
                        theme.danger,
                        cx.listener(|this, _, window, cx| this.submit_login(window, cx)),
                    ))
                    .child(
                        div()
                            .flex()
                            .gap_2()
                            .child(
                                Button::new("login-back")
                                    .label("Back")
                                    .ghost()
                                    .on_click(cx.listener(|_, _, window, _| window.remove_window())),
                            )
                            .child(
                                Button::new("login-security")
                                    .label("Security settings")
                                    .ghost()
                                    .on_click(cx.listener(|this, _, _, cx| this.open_login_security(cx))),
                            )
                            .child(
                                Button::new("login-disable-auth")
                                    .label("Disable Auth")
                                    .danger()
                                    .disabled(self.login.is_submitting())
                                    .on_click(cx.listener(|this, _, window, cx| {
                                        this.disable_auth_from_login(window, cx);
                                    })),
                            ),
                    ),
            )
    }

    fn render_login_security(&self, cx: &mut Context<Self>) -> gpui::Div {
        let theme = cx.theme();
        div()
            .size_full()
            .bg(theme.background)
            .flex()
            .flex_col()
            .child(
                div()
                    .px_6()
                    .py_4()
                    .flex()
                    .items_center()
                    .gap_3()
                    .border_b_1()
                    .border_color(theme.border)
                    .child(
                        Button::new("security-back-to-login")
                            .label("Back")
                            .ghost()
                            .on_click(cx.listener(|this, _, _, cx| this.close_login_security(cx))),
                    )
                    .child(page_header::render(
                        "Security recovery",
                        "Review environment-backed authentication without entering the protected shell.",
                        theme.foreground,
                        theme.muted_foreground,
                    )),
            )
            .child(div().flex_1().min_h_0().child(self.legacy_pages.ops.clone()))
    }

    fn render_app_shell(&self, window: &mut Window, cx: &mut Context<Self>) -> gpui::Div {
        let theme = cx.theme();
        let fixed_sidebar = uses_fixed_sidebar(window.viewport_size().width);
        let mut shell = div().size_full().flex().bg(theme.background);
        if fixed_sidebar {
            shell = shell.child(self.render_sidebar(cx));
        }

        shell.child(
            div()
                .flex_1()
                .min_w_0()
                .h_full()
                .flex()
                .flex_col()
                .child(self.render_topbar(!fixed_sidebar, fixed_sidebar, cx))
                .child(self.render_page_content(cx)),
        )
    }

    fn render_sidebar(&self, cx: &mut Context<Self>) -> Sidebar<GpuiSidebarGroup<SidebarMenu>> {
        let current = self.history.current();
        let groups = navigation_groups()
            .into_iter()
            .map(|group| self.render_sidebar_group(group, current, cx))
            .collect::<Vec<_>>();

        Sidebar::left()
            .collapsible(false)
            .header(
                div()
                    .px_4()
                    .py_4()
                    .font_semibold()
                    .text_color(cx.theme().sidebar_foreground)
                    .child("RocketMQ Dashboard"),
            )
            .children(groups)
    }

    fn render_sidebar_group(
        &self,
        group: SidebarGroup,
        current: &AppRoute,
        cx: &mut Context<Self>,
    ) -> GpuiSidebarGroup<SidebarMenu> {
        let menu = group.items.into_iter().fold(SidebarMenu::new(), |menu, item| {
            menu.child(self.render_sidebar_item(item, current, cx))
        });
        GpuiSidebarGroup::new(group.label).child(menu)
    }

    fn render_sidebar_item(&self, item: SidebarItem, current: &AppRoute, cx: &mut Context<Self>) -> SidebarMenuItem {
        let route = item.route.clone();
        SidebarMenuItem::new(item.label)
            .active(is_active(&item, current))
            .icon(item.icon)
            .on_click(cx.listener(move |this, _, window, cx| {
                this.navigate(route.clone(), window, cx);
            }))
    }

    fn render_topbar(&self, show_menu: bool, show_full_status: bool, cx: &mut Context<Self>) -> gpui::Div {
        let theme = cx.theme();
        let current = self.history.current();
        let global_connection = self.services.connection_state();
        let connection = ConnectionSummary::from_state(&global_connection, &self.session);
        let revision_label = format!("Rev {}", connection.revision);
        let compact_label = connection.compact_label();
        let mut topbar = div()
            .debug_selector(|| "topbar".to_owned())
            .h(TOPBAR_HEIGHT)
            .min_h(TOPBAR_HEIGHT)
            .max_h(TOPBAR_HEIGHT)
            .w_full()
            .flex_shrink_0()
            .px_4()
            .flex()
            .flex_nowrap()
            .items_center()
            .gap_2()
            .overflow_hidden()
            .whitespace_nowrap()
            .bg(theme.title_bar)
            .border_b_1()
            .border_color(theme.border)
            .when(show_menu, |this| {
                this.child(
                    div()
                        .id("open-navigation-trigger")
                        .track_focus(&self.navigation_trigger_focus)
                        .on_click(cx.listener(|this, _, window, cx| {
                            this.open_navigation_drawer(window, cx);
                        }))
                        .child(
                            Button::new("open-navigation")
                                .tab_stop(false)
                                .icon(IconName::Menu)
                                .ghost()
                                .tooltip("Open navigation"),
                        ),
                )
            })
            .child(
                div()
                    .debug_selector(|| "topbar-title".to_owned())
                    .flex_1()
                    .min_w_0()
                    .truncate()
                    .font_semibold()
                    .text_color(theme.foreground)
                    .child(current.title()),
            );

        if show_full_status {
            topbar = topbar
                .child(
                    status_badge::render(&connection.nameserver, theme.muted, theme.muted_foreground)
                        .debug_selector(|| "topbar-nameserver".to_owned()),
                )
                .child(
                    status_badge::render(&connection.scope, theme.muted, theme.muted_foreground)
                        .debug_selector(|| "topbar-scope".to_owned()),
                )
                .child(
                    status_badge::render(connection.tls, theme.muted, theme.muted_foreground)
                        .debug_selector(|| "topbar-tls".to_owned()),
                )
                .child(
                    status_badge::render(&revision_label, theme.muted, theme.muted_foreground)
                        .debug_selector(|| "topbar-revision".to_owned()),
                )
                .child(
                    status_badge::render(connection.admin_session_label(), theme.muted, theme.muted_foreground)
                        .debug_selector(|| "topbar-admin".to_owned()),
                )
                .child(
                    status_badge::render(&connection.session, theme.muted, theme.muted_foreground)
                        .debug_selector(|| "topbar-session".to_owned()),
                )
                .child(
                    status_badge::render(connection.health, theme.muted, theme.muted_foreground)
                        .debug_selector(|| "topbar-health".to_owned()),
                );
        } else {
            topbar = topbar.child(
                status_badge::render(&compact_label, theme.muted, theme.muted_foreground)
                    .debug_selector(|| "topbar-compact-status".to_owned()),
            );
        }

        topbar
            .child(
                Button::new("go-back")
                    .icon(IconName::ArrowLeft)
                    .ghost()
                    .tooltip("Back")
                    .on_click(cx.listener(|this, _, window, cx| this.back(window, cx))),
            )
            .child(
                Button::new("go-forward")
                    .icon(IconName::ArrowRight)
                    .ghost()
                    .tooltip("Forward")
                    .on_click(cx.listener(|this, _, window, cx| this.forward(window, cx))),
            )
            .child(
                Button::new("refresh-settings")
                    .icon(IconName::Redo)
                    .ghost()
                    .tooltip("Refresh configuration")
                    .on_click(cx.listener(|this, _, _, cx| this.start_bootstrap(cx))),
            )
            .child(
                Button::new("open-ops-settings")
                    .icon(IconName::Settings)
                    .ghost()
                    .tooltip("Operations settings")
                    .on_click(cx.listener(|this, _, window, cx| {
                        this.navigate(AppRoute::OpsSettings, window, cx);
                    })),
            )
            .when(self.can_sign_out(), |this| {
                this.child(
                    Button::new("sign-out")
                        .icon(IconName::CircleUser)
                        .ghost()
                        .tooltip("Sign out")
                        .on_click(cx.listener(|this, _, window, cx| this.request_sign_out(window, cx))),
                )
            })
    }

    fn render_page_content(&self, cx: &mut Context<Self>) -> gpui::Div {
        let theme = cx.theme();
        let route = self.history.current().clone();
        let uses_legacy_page = self.current_page_target() == PageTarget::Legacy;
        let (description, body) = match &route {
            AppRoute::Dashboard if uses_legacy_page => (
                "Connection-independent dashboard content.",
                div().size_full().child(self.legacy_pages.dashboard.clone()),
            ),
            AppRoute::Brokers if uses_legacy_page => (
                "Legacy read-only broker content is preserved until the broker delivery.",
                div().size_full().child(self.legacy_pages.brokers.clone()),
            ),
            AppRoute::Topics if uses_legacy_page => (
                "Legacy read-only topic content is preserved until the topic delivery.",
                div().size_full().child(self.legacy_pages.topics.clone()),
            ),
            AppRoute::Consumers if uses_legacy_page => (
                "Legacy read-only consumer content is preserved until the consumer delivery.",
                div().size_full().child(self.legacy_pages.consumers.clone()),
            ),
            AppRoute::Producers if uses_legacy_page => (
                "Legacy read-only producer content is preserved until the producer delivery.",
                div().size_full().child(self.legacy_pages.producers.clone()),
            ),
            AppRoute::Messages if uses_legacy_page => (
                "Legacy read-only message content is preserved until the diagnostic delivery.",
                div().size_full().child(self.legacy_pages.messages.clone()),
            ),
            AppRoute::OpsSettings if uses_legacy_page => (
                "Manage NameServers, transport security, sessions, and local storage.",
                div().size_full().child(self.legacy_pages.ops.clone()),
            ),
            AppRoute::Proxy if uses_legacy_page => (
                "Manage Proxy endpoints and the active consumer query scope.",
                div().size_full().child(self.legacy_pages.proxy.clone()),
            ),
            detail_or_future => (
                "This route is intentionally a safe placeholder until its dedicated delivery adds the required capability.",
                div()
                    .size_full()
                    .flex()
                    .flex_col()
                    .gap_4()
                    .p_6()
                    .child(query_toolbar::unavailable(
                        "Query controls are unavailable until this route receives its dedicated capability.",
                        theme.muted,
                        theme.muted_foreground,
                    ))
                    .child(div().flex_1().min_h_0().child(data_table::render(&self.legacy_pages.unavailable_table)))
                    .child(states::empty_state(
                        detail_or_future.title(),
                        "No RocketMQ runtime, provider, network request, or persisted data is available in Delivery 01.",
                        theme.foreground,
                        theme.muted_foreground,
                    )),
            ),
        };

        div()
            .debug_selector(|| "page-content".to_owned())
            .flex_1()
            .min_h_0()
            .overflow_hidden()
            .flex()
            .flex_col()
            .child(div().px_6().pt_5().pb_3().child(page_header::render(
                route.title(),
                description,
                theme.foreground,
                theme.muted_foreground,
            )))
            .child(div().flex_1().min_h_0().overflow_hidden().child(body))
    }
}

const fn page_target(route: &AppRoute) -> PageTarget {
    if LegacyPageCache::accepts_route(route) {
        PageTarget::Legacy
    } else {
        PageTarget::Placeholder
    }
}

impl Render for RocketmqDashboard {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let content = match self.startup_state {
            StartupState::Booting => self.render_startup_loading(cx),
            StartupState::Failed(_) => self.render_startup_failure(cx),
            StartupState::Ready(ReadyScreen::Login) if self.login_security_recovery => self.render_login_security(cx),
            StartupState::Ready(ReadyScreen::Login) => self.render_login(cx),
            StartupState::Ready(ReadyScreen::MainShell) => self.render_app_shell(window, cx),
        };
        let sheet_layer = Root::render_sheet_layer(window, cx);
        let notification_layer = Root::render_notification_layer(window, cx);
        let dialog_layer = Root::render_dialog_layer(window, cx);

        div()
            .relative()
            .size_full()
            .child(
                div()
                    .id("dirty-confirmation-trigger")
                    .absolute()
                    .w(px(0.))
                    .h(px(0.))
                    .overflow_hidden()
                    .track_focus(&self.dirty_confirmation_focus)
                    .on_click(cx.listener(|this, _, window, cx| {
                        this.open_pending_discard_confirmation(window, cx);
                    }))
                    .child(Button::new("dirty-confirmation-reopen").tab_stop(false).ghost()),
            )
            .child(content)
            .children(sheet_layer)
            .children(notification_layer)
            .children(dialog_layer)
    }
}

/// Rejects an attempt captured by an older request or configuration revision before it can match
/// either a success or an error result.
const fn accepts_startup_attempt(
    latest_epoch: RequestEpoch,
    request: StartupRequest,
    current_configuration_revision: u64,
) -> bool {
    latest_epoch.accepts(request.epoch) && request.configuration_revision == current_configuration_revision
}

const fn accepts_login_attempt(
    latest_epoch: RequestEpoch,
    request: LoginRequest,
    current_configuration_revision: u64,
) -> bool {
    latest_epoch.accepts(request.epoch) && request.configuration_revision == current_configuration_revision
}

fn drawer_sidebar(current: AppRoute, view: WeakEntity<RocketmqDashboard>) -> Sidebar<GpuiSidebarGroup<SidebarMenu>> {
    let groups = navigation_groups()
        .into_iter()
        .map(|group| drawer_sidebar_group(group, &current, view.clone()))
        .collect::<Vec<_>>();
    Sidebar::left().collapsible(false).children(groups)
}

fn drawer_sidebar_group(
    group: SidebarGroup,
    current: &AppRoute,
    view: WeakEntity<RocketmqDashboard>,
) -> GpuiSidebarGroup<SidebarMenu> {
    let menu = group.items.into_iter().fold(SidebarMenu::new(), |menu, item| {
        let route = item.route.clone();
        let active = is_active(&item, current);
        let item_view = view.clone();
        menu.child(
            SidebarMenuItem::new(item.label)
                .icon(item.icon)
                .active(active)
                .on_click(move |_, window, cx| {
                    let _ = item_view.update(cx, |dashboard, cx| {
                        dashboard.navigate(route.clone(), window, cx);
                    });
                }),
        )
    });
    GpuiSidebarGroup::new(group.label).child(menu)
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, rc::Rc, sync::Arc};

    use gpui::{AppContext as _, KeyUpEvent, Keystroke, point, px, size};
    use gpui_component::{Root, WindowExt as _, dialog::Dialog, notification::Notification};

    use super::{LegacyPageCache, PageTarget, ReadyScreen, SensitiveFeatureCache, StartupRequest, StartupState};
    use crate::{
        infrastructure::{
            admin_provider::GpuiAdminProvider,
            auth_state::{DesktopAuthState, LOGIN_PASSWORD_ENV, LOGIN_USERNAME_ENV, MapEnvironment},
            client_runtime::DesktopClientRuntime,
            config_store::{AuthConfig, DesktopConfig, DesktopConfigStore},
        },
        route::AppRoute,
        services::{
            AppServices, CapabilityUnavailableConfigService, ConfigRouteTransition, ConfigUpdatePhase, ConfigUpdated,
            FakeAuthService, FakeStartupService, StartupSnapshot,
        },
        state::{RequestEpoch, UiError, UiErrorCode},
    };
    use rocketmq_admin_core::read_client_adapter::TelemetryHandle;
    use rocketmq_runtime::{ProcessMemoryLimit, RuntimeConfig, RuntimeOwner};

    fn services(snapshot: StartupSnapshot, auth: FakeAuthService) -> AppServices {
        AppServices::new(
            Arc::new(FakeStartupService::ready(snapshot)),
            Arc::new(CapabilityUnavailableConfigService),
            Arc::new(auth),
        )
    }

    fn assert_topbar_layout(cx: &mut gpui::VisualTestContext, full_status: bool) {
        let topbar = cx.debug_bounds("topbar").expect("Topbar should be drawn");
        let title = cx.debug_bounds("topbar-title").expect("route title should be drawn");
        let content = cx.debug_bounds("page-content").expect("page content should be drawn");

        assert_eq!(topbar.size.height, super::TOPBAR_HEIGHT);
        assert!(title.origin.y >= topbar.origin.y);
        assert!(title.origin.y + title.size.height <= topbar.origin.y + topbar.size.height);
        assert!(title.origin.x + title.size.width <= topbar.origin.x + topbar.size.width);
        assert!(content.origin.y >= topbar.origin.y + topbar.size.height);

        if full_status {
            for selector in [
                "topbar-nameserver",
                "topbar-scope",
                "topbar-tls",
                "topbar-revision",
                "topbar-admin",
                "topbar-session",
                "topbar-health",
            ] {
                assert!(cx.debug_bounds(selector).is_some(), "missing full status: {selector}");
            }
            assert!(cx.debug_bounds("topbar-compact-status").is_none());
        } else {
            assert!(cx.debug_bounds("topbar-compact-status").is_some());
        }
    }

    #[gpui::test]
    fn topbar_layout_is_fixed_and_complete_at_1440(cx: &mut gpui::TestAppContext) {
        cx.update(gpui_component::init);
        let app_services = services(
            StartupSnapshot {
                configuration_revision: 7,
                login_required: false,
                has_valid_session: false,
            },
            FakeAuthService::authenticated(),
        );
        let (root, cx) = cx.add_window_view(move |window, cx| {
            let dashboard = cx.new(|cx| super::RocketmqDashboard::with_services(window, app_services, cx));
            Root::new(dashboard, window, cx)
        });
        cx.run_until_parked();
        cx.simulate_resize(size(px(1440.), px(900.)));
        cx.draw(point(px(0.), px(0.)), size(px(1440.), px(900.)), |_, _| root.clone());

        assert_topbar_layout(cx, true);
    }

    #[gpui::test]
    fn topbar_layout_is_fixed_and_compact_at_960(cx: &mut gpui::TestAppContext) {
        cx.update(gpui_component::init);
        let app_services = services(
            StartupSnapshot {
                configuration_revision: 7,
                login_required: false,
                has_valid_session: false,
            },
            FakeAuthService::authenticated(),
        );
        let (root, cx) = cx.add_window_view(move |window, cx| {
            let dashboard = cx.new(|cx| super::RocketmqDashboard::with_services(window, app_services, cx));
            Root::new(dashboard, window, cx)
        });
        cx.run_until_parked();
        cx.simulate_resize(size(px(960.), px(900.)));
        cx.draw(point(px(0.), px(0.)), size(px(960.), px(900.)), |_, _| root.clone());

        assert_topbar_layout(cx, false);
    }

    #[test]
    fn startup_decision_selects_login_or_shell_without_session_material() {
        let login = StartupSnapshot {
            configuration_revision: 1,
            login_required: true,
            has_valid_session: false,
        };
        let shell = StartupSnapshot {
            configuration_revision: 2,
            login_required: true,
            has_valid_session: true,
        };

        assert_eq!(login.destination(), AppRoute::Login);
        assert_eq!(ReadyScreen::from_route(&login.destination()), ReadyScreen::Login);
        assert_eq!(shell.destination(), AppRoute::Dashboard);
        assert_eq!(ReadyScreen::from_route(&shell.destination()), ReadyScreen::MainShell);
    }

    #[test]
    fn old_startup_epoch_is_rejected_before_it_can_change_navigation() {
        let mut latest = RequestEpoch::initial();
        let first = latest.advance().expect("first startup epoch is available");
        let second = latest.advance().expect("second startup epoch is available");

        assert!(!latest.accepts(first));
        assert!(latest.accepts(second));
    }

    #[test]
    fn older_configuration_revision_cannot_override_newer_startup_state() {
        let mut latest = RequestEpoch::initial();
        let request = super::StartupRequest {
            epoch: latest.advance().expect("startup epoch is available"),
            configuration_revision: 9,
        };

        assert!(super::accepts_startup_attempt(latest, request, 9));
        assert!(!super::accepts_startup_attempt(latest, request, 10));
    }

    #[test]
    fn login_success_and_failure_share_epoch_and_revision_staleness_rules() {
        let mut latest = RequestEpoch::initial();
        let first = latest.advance().expect("first login epoch");
        let second = latest.advance().expect("second login epoch");

        assert!(!super::accepts_login_attempt(
            latest,
            super::LoginRequest {
                epoch: first,
                configuration_revision: 7,
            },
            7,
        ));
        assert!(!super::accepts_login_attempt(
            latest,
            super::LoginRequest {
                epoch: second,
                configuration_revision: 7,
            },
            8,
        ));
        assert!(super::accepts_login_attempt(
            latest,
            super::LoginRequest {
                epoch: second,
                configuration_revision: 8,
            },
            8,
        ));
    }

    #[test]
    fn startup_errors_only_offer_retry_when_the_error_is_recoverable() {
        let recoverable = StartupState::Failed(UiError::new(
            "Configuration is temporarily unavailable.",
            UiErrorCode::Configuration,
            true,
        ));
        let permanent = StartupState::Failed(UiError::new(
            "Configuration is invalid.",
            UiErrorCode::Configuration,
            false,
        ));

        assert!(recoverable.can_retry());
        assert!(!permanent.can_retry());
    }

    #[gpui::test]
    fn app_startup_uses_its_owned_task_to_choose_the_safe_destination(cx: &mut gpui::TestAppContext) {
        cx.update(gpui_component::init);
        let login_services = services(
            StartupSnapshot {
                configuration_revision: 1,
                login_required: true,
                has_valid_session: false,
            },
            FakeAuthService::authenticated(),
        );
        let (login, cx) =
            cx.add_window_view(move |window, cx| super::RocketmqDashboard::with_services(window, login_services, cx));

        assert_eq!(
            cx.read(|app| login.read(app).startup_state.clone()),
            StartupState::Ready(ReadyScreen::Login)
        );

        let shell_services = services(
            StartupSnapshot {
                configuration_revision: 2,
                login_required: false,
                has_valid_session: false,
            },
            FakeAuthService::authenticated(),
        );
        let (shell, cx) =
            cx.add_window_view(move |window, cx| super::RocketmqDashboard::with_services(window, shell_services, cx));

        assert_eq!(
            cx.read(|app| shell.read(app).startup_state.clone()),
            StartupState::Ready(ReadyScreen::MainShell)
        );
        assert!(!cx.read(|app| shell.read(app).can_sign_out()));
    }

    #[gpui::test]
    fn real_desktop_services_bootstrap_on_the_gpui_executor_without_a_tokio_reactor(cx: &mut gpui::TestAppContext) {
        cx.update(gpui_component::init);
        let directory = tempfile::tempdir().expect("temporary configuration directory");
        let runtime = DesktopClientRuntime::new(TelemetryHandle::noop()).expect("desktop runtime");
        let auth = DesktopAuthState::from_process_environment();
        let store = DesktopConfigStore::new(
            directory.path().join("config.json"),
            runtime.component("test-config-store"),
        );
        let provider = GpuiAdminProvider::new(
            runtime.provider_component("test-admin-provider"),
            runtime.client_runtime(),
            Arc::clone(&auth),
        );
        let (completion_tx, completion_rx) = std::sync::mpsc::channel();
        let services = AppServices::desktop(
            store,
            Arc::clone(&provider),
            auth,
            runtime.component("test-services"),
            runtime.component("test-history"),
            runtime.component("test-monitor"),
        )
        .with_runtime_completion(completion_tx);

        let (dashboard, cx) =
            cx.add_window_view(move |window, cx| super::RocketmqDashboard::with_services(window, services, cx));
        assert_eq!(
            completion_rx
                .recv_timeout(std::time::Duration::from_secs(5))
                .expect("owned bootstrap completion"),
            "gpui-bootstrap"
        );
        cx.run_until_parked();

        assert_eq!(
            cx.read(|app| dashboard.read(app).startup_state.clone()),
            StartupState::Ready(ReadyScreen::MainShell)
        );
        let report = runtime.shutdown(provider).expect("clean desktop runtime shutdown");
        assert_eq!(report.leaked, 0);
        assert_eq!(report.timed_out, 0);
    }

    #[gpui::test]
    fn real_product_app_path_uses_store_fake_provider_enter_login_security_sign_out_and_disable_auth(
        cx: &mut gpui::TestAppContext,
    ) {
        cx.update(gpui_component::init);
        let directory = tempfile::tempdir().expect("temporary configuration directory");
        let config_path = directory.path().join("config.json");
        let config = DesktopConfig {
            auth: AuthConfig {
                enabled: true,
                credential_source: Default::default(),
            },
            ..DesktopConfig::default()
        };
        std::fs::write(
            &config_path,
            serde_json::to_vec_pretty(&config).expect("serialize test config"),
        )
        .expect("write test config");
        let runtime = RuntimeOwner::new_with_memory_limit(
            RuntimeConfig::for_parallelism("gpui-product-app", 1),
            ProcessMemoryLimit::configured(256 * 1024 * 1024).expect("memory limit"),
        )
        .expect("runtime");
        let store = DesktopConfigStore::new(config_path, runtime.root_context().component("config"));
        let auth = DesktopAuthState::new(Arc::new(MapEnvironment::new([
            (LOGIN_USERNAME_ENV, "operator"),
            (LOGIN_PASSWORD_ENV, "sensitive-password"),
        ])));
        assert!(
            auth.authenticate("operator", "sensitive-password")
                .expect("injected auth environment")
                .is_authenticated()
        );
        auth.sign_out();
        let (completion_tx, completion_rx) = std::sync::mpsc::channel();
        let app_services = AppServices::desktop_with_fake_provider(
            store,
            auth,
            runtime.root_context().component("services"),
            runtime.root_context().component("history"),
            runtime.root_context().component("monitor"),
        )
        .with_runtime_completion(completion_tx);
        let dashboard_handle = Rc::new(RefCell::new(None));
        let dashboard_capture = dashboard_handle.clone();
        let (root, cx) = cx.add_window_view(move |window, cx| {
            let dashboard = cx.new(|cx| super::RocketmqDashboard::with_services(window, app_services, cx));
            dashboard_capture.replace(Some(dashboard.clone()));
            Root::new(dashboard, window, cx)
        });
        let dashboard = dashboard_handle.borrow_mut().take().expect("product dashboard entity");
        assert_eq!(
            completion_rx
                .recv_timeout(std::time::Duration::from_secs(5))
                .expect("bootstrap completion"),
            "gpui-bootstrap"
        );
        cx.run_until_parked();
        assert_eq!(
            cx.read(|app| dashboard.read(app).startup_state.clone()),
            StartupState::Ready(ReadyScreen::Login)
        );
        cx.draw(point(px(0.), px(0.)), size(px(1024.), px(720.)), |_, _| root.clone());

        cx.update(|window, app| {
            dashboard.update(app, |dashboard, cx| {
                dashboard.login.set_values("operator", "sensitive-password", window, cx);
                dashboard.login.focus_password(window, cx);
                let (username, password) = dashboard.login.values(cx);
                assert_eq!(username, "operator");
                assert!(!password.is_empty());
            });
        });
        cx.simulate_keystrokes("enter");
        assert_eq!(
            completion_rx
                .recv_timeout(std::time::Duration::from_secs(5))
                .expect("authentication completion"),
            "gpui-authenticate"
        );
        cx.run_until_parked();
        assert_eq!(
            completion_rx.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty),
            "Enter must schedule exactly one authentication"
        );
        cx.update(|_, app| {
            let dashboard = dashboard.read(app);
            assert_eq!(
                dashboard.startup_state,
                StartupState::Ready(ReadyScreen::MainShell),
                "session={:?}, submitting={}, error={:?}",
                dashboard.session,
                dashboard.login.is_submitting(),
                dashboard.login.error_summary()
            );
            assert!(dashboard.session.is_authenticated());
        });

        cx.update(|window, app| {
            dashboard.update(app, |dashboard, cx| {
                dashboard.navigate(AppRoute::OpsSettings, window, cx);
            });
            let ops = dashboard.read(app).legacy_pages.ops.clone();
            ops.update(app, |_ops, cx| {
                cx.emit(crate::features::ops::OpsIntent::SignOut);
            });
            let _ = window;
        });
        cx.run_until_parked();
        cx.update(|window, app| assert!(window.has_active_dialog(app)));
        cx.draw(point(px(0.), px(0.)), size(px(1024.), px(720.)), |_, _| root.clone());
        cx.simulate_keystrokes("enter");
        assert_eq!(
            completion_rx
                .recv_timeout(std::time::Duration::from_secs(5))
                .expect("sign-out completion"),
            "gpui-sign-out"
        );
        cx.run_until_parked();
        cx.update(|window, app| {
            dashboard.update(app, |dashboard, cx| {
                assert_eq!(dashboard.startup_state, StartupState::Ready(ReadyScreen::Login));
                assert!(!dashboard.session.is_authenticated());
                dashboard.open_login_security(cx);
                assert!(dashboard.login_security_recovery);
                dashboard.close_login_security(cx);
                dashboard.disable_auth_from_login(window, cx);
            });
        });
        cx.run_until_parked();
        assert_eq!(
            completion_rx
                .recv_timeout(std::time::Duration::from_secs(5))
                .expect("disable auth completion"),
            "gpui-config-mutation"
        );
        cx.run_until_parked();
        cx.update(|_, app| {
            let dashboard = dashboard.read(app);
            assert_eq!(dashboard.startup_state, StartupState::Ready(ReadyScreen::MainShell));
            assert!(!dashboard.services.connection_state().config.auth.enabled);
        });

        cx.update(|window, app| {
            dashboard.update(app, |dashboard, cx| {
                dashboard.navigate(AppRoute::OpsSettings, window, cx);
                dashboard.legacy_pages.ops.update(cx, |ops, cx| {
                    ops.add_nameserver_for_test("first:9876", window, cx);
                });
            });
        });
        cx.run_until_parked();
        assert_eq!(
            completion_rx
                .recv_timeout(std::time::Duration::from_secs(5))
                .expect("NameServer mutation completion"),
            "gpui-config-mutation"
        );
        cx.run_until_parked();
        cx.update(|_, app| assert_eq!(dashboard.read(app).history.current(), &AppRoute::OpsSettings));

        cx.update(|window, app| {
            let ops = dashboard.read(app).legacy_pages.ops.clone();
            ops.update(app, |ops, cx| ops.save_transport_for_test(window, cx));
        });
        cx.run_until_parked();
        assert_eq!(
            completion_rx
                .recv_timeout(std::time::Duration::from_secs(5))
                .expect("transport mutation completion"),
            "gpui-config-mutation"
        );
        cx.run_until_parked();
        cx.update(|_, app| assert_eq!(dashboard.read(app).history.current(), &AppRoute::OpsSettings));

        cx.update(|window, app| {
            dashboard.update(app, |dashboard, cx| {
                dashboard.navigate(AppRoute::Proxy, window, cx);
                dashboard.legacy_pages.proxy.update(cx, |proxy, cx| {
                    proxy.add_proxy_for_test("first:8080", window, cx);
                });
            });
        });
        cx.run_until_parked();
        assert_eq!(
            completion_rx
                .recv_timeout(std::time::Duration::from_secs(5))
                .expect("Proxy mutation completion"),
            "gpui-config-mutation"
        );
        cx.run_until_parked();
        cx.update(|_, app| {
            dashboard.update(app, |dashboard, _| {
                assert_eq!(dashboard.history.current(), &AppRoute::Proxy);
                assert_eq!(dashboard.history.back(), Some(&AppRoute::OpsSettings));
                assert_eq!(dashboard.history.forward(), Some(&AppRoute::Proxy));
            });
        });
        runtime.shutdown_runtime_blocking().expect("owned runtime shutdown");
    }

    #[gpui::test]
    fn dirty_transport_draft_blocks_window_close_until_explicit_discard(cx: &mut gpui::TestAppContext) {
        cx.update(gpui_component::init);
        let app_services = services(
            StartupSnapshot {
                configuration_revision: 1,
                login_required: false,
                has_valid_session: false,
            },
            FakeAuthService::authenticated(),
        );
        let dashboard_handle = Rc::new(RefCell::new(None));
        let dashboard_capture = dashboard_handle.clone();
        let (_root, cx) = cx.add_window_view(move |window, cx| {
            let dashboard = cx.new(|cx| super::RocketmqDashboard::with_services(window, app_services, cx));
            dashboard_capture.replace(Some(dashboard.clone()));
            Root::new(dashboard, window, cx)
        });
        let dashboard = dashboard_handle
            .borrow_mut()
            .take()
            .expect("dirty draft dashboard entity");
        cx.update(|window, app| {
            dashboard.update(app, |dashboard, cx| {
                dashboard.navigate(AppRoute::OpsSettings, window, cx);
                dashboard
                    .legacy_pages
                    .ops
                    .update(cx, |ops, cx| ops.mark_transport_dirty(cx));
            });
        });

        assert!(!cx.simulate_close());
        cx.update(|window, app| assert!(window.has_active_dialog(app)));
        cx.simulate_keystrokes("escape");
        cx.update(|_, app| {
            let dashboard = dashboard.read(app);
            assert!(dashboard.legacy_pages.ops.read(app).has_unsaved_transport());
        });
        cx.update(|_, app| {
            let ops = dashboard.read(app).legacy_pages.ops.clone();
            ops.update(app, |ops, cx| ops.discard_unsaved_transport(cx));
        });
        assert!(cx.simulate_close());
    }

    #[gpui::test]
    fn invalidation_closes_sensitive_overlays_and_success_preserves_ops_history(cx: &mut gpui::TestAppContext) {
        cx.update(gpui_component::init);
        let app_services = services(
            StartupSnapshot {
                configuration_revision: 1,
                login_required: false,
                has_valid_session: false,
            },
            FakeAuthService::authenticated(),
        );
        let dashboard_handle = Rc::new(RefCell::new(None));
        let dashboard_capture = dashboard_handle.clone();
        let (_root, cx) = cx.add_window_view(move |window, cx| {
            let dashboard = cx.new(|cx| super::RocketmqDashboard::with_services(window, app_services, cx));
            dashboard_capture.replace(Some(dashboard.clone()));
            Root::new(dashboard, window, cx)
        });
        let dashboard = dashboard_handle.borrow_mut().take().expect("dashboard entity");

        cx.update(|window, app| {
            dashboard.update(app, |dashboard, cx| {
                dashboard.navigate(AppRoute::OpsSettings, window, cx);
                dashboard.sensitive_feature_cache = SensitiveFeatureCache::with_entries(3);
                dashboard.legacy_pages.ops.update(cx, |ops, cx| {
                    ops.set_recoverable_error_for_test(
                        UiError::new("Older recoverable OPS error.", UiErrorCode::Connection, true),
                        cx,
                    );
                });
                dashboard.legacy_pages.proxy.update(cx, |proxy, cx| {
                    proxy.set_recoverable_error_for_test(
                        UiError::new("Older recoverable Proxy error.", UiErrorCode::Connection, true),
                        cx,
                    );
                });
            });
            window.open_sheet(app, |sheet, _, _| sheet.title("Scope details"));
            window.open_dialog(app, |dialog: Dialog, _, _| {
                dialog.title("Sensitive confirmation").alert()
            });
            window.push_notification(Notification::info("Old scope result"), app);
            assert!(window.has_active_sheet(app));
            assert!(window.has_active_dialog(app));
            assert!(!window.notifications(app).is_empty());

            dashboard.update(app, |dashboard, cx| {
                dashboard.handle_config_updated(
                    ConfigUpdated {
                        revision: 2,
                        phase: ConfigUpdatePhase::Invalidated,
                        route_transition: ConfigRouteTransition::None,
                    },
                    window,
                    cx,
                );
            });
            assert!(!window.has_active_sheet(app));
            assert!(!window.has_active_dialog(app));
            assert!(window.notifications(app).is_empty());
        });
        cx.update(|window, app| {
            dashboard.update(app, |dashboard, cx| {
                assert_eq!(dashboard.history.current(), &AppRoute::OpsSettings);
                assert_eq!(dashboard.sensitive_feature_cache.entries, 0);
                assert!(dashboard.legacy_pages.ops.read(cx).has_recoverable_error());
                assert!(dashboard.legacy_pages.proxy.read(cx).has_recoverable_error());
                dashboard.handle_config_updated(
                    ConfigUpdated {
                        revision: 2,
                        phase: ConfigUpdatePhase::Completed,
                        route_transition: ConfigRouteTransition::None,
                    },
                    window,
                    cx,
                );
                assert_eq!(dashboard.history.current(), &AppRoute::OpsSettings);
                assert!(!dashboard.legacy_pages.ops.read(cx).has_recoverable_error());
                assert!(!dashboard.legacy_pages.proxy.read(cx).has_recoverable_error());
            });
        });
    }

    #[gpui::test]
    fn dirty_navigation_confirm_restores_stable_focus_and_reopens_with_keyboard(cx: &mut gpui::TestAppContext) {
        cx.update(gpui_component::init);
        let app_services = services(
            StartupSnapshot {
                configuration_revision: 1,
                login_required: false,
                has_valid_session: false,
            },
            FakeAuthService::authenticated(),
        );
        let dashboard_handle = Rc::new(RefCell::new(None));
        let dashboard_capture = dashboard_handle.clone();
        let (root, cx) = cx.add_window_view(move |window, cx| {
            let dashboard = cx.new(|cx| super::RocketmqDashboard::with_services(window, app_services, cx));
            dashboard_capture.replace(Some(dashboard.clone()));
            Root::new(dashboard, window, cx)
        });
        let dashboard = dashboard_handle.borrow_mut().take().expect("dashboard entity");
        cx.simulate_resize(size(px(960.), px(640.)));
        cx.update(|window, app| {
            dashboard.update(app, |dashboard, cx| {
                dashboard.navigate(AppRoute::OpsSettings, window, cx);
                dashboard
                    .legacy_pages
                    .ops
                    .update(cx, |ops, cx| ops.mark_transport_dirty(cx));
                dashboard.navigate(AppRoute::Proxy, window, cx);
            });
            assert!(window.has_active_dialog(app));
        });
        cx.draw(point(px(0.), px(0.)), size(px(960.), px(640.)), |_, _| root.clone());

        cx.simulate_keystrokes("escape");
        cx.update(|window, app| {
            assert!(!window.has_active_dialog(app));
            assert!(dashboard.read(app).dirty_confirmation_focus.is_focused(window));
        });
        cx.draw(point(px(0.), px(0.)), size(px(960.), px(640.)), |_, _| root.clone());
        cx.simulate_event(KeyUpEvent {
            keystroke: Keystroke::parse("enter").expect("valid Enter keystroke"),
        });
        cx.update(|window, app| assert!(window.has_active_dialog(app)));

        cx.simulate_keystrokes("escape");
        cx.draw(point(px(0.), px(0.)), size(px(960.), px(640.)), |_, _| root.clone());
        cx.simulate_event(KeyUpEvent {
            keystroke: Keystroke::parse("space").expect("valid Space keystroke"),
        });
        cx.update(|window, app| assert!(window.has_active_dialog(app)));
        cx.simulate_keystrokes("enter");
        cx.update(|_, app| {
            let dashboard = dashboard.read(app);
            assert_eq!(dashboard.history.current(), &AppRoute::Proxy);
            assert!(dashboard.pending_discard.is_none());
            assert!(!dashboard.legacy_pages.ops.read(app).has_unsaved_transport());
        });
    }

    #[gpui::test]
    fn default_unconfigured_app_never_offers_sign_out_or_an_unusable_login(cx: &mut gpui::TestAppContext) {
        cx.update(gpui_component::init);
        let (dashboard, cx) = cx.add_window_view(super::RocketmqDashboard::new);

        assert_eq!(
            cx.read(|app| dashboard.read(app).startup_state.clone()),
            StartupState::Ready(ReadyScreen::MainShell)
        );
        assert!(!cx.read(|app| dashboard.read(app).can_sign_out()));
        assert_ne!(
            cx.read(|app| dashboard.read(app).history.current().clone()),
            AppRoute::Login
        );
    }

    #[gpui::test]
    fn stale_startup_error_cannot_replace_the_newer_attempt(cx: &mut gpui::TestAppContext) {
        cx.update(gpui_component::init);
        let app_services = services(
            StartupSnapshot {
                configuration_revision: 0,
                login_required: false,
                has_valid_session: false,
            },
            FakeAuthService::authenticated(),
        );
        let dashboard_handle = Rc::new(RefCell::new(None));
        let dashboard_capture = dashboard_handle.clone();
        let (_root, cx) = cx.add_window_view(move |window, cx| {
            let dashboard = cx.new(|cx| super::RocketmqDashboard::with_services(window, app_services, cx));
            dashboard_capture.replace(Some(dashboard.clone()));
            gpui_component::Root::new(dashboard, window, cx)
        });
        let dashboard = dashboard_handle
            .borrow_mut()
            .take()
            .expect("test root retains the dashboard entity");

        cx.update(|window, app| {
            dashboard.update(app, |dashboard, cx| {
                let stale = StartupRequest {
                    epoch: dashboard.startup_epoch,
                    configuration_revision: dashboard.configuration_revision,
                };
                dashboard.start_bootstrap(cx);
                dashboard.finish_bootstrap(
                    stale,
                    Err(UiError::new(
                        "An older startup attempt failed.",
                        UiErrorCode::Configuration,
                        true,
                    )),
                    cx,
                );

                assert!(matches!(dashboard.startup_state, StartupState::Booting));
                assert!(dashboard.startup_task.is_some());
                let stale_revision = StartupRequest {
                    epoch: dashboard.startup_epoch,
                    configuration_revision: dashboard.configuration_revision,
                };
                dashboard.configuration_revision = dashboard.configuration_revision.saturating_add(1);
                dashboard.finish_bootstrap(
                    stale_revision,
                    Err(UiError::new(
                        "A configuration revision superseded this startup error.",
                        UiErrorCode::Configuration,
                        true,
                    )),
                    cx,
                );
                assert!(matches!(dashboard.startup_state, StartupState::Booting));
                let _ = window;
            });
        });
    }

    #[gpui::test]
    fn login_success_clears_the_stable_password_input_before_entering_the_shell(cx: &mut gpui::TestAppContext) {
        cx.update(gpui_component::init);
        let app_services = services(
            StartupSnapshot {
                configuration_revision: 1,
                login_required: true,
                has_valid_session: false,
            },
            FakeAuthService::authenticated(),
        );
        let (dashboard, cx) =
            cx.add_window_view(move |window, cx| super::RocketmqDashboard::with_services(window, app_services, cx));

        let password_entity = cx.update(|window, app| {
            dashboard.update(app, |dashboard, cx| {
                let password_entity = dashboard.login.password_entity_id();
                dashboard.login.set_values("operator", "secret-password", window, cx);

                dashboard.submit_login(window, cx);
                password_entity
            })
        });
        cx.run_until_parked();
        cx.update(|_, app| {
            dashboard.update(app, |dashboard, cx| {
                assert_eq!(dashboard.login.password_entity_id(), password_entity);
                let (username, password) = dashboard.login.values(cx);
                assert_eq!(username, "operator");
                assert!(password.is_empty());
                assert!(dashboard.session.is_authenticated());
                assert_eq!(dashboard.startup_state, StartupState::Ready(ReadyScreen::MainShell));
                assert_eq!(dashboard.history.current(), &AppRoute::Dashboard);
            });
        });
    }

    #[gpui::test]
    fn login_failure_preserves_username_and_clears_password(cx: &mut gpui::TestAppContext) {
        cx.update(gpui_component::init);
        let app_services = services(
            StartupSnapshot {
                configuration_revision: 1,
                login_required: true,
                has_valid_session: false,
            },
            FakeAuthService::failed(UiError::new(
                "Authentication was rejected.",
                UiErrorCode::Authentication,
                false,
            )),
        );
        let dashboard_handle = Rc::new(RefCell::new(None));
        let dashboard_capture = dashboard_handle.clone();
        let (_root, cx) = cx.add_window_view(move |window, cx| {
            let dashboard = cx.new(|cx| super::RocketmqDashboard::with_services(window, app_services, cx));
            dashboard_capture.replace(Some(dashboard.clone()));
            gpui_component::Root::new(dashboard, window, cx)
        });
        let dashboard = dashboard_handle
            .borrow_mut()
            .take()
            .expect("test root retains the dashboard entity");

        cx.update(|window, app| {
            dashboard.update(app, |dashboard, cx| {
                dashboard.login.set_values("operator", "secret-password", window, cx);

                dashboard.submit_login(window, cx);
            });
        });
        cx.run_until_parked();
        cx.update(|_, app| {
            dashboard.update(app, |dashboard, cx| {
                let (username, password) = dashboard.login.values(cx);
                assert_eq!(username, "operator");
                assert!(password.is_empty());
                assert!(!dashboard.session.is_authenticated());
                assert_eq!(dashboard.startup_state, StartupState::Ready(ReadyScreen::Login));
            });
        });
    }

    #[gpui::test]
    fn sign_out_clears_history_sensitive_state_and_rebuilds_page_entities(cx: &mut gpui::TestAppContext) {
        cx.update(gpui_component::init);
        let app_services = services(
            StartupSnapshot {
                configuration_revision: 1,
                login_required: true,
                has_valid_session: true,
            },
            FakeAuthService::authenticated(),
        );
        let (dashboard, cx) =
            cx.add_window_view(move |window, cx| super::RocketmqDashboard::with_services(window, app_services, cx));

        cx.update(|window, app| {
            dashboard.update(app, |dashboard, cx| {
                let old_page_entities = dashboard.legacy_pages.entity_ids();
                let password_entity = dashboard.login.password_entity_id();
                dashboard.history.navigate(AppRoute::Brokers);
                dashboard.history.navigate(AppRoute::Topics);
                dashboard.sensitive_feature_cache = SensitiveFeatureCache::with_entries(2);
                dashboard.login.set_values("operator", "session-secret", window, cx);

                dashboard.sign_out(window, cx);

                assert_eq!(dashboard.login.password_entity_id(), password_entity);
                assert!(dashboard.login.values(cx).1.is_empty());
                assert!(!dashboard.session.is_authenticated());
                assert_eq!(dashboard.sensitive_feature_cache.entries, 0);
                assert_eq!(dashboard.history.current(), &AppRoute::Login);
                assert_eq!(dashboard.history.back(), None);
                assert_eq!(dashboard.history.forward(), None);
                for (old_entity, new_entity) in old_page_entities.into_iter().zip(dashboard.legacy_pages.entity_ids()) {
                    assert_ne!(old_entity, new_entity);
                }
                assert_eq!(dashboard.startup_state, StartupState::Ready(ReadyScreen::Login));
            });
        });
    }

    #[gpui::test]
    fn app_route_mapping_uses_cached_legacy_pages_and_safe_placeholders(cx: &mut gpui::TestAppContext) {
        cx.update(gpui_component::init);
        let app_services = services(
            StartupSnapshot {
                configuration_revision: 1,
                login_required: false,
                has_valid_session: false,
            },
            FakeAuthService::authenticated(),
        );
        let (dashboard, cx) =
            cx.add_window_view(move |window, cx| super::RocketmqDashboard::with_services(window, app_services, cx));

        cx.update(|_, app| {
            dashboard.update(app, |dashboard, cx| {
                assert_eq!(dashboard.current_page_target(), PageTarget::Legacy);
                dashboard.history.navigate(AppRoute::Brokers);
                assert_eq!(dashboard.current_page_target(), PageTarget::Legacy);
                dashboard.history.navigate(AppRoute::OpsSettings);
                assert_eq!(dashboard.current_page_target(), PageTarget::Legacy);
                cx.notify();
            });
        });
    }

    #[gpui::test]
    fn ops_and_proxy_product_pages_render_at_the_960_drawer_breakpoint(cx: &mut gpui::TestAppContext) {
        cx.update(gpui_component::init);
        cx.update(crate::theme::apply_dark_theme);
        let app_services = services(
            StartupSnapshot {
                configuration_revision: 1,
                login_required: false,
                has_valid_session: false,
            },
            FakeAuthService::authenticated(),
        );
        let dashboard_handle = Rc::new(RefCell::new(None));
        let dashboard_capture = dashboard_handle.clone();
        let (root, cx) = cx.add_window_view(move |window, cx| {
            let dashboard = cx.new(|cx| super::RocketmqDashboard::with_services(window, app_services, cx));
            dashboard_capture.replace(Some(dashboard.clone()));
            Root::new(dashboard, window, cx)
        });
        let dashboard = dashboard_handle
            .borrow_mut()
            .take()
            .expect("test root retains the dashboard entity");
        cx.run_until_parked();
        cx.simulate_resize(size(px(960.), px(640.)));

        cx.update(|window, app| {
            dashboard.update(app, |dashboard, cx| {
                dashboard.navigate(AppRoute::OpsSettings, window, cx)
            });
        });
        cx.draw(point(px(0.), px(0.)), size(px(960.), px(640.)), |_, _| root.clone());
        cx.update(|window, app| {
            dashboard.update(app, |dashboard, cx| dashboard.navigate(AppRoute::Proxy, window, cx));
        });
        cx.draw(point(px(0.), px(0.)), size(px(960.), px(640.)), |_, _| root.clone());
        cx.update(|_, app| assert_eq!(dashboard.read(app).current_page_target(), PageTarget::Legacy));
    }

    #[gpui::test]
    fn root_owned_sheet_notification_and_dialog_layers_render_and_handle_input(cx: &mut gpui::TestAppContext) {
        cx.update(gpui_component::init);
        cx.update(crate::theme::apply_dark_theme);
        let dashboard_handle = Rc::new(RefCell::new(None));
        let dashboard_capture = dashboard_handle.clone();
        let (root, cx) = cx.add_window_view(move |window, cx| {
            let dashboard = cx.new(|cx| super::RocketmqDashboard::new(window, cx));
            dashboard_capture.replace(Some(dashboard.clone()));
            Root::new(dashboard, window, cx)
        });
        let _dashboard = dashboard_handle
            .borrow_mut()
            .take()
            .expect("test root retains the dashboard entity");

        cx.update(|window, app| {
            window.open_sheet(app, |sheet, _, _| sheet.title("Navigation"));
            assert!(window.has_active_sheet(app));
        });
        cx.draw(point(px(0.), px(0.)), size(px(1024.), px(640.)), |_, _| root.clone());
        cx.simulate_keystrokes("escape");
        cx.update(|window, app| {
            assert!(!window.has_active_sheet(app));
            window.open_dialog(app, |dialog: Dialog, _, _| dialog.title("Confirm").alert());
            assert!(window.has_active_dialog(app));
        });
        cx.draw(point(px(0.), px(0.)), size(px(1024.), px(640.)), |_, _| root.clone());
        cx.simulate_keystrokes("enter");
        cx.update(|window, app| {
            assert!(!window.has_active_dialog(app));
            window.push_notification(Notification::info("Configuration requires attention."), app);
            assert_eq!(window.notifications(app).len(), 1);
            assert!(Root::render_notification_layer(window, app).is_some());
        });
        cx.draw(point(px(0.), px(0.)), size(px(1024.), px(640.)), |_, _| root.clone());
    }

    #[gpui::test]
    fn navigation_trigger_restores_focus_after_drawer_close_and_reopens_with_keyboard(cx: &mut gpui::TestAppContext) {
        cx.update(gpui_component::init);
        cx.update(crate::theme::apply_dark_theme);
        let dashboard_handle = Rc::new(RefCell::new(None));
        let dashboard_capture = dashboard_handle.clone();
        let (root, cx) = cx.add_window_view(move |window, cx| {
            let dashboard = cx.new(|cx| super::RocketmqDashboard::new(window, cx));
            dashboard_capture.replace(Some(dashboard.clone()));
            Root::new(dashboard, window, cx)
        });
        let dashboard = dashboard_handle
            .borrow_mut()
            .take()
            .expect("test root retains the dashboard entity");

        cx.simulate_resize(size(px(960.), px(640.)));
        cx.draw(point(px(0.), px(0.)), size(px(960.), px(640.)), |_, _| root.clone());
        cx.simulate_click(point(px(32.), px(28.)), gpui::Modifiers::default());
        cx.update(|window, app| assert!(window.has_active_sheet(app)));

        cx.draw(point(px(0.), px(0.)), size(px(960.), px(640.)), |_, _| root.clone());
        cx.simulate_keystrokes("escape");
        cx.update(|window, app| {
            assert!(!window.has_active_sheet(app));
            assert!(dashboard.read(app).navigation_trigger_focus.tab_stop);
            assert!(dashboard.read(app).navigation_trigger_focus.is_focused(window));
        });

        cx.draw(point(px(0.), px(0.)), size(px(960.), px(640.)), |_, _| root.clone());
        cx.simulate_event(KeyUpEvent {
            keystroke: Keystroke::parse("enter").expect("enter is a valid GPUI keystroke"),
        });
        cx.update(|window, app| assert!(window.has_active_sheet(app)));

        cx.draw(point(px(0.), px(0.)), size(px(960.), px(640.)), |_, _| root.clone());
        cx.simulate_keystrokes("escape");
        cx.draw(point(px(0.), px(0.)), size(px(960.), px(640.)), |_, _| root.clone());
        cx.simulate_event(KeyUpEvent {
            keystroke: Keystroke::parse("space").expect("space is a valid GPUI keystroke"),
        });
        cx.update(|window, app| assert!(window.has_active_sheet(app)));
    }

    #[test]
    fn sign_out_cleanup_contract_clears_sensitive_feature_cache() {
        let mut cache = SensitiveFeatureCache::with_entries(3);
        cache.clear();

        assert_eq!(cache.entries, 0);
    }

    #[test]
    fn legacy_cache_maps_only_delivery_one_compatible_routes() {
        let compatible = [
            AppRoute::Dashboard,
            AppRoute::Brokers,
            AppRoute::Topics,
            AppRoute::Consumers,
            AppRoute::Producers,
            AppRoute::Messages,
        ];

        for route in compatible {
            assert!(LegacyPageCache::accepts_route(&route));
        }
        assert!(LegacyPageCache::accepts_route(&AppRoute::OpsSettings));
        assert!(LegacyPageCache::accepts_route(&AppRoute::Proxy));
    }
}
