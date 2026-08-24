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

#[path = "app/consumers.rs"]
mod consumers;
#[path = "app/delivery03.rs"]
mod delivery03;
#[path = "app/topics.rs"]
mod topics;

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
        brokers::BrokersView,
        consumers::ConsumersView,
        dashboard::DashboardView,
        login::LoginForm,
        ops::{OpsIntent, OpsView},
        producers::ProducersView,
        proxy::ProxyView,
        topics::TopicsView,
    },
    route::{AppRoute, NavigationHistory},
    services::{
        AppServices, ConfigMutation, ConfigRouteTransition, ConfigUpdatePhase, ConfigUpdated, SessionState,
        StartupSnapshot,
    },
    state::{RequestEpoch, UiError, UiErrorCode},
    ui::message_view::MessageView,
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
    brokers: Entity<BrokersView>,
    topics: Entity<TopicsView>,
    consumers: Entity<ConsumersView>,
    producers: Entity<ProducersView>,
    messages: Entity<MessageView>,
    ops: Entity<OpsView>,
    proxy: Entity<ProxyView>,
    unavailable_table: Entity<gpui_component::table::TableState<data_table::UnavailableTable>>,
}

impl LegacyPageCache {
    fn new(window: &mut Window, services: &AppServices, cx: &mut Context<RocketmqDashboard>) -> Self {
        Self {
            dashboard: cx.new(|cx| DashboardView::new(services.clone(), 0, cx)),
            brokers: cx.new(|cx| BrokersView::new(window, services.clone(), 0, cx)),
            topics: cx.new(|cx| TopicsView::new(window, services.clone(), 0, cx)),
            consumers: cx.new(|cx| ConsumersView::new(window, services.clone(), 0, cx)),
            producers: cx.new(|cx| ProducersView::new(window, services.clone(), 0, cx)),
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
                | AppRoute::BrokerDetail { .. }
                | AppRoute::Topics
                | AppRoute::TopicDetail { .. }
                | AppRoute::Consumers
                | AppRoute::ConsumerDetail { .. }
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
        let mut subscriptions = vec![
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
        subscriptions.extend(Self::delivery03_subscriptions(&legacy_pages, window, cx));
        subscriptions.extend(Self::topic_subscriptions(&legacy_pages, window, cx));
        subscriptions.extend(Self::consumer_subscriptions(&legacy_pages, window, cx));
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
                self.legacy_pages
                    .dashboard
                    .update(cx, |view, cx| view.set_revision(snapshot.configuration_revision, cx));
                self.legacy_pages
                    .brokers
                    .update(cx, |view, cx| view.set_revision(snapshot.configuration_revision, cx));
                self.legacy_pages
                    .topics
                    .update(cx, |view, cx| view.set_revision(snapshot.configuration_revision, cx));
                self.legacy_pages
                    .consumers
                    .update(cx, |view, cx| view.set_revision(snapshot.configuration_revision, cx));
                self.legacy_pages
                    .producers
                    .update(cx, |view, cx| view.set_revision(snapshot.configuration_revision, cx));
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
        self.sync_broker_route(window, cx);
        self.sync_topic_route(window, cx);
        self.sync_consumer_route(window, cx);
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
            self.sync_broker_route(window, cx);
            self.sync_topic_route(window, cx);
            self.sync_consumer_route(window, cx);
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
            self.sync_broker_route(window, cx);
            self.sync_topic_route(window, cx);
            self.sync_consumer_route(window, cx);
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
                            dashboard.sync_broker_route(window, cx);
                            dashboard.sync_topic_route(window, cx);
                            dashboard.sync_consumer_route(window, cx);
                            cx.notify();
                        }
                        PendingDiscardAction::Forward => {
                            let _ = dashboard.history.forward();
                            dashboard.sync_broker_route(window, cx);
                            dashboard.sync_topic_route(window, cx);
                            dashboard.sync_consumer_route(window, cx);
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
            self.legacy_pages
                .dashboard
                .update(cx, |view, cx| view.set_revision(event.revision, cx));
            self.legacy_pages
                .brokers
                .update(cx, |view, cx| view.set_revision(event.revision, cx));
            self.legacy_pages
                .topics
                .update(cx, |view, cx| view.set_revision(event.revision, cx));
            self.legacy_pages
                .consumers
                .update(cx, |view, cx| view.set_revision(event.revision, cx));
            self.legacy_pages
                .producers
                .update(cx, |view, cx| view.set_revision(event.revision, cx));
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
        let mut subscriptions = vec![
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
        subscriptions.extend(Self::delivery03_subscriptions(&pages, window, cx));
        subscriptions.extend(Self::topic_subscriptions(&pages, window, cx));
        subscriptions.extend(Self::consumer_subscriptions(&pages, window, cx));
        self.subscriptions = subscriptions;
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
            AppRoute::Brokers | AppRoute::BrokerDetail { .. } if uses_legacy_page => (
                "Filter real Broker inventory and inspect runtime or generation-aware configuration.",
                div().size_full().child(self.legacy_pages.brokers.clone()),
            ),
            AppRoute::Topics | AppRoute::TopicDetail { .. } if uses_legacy_page => (
                "Filter real Topic inventory and inspect independently loaded lifecycle resources.",
                div().size_full().child(self.legacy_pages.topics.clone()),
            ),
            AppRoute::Consumers | AppRoute::ConsumerDetail { .. } if uses_legacy_page => (
                "Filter real Consumer observations and inspect independently loaded group resources.",
                div().size_full().child(self.legacy_pages.consumers.clone()),
            ),
            AppRoute::Producers if uses_legacy_page => (
                "Inspect discovered Producer groups and apply an explicit Topic + Group client query.",
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
#[path = "app/tests.rs"]
mod tests;
