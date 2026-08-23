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
    AppContext as _, Context, Entity, IntoElement, ParentElement as _, Render, Styled as _, Task, WeakEntity, Window,
    div, prelude::FluentBuilder as _, px,
};
use gpui_component::{
    ActiveTheme as _, IconName, StyledExt as _, WindowExt as _,
    button::{Button, ButtonVariants as _},
    sidebar::{Sidebar, SidebarGroup as GpuiSidebarGroup, SidebarMenu, SidebarMenuItem},
};

use crate::{
    components::{
        app_shell::uses_fixed_sidebar,
        data_table, dialog, page_header, query_toolbar,
        sidebar::{SidebarGroup, SidebarItem, is_active, navigation_groups},
        states, status_badge, toast,
        topbar::ConnectionSummary,
    },
    features::login::LoginForm,
    route::{AppRoute, NavigationHistory},
    services::{AppServices, SessionState, StartupSnapshot},
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
    unavailable_table: Entity<gpui_component::table::TableState<data_table::UnavailableTable>>,
}

impl LegacyPageCache {
    fn new(window: &mut Window, cx: &mut Context<RocketmqDashboard>) -> Self {
        Self {
            dashboard: cx.new(|_| DashboardView::new()),
            brokers: cx.new(|_| ClusterView::new()),
            topics: cx.new(|_| TopicView::new()),
            consumers: cx.new(|_| ConsumerView::new()),
            producers: cx.new(|_| ProducerView::new()),
            messages: cx.new(|_| MessageView::new()),
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
        )
    }

    #[cfg(test)]
    fn entity_ids(&self) -> [gpui::EntityId; 7] {
        [
            self.dashboard.entity_id(),
            self.brokers.entity_id(),
            self.topics.entity_id(),
            self.consumers.entity_id(),
            self.producers.entity_id(),
            self.messages.entity_id(),
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

/// Root dashboard application state.
///
/// The only async task is retained in `startup_task`; no task is detached and the services it
/// calls are deliberately local seams until the desktop runtime delivery is implemented.
pub struct RocketmqDashboard {
    services: AppServices,
    startup_state: StartupState,
    startup_task: Option<Task<()>>,
    startup_epoch: RequestEpoch,
    configuration_revision: u64,
    history: NavigationHistory,
    session: SessionState,
    login: LoginForm,
    legacy_pages: LegacyPageCache,
    sensitive_feature_cache: SensitiveFeatureCache,
    last_intent: Option<ServiceIntent>,
    last_service_error: Option<UiError>,
}

impl RocketmqDashboard {
    /// Creates the root in Booting state and starts its owned startup task.
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        Self::with_services(window, AppServices::default(), cx)
    }

    /// Creates the root with injectable service seams for focused tests and host integration.
    pub fn with_services(window: &mut Window, services: AppServices, cx: &mut Context<Self>) -> Self {
        let mut dashboard = Self {
            services,
            startup_state: StartupState::Booting,
            startup_task: None,
            startup_epoch: RequestEpoch::initial(),
            configuration_revision: 0,
            history: NavigationHistory::new(AppRoute::Login),
            session: SessionState::signed_out(),
            login: LoginForm::new(window, cx),
            legacy_pages: LegacyPageCache::new(window, cx),
            sensitive_feature_cache: SensitiveFeatureCache::default(),
            last_intent: None,
            last_service_error: None,
        };
        dashboard.start_bootstrap(cx);
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
            let result = cx
                .background_executor()
                .spawn(async move { services.bootstrap() })
                .await;
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
            }
            Err(error) => self.startup_state = StartupState::Failed(error),
        }
        cx.notify();
    }

    fn retry_startup(&mut self, cx: &mut Context<Self>) {
        if self.startup_state.can_retry() {
            self.start_bootstrap(cx);
        }
    }

    fn open_config_location(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.last_intent = Some(ServiceIntent::OpenConfigLocation);
        self.last_service_error = self.services.open_config_location().err();
        if let Some(error) = &self.last_service_error {
            toast::ToastHost::error(error.summary().to_owned(), window, cx);
        }
        cx.notify();
    }

    fn navigate(&mut self, route: AppRoute, window: &mut Window, cx: &mut Context<Self>) {
        self.history.navigate(route);
        if window.has_active_sheet(cx) {
            window.close_sheet(cx);
        }
        cx.notify();
    }

    fn back(&mut self, cx: &mut Context<Self>) {
        if self.history.back().is_some() {
            cx.notify();
        }
    }

    fn forward(&mut self, cx: &mut Context<Self>) {
        if self.history.forward().is_some() {
            cx.notify();
        }
    }

    fn submit_login(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let credentials = self.login.credentials(cx);
        let result = self
            .services
            .authenticate(credentials.username(), credentials.password());
        drop(credentials);

        match result {
            Ok(session) if session.is_authenticated() => {
                self.login.clear_sensitive(window, cx);
                self.session = session;
                self.history.replace(AppRoute::Dashboard);
                self.startup_state = StartupState::Ready(ReadyScreen::MainShell);
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

    fn sign_out(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let _ = self.services.sign_out();
        self.session.clear();
        self.login.clear_sensitive(window, cx);
        self.sensitive_feature_cache.clear();
        self.legacy_pages = LegacyPageCache::new(window, cx);
        self.history.reset(AppRoute::Login);
        self.startup_state = StartupState::Ready(ReadyScreen::Login);
        self.last_intent = None;
        self.last_service_error = None;
        cx.notify();
    }

    fn request_sign_out(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let view = cx.entity().downgrade();
        dialog::open_confirm(
            "Sign out",
            "This clears the current session and all local sensitive dashboard state.",
            "Sign out",
            move |_, window, cx| {
                let _ = view.update(cx, |dashboard, cx| dashboard.sign_out(window, cx));
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
                cx.listener(|this, _, window, cx| this.open_config_location(window, cx)),
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
                    )),
            )
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
                .child(self.render_topbar(!fixed_sidebar, cx))
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

    fn render_topbar(&self, show_menu: bool, cx: &mut Context<Self>) -> gpui::Div {
        let theme = cx.theme();
        let current = self.history.current();
        let connection = ConnectionSummary::default();
        div()
            .h(px(56.))
            .px_4()
            .flex()
            .items_center()
            .gap_2()
            .bg(theme.title_bar)
            .border_b_1()
            .border_color(theme.border)
            .when(show_menu, |this| {
                this.child(
                    Button::new("open-navigation")
                        .icon(IconName::Menu)
                        .ghost()
                        .tooltip("Open navigation")
                        .on_click(cx.listener(|this, _, window, cx| {
                            this.open_navigation_drawer(window, cx);
                        })),
                )
            })
            .child(
                div()
                    .flex_1()
                    .min_w_0()
                    .font_semibold()
                    .text_color(theme.foreground)
                    .child(current.title()),
            )
            .child(status_badge::render(
                connection.label(),
                theme.muted,
                theme.muted_foreground,
            ))
            .child(
                Button::new("go-back")
                    .icon(IconName::ArrowLeft)
                    .ghost()
                    .tooltip("Back")
                    .on_click(cx.listener(|this, _, _, cx| this.back(cx))),
            )
            .child(
                Button::new("go-forward")
                    .icon(IconName::ArrowRight)
                    .ghost()
                    .tooltip("Forward")
                    .on_click(cx.listener(|this, _, _, cx| this.forward(cx))),
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
        match self.startup_state {
            StartupState::Booting => self.render_startup_loading(cx),
            StartupState::Failed(_) => self.render_startup_failure(cx),
            StartupState::Ready(ReadyScreen::Login) => self.render_login(cx),
            StartupState::Ready(ReadyScreen::MainShell) => self.render_app_shell(window, cx),
        }
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
                    window.close_sheet(cx);
                    let _ = item_view.update(cx, |dashboard, cx| {
                        dashboard.history.navigate(route.clone());
                        cx.notify();
                    });
                }),
        )
    });
    GpuiSidebarGroup::new(group.label).child(menu)
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, rc::Rc, sync::Arc};

    use gpui::AppContext as _;

    use super::{LegacyPageCache, PageTarget, ReadyScreen, SensitiveFeatureCache, StartupRequest, StartupState};
    use crate::{
        route::AppRoute,
        services::{
            AppServices, CapabilityUnavailableConfigService, FakeAuthService, FakeStartupService, StartupSnapshot,
        },
        state::{RequestEpoch, UiError, UiErrorCode},
    };

    fn services(snapshot: StartupSnapshot, auth: FakeAuthService) -> AppServices {
        AppServices::new(
            Arc::new(FakeStartupService::ready(snapshot)),
            Arc::new(CapabilityUnavailableConfigService),
            Arc::new(auth),
        )
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

        cx.update(|window, app| {
            dashboard.update(app, |dashboard, cx| {
                let password_entity = dashboard.login.password_entity_id();
                dashboard.login.set_values("operator", "secret-password", window, cx);

                dashboard.submit_login(window, cx);

                assert_eq!(dashboard.login.password_entity_id(), password_entity);
                assert_eq!(dashboard.login.values(cx), ("operator".to_owned(), String::new()));
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

                assert_eq!(dashboard.login.values(cx), ("operator".to_owned(), String::new()));
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
                assert_eq!(dashboard.login.values(cx).1, "");
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
                assert_eq!(dashboard.current_page_target(), PageTarget::Placeholder);
                cx.notify();
            });
        });
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
        assert!(!LegacyPageCache::accepts_route(&AppRoute::OpsSettings));
    }
}
