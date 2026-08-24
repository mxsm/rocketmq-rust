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
    route::{AppRoute, BrokerTab, RouteKey},
    services::{
        AppServices, CapabilityUnavailableConfigService, ConfigRouteTransition, ConfigUpdatePhase, ConfigUpdated,
        FakeAuthService, FakeStartupService, StartupSnapshot, delivery03::test_support::FakeDelivery03Backend,
    },
    state::{RequestEpoch, UiError, UiErrorCode},
};
use rocketmq_admin_core::read_client_adapter::TelemetryHandle;
use rocketmq_runtime::{ProcessMemoryLimit, RuntimeConfig, RuntimeOwner};

fn broker_item(address: &str) -> rocketmq_dashboard_common::BrokerInventoryItem {
    rocketmq_dashboard_common::BrokerInventoryItem {
        identity: rocketmq_dashboard_common::BrokerIdentity {
            cluster: "cluster-a".into(),
            broker_name: "broker-a".into(),
            broker_id: 0,
            address: address.into(),
        },
        role: rocketmq_dashboard_common::BrokerRole::Master,
        version: rocketmq_dashboard_common::Observed::Observed("5.3.2".into()),
        availability: rocketmq_dashboard_common::EndpointAvailability::Available,
        produce_tps: rocketmq_dashboard_common::Observed::Observed(1.0),
        consume_tps: rocketmq_dashboard_common::Observed::Observed(2.0),
    }
}

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
fn invalidation_preserves_broker_sheet_closes_dialogs_and_success_preserves_ops_history(cx: &mut gpui::TestAppContext) {
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
        assert!(window.has_active_sheet(app));
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

#[gpui::test]
fn broker_deep_route_back_forward_and_tab_replace_control_the_same_sheet(cx: &mut gpui::TestAppContext) {
    cx.update(gpui_component::init);
    let fake = Arc::new(FakeDelivery03Backend::default());
    let item = broker_item("127.0.0.1:10911");
    for _ in 0..4 {
        fake.queue_inventory(Ok(vec![item.clone()]));
    }
    fake.queue_runtime(Ok(vec![rocketmq_dashboard_common::RuntimeEntry::new(
        "brokerVersion".into(),
        "5.3.2".into(),
    )]));
    fake.queue_runtime(Ok(Vec::new()));
    let app_services = services(
        StartupSnapshot {
            configuration_revision: 7,
            login_required: false,
            has_valid_session: false,
        },
        FakeAuthService::authenticated(),
    )
    .with_delivery03_backend(fake.clone());
    let dashboard_handle = Rc::new(RefCell::new(None));
    let capture = dashboard_handle.clone();
    let (_root, cx) = cx.add_window_view(move |window, cx| {
        let dashboard = cx.new(|cx| super::RocketmqDashboard::with_services(window, app_services, cx));
        capture.replace(Some(dashboard.clone()));
        Root::new(dashboard, window, cx)
    });
    let dashboard = dashboard_handle.borrow_mut().take().expect("dashboard entity");
    cx.run_until_parked();

    let runtime_route = AppRoute::BrokerDetail {
        broker: RouteKey::parse(item.identity.address.clone()).expect("route key"),
        tab: BrokerTab::Runtime,
    };
    cx.update(|window, app| {
        dashboard.update(app, |dashboard, cx| {
            dashboard.navigate(runtime_route.clone(), window, cx)
        });
        assert!(window.has_active_sheet(app));
        assert_eq!(dashboard.read(app).history.current(), &runtime_route);
    });
    cx.run_until_parked();
    assert_eq!(
        fake.calls().runtime.len(),
        1,
        "Runtime is the only lazy detail resource loaded"
    );
    assert!(fake.calls().config.is_empty());

    let config_route = AppRoute::BrokerDetail {
        broker: RouteKey::parse(item.identity.address.clone()).expect("route key"),
        tab: BrokerTab::Configuration,
    };
    cx.update(|window, app| {
        dashboard.update(app, |dashboard, cx| {
            dashboard.handle_brokers_intent(
                crate::features::brokers::BrokersIntent::ReplaceRoute(config_route.clone()),
                window,
                cx,
            );
        });
    });
    cx.update(|_, app| assert_eq!(dashboard.read(app).history.current(), &config_route));

    cx.update(|window, app| {
        dashboard.update(app, |dashboard, cx| dashboard.back(window, cx));
        assert!(!window.has_active_sheet(app));
        assert_eq!(dashboard.read(app).history.current(), &AppRoute::Dashboard);
    });
    cx.update(|window, app| {
        dashboard.update(app, |dashboard, cx| dashboard.forward(window, cx));
        assert!(window.has_active_sheet(app));
        assert_eq!(dashboard.read(app).history.current(), &config_route);
    });
}

#[gpui::test]
fn rebuilding_delivery_pages_keeps_root_subscriptions_bounded(cx: &mut gpui::TestAppContext) {
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
    cx.update(|window, app| {
        dashboard.update(app, |dashboard, cx| {
            let expected = dashboard.subscriptions.len();
            assert_eq!(expected, 6);
            for _ in 0..5 {
                dashboard.rebuild_pages(window, cx);
                assert_eq!(dashboard.subscriptions.len(), expected);
            }
        });
    });
}
