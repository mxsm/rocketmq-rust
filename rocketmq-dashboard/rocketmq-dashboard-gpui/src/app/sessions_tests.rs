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

use super::*;
use crate::app::tests::next_owned_completion;
use gpui::{AppContext as _, point, px, size};
use gpui_component::{Root, WindowExt as _};
use std::{cell::RefCell, rc::Rc, sync::Arc};

#[gpui::test]
fn sessions_self_revoke_clears_the_root_before_its_owned_terminal_audit_finishes(cx: &mut gpui::TestAppContext) {
    use rocketmq_dashboard_common::{AuditAction, AuditFilter, AuditOutcome, AuditQuery, OperationActor};
    use std::{sync::mpsc, time::Duration};

    cx.update(gpui_component::init);
    let fixture = crate::services::sessions::tests::Fixture::new();
    let context = fixture.runtime.root_context().component("root-session-work");
    let services = AppServices::desktop_with_fake_provider(
        fixture.config.clone(),
        fixture.auth.clone(),
        context.clone(),
        fixture.runtime.root_context().component("root-history"),
        fixture.runtime.root_context().component("root-monitor"),
    );
    fixture.runtime.block_on(services.bootstrap()).unwrap();
    fixture.login();
    let (sender, completion) = mpsc::channel();
    let services = services.with_runtime_completion(sender);
    let audit_services = services.clone();
    let capture = Rc::new(RefCell::new(None));
    let captured = capture.clone();
    let (root, cx) = cx.add_window_view(move |window, cx| {
        let dashboard = cx.new(|cx| super::RocketmqDashboard::with_services(window, services, cx));
        captured.replace(Some(dashboard.clone()));
        Root::new(dashboard, window, cx)
    });
    let dashboard = capture.borrow_mut().take().unwrap();
    cx.run_until_parked();
    assert_eq!(next_owned_completion(&completion), "gpui-bootstrap");
    cx.run_until_parked();
    cx.update(|window, app| {
        dashboard.update(app, |view, cx| {
            view.sensitive_feature_cache = SensitiveFeatureCache::with_entries(4);
            view.navigate(AppRoute::Sessions, window, cx);
        })
    });
    cx.run_until_parked();
    assert_eq!(next_owned_completion(&completion), "gpui-sessions-query");
    cx.run_until_parked();
    for _ in 0..2 {
        cx.draw(point(px(0.), px(0.)), size(px(1440.), px(900.)), |_, _| root.clone());
    }
    let revoke = cx.debug_bounds("session-revoke-0").unwrap();
    cx.simulate_click(revoke.center(), gpui::Modifiers::default());
    cx.run_until_parked();
    let confirmation = cx.read(|app| {
        dashboard
            .read(app)
            .legacy_pages
            .sessions
            .read(app)
            .confirmation_input_for_test(app)
            .unwrap()
    });
    cx.update(|window, app| confirmation.update(app, |input, cx| input.set_value("operator", window, cx)));
    let (terminal_started, terminal_release) = audit_services.hold_next_audit_terminal();
    for _ in 0..2 {
        cx.draw(point(px(0.), px(0.)), size(px(1440.), px(900.)), |_, _| root.clone());
    }
    let submit = cx.debug_bounds("sessions-revoke-submit").unwrap();
    cx.simulate_click(submit.center(), gpui::Modifiers::default());
    cx.run_until_parked();
    fixture.wait_for_terminal(terminal_started, cx);
    cx.run_until_parked();
    cx.update(|window, app| {
        let view = dashboard.read(app);
        assert!(!view.session.is_authenticated());
        assert_eq!(view.history.current(), &AppRoute::Login);
        assert_eq!(view.startup_state, StartupState::Ready(ReadyScreen::Login));
        assert_eq!(view.sensitive_feature_cache.entries, 0);
        assert!(view.login.values(app).1.is_empty());
        assert!(!window.has_active_dialog(app));
        assert!(!window.has_active_sheet(app));
    });
    terminal_release.send(()).unwrap();
    assert_eq!(next_owned_completion(&completion), "gpui-session-revoke-all");
    fixture.runtime.block_on(async {
        let document = fixture.native.load_desktop().await.unwrap().unwrap().document;
        let events = document
            .audit
            .query(AuditQuery {
                filter: AuditFilter {
                    action: Some(AuditAction::SessionRevokeAll),
                    ..AuditFilter::recent(crate::services::audit::now_epoch_ms().unwrap())
                },
                limit: 25,
                cursor: None,
            })
            .unwrap()
            .events;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].outcome, AuditOutcome::Succeeded);
        assert_eq!(
            events[0].operation.context.actor,
            OperationActor::LocalOperator {
                username: "operator".into()
            }
        );
        assert!(context.task_group().shutdown(Duration::from_secs(5)).await.is_healthy());
    });
    fixture.finish();
}

#[gpui::test]
fn sessions_login_replacement_survives_an_older_owned_logout_terminal(cx: &mut gpui::TestAppContext) {
    use std::{sync::mpsc, time::Duration};

    cx.update(gpui_component::init);
    let fixture = crate::services::sessions::tests::Fixture::new();
    let context = fixture.runtime.root_context().component("root-replacement");
    let services = AppServices::desktop_with_fake_provider(
        fixture.config.clone(),
        fixture.auth.clone(),
        context.clone(),
        fixture.runtime.root_context().component("replacement-history"),
        fixture.runtime.root_context().component("replacement-monitor"),
    );
    fixture.runtime.block_on(services.bootstrap()).unwrap();
    fixture.login();
    let old = fixture.auth.session().session_id().unwrap();
    let (sender, completion) = mpsc::channel();
    let services = services.with_runtime_completion(sender);
    let audit_services = services.clone();
    let capture = Rc::new(RefCell::new(None));
    let captured = capture.clone();
    let (root, cx) = cx.add_window_view(move |window, cx| {
        let dashboard = cx.new(|cx| RocketmqDashboard::with_services(window, services, cx));
        captured.replace(Some(dashboard.clone()));
        Root::new(dashboard, window, cx)
    });
    let dashboard = capture.borrow_mut().take().unwrap();
    cx.run_until_parked();
    assert_eq!(next_owned_completion(&completion), "gpui-bootstrap");
    cx.run_until_parked();
    let (terminal_started, terminal_release) = audit_services.hold_next_audit_terminal();
    cx.update(|window, app| dashboard.update(app, |view, cx| view.request_sign_out(window, cx)));
    cx.draw(point(px(0.), px(0.)), size(px(1440.), px(900.)), |_, _| root.clone());
    let confirm = cx.debug_bounds("confirm-dialog-ok").unwrap();
    cx.simulate_click(confirm.center(), gpui::Modifiers::default());
    fixture.runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(5), terminal_started)
            .await
            .unwrap()
            .unwrap();
    });
    cx.run_until_parked();
    cx.update(|window, app| {
        dashboard.update(app, |view, cx| {
            assert_eq!(view.history.current(), &AppRoute::Login);
            view.login.set_values("operator", "private-password", window, cx);
        })
    });
    for _ in 0..2 {
        cx.draw(point(px(0.), px(0.)), size(px(1440.), px(900.)), |_, _| root.clone());
    }
    let sign_in = cx.debug_bounds("login-submit").unwrap();
    cx.simulate_click(sign_in.center(), gpui::Modifiers::default());
    assert_eq!(next_owned_completion(&completion), "gpui-authenticate");
    cx.run_until_parked();
    let replacement = fixture.auth.session().session_id().unwrap();
    assert_ne!(old, replacement);
    cx.read(|app| assert!(dashboard.read(app).session.is_authenticated()));
    terminal_release.send(()).unwrap();
    assert_eq!(next_owned_completion(&completion), "gpui-sign-out");
    cx.run_until_parked();
    cx.read(|app| {
        let view = dashboard.read(app);
        assert!(view.session.is_authenticated());
        assert_eq!(view.startup_state, StartupState::Ready(ReadyScreen::MainShell));
        assert_eq!(view.history.current(), &AppRoute::Dashboard);
    });
    fixture.runtime.block_on(async {
        assert!(
            fixture
                .native
                .access_session(replacement, crate::services::audit::now_epoch_ms().unwrap())
                .await
                .unwrap()
                .is_some()
        );
        assert!(context.task_group().shutdown(Duration::from_secs(5)).await.is_healthy());
    });
    fixture.finish();
}

#[gpui::test]
fn persisted_auth_policy_clears_a_retained_sheet_before_provider_completion_and_keeps_a_later_login(
    cx: &mut gpui::TestAppContext,
) {
    use crate::{
        route::ConsumerTab,
        services::{
            FakeAuthService, FakeStartupService, StartupSnapshot, UnavailableTestConfig,
            consumers::test_support::FakeConsumerBackend, dlq::test_support::group_inventory,
        },
        state::Loadable,
    };
    use rocketmq_dashboard_common::{ConsumerClientIdentity, ConsumerDiagnosticKind, ConsumerDiagnosticPayload};

    cx.update(gpui_component::init);
    let backend = Arc::new(FakeConsumerBackend::default());
    let services = AppServices::injected_for_test(
        Arc::new(FakeStartupService::ready(StartupSnapshot {
            configuration_revision: 0,
            login_required: false,
            has_valid_session: false,
        })),
        Arc::new(UnavailableTestConfig),
        Arc::new(FakeAuthService::authenticated()),
    )
    .with_consumer_backend(backend.clone());
    let capture = Rc::new(RefCell::new(None));
    let captured = capture.clone();
    let (root, cx) = cx.add_window_view(move |window, cx| {
        let dashboard = cx.new(|cx| RocketmqDashboard::with_services(window, services, cx));
        captured.replace(Some(dashboard.clone()));
        Root::new(dashboard, window, cx)
    });
    let dashboard = capture.borrow_mut().take().unwrap();
    cx.run_until_parked();
    let consumers = cx.read(|app| dashboard.read(app).legacy_pages.consumers.clone());
    cx.update(|window, app| {
        consumers.update(app, |view, cx| {
            view.store.inventory.replace(group_inventory());
            view.open_route("group-a", ConsumerTab::Clients, window, cx);
        })
    });
    cx.run_until_parked();
    let detail = cx.read(|app| consumers.read(app).detail_for_test().unwrap());
    backend.queue_diagnostic(Ok(ConsumerDiagnosticPayload::new(
        Vec::new(),
        Some("private-jstack".into()),
        false,
    )));
    cx.update(|_, app| {
        detail.update(app, |view, cx| {
            view.load_diagnostic_for_test(
                ConsumerClientIdentity::parse("client-a").unwrap(),
                ConsumerDiagnosticKind::Jstack,
                cx,
            )
        })
    });
    cx.run_until_parked();
    assert!(cx.read(|app| detail.read(app).store.diagnostic.state.value().is_some()));
    assert!(cx.update(|window, app| window.has_active_sheet(app)));
    cx.update(|window, app| {
        dashboard.update(app, |view, cx| {
            view.handle_config_updated(
                ConfigUpdated {
                    revision: 1,
                    phase: ConfigUpdatePhase::Persisted,
                    route_transition: ConfigRouteTransition::AuthenticationEnabled,
                },
                window,
                cx,
            )
        })
    });
    cx.update(|window, app| {
        assert!(!window.has_active_sheet(app));
        assert!(matches!(detail.read(app).store.diagnostic.state, Loadable::Idle));
        assert_eq!(
            dashboard.read(app).startup_state,
            StartupState::Ready(ReadyScreen::Login)
        );
        assert_eq!(dashboard.read(app).configuration_revision, 1);
        dashboard.update(app, |view, cx| {
            view.login.set_values("operator", "test-password", window, cx)
        });
    });
    for _ in 0..2 {
        cx.draw(point(px(0.), px(0.)), size(px(1440.), px(900.)), |_, _| root.clone());
    }
    let sign_in = cx.debug_bounds("login-submit").unwrap();
    cx.simulate_click(sign_in.center(), gpui::Modifiers::default());
    cx.run_until_parked();
    cx.update(|window, app| {
        dashboard.update(app, |view, cx| {
            assert!(view.session.is_authenticated());
            view.handle_config_updated(
                ConfigUpdated {
                    revision: 1,
                    phase: ConfigUpdatePhase::Completed,
                    route_transition: ConfigRouteTransition::AuthenticationEnabled,
                },
                window,
                cx,
            );
            assert!(
                view.session.is_authenticated(),
                "the later phase must not end the login established under the persisted policy"
            );
            assert_eq!(view.startup_state, StartupState::Ready(ReadyScreen::MainShell));
        })
    });
    backend.queue_inventory(Ok(group_inventory()));
    cx.update(|window, app| dashboard.update(app, |view, cx| view.navigate(AppRoute::Consumers, window, cx)));
    cx.run_until_parked();
    assert_eq!(
        backend.calls().inventory.last().unwrap().revision,
        1,
        "rebuilt pages must use the current connection revision"
    );
}
