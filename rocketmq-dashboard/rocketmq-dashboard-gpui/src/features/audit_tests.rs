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

use gpui::{Modifiers, point, size};
use gpui_component::Root;
use rocketmq_dashboard_common::{
    AuditAcceptedOperation, AuditConsumerCleanupFacts, AuditConsumerField, AuditEvent, AuditFacts, OperationContext,
    OperationResource,
};
use rocketmq_runtime::{ProcessMemoryLimit, RuntimeConfig, RuntimeOwner};

use super::*;
use crate::infrastructure::{
    audit_store::AuditStore,
    auth_state::{DesktopAuthState, EnvironmentReader, LOGIN_PASSWORD_ENV, LOGIN_USERNAME_ENV, MapEnvironment},
    config_store::{AuthConfig, DesktopConfig, DesktopConfigStore},
};

pub(crate) struct Fixture {
    _directory: tempfile::TempDir,
    pub runtime: RuntimeOwner,
    pub services: AppServices,
    pub auth: Arc<DesktopAuthState>,
    pub audit: AuditStore,
    pub active: EnvironmentId,
}

impl Fixture {
    pub fn new(authentication: bool) -> Self {
        Self::with_environment(
            authentication,
            Arc::new(MapEnvironment::new([
                (LOGIN_USERNAME_ENV, "operator"),
                (LOGIN_PASSWORD_ENV, "test-password"),
            ])),
        )
    }

    fn with_environment(authentication: bool, environment: Arc<dyn EnvironmentReader>) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("config.json");
        std::fs::write(
            &path,
            serde_json::to_vec(&DesktopConfig {
                schema_version: 1,
                auth: AuthConfig {
                    enabled: authentication,
                    ..Default::default()
                },
                ..Default::default()
            })
            .unwrap(),
        )
        .unwrap();
        let runtime = RuntimeOwner::plan(RuntimeConfig::for_parallelism("audit-query-test", 1))
            .unwrap()
            .with_memory_limit(ProcessMemoryLimit::configured(256 * 1024 * 1024).unwrap())
            .build()
            .unwrap();
        let store = DesktopConfigStore::new(path, runtime.root_context().component("storage"));
        let auth = DesktopAuthState::new(environment);
        let services = AppServices::desktop_with_fake_provider(
            store.clone(),
            auth.clone(),
            runtime.root_context().component("services"),
            runtime.root_context().component("history"),
            runtime.root_context().component("monitor"),
        );
        runtime.block_on(services.bootstrap()).unwrap();
        let active = store.environment_snapshot().unwrap().environment_id;
        let audit = AuditStore::new(store.clone());
        Self {
            _directory: directory,
            runtime,
            services,
            auth,
            audit,
            active,
        }
    }

    pub fn operation(&self, index: u64) -> AuditAcceptedOperation {
        AuditAcceptedOperation {
            context: OperationContext {
                environment_id: if index.is_multiple_of(4) {
                    EnvironmentId::parse("other-profile").unwrap()
                } else {
                    self.active.clone()
                },
                configuration_revision: 0,
                request_id: format!("audit-query-{index}"),
                actor: if index.is_multiple_of(2) {
                    OperationActor::LocalDesktop
                } else {
                    OperationActor::System
                },
                resource: if index.is_multiple_of(2) {
                    OperationResource::ConsumerGroup("orders-consumer".into())
                } else {
                    OperationResource::Topic("orders".into())
                },
            },
            action: if index.is_multiple_of(2) {
                AuditAction::ConsumerDeleted
            } else {
                AuditAction::TopicCreated
            },
            accepted_at_epoch_ms: now() - 1000,
            topic_fields: Default::default(),
            consumer_fields: if index.is_multiple_of(2) {
                [
                    AuditConsumerField::RetryMaxTimes,
                    AuditConsumerField::ConsumeTimeoutMinutes,
                ]
                .into_iter()
                .collect()
            } else {
                Default::default()
            },
        }
    }

    pub fn seed(&self, count: u64) {
        for index in 1..=count {
            let operation = self.operation(index);
            self.runtime
                .block_on(self.audit.finish(
                    operation,
                    if index.is_multiple_of(2) {
                        AuditOutcome::Partial
                    } else {
                        AuditOutcome::Succeeded
                    },
                    AuditFacts {
                        applied_targets: 1,
                        consumer_cleanup: index.is_multiple_of(2).then_some(AuditConsumerCleanupFacts {
                            applied_steps: 2,
                            failed_steps: 0,
                            unconfirmed_steps: 0,
                        }),
                        ..Default::default()
                    },
                    now(),
                ))
                .unwrap();
        }
    }

    pub fn events(&self) -> Vec<AuditEvent> {
        self.audit
            .query(AuditQuery {
                filter: AuditFilter::recent(now()),
                limit: 200,
                cursor: None,
            })
            .unwrap()
            .events
    }

    pub fn persisted_events(&self) -> Vec<AuditEvent> {
        let document: serde_json::Value =
            serde_json::from_slice(&std::fs::read(self._directory.path().join("config.json")).unwrap()).unwrap();
        serde_json::from_value(document["audit"]["events"].clone()).unwrap()
    }
}

struct Harness {
    view: Entity<AuditView>,
}
impl Render for Harness {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        div()
            .size_full()
            .child(self.view.clone())
            .children(Root::render_dialog_layer(window, cx))
    }
}

fn mount(
    cx: &mut gpui::TestAppContext,
    services: AppServices,
) -> (Entity<Root>, Entity<AuditView>, &mut gpui::VisualTestContext) {
    cx.update(gpui_component::init);
    let capture = Rc::new(RefCell::new(None));
    let captured = capture.clone();
    let (root, cx) = cx.add_window_view(move |window, cx| {
        let view = cx.new(|cx| AuditView::new(window, services, cx));
        view.update(cx, |view, cx| view.set_active(true, window, cx));
        captured.replace(Some(view.clone()));
        let harness = cx.new(|_| Harness { view });
        Root::new(harness, window, cx)
    });
    let view = capture.borrow_mut().take().unwrap();
    cx.simulate_resize(size(px(960.), px(640.)));
    (root, view, cx)
}

fn draw(root: &Entity<Root>, cx: &mut gpui::VisualTestContext) {
    cx.run_until_parked();
    for _ in 0..2 {
        cx.draw(point(px(0.), px(0.)), size(px(960.), px(640.)), |_, _| root.clone());
    }
}
fn click(cx: &mut gpui::VisualTestContext, selector: &'static str) {
    let bounds = cx
        .debug_bounds(selector)
        .unwrap_or_else(|| panic!("Missing {selector}"));
    cx.simulate_click(bounds.center(), Modifiers::default());
    cx.run_until_parked();
}
fn ids(view: &Entity<AuditView>, cx: &mut gpui::VisualTestContext) -> Vec<u64> {
    cx.read(|app| {
        view.read(app)
            .store
            .page()
            .unwrap()
            .events
            .iter()
            .map(|event| event.event_id)
            .collect()
    })
}

#[gpui::test]
fn audit_queued_results_and_already_rendered_cached_controls_recheck_live_auth(cx: &mut gpui::TestAppContext) {
    let fixture = Fixture::new(true);
    fixture.seed(70);
    fixture.auth.authenticate("operator", "test-password").unwrap();
    let before = fixture.events();
    let (root, view, cx) = mount(cx, fixture.services.clone());
    draw(&root, cx);
    let (started, release) = cx.update(|_, app| view.update(app, |view, _| view.hold_next_query()));
    click(cx, "audit-next");
    fixture.runtime.block_on(async {
        tokio::time::timeout(std::time::Duration::from_secs(5), started)
            .await
            .unwrap()
            .unwrap();
    });
    fixture.auth.sign_out();
    release.send(()).unwrap();
    cx.run_until_parked();
    assert_eq!(
        cx.read(|app| view.read(app).retained_record_count()),
        0,
        "A read completed while authorized must not publish after revocation"
    );
    assert_eq!(
        cx.read(|app| view.read(app).store.error.as_ref().unwrap().code()),
        UiErrorCode::Authentication
    );

    for control in ["audit-next", "audit-previous", "audit-open-0"] {
        fixture.auth.authenticate("operator", "test-password").unwrap();
        click(cx, "audit-apply");
        draw(&root, cx);
        click(cx, "audit-next");
        draw(&root, cx);
        assert_eq!(cx.read(|app| view.read(app).store.pages.len()), 2);
        if control == "audit-next" {
            click(cx, "audit-previous");
            draw(&root, cx);
        }
        let cached = cx.debug_bounds(control).unwrap();
        fixture.auth.sign_out();
        // Dispatch the old frame's callback before another render can clear the cached rows.
        cx.simulate_click(cached.center(), Modifiers::default());
        cx.run_until_parked();
        assert_eq!(
            cx.read(|app| view.read(app).retained_record_count()),
            0,
            "Denied cached control: {control}"
        );
        assert!(!cx.read(|app| view.read(app).has_detail()));
    }
    assert_eq!(fixture.events(), before);
    fixture.runtime.shutdown_runtime_blocking().unwrap();
}

#[gpui::test]
fn audit_queued_read_is_bound_to_the_current_operator_not_an_authenticated_boolean(cx: &mut gpui::TestAppContext) {
    struct Credentials(std::sync::Mutex<String>);
    impl EnvironmentReader for Credentials {
        fn read(&self, name: &'static str) -> Option<String> {
            match name {
                LOGIN_USERNAME_ENV => Some(self.0.lock().unwrap().clone()),
                LOGIN_PASSWORD_ENV => Some("test-password".into()),
                _ => None,
            }
        }
    }
    let credentials = Arc::new(Credentials(std::sync::Mutex::new("operator-a".into())));
    let fixture = Fixture::with_environment(true, credentials.clone());
    fixture.seed(40);
    fixture.auth.authenticate("operator-a", "test-password").unwrap();
    let (root, view, cx) = mount(cx, fixture.services.clone());
    draw(&root, cx);
    let (started, release) = cx.update(|_, app| view.update(app, |view, _| view.hold_next_query()));
    click(cx, "audit-next");
    fixture.runtime.block_on(async {
        tokio::time::timeout(std::time::Duration::from_secs(5), started)
            .await
            .unwrap()
            .unwrap();
    });
    fixture.auth.sign_out();
    *credentials.0.lock().unwrap() = "operator-b".into();
    fixture.auth.authenticate("operator-b", "test-password").unwrap();
    release.send(()).unwrap();
    cx.run_until_parked();
    assert!(fixture.services.audit_read_access().is_ok());
    assert_eq!(cx.read(|app| view.read(app).retained_record_count()), 0);
    assert!(cx.read(|app| view.read(app).store.applied.is_none()));
    draw(&root, cx);
    click(cx, "audit-apply");
    draw(&root, cx);
    assert_eq!(cx.read(|app| view.read(app).retained_record_count()), 25);
    fixture.runtime.shutdown_runtime_blocking().unwrap();
}

#[gpui::test]
fn audit_actual_pages_filters_and_expired_cursor_recovery_preserve_drafts_without_appending_events(
    cx: &mut gpui::TestAppContext,
) {
    let fixture = Fixture::new(false);
    fixture.seed(40);
    let seeded = fixture.events();
    let (root, view, cx) = mount(cx, fixture.services.clone());
    draw(&root, cx);
    let first = ids(&view, cx);
    assert_eq!(first.len(), 25);
    // A terminal write may commit after the first page with an earlier captured UTC timestamp.
    fixture
        .runtime
        .block_on(fixture.audit.finish(
            fixture.operation(41),
            AuditOutcome::Succeeded,
            AuditFacts::default(),
            seeded[0].timestamp_epoch_ms,
        ))
        .unwrap();
    let before = fixture.events();
    click(cx, "audit-next");
    draw(&root, cx);
    let second = ids(&view, cx);
    assert_eq!(second.len(), 5);
    assert!(second.iter().all(|id| !first.contains(id)));
    click(cx, "audit-previous");
    draw(&root, cx);
    assert_eq!(ids(&view, cx), first);
    cx.update(|window, app| {
        view.update(app, |view, cx| {
            // The service must reject the now-obsolete retained generation, as after journal eviction.
            view.store.pages[0].next_cursor.as_mut().unwrap().retention_generation += 1;
            view.store.pages.truncate(1);
            view.start
                .update(cx, |input, cx| input.set_value("2026-01-01 00:00:00.000", window, cx));
        })
    });
    click(cx, "audit-next");
    draw(&root, cx);
    assert!(cx.read(|app| view.read(app).store.error.is_some()));
    let draft = cx.read(|app| view.read(app).start.read(app).value());
    click(cx, "audit-restart");
    draw(&root, cx);
    assert_eq!(cx.read(|app| view.read(app).start.read(app).value()), draft);
    let refreshed = ids(&view, cx);
    assert_eq!(
        refreshed[0], 41,
        "a restarted query must admit the newly committed terminal event"
    );
    assert!(cx.read(|app| view.read(app).store.error.is_none()));
    click(cx, "audit-apply");
    draw(&root, cx);
    assert!(
        cx.read(|app| view.read(app).validation.is_some()),
        "oversized UTC draft must fail before replacing the applied window"
    );
    assert_eq!(ids(&view, cx), refreshed);
    click(cx, "audit-reset");
    draw(&root, cx);
    click(cx, "audit-outcome-filter");
    cx.simulate_keystrokes("down enter");
    draw(&root, cx);
    click(cx, "audit-apply");
    draw(&root, cx);
    cx.read(|app| {
        let view = view.read(app);
        assert_eq!(
            view.store.applied.as_ref().unwrap().outcome,
            Some(AuditOutcome::Succeeded)
        );
        assert!(
            view.store
                .page()
                .unwrap()
                .events
                .iter()
                .all(|event| event.outcome == AuditOutcome::Succeeded)
        );
    });
    // Select the other closed-contract filters, then apply through the real control.
    cx.update(|window, app| {
        view.update(app, |view, cx| {
            view.action.update(cx, |picker, cx| {
                picker.set_selected_value(&Some(AuditAction::ConsumerDeleted), window, cx)
            });
            view.outcome.update(cx, |picker, cx| {
                picker.set_selected_value(&Some(AuditOutcome::Partial), window, cx)
            });
            view.actor.update(cx, |picker, cx| {
                picker.set_selected_value(&ActorKind::Desktop, window, cx)
            });
            view.environment
                .update(cx, |picker, cx| picker.set_selected_value(&None, window, cx));
        })
    });
    draw(&root, cx);
    click(cx, "audit-apply");
    draw(&root, cx);
    cx.read(|app| {
        let state = &view.read(app).store;
        let applied = state.applied.as_ref().unwrap();
        assert_eq!(applied.action, Some(AuditAction::ConsumerDeleted));
        assert_eq!(applied.actor, Some(OperationActor::LocalDesktop));
        assert!(applied.environment_id.is_none());
        assert_eq!(state.page().unwrap().events.len(), 20);
        assert!(
            state
                .page()
                .unwrap()
                .events
                .iter()
                .all(|event| event.operation.action == AuditAction::ConsumerDeleted
                    && event.operation.context.actor == OperationActor::LocalDesktop)
        );
    });
    cx.update(|window, app| {
        view.update(app, |view, cx| {
            view.environment.update(cx, |picker, cx| {
                picker.set_selected_value(&Some(fixture.active.clone()), window, cx)
            });
        })
    });
    draw(&root, cx);
    click(cx, "audit-apply");
    draw(&root, cx);
    assert_eq!(ids(&view, cx).len(), 10);
    assert_eq!(
        fixture.events(),
        before,
        "read-only Audit controls must append zero events"
    );
    fixture.runtime.shutdown_runtime_blocking().unwrap();
}

#[gpui::test]
fn audit_compact_detail_copies_trusted_request_and_separates_cleanup_and_broker_facts(cx: &mut gpui::TestAppContext) {
    let fixture = Fixture::new(false);
    fixture.seed(3);
    let before = fixture.events();
    let (root, view, cx) = mount(cx, fixture.services.clone());
    draw(&root, cx);
    let results = cx.debug_bounds("audit-results").unwrap();
    let apply = cx.debug_bounds("audit-apply").unwrap();
    let next = cx.debug_bounds("audit-next").unwrap();
    assert!(results.size.height > px(100.) && results.bottom() <= next.top());
    assert!(apply.right() <= px(960.) && next.bottom() <= px(640.));
    for selector in [
        "audit-record-time",
        "audit-record-actor",
        "audit-record-resource",
        "audit-record-environment",
        "audit-record-request",
    ] {
        let field = cx.debug_bounds(selector).unwrap();
        assert!(
            field.size.height >= px(12.),
            "unreadable record field: {selector}, {field:?}"
        );
    }
    // The first row is a Topic event; the second row is the Consumer event with step facts.
    cx.simulate_event(gpui::ScrollWheelEvent {
        position: results.center(),
        delta: gpui::ScrollDelta::Pixels(point(px(0.), px(-160.))),
        ..Default::default()
    });
    draw(&root, cx);
    cx.simulate_event(gpui::ScrollWheelEvent {
        position: results.center(),
        delta: gpui::ScrollDelta::Pixels(point(px(0.), px(160.))),
        ..Default::default()
    });
    draw(&root, cx);
    let open = cx.debug_bounds("audit-open-1").unwrap();
    assert!(
        open.top() >= results.top() && open.bottom() <= results.bottom(),
        "record control must be visible after wheel: {open:?}, viewport={results:?}"
    );
    click(cx, "audit-open-1");
    draw(&root, cx);
    let detail = cx.read(|app| view.read(app).detail.clone().unwrap());
    let body = cx.debug_bounds("audit-detail-scroll").unwrap();
    assert!(body.right() <= px(960.) && body.bottom() <= px(640.));
    click(cx, "audit-copy-request-id");
    assert_eq!(
        cx.update(|_, app| app.read_from_clipboard().and_then(|item| item.text())),
        Some("audit-query-2".into())
    );
    cx.simulate_event(gpui::ScrollWheelEvent {
        position: body.center(),
        delta: gpui::ScrollDelta::Pixels(point(px(0.), px(-2000.))),
        ..Default::default()
    });
    draw(&root, cx);
    for selector in [
        "audit-broker-target-facts",
        "audit-consumer-cleanup-facts",
        "audit-consumer-field-facts",
    ] {
        let facts = cx.debug_bounds(selector).unwrap();
        assert!(facts.left() >= body.left() && facts.right() <= body.right());
    }
    let close = cx.debug_bounds("audit-detail-close").unwrap();
    assert!(close.bottom() <= px(640.));
    click(cx, "audit-detail-close");
    assert!(cx.read(|app| detail.read(app).record.is_none()));
    assert_eq!(fixture.events(), before);
    fixture.runtime.shutdown_runtime_blocking().unwrap();
}

#[gpui::test]
fn audit_real_auth_authority_pending_requests_and_context_clear_drop_all_protected_records(
    cx: &mut gpui::TestAppContext,
) {
    let fixture = Fixture::new(true);
    fixture.seed(3);
    let mut pending = fixture.operation(99);
    pending.context.request_id = "unfinished-request".into();
    fixture.runtime.block_on(fixture.audit.accept(pending)).unwrap();
    let before = fixture.events();
    let (root, view, cx) = mount(cx, fixture.services.clone());
    draw(&root, cx);
    assert!(cx.read(|app| view.read(app).store.pages.is_empty()));
    assert_eq!(
        cx.read(|app| view.read(app).store.error.as_ref().unwrap().code()),
        UiErrorCode::Authentication
    );
    fixture.auth.authenticate("operator", "test-password").unwrap();
    click(cx, "audit-apply");
    draw(&root, cx);
    assert_eq!(cx.read(|app| view.read(app).store.pending.len()), 1);
    click(cx, "audit-pending-0");
    draw(&root, cx);
    let detail = cx.read(|app| view.read(app).detail.clone().unwrap());
    click(cx, "audit-copy-request-id");
    assert_eq!(
        cx.update(|_, app| app.read_from_clipboard().and_then(|item| item.text())),
        Some("unfinished-request".into())
    );
    cx.update(|window, app| view.update(app, |view, cx| view.clear_context(window, cx)));
    assert!(cx.read(|app| detail.read(app).record.is_none()));
    assert_eq!(cx.read(|app| view.read(app).retained_record_count()), 0);
    assert!(!cx.update(|window, app| window.has_active_dialog(app)));
    fixture.auth.sign_out();
    cx.update(|window, app| view.update(app, |view, cx| view.set_active(true, window, cx)));
    draw(&root, cx);
    assert_eq!(cx.read(|app| view.read(app).retained_record_count()), 0);
    assert_eq!(fixture.events(), before);
    fixture.runtime.shutdown_runtime_blocking().unwrap();
}
