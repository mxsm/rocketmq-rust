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

use std::{cell::RefCell, rc::Rc, sync::mpsc, time::Duration};

use gpui::{Modifiers, point, size};
use gpui_component::Root;
use rocketmq_dashboard_common::{
    AuditAction, AuditFilter, AuditOutcome, AuditQuery, OperationActor, OperationResource,
};
use rocketmq_dashboard_storage::{SessionQuery, SessionRecord};

use super::*;
use crate::services::{audit::now_epoch_ms, sessions::tests::Fixture};

struct Harness {
    view: Entity<SessionsView>,
}
impl Render for Harness {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        div()
            .size_full()
            .child(self.view.clone())
            .children(Root::render_dialog_layer(window, cx))
    }
}

fn draw(root: &Entity<Root>, cx: &mut gpui::VisualTestContext) {
    cx.run_until_parked();
    for _ in 0..2 {
        cx.draw(point(px(0.), px(0.)), size(px(1440.), px(900.)), |_, _| root.clone());
    }
}

fn click(cx: &mut gpui::VisualTestContext, selector: &'static str) {
    let bounds = cx
        .debug_bounds(selector)
        .unwrap_or_else(|| panic!("Missing Sessions control {selector}"));
    cx.simulate_click(bounds.center(), Modifiers::default());
    cx.run_until_parked();
}

fn complete(cx: &mut gpui::VisualTestContext, completion: &mpsc::Receiver<&'static str>, expected: &'static str) {
    cx.run_until_parked();
    assert_eq!(completion.recv_timeout(Duration::from_secs(5)).unwrap(), expected);
    cx.run_until_parked();
}

#[gpui::test]
fn sessions_real_filter_confirmation_cancel_validation_and_duplicate_submit_use_one_native_mutation(
    cx: &mut gpui::TestAppContext,
) {
    cx.update(gpui_component::init);
    let fixture = Fixture::new();
    let context = fixture.runtime.root_context().component("app-services");
    let services = AppServices::desktop_with_fake_provider(
        fixture.config.clone(),
        fixture.auth.clone(),
        context.clone(),
        fixture.runtime.root_context().component("history"),
        fixture.runtime.root_context().component("monitor"),
    );
    fixture.runtime.block_on(services.bootstrap()).unwrap();
    fixture.login();
    fixture.runtime.block_on(async {
        let now = now_epoch_ms().unwrap();
        fixture
            .native
            .create_session(SessionRecord::new("other".into(), now, now + 60_000).unwrap(), now)
            .await
            .unwrap();
    });
    let (sender, completion) = mpsc::channel();
    let services = services.with_runtime_completion(sender);
    let audit_services = services.clone();
    let capture = Rc::new(RefCell::new(None));
    let captured = capture.clone();
    let (root, cx) = cx.add_window_view(move |window, cx| {
        let view = cx.new(|cx| SessionsView::new(window, services, cx));
        view.update(cx, |view, cx| view.set_active(true, window, cx));
        captured.replace(Some(view.clone()));
        let harness = cx.new(|_| Harness { view });
        Root::new(harness, window, cx)
    });
    let view = capture.borrow_mut().take().unwrap();
    complete(cx, &completion, "gpui-sessions-query");
    cx.update(|window, app| {
        view.update(app, |view, cx| {
            view.username
                .update(cx, |input, cx| input.set_value("other", window, cx))
        })
    });
    draw(&root, cx);
    click(cx, "sessions-apply");
    complete(cx, &completion, "gpui-sessions-query");
    cx.read(|app| {
        let page = view.read(app).store.page.as_ref().unwrap();
        assert_eq!(page.rows.len(), 1);
        assert_eq!(page.rows[0].username, "other");
        assert!(!page.rows[0].current);
    });
    draw(&root, cx);
    click(cx, "session-revoke-0");
    draw(&root, cx);
    click(cx, "sessions-revoke-cancel");
    assert!(
        fixture
            .runtime
            .block_on(fixture.native.sessions(SessionQuery::default()))
            .unwrap()
            .sessions
            .iter()
            .all(|record| record.revoked_at_epoch_ms.is_none())
    );
    draw(&root, cx);
    click(cx, "session-revoke-0");
    let confirmation = cx.read(|app| view.read(app).confirmation_input_for_test(app).unwrap());
    cx.update(|window, app| confirmation.update(app, |input, cx| input.set_value("operator", window, cx)));
    draw(&root, cx);
    click(cx, "sessions-revoke-submit");
    assert!(cx.read(|app| view.read(app).form.as_ref().unwrap().read(app).error.is_some()));
    assert!(
        completion.try_recv().is_err(),
        "invalid username confirmation must not call the service"
    );
    cx.update(|window, app| confirmation.update(app, |input, cx| input.set_value("other", window, cx)));
    let (terminal_started, terminal_release) = audit_services.hold_next_audit_terminal();
    draw(&root, cx);
    click(cx, "sessions-revoke-submit");
    click(cx, "sessions-revoke-submit");
    fixture.wait_for_terminal(terminal_started, cx);
    terminal_release.send(()).unwrap();
    complete(cx, &completion, "gpui-session-revoke-all");
    complete(cx, &completion, "gpui-sessions-query");
    fixture.runtime.block_on(async {
        let document = fixture.native.load_desktop().await.unwrap().unwrap().document;
        let events = document
            .audit
            .query(AuditQuery {
                filter: AuditFilter {
                    action: Some(AuditAction::SessionRevokeAll),
                    ..AuditFilter::recent(now_epoch_ms().unwrap())
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
        assert_eq!(
            events[0].operation.context.resource,
            OperationResource::SessionUser("other".into())
        );
        assert!(fixture.auth.session().is_authenticated());
        let report = context.task_group().shutdown(Duration::from_secs(5)).await;
        assert!(report.is_healthy());
    });
    fixture.finish();
}

#[gpui::test]
fn sessions_cursor_buttons_preserve_the_first_creation_window_after_a_later_record(cx: &mut gpui::TestAppContext) {
    cx.update(gpui_component::init);
    let fixture = Fixture::new();
    let context = fixture.runtime.root_context().component("cursor-ui");
    let services = AppServices::desktop_with_fake_provider(
        fixture.config.clone(),
        fixture.auth.clone(),
        context.clone(),
        fixture.runtime.root_context().component("history"),
        fixture.runtime.root_context().component("monitor"),
    );
    fixture.runtime.block_on(services.bootstrap()).unwrap();
    fixture.login();
    fixture.runtime.block_on(async {
        let now = now_epoch_ms().unwrap();
        for index in 0..27 {
            fixture
                .native
                .create_session(
                    SessionRecord::new("other".into(), now - 1_000 + index, now + 60_000).unwrap(),
                    now,
                )
                .await
                .unwrap();
        }
    });
    let (sender, completion) = mpsc::channel();
    let services = services.with_runtime_completion(sender);
    let capture = Rc::new(RefCell::new(None));
    let captured = capture.clone();
    let (root, cx) = cx.add_window_view(move |window, cx| {
        let view = cx.new(|cx| SessionsView::new(window, services, cx));
        view.update(cx, |view, cx| view.set_active(true, window, cx));
        captured.replace(Some(view.clone()));
        let harness = cx.new(|_| Harness { view });
        Root::new(harness, window, cx)
    });
    let view = capture.borrow_mut().take().unwrap();
    complete(cx, &completion, "gpui-sessions-query");
    cx.update(|window, app| {
        view.update(app, |view, cx| {
            view.username.update(cx, |input, cx| {
                input.set_value("other", window, cx);
                input.focus(window, cx);
            })
        })
    });
    draw(&root, cx);
    cx.simulate_keystrokes("enter");
    complete(cx, &completion, "gpui-sessions-query");
    let first = cx.read(|app| {
        view.read(app)
            .store
            .page
            .as_ref()
            .unwrap()
            .rows
            .iter()
            .map(|row| row.created_at_epoch_ms)
            .collect::<Vec<_>>()
    });
    assert_eq!(first.len(), 25);
    fixture.runtime.block_on(async {
        let now = now_epoch_ms().unwrap();
        fixture
            .native
            .create_session(SessionRecord::new("other".into(), now, now + 60_000).unwrap(), now)
            .await
            .unwrap();
    });
    draw(&root, cx);
    click(cx, "sessions-next");
    complete(cx, &completion, "gpui-sessions-query");
    cx.read(|app| {
        assert_eq!(view.read(app).store.page_index, 1);
        assert_eq!(view.read(app).store.page.as_ref().unwrap().rows.len(), 2);
    });
    draw(&root, cx);
    click(cx, "sessions-previous");
    complete(cx, &completion, "gpui-sessions-query");
    let restored = cx.read(|app| {
        view.read(app)
            .store
            .page
            .as_ref()
            .unwrap()
            .rows
            .iter()
            .map(|row| row.created_at_epoch_ms)
            .collect::<Vec<_>>()
    });
    assert_eq!(
        restored, first,
        "Previous must not start a new cursor window that includes the later record"
    );
    fixture.runtime.block_on(async {
        assert!(context.task_group().shutdown(Duration::from_secs(5)).await.is_healthy());
    });
    fixture.finish();
}
