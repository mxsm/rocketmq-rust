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
use crate::services::acl::test_support::{Fixture, user};
use gpui::{Modifiers, point, size};
use gpui_component::Root;
use std::{cell::RefCell, rc::Rc, sync::mpsc, time::Duration};

struct Harness {
    view: Entity<AclView>,
}
impl Render for Harness {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        div()
            .size_full()
            .child(self.view.clone())
            .children(Root::render_dialog_layer(window, cx))
    }
}
fn open(
    cx: &mut gpui::TestAppContext,
    services: AppServices,
) -> (Entity<Root>, Entity<AclView>, &mut gpui::VisualTestContext) {
    cx.update(gpui_component::init);
    let capture = Rc::new(RefCell::new(None));
    let captured = capture.clone();
    let (root, cx) = cx.add_window_view(move |window, cx| {
        let view = cx.new(|cx| AclView::new(window, services, 0, cx));
        view.update(cx, |view, cx| view.set_active(true, window, cx));
        captured.replace(Some(view.clone()));
        let harness = cx.new(|_| Harness { view });
        Root::new(harness, window, cx)
    });
    let view = capture.borrow_mut().take().unwrap();
    (root, view, cx)
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
        .unwrap_or_else(|| panic!("Missing actual ACL control {selector}"));
    cx.simulate_click(bounds.center(), Modifiers::default());
    cx.run_until_parked();
}
fn completed(cx: &mut gpui::VisualTestContext, completion: &mpsc::Receiver<&'static str>, expected: &'static str) {
    cx.run_until_parked();
    assert_eq!(
        completion
            .recv_timeout(Duration::from_secs(5))
            .unwrap_or_else(|error| panic!("Missing {expected}: {error}")),
        expected
    );
    cx.run_until_parked();
}
fn committed(cx: &mut gpui::VisualTestContext, completion: &mpsc::Receiver<&'static str>, users: bool) {
    completed(cx, completion, "gpui-service-acl-write");
    completed(
        cx,
        completion,
        if users {
            "gpui-service-acl-users"
        } else {
            "gpui-service-acl-policies"
        },
    );
}
fn cluster(view: &Entity<AclView>, value: &str, cx: &mut gpui::VisualTestContext) {
    cx.update(|window, app| {
        view.update(app, |view, cx| {
            view.cluster.update(cx, |picker, cx| {
                picker.set_selected_value(&value.to_owned(), window, cx);
                cx.emit(SelectEvent::Confirm(Some(value.to_owned())));
            });
        });
    });
    cx.run_until_parked();
}
fn user_form(view: &Entity<AclView>, cx: &mut gpui::VisualTestContext) -> Entity<AclUserDialog> {
    cx.read(|app| match view.read(app).form.as_ref().unwrap() {
        AclForm::User(form) => form.clone(),
        _ => panic!("Expected user form"),
    })
}
fn policy_form(view: &Entity<AclView>, cx: &mut gpui::VisualTestContext) -> Entity<AclPolicyDialog> {
    cx.read(|app| match view.read(app).form.as_ref().unwrap() {
        AclForm::Policy(form) => form.clone(),
        _ => panic!("Expected policy form"),
    })
}
fn set(input: &Entity<InputState>, value: &str, cx: &mut gpui::VisualTestContext) {
    cx.update(|window, app| input.update(app, |input, cx| input.set_value(value.to_owned(), window, cx)));
}
fn enter(cx: &mut gpui::VisualTestContext) {
    cx.simulate_keystrokes("enter");
    cx.run_until_parked();
}

#[gpui::test]
fn acl_bulk_target_notices_scroll_without_hiding_records_or_pagination(cx: &mut gpui::TestAppContext) {
    let fixture = Fixture::new();
    for index in 0..12 {
        let mut user = user();
        user.username = format!("user-{index:02}");
        fixture.backend.users.lock().unwrap().push(user);
    }
    let (sender, completion) = mpsc::channel();
    let (root, view, cx) = open(cx, fixture.services.clone().with_runtime_completion(sender));
    draw(&root, cx);
    cluster(&view, "cluster-a", cx);
    draw(&root, cx);
    click(cx, "acl-confirm");
    completed(cx, &completion, "gpui-service-acl-users");
    let targets = (0..ACL_TARGET_LIMIT)
        .map(|index| {
            let mut target = crate::services::acl::test_support::target();
            target.broker_name = format!("broker-{index:02}");
            target.address = format!("broker-{index:02}:10911");
            target
        })
        .collect::<Vec<_>>();
    for failures in [true, false] {
        cx.update(|_, app| {
            view.update(app, |view, cx| {
                if let Loadable::Ready(read) = &mut view.store.users.state {
                    read.failures = if failures {
                        targets
                            .iter()
                            .map(|target| AclTargetFailure {
                                target: target.clone(),
                                failure: AclFailure::AccessDenied,
                            })
                            .collect()
                    } else {
                        Vec::new()
                    };
                }
                view.write_result = (!failures).then(|| AclWriteResult {
                    targets: targets
                        .iter()
                        .map(|target| AclWriteTarget {
                            target: target.clone(),
                            state: AclWriteState::Unknown,
                        })
                        .collect(),
                });
                cx.notify();
            })
        });
        for (width, height) in [(930., 620.), (1440., 900.)] {
            let viewport = size(px(width), px(height));
            cx.simulate_resize(viewport);
            for _ in 0..2 {
                cx.draw(point(px(0.), px(0.)), viewport, |_, _| root.clone());
            }
            let notices = cx.debug_bounds("acl-result-notices").unwrap();
            let records = cx.debug_bounds("acl-records").unwrap();
            let footer = cx.debug_bounds("acl-pagination").unwrap();
            assert!(
                notices.size.height <= px(112.),
                "Notice bounds at {width}x{height}: {notices:?}"
            );
            assert!(
                records.size.height >= px(24.),
                "Record list must remain reachable at {width}x{height}"
            );
            assert!(footer.top() >= records.bottom() && footer.bottom() <= px(height));
            let (first, last) = if failures {
                ("acl-read-failure-0", "acl-read-failure-63")
            } else {
                ("acl-write-target-0", "acl-write-target-63")
            };
            cx.simulate_event(gpui::ScrollWheelEvent {
                position: notices.center(),
                delta: gpui::ScrollDelta::Pixels(point(px(0.), px(10000.))),
                ..Default::default()
            });
            for _ in 0..2 {
                cx.draw(point(px(0.), px(0.)), viewport, |_, _| root.clone());
            }
            let initial = cx.debug_bounds(first).unwrap();
            cx.simulate_event(gpui::ScrollWheelEvent {
                position: notices.center(),
                delta: gpui::ScrollDelta::Pixels(point(px(0.), px(-10000.))),
                ..Default::default()
            });
            for _ in 0..2 {
                cx.draw(point(px(0.), px(0.)), viewport, |_, _| root.clone());
            }
            let last = cx.debug_bounds(last).unwrap();
            assert!(cx.debug_bounds(first).unwrap().top() < initial.top());
            assert!(
                last.top() >= notices.top() && last.bottom() <= notices.bottom(),
                "Last target {last:?} must be reachable within {notices:?} at {width}x{height}"
            );
            click(cx, "acl-next");
            cx.read(|app| assert_eq!(view.read(app).store.page(), 1));
            click(cx, "acl-prev");
            cx.read(|app| assert_eq!(view.read(app).store.page(), 0));
        }
    }
    fixture.runtime.shutdown_runtime_blocking().unwrap();
}

#[gpui::test]
fn acl_scope_change_cancels_old_secret_query_and_leave_clears_a_retained_form(cx: &mut gpui::TestAppContext) {
    let fixture = Fixture::new();
    fixture.backend.users.lock().unwrap().push(user());
    let (started, _release, done) = fixture.backend.hold_query();
    let (sender, completion) = mpsc::channel();
    let (root, view, cx) = open(cx, fixture.services.clone().with_runtime_completion(sender));
    draw(&root, cx);
    cluster(&view, "cluster-a", cx);
    draw(&root, cx);
    click(cx, "acl-confirm");
    fixture.runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(5), started)
            .await
            .unwrap()
            .unwrap();
    });
    cluster(&view, "other-cluster", cx);
    fixture.runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(5), done)
            .await
            .unwrap()
            .unwrap();
    });
    completed(cx, &completion, "gpui-service-acl-users");
    cx.read(|app| {
        assert!(view.read(app).store.users.state.value().is_none());
        assert!(view.read(app).store.visible.is_empty());
    });
    cluster(&view, "cluster-a", cx);
    draw(&root, cx);
    click(cx, "acl-confirm");
    completed(cx, &completion, "gpui-service-acl-users");
    draw(&root, cx);
    click(cx, "acl-user-edit-0");
    draw(&root, cx);
    click(cx, "acl-change-password");
    let form = user_form(&view, cx);
    let secret_input = cx.read(|app| form.read(app).password.clone());
    set(&secret_input, "leaving-secret", cx);
    cx.update(|window, app| view.update(app, |view, cx| view.leave(window, cx)));
    cx.read(|app| {
        assert!(view.read(app).form.is_none());
        assert!(view.read(app).store.confirmed.is_none());
        assert!(secret_input.read(app).value().is_empty());
    });
    fixture.runtime.shutdown_runtime_blocking().unwrap();
}

#[gpui::test]
fn acl_official_user_and_policy_controls_require_confirmation_and_submit_structured_crud(
    cx: &mut gpui::TestAppContext,
) {
    let fixture = Fixture::new();
    let (sender, completion) = mpsc::channel();
    let (root, view, cx) = open(cx, fixture.services.clone().with_runtime_completion(sender));
    draw(&root, cx);
    assert!(fixture.backend.user_calls.lock().unwrap().is_empty());
    assert!(fixture.backend.policy_calls.lock().unwrap().is_empty());
    click(cx, "acl-refresh");
    assert!(fixture.backend.user_calls.lock().unwrap().is_empty());
    cluster(&view, "cluster-a", cx);
    draw(&root, cx);
    click(cx, "acl-confirm");
    completed(cx, &completion, "gpui-service-acl-users");
    draw(&root, cx);
    click(cx, "acl-create");
    draw(&root, cx);
    let form = user_form(&view, cx);
    cx.read(|app| assert!(form.read(app).password.read(app).value().is_empty()));
    set(&cx.read(|app| form.read(app).username.clone()), "alice", cx);
    set(&cx.read(|app| form.read(app).password.clone()), "explicit-secret", cx);
    click(cx, "acl-user-status-1");
    enter(cx);
    committed(cx, &completion, true);
    {
        let calls = fixture.backend.writes.lock().unwrap();
        assert!(
            matches!(&calls[0].change,AclChange::SaveUser{create:true,password:Some(password),status,..}if password.expose()=="explicit-secret"&&status=="disable")
        );
    }
    draw(&root, cx);
    click(cx, "acl-user-edit-0");
    draw(&root, cx);
    let edit = user_form(&view, cx);
    cx.read(|app| {
        assert!(!edit.read(app).replace_password);
        assert!(edit.read(app).password.read(app).value().is_empty());
    });
    click(cx, "acl-user-type-1");
    click(cx, "acl-user-status-0");
    enter(cx);
    committed(cx, &completion, true);
    assert!(
        matches!(&fixture.backend.writes.lock().unwrap()[1].change,AclChange::SaveUser{create:false,password:None,user_type,status,..}if user_type=="Super"&&status=="enable")
    );
    draw(&root, cx);
    click(cx, "acl-policies");
    completed(cx, &completion, "gpui-service-acl-policies");
    draw(&root, cx);
    click(cx, "acl-create");
    draw(&root, cx);
    let policy = policy_form(&view, cx);
    set(&cx.read(|app| policy.read(app).subject.clone()), "User:alice", cx);
    set(&cx.read(|app| policy.read(app).resource.clone()), "Topic:orders", cx);
    set(
        &cx.read(|app| policy.read(app).source_ips.clone()),
        "127.0.0.1,10.0.0.0/8",
        cx,
    );
    click(cx, "acl-action-2");
    click(cx, "acl-decision-1");
    enter(cx);
    committed(cx, &completion, false);
    assert!(
        matches!(&fixture.backend.writes.lock().unwrap()[2].change,AclChange::SavePolicy{entry,..}if entry.resource=="Topic:orders"&&entry.actions==["Pub","Sub"]&&entry.source_ips==["127.0.0.1","10.0.0.0/8"]&&entry.decision=="Deny")
    );
    draw(&root, cx);
    click(cx, "acl-policy-edit-0");
    draw(&root, cx);
    click(cx, "acl-decision-0");
    enter(cx);
    committed(cx, &completion, false);
    draw(&root, cx);
    click(cx, "acl-policy-delete-0");
    draw(&root, cx);
    enter(cx);
    committed(cx, &completion, false);
    assert!(fixture.backend.policies.lock().unwrap().is_empty());
    draw(&root, cx);
    click(cx, "acl-users");
    completed(cx, &completion, "gpui-service-acl-users");
    draw(&root, cx);
    click(cx, "acl-user-delete-0");
    draw(&root, cx);
    enter(cx);
    committed(cx, &completion, true);
    assert!(fixture.backend.users.lock().unwrap().is_empty());
    assert_eq!(fixture.backend.writes.lock().unwrap().len(), 6);
    assert_eq!(
        fixture
            .events()
            .iter()
            .filter(|event| matches!(
                event.operation.action,
                AuditAction::AclUserCreated
                    | AuditAction::AclUserUpdated
                    | AuditAction::AclUserDeleted
                    | AuditAction::AclPolicyCreated
                    | AuditAction::AclPolicyUpdated
                    | AuditAction::AclPolicyDeleted
            ))
            .count(),
        6
    );
    fixture.runtime.shutdown_runtime_blocking().unwrap();
}

#[gpui::test]
fn acl_password_entities_history_and_returned_values_clear_on_close_tab_and_scope(cx: &mut gpui::TestAppContext) {
    let fixture = Fixture::new();
    fixture.backend.users.lock().unwrap().push(user());
    let (sender, completion) = mpsc::channel();
    let (root, view, cx) = open(cx, fixture.services.clone().with_runtime_completion(sender));
    draw(&root, cx);
    cluster(&view, "cluster-a", cx);
    draw(&root, cx);
    click(cx, "acl-confirm");
    completed(cx, &completion, "gpui-service-acl-users");
    draw(&root, cx);
    click(cx, "acl-show-passwords");
    cx.read(|app| assert!(view.read(app).show_passwords));
    click(cx, "acl-user-edit-0");
    draw(&root, cx);
    click(cx, "acl-change-password");
    let form = user_form(&view, cx);
    let old = cx.read(|app| form.read(app).password.clone());
    set(&old, "never-undo-this-secret", cx);
    cx.simulate_keystrokes("escape");
    cx.run_until_parked();
    cx.read(|app| assert!(old.read(app).value().is_empty()));
    draw(&root, cx);
    click(cx, "acl-user-edit-0");
    draw(&root, cx);
    click(cx, "acl-change-password");
    let newer = user_form(&view, cx);
    let fresh = cx.read(|app| newer.read(app).password.clone());
    assert_ne!(old.entity_id(), fresh.entity_id());
    draw(&root, cx);
    click(cx, "acl-password");
    cx.simulate_keystrokes("ctrl-z");
    cx.run_until_parked();
    cx.read(|app| assert!(fresh.read(app).value().is_empty()));
    set(&fresh, "scope-draft-secret", cx);
    cluster(&view, "other-cluster", cx);
    cx.read(|app| {
        let view = view.read(app);
        assert!(view.form.is_none());
        assert!(view.store.confirmed.is_none());
        assert!(view.store.users.state.value().is_none());
        assert!(!view.show_passwords);
        assert!(fresh.read(app).value().is_empty());
    });
    assert_eq!(fixture.backend.user_calls.lock().unwrap().len(), 1);
    cluster(&view, "cluster-a", cx);
    draw(&root, cx);
    click(cx, "acl-confirm");
    completed(cx, &completion, "gpui-service-acl-users");
    draw(&root, cx);
    click(cx, "acl-show-passwords");
    click(cx, "acl-policies");
    completed(cx, &completion, "gpui-service-acl-policies");
    cx.read(|app| {
        assert!(!view.read(app).show_passwords);
        assert!(view.read(app).store.users.state.value().is_none());
    });
    fixture.runtime.shutdown_runtime_blocking().unwrap();
}

#[gpui::test]
fn acl_users_and_policies_search_pages_are_independent_and_partial_attempt_cannot_replay(
    cx: &mut gpui::TestAppContext,
) {
    let fixture = Fixture::new();
    for index in 0..12 {
        let mut user = user();
        user.username = format!("user-{index:02}");
        fixture.backend.users.lock().unwrap().push(user);
    }
    let (sender, completion) = mpsc::channel();
    let (root, view, cx) = open(cx, fixture.services.clone().with_runtime_completion(sender));
    draw(&root, cx);
    cluster(&view, "cluster-a", cx);
    draw(&root, cx);
    click(cx, "acl-confirm");
    completed(cx, &completion, "gpui-service-acl-users");
    cx.read(|app| assert_eq!(view.read(app).store.visible.len(), 10));
    draw(&root, cx);
    click(cx, "acl-next");
    cx.read(|app| assert_eq!(view.read(app).store.visible.len(), 2));
    let input = cx.read(|app| view.read(app).users_search.clone());
    set(&input, "user-11", cx);
    click(cx, "acl-search");
    cx.read(|app| assert_eq!(view.read(app).store.visible.len(), 1));
    click(cx, "acl-policies");
    completed(cx, &completion, "gpui-service-acl-policies");
    cx.read(|app| assert!(view.read(app).store.policies_search.is_empty()));
    fixture.backend.results.lock().unwrap().push_back(AclWriteResult {
        targets: vec![AclWriteTarget {
            target: crate::services::acl::test_support::target(),
            state: AclWriteState::Unknown,
        }],
    });
    draw(&root, cx);
    click(cx, "acl-create");
    draw(&root, cx);
    let form = policy_form(&view, cx);
    set(&cx.read(|app| form.read(app).subject.clone()), "User:user-11", cx);
    set(&cx.read(|app| form.read(app).resource.clone()), "Topic:orders", cx);
    enter(cx);
    committed(cx, &completion, false);
    enter(cx);
    assert_eq!(fixture.backend.writes.lock().unwrap().len(), 1);
    cx.read(|app| assert!(form.read(app).attempted));
    cx.simulate_keystrokes("escape");
    cx.run_until_parked();
    draw(&root, cx);
    click(cx, "acl-users");
    completed(cx, &completion, "gpui-service-acl-users");
    cx.read(|app| {
        assert_eq!(view.read(app).store.users_search, "user-11");
        assert_eq!(view.read(app).store.visible.len(), 1);
    });
    fixture.runtime.shutdown_runtime_blocking().unwrap();
}
