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

use gpui::{Modifiers, point, px, size};
use gpui_component::Root;
use rocketmq_dashboard_common::{AuditAction, AuditOutcome, MonitorRuleStatus};

use super::*;
use crate::services::{
    ConfigMutation,
    monitors::tests::{Fixture, saved},
};

struct Harness {
    monitors: Entity<MonitorsView>,
}
impl Render for Harness {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        div()
            .size_full()
            .child(self.monitors.clone())
            .children(Root::render_dialog_layer(window, cx))
    }
}

fn draw(root: &Entity<Root>, cx: &mut gpui::VisualTestContext) {
    cx.run_until_parked();
    cx.draw(point(px(0.), px(0.)), size(px(1440.), px(900.)), |_, _| root.clone());
    cx.draw(point(px(0.), px(0.)), size(px(1440.), px(900.)), |_, _| root.clone());
}

fn click(cx: &mut gpui::VisualTestContext, selector: &'static str) {
    let bounds = cx
        .debug_bounds(selector)
        .unwrap_or_else(|| panic!("Missing actual control {selector}"));
    cx.simulate_click(bounds.center(), Modifiers::default());
    cx.run_until_parked();
}

fn completed(cx: &mut gpui::VisualTestContext, completion: &mpsc::Receiver<&'static str>, expected: &'static str) {
    cx.run_until_parked();
    assert_eq!(
        completion
            .recv_timeout(Duration::from_secs(5))
            .unwrap_or_else(|error| panic!("Missing {expected} completion: {error}")),
        expected
    );
    cx.run_until_parked();
}

fn committed(cx: &mut gpui::VisualTestContext, completion: &mpsc::Receiver<&'static str>) {
    completed(cx, completion, "gpui-monitor-rule-commit");
    completed(cx, completion, "gpui-monitor-rules");
}

fn fill(
    view: &Entity<MonitorsView>,
    group: &str,
    min: &str,
    max: &str,
    cx: &mut gpui::VisualTestContext,
) -> Entity<MonitorDialog> {
    let form = cx.read(|app| view.read(app).form.clone().unwrap());
    cx.update(|window, app| {
        form.update(app, |form, cx| {
            if !group.is_empty() {
                form.group
                    .update(cx, |input, cx| input.set_value(group.to_owned(), window, cx));
            }
            form.min_count
                .update(cx, |input, cx| input.set_value(min.to_owned(), window, cx));
            form.max_diff_total
                .update(cx, |input, cx| input.set_value(max.to_owned(), window, cx));
        });
    });
    form
}

#[gpui::test]
fn monitor_dialog_keeps_footer_visible_and_form_scrollable_after_narrowing(cx: &mut gpui::TestAppContext) {
    cx.update(gpui_component::init);
    let fixture = Fixture::new();
    let (sender, completion) = mpsc::channel();
    let services = fixture.services.clone().with_runtime_completion(sender);
    let capture = Rc::new(RefCell::new(None));
    let captured = capture.clone();
    let (root, cx) = cx.add_window_view(move |window, cx| {
        let monitors = cx.new(|cx| MonitorsView::new(window, services, 0, cx));
        monitors.update(cx, |view, cx| view.set_active(true, window, cx));
        captured.replace(Some(monitors.clone()));
        let harness = cx.new(|_| Harness { monitors });
        Root::new(harness, window, cx)
    });
    let view = capture.borrow_mut().take().unwrap();
    completed(cx, &completion, "gpui-monitor-rules");
    cx.simulate_resize(size(px(1440.), px(900.)));
    draw(&root, cx);
    click(cx, "monitor-create");
    fill(&view, "compact-group", "2", "100", cx);

    let mut reached_control_by_scrolling = false;
    for (width, height) in [(930., 620.), (954., 437.), (1440., 900.), (960., 640.)] {
        let viewport = size(px(width), px(height));
        cx.simulate_resize(viewport);
        for _ in 0..2 {
            cx.draw(point(px(0.), px(0.)), viewport, |_, _| root.clone());
        }
        let save = cx.debug_bounds("monitor-dialog-save").unwrap();
        let close = cx.debug_bounds("monitor-dialog-close").unwrap();
        let body = cx.debug_bounds("monitor-form-scroll").unwrap();
        assert!(save.top() >= body.bottom() && close.top() >= body.bottom());
        assert!(save.bottom() <= px(height) && close.bottom() <= px(height));
        assert!(save.right() <= px(width) && close.left() >= px(0.));
        if height < 700. {
            cx.simulate_event(gpui::ScrollWheelEvent {
                position: body.center(),
                delta: gpui::ScrollDelta::Pixels(point(px(0.), px(2000.))),
                ..Default::default()
            });
            cx.draw(point(px(0.), px(0.)), viewport, |_, _| root.clone());
            let body = cx.debug_bounds("monitor-form-scroll").unwrap();
            let initial_group = cx.debug_bounds("monitor-group").unwrap();
            let initially_clipped = cx.debug_bounds("monitor-enabled").unwrap().bottom() > body.bottom();
            cx.simulate_event(gpui::ScrollWheelEvent {
                position: body.center(),
                delta: gpui::ScrollDelta::Pixels(point(px(0.), px(-2000.))),
                ..Default::default()
            });
            cx.draw(point(px(0.), px(0.)), viewport, |_, _| root.clone());
            let scrolled_group = cx.debug_bounds("monitor-group").unwrap();
            let enabled = cx.debug_bounds("monitor-enabled").unwrap();
            let explanation = cx.debug_bounds("monitor-enabled-explanation").unwrap();
            let scrolled_body = cx.debug_bounds("monitor-form-scroll").unwrap();
            let scrolled_save = cx.debug_bounds("monitor-dialog-save").unwrap();
            let moved = scrolled_group.top() < initial_group.top();
            assert!(
                !initially_clipped || moved,
                "form wheel did not reveal controls at {width}x{height}"
            );
            reached_control_by_scrolling |= initially_clipped && moved;
            assert!(enabled.top() >= scrolled_body.top() && enabled.bottom() <= scrolled_body.bottom());
            assert!(enabled.left() >= scrolled_body.left() && enabled.right() <= scrolled_body.right());
            assert!(explanation.left() >= scrolled_body.left() && explanation.right() <= scrolled_body.right());
            assert!(explanation.top() >= enabled.bottom() && explanation.bottom() <= scrolled_body.bottom());
            assert!(
                explanation.size.height > px(20.),
                "the complete explanation should wrap within the form"
            );
            assert!(scrolled_save.top() >= scrolled_body.bottom());
            assert!(scrolled_save.bottom() <= px(height));
            let before = cx.read(|app| view.read(app).form.as_ref().unwrap().read(app).enabled);
            click(cx, "monitor-enabled");
            assert_eq!(
                cx.read(|app| view.read(app).form.as_ref().unwrap().read(app).enabled),
                !before
            );
        }
    }
    assert!(
        reached_control_by_scrolling,
        "compact form must exercise actual scrolling to its lower controls"
    );
    click(cx, "monitor-dialog-save");
    committed(cx, &completion);
    cx.read(|app| assert!(view.read(app).form.is_none()));
    let viewport = size(px(960.), px(640.));
    for _ in 0..2 {
        cx.draw(point(px(0.), px(0.)), viewport, |_, _| root.clone());
    }
    click(cx, "monitor-edit-0");
    cx.draw(point(px(0.), px(0.)), viewport, |_, _| root.clone());
    assert!(cx.debug_bounds("monitor-dialog-close").unwrap().bottom() <= viewport.height);
    assert!(cx.debug_bounds("monitor-dialog-save").unwrap().bottom() <= viewport.height);
    click(cx, "monitor-dialog-close");
    cx.read(|app| assert!(view.read(app).form.is_none()));
    assert_eq!(fixture.events().len(), 1, "Close must not save an edited rule");
    fixture.runtime.shutdown_runtime_blocking().unwrap();
}

#[gpui::test]
fn monitor_official_controls_validate_search_create_edit_reconfirm_conflicts_and_delete(cx: &mut gpui::TestAppContext) {
    cx.update(gpui_component::init);
    let fixture = Fixture::new();
    let (sender, completion) = mpsc::channel();
    let services = fixture.services.clone().with_runtime_completion(sender);
    let capture = Rc::new(RefCell::new(None));
    let captured = capture.clone();
    let (root, cx) = cx.add_window_view(move |window, cx| {
        let monitors = cx.new(|cx| MonitorsView::new(window, services, 0, cx));
        monitors.update(cx, |view, cx| view.set_active(true, window, cx));
        captured.replace(Some(monitors.clone()));
        let harness = cx.new(|_| Harness { monitors });
        Root::new(harness, window, cx)
    });
    let view = capture.borrow_mut().take().unwrap();
    completed(cx, &completion, "gpui-monitor-rules");
    draw(&root, cx);
    click(cx, "monitor-create");
    draw(&root, cx);
    let form = fill(&view, "orders", "-1", "100", cx);
    cx.simulate_keystrokes("enter");
    cx.run_until_parked();
    assert!(fixture.events().is_empty(), "invalid input must not admit a write");
    cx.read(|app| assert!(form.read(app).error.is_some()));
    fill(&view, "orders", "2", "100", cx);
    draw(&root, cx);
    click(cx, "monitor-enabled");
    cx.simulate_keystrokes("enter");
    cx.run_until_parked();
    committed(cx, &completion);
    let first = fixture
        .runtime
        .block_on(fixture.services.monitor_rules(0))
        .unwrap()
        .rules
        .remove(0);
    assert!(!first.enabled);
    assert_eq!(first.thresholds.unwrap().min_count, 2);

    cx.update(|window, app| {
        view.update(app, |view, cx| {
            view.keyword
                .update(cx, |input, cx| input.set_value("missing", window, cx))
        })
    });
    draw(&root, cx);
    click(cx, "monitor-search");
    cx.read(|app| assert_eq!(view.read(app).table.read(app).delegate().rows.len(), 0));
    cx.update(|window, app| {
        view.update(app, |view, cx| {
            view.keyword
                .update(cx, |input, cx| input.set_value("orders", window, cx))
        })
    });
    click(cx, "monitor-search");
    draw(&root, cx);
    click(cx, "monitor-edit-0");
    draw(&root, cx);
    let edit = fill(&view, "", "7", "200", cx);
    let authoritative = saved(
        fixture
            .runtime
            .block_on(
                fixture
                    .services
                    .mutate_monitor_rule(0, fixture.request("orders", first.revision, 9, 900)),
            )
            .unwrap(),
    );
    cx.simulate_keystrokes("enter");
    cx.run_until_parked();
    committed(cx, &completion);
    cx.read(|app| {
        let form = edit.read(app);
        assert_eq!(form.min_count.read(app).value(), "7");
        assert_eq!(form.max_diff_total.read(app).value(), "200");
        assert_eq!(
            form.conflict.as_ref().unwrap().as_ref().unwrap().revision,
            authoritative.revision
        );
    });
    let events = fixture.events().len();
    cx.simulate_keystrokes("enter");
    cx.run_until_parked();
    assert_eq!(
        fixture.events().len(),
        events,
        "a conflict cannot automatically replay the draft"
    );
    draw(&root, cx);
    click(cx, "monitor-reconfirm");
    cx.simulate_keystrokes("enter");
    cx.run_until_parked();
    committed(cx, &completion);
    let updated = fixture
        .runtime
        .block_on(fixture.services.monitor_rules(0))
        .unwrap()
        .rules
        .remove(0);
    assert_eq!(updated.thresholds.unwrap().min_count, 7);
    assert_eq!(updated.thresholds.unwrap().max_diff_total, 200);
    assert!(!updated.enabled, "the reviewed draft retains its enabled intent");
    draw(&root, cx);
    click(cx, "monitor-delete-0");
    draw(&root, cx);
    cx.simulate_keystrokes("enter");
    cx.run_until_parked();
    committed(cx, &completion);
    assert!(
        fixture
            .runtime
            .block_on(fixture.services.monitor_rules(0))
            .unwrap()
            .rules
            .is_empty()
    );
    assert!(
        fixture
            .events()
            .iter()
            .any(|event| event.operation.action == AuditAction::MonitorDeleted
                && event.outcome == AuditOutcome::Succeeded)
    );
    fixture.runtime.shutdown_runtime_blocking().unwrap();
}

#[gpui::test]
fn monitor_legacy_completion_controls_have_no_silent_threshold_defaults(cx: &mut gpui::TestAppContext) {
    cx.update(gpui_component::init);
    let fixture = Fixture::new();
    fixture.schema_three();
    fixture.runtime.block_on(fixture.store.load()).unwrap();
    let (sender, completion) = mpsc::channel();
    let services = fixture.services.clone().with_runtime_completion(sender);
    let capture = Rc::new(RefCell::new(None));
    let captured = capture.clone();
    let (root, cx) = cx.add_window_view(move |window, cx| {
        let monitors = cx.new(|cx| MonitorsView::new(window, services, 0, cx));
        monitors.update(cx, |view, cx| view.set_active(true, window, cx));
        captured.replace(Some(monitors.clone()));
        let harness = cx.new(|_| Harness { monitors });
        Root::new(harness, window, cx)
    });
    let view = capture.borrow_mut().take().unwrap();
    completed(cx, &completion, "gpui-monitor-rules");
    draw(&root, cx);
    click(cx, "monitor-edit-1");
    draw(&root, cx);
    let form = cx.read(|app| view.read(app).form.clone().unwrap());
    cx.read(|app| {
        assert!(form.read(app).min_count.read(app).value().is_empty());
        assert!(form.read(app).max_diff_total.read(app).value().is_empty());
        assert_eq!(form.read(app).expected_revision, 1);
    });
    cx.simulate_keystrokes("enter");
    cx.run_until_parked();
    assert!(fixture.events().is_empty());
    fill(&view, "", "0", "0", cx);
    cx.simulate_keystrokes("enter");
    cx.run_until_parked();
    committed(cx, &completion);
    let snapshot = fixture.runtime.block_on(fixture.services.monitor_rules(0)).unwrap();
    assert!(snapshot.unassigned.is_empty());
    assert_eq!(
        snapshot
            .rules
            .iter()
            .filter(|rule| rule.status == MonitorRuleStatus::Ready)
            .count(),
        1
    );
    assert!(
        snapshot
            .rules
            .iter()
            .any(|rule| rule.status == MonitorRuleStatus::NeedsThresholds)
    );
    fixture.runtime.shutdown_runtime_blocking().unwrap();
}

#[gpui::test]
fn monitor_closed_dialog_owned_commit_refreshes_the_active_list_without_touching_a_new_draft(
    cx: &mut gpui::TestAppContext,
) {
    cx.update(gpui_component::init);
    let fixture = Fixture::new();
    let (started, release) = fixture.hold_next_commit();
    let (sender, completion) = mpsc::channel();
    let services = fixture.services.clone().with_runtime_completion(sender);
    let capture = Rc::new(RefCell::new(None));
    let captured = capture.clone();
    let (root, cx) = cx.add_window_view(move |window, cx| {
        let monitors = cx.new(|cx| MonitorsView::new(window, services, 0, cx));
        monitors.update(cx, |view, cx| view.set_active(true, window, cx));
        captured.replace(Some(monitors.clone()));
        let harness = cx.new(|_| Harness { monitors });
        Root::new(harness, window, cx)
    });
    let view = capture.borrow_mut().take().unwrap();
    completed(cx, &completion, "gpui-monitor-rules");
    draw(&root, cx);
    click(cx, "monitor-create");
    draw(&root, cx);
    fill(&view, "orders", "2", "30", cx);
    cx.simulate_keystrokes("enter");
    cx.run_until_parked();
    fixture.runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(5), started)
            .await
            .unwrap()
            .unwrap();
    });
    cx.simulate_keystrokes("escape");
    cx.run_until_parked();
    draw(&root, cx);
    click(cx, "monitor-create");
    cx.read(|app| {
        assert!(
            view.read(app).form.is_none(),
            "a pending owned write cannot open a second form"
        );
        assert!(view.read(app).store.visible.is_empty());
    });
    release.send(()).unwrap();
    committed(cx, &completion);
    cx.read(|app| {
        let view = view.read(app);
        assert!(view.form.is_none());
        assert!(view.mutation_task.is_none());
        assert_eq!(view.store.visible.len(), 1);
        assert_eq!(view.store.visible[0].consumer_group, "orders");
        assert_eq!(view.store.visible[0].thresholds.unwrap().max_diff_total, 30);
    });
    draw(&root, cx);
    click(cx, "monitor-create");
    draw(&root, cx);
    let next = fill(&view, "next-draft", "4", "50", cx);
    draw(&root, cx);
    cx.read(|app| {
        assert_eq!(view.read(app).form.as_ref().unwrap().entity_id(), next.entity_id());
        assert_eq!(next.read(app).group.read(app).value(), "next-draft");
        assert_eq!(next.read(app).min_count.read(app).value(), "4");
        assert!(!next.read(app).busy);
    });
    let events = fixture.events();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].outcome, AuditOutcome::Succeeded);
    fixture.runtime.shutdown_runtime_blocking().unwrap();
}

#[gpui::test]
fn monitor_environment_clear_drops_drafts_and_stale_results_but_keeps_accepted_write_ownership(
    cx: &mut gpui::TestAppContext,
) {
    cx.update(gpui_component::init);
    let fixture = Fixture::new();
    let original_environment = fixture.environment();
    let (started, release) = fixture.hold_next_commit();
    let (sender, completion) = mpsc::channel();
    let services = fixture.services.clone().with_runtime_completion(sender);
    let capture = Rc::new(RefCell::new(None));
    let captured = capture.clone();
    let (root, cx) = cx.add_window_view(move |window, cx| {
        let monitors = cx.new(|cx| MonitorsView::new(window, services, 0, cx));
        monitors.update(cx, |view, cx| view.set_active(true, window, cx));
        captured.replace(Some(monitors.clone()));
        let harness = cx.new(|_| Harness { monitors });
        Root::new(harness, window, cx)
    });
    let view = capture.borrow_mut().take().unwrap();
    completed(cx, &completion, "gpui-monitor-rules");
    draw(&root, cx);
    click(cx, "monitor-create");
    draw(&root, cx);
    fill(&view, "orders", "1", "10", cx);
    cx.simulate_keystrokes("enter");
    cx.run_until_parked();
    fixture.runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(5), started)
            .await
            .unwrap()
            .unwrap();
    });
    cx.simulate_keystrokes("escape");
    cx.run_until_parked();
    fixture.runtime.block_on(async {
        let (progress, _) = tokio::sync::mpsc::unbounded_channel();
        fixture
            .services
            .mutate_with_progress(ConfigMutation::CreateEnvironment("Staging".into()), progress)
            .await
            .unwrap();
    });
    cx.update(|window, app| {
        view.update(app, |view, cx| {
            view.clear_for_environment(fixture.revision(), window, cx)
        })
    });
    completed(cx, &completion, "gpui-monitor-rules");
    release.send(()).unwrap();
    completed(cx, &completion, "gpui-monitor-rule-commit");
    cx.read(|app| {
        let view = view.read(app);
        assert!(view.form.is_none());
        assert!(view.mutation_task.is_none());
        assert!(view.store.visible.is_empty());
        assert_eq!(
            view.store.rules.state.value().unwrap().environment_id,
            fixture.environment()
        );
    });
    let event = fixture
        .events()
        .into_iter()
        .find(|event| event.operation.action == AuditAction::MonitorSaved)
        .unwrap();
    assert_eq!(event.operation.context.environment_id, original_environment);
    assert_ne!(event.outcome, AuditOutcome::Succeeded);
    assert!(fixture.services.audit_pending().unwrap().is_empty());
    fixture.runtime.shutdown_runtime_blocking().unwrap();
}
