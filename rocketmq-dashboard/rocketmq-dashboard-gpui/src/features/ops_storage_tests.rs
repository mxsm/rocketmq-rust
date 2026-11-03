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

use super::*;
use crate::services::local_storage::tests::Fixture;

fn mount<'a>(
    cx: &'a mut gpui::TestAppContext,
    fixture: &Fixture,
) -> (
    Entity<Root>,
    Entity<OpsView>,
    mpsc::Receiver<&'static str>,
    &'a mut gpui::VisualTestContext,
) {
    cx.update(gpui_component::init);
    let (sender, completion) = mpsc::channel();
    let services = fixture.services.clone().with_runtime_completion(sender);
    let capture = Rc::new(RefCell::new(None));
    let captured = capture.clone();
    let (root, cx) = cx.add_window_view(move |window, cx| {
        let view = cx.new(|cx| OpsView::new(window, services, cx));
        view.update(cx, |view, cx| view.sync_from_services(window, cx));
        captured.replace(Some(view.clone()));
        Root::new(view, window, cx)
    });
    let view = capture.borrow_mut().take().unwrap();
    cx.simulate_resize(size(px(960.), px(640.)));
    (root, view, completion, cx)
}

fn draw(root: &Entity<Root>, cx: &mut gpui::VisualTestContext) {
    cx.run_until_parked();
    for _ in 0..2 {
        cx.draw(point(px(0.), px(0.)), size(px(960.), px(640.)), |_, _| root.clone());
    }
}

fn reach(root: &Entity<Root>, cx: &mut gpui::VisualTestContext, selector: &'static str) -> gpui::Bounds<gpui::Pixels> {
    for _ in 0..40 {
        draw(root, cx);
        let viewport = cx.debug_bounds("ops-page-viewport").unwrap();
        let control = cx
            .debug_bounds(selector)
            .unwrap_or_else(|| panic!("Missing {selector}"));
        if control.top() >= viewport.top() && control.bottom() <= viewport.bottom() {
            return control;
        }
        let delta = if control.top() < viewport.top() { 160. } else { -160. };
        cx.simulate_event(gpui::ScrollWheelEvent {
            position: viewport.center(),
            delta: gpui::ScrollDelta::Pixels(point(px(0.), px(delta))),
            ..Default::default()
        });
    }
    panic!("{selector} must be reachable by actual page scrolling");
}

fn click(root: &Entity<Root>, cx: &mut gpui::VisualTestContext, selector: &'static str) {
    let control = reach(root, cx, selector);
    cx.simulate_click(control.center(), Modifiers::default());
    cx.run_until_parked();
}

fn completed(cx: &mut gpui::VisualTestContext, completion: &mpsc::Receiver<&'static str>, name: &'static str) {
    cx.run_until_parked();
    assert_eq!(completion.recv_timeout(Duration::from_secs(5)).unwrap(), name);
    cx.run_until_parked();
}

fn interval(root: &Entity<Root>, cx: &mut gpui::VisualTestContext, value: &str) {
    let field = reach(root, cx, "ops-history-interval");
    assert!(field.size.width >= px(200.));
    cx.simulate_click(field.center(), Modifiers::default());
    cx.simulate_keystrokes("ctrl-a");
    cx.simulate_input(value);
    cx.run_until_parked();
}

struct CompactOps(Entity<OpsView>);

impl gpui::Render for CompactOps {
    fn render(&mut self, _: &mut Window, _: &mut Context<Self>) -> impl gpui::IntoElement {
        div()
            .size_full()
            .flex()
            .flex_col()
            .child(div().h(px(160.)).w_full().flex_shrink_0())
            .child(
                div()
                    .flex_1()
                    .min_w_0()
                    .min_h_0()
                    .w_full()
                    .flex()
                    .child(div().w(px(240.)).h_full().flex_shrink_0())
                    .child(div().flex_1().min_w_0().h_full().child(self.0.clone())),
            )
    }
}

#[gpui::test]
fn compact_ops_embedded_authority_keeps_values_readable_and_history_below_it(cx: &mut gpui::TestAppContext) {
    let fixture = Fixture::new();
    let (_, native) = crate::services::storage_setup::test_support::FakeStorageSetup::services(true);
    let mut status = fixture.status();
    status.native_storage = native.snapshot.lock().clone();
    status
        .native_storage
        .status
        .as_mut()
        .unwrap()
        .last_successful_write_epoch_ms = Some(1_700_000_000_000);
    let services = fixture.services.clone();
    let native_snapshot = status.native_storage.clone();
    cx.update(|cx| {
        gpui_component::init(cx);
        crate::theme::apply_dark_theme(cx);
    });
    let capture = Rc::new(RefCell::new(None));
    let captured = capture.clone();
    let (root, cx) = cx.add_window_view(move |window, cx| {
        let view = cx.new(|cx| OpsView::new(window, services, cx));
        view.update(cx, |view, cx| {
            view.sync_from_services(window, cx);
            view.section = OpsSection::Storage;
            view.storage.status = Some(status);
        });
        captured.replace(Some(view.clone()));
        let host = cx.new(|_| CompactOps(view));
        Root::new(host, window, cx)
    });
    let view = capture.borrow_mut().take().unwrap();
    cx.simulate_resize(size(px(960.), px(640.)));
    draw(&root, cx);
    cx.update(|_, app| {
        view.update(app, |view, cx| {
            view.native_storage
                .update(cx, |native, cx| native.sync_snapshot(native_snapshot, cx));
        })
    });
    draw(&root, cx);
    let viewport = cx.debug_bounds("ops-page-viewport").unwrap();
    let authority = cx.debug_bounds("storage-authority-status").unwrap();
    assert!(
        authority.size.width >= viewport.size.width * 0.8,
        "Embedded authority must use the available OPS pane: {authority:?}, {viewport:?}"
    );
    for label in ["Authority", "Status checked", "Last write recorded by shared authority"] {
        let value = cx
            .debug_bounds(match label {
                "Authority" => "ops-value-text-Authority",
                "Status checked" => "ops-value-text-Status checked",
                _ => "ops-value-text-Last write recorded by shared authority",
            })
            .unwrap();
        let row = cx
            .debug_bounds(match label {
                "Authority" => "ops-value-row-Authority",
                "Status checked" => "ops-value-row-Status checked",
                _ => "ops-value-row-Last write recorded by shared authority",
            })
            .unwrap();
        assert!(
            value.size.width > row.size.width / 3.,
            "Values need a readable column, not one character per line: {label}: {value:?}, {row:?}"
        );
        assert!(
            value.bottom() <= row.bottom(),
            "Wrapped values must stay inside their row: {label}"
        );
    }
    let engine = cx.debug_bounds("storage-setup-content").unwrap();
    let history = cx.debug_bounds("ops-history-heading").unwrap();
    assert!(
        history.top() >= engine.bottom(),
        "History controls must follow the embedded engine panel: {engine:?}, {history:?}"
    );
    interval(&root, cx, "45");
    assert_eq!(
        cx.read(|app| view.read(app).storage.inputs[0].read(app).value().to_string()),
        "45"
    );
    fixture.runtime.shutdown_runtime_blocking().unwrap();
}

#[gpui::test]
fn storage_controls_save_zero_interval_preserve_stale_draft_reconfirm_and_stop_the_owned_collector(
    cx: &mut gpui::TestAppContext,
) {
    let fixture = Fixture::new();
    let (root, view, completion, cx) = mount(cx, &fixture);
    click(&root, cx, "ops-storage");
    completed(cx, &completion, "gpui-local-storage-status");
    assert_eq!(
        cx.read(|app| view
            .read(app)
            .storage
            .status
            .as_ref()
            .unwrap()
            .configuration
            .evidence
            .read),
        FileReadState::Readable
    );
    click(&root, cx, "ops-history-enabled");
    interval(&root, cx, "0");
    click(&root, cx, "ops-save-history");
    completed(cx, &completion, "gpui-config-mutation");
    completed(cx, &completion, "gpui-local-storage-status");
    assert!(fixture.services.connection_state().config.foundations.history_enabled);
    assert_eq!(
        fixture
            .services
            .connection_state()
            .config
            .foundations
            .history_interval_seconds,
        0
    );
    assert!(!fixture.status().collector_running);
    interval(&root, cx, "30");
    fixture
        .mutate(ConfigMutation::AddNameServer("new:9876".into()))
        .unwrap();
    cx.update(|window, app| view.update(app, |view, cx| view.sync_from_services(window, cx)));
    click(&root, cx, "ops-save-history");
    completed(cx, &completion, "gpui-config-mutation");
    completed(cx, &completion, "gpui-local-storage-status");
    cx.read(|app| {
        let view = view.read(app);
        assert!(view.error.is_some() && view.storage.dirty);
        assert_eq!(view.storage.inputs[0].read(app).value(), "30");
        assert_eq!(view.storage.revision, 1);
        assert_eq!(view.config.revision, 2);
    });
    assert_eq!(
        fixture
            .services
            .connection_state()
            .config
            .foundations
            .history_interval_seconds,
        0
    );
    click(&root, cx, "ops-history-use-current-revision");
    click(&root, cx, "ops-save-history");
    completed(cx, &completion, "gpui-config-mutation");
    completed(cx, &completion, "gpui-local-storage-status");
    assert_eq!(
        fixture
            .services
            .connection_state()
            .config
            .foundations
            .history_interval_seconds,
        30
    );
    assert_eq!(fixture.history.task_group().task_count(), 1);
    assert!(!cx.read(|app| view.read(app).storage.dirty));
    click(&root, cx, "ops-history-enabled");
    click(&root, cx, "ops-save-history");
    completed(cx, &completion, "gpui-config-mutation");
    completed(cx, &completion, "gpui-local-storage-status");
    assert_eq!(fixture.history.task_group().task_count(), 0);
    assert!(!fixture.status().collector_running);
    assert!(reach(&root, cx, "ops-open-config-directory").size.width > px(100.));
    fixture.runtime.shutdown_runtime_blocking().unwrap();
}

#[gpui::test]
fn status_refresh_reports_real_read_failure_and_recovery_without_discarding_history_inputs(
    cx: &mut gpui::TestAppContext,
) {
    let fixture = Fixture::new();
    let (root, view, completion, cx) = mount(cx, &fixture);
    click(&root, cx, "ops-storage");
    completed(cx, &completion, "gpui-local-storage-status");
    interval(&root, cx, "invalid");
    let original = std::fs::read(fixture.store.path()).unwrap();
    let last_success = cx.read(|app| {
        view.read(app)
            .storage
            .status
            .as_ref()
            .unwrap()
            .configuration
            .evidence
            .last_successful_write_epoch_ms
    });
    std::fs::write(fixture.store.path(), b"{invalid").unwrap();
    click(&root, cx, "refresh-storage-status");
    completed(cx, &completion, "gpui-local-storage-status");
    cx.read(|app| {
        let view = view.read(app);
        let evidence = view.storage.status.as_ref().unwrap().configuration.evidence;
        assert_eq!(evidence.read, FileReadState::Failed);
        assert_eq!(evidence.last_write, FileWriteState::Succeeded);
        assert_eq!(evidence.last_successful_write_epoch_ms, last_success);
        assert_eq!(view.storage.inputs[0].read(app).value(), "invalid");
        assert!(view.storage.dirty);
    });
    std::fs::write(fixture.store.path(), original).unwrap();
    click(&root, cx, "refresh-storage-status");
    completed(cx, &completion, "gpui-local-storage-status");
    assert_eq!(
        cx.read(|app| view
            .read(app)
            .storage
            .status
            .as_ref()
            .unwrap()
            .configuration
            .evidence
            .read),
        FileReadState::Readable
    );
    assert_eq!(
        cx.read(|app| view.read(app).storage.inputs[0].read(app).value()),
        "invalid"
    );
    click(&root, cx, "ops-discard-history");
    assert!(!cx.read(|app| view.read(app).storage.dirty));
    assert_eq!(cx.read(|app| view.read(app).storage.inputs[0].read(app).value()), "60");
    fixture.runtime.shutdown_runtime_blocking().unwrap();
}
