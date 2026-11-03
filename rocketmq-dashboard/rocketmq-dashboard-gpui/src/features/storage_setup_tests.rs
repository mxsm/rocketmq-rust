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
use crate::services::storage_setup::test_support::FakeStorageSetup;
use gpui::AppContext as _;
use gpui::{Entity, IntoElement, Modifiers, ParentElement as _, Render, Styled as _, div, point, px, size};
use gpui_component::Root;
use std::{cell::RefCell, rc::Rc, sync::Arc};

struct Harness(Entity<StorageSetupView>);
impl Render for Harness {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        div()
            .size_full()
            .child(self.0.clone())
            .children(Root::render_dialog_layer(window, cx))
    }
}

fn mount(
    cx: &mut gpui::TestAppContext,
    initialized: bool,
) -> (
    Entity<Root>,
    Entity<StorageSetupView>,
    Arc<FakeStorageSetup>,
    &mut gpui::VisualTestContext,
) {
    cx.update(gpui_component::init);
    let (services, backend) = FakeStorageSetup::services(initialized);
    let capture = Rc::new(RefCell::new(None));
    let captured = capture.clone();
    let (root, visual) = cx.add_window_view(move |window, cx| {
        let view = cx.new(|cx| StorageSetupView::new(services, StorageSetupMode::Recovery, cx));
        view.update(cx, |view, cx| {
            view.refresh(cx);
            view.focus(window);
        });
        captured.replace(Some(view.clone()));
        let harness = cx.new(|_| Harness(view));
        Root::new(harness, window, cx)
    });
    let view = capture.borrow_mut().take().unwrap();
    visual.simulate_resize(size(px(960.), px(640.)));
    (root, view, backend, visual)
}

fn draw(root: &Entity<Root>, cx: &mut gpui::VisualTestContext) {
    cx.run_until_parked();
    for _ in 0..2 {
        cx.draw(point(px(0.), px(0.)), size(px(960.), px(640.)), |_, _| root.clone());
    }
}

fn click(root: &Entity<Root>, cx: &mut gpui::VisualTestContext, selector: &'static str) {
    for _ in 0..40 {
        draw(root, cx);
        let viewport = cx.debug_bounds("storage-setup-viewport").unwrap();
        assert!(
            viewport.bottom() <= px(640.),
            "the recovery viewport must stay within the actual window"
        );
        let control = cx
            .debug_bounds(selector)
            .unwrap_or_else(|| panic!("missing {selector}"));
        if control.top() >= viewport.top() && control.bottom() <= viewport.bottom() {
            cx.simulate_click(control.center(), Modifiers::default());
            cx.run_until_parked();
            return;
        }
        cx.simulate_event(gpui::ScrollWheelEvent {
            position: viewport.center(),
            delta: gpui::ScrollDelta::Pixels(point(
                px(0.),
                px(if control.top() < viewport.top() { 180. } else { -180. }),
            )),
            ..Default::default()
        });
    }
    panic!("unreachable {selector}");
}

fn confirm(root: &Entity<Root>, cx: &mut gpui::VisualTestContext, accept: bool) {
    draw(root, cx);
    assert!(
        cx.update(gpui_component::WindowExt::has_active_dialog),
        "the actual control must open confirmation"
    );
    let bounds = cx
        .debug_bounds(if accept {
            "confirm-dialog-ok"
        } else {
            "confirm-dialog-cancel"
        })
        .unwrap();
    cx.simulate_click(bounds.center(), Modifiers::default());
    cx.run_until_parked();
}

#[gpui::test]
fn native_workspace_controls_confirm_import_cancel_initialize_and_hold_duplicates(cx: &mut gpui::TestAppContext) {
    let (root, view, backend, cx) = mount(cx, false);
    draw(&root, cx);
    assert!(cx.read(|app| view.read(app).can_setup()));
    click(&root, cx, "storage-initialize");
    confirm(&root, cx, false);
    assert!(backend.actions.lock().is_empty());
    let (release, held) = tokio::sync::oneshot::channel();
    *backend.held.lock() = Some(held);
    click(&root, cx, "storage-import");
    confirm(&root, cx, true);
    click(&root, cx, "storage-initialize");
    assert_eq!(backend.actions.lock().len(), 1);
    assert!(
        matches!(&backend.actions.lock()[0], WorkspaceSetupAction::ImportLegacy { config_path } if config_path == &PathBuf::from("legacy/config.json"))
    );
    assert!(cx.read(|app| view.read(app).busy));
    release.send(()).unwrap();
    cx.run_until_parked();
    assert!(!cx.read(|app| view.read(app).busy));
}

#[gpui::test]
fn unknown_workspace_result_requires_refresh_then_observes_initialized_target(cx: &mut gpui::TestAppContext) {
    let (root, view, backend, cx) = mount(cx, false);
    *backend.failure.lock() = Some(UiError::new(
        "The setup response was lost",
        UiErrorCode::Connection,
        true,
    ));
    click(&root, cx, "storage-initialize");
    confirm(&root, cx, true);
    assert!(cx.read(|app| view.read(app).must_refresh));
    click(&root, cx, "storage-import");
    assert_eq!(backend.actions.lock().len(), 1);
    backend.snapshot.lock().status.as_mut().unwrap().initialized = true;
    click(&root, cx, "storage-refresh");
    assert!(!cx.read(|app| view.read(app).must_refresh));
    click(&root, cx, "storage-initialize");
    assert_eq!(backend.actions.lock().len(), 1);
    assert!(cx.read(|app| view.read(app).initialized()));
}

#[gpui::test]
fn engine_choice_control_saves_only_next_launch_and_external_override_blocks_it(cx: &mut gpui::TestAppContext) {
    let (root, view, backend, cx) = mount(cx, true);
    click(&root, cx, "storage-engine-0");
    click(&root, cx, "storage-save-engine");
    confirm(&root, cx, true);
    assert_eq!(*backend.saved.lock(), vec![StorageEngine::File]);
    assert_eq!(
        cx.read(|app| view.read(app).snapshot.as_ref().unwrap().current_engine),
        Some(StorageEngine::Sqlite)
    );
    backend.snapshot.lock().source = SelectionSource::Environment;
    click(&root, cx, "storage-refresh");
    click(&root, cx, "storage-save-engine");
    assert_eq!(backend.saved.lock().len(), 1);
}

#[gpui::test]
fn engine_choice_accepts_keyboard_activation(cx: &mut gpui::TestAppContext) {
    let (root, view, _, cx) = mount(cx, true);
    draw(&root, cx);
    cx.simulate_keystrokes("tab");
    cx.simulate_keystrokes("space");
    cx.run_until_parked();
    assert_eq!(cx.read(|app| view.read(app).draft_engine), StorageEngine::File);
}

#[gpui::test]
fn unavailable_authority_cannot_initialize_or_import_until_a_fresh_empty_observation(cx: &mut gpui::TestAppContext) {
    let (root, view, backend, cx) = mount(cx, false);
    let healthy = backend.snapshot.lock().status.clone();
    backend.snapshot.lock().status = Err(UiError::new("Database unavailable", UiErrorCode::Connection, true));
    click(&root, cx, "storage-refresh");
    draw(&root, cx);
    assert!(cx.debug_bounds("storage-authority-error").is_some());
    assert!(!cx.read(|app| view.read(app).can_setup()));
    click(&root, cx, "storage-initialize");
    click(&root, cx, "storage-import");
    assert!(backend.actions.lock().is_empty());
    backend.snapshot.lock().status = healthy;
    click(&root, cx, "storage-refresh");
    assert!(cx.read(|app| view.read(app).can_setup()));
    click(&root, cx, "storage-initialize");
    confirm(&root, cx, false);
    assert!(backend.actions.lock().is_empty());
}
