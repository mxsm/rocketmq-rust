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
use gpui::{Modifiers, point, size};
use std::{cell::RefCell, rc::Rc};

fn draw(root: &Entity<Root>, cx: &mut gpui::VisualTestContext) {
    cx.run_until_parked();
    for _ in 0..2 {
        cx.draw(point(px(0.), px(0.)), size(px(960.), px(640.)), |_, _| root.clone());
    }
}

fn click(root: &Entity<Root>, cx: &mut gpui::VisualTestContext, selector: &'static str) {
    for _ in 0..40 {
        draw(root, cx);
        let control = cx.debug_bounds(selector).unwrap();
        if let Some(viewport) = cx.debug_bounds("storage-setup-viewport")
            && (control.top() < viewport.top() || control.bottom() > viewport.bottom())
        {
            cx.simulate_event(gpui::ScrollWheelEvent {
                position: viewport.center(),
                delta: gpui::ScrollDelta::Pixels(point(
                    px(0.),
                    px(if control.top() < viewport.top() { 180. } else { -180. }),
                )),
                ..Default::default()
            });
            continue;
        }
        cx.simulate_click(control.center(), Modifiers::default());
        cx.run_until_parked();
        return;
    }
    panic!("unreachable {selector}");
}

#[gpui::test]
fn workspace_setup_completion_reenters_existing_bootstrap_and_requires_login(cx: &mut gpui::TestAppContext) {
    cx.update(gpui_component::init);
    let (services, backend) = FakeStorageSetup::services(false);
    let capture = Rc::new(RefCell::new(None));
    let captured = capture.clone();
    let (root, cx) = cx.add_window_view(move |window, cx| {
        let dashboard = cx.new(|cx| RocketmqDashboard::with_services(window, services, cx));
        captured.replace(Some(dashboard.clone()));
        Root::new(dashboard, window, cx)
    });
    let dashboard = capture.borrow_mut().take().unwrap();
    cx.simulate_resize(size(px(960.), px(640.)));
    click(&root, cx, "startup-storage-setup");
    click(&root, cx, "storage-initialize");
    draw(&root, cx);
    let confirm = cx.debug_bounds("confirm-dialog-ok").unwrap();
    cx.simulate_click(confirm.center(), Modifiers::default());
    cx.run_until_parked();
    assert_eq!(backend.actions.lock().len(), 1);
    assert_eq!(
        cx.read(|app| dashboard.read(app).startup_state.clone()),
        StartupState::Ready(ReadyScreen::Login)
    );
    assert!(cx.read(|app| dashboard.read(app).storage_setup_view.is_none()));
}
