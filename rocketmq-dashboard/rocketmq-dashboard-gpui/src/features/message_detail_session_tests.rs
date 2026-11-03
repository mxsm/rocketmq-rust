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

use std::{cell::RefCell, collections::BTreeMap, rc::Rc, sync::Arc, time::Duration};

use gpui::{AppContext as _, point, px, size};
use gpui_component::Root;

use super::*;
use crate::services::{
    messages::test_support::{FakeMessageBackend, row},
    sessions::tests::Fixture,
};

#[gpui::test]
fn cleared_message_detail_rejects_its_old_copy_and_query_callbacks_after_replacement(cx: &mut gpui::TestAppContext) {
    cx.update(gpui_component::init);
    cx.background_executor.allow_parking();
    let fixture = Fixture::new();
    let context = fixture.runtime.root_context().component("retained-message-detail");
    let backend = Arc::new(FakeMessageBackend::default());
    let services = AppServices::desktop_with_fake_provider(
        fixture.config.clone(),
        fixture.auth.clone(),
        context.clone(),
        fixture.runtime.root_context().component("retained-message-history"),
        fixture.runtime.root_context().component("retained-message-monitor"),
    )
    .with_message_backend(backend.clone());
    fixture.runtime.block_on(services.bootstrap()).unwrap();
    let old = fixture.login();
    backend.queue_payload(Ok(MessagePayload::bounded(
        row().identity,
        br#"{"private":"body"}"#.to_vec(),
        BTreeMap::new(),
    )));
    let capture = Rc::new(RefCell::new(None));
    let captured = capture.clone();
    let (root, cx) = cx.add_window_view(move |window, cx| {
        let detail = cx.new(|_| MessageDetail::new(services, 0, row()));
        captured.replace(Some(detail.clone()));
        Root::new(detail, window, cx)
    });
    let detail = capture.borrow_mut().take().unwrap();
    for _ in 0..2 {
        cx.draw(point(px(0.), px(0.)), size(px(1000.), px(800.)), |_, _| root.clone());
    }
    let body = cx.debug_bounds("message-tab-body").unwrap();
    cx.simulate_click(body.center(), gpui::Modifiers::default());
    let authorization = cx.update(|_, app| detail.update(app, |detail, _| detail.access_task.take().unwrap()));
    cx.background_executor.block_test(authorization);
    let payload = cx.update(|_, app| detail.update(app, |detail, _| detail.task.take().unwrap()));
    cx.background_executor.block_test(payload);
    assert!(cx.read(|app| detail.read(app).has_payload()));
    assert_eq!(backend.calls().payload.len(), 1);

    cx.update(|window, app| detail.update(app, |detail, cx| detail.select_tab(MessageTab::Overview, window, cx)));
    let authorization = cx.update(|_, app| detail.update(app, |detail, _| detail.access_task.take().unwrap()));
    cx.background_executor.block_test(authorization);
    cx.draw(point(px(0.), px(0.)), size(px(1000.), px(800.)), |_, _| root.clone());
    let copy = cx.debug_bounds("message-copy-id").unwrap();
    cx.update(|_, app| {
        app.write_to_clipboard(gpui::ClipboardItem::new_string("unchanged".into()));
        detail.update(app, |detail, cx| detail.clear_sensitive(cx));
    });
    let replacement = fixture.login();
    assert_ne!(old, replacement);
    fixture.runtime.block_on(fixture.native.close()).unwrap();

    // Use the actual previous frame's handler before a redraw, retaining the cleared Entity.
    cx.simulate_click(copy.center(), gpui::Modifiers::default());
    let authorization = cx.update(|_, app| detail.update(app, |detail, _| detail.access_task.take().unwrap()));
    cx.background_executor.block_test(authorization);
    cx.read(|app| {
        assert_eq!(app.read_from_clipboard().unwrap().text().as_deref(), Some("unchanged"));
        assert!(!detail.read(app).has_payload());
    });
    assert!(
        fixture.auth.session().is_authenticated(),
        "the cleared view must not validate or end the replacement session"
    );
    cx.update(|_, app| {
        detail.update(app, |detail, cx| {
            detail.load_payload(cx);
            detail.load_trace(cx);
            assert!(detail.task.is_none());
            assert!(detail.trace_task.is_none());
        });
    });
    assert_eq!(backend.calls().payload.len(), 1);
    assert!(backend.calls().query.is_empty());
    fixture.runtime.block_on(async {
        assert!(context.task_group().shutdown(Duration::from_secs(5)).await.is_healthy());
    });
    fixture.finish();
}
