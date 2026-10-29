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
use crate::{
    features::message_detail::MessageDetail,
    services::messages::test_support::{FakeMessageBackend, row, trace},
};
use gpui::{Modifiers, point, size};
use gpui_component::Root;
use rocketmq_dashboard_common::MessageSearchPage;
use std::{cell::RefCell, rc::Rc, sync::Arc};

struct Harness {
    view: Entity<MessageTraceView>,
    detail: Entity<MessageDetail>,
    inspector: bool,
}
impl Render for Harness {
    fn render(&mut self, _: &mut Window, _: &mut Context<Self>) -> impl IntoElement {
        if self.inspector {
            div().size_full().child(self.detail.clone())
        } else {
            div().size_full().child(self.view.clone())
        }
    }
}
struct Fixture {
    root: Entity<Root>,
    harness: Entity<Harness>,
    view: Entity<MessageTraceView>,
    detail: Entity<MessageDetail>,
}
fn open(cx: &mut gpui::TestAppContext, backend: Arc<FakeMessageBackend>) -> (Fixture, &mut gpui::VisualTestContext) {
    cx.update(gpui_component::init);
    let captured = Rc::new(RefCell::new(None));
    let out = captured.clone();
    let (root, cx) = cx.add_window_view(move |window, cx| {
        let services = AppServices::default().with_message_backend(backend);
        let view = cx.new(|cx| MessageTraceView::new(window, services.clone(), 7, cx));
        let detail = cx.new(|_| MessageDetail::new(services, 7, row()));
        let harness = cx.new(|_| Harness {
            view: view.clone(),
            detail: detail.clone(),
            inspector: false,
        });
        out.replace(Some((harness.clone(), view, detail)));
        Root::new(harness, window, cx)
    });
    let (harness, view, detail) = captured.borrow_mut().take().unwrap();
    (
        Fixture {
            root,
            harness,
            view,
            detail,
        },
        cx,
    )
}
fn draw(f: &Fixture, cx: &mut gpui::VisualTestContext, width: f32, height: f32) {
    let viewport = size(px(width), px(height));
    cx.simulate_resize(viewport);
    cx.run_until_parked();
    for _ in 0..2 {
        cx.draw(point(px(0.), px(0.)), viewport, |_, _| f.root.clone());
    }
}
fn click(cx: &mut gpui::VisualTestContext, selector: &'static str) {
    let bounds = cx
        .debug_bounds(selector)
        .unwrap_or_else(|| panic!("missing {selector}"));
    cx.simulate_click(bounds.center(), Modifiers::default());
    cx.run_until_parked();
}
fn inputs(f: &Fixture, cx: &mut gpui::VisualTestContext, topic: &str, id: &str) {
    cx.update(|window, app| {
        f.view.update(app, |view, cx| {
            view.topic
                .update(cx, |input, cx| input.set_value(topic.to_owned(), window, cx));
            view.identifier
                .update(cx, |input, cx| input.set_value(id.to_owned(), window, cx));
        })
    });
    cx.run_until_parked();
}

#[gpui::test]
fn trace_actual_controls_validate_find_candidates_then_load_only_the_selected_physical_message(
    cx: &mut gpui::TestAppContext,
) {
    let backend = Arc::new(FakeMessageBackend::default());
    let (f, cx) = open(cx, backend.clone());
    draw(&f, cx, 1440., 900.);
    click(cx, "trace-find");
    assert!(backend.calls().query.is_empty());
    inputs(&f, cx, "orders", "producer-id");
    backend.queue_query(Ok(MessageSearchPage {
        rows: vec![row()],
        extent: MessagePageExtent::End,
    }));
    draw(&f, cx, 1440., 900.);
    click(cx, "trace-find");
    assert_eq!(
        backend.calls().query[0].1,
        MessageSearchQuery::MessageId {
            topic: "orders".into(),
            message_id: "producer-id".into()
        }
    );
    assert!(
        backend.calls().trace.is_empty(),
        "finding candidates must not also request Trace"
    );
    backend.queue_trace(Ok(trace(row().identity)));
    draw(&f, cx, 1440., 900.);
    click(cx, "trace-select-row-0");
    draw(&f, cx, 1440., 900.);
    let query = &backend.calls().trace[0].1;
    assert_eq!(query.identity, row().identity);
    assert_eq!(query.trace_topic, DEFAULT_TRACE_TOPIC);
    assert!(cx.debug_bounds("trace-node-0").is_some());
    assert!(backend.calls().payload.is_empty() && backend.calls().consume.is_empty());
    click(cx, "trace-mode-key");
    inputs(&f, cx, "orders", "order-42");
    backend.queue_query(Ok(MessageSearchPage {
        rows: (0..64)
            .map(|index| {
                let mut candidate = row();
                candidate.identity.message_id = format!("physical-{index}");
                candidate
            })
            .collect(),
        extent: MessagePageExtent::KeyLimitReached,
    }));
    draw(&f, cx, 1440., 900.);
    click(cx, "trace-find");
    draw(&f, cx, 1440., 900.);
    assert_eq!(
        backend.calls().query[1].1,
        MessageSearchQuery::Key {
            topic: "orders".into(),
            key: "order-42".into()
        }
    );
    assert!(cx.debug_bounds("trace-candidates-truncated").is_some());
    // GPUI retains historical selector bounds, so absence is established by the current state
    // reached through the real mode/find controls rather than an old debug-map entry.
    cx.read(|app| {
        let view = f.view.read(app);
        assert!(matches!(view.store.trace.state, Loadable::Idle));
        assert!(view.store.trace.query.is_none() && view.store.candidates.selected.is_none());
        assert_eq!(view.store.candidates.rows.value().unwrap().rows.len(), 64);
    });
}

#[gpui::test]
fn trace_candidate_change_drops_pending_nodes_and_trace_topic_edit_cannot_restore_old_results(
    cx: &mut gpui::TestAppContext,
) {
    let backend = Arc::new(FakeMessageBackend::default());
    let (f, cx) = open(cx, backend.clone());
    inputs(&f, cx, "orders", "key");
    draw(&f, cx, 1440., 900.);
    click(cx, "trace-mode-key");
    let mut second = row();
    second.identity.message_id = "second-physical".into();
    backend.queue_query(Ok(MessageSearchPage {
        rows: vec![row(), second.clone()],
        extent: MessagePageExtent::End,
    }));
    draw(&f, cx, 1440., 900.);
    click(cx, "trace-find");
    backend.queue_trace(Ok(trace(row().identity)));
    let (mut started, _release, mut finished) = backend.hold_next_trace();
    draw(&f, cx, 1440., 900.);
    click(cx, "trace-select-row-0");
    assert!(started.try_recv().is_ok());
    backend.queue_trace(Ok(trace(second.identity.clone())));
    click(cx, "trace-select-row-1");
    assert!(finished.try_recv().is_ok());
    draw(&f, cx, 1440., 900.);
    cx.read(|app| {
        assert_eq!(
            f.view.read(app).store.trace.state.value().unwrap().identity,
            second.identity
        )
    });
    cx.update(|window, app| {
        f.view.update(app, |view, cx| {
            view.trace_topic
                .update(cx, |input, cx| input.set_value("custom-trace", window, cx));
        })
    });
    draw(&f, cx, 1440., 900.);
    cx.read(|app| {
        let view = f.view.read(app);
        assert!(matches!(view.store.trace.state, Loadable::Idle));
        assert!(view.store.trace.query.is_none());
    });
    backend.queue_trace(Err(UiError::new("Trace RPC failed.", UiErrorCode::Connection, true)));
    click(cx, "trace-reload");
    draw(&f, cx, 1440., 900.);
    assert_eq!(backend.calls().trace.last().unwrap().1.trace_topic, "custom-trace");
    assert!(cx.debug_bounds("trace-error").is_some());
    let mut empty = trace(second.identity);
    empty.trace_topic = "custom-trace".into();
    empty.nodes.clear();
    backend.queue_trace(Ok(empty));
    click(cx, "trace-reload");
    draw(&f, cx, 1440., 900.);
    assert!(cx.debug_bounds("trace-empty").is_some());
    cx.read(|app| {
        assert!(matches!(&f.view.read(app).store.trace.state, Loadable::Ready(trace) if trace.nodes.is_empty()));
    });
    cx.update(|window, app| f.view.update(app, |view, cx| view.set_revision(8, window, cx)));
    draw(&f, cx, 1440., 900.);
    cx.read(|app| {
        let view = f.view.read(app);
        assert!(view.store.trace.query.is_none() && view.store.candidates.selected.is_none());
        assert!(view.topic.read(app).value().is_empty() && view.identifier.read(app).value().is_empty());
    });
}

#[gpui::test]
fn trace_inspector_is_lazy_and_uses_the_same_selected_identity_service_and_timeline(cx: &mut gpui::TestAppContext) {
    let backend = Arc::new(FakeMessageBackend::default());
    let (f, cx) = open(cx, backend.clone());
    inputs(&f, cx, "orders", "producer-id");
    backend.queue_query(Ok(MessageSearchPage {
        rows: vec![row()],
        extent: MessagePageExtent::End,
    }));
    backend.queue_trace(Ok(trace(row().identity)));
    draw(&f, cx, 1440., 900.);
    click(cx, "trace-find");
    draw(&f, cx, 1440., 900.);
    click(cx, "trace-select-row-0");
    let workspace_trace = cx.read(|app| f.view.read(app).store.trace.state.value().unwrap().clone());
    cx.update(|_, app| {
        f.harness.update(app, |harness, cx| {
            harness.inspector = true;
            cx.notify();
        })
    });
    draw(&f, cx, 1440., 900.);
    assert_eq!(backend.calls().trace.len(), 1, "Inspector Overview must not load Trace");
    backend.queue_trace(Ok(trace(row().identity)));
    click(cx, "message-tab-trace");
    draw(&f, cx, 1440., 900.);
    assert_eq!(backend.calls().trace.len(), 2);
    assert_eq!(backend.calls().trace[0].1, backend.calls().trace[1].1);
    cx.read(|app| assert_eq!(f.detail.read(app).trace_for_test().unwrap(), &workspace_trace));
    assert!(cx.debug_bounds("trace-node-0").is_some());
    assert!(backend.calls().payload.is_empty() && backend.calls().consume.is_empty());
    cx.update(|_, app| f.detail.update(app, |detail, cx| detail.clear_sensitive(cx)));
    assert!(cx.read(|app| f.detail.read(app).trace_for_test().is_none()));
}

#[gpui::test]
fn trace_query_fields_and_actions_remain_readable_in_compact_windows(cx: &mut gpui::TestAppContext) {
    let (f, cx) = open(cx, Arc::new(FakeMessageBackend::default()));
    for (width, height) in [(1440., 900.), (930., 620.), (960., 640.)] {
        draw(&f, cx, width, height);
        let workspace = cx.debug_bounds("message-trace-workspace").unwrap();
        for selector in [
            "trace-message-topic-field",
            "trace-identifier-field",
            "trace-topic-field",
        ] {
            let field = cx.debug_bounds(selector).unwrap();
            assert!(field.size.width >= px(240.), "collapsed {selector}: {field:?}");
            assert!(field.right() <= workspace.right() && field.left() >= workspace.left());
        }
        let find = cx.debug_bounds("trace-find").unwrap();
        assert!(find.bottom() <= px(height) && find.right() <= workspace.right());
    }
}
