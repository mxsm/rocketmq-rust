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
use crate::services::messages::test_support::{FakeMessageBackend, page, row};
use gpui::{Modifiers, point, size};
use gpui_component::Root;
use rocketmq_dashboard_common::{MessageConsumeCode, MessageConsumeReceipt, MessagePayload, MessageSearchPage};
use std::{cell::RefCell, collections::BTreeMap, rc::Rc, sync::Arc};

struct Harness {
    messages: Entity<MessagesView>,
}
impl Render for Harness {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        div()
            .size_full()
            .child(self.messages.clone())
            .children(Root::render_sheet_layer(window, cx))
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
        .unwrap_or_else(|| panic!("missing control {selector}"));
    cx.simulate_click(bounds.center(), Modifiers::default());
    cx.run_until_parked();
}
fn open(
    cx: &mut gpui::TestAppContext,
    backend: Arc<FakeMessageBackend>,
) -> (Entity<Root>, Entity<MessagesView>, &mut gpui::VisualTestContext) {
    open_services(cx, AppServices::default().with_message_backend(backend))
}
fn open_services(
    cx: &mut gpui::TestAppContext,
    services: AppServices,
) -> (Entity<Root>, Entity<MessagesView>, &mut gpui::VisualTestContext) {
    cx.update(gpui_component::init);
    let capture = Rc::new(RefCell::new(None));
    let captured = capture.clone();
    let (root, cx) = cx.add_window_view(move |window, cx| {
        let messages = cx.new(|cx| MessagesView::new(window, services, 7, cx));
        captured.replace(Some(messages.clone()));
        let harness = cx.new(|_| Harness { messages });
        Root::new(harness, window, cx)
    });
    let messages = capture.borrow_mut().take().unwrap();
    cx.simulate_resize(size(px(1440.), px(900.)));
    (root, messages, cx)
}
fn set_topic(messages: &Entity<MessagesView>, topic: &str, cx: &mut gpui::VisualTestContext) {
    cx.update(|window, app| {
        messages.update(app, |view, cx| {
            view.topic
                .update(cx, |input, cx| input.set_value(topic.to_owned(), window, cx))
        })
    });
    cx.run_until_parked();
}

#[gpui::test]
fn message_query_fields_remain_usable_after_narrowing_in_each_mode(cx: &mut gpui::TestAppContext) {
    let backend = Arc::new(FakeMessageBackend::default());
    let (root, messages, cx) = open(cx, backend);
    draw(&root, cx);
    for (mode, field) in [
        ("message-mode-id", "message-id-field"),
        ("message-mode-key", "message-key-field"),
        ("message-mode-range", "message-begin-field"),
    ] {
        click(cx, mode);
        for (width, height) in [(1440., 900.), (930., 620.), (960., 640.)] {
            let viewport = size(px(width), px(height));
            cx.simulate_resize(viewport);
            for _ in 0..2 {
                cx.draw(point(px(0.), px(0.)), viewport, |_, _| root.clone());
            }
            let workspace = cx.debug_bounds("messages-workspace").unwrap();
            let input = cx.debug_bounds(field).unwrap();
            let search = cx.debug_bounds("message-search").unwrap();
            assert!(
                input.size.width >= px(240.),
                "collapsed {field} at {width}x{height}: {input:?}"
            );
            assert!(input.size.height >= px(28.), "collapsed {field} height: {input:?}");
            assert!(input.left() >= workspace.left() && input.right() <= workspace.right());
            assert!(
                input.bottom() <= search.top(),
                "query field overlaps actions: {input:?}, {search:?}"
            );
            assert!(search.bottom() <= px(height), "Search is outside the narrow window");
            if mode == "message-mode-range" {
                for selector in ["message-end-field", "message-page-size-field"] {
                    let bounds = cx.debug_bounds(selector).unwrap();
                    let min_width = if selector == "message-end-field" { 240. } else { 90. };
                    assert!(bounds.size.width >= px(min_width));
                    assert!(bounds.left() >= workspace.left() && bounds.right() <= workspace.right());
                    assert!(bounds.bottom() <= search.top());
                }
            } else {
                assert!(input.size.width >= workspace.size.width - px(40.));
                cx.simulate_click(input.center(), Modifiers::default());
                cx.simulate_input("readable-query");
                cx.run_until_parked();
                cx.read(|app| {
                    let view = messages.read(app);
                    let state = if mode == "message-mode-id" {
                        &view.message_id
                    } else {
                        &view.key
                    };
                    assert!(state.read(app).value().contains("readable-query"));
                });
            }
        }
    }
}

#[gpui::test]
fn message_query_controls_apply_three_modes_page_and_refresh_the_frozen_query(cx: &mut gpui::TestAppContext) {
    let backend = Arc::new(FakeMessageBackend::default());
    let (root, messages, cx) = open(cx, backend.clone());
    draw(&root, cx);
    click(cx, "message-search");
    assert!(backend.calls().query.is_empty());
    set_topic(&messages, "orders", cx);
    cx.update(|window, app| {
        messages.update(app, |view, cx| {
            view.begin
                .update(cx, |input, cx| input.set_value("2026-10-30 00:00:00", window, cx));
            view.end
                .update(cx, |input, cx| input.set_value("2026-10-30 01:00:00", window, cx));
            view.page_size.update(cx, |input, cx| input.set_value("1", window, cx));
        })
    });
    cx.run_until_parked();
    backend.queue_query(Ok(page()));
    draw(&root, cx);
    click(cx, "message-search");
    let first = backend.calls().query[0].1.clone();
    assert_eq!(
        first,
        MessageSearchQuery::TopicRange {
            topic: "orders".into(),
            begin: 1793318400000,
            end: 1793322000000,
            page: 1,
            page_size: 1
        }
    );
    backend.queue_query(Ok(MessageSearchPage {
        rows: Vec::new(),
        extent: MessagePageExtent::End,
    }));
    draw(&root, cx);
    click(cx, "message-next-page");
    assert_eq!(backend.calls().query[1].1, first.with_page(2).unwrap());
    cx.update(|_, app| assert!(messages.read(app).store.rows.value().unwrap().rows.is_empty()));
    set_topic(&messages, "other-orders", cx);
    backend.queue_query(Ok(page()));
    draw(&root, cx);
    click(cx, "message-refresh");
    assert_eq!(backend.calls().query[2].1, first.with_page(2).unwrap());
    draw(&root, cx);
    click(cx, "message-mode-key");
    cx.update(|window, app| {
        messages.update(app, |view, cx| {
            view.key.update(cx, |input, cx| input.set_value("key-a", window, cx))
        })
    });
    cx.run_until_parked();
    backend.queue_query(Ok(page()));
    draw(&root, cx);
    click(cx, "message-search");
    assert_eq!(
        backend.calls().query[3].1,
        MessageSearchQuery::Key {
            topic: "other-orders".into(),
            key: "key-a".into()
        }
    );
    draw(&root, cx);
    click(cx, "message-mode-id");
    cx.update(|window, app| {
        messages.update(app, |view, cx| {
            view.message_id
                .update(cx, |input, cx| input.set_value("producer-id", window, cx))
        })
    });
    cx.run_until_parked();
    backend.queue_query(Ok(page()));
    draw(&root, cx);
    click(cx, "message-search");
    assert_eq!(
        backend.calls().query[4].1,
        MessageSearchQuery::MessageId {
            topic: "other-orders".into(),
            message_id: "producer-id".into()
        }
    );
}

#[gpui::test]
fn message_detail_cache_and_real_consume_confirmation_cancel_and_double_click(cx: &mut gpui::TestAppContext) {
    let backend = Arc::new(FakeMessageBackend::default());
    let (root, messages, cx) = open(cx, backend.clone());
    set_topic(&messages, "orders", cx);
    backend.queue_query(Ok(page()));
    draw(&root, cx);
    click(cx, "message-search");
    draw(&root, cx);
    click(cx, "message-open-row-0");
    assert!(backend.calls().payload.is_empty());
    draw(&root, cx);
    click(cx, "message-copy-id");
    cx.read(|app| {
        assert_eq!(
            app.read_from_clipboard().unwrap().text().unwrap(),
            row().identity.message_id
        )
    });
    draw(&root, cx);
    click(cx, "message-tab-trace");
    assert!(backend.calls().payload.is_empty());
    assert_eq!(backend.calls().query.len(), 1);
    let detail = cx.read(|app| messages.read(app).detail.clone().unwrap());
    backend.queue_payload(Ok(MessagePayload::bounded(
        row().identity,
        br#"{"answer":42}"#.to_vec(),
        BTreeMap::from([("tag".into(), "value".into())]),
    )));
    draw(&root, cx);
    click(cx, "message-tab-body");
    draw(&root, cx);
    assert_eq!(backend.calls().payload.len(), 1);
    assert!(cx.read(|app| detail.read(app).has_payload()));
    click(cx, "message-body-format");
    draw(&root, cx);
    click(cx, "message-tab-properties");
    assert_eq!(backend.calls().payload.len(), 1);
    draw(&root, cx);
    click(cx, "message-tab-overview");
    draw(&root, cx);
    click(cx, "message-consume");
    draw(&root, cx);
    click(cx, "message-consume-cancel");
    assert!(backend.calls().consume.is_empty());
    draw(&root, cx);
    click(cx, "message-consume");
    draw(&root, cx);
    let form = cx.read(|app| messages.read(app).consume_form.clone().unwrap());
    cx.update(|window, app| {
        form.update(app, |form, cx| {
            form.group
                .update(cx, |input, cx| input.set_value("group-a", window, cx));
            form.client
                .update(cx, |input, cx| input.set_value("client-a", window, cx));
        })
    });
    cx.run_until_parked();
    backend.queue_consume(Ok(MessageConsumeReceipt {
        code: MessageConsumeCode::Later,
        reported_success: false,
        remark_returned: true,
    }));
    let (mut started, release, _finished) = backend.hold_next_consume();
    draw(&root, cx);
    click(cx, "message-consume-confirm");
    assert!(started.try_recv().is_ok());
    draw(&root, cx);
    click(cx, "message-consume-confirm");
    assert_eq!(backend.calls().consume.len(), 1);
    cx.simulate_keystrokes("escape");
    cx.run_until_parked();
    draw(&root, cx);
    cx.simulate_keystrokes("escape");
    cx.run_until_parked();
    assert!(!cx.read(|app| detail.read(app).has_payload()));
    assert!(cx.read(|app| messages.read(app).mutation_task.is_some()));
    release.send(()).unwrap();
    cx.run_until_parked();
    let calls = backend.calls();
    assert_eq!(calls.consume.len(), 1);
    let command = &calls.consume[0].1;
    assert_eq!(command.identity, row().identity);
    assert_eq!(command.consumer_group, "group-a");
    assert_eq!(command.client_id.as_deref(), Some("client-a"));
    assert_eq!(command.scope, ConnectionScope::NameServer);
    cx.update(|_, app| {
        let view = messages.read(app);
        assert!(view.status.as_ref().unwrap().contains("CR_LATER"));
        assert!(view.status.as_ref().unwrap().contains("success: false"));
        assert!(view.mutation_task.is_none());
    });
}

#[gpui::test]
fn message_context_change_cancels_pending_read_and_drops_sensitive_detail(cx: &mut gpui::TestAppContext) {
    let backend = Arc::new(FakeMessageBackend::default());
    let (root, messages, cx) = open(cx, backend.clone());
    set_topic(&messages, "orders", cx);
    backend.queue_query(Ok(page()));
    let (mut started, _release, mut finished) = backend.hold_next_query();
    draw(&root, cx);
    click(cx, "message-search");
    assert!(started.try_recv().is_ok());
    cx.update(|window, app| messages.update(app, |view, cx| view.set_revision(8, window, cx)));
    cx.run_until_parked();
    assert!(finished.try_recv().is_ok());
    cx.update(|_, app| {
        let view = messages.read(app);
        assert!(view.store.applied.is_none());
        assert!(view.store.selected.is_none());
        assert!(view.search_is_idle());
        assert!(view.topic.read(app).value().is_empty());
    });
}

#[gpui::test]
fn message_topic_candidates_use_real_inventory_selection_and_validated_manual_fallback(cx: &mut gpui::TestAppContext) {
    use crate::services::topics::test_support::FakeTopicBackend;
    use rocketmq_dashboard_common::{
        TopicCategory, TopicCompleteness, TopicIdentity, TopicInventory, TopicInventoryItem, TopicMessageType,
    };
    let backend = Arc::new(FakeMessageBackend::default());
    let topics = Arc::new(FakeTopicBackend::default());
    let services = AppServices::default()
        .with_message_backend(backend.clone())
        .with_topic_backend(topics.clone());
    let (root, messages, cx) = open_services(cx, services);
    topics.queue_inventory(Ok(TopicInventory {
        items: vec![TopicInventoryItem {
            identity: TopicIdentity::parse("discovered-orders").unwrap(),
            category: TopicCategory::Application,
            message_type: TopicMessageType::Normal,
            clusters: Vec::new(),
            brokers: Vec::new(),
            read_queue_count: None,
            write_queue_count: None,
            permission: None,
            ordered: None,
        }],
        targets: Vec::new(),
        completeness: TopicCompleteness::Complete,
        failures: Vec::new(),
    }));
    draw(&root, cx);
    click(cx, "message-topics-refresh");
    assert_eq!(topics.calls().inventory[0].revision, 7);
    assert!(backend.calls().query.is_empty());
    draw(&root, cx);
    click(cx, "message-topic-picker");
    cx.simulate_keystrokes("down enter");
    cx.run_until_parked();
    assert_eq!(
        cx.read(|app| messages.read(app).topic.read(app).value().to_string()),
        "discovered-orders"
    );
    backend.queue_query(Ok(page()));
    draw(&root, cx);
    click(cx, "message-search");
    assert_eq!(backend.calls().query[0].1.topic(), "discovered-orders");
    topics.queue_inventory(Err(UiError::new(
        "Topic discovery failed.",
        UiErrorCode::Connection,
        true,
    )));
    draw(&root, cx);
    click(cx, "message-topics-refresh");
    set_topic(&messages, "manual-orders", cx);
    backend.queue_query(Ok(page()));
    draw(&root, cx);
    click(cx, "message-search");
    assert_eq!(backend.calls().query[1].1.topic(), "manual-orders");
    cx.update(|window, app| messages.update(app, |view, cx| view.set_revision(8, window, cx)));
    cx.run_until_parked();
    cx.read(|app| {
        let view = messages.read(app);
        assert!(matches!(view.topic_candidates.state, Loadable::Idle));
        assert!(view.topic_picker.read(app).selected_value().is_none());
    });
}
