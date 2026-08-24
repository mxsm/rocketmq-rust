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

use gpui::{
    AppContext as _, Context, Entity, IntoElement, Modifiers, ParentElement as _, Render, Styled as _, Window, div,
    point, px, size,
};
use gpui_component::{Root, WindowExt as _};
use rocketmq_dashboard_common::{
    TopicCategory, TopicCompleteness, TopicConfigTargetView, TopicConfigView, TopicConsumerView, TopicConsumersView,
    TopicFailureCode, TopicFailureStage, TopicIdentity, TopicInventory, TopicInventoryItem, TopicMessageType,
    TopicMutationGuarantee, TopicMutationKind, TopicPartialOutcome, TopicPermission, TopicRouteView, TopicStatsView,
    TopicTargetFailure, TopicTargetIdentity, TopicTargetOutcome,
};

use super::{
    topic_detail::{TopicDetail, TopicDetailIntent},
    topic_dialogs::{TopicDialogKind, TopicEditDraft, TopicSendDraft},
    topics::TopicsView,
};
use crate::{
    route::TopicTab,
    services::{
        AppServices,
        topics::{BackendTopicQueuePatchResult, test_support::FakeTopicBackend},
    },
    state::{Loadable, UiError, UiErrorCode},
};

struct TopicsHarness {
    topics: Entity<TopicsView>,
}

impl Render for TopicsHarness {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        div()
            .size_full()
            .child(self.topics.clone())
            .children(Root::render_sheet_layer(window, cx))
            .children(Root::render_dialog_layer(window, cx))
    }
}

fn topic() -> TopicIdentity {
    TopicIdentity::parse("orders").expect("topic")
}

fn target(cluster: &str, broker: &str, port: u16) -> TopicTargetIdentity {
    TopicTargetIdentity::parse(cluster, broker, format!("127.0.0.1:{port}")).expect("target")
}

fn item(read: u32, write: u32) -> TopicInventoryItem {
    TopicInventoryItem {
        identity: topic(),
        category: TopicCategory::Application,
        message_type: TopicMessageType::Normal,
        clusters: vec!["cluster-a".into(), "cluster-b".into()],
        brokers: vec!["broker-a".into(), "broker-b".into()],
        read_queue_count: Some(read),
        write_queue_count: Some(write),
        permission: TopicPermission::parse(6).ok(),
        ordered: Some(false),
    }
}

fn inventory(read: u32, write: u32) -> TopicInventory {
    TopicInventory {
        items: vec![item(read, write)],
        targets: vec![
            target("cluster-a", "broker-a", 10911),
            target("cluster-b", "broker-b", 20911),
        ],
        completeness: TopicCompleteness::Complete,
        failures: Vec::new(),
    }
}

fn partial_inventory(items: Vec<TopicInventoryItem>) -> TopicInventory {
    TopicInventory {
        items,
        targets: vec![target("cluster-a", "broker-a", 10911)],
        completeness: TopicCompleteness::Partial {
            successful_target_count: 1,
            failed_target_count: 1,
        },
        failures: vec![TopicTargetFailure {
            target: "broker-b".into(),
            stage: TopicFailureStage::CatalogConfig,
            code: TopicFailureCode::Unavailable,
            retryable: true,
        }],
    }
}

fn consumers() -> TopicConsumersView {
    TopicConsumersView {
        topic: topic(),
        items: vec![TopicConsumerView {
            consumer_group: "group-a".into(),
            total_diff: 17,
            inflight_diff: 2,
            consume_tps: 0.0,
        }],
        completeness: TopicCompleteness::Complete,
        failures: Vec::new(),
    }
}

fn config(read: u32, write: u32, version: u64) -> TopicConfigView {
    TopicConfigView {
        topic: topic(),
        targets: vec![TopicConfigTargetView {
            target: target("cluster-a", "broker-a", 10911),
            version,
            read_queue_count: read,
            write_queue_count: write,
            permission: TopicPermission::parse(6).ok(),
            ordered: false,
            message_type: TopicMessageType::Normal,
        }],
        inconsistent_fields: Vec::new(),
        completeness: TopicCompleteness::Complete,
        failures: Vec::new(),
    }
}

fn outcome(kind: TopicMutationKind, partial: bool) -> TopicPartialOutcome {
    let mut targets = vec![TopicTargetOutcome {
        target: "broker-a / queue 0".into(),
        stage: rocketmq_dashboard_common::TopicFailureStage::Mutation,
        applied: true,
        failure: None,
        retryable: false,
    }];
    if partial {
        targets.push(TopicTargetOutcome {
            target: "broker-b / queue 1".into(),
            stage: rocketmq_dashboard_common::TopicFailureStage::Mutation,
            applied: false,
            failure: Some(rocketmq_dashboard_common::TopicFailureCode::Unavailable),
            retryable: true,
        });
    }
    TopicPartialOutcome {
        topic: topic(),
        kind,
        guarantee: TopicMutationGuarantee::PreflightBestEffort,
        targets,
        reload_failed: false,
    }
}

fn draw(root: &Entity<Root>, cx: &mut gpui::VisualTestContext) {
    cx.run_until_parked();
    cx.draw(point(px(0.), px(0.)), size(px(1440.), px(900.)), |_, _| root.clone());
    cx.draw(point(px(0.), px(0.)), size(px(1440.), px(900.)), |_, _| root.clone());
}

fn close_dialog(cx: &mut gpui::VisualTestContext) {
    cx.simulate_keystrokes("escape");
    cx.run_until_parked();
}

fn click_debug(cx: &mut gpui::VisualTestContext, selector: &'static str) {
    let bounds = cx
        .debug_bounds(selector)
        .unwrap_or_else(|| panic!("missing selector: {selector}"));
    cx.simulate_click(bounds.center(), Modifiers::default());
    cx.run_until_parked();
}

#[gpui::test]
fn create_dialog_uses_official_controls_and_submits_every_explicit_option(cx: &mut gpui::TestAppContext) {
    cx.update(gpui_component::init);
    let fake = Arc::new(FakeTopicBackend::default());
    fake.queue_inventory(Ok(inventory(8, 8)));
    let services = AppServices::default().with_topic_backend(fake.clone());
    let capture = Rc::new(RefCell::new(None));
    let capture_topics = capture.clone();
    let (root, cx) = cx.add_window_view(move |window, cx| {
        let topics = cx.new(|cx| TopicsView::new(window, services, 7, cx));
        topics.update(cx, |topics, cx| topics.ensure_loaded(window, cx));
        capture_topics.replace(Some(topics.clone()));
        let harness = cx.new(|_| TopicsHarness { topics });
        Root::new(harness, window, cx)
    });
    let topics = capture.borrow_mut().take().expect("Topics entity");
    cx.run_until_parked();

    draw(&root, cx);
    click_debug(cx, "topic-create");
    cx.update(|window, app| assert!(window.has_active_dialog(app)));
    draw(&root, cx);
    let form = cx.read(|app| topics.read(app).dialog_form_for_test().expect("Create form"));
    fake.queue_create(Ok(outcome(TopicMutationKind::Create, false)));
    fake.queue_inventory(Ok(inventory(12, 13)));
    cx.update(|window, app| {
        form.update(app, |form, cx| {
            form.set_create_text("created-orders", 12, 13, window, cx);
            form.set_create_options(&[1], 7, TopicMessageType::Fifo, true, cx);
        });
    });
    cx.simulate_keystrokes("enter");
    cx.run_until_parked();

    let calls = fake.calls();
    assert_eq!(calls.create.len(), 1);
    let call = &calls.create[0];
    assert_eq!(call.topic.as_str(), "created-orders");
    assert_eq!(call.targets, [target("cluster-b", "broker-b", 20911)]);
    assert_eq!((call.read_queue_count, call.write_queue_count), (12, 13));
    assert_eq!(call.permission, TopicPermission::parse(7).expect("permission"));
    assert_eq!(call.message_type, TopicMessageType::Fifo);
    assert!(call.ordered);
}

#[gpui::test]
fn visible_detail_buttons_open_and_submit_the_six_exact_mutation_intents(cx: &mut gpui::TestAppContext) {
    cx.update(gpui_component::init);
    let fake = Arc::new(FakeTopicBackend::default());
    fake.queue_inventory(Ok(inventory(8, 8)));
    let services = AppServices::default().with_topic_backend(fake.clone());
    let capture = Rc::new(RefCell::new(None));
    let capture_topics = capture.clone();
    let (root, cx) = cx.add_window_view(move |window, cx| {
        let topics = cx.new(|cx| TopicsView::new(window, services, 7, cx));
        topics.update(cx, |topics, cx| topics.ensure_loaded(window, cx));
        capture_topics.replace(Some(topics.clone()));
        let harness = cx.new(|_| TopicsHarness { topics });
        Root::new(harness, window, cx)
    });
    let topics = capture.borrow_mut().take().expect("Topics entity");
    cx.run_until_parked();

    fake.queue_inventory(Ok(partial_inventory(vec![item(8, 8)])));
    cx.update(|_, app| topics.update(app, |topics, cx| topics.refresh(cx)));
    cx.run_until_parked();
    draw(&root, cx);
    click_debug(cx, "topic-create");
    cx.update(|window, app| assert!(!window.has_active_dialog(app)));

    fake.queue_inventory(Ok(inventory(8, 8)));
    cx.update(|_, app| topics.update(app, |topics, cx| topics.refresh(cx)));
    cx.run_until_parked();
    fake.queue_inventory(Ok(inventory(8, 8)));
    cx.update(|window, app| {
        topics.update(app, |topics, cx| {
            topics.open_route("orders", TopicTab::Overview, window, cx)
        });
    });
    cx.run_until_parked();
    draw(&root, cx);

    fake.queue_send(Ok(()));
    click_debug(cx, "topic-overview-send");
    let send_form = cx.read(|app| topics.read(app).dialog_form_for_test().expect("Send form"));
    cx.update(|window, app| {
        let form = send_form.read(app);
        assert!(matches!(&form.state.kind, TopicDialogKind::Send(draft) if draft.topic == topic()));
        send_form.update(app, |form, cx| {
            form.set_send_text("key", "tag", "ephemeral body", window, cx)
        });
    });
    cx.simulate_keystrokes("enter");
    cx.run_until_parked();
    close_dialog(cx);

    let detail = cx.read(|app| topics.read(app).detail.clone().expect("detail entity"));
    fake.queue_config(Ok(config(8, 8, 4)));
    cx.update(|_, app| detail.update(app, |detail, cx| detail.set_tab(TopicTab::Configuration, cx)));
    cx.run_until_parked();
    draw(&root, cx);

    click_debug(cx, "topic-config-edit-0");
    let edit_form = cx.read(|app| topics.read(app).dialog_form_for_test().expect("Edit form"));
    cx.update(|window, app| {
        let form = edit_form.read(app);
        assert!(matches!(
            &form.state.kind,
            TopicDialogKind::Edit(draft)
                if draft.topic == topic()
                    && draft.target == target("cluster-a", "broker-a", 10911)
                    && draft.expected_version == 4
        ));
        edit_form.update(app, |form, cx| form.set_edit_queue_counts(10, 11, window, cx));
    });
    fake.queue_patch(Ok(BackendTopicQueuePatchResult::Applied {
        previous_version: 4,
        version: 5,
    }));
    fake.queue_config(Ok(config(10, 11, 5)));
    fake.queue_inventory(Ok(inventory(10, 11)));
    cx.simulate_keystrokes("enter");
    cx.run_until_parked();
    close_dialog(cx);
    draw(&root, cx);

    click_debug(cx, "topic-config-delete-broker-0");
    let delete_broker_form = cx.read(|app| topics.read(app).dialog_form_for_test().expect("Delete Broker form"));
    cx.update(|_, app| {
        assert!(matches!(
            &delete_broker_form.read(app).state.kind,
            TopicDialogKind::DeleteBroker { topic: command_topic, target: command_target }
                if command_topic == &topic() && command_target == &target("cluster-a", "broker-a", 10911)
        ));
    });
    fake.queue_delete_broker(Ok(outcome(TopicMutationKind::DeleteBroker, false)));
    fake.queue_inventory(Ok(inventory(10, 11)));
    cx.simulate_keystrokes("enter");
    cx.run_until_parked();
    close_dialog(cx);

    fake.queue_consumers(Ok(consumers()));
    cx.update(|_, app| detail.update(app, |detail, cx| detail.set_tab(TopicTab::Consumers, cx)));
    cx.run_until_parked();
    draw(&root, cx);

    click_debug(cx, "topic-consumer-reset-0");
    let reset_form = cx.read(|app| topics.read(app).dialog_form_for_test().expect("Reset form"));
    cx.update(|window, app| {
        assert!(matches!(
            &reset_form.read(app).state.kind,
            TopicDialogKind::ResetOffset { topic: command_topic, consumer_group, clusters, .. }
                if command_topic == &topic()
                    && consumer_group == "group-a"
                    && clusters == &["cluster-a".to_owned(), "cluster-b".to_owned()]
        ));
        reset_form.update(app, |form, cx| {
            form.set_reset_timestamp(1234, window, cx);
            form.select_exact_cluster("cluster-b", cx);
        });
    });
    fake.queue_reset(Ok(outcome(TopicMutationKind::ResetOffset, false)));
    fake.queue_consumers(Ok(consumers()));
    cx.simulate_keystrokes("enter");
    cx.run_until_parked();
    close_dialog(cx);
    draw(&root, cx);

    click_debug(cx, "topic-consumer-skip-0");
    let skip_form = cx.read(|app| topics.read(app).dialog_form_for_test().expect("Skip form"));
    cx.update(|_, app| {
        assert!(matches!(
            &skip_form.read(app).state.kind,
            TopicDialogKind::SkipAccumulated { topic: command_topic, consumer_group, clusters, .. }
                if command_topic == &topic()
                    && consumer_group == "group-a"
                    && clusters == &["cluster-a".to_owned(), "cluster-b".to_owned()]
        ));
        skip_form.update(app, |form, cx| form.select_exact_cluster("cluster-a", cx));
    });
    fake.queue_skip(Ok(outcome(TopicMutationKind::SkipBacklog, false)));
    fake.queue_consumers(Ok(consumers()));
    cx.simulate_keystrokes("enter");
    cx.run_until_parked();
    close_dialog(cx);

    fake.queue_inventory(Ok(inventory(10, 11)));
    cx.update(|_, app| detail.update(app, |detail, cx| detail.set_tab(TopicTab::Overview, cx)));
    cx.run_until_parked();
    draw(&root, cx);
    click_debug(cx, "topic-overview-delete");
    let delete_form = cx.read(|app| topics.read(app).dialog_form_for_test().expect("Delete Topic form"));
    cx.update(|window, app| {
        assert!(matches!(
            &delete_form.read(app).state.kind,
            TopicDialogKind::DeleteTopic { topic: command_topic, clusters }
                if command_topic == &topic()
                    && clusters == &["cluster-a".to_owned(), "cluster-b".to_owned()]
        ));
        delete_form.update(app, |form, cx| form.set_delete_confirmation("orders", window, cx));
    });
    fake.queue_delete(Ok(outcome(TopicMutationKind::DeleteTopic, false)));
    fake.queue_inventory(Ok(inventory(10, 11)));
    cx.simulate_keystrokes("enter");
    cx.run_until_parked();

    let calls = fake.calls();
    assert_eq!(calls.send.len(), 1);
    assert_eq!(calls.send[0].body_length, "ephemeral body".len());
    assert_eq!(calls.patch.len(), 1);
    assert_eq!(calls.patch[0].expected_version, 4);
    assert_eq!(calls.delete_broker.len(), 1);
    assert_eq!(calls.delete_broker[0].2, target("cluster-a", "broker-a", 10911));
    assert_eq!(calls.reset.len(), 1);
    assert_eq!(calls.reset[0].cluster_name, "cluster-b");
    assert_eq!(calls.reset[0].timestamp, Some(1234));
    assert_eq!(calls.skip.len(), 1);
    assert_eq!(calls.skip[0].cluster_name, "cluster-a");
    assert_eq!(calls.delete.len(), 1);
    assert_eq!(calls.delete[0].2, ["cluster-a", "cluster-b"]);
}

#[gpui::test]
fn remaining_six_dialogs_render_and_reach_the_coordinator_with_exact_commands(cx: &mut gpui::TestAppContext) {
    cx.update(gpui_component::init);
    let fake = Arc::new(FakeTopicBackend::default());
    fake.queue_inventory(Ok(inventory(8, 8)));
    let services = AppServices::default().with_topic_backend(fake.clone());
    let capture = Rc::new(RefCell::new(None));
    let capture_topics = capture.clone();
    let (root, cx) = cx.add_window_view(move |window, cx| {
        let topics = cx.new(|cx| TopicsView::new(window, services, 7, cx));
        topics.update(cx, |topics, cx| topics.ensure_loaded(window, cx));
        capture_topics.replace(Some(topics.clone()));
        let harness = cx.new(|_| TopicsHarness { topics });
        Root::new(harness, window, cx)
    });
    let topics = capture.borrow_mut().take().expect("Topics entity");
    cx.run_until_parked();
    let broker_target = target("cluster-a", "broker-a", 10911);

    fake.queue_patch(Ok(BackendTopicQueuePatchResult::Applied {
        previous_version: 4,
        version: 5,
    }));
    fake.queue_config(Ok(config(10, 11, 5)));
    fake.queue_inventory(Ok(inventory(10, 11)));
    cx.update(|window, app| {
        topics.update(app, |topics, cx| {
            topics.open_topic_dialog(
                TopicDialogKind::Edit(TopicEditDraft {
                    topic: topic(),
                    target: broker_target.clone(),
                    expected_version: 4,
                    read_queue_count: 8,
                    write_queue_count: 8,
                }),
                window,
                cx,
            );
        });
    });
    draw(&root, cx);
    let form = cx.read(|app| topics.read(app).dialog_form_for_test().expect("Edit form"));
    cx.update(|window, app| {
        form.update(app, |form, cx| form.set_edit_queue_counts(10, 11, window, cx));
    });
    cx.simulate_keystrokes("enter");
    cx.run_until_parked();
    close_dialog(cx);

    fake.queue_delete(Ok(outcome(TopicMutationKind::DeleteTopic, false)));
    fake.queue_inventory(Ok(inventory(10, 11)));
    cx.update(|window, app| {
        topics.update(app, |topics, cx| {
            topics.open_topic_dialog(
                TopicDialogKind::DeleteTopic {
                    topic: topic(),
                    clusters: vec!["cluster-a".into(), "cluster-b".into()],
                },
                window,
                cx,
            );
        });
    });
    draw(&root, cx);
    let form = cx.read(|app| topics.read(app).dialog_form_for_test().expect("Delete form"));
    cx.update(|window, app| {
        form.update(app, |form, cx| form.set_delete_confirmation("orders", window, cx));
    });
    cx.simulate_keystrokes("enter");
    cx.run_until_parked();
    close_dialog(cx);

    fake.queue_delete_broker(Ok(outcome(TopicMutationKind::DeleteBroker, false)));
    fake.queue_inventory(Ok(inventory(10, 11)));
    cx.update(|window, app| {
        topics.update(app, |topics, cx| {
            topics.open_topic_dialog(
                TopicDialogKind::DeleteBroker {
                    topic: topic(),
                    target: broker_target.clone(),
                },
                window,
                cx,
            );
            assert!(window.has_active_dialog(cx));
        });
    });
    draw(&root, cx);
    cx.simulate_keystrokes("enter");
    cx.run_until_parked();
    close_dialog(cx);

    fake.queue_send(Ok(()));
    cx.update(|window, app| {
        topics.update(app, |topics, cx| {
            topics.open_topic_dialog(TopicDialogKind::Send(TopicSendDraft::new(topic())), window, cx);
        });
    });
    draw(&root, cx);
    let form = cx.read(|app| topics.read(app).dialog_form_for_test().expect("Send form"));
    cx.update(|window, app| {
        form.update(app, |form, cx| {
            form.set_send_text("key", "tag", "ephemeral body", window, cx)
        });
    });
    cx.simulate_keystrokes("enter");
    cx.run_until_parked();
    close_dialog(cx);

    fake.queue_reset(Ok(outcome(TopicMutationKind::ResetOffset, true)));
    fake.queue_consumers(Ok(TopicConsumersView {
        topic: topic(),
        items: Vec::new(),
        completeness: TopicCompleteness::Complete,
        failures: Vec::new(),
    }));
    cx.update(|window, app| {
        topics.update(app, |topics, cx| {
            topics.open_topic_dialog(
                TopicDialogKind::ResetOffset {
                    topic: topic(),
                    consumer_group: "group-a".into(),
                    clusters: vec!["cluster-a".into(), "cluster-b".into()],
                    timestamp: 0,
                    force: true,
                },
                window,
                cx,
            );
        });
    });
    draw(&root, cx);
    let form = cx.read(|app| topics.read(app).dialog_form_for_test().expect("Reset form"));
    cx.update(|window, app| {
        form.update(app, |form, cx| {
            form.set_reset_timestamp(1234, window, cx);
            form.select_exact_cluster("cluster-b", cx);
        });
    });
    cx.simulate_keystrokes("enter");
    cx.run_until_parked();
    close_dialog(cx);

    fake.queue_skip(Ok(outcome(TopicMutationKind::SkipBacklog, true)));
    fake.queue_consumers(Ok(TopicConsumersView {
        topic: topic(),
        items: Vec::new(),
        completeness: TopicCompleteness::Complete,
        failures: Vec::new(),
    }));
    cx.update(|window, app| {
        topics.update(app, |topics, cx| {
            topics.open_topic_dialog(
                TopicDialogKind::SkipAccumulated {
                    topic: topic(),
                    consumer_group: "group-a".into(),
                    clusters: vec!["cluster-a".into(), "cluster-b".into()],
                    force: false,
                },
                window,
                cx,
            );
        });
    });
    draw(&root, cx);
    let form = cx.read(|app| topics.read(app).dialog_form_for_test().expect("Skip form"));
    cx.update(|_, app| {
        form.update(app, |form, cx| form.select_exact_cluster("cluster-a", cx));
    });
    cx.simulate_keystrokes("enter");
    cx.run_until_parked();

    let calls = fake.calls();
    assert_eq!(calls.patch.len(), 1);
    assert_eq!(
        (calls.patch[0].read_queue_count, calls.patch[0].write_queue_count),
        (Some(10), Some(11))
    );
    assert_eq!(calls.delete[0].2, ["cluster-a", "cluster-b"]);
    assert_eq!(calls.delete_broker[0].2, broker_target);
    assert_eq!(calls.send[0].body_length, "ephemeral body".len());
    assert!(calls.send[0].has_key && calls.send[0].has_tag);
    assert_eq!(calls.reset[0].cluster_name, "cluster-b");
    assert_eq!(calls.reset[0].timestamp, Some(1234));
    assert!(calls.reset[0].force);
    assert_eq!(calls.skip[0].cluster_name, "cluster-a");
    assert_eq!(calls.skip[0].timestamp, None);
    assert_eq!(calls.consumers.len(), 2);
    cx.update(|_, app| {
        let form = topics.read(app).dialog_form_for_test().expect("Skip form");
        assert!(matches!(
            form.read(app).state.submission,
            super::topic_dialogs::TopicSubmissionState::PartiallySucceeded(_)
        ));
        assert!(matches!(topics.read(app).store.inventory.state, Loadable::Ready(_)));
    });
}

#[gpui::test]
fn partial_inventory_missing_or_containing_the_selection_stays_unverified_until_complete_reload(
    cx: &mut gpui::TestAppContext,
) {
    cx.update(gpui_component::init);
    let fake = Arc::new(FakeTopicBackend::default());
    fake.queue_inventory(Ok(inventory(8, 8)));
    let services = AppServices::default().with_topic_backend(fake.clone());
    let capture = Rc::new(RefCell::new(None));
    let capture_topics = capture.clone();
    let (root, cx) = cx.add_window_view(move |window, cx| {
        let topics = cx.new(|cx| TopicsView::new(window, services, 7, cx));
        topics.update(cx, |topics, cx| topics.ensure_loaded(window, cx));
        capture_topics.replace(Some(topics.clone()));
        let harness = cx.new(|_| TopicsHarness { topics });
        Root::new(harness, window, cx)
    });
    let topics = capture.borrow_mut().take().expect("Topics entity");
    cx.run_until_parked();

    fake.queue_inventory(Ok(inventory(8, 8)));
    cx.update(|window, app| {
        topics.update(app, |topics, cx| {
            topics.open_route("orders", TopicTab::Overview, window, cx)
        });
    });
    cx.run_until_parked();

    fake.queue_inventory(Ok(partial_inventory(Vec::new())));
    cx.update(|_, app| topics.update(app, |topics, cx| topics.refresh(cx)));
    cx.run_until_parked();
    draw(&root, cx);
    cx.update(|window, app| {
        let view = topics.read(app);
        let selected = &view.store.detail.as_ref().expect("retained selection").selected;
        assert_eq!(selected.item.identity, topic());
        assert!(selected.stale);
        assert!(!selected.inventory_verified());
        assert_eq!(selected.inventory_failures[0].target, "broker-b");
        assert!(!window.has_active_dialog(app));
    });
    assert!(cx.debug_bounds("topic-inventory-partial-evidence").is_some());

    let mut refreshed = item(16, 17);
    refreshed.message_type = TopicMessageType::Fifo;
    fake.queue_inventory(Ok(partial_inventory(vec![refreshed.clone()])));
    cx.update(|_, app| topics.update(app, |topics, cx| topics.refresh(cx)));
    cx.run_until_parked();
    let detail = cx.read(|app| topics.read(app).detail.clone().expect("detail entity"));
    fake.queue_inventory(Ok(partial_inventory(vec![refreshed])));
    cx.update(|_, app| detail.update(app, |detail, cx| detail.retry_for_test(TopicTab::Overview, cx)));
    cx.run_until_parked();
    draw(&root, cx);
    cx.update(|window, app| {
        let selected = &topics.read(app).store.detail.as_ref().expect("selection").selected;
        assert_eq!(selected.item.read_queue_count, Some(16));
        assert!(selected.stale);
        assert!(!selected.inventory_verified());
        assert!(!window.has_active_dialog(app));
    });
    click_debug(cx, "topic-overview-send");
    cx.update(|window, app| assert!(!window.has_active_dialog(app)));
    click_debug(cx, "topic-overview-delete");
    cx.update(|window, app| assert!(!window.has_active_dialog(app)));

    fake.queue_config(Ok(config(16, 17, 7)));
    cx.update(|_, app| detail.update(app, |detail, cx| detail.set_tab(TopicTab::Configuration, cx)));
    cx.run_until_parked();
    draw(&root, cx);
    click_debug(cx, "topic-config-edit-0");
    cx.update(|window, app| assert!(!window.has_active_dialog(app)));
    click_debug(cx, "topic-config-delete-broker-0");
    cx.update(|window, app| assert!(!window.has_active_dialog(app)));

    fake.queue_consumers(Ok(consumers()));
    cx.update(|_, app| detail.update(app, |detail, cx| detail.set_tab(TopicTab::Consumers, cx)));
    cx.run_until_parked();
    draw(&root, cx);
    click_debug(cx, "topic-consumer-reset-0");
    cx.update(|window, app| assert!(!window.has_active_dialog(app)));
    click_debug(cx, "topic-consumer-skip-0");
    cx.update(|window, app| assert!(!window.has_active_dialog(app)));

    fake.queue_inventory(Ok(inventory(16, 17)));
    cx.update(|_, app| topics.update(app, |topics, cx| topics.refresh(cx)));
    cx.run_until_parked();
    cx.update(|_, app| detail.update(app, |detail, cx| detail.set_tab(TopicTab::Overview, cx)));
    draw(&root, cx);
    cx.update(|_, app| {
        assert!(
            topics
                .read(app)
                .store
                .detail
                .as_ref()
                .expect("selection")
                .selected
                .inventory_verified()
        );
    });
    click_debug(cx, "topic-overview-send");
    cx.update(|window, app| assert!(window.has_active_dialog(app)));
    close_dialog(cx);
}

#[gpui::test]
fn independent_overview_partial_inventory_revokes_parent_verification_until_complete_retry(
    cx: &mut gpui::TestAppContext,
) {
    cx.update(gpui_component::init);
    let fake = Arc::new(FakeTopicBackend::default());
    fake.queue_inventory(Ok(inventory(8, 8)));
    let services = AppServices::default().with_topic_backend(fake.clone());
    let capture = Rc::new(RefCell::new(None));
    let capture_topics = capture.clone();
    let (root, cx) = cx.add_window_view(move |window, cx| {
        let topics = cx.new(|cx| TopicsView::new(window, services, 7, cx));
        topics.update(cx, |topics, cx| topics.ensure_loaded(window, cx));
        capture_topics.replace(Some(topics.clone()));
        let harness = cx.new(|_| TopicsHarness { topics });
        Root::new(harness, window, cx)
    });
    let topics = capture.borrow_mut().take().expect("Topics entity");
    cx.run_until_parked();

    let mut partial_item = item(16, 17);
    partial_item.message_type = TopicMessageType::Fifo;
    fake.queue_inventory(Ok(partial_inventory(vec![partial_item])));
    cx.update(|window, app| {
        topics.update(app, |topics, cx| {
            topics.open_route("orders", TopicTab::Overview, window, cx)
        });
    });
    cx.run_until_parked();
    draw(&root, cx);

    let detail = cx.read(|app| topics.read(app).detail.clone().expect("detail entity"));
    cx.update(|window, app| {
        let selected = &detail.read(app).store.selected;
        assert_eq!(selected.item.read_queue_count, Some(16));
        assert!(selected.stale);
        assert!(!selected.inventory_verified());
        assert_eq!(selected.inventory_failures[0].target, "broker-b");
        let parent_selected = &topics
            .read(app)
            .store
            .detail
            .as_ref()
            .expect("parent selection")
            .selected;
        assert_eq!(parent_selected, selected);
        assert!(!parent_selected.inventory_verified());
        assert!(!window.has_active_dialog(app));
    });
    assert!(cx.debug_bounds("topic-inventory-partial-evidence").is_some());
    click_debug(cx, "topic-overview-send");
    cx.update(|window, app| assert!(!window.has_active_dialog(app)));
    click_debug(cx, "topic-overview-delete");
    cx.update(|window, app| assert!(!window.has_active_dialog(app)));

    cx.update(|_, app| {
        detail.update(app, |detail, cx| {
            cx.emit(TopicDetailIntent::Send(detail.store.selected.item.identity.clone()));
        });
    });
    cx.run_until_parked();
    cx.update(|window, app| assert!(!window.has_active_dialog(app)));

    let mut forged = cx.read(|app| detail.read(app).store.selected.clone());
    forged.stale = false;
    forged.inventory_completeness = TopicCompleteness::Complete;
    forged.inventory_failures.clear();
    cx.update(|_, app| {
        detail.update(app, |_, cx| {
            cx.emit(TopicDetailIntent::SelectionEvidenceUpdated {
                revision: 8,
                topic: topic(),
                selected: forged,
            });
        });
    });
    cx.run_until_parked();
    cx.update(|_, app| {
        assert!(
            !topics
                .read(app)
                .store
                .detail
                .as_ref()
                .expect("parent selection")
                .selected
                .inventory_verified()
        );
    });

    fake.queue_inventory(Ok(inventory(20, 21)));
    cx.update(|_, app| detail.update(app, |detail, cx| detail.retry_for_test(TopicTab::Overview, cx)));
    cx.run_until_parked();
    draw(&root, cx);
    cx.update(|_, app| {
        let selected = &detail.read(app).store.selected;
        assert_eq!(selected.item.read_queue_count, Some(20));
        assert!(selected.inventory_verified());
        let parent_selected = &topics
            .read(app)
            .store
            .detail
            .as_ref()
            .expect("parent selection")
            .selected;
        assert_eq!(parent_selected, selected);
        assert!(parent_selected.inventory_verified());
    });
    click_debug(cx, "topic-overview-send");
    cx.update(|window, app| assert!(window.has_active_dialog(app)));
    close_dialog(cx);
}

#[gpui::test]
fn detail_failed_retry_partial_evidence_and_empty_route_use_real_product_states(cx: &mut gpui::TestAppContext) {
    cx.update(gpui_component::init);
    let fake = Arc::new(FakeTopicBackend::default());
    fake.queue_stats(Err(UiError::new(
        "Stats are temporarily unavailable.",
        UiErrorCode::Connection,
        true,
    )));
    fake.queue_stats(Ok(TopicStatsView {
        topic: topic(),
        total_message_count: 0,
        offsets: Vec::new(),
        completeness: TopicCompleteness::Partial {
            successful_target_count: 1,
            failed_target_count: 1,
        },
        failures: vec![TopicTargetFailure {
            target: "broker-b".into(),
            stage: TopicFailureStage::Stats,
            code: TopicFailureCode::Unavailable,
            retryable: true,
        }],
    }));
    let services = AppServices::default().with_topic_backend(fake.clone());
    let capture = Rc::new(RefCell::new(None));
    let capture_detail = capture.clone();
    let (root, cx) = cx.add_window_view(move |window, cx| {
        let detail = cx.new(|cx| TopicDetail::new(services, 7, item(8, 8), TopicTab::Stats, cx));
        capture_detail.replace(Some(detail.clone()));
        Root::new(detail, window, cx)
    });
    let detail = capture.borrow_mut().take().expect("Topic detail entity");
    cx.run_until_parked();
    cx.update(|_, app| assert!(matches!(detail.read(app).store.stats.state, Loadable::Failed { .. })));
    draw(&root, cx);
    cx.update(|_, app| detail.update(app, |detail, cx| detail.retry_for_test(TopicTab::Stats, cx)));
    cx.run_until_parked();
    cx.update(|_, app| {
        let Loadable::Ready(stats) = &detail.read(app).store.stats.state else {
            panic!("Retry should install the partial Stats evidence");
        };
        assert_eq!(stats.failures.len(), 1);
        assert!(matches!(stats.completeness, TopicCompleteness::Partial { .. }));
    });
    fake.queue_stats(Ok(TopicStatsView {
        topic: topic(),
        total_message_count: 0,
        offsets: Vec::new(),
        completeness: TopicCompleteness::Complete,
        failures: Vec::new(),
    }));
    cx.update(|_, app| detail.update(app, |detail, cx| detail.retry_for_test(TopicTab::Stats, cx)));
    cx.run_until_parked();
    cx.update(|_, app| assert!(matches!(detail.read(app).store.stats.state, Loadable::Empty)));
    let calls = fake.calls();
    assert_eq!(calls.stats.len(), 3);
    assert!(calls.stats[1].0.epoch > calls.stats[0].0.epoch);
    assert!(calls.stats[2].0.epoch > calls.stats[1].0.epoch);

    fake.queue_route(Ok(TopicRouteView {
        topic: topic(),
        brokers: Vec::new(),
        queues: Vec::new(),
    }));
    cx.update(|_, app| detail.update(app, |detail, cx| detail.set_tab(TopicTab::Route, cx)));
    cx.run_until_parked();
    cx.update(|_, app| assert!(matches!(detail.read(app).store.route.state, Loadable::Empty)));
    assert_eq!(fake.calls().route.len(), 1);
}

#[gpui::test]
fn edit_conflict_keeps_the_submitted_draft_until_the_user_resolves_it_in_the_official_dialog(
    cx: &mut gpui::TestAppContext,
) {
    cx.update(gpui_component::init);
    let fake = Arc::new(FakeTopicBackend::default());
    fake.queue_inventory(Ok(inventory(8, 8)));
    fake.queue_patch(Ok(BackendTopicQueuePatchResult::VersionConflict {
        expected_version: 7,
        actual_version: 9,
        latest: rocketmq_admin_core::core::topic::TopicConfigCasState {
            version: 9,
            read_queue_nums: 20,
            write_queue_nums: 21,
            order: false,
        },
    }));
    let services = AppServices::default().with_topic_backend(fake.clone());
    let capture = Rc::new(RefCell::new(None));
    let capture_topics = capture.clone();
    let (root, cx) = cx.add_window_view(move |window, cx| {
        let topics = cx.new(|cx| TopicsView::new(window, services, 7, cx));
        topics.update(cx, |topics, cx| topics.ensure_loaded(window, cx));
        capture_topics.replace(Some(topics.clone()));
        let harness = cx.new(|_| TopicsHarness { topics });
        Root::new(harness, window, cx)
    });
    let topics = capture.borrow_mut().take().expect("Topics entity");
    cx.run_until_parked();
    cx.update(|window, app| {
        topics.update(app, |topics, cx| {
            topics.open_topic_dialog(
                TopicDialogKind::Edit(TopicEditDraft {
                    topic: topic(),
                    target: target("cluster-a", "broker-a", 10911),
                    expected_version: 7,
                    read_queue_count: 8,
                    write_queue_count: 8,
                }),
                window,
                cx,
            );
        });
    });
    draw(&root, cx);
    let form = cx.read(|app| topics.read(app).dialog_form_for_test().expect("Edit form"));
    cx.update(|window, app| {
        form.update(app, |form, cx| form.set_edit_queue_counts(12, 13, window, cx));
    });
    cx.simulate_keystrokes("enter");
    cx.run_until_parked();
    cx.update(|_, app| {
        assert!(matches!(
            form.read(app).state.submission,
            super::topic_dialogs::TopicSubmissionState::Conflict {
                actual_version: 9,
                submitted_read_queue_count: 12,
                submitted_write_queue_count: 13,
                authoritative_read_queue_count: 20,
                authoritative_write_queue_count: 21,
            }
        ));
    });
    draw(&root, cx);
    cx.update(|window, app| {
        form.update(app, |form, cx| form.keep_submitted_after_conflict(window, cx));
    });

    fake.queue_patch(Ok(BackendTopicQueuePatchResult::Applied {
        previous_version: 9,
        version: 10,
    }));
    fake.queue_config(Ok(config(12, 13, 10)));
    fake.queue_inventory(Ok(inventory(12, 13)));
    cx.simulate_keystrokes("enter");
    cx.run_until_parked();
    let calls = fake.calls();
    assert_eq!(calls.patch.len(), 2);
    assert_eq!(calls.patch[0].expected_version, 7);
    assert_eq!(calls.patch[1].expected_version, 9);
    assert_eq!(
        (calls.patch[1].read_queue_count, calls.patch[1].write_queue_count),
        (Some(12), Some(13))
    );
}

#[gpui::test]
fn delete_target_race_is_rejected_without_reload_or_replay(cx: &mut gpui::TestAppContext) {
    cx.update(gpui_component::init);
    let fake = Arc::new(FakeTopicBackend::default());
    fake.queue_inventory(Ok(inventory(8, 8)));
    fake.queue_delete(Ok(TopicPartialOutcome {
        topic: topic(),
        kind: TopicMutationKind::DeleteTopic,
        guarantee: TopicMutationGuarantee::PreflightBestEffort,
        targets: vec![TopicTargetOutcome {
            target: "cluster set changed".into(),
            stage: TopicFailureStage::Mutation,
            applied: false,
            failure: Some(TopicFailureCode::Conflict),
            retryable: false,
        }],
        reload_failed: false,
    }));
    let services = AppServices::default().with_topic_backend(fake.clone());
    let capture = Rc::new(RefCell::new(None));
    let capture_topics = capture.clone();
    let (root, cx) = cx.add_window_view(move |window, cx| {
        let topics = cx.new(|cx| TopicsView::new(window, services, 7, cx));
        topics.update(cx, |topics, cx| topics.ensure_loaded(window, cx));
        capture_topics.replace(Some(topics.clone()));
        let harness = cx.new(|_| TopicsHarness { topics });
        Root::new(harness, window, cx)
    });
    let topics = capture.borrow_mut().take().expect("Topics entity");
    cx.run_until_parked();
    cx.update(|window, app| {
        topics.update(app, |topics, cx| {
            topics.open_topic_dialog(
                TopicDialogKind::DeleteTopic {
                    topic: topic(),
                    clusters: vec!["cluster-a".into(), "cluster-b".into()],
                },
                window,
                cx,
            );
        });
    });
    draw(&root, cx);
    let form = cx.read(|app| topics.read(app).dialog_form_for_test().expect("Delete form"));
    cx.update(|window, app| {
        form.update(app, |form, cx| form.set_delete_confirmation("orders", window, cx));
    });
    cx.simulate_keystrokes("enter");
    cx.run_until_parked();
    cx.update(|_, app| {
        form.update(app, |form, _| {
            assert!(matches!(
                form.state.submission,
                super::topic_dialogs::TopicSubmissionState::Rejected(_)
            ));
            assert!(form.state.begin_submit(7).is_none());
        });
    });
    let calls = fake.calls();
    assert_eq!(calls.delete.len(), 1);
    assert_eq!(calls.inventory.len(), 1, "rejected preflight must not trigger a reload");
}
