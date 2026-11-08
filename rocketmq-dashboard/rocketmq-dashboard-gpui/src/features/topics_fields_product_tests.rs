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

//! Full-field Topic paths through official controls and the service boundary.

use super::*;

#[gpui::test]
fn send_trace_checkbox_submits_default_enabled_and_disabled_values(cx: &mut gpui::TestAppContext) {
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
    let topics = capture.borrow_mut().take().unwrap();
    cx.run_until_parked();
    for clicks in [0, 1, 2] {
        fake.queue_send(Ok(()));
        cx.update(|window, app| {
            topics.update(app, |topics, cx| {
                topics.open_topic_dialog(TopicDialogKind::Send(TopicSendDraft::new(topic())), window, cx);
            })
        });
        let form = cx.read(|app| topics.read(app).dialog_form_for_test().unwrap());
        cx.update(|window, app| form.update(app, |form, cx| form.set_send_text("", "", "ephemeral body", window, cx)));
        draw(&root, cx);
        for _ in 0..clicks {
            click_debug(cx, "topic-send-trace");
            draw(&root, cx);
        }
        cx.simulate_keystrokes("enter");
        cx.run_until_parked();
        assert_eq!(fake.calls().send.len(), clicks + 1);
        assert_eq!(fake.calls().send.last().unwrap().trace_enabled, clicks == 1);
        cx.read(|app| assert!(form.read(app).send_body_is_empty(app)));
        close_dialog(cx);
    }
    assert_eq!(
        fake.calls()
            .send
            .iter()
            .map(|call| call.trace_enabled)
            .collect::<Vec<_>>(),
        [false, true, false]
    );
}

#[gpui::test]
fn full_field_controls_freeze_cluster_targets_and_rebase_only_conflicted_brokers(cx: &mut gpui::TestAppContext) {
    cx.update(gpui_component::init);
    let fake = Arc::new(FakeTopicBackend::default());
    let mut catalog = inventory(8, 8);
    catalog.targets = vec![
        target("cluster-a", "broker-a", 10911),
        target("cluster-a", "broker-b", 20911),
        target("cluster-b", "broker-c", 30911),
    ];
    fake.queue_inventory(Ok(catalog.clone()));
    let primary = config(8, 8, 7).targets.remove(0);
    let second = TopicConfigTargetView {
        target: target("cluster-a", "broker-b", 20911),
        version: 9,
        read_queue_count: 10,
        write_queue_count: 16,
        permission: TopicPermission::parse(2).ok(),
        message_type: TopicMessageType::Delay,
        ..primary.clone()
    };
    let third = TopicConfigTargetView {
        target: target("cluster-b", "broker-c", 30911),
        version: 3,
        ..primary.clone()
    };
    let latest = TopicConfigTargetView {
        version: 11,
        read_queue_count: 20,
        write_queue_count: 21,
        permission: TopicPermission::parse(4).ok(),
        ..second.clone()
    };
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
    let topics = capture.borrow_mut().take().unwrap();
    cx.run_until_parked();
    cx.update(|window, app| {
        topics.update(app, |topics, cx| {
            topics.open_topic_dialog(
                TopicDialogKind::Edit(
                    TopicEditDraft::new(topic(), &primary, vec![primary.clone(), second, third]).unwrap(),
                ),
                window,
                cx,
            );
        })
    });
    let form = cx.read(|app| topics.read(app).dialog_form_for_test().unwrap());
    cx.update(|window, app| form.update(app, |form, cx| form.set_edit_queue_counts(12, 8, window, cx)));
    draw(&root, cx);
    for selector in [
        "topic-edit-cluster-0",
        "topic-edit-permission-7",
        "topic-edit-message-type-1",
        "topic-edit-ordered-true",
    ] {
        click_debug(cx, selector);
        draw(&root, cx);
    }
    fake.queue_fields(Ok(TopicConfigEditOutcome {
        outcome: TopicPartialOutcome {
            topic: topic(),
            kind: TopicMutationKind::Edit,
            guarantee: TopicMutationGuarantee::VersionCas,
            targets: vec![
                TopicTargetOutcome {
                    target: "broker-a".into(),
                    stage: TopicFailureStage::Mutation,
                    applied: true,
                    failure: None,
                    retryable: false,
                },
                TopicTargetOutcome {
                    target: "broker-b".into(),
                    stage: TopicFailureStage::Mutation,
                    applied: false,
                    failure: Some(TopicFailureCode::Conflict),
                    retryable: false,
                },
            ],
            reload_failed: false,
        },
        conflicts: vec![latest.clone()],
    }));
    fake.queue_config(Ok(config(12, 8, 8)));
    fake.queue_inventory(Ok(catalog.clone()));
    cx.simulate_keystrokes("enter");
    cx.run_until_parked();
    let first = fake.calls().fields.remove(0);
    assert_eq!(
        first
            .targets
            .iter()
            .map(|target| target.target.broker_name())
            .collect::<Vec<_>>(),
        ["broker-a", "broker-b"]
    );
    assert_eq!(
        first.targets.iter().map(|target| target.version).collect::<Vec<_>>(),
        [7, 9]
    );
    assert_eq!(
        first.patch,
        TopicConfigFieldsPatch {
            read_queue_count: Some(12),
            permission: TopicPermission::parse(7).ok(),
            ordered: Some(true),
            message_type: Some(TopicMessageType::Fifo),
            ..Default::default()
        }
    );
    cx.simulate_keystrokes("enter");
    cx.run_until_parked();
    assert_eq!(fake.calls().fields.len(), 1);
    draw(&root, cx);
    click_debug(cx, "topic-edit-keep-submitted");
    draw(&root, cx);
    fake.queue_fields(Ok(TopicConfigEditOutcome {
        outcome: outcome(TopicMutationKind::Edit, false),
        conflicts: Vec::new(),
    }));
    fake.queue_config(Ok(config(12, 21, 12)));
    fake.queue_inventory(Ok(catalog));
    cx.simulate_keystrokes("enter");
    cx.run_until_parked();
    let calls = fake.calls();
    assert_eq!(calls.fields.len(), 2);
    assert_eq!(calls.fields[1].targets.len(), 1);
    assert_eq!(calls.fields[1].targets[0].target.broker_name(), "broker-b");
    assert_eq!(calls.fields[1].targets[0].version, 11);
    let values = latest.editable_values().unwrap().with_patch(calls.fields[1].patch);
    assert_eq!(values.read_queue_count, 12);
    assert_eq!(values.write_queue_count, 21);
    assert_eq!(values.permission.bits(), 7);
    assert!(values.ordered);
    assert_eq!(values.message_type, TopicMessageType::Fifo);
}

#[gpui::test]
fn environment_clear_keeps_accepted_field_edit_owned_and_rejects_its_late_refresh(cx: &mut gpui::TestAppContext) {
    cx.update(gpui_component::init);
    let fake = Arc::new(FakeTopicBackend::default());
    fake.queue_inventory(Ok(inventory(8, 8)));
    let completion = fake.queue_pending_fields();
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
    let topics = capture.borrow_mut().take().unwrap();
    cx.run_until_parked();
    cx.update(|window, app| {
        topics.update(app, |topics, cx| {
            topics.open_topic_dialog(TopicDialogKind::Edit(edit_draft(8, 8, 7)), window, cx)
        })
    });
    let form = cx.read(|app| topics.read(app).dialog_form_for_test().unwrap());
    cx.update(|window, app| form.update(app, |form, cx| form.set_edit_queue_counts(12, 8, window, cx)));
    draw(&root, cx);
    cx.simulate_keystrokes("enter");
    cx.run_until_parked();
    assert_eq!(fake.calls().fields.len(), 1);
    cx.update(|window, app| {
        topics.update(app, |topics, cx| {
            assert!(topics.mutation_task.is_some());
            topics.clear_for_environment(8, window, cx);
            assert!(
                topics.mutation_task.is_some(),
                "accepted mutations retain their owner across environment clearing"
            );
            assert!(topics.dialog_form_for_test().is_none());
        });
        window.close_all_dialogs(app);
    });
    fake.queue_config(Ok(config(12, 8, 8)));
    fake.queue_inventory(Ok(inventory(12, 8)));
    assert!(
        completion
            .send(Ok(TopicConfigEditOutcome {
                outcome: outcome(TopicMutationKind::Edit, false),
                conflicts: Vec::new()
            }))
            .is_ok()
    );
    cx.run_until_parked();
    cx.read(|app| {
        let view = topics.read(app);
        assert!(view.mutation_task.is_none());
        assert!(view.dialog_form_for_test().is_none());
        assert!(matches!(view.store.inventory.state, Loadable::Idle));
        assert!(view.detail.is_none());
    });
    assert_eq!(fake.calls().fields.len(), 1);
    assert!(fake.calls().config.iter().all(|(scope, _)| scope.revision == 7));
}
