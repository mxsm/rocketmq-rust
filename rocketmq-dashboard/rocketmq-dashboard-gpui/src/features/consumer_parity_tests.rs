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
use rocketmq_dashboard_common::{
    ConsumerConfigPatch, ConsumerQueryScope, TopicCategory, TopicInventory, TopicInventoryItem, TopicMessageType,
    TopicPermission,
};

fn second_target() -> ConsumerTargetIdentity {
    ConsumerTargetIdentity::parse("cluster-a", "broker-b", "127.0.0.1:10912").unwrap()
}
fn two_target_inventory() -> ConsumerInventory {
    let mut value = inventory(vec![group_observation()]);
    value.targets.push(second_target());
    value.groups[0].targets.push(second_target());
    value
}
fn set_number(
    form: &Entity<super::super::consumer_dialogs::ConsumerDialogForm>,
    field: ConsumerNumberField,
    value: &str,
    cx: &mut gpui::VisualTestContext,
) {
    let input = cx.read(|app| form.read(app).number_input(field));
    click_debug(cx, field.selector());
    cx.simulate_keystrokes("ctrl-a");
    cx.simulate_input(value);
    cx.run_until_parked();
    assert_eq!(cx.read(|app| input.read(app).value().to_string()), value);
}

fn reach_compact_consumer_flags(root: &Entity<Root>, cx: &mut gpui::VisualTestContext, width: f32, height: f32) {
    cx.simulate_resize(size(px(width), px(height)));
    draw(root, cx, width, height);
    let body = cx.debug_bounds("consumer-mutation-form").unwrap();
    let confirm = cx.debug_bounds("consumer-dialog-confirm").unwrap();
    let close = cx.debug_bounds("consumer-dialog-close").unwrap();
    assert!(body.bottom() <= confirm.top() && body.bottom() <= close.top());
    assert!(confirm.bottom() <= px(height) && close.bottom() <= px(height));
    cx.simulate_event(gpui::ScrollWheelEvent {
        position: body.center(),
        delta: gpui::ScrollDelta::Pixels(point(px(0.), px(-3000.))),
        ..Default::default()
    });
    draw(root, cx, width, height);
    let body = cx.debug_bounds("consumer-mutation-form").unwrap();
    for selector in [
        "consumer-flag-0",
        "consumer-flag-1",
        "consumer-flag-2",
        "consumer-flag-3",
    ] {
        let flag = cx.debug_bounds(selector).unwrap();
        assert!(
            flag.top() >= body.top() && flag.bottom() <= body.bottom(),
            "unreachable {selector}: {flag:?}, {body:?}"
        );
        assert!(flag.left() >= body.left() && flag.right() <= body.right());
    }
    assert!(cx.debug_bounds("consumer-dialog-confirm").unwrap().bottom() <= px(height));
}

#[gpui::test]
fn compact_consumer_dialogs_scroll_to_flags_and_retained_results_leave_the_list_reachable(
    cx: &mut gpui::TestAppContext,
) {
    let backend = Arc::new(FakeConsumerBackend::default());
    let mut observed = inventory(vec![group_observation()]);
    observed.targets = (0..20)
        .map(|index| {
            ConsumerTargetIdentity::parse(
                "cluster-a",
                format!("broker-{index}"),
                format!("127.0.0.1:{}", 10911 + index),
            )
            .unwrap()
        })
        .collect();
    backend.queue_inventory(Ok(observed.clone()));
    let (root, cx, view) = mount(cx, AppServices::default().with_consumer_backend(backend.clone()));
    draw(&root, cx, 1440., 900.);
    let group_input = cx.read(|app| view.read(app).create_group_for_test());
    cx.update(|window, app| group_input.update(app, |input, cx| input.set_value("compact-consumer", window, cx)));
    click_debug(cx, "consumer-create");
    draw(&root, cx, 1440., 900.);
    click_debug(cx, "consumer-select-cluster-0");
    reach_compact_consumer_flags(&root, cx, 986., 610.);
    click_debug(cx, "consumer-flag-3");
    reach_compact_consumer_flags(&root, cx, 1440., 900.);
    reach_compact_consumer_flags(&root, cx, 960., 640.);
    let mut outcome = consumer_outcome(ConsumerMutationKind::Create, false);
    outcome.group = ConsumerIdentity::parse("compact-consumer").unwrap();
    outcome.targets = (0..20)
        .map(|index| ConsumerTargetOutcome {
            target: format!("broker-{index}"),
            ..outcome.targets[0].clone()
        })
        .collect();
    backend.queue_create(Ok(outcome));
    backend.queue_inventory(Ok(observed));
    click_debug(cx, "consumer-dialog-confirm");
    assert_eq!(backend.calls().create.len(), 1);
    assert!(backend.calls().create[0].1.entries.consume_message_orderly);
    draw(&root, cx, 960., 640.);
    click_debug(cx, "consumer-dialog-close");
    for (width, height) in [(986., 610.), (960., 640.)] {
        cx.simulate_resize(size(px(width), px(height)));
        draw(&root, cx, width, height);
        let notices = cx.debug_bounds("consumer-result-notices").unwrap();
        let table = cx.debug_bounds("consumer-inventory-table").unwrap();
        assert!(
            table.size.height > px(100.),
            "retained results collapsed the list: {table:?}"
        );
        assert!(table.top() >= notices.bottom() && table.bottom() <= px(height));
        let first_before = cx.debug_bounds("consumer-mutation-target-0").unwrap();
        cx.simulate_event(gpui::ScrollWheelEvent {
            position: notices.center(),
            delta: gpui::ScrollDelta::Pixels(point(px(0.), px(-3000.))),
            ..Default::default()
        });
        draw(&root, cx, width, height);
        let last = cx.debug_bounds("consumer-mutation-target-19").unwrap();
        assert!(last.top() >= notices.top() && last.bottom() <= notices.bottom());
        if width == 986. {
            assert!(cx.debug_bounds("consumer-mutation-target-0").unwrap().top() < first_before.top());
        }
        let row = cx.debug_bounds("consumer-copy-row-0").unwrap();
        assert!(row.top() >= table.top() && row.bottom() <= table.bottom());
        click_debug(cx, "consumer-copy-row-0");
        assert_eq!(
            cx.update(|_, app| app.read_from_clipboard().and_then(|value| value.text())),
            Some(group().as_str().to_owned())
        );
    }
    backend.queue_configuration(Ok(configuration(7, 16)));
    cx.update(|window, app| {
        view.update(app, |view, cx| {
            view.open_route(group().as_str(), ConsumerTab::Configuration, window, cx)
        })
    });
    draw(&root, cx, 960., 640.);
    click_debug(cx, "consumer-edit-config-0");
    reach_compact_consumer_flags(&root, cx, 986., 610.);
    click_debug(cx, "consumer-flag-3");
    reach_compact_consumer_flags(&root, cx, 1440., 900.);
    reach_compact_consumer_flags(&root, cx, 960., 640.);
    click_debug(cx, "consumer-dialog-close");
    let detail = cx.read(|app| view.read(app).detail_for_test().unwrap());
    assert!(cx.read(|app| detail.read(app).mutation_dialog_for_test().is_none()));
    assert!(
        backend.calls().patch.is_empty(),
        "closing the reachable Edit footer must not submit"
    );
}

#[gpui::test]
fn consumer_create_form_submits_real_nondefault_fields_and_only_the_selected_cluster_subset(
    cx: &mut gpui::TestAppContext,
) {
    let backend = Arc::new(FakeConsumerBackend::default());
    let mut partial = two_target_inventory();
    partial.observation = ConsumerObservationState::Partial;
    partial.groups[0].client_count = ConsumerObservation::Unknown {
        reason: rocketmq_dashboard_common::ConsumerUnknownReason::Unavailable,
    };
    partial.groups[0].lag = ConsumerObservation::Unknown {
        reason: rocketmq_dashboard_common::ConsumerUnknownReason::Unavailable,
    };
    partial.failures = [ConsumerFailureStage::Clients, ConsumerFailureStage::Progress]
        .into_iter()
        .map(|stage| ConsumerTargetFailure {
            target: "offline-unrelated-group".into(),
            stage,
            code: ConsumerFailureCode::Unavailable,
            retryable: true,
        })
        .collect();
    backend.queue_inventory(Ok(partial));
    let (root, cx, view) = mount(cx, AppServices::default().with_consumer_backend(backend.clone()));
    cx.run_until_parked();
    let group_input = cx.read(|app| view.read(app).create_group_for_test());
    cx.update(|window, app| group_input.update(app, |input, cx| input.set_value("new-consumer", window, cx)));
    draw(&root, cx, 1440., 1100.);
    click_debug(cx, "consumer-create");
    draw(&root, cx, 1440., 1100.);
    let form = cx.read(|app| view.read(app).create_dialog_for_test().unwrap());
    assert!(cx.read(|app| matches!(
        view.read(app).store.inventory.state.value().unwrap().groups[0].lag,
        ConsumerObservation::Unknown { .. }
    )));
    set_number(&form, ConsumerNumberField::RetryMax, "3", cx);
    set_number(&form, ConsumerNumberField::RetryQueues, "0", cx);
    set_number(&form, ConsumerNumberField::Timeout, "7", cx);
    set_number(&form, ConsumerNumberField::BrokerId, "2", cx);
    set_number(&form, ConsumerNumberField::SlowBrokerId, "4", cx);
    click_debug(cx, "consumer-select-cluster-0");
    draw(&root, cx, 1440., 1100.);
    click_debug(cx, "consumer-target-0");
    draw(&root, cx, 1440., 1100.);
    let bounds = cx.debug_bounds("consumer-mutation-form").unwrap();
    cx.simulate_event(gpui::ScrollWheelEvent {
        position: bounds.center(),
        delta: gpui::ScrollDelta::Pixels(point(px(0.), px(-300.))),
        ..Default::default()
    });
    for selector in [
        "consumer-flag-0",
        "consumer-flag-1",
        "consumer-flag-2",
        "consumer-flag-3",
    ] {
        draw(&root, cx, 1440., 1100.);
        click_debug(cx, selector);
    }
    let mut outcome = consumer_outcome(ConsumerMutationKind::Create, false);
    outcome.group = ConsumerIdentity::parse("new-consumer").unwrap();
    outcome.targets[0].target = "broker-b".into();
    backend.queue_create(Ok(outcome));
    backend.queue_inventory(Ok(two_target_inventory()));
    draw(&root, cx, 1440., 1100.);
    click_debug(cx, "consumer-dialog-confirm");
    let calls = backend.calls();
    assert_eq!(calls.create.len(), 1);
    let command = &calls.create[0].1;
    assert_eq!(command.targets, [second_target()]);
    assert_eq!(command.entries.retry_max_times, 3);
    assert_eq!(command.entries.retry_queue_nums, 0);
    assert_eq!(command.entries.consume_timeout_minutes, 7);
    assert_eq!(command.entries.broker_id, 2);
    assert_eq!(command.entries.which_broker_when_consume_slowly, 4);
    assert!(!command.entries.consume_enable);
    assert!(command.entries.consume_from_min_enable);
    assert!(command.entries.consume_broadcast_enable);
    assert!(command.entries.consume_message_orderly);
    draw(&root, cx, 1440., 1100.);
    click_debug(cx, "consumer-dialog-confirm");
    assert_eq!(backend.calls().create.len(), 1);
}

#[gpui::test]
fn consumer_create_stays_disabled_without_trusted_nonempty_target_discovery(cx: &mut gpui::TestAppContext) {
    let backend = Arc::new(FakeConsumerBackend::default());
    let mut untrusted = two_target_inventory();
    untrusted.target_observation = ConsumerObservationState::Unknown;
    backend.queue_inventory(Ok(untrusted));
    let (root, cx, view) = mount(cx, AppServices::default().with_consumer_backend(backend.clone()));
    draw(&root, cx, 930., 620.);
    click_debug(cx, "consumer-create");
    assert!(cx.read(|app| view.read(app).create_dialog_for_test().is_none()));
    let mut missing = two_target_inventory();
    missing.targets.clear();
    backend.queue_inventory(Ok(missing));
    click_debug(cx, "consumer-refresh");
    draw(&root, cx, 930., 620.);
    click_debug(cx, "consumer-create");
    assert!(cx.read(|app| view.read(app).create_dialog_for_test().is_none()));
    assert!(backend.calls().create.is_empty());
}

#[gpui::test]
fn consumer_long_identity_copy_and_diagnostics_stay_inside_narrow_sheet(cx: &mut gpui::TestAppContext) {
    let backend = Arc::new(FakeConsumerBackend::default());
    let mut observed = group_observation();
    let long_group = format!("orders-{}", "long-consumer-identity-".repeat(9));
    observed.identity = ConsumerIdentity::parse(long_group.clone()).unwrap();
    backend.queue_inventory(Ok(inventory(vec![observed])));
    let (root, cx, view) = mount(cx, AppServices::default().with_consumer_backend(backend.clone()));
    cx.simulate_resize(size(px(930.), px(620.)));
    draw(&root, cx, 930., 620.);
    let page = cx.debug_bounds("consumer-inventory-table").unwrap();
    let description = cx.debug_bounds("consumer-read-description").unwrap();
    assert!(description.origin.x + description.size.width <= page.origin.x + page.size.width);
    click_debug(cx, "consumer-copy-row-0");
    assert_eq!(
        cx.update(|_, app| app.read_from_clipboard().and_then(|value| value.text())),
        Some(long_group.clone())
    );
    let long_client = format!("client-{}", "long-instance-id-".repeat(25));
    let ConsumerObservation::Complete(mut clients) = clients() else {
        unreachable!()
    };
    clients.group = ConsumerIdentity::parse(long_group.clone()).unwrap();
    clients.clients[0].identity = ConsumerClientIdentity::parse(long_client.clone()).unwrap();
    clients.clients[0].address = format!("{}:10911", "very-long-address-component-".repeat(12));
    clients.subscriptions = vec![rocketmq_dashboard_common::ConsumerSubscription {
        topic: "orders".repeat(20),
        expression_type: "TAG".into(),
        expression: "tag-with-a-long-name-".repeat(25),
    }];
    backend.queue_clients(Ok(ConsumerObservation::Complete(clients)));
    cx.update(|window, app| {
        view.update(app, |view, cx| {
            view.open_route(&long_group, ConsumerTab::Clients, window, cx)
        })
    });
    draw(&root, cx, 930., 620.);
    click_debug(cx, "consumer-copy-group");
    assert_eq!(
        cx.update(|_, app| app.read_from_clipboard().and_then(|value| value.text())),
        Some(long_group.clone())
    );
    let viewport = cx.debug_bounds("consumer-detail").unwrap();
    assert!(
        viewport.size.height <= px(620.),
        "detail viewport must fit the actual window: {viewport:?}"
    );
    cx.simulate_event(gpui::ScrollWheelEvent {
        position: viewport.center(),
        delta: gpui::ScrollDelta::Pixels(point(px(0.), px(-300.))),
        ..Default::default()
    });
    draw(&root, cx, 930., 620.);
    let client_copy = cx.debug_bounds("consumer-copy-client-0").unwrap();
    let sheet = cx.debug_bounds("consumer-detail").unwrap();
    assert!(client_copy.origin.x + client_copy.size.width <= sheet.origin.x + sheet.size.width);
    assert!(
        client_copy.origin.y >= px(0.) && client_copy.origin.y + client_copy.size.height <= px(620.),
        "copy button must be reachable through the actual sheet scroll: {client_copy:?}, viewport={viewport:?}, sheet={sheet:?}"
    );
    click_debug(cx, "consumer-copy-client-0");
    assert_eq!(
        cx.update(|_, app| app.read_from_clipboard().and_then(|value| value.text())),
        Some(long_client)
    );
    backend.queue_diagnostic(Ok(ConsumerDiagnosticPayload::new(
        vec![("Long property".into(), "x".repeat(500))],
        Some("bounded long diagnostic line ".repeat(80)),
        false,
    )));
    click_debug(cx, "consumer-running-info-0");
    draw(&root, cx, 930., 620.);
    for width in [930., 580.] {
        cx.simulate_resize(size(px(width), px(620.)));
        draw(&root, cx, width, 620.);
        let sheet = cx.debug_bounds("consumer-detail").unwrap();
        assert!(
            sheet.size.width <= px(width),
            "detail must fit window width {width}: {sheet:?}"
        );
        assert!(
            sheet.origin.x >= px(0.) && sheet.origin.x + sheet.size.width <= px(width),
            "detail must stay within the actual window: {sheet:?}"
        );
        assert!(
            sheet.origin.y + sheet.size.height <= px(620.),
            "detail scroll viewport must stay above the window bottom: {sheet:?}"
        );
        for selector in [
            "consumer-detail-group",
            "consumer-client-identity-0",
            "consumer-running-info-0",
            "consumer-jstack-0",
            "consumer-diagnostic-scroll",
        ] {
            let bounds = cx
                .debug_bounds(selector)
                .unwrap_or_else(|| panic!("missing {selector}"));
            assert!(bounds.origin.x >= sheet.origin.x, "{selector} crosses the left edge");
            assert!(
                bounds.origin.x + bounds.size.width <= sheet.origin.x + sheet.size.width,
                "{selector} crosses the right edge"
            );
        }
        let scroll = cx.debug_bounds("consumer-diagnostic-scroll").unwrap();
        let text = cx.debug_bounds("consumer-diagnostic-text").unwrap();
        let identity = cx.debug_bounds("consumer-client-identity-0").unwrap();
        assert!(
            identity.size.height > px(24.),
            "the full long Client identity must wrap"
        );
        assert!(
            text.size.width > scroll.size.width,
            "long diagnostic lines need a horizontal scroll range"
        );
    }
    let viewport = cx.debug_bounds("consumer-detail").unwrap();
    cx.simulate_event(gpui::ScrollWheelEvent {
        position: viewport.center(),
        delta: gpui::ScrollDelta::Pixels(point(px(0.), px(-2000.))),
        ..Default::default()
    });
    draw(&root, cx, 580., 620.);
    let diagnostic = cx.debug_bounds("consumer-diagnostic-scroll").unwrap();
    assert!(
        diagnostic.origin.y >= viewport.origin.y
            && diagnostic.origin.y + diagnostic.size.height <= viewport.origin.y + viewport.size.height,
        "the full diagnostic viewport must be reachable by scrolling: {diagnostic:?}"
    );
    let before = cx.debug_bounds("consumer-diagnostic-text").unwrap();
    cx.simulate_event(gpui::ScrollWheelEvent {
        position: diagnostic.center(),
        delta: gpui::ScrollDelta::Pixels(point(px(-300.), px(0.))),
        ..Default::default()
    });
    draw(&root, cx, 580., 620.);
    let after = cx.debug_bounds("consumer-diagnostic-text").unwrap();
    assert!(
        after.origin.x < before.origin.x,
        "horizontal scrolling must expose the rest of the diagnostic line"
    );
}

#[gpui::test]
fn consumer_edit_form_decreases_default_sixteen_and_submits_a_multi_field_versioned_patch(
    cx: &mut gpui::TestAppContext,
) {
    let backend = Arc::new(FakeConsumerBackend::default());
    backend.queue_inventory(Ok(inventory(vec![group_observation()])));
    let (root, cx, view) = mount(cx, AppServices::default().with_consumer_backend(backend.clone()));
    cx.run_until_parked();
    let mut config = configuration(7, 16);
    config.snapshots[0].entries.retry_queue_nums = 4;
    config.snapshots[0].entries.consume_timeout_minutes = 30;
    config.snapshots[0].entries.group_sys_flag = 23;
    config.snapshots[0].entries.notify_consumer_ids_changed_enable = false;
    config.snapshots[0].read_only.attributes = vec![("custom".into(), "preserved".into())];
    backend.queue_configuration(Ok(config.clone()));
    cx.update(|window, app| {
        view.update(app, |view, cx| {
            view.open_route(group().as_str(), ConsumerTab::Configuration, window, cx)
        })
    });
    draw(&root, cx, 1440., 1100.);
    click_debug(cx, "consumer-edit-config-0");
    draw(&root, cx, 1440., 1100.);
    let detail = cx.read(|app| view.read(app).detail_for_test().unwrap());
    let form = cx.read(|app| detail.read(app).mutation_dialog_for_test().unwrap());
    set_number(&form, ConsumerNumberField::RetryMax, "2", cx);
    set_number(&form, ConsumerNumberField::RetryQueues, "1", cx);
    set_number(&form, ConsumerNumberField::Timeout, "4", cx);
    set_number(&form, ConsumerNumberField::BrokerId, "5", cx);
    set_number(&form, ConsumerNumberField::SlowBrokerId, "6", cx);
    for selector in [
        "consumer-flag-0",
        "consumer-flag-1",
        "consumer-flag-2",
        "consumer-flag-3",
    ] {
        draw(&root, cx, 1440., 1100.);
        click_debug(cx, selector);
    }
    backend.queue_patch(Ok(ConsumerConfigPatchOutcome::Applied {
        previous_generation: 7,
        generation: 8,
    }));
    backend.queue_configuration(Ok(config));
    backend.queue_inventory(Ok(inventory(vec![group_observation()])));
    draw(&root, cx, 1440., 1100.);
    click_debug(cx, "consumer-dialog-confirm");
    let calls = backend.calls();
    assert_eq!(calls.patch.len(), 1);
    let command = &calls.patch[0].1;
    assert_eq!(command.snapshot.generation, 7);
    assert_eq!(
        command.patch,
        ConsumerConfigPatch {
            retry_max_times: Some(2),
            retry_queue_nums: Some(1),
            consume_timeout_minutes: Some(4),
            consume_enable: Some(false),
            consume_from_min_enable: Some(true),
            consume_broadcast_enable: Some(true),
            consume_message_orderly: Some(true),
            broker_id: Some(5),
            which_broker_when_consume_slowly: Some(6)
        }
    );
    assert_eq!(command.snapshot.entries.group_sys_flag, 23);
    assert!(!command.snapshot.entries.notify_consumer_ids_changed_enable);
    assert_eq!(command.snapshot.read_only.attributes[0].1, "preserved");
    draw(&root, cx, 1440., 1100.);
    click_debug(cx, "consumer-dialog-confirm");
    assert_eq!(backend.calls().patch.len(), 1);
}

#[gpui::test]
fn consumer_delete_status_reports_one_broker_when_two_cleanup_steps_succeed(cx: &mut gpui::TestAppContext) {
    let backend = Arc::new(FakeConsumerBackend::default());
    backend.queue_inventory(Ok(inventory(vec![group_observation()])));
    let (root, cx, view) = mount(cx, AppServices::default().with_consumer_backend(backend.clone()));
    backend.queue_inventory(Ok(inventory(vec![group_observation()])));
    cx.update(|window, app| {
        view.update(app, |view, cx| {
            view.open_route(group().as_str(), ConsumerTab::Overview, window, cx)
        })
    });
    draw(&root, cx, 1440., 960.);
    let detail = cx.read(|app| view.read(app).detail_for_test().unwrap());
    click_debug(cx, "consumer-delete");
    draw(&root, cx, 1440., 960.);
    click_debug(cx, "consumer-target-0");
    let mut outcome = consumer_outcome(ConsumerMutationKind::Delete, false);
    for name in ["retry-topic", "dlq-topic"] {
        outcome.targets.push(ConsumerTargetOutcome {
            target: name.into(),
            stage: ConsumerFailureStage::Cleanup,
            applied: true,
            failure: None,
            retryable: false,
        });
    }
    backend.queue_delete(Ok(outcome));
    backend.queue_inventory(Ok(inventory(Vec::new())));
    draw(&root, cx, 1440., 960.);
    click_debug(cx, "consumer-dialog-confirm");
    assert_eq!(
        cx.read(|app| detail.read(app).mutation_status_for_test().map(str::to_owned)),
        Some("Deleted on 1 Broker target(s); authoritative inventory reloaded.".into())
    );
    assert_eq!(backend.calls().delete.len(), 1);
}

#[gpui::test]
fn consumer_delete_form_keeps_authoritative_targets_and_submits_only_the_checked_subset(cx: &mut gpui::TestAppContext) {
    let backend = Arc::new(FakeConsumerBackend::default());
    backend.queue_inventory(Ok(two_target_inventory()));
    let (root, cx, view) = mount(cx, AppServices::default().with_consumer_backend(backend.clone()));
    cx.run_until_parked();
    backend.queue_inventory(Ok(two_target_inventory()));
    cx.update(|window, app| {
        view.update(app, |view, cx| {
            view.open_route(group().as_str(), ConsumerTab::Overview, window, cx)
        })
    });
    draw(&root, cx, 1440., 960.);
    click_debug(cx, "consumer-delete");
    draw(&root, cx, 1440., 960.);
    click_debug(cx, "consumer-target-1");
    let mut outcome = consumer_outcome(ConsumerMutationKind::Delete, false);
    outcome.targets[0].target = "broker-b".into();
    backend.queue_delete(Ok(outcome));
    backend.queue_inventory(Ok(inventory(vec![group_observation()])));
    click_debug(cx, "consumer-dialog-confirm");
    let calls = backend.calls();
    assert_eq!(calls.delete.len(), 1);
    assert_eq!(calls.delete[0].1.selected_targets, [second_target()]);
    assert_eq!(calls.delete[0].1.authoritative_targets, [target(), second_target()]);
    assert!(
        cx.read(|app| view.read(app).detail_for_test().is_some()),
        "A group remaining on broker-a keeps its Sheet"
    );
}

#[gpui::test]
fn consumer_query_scope_change_cancels_the_old_waiter_and_keeps_new_direct_observation(cx: &mut gpui::TestAppContext) {
    let backend = Arc::new(FakeConsumerBackend::default());
    backend.queue_inventory(Ok(inventory(vec![group_observation()])));
    let (root, cx, view) = mount(cx, AppServices::default().with_consumer_backend(backend.clone()));
    cx.run_until_parked();
    let mut stale = group_observation();
    stale.lag = ConsumerObservation::Complete(999);
    backend.queue_inventory(Ok(inventory(vec![stale])));
    let release = backend.hold_next_inventory();
    draw(&root, cx, 1440., 960.);
    click_debug(cx, "consumer-refresh");
    let mut current = group_observation();
    current.lag = ConsumerObservation::Complete(-7);
    backend.queue_inventory(Ok(inventory(vec![current])));
    draw(&root, cx, 1440., 960.);
    click_debug(cx, "consumer-query-scope");
    cx.run_until_parked();
    assert!(
        release.send(()).is_err(),
        "Leaving a query scope drops its old receiver"
    );
    let calls = backend.calls();
    assert_eq!(calls.inventory[1].query_scope, ConsumerQueryScope::Configured);
    assert_eq!(calls.inventory[2].query_scope, ConsumerQueryScope::BrokerDirect);
    assert_eq!(
        cx.read(|app| view.read(app).store.inventory.state.value().unwrap().groups[0]
            .lag
            .value()
            .copied()),
        Some(-7)
    );
}

#[gpui::test]
fn consumer_actual_diagnostic_control_clears_jstack_on_programmatic_detail_close(cx: &mut gpui::TestAppContext) {
    let backend = Arc::new(FakeConsumerBackend::default());
    backend.queue_inventory(Ok(inventory(vec![group_observation()])));
    let (root, cx, view) = mount(cx, AppServices::default().with_consumer_backend(backend.clone()));
    cx.run_until_parked();
    backend.queue_clients(Ok(clients()));
    cx.update(|window, app| {
        view.update(app, |view, cx| {
            view.open_route(group().as_str(), ConsumerTab::Clients, window, cx)
        })
    });
    backend.queue_diagnostic(Ok(ConsumerDiagnosticPayload::new(
        Vec::new(),
        Some("private-jstack".into()),
        false,
    )));
    draw(&root, cx, 1440., 960.);
    click_debug(cx, "consumer-jstack-0");
    let old_detail = cx.read(|app| view.read(app).detail_for_test().unwrap());
    assert_eq!(
        cx.read(|app| old_detail
            .read(app)
            .store
            .diagnostic
            .state
            .value()
            .unwrap()
            .text()
            .map(str::to_owned)),
        Some("private-jstack".into())
    );
    cx.update(|window, app| view.update(app, |view, cx| view.close_detail(window, cx)));
    draw(&root, cx, 1440., 960.);
    assert!(cx.read(|app| matches!(old_detail.read(app).store.diagnostic.state, Loadable::Idle)));
    assert!(cx.read(|app| view.read(app).detail_for_test().is_none()));
}

fn mount_producer(
    cx: &mut gpui::TestAppContext,
    services: AppServices,
) -> (Entity<Root>, &mut gpui::VisualTestContext, Entity<ProducersView>) {
    cx.update(gpui_component::init);
    let capture = Rc::new(RefCell::new(None));
    let copy = capture.clone();
    let (root, cx) = cx.add_window_view(move |window, cx| {
        let view = cx.new(|cx| ProducersView::new(window, services, 7, cx));
        view.update(cx, |view, cx| view.ensure_loaded(window, cx));
        copy.replace(Some(view.clone()));
        let harness = cx.new(|_| ProducersHarness { producers: view });
        Root::new(harness, window, cx)
    });
    let view = capture.borrow_mut().take().unwrap();
    (root, cx, view)
}
fn producer_inventory() -> ProducerInventory {
    ProducerInventory {
        groups: vec![ProducerGroupObservation {
            identity: ProducerIdentity::parse("orders-producer").unwrap(),
            client_count: ConsumerObservation::Complete(1),
        }],
        observation: ConsumerObservationState::Complete,
        failures: Vec::new(),
        capabilities: ProducerCapabilities::for_scope(ConnectionScope::NameServer),
    }
}
fn producer_query(topic: &str) -> ProducerConnectionQuery {
    ProducerConnectionQuery::try_from_draft(&ProducerConnectionQueryDraft {
        topic: topic.into(),
        group: "orders-producer".into(),
    })
    .unwrap()
}

#[gpui::test]
fn producer_real_topic_picker_and_group_row_submit_one_applied_pair_and_copy_client_fields(
    cx: &mut gpui::TestAppContext,
) {
    let backend = Arc::new(FakeConsumerBackend::default());
    backend.queue_producer_inventory(Ok(producer_inventory()));
    let topics = Arc::new(FakeTopicBackend::default());
    topics.queue_inventory(Ok(TopicInventory {
        items: ["orders", "payments"]
            .into_iter()
            .map(|name| TopicInventoryItem {
                identity: TopicIdentity::parse(name).unwrap(),
                category: TopicCategory::Application,
                message_type: TopicMessageType::Normal,
                clusters: vec!["cluster-a".into()],
                brokers: vec!["broker-a".into()],
                read_queue_count: Some(8),
                write_queue_count: Some(8),
                permission: TopicPermission::parse(6).ok(),
                ordered: Some(false),
            })
            .collect(),
        targets: Vec::new(),
        completeness: TopicCompleteness::Complete,
        failures: Vec::new(),
    }));
    let services = AppServices::default()
        .with_consumer_backend(backend.clone())
        .with_topic_backend(topics);
    let (root, cx, view) = mount_producer(cx, services);
    draw(&root, cx, 1440., 1100.);
    click_debug(cx, "producer-group-row-0");
    click_debug(cx, "producer-topic-candidates");
    cx.simulate_input("payments");
    cx.run_until_parked();
    cx.simulate_keystrokes("down enter");
    cx.run_until_parked();
    assert_eq!(
        cx.read(|app| view.read(app).store.draft_query.topic.clone()),
        "payments"
    );
    assert!(backend.calls().producer_connections.is_empty());
    backend.queue_producer_connections(Ok(ConsumerObservation::Complete(ProducerConnections {
        query: producer_query("payments"),
        clients: vec![client()],
    })));
    draw(&root, cx, 1440., 1100.);
    click_debug(cx, "producer-apply-query");
    let calls = backend.calls();
    assert_eq!(calls.producer_connections.len(), 1);
    assert_eq!(calls.producer_connections[0].1, producer_query("payments"));
    draw(&root, cx, 1440., 1100.);
    click_debug(cx, "producer-open-client-0");
    draw(&root, cx, 1440., 1100.);
    click_debug(cx, "producer-copy-address");
    assert_eq!(
        cx.read(|app| app.read_from_clipboard().and_then(|item| item.text())),
        Some(client().address)
    );
}

#[gpui::test]
fn producer_manual_fallback_validates_the_pair_and_drops_a_late_previous_query(cx: &mut gpui::TestAppContext) {
    let backend = Arc::new(FakeConsumerBackend::default());
    backend.queue_producer_inventory(Err(UiError::new(
        "discovery unavailable",
        UiErrorCode::Connection,
        true,
    )));
    let (root, cx, view) = mount_producer(cx, AppServices::default().with_consumer_backend(backend.clone()));
    cx.run_until_parked();
    let (topic, group) = cx.read(|app| view.read(app).query_inputs_for_test());
    cx.update(|window, app| {
        topic.update(app, |input, cx| input.set_value("invalid topic", window, cx));
        group.update(app, |input, cx| input.set_value("orders-producer", window, cx));
    });
    draw(&root, cx, 1440., 960.);
    click_debug(cx, "producer-apply-query");
    assert!(backend.calls().producer_connections.is_empty());
    backend.queue_producer_connections(Ok(ConsumerObservation::Complete(ProducerConnections {
        query: producer_query("orders"),
        clients: vec![client()],
    })));
    let release = backend.hold_next_producer_connections();
    cx.update(|window, app| topic.update(app, |input, cx| input.set_value("orders", window, cx)));
    draw(&root, cx, 1440., 960.);
    click_debug(cx, "producer-apply-query");
    backend.queue_producer_connections(Ok(ConsumerObservation::Complete(ProducerConnections {
        query: producer_query("payments"),
        clients: Vec::new(),
    })));
    cx.update(|window, app| topic.update(app, |input, cx| input.set_value("payments", window, cx)));
    draw(&root, cx, 1440., 960.);
    assert_eq!(backend.calls().producer_connections.len(), 1);
    click_debug(cx, "producer-apply-query");
    assert!(release.send(()).is_err());
    assert_eq!(
        cx.read(|app| view.read(app).store.applied_query.clone()),
        Some(producer_query("payments"))
    );
    assert!(cx.read(|app| {
        view.read(app)
            .store
            .connections
            .state
            .value()
            .unwrap()
            .value()
            .unwrap()
            .clients
            .is_empty()
    }));
    assert_eq!(backend.calls().producer_connections.len(), 2);
}
