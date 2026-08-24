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
    AppContext as _, Context, Entity, IntoElement, Modifiers, ParentElement as _, Render, Styled as _, div, point, px,
    size,
};
use gpui_component::{Root, WindowExt as _};
use rocketmq_dashboard_common::{
    ConnectionScope, ConsumerCapabilities, ConsumerCategory, ConsumerClientIdentity, ConsumerClientObservation,
    ConsumerClients, ConsumerConfigEntries, ConsumerConfigIdentity, ConsumerConfigPatchOutcome, ConsumerConfigSnapshot,
    ConsumerConfiguration, ConsumerConnectionState, ConsumerDiagnosticKind, ConsumerDiagnosticPayload,
    ConsumerFailureCode, ConsumerFailureStage, ConsumerGroupObservation, ConsumerIdentity, ConsumerInventory,
    ConsumerMutationGuarantee, ConsumerMutationKind, ConsumerObservation, ConsumerObservationState,
    ConsumerPartialOutcome, ConsumerProgress, ConsumerProgressRow, ConsumerTargetFailure, ConsumerTargetIdentity,
    ConsumerTargetOutcome, ProducerCapabilities, ProducerConnectionQuery, ProducerConnectionQueryDraft,
    ProducerConnections, ProducerGroupObservation, ProducerIdentity, ProducerInventory, TopicCompleteness,
    TopicConsumerView, TopicConsumersView, TopicIdentity, TopicMutationGuarantee, TopicMutationKind,
    TopicPartialOutcome, TopicTargetOutcome,
};

use super::{consumers::ConsumersView, producers::ProducersView};
use crate::{
    route::ConsumerTab,
    services::{AppServices, consumers::test_support::FakeConsumerBackend, topics::test_support::FakeTopicBackend},
    state::{Loadable, UiError, UiErrorCode},
};

struct ConsumersHarness {
    consumers: Entity<ConsumersView>,
}

struct ProducersHarness {
    producers: Entity<ProducersView>,
}

impl Render for ProducersHarness {
    fn render(&mut self, window: &mut gpui::Window, cx: &mut Context<Self>) -> impl IntoElement {
        div()
            .size_full()
            .child(self.producers.clone())
            .children(Root::render_sheet_layer(window, cx))
    }
}

impl Render for ConsumersHarness {
    fn render(&mut self, window: &mut gpui::Window, cx: &mut Context<Self>) -> impl IntoElement {
        div()
            .size_full()
            .child(self.consumers.clone())
            .children(Root::render_sheet_layer(window, cx))
            .children(Root::render_dialog_layer(window, cx))
    }
}

fn group() -> ConsumerIdentity {
    ConsumerIdentity::parse("orders-consumer").expect("group")
}

fn target() -> ConsumerTargetIdentity {
    ConsumerTargetIdentity::parse("cluster-a", "broker-a", "127.0.0.1:10911").expect("target")
}

fn group_observation() -> ConsumerGroupObservation {
    ConsumerGroupObservation {
        identity: group(),
        category: ConsumerCategory::Application,
        connection_state: ConsumerObservation::Complete(ConsumerConnectionState::Disconnected),
        client_count: ConsumerObservation::Complete(0),
        lag: ConsumerObservation::Complete(-3),
        consume_type: ConsumerObservation::Complete("PUSH".into()),
        message_model: ConsumerObservation::Complete("CLUSTERING".into()),
        targets: vec![target()],
    }
}

fn inventory(groups: Vec<ConsumerGroupObservation>) -> ConsumerInventory {
    ConsumerInventory {
        groups,
        targets: vec![target()],
        observation: ConsumerObservationState::Complete,
        failures: Vec::new(),
        capabilities: ConsumerCapabilities::for_scope(ConnectionScope::NameServer),
    }
}

fn configuration(generation: u64, retry_max_times: u32) -> ConsumerConfiguration {
    ConsumerConfiguration {
        group: group(),
        snapshots: vec![ConsumerConfigSnapshot {
            identity: ConsumerConfigIdentity {
                group: group(),
                target: target(),
            },
            generation,
            entries: ConsumerConfigEntries {
                retry_max_times,
                retry_queue_nums: 1,
                consume_timeout_minutes: 15,
            },
        }],
        observation: ConsumerObservationState::Complete,
        failures: Vec::new(),
    }
}

fn progress(delta: i64) -> ConsumerObservation<ConsumerProgress> {
    ConsumerObservation::Complete(ConsumerProgress::from_rows(
        group(),
        vec![ConsumerProgressRow {
            topic: "orders".into(),
            broker_name: "broker-a".into(),
            queue_id: 0,
            broker_offset: 7,
            consumer_offset: 10,
            delta,
            last_timestamp: 11,
        }],
    ))
}

fn partial_progress(delta: i64) -> ConsumerObservation<ConsumerProgress> {
    let ConsumerObservation::Complete(value) = progress(delta) else {
        unreachable!("progress fixture is complete")
    };
    ConsumerObservation::Partial {
        value,
        successful_target_count: 1,
        failures: vec![ConsumerTargetFailure {
            target: "broker-b".into(),
            stage: ConsumerFailureStage::Progress,
            code: ConsumerFailureCode::Unavailable,
            retryable: true,
        }],
    }
}

fn client() -> ConsumerClientObservation {
    ConsumerClientObservation {
        identity: ConsumerClientIdentity::parse("client-a").expect("client"),
        address: "127.0.0.1:31000".into(),
        language: "RUST".into(),
        version: 1,
        version_description: "V1".into(),
    }
}

fn clients() -> ConsumerObservation<ConsumerClients> {
    ConsumerObservation::Complete(ConsumerClients {
        group: group(),
        clients: vec![client()],
        consume_type: ConsumerObservation::Complete("PUSH".into()),
        message_model: ConsumerObservation::Complete("CLUSTERING".into()),
        subscriptions: Vec::new(),
    })
}

fn consumer_outcome(kind: ConsumerMutationKind, partial: bool) -> ConsumerPartialOutcome {
    let mut targets = vec![ConsumerTargetOutcome {
        target: "broker-a".into(),
        stage: rocketmq_dashboard_common::ConsumerFailureStage::Mutation,
        applied: true,
        failure: None,
        retryable: false,
    }];
    if partial {
        targets.push(ConsumerTargetOutcome {
            target: "broker-b".into(),
            stage: rocketmq_dashboard_common::ConsumerFailureStage::Mutation,
            applied: false,
            failure: Some(rocketmq_dashboard_common::ConsumerFailureCode::Unavailable),
            retryable: true,
        });
    }
    ConsumerPartialOutcome {
        group: group(),
        kind,
        guarantee: ConsumerMutationGuarantee::PreflightBestEffort,
        targets,
        reload_failed: false,
    }
}

fn topic_outcome(kind: TopicMutationKind) -> TopicPartialOutcome {
    TopicPartialOutcome {
        topic: TopicIdentity::parse("orders").expect("topic"),
        kind,
        guarantee: TopicMutationGuarantee::PreflightBestEffort,
        targets: vec![
            TopicTargetOutcome {
                target: "broker-a / queue 0".into(),
                stage: rocketmq_dashboard_common::TopicFailureStage::Mutation,
                applied: true,
                failure: None,
                retryable: false,
            },
            TopicTargetOutcome {
                target: "broker-a / queue 1".into(),
                stage: rocketmq_dashboard_common::TopicFailureStage::Mutation,
                applied: false,
                failure: Some(rocketmq_dashboard_common::TopicFailureCode::Unavailable),
                retryable: true,
            },
        ],
        reload_failed: false,
    }
}

fn topic_consumers() -> TopicConsumersView {
    TopicConsumersView {
        topic: TopicIdentity::parse("orders").expect("topic"),
        items: vec![TopicConsumerView {
            consumer_group: group().as_str().into(),
            total_diff: -3,
            inflight_diff: 0,
            consume_tps: 0.0,
        }],
        completeness: TopicCompleteness::Complete,
        failures: Vec::new(),
    }
}

fn draw(root: &Entity<Root>, cx: &mut gpui::VisualTestContext, width: f32, height: f32) {
    cx.run_until_parked();
    cx.draw(point(px(0.), px(0.)), size(px(width), px(height)), |_, _| root.clone());
    cx.draw(point(px(0.), px(0.)), size(px(width), px(height)), |_, _| root.clone());
}

fn click_debug(cx: &mut gpui::VisualTestContext, selector: &'static str) {
    let bounds = cx
        .debug_bounds(selector)
        .unwrap_or_else(|| panic!("missing selector: {selector}"));
    cx.simulate_click(bounds.center(), Modifiers::default());
    cx.run_until_parked();
}

fn mount(
    cx: &mut gpui::TestAppContext,
    services: AppServices,
) -> (Entity<Root>, &mut gpui::VisualTestContext, Entity<ConsumersView>) {
    cx.update(gpui_component::init);
    let capture = Rc::new(RefCell::new(None));
    let capture_view = Rc::clone(&capture);
    let (root, visual) = cx.add_window_view(move |window, cx| {
        let consumers = cx.new(|cx| ConsumersView::new(window, services, 7, cx));
        consumers.update(cx, |consumers, cx| consumers.ensure_loaded(window, cx));
        capture_view.replace(Some(consumers.clone()));
        let harness = cx.new(|_| ConsumersHarness { consumers });
        Root::new(harness, window, cx)
    });
    let consumers = capture.borrow_mut().take().expect("Consumers entity");
    (root, visual, consumers)
}

#[gpui::test]
fn create_button_submits_exact_targets_and_authoritatively_reloads(cx: &mut gpui::TestAppContext) {
    let fake = Arc::new(FakeConsumerBackend::default());
    fake.queue_inventory(Ok(inventory(vec![group_observation()])));
    let services = AppServices::default().with_consumer_backend(fake.clone());
    let (root, cx, consumers) = mount(cx, services);
    cx.run_until_parked();
    let input = cx.read(|app| consumers.read(app).create_group_for_test());
    cx.update(|window, app| input.update(app, |input, cx| input.set_value("new-consumer", window, cx)));
    fake.queue_create(Ok(consumer_outcome(ConsumerMutationKind::Create, false)));
    fake.queue_inventory(Ok(inventory(vec![group_observation()])));
    draw(&root, cx, 1440., 960.);
    click_debug(cx, "consumer-create");

    let calls = fake.calls();
    assert_eq!(calls.create.len(), 1);
    assert_eq!(calls.create[0].1.group.as_str(), "new-consumer");
    assert_eq!(calls.create[0].1.targets, [target()]);
    assert_eq!(calls.inventory.len(), 2);
    assert!(cx.read(|app| {
        consumers
            .read(app)
            .mutation_status_for_test()
            .is_some_and(|status| status.contains("authoritative inventory reloaded"))
    }));
    draw(&root, cx, 960., 720.);
}

#[gpui::test]
fn configuration_buttons_retain_conflicted_draft_and_block_same_command_replay(cx: &mut gpui::TestAppContext) {
    let fake = Arc::new(FakeConsumerBackend::default());
    fake.queue_inventory(Ok(inventory(vec![group_observation()])));
    let services = AppServices::default().with_consumer_backend(fake.clone());
    let (root, cx, consumers) = mount(cx, services);
    cx.run_until_parked();
    fake.queue_configuration(Ok(configuration(7, 15)));
    cx.update(|window, app| {
        consumers.update(app, |view, cx| {
            view.open_route(group().as_str(), ConsumerTab::Configuration, window, cx)
        });
    });
    cx.run_until_parked();
    draw(&root, cx, 1440., 960.);
    let detail = cx.read(|app| consumers.read(app).detail_for_test().expect("detail"));
    click_debug(cx, "draft-retry-max-0");
    draw(&root, cx, 1440., 960.);
    fake.queue_patch(Ok(ConsumerConfigPatchOutcome::GenerationConflict {
        expected_generation: 7,
        actual_generation: 8,
    }));
    click_debug(cx, "apply-consumer-config-draft");
    assert!(cx.read(|app| {
        detail
            .read(app)
            .mutation_status_for_test()
            .is_some_and(|status| status.contains("Draft retained"))
    }));

    draw(&root, cx, 1440., 960.);
    click_debug(cx, "apply-consumer-config-draft");
    assert_eq!(fake.calls().patch.len(), 1);
}

#[gpui::test]
fn offset_action_reuses_d4_exact_coordinator_and_never_replays_partial(cx: &mut gpui::TestAppContext) {
    let consumer = Arc::new(FakeConsumerBackend::default());
    let topic = Arc::new(FakeTopicBackend::default());
    consumer.queue_inventory(Ok(inventory(vec![group_observation()])));
    let services = AppServices::default()
        .with_consumer_backend(consumer.clone())
        .with_topic_backend(topic.clone());
    let (root, cx, consumers) = mount(cx, services);
    cx.run_until_parked();
    consumer.queue_progress(Ok(progress(-3)));
    cx.update(|window, app| {
        consumers.update(app, |view, cx| {
            view.open_route(group().as_str(), ConsumerTab::OffsetActions, window, cx)
        });
    });
    cx.run_until_parked();
    draw(&root, cx, 1440., 960.);
    let detail = cx.read(|app| consumers.read(app).detail_for_test().expect("detail"));
    topic.queue_reset(Ok(topic_outcome(TopicMutationKind::ResetOffset)));
    topic.queue_consumers(Ok(topic_consumers()));
    consumer.queue_progress(Ok(progress(-7)));
    click_debug(cx, "consumer-reset-offset-0");
    draw(&root, cx, 1440., 960.);
    let reset_form = cx.read(|app| detail.read(app).offset_dialog_for_test().expect("Reset dialog"));
    cx.update(|window, app| {
        reset_form.update(app, |form, cx| form.set_reset_timestamp(1_700_000_000_000, window, cx));
    });
    draw(&root, cx, 1440., 960.);
    click_debug(cx, "consumer-offset-dialog-confirm");

    let topic_calls = topic.calls();
    assert_eq!(topic_calls.reset.len(), 1);
    assert_eq!(topic_calls.reset[0].topic.as_str(), "orders");
    assert_eq!(topic_calls.reset[0].consumer_group, group().as_str());
    assert_eq!(topic_calls.reset[0].cluster_name, "cluster-a");
    assert_eq!(topic_calls.reset[0].timestamp, Some(1_700_000_000_000));
    assert!(!topic_calls.reset[0].force);
    assert_eq!(consumer.calls().progress.len(), 2);
    assert_eq!(
        detail.read_with(cx, |detail, _| detail
            .store
            .offset_actions
            .state
            .value()
            .expect("progress")
            .value()
            .expect("value")
            .total_delta),
        -7
    );

    draw(&root, cx, 1440., 960.);
    click_debug(cx, "consumer-offset-dialog-confirm");
    assert_eq!(topic.calls().reset.len(), 1);
    click_debug(cx, "consumer-offset-dialog-close");
    draw(&root, cx, 1440., 960.);
    assert!(cx.read(|app| detail.read(app).offset_dialog_for_test().is_none()));
    assert_eq!(
        cx.read(|app| detail.read(app).offset_blockers_for_test()),
        (false, false, false)
    );
}

#[gpui::test]
fn skip_button_uses_independent_official_dialog_and_has_no_timestamp(cx: &mut gpui::TestAppContext) {
    let consumer = Arc::new(FakeConsumerBackend::default());
    let topic = Arc::new(FakeTopicBackend::default());
    consumer.queue_inventory(Ok(inventory(vec![group_observation()])));
    let services = AppServices::default()
        .with_consumer_backend(consumer.clone())
        .with_topic_backend(topic.clone());
    let (root, cx, consumers) = mount(cx, services);
    cx.run_until_parked();
    consumer.queue_progress(Ok(progress(-3)));
    cx.update(|window, app| {
        consumers.update(app, |view, cx| {
            view.open_route(group().as_str(), ConsumerTab::OffsetActions, window, cx)
        });
    });
    cx.run_until_parked();
    draw(&root, cx, 1440., 960.);
    topic.queue_skip(Ok(topic_outcome(TopicMutationKind::SkipBacklog)));
    topic.queue_consumers(Ok(topic_consumers()));
    consumer.queue_progress(Ok(progress(-9)));
    click_debug(cx, "consumer-skip-offset-0");
    draw(&root, cx, 1440., 960.);
    click_debug(cx, "consumer-offset-dialog-confirm");
    let calls = topic.calls();
    assert_eq!(calls.skip.len(), 1);
    assert_eq!(calls.skip[0].topic.as_str(), "orders");
    assert_eq!(calls.skip[0].consumer_group, group().as_str());
    assert_eq!(calls.skip[0].cluster_name, "cluster-a");
    assert_eq!(calls.skip[0].timestamp, None);
}

#[gpui::test]
fn partial_offset_reload_keeps_previous_authoritative_progress_and_blocks_replay(cx: &mut gpui::TestAppContext) {
    let consumer = Arc::new(FakeConsumerBackend::default());
    let topic = Arc::new(FakeTopicBackend::default());
    consumer.queue_inventory(Ok(inventory(vec![group_observation()])));
    let services = AppServices::default()
        .with_consumer_backend(consumer.clone())
        .with_topic_backend(topic.clone());
    let (root, cx, consumers) = mount(cx, services);
    cx.run_until_parked();
    consumer.queue_progress(Ok(progress(-3)));
    cx.update(|window, app| {
        consumers.update(app, |view, cx| {
            view.open_route(group().as_str(), ConsumerTab::OffsetActions, window, cx)
        });
    });
    cx.run_until_parked();
    draw(&root, cx, 1440., 960.);
    let detail = cx.read(|app| consumers.read(app).detail_for_test().expect("detail"));
    topic.queue_reset(Ok(topic_outcome(TopicMutationKind::ResetOffset)));
    topic.queue_consumers(Ok(topic_consumers()));
    consumer.queue_progress(Ok(partial_progress(-99)));
    click_debug(cx, "consumer-reset-offset-0");
    draw(&root, cx, 1440., 960.);
    click_debug(cx, "consumer-offset-dialog-confirm");

    assert_eq!(
        detail.read_with(cx, |detail, _| detail
            .store
            .offset_actions
            .state
            .value()
            .expect("previous progress")
            .value()
            .expect("complete previous value")
            .total_delta),
        -3
    );
    assert!(cx.read(|app| {
        detail
            .read(app)
            .mutation_status_for_test()
            .is_some_and(|status| status.contains("previous authoritative progress remains visible"))
    }));
    draw(&root, cx, 1440., 960.);
    click_debug(cx, "consumer-offset-dialog-confirm");
    assert_eq!(topic.calls().reset.len(), 1);
}

#[gpui::test]
fn delete_button_uses_confirm_dialog_and_single_backend_attempt(cx: &mut gpui::TestAppContext) {
    let fake = Arc::new(FakeConsumerBackend::default());
    fake.queue_inventory(Ok(inventory(vec![group_observation()])));
    let services = AppServices::default().with_consumer_backend(fake.clone());
    let (root, cx, consumers) = mount(cx, services);
    cx.run_until_parked();
    fake.queue_inventory(Ok(inventory(vec![group_observation()])));
    cx.update(|window, app| {
        consumers.update(app, |view, cx| {
            view.open_route(group().as_str(), ConsumerTab::Overview, window, cx)
        });
    });
    cx.run_until_parked();
    fake.queue_delete(Ok(consumer_outcome(ConsumerMutationKind::Delete, false)));
    fake.queue_inventory(Ok(inventory(Vec::new())));
    draw(&root, cx, 1440., 960.);
    click_debug(cx, "consumer-delete");
    draw(&root, cx, 1440., 960.);
    click_debug(cx, "confirm-dialog-ok");

    let calls = fake.calls();
    assert_eq!(calls.delete.len(), 1);
    assert_eq!(calls.delete[0].1.selected_targets, [target()]);
    assert_eq!(calls.delete[0].1.authoritative_targets, [target()]);
}

#[gpui::test]
fn partial_delete_reloads_inventory_but_keeps_sheet_and_failed_target_visible(cx: &mut gpui::TestAppContext) {
    let fake = Arc::new(FakeConsumerBackend::default());
    fake.queue_inventory(Ok(inventory(vec![group_observation()])));
    let services = AppServices::default().with_consumer_backend(fake.clone());
    let (root, cx, consumers) = mount(cx, services);
    cx.run_until_parked();
    fake.queue_inventory(Ok(inventory(vec![group_observation()])));
    cx.update(|window, app| {
        consumers.update(app, |view, cx| {
            view.open_route(group().as_str(), ConsumerTab::Overview, window, cx)
        });
    });
    cx.run_until_parked();
    fake.queue_delete(Ok(consumer_outcome(ConsumerMutationKind::Delete, true)));
    fake.queue_inventory(Ok(inventory(vec![group_observation()])));
    draw(&root, cx, 1440., 960.);
    click_debug(cx, "consumer-delete");
    draw(&root, cx, 1440., 960.);
    click_debug(cx, "confirm-dialog-ok");
    draw(&root, cx, 1440., 960.);

    assert!(cx.read(|app| consumers.read(app).detail_for_test().is_some()));
    assert!(cx.debug_bounds("consumer-mutation-failed-1").is_some());
    assert!(cx.read(|app| {
        consumers.read(app).detail_for_test().is_some_and(|detail| {
            detail
                .read(app)
                .mutation_status_for_test()
                .is_some_and(|status| status.contains("1/2"))
        })
    }));
    assert_eq!(fake.calls().inventory.len(), 3);
    draw(&root, cx, 1440., 960.);
    click_debug(cx, "consumer-delete");
    assert_eq!(fake.calls().delete.len(), 1);
}

#[gpui::test]
fn five_tabs_are_lazy_retryable_and_switching_clears_jstack(cx: &mut gpui::TestAppContext) {
    let fake = Arc::new(FakeConsumerBackend::default());
    fake.queue_inventory(Ok(inventory(vec![group_observation()])));
    let services = AppServices::default().with_consumer_backend(fake.clone());
    let (root, cx, consumers) = mount(cx, services);
    cx.run_until_parked();
    fake.queue_inventory(Ok(inventory(vec![group_observation()])));
    cx.update(|window, app| {
        consumers.update(app, |view, cx| {
            view.open_route(group().as_str(), ConsumerTab::Overview, window, cx)
        });
    });
    cx.run_until_parked();
    let detail = cx.read(|app| consumers.read(app).detail_for_test().expect("detail"));

    fake.queue_clients(Err(UiError::new("clients unavailable", UiErrorCode::Connection, true)));
    cx.update(|_, app| detail.update(app, |detail, cx| detail.set_tab(ConsumerTab::Clients, cx)));
    cx.run_until_parked();
    fake.queue_clients(Ok(clients()));
    cx.update(|_, app| detail.update(app, |detail, cx| detail.retry_active(cx)));
    cx.run_until_parked();
    fake.queue_diagnostic(Ok(ConsumerDiagnosticPayload::new(
        vec![("consumeType".into(), "PUSH".into())],
        Some("bounded-jstack".into()),
        true,
    )));
    cx.update(|_, app| {
        detail.update(app, |detail, cx| {
            detail.load_diagnostic_for_test(client().identity, ConsumerDiagnosticKind::Jstack, cx)
        });
    });
    cx.run_until_parked();
    assert!(cx.read(|app| {
        detail
            .read(app)
            .store
            .diagnostic
            .state
            .value()
            .is_some_and(|payload| payload.text() == Some("bounded-jstack") && payload.truncated())
    }));

    fake.queue_progress(Ok(progress(-3)));
    cx.update(|_, app| detail.update(app, |detail, cx| detail.set_tab(ConsumerTab::Progress, cx)));
    cx.run_until_parked();
    assert!(cx.read(|app| matches!(detail.read(app).store.diagnostic.state, Loadable::Idle)));
    fake.queue_configuration(Ok(configuration(7, 15)));
    cx.update(|_, app| detail.update(app, |detail, cx| detail.set_tab(ConsumerTab::Configuration, cx)));
    cx.run_until_parked();
    fake.queue_progress(Ok(progress(-3)));
    cx.update(|_, app| detail.update(app, |detail, cx| detail.set_tab(ConsumerTab::OffsetActions, cx)));
    cx.run_until_parked();
    cx.update(|_, app| detail.update(app, |detail, cx| detail.set_tab(ConsumerTab::Overview, cx)));
    cx.run_until_parked();
    draw(&root, cx, 960., 720.);

    let calls = fake.calls();
    assert_eq!(calls.inventory.len(), 2);
    assert_eq!(calls.clients.len(), 2);
    assert_eq!(calls.progress.len(), 2);
    assert_eq!(calls.configuration.len(), 1);
    assert_eq!(calls.diagnostic.len(), 1);
}

#[gpui::test]
fn producer_apply_uses_both_draft_fields_as_one_identity(cx: &mut gpui::TestAppContext) {
    cx.update(gpui_component::init);
    let fake = Arc::new(FakeConsumerBackend::default());
    let producer_inventory = ProducerInventory {
        groups: vec![ProducerGroupObservation {
            identity: ProducerIdentity::parse("orders-producer").expect("Producer group"),
            client_count: ConsumerObservation::Unknown {
                reason: rocketmq_dashboard_common::ConsumerUnknownReason::Unavailable,
            },
        }],
        observation: ConsumerObservationState::Unknown,
        failures: Vec::new(),
        capabilities: ProducerCapabilities::for_scope(ConnectionScope::NameServer),
    };
    fake.queue_producer_inventory(Ok(producer_inventory));
    let query = ProducerConnectionQuery::try_from_draft(&ProducerConnectionQueryDraft {
        topic: "orders".into(),
        group: "orders-producer".into(),
    })
    .expect("query");
    fake.queue_producer_connections(Ok(ConsumerObservation::Complete(ProducerConnections {
        query,
        clients: vec![client()],
    })));
    let services = AppServices::default().with_consumer_backend(fake.clone());
    let capture = Rc::new(RefCell::new(None));
    let capture_view = Rc::clone(&capture);
    let (root, cx) = cx.add_window_view(move |window, cx| {
        let producers = cx.new(|cx| ProducersView::new(window, services, 7, cx));
        producers.update(cx, |producers, cx| producers.ensure_loaded(window, cx));
        capture_view.replace(Some(producers.clone()));
        let harness = cx.new(|_| ProducersHarness { producers });
        Root::new(harness, window, cx)
    });
    let producers = capture.borrow_mut().take().expect("Producers entity");
    cx.run_until_parked();
    let (topic, group_input) = cx.read(|app| producers.read(app).query_inputs_for_test());
    cx.update(|window, app| {
        group_input.update(app, |input, cx| input.set_value("orders-producer", window, cx));
    });
    draw(&root, cx, 1440., 960.);
    assert!(fake.calls().producer_connections.is_empty());
    cx.update(|window, app| {
        topic.update(app, |input, cx| input.set_value("orders", window, cx));
    });
    draw(&root, cx, 1440., 960.);
    click_debug(cx, "producer-apply-query");

    let calls = fake.calls();
    assert_eq!(calls.producer_connections.len(), 1);
    assert_eq!(calls.producer_connections[0].1.topic(), "orders");
    assert_eq!(calls.producer_connections[0].1.group().as_str(), "orders-producer");

    draw(&root, cx, 1440., 960.);
    click_debug(cx, "producer-open-client-0");
    assert!(cx.read(|app| producers.read(app).store.selected_client.is_some()));
    assert!(cx.update(|window, app| window.has_active_sheet(app)));

    let replacement_query = ProducerConnectionQuery::try_from_draft(&ProducerConnectionQueryDraft {
        topic: "orders-v2".into(),
        group: "orders-producer".into(),
    })
    .expect("replacement query");
    fake.queue_producer_connections(Ok(ConsumerObservation::Complete(ProducerConnections {
        query: replacement_query,
        clients: Vec::new(),
    })));
    cx.update(|window, app| {
        topic.update(app, |input, cx| input.set_value("orders-v2", window, cx));
    });
    draw(&root, cx, 1440., 960.);
    click_debug(cx, "producer-sheet-apply-query");
    assert!(cx.read(|app| producers.read(app).store.selected_client.is_none()));
    assert!(!cx.update(|window, app| window.has_active_sheet(app)));
    assert_eq!(fake.calls().producer_connections.len(), 2);
}
