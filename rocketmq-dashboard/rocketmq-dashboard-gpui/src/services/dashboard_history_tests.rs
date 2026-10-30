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

use std::{
    future::Future,
    sync::mpsc,
    task::{Context, Waker},
    time::Duration,
};

use gpui::TestAppContext;
use rocketmq_dashboard_common::{HistoryRetention, OperationActor, OperationResource};
use rocketmq_runtime::{ProcessMemoryLimit, RuntimeConfig, RuntimeOwner};

use super::*;
use crate::infrastructure::{config_store::DesktopConfigStore, environment_store::EnvironmentChange};

struct Fixture {
    _directory: tempfile::TempDir,
    runtime: RuntimeOwner,
    context: ChildServiceContext,
    configuration: Arc<DesktopConfigStore>,
    history: Arc<HistoryStore>,
    service: Arc<DashboardService>,
    revision: u64,
    completions: mpsc::Receiver<&'static str>,
}

impl Fixture {
    fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let runtime = RuntimeOwner::plan(RuntimeConfig::for_parallelism("history-query-test", 1))
            .unwrap()
            .with_memory_limit(ProcessMemoryLimit::configured(256 * 1024 * 1024).unwrap())
            .build()
            .unwrap();
        let context = runtime.root_context().component("history");
        let configuration = DesktopConfigStore::new(
            directory.path().join("config.json"),
            runtime.root_context().component("configuration"),
        );
        let revision = runtime.block_on(configuration.load()).unwrap().revision;
        let history = HistoryStore::with_config_store(
            directory.path().join("history.json"),
            context.clone(),
            Arc::clone(&configuration),
        );
        let (completion, completions) = mpsc::channel();
        let service = Arc::new(DashboardService {
            provider: None,
            history: Some(Arc::clone(&history)),
            history_runtime: Some(RuntimeBridge {
                context: context.clone(),
                completion: Some(completion),
            }),
        });
        Self {
            _directory: directory,
            runtime,
            context,
            configuration,
            history,
            service,
            revision,
            completions,
        }
    }

    fn append(&self, points: Vec<HistoryPoint>) {
        let environment = self.configuration.environment_snapshot().unwrap().environment_id;
        self.runtime
            .block_on(self.history.append_environment_sample(
                environment,
                points,
                HistoryRetention {
                    max_points_per_series: 10,
                    max_series: 10,
                    max_total_points: 100,
                },
            ))
            .unwrap();
    }

    fn switch_environment(&self) -> u64 {
        let context = self
            .configuration
            .operation_context(
                self.revision,
                OperationActor::LocalDesktop,
                OperationResource::Configuration,
            )
            .unwrap();
        self.runtime
            .block_on(
                self.configuration
                    .change_environment(EnvironmentChange::Create { name: "Staging".into() }, context),
            )
            .unwrap()
            .revision
    }

    fn completed(&self) {
        assert_eq!(
            self.completions.recv_timeout(Duration::from_secs(5)).unwrap(),
            "gpui-history-query"
        );
    }
}

fn identity() -> BrokerIdentity {
    BrokerIdentity {
        cluster: "development".into(),
        broker_name: "broker-a".into(),
        broker_id: 0,
        address: "broker-a:10911".into(),
    }
}

fn point(
    metric: HistoryMetricKind,
    series: String,
    revision: u64,
    timestamp_epoch_ms: u64,
    value: f64,
) -> HistoryPoint {
    HistoryPoint {
        metric,
        series_identity: series,
        timestamp_epoch_ms,
        value,
        source_revision: revision,
    }
}

#[gpui::test]
fn history_queries_run_from_the_gpui_executor_and_keep_profile_attribution(cx: &mut TestAppContext) {
    let fixture = Fixture::new();
    assert!(tokio::runtime::Handle::try_current().is_err());
    cx.background_executor.allow_parking();
    let broker = identity();
    let topic = point(
        HistoryMetricKind::TopicMessages,
        "orders".into(),
        fixture.revision,
        10,
        3.0,
    );
    let produce = point(
        HistoryMetricKind::BrokerProduceTps,
        broker_history_series_identity(&broker),
        fixture.revision,
        10,
        5.0,
    );
    let consume = point(
        HistoryMetricKind::BrokerConsumeTps,
        broker_history_series_identity(&broker),
        fixture.revision,
        10,
        7.0,
    );
    fixture.append(vec![
        topic.clone(),
        produce.clone(),
        consume.clone(),
        point(
            HistoryMetricKind::TopicMessages,
            "orders".into(),
            fixture.revision,
            30,
            99.0,
        ),
        point(
            HistoryMetricKind::TopicMessages,
            "other".into(),
            fixture.revision,
            10,
            99.0,
        ),
    ]);
    assert_eq!(
        cx.background_executor
            .block(fixture.service.topic_history(fixture.revision, "orders".into(), 0, 20))
            .unwrap(),
        vec![topic]
    );
    for expected in [produce, consume] {
        assert_eq!(
            cx.background_executor
                .block(
                    fixture
                        .service
                        .broker_history(fixture.revision, expected.metric, broker.clone(), 0, 20,)
                )
                .unwrap(),
            vec![expected]
        );
    }
    let revision = fixture.switch_environment();
    fixture.append(vec![point(
        HistoryMetricKind::TopicMessages,
        "orders".into(),
        revision,
        10,
        11.0,
    )]);
    assert!(
        cx.background_executor
            .block(fixture.service.topic_history(fixture.revision, "orders".into(), 0, 20))
            .is_err()
    );
    assert_eq!(
        cx.background_executor
            .block(fixture.service.topic_history(revision, "orders".into(), 0, 20))
            .unwrap()[0]
            .value,
        11.0
    );
    assert!(
        cx.background_executor
            .block(
                fixture
                    .service
                    .broker_history(revision, HistoryMetricKind::BrokerProduceTps, broker, 0, 20)
            )
            .unwrap()
            .is_empty()
    );
    fixture.runtime.shutdown_runtime_blocking().unwrap();
}

#[gpui::test]
fn history_query_rejects_environment_changes_while_waiting_for_storage(cx: &mut TestAppContext) {
    let fixture = Fixture::new();
    cx.background_executor.allow_parking();
    let pause = fixture.runtime.block_on(fixture.history.pause_queries());
    let mut topic = Box::pin(fixture.service.topic_history(fixture.revision, "orders".into(), 0, 20));
    let mut broker = Box::pin(fixture.service.broker_history(
        fixture.revision,
        HistoryMetricKind::BrokerProduceTps,
        identity(),
        0,
        20,
    ));
    let mut poll_context = Context::from_waker(Waker::noop());
    assert!(topic.as_mut().poll(&mut poll_context).is_pending());
    assert!(broker.as_mut().poll(&mut poll_context).is_pending());
    fixture.switch_environment();
    drop(pause);
    for result in [
        cx.background_executor.block(topic),
        cx.background_executor.block(broker),
    ] {
        assert_eq!(
            result.unwrap_err(),
            UiError::new(
                "The connection changed while History was queried.",
                UiErrorCode::Connection,
                true
            )
        );
    }
    fixture.runtime.shutdown_runtime_blocking().unwrap();
}

#[gpui::test]
fn history_queries_cancel_on_caller_drop_and_drain_with_the_owner(cx: &mut TestAppContext) {
    let fixture = Fixture::new();
    cx.background_executor.allow_parking();
    let pause = fixture.runtime.block_on(fixture.history.pause_queries());
    let mut abandoned = Box::pin(fixture.service.topic_history(fixture.revision, "orders".into(), 0, 20));
    let mut poll_context = Context::from_waker(Waker::noop());
    assert!(abandoned.as_mut().poll(&mut poll_context).is_pending());
    drop(abandoned);
    fixture.completed();
    let mut pending = Box::pin(fixture.service.broker_history(
        fixture.revision,
        HistoryMetricKind::BrokerConsumeTps,
        identity(),
        0,
        20,
    ));
    assert!(pending.as_mut().poll(&mut poll_context).is_pending());
    let report = fixture
        .runtime
        .block_on(fixture.context.task_group().shutdown(Duration::from_secs(5)));
    assert!(report.is_healthy(), "{}", report.to_json());
    fixture.completed();
    assert_eq!(
        cx.background_executor.block(pending).unwrap_err(),
        super::super::runtime_cancelled()
    );
    assert_eq!(fixture.context.task_group().task_count(), 0);
    drop(pause);
    assert!(
        cx.background_executor
            .block(fixture.service.topic_history(fixture.revision, "orders".into(), 0, 20))
            .is_err()
    );
    fixture.runtime.shutdown_runtime_blocking().unwrap();
}

#[gpui::test]
fn count_history_is_bounded_after_environment_metric_and_utc_day_filtering(cx: &mut TestAppContext) {
    let fixture = Fixture::new();
    cx.background_executor.allow_parking();
    let start = 1_792_966_400_000;
    let environment = fixture.configuration.environment_snapshot().unwrap().environment_id;
    let mut points = (0..1_450)
        .map(|index| {
            point(
                HistoryMetricKind::TopicCount,
                HISTORY_CLUSTER_SERIES.into(),
                fixture.revision,
                start + index,
                index as f64,
            )
        })
        .collect::<Vec<_>>();
    points.push(point(
        HistoryMetricKind::TopicCount,
        HISTORY_CLUSTER_SERIES.into(),
        fixture.revision,
        start - 1,
        99.0,
    ));
    points.push(point(
        HistoryMetricKind::BrokerCount,
        HISTORY_CLUSTER_SERIES.into(),
        fixture.revision,
        start,
        99.0,
    ));
    fixture
        .runtime
        .block_on(fixture.history.append_environment_sample(
            environment,
            points,
            HistoryRetention {
                max_points_per_series: 2_000,
                max_series: 10,
                max_total_points: 3_000,
            },
        ))
        .unwrap();
    let result = cx
        .background_executor
        .block(fixture.service.count_history(
            fixture.revision,
            HistoryMetricKind::TopicCount,
            start,
            start + 86_399_999,
        ))
        .unwrap();
    assert_eq!(result.len(), 1_440);
    assert_eq!(result.first().unwrap().value, 10.0);
    assert_eq!(result.last().unwrap().value, 1_449.0);
    assert_eq!(
        cx.background_executor
            .block(fixture.service.count_history(
                fixture.revision,
                HistoryMetricKind::BrokerCount,
                start,
                start + 86_399_999,
            ))
            .unwrap()[0]
            .value,
        99.0
    );
    let revision = fixture.switch_environment();
    assert!(
        cx.background_executor
            .block(
                fixture
                    .service
                    .count_history(revision, HistoryMetricKind::TopicCount, start, start + 86_399_999,)
            )
            .unwrap()
            .is_empty()
    );
    fixture.runtime.shutdown_runtime_blocking().unwrap();
}

#[gpui::test]
fn history_topic_picker_searches_an_unsampled_topic_and_loads_its_persisted_points(cx: &mut TestAppContext) {
    use crate::{
        features::dashboard::DashboardView,
        services::{AppServices, dashboard_broker::test_support::FakeDashboardBrokerOperations},
    };
    use gpui::{AppContext as _, Modifiers, point as ui_point, px, size};
    use gpui_component::Root;
    use std::{cell::RefCell, rc::Rc};

    let fixture = Fixture::new();
    cx.update(gpui_component::init);
    let fake = Arc::new(FakeDashboardBrokerOperations::default());
    fake.use_history_service(Arc::clone(&fixture.service));
    let names = (0..205).map(|index| format!("topic-{index:03}")).collect::<Vec<_>>();
    fake.queue_topic_load(Ok(TopicCurrentLoad {
        total_topics: names.len(),
        application_topics: names.len(),
        candidates: names.iter().take(200).cloned().collect(),
        available_topics: names,
        omitted_topics: 5,
        items: vec![TopicCurrentObservation {
            topic: "topic-000".into(),
            total_messages: Observed::Observed(0),
            produce_tps: Observed::Observed(0.0),
            consume_tps: Observed::Observed(0.0),
        }],
        ..Default::default()
    }));
    let capture = Rc::new(RefCell::new(None));
    let capturing = capture.clone();
    let services = AppServices::default().with_dashboard_broker_operations(fake.clone());
    let revision = fixture.revision;
    let (root, cx) = cx.add_window_view(move |window, cx| {
        let view = cx.new(|cx| DashboardView::new(window, services, revision, cx));
        capturing.replace(Some(view.clone()));
        Root::new(view, window, cx)
    });
    let view = capture.borrow_mut().take().unwrap();
    cx.run_until_parked();
    let (_, start, end) = fake
        .calls()
        .count_history
        .into_iter()
        .find(|(metric, _, _)| *metric == HistoryMetricKind::TopicCount)
        .unwrap();
    let persisted = point(
        HistoryMetricKind::TopicMessages,
        "topic-204".into(),
        fixture.revision,
        start + 1_000,
        73.0,
    );
    fixture.append(vec![persisted.clone()]);
    cx.simulate_resize(size(px(1440.), px(1400.)));
    cx.draw(ui_point(px(0.), px(0.)), size(px(1440.), px(1400.)), |_, _| {
        root.clone()
    });
    let bounds = cx.debug_bounds("dashboard-history-topic").unwrap();
    cx.simulate_click(bounds.center(), Modifiers::default());
    cx.run_until_parked();
    cx.simulate_input("topic-204");
    cx.run_until_parked();
    cx.simulate_keystrokes("down enter");
    cx.run_until_parked();
    assert_eq!(
        fake.calls().topic_history.last(),
        Some(&("topic-204".into(), start, end))
    );
    fixture.completed();
    cx.run_until_parked();
    cx.read(|app| assert_eq!(view.read(app).topic_history_for_test().value(), Some(&vec![persisted])));
    fixture.runtime.shutdown_runtime_blocking().unwrap();
}
