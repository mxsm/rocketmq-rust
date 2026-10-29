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
    service: DashboardService,
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
        let service = DashboardService {
            provider: None,
            history: Some(Arc::clone(&history)),
            history_runtime: Some(RuntimeBridge {
                context: context.clone(),
                completion: Some(completion),
            }),
        };
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
