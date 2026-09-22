// Copyright 2023 The RocketMQ Rust Authors
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
use tracing::instrument::WithSubscriber;

#[derive(Clone, Default)]
struct DrainEvents(Arc<StdMutex<Vec<String>>>);

impl tracing::Subscriber for DrainEvents {
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        #[derive(Default)]
        struct Fields {
            event: String,
            outcome: String,
        }
        impl tracing::field::Visit for Fields {
            fn record_debug(&mut self, _: &tracing::field::Field, _: &dyn std::fmt::Debug) {}
            fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
                match field.name() {
                    "event" => self.event = value.to_owned(),
                    "outcome" => self.outcome = value.to_owned(),
                    _ => {}
                }
            }
        }
        let mut fields = Fields::default();
        event.record(&mut fields);
        if fields.event == rocketmq_observability::semantic::events::RUNTIME_BUSINESS_DRAIN {
            self.0.lock().unwrap().push(fields.outcome);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn initialized_broker_emits_business_drain_and_finalizes_telemetry() {
    let mut runtime = new_phase3_lifecycle_test_runtime("business-drain-event").await;
    runtime.set_telemetry_runtime_guard(rocketmq_observability::TelemetryRuntimeGuard::noop());
    let sources = rocketmq_observability::RuntimeDiagnosticsSources::default();
    runtime.set_runtime_diagnostics_sources(sources.clone());
    let events = DrainEvents::default();
    let report = runtime
        .shutdown_basic_service_until(ShutdownDeadline::after(Duration::from_secs(10)))
        .with_subscriber(events.clone())
        .await;
    assert!(report.is_healthy(), "{report:?}");
    assert!(report.observability.present);
    assert!(report.observability.healthy);
    assert!(runtime.composition.state.observability_guard.is_none());
    assert_eq!(*events.0.lock().unwrap(), vec!["drained"]);
    use rocketmq_observability::RuntimeDiagnosticsDataProvider;
    assert!(sources.snapshot().shutdown.is_some());
}

#[tokio::test]
async fn expired_broker_shutdown_emits_one_deadline_event_without_finalizing_early() {
    let context = RuntimeContext::from_current("expired-drain-event");
    let mut runtime = BrokerRuntime::new_with_service_context(
        Arc::new(BrokerConfig::default()),
        Arc::new(MessageStoreConfig::default()),
        context.service_context("broker"),
    );
    runtime.set_telemetry_runtime_guard(rocketmq_observability::TelemetryRuntimeGuard::noop());
    let events = DrainEvents::default();
    let report = runtime
        .shutdown_basic_service_until(ShutdownDeadline::at(Instant::now()))
        .with_subscriber(events.clone())
        .await;
    assert!(report.deadline.timed_out);
    assert!(!report.is_healthy());
    assert_eq!(*events.0.lock().unwrap(), vec!["deadline_exceeded"]);
    assert!(runtime.composition.state.observability_guard.is_some());
    context.shutdown_tasks(Duration::from_secs(1)).await;
}
