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

//! Broker-free observability hot-path benchmarks.
//!
//! These benches measure local overhead for label normalization, metric record
//! calls, and message property propagation without starting a broker or
//! external collector.

use std::collections::HashMap;
use std::hint::black_box;

use cheetah_string::CheetahString;
use criterion::criterion_group;
use criterion::criterion_main;
use criterion::BatchSize;
#[cfg(feature = "otel-traces")]
use criterion::BenchmarkId;
use criterion::Criterion;
use rocketmq_observability::metrics::labels::LabelGuard;
use rocketmq_observability::SamplingGate;

#[cfg(feature = "otel-traces")]
fn trace_runtime(record_message_id: bool, record_message_keys: bool) -> rocketmq_observability::TelemetryRuntimeGuard {
    let mut config = rocketmq_observability::ObservabilityConfig {
        enabled: true,
        ..rocketmq_observability::ObservabilityConfig::default()
    };
    config.traces.enabled = true;
    config.traces.exporter = rocketmq_observability::TraceExporter::Disable;
    config.traces.propagate_context = true;
    config.traces.record_message_id = record_message_id;
    config.traces.record_message_keys = record_message_keys;
    config.traces.record_body_size = true;
    rocketmq_observability::init_observability(&config).expect("benchmark telemetry runtime should initialize")
}

fn bench_label_guard(c: &mut Criterion) {
    let mut group = c.benchmark_group("observability_label_guard");

    group.bench_function("allowed_static_label", |b| {
        let mut guard = LabelGuard::default();
        b.iter(|| {
            black_box(guard.normalize_metric_label(black_box("cluster"), black_box("DefaultCluster")));
        })
    });

    group.bench_function("bounded_topic_existing", |b| {
        let mut guard = LabelGuard::new(1024, true, true);
        let _ = guard.normalize_metric_label("topic", "BenchTopic");
        b.iter(|| {
            black_box(guard.normalize_metric_label(black_box("topic"), black_box("BenchTopic")));
        })
    });

    group.bench_function("rejected_high_cardinality_key", |b| {
        let mut guard = LabelGuard::default();
        b.iter(|| {
            black_box(guard.normalize_metric_label(black_box("message_id"), black_box("msg-123")));
        })
    });

    group.finish();
}

fn bench_broker_metrics_record(c: &mut Criterion) {
    let mut group = c.benchmark_group("observability_broker_metrics");

    #[cfg(feature = "otel-metrics")]
    {
        use opentelemetry::KeyValue;
        use rocketmq_observability::metrics::broker::BrokerMetrics;

        let mut config = rocketmq_observability::ObservabilityConfig {
            enabled: true,
            ..rocketmq_observability::ObservabilityConfig::default()
        };
        config.metrics.enabled = true;
        config.metrics.exporter = rocketmq_observability::MetricsExporter::Disable;
        let runtime =
            rocketmq_observability::init_observability(&config).expect("benchmark metrics runtime should initialize");
        let metrics =
            BrokerMetrics::from_handle(&runtime.handle()).expect("benchmark broker metrics should be available");
        let attributes = [
            KeyValue::new("cluster", "DefaultCluster"),
            KeyValue::new("node_type", "broker"),
            KeyValue::new("topic", "BenchTopic"),
        ];

        group.bench_function("messages_in_total", |b| {
            b.iter(|| metrics.record_messages_in_total(black_box(1), black_box(&attributes)))
        });

        group.bench_function("message_size", |b| {
            b.iter(|| metrics.record_message_size(black_box(1024), black_box(&attributes)))
        });
        runtime
            .shutdown()
            .into_result()
            .expect("benchmark metrics runtime should shut down");
    }

    #[cfg(not(feature = "otel-metrics"))]
    {
        group.bench_function("otel_metrics_feature_disabled", |b| b.iter(|| black_box(())));
    }

    group.finish();
}

fn bench_sampling_gate(c: &mut Criterion) {
    let mut group = c.benchmark_group("observability_sampling_gate");

    let full = SamplingGate::new(1.0);
    group.bench_function("full", |b| b.iter(|| black_box(full.should_sample())));

    let ten_percent = SamplingGate::new(0.1);
    group.bench_function("ten_percent", |b| b.iter(|| black_box(ten_percent.should_sample())));

    let disabled = SamplingGate::new(0.0);
    group.bench_function("disabled", |b| b.iter(|| black_box(disabled.should_sample())));

    group.finish();
}

fn build_message_properties() -> HashMap<CheetahString, CheetahString> {
    HashMap::from([
        (
            CheetahString::from_static_str("UNIQ_KEY"),
            CheetahString::from_static_str("msg-123"),
        ),
        (
            CheetahString::from_static_str("KEYS"),
            CheetahString::from_static_str("key-a"),
        ),
    ])
}

fn bench_trace_property_carrier(c: &mut Criterion) {
    let mut group = c.benchmark_group("observability_trace_properties");

    #[cfg(feature = "otel-traces")]
    {
        use rocketmq_observability::extract_context_with_handle;
        use rocketmq_observability::inject_current_context_with_handle;

        let runtime = trace_runtime(false, false);
        let telemetry = runtime.handle();
        group.bench_function("inject_current_context", |b| {
            b.iter_batched(
                build_message_properties,
                |mut properties| {
                    inject_current_context_with_handle(black_box(&telemetry), black_box(&mut properties));
                    black_box(properties)
                },
                BatchSize::SmallInput,
            )
        });

        let mut properties = build_message_properties();
        inject_current_context_with_handle(&telemetry, &mut properties);
        group.bench_with_input(
            BenchmarkId::new("extract_context", "hash_map_properties"),
            &properties,
            |b, properties| {
                b.iter(|| {
                    black_box(extract_context_with_handle(
                        black_box(&telemetry),
                        black_box(properties),
                    ))
                })
            },
        );
        runtime
            .shutdown()
            .into_result()
            .expect("benchmark telemetry runtime should shut down");
    }

    #[cfg(not(feature = "otel-traces"))]
    {
        group.bench_function("otel_traces_feature_disabled", |b| {
            b.iter_batched(build_message_properties, black_box, BatchSize::SmallInput)
        });
    }

    group.finish();
}

fn bench_trace_message_attributes(c: &mut Criterion) {
    let mut group = c.benchmark_group("observability_trace_message_attributes");

    #[cfg(feature = "otel-traces")]
    {
        let properties = build_message_properties();
        let span = tracing::Span::none();

        let default_runtime = trace_runtime(false, false);
        let default_telemetry = default_runtime.handle();
        group.bench_function("record_default_body_size", |b| {
            b.iter(|| {
                rocketmq_observability::trace::record_message_properties_with_handle(
                    black_box(&default_telemetry),
                    black_box(&span),
                    black_box(&properties),
                    black_box(Some(1024)),
                )
            })
        });

        let all_fields_runtime = trace_runtime(true, true);
        let all_fields_telemetry = all_fields_runtime.handle();
        group.bench_function("record_all_message_fields", |b| {
            b.iter(|| {
                rocketmq_observability::trace::record_message_properties_with_handle(
                    black_box(&all_fields_telemetry),
                    black_box(&span),
                    black_box(&properties),
                    black_box(Some(1024)),
                )
            })
        });
        group.finish();
        default_runtime
            .shutdown()
            .into_result()
            .expect("default benchmark telemetry runtime should shut down");
        all_fields_runtime
            .shutdown()
            .into_result()
            .expect("all-fields benchmark telemetry runtime should shut down");
    }

    #[cfg(not(feature = "otel-traces"))]
    {
        group.bench_function("otel_traces_feature_disabled", |b| b.iter(|| black_box(())));
        group.finish();
    }
}

criterion_group!(
    benches,
    bench_label_guard,
    bench_broker_metrics_record,
    bench_sampling_gate,
    bench_trace_property_carrier,
    bench_trace_message_attributes
);
criterion_main!(benches);
