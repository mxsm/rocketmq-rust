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
use rocketmq_common::common::message::MessageConst;
use rocketmq_observability::config::TracesConfig;
use rocketmq_observability::metrics::labels::LabelGuard;
use rocketmq_observability::sampling::SamplingGate;

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
        use opentelemetry::metrics::MeterProvider;
        use opentelemetry::KeyValue;
        use opentelemetry_sdk::metrics::SdkMeterProvider;
        use rocketmq_observability::metrics::broker::BrokerMetrics;

        let provider = SdkMeterProvider::builder().build();
        let meter = provider.meter("observability-hot-path-bench");
        let metrics = BrokerMetrics::new(&meter);
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
            CheetahString::from_static_str(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX),
            CheetahString::from_static_str("msg-123"),
        ),
        (
            CheetahString::from_static_str(MessageConst::PROPERTY_KEYS),
            CheetahString::from_static_str("key-a"),
        ),
    ])
}

fn bench_trace_property_carrier(c: &mut Criterion) {
    let mut group = c.benchmark_group("observability_trace_properties");

    #[cfg(feature = "otel-traces")]
    {
        use rocketmq_observability::propagation;

        propagation::install_trace_context_propagators();

        group.bench_function("inject_current_context", |b| {
            b.iter_batched(
                build_message_properties,
                |mut properties| {
                    propagation::inject_current_context(black_box(&mut properties));
                    black_box(properties)
                },
                BatchSize::SmallInput,
            )
        });

        let mut properties = build_message_properties();
        propagation::inject_current_context(&mut properties);
        group.bench_with_input(
            BenchmarkId::new("extract_context", "hash_map_properties"),
            &properties,
            |b, properties| b.iter(|| black_box(propagation::extract_context(black_box(properties)))),
        );
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
    let properties = build_message_properties();
    let span = tracing::Span::none();

    rocketmq_observability::trace::configure_message_span_recording(&TracesConfig::default());
    group.bench_function("record_default_body_size", |b| {
        b.iter(|| {
            rocketmq_observability::trace::record_message_properties(
                black_box(&span),
                black_box(&properties),
                black_box(Some(1024)),
            )
        })
    });

    rocketmq_observability::trace::configure_message_span_recording(&TracesConfig {
        record_message_id: true,
        record_message_keys: true,
        record_body_size: true,
        ..TracesConfig::default()
    });
    group.bench_function("record_all_message_fields", |b| {
        b.iter(|| {
            rocketmq_observability::trace::record_message_properties(
                black_box(&span),
                black_box(&properties),
                black_box(Some(1024)),
            )
        })
    });

    rocketmq_observability::trace::configure_message_span_recording(&TracesConfig::default());
    group.finish();
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
