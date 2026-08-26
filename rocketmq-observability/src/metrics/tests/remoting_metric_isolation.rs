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

#![cfg(feature = "otel-metrics")]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use crate::metrics::remoting::RemotingMetrics;
use crate::metrics::remoting::RequestMetricsGuard;
use crate::metrics::remoting::RPC_LATENCY;
use crate::metrics::remoting::TRANSPORT_INBOUND_DECODED_PLAINTEXT_BYTES;
use crate::metrics::remoting::TRANSPORT_REQUESTS_TOTAL;
use crate::metrics::remoting::TRANSPORT_REQUEST_LATENCY;
use crate::TelemetryHandle;
use opentelemetry::metrics::Meter;
use opentelemetry::metrics::MeterProvider;
use opentelemetry::InstrumentationScope;
use opentelemetry::Value;
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::metrics::data::AggregatedMetrics;
use opentelemetry_sdk::metrics::data::MetricData;
use opentelemetry_sdk::metrics::data::ResourceMetrics;
use opentelemetry_sdk::metrics::exporter::PushMetricExporter;
use opentelemetry_sdk::metrics::PeriodicReader;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::metrics::Temporality;

#[derive(Clone, Debug, PartialEq)]
enum CapturedValue {
    Bool(bool),
    I64(i64),
    String(String),
    Other,
}

#[derive(Clone, Debug, PartialEq)]
struct CapturedPoint {
    metric: String,
    value: u64,
    attributes: BTreeMap<String, CapturedValue>,
}

#[derive(Clone, Debug, Default)]
struct CapturingExporter {
    points: Arc<Mutex<Vec<CapturedPoint>>>,
}

impl CapturingExporter {
    fn points(&self) -> Vec<CapturedPoint> {
        self.points.lock().expect("capture lock").clone()
    }
}

impl PushMetricExporter for CapturingExporter {
    async fn export(&self, metrics: &ResourceMetrics) -> OTelSdkResult {
        let mut captured = Vec::new();
        for scope in metrics.scope_metrics() {
            for metric in scope.metrics() {
                match metric.data() {
                    AggregatedMetrics::U64(MetricData::Sum(sum)) => {
                        for point in sum.data_points() {
                            captured.push(CapturedPoint {
                                metric: metric.name().to_string(),
                                value: point.value(),
                                attributes: attributes(point.attributes()),
                            });
                        }
                    }
                    AggregatedMetrics::U64(MetricData::Histogram(histogram)) => {
                        for point in histogram.data_points() {
                            captured.push(CapturedPoint {
                                metric: metric.name().to_string(),
                                value: point.count(),
                                attributes: attributes(point.attributes()),
                            });
                        }
                    }
                    _ => {}
                }
            }
        }
        *self.points.lock().expect("capture lock") = captured;
        Ok(())
    }

    fn force_flush(&self) -> OTelSdkResult {
        Ok(())
    }

    fn shutdown_with_timeout(&self, _timeout: Duration) -> OTelSdkResult {
        Ok(())
    }

    fn temporality(&self) -> Temporality {
        Temporality::Cumulative
    }
}

fn attributes<'a>(attributes: impl Iterator<Item = &'a opentelemetry::KeyValue>) -> BTreeMap<String, CapturedValue> {
    attributes
        .map(|attribute| {
            let value = match &attribute.value {
                Value::Bool(value) => CapturedValue::Bool(*value),
                Value::I64(value) => CapturedValue::I64(*value),
                Value::String(value) => CapturedValue::String(value.to_string()),
                _ => CapturedValue::Other,
            };
            (attribute.key.as_str().to_owned(), value)
        })
        .collect()
}

fn test_provider(exporter: CapturingExporter) -> SdkMeterProvider {
    let reader = PeriodicReader::builder(exporter)
        .with_interval(Duration::from_secs(3_600))
        .build();
    SdkMeterProvider::builder().with_reader(reader).build()
}

fn metric_value(points: &[CapturedPoint], metric: &str) -> u64 {
    points
        .iter()
        .filter(|point| point.metric == metric)
        .map(|point| point.value)
        .sum()
}

#[test]
fn request_guards_record_each_terminal_outcome_once_and_keep_instances_isolated() {
    let first_exporter = CapturingExporter::default();
    let second_exporter = CapturingExporter::default();
    let first_provider = test_provider(first_exporter.clone());
    let second_provider = test_provider(second_exporter.clone());
    let first = RemotingMetrics::new(&first_provider.meter("rocketmq-transport"));
    let second = RemotingMetrics::new(&second_provider.meter("rocketmq-transport"));
    first.record_inbound_decoded_plaintext_bytes(21);
    second.record_inbound_decoded_plaintext_bytes(17);

    let mut success = RequestMetricsGuard::start(first.clone(), 10, 5, false);
    success.complete_response(0);
    success.complete_cancelled();
    drop(success);

    drop(RequestMetricsGuard::start(first.clone(), 11, 7, true));

    let mut legacy_ambiguous_none = RequestMetricsGuard::start(first.clone(), 12, 9, false);
    legacy_ambiguous_none.complete_legacy_ambiguous_none();
    legacy_ambiguous_none.complete_cancelled();
    drop(legacy_ambiguous_none);

    let mut failure = RequestMetricsGuard::start(first, 13, 11, false);
    failure.complete_process_request_failed(1);
    failure.complete_response(0);
    drop(failure);

    let mut isolated = RequestMetricsGuard::start(second, 20, 17, false);
    isolated.complete_write_channel_failed(2);
    drop(isolated);

    first_provider.force_flush().expect("collect first metrics");
    second_provider.force_flush().expect("collect second metrics");

    let first_points = first_exporter.points();
    let second_points = second_exporter.points();
    assert_eq!(metric_value(&first_points, TRANSPORT_REQUESTS_TOTAL), 4);
    assert_eq!(
        metric_value(&first_points, TRANSPORT_INBOUND_DECODED_PLAINTEXT_BYTES),
        21
    );
    assert_eq!(metric_value(&first_points, TRANSPORT_REQUEST_LATENCY), 4);
    assert_eq!(metric_value(&first_points, RPC_LATENCY), 4);
    assert_eq!(metric_value(&second_points, TRANSPORT_REQUESTS_TOTAL), 1);
    assert_eq!(
        metric_value(&second_points, TRANSPORT_INBOUND_DECODED_PLAINTEXT_BYTES),
        17
    );
    assert_eq!(metric_value(&second_points, TRANSPORT_REQUEST_LATENCY), 1);
    assert_eq!(metric_value(&second_points, RPC_LATENCY), 1);

    let first_rpc = first_points
        .iter()
        .filter(|point| point.metric == RPC_LATENCY)
        .collect::<Vec<_>>();
    assert_eq!(first_rpc.len(), 4);
    assert!(first_rpc.iter().any(|point| {
        point.attributes.get("request_code") == Some(&CapturedValue::I64(10))
            && point.attributes.get("response_code") == Some(&CapturedValue::I64(0))
            && point.attributes.get("is_long_polling") == Some(&CapturedValue::Bool(false))
            && point.attributes.get("result") == Some(&CapturedValue::String("success".to_owned()))
    }));
    assert!(first_rpc.iter().any(|point| {
        point.attributes.get("request_code") == Some(&CapturedValue::I64(11))
            && point.attributes.get("response_code") == Some(&CapturedValue::I64(-1))
            && point.attributes.get("result") == Some(&CapturedValue::String("cancelled".to_owned()))
    }));
    let legacy_ambiguous_none = first_rpc
        .iter()
        .filter(|point| {
            point.attributes.get("request_code") == Some(&CapturedValue::I64(12))
                && point.attributes.get("response_code") == Some(&CapturedValue::I64(-1))
                && point.attributes.get("result") == Some(&CapturedValue::String("legacy_ambiguous_none".to_owned()))
        })
        .collect::<Vec<_>>();
    assert_eq!(legacy_ambiguous_none.len(), 1);
    assert_eq!(legacy_ambiguous_none[0].value, 1);
    assert!(first_rpc.iter().any(|point| {
        point.attributes.get("result") == Some(&CapturedValue::String("process_request_failed".to_owned()))
    }));

    first_provider.shutdown().expect("shutdown first provider");
    second_provider.shutdown().expect("shutdown second provider");
}

#[derive(Debug)]
struct PanicOnMeterRead;

impl MeterProvider for PanicOnMeterRead {
    fn meter_with_scope(&self, _scope: InstrumentationScope) -> Meter {
        panic!("no-op remoting recorder read the process-global meter provider");
    }
}

#[test]
fn noop_handle_never_reads_global_meter_provider() {
    opentelemetry::global::set_meter_provider(PanicOnMeterRead);

    let metrics = RemotingMetrics::from_handle(&TelemetryHandle::noop());
    let mut guard = RequestMetricsGuard::start(metrics.clone(), 10, 128, false);
    guard.complete_response(0);
    metrics.record_outbound_attempted_plaintext_bytes(256);
    metrics.record_outbound_accepted_plaintext_bytes(256);
    metrics.record_outbound_written_plaintext_bytes(256);
}
