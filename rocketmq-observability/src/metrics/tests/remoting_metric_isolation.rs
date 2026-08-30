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
use crate::metrics::remoting::RequestCodeClass;
use crate::metrics::remoting::RequestMetricsGuard;
use crate::metrics::remoting::RequestOutcome;
use crate::metrics::remoting::ResponseAbandonedReason;
use crate::metrics::remoting::ResponseMode;
use crate::metrics::remoting::ResponseResult;
use crate::metrics::remoting::RPC_LATENCY;
use crate::metrics::remoting::TRANSPORT_DEFERRED_INFLIGHT;
use crate::metrics::remoting::TRANSPORT_DEFERRED_RETAINED_BYTES;
use crate::metrics::remoting::TRANSPORT_DEFERRED_TERMINAL_TOTAL;
use crate::metrics::remoting::TRANSPORT_INBOUND_DECODED_PLAINTEXT_BYTES;
use crate::metrics::remoting::TRANSPORT_REQUESTS_TOTAL;
use crate::metrics::remoting::TRANSPORT_REQUEST_DURATION_SECONDS;
use crate::metrics::remoting::TRANSPORT_REQUEST_LATENCY;
use crate::metrics::remoting::TRANSPORT_RESPONSE_ABANDONED_TOTAL;
use crate::metrics::remoting::TRANSPORT_RESPONSE_DUPLICATE_TOTAL;
use crate::metrics::remoting::TRANSPORT_RESPONSE_QUEUE_WAIT_SECONDS;
use crate::metrics::remoting::TRANSPORT_RESPONSE_TOTAL;
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

#[derive(Clone, Copy, Debug, PartialEq)]
enum CapturedNumber {
    U64(u64),
    I64(i64),
    HistogramCount(u64),
}

impl PartialEq<u64> for CapturedNumber {
    fn eq(&self, other: &u64) -> bool {
        matches!(self, Self::U64(value) | Self::HistogramCount(value) if value == other)
    }
}

#[derive(Clone, Debug, PartialEq)]
struct CapturedPoint {
    metric: String,
    value: CapturedNumber,
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
                                value: CapturedNumber::U64(point.value()),
                                attributes: attributes(point.attributes()),
                            });
                        }
                    }
                    AggregatedMetrics::U64(MetricData::Histogram(histogram)) => {
                        for point in histogram.data_points() {
                            captured.push(CapturedPoint {
                                metric: metric.name().to_string(),
                                value: CapturedNumber::HistogramCount(point.count()),
                                attributes: attributes(point.attributes()),
                            });
                        }
                    }
                    AggregatedMetrics::I64(MetricData::Sum(sum)) => {
                        for point in sum.data_points() {
                            captured.push(CapturedPoint {
                                metric: metric.name().to_string(),
                                value: CapturedNumber::I64(point.value()),
                                attributes: attributes(point.attributes()),
                            });
                        }
                    }
                    AggregatedMetrics::F64(MetricData::Histogram(histogram)) => {
                        for point in histogram.data_points() {
                            captured.push(CapturedPoint {
                                metric: metric.name().to_string(),
                                value: CapturedNumber::HistogramCount(point.count()),
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
        .map(|point| match point.value {
            CapturedNumber::U64(value) | CapturedNumber::HistogramCount(value) => value,
            CapturedNumber::I64(_) => 0,
        })
        .sum()
}

fn signed_metric_value(points: &[CapturedPoint], metric: &str) -> i64 {
    points
        .iter()
        .filter(|point| point.metric == metric)
        .map(|point| match point.value {
            CapturedNumber::I64(value) => value,
            CapturedNumber::U64(_) | CapturedNumber::HistogramCount(_) => 0,
        })
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
    first.record_deferred_terminal("pull_message", "owner_deadline");
    first.record_deferred_terminal("pull_message", "owner_deadline");
    first.record_response(ResponseMode::Inline, ResponseResult::TransportWritten);
    first.record_response(ResponseMode::Deferred, ResponseResult::TransportWritten);
    first.record_response_duplicate(RequestCodeClass::PullMessage);
    first.record_response_abandoned(ResponseAbandonedReason::Abandoned);
    first.adjust_deferred(RequestCodeClass::PullMessage, 1, 512);
    first.adjust_deferred(RequestCodeClass::PullMessage, -1, -512);
    first.record_response_queue_wait(0.125);
    second.record_deferred_terminal("other", "service_stopping");

    let mut success = RequestMetricsGuard::start(first.clone(), 10, 5, false, RequestCodeClass::Other);
    success.complete(0, RequestOutcome::ReplyEmpty);
    success.complete_cancelled();
    drop(success);

    drop(RequestMetricsGuard::start(
        first.clone(),
        11,
        7,
        true,
        RequestCodeClass::Other,
    ));

    let mut deferred = RequestMetricsGuard::start(first.clone(), 11, 13, true, RequestCodeClass::PullMessage);
    deferred.record_deferred_registered();
    deferred.record_deferred_registered();
    deferred.complete(0, RequestOutcome::DeferredResumed);
    deferred.complete(1, RequestOutcome::Failed);
    drop(deferred);

    let mut failure = RequestMetricsGuard::start(first, 13, 11, false, RequestCodeClass::Other);
    failure.complete_process_request_failed(1);
    failure.complete(0, RequestOutcome::ReplyEmpty);
    drop(failure);

    let mut isolated = RequestMetricsGuard::start(second, 20, 17, false, RequestCodeClass::Other);
    isolated.complete_write_channel_failed(2);
    drop(isolated);

    first_provider.force_flush().expect("collect first metrics");
    second_provider.force_flush().expect("collect second metrics");

    let first_points = first_exporter.points();
    let second_points = second_exporter.points();
    assert_eq!(metric_value(&first_points, TRANSPORT_REQUESTS_TOTAL), 5);
    assert_eq!(
        metric_value(&first_points, TRANSPORT_INBOUND_DECODED_PLAINTEXT_BYTES),
        21
    );
    assert_eq!(metric_value(&first_points, TRANSPORT_REQUEST_LATENCY), 4);
    assert_eq!(metric_value(&first_points, TRANSPORT_REQUEST_DURATION_SECONDS), 5);
    assert_eq!(metric_value(&first_points, TRANSPORT_RESPONSE_QUEUE_WAIT_SECONDS), 1);
    assert_eq!(signed_metric_value(&first_points, TRANSPORT_DEFERRED_INFLIGHT), 0);
    assert_eq!(signed_metric_value(&first_points, TRANSPORT_DEFERRED_RETAINED_BYTES), 0);
    assert_eq!(metric_value(&first_points, RPC_LATENCY), 4);
    assert_eq!(metric_value(&first_points, TRANSPORT_RESPONSE_TOTAL), 2);
    assert_eq!(metric_value(&first_points, TRANSPORT_RESPONSE_DUPLICATE_TOTAL), 1);
    assert_eq!(metric_value(&first_points, TRANSPORT_RESPONSE_ABANDONED_TOTAL), 1);
    assert_eq!(metric_value(&second_points, TRANSPORT_REQUESTS_TOTAL), 1);
    assert_eq!(
        metric_value(&second_points, TRANSPORT_INBOUND_DECODED_PLAINTEXT_BYTES),
        17
    );
    assert_eq!(metric_value(&second_points, TRANSPORT_REQUEST_LATENCY), 1);
    assert_eq!(metric_value(&second_points, RPC_LATENCY), 1);

    for metric in [TRANSPORT_DEFERRED_INFLIGHT, TRANSPORT_DEFERRED_RETAINED_BYTES] {
        let matching = first_points
            .iter()
            .filter(|point| point.metric == metric)
            .collect::<Vec<_>>();
        assert_eq!(matching.len(), 1, "deferred metric {metric}");
        assert_eq!(matching[0].value, CapturedNumber::I64(0));
        assert_eq!(
            matching[0].attributes,
            BTreeMap::from([("code".to_owned(), CapturedValue::String("pull_message".to_owned()),)])
        );
    }
    let queue_wait = first_points
        .iter()
        .filter(|point| point.metric == TRANSPORT_RESPONSE_QUEUE_WAIT_SECONDS)
        .collect::<Vec<_>>();
    assert_eq!(queue_wait.len(), 1);
    assert_eq!(queue_wait[0].value, CapturedNumber::HistogramCount(1));
    assert!(queue_wait[0].attributes.is_empty());

    let request_outcomes = first_points
        .iter()
        .filter(|point| point.metric == TRANSPORT_REQUESTS_TOTAL)
        .collect::<Vec<_>>();
    assert!(request_outcomes
        .iter()
        .all(|point| point.attributes.contains_key("code")
            && point.attributes.contains_key("outcome")
            && point.attributes.len() == 2));
    for outcome in ["deferred_registered", "deferred_resumed"] {
        let matching = request_outcomes
            .iter()
            .filter(|point| {
                point.attributes.get("code") == Some(&CapturedValue::String("pull_message".to_owned()))
                    && point.attributes.get("outcome") == Some(&CapturedValue::String(outcome.to_owned()))
            })
            .collect::<Vec<_>>();
        assert_eq!(matching.len(), 1, "request outcome {outcome}");
        assert_eq!(matching[0].value, 1, "request outcome {outcome}");
    }
    let failed = request_outcomes
        .iter()
        .find(|point| {
            point.attributes.get("code") == Some(&CapturedValue::String("other".to_owned()))
                && point.attributes.get("outcome") == Some(&CapturedValue::String("failed".to_owned()))
        })
        .expect("dropped and explicitly failed requests should use the failed outcome");
    assert_eq!(failed.value, 2);
    let response_outcomes = first_points
        .iter()
        .filter(|point| point.metric == TRANSPORT_RESPONSE_TOTAL)
        .collect::<Vec<_>>();
    assert!(response_outcomes.iter().any(|point| {
        point.attributes.get("mode") == Some(&CapturedValue::String("deferred".to_owned()))
            && point.attributes.get("result") == Some(&CapturedValue::String("transport_written".to_owned()))
    }));

    let first_deferred = first_points
        .iter()
        .filter(|point| point.metric == TRANSPORT_DEFERRED_TERMINAL_TOTAL)
        .collect::<Vec<_>>();
    assert_eq!(first_deferred.len(), 1);
    assert_eq!(first_deferred[0].value, 2);
    assert_eq!(
        first_deferred[0].attributes,
        BTreeMap::from([
            (
                "request_code_bucket".to_owned(),
                CapturedValue::String("pull_message".to_owned()),
            ),
            ("reason".to_owned(), CapturedValue::String("owner_deadline".to_owned()),),
        ])
    );
    let second_deferred = second_points
        .iter()
        .filter(|point| point.metric == TRANSPORT_DEFERRED_TERMINAL_TOTAL)
        .collect::<Vec<_>>();
    assert_eq!(second_deferred.len(), 1);
    assert_eq!(second_deferred[0].value, 1);
    assert_eq!(
        second_deferred[0].attributes,
        BTreeMap::from([
            (
                "request_code_bucket".to_owned(),
                CapturedValue::String("other".to_owned()),
            ),
            (
                "reason".to_owned(),
                CapturedValue::String("service_stopping".to_owned()),
            ),
        ])
    );

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
            && point.attributes.get("result") == Some(&CapturedValue::String("process_request_failed".to_owned()))
    }));
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
    let mut guard = RequestMetricsGuard::start(metrics.clone(), 10, 128, false, RequestCodeClass::Other);
    guard.complete(0, RequestOutcome::ReplyEmpty);
    metrics.record_outbound_attempted_plaintext_bytes(256);
    metrics.record_outbound_accepted_plaintext_bytes(256);
    metrics.record_outbound_written_plaintext_bytes(256);
    metrics.record_deferred_terminal("other", "abandoned");
}
