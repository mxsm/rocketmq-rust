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

use crate::metrics::namesrv::NameServerAdmissionOutcome;
use crate::metrics::namesrv::NameServerMetrics;
use crate::metrics::namesrv::NameServerRouteCacheOutcome;
use crate::metrics::namesrv::NameServerWorkloadClass;
use crate::metrics::namesrv::NAMESRV_ROUTE_CACHE_EVENTS_TOTAL;
use crate::metrics::namesrv::NAMESRV_ROUTE_FRESHNESS_SAMPLED_TOTAL;
use crate::metrics::namesrv::NAMESRV_ROUTE_REQUEST_TOTAL;
use crate::metrics::namesrv::NAMESRV_ROUTE_RESPONSE_WRITE_ERRORS_TOTAL;
use crate::metrics::namesrv::NAMESRV_WORKLOAD_ADMISSION_EVENTS_TOTAL;
use crate::metrics::proxy::ProxyMetrics;
use crate::metrics::proxy::ProxyUpAttributes;
use crate::metrics::proxy::PROXY_ACTIVE_CONNECTIONS;
use crate::metrics::proxy::PROXY_UP;
use crate::metrics::store::StoreMetrics;
use crate::metrics::store::STORE_TRANSFER_BATCH_TOTAL;
use crate::metrics::tiered_store::TieredStoreOtelMetrics;
use crate::metrics::tiered_store::TIERED_STORE_MESSAGES_DISPATCH_TOTAL;
use crate::metrics::tiered_store::TIERED_STORE_PROVIDER_UPLOAD_BYTES;
use opentelemetry::metrics::MeterProvider;
use opentelemetry::Value;
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::metrics::data::AggregatedMetrics;
use opentelemetry_sdk::metrics::data::MetricData;
use opentelemetry_sdk::metrics::data::ResourceMetrics;
use opentelemetry_sdk::metrics::exporter::PushMetricExporter;
use opentelemetry_sdk::metrics::PeriodicReader;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::metrics::Temporality;

#[derive(Clone, Debug, Default)]
struct CapturingExporter {
    points: Arc<Mutex<Vec<CapturedPoint>>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct CapturedPoint {
    metric: String,
    value: i128,
    attributes: BTreeMap<String, String>,
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
                                value: i128::from(point.value()),
                                attributes: string_attributes(point.attributes()),
                            });
                        }
                    }
                    AggregatedMetrics::U64(MetricData::Gauge(gauge)) => {
                        for point in gauge.data_points() {
                            captured.push(CapturedPoint {
                                metric: metric.name().to_string(),
                                value: i128::from(point.value()),
                                attributes: string_attributes(point.attributes()),
                            });
                        }
                    }
                    AggregatedMetrics::I64(MetricData::Gauge(gauge)) => {
                        for point in gauge.data_points() {
                            captured.push(CapturedPoint {
                                metric: metric.name().to_string(),
                                value: i128::from(point.value()),
                                attributes: string_attributes(point.attributes()),
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

fn string_attributes<'a>(attributes: impl Iterator<Item = &'a opentelemetry::KeyValue>) -> BTreeMap<String, String> {
    attributes
        .map(|attribute| {
            let value = match &attribute.value {
                Value::String(value) => value.to_string(),
                other => format!("{other:?}"),
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

#[test]
fn namesrv_and_proxy_recorders_keep_instance_counts_and_labels_isolated() {
    let first_exporter = CapturingExporter::default();
    let second_exporter = CapturingExporter::default();
    let first_provider = test_provider(first_exporter.clone());
    let second_provider = test_provider(second_exporter.clone());
    let first_meter = first_provider.meter("rocketmq-instance-isolation");
    let second_meter = second_provider.meter("rocketmq-instance-isolation");

    let first_namesrv = NameServerMetrics::new(&first_meter);
    let second_namesrv = NameServerMetrics::new(&second_meter);
    first_namesrv.record_route_request_total(2, &[opentelemetry::KeyValue::new("instance", "namesrv-first")]);
    second_namesrv.record_route_request_total(7, &[opentelemetry::KeyValue::new("instance", "namesrv-second")]);

    let first_proxy = ProxyMetrics::new_with_proxy_up(
        &first_meter,
        ProxyUpAttributes::new("proxy", "ClusterA", "proxy-first", "cluster"),
    );
    let second_proxy = ProxyMetrics::new_with_proxy_up(
        &second_meter,
        ProxyUpAttributes::new("proxy", "ClusterB", "proxy-second", "local"),
    );
    first_proxy.record_active_connections(3);
    second_proxy.record_active_connections(9);

    first_provider.force_flush().expect("collect first instance metrics");
    second_provider.force_flush().expect("collect second instance metrics");

    let first = first_exporter.points();
    let second = second_exporter.points();
    assert!(first.iter().any(|point| {
        point.metric == NAMESRV_ROUTE_REQUEST_TOTAL
            && point.value == 2
            && point.attributes.get("instance").map(String::as_str) == Some("namesrv-first")
    }));
    assert!(second.iter().any(|point| {
        point.metric == NAMESRV_ROUTE_REQUEST_TOTAL
            && point.value == 7
            && point.attributes.get("instance").map(String::as_str) == Some("namesrv-second")
    }));
    assert!(first
        .iter()
        .any(|point| point.metric == PROXY_ACTIVE_CONNECTIONS && point.value == 3));
    assert!(second
        .iter()
        .any(|point| point.metric == PROXY_ACTIVE_CONNECTIONS && point.value == 9));
    assert!(first.iter().any(|point| {
        point.metric == PROXY_UP
            && point.value == 1
            && point.attributes.get("node_id").map(String::as_str) == Some("proxy-first")
    }));
    assert!(second.iter().any(|point| {
        point.metric == PROXY_UP
            && point.value == 1
            && point.attributes.get("node_id").map(String::as_str) == Some("proxy-second")
    }));
    assert!(!first.iter().any(|point| {
        point
            .attributes
            .values()
            .any(|value| matches!(value.as_str(), "namesrv-second" | "proxy-second" | "ClusterB" | "local"))
    }));
    assert!(!second.iter().any(|point| {
        point
            .attributes
            .values()
            .any(|value| matches!(value.as_str(), "namesrv-first" | "proxy-first" | "ClusterA" | "cluster"))
    }));

    first_provider.shutdown().expect("shutdown first provider");
    second_provider.shutdown().expect("shutdown second provider");
}

#[test]
fn namesrv_read_path_metrics_use_only_bounded_labels() {
    let exporter = CapturingExporter::default();
    let provider = test_provider(exporter.clone());
    let meter = provider.meter("rocketmq-namesrv-read-path-metrics");
    let metrics = NameServerMetrics::new(&meter);

    metrics.record_route_freshness_sampled();
    metrics.record_route_cache(NameServerRouteCacheOutcome::Hit, 4096);
    metrics.record_workload_admission(
        NameServerWorkloadClass::RouteRead,
        NameServerAdmissionOutcome::Rejected,
        8,
        50,
    );
    metrics.record_route_response_write(Duration::from_micros(5), Duration::from_micros(50), false);
    provider.force_flush().expect("collect NameServer read-path metrics");

    let points = exporter.points();
    assert!(points
        .iter()
        .any(|point| point.metric == NAMESRV_ROUTE_FRESHNESS_SAMPLED_TOTAL && point.value == 1));
    assert!(points.iter().any(|point| {
        point.metric == NAMESRV_ROUTE_CACHE_EVENTS_TOTAL
            && point.attributes.get("result").map(String::as_str) == Some("hit")
    }));
    assert!(points.iter().any(|point| {
        point.metric == NAMESRV_WORKLOAD_ADMISSION_EVENTS_TOTAL
            && point.attributes.get("request_type").map(String::as_str) == Some("route-read")
            && point.attributes.get("result").map(String::as_str) == Some("rejected")
    }));
    assert!(points
        .iter()
        .any(|point| point.metric == NAMESRV_ROUTE_RESPONSE_WRITE_ERRORS_TOTAL && point.value == 1));
    for point in points.iter().filter(|point| {
        matches!(
            point.metric.as_str(),
            NAMESRV_ROUTE_CACHE_EVENTS_TOTAL | NAMESRV_WORKLOAD_ADMISSION_EVENTS_TOTAL
        )
    }) {
        assert!(point
            .attributes
            .keys()
            .all(|key| matches!(key.as_str(), "result" | "request_type")));
    }
}

#[test]
fn store_and_tiered_recorders_keep_provider_and_dispatch_counts_isolated() {
    let first_exporter = CapturingExporter::default();
    let second_exporter = CapturingExporter::default();
    let first_provider = test_provider(first_exporter.clone());
    let second_provider = test_provider(second_exporter.clone());
    let first_meter = first_provider.meter("rocketmq-store-instance-isolation");
    let second_meter = second_provider.meter("rocketmq-store-instance-isolation");
    let first_attributes = [opentelemetry::KeyValue::new("instance", "store-first")];
    let second_attributes = [opentelemetry::KeyValue::new("instance", "store-second")];

    let first_store = StoreMetrics::new(&first_meter);
    let second_store = StoreMetrics::new(&second_meter);
    first_store.record_transfer_batch_total(3, &first_attributes);
    second_store.record_transfer_batch_total(11, &second_attributes);

    let first_tiered = TieredStoreOtelMetrics::new(&first_meter);
    let second_tiered = TieredStoreOtelMetrics::new(&second_meter);
    first_tiered.record_provider_upload_bytes(128, &first_attributes);
    second_tiered.record_provider_upload_bytes(512, &second_attributes);
    first_tiered.record_messages_dispatch(5, &first_attributes);
    second_tiered.record_messages_dispatch(13, &second_attributes);

    first_provider
        .force_flush()
        .expect("collect first Store instance metrics");
    second_provider
        .force_flush()
        .expect("collect second Store instance metrics");

    let first = first_exporter.points();
    let second = second_exporter.points();
    assert!(first.iter().any(|point| {
        point.metric == STORE_TRANSFER_BATCH_TOTAL
            && point.value == 3
            && point.attributes.get("instance").map(String::as_str) == Some("store-first")
    }));
    assert!(second.iter().any(|point| {
        point.metric == STORE_TRANSFER_BATCH_TOTAL
            && point.value == 11
            && point.attributes.get("instance").map(String::as_str) == Some("store-second")
    }));
    assert!(first.iter().any(|point| {
        point.metric == TIERED_STORE_PROVIDER_UPLOAD_BYTES
            && point.value == 128
            && point.attributes.get("instance").map(String::as_str) == Some("store-first")
    }));
    assert!(second.iter().any(|point| {
        point.metric == TIERED_STORE_PROVIDER_UPLOAD_BYTES
            && point.value == 512
            && point.attributes.get("instance").map(String::as_str) == Some("store-second")
    }));
    assert!(first
        .iter()
        .any(|point| point.metric == TIERED_STORE_MESSAGES_DISPATCH_TOTAL && point.value == 5));
    assert!(second
        .iter()
        .any(|point| point.metric == TIERED_STORE_MESSAGES_DISPATCH_TOTAL && point.value == 13));
    assert!(!first
        .iter()
        .any(|point| point.attributes.get("instance").map(String::as_str) == Some("store-second")));
    assert!(!second
        .iter()
        .any(|point| point.attributes.get("instance").map(String::as_str) == Some("store-first")));

    first_provider.shutdown().expect("shutdown first provider");
    second_provider.shutdown().expect("shutdown second provider");
}
