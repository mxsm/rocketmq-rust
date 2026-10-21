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

use std::sync::Arc;
use std::sync::Barrier;
use std::time::Duration;

use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::metrics::data::AggregatedMetrics;
use opentelemetry_sdk::metrics::data::MetricData;
use opentelemetry_sdk::metrics::data::ResourceMetrics;
use opentelemetry_sdk::metrics::exporter::PushMetricExporter;
use opentelemetry_sdk::metrics::PeriodicReader;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::metrics::Temporality;
use parking_lot::Mutex;

use super::UnregistrationAgeSource;
use crate::metrics::namesrv::NameServerMetrics;
use crate::metrics::namesrv::NAMESRV_UNREGISTRATION_OLDEST_AGE;
use crate::ObservabilityConfig;
use crate::TelemetryHandle;

#[derive(Clone, Debug, Default)]
struct AgeExporter(Arc<Mutex<Vec<u64>>>);

impl PushMetricExporter for AgeExporter {
    async fn export(&self, metrics: &ResourceMetrics) -> OTelSdkResult {
        let ages = metrics
            .scope_metrics()
            .flat_map(|scope| scope.metrics())
            .filter(|metric| metric.name() == NAMESRV_UNREGISTRATION_OLDEST_AGE)
            .flat_map(|metric| match metric.data() {
                AggregatedMetrics::U64(MetricData::Gauge(gauge)) => {
                    gauge.data_points().map(|point| point.value()).collect::<Vec<_>>()
                }
                _ => Vec::new(),
            })
            .collect();
        *self.0.lock() = ages;
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

#[derive(Default)]
struct AgeSource(Mutex<Duration>);

impl UnregistrationAgeSource for AgeSource {
    fn oldest_age(&self) -> Duration {
        *self.0.lock()
    }
}

fn fixture() -> (SdkMeterProvider, TelemetryHandle, NameServerMetrics, AgeExporter) {
    let exporter = AgeExporter::default();
    let reader = PeriodicReader::builder(exporter.clone())
        .with_interval(Duration::from_secs(3600))
        .build();
    let provider = SdkMeterProvider::builder().with_reader(reader).build();
    let mut config = ObservabilityConfig {
        enabled: true,
        ..Default::default()
    };
    config.metrics.enabled = true;
    let handle = TelemetryHandle::active(&config, Some(&provider));
    let metrics = NameServerMetrics::from_handle(&handle);
    (provider, handle, metrics, exporter)
}

fn assert_age(provider: &SdkMeterProvider, exporter: &AgeExporter, expected: u64) {
    provider.force_flush().unwrap();
    assert_eq!(
        *exporter.0.lock(),
        vec![expected],
        "a real reader must export one age gauge"
    );
}

#[test]
fn collection_reads_live_age_without_any_new_queue_event() {
    let (provider, handle, metrics, exporter) = fixture();
    let source = Arc::new(AgeSource::default());
    metrics.register_unregistration_age_source(&source);
    assert_age(&provider, &exporter, 0);

    // No metric-recording operation accompanies this passage of pending time.
    *source.0.lock() = Duration::from_secs(1);
    assert_age(&provider, &exporter, 1000);

    // Rebuilding a recorder must share the callback instead of registering an
    // empty observer that overwrites the live queue's value with zero.
    let _second_recorder = NameServerMetrics::from_handle(&handle);
    assert_age(&provider, &exporter, 1000);
    *source.0.lock() = Duration::ZERO;
    assert_age(&provider, &exporter, 0);
    provider.shutdown().unwrap();
}

#[test]
fn collection_uses_the_oldest_live_queue_and_does_not_retain_sources() {
    let (provider, _handle, metrics, exporter) = fixture();
    let first = Arc::new(AgeSource(Mutex::new(Duration::from_millis(10))));
    let second = Arc::new(AgeSource(Mutex::new(Duration::from_millis(20))));
    let first_weak = Arc::downgrade(&first);
    let second_weak = Arc::downgrade(&second);
    metrics.register_unregistration_age_source(&first);
    metrics.register_unregistration_age_source(&second);
    assert_age(&provider, &exporter, 20);
    drop(second);
    assert!(second_weak.upgrade().is_none());
    assert_age(&provider, &exporter, 10);
    drop(first);
    assert!(first_weak.upgrade().is_none());
    assert_age(&provider, &exporter, 0);
    provider.shutdown().unwrap();
}

#[test]
fn explicit_age_records_still_work_without_a_live_source() {
    let (provider, _handle, metrics, exporter) = fixture();
    metrics.record_unregistration_queue_state(1, Duration::from_millis(10));
    assert_age(&provider, &exporter, 10);
    metrics.record_unregistration_oldest_age(Duration::from_millis(20));
    assert_age(&provider, &exporter, 20);
    metrics.record_unregistration_queue_state(0, Duration::ZERO);
    assert_age(&provider, &exporter, 0);
    provider.shutdown().unwrap();
}

#[test]
fn collection_and_locked_state_publication_complete_concurrently() {
    struct SignallingSource {
        age: Mutex<Duration>,
        first_sample: std::sync::atomic::AtomicBool,
        entered: Barrier,
    }

    impl UnregistrationAgeSource for SignallingSource {
        fn oldest_age(&self) -> Duration {
            if self.first_sample.swap(false, std::sync::atomic::Ordering::Relaxed) {
                self.entered.wait();
            }
            *self.age.lock()
        }
    }

    let (provider, _handle, metrics, exporter) = fixture();
    let source = Arc::new(SignallingSource {
        age: Mutex::new(Duration::ZERO),
        first_sample: std::sync::atomic::AtomicBool::new(true),
        entered: Barrier::new(2),
    });
    metrics.register_unregistration_age_source(&source);
    let mut state = source.age.lock();
    std::thread::scope(|scope| {
        let collecting = scope.spawn(|| provider.force_flush().unwrap());
        source.entered.wait();
        // Collection has entered the callback while publication owns queue
        // state. SDK recording must complete without needing the collector's
        // pipeline lock; otherwise both sides would wait for each other.
        metrics.record_unregistration_queue("queued", 1);
        *state = Duration::from_millis(999);
        drop(state);
        collecting.join().unwrap();
    });
    assert_age(&provider, &exporter, 999);
    *source.age.lock() = Duration::ZERO;
    assert_age(&provider, &exporter, 0);
    provider.shutdown().unwrap();
}
