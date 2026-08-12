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

//! Low-cardinality bounded-resource gauges used by long-running qualification.

/// Current state of one bounded queue or in-flight resource.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ResourceQueueSnapshot {
    pub items: u64,
    pub bytes: u64,
    pub oldest_age_millis: u64,
    pub capacity_items: u64,
    pub capacity_bytes: u64,
    pub active: u64,
    pub rejected_total: u64,
}

/// Current state of one bounded cache or native-memory owner.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ResourceCacheSnapshot {
    pub usage_bytes: u64,
    pub budget_bytes: u64,
}

/// Current receipt-renewal scheduler health.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReceiptRenewalSnapshot {
    pub max_due_lag_micros: u64,
    pub expired_before_renewal: u64,
}

/// Registers observable resource gauges on one fixed component meter.
#[derive(Clone)]
pub struct ResourceStabilityMetrics {
    #[cfg(feature = "otel-metrics")]
    telemetry: crate::TelemetryRecorder,
}

impl ResourceStabilityMetrics {
    #[must_use]
    pub fn from_handle(handle: &crate::TelemetryHandle, scope: &'static str) -> Self {
        #[cfg(feature = "otel-metrics")]
        {
            Self {
                telemetry: handle.child(scope),
            }
        }
        #[cfg(not(feature = "otel-metrics"))]
        {
            let _ = (handle, scope);
            Self {}
        }
    }

    pub fn register_queue<F>(&self, component: &'static str, budget: &'static str, lane: &'static str, source: F)
    where
        F: Fn() -> ResourceQueueSnapshot + Send + Sync + 'static,
    {
        #[cfg(feature = "otel-metrics")]
        if let Some(meter) = self.telemetry.meter() {
            register_queue_observers(&meter, component, budget, lane, source);
        }
        #[cfg(not(feature = "otel-metrics"))]
        let _ = (component, budget, lane, source);
    }

    pub fn register_cache<F>(&self, component: &'static str, budget: &'static str, source: F)
    where
        F: Fn() -> ResourceCacheSnapshot + Send + Sync + 'static,
    {
        #[cfg(feature = "otel-metrics")]
        if let Some(meter) = self.telemetry.meter() {
            register_cache_observers(&meter, component, budget, source);
        }
        #[cfg(not(feature = "otel-metrics"))]
        let _ = (component, budget, source);
    }

    pub fn register_receipt_renewal<F>(&self, component: &'static str, source: F)
    where
        F: Fn() -> ReceiptRenewalSnapshot + Send + Sync + 'static,
    {
        #[cfg(feature = "otel-metrics")]
        if let Some(meter) = self.telemetry.meter() {
            register_receipt_renewal_observers(&meter, component, source);
        }
        #[cfg(not(feature = "otel-metrics"))]
        let _ = (component, source);
    }
}

#[cfg(feature = "otel-metrics")]
fn resource_attributes(
    component: &'static str,
    budget: &'static str,
    lane: &'static str,
) -> [opentelemetry::KeyValue; 3] {
    [
        opentelemetry::KeyValue::new(crate::semantic::labels::COMPONENT, component),
        opentelemetry::KeyValue::new(crate::semantic::labels::BUDGET, budget),
        opentelemetry::KeyValue::new(crate::semantic::labels::LANE, lane),
    ]
}

#[cfg(feature = "otel-metrics")]
fn register_queue_observers<F>(
    meter: &opentelemetry::metrics::Meter,
    component: &'static str,
    budget: &'static str,
    lane: &'static str,
    source: F,
) where
    F: Fn() -> ResourceQueueSnapshot + Send + Sync + 'static,
{
    use std::sync::Arc;

    let source = Arc::new(source);
    macro_rules! gauge {
        ($name:expr, $description:expr, $unit:expr, $field:ident) => {{
            let source = Arc::clone(&source);
            let _instrument = meter
                .u64_observable_gauge($name)
                .with_description($description)
                .with_unit($unit)
                .with_callback(move |observer| {
                    observer.observe(source().$field, &resource_attributes(component, budget, lane));
                })
                .build();
        }};
    }
    gauge!(
        crate::semantic::metrics::RESOURCE_QUEUE_ITEMS,
        "Items retained by a bounded queue or in-flight budget",
        "{item}",
        items
    );
    gauge!(
        crate::semantic::metrics::RESOURCE_QUEUE_BYTES,
        "Bytes retained by a bounded queue or in-flight budget",
        "By",
        bytes
    );
    gauge!(
        crate::semantic::metrics::RESOURCE_QUEUE_OLDEST_AGE_MILLIS,
        "Age of the oldest queued item",
        "ms",
        oldest_age_millis
    );
    gauge!(
        crate::semantic::metrics::RESOURCE_QUEUE_CAPACITY_ITEMS,
        "Configured bounded item capacity",
        "{item}",
        capacity_items
    );
    gauge!(
        crate::semantic::metrics::RESOURCE_QUEUE_CAPACITY_BYTES,
        "Configured bounded byte capacity",
        "By",
        capacity_bytes
    );
    gauge!(
        crate::semantic::metrics::RESOURCE_QUEUE_ACTIVE,
        "Active operations using the bounded resource",
        "{operation}",
        active
    );
    let rejected_source = Arc::clone(&source);
    let _rejected = meter
        .u64_observable_counter(crate::semantic::metrics::RESOURCE_QUEUE_REJECTED_TOTAL)
        .with_description("Cumulative admissions rejected by the bounded resource")
        .with_unit("{rejection}")
        .with_callback(move |observer| {
            observer.observe(
                rejected_source().rejected_total,
                &resource_attributes(component, budget, lane),
            );
        })
        .build();
}

#[cfg(feature = "otel-metrics")]
pub(crate) fn register_cache_observers<F>(
    meter: &opentelemetry::metrics::Meter,
    component: &'static str,
    budget: &'static str,
    source: F,
) where
    F: Fn() -> ResourceCacheSnapshot + Send + Sync + 'static,
{
    use std::sync::Arc;

    let source = Arc::new(source);
    let usage_source = Arc::clone(&source);
    let _usage = meter
        .u64_observable_gauge(crate::semantic::metrics::RESOURCE_CACHE_USAGE_BYTES)
        .with_description("Current native or heap cache usage")
        .with_unit("By")
        .with_callback(move |observer| {
            observer.observe(
                usage_source().usage_bytes,
                &resource_attributes(component, budget, "cache"),
            );
        })
        .build();
    let _budget = meter
        .u64_observable_gauge(crate::semantic::metrics::RESOURCE_CACHE_BUDGET_BYTES)
        .with_description("Configured cache or native-memory budget")
        .with_unit("By")
        .with_callback(move |observer| {
            observer.observe(source().budget_bytes, &resource_attributes(component, budget, "cache"));
        })
        .build();
}

#[cfg(feature = "otel-metrics")]
fn register_receipt_renewal_observers<F>(meter: &opentelemetry::metrics::Meter, component: &'static str, source: F)
where
    F: Fn() -> ReceiptRenewalSnapshot + Send + Sync + 'static,
{
    use std::sync::Arc;

    let source = Arc::new(source);
    let lag_source = Arc::clone(&source);
    let attributes = [opentelemetry::KeyValue::new(
        crate::semantic::labels::COMPONENT,
        component,
    )];
    let lag_attributes = attributes.clone();
    let _lag = meter
        .u64_observable_gauge(crate::semantic::metrics::RECEIPT_RENEWAL_DUE_LAG_MICROS)
        .with_description("Largest receipt-renewal deadline lateness")
        .with_unit("us")
        .with_callback(move |observer| observer.observe(lag_source().max_due_lag_micros, &lag_attributes))
        .build();
    let _expired = meter
        .u64_observable_counter(crate::semantic::metrics::RECEIPT_RENEWAL_EXPIRED_TOTAL)
        .with_description("Receipt handles that expired before renewal")
        .with_unit("{receipt}")
        .with_callback(move |observer| observer.observe(source().expired_before_renewal, &attributes))
        .build();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn noop_registration_accepts_bounded_snapshots() {
        let metrics = ResourceStabilityMetrics::from_handle(&crate::TelemetryHandle::noop(), crate::PROXY_METER_SCOPE);
        metrics.register_queue("proxy", "commands", "control", || ResourceQueueSnapshot {
            items: 1,
            capacity_items: 8,
            ..ResourceQueueSnapshot::default()
        });
        metrics.register_cache("store", "rocksdb", ResourceCacheSnapshot::default);
        metrics.register_receipt_renewal("proxy", ReceiptRenewalSnapshot::default);
    }
}
