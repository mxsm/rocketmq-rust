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

//! Instance-owned Controller metrics built from an injected telemetry capability.

use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::RwLock;

use crate::metrics::controller::ControllerMetrics;
use crate::metrics::controller_constants::*;
use crate::metrics::labels::LabelGuard;
use crate::metrics::noop_instruments::NopLongCounter;
use crate::metrics::noop_instruments::NopLongHistogram;
use crate::metrics::owner_instruments::Counter;
use crate::metrics::owner_instruments::Histogram;
use crate::metrics::owner_instruments::KeyValue;
use crate::metrics::owner_instruments::Meter;
use crate::metrics::owner_instruments::ObservableGauge;
use crate::metrics::owner_instruments::UpDownCounter;
use crate::TelemetryHandle;
use crate::TelemetryRecorder;
use crate::CONTROLLER_METER_SCOPE;
use tracing::error;

const CONTROLLER_ROLE_LEADER: i64 = 3;
const DEFAULT_CARDINALITY_LIMIT: usize = 10_000;

#[derive(Debug, Clone)]
pub struct ControllerMetricsConfig {
    pub listen_addr: String,
    pub controller_type: String,
    pub node_id: String,
    pub metrics_label: String,
    pub storage_path: String,
    pub controller_store_path: String,
    pub cardinality_limit: usize,
}

impl Default for ControllerMetricsConfig {
    fn default() -> Self {
        Self {
            listen_addr: "0.0.0.0:9878".to_owned(),
            controller_type: "controller".to_owned(),
            node_id: "0".to_owned(),
            metrics_label: String::new(),
            storage_path: String::new(),
            controller_store_path: String::new(),
            cardinality_limit: DEFAULT_CARDINALITY_LIMIT,
        }
    }
}

enum CounterInstrument {
    Real(Counter<u64>),
    Noop(NopLongCounter),
}

impl CounterInstrument {
    fn add(&self, value: u64, attributes: &[KeyValue]) {
        match self {
            Self::Real(counter) => counter.add(value, attributes),
            Self::Noop(_) => {}
        }
    }
}

enum HistogramInstrument {
    Real(Histogram<u64>),
    Noop(NopLongHistogram),
}

impl HistogramInstrument {
    fn record(&self, value: u64, attributes: &[KeyValue]) {
        match self {
            Self::Real(histogram) => histogram.record(value, attributes),
            Self::Noop(_) => {}
        }
    }
}

enum RoleInstrument {
    Real(UpDownCounter<i64>),
    Noop,
}

impl RoleInstrument {
    fn add(&self, value: i64, attributes: &[KeyValue]) {
        match self {
            Self::Real(counter) => counter.add(value, attributes),
            Self::Noop => {}
        }
    }
}

/// Metrics and label-cardinality state owned by one Controller instance.
pub struct ControllerMetricsManager {
    telemetry: TelemetryRecorder,
    role: RoleInstrument,
    request_total: CounterInstrument,
    request_latency: HistogramInstrument,
    dledger_op_total: CounterInstrument,
    dledger_op_latency: HistogramInstrument,
    election_total: CounterInstrument,
    controller_metrics: ControllerMetrics,
    base_attributes: Arc<Vec<KeyValue>>,
    label_guard: RwLock<LabelGuard>,
    _observable_gauges: Vec<ObservableGauge<u64>>,
    #[cfg(test)]
    noop: bool,
}

impl ControllerMetricsManager {
    /// Creates one metrics manager from the caller's telemetry runtime.
    ///
    /// A no-op, closing, or closed handle creates instance-owned no-op instruments.
    pub fn new<F>(
        config: ControllerMetricsConfig,
        telemetry_handle: &TelemetryHandle,
        active_broker_source: F,
    ) -> Arc<Self>
    where
        F: Fn() -> u64 + Send + Sync + 'static,
    {
        let mut label_guard = LabelGuard::new(config.cardinality_limit, true, true);
        let base_attributes = Arc::new(base_attributes(&config, &mut label_guard));
        let telemetry = telemetry_handle.child(CONTROLLER_METER_SCOPE);

        match telemetry.meter() {
            Some(meter) => Arc::new(Self::from_meter(
                telemetry,
                meter,
                config,
                Arc::new(active_broker_source),
                base_attributes,
                label_guard,
            )),
            None => Arc::new(Self::noop(telemetry, base_attributes, label_guard)),
        }
    }

    fn from_meter(
        telemetry: TelemetryRecorder,
        meter: Meter,
        config: ControllerMetricsConfig,
        active_broker_source: Arc<dyn Fn() -> u64 + Send + Sync>,
        base_attributes: Arc<Vec<KeyValue>>,
        label_guard: LabelGuard,
    ) -> Self {
        let role = meter
            .i64_up_down_counter(GAUGE_ROLE)
            .with_description("Role of current controller node (0=UNKNOWN, 1=CANDIDATE, 2=FOLLOWER, 3=LEADER)")
            .build();
        let request_total = meter
            .u64_counter(COUNTER_REQUEST_TOTAL)
            .with_description("Total number of controller requests")
            .build();
        let request_latency = meter
            .u64_histogram(HISTOGRAM_REQUEST_LATENCY)
            .with_description("Controller request latency in microseconds")
            .with_unit("us")
            .build();
        let dledger_op_total = meter
            .u64_counter(COUNTER_DLEDGER_OP_TOTAL)
            .with_description("Total number of DLedger operations")
            .build();
        let dledger_op_latency = meter
            .u64_histogram(HISTOGRAM_DLEDGER_OP_LATENCY)
            .with_description("DLedger operation latency in microseconds")
            .with_unit("us")
            .build();
        let election_total = meter
            .u64_counter(COUNTER_ELECTION_TOTAL)
            .with_description("Total number of controller elections")
            .build();
        let controller_metrics = ControllerMetrics::new(&meter);

        let disk_attributes = Arc::clone(&base_attributes);
        let dledger_disk_usage_path = controller_storage_path(&config);
        let disk_telemetry = telemetry.clone();
        let dledger_disk_usage = meter
            .u64_observable_gauge(GAUGE_DLEDGER_DISK_USAGE)
            .with_description("Disk usage of dledger storage in bytes")
            .with_unit("bytes")
            .with_callback(move |observer| {
                let Some(storage_usage) = read_observable(&disk_telemetry, || {
                    if !dledger_disk_usage_path.exists() {
                        return None;
                    }
                    Some(calculate_directory_size(&dledger_disk_usage_path))
                }) else {
                    return;
                };
                let Some(storage_usage) = storage_usage else {
                    return;
                };

                match storage_usage {
                    Ok(size) if disk_telemetry.is_active() => observer.observe(size, disk_attributes.as_ref()),
                    Ok(_) => {}
                    Err(error) => {
                        error!(
                            path = %dledger_disk_usage_path.display(),
                            %error,
                            "failed to calculate Controller storage usage"
                        );
                    }
                }
            })
            .build();

        let broker_attributes = Arc::clone(&base_attributes);
        let callback_controller_metrics = controller_metrics.clone();
        let broker_telemetry = telemetry.clone();
        let active_broker_num = meter
            .u64_observable_gauge(GAUGE_ACTIVE_BROKER_NUM)
            .with_description("Number of currently active brokers")
            .with_callback(move |observer| {
                let Some(count) = read_observable(&broker_telemetry, active_broker_source.as_ref()) else {
                    return;
                };
                if !broker_telemetry.is_active() {
                    return;
                }
                observer.observe(count, broker_attributes.as_ref());
                callback_controller_metrics.record_active_brokers(count, broker_attributes.as_ref());
            })
            .build();

        Self {
            telemetry,
            role: RoleInstrument::Real(role),
            request_total: CounterInstrument::Real(request_total),
            request_latency: HistogramInstrument::Real(request_latency),
            dledger_op_total: CounterInstrument::Real(dledger_op_total),
            dledger_op_latency: HistogramInstrument::Real(dledger_op_latency),
            election_total: CounterInstrument::Real(election_total),
            controller_metrics,
            base_attributes,
            label_guard: RwLock::new(label_guard),
            _observable_gauges: vec![dledger_disk_usage, active_broker_num],
            #[cfg(test)]
            noop: false,
        }
    }

    fn noop(telemetry: TelemetryRecorder, base_attributes: Arc<Vec<KeyValue>>, label_guard: LabelGuard) -> Self {
        Self {
            telemetry,
            role: RoleInstrument::Noop,
            request_total: CounterInstrument::Noop(NopLongCounter::new()),
            request_latency: HistogramInstrument::Noop(NopLongHistogram::new()),
            dledger_op_total: CounterInstrument::Noop(NopLongCounter::new()),
            dledger_op_latency: HistogramInstrument::Noop(NopLongHistogram::new()),
            election_total: CounterInstrument::Noop(NopLongCounter::new()),
            controller_metrics: ControllerMetrics::noop(),
            base_attributes,
            label_guard: RwLock::new(label_guard),
            _observable_gauges: Vec::new(),
            #[cfg(test)]
            noop: true,
        }
    }

    fn recording_attributes(&self) -> Option<Vec<KeyValue>> {
        self.telemetry
            .is_active()
            .then(|| self.base_attributes.as_ref().clone())
    }

    fn guarded_attribute(&self, key: &'static str, value: &str) -> KeyValue {
        let normalized = self
            .label_guard
            .write()
            .map(|mut guard| guard.normalize_metric_label(key, value).into_owned())
            .unwrap_or_else(|poisoned| poisoned.into_inner().normalize_metric_label(key, value).into_owned());
        KeyValue::new(key, normalized)
    }

    pub fn record_role_change(&self, new_role: i64, old_role: i64) {
        if !self.telemetry.is_active() {
            return;
        }

        self.role.add(new_role - old_role, self.base_attributes.as_ref());

        if is_leader_role_transition(new_role, old_role) {
            self.controller_metrics
                .record_leader_changes_total(1, self.base_attributes.as_ref());
        }
    }

    pub fn inc_request_total(&self, request_type: &str, status: RequestHandleStatus) {
        let Some(mut attributes) = self.recording_attributes() else {
            return;
        };
        attributes.push(self.guarded_attribute(LABEL_REQUEST_TYPE, request_type));
        attributes.push(self.guarded_attribute(LABEL_REQUEST_HANDLE_STATUS, status.get_lower_case_name()));
        self.request_total.add(1, &attributes);
    }

    pub fn record_request_latency(&self, request_type: &str, latency_us: u64) {
        let Some(mut attributes) = self.recording_attributes() else {
            return;
        };
        attributes.push(self.guarded_attribute(LABEL_REQUEST_TYPE, request_type));
        self.request_latency.record(latency_us, &attributes);
    }

    pub fn inc_dledger_op_total(&self, operation: DLedgerOperation, status: DLedgerOperationStatus) {
        let Some(mut attributes) = self.recording_attributes() else {
            return;
        };
        attributes.push(self.guarded_attribute(LABEL_DLEDGER_OPERATION, operation.get_lower_case_name()));
        attributes.push(self.guarded_attribute(LABEL_DLEDGER_OPERATION_STATUS, status.get_lower_case_name()));
        self.dledger_op_total.add(1, &attributes);
    }

    pub fn record_dledger_op_latency(&self, operation: DLedgerOperation, latency_us: u64) {
        let Some(mut attributes) = self.recording_attributes() else {
            return;
        };
        attributes.push(self.guarded_attribute(LABEL_DLEDGER_OPERATION, operation.get_lower_case_name()));
        self.dledger_op_latency.record(latency_us, &attributes);
    }

    pub fn inc_election_total(&self, result: ElectionResult) {
        let Some(mut attributes) = self.recording_attributes() else {
            return;
        };
        attributes.push(self.guarded_attribute(LABEL_ELECTION_RESULT, result.get_lower_case_name()));
        self.election_total.add(1, &attributes);
        self.controller_metrics.record_election_total(1, &attributes);
    }

    pub fn record_election_latency(&self, latency_ms: u64) {
        if !self.telemetry.is_active() {
            return;
        }

        self.controller_metrics
            .record_election_latency(latency_ms, self.base_attributes.as_ref());
    }

    #[cfg(test)]
    fn is_noop(&self) -> bool {
        self.noop
    }

    #[cfg(test)]
    fn dropped_metric_labels(&self) -> u64 {
        self.label_guard
            .read()
            .map(|guard| guard.dropped_labels())
            .unwrap_or_else(|poisoned| poisoned.into_inner().dropped_labels())
    }

    #[cfg(test)]
    fn base_attribute_values(&self) -> Vec<String> {
        self.base_attributes
            .iter()
            .map(|attribute| attribute.value.to_string())
            .collect()
    }

    #[cfg(test)]
    fn normalize_test_label(&self, value: &str) -> Option<KeyValue> {
        self.telemetry
            .is_active()
            .then(|| self.guarded_attribute("unsupported_test_label", value))
    }
}

fn read_observable<T>(telemetry: &TelemetryRecorder, source: impl FnOnce() -> T) -> Option<T> {
    telemetry.is_active().then(source)
}

fn base_attributes(config: &ControllerMetricsConfig, label_guard: &mut LabelGuard) -> Vec<KeyValue> {
    let mut labels = parse_key_value_list(&config.metrics_label);
    labels.insert(LABEL_ADDRESS.to_owned(), config.listen_addr.clone());
    labels.insert(LABEL_GROUP.to_owned(), config.controller_type.clone());
    labels.insert(LABEL_PEER_ID.to_owned(), config.node_id.clone());

    labels
        .into_iter()
        .map(|(key, value)| {
            let value = label_guard.normalize_metric_label(&key, &value).into_owned();
            KeyValue::new(key, value)
        })
        .collect()
}

#[inline]
fn is_leader_role_transition(new_role: i64, old_role: i64) -> bool {
    new_role != old_role && (new_role == CONTROLLER_ROLE_LEADER || old_role == CONTROLLER_ROLE_LEADER)
}

fn controller_storage_path(config: &ControllerMetricsConfig) -> PathBuf {
    if !config.storage_path.trim().is_empty() {
        return PathBuf::from(&config.storage_path);
    }
    if !config.controller_store_path.trim().is_empty() {
        return PathBuf::from(&config.controller_store_path);
    }
    PathBuf::from("./controller-store")
}

fn calculate_directory_size(path: &Path) -> std::io::Result<u64> {
    let mut total_size = 0u64;

    if path.is_file() {
        return Ok(fs::metadata(path)?.len());
    }

    if path.is_dir() {
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let metadata = entry.metadata()?;

            if metadata.is_file() {
                total_size += metadata.len();
            } else if metadata.is_dir() {
                total_size += calculate_directory_size(&entry.path())?;
            }
        }
    }

    Ok(total_size)
}

fn parse_key_value_list(value: &str) -> HashMap<String, String> {
    value
        .split(',')
        .filter_map(|entry| {
            let entry = entry.trim();
            if entry.is_empty() {
                return None;
            }

            let (key, value) = entry.split_once(':').or_else(|| entry.split_once('='))?;
            let key = key.trim();
            if key.is_empty() {
                return None;
            }

            Some((key.to_owned(), value.trim().to_owned()))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use super::*;

    fn active_metrics_guard() -> crate::TelemetryRuntimeGuard {
        let mut config = crate::ObservabilityConfig {
            enabled: true,
            ..crate::ObservabilityConfig::default()
        };
        config.metrics.enabled = true;
        config.metrics.exporter = crate::MetricsExporter::Disable;
        crate::init_observability(&config).expect("test metrics runtime should initialize")
    }

    #[test]
    fn noop_handle_creates_instance_owned_noop_instruments() {
        let manager = ControllerMetricsManager::new(ControllerMetricsConfig::default(), &TelemetryHandle::noop(), || 0);

        assert!(manager.is_noop());
        manager.record_role_change(3, 2);
        manager.inc_request_total("controller_register_broker", RequestHandleStatus::Success);
        manager.record_request_latency("controller_register_broker", 10);
        manager.inc_dledger_op_total(DLedgerOperation::Append, DLedgerOperationStatus::Success);
        manager.record_dledger_op_latency(DLedgerOperation::Append, 20);
        manager.inc_election_total(ElectionResult::NewMasterElected);
        manager.record_election_latency(30);
    }

    #[test]
    fn manager_instances_do_not_share_labels_or_cardinality_state() {
        let first = ControllerMetricsManager::new(
            ControllerMetricsConfig {
                listen_addr: "127.0.0.1:9876".to_owned(),
                controller_type: "controller-one".to_owned(),
                node_id: "1".to_owned(),
                metrics_label: "unsupported_label:first".to_owned(),
                cardinality_limit: 1,
                ..ControllerMetricsConfig::default()
            },
            &TelemetryHandle::noop(),
            || 1,
        );
        let second = ControllerMetricsManager::new(
            ControllerMetricsConfig {
                listen_addr: "127.0.0.1:9877".to_owned(),
                controller_type: "controller-two".to_owned(),
                node_id: "2".to_owned(),
                cardinality_limit: 1,
                ..ControllerMetricsConfig::default()
            },
            &TelemetryHandle::noop(),
            || 2,
        );

        assert_eq!(first.dropped_metric_labels(), 1);
        assert_eq!(second.dropped_metric_labels(), 0);
        assert!(first
            .base_attribute_values()
            .iter()
            .any(|value| value == "controller-one"));
        assert!(second
            .base_attribute_values()
            .iter()
            .any(|value| value == "controller-two"));
        assert!(!second
            .base_attribute_values()
            .iter()
            .any(|value| value == "controller-one"));
    }

    #[test]
    fn closed_runtime_rejects_records_before_label_normalization() {
        let guard = active_metrics_guard();
        let handle = guard.handle();
        let manager = ControllerMetricsManager::new(ControllerMetricsConfig::default(), &handle, || 0);

        assert!(!manager.is_noop());
        assert!(manager.normalize_test_label("while-active").is_some());
        let dropped_before_shutdown = manager.dropped_metric_labels();

        guard
            .shutdown()
            .into_result()
            .expect("test metrics runtime should shut down");

        manager.record_role_change(3, 2);
        manager.inc_request_total("controller_register_broker", RequestHandleStatus::Success);
        manager.record_request_latency("controller_register_broker", 10);
        manager.inc_dledger_op_total(DLedgerOperation::Append, DLedgerOperationStatus::Success);
        manager.record_dledger_op_latency(DLedgerOperation::Append, 20);
        manager.inc_election_total(ElectionResult::NewMasterElected);
        manager.record_election_latency(30);
        assert!(manager.normalize_test_label("after-close").is_none());
        assert_eq!(manager.dropped_metric_labels(), dropped_before_shutdown);
    }

    #[test]
    fn closed_runtime_does_not_call_observable_source() {
        let guard = active_metrics_guard();
        let handle = guard.handle();
        let telemetry = handle.child(CONTROLLER_METER_SCOPE);
        let source_calls = AtomicUsize::new(0);

        assert_eq!(
            read_observable(&telemetry, || source_calls.fetch_add(1, Ordering::Relaxed)),
            Some(0)
        );
        guard
            .shutdown()
            .into_result()
            .expect("test metrics runtime should shut down");

        assert_eq!(
            read_observable(&telemetry, || source_calls.fetch_add(1, Ordering::Relaxed)),
            None
        );
        assert_eq!(source_calls.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn controller_metrics_source_has_no_global_manager_access() {
        let source = include_str!("controller_manager.rs")
            .split("#[cfg(test)]")
            .next()
            .expect("Controller metrics production source should precede its tests");

        for forbidden in [
            concat!("static ", "INSTANCE"),
            concat!("static ", "LABEL_MAP"),
            concat!("Once", "Lock"),
            concat!("init_", "observability"),
            concat!("SdkMeter", "Provider"),
            concat!("Telemetry", "Guard"),
            concat!("_static", "("),
        ] {
            assert!(
                !source.contains(forbidden),
                "Controller metrics must remain instance-scoped: {forbidden}"
            );
        }
    }

    #[test]
    fn parses_metrics_key_value_list() {
        let values = parse_key_value_list("instance_id:controller-a,region=local, invalid, :empty");

        assert_eq!(values.get("instance_id").map(String::as_str), Some("controller-a"));
        assert_eq!(values.get("region").map(String::as_str), Some("local"));
        assert!(!values.contains_key("invalid"));
        assert!(!values.contains_key(""));
    }

    #[test]
    fn controller_storage_path_prefers_runtime_storage_path() {
        let mut config = ControllerMetricsConfig {
            controller_store_path: "controller-store-path".to_owned(),
            storage_path: "runtime-storage-path".to_owned(),
            ..ControllerMetricsConfig::default()
        };

        assert_eq!(controller_storage_path(&config), PathBuf::from("runtime-storage-path"));

        config.storage_path.clear();
        assert_eq!(controller_storage_path(&config), PathBuf::from("controller-store-path"));
    }
}
