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

//! Cloneable, lifecycle-safe telemetry access for business components.
//!
//! A [`TelemetryHandle`] never owns or exposes an OpenTelemetry SDK provider. Providers and
//! exporter workers stay under `TelemetryRuntimeGuard`; the handle contains only immutable
//! policy, pre-created instruments, and a shared lifecycle gate.

use std::fmt;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::config::ObservabilityConfig;
use crate::metrics::labels::MetricLabelPolicy;
#[cfg(feature = "otel-metrics")]
use crate::metrics::release_identity::ReleaseIdentityRegistration;
#[cfg(feature = "otel-metrics")]
use crate::metrics::release_identity::ReleaseIdentityRegistrationStatus;
#[cfg(feature = "otel-metrics")]
use crate::metrics::release_identity::ValidatedReleaseIdentity;

/// Fixed instrumentation scope for broker metrics.
pub const BROKER_METER_SCOPE: &str = "rocketmq-broker";
/// Fixed instrumentation scope for store metrics.
pub const STORE_METER_SCOPE: &str = "rocketmq-store";
/// Fixed instrumentation scope for client metrics.
pub const CLIENT_METER_SCOPE: &str = "rocketmq-client";
/// Fixed instrumentation scope for transport metrics.
pub const TRANSPORT_METER_SCOPE: &str = "rocketmq-transport";
/// Fixed instrumentation scope for controller metrics.
pub const CONTROLLER_METER_SCOPE: &str = "rocketmq-controller";
/// Fixed instrumentation scope for NameServer metrics.
pub const NAMESRV_METER_SCOPE: &str = "rocketmq-namesrv";
/// Fixed instrumentation scope for proxy metrics.
pub const PROXY_METER_SCOPE: &str = "rocketmq-proxy";
/// Fixed instrumentation scope for MCP metrics.
pub const MCP_METER_SCOPE: &str = "rocketmq-mcp";
/// Fixed instrumentation scope for runtime lifecycle and scheduler metrics.
pub const RUNTIME_METER_SCOPE: &str = "rocketmq-runtime";
/// Fixed instrumentation scope for AI SRE Control Plane metrics.
pub const SRE_CONTROL_PLANE_METER_SCOPE: &str = "rocketmq-sre-control-plane";
/// Fixed instrumentation scope for AI SRE Connector metrics.
pub const SRE_CONNECTOR_METER_SCOPE: &str = "rocketmq-sre-connector";
/// Fixed instrumentation scope for tiered-store metrics.
pub const TIERED_STORE_METER_SCOPE: &str = "rocketmq-tieredstore";

/// Current state of a telemetry handle's runtime-owned lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TelemetryState {
    /// The runtime accepts telemetry records.
    Active = 0,
    /// Shutdown has closed new telemetry admission while owned providers drain.
    Closing = 1,
    /// Provider shutdown is complete; all handle operations are no-ops.
    Closed = 2,
}

impl TelemetryState {
    fn from_raw(value: u8) -> Self {
        match value {
            value if value == Self::Active as u8 => Self::Active,
            value if value == Self::Closing as u8 => Self::Closing,
            _ => Self::Closed,
        }
    }
}

/// Immutable per-handle tracing and message-correlation policy.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TracePolicy {
    /// Whether this handle may create exportable tracing spans.
    pub enabled: bool,
    /// Whether message trace context should be injected and extracted.
    pub propagate_context: bool,
    /// Whether spans may include an opaque message identifier.
    pub record_message_id: bool,
    /// Whether spans may include message keys.
    pub record_message_keys: bool,
    /// Whether spans may include message body size.
    pub record_body_size: bool,
}

impl TracePolicy {
    fn from_config(config: &ObservabilityConfig) -> Self {
        Self {
            enabled: config.enabled && config.traces.enabled,
            propagate_context: config.traces.propagate_context,
            record_message_id: config.traces.record_message_id,
            record_message_keys: config.traces.record_message_keys,
            record_body_size: config.traces.record_body_size,
        }
    }

    const fn disabled() -> Self {
        Self {
            enabled: false,
            propagate_context: false,
            record_message_id: false,
            record_message_keys: false,
            record_body_size: false,
        }
    }
}

/// Immutable, non-sensitive metrics settings captured from the final resolved configuration.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MetricsRuntimePolicy {
    /// Whether this handle may record metrics.
    pub enabled: bool,
    /// Fraction of sampled Broker metric operations.
    pub sample_ratio: f64,
    /// Export and dependent snapshot refresh cadence in milliseconds.
    pub export_interval_millis: u64,
    /// Maximum cardinality and bounded Broker metric work.
    pub cardinality_limit: usize,
}

impl MetricsRuntimePolicy {
    fn from_config(config: &ObservabilityConfig) -> Self {
        Self {
            enabled: config.enabled && config.metrics.enabled,
            sample_ratio: config.metrics.sample_ratio,
            export_interval_millis: config.metrics.export_interval_millis,
            cardinality_limit: config.metrics.cardinality_limit,
        }
    }

    const fn disabled() -> Self {
        Self {
            enabled: false,
            sample_ratio: 0.0,
            export_interval_millis: 0,
            cardinality_limit: 0,
        }
    }
}

impl Default for MetricsRuntimePolicy {
    fn default() -> Self {
        Self::disabled()
    }
}

#[derive(Clone)]
enum HandleBackend {
    Noop,
    Active(Arc<ActiveHandle>),
}

struct ActiveHandle {
    state: AtomicU8,
    metrics_runtime_policy: MetricsRuntimePolicy,
    trace_policy: TracePolicy,
    metric_label_policy: MetricLabelPolicy,
    #[cfg(feature = "otel-metrics")]
    meters: Option<FixedMeters>,
    #[cfg(feature = "otel-metrics")]
    release_identity: parking_lot::Mutex<Option<ReleaseIdentityRegistration>>,
}

/// Failure to register a release identity on an explicit telemetry handle.
#[cfg(feature = "otel-metrics")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ReleaseIdentityRegistrationError {
    /// The handle is no-op, closing, or closed.
    #[error("telemetry handle is not active")]
    Inactive,
    /// Metrics were not initialized for this runtime.
    #[error("telemetry handle has no meter provider")]
    MeterUnavailable,
    /// The release service does not match a fixed RocketMQ instrumentation scope.
    #[error("release service does not match a fixed RocketMQ instrumentation scope")]
    UnsupportedService,
    /// This handle already registered a different release identity.
    #[error("telemetry handle already registered a different release identity")]
    ConflictingIdentity,
}

/// Cloneable telemetry capability injected into business components.
///
/// This type deliberately has no shutdown method and contains no SDK provider. Clone it freely;
/// the corresponding non-cloneable `TelemetryRuntimeGuard` remains the sole shutdown owner.
#[derive(Clone)]
pub struct TelemetryHandle {
    backend: HandleBackend,
}

impl fmt::Debug for TelemetryHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TelemetryHandle")
            .field("state", &self.state())
            .field("trace_policy", &self.trace_policy())
            .finish_non_exhaustive()
    }
}

impl Default for TelemetryHandle {
    fn default() -> Self {
        Self::noop()
    }
}

impl TelemetryHandle {
    /// Creates a handle that never reads or installs OpenTelemetry global state.
    #[must_use]
    pub const fn noop() -> Self {
        Self {
            backend: HandleBackend::Noop,
        }
    }

    #[cfg(feature = "otel-metrics")]
    pub(crate) fn active(
        config: &ObservabilityConfig,
        meter_provider: Option<&opentelemetry_sdk::metrics::SdkMeterProvider>,
    ) -> Self {
        Self {
            backend: HandleBackend::Active(Arc::new(ActiveHandle {
                state: AtomicU8::new(TelemetryState::Active as u8),
                metrics_runtime_policy: MetricsRuntimePolicy::from_config(config),
                trace_policy: TracePolicy::from_config(config),
                metric_label_policy: MetricLabelPolicy::new(
                    config.metrics.cardinality_limit,
                    config.metrics.topic_label_enabled,
                    config.metrics.consumer_group_label_enabled,
                ),
                meters: meter_provider.map(FixedMeters::new),
                release_identity: parking_lot::Mutex::new(None),
            })),
        }
    }

    #[cfg(not(feature = "otel-metrics"))]
    pub(crate) fn active(config: &ObservabilityConfig) -> Self {
        Self {
            backend: HandleBackend::Active(Arc::new(ActiveHandle {
                state: AtomicU8::new(TelemetryState::Active as u8),
                metrics_runtime_policy: MetricsRuntimePolicy::from_config(config),
                trace_policy: TracePolicy::from_config(config),
                metric_label_policy: MetricLabelPolicy::new(
                    config.metrics.cardinality_limit,
                    config.metrics.topic_label_enabled,
                    config.metrics.consumer_group_label_enabled,
                ),
            })),
        }
    }

    /// Returns the current shared lifecycle state.
    #[must_use]
    pub fn state(&self) -> TelemetryState {
        match &self.backend {
            HandleBackend::Noop => TelemetryState::Closed,
            HandleBackend::Active(inner) => TelemetryState::from_raw(inner.state.load(Ordering::Acquire)),
        }
    }

    /// Returns whether this handle currently admits telemetry operations.
    #[must_use]
    pub fn is_active(&self) -> bool {
        self.state() == TelemetryState::Active
    }

    /// Returns whether this active runtime currently enables metrics.
    #[must_use]
    pub fn metrics_enabled(&self) -> bool {
        self.metrics_runtime_policy().enabled
    }

    /// Returns final metrics runtime policy while the telemetry lifecycle is active.
    ///
    /// No-op, closing, and closed handles return a disabled policy so surviving clones cannot
    /// continue metrics work after their runtime owner begins shutdown.
    #[must_use]
    pub fn metrics_runtime_policy(&self) -> MetricsRuntimePolicy {
        match &self.backend {
            HandleBackend::Active(inner) if self.is_active() => inner.metrics_runtime_policy,
            HandleBackend::Noop | HandleBackend::Active(_) => MetricsRuntimePolicy::disabled(),
        }
    }

    /// Returns this handle's immutable trace policy while the runtime is active.
    ///
    /// A closing or closed handle returns a disabled policy so clones that outlive their runtime
    /// cannot continue producing telemetry.
    #[must_use]
    pub fn trace_policy(&self) -> TracePolicy {
        match &self.backend {
            HandleBackend::Active(inner) if self.is_active() => inner.trace_policy,
            HandleBackend::Noop | HandleBackend::Active(_) => TracePolicy::disabled(),
        }
    }

    /// Returns this telemetry instance's shared metric-label policy.
    ///
    /// Clones of this handle share one bounded-cardinality budget. Independently initialized
    /// handles receive independent policies and budgets.
    #[must_use]
    pub fn metric_label_policy(&self) -> MetricLabelPolicy {
        match &self.backend {
            HandleBackend::Active(inner) if self.is_active() => inner.metric_label_policy.clone(),
            HandleBackend::Noop | HandleBackend::Active(_) => MetricLabelPolicy::disabled(),
        }
    }

    /// Returns a recorder bound to a fixed instrumentation scope.
    #[must_use]
    pub fn child(&self, scope: &'static str) -> TelemetryRecorder {
        TelemetryRecorder {
            handle: self.clone(),
            scope,
        }
    }

    /// Returns a pre-created meter to crate-owned typed metric recorders.
    ///
    /// Raw meters never cross the observability crate boundary. Unsupported scopes and closing,
    /// closed, or no-op handles return `None`.
    #[cfg(feature = "otel-metrics")]
    #[must_use]
    pub(crate) fn meter(&self, scope: &str) -> Option<opentelemetry::metrics::Meter> {
        match &self.backend {
            HandleBackend::Active(inner) if self.is_active() => {
                inner.meters.as_ref().and_then(|meters| meters.get(scope))
            }
            HandleBackend::Noop | HandleBackend::Active(_) => None,
        }
    }

    /// Registers one instance-scoped release identity before service readiness.
    ///
    /// Registration is idempotent for the same identity. Registering a different identity on the
    /// same runtime fails closed, and no process-global meter provider is read or modified.
    ///
    /// # Errors
    ///
    /// Returns [`ReleaseIdentityRegistrationError`] when the runtime is inactive, metrics were not
    /// initialized, the service does not name a fixed scope, or a conflicting identity was
    /// registered previously.
    #[cfg(feature = "otel-metrics")]
    pub fn register_release_identity(
        &self,
        identity: ValidatedReleaseIdentity,
    ) -> Result<ReleaseIdentityRegistrationStatus, ReleaseIdentityRegistrationError> {
        let HandleBackend::Active(inner) = &self.backend else {
            return Err(ReleaseIdentityRegistrationError::Inactive);
        };

        let mut registration = inner.release_identity.lock();
        if TelemetryState::from_raw(inner.state.load(Ordering::Acquire)) != TelemetryState::Active {
            return Err(ReleaseIdentityRegistrationError::Inactive);
        }

        if let Some(existing) = registration.as_ref() {
            return if existing.identity() == &identity {
                Ok(ReleaseIdentityRegistrationStatus::AlreadyRegistered)
            } else {
                Err(ReleaseIdentityRegistrationError::ConflictingIdentity)
            };
        }

        let meters = inner
            .meters
            .as_ref()
            .ok_or(ReleaseIdentityRegistrationError::MeterUnavailable)?;
        let meter = meters
            .get(identity.service())
            .ok_or(ReleaseIdentityRegistrationError::UnsupportedService)?;
        let release_identity = ReleaseIdentityRegistration::new(identity);
        let status = release_identity.register(&meter);
        *registration = Some(release_identity);
        Ok(status)
    }

    /// Returns whether this active runtime registered a release identity.
    #[must_use]
    pub fn release_identity_registered(&self) -> bool {
        #[cfg(feature = "otel-metrics")]
        {
            let HandleBackend::Active(inner) = &self.backend else {
                return false;
            };
            self.is_active()
                && inner
                    .release_identity
                    .lock()
                    .as_ref()
                    .is_some_and(ReleaseIdentityRegistration::is_registered)
        }

        #[cfg(not(feature = "otel-metrics"))]
        {
            false
        }
    }

    pub(crate) fn begin_closing(&self) {
        if let HandleBackend::Active(inner) = &self.backend {
            #[cfg(feature = "otel-metrics")]
            let _registration = inner.release_identity.lock();
            let _ = inner.state.compare_exchange(
                TelemetryState::Active as u8,
                TelemetryState::Closing as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            );
        }
    }

    pub(crate) fn mark_closed(&self) {
        if let HandleBackend::Active(inner) = &self.backend {
            inner.state.store(TelemetryState::Closed as u8, Ordering::Release);
        }
    }
}

/// Lightweight recorder scoped to one business component.
#[derive(Debug, Clone)]
pub struct TelemetryRecorder {
    handle: TelemetryHandle,
    scope: &'static str,
}

impl TelemetryRecorder {
    /// Returns the recorder's fixed instrumentation scope.
    #[must_use]
    pub const fn scope(&self) -> &'static str {
        self.scope
    }

    /// Returns whether the parent runtime currently admits telemetry records.
    #[must_use]
    pub fn is_active(&self) -> bool {
        self.handle.is_active()
    }

    /// Returns the parent handle's current trace policy.
    #[must_use]
    pub fn trace_policy(&self) -> TracePolicy {
        self.handle.trace_policy()
    }

    /// Returns the parent handle's instance-scoped metric-label policy.
    #[must_use]
    pub fn metric_label_policy(&self) -> MetricLabelPolicy {
        self.handle.metric_label_policy()
    }

    /// Returns the recorder's pre-created meter to crate-owned typed metric recorders.
    #[cfg(feature = "otel-metrics")]
    #[must_use]
    pub(crate) fn meter(&self) -> Option<opentelemetry::metrics::Meter> {
        self.handle.meter(self.scope)
    }
}

#[cfg(feature = "otel-metrics")]
#[derive(Clone)]
struct FixedMeters {
    broker: opentelemetry::metrics::Meter,
    store: opentelemetry::metrics::Meter,
    client: opentelemetry::metrics::Meter,
    transport: opentelemetry::metrics::Meter,
    controller: opentelemetry::metrics::Meter,
    namesrv: opentelemetry::metrics::Meter,
    proxy: opentelemetry::metrics::Meter,
    mcp: opentelemetry::metrics::Meter,
    runtime: opentelemetry::metrics::Meter,
    sre_control_plane: opentelemetry::metrics::Meter,
    sre_connector: opentelemetry::metrics::Meter,
    tiered_store: opentelemetry::metrics::Meter,
}

#[cfg(feature = "otel-metrics")]
impl FixedMeters {
    fn new(provider: &opentelemetry_sdk::metrics::SdkMeterProvider) -> Self {
        use opentelemetry::metrics::MeterProvider;

        Self {
            broker: provider.meter(BROKER_METER_SCOPE),
            store: provider.meter(STORE_METER_SCOPE),
            client: provider.meter(CLIENT_METER_SCOPE),
            transport: provider.meter(TRANSPORT_METER_SCOPE),
            controller: provider.meter(CONTROLLER_METER_SCOPE),
            namesrv: provider.meter(NAMESRV_METER_SCOPE),
            proxy: provider.meter(PROXY_METER_SCOPE),
            mcp: provider.meter(MCP_METER_SCOPE),
            runtime: provider.meter(RUNTIME_METER_SCOPE),
            sre_control_plane: provider.meter(SRE_CONTROL_PLANE_METER_SCOPE),
            sre_connector: provider.meter(SRE_CONNECTOR_METER_SCOPE),
            tiered_store: provider.meter(TIERED_STORE_METER_SCOPE),
        }
    }

    fn get(&self, scope: &str) -> Option<opentelemetry::metrics::Meter> {
        match scope {
            BROKER_METER_SCOPE => Some(self.broker.clone()),
            STORE_METER_SCOPE => Some(self.store.clone()),
            CLIENT_METER_SCOPE => Some(self.client.clone()),
            TRANSPORT_METER_SCOPE => Some(self.transport.clone()),
            CONTROLLER_METER_SCOPE => Some(self.controller.clone()),
            NAMESRV_METER_SCOPE => Some(self.namesrv.clone()),
            PROXY_METER_SCOPE => Some(self.proxy.clone()),
            MCP_METER_SCOPE => Some(self.mcp.clone()),
            RUNTIME_METER_SCOPE => Some(self.runtime.clone()),
            SRE_CONTROL_PLANE_METER_SCOPE => Some(self.sre_control_plane.clone()),
            SRE_CONNECTOR_METER_SCOPE => Some(self.sre_connector.clone()),
            TIERED_STORE_METER_SCOPE => Some(self.tiered_store.clone()),
            _ => None,
        }
    }
}
