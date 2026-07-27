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

//! Validated, low-cardinality release identity metrics.

use std::net::SocketAddr;
#[cfg(feature = "otel-metrics")]
use std::sync::atomic::AtomicBool;
#[cfg(feature = "otel-metrics")]
use std::sync::atomic::Ordering;
#[cfg(feature = "otel-metrics")]
use std::sync::Arc;

use thiserror::Error;

const MAX_STABLE_LABEL_LEN: usize = 63;
const GIT_COMMIT_LEN: usize = 40;
const DEFAULT_LOCAL_COMMIT: &str = "0000000000000000000000000000000000000000";
const DEFAULT_LOCAL_NONCE: &str = "local";
const DEFAULT_PROMETHEUS_BIND_ADDR: &str = "127.0.0.1:5557";
const DEFAULT_PROMETHEUS_PATH: &str = "/metrics";
const MAX_PROMETHEUS_PATH_LEN: usize = 128;

/// Environment variable carrying the exact source revision.
pub const RELEASE_COMMIT_ENV: &str = "ROCKETMQ_RELEASE_COMMIT";
/// Environment variable distinguishing one rollout of a source revision.
pub const RELEASE_NONCE_ENV: &str = "ROCKETMQ_RELEASE_NONCE";
/// Environment variable enabling or disabling metrics.
pub const METRICS_ENABLED_ENV: &str = "ROCKETMQ_METRICS_ENABLED";
/// Environment variable selecting the metrics exporter.
pub const METRICS_EXPORTER_ENV: &str = "ROCKETMQ_METRICS_EXPORTER";
/// Environment variable selecting the Prometheus listener address.
pub const METRICS_BIND_ADDR_ENV: &str = "ROCKETMQ_METRICS_BIND_ADDR";
/// Environment variable selecting the Prometheus scrape path.
pub const METRICS_PATH_ENV: &str = "ROCKETMQ_METRICS_PATH";

/// A validated release identity suitable for metric labels.
///
/// All three values are deliberately bounded to keep release metadata stable
/// and low-cardinality. The nonce identifies one rollout instance and must be
/// supplied by the composition root; this type never generates process-global
/// state.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ValidatedReleaseIdentity {
    service: String,
    commit: String,
    nonce: String,
}

impl ValidatedReleaseIdentity {
    /// Validates and constructs a release identity.
    ///
    /// `commit` must be a full, lowercase 40-character Git object id. `service`
    /// and `nonce` must contain 1 to 63 lowercase ASCII letters, digits, or
    /// interior hyphens, and must begin and end with an ASCII letter or digit.
    ///
    /// # Errors
    ///
    /// Returns [`ReleaseIdentityError`] when any field is not in its canonical,
    /// bounded form.
    pub fn try_new(
        service: impl Into<String>,
        commit: impl Into<String>,
        nonce: impl Into<String>,
    ) -> Result<Self, ReleaseIdentityError> {
        let service = service.into();
        let commit = commit.into();
        let nonce = nonce.into();

        if !is_stable_label(&service) {
            return Err(ReleaseIdentityError::InvalidService);
        }
        if !is_full_lowercase_commit(&commit) {
            return Err(ReleaseIdentityError::InvalidCommit);
        }
        if !is_stable_label(&nonce) {
            return Err(ReleaseIdentityError::InvalidNonce);
        }

        Ok(Self { service, commit, nonce })
    }

    /// Returns the stable service identifier.
    pub fn service(&self) -> &str {
        &self.service
    }

    /// Returns the full lowercase Git commit id.
    pub fn commit(&self) -> &str {
        &self.commit
    }

    /// Returns the rollout nonce.
    pub fn nonce(&self) -> &str {
        &self.nonce
    }

    #[cfg(feature = "otel-metrics")]
    fn attributes(&self) -> [opentelemetry::KeyValue; 3] {
        [
            opentelemetry::KeyValue::new(crate::semantic::labels::SERVICE, self.service.clone()),
            opentelemetry::KeyValue::new(crate::semantic::labels::RELEASE_COMMIT, self.commit.clone()),
            opentelemetry::KeyValue::new(crate::semantic::labels::RELEASE_NONCE, self.nonce.clone()),
        ]
    }
}

/// Identifies the release identity field that failed validation.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum ReleaseIdentityError {
    /// The service name is not a canonical stable label.
    #[error("service must be 1..=63 lowercase ASCII letters, digits, or interior hyphens")]
    InvalidService,
    /// The commit is not a full lowercase Git object id.
    #[error("commit must be exactly 40 lowercase hexadecimal characters")]
    InvalidCommit,
    /// The rollout nonce is not a canonical stable label.
    #[error("nonce must be 1..=63 lowercase ASCII letters, digits, or interior hyphens")]
    InvalidNonce,
}

/// Validated process-root inputs for release identity and metrics bootstrap.
///
/// Parsing is intentionally separated from environment access so service
/// composition roots can validate deterministic values in tests and fail
/// closed before advertising readiness.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProcessTelemetryConfig {
    release_identity: ValidatedReleaseIdentity,
    metrics_enabled: bool,
    metrics_exporter: crate::MetricsExporter,
    prometheus_bind_addr: SocketAddr,
    prometheus_host: String,
    prometheus_port: u16,
    prometheus_path: String,
}

impl ProcessTelemetryConfig {
    /// Parses process telemetry inputs without reading global process state.
    ///
    /// A missing runtime commit uses a real build-time
    /// `ROCKETMQ_BUILD_COMMIT`. When no real build revision was embedded, the
    /// all-zero sentinel remains available only as the local host-development
    /// default. An explicit all-zero runtime commit is rejected, and an
    /// explicit runtime commit must match a real embedded build commit. Other
    /// missing values select `local` nonce, disabled metrics, loopback binding,
    /// and `/metrics`. An explicitly enabled exporter must be non-disabled,
    /// while a disabled metrics setting must use the disabled exporter.
    ///
    /// # Errors
    ///
    /// Returns [`ProcessTelemetryConfigError`] for an invalid release identity,
    /// boolean, exporter, bind address, path, or inconsistent enabled/exporter
    /// selection.
    #[allow(
        clippy::too_many_arguments,
        reason = "mirrors the seven explicit process-root inputs"
    )]
    pub fn try_from_values(
        service: impl Into<String>,
        commit: Option<&str>,
        nonce: Option<&str>,
        metrics_enabled: Option<&str>,
        exporter: Option<&str>,
        bind_addr: Option<&str>,
        path: Option<&str>,
    ) -> Result<Self, ProcessTelemetryConfigError> {
        let commit = resolve_release_commit(commit, option_env!("ROCKETMQ_BUILD_COMMIT"))?;
        let release_identity =
            ValidatedReleaseIdentity::try_new(service, commit, nonce.unwrap_or(DEFAULT_LOCAL_NONCE))?;
        let metrics_enabled = parse_metrics_enabled(metrics_enabled)?;
        let metrics_exporter = parse_metrics_exporter(exporter)?;
        validate_metrics_selection(metrics_enabled, metrics_exporter)?;

        let bind_addr = bind_addr.unwrap_or(DEFAULT_PROMETHEUS_BIND_ADDR);
        let bind_addr = bind_addr
            .parse::<SocketAddr>()
            .map_err(|_| ProcessTelemetryConfigError::InvalidPrometheusBindAddress)?;
        if bind_addr.port() == 0 {
            return Err(ProcessTelemetryConfigError::InvalidPrometheusBindAddress);
        }

        let prometheus_path = path.unwrap_or(DEFAULT_PROMETHEUS_PATH);
        if !is_canonical_metrics_path(prometheus_path) {
            return Err(ProcessTelemetryConfigError::InvalidPrometheusPath);
        }

        Ok(Self {
            release_identity,
            metrics_enabled,
            metrics_exporter,
            prometheus_bind_addr: bind_addr,
            prometheus_host: match bind_addr.ip() {
                std::net::IpAddr::V4(address) => address.to_string(),
                std::net::IpAddr::V6(address) => format!("[{address}]"),
            },
            prometheus_port: bind_addr.port(),
            prometheus_path: prometheus_path.to_owned(),
        })
    }

    /// Reads and validates the supported process environment variables.
    ///
    /// # Errors
    ///
    /// Returns [`ProcessTelemetryConfigError`] when an environment value is not
    /// Unicode or fails the same validation as [`Self::try_from_values`].
    pub fn from_process_env(service: impl Into<String>) -> Result<Self, ProcessTelemetryConfigError> {
        let commit = read_process_env(RELEASE_COMMIT_ENV)?;
        let nonce = read_process_env(RELEASE_NONCE_ENV)?;
        let metrics_enabled = read_process_env(METRICS_ENABLED_ENV)?;
        let exporter = read_process_env(METRICS_EXPORTER_ENV)?;
        let bind_addr = read_process_env(METRICS_BIND_ADDR_ENV)?;
        let path = read_process_env(METRICS_PATH_ENV)?;

        Self::try_from_values(
            service,
            commit.as_deref(),
            nonce.as_deref(),
            metrics_enabled.as_deref(),
            exporter.as_deref(),
            bind_addr.as_deref(),
            path.as_deref(),
        )
    }

    /// Returns the validated release identity.
    pub fn release_identity(&self) -> &ValidatedReleaseIdentity {
        &self.release_identity
    }

    /// Returns whether metrics are enabled for this process.
    pub const fn metrics_enabled(&self) -> bool {
        self.metrics_enabled
    }

    /// Returns the selected metrics exporter.
    pub const fn metrics_exporter(&self) -> crate::MetricsExporter {
        self.metrics_exporter
    }

    /// Returns the Prometheus listener that will actually be bound by this process.
    pub fn prometheus_listener_addr(&self) -> Option<SocketAddr> {
        (self.metrics_enabled && self.metrics_exporter == crate::MetricsExporter::Prometheus)
            .then_some(self.prometheus_bind_addr)
    }

    /// Returns the validated Prometheus listener host.
    pub fn prometheus_host(&self) -> &str {
        &self.prometheus_host
    }

    /// Returns the validated Prometheus listener port.
    pub const fn prometheus_port(&self) -> u16 {
        self.prometheus_port
    }

    /// Returns the canonical Prometheus scrape path.
    pub fn prometheus_path(&self) -> &str {
        &self.prometheus_path
    }

    /// Applies the validated process-root values to observability configuration.
    ///
    /// Enabling metrics necessarily enables the observability runtime. Disabling
    /// metrics does not disable independently configured traces or logs.
    pub fn apply_to(&self, config: &mut crate::ObservabilityConfig) {
        config.service_name.clone_from(&self.release_identity.service);
        config.metrics.enabled = self.metrics_enabled;
        config.metrics.exporter = self.metrics_exporter;
        config.prometheus.host.clone_from(&self.prometheus_host);
        config.prometheus.port = self.prometheus_port;
        config.prometheus.path.clone_from(&self.prometheus_path);
        if self.metrics_enabled {
            config.enabled = true;
        }
    }
}

/// Failure while validating process telemetry inputs.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum ProcessTelemetryConfigError {
    /// A release identity field is invalid.
    #[error(transparent)]
    ReleaseIdentity(#[from] ReleaseIdentityError),
    /// The runtime commit is the all-zero null identity.
    #[error("ROCKETMQ_RELEASE_COMMIT must not be the all-zero null commit")]
    NullReleaseCommit,
    /// The runtime commit disagrees with the commit embedded in the binary.
    #[error("ROCKETMQ_RELEASE_COMMIT must match the embedded ROCKETMQ_BUILD_COMMIT")]
    ReleaseCommitMismatch,
    /// The metrics-enabled value is not the canonical `true` or `false`.
    #[error("ROCKETMQ_METRICS_ENABLED must be either true or false")]
    InvalidMetricsEnabled,
    /// The metrics exporter name is unsupported.
    #[error("ROCKETMQ_METRICS_EXPORTER must be disable, otlp_grpc, prometheus, or log")]
    InvalidMetricsExporter,
    /// The enabled flag and exporter selection disagree.
    #[error("enabled metrics require a non-disabled exporter and disabled metrics require the disable exporter")]
    InconsistentMetricsSelection,
    /// The Prometheus address is not a nonzero IP socket address.
    #[error("ROCKETMQ_METRICS_BIND_ADDR must be an IP address and nonzero port")]
    InvalidPrometheusBindAddress,
    /// The Prometheus path is not canonical and bounded.
    #[error("ROCKETMQ_METRICS_PATH must be a canonical absolute path of at most 128 ASCII characters")]
    InvalidPrometheusPath,
    /// An explicitly supplied environment value is not Unicode.
    #[error("{0} must contain valid Unicode")]
    NonUnicodeEnvironment(&'static str),
}

fn parse_metrics_enabled(value: Option<&str>) -> Result<bool, ProcessTelemetryConfigError> {
    match value {
        None | Some("false") => Ok(false),
        Some("true") => Ok(true),
        Some(_) => Err(ProcessTelemetryConfigError::InvalidMetricsEnabled),
    }
}

fn resolve_release_commit<'a>(
    runtime_commit: Option<&'a str>,
    build_commit: Option<&'a str>,
) -> Result<&'a str, ProcessTelemetryConfigError> {
    if let Some(runtime_commit) = runtime_commit {
        if runtime_commit == DEFAULT_LOCAL_COMMIT {
            return Err(ProcessTelemetryConfigError::NullReleaseCommit);
        }
        if !is_full_lowercase_commit(runtime_commit) {
            return Err(ReleaseIdentityError::InvalidCommit.into());
        }
    }

    let embedded_commit = build_commit.filter(|commit| is_real_commit(commit));
    match (runtime_commit, embedded_commit) {
        (Some(runtime_commit), Some(embedded_commit)) if runtime_commit != embedded_commit => {
            Err(ProcessTelemetryConfigError::ReleaseCommitMismatch)
        }
        (Some(runtime_commit), _) => Ok(runtime_commit),
        (None, Some(embedded_commit)) => Ok(embedded_commit),
        (None, None) => Ok(DEFAULT_LOCAL_COMMIT),
    }
}

fn parse_metrics_exporter(value: Option<&str>) -> Result<crate::MetricsExporter, ProcessTelemetryConfigError> {
    match value {
        None | Some("disable") => Ok(crate::MetricsExporter::Disable),
        Some("otlp_grpc") => Ok(crate::MetricsExporter::OtlpGrpc),
        Some("prometheus") => Ok(crate::MetricsExporter::Prometheus),
        Some("log") => Ok(crate::MetricsExporter::Log),
        Some(_) => Err(ProcessTelemetryConfigError::InvalidMetricsExporter),
    }
}

fn validate_metrics_selection(
    enabled: bool,
    exporter: crate::MetricsExporter,
) -> Result<(), ProcessTelemetryConfigError> {
    if enabled == exporter.is_enabled() {
        Ok(())
    } else {
        Err(ProcessTelemetryConfigError::InconsistentMetricsSelection)
    }
}

fn is_canonical_metrics_path(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_PROMETHEUS_PATH_LEN
        && value.starts_with('/')
        && (value == "/"
            || value
                .split('/')
                .skip(1)
                .all(|segment| !segment.is_empty() && segment != "." && segment != ".."))
        && value
            .as_bytes()
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(*byte, b'/' | b'-' | b'_' | b'.'))
}

fn read_process_env(name: &'static str) -> Result<Option<String>, ProcessTelemetryConfigError> {
    match std::env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => Err(ProcessTelemetryConfigError::NonUnicodeEnvironment(name)),
    }
}

fn is_full_lowercase_commit(value: &str) -> bool {
    value.len() == GIT_COMMIT_LEN
        && value
            .as_bytes()
            .iter()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
}

fn is_real_commit(value: &str) -> bool {
    value != DEFAULT_LOCAL_COMMIT && is_full_lowercase_commit(value)
}

fn is_stable_label(value: &str) -> bool {
    if value.is_empty() || value.len() > MAX_STABLE_LABEL_LEN {
        return false;
    }

    let bytes = value.as_bytes();
    bytes.first().is_some_and(u8::is_ascii_alphanumeric)
        && bytes.last().is_some_and(u8::is_ascii_alphanumeric)
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'-')
}

/// Outcome of registering a release identity metric.
#[cfg(feature = "otel-metrics")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReleaseIdentityRegistrationStatus {
    /// This instance registered its identity on the supplied meter.
    Registered,
    /// This instance, or one of its clones, had already registered.
    AlreadyRegistered,
}

/// Instance-scoped registration for the release identity gauge.
///
/// Clones share only this registration's local state. No global meter provider
/// or process-wide registration cell is read or mutated.
#[cfg(feature = "otel-metrics")]
#[derive(Clone, Debug)]
pub struct ReleaseIdentityRegistration {
    inner: Arc<ReleaseIdentityRegistrationInner>,
}

#[cfg(feature = "otel-metrics")]
#[derive(Debug)]
struct ReleaseIdentityRegistrationInner {
    identity: ValidatedReleaseIdentity,
    registered: AtomicBool,
    instrument: parking_lot::Mutex<Option<opentelemetry::metrics::Gauge<u64>>>,
}

#[cfg(feature = "otel-metrics")]
impl ReleaseIdentityRegistration {
    /// Creates an unregistered, instance-scoped release identity.
    pub fn new(identity: ValidatedReleaseIdentity) -> Self {
        Self {
            inner: Arc::new(ReleaseIdentityRegistrationInner {
                identity,
                registered: AtomicBool::new(false),
                instrument: parking_lot::Mutex::new(None),
            }),
        }
    }

    /// Returns this registration's validated identity.
    pub fn identity(&self) -> &ValidatedReleaseIdentity {
        &self.inner.identity
    }

    /// Returns whether this instance, or one of its clones, registered.
    pub fn is_registered(&self) -> bool {
        self.inner.registered.load(Ordering::Acquire)
    }

    /// Registers exactly one release identity series on an explicit meter.
    ///
    /// A second call on this instance or any clone is idempotent. Separately
    /// constructed instances retain independent registration state.
    pub(crate) fn register(&self, meter: &opentelemetry::metrics::Meter) -> ReleaseIdentityRegistrationStatus {
        if self
            .inner
            .registered
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return ReleaseIdentityRegistrationStatus::AlreadyRegistered;
        }

        let gauge = meter
            .u64_gauge(crate::semantic::metrics::RELEASE_INFO)
            .with_description("RocketMQ service build and rollout identity")
            .with_unit("1")
            .build();
        gauge.record(1, &self.inner.identity.attributes());
        *self.inner.instrument.lock() = Some(gauge);

        ReleaseIdentityRegistrationStatus::Registered
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stable_label_accepts_only_canonical_bounded_values() {
        assert!(is_stable_label("a"));
        assert!(is_stable_label("rocketmq-broker-01"));
        assert!(is_stable_label(&"a".repeat(MAX_STABLE_LABEL_LEN)));

        assert!(!is_stable_label(""));
        assert!(!is_stable_label("-broker"));
        assert!(!is_stable_label("broker-"));
        assert!(!is_stable_label("Rocketmq-broker"));
        assert!(!is_stable_label("rocketmq_broker"));
        assert!(!is_stable_label(&"a".repeat(MAX_STABLE_LABEL_LEN + 1)));
    }

    #[test]
    fn commit_requires_full_lowercase_hex() {
        assert!(is_full_lowercase_commit("0123456789abcdef0123456789abcdef01234567"));
        assert!(!is_full_lowercase_commit("0123456789ABCDEF0123456789ABCDEF01234567"));
        assert!(!is_full_lowercase_commit("g123456789abcdef0123456789abcdef01234567"));
        assert!(!is_full_lowercase_commit("0123456789abcdef0123456789abcdef0123456"));
    }

    #[test]
    fn local_host_build_without_real_embedded_commit_keeps_local_default() {
        assert_eq!(resolve_release_commit(None, None), Ok(DEFAULT_LOCAL_COMMIT));
        assert_eq!(
            resolve_release_commit(None, Some(DEFAULT_LOCAL_COMMIT)),
            Ok(DEFAULT_LOCAL_COMMIT)
        );
        assert_eq!(
            resolve_release_commit(None, Some("not-a-build-commit")),
            Ok(DEFAULT_LOCAL_COMMIT)
        );
    }

    #[test]
    fn real_embedded_commit_binds_runtime_identity() {
        const BUILD_COMMIT: &str = "0123456789abcdef0123456789abcdef01234567";
        const OTHER_COMMIT: &str = "89abcdef0123456789abcdef0123456789abcdef";

        assert_eq!(resolve_release_commit(None, Some(BUILD_COMMIT)), Ok(BUILD_COMMIT));
        assert_eq!(
            resolve_release_commit(Some(BUILD_COMMIT), Some(BUILD_COMMIT)),
            Ok(BUILD_COMMIT)
        );
        assert_eq!(
            resolve_release_commit(Some(OTHER_COMMIT), Some(BUILD_COMMIT)),
            Err(ProcessTelemetryConfigError::ReleaseCommitMismatch)
        );
    }

    #[test]
    fn explicit_null_runtime_commit_fails_closed() {
        const BUILD_COMMIT: &str = "0123456789abcdef0123456789abcdef01234567";

        assert_eq!(
            resolve_release_commit(Some(DEFAULT_LOCAL_COMMIT), None),
            Err(ProcessTelemetryConfigError::NullReleaseCommit)
        );
        assert_eq!(
            resolve_release_commit(Some(DEFAULT_LOCAL_COMMIT), Some(BUILD_COMMIT)),
            Err(ProcessTelemetryConfigError::NullReleaseCommit)
        );
    }
}
