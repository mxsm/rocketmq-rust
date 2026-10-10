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

//! Runtime signals of the control service: stderr logs and optional OTLP metrics.
//!
//! Every value written by this module is a closed label, a count, a duration, an audit sequence
//! number, or an authorized logical cluster name. Operator identity, request reasons, network
//! addresses, tokens, and backend error text never reach a log line or a metric attribute.

use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::time::Duration;
use std::time::Instant;

use rocketmq_observability::metrics::mcp::McpFailureLabel;
use rocketmq_observability::metrics::mcp::McpMetricsRecorder;
use rocketmq_observability::metrics::mcp::McpOperationKind;
use rocketmq_observability::metrics::mcp::McpOperationStatus;
use rocketmq_observability::ObservabilityError;
use rocketmq_observability::SubscriberInstallPolicy;
use rocketmq_observability::TelemetryBootstrapConfig;
use rocketmq_observability::TelemetryRuntimeGuard;
use rocketmq_runtime::ChildServiceContext;

use crate::error::ControlError;
use crate::error::ControlErrorCode;

const SERVICE_NAME: &str = "rocketmq-mcp-control";

/// Only this crate's events are written. Dependency targets stay off because their events can
/// carry network addresses and backend error text, which this service must not log.
const LOG_FILTER: &str = "off,rocketmq_mcp_control=info";

/// Rejections of one kind produce at most one log line per interval.
const REJECTION_LOG_INTERVAL: Duration = Duration::from_secs(10);

/// Budget for closing the logging-only fallback, which has no exporter to flush.
const LOGGING_ONLY_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);

/// Metric operation for requests refused by the Host and Origin policy.
const HTTP_REQUEST_OPERATION: &str = "http_request";

/// Metric operation for requests refused by OAuth authentication.
const AUTHENTICATION_OPERATION: &str = "authentication";

/// Process telemetry, installed before the configuration file is read so that a rejected
/// configuration can be explained.
#[must_use = "telemetry must be shut down explicitly so that pending metrics are flushed"]
pub struct Telemetry {
    guard: TelemetryRuntimeGuard,
    signals: ControlSignals,
}

impl Telemetry {
    /// Installs the stderr log subscriber and, in an `otlp` build with the OTLP endpoint
    /// configured, the metrics exporter.
    ///
    /// Exporter tasks are owned by `service_context`; pass the same context to
    /// [`Self::shutdown`].
    ///
    /// # Errors
    ///
    /// Returns `invalid_config` when the OTLP environment is invalid, the exporter cannot start,
    /// or the process already has a log subscriber. The reason is logged when logging alone can
    /// still be installed.
    pub async fn install(service_context: &ChildServiceContext) -> Result<Self, ControlError> {
        let failure = match environment_bootstrap() {
            Ok(bootstrap) => match Self::install_bootstrap(&bootstrap, service_context).await {
                Ok(telemetry) => return Ok(telemetry),
                Err(failure) => failure,
            },
            Err(failure) => failure,
        };
        // The requested telemetry is unusable. Logging is installed on its own so that the
        // reason is visible before the process stops.
        if let Ok(logging) = Self::install_bootstrap(&service_bootstrap(), service_context).await {
            tracing::error!(
                stage = "telemetry",
                operation = ?failure.operation(),
                "control telemetry could not start"
            );
            logging.shutdown(service_context, LOGGING_ONLY_SHUTDOWN_TIMEOUT).await;
        }
        Err(ControlError::invalid_config())
    }

    async fn install_bootstrap(
        bootstrap: &TelemetryBootstrapConfig,
        service_context: &ChildServiceContext,
    ) -> Result<Self, ObservabilityError> {
        let guard = rocketmq_observability::install_global_with_service_context(bootstrap, service_context).await?;
        let signals = ControlSignals::with_metrics(McpMetricsRecorder::from_handle(&guard.handle()));
        Ok(Self { guard, signals })
    }

    /// Returns the recorder that request paths use for logs and metrics.
    pub fn signals(&self) -> ControlSignals {
        self.signals.clone()
    }

    /// Flushes pending metrics within `timeout`. An incomplete flush is logged and otherwise
    /// ignored, because the service has already stopped serving.
    pub async fn shutdown(self, service_context: &ChildServiceContext, timeout: Duration) {
        let report = self.guard.shutdown_with_service_context(service_context, timeout).await;
        if !report.is_healthy() {
            tracing::warn!(
                metrics_flushed = report.metrics_shutdown_ok,
                "control telemetry did not shut down cleanly"
            );
        }
    }
}

impl fmt::Debug for Telemetry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("Telemetry").finish_non_exhaustive()
    }
}

fn service_bootstrap() -> TelemetryBootstrapConfig {
    let mut bootstrap = TelemetryBootstrapConfig::default();
    bootstrap.observability.service_name = SERVICE_NAME.to_owned();
    bootstrap.observability.service_version = env!("CARGO_PKG_VERSION").to_owned();
    bootstrap.observability.node_type = "mcp-control".to_owned();
    bootstrap.observability.subscriber_install_policy = SubscriberInstallPolicy::Required;
    bootstrap.logging.filter = LOG_FILTER.to_owned();
    // Escape sequences would split the `name=value` pairs that operators search for.
    bootstrap.logging.console.ansi = false;
    bootstrap
}

fn environment_bootstrap() -> Result<TelemetryBootstrapConfig, ObservabilityError> {
    #[cfg(feature = "otlp")]
    {
        let endpoint = std::env::var_os(rocketmq_observability::OTEL_EXPORTER_OTLP_ENDPOINT);
        let protocol = std::env::var_os(rocketmq_observability::OTEL_EXPORTER_OTLP_PROTOCOL);
        otlp_bootstrap(endpoint.as_deref(), protocol.as_deref())
    }
    // A build without the exporter keeps ignoring the OTLP variables, as it did before
    // telemetry existed.
    #[cfg(not(feature = "otlp"))]
    {
        Ok(service_bootstrap())
    }
}

#[cfg(feature = "otlp")]
fn otlp_bootstrap(
    endpoint: Option<&std::ffi::OsStr>,
    protocol: Option<&std::ffi::OsStr>,
) -> Result<TelemetryBootstrapConfig, ObservabilityError> {
    let mut bootstrap = service_bootstrap();
    rocketmq_observability::apply_standard_otlp_environment_values(&mut bootstrap, endpoint, protocol)?;
    // The standard variables switch on every signal. This service exports metrics only: spans
    // and exported log records are not part of its reviewed output.
    let observability = &mut bootstrap.observability;
    observability.traces.enabled = false;
    observability.traces.exporter = rocketmq_observability::TraceExporter::Disable;
    observability.logs.enabled = false;
    observability.logs.exporter = rocketmq_observability::LogsExporter::Disable;
    observability.enabled = observability.metrics.enabled;
    rocketmq_observability::normalize_and_validate(observability)?;
    Ok(bootstrap)
}

/// Cloneable recorder for the logs and metrics of one control server instance.
///
/// The default recorder writes logs and records no metrics.
#[derive(Clone)]
pub struct ControlSignals {
    metrics: McpMetricsRecorder,
    rejections: Arc<RejectionLogGate>,
}

impl ControlSignals {
    fn with_metrics(metrics: McpMetricsRecorder) -> Self {
        Self {
            metrics,
            rejections: Arc::new(RejectionLogGate::new()),
        }
    }

    /// Records a request refused by the Host and Origin policy before authentication.
    pub(crate) fn request_rejected(&self) {
        self.metrics.record_error(
            McpOperationKind::Tool,
            HTTP_REQUEST_OPERATION,
            McpFailureLabel::InvalidRequest,
        );
        let code = ControlErrorCode::RequestRejected.as_str();
        if let Some(suppressed) = self.rejections.admit("origin", code) {
            tracing::warn!(
                site = "origin",
                code,
                suppressed,
                "request was rejected before authentication"
            );
        }
    }

    /// Records a request refused by OAuth authentication. `code` is the code of the returned
    /// error envelope.
    pub(crate) fn authentication_rejected(&self, rejection: AuthenticationRejection, code: ControlErrorCode) {
        self.metrics
            .record_error(McpOperationKind::Tool, AUTHENTICATION_OPERATION, rejection.failure());
        if let Some(suppressed) = self.rejections.admit("authentication", rejection.as_str()) {
            tracing::warn!(
                site = "authentication",
                code = code.as_str(),
                reason = rejection.as_str(),
                suppressed,
                "request was rejected by authentication"
            );
        }
    }

    /// Records a tool call that ended before a supervised mutation started. `tool` is a reviewed
    /// tool name or `unknown`, never the caller's text.
    pub(crate) fn call_rejected(&self, tool: &'static str, code: ControlErrorCode, elapsed: Duration) {
        self.metrics
            .record_operation(McpOperationKind::Tool, tool, operation_status(code), elapsed);
        self.metrics
            .record_error(McpOperationKind::Tool, tool, failure_label(code));
        if let Some(suppressed) = self.rejections.admit("call", code.as_str()) {
            tracing::warn!(
                site = "call",
                tool,
                code = code.as_str(),
                suppressed,
                "mutation call was rejected before execution"
            );
        }
    }

    /// Records the terminal state of one supervised mutation.
    #[cfg(feature = "write-tools")]
    pub(crate) fn mutation_finished(&self, record: MutationRecord<'_>) {
        // The caller receives `audit_unavailable` when the terminal record was not persisted.
        let reported = if record.audit_recorded {
            record.error_code
        } else {
            Some(ControlErrorCode::AuditUnavailable)
        };
        let operation = mutation_metric_operation(record.operation, record.dry_run);
        let status = match reported {
            None => McpOperationStatus::Success,
            Some(_) => McpOperationStatus::Failure,
        };
        self.metrics
            .record_operation(McpOperationKind::Tool, operation, status, record.elapsed);
        if let Some(code) = reported {
            self.metrics
                .record_error(McpOperationKind::Tool, operation, failure_label(code));
        }

        let mode = if record.dry_run { "dry_run" } else { "execute" };
        let elapsed_ms = u64::try_from(record.elapsed.as_millis()).unwrap_or(u64::MAX);
        let clean = record.audit_recorded
            && matches!(
                record.result,
                crate::audit::AuditResult::Planned | crate::audit::AuditResult::Applied
            );
        if clean {
            tracing::info!(
                operation = record.operation.as_str(),
                cluster = record.cluster.as_str(),
                mode,
                result = audit_result_label(record.result),
                invocation_id = record.invocation.get(),
                elapsed_ms,
                "mutation finished"
            );
        } else {
            tracing::warn!(
                operation = record.operation.as_str(),
                cluster = record.cluster.as_str(),
                mode,
                result = audit_result_label(record.result),
                code = record.error_code.map(ControlErrorCode::as_str),
                audit_recorded = record.audit_recorded,
                invocation_id = record.invocation.get(),
                elapsed_ms,
                "mutation finished without a clean result"
            );
        }
    }

    /// Records how the request-key cache admitted one mutation.
    #[cfg(feature = "write-tools")]
    pub(crate) fn cache_event(&self, event: rocketmq_observability::metrics::mcp::McpCacheEvent) {
        self.metrics.record_cache_event(event);
    }

    /// Records a durable audit record that could not be persisted or confirmed.
    #[cfg(feature = "write-tools")]
    pub(crate) fn audit_failed(&self, stage: AuditStage) {
        self.metrics
            .record_audit_failure(rocketmq_observability::metrics::mcp::McpAuditFailureKind::Sink);
        if let Some(suppressed) = self.rejections.admit("audit", stage.as_str()) {
            tracing::error!(
                site = "audit",
                stage = stage.as_str(),
                suppressed,
                "durable audit record is unavailable"
            );
        }
    }
}

impl Default for ControlSignals {
    fn default() -> Self {
        Self::with_metrics(McpMetricsRecorder::noop())
    }
}

impl fmt::Debug for ControlSignals {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("ControlSignals").finish_non_exhaustive()
    }
}

/// Closed reasons for an OAuth rejection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AuthenticationRejection {
    /// The bearer token is missing, malformed, expired, or not signed by a trusted key.
    InvalidToken,
    /// The token is valid but does not carry the required write scope.
    InsufficientScope,
    /// The signing keys could not be obtained.
    KeysUnavailable,
}

impl AuthenticationRejection {
    const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidToken => "invalid_token",
            Self::InsufficientScope => "insufficient_scope",
            Self::KeysUnavailable => "keys_unavailable",
        }
    }

    const fn failure(self) -> McpFailureLabel {
        match self {
            Self::InvalidToken | Self::InsufficientScope => McpFailureLabel::PermissionDenied,
            Self::KeysUnavailable => McpFailureLabel::SourceUnavailable,
        }
    }
}

/// Closed server stages named in failure logs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ServerStage {
    /// The remote signing keys could not be fetched or are unusable.
    OAuthKeys,
    /// The server certificate or private key could not be loaded.
    Tls,
    /// The configured address could not be bound.
    Listener,
    /// The mutation tools could not be prepared.
    #[cfg(feature = "write-tools")]
    MutationTools,
    /// The HTTPS server stopped with an error.
    Serve,
}

impl ServerStage {
    const fn as_str(self) -> &'static str {
        match self {
            Self::OAuthKeys => "oauth_keys",
            Self::Tls => "tls",
            Self::Listener => "listener",
            #[cfg(feature = "write-tools")]
            Self::MutationTools => "mutation_tools",
            Self::Serve => "serve",
        }
    }
}

/// Logs the stage at which the server failed and hands the error back to the caller.
pub(crate) fn server_failed(stage: ServerStage, error: ControlError) -> ControlError {
    tracing::error!(
        stage = stage.as_str(),
        code = error.code().as_str(),
        "control server failed"
    );
    error
}

/// Logs that the server accepts requests. The fields repeat the capability resource.
pub(crate) fn server_ready(capabilities: &crate::model::ControlCapabilities) {
    tracing::info!(
        version = env!("CARGO_PKG_VERSION"),
        write_tools_compiled = capabilities.write_tools_compiled(),
        mutations_runtime_enabled = capabilities.mutations_runtime_enabled(),
        registered_operations = capabilities.registered_operations(),
        "rocketmq-mcp-control authenticated HTTPS transport is ready"
    );
}

/// Terminal state of one supervised mutation, as written to the log and the metrics.
#[cfg(feature = "write-tools")]
pub(crate) struct MutationRecord<'a> {
    pub(crate) operation: crate::model::ControlOperation,
    pub(crate) cluster: &'a crate::model::ClusterName,
    pub(crate) dry_run: bool,
    pub(crate) result: crate::audit::AuditResult,
    pub(crate) error_code: Option<ControlErrorCode>,
    /// Sequence number shared with the durable `started` record.
    pub(crate) invocation: crate::audit::AuditInvocationId,
    /// Whether the terminal audit record was persisted.
    pub(crate) audit_recorded: bool,
    pub(crate) elapsed: Duration,
}

/// Where a durable audit record was lost.
#[cfg(feature = "write-tools")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AuditStage {
    /// The `started` record was not persisted, so no session or RPC began.
    Started,
    /// The mutation ran but its terminal record was not persisted.
    Terminal,
    /// The supervisor stopped before reporting, so the terminal state is unknown.
    Supervisor,
}

#[cfg(feature = "write-tools")]
impl AuditStage {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Started => "started",
            Self::Terminal => "terminal",
            Self::Supervisor => "supervisor",
        }
    }
}

/// Returns the reviewed tool name of an operation, for the metric and log `tool` label.
#[cfg(feature = "write-tools")]
pub(crate) const fn tool_name(operation: crate::model::ControlOperation) -> &'static str {
    use crate::model::ControlOperation;
    match operation {
        ControlOperation::TopicUpsert => crate::tools::UPSERT_TOPIC_TOOL,
        ControlOperation::ConsumerGroupUpsert => crate::tools::UPSERT_CONSUMER_GROUP_TOOL,
        ControlOperation::ConsumerOffsetReset => crate::tools::RESET_CONSUMER_OFFSET_TOOL,
        ControlOperation::BrokerConfigPatch => crate::tools::PATCH_BROKER_CONFIG_TOOL,
        ControlOperation::ConsumerRequestMode => crate::tools::SET_CONSUMER_REQUEST_MODE_TOOL,
    }
}

/// Returns the metric operation of a supervised mutation.
///
/// The shared MCP recorder has no mode attribute, so the mode is part of the static operation
/// name. A dry run and an execution must never be counted together.
#[cfg(feature = "write-tools")]
const fn mutation_metric_operation(operation: crate::model::ControlOperation, dry_run: bool) -> &'static str {
    use crate::model::ControlOperation;
    match (operation, dry_run) {
        (ControlOperation::TopicUpsert, true) => "rocketmq_upsert_topic.dry_run",
        (ControlOperation::TopicUpsert, false) => "rocketmq_upsert_topic.execute",
        (ControlOperation::ConsumerGroupUpsert, true) => "rocketmq_upsert_consumer_group.dry_run",
        (ControlOperation::ConsumerGroupUpsert, false) => "rocketmq_upsert_consumer_group.execute",
        (ControlOperation::ConsumerOffsetReset, true) => "rocketmq_reset_consumer_offset.dry_run",
        (ControlOperation::ConsumerOffsetReset, false) => "rocketmq_reset_consumer_offset.execute",
        (ControlOperation::BrokerConfigPatch, true) => "rocketmq_patch_broker_config.dry_run",
        (ControlOperation::BrokerConfigPatch, false) => "rocketmq_patch_broker_config.execute",
        (ControlOperation::ConsumerRequestMode, true) => "rocketmq_set_consumer_request_mode.dry_run",
        (ControlOperation::ConsumerRequestMode, false) => "rocketmq_set_consumer_request_mode.execute",
    }
}

#[cfg(feature = "write-tools")]
const fn audit_result_label(result: crate::audit::AuditResult) -> &'static str {
    use crate::audit::AuditResult;
    match result {
        AuditResult::Started => "started",
        AuditResult::Planned => "planned",
        AuditResult::Applied => "applied",
        AuditResult::Partial => "partial",
        AuditResult::Conflict => "conflict",
        AuditResult::Failed => "failed",
    }
}

/// Authorization failures are `denied`; every other error is a `failure`.
const fn operation_status(code: ControlErrorCode) -> McpOperationStatus {
    match code {
        ControlErrorCode::Unauthorized
        | ControlErrorCode::PermissionDenied
        | ControlErrorCode::ClusterNotAllowed
        | ControlErrorCode::OperationNotAllowed
        | ControlErrorCode::MutationDisabled => McpOperationStatus::Denied,
        ControlErrorCode::InvalidConfig
        | ControlErrorCode::RequestRejected
        | ControlErrorCode::OperationUnavailable
        | ControlErrorCode::ConfirmationRequired
        | ControlErrorCode::InvalidArgument
        | ControlErrorCode::AuditUnavailable
        | ControlErrorCode::PreconditionConflict
        | ControlErrorCode::PartialApply
        | ControlErrorCode::VerificationFailed
        | ControlErrorCode::Timeout
        | ControlErrorCode::Cancelled
        | ControlErrorCode::ExecutionFailed
        | ControlErrorCode::ShutdownFailed => McpOperationStatus::Failure,
    }
}

/// Folds the control error vocabulary into the bounded failure classes of the shared MCP metrics.
const fn failure_label(code: ControlErrorCode) -> McpFailureLabel {
    match code {
        ControlErrorCode::Unauthorized
        | ControlErrorCode::PermissionDenied
        | ControlErrorCode::ClusterNotAllowed
        | ControlErrorCode::OperationNotAllowed
        | ControlErrorCode::MutationDisabled => McpFailureLabel::PermissionDenied,
        ControlErrorCode::RequestRejected
        | ControlErrorCode::ConfirmationRequired
        | ControlErrorCode::InvalidArgument
        | ControlErrorCode::PreconditionConflict => McpFailureLabel::InvalidRequest,
        ControlErrorCode::OperationUnavailable
        | ControlErrorCode::AuditUnavailable
        | ControlErrorCode::PartialApply
        | ControlErrorCode::VerificationFailed
        | ControlErrorCode::Timeout
        | ControlErrorCode::ShutdownFailed => McpFailureLabel::SourceUnavailable,
        ControlErrorCode::InvalidConfig | ControlErrorCode::Cancelled | ControlErrorCode::ExecutionFailed => {
            McpFailureLabel::Internal
        }
    }
}

/// Bounds the log volume of rejection paths.
///
/// Unauthenticated callers decide how often those paths run, so each `(site, code)` pair is
/// logged at most once per interval and the next line reports how many were skipped. Metrics
/// still count every rejection.
struct RejectionLogGate {
    started: Instant,
    slots: Mutex<BTreeMap<(&'static str, &'static str), GateSlot>>,
}

struct GateSlot {
    /// Time since the gate started at which this pair was last logged.
    logged_at: Duration,
    suppressed: u64,
}

impl RejectionLogGate {
    fn new() -> Self {
        Self {
            started: Instant::now(),
            slots: Mutex::new(BTreeMap::new()),
        }
    }

    /// Returns the number of skipped rejections when this one should be logged.
    fn admit(&self, site: &'static str, code: &'static str) -> Option<u64> {
        self.admit_at(site, code, self.started.elapsed())
    }

    fn admit_at(&self, site: &'static str, code: &'static str, elapsed: Duration) -> Option<u64> {
        let mut slots = self.slots.lock().unwrap_or_else(PoisonError::into_inner);
        match slots.entry((site, code)) {
            Entry::Vacant(entry) => {
                entry.insert(GateSlot {
                    logged_at: elapsed,
                    suppressed: 0,
                });
                Some(0)
            }
            Entry::Occupied(mut entry) => {
                let slot = entry.get_mut();
                if elapsed.saturating_sub(slot.logged_at) < REJECTION_LOG_INTERVAL {
                    slot.suppressed = slot.suppressed.saturating_add(1);
                    None
                } else {
                    slot.logged_at = elapsed;
                    Some(std::mem::take(&mut slot.suppressed))
                }
            }
        }
    }
}

#[cfg(test)]
pub(crate) mod testing {
    use std::io::Write;
    use std::sync::Arc;
    use std::sync::Mutex;

    use tracing_subscriber::fmt::MakeWriter;

    /// Captures the log events of the current thread as plain text until it is dropped.
    pub(crate) struct LogCapture {
        buffer: SharedBuffer,
        _guard: tracing::subscriber::DefaultGuard,
    }

    impl LogCapture {
        pub(crate) fn start() -> Self {
            let buffer = SharedBuffer::default();
            let subscriber = tracing_subscriber::fmt()
                .without_time()
                .with_ansi(false)
                .with_writer(buffer.clone())
                .finish();
            Self {
                buffer,
                _guard: tracing::subscriber::set_default(subscriber),
            }
        }

        /// Captures only the events that `filter` lets through.
        pub(crate) fn with_filter(filter: &str) -> Self {
            let buffer = SharedBuffer::default();
            let subscriber = tracing_subscriber::fmt()
                .with_env_filter(tracing_subscriber::EnvFilter::try_new(filter).unwrap())
                .without_time()
                .with_ansi(false)
                .with_writer(buffer.clone())
                .finish();
            Self {
                buffer,
                _guard: tracing::subscriber::set_default(subscriber),
            }
        }

        pub(crate) fn text(&self) -> String {
            String::from_utf8(self.buffer.0.lock().unwrap().clone()).unwrap()
        }
    }

    #[derive(Clone, Default)]
    struct SharedBuffer(Arc<Mutex<Vec<u8>>>);

    impl Write for SharedBuffer {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'writer> MakeWriter<'writer> for SharedBuffer {
        type Writer = Self;

        fn make_writer(&'writer self) -> Self::Writer {
            self.clone()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::testing::LogCapture;
    use super::*;

    #[test]
    fn only_this_crate_is_logged_and_no_metrics_are_exported_by_default() {
        let bootstrap = service_bootstrap();
        {
            let logs = LogCapture::with_filter(&bootstrap.logging.filter);
            tracing::error!(target: "rocketmq_client_rust::factory", "dependency event");
            tracing::error!(target: "hyper::client", "dependency event");
            tracing::debug!("service debug event");
            tracing::info!("service event");
            let text = logs.text();
            assert!(text.contains("service event"));
            assert!(!text.contains("dependency event"));
            assert!(!text.contains("service debug event"));
        }
        assert!(bootstrap.logging.console.enabled);
        assert!(!bootstrap.logging.console.ansi);
        assert!(!bootstrap.logging.file.enabled);
        assert!(!bootstrap.observability.enabled);
        assert!(!bootstrap.observability.metrics.enabled);
        assert_eq!(bootstrap.observability.service_name, SERVICE_NAME);
    }

    #[test]
    fn rejection_logs_are_bounded_per_site_and_code() {
        let gate = RejectionLogGate::new();
        let at = Duration::from_secs;
        assert_eq!(gate.admit_at("authentication", "invalid_token", at(0)), Some(0));
        assert_eq!(gate.admit_at("authentication", "invalid_token", at(1)), None);
        assert_eq!(gate.admit_at("authentication", "invalid_token", at(9)), None);
        // A different code or site is not hidden by the flood.
        assert_eq!(gate.admit_at("authentication", "insufficient_scope", at(9)), Some(0));
        assert_eq!(gate.admit_at("call", "invalid_token", at(9)), Some(0));
        // Once the interval has passed, the next line reports what was skipped.
        assert_eq!(gate.admit_at("authentication", "invalid_token", at(10)), Some(2));
        assert_eq!(gate.admit_at("authentication", "invalid_token", at(19)), None);
        assert_eq!(gate.admit_at("authentication", "invalid_token", at(20)), Some(1));
    }

    #[test]
    fn rejections_log_closed_labels_once_per_interval() {
        let logs = LogCapture::start();
        let signals = ControlSignals::default();
        signals.request_rejected();
        signals.request_rejected();
        signals.authentication_rejected(AuthenticationRejection::InvalidToken, ControlErrorCode::Unauthorized);
        signals.authentication_rejected(
            AuthenticationRejection::InsufficientScope,
            ControlErrorCode::PermissionDenied,
        );
        signals.call_rejected("unknown", ControlErrorCode::ClusterNotAllowed, Duration::from_millis(3));
        let text = logs.text();
        assert_eq!(text.matches("request was rejected before authentication").count(), 1);
        assert!(text.contains(r#"site="origin" code="request_rejected" suppressed=0"#));
        assert!(text.contains(r#"site="authentication" code="unauthorized" reason="invalid_token" suppressed=0"#));
        assert!(text.contains(r#"code="permission_denied" reason="insufficient_scope""#));
        assert!(text.contains(r#"site="call" tool="unknown" code="cluster_not_allowed" suppressed=0"#));
        assert_eq!(text.matches("WARN").count(), 4);
    }

    #[test]
    fn startup_and_ready_logs_name_stages_and_capabilities_only() {
        let logs = LogCapture::start();
        let error = server_failed(ServerStage::Tls, ControlError::invalid_config());
        assert_eq!(error.code(), ControlErrorCode::InvalidConfig);
        let policy = crate::config::MutationPolicyConfig::default();
        let catalog = crate::catalog::OperationCatalog::from_policy(&policy);
        server_ready(&crate::model::ControlCapabilities::from_catalog(false, &catalog));
        let text = logs.text();
        assert!(text.contains(r#"stage="tls" code="invalid_config""#));
        assert!(text.contains("mutations_runtime_enabled=false registered_operations=0"));
        assert_eq!(text.matches("authenticated HTTPS transport is ready").count(), 1);
    }

    #[cfg(feature = "write-tools")]
    #[test]
    fn metric_operations_extend_the_reviewed_tool_names() {
        use crate::model::ControlOperation;
        for operation in [
            ControlOperation::TopicUpsert,
            ControlOperation::ConsumerGroupUpsert,
            ControlOperation::ConsumerOffsetReset,
            ControlOperation::BrokerConfigPatch,
            ControlOperation::ConsumerRequestMode,
        ] {
            let tool = tool_name(operation);
            assert_eq!(mutation_metric_operation(operation, true), format!("{tool}.dry_run"));
            assert_eq!(mutation_metric_operation(operation, false), format!("{tool}.execute"));
        }
    }

    #[cfg(feature = "write-tools")]
    #[tokio::test]
    async fn mutation_logs_carry_the_audit_correlation_and_no_operator_evidence() {
        use crate::audit::AuditContext;
        use crate::audit::AuditResult;
        use crate::audit::AuditTrail;
        use crate::audit::MemoryAuditSink;
        use crate::model::ClusterName;
        use crate::model::ControlOperation;

        let cluster = ClusterName::try_new("cluster-a").unwrap();
        let trail = AuditTrail::new(Arc::new(MemoryAuditSink::new(16, 4096)));
        let context = AuditContext::try_new("alice@example.com", Some("planned operation")).unwrap();
        let invocation = trail
            .start(&context, ControlOperation::TopicUpsert, &cluster, false)
            .await
            .unwrap();

        let logs = LogCapture::start();
        let signals = ControlSignals::default();
        let record = |result, error_code, audit_recorded| MutationRecord {
            operation: ControlOperation::TopicUpsert,
            cluster: &cluster,
            dry_run: false,
            result,
            error_code,
            invocation: invocation.id(),
            audit_recorded,
            elapsed: Duration::from_millis(42),
        };
        signals.mutation_finished(record(AuditResult::Applied, None, true));
        signals.mutation_finished(record(AuditResult::Partial, Some(ControlErrorCode::PartialApply), true));
        signals.mutation_finished(record(AuditResult::Applied, None, false));
        signals.audit_failed(AuditStage::Terminal);
        signals.audit_failed(AuditStage::Terminal);

        let text = logs.text();
        assert!(text.contains(
            r#"INFO rocketmq_mcp_control::telemetry: mutation finished operation="topic_upsert" cluster="cluster-a" mode="execute" result="applied" invocation_id=1 elapsed_ms=42"#
        ));
        assert!(text.contains(r#"result="partial" code="partial_apply" audit_recorded=true"#));
        assert!(text.contains(r#"result="applied" audit_recorded=false"#));
        assert_eq!(text.matches("durable audit record is unavailable").count(), 1);
        assert!(text.contains(r#"site="audit" stage="terminal" suppressed=0"#));
        for forbidden in ["alice", "example.com", "planned operation"] {
            assert!(!text.contains(forbidden), "log exposed {forbidden}");
        }
    }

    #[cfg(feature = "otlp")]
    #[test]
    fn otlp_environment_enables_metrics_only_and_fails_closed() {
        use std::ffi::OsStr;

        use rocketmq_observability::MetricsExporter;

        let disabled = otlp_bootstrap(None, None).unwrap();
        assert!(!disabled.observability.enabled);

        let endpoint = OsStr::new("http://collector.example.test:4317");
        let enabled = otlp_bootstrap(Some(endpoint), Some(OsStr::new("grpc"))).unwrap();
        assert!(enabled.observability.enabled);
        assert!(enabled.observability.metrics.enabled);
        assert_eq!(enabled.observability.metrics.exporter, MetricsExporter::OtlpGrpc);
        assert!(!enabled.observability.traces.enabled);
        assert!(!enabled.observability.logs.enabled);

        for protocol in [None, Some(OsStr::new("http/protobuf"))] {
            let error = otlp_bootstrap(Some(endpoint), protocol).unwrap_err();
            assert!(!error.to_string().contains("collector.example.test"));
        }
    }
}
