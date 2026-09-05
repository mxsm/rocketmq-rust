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

use std::backtrace::Backtrace;
use std::error::Error;
use std::fmt;
use std::panic::Location;
use std::sync::Arc;

use rocketmq_error::CanonicalCondition;
use rocketmq_error::DiagnosticView;
use rocketmq_error::Error as CanonicalError;
use rocketmq_error::ErrorCode;
use rocketmq_error::ErrorContext;
use rocketmq_error::ErrorDescriptor;
use rocketmq_error::PublicErrorView;
use rocketmq_error::RecoveryHint;
use rocketmq_error::SharedError;

use crate::resource_budget::BudgetDimension;

/// Alias for an operational runtime result.
pub type RuntimeResult<T> = Result<T, RuntimeError>;

/// A closed, low-cardinality runtime operation identifier.
///
/// Runtime failures carry this value instead of caller-provided text so that
/// diagnostics remain aggregatable and cannot disclose request, path, or
/// configuration values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RuntimeOperation {
    /// Builds an owned Tokio runtime.
    BuildTokioRuntime,
    /// Builds a futures thread pool.
    BuildFuturesThreadPool,
    /// Loads a runtime configuration file.
    LoadConfigFile,
    /// Deserializes a runtime configuration file.
    DeserializeConfigFile,
    /// Detects the process memory limit.
    DetectProcessMemoryLimit,
    /// Reads a file.
    ReadFile,
    /// Reads a file before creating a backup.
    ReadFileBackup,
    /// Checks a file path.
    CheckFile,
    /// Copies a file backup.
    CopyFileBackup,
    /// Creates a file parent directory.
    CreateFileParent,
    /// Creates a file.
    CreateFile,
    /// Writes a file.
    WriteFile,
    /// Flushes a file.
    FlushFile,
    /// Persists metadata through a compatibility filesystem path.
    PersistMetadata,
    /// Creates a metadata parent directory.
    MetadataCreateParent,
    /// Creates a metadata temporary file.
    MetadataCreateTemporary,
    /// Writes a metadata temporary file.
    MetadataWriteTemporary,
    /// Synchronizes a metadata temporary file.
    MetadataSyncTemporary,
    /// Replaces a metadata target file.
    MetadataReplaceTarget,
    /// Synchronizes a metadata parent directory.
    MetadataSyncParent,
    /// Removes a metadata temporary file.
    MetadataRemoveTemporary,
    /// Waits for metadata durability.
    WaitForDurableMetadata,
    /// Admits a metadata snapshot.
    AdmitMetadataSnapshot,
    /// Admits metadata work.
    AdmitMetadataOperation,
    /// Admits metadata bytes.
    AdmitMetadataBytes,
    /// Uses a stopped metadata worker.
    MetadataWorkerStopped,
    /// Uses a closed metadata actor.
    MetadataIoClosed,
    /// Resolves a local IP address.
    ResolveLocalIp,
    /// Registers the SIGTERM handler.
    RegisterSigtermHandler,
    /// Registers the SIGINT handler.
    RegisterSigintHandler,
    /// Waits for an operating-system signal.
    WaitForSignal,
    /// Creates a task-group child.
    CreateTaskGroupChild,
    /// Spawns a task-group task.
    SpawnTaskGroupTask,
    /// Spawns a service task.
    SpawnServiceTask,
    /// Spawns a scheduler task.
    SpawnSchedulerTask,
    /// Creates an executor task group.
    CreateExecutorTaskGroup,
    /// Spawns an executor task.
    SpawnExecutorTask,
    /// Uses the ambient runtime context.
    RuntimeContext,
    /// Starts a service lifecycle.
    StartServiceLifecycle,
    /// Uses a service lifecycle task group.
    ServiceLifecycleTaskGroup,
    /// Binds a service health probe.
    BindServiceHealthProbe,
    /// Inspects a service health probe.
    InspectServiceHealthProbe,
    /// Marks a service ready.
    MarkServiceReady,
    /// Suspends service readiness.
    SuspendServiceReadiness,
    /// Restores service readiness.
    RestoreServiceReadiness,
    /// Shuts down an owned runtime from a blocking caller.
    ShutdownRuntimeBlocking,
    /// Performs blocking executor admission.
    BlockingQueueAdmission,
    /// Runs a blocking task.
    RunBlockingTask,
    /// Waits for a blocking task deadline.
    BlockingTaskDeadline,
    /// Waits for a blocking task.
    BlockingTask,
    /// Uses an unsupported blocking executor kind.
    BlockingExecutorKind,
    /// Clears completed scheduled registrations.
    ClearCompletedSchedules,
    /// Registers a scheduled task.
    RegisterScheduledTask,
    /// Spawns an operation.
    SpawnOperation,
    /// Validates an operation owner.
    OperationOwner,
    /// Uses a transport listener.
    TransportListener,
    /// Uses a transport session executor.
    SessionExecutor,
    /// Performs metadata I/O through an adapter.
    MetadataIo,
    /// Resumes a transport session executor.
    DeferredResumeSessionExecutor,
    /// Uses the auth metadata I/O lane.
    AuthMetadataIoLane,
    /// Runs high-availability runtime work.
    HaRuntime,
    /// Initializes a broker.
    InitializeBroker,
    /// Starts a broker.
    StartBroker,
    /// Shuts down a broker.
    ShutdownBroker,
    /// Uses a KV mutation worker.
    KvMutationWorker,
    /// Admits a KV mutation.
    AdmitKvMutation,
    /// Admits KV mutation bytes.
    AdmitKvMutationBytes,
    /// Persists KV metadata in a test fault path.
    KvPersistenceFault,
    /// Represents a test-only runtime failure injection.
    TestFailure,
    /// Validates the process resource budget.
    ProcessResourceBudget,
    /// Validates a service-context scope.
    ServiceContextScope,
    /// Reads service lifecycle environment configuration.
    ServiceLifecycleEnvironment,
    /// Parses a service lifecycle probe address.
    ServiceLifecycleProbeAddress,
    /// Parses a service lifecycle duration.
    ServiceLifecycleDuration,
    /// Validates a service lifecycle duration range.
    ServiceLifecycleDurationRange,
    /// Validates metadata I/O configuration.
    MetadataIoConfiguration,
    /// Validates metadata resource targeting.
    MetadataResourceTarget,
    /// Validates executor-service configuration.
    ExecutorServiceConfiguration,
    /// Uses a tiered-store runtime adapter.
    TieredStoreRuntime,
    /// Uses a tiered-store cleanup task group.
    CleanupTaskGroup,
    /// Uses a tiered-store dispatcher task group.
    DispatcherTaskGroup,
    /// Completes a runtime future exceptionally.
    CompletableFuture,
    /// Persists runtime metadata in a test.
    PersistRuntimeMetadata,
}

impl RuntimeOperation {
    const fn diagnostic_label(self) -> &'static str {
        match self {
            Self::BuildTokioRuntime => "build-tokio-runtime",
            Self::BuildFuturesThreadPool => "build-futures-thread-pool",
            Self::LoadConfigFile => "load-config-file",
            Self::DeserializeConfigFile => "deserialize-config-file",
            Self::DetectProcessMemoryLimit => "detect-process-memory-limit",
            Self::ReadFile => "read-file",
            Self::ReadFileBackup => "read-file-backup",
            Self::CheckFile => "check-file",
            Self::CopyFileBackup => "copy-file-backup",
            Self::CreateFileParent => "create-file-parent",
            Self::CreateFile => "create-file",
            Self::WriteFile => "write-file",
            Self::FlushFile => "flush-file",
            Self::PersistMetadata => "metadata-persistence",
            Self::MetadataCreateParent => "metadata-create-parent",
            Self::MetadataCreateTemporary => "metadata-create-temporary",
            Self::MetadataWriteTemporary => "metadata-write-temporary",
            Self::MetadataSyncTemporary => "metadata-sync-temporary",
            Self::MetadataReplaceTarget => "metadata-replace-target",
            Self::MetadataSyncParent => "metadata-sync-parent",
            Self::MetadataRemoveTemporary => "metadata-remove-temporary",
            Self::WaitForDurableMetadata => "wait-for-durable-metadata",
            Self::AdmitMetadataSnapshot => "admit-metadata-snapshot",
            Self::AdmitMetadataOperation => "admit-metadata-operation",
            Self::AdmitMetadataBytes => "admit-metadata-bytes",
            Self::MetadataWorkerStopped => "metadata-worker-stopped",
            Self::MetadataIoClosed => "metadata-io-closed",
            Self::ResolveLocalIp => "resolve-local-ip",
            Self::RegisterSigtermHandler => "register-sigterm-handler",
            Self::RegisterSigintHandler => "register-sigint-handler",
            Self::WaitForSignal => "wait-for-signal",
            Self::CreateTaskGroupChild => "create-task-group-child",
            Self::SpawnTaskGroupTask => "spawn-task-group-task",
            Self::SpawnServiceTask => "spawn-service-task",
            Self::SpawnSchedulerTask => "spawn-scheduler-task",
            Self::CreateExecutorTaskGroup => "create-executor-task-group",
            Self::SpawnExecutorTask => "spawn-executor-task",
            Self::RuntimeContext => "runtime-context",
            Self::StartServiceLifecycle => "start-service-lifecycle",
            Self::ServiceLifecycleTaskGroup => "service-lifecycle-task-group",
            Self::BindServiceHealthProbe => "bind-service-health-probe",
            Self::InspectServiceHealthProbe => "inspect-service-health-probe",
            Self::MarkServiceReady => "mark-service-ready",
            Self::SuspendServiceReadiness => "suspend-service-readiness",
            Self::RestoreServiceReadiness => "restore-service-readiness",
            Self::ShutdownRuntimeBlocking => "shutdown-runtime-blocking",
            Self::BlockingQueueAdmission => "blocking-queue-admission",
            Self::RunBlockingTask => "run-blocking-task",
            Self::BlockingTaskDeadline => "blocking-task-deadline",
            Self::BlockingTask => "blocking-task",
            Self::BlockingExecutorKind => "blocking-executor-kind",
            Self::ClearCompletedSchedules => "clear-completed-schedules",
            Self::RegisterScheduledTask => "register-scheduled-task",
            Self::SpawnOperation => "spawn-operation",
            Self::OperationOwner => "operation-owner",
            Self::TransportListener => "transport-listener",
            Self::SessionExecutor => "session-executor",
            Self::MetadataIo => "metadata-io",
            Self::DeferredResumeSessionExecutor => "deferred-resume-session-executor",
            Self::AuthMetadataIoLane => "auth-metadata-io-lane",
            Self::HaRuntime => "ha-runtime",
            Self::InitializeBroker => "initialize-broker",
            Self::StartBroker => "start-broker",
            Self::ShutdownBroker => "shutdown-broker",
            Self::KvMutationWorker => "kv-mutation-worker",
            Self::AdmitKvMutation => "admit-kv-mutation",
            Self::AdmitKvMutationBytes => "admit-kv-mutation-bytes",
            Self::KvPersistenceFault => "injected-kv-persistence-failure",
            Self::TestFailure => "test-runtime-failure",
            Self::ProcessResourceBudget => "process-resource-budget",
            Self::ServiceContextScope => "service-context-scope",
            Self::ServiceLifecycleEnvironment => "service-lifecycle-environment",
            Self::ServiceLifecycleProbeAddress => "service-lifecycle-probe-address",
            Self::ServiceLifecycleDuration => "service-lifecycle-duration",
            Self::ServiceLifecycleDurationRange => "service-lifecycle-duration-range",
            Self::MetadataIoConfiguration => "metadata-io-config",
            Self::MetadataResourceTarget => "metadata-resource-target",
            Self::ExecutorServiceConfiguration => "executor-service-config",
            Self::TieredStoreRuntime => "tieredstore-runtime",
            Self::CleanupTaskGroup => "cleanup-task-group",
            Self::DispatcherTaskGroup => "dispatcher-task-group",
            Self::CompletableFuture => "completable-future",
            Self::PersistRuntimeMetadata => "persist-runtime-metadata",
        }
    }
}

impl fmt::Display for RuntimeOperation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.diagnostic_label())
    }
}

/// A closed identifier for a deterministic runtime contract rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeContractPolicy {
    /// The configured maximum blocking threads is outside its supported range.
    MaxBlockingThreadsWithinSupportedRange,
    /// The configured worker-thread count must be positive.
    WorkerThreadsPositive,
    /// The configured maximum blocking-thread count must be positive.
    MaxBlockingThreadsPositive,
    /// The configured runtime thread name must not be blank.
    ThreadNameNotBlank,
    /// The configured runtime thread-stack size must be positive.
    ThreadStackSizePositive,
    /// The blocking lane concurrency must be positive.
    BlockingMaxConcurrencyPositive,
    /// The blocking lane queue depth must be positive.
    BlockingMaxQueueDepthPositive,
    /// The combined blocking timeout must be representable.
    BlockingTimeoutRepresentable,
    /// The global blocking capacity must cover every lane.
    BlockingGlobalCapacityCoversLanes,
    /// The futures executor pool size must be positive.
    FuturesExecutorPoolSizePositive,
    /// A service-context scope must not be blank.
    ServiceContextScopeNotBlank,
    /// An explicitly configured process memory limit must be positive.
    ConfiguredMemoryLimitPositive,
    /// A process-memory fraction must be positive and bounded.
    MemoryFractionPositiveAndBounded,
    /// A cron schedule expression must be valid.
    CronExpression,
    /// A schedule interval must be positive.
    IntervalMustBePositive,
    /// A delayed schedule interval must be positive.
    DelayedIntervalMustBePositive,
    /// The metadata I/O operation count must be positive.
    MetadataMaxPendingOperationsPositive,
    /// The metadata I/O byte capacity must be positive.
    MetadataMaxPendingBytesPositive,
    /// The metadata I/O blocking timeout must be positive.
    MetadataBlockingTaskTimeoutPositive,
}

impl RuntimeContractPolicy {
    const fn diagnostic_label(self) -> &'static str {
        match self {
            Self::MaxBlockingThreadsWithinSupportedRange => "max-blocking-threads-within-supported-range",
            Self::WorkerThreadsPositive => "worker-threads-positive",
            Self::MaxBlockingThreadsPositive => "max-blocking-threads-positive",
            Self::ThreadNameNotBlank => "thread-name-not-blank",
            Self::ThreadStackSizePositive => "thread-stack-size-positive",
            Self::BlockingMaxConcurrencyPositive => "blocking-max-concurrency-positive",
            Self::BlockingMaxQueueDepthPositive => "blocking-max-queue-depth-positive",
            Self::BlockingTimeoutRepresentable => "blocking-timeout-representable",
            Self::BlockingGlobalCapacityCoversLanes => "blocking-global-capacity-covers-lanes",
            Self::FuturesExecutorPoolSizePositive => "futures-executor-pool-size-positive",
            Self::ServiceContextScopeNotBlank => "service-context-scope-not-blank",
            Self::ConfiguredMemoryLimitPositive => "configured-limit-must-be-positive",
            Self::MemoryFractionPositiveAndBounded => "fraction-must-be-positive-and-bounded",
            Self::CronExpression => "cron-expression",
            Self::IntervalMustBePositive => "interval-must-be-positive",
            Self::DelayedIntervalMustBePositive => "delayed-interval-must-be-positive",
            Self::MetadataMaxPendingOperationsPositive => "max-pending-operations-positive",
            Self::MetadataMaxPendingBytesPositive => "max-pending-bytes-positive",
            Self::MetadataBlockingTaskTimeoutPositive => "blocking-task-timeout-positive",
        }
    }
}

impl fmt::Display for RuntimeContractPolicy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.diagnostic_label())
    }
}

/// A deterministic caller configuration or invariant violation.
///
/// This type deliberately has no operational source or retry metadata. Its
/// fields identify only bounded policy and invariant names; callers must not
/// use it to carry configuration contents.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RuntimeContractViolation {
    /// A runtime configuration field violated its documented invariant.
    #[error("runtime configuration violates {policy}")]
    InvalidConfiguration {
        /// The closed policy identifier.
        policy: RuntimeContractPolicy,
    },
    /// A resource-budget name is blank.
    #[error("resource budget name must not be blank")]
    EmptyBudgetName,
    /// A resource-budget name is structurally invalid.
    #[error("resource budget name must not contain '/'")]
    InvalidBudgetName,
    /// A budget capacity is zero.
    #[error("resource budget capacity is zero")]
    ZeroBudgetCapacity {
        /// The affected budget dimension.
        dimension: BudgetDimension,
    },
    /// A configured rate has a zero limit or burst.
    #[error("resource budget rate must be positive")]
    ZeroBudgetRate,
    /// A configured budget maximum age is zero.
    #[error("resource budget maximum age must be positive")]
    ZeroBudgetMaxAge,
    /// A reserve exceeds its own capacity.
    #[error("resource budget reserve exceeds capacity")]
    ReserveExceedsBudgetCapacity {
        /// The affected budget dimension.
        dimension: BudgetDimension,
    },
    /// A reserve was configured without the corresponding capacity.
    #[error("resource budget reserve requires capacity")]
    ReserveWithoutBudgetCapacity {
        /// The affected budget dimension.
        dimension: BudgetDimension,
    },
    /// A child limit exceeds its parent limit.
    #[error("resource budget child limit exceeds parent")]
    ChildBudgetExceedsParent {
        /// The affected budget dimension.
        dimension: BudgetDimension,
    },
    /// A child maximum age exceeds its parent maximum age.
    #[error("resource budget child maximum age exceeds parent")]
    ChildBudgetMaxAgeExceedsParent,
    /// A permit was asked to move across unrelated budget trees.
    #[error("resource permit target belongs to a different budget tree")]
    PermitTargetInDifferentTree,
    /// A memory limit input is not a positive finite value.
    #[error("process memory limit violates {policy}")]
    InvalidMemoryLimit {
        /// The closed memory-limit policy identifier.
        policy: RuntimeContractPolicy,
    },
    /// A scheduler definition is invalid.
    #[error("schedule definition violates {policy}")]
    InvalidSchedule {
        /// The closed schedule policy identifier.
        policy: RuntimeContractPolicy,
    },
    /// Metadata actor configuration is invalid.
    #[error("metadata I/O configuration violates {policy}")]
    InvalidMetadataConfiguration {
        /// The closed metadata policy identifier.
        policy: RuntimeContractPolicy,
    },
}

impl RuntimeContractViolation {
    /// Returns a stable contract condition for this deterministic violation.
    #[must_use]
    pub const fn condition(&self) -> CanonicalCondition {
        CanonicalCondition::InvalidArgument
    }
}

/// Stable, catalog-backed operational runtime failure.
///
/// The facade exposes descriptor identity, closed operation/component labels,
/// a bounded context and an optional typed source. It intentionally does not
/// expose implementation variants for callers to match.
#[derive(Clone)]
pub struct RuntimeError {
    error: SharedError,
    operation: RuntimeOperation,
    component: Arc<str>,
}

impl RuntimeError {
    /// Creates a source-free operational failure.
    #[must_use]
    #[track_caller]
    fn new(descriptor: &'static ErrorDescriptor, operation: RuntimeOperation, component: impl Into<Arc<str>>) -> Self {
        let error = CanonicalError::new(descriptor).with_context(runtime_context(operation, false));
        Self {
            error: Arc::new(error),
            operation,
            component: component.into(),
        }
    }

    /// Creates an operational failure while retaining its typed cause.
    #[must_use]
    #[track_caller]
    fn caused_by(
        descriptor: &'static ErrorDescriptor,
        operation: RuntimeOperation,
        component: impl Into<Arc<str>>,
        source: impl Error + Send + Sync + 'static,
    ) -> Self {
        let error = CanonicalError::caused_by(descriptor, source).with_context(runtime_context(operation, true));
        Self {
            error: Arc::new(error),
            operation,
            component: component.into(),
        }
    }

    /// Returns the catalog descriptor.
    #[must_use]
    pub fn descriptor(&self) -> &'static ErrorDescriptor {
        self.error.descriptor()
    }

    /// Returns the stable catalog code.
    #[must_use]
    pub fn code(&self) -> ErrorCode {
        self.error.code()
    }

    /// Returns the descriptor-owned canonical condition.
    #[must_use]
    pub fn condition(&self) -> CanonicalCondition {
        self.error.condition()
    }

    /// Returns the descriptor-owned recovery hint.
    #[must_use]
    pub fn recovery_hint(&self) -> RecoveryHint {
        self.error.recovery_hint()
    }

    /// Returns the closed operation label.
    #[must_use]
    pub const fn operation(&self) -> RuntimeOperation {
        self.operation
    }

    /// Returns the closed runtime component label.
    #[must_use]
    pub fn component(&self) -> &str {
        &self.component
    }

    /// Returns bounded descriptor context.
    #[must_use]
    pub fn context(&self) -> &ErrorContext {
        self.error.context()
    }

    /// Returns the first-promotion caller location.
    #[must_use]
    pub fn location(&self) -> &'static Location<'static> {
        self.error.location()
    }

    /// Returns the catalog-controlled captured backtrace, when enabled.
    #[must_use]
    pub fn backtrace(&self) -> Option<&Backtrace> {
        self.error.backtrace()
    }

    /// Creates a safe public projection when the context matches the descriptor.
    ///
    /// # Errors
    ///
    /// Returns an error only if an internal descriptor/context invariant has
    /// been violated.
    pub fn public_view(&self) -> Result<PublicErrorView<'_>, rocketmq_error::ViewContextViolation> {
        self.error.public_view()
    }

    /// Creates a controlled diagnostic projection when the context matches the descriptor.
    ///
    /// # Errors
    ///
    /// Returns an error only if an internal descriptor/context invariant has
    /// been violated.
    pub fn diagnostic_view(&self) -> Result<DiagnosticView<'_>, rocketmq_error::ViewContextViolation> {
        self.error.diagnostic_view()
    }

    /// Creates an external configuration failure.
    #[must_use]
    #[track_caller]
    pub fn configuration(operation: RuntimeOperation) -> Self {
        Self::new(
            &rocketmq_error::RUNTIME_CONFIGURATION_FAILED,
            operation,
            Arc::<str>::from("runtime"),
        )
    }

    /// Creates a configuration-loading failure while retaining its typed source.
    #[must_use]
    #[track_caller]
    pub fn configuration_failure(operation: RuntimeOperation, source: impl Error + Send + Sync + 'static) -> Self {
        Self::caused_by(
            &rocketmq_error::RUNTIME_CONFIGURATION_FAILED,
            operation,
            Arc::<str>::from("runtime"),
            source,
        )
    }

    /// Creates a runtime-build failure while retaining the build source.
    #[must_use]
    #[track_caller]
    pub fn build(operation: RuntimeOperation, source: std::io::Error) -> Self {
        Self::caused_by(
            &rocketmq_error::RUNTIME_BUILD_FAILED,
            operation,
            Arc::<str>::from("runtime"),
            source,
        )
    }

    /// Creates an I/O failure while retaining the I/O source.
    #[must_use]
    #[track_caller]
    pub fn io(operation: RuntimeOperation, source: std::io::Error) -> Self {
        Self::caused_by(
            &rocketmq_error::RUNTIME_IO_FAILED,
            operation,
            Arc::<str>::from("runtime"),
            source,
        )
    }

    /// Creates a runtime-context-unavailable failure.
    #[must_use]
    #[track_caller]
    pub fn context_unavailable(operation: RuntimeOperation) -> Self {
        Self::new(
            &rocketmq_error::RUNTIME_CONTEXT_UNAVAILABLE,
            operation,
            Arc::<str>::from("runtime"),
        )
    }

    /// Creates an operational capacity-exhausted failure.
    #[must_use]
    #[track_caller]
    pub fn capacity(operation: RuntimeOperation) -> Self {
        Self::new(
            &rocketmq_error::RUNTIME_CAPACITY_EXHAUSTED,
            operation,
            Arc::<str>::from("runtime"),
        )
    }

    /// Creates an operation timeout failure.
    #[must_use]
    #[track_caller]
    pub fn timed_out(operation: RuntimeOperation) -> Self {
        Self::new(
            &rocketmq_error::RUNTIME_OPERATION_TIMED_OUT,
            operation,
            Arc::<str>::from("runtime"),
        )
    }

    /// Creates an unsupported-operation failure.
    #[must_use]
    #[track_caller]
    pub fn unsupported(operation: RuntimeOperation) -> Self {
        Self::new(
            &rocketmq_error::RUNTIME_OPERATION_UNSUPPORTED,
            operation,
            Arc::<str>::from("runtime"),
        )
    }

    /// Creates a task join failure while retaining the join source.
    #[must_use]
    #[track_caller]
    pub fn join(operation: RuntimeOperation, source: tokio::task::JoinError) -> Self {
        Self::caused_by(
            &rocketmq_error::RUNTIME_TASK_JOIN_FAILED,
            operation,
            Arc::<str>::from("task"),
            source,
        )
    }

    /// Creates an internal runtime failure while retaining a typed source.
    #[must_use]
    #[track_caller]
    pub fn internal(operation: RuntimeOperation, source: impl Error + Send + Sync + 'static) -> Self {
        Self::caused_by(
            &rocketmq_error::RUNTIME_INTERNAL_FAILURE,
            operation,
            Arc::<str>::from("runtime"),
            source,
        )
    }

    /// Creates a source-free internal runtime failure.
    #[must_use]
    #[track_caller]
    pub fn internal_failure(operation: RuntimeOperation) -> Self {
        Self::new(
            &rocketmq_error::RUNTIME_INTERNAL_FAILURE,
            operation,
            Arc::<str>::from("runtime"),
        )
    }
}

fn runtime_context(operation: RuntimeOperation, source_present: bool) -> ErrorContext {
    let context = ErrorContext::new().with_text(
        rocketmq_error::fields::OPERATION_DIAGNOSTIC,
        operation.diagnostic_label(),
    );
    if source_present {
        context.with_secret_presence(rocketmq_error::fields::SOURCE_PRESENT)
    } else {
        context
    }
}

impl fmt::Display for RuntimeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self.error.as_ref(), formatter)
    }
}

impl fmt::Debug for RuntimeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RuntimeError")
            .field("code", &self.code())
            .field("condition", &self.condition())
            .field("operation", &self.operation)
            .field("component", &self.component)
            .field("has_source", &self.error.source().is_some())
            .finish()
    }
}

impl Error for RuntimeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.error.source()
    }
}

impl From<std::io::Error> for RuntimeError {
    #[track_caller]
    fn from(source: std::io::Error) -> Self {
        Self::io(RuntimeOperation::ReadFile, source)
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error as _;
    use std::io;

    use rocketmq_error::CanonicalCondition;
    use rocketmq_error::RecoveryHint;

    use super::*;

    #[test]
    fn operational_runtime_error_preserves_source_and_catalog_metadata_without_rendering_it() {
        const SENTINEL: &str = "runtime-source-secret";
        let error = RuntimeError::io(RuntimeOperation::PersistRuntimeMetadata, io::Error::other(SENTINEL));

        assert_eq!(error.code(), rocketmq_error::RUNTIME_IO_FAILED.code());
        assert_eq!(error.condition(), CanonicalCondition::Internal);
        assert_eq!(error.recovery_hint(), RecoveryHint::OperatorAction);
        assert_eq!(error.operation(), RuntimeOperation::PersistRuntimeMetadata);
        assert_eq!(error.component(), "runtime");
        assert!(error
            .source()
            .and_then(|source| source.downcast_ref::<io::Error>())
            .is_some());
        assert!(!error.to_string().contains(SENTINEL));
        assert!(!format!("{error:?}").contains(SENTINEL));
        assert!(error.public_view().is_ok());
        assert!(error.diagnostic_view().is_ok());
    }

    #[test]
    fn runtime_configuration_facade_uses_corrected_boundary_projection() {
        let error = RuntimeError::configuration(RuntimeOperation::LoadConfigFile);
        let public = error.public_view().expect("valid runtime public view");
        let projection = public.projection();

        assert_eq!("runtime.configuration.failed", public.code().as_str());
        assert_eq!(rocketmq_error::HttpStatusCode::BAD_REQUEST, projection.http().status);
        assert_eq!(rocketmq_error::CliExitCode::CONFIG, projection.cli().exit_code);
    }

    #[test]
    fn runtime_clone_shares_source_location_and_backtrace() {
        let caller_line = line!() + 1;
        let error = RuntimeError::internal(RuntimeOperation::PersistRuntimeMetadata, io::Error::other("typed leaf"));
        let cloned = error.clone();

        assert!(Arc::ptr_eq(&error.error, &cloned.error));
        assert!(std::ptr::eq(
            error.source().expect("runtime source"),
            cloned.source().expect("cloned runtime source")
        ));
        assert!(error
            .source()
            .and_then(|source| source.downcast_ref::<io::Error>())
            .is_some());
        assert_eq!(error.location().file(), file!());
        assert_eq!(error.location().line(), caller_line);

        match (error.backtrace(), cloned.backtrace()) {
            (Some(left), Some(right)) => assert!(std::ptr::eq(left, right)),
            (None, None) => {}
            _ => panic!("a clone must share the canonical backtrace state"),
        }
    }

    #[test]
    fn operation_debug_output_is_closed_for_sentinel_control_and_unbounded_text() {
        let error = RuntimeError::internal_failure(RuntimeOperation::TestFailure);
        let debug = format!("{error:?}");
        let control = "\u{0000}\u{001b}[31m";
        let unbounded = "unbounded-operation-".repeat(4096);

        assert!(debug.contains("TestFailure"));
        assert!(!debug.contains("runtime-source-secret"));
        assert!(!debug.contains("operation_diagnostic"));
        assert!(!debug.contains(control));
        assert!(!debug.contains(&unbounded));

        let constructor: fn(RuntimeOperation) -> RuntimeError = RuntimeError::internal_failure;
        assert_eq!(
            constructor(RuntimeOperation::TestFailure).operation(),
            RuntimeOperation::TestFailure
        );
    }

    #[test]
    fn contract_violations_stay_outside_the_operational_runtime_error_channel() {
        let violation = RuntimeContractViolation::PermitTargetInDifferentTree;

        assert_eq!(violation.condition(), CanonicalCondition::InvalidArgument);
        assert_eq!(
            violation.to_string(),
            "resource permit target belongs to a different budget tree"
        );

        let closed_policy = RuntimeContractViolation::InvalidConfiguration {
            policy: RuntimeContractPolicy::WorkerThreadsPositive,
        };
        assert_eq!(
            closed_policy.to_string(),
            "runtime configuration violates worker-threads-positive"
        );
    }
}
