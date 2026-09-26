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

//! Bounded, generation-aware metadata persistence.
//!
//! [`MetadataIoActor`] is the sole owner of admitted metadata snapshots. It
//! coalesces queued generations for the same logical resource, executes
//! synchronous filesystem work on a dedicated [`BlockingExecutor`] lane, and
//! advances the durable generation only after the atomic replacement protocol
//! completes.
//!
//! Acceptance reserves ownership and bytes; it is not a durability result.
//! An observation timeout stops waiting, not an already-started filesystem
//! write. Unconfirmed writes retain ordering and reconciliation obligations.
//! Target ownership is process-local and does not replace an application's
//! cross-process locking or format-specific recovery protocol.

mod filesystem;

use std::collections::HashMap;
use std::collections::VecDeque;
use std::fmt;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Weak;
use std::time::Duration;

use parking_lot::Mutex;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use crate::metadata_target::MetadataTargetIdentity;
use crate::metadata_target::MetadataTargetRegistration;
use crate::metadata_target::MetadataTargetRegistrationOutcome;
use crate::metadata_target::MetadataTargetRegistry;
use crate::metadata_target::MetadataTargetRetirementOutcome;
use crate::shutdown_deadline::ShutdownDeadline;
use crate::BlockingExecutor;
use crate::BlockingPoolPolicy;
use crate::ChildServiceContext;
use crate::RuntimeContractPolicy;
use crate::RuntimeContractViolation;
use crate::RuntimeError;
use crate::RuntimeResult;

/// An immutable absolute deadline for metadata admission and durability waits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetadataDeadline {
    at: tokio::time::Instant,
}

impl MetadataDeadline {
    /// Freezes a relative timeout into one absolute Tokio deadline.
    #[must_use]
    pub fn after(timeout: Duration) -> Self {
        let now = tokio::time::Instant::now();
        Self {
            at: now.checked_add(timeout).unwrap_or(now),
        }
    }

    /// Uses an existing absolute Tokio deadline.
    #[must_use]
    pub const fn at(at: tokio::time::Instant) -> Self {
        Self { at }
    }

    /// Returns the immutable absolute expiry instant.
    #[must_use]
    pub const fn instant(self) -> tokio::time::Instant {
        self.at
    }

    /// Returns the remaining budget without extending the deadline.
    #[must_use]
    pub fn remaining(self) -> Duration {
        self.at.saturating_duration_since(tokio::time::Instant::now())
    }

    /// Returns whether the absolute deadline has elapsed.
    #[must_use]
    pub fn is_expired(self) -> bool {
        tokio::time::Instant::now() >= self.at
    }
}

/// A monotonically increasing generation for one metadata resource.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MetadataGeneration(u64);

impl MetadataGeneration {
    /// Creates a generation identifier.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the underlying generation value.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl From<u64> for MetadataGeneration {
    fn from(value: u64) -> Self {
        Self::new(value)
    }
}

/// The caller-visible completion boundary for a persistence request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataDurability {
    /// The immutable snapshot has been accepted by the bounded actor.
    Accepted,
    /// The snapshot generation, or a newer coalesced generation, is durable.
    Durable(MetadataGeneration),
}

/// A filesystem step in the atomic replacement protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataIoOperation {
    /// Represents the create parent case.
    CreateParent,
    /// Represents the create temporary case.
    CreateTemporary,
    /// Represents the write temporary case.
    WriteTemporary,
    /// Represents the sync temporary case.
    SyncTemporary,
    /// Represents the replace target case.
    ReplaceTarget,
    /// Represents the sync parent case.
    SyncParent,
    /// Represents the remove temporary case.
    RemoveTemporary,
}

impl fmt::Display for MetadataIoOperation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            Self::CreateParent => "create parent directory",
            Self::CreateTemporary => "create temporary file",
            Self::WriteTemporary => "write temporary file",
            Self::SyncTemporary => "sync temporary file",
            Self::ReplaceTarget => "replace target file",
            Self::SyncParent => "sync parent directory",
            Self::RemoveTemporary => "remove temporary file",
        };
        formatter.write_str(value)
    }
}

impl MetadataIoOperation {
    /// Returns the closed runtime diagnostic operation for this filesystem step.
    #[must_use]
    pub const fn runtime_operation(self) -> crate::RuntimeOperation {
        match self {
            Self::CreateParent => crate::RuntimeOperation::MetadataCreateParent,
            Self::CreateTemporary => crate::RuntimeOperation::MetadataCreateTemporary,
            Self::WriteTemporary => crate::RuntimeOperation::MetadataWriteTemporary,
            Self::SyncTemporary => crate::RuntimeOperation::MetadataSyncTemporary,
            Self::ReplaceTarget => crate::RuntimeOperation::MetadataReplaceTarget,
            Self::SyncParent => crate::RuntimeOperation::MetadataSyncParent,
            Self::RemoveTemporary => crate::RuntimeOperation::MetadataRemoveTemporary,
        }
    }
}

fn metadata_io_failure(operation: MetadataIoOperation, _path: &Path, source: std::io::Error) -> RuntimeError {
    RuntimeError::io(operation.runtime_operation(), source)
}

/// An immutable write request accepted by [`MetadataIoActor`].
#[derive(Debug, Clone)]
pub struct MetadataWriteRequest {
    resource: Arc<str>,
    generation: MetadataGeneration,
    target: Arc<Path>,
    bytes: Arc<[u8]>,
    lane_deadline: Option<MetadataDeadline>,
}

impl MetadataWriteRequest {
    /// Creates an immutable resource snapshot.
    #[must_use]
    pub fn new(
        resource: impl Into<Arc<str>>,
        generation: impl Into<MetadataGeneration>,
        target: impl Into<PathBuf>,
        bytes: impl Into<Vec<u8>>,
    ) -> Self {
        Self {
            resource: resource.into(),
            generation: generation.into(),
            target: Arc::from(target.into()),
            bytes: Arc::from(bytes.into()),
            lane_deadline: None,
        }
    }

    /// Bounds this request against the shared blocking lane.
    ///
    /// The value is combined with the lane's phase budgets, so it can only
    /// tighten them and can never widen a shared lane limit. It is deliberately
    /// separate from the durability deadline passed to
    /// [`MetadataIoActor::submit`]: an admitted write keeps its ordering and
    /// byte charge when the caller stops observing it, while a request whose
    /// lane deadline has already elapsed is refused instead of started.
    #[must_use]
    pub fn with_lane_deadline(mut self, deadline: MetadataDeadline) -> Self {
        self.lane_deadline = Some(deadline);
        self
    }

    /// Returns the optional request-level lane deadline.
    #[must_use]
    pub const fn lane_deadline(&self) -> Option<MetadataDeadline> {
        self.lane_deadline
    }

    /// Returns the logical resource identifier.
    #[must_use]
    pub fn resource(&self) -> &str {
        &self.resource
    }

    /// Returns the immutable generation.
    #[must_use]
    pub const fn generation(&self) -> MetadataGeneration {
        self.generation
    }

    /// Returns the target path.
    #[must_use]
    pub fn target(&self) -> &Path {
        &self.target
    }

    /// Returns the retained snapshot size.
    #[must_use]
    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    /// Returns whether the immutable snapshot is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
}

/// Filesystem abstraction used by the metadata actor.
///
/// Implementations must not publish partial target files. Production callers
/// should use [`LocalMetadataFileSystem`]; the abstraction also permits
/// deterministic fault injection without relying on a particular host
/// filesystem.
pub trait MetadataFileSystem: fmt::Debug + Send + Sync + 'static {
    /// Atomically persists one immutable snapshot.
    ///
    /// # Errors
    ///
    /// Returns a typed error that identifies the failed durability step and
    /// retains the original I/O source.
    fn persist_atomic(&self, target: &Path, bytes: &[u8]) -> RuntimeResult<()>;
}

/// The production local filesystem implementation.
#[derive(Debug, Default)]
pub struct LocalMetadataFileSystem;

impl MetadataFileSystem for LocalMetadataFileSystem {
    fn persist_atomic(&self, target: &Path, bytes: &[u8]) -> RuntimeResult<()> {
        filesystem::persist_atomic_local(target, bytes)
    }
}

/// Metadata actor admission limits and legacy blocking-lane fields.
///
/// The actor owns its admission bounds, `max_pending_operations` and
/// `max_pending_bytes`. The shared blocking lane policy owns lane capacity,
/// phase timeouts, and the warn threshold, so the three `blocking_*` fields are
/// accepted for source compatibility and never override the lane.
/// [`MetadataIoActor::effective_profile`] reports the value that applies to each
/// one.
#[derive(Debug, Clone)]
pub struct MetadataIoConfig {
    /// The max pending operations value.
    pub max_pending_operations: usize,
    /// The max pending size in bytes.
    pub max_pending_bytes: usize,
    /// Legacy lane queue wait, retained for source compatibility.
    ///
    /// The shared blocking lane policy owns the queue wait in force, so this
    /// value does not change admission. Use
    /// [`MetadataWriteRequest::with_lane_deadline`] to tighten a single request.
    pub blocking_queue_timeout: Duration,
    /// Legacy lane execution budget, retained for source compatibility.
    ///
    /// The shared blocking lane policy owns the execution budget in force, so
    /// this value does not bound a running write. It is still validated for
    /// compatibility with existing configuration literals.
    pub blocking_task_timeout: Duration,
    /// Legacy lane warn threshold, retained for source compatibility.
    ///
    /// The warn threshold belongs to the shared blocking lane policy, so this
    /// value is never emitted.
    pub blocking_warn_after: Duration,
}

/// Metadata actor settings that passed deterministic admission validation.
#[derive(Debug, Clone)]
pub struct MetadataIoPlan {
    config: MetadataIoConfig,
}

impl MetadataIoConfig {
    /// Validates bounded metadata actor configuration before startup.
    ///
    /// The zero `blocking_task_timeout` check is retained for compatibility with
    /// existing configuration literals, even though the field does not drive
    /// execution.
    ///
    /// # Errors
    ///
    /// Returns a deterministic contract violation when an admission bound or
    /// blocking duration is invalid. This method performs no I/O.
    pub fn validate(&self) -> Result<(), RuntimeContractViolation> {
        if self.max_pending_operations == 0 {
            return Err(RuntimeContractViolation::InvalidMetadataConfiguration {
                policy: RuntimeContractPolicy::MetadataMaxPendingOperationsPositive,
            });
        }
        if self.max_pending_bytes == 0 {
            return Err(RuntimeContractViolation::InvalidMetadataConfiguration {
                policy: RuntimeContractPolicy::MetadataMaxPendingBytesPositive,
            });
        }
        if self.blocking_task_timeout.is_zero() {
            return Err(RuntimeContractViolation::InvalidMetadataConfiguration {
                policy: RuntimeContractPolicy::MetadataBlockingTaskTimeoutPositive,
            });
        }
        Ok(())
    }

    /// Creates a validated metadata actor startup plan.
    ///
    /// # Errors
    ///
    /// Returns a deterministic contract violation when an admission bound or
    /// blocking duration is invalid.
    pub fn into_plan(self) -> Result<MetadataIoPlan, RuntimeContractViolation> {
        self.validate()?;
        Ok(MetadataIoPlan { config: self })
    }
}

impl MetadataIoPlan {
    /// Starts the validated actor with the production filesystem.
    ///
    /// # Errors
    ///
    /// Returns an operational lifecycle error when the owned coordinator
    /// cannot start.
    pub fn start(self, service_context: &ChildServiceContext) -> RuntimeResult<MetadataIoActor> {
        MetadataIoActor::start_validated(service_context, self.config, Arc::new(LocalMetadataFileSystem))
    }

    /// Starts the validated actor with an injected filesystem implementation.
    ///
    /// # Errors
    ///
    /// Returns an operational lifecycle error when the owned coordinator
    /// cannot start.
    pub fn start_with_file_system(
        self,
        service_context: &ChildServiceContext,
        file_system: Arc<dyn MetadataFileSystem>,
    ) -> RuntimeResult<MetadataIoActor> {
        MetadataIoActor::start_validated(service_context, self.config, file_system)
    }
}

impl Default for MetadataIoConfig {
    fn default() -> Self {
        Self {
            max_pending_operations: 1_024,
            max_pending_bytes: 64 * 1024 * 1024,
            blocking_queue_timeout: Duration::from_secs(5),
            blocking_task_timeout: Duration::from_secs(30),
            blocking_warn_after: Duration::from_secs(1),
        }
    }
}

/// Identifies the owner of one effective metadata limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataLimitSource {
    /// The shared root blocking lane policy owns the value.
    BlockingLanePolicy,
    /// The metadata actor configuration owns the value.
    ActorConfiguration,
}

/// One retained actor field together with the value that applies instead.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataIoLegacyBlockingField {
    /// The configuration field name.
    pub field: &'static str,
    /// The value a caller configured.
    pub configured: Duration,
    /// The value that drives execution.
    pub effective: Duration,
    /// The owner of the effective value.
    pub effective_source: MetadataLimitSource,
}

/// The shared blocking lane limits in force for the metadata actor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataIoBlockingLaneProfile {
    /// The lane name from the root blocking lane policy.
    pub name: String,
    /// The admitted concurrency ceiling.
    pub max_concurrency: usize,
    /// The admitted queue depth ceiling.
    pub max_queue_depth: usize,
    /// The queue wait budget.
    pub queue_timeout: Duration,
    /// The execution budget.
    pub task_timeout: Duration,
    /// The warn threshold for work that completed above it.
    pub warn_after: Duration,
}

/// The admission limits the metadata actor owns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataIoActorLimitsProfile {
    /// The admitted pending operation ceiling.
    pub max_pending_operations: usize,
    /// The admitted retained byte ceiling.
    pub max_pending_bytes: usize,
    /// The waiter ceiling derived from `max_pending_operations`.
    pub max_waiters: usize,
}

/// A read-only view of the metadata I/O limits actually in force.
///
/// `blocking_lane` carries the shared lane limits owned by the root
/// [`BlockingLanePolicies`](crate::BlockingLanePolicies), `actor_limits` carries
/// the admission bounds the actor owns, and `legacy_blocking_fields` reports
/// each retained configuration field beside the value that applies instead.
///
/// A request may tighten the lane phase budgets with
/// [`MetadataWriteRequest::with_lane_deadline`]. The executor combines the
/// request deadline with the lane budget by taking the earlier expiry, so no
/// request can widen a shared lane limit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataIoEffectiveProfile {
    /// The shared blocking lane limits in force.
    pub blocking_lane: MetadataIoBlockingLaneProfile,
    /// The admission limits owned by the actor.
    pub actor_limits: MetadataIoActorLimitsProfile,
    /// The retained configuration fields that do not override the lane.
    pub legacy_blocking_fields: [MetadataIoLegacyBlockingField; 3],
}

/// The waiter ceiling derived from the admitted pending-operation bound.
fn max_metadata_waiters(max_pending_operations: usize) -> usize {
    max_pending_operations.saturating_mul(4).max(1)
}

fn effective_profile(config: &MetadataIoConfig, blocking_policy: &BlockingPoolPolicy) -> MetadataIoEffectiveProfile {
    let legacy_blocking_field =
        |field: &'static str, configured: Duration, effective: Duration| MetadataIoLegacyBlockingField {
            field,
            configured,
            effective,
            effective_source: MetadataLimitSource::BlockingLanePolicy,
        };
    MetadataIoEffectiveProfile {
        blocking_lane: MetadataIoBlockingLaneProfile {
            name: blocking_policy.name.clone(),
            max_concurrency: blocking_policy.max_concurrency,
            max_queue_depth: blocking_policy.max_queue_depth,
            queue_timeout: blocking_policy.queue_timeout,
            task_timeout: blocking_policy.task_timeout,
            warn_after: blocking_policy.warn_after,
        },
        actor_limits: MetadataIoActorLimitsProfile {
            max_pending_operations: config.max_pending_operations,
            max_pending_bytes: config.max_pending_bytes,
            max_waiters: max_metadata_waiters(config.max_pending_operations),
        },
        legacy_blocking_fields: [
            legacy_blocking_field(
                "blocking_queue_timeout",
                config.blocking_queue_timeout,
                blocking_policy.queue_timeout,
            ),
            legacy_blocking_field(
                "blocking_task_timeout",
                config.blocking_task_timeout,
                blocking_policy.task_timeout,
            ),
            legacy_blocking_field(
                "blocking_warn_after",
                config.blocking_warn_after,
                blocking_policy.warn_after,
            ),
        ],
    }
}

/// Reports retained blocking fields that a caller configured away from their
/// defaults.
///
/// The notice is emitted once per actor start, so a non-default value cannot
/// turn into per-operation logging. The lane policy always owns the value in
/// force, which [`MetadataIoEffectiveProfile`] reports for every field.
fn warn_legacy_blocking_fields(config: &MetadataIoConfig, blocking_policy: &BlockingPoolPolicy) {
    let defaults = MetadataIoConfig::default();
    let changed = [
        (
            "blocking_queue_timeout",
            config.blocking_queue_timeout,
            defaults.blocking_queue_timeout,
        ),
        (
            "blocking_task_timeout",
            config.blocking_task_timeout,
            defaults.blocking_task_timeout,
        ),
        (
            "blocking_warn_after",
            config.blocking_warn_after,
            defaults.blocking_warn_after,
        ),
    ]
    .into_iter()
    .filter(|(_field, configured, default)| configured != default)
    .map(|(field, _configured, _default)| field)
    .collect::<Vec<_>>();
    if changed.is_empty() {
        return;
    }
    tracing::warn!(
        fields = ?changed,
        lane = %blocking_policy.name,
        "metadata blocking fields are retained for source compatibility and do not override the shared blocking lane policy"
    );
}

/// A snapshot of one logical metadata resource.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataIoResourceSnapshot {
    /// The resource value.
    pub resource: Arc<str>,
    /// The target value.
    pub target: Option<Arc<Path>>,
    /// The durable generation value.
    pub durable_generation: Option<MetadataGeneration>,
    /// The in flight generation value.
    pub in_flight_generation: Option<MetadataGeneration>,
    /// The queued generation value.
    pub queued_generation: Option<MetadataGeneration>,
    /// The number of waiter entries.
    pub waiter_count: usize,
}

/// A point-in-time actor snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataIoSnapshot {
    /// Whether accepting.
    pub accepting: bool,
    /// The pending operations value.
    pub pending_operations: usize,
    /// The pending size in bytes.
    pub pending_bytes: usize,
    /// The max pending operations value.
    pub max_pending_operations: usize,
    /// The max pending size in bytes.
    pub max_pending_bytes: usize,
    /// The resources value.
    pub resources: Vec<MetadataIoResourceSnapshot>,
}

/// Result of stopping admission and draining accepted work.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataIoShutdownReport {
    /// Whether timed out.
    pub timed_out: bool,
    /// The pending operations value.
    pub pending_operations: usize,
    /// The pending size in bytes.
    pub pending_bytes: usize,
    /// The unfinished value.
    pub unfinished: Vec<MetadataIoResourceSnapshot>,
}

/// A receipt for an accepted immutable snapshot.
#[derive(Debug)]
pub struct MetadataIoReceipt {
    generation: MetadataGeneration,
    identity: MetadataTargetIdentity,
    durable: oneshot::Receiver<RuntimeResult<MetadataIoCommitOutcome>>,
}

/// The normal admission result for one metadata write request.
///
/// A target conflict retains the original immutable request so the caller can
/// retry it after reconciling the resource-to-target mapping.
#[derive(Debug)]
pub enum MetadataIoAdmissionOutcome {
    /// The actor accepted the snapshot and returned its durability receipt.
    Accepted(MetadataIoReceipt),
    /// The resource already has pending work for a different target path.
    TargetConflict(MetadataWriteRequest),
}

/// The normal durable-completion result for one metadata write request.
#[derive(Debug)]
pub enum MetadataIoDurabilityOutcome {
    /// The accepted snapshot, or a coalesced newer snapshot, became durable.
    Durable(MetadataGeneration),
    /// The request was not admitted because its resource has a pending
    /// snapshot for a different target path.
    TargetConflict(MetadataWriteRequest),
}

/// The real persistence conclusion for an accepted metadata snapshot.
#[derive(Debug, Clone)]
pub enum MetadataIoCommitOutcome {
    /// The snapshot or a newer coalesced snapshot completed the persistence
    /// protocol and is durable.
    Durable(MetadataGeneration),
    /// The target file was not replaced.
    FailedBeforeCommit(RuntimeError),
    /// The target may have been replaced, but the durability protocol did not
    /// complete. The caller must reconcile before publishing or retrying.
    CommitOutcomeUnknown(RuntimeError),
}

/// Result of waiting for a committed metadata snapshot.
#[derive(Debug)]
pub enum MetadataIoCommitAdmissionOutcome {
    /// The accepted snapshot reached a terminal persistence conclusion.
    Completed(MetadataIoCommitOutcome),
    /// The request was not admitted because the resource or target has a
    /// different process-local writer binding.
    TargetConflict(MetadataWriteRequest),
}

/// What a caller actually confirmed about one admitted metadata snapshot.
///
/// Unlike [`MetadataIoCommitOutcome`], this models the case where the caller
/// stopped observing before the persistence protocol reached a terminal
/// conclusion. An unobserved generation is not evidence that the write did not
/// happen: the admitted snapshot keeps its ordering and byte charge, so it may
/// still become durable, fail, or end unconfirmed after the caller's deadline.
/// Business owners must treat that generation as unconfirmed rather than as a
/// definite failure.
///
/// Every conclusion except a target conflict carries the observed generation,
/// so a caller can record the change identity and later resolve it against
/// [`MetadataIoActor::confirmed_durable_generation`].
#[derive(Debug)]
pub enum MetadataIoCommitObservation {
    /// The persistence protocol reached a terminal conclusion within the
    /// caller's deadline.
    Settled {
        /// The generation the caller observed.
        generation: MetadataGeneration,
        /// The conclusion the actor delivered.
        outcome: MetadataIoCommitOutcome,
    },
    /// The caller stopped observing first. The generation identifies the
    /// change whose durability was not confirmed.
    Unobserved {
        /// The generation whose durability was not confirmed.
        generation: MetadataGeneration,
    },
    /// The request was not admitted because the resource is bound to a
    /// different process-local target.
    TargetConflict(MetadataWriteRequest),
}

impl MetadataIoCommitObservation {
    /// Returns whether the caller must treat durable state as unconfirmed.
    ///
    /// True for an unobserved generation and for a replacement that the
    /// durability protocol did not confirm. False for a confirmed durable
    /// write, a failure before target replacement, and a target conflict.
    #[must_use]
    pub fn requires_reconciliation(&self) -> bool {
        match self {
            MetadataIoCommitObservation::Settled { outcome, .. } => {
                matches!(outcome, MetadataIoCommitOutcome::CommitOutcomeUnknown(_))
            }
            MetadataIoCommitObservation::Unobserved { .. } => true,
            MetadataIoCommitObservation::TargetConflict(_) => false,
        }
    }

    /// Consumes the observation and returns the terminal conclusion, if any.
    #[must_use]
    pub fn settled(self) -> Option<MetadataIoCommitOutcome> {
        match self {
            MetadataIoCommitObservation::Settled { outcome, .. } => Some(outcome),
            MetadataIoCommitObservation::Unobserved { .. } | MetadataIoCommitObservation::TargetConflict(_) => None,
        }
    }

    /// Returns the observed generation, or `None` for a target conflict.
    #[must_use]
    pub const fn generation(&self) -> Option<MetadataGeneration> {
        match self {
            MetadataIoCommitObservation::Settled { generation, .. }
            | MetadataIoCommitObservation::Unobserved { generation } => Some(*generation),
            MetadataIoCommitObservation::TargetConflict(_) => None,
        }
    }

    /// Returns the generation whose durability was not confirmed.
    #[must_use]
    pub const fn unobserved_generation(&self) -> Option<MetadataGeneration> {
        match self {
            MetadataIoCommitObservation::Unobserved { generation } => Some(*generation),
            MetadataIoCommitObservation::Settled { .. } | MetadataIoCommitObservation::TargetConflict(_) => None,
        }
    }
}

/// The private receipt conclusion that keeps the coordinator-abort case
/// distinct from a deadline expiry.
///
/// [`MetadataIoReceipt::wait_until_outcome`] reports a stopped coordinator as
/// an unavailable context, while [`MetadataIoReceipt::observe_until`] folds it
/// into an unobserved generation. Both mappings are derived from this single
/// classification so the receipt channel is interpreted in exactly one place.
enum ReceiptConclusion {
    /// The receipt channel delivered the actor's result. That is either a
    /// terminal persistence conclusion or the worker-stopped error, which the
    /// actor only produces for a generation that never ran.
    Delivered(RuntimeResult<MetadataIoCommitOutcome>),
    /// The caller's absolute deadline elapsed first.
    Expired,
    /// The coordinator task was dropped without delivering a conclusion.
    /// A blocking closure it submitted may still be running.
    CoordinatorStopped,
}

impl MetadataIoReceipt {
    /// Returns the history identity to pair with this receipt's generation.
    ///
    /// A retired history cannot prove durability in a later target identity.
    pub fn target_identity(&self) -> MetadataTargetIdentity {
        self.identity.clone()
    }
    /// Returns the accepted resource generation.
    #[must_use]
    pub const fn generation(&self) -> MetadataGeneration {
        self.generation
    }

    /// Waits for this generation, or a newer coalesced generation, to become
    /// durable without extending the caller's absolute deadline.
    ///
    /// Expiry abandons this observation. Accepted writes retain their ordering
    /// and byte charge until actual completion, and may still become durable.
    ///
    /// # Errors
    ///
    /// Returns an operational runtime failure if persistence cannot complete.
    pub async fn wait_until_outcome(self, deadline: MetadataDeadline) -> RuntimeResult<MetadataIoCommitOutcome> {
        if deadline.is_expired() {
            return Err(RuntimeError::timed_out(crate::RuntimeOperation::WaitForDurableMetadata));
        }
        match self.conclude(deadline).await {
            ReceiptConclusion::Delivered(result) => result,
            ReceiptConclusion::CoordinatorStopped => {
                Err(RuntimeError::closed(crate::RuntimeOperation::MetadataWorkerStopped))
            }
            ReceiptConclusion::Expired => Err(RuntimeError::timed_out(crate::RuntimeOperation::WaitForDurableMetadata)),
        }
    }

    /// Observes this generation without collapsing an observation timeout into
    /// the persistence error channel.
    ///
    /// Expiry returns [`MetadataIoCommitObservation::Unobserved`] rather than
    /// an error, because the accepted snapshot retains its ordering and byte
    /// charge and may still become durable. A caller that needs a definite
    /// answer must reconcile against the actor's confirmed durable generation
    /// instead of treating the expiry as a failure.
    ///
    /// This method never returns [`MetadataIoCommitObservation::TargetConflict`]:
    /// a receipt already identifies an admitted write. It retains only the
    /// result and history identity, not write authority.
    pub async fn observe_until(self, deadline: MetadataDeadline) -> MetadataIoCommitObservation {
        let generation = self.generation;
        match self.conclude(deadline).await {
            ReceiptConclusion::Delivered(Ok(outcome)) => MetadataIoCommitObservation::Settled { generation, outcome },
            // The actor delivered the worker-stopped error, which it only
            // produces for a generation that never ran. That is a definite
            // pre-commit failure rather than an unconfirmed replacement.
            ReceiptConclusion::Delivered(Err(error)) => MetadataIoCommitObservation::Settled {
                generation,
                outcome: MetadataIoCommitOutcome::FailedBeforeCommit(error),
            },
            // A stopped coordinator and an elapsed deadline are both
            // "no conclusion observed": a submitted closure may still be
            // running, and the target may still change.
            ReceiptConclusion::CoordinatorStopped | ReceiptConclusion::Expired => {
                MetadataIoCommitObservation::Unobserved { generation }
            }
        }
    }

    /// Classifies the receipt channel exactly once for every caller.
    async fn conclude(self, deadline: MetadataDeadline) -> ReceiptConclusion {
        match tokio::time::timeout_at(deadline.instant(), self.durable).await {
            Ok(Ok(result)) => ReceiptConclusion::Delivered(result),
            Ok(Err(_closed)) => ReceiptConclusion::CoordinatorStopped,
            Err(_elapsed) => ReceiptConclusion::Expired,
        }
    }

    /// Waits for this generation, or a newer coalesced generation, to become
    /// durable without extending the caller's absolute deadline.
    ///
    /// Expiry abandons this observation. Accepted writes retain their ordering
    /// and byte charge until actual completion, and may still become durable.
    ///
    /// # Errors
    ///
    /// Returns an operational runtime failure if persistence cannot complete.
    pub async fn wait_until(self, deadline: MetadataDeadline) -> RuntimeResult<MetadataGeneration> {
        match self.wait_until_outcome(deadline).await? {
            MetadataIoCommitOutcome::Durable(generation) => Ok(generation),
            MetadataIoCommitOutcome::FailedBeforeCommit(source)
            | MetadataIoCommitOutcome::CommitOutcomeUnknown(source) => Err(source),
        }
    }
}

/// A cloneable handle to the bounded metadata I/O owner.
#[derive(Debug, Clone)]
pub struct MetadataIoActor {
    inner: Arc<ActorInner>,
    sender: mpsc::Sender<Arc<str>>,
}

/// Read-only access that does not retain metadata admission or target ownership.
#[derive(Debug, Clone)]
pub struct MetadataIoObserver {
    inner: Weak<ActorInner>,
}

impl MetadataIoObserver {
    /// Reads bounded actor state, or returns `None` after its owners are released.
    ///
    /// The registry capacity bounds the resource scan. This call performs no I/O.
    pub fn snapshot(&self) -> Option<MetadataIoSnapshot> {
        self.inner.upgrade().map(|inner| snapshot(&inner))
    }
}

#[derive(Debug)]
struct ActorInner {
    config: MetadataIoConfig,
    blocking_policy: BlockingPoolPolicy,
    targets: MetadataTargetRegistry,
    waiter_count: Arc<AtomicUsize>,
    state: Mutex<ActorState>,
    shutdown: Notify,
    worker_finished: Notify,
}

#[derive(Debug)]
struct ActorState {
    accepting: bool,
    worker_finished: bool,
    pending_operations: usize,
    pending_bytes: usize,
    resources: HashMap<Arc<str>, ResourceState>,
}

#[derive(Debug, Default)]
struct ResourceState {
    identity: Option<MetadataTargetIdentity>,
    target: Option<Arc<Path>>,
    target_registration: Option<MetadataTargetRegistration>,
    durable_generation: Option<MetadataGeneration>,
    in_flight: Option<WorkMeta>,
    queued: Option<QueuedMetadataWrite>,
    waiters: Vec<GenerationWaiter>,
}

#[derive(Debug)]
struct QueuedMetadataWrite {
    request: MetadataWriteRequest,
    registration: MetadataTargetRegistration,
}

#[derive(Debug, Clone)]
struct WorkMeta {
    generation: MetadataGeneration,
    bytes: usize,
}

#[derive(Debug)]
struct GenerationWaiter {
    generation: MetadataGeneration,
    sender: oneshot::Sender<RuntimeResult<MetadataIoCommitOutcome>>,
    _permit: WaiterPermit,
}

#[derive(Debug)]
struct WaiterPermit {
    waiter_count: Arc<AtomicUsize>,
}

impl Drop for WaiterPermit {
    fn drop(&mut self) {
        self.waiter_count.fetch_sub(1, Ordering::AcqRel);
    }
}

impl ActorInner {
    fn reserve_waiter(&self, resource_waiters: usize) -> RuntimeResult<WaiterPermit> {
        let max_waiters = max_metadata_waiters(self.config.max_pending_operations);
        if resource_waiters >= max_waiters {
            return Err(RuntimeError::capacity(crate::RuntimeOperation::AdmitMetadataOperation));
        }

        let mut current = self.waiter_count.load(Ordering::Acquire);
        loop {
            if current >= max_waiters {
                return Err(RuntimeError::capacity(crate::RuntimeOperation::AdmitMetadataOperation));
            }
            match self
                .waiter_count
                .compare_exchange_weak(current, current + 1, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => {
                    return Ok(WaiterPermit {
                        waiter_count: Arc::clone(&self.waiter_count),
                    });
                }
                Err(observed) => current = observed,
            }
        }
    }
}

impl MetadataIoActor {
    fn start_validated(
        service_context: &ChildServiceContext,
        config: MetadataIoConfig,
        file_system: Arc<dyn MetadataFileSystem>,
    ) -> RuntimeResult<Self> {
        let task_group = service_context.component("metadata-io").task_group().clone();
        let blocking = service_context.metadata_io().clone();
        let blocking_policy = blocking.policy().clone();
        warn_legacy_blocking_fields(&config, &blocking_policy);
        let targets = service_context.resources().metadata_targets();
        let (sender, receiver) = mpsc::channel(config.max_pending_operations);
        let inner = Arc::new(ActorInner {
            config,
            blocking_policy,
            targets,
            waiter_count: Arc::new(AtomicUsize::new(0)),
            state: Mutex::new(ActorState {
                accepting: true,
                worker_finished: false,
                pending_operations: 0,
                pending_bytes: 0,
                resources: HashMap::new(),
            }),
            shutdown: Notify::new(),
            worker_finished: Notify::new(),
        });
        let actor = Self {
            inner: inner.clone(),
            sender,
        };
        let cancellation = task_group.cancellation_token();
        task_group
            .spawn_service(
                "metadata-io.coordinator",
                run_actor(inner, receiver, blocking, file_system, cancellation),
            )
            .map_err(startup_error)?;
        Ok(actor)
    }

    /// Attempts to admit one immutable snapshot without waiting for disk durability.
    ///
    /// The call is deliberately non-blocking. Queue or byte saturation is
    /// returned immediately and the absolute deadline is checked before
    /// admission.
    ///
    /// Returns a normal [`MetadataIoAdmissionOutcome::TargetConflict`] when
    /// the same resource already has pending work for a different target.
    ///
    /// # Errors
    ///
    /// Returns a typed deadline, capacity, or lifecycle failure.
    pub fn submit(
        &self,
        request: MetadataWriteRequest,
        deadline: MetadataDeadline,
    ) -> RuntimeResult<MetadataIoAdmissionOutcome> {
        if deadline.is_expired() {
            return Err(RuntimeError::timed_out(crate::RuntimeOperation::AdmitMetadataSnapshot));
        }

        let resource = request.resource.clone();
        let generation = request.generation;
        let (waiter_sender, durable) = oneshot::channel();
        let mut state = self.inner.state.lock();
        if !state.accepting {
            return Err(RuntimeError::closed(crate::RuntimeOperation::MetadataIoClosed));
        }

        let Some(target_registration) = ensure_target_registration(&mut state, &self.inner.targets, &request)? else {
            return Ok(MetadataIoAdmissionOutcome::TargetConflict(request));
        };

        let existing = state.resources.get(&resource);
        if let Some(existing) = existing {
            if let Some(durable_generation) = existing.durable_generation {
                if generation <= durable_generation {
                    let _ = waiter_sender.send(Ok(MetadataIoCommitOutcome::Durable(durable_generation)));
                    return Ok(MetadataIoAdmissionOutcome::Accepted(MetadataIoReceipt {
                        generation,
                        identity: target_registration.identity(),
                        durable,
                    }));
                }
            }
            if let Some(in_flight) = &existing.in_flight {
                if generation <= in_flight.generation {
                    let waiter_permit = self.inner.reserve_waiter(existing.waiters.len())?;
                    let Some(resource_state) = state.resources.get_mut(&resource) else {
                        return Err(RuntimeError::closed(crate::RuntimeOperation::MetadataWorkerStopped));
                    };
                    resource_state.waiters.push(GenerationWaiter {
                        generation,
                        sender: waiter_sender,
                        _permit: waiter_permit,
                    });
                    return Ok(MetadataIoAdmissionOutcome::Accepted(MetadataIoReceipt {
                        generation,
                        identity: target_registration.identity(),
                        durable,
                    }));
                }
            }
            if let Some(queued) = &existing.queued {
                if generation <= queued.request.generation {
                    let waiter_permit = self.inner.reserve_waiter(existing.waiters.len())?;
                    let Some(resource_state) = state.resources.get_mut(&resource) else {
                        return Err(RuntimeError::closed(crate::RuntimeOperation::MetadataWorkerStopped));
                    };
                    resource_state.waiters.push(GenerationWaiter {
                        generation,
                        sender: waiter_sender,
                        _permit: waiter_permit,
                    });
                    return Ok(MetadataIoAdmissionOutcome::Accepted(MetadataIoReceipt {
                        generation,
                        identity: target_registration.identity(),
                        durable,
                    }));
                }
            }
        }

        let waiter_permit = self
            .inner
            .reserve_waiter(existing.map_or(0, |resource_state| resource_state.waiters.len()))?;
        let old_queued_bytes = existing
            .and_then(|resource_state| resource_state.queued.as_ref())
            .map_or(0, |queued| queued.request.len());
        let adds_operation = existing.is_none_or(|resource_state| resource_state.queued.is_none());
        let needs_queue_token =
            existing.is_none_or(|resource_state| resource_state.in_flight.is_none() && resource_state.queued.is_none());
        let next_operations = state.pending_operations + usize::from(adds_operation);
        if next_operations > self.inner.config.max_pending_operations {
            return Err(RuntimeError::capacity(crate::RuntimeOperation::AdmitMetadataOperation));
        }
        let retained_without_replaced = state.pending_bytes.saturating_sub(old_queued_bytes);
        let next_bytes = retained_without_replaced
            .checked_add(request.len())
            .ok_or_else(|| RuntimeError::capacity(crate::RuntimeOperation::AdmitMetadataBytes))?;
        if next_bytes > self.inner.config.max_pending_bytes {
            return Err(RuntimeError::capacity(crate::RuntimeOperation::AdmitMetadataBytes));
        }

        let queue_permit = if needs_queue_token {
            Some(
                self.sender
                    .try_reserve()
                    .map_err(|_error| RuntimeError::capacity(crate::RuntimeOperation::AdmitMetadataOperation))?,
            )
        } else {
            None
        };

        state.pending_operations = next_operations;
        state.pending_bytes = next_bytes;
        let resource_state = state.resources.entry(resource.clone()).or_default();
        let identity = target_registration.identity();
        resource_state.target = Some(request.target.clone());
        resource_state.target_registration = Some(target_registration.clone());
        resource_state.queued = Some(QueuedMetadataWrite {
            request,
            registration: target_registration,
        });
        resource_state.waiters.push(GenerationWaiter {
            generation,
            sender: waiter_sender,
            _permit: waiter_permit,
        });
        drop(state);
        if let Some(permit) = queue_permit {
            permit.send(resource.clone());
        }
        Ok(MetadataIoAdmissionOutcome::Accepted(MetadataIoReceipt {
            generation,
            identity,
            durable,
        }))
    }

    /// Assigns the next owner-lifetime generation and accepts an immutable
    /// snapshot without waiting for durability.
    ///
    /// Generation values are unique across every metadata actor that shares
    /// this runtime owner, so a generation stays a usable change identity
    /// after an actor is replaced. Gaps are allowed when admission rejects a
    /// request; they do not weaken ordering.
    ///
    /// # Errors
    ///
    /// Returns the same normal outcome and typed failures as [`Self::submit`].
    pub fn submit_next(
        &self,
        resource: impl Into<Arc<str>>,
        target: impl Into<PathBuf>,
        bytes: impl Into<Vec<u8>>,
        deadline: MetadataDeadline,
    ) -> RuntimeResult<MetadataIoAdmissionOutcome> {
        let generation = self.inner.targets.next_generation();
        self.submit(MetadataWriteRequest::new(resource, generation, target, bytes), deadline)
    }

    /// Accepts a snapshot and waits for durable completion using the same
    /// absolute deadline.
    ///
    /// Returns [`MetadataIoDurabilityOutcome::TargetConflict`] without
    /// starting I/O when a pending request owns a different target path.
    ///
    /// # Errors
    ///
    /// Returns a typed admission, persistence, worker, or deadline failure.
    pub async fn submit_durable(
        &self,
        request: MetadataWriteRequest,
        deadline: MetadataDeadline,
    ) -> RuntimeResult<MetadataIoDurabilityOutcome> {
        match self.submit(request, deadline)? {
            MetadataIoAdmissionOutcome::Accepted(receipt) => receipt
                .wait_until(deadline)
                .await
                .map(MetadataIoDurabilityOutcome::Durable),
            MetadataIoAdmissionOutcome::TargetConflict(request) => {
                Ok(MetadataIoDurabilityOutcome::TargetConflict(request))
            }
        }
    }

    /// Accepts a snapshot and returns its real persistence conclusion.
    ///
    /// This distinguishes a failure before target replacement from an
    /// unconfirmed replacement followed by a parent-directory sync failure.
    pub async fn submit_commit(
        &self,
        request: MetadataWriteRequest,
        deadline: MetadataDeadline,
    ) -> RuntimeResult<MetadataIoCommitAdmissionOutcome> {
        match self.submit(request, deadline)? {
            MetadataIoAdmissionOutcome::Accepted(receipt) => receipt
                .wait_until_outcome(deadline)
                .await
                .map(MetadataIoCommitAdmissionOutcome::Completed),
            MetadataIoAdmissionOutcome::TargetConflict(request) => {
                Ok(MetadataIoCommitAdmissionOutcome::TargetConflict(request))
            }
        }
    }

    /// Assigns the next process-lifetime generation, accepts the immutable
    /// snapshot, and waits for durable completion using one absolute deadline.
    ///
    /// # Errors
    ///
    /// Returns the same normal outcome and typed failures as
    /// [`Self::submit_durable`].
    pub async fn submit_next_durable(
        &self,
        resource: impl Into<Arc<str>>,
        target: impl Into<PathBuf>,
        bytes: impl Into<Vec<u8>>,
        deadline: MetadataDeadline,
    ) -> RuntimeResult<MetadataIoDurabilityOutcome> {
        match self.submit_next(resource, target, bytes, deadline)? {
            MetadataIoAdmissionOutcome::Accepted(receipt) => receipt
                .wait_until(deadline)
                .await
                .map(MetadataIoDurabilityOutcome::Durable),
            MetadataIoAdmissionOutcome::TargetConflict(request) => {
                Ok(MetadataIoDurabilityOutcome::TargetConflict(request))
            }
        }
    }

    /// Assigns the next owner-lifetime generation and returns its real
    /// persistence conclusion.
    pub async fn submit_next_commit(
        &self,
        resource: impl Into<Arc<str>>,
        target: impl Into<PathBuf>,
        bytes: impl Into<Vec<u8>>,
        deadline: MetadataDeadline,
    ) -> RuntimeResult<MetadataIoCommitAdmissionOutcome> {
        match self.submit_next(resource, target, bytes, deadline)? {
            MetadataIoAdmissionOutcome::Accepted(receipt) => receipt
                .wait_until_outcome(deadline)
                .await
                .map(MetadataIoCommitAdmissionOutcome::Completed),
            MetadataIoAdmissionOutcome::TargetConflict(request) => {
                Ok(MetadataIoCommitAdmissionOutcome::TargetConflict(request))
            }
        }
    }

    /// Accepts a snapshot and reports what the caller actually confirmed about
    /// its persistence.
    ///
    /// An `Err` result means the request was never admitted, so the caller may
    /// safely discard its change. Every other case is expressed as a
    /// [`MetadataIoCommitObservation`], including an expiry that leaves the
    /// change unconfirmed.
    ///
    /// # Errors
    ///
    /// Returns a typed admission, capacity, or lifecycle failure.
    pub async fn submit_observed(
        &self,
        request: MetadataWriteRequest,
        deadline: MetadataDeadline,
    ) -> RuntimeResult<MetadataIoCommitObservation> {
        match self.submit(request, deadline)? {
            MetadataIoAdmissionOutcome::Accepted(receipt) => Ok(receipt.observe_until(deadline).await),
            MetadataIoAdmissionOutcome::TargetConflict(request) => {
                Ok(MetadataIoCommitObservation::TargetConflict(request))
            }
        }
    }

    /// Assigns the next owner-lifetime generation and reports what the caller
    /// actually confirmed about its persistence.
    ///
    /// # Errors
    ///
    /// Returns the same normal outcome and typed failures as
    /// [`Self::submit_observed`].
    pub async fn submit_next_observed(
        &self,
        resource: impl Into<Arc<str>>,
        target: impl Into<PathBuf>,
        bytes: impl Into<Vec<u8>>,
        deadline: MetadataDeadline,
    ) -> RuntimeResult<MetadataIoCommitObservation> {
        match self.submit_next(resource, target, bytes, deadline)? {
            MetadataIoAdmissionOutcome::Accepted(receipt) => Ok(receipt.observe_until(deadline).await),
            MetadataIoAdmissionOutcome::TargetConflict(request) => {
                Ok(MetadataIoCommitObservation::TargetConflict(request))
            }
        }
    }

    /// Returns the highest generation this actor confirmed durable for one
    /// logical resource.
    ///
    /// This is the evidence a caller needs to resolve an earlier
    /// [`MetadataIoCommitObservation::Unobserved`] conclusion without holding
    /// completion history: when the value reaches the unobserved generation,
    /// the change is durable after all. The value is seeded from the
    /// owner-scoped target registration, so it survives actor replacement for
    /// the same resource and target. After explicit retirement, compare
    /// [`Self::target_identity`] with the receipt's identity first: generations
    /// from a different history are not evidence about the old write.
    #[must_use]
    pub fn confirmed_durable_generation(&self, resource: &str) -> Option<MetadataGeneration> {
        let state = self.inner.state.lock();
        state
            .resources
            .get(resource)
            .and_then(|resource_state| resource_state.durable_generation)
    }

    /// Stops new admission and drains accepted work until the absolute
    /// deadline.
    #[must_use]
    pub async fn shutdown_until(&self, deadline: MetadataDeadline) -> MetadataIoShutdownReport {
        self.stop_admission();
        loop {
            let finished_notification = self.inner.worker_finished.notified();
            tokio::pin!(finished_notification);
            finished_notification.as_mut().enable();
            let finished = self.inner.state.lock().worker_finished;
            if finished {
                return shutdown_report(&self.inner, false);
            }
            if deadline.is_expired() {
                return shutdown_report(&self.inner, true);
            }
            if tokio::time::timeout_at(deadline.instant(), finished_notification)
                .await
                .is_err()
            {
                return shutdown_report(&self.inner, true);
            }
        }
    }

    /// Rejects future submissions while allowing accepted work to drain.
    pub fn stop_admission(&self) {
        stop_admission(&self.inner);
    }

    /// Returns current queue, byte, generation, and waiter state.
    #[must_use]
    pub fn snapshot(&self) -> MetadataIoSnapshot {
        snapshot(&self.inner)
    }

    /// Creates a diagnostic observer without retaining write authority.
    pub fn observer(&self) -> MetadataIoObserver {
        MetadataIoObserver {
            inner: Arc::downgrade(&self.inner),
        }
    }

    /// Returns the target history associated with this actor's resource state.
    pub fn target_identity(&self, resource: &str) -> Option<MetadataTargetIdentity> {
        self.inner
            .state
            .lock()
            .resources
            .get(resource)
            .and_then(|state| state.identity.clone())
    }

    /// Ends a closed, drained resource's durable history to reclaim registry capacity.
    ///
    /// This is an explicit identity boundary, not ordinary actor replacement.
    /// Future actors acquire a new identity and may bind a different resource
    /// to the path. Old actors cannot resume that history; retained read-only
    /// receipts keep their original result and identity. Compare generations
    /// only within the same identity.
    ///
    /// Unconfirmed replacements retain their fence. The runtime cannot verify
    /// business file formats and does not offer unconditional fence clearing.
    ///
    /// # Errors
    ///
    /// Returns a typed path normalization error without modifying the history.
    pub fn retire_target_for_new_identity(&self, resource: &str) -> RuntimeResult<MetadataTargetRetirementOutcome> {
        let mut state = self.inner.state.lock();
        if state.accepting {
            return Ok(MetadataTargetRetirementOutcome::AdmissionOpen);
        }
        if !state.worker_finished {
            return Ok(MetadataTargetRetirementOutcome::WorkInProgress);
        }
        let Some(resource_state) = state.resources.get_mut(resource) else {
            return Ok(MetadataTargetRetirementOutcome::NotFound);
        };
        if resource_state.in_flight.is_some() || resource_state.queued.is_some() || !resource_state.waiters.is_empty() {
            return Ok(MetadataTargetRetirementOutcome::WorkInProgress);
        }
        let (Some(target), Some(identity)) = (&resource_state.target, &resource_state.identity) else {
            return Ok(MetadataTargetRetirementOutcome::NotFound);
        };
        // A cached, idle registration has no pending work after coordinator completion.
        // Real blocking closures retain their own clone and still prevent retirement.
        resource_state.target_registration.take();
        let outcome = self
            .inner
            .targets
            .retire(target, resource, identity, resource_state.durable_generation)?;
        if outcome == MetadataTargetRetirementOutcome::Retired {
            state.resources.remove(resource);
        }
        Ok(outcome)
    }

    /// Returns the metadata I/O limits that are actually in force.
    ///
    /// The view is read-only and derived from the validated configuration and
    /// the root blocking lane policy captured at startup, so it reports the
    /// values that drive execution rather than the values a caller requested.
    #[must_use]
    pub fn effective_profile(&self) -> MetadataIoEffectiveProfile {
        effective_profile(&self.inner.config, &self.inner.blocking_policy)
    }
}

fn ensure_target_registration(
    state: &mut ActorState,
    registry: &MetadataTargetRegistry,
    request: &MetadataWriteRequest,
) -> RuntimeResult<Option<MetadataTargetRegistration>> {
    let resource = &request.resource;
    // Retired identities remain tombstones in older actors. Bound this cache
    // independently of the shared registry, whose slots can be reused.
    if !state.resources.contains_key(resource) && state.resources.len() >= registry.capacity() {
        return Err(RuntimeError::capacity(crate::RuntimeOperation::AdmitMetadataOperation));
    }
    let mut target_changed = false;
    if let Some(existing) = state.resources.get(resource) {
        if let Some(target) = &existing.target {
            target_changed = !registry.same_target(target.as_ref(), request.target.as_ref())?;
            if target_changed && (existing.in_flight.is_some() || existing.queued.is_some()) {
                return Ok(None);
            }
            if !target_changed {
                if let Some(registration) = &existing.target_registration {
                    // Reusing a cached registration skips the registry check
                    // below, so it has to consult the target's reconciliation
                    // fence explicitly. Without this, a queued generation would
                    // write over a target whose previous replacement never
                    // completed the durability protocol.
                    if registration.reconciliation_required() {
                        return Err(RuntimeError::context_unavailable(
                            crate::RuntimeOperation::MetadataResourceTarget,
                        ));
                    }
                    return Ok(Some(registration.clone()));
                }
            }
        }
    }

    let expected = state
        .resources
        .get(resource)
        .filter(|_| !target_changed)
        .and_then(|state| state.identity.as_ref());
    let registration = match registry.register_with_identity(request.target.as_ref(), Arc::clone(resource), expected)? {
        MetadataTargetRegistrationOutcome::Registered(registration) => registration,
        MetadataTargetRegistrationOutcome::Conflict => return Ok(None),
        MetadataTargetRegistrationOutcome::ReconciliationRequired => {
            return Err(RuntimeError::context_unavailable(
                crate::RuntimeOperation::MetadataResourceTarget,
            ));
        }
    };
    let durable_generation = registration.durable_generation();
    let resource_state = state.resources.entry(Arc::clone(resource)).or_default();
    if target_changed {
        resource_state.target_registration = None;
        resource_state.durable_generation = None;
    }
    resource_state.target = Some(request.target.clone());
    resource_state.identity = Some(registration.identity());
    resource_state.target_registration = Some(registration.clone());
    if let Some(durable_generation) = durable_generation {
        resource_state.durable_generation = Some(
            resource_state
                .durable_generation
                .map_or(durable_generation, |current| current.max(durable_generation)),
        );
    }
    Ok(Some(registration))
}

async fn run_actor(
    inner: Arc<ActorInner>,
    mut receiver: mpsc::Receiver<Arc<str>>,
    blocking: BlockingExecutor,
    file_system: Arc<dyn MetadataFileSystem>,
    cancellation: CancellationToken,
) {
    let mut cancellation_observed = false;
    let mut ready: VecDeque<Arc<str>> = VecDeque::new();
    loop {
        if !cancellation_observed && cancellation.is_cancelled() {
            cancellation_observed = true;
            stop_admission(&inner);
        }
        if should_finish(&inner) {
            break;
        }
        if let Some(resource) = ready.pop_front() {
            let has_more = process_resource(&inner, &blocking, &file_system, resource.clone()).await;
            while let Ok(resource) = receiver.try_recv() {
                ready.push_back(resource);
            }
            if has_more {
                ready.push_back(resource);
            }
            continue;
        }
        tokio::select! {
            biased;
            _ = cancellation.cancelled(), if !cancellation_observed => {
                cancellation_observed = true;
                stop_admission(&inner);
            }
            _ = inner.shutdown.notified() => {}
            resource = receiver.recv() => {
                match resource {
                    Some(resource) => {
                        let has_more =
                            process_resource(&inner, &blocking, &file_system, resource.clone()).await;
                        while let Ok(pending) = receiver.try_recv() {
                            ready.push_back(pending);
                        }
                        if has_more {
                            ready.push_back(resource);
                        }
                    }
                    None => {
                        stop_admission(&inner);
                    }
                }
            }
        }
    }
    finish_worker(&inner);
}

async fn process_resource(
    inner: &Arc<ActorInner>,
    blocking: &BlockingExecutor,
    file_system: &Arc<dyn MetadataFileSystem>,
    resource: Arc<str>,
) -> bool {
    let Some(queued) = take_next_request(inner, &resource) else {
        return false;
    };
    let QueuedMetadataWrite { request, registration } = queued;
    let MetadataWriteRequest {
        target,
        bytes,
        generation,
        lane_deadline,
        ..
    } = request;
    // A request-level deadline is the only value that may tighten the lane
    // phase budgets. The durability deadline a caller passes to `submit` is
    // deliberately not reused here: an admitted write keeps its ordering and
    // byte charge after the caller stops observing it.
    let lane_deadline = lane_deadline.map(|deadline| ShutdownDeadline::at(deadline.instant().into_std()));
    let worker_file_system = file_system.clone();
    let completion_registration = registration.clone();
    let result = match blocking
        .submit_io_until(format!("metadata-io:{resource}"), lane_deadline, move || {
            let result = worker_file_system.persist_atomic(&target, &bytes);
            match &result {
                Ok(()) => {
                    let _ = completion_registration.record_durable(generation);
                }
                Err(error) if error.operation() == crate::RuntimeOperation::MetadataSyncParent => {
                    let _ = completion_registration.record_reconciliation_required();
                }
                Err(_) => {}
            }
            result
        })
        .await
    {
        // A caller/lane wait timeout cannot transfer write ownership: Tokio
        // may still be running this closure. Receipts have their own deadlines.
        Ok(mut task) => task.wait().await.and_then(|result| result),
        Err(source) => Err(source),
    };
    // A request's file mutation and durability accounting are complete once its blocking
    // closure returns, so this writer's target authority is released before the completion
    // is published. Publishing while the authority is still held would let a caller that
    // observes the confirmed generation race this release: the completion clears the
    // cached registration, so the follow-up write for this resource and target has to
    // re-register and would find the previous generation's registration still alive. A
    // queued generation keeps its own clone, so its authority outlives this release.
    drop(registration);
    finish_request(inner, &resource, generation, result)
}

fn take_next_request(inner: &ActorInner, resource: &Arc<str>) -> Option<QueuedMetadataWrite> {
    let mut state = inner.state.lock();
    let resource_state = state.resources.get_mut(resource)?;
    let request = resource_state.queued.take()?;
    resource_state.in_flight = Some(WorkMeta {
        generation: request.request.generation,
        bytes: request.request.len(),
    });
    Some(request)
}

fn finish_request(
    inner: &ActorInner,
    resource: &Arc<str>,
    generation: MetadataGeneration,
    result: RuntimeResult<()>,
) -> bool {
    let mut completed_waiters = Vec::new();
    let (has_queued, outcome) = {
        let mut state = inner.state.lock();
        let Some(mut resource_state) = state.resources.remove(resource) else {
            return false;
        };
        let Some(in_flight) = resource_state.in_flight.take() else {
            state.resources.insert(resource.clone(), resource_state);
            return false;
        };
        debug_assert_eq!(in_flight.generation, generation);
        state.pending_operations = state.pending_operations.saturating_sub(1);
        state.pending_bytes = state.pending_bytes.saturating_sub(in_flight.bytes);

        let outcome = commit_outcome(generation, result);
        if matches!(outcome, MetadataIoCommitOutcome::Durable(_)) {
            resource_state.durable_generation = Some(
                resource_state
                    .durable_generation
                    .map_or(generation, |durable| durable.max(generation)),
            );
        }
        let mut retained = Vec::with_capacity(resource_state.waiters.len());
        for waiter in resource_state.waiters.drain(..) {
            if waiter.generation <= generation {
                completed_waiters.push(waiter);
            } else {
                retained.push(waiter);
            }
        }
        resource_state.waiters = retained;
        let has_queued = resource_state.queued.is_some();
        if !has_queued {
            resource_state.target_registration = None;
        }
        state.resources.insert(resource.clone(), resource_state);
        (has_queued, outcome)
    };

    for waiter in completed_waiters {
        let _ = waiter.sender.send(Ok(outcome.clone()));
    }
    has_queued
}

fn commit_outcome(generation: MetadataGeneration, result: RuntimeResult<()>) -> MetadataIoCommitOutcome {
    match result {
        Ok(()) => MetadataIoCommitOutcome::Durable(generation),
        Err(error) if error.operation() == crate::RuntimeOperation::MetadataSyncParent => {
            MetadataIoCommitOutcome::CommitOutcomeUnknown(error)
        }
        Err(error) => MetadataIoCommitOutcome::FailedBeforeCommit(error),
    }
}

fn stop_admission(inner: &ActorInner) {
    let changed = {
        let mut state = inner.state.lock();
        let changed = state.accepting;
        state.accepting = false;
        changed
    };
    if changed {
        inner.shutdown.notify_one();
    }
}

fn should_finish(inner: &ActorInner) -> bool {
    let state = inner.state.lock();
    !state.accepting && state.pending_operations == 0
}

fn finish_worker(inner: &ActorInner) {
    let mut abandoned = Vec::new();
    {
        let mut state = inner.state.lock();
        state.accepting = false;
        state.worker_finished = true;
        for (resource, resource_state) in &mut state.resources {
            for waiter in resource_state.waiters.drain(..) {
                abandoned.push((resource.clone(), waiter));
            }
        }
    }
    for (_resource, waiter) in abandoned {
        let _ = waiter.sender.send(Err(RuntimeError::closed(
            crate::RuntimeOperation::MetadataWorkerStopped,
        )));
    }
    inner.worker_finished.notify_waiters();
}

fn snapshot(inner: &ActorInner) -> MetadataIoSnapshot {
    let state = inner.state.lock();
    let mut resources = state
        .resources
        .iter()
        .map(|(resource, resource_state)| resource_snapshot(resource, resource_state))
        .collect::<Vec<_>>();
    resources.sort_by(|left, right| left.resource.cmp(&right.resource));
    MetadataIoSnapshot {
        accepting: state.accepting,
        pending_operations: state.pending_operations,
        pending_bytes: state.pending_bytes,
        max_pending_operations: inner.config.max_pending_operations,
        max_pending_bytes: inner.config.max_pending_bytes,
        resources,
    }
}

fn shutdown_report(inner: &ActorInner, timed_out: bool) -> MetadataIoShutdownReport {
    let snapshot = snapshot(inner);
    let unfinished = snapshot
        .resources
        .into_iter()
        .filter(|resource| resource.in_flight_generation.is_some() || resource.queued_generation.is_some())
        .collect();
    MetadataIoShutdownReport {
        timed_out,
        pending_operations: snapshot.pending_operations,
        pending_bytes: snapshot.pending_bytes,
        unfinished,
    }
}

fn resource_snapshot(resource: &Arc<str>, state: &ResourceState) -> MetadataIoResourceSnapshot {
    MetadataIoResourceSnapshot {
        resource: resource.clone(),
        target: state.target.clone(),
        durable_generation: state.durable_generation,
        in_flight_generation: state.in_flight.as_ref().map(|work| work.generation),
        queued_generation: state.queued.as_ref().map(|queued| queued.request.generation),
        waiter_count: state.waiters.len(),
    }
}

fn startup_error(source: RuntimeError) -> RuntimeError {
    source
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::*;

    #[test]
    fn metadata_io_config_rejects_unbounded_zero_limits() {
        let zero_operations = MetadataIoConfig {
            max_pending_operations: 0,
            ..MetadataIoConfig::default()
        };
        assert_eq!(
            zero_operations.validate(),
            Err(RuntimeContractViolation::InvalidMetadataConfiguration {
                policy: RuntimeContractPolicy::MetadataMaxPendingOperationsPositive,
            })
        );

        let zero_bytes = MetadataIoConfig {
            max_pending_bytes: 0,
            ..MetadataIoConfig::default()
        };
        assert_eq!(
            zero_bytes.validate(),
            Err(RuntimeContractViolation::InvalidMetadataConfiguration {
                policy: RuntimeContractPolicy::MetadataMaxPendingBytesPositive,
            })
        );

        assert!(matches!(
            MetadataIoConfig {
                max_pending_operations: 0,
                ..MetadataIoConfig::default()
            }
            .into_plan(),
            Err(RuntimeContractViolation::InvalidMetadataConfiguration {
                policy: RuntimeContractPolicy::MetadataMaxPendingOperationsPositive,
            })
        ));
    }

    #[test]
    fn metadata_io_error_retains_the_original_io_source() {
        let error = metadata_io_failure(
            MetadataIoOperation::WriteTemporary,
            Path::new("metadata.json"),
            std::io::Error::new(std::io::ErrorKind::WriteZero, "partial write"),
        );
        let source = error.source().expect("metadata error should retain its source");
        let source = source
            .downcast_ref::<std::io::Error>()
            .expect("metadata error source should remain a std::io::Error");
        assert_eq!(source.kind(), std::io::ErrorKind::WriteZero);
    }

    #[test]
    fn commit_observation_requires_reconciliation_only_for_unconfirmed_outcomes() {
        let step_error = || {
            RuntimeError::io(
                crate::RuntimeOperation::MetadataSyncParent,
                std::io::Error::other("injected failure"),
            )
        };

        let confirmed = [
            MetadataIoCommitObservation::Settled {
                generation: MetadataGeneration::new(7),
                outcome: MetadataIoCommitOutcome::Durable(MetadataGeneration::new(7)),
            },
            MetadataIoCommitObservation::Settled {
                generation: MetadataGeneration::new(7),
                outcome: MetadataIoCommitOutcome::FailedBeforeCommit(step_error()),
            },
        ];
        for observation in confirmed {
            assert!(
                !observation.requires_reconciliation(),
                "a settled conclusion cannot require reconciliation"
            );
            assert_eq!(observation.unobserved_generation(), None);
            assert_eq!(observation.generation(), Some(MetadataGeneration::new(7)));
        }

        let unconfirmed = MetadataIoCommitObservation::Settled {
            generation: MetadataGeneration::new(7),
            outcome: MetadataIoCommitOutcome::CommitOutcomeUnknown(step_error()),
        };
        assert!(unconfirmed.requires_reconciliation());
        assert_eq!(unconfirmed.generation(), Some(MetadataGeneration::new(7)));
        assert!(unconfirmed.settled().is_some());

        let unobserved = MetadataIoCommitObservation::Unobserved {
            generation: MetadataGeneration::new(9),
        };
        assert!(unobserved.requires_reconciliation());
        assert_eq!(unobserved.unobserved_generation(), Some(MetadataGeneration::new(9)));
        assert_eq!(unobserved.generation(), Some(MetadataGeneration::new(9)));
        assert!(unobserved.settled().is_none());
    }

    #[test]
    fn target_conflict_observation_is_not_a_durability_conclusion() {
        let conflict = MetadataIoCommitObservation::TargetConflict(MetadataWriteRequest::new(
            "resource",
            MetadataGeneration::new(1),
            Path::new("metadata.json"),
            b"snapshot".to_vec(),
        ));
        assert!(!conflict.requires_reconciliation());
        assert_eq!(conflict.unobserved_generation(), None);
        assert_eq!(conflict.generation(), None);
        assert!(conflict.settled().is_none());
    }
}
