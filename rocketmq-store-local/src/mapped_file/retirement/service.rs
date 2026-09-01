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

//! Bounded, synchronous managed-retirement batches owned by the Store runtime.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use parking_lot::Mutex;
use rocketmq_store_api::StoreComponent;
use rocketmq_store_api::StoreError;
use rocketmq_store_api::StoreOperation;
use thiserror::Error;

use super::activation::ActiveManagedLifecycle;
use super::codec::RetirementReason;
use super::io::FileLedgerIo;
use super::io::LedgerIo;
use super::platform::VerifiedNamespaceRoot;
use super::registry::reaper::drive_logical_namespace;
use super::registry::reaper::drive_tombstone_namespace;
use super::registry::reaper::LogicalNamespaceProgress;
use super::registry::reaper::ReaperDriveError;
use super::registry::reaper::TombstoneNamespaceProgress;
use super::registry::LogicalRemovedCapability;
use super::registry::ManagedMappedFileQueueGeneration;
use super::registry::NamespaceAbsentCapability;
use super::registry::RecoveredRetirementWork;
use super::registry::RegistryViolation;
use super::registry::RetirementHandoffCapability;
use super::registry::RetirementIntentBinding;
use super::registry::RetirementRegistry;
use super::registry::TombstonedCapability;
use super::writer::ManagedLedgerWriter;
use super::writer::ManagedLedgerWriterError;
use crate::mapped_file::DefaultMappedFile;

mod creation;
pub(crate) use creation::ManagedIncarnationCreationError;
pub use creation::{ManagedIncarnationCreateRequest, ManagedIncarnationCreation};

const RETRY_BASE_DELAY: Duration = Duration::from_millis(100);
const RETRY_MAX_DELAY: Duration = Duration::from_secs(30);

/// Durable stage currently owned by the managed retirement service.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManagedRetirementStage {
    /// Durable intent exists but the exact queue CAS has not completed yet.
    QueueHandoff,
    /// Queue publication has been removed but `LogicalRemoved` is not durable yet.
    LogicalRemoval,
    /// The canonical namespace entry is being reconciled.
    Namespace,
    /// A durable tombstone is being removed.
    TombstoneRemoval,
    /// Verified absence is durable and awaits `Completed`.
    Completion,
}

/// Stable caller-facing reason encoded into a durable retirement intent.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManagedRetirementReason {
    TtlExpired,
    OffsetTruncate,
    Reset,
    DeleteLast,
    StoreDestroy,
    AllocationOrphan,
    TopicRetirement,
    DerivedFileRetirement,
    AuditedOperatorRequest,
}

impl From<ManagedRetirementReason> for RetirementReason {
    fn from(reason: ManagedRetirementReason) -> Self {
        match reason {
            ManagedRetirementReason::TtlExpired => Self::TtlExpired,
            ManagedRetirementReason::OffsetTruncate => Self::OffsetTruncate,
            ManagedRetirementReason::Reset => Self::Reset,
            ManagedRetirementReason::DeleteLast => Self::DeleteLast,
            ManagedRetirementReason::StoreDestroy => Self::StoreDestroy,
            ManagedRetirementReason::AllocationOrphan => Self::AllocationOrphan,
            ManagedRetirementReason::TopicRetirement => Self::TopicRetirement,
            ManagedRetirementReason::DerivedFileRetirement => Self::DerivedFileRetirement,
            ManagedRetirementReason::AuditedOperatorRequest => Self::AuditedOperatorRequest,
        }
    }
}

/// Durable submission accepted by the writer and retained by either the queue or reaper backlog.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ManagedRetirementSubmission {
    ticket_id: u64,
    stage: ManagedRetirementStage,
}

impl ManagedRetirementSubmission {
    pub const fn ticket_id(&self) -> u64 {
        self.ticket_id
    }

    pub const fn stage(&self) -> ManagedRetirementStage {
        self.stage
    }
}

/// Private retirement-submission leaf with its former kind folded in.
#[derive(Debug, Error)]
pub(crate) enum ManagedRetirementSubmissionError {
    #[error(transparent)]
    Registry(#[from] RegistryViolation),
    #[error(transparent)]
    Writer(#[from] ManagedLedgerWriterError),
    #[error("managed retirement state requires replay")]
    RecoveryRequired,
    #[error("managed retirement admission is closed")]
    AdmissionClosed,
}

impl ManagedRetirementSubmissionError {
    /// Promotes this leaf into the canonical storage facade exactly once.
    ///
    /// Every rejected or failed durable retirement submission is reported as
    /// an administrative write failure of the mapped-file component with the
    /// complete leaf preserved as the typed source.
    fn into_store_error(self) -> StoreError {
        StoreError::new(&rocketmq_error::STORAGE_WRITE_FAILED, StoreOperation::Admin)
            .in_component(StoreComponent::MappedFile)
            .with_source(self)
    }

    fn registry(source: RegistryViolation) -> Self {
        Self::Registry(source)
    }

    fn writer(source: ManagedLedgerWriterError) -> Self {
        Self::Writer(source)
    }

    fn recovery_required() -> Self {
        Self::RecoveryRequired
    }

    fn admission_closed() -> Self {
        Self::AdmissionClosed
    }
}

/// Snapshot produced by one bounded synchronous retirement batch.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ManagedRetirementBatchReport {
    attempted: usize,
    completed: usize,
    pending_tickets: usize,
    tombstone_backlog: usize,
    oldest_pending_age: Duration,
    last_failure_stage: Option<ManagedRetirementStage>,
    recovery_required: bool,
}

impl ManagedRetirementBatchReport {
    /// Number of durable or namespace transitions attempted in this batch.
    pub const fn attempted(&self) -> usize {
        self.attempted
    }

    /// Number of tickets that reached durable `Completed` in this batch.
    pub const fn completed(&self) -> usize {
        self.completed
    }

    /// Number of registry identities still retained after this batch.
    pub const fn pending_tickets(&self) -> usize {
        self.pending_tickets
    }

    /// Number of queued entries at the durable tombstone stage.
    pub const fn tombstone_backlog(&self) -> usize {
        self.tombstone_backlog
    }

    /// Age of the oldest in-memory retry authority retained by this process.
    pub const fn oldest_pending_age(&self) -> Duration {
        self.oldest_pending_age
    }

    /// Last stage that returned a typed pending outcome.
    pub const fn last_failure_stage(&self) -> Option<ManagedRetirementStage> {
        self.last_failure_stage
    }

    /// Whether replay is required before any further lifecycle capability transition.
    pub const fn recovery_required(&self) -> bool {
        self.recovery_required
    }
}

/// Cloneable handle to one activated Store's synchronous retirement core.
///
/// The handle does not create tasks. Store code must execute [`Self::drive_batch`] inside one
/// injected storage `BlockingExecutor` operation and stop scheduling new batches before shutdown
/// drain begins.
#[doc(hidden)]
#[derive(Clone)]
pub struct ManagedLifecycleRuntime {
    inner: Arc<Mutex<ManagedLifecycleRuntimeInner>>,
}

struct ManagedLifecycleRuntimeInner {
    // Retains the exclusive Store-root lease and replay proof for the writer's lifetime.
    _session: super::state::reconciliation::ReconciledLifecycleSession,
    core: ManagedRetirementCore<FileLedgerIo, VerifiedNamespaceRoot, DefaultMappedFile>,
    admission: RuntimeAdmission,
    queue_generations: Vec<ManagedMappedFileQueueGeneration<DefaultMappedFile>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RuntimeAdmission {
    Running,
    Shutdown,
    StoreDestroy,
}

impl RuntimeAdmission {
    fn enter_store_destroy(&mut self) -> bool {
        match self {
            Self::Running => false,
            Self::Shutdown => {
                *self = Self::StoreDestroy;
                true
            }
            Self::StoreDestroy => true,
        }
    }
}

impl std::fmt::Debug for ManagedLifecycleRuntime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self.inner.lock();
        formatter
            .debug_struct("ManagedLifecycleRuntime")
            .field("pending_tickets", &inner.core.registry.retained_identity_count())
            .field("recovery_required", &inner.core.recovery_required)
            .field("admission", &inner.admission)
            .finish_non_exhaustive()
    }
}

impl ManagedLifecycleRuntime {
    pub(super) fn from_active(active: ActiveManagedLifecycle) -> Self {
        let ActiveManagedLifecycle {
            session,
            store_root,
            registry,
            writer,
            namespace_root,
            recovered_work,
        } = active;
        let store_uuid = session.writer_frontier().store_uuid();
        let create_high_water = session.state().recovered().create_high_water();
        let mut core = ManagedRetirementCore::new(registry, writer, namespace_root, recovered_work, Instant::now());
        core.configure_creation(store_root, store_uuid, create_high_water);
        Self {
            inner: Arc::new(Mutex::new(ManagedLifecycleRuntimeInner {
                _session: session,
                core,
                admission: RuntimeAdmission::Running,
                queue_generations: Vec::new(),
            })),
        }
    }

    /// Creates an empty queue generation bound to this managed runtime's trust boundary.
    ///
    /// The generation contains no owner and cannot publish one without a durable creation
    /// receipt. Admission remains controlled by the runtime when allocation is attempted.
    #[must_use]
    pub fn empty_queue_generation(&self) -> ManagedMappedFileQueueGeneration<DefaultMappedFile> {
        let generation = ManagedMappedFileQueueGeneration::new_write_disabled();
        self.track_queue_generation(&generation);
        generation
    }

    /// Registers a reconciled queue generation with this runtime's Store-wide lifecycle owner.
    ///
    /// Registration is process-local and idempotent. Durable publication and replay validation
    /// remain prerequisites performed by activation before this method is called.
    #[doc(hidden)]
    pub fn track_queue_generation(&self, generation: &ManagedMappedFileQueueGeneration<DefaultMappedFile>) {
        let mut inner = self.inner.lock();
        if inner
            .queue_generations
            .iter()
            .any(|tracked| tracked.same_queue_as(generation))
        {
            return;
        }
        inner.queue_generations.push(generation.clone());
    }

    /// Executes at most `max_actions` durable or namespace transitions synchronously.
    ///
    /// This function performs blocking file operations and must not run directly on an async
    /// executor thread.
    #[must_use]
    pub fn drive_batch(&self, max_actions: usize) -> ManagedRetirementBatchReport {
        let now = Instant::now();
        self.inner.lock().core.drive_batch_at(max_actions, now, unix_time_ns())
    }

    /// Returns a zero-action backlog snapshot.
    #[must_use]
    pub fn snapshot(&self) -> ManagedRetirementBatchReport {
        let now = Instant::now();
        self.inner.lock().core.report(now, 0, 0)
    }

    /// Stops admission of new durable intents while preserving replayable backlog authority.
    pub fn begin_shutdown(&self) {
        self.inner.lock().admission = RuntimeAdmission::Shutdown;
    }

    /// Durably submits every currently active member of every tracked managed queue.
    ///
    /// The simple lifecycle gate permits only `Shutdown -> StoreDestroy`; retries remain in
    /// `StoreDestroy`. This function performs blocking ledger I/O and must run inside one Store
    /// storage-IO operation.
    /// Submits every remaining owner for Store-destroy retirement.
    ///
    /// # Errors
    ///
    /// Returns `STORAGE_WRITE_FAILED` when the durable submission fails closed.
    pub fn submit_store_destroy_retirements(&self) -> Result<usize, StoreError> {
        self.submit_store_destroy_retirements_typed()
            .map_err(ManagedRetirementSubmissionError::into_store_error)
    }

    fn submit_store_destroy_retirements_typed(&self) -> Result<usize, ManagedRetirementSubmissionError> {
        let mut inner = self.inner.lock();
        if !inner.admission.enter_store_destroy() {
            return Err(ManagedRetirementSubmissionError::admission_closed());
        }
        let generations = inner.queue_generations.clone();
        inner
            .core
            .submit_store_destroy_at(&generations, store_destroy_nonce, Instant::now())
    }

    /// Returns whether every tracked queue member completed namespace retirement.
    #[must_use]
    pub fn store_destroy_complete(&self) -> bool {
        let inner = self.inner.lock();
        !inner.core.recovery_required
            && inner.core.backlog.is_empty()
            && inner
                .queue_generations
                .iter()
                .all(|generation| generation.snapshot().is_empty())
    }

    /// Returns whether a prior retirement submission still owns unfinished durable work.
    #[must_use]
    pub fn has_retirement_backlog(&self) -> bool {
        let inner = self.inner.lock();
        inner.core.recovery_required || !inner.core.backlog.is_empty()
    }

    /// Executes one bounded drain batch without waiting for retry backoff.
    #[must_use]
    pub fn drive_drain_batch(&self, max_actions: usize) -> ManagedRetirementBatchReport {
        let now = Instant::now();
        let mut inner = self.inner.lock();
        inner.core.make_all_due(now);
        inner.core.drive_batch_at(max_actions, now, unix_time_ns())
    }

    /// Durably accepts one exact managed queue member for retirement.
    ///
    /// Like [`Self::drive_batch`], this performs blocking ledger I/O and must run inside one Store
    /// storage-IO operation. The nonce must be independently generated and nonzero.
    pub fn submit_retirement(
        &self,
        queue: &ManagedMappedFileQueueGeneration<DefaultMappedFile>,
        owner: &Arc<DefaultMappedFile>,
        reason: ManagedRetirementReason,
        retirement_nonce: [u8; 16],
    ) -> Result<ManagedRetirementSubmission, StoreError> {
        self.submit_retirement_typed(queue, owner, reason, retirement_nonce)
            .map_err(ManagedRetirementSubmissionError::into_store_error)
    }

    fn submit_retirement_typed(
        &self,
        queue: &ManagedMappedFileQueueGeneration<DefaultMappedFile>,
        owner: &Arc<DefaultMappedFile>,
        reason: ManagedRetirementReason,
        retirement_nonce: [u8; 16],
    ) -> Result<ManagedRetirementSubmission, ManagedRetirementSubmissionError> {
        let mut inner = self.inner.lock();
        if inner.admission != RuntimeAdmission::Running {
            return Err(ManagedRetirementSubmissionError::admission_closed());
        }
        inner
            .core
            .submit_at(queue, owner, reason.into(), retirement_nonce, Instant::now())
    }
}

impl ActiveManagedLifecycle {
    pub(super) fn into_runtime(self) -> ManagedLifecycleRuntime {
        ManagedLifecycleRuntime::from_active(self)
    }
}

fn unix_time_ns() -> u64 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    u64::try_from(nanos).unwrap_or(u64::MAX)
}

fn store_destroy_nonce() -> [u8; 16] {
    static NEXT_NONCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

    let sequence = NEXT_NONCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let mut nonce = [0u8; 16];
    nonce[..8].copy_from_slice(&sequence.to_le_bytes());
    nonce[8..].copy_from_slice(&unix_time_ns().to_le_bytes());
    if nonce == [0; 16] {
        nonce[0] = 1;
    }
    nonce
}

pub(super) trait NamespaceDriver<I: LedgerIo, O> {
    fn drive_logical(
        &mut self,
        writer: &mut ManagedLedgerWriter<I>,
        capability: LogicalRemovedCapability<O>,
        observation_time_ns: u64,
    ) -> Result<LogicalNamespaceProgress<O>, ReaperDriveError>;

    fn drive_tombstone(
        &mut self,
        writer: &mut ManagedLedgerWriter<I>,
        capability: TombstonedCapability<O>,
        observation_time_ns: u64,
    ) -> Result<TombstoneNamespaceProgress<O>, ReaperDriveError>;
}

impl<I: LedgerIo, O> NamespaceDriver<I, O> for VerifiedNamespaceRoot {
    fn drive_logical(
        &mut self,
        writer: &mut ManagedLedgerWriter<I>,
        capability: LogicalRemovedCapability<O>,
        observation_time_ns: u64,
    ) -> Result<LogicalNamespaceProgress<O>, ReaperDriveError> {
        drive_logical_namespace(
            self,
            writer,
            capability,
            initial_namespace_transition(),
            observation_time_ns,
        )
    }

    fn drive_tombstone(
        &mut self,
        writer: &mut ManagedLedgerWriter<I>,
        capability: TombstonedCapability<O>,
        observation_time_ns: u64,
    ) -> Result<TombstoneNamespaceProgress<O>, ReaperDriveError> {
        drive_tombstone_namespace(self, writer, capability, observation_time_ns)
    }
}

#[cfg(target_os = "linux")]
const fn initial_namespace_transition() -> super::platform::NamespaceTransition {
    super::platform::NamespaceTransition::DirectUnlink
}

#[cfg(not(target_os = "linux"))]
const fn initial_namespace_transition() -> super::platform::NamespaceTransition {
    // Windows requires a same-directory tombstone before removal. Other backends return their
    // typed Unsupported outcome before mutating namespace state.
    super::platform::NamespaceTransition::MoveToTombstone
}

enum PendingRetirement<O> {
    QueueHandoff {
        queue: ManagedMappedFileQueueGeneration<O>,
        token: super::registry::DurableRetirementToken<O>,
        binding: RetirementIntentBinding,
    },
    LogicalRemoval(RetirementHandoffCapability<O>),
    Namespace(LogicalRemovedCapability<O>),
    TombstoneRemoval(TombstonedCapability<O>),
    Completion(NamespaceAbsentCapability<O>),
}

impl<O> PendingRetirement<O> {
    const fn stage(&self) -> ManagedRetirementStage {
        match self {
            Self::QueueHandoff { .. } => ManagedRetirementStage::QueueHandoff,
            Self::LogicalRemoval(_) => ManagedRetirementStage::LogicalRemoval,
            Self::Namespace(_) => ManagedRetirementStage::Namespace,
            Self::TombstoneRemoval(_) => ManagedRetirementStage::TombstoneRemoval,
            Self::Completion(_) => ManagedRetirementStage::Completion,
        }
    }

    fn ticket_value(&self) -> u64 {
        match self {
            Self::QueueHandoff { binding, .. } => binding.ticket_id().get(),
            Self::LogicalRemoval(capability) => capability.binding().ticket_id().get(),
            Self::Namespace(capability) => capability.binding().ticket_id().get(),
            Self::TombstoneRemoval(capability) => capability.binding().ticket_id().get(),
            Self::Completion(capability) => capability.binding().ticket_id().get(),
        }
    }
}

impl<O> From<RecoveredRetirementWork<O>> for PendingRetirement<O> {
    fn from(work: RecoveredRetirementWork<O>) -> Self {
        match work {
            RecoveredRetirementWork::LogicalRemoval(capability) => Self::LogicalRemoval(capability),
            RecoveredRetirementWork::Namespace(capability) => Self::Namespace(capability),
            RecoveredRetirementWork::TombstoneRemoval(capability) => Self::TombstoneRemoval(capability),
            RecoveredRetirementWork::Completion(capability) => Self::Completion(capability),
        }
    }
}

struct ScheduledRetirement<O> {
    work: PendingRetirement<O>,
    first_pending_at: Instant,
    next_attempt_at: Instant,
    attempts: u32,
}

enum WorkAdvance<O> {
    Advanced(PendingRetirement<O>),
    Pending(PendingRetirement<O>),
    Completed,
    RecoveryRequired,
}

/// Synchronous retirement state machine. Callers must execute a complete batch on the Store
/// `BlockingExecutor`; no lock held by this type may cross an async suspension point.
pub(super) struct ManagedRetirementCore<I: LedgerIo, D, O> {
    registry: RetirementRegistry<O>,
    writer: ManagedLedgerWriter<I>,
    namespace: D,
    backlog: VecDeque<ScheduledRetirement<O>>,
    last_failure_stage: Option<ManagedRetirementStage>,
    recovery_required: bool,
    creation: Option<creation::ManagedCreationContext>,
}

impl<I: LedgerIo, D: NamespaceDriver<I, O>, O> ManagedRetirementCore<I, D, O> {
    pub(super) fn new(
        registry: RetirementRegistry<O>,
        writer: ManagedLedgerWriter<I>,
        namespace: D,
        recovered_work: Vec<RecoveredRetirementWork<O>>,
        now: Instant,
    ) -> Self {
        let backlog = recovered_work
            .into_iter()
            .map(|work| ScheduledRetirement {
                work: work.into(),
                first_pending_at: now,
                next_attempt_at: now,
                attempts: 0,
            })
            .collect();
        Self {
            registry,
            writer,
            namespace,
            backlog,
            last_failure_stage: None,
            recovery_required: false,
            creation: None,
        }
    }

    pub(super) const fn registry(&self) -> &RetirementRegistry<O> {
        &self.registry
    }

    fn submit_at(
        &mut self,
        queue: &ManagedMappedFileQueueGeneration<O>,
        owner: &Arc<O>,
        reason: RetirementReason,
        retirement_nonce: [u8; 16],
        now: Instant,
    ) -> Result<ManagedRetirementSubmission, ManagedRetirementSubmissionError> {
        if self.recovery_required || self.registry.needs_recovery() {
            return Err(ManagedRetirementSubmissionError::recovery_required());
        }
        let (operation, queue_identity) = queue
            .retirement_operation(owner, reason, retirement_nonce)
            .map_err(ManagedRetirementSubmissionError::registry)?;
        let reservation = self
            .registry
            .prepare_retirement(operation, owner, &queue_identity)
            .map_err(ManagedRetirementSubmissionError::registry)?;
        let binding = reservation.binding().clone();
        let ticket_id = binding.ticket_id().get();
        let token = self
            .writer
            .append_retirement_intent(reservation.begin_append())
            .map_err(|source| {
                self.recovery_required = true;
                ManagedRetirementSubmissionError::writer(source)
            })?;
        match queue.handoff_retirement(&self.registry, token, &binding) {
            Ok(capability) => {
                let capability = self.writer.append_logical_removed(capability).map_err(|source| {
                    self.recovery_required = true;
                    ManagedRetirementSubmissionError::writer(source)
                })?;
                self.push_work(PendingRetirement::Namespace(capability), now);
                Ok(ManagedRetirementSubmission {
                    ticket_id,
                    stage: ManagedRetirementStage::Namespace,
                })
            }
            Err(failure) => match failure.into_retryable_parts() {
                Ok((token, _reason)) => {
                    self.push_work(
                        PendingRetirement::QueueHandoff {
                            queue: queue.clone(),
                            token,
                            binding,
                        },
                        now,
                    );
                    Ok(ManagedRetirementSubmission {
                        ticket_id,
                        stage: ManagedRetirementStage::QueueHandoff,
                    })
                }
                Err(source) => {
                    self.recovery_required = true;
                    Err(ManagedRetirementSubmissionError::registry(source))
                }
            },
        }
    }

    fn submit_store_destroy_at<F>(
        &mut self,
        generations: &[ManagedMappedFileQueueGeneration<O>],
        mut next_nonce: F,
        now: Instant,
    ) -> Result<usize, ManagedRetirementSubmissionError>
    where
        F: FnMut() -> [u8; 16],
    {
        let mut submitted = 0usize;
        for generation in generations {
            let owners = generation.snapshot();
            for owner in owners.iter() {
                self.submit_at(generation, owner, RetirementReason::StoreDestroy, next_nonce(), now)?;
                submitted = submitted.saturating_add(1);
            }
        }
        Ok(submitted)
    }

    fn push_work(&mut self, work: PendingRetirement<O>, now: Instant) {
        self.backlog.push_back(ScheduledRetirement {
            work,
            first_pending_at: now,
            next_attempt_at: now,
            attempts: 0,
        });
    }

    fn make_all_due(&mut self, now: Instant) {
        for entry in &mut self.backlog {
            entry.next_attempt_at = now;
        }
    }

    pub(super) fn drive_batch_at(
        &mut self,
        max_actions: usize,
        now: Instant,
        observation_time_ns: u64,
    ) -> ManagedRetirementBatchReport {
        let mut attempted = 0;
        let mut completed = 0;
        if !self.recovery_required && !self.registry.needs_recovery() {
            while attempted < max_actions {
                let Some(index) = self.backlog.iter().position(|entry| entry.next_attempt_at <= now) else {
                    break;
                };
                let mut scheduled = self
                    .backlog
                    .remove(index)
                    .expect("the located retirement entry must remain in the same synchronous queue");
                let stage = scheduled.work.stage();
                let ticket_value = scheduled.work.ticket_value();
                attempted += 1;
                match self.advance_one(scheduled.work, observation_time_ns) {
                    WorkAdvance::Advanced(work) => {
                        scheduled.work = work;
                        scheduled.attempts = scheduled.attempts.saturating_add(1);
                        scheduled.next_attempt_at = now;
                        self.backlog.push_back(scheduled);
                    }
                    WorkAdvance::Pending(work) => {
                        scheduled.work = work;
                        scheduled.attempts = scheduled.attempts.saturating_add(1);
                        scheduled.next_attempt_at = now + retry_delay(scheduled.attempts, ticket_value);
                        self.last_failure_stage = Some(stage);
                        self.backlog.push_back(scheduled);
                    }
                    WorkAdvance::Completed => completed += 1,
                    WorkAdvance::RecoveryRequired => {
                        self.recovery_required = true;
                        self.last_failure_stage = Some(stage);
                        break;
                    }
                }
            }
        }
        self.report(now, attempted, completed)
    }

    fn advance_one(&mut self, work: PendingRetirement<O>, observation_time_ns: u64) -> WorkAdvance<O> {
        match work {
            PendingRetirement::QueueHandoff { queue, token, binding } => {
                match queue.handoff_retirement(&self.registry, token, &binding) {
                    Ok(capability) => match self.writer.append_logical_removed(capability) {
                        Ok(capability) => WorkAdvance::Advanced(PendingRetirement::Namespace(capability)),
                        Err(_) => WorkAdvance::RecoveryRequired,
                    },
                    Err(failure) => match failure.into_retryable_parts() {
                        Ok((token, _reason)) => {
                            WorkAdvance::Pending(PendingRetirement::QueueHandoff { queue, token, binding })
                        }
                        Err(_) => WorkAdvance::RecoveryRequired,
                    },
                }
            }
            PendingRetirement::LogicalRemoval(capability) => match self.writer.append_logical_removed(capability) {
                Ok(capability) => WorkAdvance::Advanced(PendingRetirement::Namespace(capability)),
                Err(_) => WorkAdvance::RecoveryRequired,
            },
            PendingRetirement::Namespace(capability) => {
                match self
                    .namespace
                    .drive_logical(&mut self.writer, capability, observation_time_ns)
                {
                    Ok(LogicalNamespaceProgress::Tombstoned(capability)) => {
                        WorkAdvance::Advanced(PendingRetirement::TombstoneRemoval(capability))
                    }
                    Ok(LogicalNamespaceProgress::NamespaceAbsent(capability)) => {
                        WorkAdvance::Advanced(PendingRetirement::Completion(capability))
                    }
                    Ok(LogicalNamespaceProgress::Pending { capability, .. }) => {
                        WorkAdvance::Pending(PendingRetirement::Namespace(capability))
                    }
                    Err(_) => WorkAdvance::RecoveryRequired,
                }
            }
            PendingRetirement::TombstoneRemoval(capability) => {
                match self
                    .namespace
                    .drive_tombstone(&mut self.writer, capability, observation_time_ns)
                {
                    Ok(TombstoneNamespaceProgress::NamespaceAbsent(capability)) => {
                        WorkAdvance::Advanced(PendingRetirement::Completion(capability))
                    }
                    Ok(TombstoneNamespaceProgress::Pending { capability, .. }) => {
                        WorkAdvance::Pending(PendingRetirement::TombstoneRemoval(capability))
                    }
                    Err(_) => WorkAdvance::RecoveryRequired,
                }
            }
            PendingRetirement::Completion(capability) => {
                match self.writer.append_completed(capability, observation_time_ns) {
                    Ok(_) => WorkAdvance::Completed,
                    Err(_) => WorkAdvance::RecoveryRequired,
                }
            }
        }
    }

    fn report(&self, now: Instant, attempted: usize, completed: usize) -> ManagedRetirementBatchReport {
        let oldest_pending_age = self
            .backlog
            .iter()
            .map(|entry| now.saturating_duration_since(entry.first_pending_at))
            .max()
            .unwrap_or_default();
        let tombstone_backlog = self
            .backlog
            .iter()
            .filter(|entry| matches!(entry.work, PendingRetirement::TombstoneRemoval(_)))
            .count();
        ManagedRetirementBatchReport {
            attempted,
            completed,
            pending_tickets: self.registry.retained_identity_count(),
            tombstone_backlog,
            oldest_pending_age,
            last_failure_stage: self.last_failure_stage,
            recovery_required: self.recovery_required || self.registry.needs_recovery(),
        }
    }
}

fn retry_delay(attempts: u32, ticket_value: u64) -> Duration {
    let exponent = attempts.saturating_sub(1).min(8);
    let scaled_millis = RETRY_BASE_DELAY
        .as_millis()
        .saturating_mul(1_u128 << exponent)
        .min(RETRY_MAX_DELAY.as_millis());
    let jitter_window = (scaled_millis / 4).max(1);
    let jitter = u128::from(ticket_value.rotate_left(attempts & 63)) % jitter_window;
    Duration::from_millis(u64::try_from((scaled_millis + jitter).min(RETRY_MAX_DELAY.as_millis())).unwrap_or(u64::MAX))
}

#[cfg(test)]
mod tests;
