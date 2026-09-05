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

use std::collections::HashMap;
use std::collections::VecDeque;
use std::fmt;
#[cfg(unix)]
use std::fs::File;
use std::fs::OpenOptions;
use std::io::Write;
#[cfg(windows)]
use std::iter;
#[cfg(windows)]
use std::os::windows::ffi::OsStrExt;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::BlockingExecutor;
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
        }
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
        persist_atomic_local(target, bytes)
    }
}

/// Admission and dedicated blocking-lane limits.
#[derive(Debug, Clone)]
pub struct MetadataIoConfig {
    /// The max pending operations value.
    pub max_pending_operations: usize,
    /// The max pending size in bytes.
    pub max_pending_bytes: usize,
    /// The blocking queue timeout value.
    pub blocking_queue_timeout: Duration,
    /// The blocking task timeout value.
    pub blocking_task_timeout: Duration,
    /// The blocking warn after value.
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
    durable: oneshot::Receiver<RuntimeResult<MetadataGeneration>>,
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

impl MetadataIoReceipt {
    /// Returns the accepted resource generation.
    #[must_use]
    pub const fn generation(&self) -> MetadataGeneration {
        self.generation
    }

    /// Waits for this generation, or a newer coalesced generation, to become
    /// durable without extending the caller's absolute deadline.
    ///
    /// # Errors
    ///
    /// Returns an operational runtime failure if persistence cannot complete.
    pub async fn wait_until(self, deadline: MetadataDeadline) -> RuntimeResult<MetadataGeneration> {
        if deadline.is_expired() {
            return Err(RuntimeError::timed_out(crate::RuntimeOperation::WaitForDurableMetadata));
        }
        match tokio::time::timeout_at(deadline.instant(), self.durable).await {
            Ok(Ok(result)) => result,
            Ok(Err(_closed)) => Err(RuntimeError::context_unavailable(
                crate::RuntimeOperation::MetadataWorkerStopped,
            )),
            Err(_elapsed) => Err(RuntimeError::timed_out(crate::RuntimeOperation::WaitForDurableMetadata)),
        }
    }
}

/// A cloneable handle to the bounded metadata I/O owner.
#[derive(Debug, Clone)]
pub struct MetadataIoActor {
    inner: Arc<ActorInner>,
    sender: mpsc::Sender<Arc<str>>,
}

#[derive(Debug)]
struct ActorInner {
    config: MetadataIoConfig,
    next_generation: AtomicU64,
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
    target: Option<Arc<Path>>,
    durable_generation: Option<MetadataGeneration>,
    in_flight: Option<WorkMeta>,
    queued: Option<MetadataWriteRequest>,
    waiters: Vec<GenerationWaiter>,
}

#[derive(Debug, Clone)]
struct WorkMeta {
    generation: MetadataGeneration,
    bytes: usize,
}

#[derive(Debug)]
struct GenerationWaiter {
    generation: MetadataGeneration,
    sender: oneshot::Sender<RuntimeResult<MetadataGeneration>>,
}

impl MetadataIoActor {
    fn start_validated(
        service_context: &ChildServiceContext,
        config: MetadataIoConfig,
        file_system: Arc<dyn MetadataFileSystem>,
    ) -> RuntimeResult<Self> {
        let task_group = service_context.component("metadata-io").task_group().clone();
        let blocking = service_context.metadata_io().clone();
        let (sender, receiver) = mpsc::channel(config.max_pending_operations);
        let inner = Arc::new(ActorInner {
            config,
            next_generation: AtomicU64::new(1),
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
            return Err(RuntimeError::context_unavailable(
                crate::RuntimeOperation::MetadataIoClosed,
            ));
        }

        let existing = state.resources.get(&resource);
        if let Some(existing) = existing {
            if let Some(target) = &existing.target {
                if target.as_ref() != request.target.as_ref()
                    && (existing.in_flight.is_some() || existing.queued.is_some())
                {
                    return Ok(MetadataIoAdmissionOutcome::TargetConflict(request));
                }
            }
            if let Some(durable_generation) = existing.durable_generation {
                if generation <= durable_generation {
                    let _ = waiter_sender.send(Ok(durable_generation));
                    return Ok(MetadataIoAdmissionOutcome::Accepted(MetadataIoReceipt {
                        generation,
                        durable,
                    }));
                }
            }
            if let Some(in_flight) = &existing.in_flight {
                if generation <= in_flight.generation {
                    let Some(resource_state) = state.resources.get_mut(&resource) else {
                        return Err(RuntimeError::context_unavailable(
                            crate::RuntimeOperation::MetadataWorkerStopped,
                        ));
                    };
                    resource_state.waiters.push(GenerationWaiter {
                        generation,
                        sender: waiter_sender,
                    });
                    return Ok(MetadataIoAdmissionOutcome::Accepted(MetadataIoReceipt {
                        generation,
                        durable,
                    }));
                }
            }
            if let Some(queued) = &existing.queued {
                if generation <= queued.generation {
                    let Some(resource_state) = state.resources.get_mut(&resource) else {
                        return Err(RuntimeError::context_unavailable(
                            crate::RuntimeOperation::MetadataWorkerStopped,
                        ));
                    };
                    resource_state.waiters.push(GenerationWaiter {
                        generation,
                        sender: waiter_sender,
                    });
                    return Ok(MetadataIoAdmissionOutcome::Accepted(MetadataIoReceipt {
                        generation,
                        durable,
                    }));
                }
            }
        }

        let old_queued_bytes = existing
            .and_then(|resource_state| resource_state.queued.as_ref())
            .map_or(0, MetadataWriteRequest::len);
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
        resource_state.target = Some(request.target.clone());
        resource_state.queued = Some(request);
        resource_state.waiters.push(GenerationWaiter {
            generation,
            sender: waiter_sender,
        });
        drop(state);
        if let Some(permit) = queue_permit {
            permit.send(resource.clone());
        }
        Ok(MetadataIoAdmissionOutcome::Accepted(MetadataIoReceipt {
            generation,
            durable,
        }))
    }

    /// Assigns the next process-lifetime generation and accepts an immutable
    /// snapshot without waiting for durability.
    ///
    /// Generation values are unique across this actor instance. Gaps are
    /// allowed when admission rejects a request; they do not weaken ordering.
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
        let mut generation = self.inner.next_generation.load(Ordering::Relaxed);
        loop {
            let next = if generation == u64::MAX { 1 } else { generation + 1 };
            match self.inner.next_generation.compare_exchange_weak(
                generation,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(observed) => generation = observed,
            }
        }
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
    let Some(request) = take_next_request(inner, &resource) else {
        return false;
    };
    let target = request.target.clone();
    let bytes = request.bytes.clone();
    let generation = request.generation;
    let worker_file_system = file_system.clone();
    let result = match blocking
        .spawn_io(format!("metadata-io:{resource}"), move || {
            worker_file_system.persist_atomic(&target, &bytes)
        })
        .await
    {
        Ok(result) => result,
        Err(source) => Err(source),
    };
    finish_request(inner, &resource, generation, result)
}

fn take_next_request(inner: &ActorInner, resource: &Arc<str>) -> Option<MetadataWriteRequest> {
    let mut state = inner.state.lock();
    let resource_state = state.resources.get_mut(resource)?;
    let request = resource_state.queued.take()?;
    resource_state.in_flight = Some(WorkMeta {
        generation: request.generation,
        bytes: request.len(),
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
    let has_queued = {
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

        if result.is_ok() {
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
        state.resources.insert(resource.clone(), resource_state);
        has_queued
    };

    for waiter in completed_waiters {
        let completion = match &result {
            Ok(()) => Ok(generation),
            Err(error) => Err(error.clone()),
        };
        let _ = waiter.sender.send(completion);
    }
    has_queued
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
        let _ = waiter.sender.send(Err(RuntimeError::context_unavailable(
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
        queued_generation: state.queued.as_ref().map(|request| request.generation),
        waiter_count: state.waiters.len(),
    }
}

fn startup_error(source: RuntimeError) -> RuntimeError {
    source
}

fn persist_atomic_local(target: &Path, bytes: &[u8]) -> RuntimeResult<()> {
    let parent = target
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    std::fs::create_dir_all(parent)
        .map_err(|source| metadata_io_failure(MetadataIoOperation::CreateParent, parent, source))?;

    let file_name = target.file_name().and_then(|name| name.to_str()).unwrap_or("metadata");
    let temporary = parent.join(format!(".{file_name}.{}.tmp", Uuid::new_v4()));
    let result = persist_temporary_and_replace(&temporary, target, parent, bytes);
    if result.is_err() {
        if let Err(source) = std::fs::remove_file(&temporary) {
            if source.kind() != std::io::ErrorKind::NotFound {
                tracing::warn!(
                    path = %temporary.display(),
                    error = %source,
                    "failed to remove metadata temporary file after persistence failure"
                );
            }
        }
    }
    result
}

fn persist_temporary_and_replace(temporary: &Path, target: &Path, parent: &Path, bytes: &[u8]) -> RuntimeResult<()> {
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(temporary)
        .map_err(|source| metadata_io_failure(MetadataIoOperation::CreateTemporary, temporary, source))?;
    file.write_all(bytes)
        .map_err(|source| metadata_io_failure(MetadataIoOperation::WriteTemporary, temporary, source))?;
    file.sync_all()
        .map_err(|source| metadata_io_failure(MetadataIoOperation::SyncTemporary, temporary, source))?;
    drop(file);

    replace_file(temporary, target)
        .map_err(|source| metadata_io_failure(MetadataIoOperation::ReplaceTarget, target, source))?;
    sync_directory(parent).map_err(|source| metadata_io_failure(MetadataIoOperation::SyncParent, parent, source))
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> std::io::Result<()> {
    File::open(path)?.sync_all()
}

#[cfg(windows)]
fn sync_directory(path: &Path) -> std::io::Result<()> {
    use std::os::windows::fs::OpenOptionsExt;

    let directory = OpenOptions::new()
        .read(true)
        .write(true)
        .share_mode(
            windows_sys::Win32::Storage::FileSystem::FILE_SHARE_READ
                | windows_sys::Win32::Storage::FileSystem::FILE_SHARE_WRITE
                | windows_sys::Win32::Storage::FileSystem::FILE_SHARE_DELETE,
        )
        .custom_flags(windows_sys::Win32::Storage::FileSystem::FILE_FLAG_BACKUP_SEMANTICS)
        .open(path)?;
    directory.sync_all()
}

#[cfg(not(any(unix, windows)))]
fn sync_directory(_path: &Path) -> std::io::Result<()> {
    Ok(())
}

#[cfg(not(windows))]
fn replace_file(source: &Path, destination: &Path) -> std::io::Result<()> {
    std::fs::rename(source, destination)
}

#[cfg(windows)]
fn replace_file(source: &Path, destination: &Path) -> std::io::Result<()> {
    if !destination.exists() {
        let source = wide_path(source);
        let destination = wide_path(destination);
        // SAFETY: Both UTF-16 buffers are NUL-terminated and remain alive for the call.
        let moved = unsafe {
            windows_sys::Win32::Storage::FileSystem::MoveFileExW(
                source.as_ptr(),
                destination.as_ptr(),
                windows_sys::Win32::Storage::FileSystem::MOVEFILE_WRITE_THROUGH,
            )
        };
        return if moved == 0 {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(())
        };
    }

    let source = wide_path(source);
    let destination = wide_path(destination);
    // SAFETY: Both UTF-16 buffers are NUL-terminated and remain alive for the call; optional
    // backup and reserved pointers are null as allowed by ReplaceFileW.
    let replaced = unsafe {
        windows_sys::Win32::Storage::FileSystem::ReplaceFileW(
            destination.as_ptr(),
            source.as_ptr(),
            std::ptr::null(),
            windows_sys::Win32::Storage::FileSystem::REPLACEFILE_WRITE_THROUGH,
            std::ptr::null(),
            std::ptr::null(),
        )
    };
    if replaced == 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(windows)]
fn wide_path(path: &Path) -> Vec<u16> {
    path.as_os_str().encode_wide().chain(iter::once(0)).collect()
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
}
