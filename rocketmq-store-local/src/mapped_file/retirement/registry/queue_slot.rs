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

//! Queue-generation ownership and the exact durable-retirement handoff.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt;
use std::io;
use std::ops::Deref;
use std::sync::Arc;

use arc_swap::ArcSwap;
use parking_lot::Mutex;

use crate::mapped_file::queue_lifecycle::mapped_files_after_removal;
use crate::mapped_file::queue_lifecycle::MappedFileQueueDeletion;
use crate::mapped_file::DefaultMappedFile;

use super::DurableRetirementToken;
use super::PreparedQueueHandoff;
use super::PublishedFileRegistration;
use super::QueueIdentity;
use super::RegistryFault;
use super::RegistryViolation;
use super::RetirementHandoffCapability;
use super::RetirementIntentBinding;
use super::RetirementOperation;
use super::RetirementRegistry;
use crate::mapped_file::retirement::codec::RetirementReason;
use crate::mapped_file::retirement::identity::FileIncarnationId;
use crate::mapped_file::retirement::identity::PhysicalFileKey;
use crate::mapped_file::retirement::identity::StoreRelativePath;
use crate::mapped_file::retirement::writer::PublishedIncarnationReceipt;

/// Immutable owner snapshot of one atomically published queue generation.
///
/// A snapshot retains the exact generation it observed. It intentionally exposes neither the
/// queue's `ArcSwap` nor any mutation operation.
pub struct MappedFileQueueSnapshot<T> {
    files: Arc<Vec<Arc<T>>>,
}

impl<T> Clone for MappedFileQueueSnapshot<T> {
    fn clone(&self) -> Self {
        Self {
            files: Arc::clone(&self.files),
        }
    }
}

impl<T> Deref for MappedFileQueueSnapshot<T> {
    type Target = [Arc<T>];

    fn deref(&self) -> &Self::Target {
        self.files.as_slice()
    }
}

impl<T> AsRef<[Arc<T>]> for MappedFileQueueSnapshot<T> {
    fn as_ref(&self) -> &[Arc<T>] {
        self.files.as_slice()
    }
}

impl<T> fmt::Debug for MappedFileQueueSnapshot<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MappedFileQueueSnapshot")
            .field("len", &self.files.len())
            .finish()
    }
}

/// Wave-A legacy queue generation with an intentionally narrow mutation surface.
///
/// Managed lifecycle queues use a private type-state and cannot call these legacy publication or
/// recovery methods. The raw `ArcSwap` is never returned to callers.
pub struct MappedFileQueueGeneration<T> {
    slot: Arc<QueueSlot<T>>,
}

impl<T> Clone for MappedFileQueueGeneration<T> {
    fn clone(&self) -> Self {
        Self {
            slot: Arc::clone(&self.slot),
        }
    }
}

impl<T> Default for MappedFileQueueGeneration<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> MappedFileQueueGeneration<T> {
    /// Creates an empty legacy queue generation.
    pub fn new() -> Self {
        Self {
            slot: Arc::new(QueueSlot::new(Vec::new())),
        }
    }

    /// Creates a legacy queue from files already accepted by queue recovery.
    pub fn from_recovery_files(files: Vec<Arc<T>>) -> Self {
        Self {
            slot: Arc::new(QueueSlot::new(files)),
        }
    }

    /// Loads an immutable snapshot of the currently published owners.
    pub fn snapshot(&self) -> MappedFileQueueSnapshot<T> {
        self.slot.snapshot()
    }

    /// Installs an authoritative legacy recovery generation.
    ///
    /// The caller must own the queue lifecycle exclusively; append and cleanup publishers must not
    /// be running. Managed queue type-state has no corresponding method.
    pub fn install_recovery_generation(&self, files: Vec<Arc<T>>) {
        self.slot.files.store(Arc::new(files));
    }

    /// Extends the generation with files accepted by one legacy load operation.
    ///
    /// The caller must own the queue lifecycle exclusively. This preserves the existing partial
    /// load behavior while keeping the publication primitive private.
    pub fn extend_recovery_generation(&self, loaded_files: Vec<Arc<T>>) {
        if loaded_files.is_empty() {
            return;
        }
        let mut files = self.slot.files.load_full().as_slice().to_vec();
        files.extend(loaded_files);
        self.slot.files.store(Arc::new(files));
    }
}

impl fmt::Debug for MappedFileQueueGeneration<DefaultMappedFile> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MappedFileQueueGeneration")
            .field("len", &self.slot.files.load().len())
            .finish_non_exhaustive()
    }
}

impl MappedFileQueueGeneration<DefaultMappedFile> {
    /// Publishes one file created by the Wave-A legacy allocator.
    ///
    /// This method only adds an owner. Managed queues cannot call it because they use a separate
    /// private type-state.
    pub fn publish_legacy_created_file(&self, mapped_file: Arc<DefaultMappedFile>) {
        self.slot.publish_member(mapped_file);
    }

    /// Removes only owners carried by a successful legacy namespace-removal outcome.
    ///
    /// `MappedFileQueueDeletion` has no public constructor, so a caller cannot turn a path lookup
    /// or an unverified `NotFound` into queue-removal authority.
    pub fn apply_legacy_namespace_removal(&self, deletion: MappedFileQueueDeletion) -> i32 {
        let deleted_count = deletion.deleted_count();
        let removal_candidates = deletion.into_mapped_files();
        if removal_candidates.is_empty() {
            return deleted_count;
        }

        loop {
            let current = self.slot.files.load_full();
            let next = mapped_files_after_removal(current.as_slice(), &removal_candidates);
            if self.slot.compare_and_swap(&current, next) {
                return deleted_count;
            }
        }
    }
}

struct QueueSlot<T> {
    files: ArcSwap<Vec<Arc<T>>>,
    identity: QueueIdentity,
    managed_members: Mutex<BTreeMap<usize, ManagedMemberBinding>>,
}

impl<T> QueueSlot<T> {
    fn new(files: Vec<Arc<T>>) -> Self {
        Self {
            files: ArcSwap::from_pointee(files),
            identity: QueueIdentity::allocate(),
            managed_members: Mutex::new(BTreeMap::new()),
        }
    }

    fn snapshot(&self) -> MappedFileQueueSnapshot<T> {
        MappedFileQueueSnapshot {
            files: self.files.load_full(),
        }
    }

    fn publish_member(&self, owner: Arc<T>) {
        loop {
            let current = self.files.load_full();
            let mut next = current.as_slice().to_vec();
            next.push(Arc::clone(&owner));
            if self.compare_and_swap(&current, next) {
                return;
            }
        }
    }

    fn compare_and_swap(&self, current: &Arc<Vec<Arc<T>>>, next: Vec<Arc<T>>) -> bool {
        let previous = self.files.compare_and_swap(current, Arc::new(next));
        Arc::ptr_eq(&previous, current)
    }
}

fn owner_identity<T>(owner: &Arc<T>) -> usize {
    Arc::as_ptr(owner).cast::<()>() as usize
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ManagedMemberBinding {
    incarnation: FileIncarnationId,
    physical_key: PhysicalFileKey,
    canonical_path: StoreRelativePath,
    segment_offset: u64,
    expected_length: u64,
    mapping_generation: u64,
}

/// Complete replay-validated identity for one managed queue member.
///
/// Construction validates the fields shared by queue publication, registry ownership, and the
/// durable retirement record before the owner can enter a managed generation.
pub(in crate::mapped_file) struct ManagedQueueMember<T> {
    owner: Arc<T>,
    binding: ManagedMemberBinding,
}

impl<T> ManagedQueueMember<T> {
    #[allow(
        clippy::too_many_arguments,
        reason = "a managed queue member mirrors one complete durable incarnation binding"
    )]
    pub(in crate::mapped_file) fn new(
        owner: Arc<T>,
        incarnation: FileIncarnationId,
        physical_key: PhysicalFileKey,
        canonical_path: StoreRelativePath,
        segment_offset: u64,
        expected_length: u64,
        mapping_generation: u64,
    ) -> io::Result<Self> {
        if expected_length == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "managed queue member has zero expected length",
            ));
        }
        if mapping_generation == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "managed queue member has zero mapping generation",
            ));
        }
        canonical_path
            .validate_segment_binding(segment_offset)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        Ok(Self {
            owner,
            binding: ManagedMemberBinding {
                incarnation,
                physical_key,
                canonical_path,
                segment_offset,
                expected_length,
                mapping_generation,
            },
        })
    }
}

/// Write-disabled managed queue type-state.
///
/// It has no legacy publication, recovery-install, or namespace-removal methods. A future Wave-B
/// replay adapter must populate it through the registry trust boundary before it becomes useful.
#[doc(hidden)]
pub struct ManagedMappedFileQueueGeneration<T> {
    slot: Arc<QueueSlot<T>>,
}

impl<T> Clone for ManagedMappedFileQueueGeneration<T> {
    fn clone(&self) -> Self {
        Self {
            slot: Arc::clone(&self.slot),
        }
    }
}

impl<T> ManagedMappedFileQueueGeneration<T> {
    pub(in crate::mapped_file) fn from_reconciled_members(members: Vec<ManagedQueueMember<T>>) -> io::Result<Self> {
        let mut files = Vec::new();
        let mut bindings = BTreeMap::new();
        let mut incarnations = BTreeSet::new();
        files
            .try_reserve_exact(members.len())
            .map_err(|_| io::Error::other("failed to reserve managed queue generation"))?;
        for member in members {
            let owner = member.owner;
            let binding = member.binding;
            if !incarnations.insert(binding.incarnation) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "managed queue contains a duplicate file incarnation",
                ));
            }
            if bindings.insert(owner_identity(&owner), binding).is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "managed queue contains a duplicate owner",
                ));
            }
            files.push(owner);
        }
        Ok(Self {
            slot: Arc::new(QueueSlot {
                files: ArcSwap::from_pointee(files),
                identity: QueueIdentity::allocate(),
                managed_members: Mutex::new(bindings),
            }),
        })
    }

    pub(in crate::mapped_file::retirement) fn new_write_disabled() -> Self {
        Self {
            slot: Arc::new(QueueSlot::new(Vec::new())),
        }
    }

    /// Loads an immutable snapshot of the managed queue generation.
    pub fn snapshot(&self) -> MappedFileQueueSnapshot<T> {
        self.slot.snapshot()
    }

    pub(in crate::mapped_file::retirement) fn same_queue_as(&self, other: &Self) -> bool {
        self.slot.identity.same_as(&other.slot.identity)
    }

    pub(in crate::mapped_file::retirement) fn queue_identity(&self) -> QueueIdentity {
        self.slot.identity.clone()
    }

    /// Publishes a newly created owner only after the exact `PublishIncarnation` receipt exists.
    ///
    /// Every fallible queue and registry check completes before the owner becomes visible. A
    /// rejected attempt returns the same non-clone receipt and owner so the caller can retain them
    /// for replay or an explicit retry. After registry registration succeeds, publication itself
    /// has no fallible branch.
    pub(in crate::mapped_file::retirement) fn publish_created_member(
        &self,
        registry: &RetirementRegistry<T>,
        receipt: PublishedIncarnationReceipt,
        owner: Arc<T>,
        mapping_generation: u64,
    ) -> Result<(), CreationPublicationFailure<T>> {
        if mapping_generation == 0 {
            return Err(CreationPublicationFailure::new(
                receipt,
                owner,
                RegistryViolation::ZeroMappingGeneration,
            ));
        }

        let owner_identity = owner_identity(&owner);
        let mut managed_members = self.slot.managed_members.lock();
        if let Some(incumbent) = managed_members.get(&owner_identity) {
            return Err(CreationPublicationFailure::new(
                receipt,
                owner,
                RegistryViolation::OwnerAlreadyRegistered {
                    incumbent: incumbent.incarnation,
                },
            ));
        }
        if self
            .slot
            .files
            .load_full()
            .iter()
            .any(|candidate| Arc::ptr_eq(candidate, &owner))
        {
            return Err(CreationPublicationFailure::new(
                receipt,
                owner,
                RegistryViolation::ManagedQueueBindingMissing,
            ));
        }

        let binding = ManagedMemberBinding {
            incarnation: receipt.incarnation(),
            physical_key: receipt.physical_key(),
            canonical_path: receipt.canonical_path().clone(),
            segment_offset: receipt.segment_offset(),
            expected_length: receipt.expected_length(),
            mapping_generation,
        };
        let registration = match PublishedFileRegistration::new(
            binding.incarnation,
            binding.physical_key,
            binding.canonical_path.clone(),
            binding.segment_offset,
            binding.expected_length,
            Arc::clone(&owner),
            self.slot.identity.clone(),
        ) {
            Ok(registration) => registration,
            Err(error) => return Err(CreationPublicationFailure::new(receipt, owner, error)),
        };
        if let Err(error) = registry.register_published(registration) {
            return Err(CreationPublicationFailure::new(receipt, owner, error));
        }

        managed_members.insert(owner_identity, binding);
        self.slot.publish_member(owner);
        Ok(())
    }

    pub(in crate::mapped_file::retirement) fn retirement_operation(
        &self,
        owner: &Arc<T>,
        reason: RetirementReason,
        retirement_nonce: [u8; 16],
    ) -> Result<(RetirementOperation, QueueIdentity), RegistryViolation> {
        let binding = self
            .slot
            .managed_members
            .lock()
            .get(&owner_identity(owner))
            .cloned()
            .ok_or(RegistryViolation::ManagedQueueBindingMissing)?;
        let operation = RetirementOperation::new(
            binding.incarnation,
            reason,
            binding.mapping_generation,
            binding.segment_offset,
            binding.expected_length,
            retirement_nonce,
            binding.physical_key,
            binding.canonical_path,
        )?;
        Ok((operation, self.slot.identity.clone()))
    }

    /// Registers the entire reconciled queue generation as one atomic registry transition.
    pub(in crate::mapped_file::retirement) fn register_reconciled_members(
        &self,
        registry: &RetirementRegistry<T>,
    ) -> Result<(), RegistryFault> {
        let files = self.slot.files.load_full();
        let bindings = self.slot.managed_members.lock();
        let mut registrations = Vec::new();
        registrations
            .try_reserve_exact(files.len())
            .map_err(RegistryFault::Allocation)?;
        for owner in files.iter() {
            let binding = bindings
                .get(&owner_identity(owner))
                .ok_or(RegistryViolation::ManagedQueueBindingMissing)?;
            registrations.push(PublishedFileRegistration::new(
                binding.incarnation,
                binding.physical_key,
                binding.canonical_path.clone(),
                binding.segment_offset,
                binding.expected_length,
                Arc::clone(owner),
                self.slot.identity.clone(),
            )?);
        }
        drop(bindings);
        registry.register_published_batch(registrations).map_err(Into::into)
    }

    pub(in crate::mapped_file::retirement) fn handoff_retirement(
        &self,
        registry: &RetirementRegistry<T>,
        token: DurableRetirementToken<T>,
        expected: &RetirementIntentBinding,
    ) -> Result<RetirementHandoffCapability<T>, QueueHandoffFailure<T>> {
        self.handoff_retirement_with_before_cas(registry, token, expected, || {})
    }

    fn handoff_retirement_with_before_cas<F>(
        &self,
        registry: &RetirementRegistry<T>,
        token: DurableRetirementToken<T>,
        expected: &RetirementIntentBinding,
        before_cas: F,
    ) -> Result<RetirementHandoffCapability<T>, QueueHandoffFailure<T>>
    where
        F: FnOnce(),
    {
        let prepared = registry
            .prepare_handoff(token, expected, &self.slot.identity)
            .map_err(|failure| {
                let (token, error) = failure.into_parts();
                QueueHandoffFailure::Retryable {
                    token: Box::new(token),
                    reason: QueueHandoffFailureReason::Registry(error),
                }
            })?;
        let Some(owner) = prepared.owner().cloned() else {
            drop(prepared);
            return Err(QueueHandoffFailure::Fenced(RegistryViolation::NeedsRecovery));
        };
        let current = self.slot.files.load_full();
        let mut matching = current
            .iter()
            .enumerate()
            .filter(|(_, candidate)| Arc::ptr_eq(candidate, &owner));
        let Some((candidate_index, _)) = matching.next() else {
            return Err(rollback_failure(prepared, QueueHandoffFailureReason::CandidateMissing));
        };
        if matching.next().is_some() {
            return Err(rollback_failure(
                prepared,
                QueueHandoffFailureReason::CandidateDuplicated,
            ));
        }

        let member = self.slot.managed_members.lock().get(&owner_identity(&owner)).cloned();
        let Some(member) = member else {
            return Err(rollback_failure(
                prepared,
                QueueHandoffFailureReason::CandidateBindingMissing,
            ));
        };
        if member.incarnation != expected.incarnation() {
            return Err(rollback_failure(
                prepared,
                QueueHandoffFailureReason::IncarnationMismatch,
            ));
        }
        if member.mapping_generation != expected.mapping_generation() {
            return Err(rollback_failure(
                prepared,
                QueueHandoffFailureReason::MappingGenerationMismatch {
                    expected: expected.mapping_generation(),
                    actual: member.mapping_generation,
                },
            ));
        }

        let mut next = current.as_slice().to_vec();
        next.remove(candidate_index);
        before_cas();
        if !self.slot.compare_and_swap(&current, next) {
            return Err(rollback_failure(
                prepared,
                QueueHandoffFailureReason::CompareExchangeConflict,
            ));
        }

        match prepared.commit() {
            Ok(capability) => {
                self.slot.managed_members.lock().remove(&owner_identity(&owner));
                Ok(capability)
            }
            Err(error) => Err(QueueHandoffFailure::Fenced(error)),
        }
    }

    #[cfg(test)]
    pub(in crate::mapped_file::retirement::registry) fn install_managed_member_for_test(
        &self,
        owner: Arc<T>,
        incarnation: FileIncarnationId,
        physical_key: PhysicalFileKey,
        canonical_path: StoreRelativePath,
        segment_offset: u64,
        expected_length: u64,
        mapping_generation: u64,
    ) -> Result<(), RegistryViolation> {
        if mapping_generation == 0 {
            return Err(RegistryViolation::ZeroMappingGeneration);
        }
        let owner_identity = owner_identity(&owner);
        let mut managed_members = self.slot.managed_members.lock();
        if let Some(binding) = managed_members.get(&owner_identity) {
            return Err(RegistryViolation::OwnerAlreadyRegistered {
                incumbent: binding.incarnation,
            });
        }
        managed_members.insert(
            owner_identity,
            ManagedMemberBinding {
                incarnation,
                physical_key,
                canonical_path,
                segment_offset,
                expected_length,
                mapping_generation,
            },
        );
        drop(managed_members);
        self.slot.publish_member(owner);
        Ok(())
    }

    #[cfg(test)]
    fn remove_member_for_conflict_test(&self, owner: &Arc<T>) {
        loop {
            let current = self.slot.files.load_full();
            let next: Vec<_> = current
                .iter()
                .filter(|candidate| !Arc::ptr_eq(candidate, owner))
                .cloned()
                .collect();
            if self.slot.compare_and_swap(&current, next) {
                return;
            }
        }
    }

    #[cfg(test)]
    fn install_member_for_conflict_test(&self, owner: Arc<T>) {
        self.slot.publish_member(owner);
    }

    #[cfg(test)]
    fn set_managed_generation_for_test(&self, owner: &Arc<T>, mapping_generation: u64) {
        if let Some(binding) = self.slot.managed_members.lock().get_mut(&owner_identity(owner)) {
            binding.mapping_generation = mapping_generation;
        }
    }

    #[cfg(test)]
    fn handoff_retirement_with_before_cas_for_test<F>(
        &self,
        registry: &RetirementRegistry<T>,
        token: DurableRetirementToken<T>,
        expected: &RetirementIntentBinding,
        before_cas: F,
    ) -> Result<RetirementHandoffCapability<T>, QueueHandoffFailure<T>>
    where
        F: FnOnce(),
    {
        self.handoff_retirement_with_before_cas(registry, token, expected, before_cas)
    }
}

/// Failed managed publication retaining both non-reconstructible inputs.
#[derive(Debug)]
pub(in crate::mapped_file::retirement) struct CreationPublicationFailure<T> {
    receipt: Box<PublishedIncarnationReceipt>,
    owner: Arc<T>,
    error: RegistryViolation,
}

impl<T> CreationPublicationFailure<T> {
    fn new(receipt: PublishedIncarnationReceipt, owner: Arc<T>, error: RegistryViolation) -> Self {
        Self {
            receipt: Box::new(receipt),
            owner,
            error,
        }
    }

    pub(in crate::mapped_file::retirement) fn into_parts(
        self,
    ) -> (PublishedIncarnationReceipt, Arc<T>, RegistryViolation) {
        (*self.receipt, self.owner, self.error)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::mapped_file::retirement) enum QueueHandoffFailureReason {
    Registry(RegistryViolation),
    CandidateMissing,
    CandidateDuplicated,
    CandidateBindingMissing,
    IncarnationMismatch,
    MappingGenerationMismatch { expected: u64, actual: u64 },
    CompareExchangeConflict,
}

#[derive(Debug)]
pub(in crate::mapped_file::retirement) enum QueueHandoffFailure<T> {
    Retryable {
        token: Box<DurableRetirementToken<T>>,
        reason: QueueHandoffFailureReason,
    },
    Fenced(RegistryViolation),
}

impl<T> QueueHandoffFailure<T> {
    pub(in crate::mapped_file::retirement) fn into_retryable_parts(
        self,
    ) -> Result<(DurableRetirementToken<T>, QueueHandoffFailureReason), RegistryViolation> {
        match self {
            Self::Retryable { token, reason } => Ok((*token, reason)),
            Self::Fenced(error) => Err(error),
        }
    }
}

fn rollback_failure<T>(
    prepared: PreparedQueueHandoff<'_, T>,
    reason: QueueHandoffFailureReason,
) -> QueueHandoffFailure<T> {
    match prepared.rollback() {
        Ok(token) => QueueHandoffFailure::Retryable {
            token: Box::new(token),
            reason,
        },
        Err(error) => QueueHandoffFailure::Fenced(error),
    }
}

#[cfg(test)]
mod tests;
