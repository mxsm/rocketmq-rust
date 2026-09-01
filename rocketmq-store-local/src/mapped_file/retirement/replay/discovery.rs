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

use rocketmq_store_api::StoreComponent;
use rocketmq_store_api::StoreError;
use rocketmq_store_api::StoreOperation;
use std::collections::BTreeMap;
use std::fs::File;
use std::sync::Arc;

use super::replay;
use super::GenerationBytes;
use super::RecoveryDecision;
use super::ReplayInput;
use super::ReplayLimits;
use super::ReplayViolation;
use crate::mapped_file::retirement::codec::CodecViolation;
use crate::mapped_file::retirement::codec::ACKNOWLEDGEMENT_FILE_LENGTH;
use crate::mapped_file::retirement::codec::ACKNOWLEDGEMENT_SLOT_LENGTH;
use crate::mapped_file::retirement::sidecar::decode_enabled_marker_file;
use crate::mapped_file::retirement::sidecar::decode_snapshot;
use crate::mapped_file::retirement::sidecar::decode_store_meta;
use crate::mapped_file::retirement::sidecar::EnabledMarkerFile;
use crate::mapped_file::retirement::sidecar::SidecarViolation;
use crate::mapped_file::retirement::sidecar::StoreMeta;
use crate::mapped_file::retirement::sidecar::ENABLED_MARKER_FILE_LENGTH;
use crate::mapped_file::retirement::sidecar::STORE_META_LENGTH;

mod bootstrap;
pub(in crate::mapped_file::retirement) mod platform;
mod quarantine;
mod reading;
mod types;

use bootstrap::validate_marker_absent_bootstrap;
use quarantine::read_tail_evidence;
use quarantine::validate_required_tail_evidence;
use quarantine::OwnedTailEvidence;
use quarantine::QuarantinePlan;
use quarantine::QuarantineRead;
use reading::read_exact_file;
use reading::read_snapshot_file;
use reading::validate_snapshot_prefix;
pub(crate) use types::ManagedLifecycleReadFailure;
use types::{corruption, io_error, limit_error};
pub use types::{
    LockedManagedLifecycleInspection, ManagedLifecycleReadLimits, ManagedLifecycleReadOutcome,
    ManagedLifecycleRecoveryReason, ManagedLifecycleSession,
};

const LIFECYCLE_DIRECTORY: &str = ".rocketmq-lifecycle";
const STORE_META_FILE: &str = "store.meta";
const ENABLED_MARKER_FILE: &str = "ENABLED.v1";
const ACKNOWLEDGEMENT_FILE: &str = "ACKNOWLEDGED.v1";
const QUARANTINE_DIRECTORY: &str = "quarantine";
const SNAPSHOT_PREFIX: &str = "manifest.snapshot.g";
const LOG_PREFIX: &str = "retirement.log.g";
const GENERATION_DIGITS: usize = 20;
const MAX_DIRECTORY_ENTRIES: usize = 256;
const MAX_LOG_FILE_LENGTH: u64 = 512 * 1024 * 1024;
const MAX_TOTAL_READ_BYTES: u64 = 1024 * 1024 * 1024;

/// Inspects lifecycle artifacts relative to an already-retained Store-root handle.
///
/// This function performs no filesystem writes and never grants publication authority.
///
/// # Errors
///
/// Returns a typed failure if containment, complete inventory, bounded decoding, or replay cannot be proven.
#[doc(hidden)]
pub(crate) fn inspect_managed_lifecycle_read_only(
    store_root: &File,
) -> Result<ManagedLifecycleReadOutcome, ManagedLifecycleReadFailure> {
    inspect_with_hook(store_root, || {})
}

/// Inspects lifecycle artifacts with caller-selected work bounds.
///
/// # Errors
///
/// Returns a typed failure for invalid/insufficient bounds or if containment, complete inventory,
/// bounded decoding, or replay cannot be proven.
#[doc(hidden)]
pub(crate) fn inspect_managed_lifecycle_read_only_with_limits(
    store_root: &File,
    limits: ManagedLifecycleReadLimits,
) -> Result<ManagedLifecycleReadOutcome, ManagedLifecycleReadFailure> {
    inspect_with_limits_and_hooks(store_root, limits, || {}, || {})
}

/// Inspects managed lifecycle evidence while the caller retains the exclusive Store-root lease.
///
/// Unlike [`inspect_managed_lifecycle_read_only`], a managed result retains the replay decision
/// and an exact duplicate of the already-open root handle. It still grants no publication or
/// namespace-mutation authority.
///
/// # Safety
///
/// `store_root` must be the no-follow root handle protected by the exact exclusive Store lock held
/// by `exclusive_lease`. That keepalive must own both handles and their validated
/// configured-path-to-handle identity bindings; dropping every other reference to the lease must
/// not release the lock while the returned inspection exists. Safe Store code should call this
/// only through its owned root-lease wrapper.
///
/// # Errors
///
/// Returns a typed failure if bounded, stable, handle-relative discovery and replay cannot be
/// proven, or if the retained root handle cannot be duplicated.
#[doc(hidden)]
pub(crate) unsafe fn inspect_managed_lifecycle_under_exclusive_lock(
    store_root: &File,
    exclusive_lease: Arc<dyn Send + Sync>,
) -> Result<LockedManagedLifecycleInspection, ManagedLifecycleReadFailure> {
    let classification =
        inspect_stable_with_limits_and_hooks(store_root, ManagedLifecycleReadLimits::default(), || {}, || {})?;
    match classification {
        StableLifecycleInspection::LegacyAbsent => Ok(LockedManagedLifecycleInspection::LegacyAbsent),
        StableLifecycleInspection::Managed(managed) => {
            let retained_root = store_root.try_clone().map_err(io_error)?;
            Ok(LockedManagedLifecycleInspection::Managed(Box::new(
                ManagedLifecycleSession::new(
                    retained_root,
                    exclusive_lease,
                    managed.outcome,
                    managed.store_uuid,
                    managed.decision,
                ),
            )))
        }
    }
}

fn inspect_with_hook(
    store_root: &File,
    after_first_inventory: impl FnOnce(),
) -> Result<ManagedLifecycleReadOutcome, ManagedLifecycleReadFailure> {
    inspect_with_limits_and_hooks(
        store_root,
        ManagedLifecycleReadLimits::default(),
        after_first_inventory,
        || {},
    )
}

fn inspect_with_hooks(
    store_root: &File,
    after_first_inventory: impl FnOnce(),
    before_third_inventory: impl FnOnce(),
) -> Result<ManagedLifecycleReadOutcome, ManagedLifecycleReadFailure> {
    inspect_with_limits_and_hooks(
        store_root,
        ManagedLifecycleReadLimits::default(),
        after_first_inventory,
        before_third_inventory,
    )
}

fn inspect_with_limits_and_hooks(
    store_root: &File,
    limits: ManagedLifecycleReadLimits,
    after_first_inventory: impl FnOnce(),
    before_third_inventory: impl FnOnce(),
) -> Result<ManagedLifecycleReadOutcome, ManagedLifecycleReadFailure> {
    inspect_stable_with_limits_and_hooks(store_root, limits, after_first_inventory, before_third_inventory)
        .map(StableLifecycleInspection::outcome)
}

fn inspect_stable_with_limits_and_hooks(
    store_root: &File,
    limits: ManagedLifecycleReadLimits,
    after_first_inventory: impl FnOnce(),
    before_third_inventory: impl FnOnce(),
) -> Result<StableLifecycleInspection, ManagedLifecycleReadFailure> {
    let limits = limits.validate()?;
    let Some(lifecycle) =
        platform::LifecycleDirectory::open(store_root, LIFECYCLE_DIRECTORY).map_err(map_platform_error)?
    else {
        return Ok(StableLifecycleInspection::LegacyAbsent);
    };

    let first = lifecycle
        .enumerate(limits.max_directory_entries)
        .map_err(map_platform_error)?;
    let plan = InventoryPlan::parse(&first, limits)?;
    after_first_inventory();

    let mut opened = Vec::new();
    opened
        .try_reserve_exact(first.entries.len())
        .map_err(|_| limit_error("open handles", first.entries.len(), limits.max_directory_entries))?;
    for entry in &first.entries {
        opened.push(lifecycle.open_entry(entry).map_err(map_platform_error)?);
    }

    let mut quarantine = if let Some(index) = plan.quarantine {
        let remaining = limits.max_directory_entries.saturating_sub(first.entries.len());
        let quarantine_first = opened[index].enumerate(remaining).map_err(map_platform_error)?;
        let quarantine_plan = QuarantinePlan::parse(&first, &quarantine_first)?;
        let mut quarantine_opened = Vec::new();
        quarantine_opened
            .try_reserve_exact(quarantine_first.entries.len())
            .map_err(|_| limit_error("quarantine open handles", quarantine_first.entries.len(), remaining))?;
        for entry in &quarantine_first.entries {
            quarantine_opened.push(opened[index].open_entry(entry).map_err(map_platform_error)?);
        }
        let quarantine_second = opened[index].enumerate(remaining).map_err(map_platform_error)?;
        if quarantine_second != quarantine_first {
            return Err(ManagedLifecycleReadFailure::InventoryChanged(
                "quarantine inventory changed while entries were opened".to_owned(),
            ));
        }
        Some(QuarantineRead {
            first: quarantine_first,
            opened: quarantine_opened,
            plan: quarantine_plan,
        })
    } else {
        None
    };

    let second = lifecycle
        .enumerate(limits.max_directory_entries)
        .map_err(map_platform_error)?;
    if second != first {
        return Err(ManagedLifecycleReadFailure::InventoryChanged(
            "lifecycle inventory changed while entries were opened".to_owned(),
        ));
    }

    let mut total_read = 0_u64;
    let mut decoded = read_and_decode_inventory(&plan, &mut opened, &mut total_read, limits)?;
    if let Some(quarantine) = &mut quarantine {
        decoded.tail_evidence = read_tail_evidence(
            &quarantine.plan,
            &mut quarantine.opened,
            &mut total_read,
            limits.max_total_read_bytes,
        )?;
    }

    for (entry, handle) in first.entries.iter().zip(&opened) {
        handle.verify(entry).map_err(map_platform_error)?;
    }
    if let Some(quarantine) = &quarantine {
        for (entry, handle) in quarantine.first.entries.iter().zip(&quarantine.opened) {
            handle.verify(entry).map_err(map_platform_error)?;
        }
    }
    before_third_inventory();
    if let Some(index) = plan.quarantine {
        let remaining = limits.max_directory_entries.saturating_sub(first.entries.len());
        let quarantine_third = opened[index].enumerate(remaining).map_err(map_platform_error)?;
        if quarantine
            .as_ref()
            .is_none_or(|expected| quarantine_third != expected.first)
        {
            return Err(ManagedLifecycleReadFailure::InventoryChanged(
                "quarantine inventory changed while evidence was read".to_owned(),
            ));
        }
    }
    let third = lifecycle
        .enumerate(limits.max_directory_entries)
        .map_err(map_platform_error)?;
    if third != first {
        return Err(ManagedLifecycleReadFailure::InventoryChanged(
            "lifecycle inventory changed while sidecars were read".to_owned(),
        ));
    }

    StableInventoryProof::mint(decoded)
        .classify(limits)
        .map(|managed| StableLifecycleInspection::Managed(Box::new(managed)))
}

enum StableLifecycleInspection {
    LegacyAbsent,
    Managed(Box<StableManagedLifecycle>),
}

impl StableLifecycleInspection {
    fn outcome(self) -> ManagedLifecycleReadOutcome {
        match self {
            Self::LegacyAbsent => ManagedLifecycleReadOutcome::LegacyAbsent,
            Self::Managed(managed) => managed.outcome,
        }
    }
}

struct StableManagedLifecycle {
    outcome: ManagedLifecycleReadOutcome,
    store_uuid: Option<crate::mapped_file::retirement::identity::StoreUuid>,
    decision: Option<RecoveryDecision>,
}

impl StableManagedLifecycle {
    const fn recovery_required(
        reason: ManagedLifecycleRecoveryReason,
        store_uuid: Option<crate::mapped_file::retirement::identity::StoreUuid>,
    ) -> Self {
        Self {
            outcome: ManagedLifecycleReadOutcome::RecoveryWriteRequired(reason),
            store_uuid,
            decision: None,
        }
    }
}

#[derive(Debug)]
struct InventoryPlan {
    store_meta: Option<usize>,
    marker: Option<usize>,
    acknowledgement: Option<usize>,
    quarantine: Option<usize>,
    generations: BTreeMap<u64, GenerationPlan>,
    has_temporary: bool,
}

#[derive(Debug, Default)]
struct GenerationPlan {
    snapshot: Option<usize>,
    log: Option<usize>,
}

impl InventoryPlan {
    fn parse(
        inventory: &platform::InventorySnapshot,
        limits: ManagedLifecycleReadLimits,
    ) -> Result<Self, ManagedLifecycleReadFailure> {
        let mut plan = Self {
            store_meta: None,
            marker: None,
            acknowledgement: None,
            quarantine: None,
            generations: BTreeMap::new(),
            has_temporary: false,
        };
        let mut case_folded = BTreeMap::<String, &str>::new();
        let mut physical_files = BTreeMap::<(u64, [u8; 16]), &str>::new();
        for (index, entry) in inventory.entries.iter().enumerate() {
            if entry.kind == platform::EntryKind::Reparse {
                return Err(ManagedLifecycleReadFailure::UnsafeNamespace(format!(
                    "lifecycle entry {:?} is a symlink or reparse point",
                    entry.name
                )));
            }
            let folded = entry.name.to_ascii_lowercase();
            if let Some(previous) = case_folded.insert(folded, &entry.name) {
                return Err(corruption(format!(
                    "case-fold collision between {previous:?} and {:?}",
                    entry.name
                )));
            }
            if entry.kind == platform::EntryKind::File {
                if entry.stamp.link_count != 1 {
                    return Err(ManagedLifecycleReadFailure::UnsafeNamespace(format!(
                        "lifecycle file {:?} has {} hard links; exactly one is required",
                        entry.name, entry.stamp.link_count
                    )));
                }
                let physical_id = (entry.stamp.volume, entry.stamp.file_id);
                if let Some(previous) = physical_files.insert(physical_id, &entry.name) {
                    return Err(ManagedLifecycleReadFailure::UnsafeNamespace(format!(
                        "lifecycle files {previous:?} and {:?} are hard-link aliases",
                        entry.name
                    )));
                }
            }
            match entry.name.as_str() {
                STORE_META_FILE => {
                    require_file(entry)?;
                    plan.store_meta = Some(index);
                }
                ENABLED_MARKER_FILE => {
                    require_file(entry)?;
                    plan.marker = Some(index);
                }
                ACKNOWLEDGEMENT_FILE => {
                    require_file(entry)?;
                    plan.acknowledgement = Some(index);
                }
                QUARANTINE_DIRECTORY => {
                    if entry.kind != platform::EntryKind::Directory {
                        return Err(corruption("quarantine is not a directory"));
                    }
                    plan.quarantine = Some(index);
                }
                name if temporary_name(name) => {
                    require_file(entry)?;
                    plan.has_temporary = true;
                }
                name => {
                    if let Some(generation) = generation_from_name(name, SNAPSHOT_PREFIX) {
                        require_file(entry)?;
                        let pair = plan.generations.entry(generation).or_default();
                        if pair.snapshot.replace(index).is_some() {
                            return Err(corruption("duplicate snapshot generation"));
                        }
                    } else if let Some(generation) = generation_from_name(name, LOG_PREFIX) {
                        require_file(entry)?;
                        let pair = plan.generations.entry(generation).or_default();
                        if pair.log.replace(index).is_some() {
                            return Err(corruption("duplicate log generation"));
                        }
                    } else {
                        return Err(corruption(format!("unexplained lifecycle artifact {name:?}")));
                    }
                }
            }
        }
        if plan.generations.len() > limits.max_generations {
            return Err(limit_error(
                "generations",
                plan.generations.len(),
                limits.max_generations,
            ));
        }
        for (generation, pair) in &plan.generations {
            let invalid_pair = if plan.marker.is_some() {
                pair.snapshot.is_none() || pair.log.is_none()
            } else {
                *generation != 0 || (pair.snapshot.is_some() && pair.log.is_none())
            };
            if invalid_pair {
                return Err(corruption(format!(
                    "generation {generation} is missing its snapshot/log half"
                )));
            }
        }
        Ok(plan)
    }
}

fn require_file(entry: &platform::InventoryEntry) -> Result<(), ManagedLifecycleReadFailure> {
    if entry.kind != platform::EntryKind::File {
        return Err(corruption(format!(
            "lifecycle artifact {:?} is not a regular file",
            entry.name
        )));
    }
    Ok(())
}

fn generation_from_name(name: &str, prefix: &str) -> Option<u64> {
    let digits = name.strip_prefix(prefix)?;
    if digits.len() != GENERATION_DIGITS || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let generation = digits.parse::<u64>().ok()?;
    (format!("{generation:020}") == digits).then_some(generation)
}

fn temporary_name(name: &str) -> bool {
    let Some((base, nonce)) = name.rsplit_once(".tmp.") else {
        return false;
    };
    !base.is_empty()
        && nonce.len() == 32
        && nonce
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

struct DecodedInventory {
    store_meta: Option<StoreMeta>,
    marker: Option<EnabledMarkerFile>,
    acknowledgement: Option<Vec<u8>>,
    generations: Vec<OwnedGeneration>,
    bootstrap_log: Option<Vec<u8>>,
    tail_evidence: Vec<OwnedTailEvidence>,
    has_quarantine: bool,
    has_temporary: bool,
}

/// Private capability minted only after A/B/C inventory equality and retained-handle verification.
struct StableInventoryProof {
    decoded: DecodedInventory,
}

impl StableInventoryProof {
    fn mint(decoded: DecodedInventory) -> Self {
        Self { decoded }
    }

    fn classify(
        self,
        limits: ManagedLifecycleReadLimits,
    ) -> Result<StableManagedLifecycle, ManagedLifecycleReadFailure> {
        self.decoded.classify(limits)
    }
}

struct OwnedGeneration {
    generation: u64,
    snapshot: Vec<u8>,
    log: Vec<u8>,
}

fn read_and_decode_inventory(
    plan: &InventoryPlan,
    opened: &mut [platform::OpenedEntry],
    total_read: &mut u64,
    limits: ManagedLifecycleReadLimits,
) -> Result<DecodedInventory, ManagedLifecycleReadFailure> {
    let store_meta = plan
        .store_meta
        .map(|index| {
            let bytes = read_exact_file(
                &mut opened[index],
                Some(STORE_META_LENGTH),
                STORE_META_LENGTH as u64,
                total_read,
                limits.max_total_read_bytes,
            )?;
            decode_store_meta(&bytes).map_err(map_sidecar_error)
        })
        .transpose()?;
    let marker = plan
        .marker
        .map(|index| {
            let bytes = read_exact_file(
                &mut opened[index],
                Some(ENABLED_MARKER_FILE_LENGTH),
                ENABLED_MARKER_FILE_LENGTH as u64,
                total_read,
                limits.max_total_read_bytes,
            )?;
            decode_enabled_marker_file(&bytes).map_err(map_sidecar_error)
        })
        .transpose()?;
    let acknowledgement = plan
        .acknowledgement
        .map(|index| {
            let bytes = read_exact_file(
                &mut opened[index],
                Some(ACKNOWLEDGEMENT_FILE_LENGTH),
                ACKNOWLEDGEMENT_FILE_LENGTH as u64,
                total_read,
                limits.max_total_read_bytes,
            )?;
            Ok::<_, ManagedLifecycleReadFailure>(bytes)
        })
        .transpose()?;

    let mut generations = Vec::new();
    generations
        .try_reserve_exact(plan.generations.len())
        .map_err(|_| limit_error("generations", plan.generations.len(), limits.max_generations))?;
    let mut bootstrap_log = None;
    for (&generation, pair) in &plan.generations {
        let Some(snapshot_index) = pair.snapshot else {
            let log_index = pair.log.ok_or_else(|| corruption("missing bootstrap log"))?;
            bootstrap_log = Some(read_exact_file(
                &mut opened[log_index],
                None,
                limits.max_log_file_length,
                total_read,
                limits.max_total_read_bytes,
            )?);
            continue;
        };
        let header = validate_snapshot_prefix(&mut opened[snapshot_index])?;
        let snapshot = read_snapshot_file(
            &mut opened[snapshot_index],
            &header,
            total_read,
            limits.max_total_read_bytes,
        )?;
        decode_snapshot(&snapshot).map_err(map_sidecar_error)?;
        let log = pair
            .log
            .map(|log_index| {
                read_exact_file(
                    &mut opened[log_index],
                    None,
                    limits.max_log_file_length,
                    total_read,
                    limits.max_total_read_bytes,
                )
            })
            .transpose()?
            .unwrap_or_default();
        generations.push(OwnedGeneration {
            generation,
            snapshot,
            log,
        });
    }
    Ok(DecodedInventory {
        store_meta,
        marker,
        acknowledgement,
        generations,
        bootstrap_log,
        tail_evidence: Vec::new(),
        has_quarantine: plan.quarantine.is_some(),
        has_temporary: plan.has_temporary,
    })
}

impl DecodedInventory {
    fn classify(
        self,
        limits: ManagedLifecycleReadLimits,
    ) -> Result<StableManagedLifecycle, ManagedLifecycleReadFailure> {
        let Some(marker) = self.marker else {
            if self.has_quarantine || !self.tail_evidence.is_empty() {
                return Err(corruption("quarantine artifacts cannot precede ENABLED.v1"));
            }
            let Some(meta) = self.store_meta else {
                if self.acknowledgement.is_some() || !self.generations.is_empty() || self.bootstrap_log.is_some() {
                    return Err(corruption(
                        "acknowledgement or generation artifacts exist before store.meta",
                    ));
                }
                if self.has_temporary {
                    return Ok(StableManagedLifecycle::recovery_required(
                        ManagedLifecycleRecoveryReason::TemporaryArtifact,
                        None,
                    ));
                }
                return Ok(StableManagedLifecycle::recovery_required(
                    ManagedLifecycleRecoveryReason::BootstrapResume,
                    None,
                ));
            };
            validate_marker_absent_bootstrap(
                &meta,
                self.acknowledgement.as_deref(),
                &self.generations,
                self.bootstrap_log.as_deref(),
                limits,
            )?;
            let reason = if self.has_temporary {
                ManagedLifecycleRecoveryReason::TemporaryArtifact
            } else {
                ManagedLifecycleRecoveryReason::BootstrapResume
            };
            return Ok(StableManagedLifecycle::recovery_required(reason, Some(meta.store_uuid)));
        };
        if self.bootstrap_log.is_some() {
            return Err(corruption("ENABLED.v1 exists with a snapshot-less generation log"));
        }
        let meta = self
            .store_meta
            .ok_or_else(|| corruption("ENABLED.v1 exists without store.meta"))?;
        let acknowledgement = self
            .acknowledgement
            .ok_or_else(|| corruption("ENABLED.v1 exists without ACKNOWLEDGED.v1"))?;
        validate_required_tail_evidence(&self.generations, &self.tail_evidence)?;
        let has_temporary = self.has_temporary;
        let mut generations = Vec::new();
        generations
            .try_reserve_exact(self.generations.len())
            .map_err(|_| limit_error("replay generations", self.generations.len(), limits.max_generations))?;
        for generation in &self.generations {
            generations.push(GenerationBytes {
                generation: generation.generation,
                snapshot: &generation.snapshot,
                log: &generation.log,
            });
        }
        let decision = replay(ReplayInput {
            store_meta: &meta,
            marker: &marker,
            acknowledgement_slots: [
                &acknowledgement[..ACKNOWLEDGEMENT_SLOT_LENGTH],
                &acknowledgement[ACKNOWLEDGEMENT_SLOT_LENGTH..],
            ],
            generations,
            limits: ReplayLimits {
                max_generations: limits.max_generations,
                max_sealed_units: limits.max_sealed_units,
            },
        })
        .map_err(map_replay_error)?;
        let outcome = match &decision {
            RecoveryDecision::NeedsReconciliation(_) => ManagedLifecycleReadOutcome::ManagedNeedsReconciliation,
            RecoveryDecision::AcknowledgeSelectedAnchor(_) => ManagedLifecycleReadOutcome::RecoveryWriteRequired(
                ManagedLifecycleRecoveryReason::AcknowledgeSelectedAnchor,
            ),
            RecoveryDecision::CompleteSeal(_) => {
                ManagedLifecycleReadOutcome::RecoveryWriteRequired(ManagedLifecycleRecoveryReason::CompleteSeal)
            }
            RecoveryDecision::CompleteMarkerWitness(_) => ManagedLifecycleReadOutcome::RecoveryWriteRequired(
                ManagedLifecycleRecoveryReason::CompleteMarkerWitness,
            ),
            RecoveryDecision::TailRepair(_) => {
                ManagedLifecycleReadOutcome::RecoveryWriteRequired(ManagedLifecycleRecoveryReason::TailRepair)
            }
            RecoveryDecision::ResumeGeneration(_) => {
                ManagedLifecycleReadOutcome::RecoveryWriteRequired(ManagedLifecycleRecoveryReason::ResumeGeneration)
            }
        };
        let outcome = if has_temporary && outcome == ManagedLifecycleReadOutcome::ManagedNeedsReconciliation {
            ManagedLifecycleReadOutcome::RecoveryWriteRequired(ManagedLifecycleRecoveryReason::TemporaryArtifact)
        } else {
            outcome
        };
        Ok(StableManagedLifecycle {
            outcome,
            store_uuid: Some(meta.store_uuid),
            decision: Some(decision),
        })
    }
}

fn map_platform_error(error: platform::PlatformFailure) -> ManagedLifecycleReadFailure {
    ManagedLifecycleReadFailure::Platform(error)
}

fn lifecycle_read_store_error(error: ManagedLifecycleReadFailure) -> StoreError {
    let descriptor = match &error {
        ManagedLifecycleReadFailure::Io(_)
        | ManagedLifecycleReadFailure::Platform(platform::PlatformFailure::Io { .. }) => {
            &rocketmq_error::STORAGE_IO_FAILED
        }
        #[cfg(windows)]
        ManagedLifecycleReadFailure::Platform(platform::PlatformFailure::Windows { .. }) => {
            &rocketmq_error::STORAGE_IO_FAILED
        }
        ManagedLifecycleReadFailure::Platform(platform::PlatformFailure::Changed { .. })
        | ManagedLifecycleReadFailure::InventoryChanged(_) => &rocketmq_error::STORAGE_BACKEND_UNAVAILABLE,
        ManagedLifecycleReadFailure::Platform(platform::PlatformFailure::Limit { .. })
        | ManagedLifecycleReadFailure::Limit(_)
        | ManagedLifecycleReadFailure::ReplayLimit(_) => &rocketmq_error::STORAGE_CAPACITY_EXHAUSTED,
        ManagedLifecycleReadFailure::Platform(platform::PlatformFailure::Unsupported) => {
            &rocketmq_error::STORAGE_OPERATION_UNSUPPORTED
        }
        ManagedLifecycleReadFailure::Platform(platform::PlatformFailure::UnsafeNamespace { .. })
        | ManagedLifecycleReadFailure::Sidecar(_)
        | ManagedLifecycleReadFailure::UnknownSidecarVersion(_)
        | ManagedLifecycleReadFailure::Codec(_)
        | ManagedLifecycleReadFailure::UnknownCodecVersion(_)
        | ManagedLifecycleReadFailure::Replay(_)
        | ManagedLifecycleReadFailure::ReplayUnknownVersion(_)
        | ManagedLifecycleReadFailure::Corruption(_)
        | ManagedLifecycleReadFailure::UnsafeNamespace(_)
        | ManagedLifecycleReadFailure::UnknownVersion(_) => &rocketmq_error::STORAGE_STATE_CORRUPTED,
    };
    StoreError::new(descriptor, StoreOperation::Load)
        .in_component(StoreComponent::MappedFile)
        .with_detail("managed lifecycle read failed")
        .with_source(error)
}

#[doc(hidden)]
pub fn inspect_managed_lifecycle_read_only_for_store(
    store_root: &File,
) -> Result<ManagedLifecycleReadOutcome, StoreError> {
    inspect_managed_lifecycle_read_only(store_root).map_err(lifecycle_read_store_error)
}

#[doc(hidden)]
pub fn inspect_managed_lifecycle_read_only_with_limits_for_store(
    store_root: &File,
    limits: ManagedLifecycleReadLimits,
) -> Result<ManagedLifecycleReadOutcome, StoreError> {
    inspect_managed_lifecycle_read_only_with_limits(store_root, limits).map_err(lifecycle_read_store_error)
}

/// Inspects managed lifecycle evidence for the Store owner while retaining its exclusive lease.
///
/// # Safety
///
/// The caller must uphold the same retained-root and exclusive-lock invariants as the checked
/// internal inspector.
#[doc(hidden)]
pub unsafe fn inspect_managed_lifecycle_under_exclusive_lock_for_store(
    store_root: &File,
    exclusive_lease: Arc<dyn Send + Sync>,
) -> Result<LockedManagedLifecycleInspection, StoreError> {
    // SAFETY: the public Store owner is required to uphold the checked inspector's invariants.
    unsafe { inspect_managed_lifecycle_under_exclusive_lock(store_root, exclusive_lease) }
        .map_err(lifecycle_read_store_error)
}

fn map_sidecar_error(error: SidecarViolation) -> ManagedLifecycleReadFailure {
    if matches!(
        error,
        SidecarViolation::UnsupportedVersion { .. } | SidecarViolation::UnsupportedSnapshotEntryVersion { .. }
    ) {
        ManagedLifecycleReadFailure::UnknownSidecarVersion(error)
    } else {
        ManagedLifecycleReadFailure::Sidecar(error)
    }
}

fn map_codec_error(error: CodecViolation) -> ManagedLifecycleReadFailure {
    if matches!(
        error,
        CodecViolation::UnsupportedFormatVersion { .. } | CodecViolation::UnsupportedRecordVersion { .. }
    ) {
        ManagedLifecycleReadFailure::UnknownCodecVersion(error)
    } else {
        ManagedLifecycleReadFailure::Codec(error)
    }
}

fn map_replay_error(error: ReplayViolation) -> ManagedLifecycleReadFailure {
    match &error {
        ReplayViolation::LimitExceeded { .. } => ManagedLifecycleReadFailure::ReplayLimit(error),
        ReplayViolation::Snapshot(
            SidecarViolation::UnsupportedVersion { .. } | SidecarViolation::UnsupportedSnapshotEntryVersion { .. },
        )
        | ReplayViolation::Marker(
            SidecarViolation::UnsupportedVersion { .. } | SidecarViolation::UnsupportedSnapshotEntryVersion { .. },
        )
        | ReplayViolation::InvalidLog {
            source: CodecViolation::UnsupportedFormatVersion { .. } | CodecViolation::UnsupportedRecordVersion { .. },
            ..
        } => ManagedLifecycleReadFailure::ReplayUnknownVersion(error),
        _ => ManagedLifecycleReadFailure::Replay(error),
    }
}

#[cfg(test)]
mod tests;
