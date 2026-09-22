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

use std::collections::HashMap;
use std::ffi::OsString;
use std::fmt;
use std::path::Path;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::error::RuntimeError;
use crate::error::RuntimeResult;
use crate::metadata_io::MetadataGeneration;
use crate::RuntimeOperation;

pub(crate) const DEFAULT_MAX_METADATA_TARGETS: usize = 4_096;

/// Process-local identity of one retained target history.
///
/// Normal actor replacement preserves this identity. Explicit retirement ends
/// that history; generations and receipts from different identities must never
/// be compared as durability evidence. Holding an identity grants no write access.
#[derive(Clone, Debug)]
pub struct MetadataTargetIdentity(Arc<()>);

impl PartialEq for MetadataTargetIdentity {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for MetadataTargetIdentity {}

/// Aggregate registry occupancy without resource names or target paths.
#[derive(Debug, Clone, Copy, serde::Serialize)]
pub struct MetadataTargetRegistryStats {
    /// Maximum retained target histories, including fenced and idle entries.
    pub capacity: usize,
    /// Current retained target histories.
    pub retained_targets: usize,
    /// Histories with at least one live write registration.
    pub live_owners: usize,
    /// Histories without a live writer; this includes idle fenced histories.
    pub idle_histories: usize,
    /// Histories requiring format-specific reconciliation, with or without a writer.
    pub fenced_targets: usize,
    /// Capacity available for distinct target histories.
    pub remaining_capacity: usize,
}

/// Result of explicitly ending a quiescent target's history.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataTargetRetirementOutcome {
    /// History was removed; future actors must acquire a new identity.
    Retired,
    /// This actor has no history for the requested resource.
    NotFound,
    /// The actor can still accept writes.
    AdmissionOpen,
    /// Coordinator work, waiters, or actual write authority remain alive.
    WorkInProgress,
    /// An unconfirmed replacement requires domain-specific recovery.
    ReconciliationRequired,
    /// The registry's identity or durable history differs from this actor's evidence.
    HistoryChanged,
}

/// One process-local registry shared by metadata actors under a `RuntimeOwner`.
#[derive(Clone)]
pub(crate) struct MetadataTargetRegistry {
    inner: Arc<RegistryInner>,
}

struct RegistryInner {
    state: Mutex<RegistryState>,
    next_owner: AtomicU64,
    next_generation: AtomicU64,
    max_entries: usize,
}

#[derive(Default)]
struct RegistryState {
    targets: HashMap<NormalizedMetadataTarget, TargetEntry>,
}

struct TargetEntry {
    identity: MetadataTargetIdentity,
    resource: Arc<str>,
    owner: Option<u64>,
    durable_generation: Option<MetadataGeneration>,
    reconciliation_required: bool,
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct NormalizedMetadataTarget(OsString);

/// A registration that keeps the target write authority alive until every
/// clone, including an in-flight blocking closure, has been destroyed.
#[derive(Clone)]
pub(crate) struct MetadataTargetRegistration {
    inner: Arc<RegistrationInner>,
}

struct RegistrationInner {
    identity: MetadataTargetIdentity,
    registry: MetadataTargetRegistry,
    target: NormalizedMetadataTarget,
    resource: Arc<str>,
    owner: u64,
    durable_generation: Option<MetadataGeneration>,
}

#[derive(Debug)]
pub(crate) enum MetadataTargetRegistrationOutcome {
    Registered(MetadataTargetRegistration),
    Conflict,
    ReconciliationRequired,
}

impl MetadataTargetRegistry {
    #[cfg(test)]
    pub(crate) fn new() -> Self {
        Self::with_max_entries(DEFAULT_MAX_METADATA_TARGETS)
    }

    pub(crate) fn with_max_entries(max_entries: usize) -> Self {
        Self {
            inner: Arc::new(RegistryInner {
                state: Mutex::new(RegistryState::default()),
                next_owner: AtomicU64::new(1),
                next_generation: AtomicU64::new(1),
                max_entries,
            }),
        }
    }

    /// Reserves the next generation for a snapshot admitted under this owner.
    ///
    /// The counter belongs to the owner-scoped registry rather than to one
    /// actor, so a generation remains a usable change identity after an actor
    /// is replaced. A per-actor counter would restart at one and let a rebuilt
    /// actor reuse a generation whose durability is still unconfirmed.
    ///
    /// Values wrap from `u64::MAX` back to one. Gaps are allowed when admission
    /// rejects a request; they do not weaken ordering.
    pub(crate) fn next_generation(&self) -> u64 {
        let mut generation = self.inner.next_generation.load(Ordering::Relaxed);
        loop {
            let next = if generation == u64::MAX { 1 } else { generation + 1 };
            match self.inner.next_generation.compare_exchange_weak(
                generation,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return generation,
                Err(observed) => generation = observed,
            }
        }
    }

    /// Returns whether the target still requires reconciliation.
    fn requires_reconciliation(&self, target: &NormalizedMetadataTarget) -> bool {
        let state = self.inner.state.lock();
        state
            .targets
            .get(target)
            .is_some_and(|entry| entry.reconciliation_required)
    }

    #[cfg(test)]
    pub(crate) fn register(
        &self,
        target: &Path,
        resource: Arc<str>,
    ) -> RuntimeResult<MetadataTargetRegistrationOutcome> {
        self.register_with_identity(target, resource, None)
    }

    pub(crate) fn register_with_identity(
        &self,
        target: &Path,
        resource: Arc<str>,
        expected: Option<&MetadataTargetIdentity>,
    ) -> RuntimeResult<MetadataTargetRegistrationOutcome> {
        let target = normalize_target(target)?;
        let mut state = self.inner.state.lock();
        if expected.is_some_and(|identity| {
            state
                .targets
                .get(&target)
                .is_none_or(|entry| &entry.identity != identity)
        }) {
            return Err(RuntimeError::context_unavailable(
                RuntimeOperation::MetadataResourceTarget,
            ));
        }

        if let Some(entry) = state.targets.get(&target) {
            if entry.reconciliation_required {
                return Ok(MetadataTargetRegistrationOutcome::ReconciliationRequired);
            }
            if entry.resource != resource || entry.owner.is_some() {
                return Ok(MetadataTargetRegistrationOutcome::Conflict);
            }
        } else if state.targets.len() >= self.inner.max_entries {
            return Err(RuntimeError::capacity(RuntimeOperation::AdmitMetadataOperation));
        }

        let owner = self.inner.next_owner.fetch_add(1, Ordering::Relaxed);
        let entry = state.targets.entry(target.clone()).or_insert_with(|| TargetEntry {
            identity: MetadataTargetIdentity(Arc::new(())),
            resource: Arc::clone(&resource),
            owner: None,
            durable_generation: None,
            reconciliation_required: false,
        });
        entry.owner = Some(owner);
        let durable_generation = entry.durable_generation;

        Ok(MetadataTargetRegistrationOutcome::Registered(
            MetadataTargetRegistration {
                inner: Arc::new(RegistrationInner {
                    identity: entry.identity.clone(),
                    registry: self.clone(),
                    target,
                    resource,
                    owner,
                    durable_generation,
                }),
            },
        ))
    }

    pub(crate) fn same_target(&self, left: &Path, right: &Path) -> RuntimeResult<bool> {
        Ok(normalize_target(left)? == normalize_target(right)?)
    }

    pub(crate) fn capacity(&self) -> usize {
        self.inner.max_entries
    }

    pub(crate) fn stats(&self) -> MetadataTargetRegistryStats {
        let state = self.inner.state.lock();
        let live_owners = state.targets.values().filter(|entry| entry.owner.is_some()).count();
        MetadataTargetRegistryStats {
            capacity: self.inner.max_entries,
            retained_targets: state.targets.len(),
            live_owners,
            idle_histories: state.targets.len() - live_owners,
            fenced_targets: state
                .targets
                .values()
                .filter(|entry| entry.reconciliation_required)
                .count(),
            remaining_capacity: self.inner.max_entries.saturating_sub(state.targets.len()),
        }
    }

    pub(crate) fn retire(
        &self,
        target: &Path,
        resource: &str,
        identity: &MetadataTargetIdentity,
        durable_generation: Option<MetadataGeneration>,
    ) -> RuntimeResult<MetadataTargetRetirementOutcome> {
        let target = normalize_target(target)?;
        let mut state = self.inner.state.lock();
        let Some(entry) = state.targets.get(&target) else {
            return Ok(MetadataTargetRetirementOutcome::HistoryChanged);
        };
        if &entry.identity != identity
            || entry.resource.as_ref() != resource
            || entry.durable_generation != durable_generation
        {
            return Ok(MetadataTargetRetirementOutcome::HistoryChanged);
        }
        if entry.reconciliation_required {
            return Ok(MetadataTargetRetirementOutcome::ReconciliationRequired);
        }
        if entry.owner.is_some() {
            return Ok(MetadataTargetRetirementOutcome::WorkInProgress);
        }
        state.targets.remove(&target);
        Ok(MetadataTargetRetirementOutcome::Retired)
    }

    fn record_durable(
        &self,
        target: &NormalizedMetadataTarget,
        resource: &Arc<str>,
        owner: u64,
        generation: MetadataGeneration,
    ) -> bool {
        let mut state = self.inner.state.lock();
        let Some(entry) = state.targets.get_mut(target) else {
            return false;
        };
        if entry.resource != *resource || entry.owner != Some(owner) {
            return false;
        }
        entry.durable_generation = Some(
            entry
                .durable_generation
                .map_or(generation, |durable| durable.max(generation)),
        );
        true
    }

    fn release(&self, target: &NormalizedMetadataTarget, owner: u64) {
        let mut state = self.inner.state.lock();
        let Some(entry) = state.targets.get_mut(target) else {
            return;
        };
        if entry.owner == Some(owner) {
            entry.owner = None;
        }
    }

    fn record_reconciliation_required(
        &self,
        target: &NormalizedMetadataTarget,
        resource: &Arc<str>,
        owner: u64,
    ) -> bool {
        let mut state = self.inner.state.lock();
        let Some(entry) = state.targets.get_mut(target) else {
            return false;
        };
        if entry.resource != *resource || entry.owner != Some(owner) {
            return false;
        }
        entry.reconciliation_required = true;
        true
    }
}

impl MetadataTargetRegistration {
    pub(crate) fn identity(&self) -> MetadataTargetIdentity {
        self.inner.identity.clone()
    }
    pub(crate) fn durable_generation(&self) -> Option<MetadataGeneration> {
        self.inner.durable_generation
    }

    pub(crate) fn record_durable(&self, generation: MetadataGeneration) -> bool {
        self.inner
            .registry
            .record_durable(&self.inner.target, &self.inner.resource, self.inner.owner, generation)
    }

    pub(crate) fn record_reconciliation_required(&self) -> bool {
        self.inner
            .registry
            .record_reconciliation_required(&self.inner.target, &self.inner.resource, self.inner.owner)
    }

    /// Returns whether the bound target still requires reconciliation.
    ///
    /// The flag describes the *target*, not this writer: an earlier generation
    /// may have replaced the file without completing the durability protocol.
    /// A caller that reuses a cached registration must consult it before
    /// exercising write authority again, because the registry conflict check
    /// is skipped on that path.
    pub(crate) fn reconciliation_required(&self) -> bool {
        self.inner.registry.requires_reconciliation(&self.inner.target)
    }
}

impl Drop for RegistrationInner {
    fn drop(&mut self) {
        self.registry.release(&self.target, self.owner);
    }
}

impl fmt::Debug for MetadataTargetRegistry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let entries = self.inner.state.lock().targets.len();
        formatter
            .debug_struct("MetadataTargetRegistry")
            .field("entries", &entries)
            .field("max_entries", &self.inner.max_entries)
            .finish()
    }
}

impl fmt::Debug for MetadataTargetRegistration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MetadataTargetRegistration")
            .field("resource", &self.inner.resource)
            .finish_non_exhaustive()
    }
}

impl fmt::Debug for NormalizedMetadataTarget {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("<metadata-target>")
    }
}

fn normalize_target(target: &Path) -> RuntimeResult<NormalizedMetadataTarget> {
    let absolute = std::path::absolute(target)
        .map_err(|source| RuntimeError::io(RuntimeOperation::MetadataResourceTarget, source))?;

    #[cfg(windows)]
    {
        let folded = absolute.as_os_str().to_string_lossy().to_lowercase();
        Ok(NormalizedMetadataTarget(OsString::from(folded)))
    }

    #[cfg(not(windows))]
    {
        Ok(NormalizedMetadataTarget(absolute.into_os_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn target_generations_are_allocated_by_the_owner_scoped_registry() {
        let registry = MetadataTargetRegistry::new();
        assert_eq!(registry.next_generation(), 1);
        assert_eq!(registry.next_generation(), 2);
        assert_eq!(registry.next_generation(), 3);

        // A clone shares the counter, so two actors derived from one owner
        // cannot reuse a generation whose durability is still unconfirmed.
        let shared = registry.clone();
        assert_eq!(shared.next_generation(), 4);
        assert_eq!(registry.next_generation(), 5);
    }

    #[test]
    fn registration_reports_the_target_reconciliation_fence() {
        let registry = MetadataTargetRegistry::new();
        let target = std::env::temp_dir().join("rocketmq-runtime-registration-fence");
        let outcome = registry.register(&target, Arc::<str>::from("resource")).unwrap();
        let MetadataTargetRegistrationOutcome::Registered(registration) = outcome else {
            panic!("a free target should register");
        };

        assert!(!registration.reconciliation_required());
        assert!(registration.record_reconciliation_required());
        assert!(
            registration.reconciliation_required(),
            "a cached registration must observe an unconfirmed replacement"
        );
    }

    #[test]
    fn different_resources_cannot_bind_the_same_target() {
        let registry = MetadataTargetRegistry::new();
        let target = std::env::temp_dir().join("rocketmq-runtime-target-registry");

        let first = registry.register(&target, Arc::<str>::from("first")).unwrap();
        assert!(matches!(first, MetadataTargetRegistrationOutcome::Registered(_)));
        assert!(matches!(
            registry.register(&target, Arc::<str>::from("second")).unwrap(),
            MetadataTargetRegistrationOutcome::Conflict
        ));
    }

    #[test]
    fn target_registry_capacity_is_bounded() {
        let registry = MetadataTargetRegistry::with_max_entries(1);
        let first = std::env::temp_dir().join("rocketmq-runtime-target-capacity-first");
        let second = std::env::temp_dir().join("rocketmq-runtime-target-capacity-second");

        assert!(matches!(
            registry.register(&first, Arc::<str>::from("first")).unwrap(),
            MetadataTargetRegistrationOutcome::Registered(_)
        ));
        let error = registry
            .register(&second, Arc::<str>::from("second"))
            .expect_err("target registration capacity must be bounded");
        assert_eq!(error.condition(), rocketmq_error::CanonicalCondition::ResourceExhausted);
    }

    #[test]
    fn registration_release_and_durable_generation_outlive_replacement() {
        let registry = MetadataTargetRegistry::new();
        let target = std::env::temp_dir().join("rocketmq-runtime-target-replacement");
        let resource = Arc::<str>::from("resource");

        let first = match registry.register(&target, resource.clone()).unwrap() {
            MetadataTargetRegistrationOutcome::Registered(registration) => registration,
            MetadataTargetRegistrationOutcome::Conflict => panic!("first registration must succeed"),
            MetadataTargetRegistrationOutcome::ReconciliationRequired => {
                panic!("first registration requires no reconciliation")
            }
        };
        assert!(first.record_durable(MetadataGeneration::new(7)));
        assert!(matches!(
            registry.register(&target, resource.clone()).unwrap(),
            MetadataTargetRegistrationOutcome::Conflict
        ));

        drop(first);
        let replacement = match registry.register(&target, resource).unwrap() {
            MetadataTargetRegistrationOutcome::Registered(registration) => registration,
            MetadataTargetRegistrationOutcome::Conflict => panic!("replacement registration must succeed"),
            MetadataTargetRegistrationOutcome::ReconciliationRequired => {
                panic!("replacement registration requires no reconciliation")
            }
        };
        assert_eq!(replacement.durable_generation(), Some(MetadataGeneration::new(7)));
    }

    #[test]
    fn registration_owner_is_released_only_after_every_clone_is_dropped() {
        let registry = MetadataTargetRegistry::new();
        let target = std::env::temp_dir().join("rocketmq-runtime-target-clone");
        let resource = Arc::<str>::from("resource");

        let first = match registry.register(&target, resource.clone()).unwrap() {
            MetadataTargetRegistrationOutcome::Registered(registration) => registration,
            MetadataTargetRegistrationOutcome::Conflict => panic!("first registration must succeed"),
            MetadataTargetRegistrationOutcome::ReconciliationRequired => {
                panic!("first registration requires no reconciliation")
            }
        };
        let cloned = first.clone();
        drop(first);
        assert!(matches!(
            registry.register(&target, resource.clone()).unwrap(),
            MetadataTargetRegistrationOutcome::Conflict
        ));

        drop(cloned);
        assert!(matches!(
            registry.register(&target, resource).unwrap(),
            MetadataTargetRegistrationOutcome::Registered(_)
        ));
    }

    #[test]
    fn same_resource_can_switch_targets_after_the_writer_is_released() {
        let registry = MetadataTargetRegistry::new();
        let first_target = std::env::temp_dir().join("rocketmq-runtime-target-first");
        let second_target = std::env::temp_dir().join("rocketmq-runtime-target-second");
        let resource = Arc::<str>::from("resource");

        let first = match registry.register(&first_target, resource.clone()).unwrap() {
            MetadataTargetRegistrationOutcome::Registered(registration) => registration,
            MetadataTargetRegistrationOutcome::Conflict => panic!("first registration must succeed"),
            MetadataTargetRegistrationOutcome::ReconciliationRequired => {
                panic!("first registration requires no reconciliation")
            }
        };
        drop(first);

        assert!(matches!(
            registry.register(&second_target, resource).unwrap(),
            MetadataTargetRegistrationOutcome::Registered(_)
        ));
    }

    #[test]
    fn unknown_commit_outcome_blocks_re_registration() {
        let registry = MetadataTargetRegistry::new();
        let target = std::env::temp_dir().join("rocketmq-runtime-target-reconcile");
        let resource = Arc::<str>::from("resource");

        let registration = match registry.register(&target, resource.clone()).unwrap() {
            MetadataTargetRegistrationOutcome::Registered(registration) => registration,
            MetadataTargetRegistrationOutcome::Conflict => panic!("registration must succeed"),
            MetadataTargetRegistrationOutcome::ReconciliationRequired => {
                panic!("a fresh target must not require reconciliation")
            }
        };
        assert!(registration.record_reconciliation_required());
        drop(registration);
        assert!(matches!(
            registry.register(&target, resource).unwrap(),
            MetadataTargetRegistrationOutcome::ReconciliationRequired
        ));
    }

    #[test]
    fn retirement_checks_live_authority_and_history_before_reusing_capacity() {
        let registry = MetadataTargetRegistry::with_max_entries(1);
        let path = std::env::temp_dir().join("retirement-history");
        let resource = Arc::<str>::from("resource");
        let MetadataTargetRegistrationOutcome::Registered(first) = registry.register(&path, resource.clone()).unwrap()
        else {
            panic!("free target");
        };
        let identity = first.identity();
        let target = first.inner.target.clone();
        let old_owner = first.inner.owner;
        let closure = first.clone();
        assert!(first.record_durable(MetadataGeneration::new(1)));
        drop(first);
        assert_eq!(
            registry.retire(&path, &resource, &identity, None).unwrap(),
            MetadataTargetRetirementOutcome::HistoryChanged
        );
        assert_eq!(
            registry
                .retire(&path, &resource, &identity, Some(MetadataGeneration::new(1)))
                .unwrap(),
            MetadataTargetRetirementOutcome::WorkInProgress
        );
        drop(closure);
        assert_eq!(
            registry
                .retire(&path, &resource, &identity, Some(MetadataGeneration::new(1)))
                .unwrap(),
            MetadataTargetRetirementOutcome::Retired
        );
        let MetadataTargetRegistrationOutcome::Registered(next) = registry.register(&path, resource.clone()).unwrap()
        else {
            panic!("retired capacity");
        };
        assert_ne!(identity, next.identity());
        assert!(!registry.record_durable(&target, &resource, old_owner, MetadataGeneration::new(99)));
        assert!(!registry.record_reconciliation_required(&target, &resource, old_owner));
        assert_eq!(next.durable_generation(), None);
        assert!(!next.reconciliation_required());
        assert_eq!(registry.stats().retained_targets, 1);
    }

    #[cfg(windows)]
    #[test]
    fn windows_target_identity_is_case_insensitive() {
        let registry = MetadataTargetRegistry::new();
        let lower = std::env::temp_dir().join("rocketmq-runtime-target-case");
        let upper = std::env::temp_dir().join("ROCKETMQ-RUNTIME-TARGET-CASE");

        assert!(matches!(
            registry.register(&lower, Arc::<str>::from("first")).unwrap(),
            MetadataTargetRegistrationOutcome::Registered(_)
        ));
        assert!(matches!(
            registry.register(&upper, Arc::<str>::from("second")).unwrap(),
            MetadataTargetRegistrationOutcome::Conflict
        ));
    }
}
