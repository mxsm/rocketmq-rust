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

const DEFAULT_MAX_METADATA_TARGETS: usize = 4_096;

/// One process-local registry shared by metadata actors under a `RuntimeOwner`.
#[derive(Clone)]
pub(crate) struct MetadataTargetRegistry {
    inner: Arc<RegistryInner>,
}

struct RegistryInner {
    state: Mutex<RegistryState>,
    next_owner: AtomicU64,
    max_entries: usize,
}

#[derive(Default)]
struct RegistryState {
    targets: HashMap<NormalizedMetadataTarget, TargetEntry>,
}

struct TargetEntry {
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
    pub(crate) fn new() -> Self {
        Self::with_max_entries(DEFAULT_MAX_METADATA_TARGETS)
    }

    fn with_max_entries(max_entries: usize) -> Self {
        Self {
            inner: Arc::new(RegistryInner {
                state: Mutex::new(RegistryState::default()),
                next_owner: AtomicU64::new(1),
                max_entries,
            }),
        }
    }

    pub(crate) fn register(
        &self,
        target: &Path,
        resource: Arc<str>,
    ) -> RuntimeResult<MetadataTargetRegistrationOutcome> {
        let target = normalize_target(target)?;
        let mut state = self.inner.state.lock();

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
                    registry: self.clone(),
                    target,
                    resource,
                    owner,
                    durable_generation,
                }),
            },
        ))
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
