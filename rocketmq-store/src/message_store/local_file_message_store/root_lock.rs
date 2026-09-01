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

use std::fs::File;
use std::io;
use std::io::Write;
use std::ops::Deref;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use fs2::FileExt;
use rocketmq_store_local::mapped_file::bootstrap_managed_lifecycle_under_exclusive_lock;
use rocketmq_store_local::mapped_file::inspect_managed_lifecycle_read_only_for_store;
use rocketmq_store_local::mapped_file::inspect_managed_lifecycle_under_exclusive_lock_for_store;
use rocketmq_store_local::mapped_file::LockedManagedLifecycleInspection;
use rocketmq_store_local::mapped_file::ManagedLifecycleReadOutcome;
use rocketmq_store_local::mapped_file::ManagedLifecycleRecoveryReason;

use crate::store_error::StoreComponent;
use crate::store_error::StoreError;
use crate::store_error::StoreOperation;

const LOCK_FILE_NAME: &str = "lock";
const LIFECYCLE_DIRECTORY_NAME: &str = ".rocketmq-lifecycle";
const ABORT_FILE_NAME: &str = "abort";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct FileIdentity {
    volume: u64,
    file_id: [u8; 16],
}

/// Lifecycle protocol selected for one exclusively leased Store root.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum StoreRootMode {
    Legacy,
    Managed,
}

impl StoreRootMode {
    pub(super) fn from_read_outcome(
        outcome: ManagedLifecycleReadOutcome,
    ) -> Result<Self, ManagedLifecycleRecoveryReason> {
        match outcome {
            ManagedLifecycleReadOutcome::LegacyAbsent => Ok(Self::Legacy),
            ManagedLifecycleReadOutcome::ManagedNeedsReconciliation => Ok(Self::Managed),
            ManagedLifecycleReadOutcome::RecoveryWriteRequired(reason) => Err(reason),
        }
    }
}

/// Exclusive lease for one verified Store root.
///
/// The root and lock handles remain owned for the complete lifetime of the Store. Every operation
/// reopens the configured root without following links, compares its physical identity, verifies
/// that the same ordinary lock file is still bound below it, and classifies lifecycle evidence
/// relative to the retained root handle.
pub(super) struct StoreRootLease {
    inner: Arc<StoreRootLeaseInner>,
}

pub(super) struct StoreRootLeaseInner {
    configured_root: PathBuf,
    root: File,
    root_identity: FileIdentity,
    lock: File,
    lock_identity: FileIdentity,
    #[cfg(test)]
    before_abort_remove_hook: std::sync::Mutex<Option<Box<dyn FnOnce() + Send>>>,
}

impl Deref for StoreRootLease {
    type Target = StoreRootLeaseInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl StoreRootLease {
    pub(super) fn acquire(configured_root: &Path, operation: StoreOperation) -> Result<Self, StoreError> {
        let lease = Self::acquire_unclassified(configured_root, operation)?;
        lease.validate_legacy(operation)?;
        Ok(lease)
    }

    pub(super) fn classify(&self, operation: StoreOperation) -> Result<StoreRootMode, StoreError> {
        let outcome = self.lifecycle_outcome(operation)?;
        StoreRootMode::from_read_outcome(outcome)
            .map_err(|reason| managed_lifecycle_fence(operation, recovery_requirement(reason)))
    }

    pub(super) fn lifecycle_outcome(
        &self,
        operation: StoreOperation,
    ) -> Result<ManagedLifecycleReadOutcome, StoreError> {
        self.validate_root_binding(operation)?;
        inspect_managed_lifecycle_read_only_for_store(&self.root)
    }

    pub(super) fn bootstrap_managed_lifecycle(&self, operation: StoreOperation) -> Result<(), StoreError> {
        self.validate_root_binding(operation)?;
        // SAFETY: this lease retains the exact no-follow Store-root and lock handles, owns the
        // exclusive lock, and revalidated both configured-path bindings immediately above. Store
        // construction has not published or started any legacy component.
        unsafe { bootstrap_managed_lifecycle_under_exclusive_lock(&self.root) }?;
        self.validate_root_binding(operation)
    }

    pub(super) fn acquire_unclassified(configured_root: &Path, operation: StoreOperation) -> Result<Self, StoreError> {
        let root = platform::open_root(configured_root, true).map_err(|error| {
            root_io_error(
                operation,
                format!("failed to open verified Store root {}", configured_root.display()),
                error,
            )
        })?;
        platform::verify_root_directory(&root).map_err(|error| {
            corruption_error(
                operation,
                format!(
                    "configured Store root is not a real no-follow directory: {}",
                    configured_root.display()
                ),
                error,
            )
        })?;
        let root_identity = platform::file_identity(&root).map_err(|error| {
            corruption_error(
                operation,
                format!("failed to identify configured Store root {}", configured_root.display()),
                error,
            )
        })?;

        let mut lock = platform::open_lock_file(&root, true).map_err(|error| {
            root_io_error(
                operation,
                format!(
                    "failed to open no-follow Store lock under {}",
                    configured_root.display()
                ),
                error,
            )
        })?;
        platform::verify_lock_file(&lock).map_err(|error| {
            corruption_error(
                operation,
                format!(
                    "Store lock is not an ordinary no-follow file under {}",
                    configured_root.display()
                ),
                error,
            )
        })?;
        let lock_identity = platform::file_identity(&lock).map_err(|error| {
            corruption_error(
                operation,
                format!("failed to identify Store lock under {}", configured_root.display()),
                error,
            )
        })?;
        lock.try_lock_exclusive().map_err(|error| {
            StoreError::new(&rocketmq_error::STORAGE_BACKEND_UNAVAILABLE, operation)
                .with_detail(format!(
                    "message store lock file is held by another instance: {}",
                    configured_root.join(LOCK_FILE_NAME).display()
                ))
                .with_source(error)
        })?;

        // The target has already been opened without following links, verified as an ordinary
        // file, physically identified, and exclusively locked. Only now may diagnostic content be
        // replaced.
        lock.set_len(0).map_err(|error| {
            StoreError::new(&rocketmq_error::STORAGE_IO_FAILED, operation)
                .with_detail(format!(
                    "failed to truncate Store lock under {}",
                    configured_root.display()
                ))
                .with_source(error)
        })?;
        writeln!(lock, "pid={}", std::process::id()).map_err(|error| {
            StoreError::new(&rocketmq_error::STORAGE_IO_FAILED, operation)
                .with_detail(format!(
                    "failed to write Store lock under {}",
                    configured_root.display()
                ))
                .with_source(error)
        })?;

        Ok(Self {
            inner: Arc::new(StoreRootLeaseInner {
                configured_root: configured_root.to_path_buf(),
                root,
                root_identity,
                lock,
                lock_identity,
                #[cfg(test)]
                before_abort_remove_hook: std::sync::Mutex::new(None),
            }),
        })
    }

    fn validate_root_binding(&self, operation: StoreOperation) -> Result<(), StoreError> {
        let current_root = platform::open_root(&self.configured_root, false).map_err(|error| {
            corruption_error(
                operation,
                format!(
                    "configured Store root disappeared or became unsafe: {}",
                    self.configured_root.display()
                ),
                error,
            )
        })?;
        platform::verify_root_directory(&current_root).map_err(|error| {
            corruption_error(
                operation,
                format!(
                    "configured Store root is no longer a real directory: {}",
                    self.configured_root.display()
                ),
                error,
            )
        })?;
        let current_root_identity = platform::file_identity(&current_root).map_err(|error| {
            corruption_error(
                operation,
                format!(
                    "failed to re-identify configured Store root {}",
                    self.configured_root.display()
                ),
                error,
            )
        })?;
        if current_root_identity != self.root_identity {
            return Err(StoreError::new(&rocketmq_error::STORAGE_STATE_CORRUPTED, operation)
                .in_component(StoreComponent::MappedFile)
                .with_detail(format!(
                    "configured Store root was replaced while its lease remained active: {}",
                    self.configured_root.display()
                )));
        }

        let current_lock = platform::open_lock_file(&self.root, false).map_err(|error| {
            corruption_error(
                operation,
                format!("Store lock binding changed under {}", self.configured_root.display()),
                error,
            )
        })?;
        platform::verify_lock_file(&current_lock).map_err(|error| {
            corruption_error(
                operation,
                format!(
                    "Store lock is no longer an ordinary file under {}",
                    self.configured_root.display()
                ),
                error,
            )
        })?;
        let current_lock_identity = platform::file_identity(&current_lock).map_err(|error| {
            corruption_error(
                operation,
                format!(
                    "failed to re-identify Store lock under {}",
                    self.configured_root.display()
                ),
                error,
            )
        })?;
        if current_lock_identity != self.lock_identity {
            return Err(StoreError::new(&rocketmq_error::STORAGE_STATE_CORRUPTED, operation)
                .in_component(StoreComponent::MappedFile)
                .with_detail(format!(
                    "Store lock file was replaced while its lease remained active: {}",
                    self.configured_root.join(LOCK_FILE_NAME).display()
                )));
        }

        Ok(())
    }

    pub(super) fn validate_legacy(&self, operation: StoreOperation) -> Result<(), StoreError> {
        self.validate_root_binding(operation)?;

        match inspect_managed_lifecycle_read_only_for_store(&self.root) {
            Ok(ManagedLifecycleReadOutcome::LegacyAbsent) => Ok(()),
            Ok(ManagedLifecycleReadOutcome::ManagedNeedsReconciliation) => Err(managed_lifecycle_fence(
                operation,
                "reconciliation before segment publication",
            )),
            Ok(ManagedLifecycleReadOutcome::RecoveryWriteRequired(reason)) => {
                Err(managed_lifecycle_fence(operation, recovery_requirement(reason)))
            }
            Err(error) => Err(error),
        }
    }

    pub(super) fn validate_mode(&self, mode: StoreRootMode, operation: StoreOperation) -> Result<(), StoreError> {
        let actual = self.classify(operation)?;
        if actual == mode {
            return Ok(());
        }
        Err(StoreError::new(&rocketmq_error::STORAGE_STATE_CORRUPTED, operation)
            .in_component(StoreComponent::MappedFile)
            .with_detail(format!(
                "Store root lifecycle mode changed while its exclusive lease remained active: expected {mode:?}, observed {actual:?}"
            )))
    }

    pub(super) fn inspect_managed_lifecycle(
        &self,
        operation: StoreOperation,
    ) -> Result<LockedManagedLifecycleInspection, StoreError> {
        self.validate_root_binding(operation)?;
        let exclusive_lease: Arc<dyn Send + Sync> = self.inner.clone();
        // SAFETY: this lease owns the exact no-follow root and ordinary lock handles, holds the
        // exclusive lock, and revalidated both configured-path bindings immediately above. The
        // opaque Arc passed to store-local owns those handles and therefore keeps every invariant
        // alive until the returned session and all capabilities derived from it are dropped.
        unsafe { inspect_managed_lifecycle_under_exclusive_lock_for_store(&self.root, exclusive_lease) }
    }

    pub(super) fn abort_marker_present(&self, operation: StoreOperation) -> Result<bool, StoreError> {
        platform::abort_marker_present(&self.root).map_err(|error| {
            root_io_error(
                operation,
                format!(
                    "failed to inspect abort marker relative to retained Store root {}",
                    self.configured_root.join(ABORT_FILE_NAME).display()
                ),
                error,
            )
        })
    }

    pub(super) fn create_abort_marker(&self, operation: StoreOperation, contents: &[u8]) -> Result<(), StoreError> {
        platform::create_abort_marker(&self.root, contents).map_err(|error| {
            root_io_error(
                operation,
                format!(
                    "failed to create abort marker relative to retained Store root {}",
                    self.configured_root.join(ABORT_FILE_NAME).display()
                ),
                error,
            )
        })
    }

    pub(super) fn remove_abort_marker(&self, operation: StoreOperation) -> Result<(), StoreError> {
        #[cfg(test)]
        {
            let hook = self
                .before_abort_remove_hook
                .lock()
                .expect("abort-remove test hook mutex poisoned")
                .take();
            if let Some(hook) = hook {
                hook();
            }
        }

        platform::remove_abort_marker(&self.root).map_err(|error| {
            root_io_error(
                operation,
                format!(
                    "failed to remove abort marker relative to retained Store root {}",
                    self.configured_root.join(ABORT_FILE_NAME).display()
                ),
                error,
            )
        })
    }

    #[cfg(test)]
    pub(super) fn set_before_abort_remove_hook_for_testing(&self, hook: impl FnOnce() + Send + 'static) {
        *self
            .before_abort_remove_hook
            .lock()
            .expect("abort-remove test hook mutex poisoned") = Some(Box::new(hook));
    }
}

impl Drop for StoreRootLeaseInner {
    fn drop(&mut self) {
        let _ = self.lock.sync_all();
        let _ = FileExt::unlock(&self.lock);
    }
}

fn root_io_error(operation: StoreOperation, detail: String, error: io::Error) -> StoreError {
    if platform::is_unsafe_path_error(&error) {
        corruption_error(operation, detail, error)
    } else {
        StoreError::new(&rocketmq_error::STORAGE_IO_FAILED, operation)
            .in_component(StoreComponent::MappedFile)
            .with_detail(detail)
            .with_source(error)
    }
}

fn corruption_error(operation: StoreOperation, detail: String, error: io::Error) -> StoreError {
    StoreError::new(&rocketmq_error::STORAGE_STATE_CORRUPTED, operation)
        .in_component(StoreComponent::MappedFile)
        .with_detail(detail)
        .with_source(error)
}

fn managed_lifecycle_fence(operation: StoreOperation, requirement: &'static str) -> StoreError {
    StoreError::new(&rocketmq_error::STORAGE_OPERATION_UNSUPPORTED, operation)
        .in_component(StoreComponent::MappedFile)
        .with_detail(format!(
            "managed mapped-file lifecycle state requires {requirement}; \
             legacy numeric Store loading is fenced while lifecycle writes remain disabled"
        ))
}

const fn recovery_requirement(reason: ManagedLifecycleRecoveryReason) -> &'static str {
    match reason {
        ManagedLifecycleRecoveryReason::BootstrapResume => "bootstrap recovery",
        ManagedLifecycleRecoveryReason::AcknowledgeSelectedAnchor => "acknowledgement-anchor recovery",
        ManagedLifecycleRecoveryReason::CompleteSeal => "commit-seal completion",
        ManagedLifecycleRecoveryReason::CompleteMarkerWitness => "marker-witness completion",
        ManagedLifecycleRecoveryReason::TailRepair => "tail repair",
        ManagedLifecycleRecoveryReason::ResumeGeneration => "generation recovery",
        ManagedLifecycleRecoveryReason::TemporaryArtifact => "temporary-artifact recovery",
    }
}

#[cfg(unix)]
#[path = "root_lock/unix.rs"]
mod platform;

#[cfg(windows)]
#[path = "root_lock/windows.rs"]
mod platform;

#[cfg(not(any(unix, windows)))]
#[path = "root_lock/unsupported.rs"]
mod platform;

#[cfg(test)]
#[path = "root_lock/unsupported.rs"]
mod unsupported_contract;

#[cfg(test)]
mod unsupported_contract_tests {
    use std::fs::File;
    use std::io;

    use super::unsupported_contract;

    #[test]
    fn unsupported_platform_operations_fail_closed_instead_of_panicking() {
        let file = File::open(std::env::current_exe().expect("resolve current test executable"))
            .expect("open current test executable");

        assert_unsupported(unsupported_contract::verify_root_directory(&file));
        assert_unsupported(unsupported_contract::open_lock_file(&file, false));
        assert_unsupported(unsupported_contract::verify_lock_file(&file));
        assert_unsupported(unsupported_contract::file_identity(&file));
        assert_unsupported(unsupported_contract::abort_marker_present(&file));
        assert_unsupported(unsupported_contract::create_abort_marker(&file, b"test"));
        assert_unsupported(unsupported_contract::remove_abort_marker(&file));
    }

    fn assert_unsupported<T>(result: io::Result<T>) {
        let error = match result {
            Ok(_) => panic!("unsupported Store root operation must fail"),
            Err(error) => error,
        };
        assert_eq!(io::ErrorKind::Unsupported, error.kind());
    }
}
