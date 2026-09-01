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

use std::fmt;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;

use thiserror::Error;

use super::ManagedLifecycleRuntime;
use super::ManagedRetirementCore;
use crate::base::transient_store_pool::TransientStorePool;
use crate::mapped_file::retirement::identity::FileIncarnationId;
use crate::mapped_file::retirement::identity::IdentityViolation;
use crate::mapped_file::retirement::identity::StoreRelativePath;
use crate::mapped_file::retirement::identity::StoreUuid;
use crate::mapped_file::retirement::io::LedgerIo;
use crate::mapped_file::retirement::platform::IncarnationCreationError;
use crate::mapped_file::retirement::platform::VerifiedNamespaceRoot;
use crate::mapped_file::retirement::registry::CreationPublicationFailure;
use crate::mapped_file::retirement::registry::ManagedMappedFileQueueGeneration;
use crate::mapped_file::retirement::registry::RegistryViolation;
use crate::mapped_file::retirement::writer::IncarnationAllocationPlan;
use crate::mapped_file::retirement::writer::IncarnationWriteError;
use crate::mapped_file::DefaultMappedFile;

/// Validated request for one new managed mapped-file incarnation.
#[doc(hidden)]
pub struct ManagedIncarnationCreateRequest {
    directory: StoreRelativePath,
    segment_offset: u64,
    expected_length: u64,
    create_nonce: [u8; 16],
    transient_store_pool: Option<TransientStorePool>,
}

impl fmt::Debug for ManagedIncarnationCreateRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ManagedIncarnationCreateRequest")
            .field("directory", &self.directory)
            .field("segment_offset", &self.segment_offset)
            .field("expected_length", &self.expected_length)
            .field("has_transient_store_pool", &self.transient_store_pool.is_some())
            .finish_non_exhaustive()
    }
}

impl ManagedIncarnationCreateRequest {
    /// Validates namespace-independent creation fields before any ledger I/O.
    #[allow(
        clippy::result_large_err,
        reason = "the merged namespace outcome intentionally retains typed proof and disposition data"
    )]
    pub(crate) fn new(
        directory: &str,
        segment_offset: u64,
        expected_length: u64,
        create_nonce: [u8; 16],
    ) -> Result<Self, ManagedIncarnationCreationError> {
        if expected_length == 0 {
            return Err(ManagedIncarnationCreationError::preflight(
                "managed creation expected length is zero",
            ));
        }
        if create_nonce == [0; 16] {
            return Err(ManagedIncarnationCreationError::preflight(
                "managed creation nonce is zero",
            ));
        }
        let directory = StoreRelativePath::new(directory).map_err(ManagedIncarnationCreationError::identity)?;
        Ok(Self {
            directory,
            segment_offset,
            expected_length,
            create_nonce,
            transient_store_pool: None,
        })
    }

    /// Attaches the Store-owned transient pool; its borrowed buffer remains RAII-managed.
    pub fn with_transient_store_pool(mut self, pool: TransientStorePool) -> Self {
        self.transient_store_pool = Some(pool);
        self
    }
}

/// Newly published owner already visible in its managed queue and registry.
#[doc(hidden)]
pub struct ManagedIncarnationCreation {
    incarnation: FileIncarnationId,
    mapped_file: Arc<DefaultMappedFile>,
}

impl fmt::Debug for ManagedIncarnationCreation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ManagedIncarnationCreation")
            .field("incarnation", &self.incarnation)
            .finish_non_exhaustive()
    }
}

impl ManagedIncarnationCreation {
    pub fn create_sequence(&self) -> u64 {
        self.incarnation.create_seq()
    }

    pub const fn mapped_file(&self) -> &Arc<DefaultMappedFile> {
        &self.mapped_file
    }

    pub fn into_mapped_file(self) -> Arc<DefaultMappedFile> {
        self.mapped_file
    }
}

/// Private incarnation-creation orchestration leaf with its former kind folded in.
#[derive(Debug, Error)]
pub(crate) enum ManagedIncarnationCreationError {
    #[error("{0}")]
    Preflight(&'static str),
    #[error("managed lifecycle admission is closed")]
    AdmissionClosed,
    #[error("managed lifecycle requires replay before another creation")]
    RecoveryRequired,
    #[error("managed create sequence domain is exhausted")]
    SequenceExhausted,
    #[error(transparent)]
    Identity(#[from] IdentityViolation),
    #[error(transparent)]
    Writer(#[from] IncarnationWriteError),
    #[error(transparent)]
    Namespace(#[from] IncarnationCreationError),
    #[error("managed mapping construction failed: {0}")]
    Mapping(#[source] io::Error),
    #[error(transparent)]
    Registry(#[from] RegistryViolation),
}

impl ManagedIncarnationCreationError {
    fn preflight(reason: &'static str) -> Self {
        Self::Preflight(reason)
    }

    fn identity(source: IdentityViolation) -> Self {
        Self::Identity(source)
    }

    fn recovery_required() -> Self {
        Self::RecoveryRequired
    }
}

pub(super) struct ManagedCreationContext {
    store_root: PathBuf,
    store_uuid: StoreUuid,
    create_high_water: u64,
}

impl ManagedLifecycleRuntime {
    /// Creates and publishes one managed mapped file inside a single synchronous lifecycle lock.
    ///
    /// This method performs blocking ledger and filesystem I/O. Store code must execute it through
    /// the storage `BlockingExecutor`, exactly like retirement submission and reaper batches.
    #[allow(
        clippy::result_large_err,
        reason = "the merged namespace outcome intentionally retains typed proof and disposition data"
    )]
    pub(crate) fn create_mapped_file(
        &self,
        queue: &ManagedMappedFileQueueGeneration<DefaultMappedFile>,
        request: ManagedIncarnationCreateRequest,
    ) -> Result<ManagedIncarnationCreation, ManagedIncarnationCreationError> {
        let mut inner = self.inner.lock();
        if inner.admission != super::RuntimeAdmission::Running {
            return Err(ManagedIncarnationCreationError::AdmissionClosed);
        }
        inner.core.create_mapped_file(queue, request)
    }
}

impl<I: LedgerIo> ManagedRetirementCore<I, VerifiedNamespaceRoot, DefaultMappedFile> {
    pub(super) fn configure_creation(&mut self, store_root: PathBuf, store_uuid: StoreUuid, create_high_water: u64) {
        self.creation = Some(ManagedCreationContext {
            store_root,
            store_uuid,
            create_high_water,
        });
    }

    #[allow(
        clippy::result_large_err,
        reason = "the merged namespace outcome intentionally retains typed proof and disposition data"
    )]
    fn create_mapped_file(
        &mut self,
        queue: &ManagedMappedFileQueueGeneration<DefaultMappedFile>,
        request: ManagedIncarnationCreateRequest,
    ) -> Result<ManagedIncarnationCreation, ManagedIncarnationCreationError> {
        if self.recovery_required || self.registry.needs_recovery() {
            return Err(ManagedIncarnationCreationError::recovery_required());
        }
        let (store_root, store_uuid, create_high_water) = self
            .creation
            .as_ref()
            .map(|context| {
                (
                    context.store_root.clone(),
                    context.store_uuid,
                    context.create_high_water,
                )
            })
            .ok_or_else(|| ManagedIncarnationCreationError::preflight("managed creation context is not configured"))?;
        let next_create_sequence = create_high_water
            .checked_add(1)
            .ok_or(ManagedIncarnationCreationError::SequenceExhausted)?;
        let incarnation = FileIncarnationId::new(store_uuid, next_create_sequence)
            .map_err(ManagedIncarnationCreationError::identity)?;
        let canonical_path = StoreRelativePath::new(&format!(
            "{}/{:020}",
            request.directory.as_str(),
            request.segment_offset
        ))
        .map_err(ManagedIncarnationCreationError::identity)?;
        let create_file_path = canonical_path
            .create_file_path(incarnation, request.segment_offset, &request.create_nonce)
            .map_err(ManagedIncarnationCreationError::identity)?;
        let plan = IncarnationAllocationPlan::new(
            incarnation,
            request.segment_offset,
            request.expected_length,
            request.create_nonce,
            canonical_path.clone(),
            create_file_path,
        )
        .map_err(ManagedIncarnationCreationError::Writer)?;

        let allocated = self.writer.append_allocate_incarnation(plan).map_err(|source| {
            self.recovery_required = true;
            ManagedIncarnationCreationError::Writer(source)
        })?;
        let Some(context) = self.creation.as_mut() else {
            self.recovery_required = true;
            return Err(ManagedIncarnationCreationError::recovery_required());
        };
        context.create_high_water = next_create_sequence;

        let created = self.namespace.create_incarnation_temp(&allocated).map_err(|source| {
            self.recovery_required = true;
            ManagedIncarnationCreationError::Namespace(source)
        })?;
        let physical_key = created.physical_key();
        let bound = self
            .writer
            .append_bind_incarnation(allocated, physical_key)
            .map_err(|source| {
                self.recovery_required = true;
                ManagedIncarnationCreationError::Writer(source)
            })?;
        let verified = self
            .namespace
            .publish_bound_incarnation(created, &bound)
            .map_err(|source| {
                self.recovery_required = true;
                ManagedIncarnationCreationError::Namespace(source)
            })?;
        let published = self.writer.append_publish_incarnation(bound).map_err(|source| {
            self.recovery_required = true;
            ManagedIncarnationCreationError::Writer(source)
        })?;

        let mapped_file = DefaultMappedFile::try_new_managed_created(
            canonical_path.join_under(&store_root),
            published.segment_offset(),
            published.expected_length(),
            verified.physical_key(),
            verified.into_file(),
            request.transient_store_pool,
        )
        .map(Arc::new)
        .map_err(|source| {
            self.recovery_required = true;
            ManagedIncarnationCreationError::Mapping(source)
        })?;
        let mapping_generation = mapped_file.current_mapping_generation_id().ok_or_else(|| {
            self.recovery_required = true;
            ManagedIncarnationCreationError::Mapping(io::Error::other(
                "new managed mapped file has no published mapping generation",
            ))
        })?;
        let returned_owner = Arc::clone(&mapped_file);
        queue
            .publish_created_member(&self.registry, published, mapped_file, mapping_generation)
            .map_err(|failure| self.publication_failure(failure))?;
        Ok(ManagedIncarnationCreation {
            incarnation,
            mapped_file: returned_owner,
        })
    }

    fn publication_failure(
        &mut self,
        failure: CreationPublicationFailure<DefaultMappedFile>,
    ) -> ManagedIncarnationCreationError {
        self.recovery_required = true;
        let (_receipt, _owner, source) = failure.into_parts();
        ManagedIncarnationCreationError::Registry(source)
    }

    #[cfg(test)]
    fn creation_high_water_for_test(&self) -> Option<u64> {
        self.creation.as_ref().map(|context| context.create_high_water)
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    #[cfg(windows)]
    use std::fs::OpenOptions;

    use tempfile::TempDir;

    use super::super::ManagedRetirementCore;
    use crate::mapped_file::retirement::identity::StoreUuid;
    use crate::mapped_file::retirement::platform::VerifiedNamespaceRoot;
    use crate::mapped_file::retirement::registry::ManagedMappedFileQueueGeneration;
    use crate::mapped_file::retirement::registry::RetirementRegistry;
    use crate::mapped_file::retirement::service::ManagedIncarnationCreateRequest;
    use crate::mapped_file::retirement::writer::model_io::ModelLedgerIo;
    use crate::mapped_file::retirement::writer::ManagedLedgerWriter;
    use crate::mapped_file::DefaultMappedFile;
    use crate::mapped_file::MappedFile;

    #[test]
    fn durable_creation_enters_registry_and_queue_only_after_publish() {
        let mut fixture = Fixture::new();
        let (mut core, queue) = fixture.core(0);
        let request =
            ManagedIncarnationCreateRequest::new("commitlog", 0, 4096, [0x51; 16]).expect("creation request is valid");

        let created = core
            .create_mapped_file(&queue, request)
            .expect("managed creation completes");

        assert_eq!(created.create_sequence(), 1);
        assert_eq!(created.mapped_file().get_file_size(), 4096);
        assert_eq!(queue.snapshot().len(), 1);
        assert!(std::sync::Arc::ptr_eq(&queue.snapshot()[0], created.mapped_file()));
        assert_eq!(core.registry().retained_identity_count(), 1);
        assert_eq!(core.creation_high_water_for_test(), Some(1));
        assert!(!core.report(std::time::Instant::now(), 0, 0).recovery_required());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn durable_creation_builds_a_new_queue_directory_under_the_retained_root() {
        let mut fixture = Fixture::new_without_queue_directory();
        let (mut core, queue) = fixture.core(0);
        let request = ManagedIncarnationCreateRequest::new("consumequeue/topic-a/3", 0, 4096, [0x54; 16])
            .expect("creation request is valid");

        core.create_mapped_file(&queue, request)
            .expect("managed creation builds missing queue directories safely");

        assert!(fixture
            .root
            .path()
            .join("consumequeue/topic-a/3/00000000000000000000")
            .is_file());
    }

    #[test]
    fn failure_after_allocate_advances_high_water_and_recovery_fences_creation() {
        let mut fixture = Fixture::new();
        std::fs::write(fixture.root.path().join("commitlog/00000000000000000000"), b"collision")
            .expect("install canonical collision");
        let (mut core, queue) = fixture.core(0);
        let request =
            ManagedIncarnationCreateRequest::new("commitlog", 0, 4096, [0x52; 16]).expect("creation request is valid");

        let first = core
            .create_mapped_file(&queue, request)
            .expect_err("namespace collision follows durable Allocate");
        assert!(matches!(first, super::ManagedIncarnationCreationError::Namespace(_)));
        assert_eq!(core.creation_high_water_for_test(), Some(1));
        assert!(core.report(std::time::Instant::now(), 0, 0).recovery_required());
        assert!(queue.snapshot().is_empty());
        assert_eq!(core.registry().retained_identity_count(), 0);

        let retry = ManagedIncarnationCreateRequest::new("commitlog", 1_000_000, 4096, [0x53; 16])
            .expect("retry request is valid");
        let error = core
            .create_mapped_file(&queue, retry)
            .expect_err("replay fence rejects a second allocation");
        assert!(matches!(
            error,
            super::ManagedIncarnationCreationError::RecoveryRequired
        ));
    }

    struct Fixture {
        root: TempDir,
        namespace: Option<VerifiedNamespaceRoot>,
    }

    impl Fixture {
        fn new() -> Self {
            let root = tempfile::tempdir().expect("create temporary Store root");
            std::fs::create_dir(root.path().join("commitlog")).expect("create commitlog directory");
            Self::from_root(root)
        }

        fn new_without_queue_directory() -> Self {
            Self::from_root(tempfile::tempdir().expect("create temporary Store root"))
        }

        fn from_root(root: TempDir) -> Self {
            let handle = open_root_handle(root.path()).expect("open Store root handle");
            let namespace = VerifiedNamespaceRoot::open(handle, store_uuid()).expect("verify Store root");
            Self {
                root,
                namespace: Some(namespace),
            }
        }

        fn core(
            &mut self,
            create_high_water: u64,
        ) -> (
            ManagedRetirementCore<ModelLedgerIo, VerifiedNamespaceRoot, DefaultMappedFile>,
            ManagedMappedFileQueueGeneration<DefaultMappedFile>,
        ) {
            let registry = RetirementRegistry::new_for_test(store_uuid(), 0);
            let writer =
                ManagedLedgerWriter::for_test(ModelLedgerIo::empty(), store_uuid(), [0x61; 16], 2, 100, 77, 0, true, 5)
                    .expect("managed writer is valid");
            let mut core = ManagedRetirementCore::new(
                registry,
                writer,
                self.namespace.take().expect("namespace is available"),
                Vec::new(),
                std::time::Instant::now(),
            );
            core.configure_creation(self.root.path().to_path_buf(), store_uuid(), create_high_water);
            let queue = ManagedMappedFileQueueGeneration::from_reconciled_members(Vec::new())
                .expect("empty managed queue generation is valid");
            (core, queue)
        }
    }

    fn store_uuid() -> StoreUuid {
        StoreUuid::new([0x41; 16]).expect("Store UUID is nonzero")
    }

    #[cfg(windows)]
    fn open_root_handle(path: &std::path::Path) -> std::io::Result<File> {
        use std::os::windows::fs::OpenOptionsExt;

        use windows::Win32::Storage::FileSystem::FILE_FLAG_BACKUP_SEMANTICS;
        use windows::Win32::Storage::FileSystem::FILE_FLAG_OPEN_REPARSE_POINT;
        use windows::Win32::Storage::FileSystem::FILE_SHARE_DELETE;
        use windows::Win32::Storage::FileSystem::FILE_SHARE_READ;
        use windows::Win32::Storage::FileSystem::FILE_SHARE_WRITE;

        OpenOptions::new()
            .read(true)
            .share_mode(FILE_SHARE_READ.0 | FILE_SHARE_WRITE.0 | FILE_SHARE_DELETE.0)
            .custom_flags(FILE_FLAG_BACKUP_SEMANTICS.0 | FILE_FLAG_OPEN_REPARSE_POINT.0)
            .open(path)
    }

    #[cfg(not(windows))]
    fn open_root_handle(path: &std::path::Path) -> std::io::Result<File> {
        File::open(path)
    }
}
