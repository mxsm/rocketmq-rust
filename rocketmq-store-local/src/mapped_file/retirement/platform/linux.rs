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

use std::ffi::CString;
use std::fs::File;
use std::io;
use std::mem::size_of;
use std::os::fd::AsRawFd;
use std::os::fd::FromRawFd;
use std::os::unix::fs::MetadataExt;

use super::engine::BackendFailure;
use super::engine::EntryObservation;
use super::engine::NamespaceIo;
use super::engine::NamespaceSnapshot;
use super::physical_key;
use super::types::NamespaceEntry;
use super::types::NamespaceFailureClass;
use super::types::NamespaceOperation;
use super::types::NamespacePolicyViolation;
use super::types::NamespaceRetirementRequest;
use super::types::NamespaceTransition;
use super::types::NamespaceTransitionOutcome;
use crate::mapped_file::retirement::identity::PhysicalFileKey;
use crate::mapped_file::retirement::identity::StoreRelativePath;
use crate::mapped_file::retirement::writer::AllocatedIncarnationReceipt;
use crate::mapped_file::retirement::writer::BoundIncarnationReceipt;

use super::creation::IncarnationCreationError;
use super::creation::IncarnationCreationStage;

const STRICT_RESOLVE: u64 =
    libc::RESOLVE_BENEATH | libc::RESOLVE_NO_MAGICLINKS | libc::RESOLVE_NO_SYMLINKS | libc::RESOLVE_NO_XDEV;
const OPENAT2_UNAVAILABLE: &str = "strict openat2 resolution is unavailable";

/// Version-zero `struct open_how` passed with its exact 24-byte ABI size.
#[repr(C)]
struct OpenHow {
    flags: u64,
    mode: u64,
    resolve: u64,
}

pub(super) struct NamespaceRoot {
    file: File,
    device: u64,
}

impl NamespaceRoot {
    #[allow(
        clippy::result_large_err,
        reason = "the merged namespace outcome intentionally retains typed proof and disposition data"
    )]
    pub(super) fn open(file: File) -> Result<Self, NamespaceTransitionOutcome> {
        let metadata = file
            .metadata()
            .map_err(|error| classify_io(NamespaceOperation::VerifyRoot, error).into_verification_error())?;
        if !metadata.is_dir() {
            return Err(NamespaceTransitionOutcome::Rejected(
                NamespacePolicyViolation::RootIsNotDirectory,
            ));
        }
        Ok(Self {
            file,
            device: metadata.dev(),
        })
    }

    #[allow(
        clippy::result_large_err,
        reason = "the merged namespace outcome intentionally retains typed proof and disposition data"
    )]
    pub(super) fn reserve(
        &self,
        request: &NamespaceRetirementRequest,
        _transition: NamespaceTransition,
    ) -> Result<NamespaceReservation, NamespaceTransitionOutcome> {
        if !matches!(request.physical_key(), PhysicalFileKey::Unix(_)) {
            return Err(NamespaceTransitionOutcome::Rejected(
                NamespacePolicyViolation::PhysicalKeyPlatformMismatch,
            ));
        }
        let (parent_path, canonical_name) = split_parent(request.canonical_path().as_str());
        let (tombstone_parent, tombstone_name) = split_parent(request.tombstone_path().as_str());
        if parent_path != tombstone_parent {
            return Err(NamespaceTransitionOutcome::Rejected(
                NamespacePolicyViolation::ParentEscapedRoot,
            ));
        }
        let parent = open_parent_strict(&self.file, parent_path)?;
        let metadata = parent
            .metadata()
            .map_err(|error| classify_io(NamespaceOperation::OpenParent, error).into_verification_error())?;
        if !metadata.is_dir() || metadata.dev() != self.device {
            return Err(NamespaceTransitionOutcome::Rejected(
                NamespacePolicyViolation::ParentEscapedRoot,
            ));
        }
        Ok(NamespaceReservation {
            parent,
            canonical_name: CString::new(canonical_name)
                .map_err(|_| NamespaceTransitionOutcome::Rejected(NamespacePolicyViolation::ParentEscapedRoot))?,
            tombstone_name: CString::new(tombstone_name)
                .map_err(|_| NamespaceTransitionOutcome::Rejected(NamespacePolicyViolation::ParentEscapedRoot))?,
            canonical: None,
            tombstone: None,
        })
    }

    #[allow(
        clippy::result_large_err,
        reason = "the merged namespace outcome intentionally retains typed proof and disposition data"
    )]
    pub(super) fn open_active_segment(
        &self,
        path: &StoreRelativePath,
        expected_key: PhysicalFileKey,
        expected_length: u64,
    ) -> Result<File, NamespaceTransitionOutcome> {
        if !matches!(expected_key, PhysicalFileKey::Unix(_)) {
            return Err(NamespaceTransitionOutcome::Rejected(
                NamespacePolicyViolation::PhysicalKeyPlatformMismatch,
            ));
        }
        let (parent_path, file_name) = split_parent(path.as_str());
        let parent = open_parent_strict(&self.file, parent_path)?;
        let parent_metadata = parent
            .metadata()
            .map_err(|error| classify_io(NamespaceOperation::OpenParent, error).into_verification_error())?;
        if !parent_metadata.is_dir() || parent_metadata.dev() != self.device {
            return Err(NamespaceTransitionOutcome::Rejected(
                NamespacePolicyViolation::ParentEscapedRoot,
            ));
        }
        let file_name = CString::new(file_name)
            .map_err(|_| NamespaceTransitionOutcome::Rejected(NamespacePolicyViolation::ParentEscapedRoot))?;
        let file = openat2(&parent, &file_name, libc::O_RDWR | libc::O_CLOEXEC | libc::O_NOFOLLOW)
            .map_err(|error| classify_io(NamespaceOperation::VerifyCanonical, error).into_verification_error())?;
        let metadata = file
            .metadata()
            .map_err(|error| classify_io(NamespaceOperation::VerifyCanonical, error).into_verification_error())?;
        if !metadata.is_file() || metadata.dev() != self.device {
            return Err(NamespaceTransitionOutcome::Rejected(
                NamespacePolicyViolation::UnexpectedEntryType {
                    entry: NamespaceEntry::Canonical,
                },
            ));
        }
        if metadata.len() != expected_length {
            return Err(NamespaceTransitionOutcome::Rejected(
                NamespacePolicyViolation::ExpectedLengthMismatch {
                    entry: NamespaceEntry::Canonical,
                    expected: expected_length,
                    actual: metadata.len(),
                },
            ));
        }
        let actual_key = physical_key::capture(&file)
            .map_err(|error| classify_io(NamespaceOperation::VerifyCanonical, error).into_verification_error())?;
        if actual_key != expected_key {
            return Err(NamespaceTransitionOutcome::Rejected(
                NamespacePolicyViolation::NamespaceChangedDuringVerification,
            ));
        }
        Ok(file)
    }

    pub(super) fn create_incarnation_temp(
        &self,
        allocated: &AllocatedIncarnationReceipt,
    ) -> Result<CreatedIncarnationTemp, IncarnationCreationError> {
        let (parent_path, canonical_name) = split_parent(allocated.canonical_path().as_str());
        let (create_parent, create_name) = split_parent(allocated.create_file_path().as_str());
        if parent_path != create_parent {
            return Err(IncarnationCreationError::policy(
                IncarnationCreationStage::VerifyNames,
                "canonical and create-file paths have different parents",
            ));
        }
        let parent = open_or_create_parent_strict(&self.file, parent_path)
            .map_err(|error| IncarnationCreationError::namespace(IncarnationCreationStage::OpenParent, error))?;
        let metadata = parent
            .metadata()
            .map_err(|error| IncarnationCreationError::io(IncarnationCreationStage::OpenParent, error))?;
        if !metadata.is_dir() || metadata.dev() != self.device {
            return Err(IncarnationCreationError::policy(
                IncarnationCreationStage::OpenParent,
                "create-file parent escaped the retained Store filesystem",
            ));
        }
        let canonical_name = CString::new(canonical_name).map_err(|_| {
            IncarnationCreationError::policy(IncarnationCreationStage::VerifyNames, "canonical name contains NUL")
        })?;
        let create_name = CString::new(create_name).map_err(|_| {
            IncarnationCreationError::policy(IncarnationCreationStage::VerifyNames, "create name contains NUL")
        })?;
        require_missing(&parent, &canonical_name, IncarnationCreationStage::VerifyNames)?;
        require_missing(&parent, &create_name, IncarnationCreationStage::VerifyNames)?;

        let file = openat2_with_mode(
            &parent,
            &create_name,
            libc::O_RDWR | libc::O_CREAT | libc::O_EXCL | libc::O_CLOEXEC | libc::O_NOFOLLOW,
            0o600,
        )
        .map_err(|error| IncarnationCreationError::io(IncarnationCreationStage::CreateTemp, error))?;
        file.set_len(allocated.expected_length())
            .map_err(|error| IncarnationCreationError::io(IncarnationCreationStage::SizeTemp, error))?;
        file.sync_all()
            .map_err(|error| IncarnationCreationError::io(IncarnationCreationStage::SyncTemp, error))?;
        let physical_key = physical_key::capture(&file)
            .map_err(|error| IncarnationCreationError::io(IncarnationCreationStage::CapturePhysicalKey, error))?;
        if !matches!(physical_key, PhysicalFileKey::Unix(_)) {
            return Err(IncarnationCreationError::policy(
                IncarnationCreationStage::CapturePhysicalKey,
                "created file returned a non-Unix physical key",
            ));
        }
        Ok(CreatedIncarnationTemp {
            parent,
            file,
            canonical_name,
            create_name,
            physical_key,
            expected_length: allocated.expected_length(),
        })
    }

    pub(super) fn publish_bound_incarnation(
        &self,
        created: CreatedIncarnationTemp,
        bound: &BoundIncarnationReceipt,
    ) -> Result<(File, PhysicalFileKey), IncarnationCreationError> {
        if bound.physical_key() != created.physical_key || bound.expected_length() != created.expected_length {
            return Err(IncarnationCreationError::policy(
                IncarnationCreationStage::VerifyNames,
                "BindIncarnation differs from the created file",
            ));
        }
        let (bound_parent, bound_canonical_name) = split_parent(bound.canonical_path().as_str());
        let (bound_create_parent, bound_create_name) = split_parent(bound.create_file_path().as_str());
        if bound_parent != bound_create_parent
            || created.canonical_name.as_bytes() != bound_canonical_name.as_bytes()
            || created.create_name.as_bytes() != bound_create_name.as_bytes()
        {
            return Err(IncarnationCreationError::policy(
                IncarnationCreationStage::VerifyNames,
                "BindIncarnation paths differ from the created file",
            ));
        }

        // SAFETY: both names are validated single components relative to the same retained parent;
        // RENAME_NOREPLACE prevents replacing a concurrently installed canonical incarnation.
        let result = unsafe {
            libc::syscall(
                libc::SYS_renameat2,
                created.parent.as_raw_fd(),
                created.create_name.as_ptr(),
                created.parent.as_raw_fd(),
                created.canonical_name.as_ptr(),
                libc::RENAME_NOREPLACE,
            )
        };
        if result != 0 {
            return Err(IncarnationCreationError::io(
                IncarnationCreationStage::RenameNoReplace,
                io::Error::last_os_error(),
            ));
        }
        created
            .parent
            .sync_all()
            .map_err(|error| IncarnationCreationError::io(IncarnationCreationStage::SyncParent, error))?;
        drop(created.file);

        let canonical = openat2(
            &created.parent,
            &created.canonical_name,
            libc::O_RDWR | libc::O_CLOEXEC | libc::O_NOFOLLOW,
        )
        .map_err(|error| IncarnationCreationError::io(IncarnationCreationStage::ReopenCanonical, error))?;
        let metadata = canonical
            .metadata()
            .map_err(|error| IncarnationCreationError::io(IncarnationCreationStage::VerifyCanonical, error))?;
        let reopened_key = physical_key::capture(&canonical)
            .map_err(|error| IncarnationCreationError::io(IncarnationCreationStage::VerifyCanonical, error))?;
        if !metadata.is_file() || metadata.len() != created.expected_length || reopened_key != created.physical_key {
            return Err(IncarnationCreationError::policy(
                IncarnationCreationStage::VerifyCanonical,
                "reopened canonical file differs from the durable binding",
            ));
        }
        require_missing(
            &created.parent,
            &created.create_name,
            IncarnationCreationStage::VerifyCanonical,
        )?;
        Ok((canonical, reopened_key))
    }
}

pub(super) struct CreatedIncarnationTemp {
    parent: File,
    file: File,
    canonical_name: CString,
    create_name: CString,
    physical_key: PhysicalFileKey,
    expected_length: u64,
}

impl CreatedIncarnationTemp {
    pub(super) const fn physical_key(&self) -> PhysicalFileKey {
        self.physical_key
    }
}

pub(super) struct NamespaceReservation {
    parent: File,
    canonical_name: CString,
    tombstone_name: CString,
    canonical: Option<File>,
    tombstone: Option<File>,
}

impl NamespaceIo for NamespaceReservation {
    fn snapshot(
        &mut self,
        expected_key: PhysicalFileKey,
        expected_length: u64,
    ) -> Result<NamespaceSnapshot, BackendFailure> {
        self.release_for_reverification();
        let canonical = observe_entry(
            &self.parent,
            &self.canonical_name,
            expected_key,
            expected_length,
            NamespaceOperation::VerifyCanonical,
        )?;
        let tombstone = observe_entry(
            &self.parent,
            &self.tombstone_name,
            expected_key,
            expected_length,
            NamespaceOperation::VerifyTombstone,
        )?;
        self.canonical = canonical.handle;
        self.tombstone = tombstone.handle;
        Ok(NamespaceSnapshot {
            canonical: canonical.observation,
            tombstone: tombstone.observation,
        })
    }

    fn rename_to_tombstone(&mut self) -> Result<(), BackendFailure> {
        if self.canonical.is_none() {
            return Err(BackendFailure::failed(
                NamespaceOperation::Rename,
                NamespaceFailureClass::OtherIo,
                None,
            ));
        }
        // SAFETY: both names are validated single C-string components and both directory
        // descriptors are the same retained verified parent. RENAME_NOREPLACE forbids collision.
        let result = unsafe {
            libc::syscall(
                libc::SYS_renameat2,
                self.parent.as_raw_fd(),
                self.canonical_name.as_ptr(),
                self.parent.as_raw_fd(),
                self.tombstone_name.as_ptr(),
                libc::RENAME_NOREPLACE,
            )
        };
        if result == 0 {
            Ok(())
        } else {
            Err(classify_io(NamespaceOperation::Rename, io::Error::last_os_error()))
        }
    }

    fn unlink(&mut self, entry: NamespaceEntry) -> Result<(), BackendFailure> {
        let (name, handle_present) = match entry {
            NamespaceEntry::Canonical => (&self.canonical_name, self.canonical.is_some()),
            NamespaceEntry::Tombstone => (&self.tombstone_name, self.tombstone.is_some()),
        };
        if !handle_present {
            return Err(BackendFailure::failed(
                NamespaceOperation::Unlink,
                NamespaceFailureClass::OtherIo,
                None,
            ));
        }
        // SAFETY: `name` is one validated component relative to the retained verified parent. Zero
        // flags can unlink only a non-directory entry and never requests recursive removal.
        let result = unsafe { libc::unlinkat(self.parent.as_raw_fd(), name.as_ptr(), 0) };
        if result == 0 {
            Ok(())
        } else {
            Err(classify_io(NamespaceOperation::Unlink, io::Error::last_os_error()))
        }
    }

    fn sync_after_namespace(&mut self, _transition: NamespaceTransition) -> Result<(), BackendFailure> {
        self.parent
            .sync_all()
            .map_err(|error| classify_io(NamespaceOperation::SyncParentOrHandle, error))
    }

    fn release_for_reverification(&mut self) {
        self.canonical = None;
        self.tombstone = None;
    }
}

struct ObservedEntry {
    observation: EntryObservation,
    handle: Option<File>,
}

fn observe_entry(
    parent: &File,
    name: &CString,
    expected_key: PhysicalFileKey,
    expected_length: u64,
    operation: NamespaceOperation,
) -> Result<ObservedEntry, BackendFailure> {
    let file = match openat2(parent, name, libc::O_PATH | libc::O_CLOEXEC | libc::O_NOFOLLOW) {
        Ok(file) => file,
        Err(error) if error.raw_os_error() == Some(libc::ENOENT) => {
            return Ok(ObservedEntry {
                observation: EntryObservation::Missing,
                handle: None,
            });
        }
        Err(error) if error.raw_os_error() == Some(libc::ELOOP) => {
            return Ok(ObservedEntry {
                observation: EntryObservation::ReparsePoint,
                handle: None,
            });
        }
        Err(error) => return Err(classify_io(operation, error)),
    };
    let metadata = file.metadata().map_err(|error| classify_io(operation, error))?;
    if !metadata.is_file() {
        return Ok(ObservedEntry {
            observation: EntryObservation::Directory,
            handle: Some(file),
        });
    }
    let key = physical_key::capture(&file).map_err(|error| classify_io(operation, error))?;
    let observation = if key == expected_key {
        if metadata.len() == expected_length {
            EntryObservation::ExpectedFile
        } else {
            EntryObservation::ExpectedFileWrongLength(metadata.len())
        }
    } else {
        EntryObservation::OtherFile(key)
    };
    Ok(ObservedEntry {
        observation,
        handle: Some(file),
    })
}

#[allow(
    clippy::result_large_err,
    reason = "the merged namespace outcome intentionally retains typed proof and disposition data"
)]
fn open_parent_strict(root: &File, parent_path: &str) -> Result<File, NamespaceTransitionOutcome> {
    let path = CString::new(if parent_path.is_empty() { "." } else { parent_path })
        .map_err(|_| NamespaceTransitionOutcome::Rejected(NamespacePolicyViolation::ParentEscapedRoot))?;
    match openat2(
        root,
        &path,
        libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
    ) {
        Ok(parent) => Ok(parent),
        Err(error) if error.raw_os_error() == Some(libc::ENOSYS) => Err(NamespaceTransitionOutcome::Unsupported {
            platform: "linux",
            reason: OPENAT2_UNAVAILABLE,
        }),
        Err(error) if error.raw_os_error() == Some(libc::EXDEV) || error.raw_os_error() == Some(libc::ELOOP) => Err(
            NamespaceTransitionOutcome::Rejected(NamespacePolicyViolation::ParentEscapedRoot),
        ),
        Err(error) => Err(classify_io(NamespaceOperation::OpenParent, error).into_verification_error()),
    }
}

#[allow(
    clippy::result_large_err,
    reason = "the merged namespace outcome intentionally retains typed proof and disposition data"
)]
fn open_or_create_parent_strict(root: &File, parent_path: &str) -> Result<File, NamespaceTransitionOutcome> {
    let root_device = root
        .metadata()
        .map_err(|error| classify_io(NamespaceOperation::VerifyRoot, error).into_verification_error())?
        .dev();
    if parent_path.is_empty() {
        return root
            .try_clone()
            .map_err(|error| classify_io(NamespaceOperation::OpenParent, error).into_verification_error());
    }

    let mut parent = root
        .try_clone()
        .map_err(|error| classify_io(NamespaceOperation::OpenParent, error).into_verification_error())?;
    for component in parent_path.split('/') {
        if component.is_empty() || component == "." || component == ".." {
            return Err(NamespaceTransitionOutcome::Rejected(
                NamespacePolicyViolation::ParentEscapedRoot,
            ));
        }
        let component = CString::new(component)
            .map_err(|_| NamespaceTransitionOutcome::Rejected(NamespacePolicyViolation::ParentEscapedRoot))?;
        let next = match openat2(
            &parent,
            &component,
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
        ) {
            Ok(next) => next,
            Err(error) if error.raw_os_error() == Some(libc::ENOENT) => {
                // SAFETY: `component` is one validated relative name and `parent` is a retained,
                // verified directory handle beneath the Store root. mkdirat cannot traverse it.
                let result = unsafe { libc::mkdirat(parent.as_raw_fd(), component.as_ptr(), 0o700) };
                if result != 0 {
                    let error = io::Error::last_os_error();
                    if error.raw_os_error() != Some(libc::EEXIST) {
                        return Err(classify_io(NamespaceOperation::OpenParent, error).into_verification_error());
                    }
                }
                parent.sync_all().map_err(|error| {
                    classify_io(NamespaceOperation::SyncParentOrHandle, error).into_verification_error()
                })?;
                openat2(
                    &parent,
                    &component,
                    libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
                )
                .map_err(|error| classify_io(NamespaceOperation::OpenParent, error).into_verification_error())?
            }
            Err(error) if error.raw_os_error() == Some(libc::ENOSYS) => {
                return Err(NamespaceTransitionOutcome::Unsupported {
                    platform: "linux",
                    reason: OPENAT2_UNAVAILABLE,
                });
            }
            Err(error) if error.raw_os_error() == Some(libc::EXDEV) || error.raw_os_error() == Some(libc::ELOOP) => {
                return Err(NamespaceTransitionOutcome::Rejected(
                    NamespacePolicyViolation::ParentEscapedRoot,
                ));
            }
            Err(error) => {
                return Err(classify_io(NamespaceOperation::OpenParent, error).into_verification_error());
            }
        };
        let metadata = next
            .metadata()
            .map_err(|error| classify_io(NamespaceOperation::OpenParent, error).into_verification_error())?;
        if !metadata.is_dir() || metadata.dev() != root_device {
            return Err(NamespaceTransitionOutcome::Rejected(
                NamespacePolicyViolation::ParentEscapedRoot,
            ));
        }
        parent = next;
    }
    Ok(parent)
}

fn openat2(parent: &File, path: &CString, flags: i32) -> io::Result<File> {
    openat2_with_mode(parent, path, flags, 0)
}

fn openat2_with_mode(parent: &File, path: &CString, flags: i32, mode: u32) -> io::Result<File> {
    let how = OpenHow {
        flags: flags as u64,
        mode: u64::from(mode),
        resolve: STRICT_RESOLVE,
    };
    // SAFETY: `path` is NUL-terminated, `how` is the kernel ABI structure with the exact supplied
    // size, and the retained parent descriptor remains valid. A successful descriptor is uniquely
    // transferred to File immediately below.
    let descriptor = unsafe {
        libc::syscall(
            libc::SYS_openat2,
            parent.as_raw_fd(),
            path.as_ptr(),
            &how,
            size_of::<OpenHow>(),
        )
    };
    if descriptor < 0 {
        return Err(io::Error::last_os_error());
    }
    let descriptor =
        i32::try_from(descriptor).map_err(|_| io::Error::other("openat2 returned an invalid descriptor"))?;
    // SAFETY: openat2 returned this descriptor uniquely and it has not been wrapped or closed.
    Ok(unsafe { File::from_raw_fd(descriptor) })
}

fn require_missing(
    parent: &File,
    name: &CString,
    stage: IncarnationCreationStage,
) -> Result<(), IncarnationCreationError> {
    match openat2(parent, name, libc::O_PATH | libc::O_CLOEXEC | libc::O_NOFOLLOW) {
        Err(error) if error.raw_os_error() == Some(libc::ENOENT) => Ok(()),
        Ok(_) => Err(IncarnationCreationError::io(
            stage,
            io::Error::new(io::ErrorKind::AlreadyExists, "managed creation name already exists"),
        )),
        Err(error) => Err(IncarnationCreationError::io(stage, error)),
    }
}

fn classify_io(operation: NamespaceOperation, error: io::Error) -> BackendFailure {
    let raw_code = error.raw_os_error();
    let class = match raw_code {
        Some(code) if code == libc::EACCES || code == libc::EPERM => NamespaceFailureClass::PermissionDenied,
        Some(libc::EBUSY) => NamespaceFailureClass::SharingViolation,
        Some(libc::ENOENT) => NamespaceFailureClass::NotFoundNeedsReconciliation,
        Some(libc::EINTR) => NamespaceFailureClass::Interrupted,
        _ => NamespaceFailureClass::OtherIo,
    };
    if matches!(
        class,
        NamespaceFailureClass::PermissionDenied
            | NamespaceFailureClass::SharingViolation
            | NamespaceFailureClass::NotFoundNeedsReconciliation
            | NamespaceFailureClass::Interrupted
    ) {
        BackendFailure::retryable(operation, class, raw_code)
    } else {
        BackendFailure::failed(operation, class, raw_code)
    }
}

fn split_parent(path: &str) -> (&str, &str) {
    path.rsplit_once('/')
        .map_or(("", path), |(parent, name)| (parent, name))
}
