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

use std::ffi::c_void;
use std::fs::File;
use std::io;
use std::mem::size_of;
use std::mem::MaybeUninit;
use std::os::windows::io::AsRawHandle;
use std::os::windows::io::FromRawHandle;
use std::ptr;

use windows::core::PWSTR;
use windows::Wdk::Foundation::OBJECT_ATTRIBUTES;
use windows::Wdk::Storage::FileSystem::FileRenameInformation;
use windows::Wdk::Storage::FileSystem::FileRenameInformationEx;
use windows::Wdk::Storage::FileSystem::NtCreateFile;
use windows::Wdk::Storage::FileSystem::NtSetInformationFile;
use windows::Wdk::Storage::FileSystem::FILE_CREATE;
use windows::Wdk::Storage::FileSystem::FILE_DIRECTORY_FILE;
use windows::Wdk::Storage::FileSystem::FILE_NON_DIRECTORY_FILE;
use windows::Wdk::Storage::FileSystem::FILE_OPEN;
use windows::Wdk::Storage::FileSystem::FILE_OPEN_IF;
use windows::Wdk::Storage::FileSystem::FILE_OPEN_REPARSE_POINT;
use windows::Wdk::Storage::FileSystem::FILE_RENAME_INFORMATION;
use windows::Wdk::Storage::FileSystem::FILE_SYNCHRONOUS_IO_NONALERT;
use windows::Wdk::Storage::FileSystem::NTCREATEFILE_CREATE_OPTIONS;
use windows::Win32::Foundation::RtlNtStatusToDosError;
use windows::Win32::Foundation::ERROR_ACCESS_DENIED;
use windows::Win32::Foundation::ERROR_DELETE_PENDING;
use windows::Win32::Foundation::ERROR_FILE_NOT_FOUND;
use windows::Win32::Foundation::ERROR_INVALID_PARAMETER;
use windows::Win32::Foundation::ERROR_LOCK_VIOLATION;
use windows::Win32::Foundation::ERROR_NOT_SUPPORTED;
use windows::Win32::Foundation::ERROR_PATH_NOT_FOUND;
use windows::Win32::Foundation::ERROR_SHARING_VIOLATION;
use windows::Win32::Foundation::HANDLE;
use windows::Win32::Foundation::NTSTATUS;
use windows::Win32::Foundation::OBJ_DONT_REPARSE;
use windows::Win32::Foundation::STATUS_OBJECT_NAME_NOT_FOUND;
use windows::Win32::Foundation::STATUS_OBJECT_PATH_NOT_FOUND;
use windows::Win32::Foundation::UNICODE_STRING;
use windows::Win32::Foundation::WIN32_ERROR;
use windows::Win32::Storage::FileSystem::FileAttributeTagInfo;
use windows::Win32::Storage::FileSystem::FileDispositionInfo;
use windows::Win32::Storage::FileSystem::FileDispositionInfoEx;
use windows::Win32::Storage::FileSystem::GetFileInformationByHandleEx;
use windows::Win32::Storage::FileSystem::GetVolumeInformationByHandleW;
use windows::Win32::Storage::FileSystem::SetFileInformationByHandle;
use windows::Win32::Storage::FileSystem::DELETE;
use windows::Win32::Storage::FileSystem::FILE_ACCESS_RIGHTS;
use windows::Win32::Storage::FileSystem::FILE_ATTRIBUTE_DIRECTORY;
use windows::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT;
use windows::Win32::Storage::FileSystem::FILE_ATTRIBUTE_TAG_INFO;
use windows::Win32::Storage::FileSystem::FILE_DISPOSITION_FLAG_DELETE;
use windows::Win32::Storage::FileSystem::FILE_DISPOSITION_FLAG_POSIX_SEMANTICS;
use windows::Win32::Storage::FileSystem::FILE_DISPOSITION_INFO;
use windows::Win32::Storage::FileSystem::FILE_DISPOSITION_INFO_EX;
use windows::Win32::Storage::FileSystem::FILE_DISPOSITION_INFO_EX_FLAGS;
use windows::Win32::Storage::FileSystem::FILE_FLAGS_AND_ATTRIBUTES;
use windows::Win32::Storage::FileSystem::FILE_LIST_DIRECTORY;
use windows::Win32::Storage::FileSystem::FILE_READ_ATTRIBUTES;
use windows::Win32::Storage::FileSystem::FILE_READ_DATA;
use windows::Win32::Storage::FileSystem::FILE_SHARE_DELETE;
use windows::Win32::Storage::FileSystem::FILE_SHARE_MODE;
use windows::Win32::Storage::FileSystem::FILE_SHARE_READ;
use windows::Win32::Storage::FileSystem::FILE_SHARE_WRITE;
use windows::Win32::Storage::FileSystem::FILE_TRAVERSE;
use windows::Win32::Storage::FileSystem::FILE_WRITE_DATA;
use windows::Win32::Storage::FileSystem::SYNCHRONIZE;
use windows::Win32::System::IO::IO_STATUS_BLOCK;

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
use super::types::NamespaceVerificationError;
use crate::mapped_file::retirement::identity::PhysicalFileKey;
use crate::mapped_file::retirement::identity::StoreRelativePath;
use crate::mapped_file::retirement::writer::AllocatedIncarnationReceipt;
use crate::mapped_file::retirement::writer::BoundIncarnationReceipt;

use super::creation::IncarnationCreationError;
use super::creation::IncarnationCreationStage;

const UNSUPPORTED_REASON: &str = "Windows direct unlink is forbidden; v1 requires the unique tombstone transition";
const UNQUALIFIED_WRITER_REASON: &str = "Windows namespace mutation is qualified only for an NTFS Store root";

pub(super) struct NamespaceRoot {
    file: File,
    writer_qualified: bool,
}

impl NamespaceRoot {
    pub(super) fn open(file: File) -> Result<Self, NamespaceVerificationError> {
        let attributes =
            query_attributes(&file, NamespaceOperation::VerifyRoot).map_err(BackendFailure::into_verification_error)?;
        if attributes.FileAttributes & FILE_ATTRIBUTE_REPARSE_POINT.0 != 0 {
            return Err(NamespaceVerificationError::Rejected(
                NamespacePolicyViolation::RootIsReparsePoint,
            ));
        }
        if attributes.FileAttributes & FILE_ATTRIBUTE_DIRECTORY.0 == 0 {
            return Err(NamespaceVerificationError::Rejected(
                NamespacePolicyViolation::RootIsNotDirectory,
            ));
        }
        let writer_qualified = is_ntfs_volume(&file)
            .map_err(|error| classify_io(NamespaceOperation::VerifyRoot, error).into_verification_error())?;
        Ok(Self { file, writer_qualified })
    }

    pub(super) fn reserve(
        &self,
        request: &NamespaceRetirementRequest,
        transition: NamespaceTransition,
    ) -> Result<NamespaceReservation, NamespaceVerificationError> {
        if !matches!(request.physical_key(), PhysicalFileKey::Windows(_)) {
            return Err(NamespaceVerificationError::Rejected(
                NamespacePolicyViolation::PhysicalKeyPlatformMismatch,
            ));
        }
        if transition == NamespaceTransition::DirectUnlink {
            return Err(NamespaceVerificationError::Rejected(
                NamespacePolicyViolation::UnsupportedTransition { transition },
            ));
        }
        if !self.writer_qualified {
            return Err(NamespaceVerificationError::Unsupported {
                platform: "windows",
                reason: UNQUALIFIED_WRITER_REASON,
            });
        }
        let (parent_path, canonical_name) = split_parent(request.canonical_path().as_str());
        let (tombstone_parent, tombstone_name) = split_parent(request.tombstone_path().as_str());
        if parent_path != tombstone_parent {
            return Err(NamespaceVerificationError::Rejected(
                NamespacePolicyViolation::ParentEscapedRoot,
            ));
        }
        let parent = open_parent(&self.file, parent_path)?;
        Ok(NamespaceReservation {
            parent,
            canonical_name: canonical_name.to_owned(),
            tombstone_name: tombstone_name.to_owned(),
            canonical: None,
            tombstone: None,
        })
    }

    pub(super) fn open_active_segment(
        &self,
        path: &StoreRelativePath,
        expected_key: PhysicalFileKey,
        expected_length: u64,
    ) -> Result<File, NamespaceVerificationError> {
        if !self.writer_qualified {
            return Err(NamespaceVerificationError::Unsupported {
                platform: "windows",
                reason: UNQUALIFIED_WRITER_REASON,
            });
        }
        if !matches!(expected_key, PhysicalFileKey::Windows(_)) {
            return Err(NamespaceVerificationError::Rejected(
                NamespacePolicyViolation::PhysicalKeyPlatformMismatch,
            ));
        }
        let (parent_path, file_name) = split_parent(path.as_str());
        let parent = open_parent(&self.file, parent_path)?;
        let file = open_writable_relative(&parent, file_name, FILE_OPEN, false)
            .map_err(|error| classify_io(NamespaceOperation::VerifyCanonical, error).into_verification_error())?;
        let attributes = query_attributes(&file, NamespaceOperation::VerifyCanonical)
            .map_err(BackendFailure::into_verification_error)?;
        if attributes.FileAttributes & (FILE_ATTRIBUTE_DIRECTORY.0 | FILE_ATTRIBUTE_REPARSE_POINT.0) != 0 {
            return Err(NamespaceVerificationError::Rejected(
                NamespacePolicyViolation::UnexpectedEntryType {
                    entry: NamespaceEntry::Canonical,
                },
            ));
        }
        let metadata = file
            .metadata()
            .map_err(|error| classify_io(NamespaceOperation::VerifyCanonical, error).into_verification_error())?;
        if metadata.len() != expected_length {
            return Err(NamespaceVerificationError::Rejected(
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
            return Err(NamespaceVerificationError::Rejected(
                NamespacePolicyViolation::NamespaceChangedDuringVerification,
            ));
        }
        Ok(file)
    }

    pub(super) fn create_incarnation_temp(
        &self,
        allocated: &AllocatedIncarnationReceipt,
    ) -> Result<CreatedIncarnationTemp, IncarnationCreationError> {
        if !self.writer_qualified {
            return Err(IncarnationCreationError::unsupported(
                IncarnationCreationStage::OpenParent,
                "windows",
                UNQUALIFIED_WRITER_REASON,
            ));
        }
        let (parent_path, canonical_name) = split_parent(allocated.canonical_path().as_str());
        let (create_parent, create_name) = split_parent(allocated.create_file_path().as_str());
        if parent_path != create_parent {
            return Err(IncarnationCreationError::policy(
                IncarnationCreationStage::VerifyNames,
                "canonical and create-file paths have different parents",
            ));
        }
        let parent = open_or_create_parent(&self.file, parent_path)
            .map_err(|error| IncarnationCreationError::namespace(IncarnationCreationStage::OpenParent, error))?;
        require_missing(&parent, canonical_name, IncarnationCreationStage::VerifyNames)?;
        require_missing(&parent, create_name, IncarnationCreationStage::VerifyNames)?;
        let file = create_relative(&parent, create_name)?;
        let attributes = query_attributes(&file, NamespaceOperation::VerifyCanonical).map_err(|failure| {
            IncarnationCreationError::namespace(IncarnationCreationStage::CreateTemp, failure.into_verification_error())
        })?;
        if attributes.FileAttributes & (FILE_ATTRIBUTE_REPARSE_POINT.0 | FILE_ATTRIBUTE_DIRECTORY.0) != 0 {
            return Err(IncarnationCreationError::policy(
                IncarnationCreationStage::CreateTemp,
                "created namespace object is not a regular non-reparse file",
            ));
        }
        file.set_len(allocated.expected_length())
            .map_err(|error| IncarnationCreationError::io(IncarnationCreationStage::SizeTemp, error))?;
        file.sync_all()
            .map_err(|error| IncarnationCreationError::io(IncarnationCreationStage::SyncTemp, error))?;
        let physical_key = physical_key::capture(&file)
            .map_err(|error| IncarnationCreationError::io(IncarnationCreationStage::CapturePhysicalKey, error))?;
        if !matches!(physical_key, PhysicalFileKey::Windows(_)) {
            return Err(IncarnationCreationError::policy(
                IncarnationCreationStage::CapturePhysicalKey,
                "created file returned a non-Windows physical key",
            ));
        }
        Ok(CreatedIncarnationTemp {
            parent,
            file,
            canonical_name: canonical_name.to_owned(),
            create_name: create_name.to_owned(),
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
            || created.canonical_name != bound_canonical_name
            || created.create_name != bound_create_name
        {
            return Err(IncarnationCreationError::policy(
                IncarnationCreationStage::VerifyNames,
                "BindIncarnation paths differ from the created file",
            ));
        }

        rename_handle_no_replace(&created.file, &created.parent, &created.canonical_name).map_err(|failure| {
            IncarnationCreationError::namespace(
                IncarnationCreationStage::RenameNoReplace,
                failure.into_verification_error(),
            )
        })?;
        // Windows has no POSIX parent-directory fsync. Close the mutation handle, then reopen both
        // names relative to the retained parent and bind the observed key and length.
        drop(created.file);
        let canonical = open_created_relative(&created.parent, &created.canonical_name, FILE_OPEN)
            .map_err(|error| IncarnationCreationError::io(IncarnationCreationStage::ReopenCanonical, error))?;
        let attributes = query_attributes(&canonical, NamespaceOperation::VerifyCanonical).map_err(|failure| {
            IncarnationCreationError::namespace(
                IncarnationCreationStage::VerifyCanonical,
                failure.into_verification_error(),
            )
        })?;
        let reopened_key = physical_key::capture(&canonical)
            .map_err(|error| IncarnationCreationError::io(IncarnationCreationStage::VerifyCanonical, error))?;
        let actual_length = canonical
            .metadata()
            .map_err(|error| IncarnationCreationError::io(IncarnationCreationStage::VerifyCanonical, error))?
            .len();
        if attributes.FileAttributes & (FILE_ATTRIBUTE_REPARSE_POINT.0 | FILE_ATTRIBUTE_DIRECTORY.0) != 0
            || reopened_key != created.physical_key
            || actual_length != created.expected_length
        {
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
    canonical_name: String,
    create_name: String,
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
    canonical_name: String,
    tombstone_name: String,
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
        let source = self
            .canonical
            .as_ref()
            .ok_or_else(|| BackendFailure::failed(NamespaceOperation::Rename, NamespaceFailureClass::OtherIo, None))?;
        rename_handle_no_replace(source, &self.parent, &self.tombstone_name)
    }

    fn unlink(&mut self, entry: NamespaceEntry) -> Result<(), BackendFailure> {
        if entry != NamespaceEntry::Tombstone {
            return Err(BackendFailure::failed(
                NamespaceOperation::Unlink,
                NamespaceFailureClass::OtherIo,
                None,
            ));
        }
        let target = self
            .tombstone
            .as_ref()
            .ok_or_else(|| BackendFailure::failed(NamespaceOperation::Unlink, NamespaceFailureClass::OtherIo, None))?;
        disposition_handle(target)
    }

    fn sync_after_namespace(&mut self, _transition: NamespaceTransition) -> Result<(), BackendFailure> {
        // Windows exposes no POSIX directory-fsync equivalent. FlushFileBuffers on a rename or
        // delete handle is neither a parent-directory durability proof nor guaranteed to be
        // permitted for a DELETE|READ_ATTRIBUTES handle. The durable intent therefore remains
        // authoritative: close every mutation handle here, then let the engine reopen both names
        // relative to the retained parent and bind the observed key/length to its typed outcome.
        self.release_for_reverification();
        Ok(())
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
    name: &str,
    expected_key: PhysicalFileKey,
    expected_length: u64,
    operation: NamespaceOperation,
) -> Result<ObservedEntry, BackendFailure> {
    let Some(file) = open_relative(parent, name, false, operation)? else {
        return Ok(ObservedEntry {
            observation: EntryObservation::Missing,
            handle: None,
        });
    };
    let attributes = query_attributes(&file, operation)?;
    if attributes.FileAttributes & FILE_ATTRIBUTE_REPARSE_POINT.0 != 0 {
        return Ok(ObservedEntry {
            observation: EntryObservation::ReparsePoint,
            handle: Some(file),
        });
    }
    if attributes.FileAttributes & FILE_ATTRIBUTE_DIRECTORY.0 != 0 {
        return Ok(ObservedEntry {
            observation: EntryObservation::Directory,
            handle: Some(file),
        });
    }
    let key = physical_key::capture(&file).map_err(|error| classify_io(operation, error))?;
    let observation = if key == expected_key {
        let actual_length = file.metadata().map_err(|error| classify_io(operation, error))?.len();
        if actual_length == expected_length {
            EntryObservation::ExpectedFile
        } else {
            EntryObservation::ExpectedFileWrongLength(actual_length)
        }
    } else {
        EntryObservation::OtherFile(key)
    };
    Ok(ObservedEntry {
        observation,
        handle: Some(file),
    })
}

fn open_parent(root: &File, parent_path: &str) -> Result<File, NamespaceVerificationError> {
    let mut parent = root
        .try_clone()
        .map_err(|error| classify_io(NamespaceOperation::OpenParent, error).into_verification_error())?;
    if parent_path.is_empty() {
        return Ok(parent);
    }
    for component in parent_path.split('/') {
        parent = open_relative(&parent, component, true, NamespaceOperation::OpenParent)
            .map_err(BackendFailure::into_verification_error)?
            .ok_or_else(|| {
                BackendFailure::retryable(
                    NamespaceOperation::OpenParent,
                    NamespaceFailureClass::NotFoundNeedsReconciliation,
                    Some(ERROR_PATH_NOT_FOUND.0 as i32),
                )
                .into_verification_error()
            })?;
        let attributes = query_attributes(&parent, NamespaceOperation::OpenParent)
            .map_err(BackendFailure::into_verification_error)?;
        if attributes.FileAttributes & FILE_ATTRIBUTE_REPARSE_POINT.0 != 0 {
            return Err(NamespaceVerificationError::Rejected(
                NamespacePolicyViolation::ParentEscapedRoot,
            ));
        }
        if attributes.FileAttributes & FILE_ATTRIBUTE_DIRECTORY.0 == 0 {
            return Err(NamespaceVerificationError::Rejected(
                NamespacePolicyViolation::ParentEscapedRoot,
            ));
        }
    }
    Ok(parent)
}

fn open_or_create_parent(root: &File, parent_path: &str) -> Result<File, NamespaceVerificationError> {
    let mut parent = root
        .try_clone()
        .map_err(|error| classify_io(NamespaceOperation::OpenParent, error).into_verification_error())?;
    if parent_path.is_empty() {
        return Ok(parent);
    }
    for component in parent_path.split('/') {
        if component.is_empty() || component == "." || component == ".." {
            return Err(NamespaceVerificationError::Rejected(
                NamespacePolicyViolation::ParentEscapedRoot,
            ));
        }
        let next = open_or_create_directory_component(&parent, component)?;
        let attributes =
            query_attributes(&next, NamespaceOperation::OpenParent).map_err(BackendFailure::into_verification_error)?;
        if attributes.FileAttributes & FILE_ATTRIBUTE_REPARSE_POINT.0 != 0
            || attributes.FileAttributes & FILE_ATTRIBUTE_DIRECTORY.0 == 0
        {
            return Err(NamespaceVerificationError::Rejected(
                NamespacePolicyViolation::ParentEscapedRoot,
            ));
        }
        let reopened = open_relative(&parent, component, true, NamespaceOperation::OpenParent)
            .map_err(BackendFailure::into_verification_error)?
            .ok_or_else(|| {
                BackendFailure::retryable(
                    NamespaceOperation::OpenParent,
                    NamespaceFailureClass::NotFoundNeedsReconciliation,
                    Some(ERROR_PATH_NOT_FOUND.0 as i32),
                )
                .into_verification_error()
            })?;
        let created_key = physical_key::capture(&next)
            .map_err(|error| classify_io(NamespaceOperation::OpenParent, error).into_verification_error())?;
        let reopened_key = physical_key::capture(&reopened)
            .map_err(|error| classify_io(NamespaceOperation::OpenParent, error).into_verification_error())?;
        if created_key != reopened_key {
            return Err(NamespaceVerificationError::Rejected(
                NamespacePolicyViolation::ParentEscapedRoot,
            ));
        }
        parent = reopened;
    }
    Ok(parent)
}

fn open_or_create_directory_component(parent: &File, name: &str) -> Result<File, NamespaceVerificationError> {
    let mut wide = name.encode_utf16().collect::<Vec<_>>();
    let byte_length = wide
        .len()
        .checked_mul(size_of::<u16>())
        .and_then(|value| u16::try_from(value).ok())
        .ok_or(NamespaceVerificationError::Rejected(
            NamespacePolicyViolation::ParentEscapedRoot,
        ))?;
    let unicode = UNICODE_STRING {
        Length: byte_length,
        MaximumLength: byte_length,
        Buffer: PWSTR(wide.as_mut_ptr()),
    };
    let attributes = OBJECT_ATTRIBUTES {
        Length: size_of::<OBJECT_ATTRIBUTES>() as u32,
        RootDirectory: HANDLE(parent.as_raw_handle()),
        ObjectName: &unicode,
        Attributes: OBJ_DONT_REPARSE,
        SecurityDescriptor: ptr::null(),
        SecurityQualityOfService: ptr::null(),
    };
    let desired = FILE_ACCESS_RIGHTS(FILE_LIST_DIRECTORY.0 | FILE_TRAVERSE.0 | FILE_READ_ATTRIBUTES.0 | SYNCHRONIZE.0);
    let share = FILE_SHARE_MODE(FILE_SHARE_READ.0 | FILE_SHARE_WRITE.0 | FILE_SHARE_DELETE.0);
    let options =
        NTCREATEFILE_CREATE_OPTIONS(FILE_OPEN_REPARSE_POINT.0 | FILE_SYNCHRONOUS_IO_NONALERT.0 | FILE_DIRECTORY_FILE.0);
    let mut handle = HANDLE(ptr::null_mut());
    let mut io_status = IO_STATUS_BLOCK::default();
    // SAFETY: the retained parent handle and one-component UTF-16 name remain live for this
    // synchronous call. The directory/open-reparse options prevent traversal through the child.
    let status = unsafe {
        NtCreateFile(
            &mut handle,
            desired,
            &attributes,
            &mut io_status,
            None,
            FILE_FLAGS_AND_ATTRIBUTES(0),
            share,
            FILE_OPEN_IF,
            options,
            None,
            0,
        )
    };
    if status.0 < 0 {
        return Err(classify_io(NamespaceOperation::OpenParent, status_error(status)).into_verification_error());
    }
    if handle.is_invalid() {
        return Err(
            BackendFailure::failed(NamespaceOperation::OpenParent, NamespaceFailureClass::OtherIo, None)
                .into_verification_error(),
        );
    }
    // SAFETY: successful NtCreateFile returned unique ownership of a valid kernel handle.
    Ok(unsafe { File::from_raw_handle(handle.0) })
}

fn open_relative(
    parent: &File,
    name: &str,
    directory: bool,
    operation: NamespaceOperation,
) -> Result<Option<File>, BackendFailure> {
    let mut wide = name.encode_utf16().collect::<Vec<_>>();
    let byte_length = wide
        .len()
        .checked_mul(size_of::<u16>())
        .and_then(|value| u16::try_from(value).ok())
        .ok_or_else(|| BackendFailure::failed(operation, NamespaceFailureClass::OtherIo, None))?;
    let unicode = UNICODE_STRING {
        Length: byte_length,
        MaximumLength: byte_length,
        Buffer: PWSTR(wide.as_mut_ptr()),
    };
    let attributes = OBJECT_ATTRIBUTES {
        Length: size_of::<OBJECT_ATTRIBUTES>() as u32,
        RootDirectory: HANDLE(parent.as_raw_handle()),
        ObjectName: &unicode,
        Attributes: OBJ_DONT_REPARSE,
        SecurityDescriptor: ptr::null(),
        SecurityQualityOfService: ptr::null(),
    };
    let desired = if directory {
        FILE_ACCESS_RIGHTS(FILE_LIST_DIRECTORY.0 | FILE_TRAVERSE.0 | FILE_READ_ATTRIBUTES.0 | SYNCHRONIZE.0)
    } else {
        FILE_ACCESS_RIGHTS(DELETE.0 | FILE_READ_ATTRIBUTES.0 | SYNCHRONIZE.0)
    };
    let share = FILE_SHARE_MODE(FILE_SHARE_READ.0 | FILE_SHARE_WRITE.0 | FILE_SHARE_DELETE.0);
    let mut options = FILE_OPEN_REPARSE_POINT.0 | FILE_SYNCHRONOUS_IO_NONALERT.0;
    if directory {
        options |= windows::Wdk::Storage::FileSystem::FILE_DIRECTORY_FILE.0;
    }
    let mut handle = HANDLE(ptr::null_mut());
    let mut io_status = IO_STATUS_BLOCK::default();
    // SAFETY: pointers refer to live fixed storage for this synchronous call; RootDirectory is a
    // retained verified handle and `name` is one already-validated relative component. The call
    // opens only an existing entry and requests no reparse traversal.
    let status = unsafe {
        NtCreateFile(
            &mut handle,
            desired,
            &attributes,
            &mut io_status,
            None,
            FILE_FLAGS_AND_ATTRIBUTES(0),
            share,
            FILE_OPEN,
            NTCREATEFILE_CREATE_OPTIONS(options),
            None,
            0,
        )
    };
    if status == STATUS_OBJECT_NAME_NOT_FOUND || status == STATUS_OBJECT_PATH_NOT_FOUND {
        return Ok(None);
    }
    if status.0 < 0 {
        return Err(classify_io(operation, status_error(status)));
    }
    if handle.is_invalid() {
        return Err(BackendFailure::failed(operation, NamespaceFailureClass::OtherIo, None));
    }
    // SAFETY: successful NtCreateFile returned unique ownership of a valid kernel handle.
    Ok(Some(unsafe { File::from_raw_handle(handle.0) }))
}

fn create_relative(parent: &File, name: &str) -> Result<File, IncarnationCreationError> {
    open_created_relative(parent, name, FILE_CREATE)
        .map_err(|error| IncarnationCreationError::io(IncarnationCreationStage::CreateTemp, error))
}

fn open_created_relative(
    parent: &File,
    name: &str,
    disposition: windows::Wdk::Storage::FileSystem::NTCREATEFILE_CREATE_DISPOSITION,
) -> io::Result<File> {
    open_writable_relative(parent, name, disposition, true)
}

fn open_writable_relative(
    parent: &File,
    name: &str,
    disposition: windows::Wdk::Storage::FileSystem::NTCREATEFILE_CREATE_DISPOSITION,
    request_delete_access: bool,
) -> io::Result<File> {
    let mut wide = name.encode_utf16().collect::<Vec<_>>();
    let byte_length = wide
        .len()
        .checked_mul(size_of::<u16>())
        .and_then(|value| u16::try_from(value).ok())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "relative file name is too long"))?;
    let unicode = UNICODE_STRING {
        Length: byte_length,
        MaximumLength: byte_length,
        Buffer: PWSTR(wide.as_mut_ptr()),
    };
    let attributes = OBJECT_ATTRIBUTES {
        Length: size_of::<OBJECT_ATTRIBUTES>() as u32,
        RootDirectory: HANDLE(parent.as_raw_handle()),
        ObjectName: &unicode,
        Attributes: OBJ_DONT_REPARSE,
        SecurityDescriptor: ptr::null(),
        SecurityQualityOfService: ptr::null(),
    };
    let mut desired = FILE_READ_DATA.0 | FILE_WRITE_DATA.0 | FILE_READ_ATTRIBUTES.0 | SYNCHRONIZE.0;
    if request_delete_access {
        desired |= DELETE.0;
    }
    let desired = FILE_ACCESS_RIGHTS(desired);
    let share = FILE_SHARE_MODE(FILE_SHARE_READ.0 | FILE_SHARE_WRITE.0 | FILE_SHARE_DELETE.0);
    let options = FILE_OPEN_REPARSE_POINT.0 | FILE_SYNCHRONOUS_IO_NONALERT.0 | FILE_NON_DIRECTORY_FILE.0;
    let mut handle = HANDLE(ptr::null_mut());
    let mut io_status = IO_STATUS_BLOCK::default();
    // SAFETY: all structures and UTF-16 storage remain live for this synchronous call; the root is
    // a retained verified directory handle, and the no-reparse/non-directory options constrain the
    // single validated component. A successful handle is transferred exactly once to `File`.
    let status = unsafe {
        NtCreateFile(
            &mut handle,
            desired,
            &attributes,
            &mut io_status,
            None,
            FILE_FLAGS_AND_ATTRIBUTES(0),
            share,
            disposition,
            NTCREATEFILE_CREATE_OPTIONS(options),
            None,
            0,
        )
    };
    if status.0 < 0 {
        return Err(status_error(status));
    }
    if handle.is_invalid() {
        return Err(io::Error::other("NtCreateFile returned an invalid file handle"));
    }
    // SAFETY: NtCreateFile returned unique ownership of this valid kernel handle.
    Ok(unsafe { File::from_raw_handle(handle.0) })
}

fn require_missing(parent: &File, name: &str, stage: IncarnationCreationStage) -> Result<(), IncarnationCreationError> {
    match open_relative(parent, name, false, NamespaceOperation::VerifyCanonical) {
        Ok(None) => Ok(()),
        Ok(Some(_)) => Err(IncarnationCreationError::io(
            stage,
            io::Error::new(io::ErrorKind::AlreadyExists, "managed creation name already exists"),
        )),
        Err(failure) => Err(IncarnationCreationError::namespace(
            stage,
            failure.into_verification_error(),
        )),
    }
}

fn query_attributes(file: &File, operation: NamespaceOperation) -> Result<FILE_ATTRIBUTE_TAG_INFO, BackendFailure> {
    let mut attributes = MaybeUninit::<FILE_ATTRIBUTE_TAG_INFO>::uninit();
    // SAFETY: the handle remains borrowed; `attributes` is aligned writable storage of the exact
    // fixed information class size and is completely initialized on success.
    unsafe {
        GetFileInformationByHandleEx(
            HANDLE(file.as_raw_handle()),
            FileAttributeTagInfo,
            attributes.as_mut_ptr().cast(),
            size_of::<FILE_ATTRIBUTE_TAG_INFO>() as u32,
        )
        .map_err(|error| classify_io(operation, windows_error_to_io(error)))?;
        Ok(attributes.assume_init())
    }
}

fn rename_handle_no_replace(source: &File, parent: &File, tombstone_name: &str) -> Result<(), BackendFailure> {
    let extended = rename_buffer(parent, tombstone_name, true)?;
    let extended_length = rename_buffer_length(tombstone_name)?;
    let mut io_status = IO_STATUS_BLOCK::default();
    // SAFETY: the aligned buffer contains a fully initialized FILE_RENAME_INFORMATION followed by
    // exactly FileNameLength bytes; the source and verified parent handles remain live for this
    // synchronous native call, and `io_status` is writable for its complete duration.
    let status = unsafe {
        NtSetInformationFile(
            HANDLE(source.as_raw_handle()),
            &mut io_status,
            extended.as_ptr().cast::<c_void>(),
            extended_length,
            FileRenameInformationEx,
        )
    };
    if status.0 >= 0 {
        return Ok(());
    }
    if is_status_information_class_unsupported(status) {
        let mut fallback = rename_buffer(parent, tombstone_name, false)?;
        let mut fallback_status = IO_STATUS_BLOCK::default();
        // SAFETY: this is the same verified source handle and no-replace relative target; only
        // the documented legacy information class changes, and the status block remains live.
        let status = unsafe {
            NtSetInformationFile(
                HANDLE(source.as_raw_handle()),
                &mut fallback_status,
                fallback.as_mut_ptr().cast::<c_void>(),
                extended_length,
                FileRenameInformation,
            )
        };
        return status_result(NamespaceOperation::Rename, status);
    }
    status_result(NamespaceOperation::Rename, status)
}

fn rename_buffer(parent: &File, name: &str, extended: bool) -> Result<Vec<usize>, BackendFailure> {
    let wide = name.encode_utf16().collect::<Vec<_>>();
    let byte_length = wide
        .len()
        .checked_mul(size_of::<u16>())
        .ok_or_else(|| BackendFailure::failed(NamespaceOperation::Rename, NamespaceFailureClass::OtherIo, None))?;
    let total = size_of::<FILE_RENAME_INFORMATION>()
        .checked_add(byte_length)
        .ok_or_else(|| BackendFailure::failed(NamespaceOperation::Rename, NamespaceFailureClass::OtherIo, None))?;
    let words = total.div_ceil(size_of::<usize>());
    let mut storage = vec![0_usize; words];
    let info = storage.as_mut_ptr().cast::<FILE_RENAME_INFORMATION>();
    // SAFETY: Vec<usize> provides sufficient alignment and capacity for `total` bytes. Every fixed
    // field is initialized before the buffer is passed to Windows, and the variable name copy is
    // bounded by the allocation computed above.
    unsafe {
        if extended {
            (*info).Anonymous.Flags = 0;
        } else {
            (*info).Anonymous.ReplaceIfExists = false;
        }
        (*info).RootDirectory = HANDLE(parent.as_raw_handle());
        (*info).FileNameLength = u32::try_from(byte_length)
            .map_err(|_| BackendFailure::failed(NamespaceOperation::Rename, NamespaceFailureClass::OtherIo, None))?;
        ptr::copy_nonoverlapping(wide.as_ptr(), (*info).FileName.as_mut_ptr(), wide.len());
    }
    Ok(storage)
}

fn rename_buffer_length(name: &str) -> Result<u32, BackendFailure> {
    let name_bytes = name
        .encode_utf16()
        .count()
        .checked_mul(size_of::<u16>())
        .ok_or_else(|| BackendFailure::failed(NamespaceOperation::Rename, NamespaceFailureClass::OtherIo, None))?;
    let total = size_of::<FILE_RENAME_INFORMATION>()
        .checked_add(name_bytes)
        .ok_or_else(|| BackendFailure::failed(NamespaceOperation::Rename, NamespaceFailureClass::OtherIo, None))?;
    u32::try_from(total)
        .map_err(|_| BackendFailure::failed(NamespaceOperation::Rename, NamespaceFailureClass::OtherIo, None))
}

fn disposition_handle(target: &File) -> Result<(), BackendFailure> {
    let extended = FILE_DISPOSITION_INFO_EX {
        Flags: FILE_DISPOSITION_INFO_EX_FLAGS(FILE_DISPOSITION_FLAG_DELETE.0 | FILE_DISPOSITION_FLAG_POSIX_SEMANTICS.0),
    };
    // SAFETY: `extended` is the exact fixed structure for FileDispositionInfoEx and the verified
    // target handle remains live. No ignore-readonly or on-close flag is supplied.
    let result = unsafe {
        SetFileInformationByHandle(
            HANDLE(target.as_raw_handle()),
            FileDispositionInfoEx,
            ptr::from_ref(&extended).cast::<c_void>(),
            size_of::<FILE_DISPOSITION_INFO_EX>() as u32,
        )
    };
    match result {
        Ok(()) => Ok(()),
        Err(error) if is_information_class_unsupported(&error) => {
            let fallback = FILE_DISPOSITION_INFO { DeleteFile: true };
            // SAFETY: the documented legacy disposition class is applied to the same verified
            // target handle and requests deletion without any path fallback.
            unsafe {
                SetFileInformationByHandle(
                    HANDLE(target.as_raw_handle()),
                    FileDispositionInfo,
                    ptr::from_ref(&fallback).cast::<c_void>(),
                    size_of::<FILE_DISPOSITION_INFO>() as u32,
                )
            }
            .map_err(|error| classify_io(NamespaceOperation::Unlink, windows_error_to_io(error)))
        }
        Err(error) => Err(classify_io(NamespaceOperation::Unlink, windows_error_to_io(error))),
    }
}

fn is_information_class_unsupported(error: &windows::core::Error) -> bool {
    let code = WIN32_ERROR::from_error(error).map(|code| code.0);
    code == Some(ERROR_INVALID_PARAMETER.0) || code == Some(ERROR_NOT_SUPPORTED.0)
}

fn is_status_information_class_unsupported(status: NTSTATUS) -> bool {
    // SAFETY: `RtlNtStatusToDosError` accepts any NTSTATUS value and does not retain pointers.
    let code = unsafe { RtlNtStatusToDosError(status) };
    code == ERROR_INVALID_PARAMETER.0 || code == ERROR_NOT_SUPPORTED.0
}

fn status_result(operation: NamespaceOperation, status: NTSTATUS) -> Result<(), BackendFailure> {
    if status.0 >= 0 {
        Ok(())
    } else {
        Err(classify_io(operation, status_error(status)))
    }
}

fn is_ntfs_volume(file: &File) -> io::Result<bool> {
    let mut filesystem_name = [0_u16; 32];
    // SAFETY: the retained root handle remains borrowed for the synchronous call and the fixed
    // buffer is valid writable UTF-16 storage for its complete duration.
    unsafe {
        GetVolumeInformationByHandleW(
            HANDLE(file.as_raw_handle()),
            None,
            None,
            None,
            None,
            Some(&mut filesystem_name),
        )
        .map_err(windows_error_to_io)?;
    }
    let length = filesystem_name
        .iter()
        .position(|unit| *unit == 0)
        .unwrap_or(filesystem_name.len());
    Ok(String::from_utf16_lossy(&filesystem_name[..length]).eq_ignore_ascii_case("NTFS"))
}

fn classify_io(operation: NamespaceOperation, error: io::Error) -> BackendFailure {
    let raw_code = error.raw_os_error();
    let class = match raw_code.map(|code| code as u32) {
        Some(code) if code == ERROR_SHARING_VIOLATION.0 => NamespaceFailureClass::SharingViolation,
        Some(code) if code == ERROR_LOCK_VIOLATION.0 => NamespaceFailureClass::LockViolation,
        Some(code) if code == ERROR_DELETE_PENDING.0 => NamespaceFailureClass::DeletePending,
        Some(code) if code == ERROR_ACCESS_DENIED.0 => NamespaceFailureClass::PermissionDenied,
        Some(code) if code == ERROR_FILE_NOT_FOUND.0 || code == ERROR_PATH_NOT_FOUND.0 => {
            NamespaceFailureClass::NotFoundNeedsReconciliation
        }
        _ if error.kind() == io::ErrorKind::Interrupted => NamespaceFailureClass::Interrupted,
        _ => NamespaceFailureClass::OtherIo,
    };
    if matches!(
        class,
        NamespaceFailureClass::SharingViolation
            | NamespaceFailureClass::LockViolation
            | NamespaceFailureClass::DeletePending
            | NamespaceFailureClass::PermissionDenied
            | NamespaceFailureClass::NotFoundNeedsReconciliation
            | NamespaceFailureClass::Interrupted
    ) {
        BackendFailure::retryable(operation, class, raw_code)
    } else {
        BackendFailure::failed(operation, class, raw_code)
    }
}

fn windows_error_to_io(error: windows::core::Error) -> io::Error {
    WIN32_ERROR::from_error(&error)
        .map(|code| io::Error::from_raw_os_error(code.0 as i32))
        .unwrap_or_else(|| io::Error::other(error))
}

fn status_error(status: NTSTATUS) -> io::Error {
    // SAFETY: RtlNtStatusToDosError accepts every NTSTATUS value and has no pointer arguments.
    let code = unsafe { RtlNtStatusToDosError(status) };
    io::Error::from_raw_os_error(code as i32)
}

fn split_parent(path: &str) -> (&str, &str) {
    path.rsplit_once('/')
        .map_or(("", path), |(parent, name)| (parent, name))
}

pub(super) const fn unsupported_reason() -> &'static str {
    UNSUPPORTED_REASON
}
