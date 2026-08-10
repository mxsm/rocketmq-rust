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
use std::os::windows::fs::FileExt;
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
use windows::Wdk::Storage::FileSystem::NTCREATEFILE_CREATE_DISPOSITION;
use windows::Wdk::Storage::FileSystem::NTCREATEFILE_CREATE_OPTIONS;
use windows::Win32::Foundation::RtlNtStatusToDosError;
use windows::Win32::Foundation::ERROR_INVALID_PARAMETER;
use windows::Win32::Foundation::ERROR_NOT_SUPPORTED;
use windows::Win32::Foundation::HANDLE;
use windows::Win32::Foundation::NTSTATUS;
use windows::Win32::Foundation::OBJ_DONT_REPARSE;
use windows::Win32::Foundation::STATUS_OBJECT_NAME_NOT_FOUND;
use windows::Win32::Foundation::UNICODE_STRING;
use windows::Win32::Storage::FileSystem::FileAttributeTagInfo;
use windows::Win32::Storage::FileSystem::FileStandardInfo;
use windows::Win32::Storage::FileSystem::GetFileInformationByHandleEx;
use windows::Win32::Storage::FileSystem::GetVolumeInformationByHandleW;
use windows::Win32::Storage::FileSystem::DELETE;
use windows::Win32::Storage::FileSystem::FILE_ACCESS_RIGHTS;
use windows::Win32::Storage::FileSystem::FILE_APPEND_DATA;
use windows::Win32::Storage::FileSystem::FILE_ATTRIBUTE_DIRECTORY;
use windows::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT;
use windows::Win32::Storage::FileSystem::FILE_ATTRIBUTE_TAG_INFO;
use windows::Win32::Storage::FileSystem::FILE_FLAGS_AND_ATTRIBUTES;
use windows::Win32::Storage::FileSystem::FILE_LIST_DIRECTORY;
use windows::Win32::Storage::FileSystem::FILE_READ_ATTRIBUTES;
use windows::Win32::Storage::FileSystem::FILE_READ_DATA;
use windows::Win32::Storage::FileSystem::FILE_SHARE_DELETE;
use windows::Win32::Storage::FileSystem::FILE_SHARE_MODE;
use windows::Win32::Storage::FileSystem::FILE_SHARE_READ;
use windows::Win32::Storage::FileSystem::FILE_SHARE_WRITE;
use windows::Win32::Storage::FileSystem::FILE_STANDARD_INFO;
use windows::Win32::Storage::FileSystem::FILE_TRAVERSE;
use windows::Win32::Storage::FileSystem::FILE_WRITE_DATA;
use windows::Win32::Storage::FileSystem::SYNCHRONIZE;
use windows::Win32::System::IO::IO_STATUS_BLOCK;

use super::InitialBootstrapFoundationError;
use super::PreparedInitialBootstrapFoundation;
use crate::mapped_file::retirement::bootstrap::proof::BootstrapFoundationEvidence;
use crate::mapped_file::retirement::bootstrap::proof::CanonicalStoreMetaEvidence;
use crate::mapped_file::retirement::bootstrap::types::ImmutableArtifactProgress;
use crate::mapped_file::retirement::bootstrap::types::ImmutableArtifactStep;
use crate::mapped_file::retirement::bootstrap::types::InitialMarkerProgress;
use crate::mapped_file::retirement::bootstrap::types::InitialMarkerStep;
use crate::mapped_file::retirement::bootstrap::types::InitialMarkerVerificationEvidence;
use crate::mapped_file::retirement::bootstrap::types::PlannedInitialMarker;
use crate::mapped_file::retirement::bootstrap::types::PlannedSnapshot;
use crate::mapped_file::retirement::identity::PhysicalFileKey;
use crate::mapped_file::retirement::io::FileLedgerIo;
use crate::mapped_file::retirement::platform::physical_file_key;
use crate::mapped_file::retirement::sidecar::decode_store_meta;
use crate::mapped_file::retirement::sidecar::encode_store_meta;
use crate::mapped_file::retirement::sidecar::StoreMeta;
use crate::mapped_file::retirement::sidecar::STORE_META_LENGTH;

const LIFECYCLE_DIRECTORY: &str = ".rocketmq-lifecycle";
const STORE_META_FILE: &str = "store.meta";
const STORE_META_TEMP_FILE: &str = "store.meta.bootstrap.tmp";
const ACKNOWLEDGEMENT_FILE: &str = "ACKNOWLEDGED.v1";
const GENERATION_ZERO_LOG: &str = "retirement.log.g00000000000000000000";
const GENERATION_ZERO_SNAPSHOT: &str = "manifest.snapshot.g00000000000000000000";
const GENERATION_ZERO_SNAPSHOT_TEMP: &str = "manifest.snapshot.g00000000000000000000.bootstrap.tmp";
const ENABLED_MARKER: &str = "ENABLED.v1";
const ENABLED_MARKER_TEMP: &str = "ENABLED.v1.bootstrap.tmp";
const ACKNOWLEDGEMENT_FILE_LENGTH: usize = 208;
const MAX_INTERRUPTED_RETRIES: usize = 16;
const NON_NTFS_REASON: &str = "managed lifecycle bootstrap is qualified only for an NTFS Store root";

pub(super) fn prepare(
    store_root: File,
    expected_meta: &StoreMeta,
) -> Result<PreparedInitialBootstrapFoundation, InitialBootstrapFoundationError> {
    validate_directory(&store_root, "Store root")?;
    if !is_ntfs_volume(&store_root)? {
        return Err(InitialBootstrapFoundationError::unsupported(NON_NTFS_REASON));
    }
    let lifecycle = open_or_create_lifecycle_directory(&store_root)?;
    let canonical_meta = encode_store_meta(expected_meta).map_err(InitialBootstrapFoundationError::sidecar)?;
    publish_store_meta(&lifecycle, &canonical_meta)?;
    let acknowledgement = ensure_acknowledgement(&lifecycle)?;
    ensure_generation_zero_log(&lifecycle, &acknowledgement)?;

    let decoded_meta = decode_store_meta(&canonical_meta).map_err(InitialBootstrapFoundationError::sidecar)?;
    let foundation = BootstrapFoundationEvidence {
        store_meta: CanonicalStoreMetaEvidence {
            meta: decoded_meta,
            canonical_bytes: canonical_meta,
            stored_crc32: u32::from_le_bytes(
                canonical_meta[60..64]
                    .try_into()
                    .map_err(|_| InitialBootstrapFoundationError::invalid("store.meta CRC field is unavailable"))?,
            ),
        },
    };
    let ledger = FileLedgerIo::open_from_store_root(&store_root, 0).map_err(InitialBootstrapFoundationError::ledger)?;
    let artifacts = InitialArtifactStore::new(
        store_root.try_clone().map_err(InitialBootstrapFoundationError::io)?,
        lifecycle.try_clone().map_err(InitialBootstrapFoundationError::io)?,
    )?;
    Ok(PreparedInitialBootstrapFoundation::new(
        foundation,
        ledger,
        store_root,
        artifacts,
        expected_meta.clone(),
    ))
}

pub(super) struct InitialArtifactStore {
    store_root: File,
    lifecycle: File,
    lifecycle_identity: PhysicalFileKey,
    snapshot_temporary_synced: bool,
    snapshot_verified: bool,
    marker_temporary_synced: bool,
    marker_directory_synced: bool,
    marker_verified: bool,
}

impl InitialArtifactStore {
    fn new(store_root: File, lifecycle: File) -> Result<Self, InitialBootstrapFoundationError> {
        validate_directory(&lifecycle, "retained lifecycle directory")?;
        let lifecycle_identity = physical_identity(&lifecycle)?;
        Ok(Self {
            store_root,
            lifecycle,
            lifecycle_identity,
            snapshot_temporary_synced: false,
            snapshot_verified: false,
            marker_temporary_synced: false,
            marker_directory_synced: false,
            marker_verified: false,
        })
    }

    pub(super) fn inspect_snapshot(
        &self,
        planned: &PlannedSnapshot,
    ) -> Result<ImmutableArtifactProgress, InitialBootstrapFoundationError> {
        self.verify_lifecycle()?;
        let final_file = open_optional_file(&self.lifecycle, GENERATION_ZERO_SNAPSHOT, false)?;
        let temporary = open_optional_file(&self.lifecycle, GENERATION_ZERO_SNAPSHOT_TEMP, true)?;
        match (final_file, temporary) {
            (Some(final_file), None) => {
                require_exact_file(&final_file, &planned.encoded, "bootstrap snapshot")?;
                Ok(if self.snapshot_verified {
                    ImmutableArtifactProgress::Verified
                } else {
                    ImmutableArtifactProgress::Published
                })
            }
            (Some(_), Some(_)) => Err(InitialBootstrapFoundationError::invalid(
                "bootstrap snapshot final and temporary both exist",
            )),
            (None, Some(temporary)) => {
                let complete = require_exact_prefix(&temporary, &planned.encoded, "bootstrap snapshot temporary")?;
                Ok(if !complete {
                    ImmutableArtifactProgress::Missing
                } else if self.snapshot_temporary_synced {
                    ImmutableArtifactProgress::TemporarySynced
                } else {
                    ImmutableArtifactProgress::TemporaryWritten
                })
            }
            (None, None) => Ok(ImmutableArtifactProgress::Missing),
        }
    }

    pub(super) fn advance_snapshot(
        &mut self,
        planned: &PlannedSnapshot,
        step: ImmutableArtifactStep,
    ) -> Result<(), InitialBootstrapFoundationError> {
        self.verify_lifecycle()?;
        match step {
            ImmutableArtifactStep::WriteTemporary => {
                let temporary = match open_optional_file(&self.lifecycle, GENERATION_ZERO_SNAPSHOT_TEMP, true)? {
                    Some(file) => file,
                    None => create_exclusive_file(&self.lifecycle, GENERATION_ZERO_SNAPSHOT_TEMP)?,
                };
                complete_exact_prefix(&temporary, &planned.encoded, "bootstrap snapshot temporary")
            }
            ImmutableArtifactStep::SyncTemporary => {
                let temporary = open_required_file(&self.lifecycle, GENERATION_ZERO_SNAPSHOT_TEMP, true)?;
                require_exact_file(&temporary, &planned.encoded, "bootstrap snapshot temporary")?;
                temporary.sync_all().map_err(InitialBootstrapFoundationError::io)?;
                self.snapshot_temporary_synced = true;
                Ok(())
            }
            ImmutableArtifactStep::PublishFinalNoReplace => {
                if !self.snapshot_temporary_synced {
                    return Err(InitialBootstrapFoundationError::invalid(
                        "bootstrap snapshot temporary was not synced in this process",
                    ));
                }
                let temporary = open_required_file(&self.lifecycle, GENERATION_ZERO_SNAPSHOT_TEMP, true)?;
                rename_handle_no_replace(&temporary, &self.lifecycle, GENERATION_ZERO_SNAPSHOT)
            }
            ImmutableArtifactStep::ReopenAndVerify => {
                self.verify_lifecycle()?;
                let final_file = open_required_file(&self.lifecycle, GENERATION_ZERO_SNAPSHOT, false)?;
                require_exact_file(&final_file, &planned.encoded, "bootstrap snapshot")?;
                self.snapshot_verified = true;
                Ok(())
            }
        }
    }

    pub(super) fn inspect_initial_marker(
        &self,
        planned: &PlannedInitialMarker,
    ) -> Result<InitialMarkerProgress, InitialBootstrapFoundationError> {
        self.verify_lifecycle()?;
        let final_file = open_optional_file(&self.lifecycle, ENABLED_MARKER, false)?;
        let temporary = open_optional_file(&self.lifecycle, ENABLED_MARKER_TEMP, true)?;
        match (final_file, temporary) {
            (Some(final_file), None) => {
                require_exact_file(&final_file, &planned.encoded_file, "ENABLED.v1")?;
                if self.marker_verified {
                    let evidence =
                        InitialMarkerVerificationEvidence::from_reopened_bytes(planned.encoded_file, planned)
                            .ok_or_else(|| InitialBootstrapFoundationError::invalid("marker verification mismatch"))?;
                    Ok(InitialMarkerProgress::Verified(Box::new(evidence)))
                } else if self.marker_directory_synced {
                    Ok(InitialMarkerProgress::DirectorySynced)
                } else {
                    Ok(InitialMarkerProgress::Published)
                }
            }
            (Some(_), Some(_)) => Err(InitialBootstrapFoundationError::invalid(
                "ENABLED.v1 final and temporary both exist",
            )),
            (None, Some(temporary)) => {
                let complete = require_exact_prefix(&temporary, &planned.encoded_file, "ENABLED.v1 temporary")?;
                Ok(if !complete {
                    InitialMarkerProgress::Missing
                } else if self.marker_temporary_synced {
                    InitialMarkerProgress::TemporarySynced
                } else {
                    InitialMarkerProgress::TemporaryWritten
                })
            }
            (None, None) => Ok(InitialMarkerProgress::Missing),
        }
    }

    pub(super) fn advance_initial_marker(
        &mut self,
        planned: &PlannedInitialMarker,
        step: InitialMarkerStep,
    ) -> Result<(), InitialBootstrapFoundationError> {
        self.verify_lifecycle()?;
        match step {
            InitialMarkerStep::WriteTemporary => {
                let temporary = match open_optional_file(&self.lifecycle, ENABLED_MARKER_TEMP, true)? {
                    Some(file) => file,
                    None => create_exclusive_file(&self.lifecycle, ENABLED_MARKER_TEMP)?,
                };
                complete_exact_prefix(&temporary, &planned.encoded_file, "ENABLED.v1 temporary")
            }
            InitialMarkerStep::SyncTemporary => {
                let temporary = open_required_file(&self.lifecycle, ENABLED_MARKER_TEMP, true)?;
                require_exact_file(&temporary, &planned.encoded_file, "ENABLED.v1 temporary")?;
                temporary.sync_all().map_err(InitialBootstrapFoundationError::io)?;
                self.marker_temporary_synced = true;
                Ok(())
            }
            InitialMarkerStep::PublishFinalNoReplace => {
                if !self.marker_temporary_synced {
                    return Err(InitialBootstrapFoundationError::invalid(
                        "ENABLED.v1 temporary was not synced in this process",
                    ));
                }
                let temporary = open_required_file(&self.lifecycle, ENABLED_MARKER_TEMP, true)?;
                rename_handle_no_replace(&temporary, &self.lifecycle, ENABLED_MARKER)
            }
            InitialMarkerStep::SyncLifecycleDirectory => {
                self.verify_lifecycle()?;
                self.marker_directory_synced = true;
                Ok(())
            }
            InitialMarkerStep::ReopenAndVerifyEntireFile => {
                if !self.marker_directory_synced {
                    return Err(InitialBootstrapFoundationError::invalid(
                        "ENABLED.v1 directory entry was not verified in this process",
                    ));
                }
                let final_file = open_required_file(&self.lifecycle, ENABLED_MARKER, false)?;
                require_exact_file(&final_file, &planned.encoded_file, "ENABLED.v1")?;
                self.marker_verified = true;
                Ok(())
            }
        }
    }

    fn verify_lifecycle(&self) -> Result<(), InitialBootstrapFoundationError> {
        validate_directory(&self.lifecycle, "retained lifecycle directory")?;
        require_identity(
            physical_identity(&self.lifecycle)?,
            self.lifecycle_identity,
            "retained lifecycle directory identity changed",
        )?;
        let reopened = open_required_directory(&self.store_root, LIFECYCLE_DIRECTORY)?;
        require_identity(
            physical_identity(&reopened)?,
            self.lifecycle_identity,
            "lifecycle directory is no longer bound beneath the retained Store root",
        )
    }
}

fn open_or_create_lifecycle_directory(store_root: &File) -> Result<File, InitialBootstrapFoundationError> {
    let lifecycle = open_relative(store_root, LIFECYCLE_DIRECTORY, true, FILE_OPEN_IF, true)?;
    validate_directory(&lifecycle, "lifecycle directory")?;
    let reopened = open_required_directory(store_root, LIFECYCLE_DIRECTORY)?;
    require_identity(
        physical_identity(&lifecycle)?,
        physical_identity(&reopened)?,
        "lifecycle directory publication changed identity",
    )?;
    Ok(lifecycle)
}

fn publish_store_meta(
    lifecycle: &File,
    expected: &[u8; STORE_META_LENGTH],
) -> Result<(), InitialBootstrapFoundationError> {
    let final_file = open_optional_file(lifecycle, STORE_META_FILE, false)?;
    let temporary = open_optional_file(lifecycle, STORE_META_TEMP_FILE, true)?;
    match (final_file, temporary) {
        (Some(final_file), None) => require_exact_file(&final_file, expected, "store.meta"),
        (Some(_), Some(_)) => Err(InitialBootstrapFoundationError::invalid(
            "store.meta final and bootstrap temporary both exist",
        )),
        (None, temporary) => {
            let temporary = match temporary {
                Some(file) => file,
                None => create_exclusive_file(lifecycle, STORE_META_TEMP_FILE)?,
            };
            complete_exact_prefix(&temporary, expected, "store.meta bootstrap temporary")?;
            temporary.sync_all().map_err(InitialBootstrapFoundationError::io)?;
            require_exact_file(&temporary, expected, "store.meta bootstrap temporary")?;
            rename_handle_no_replace(&temporary, lifecycle, STORE_META_FILE)?;
            let final_file = open_required_file(lifecycle, STORE_META_FILE, false)?;
            require_same_file(&temporary, &final_file, "store.meta publication changed identity")?;
            require_exact_file(&final_file, expected, "store.meta")
        }
    }
}

fn ensure_acknowledgement(
    lifecycle: &File,
) -> Result<[u8; ACKNOWLEDGEMENT_FILE_LENGTH], InitialBootstrapFoundationError> {
    let file = match open_optional_file(lifecycle, ACKNOWLEDGEMENT_FILE, true)? {
        Some(file) => file,
        None => create_exclusive_file(lifecycle, ACKNOWLEDGEMENT_FILE)?,
    };
    validate_regular(&file, "ACKNOWLEDGED.v1")?;
    let length = usize::try_from(file.metadata().map_err(InitialBootstrapFoundationError::io)?.len())
        .map_err(|_| InitialBootstrapFoundationError::invalid("ACKNOWLEDGED.v1 length is not representable"))?;
    if length > ACKNOWLEDGEMENT_FILE_LENGTH {
        return Err(InitialBootstrapFoundationError::invalid("ACKNOWLEDGED.v1 is oversized"));
    }
    let mut bytes = [0_u8; ACKNOWLEDGEMENT_FILE_LENGTH];
    if length != 0 {
        read_exact_at(&file, &mut bytes[..length], 0)?;
    }
    if length < ACKNOWLEDGEMENT_FILE_LENGTH {
        if bytes[..length].iter().any(|byte| *byte != 0) {
            return Err(InitialBootstrapFoundationError::invalid(
                "partial ACKNOWLEDGED.v1 contains nonzero bytes",
            ));
        }
        write_all_at(&file, &bytes[length..], length as u64)?;
    }
    file.sync_all().map_err(InitialBootstrapFoundationError::io)?;
    require_exact_file(&file, &bytes, "ACKNOWLEDGED.v1")?;
    Ok(bytes)
}

fn ensure_generation_zero_log(
    lifecycle: &File,
    acknowledgement: &[u8; ACKNOWLEDGEMENT_FILE_LENGTH],
) -> Result<(), InitialBootstrapFoundationError> {
    if let Some(file) = open_optional_file(lifecycle, GENERATION_ZERO_LOG, true)? {
        validate_regular(&file, "generation-0 log")?;
        return Ok(());
    }
    if acknowledgement.iter().any(|byte| *byte != 0) {
        return Err(InitialBootstrapFoundationError::invalid(
            "acknowledgement bytes exist without generation-0 log",
        ));
    }
    let file = create_exclusive_file(lifecycle, GENERATION_ZERO_LOG)?;
    file.sync_all().map_err(InitialBootstrapFoundationError::io)
}

fn open_required_directory(parent: &File, name: &str) -> Result<File, InitialBootstrapFoundationError> {
    let file = open_relative(parent, name, true, FILE_OPEN, false)?;
    validate_directory(&file, "lifecycle directory")?;
    Ok(file)
}

fn open_optional_file(
    parent: &File,
    name: &str,
    writable: bool,
) -> Result<Option<File>, InitialBootstrapFoundationError> {
    match open_relative_optional(parent, name, false, FILE_OPEN, writable)? {
        Some(file) => {
            validate_regular(&file, "bootstrap sidecar")?;
            Ok(Some(file))
        }
        None => Ok(None),
    }
}

fn open_required_file(parent: &File, name: &str, writable: bool) -> Result<File, InitialBootstrapFoundationError> {
    let file = open_relative(parent, name, false, FILE_OPEN, writable)?;
    validate_regular(&file, "bootstrap sidecar")?;
    Ok(file)
}

fn create_exclusive_file(parent: &File, name: &str) -> Result<File, InitialBootstrapFoundationError> {
    let file = open_relative(parent, name, false, FILE_CREATE, true)?;
    validate_regular(&file, "bootstrap sidecar")?;
    Ok(file)
}

fn open_relative_optional(
    parent: &File,
    name: &str,
    directory: bool,
    disposition: NTCREATEFILE_CREATE_DISPOSITION,
    writable: bool,
) -> Result<Option<File>, InitialBootstrapFoundationError> {
    match open_relative_raw(parent, name, directory, disposition, writable) {
        Ok(file) => Ok(Some(file)),
        Err(OpenRelativeError::Missing) => Ok(None),
        Err(OpenRelativeError::Io(source)) => Err(InitialBootstrapFoundationError::io(source)),
    }
}

fn open_relative(
    parent: &File,
    name: &str,
    directory: bool,
    disposition: NTCREATEFILE_CREATE_DISPOSITION,
    writable: bool,
) -> Result<File, InitialBootstrapFoundationError> {
    open_relative_raw(parent, name, directory, disposition, writable).map_err(|error| match error {
        OpenRelativeError::Missing => InitialBootstrapFoundationError::io(io::Error::new(
            io::ErrorKind::NotFound,
            "required bootstrap artifact is absent",
        )),
        OpenRelativeError::Io(source) => InitialBootstrapFoundationError::io(source),
    })
}

enum OpenRelativeError {
    Missing,
    Io(io::Error),
}

fn open_relative_raw(
    parent: &File,
    name: &str,
    directory: bool,
    disposition: NTCREATEFILE_CREATE_DISPOSITION,
    writable: bool,
) -> Result<File, OpenRelativeError> {
    let mut wide = name.encode_utf16().collect::<Vec<_>>();
    let byte_length = wide
        .len()
        .checked_mul(size_of::<u16>())
        .and_then(|value| u16::try_from(value).ok())
        .ok_or_else(|| OpenRelativeError::Io(io::Error::new(io::ErrorKind::InvalidInput, "relative name too long")))?;
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
        let write = if writable {
            FILE_WRITE_DATA.0 | FILE_APPEND_DATA.0 | DELETE.0
        } else {
            0
        };
        FILE_ACCESS_RIGHTS(FILE_READ_DATA.0 | FILE_READ_ATTRIBUTES.0 | SYNCHRONIZE.0 | write)
    };
    let share = FILE_SHARE_MODE(FILE_SHARE_READ.0 | FILE_SHARE_WRITE.0 | FILE_SHARE_DELETE.0);
    let type_option = if directory {
        FILE_DIRECTORY_FILE.0
    } else {
        FILE_NON_DIRECTORY_FILE.0
    };
    let options = NTCREATEFILE_CREATE_OPTIONS(FILE_OPEN_REPARSE_POINT.0 | FILE_SYNCHRONOUS_IO_NONALERT.0 | type_option);
    let mut handle = HANDLE(ptr::null_mut());
    let mut io_status = IO_STATUS_BLOCK::default();
    // SAFETY: all pointers refer to live storage for this synchronous call. `parent` is a retained
    // verified directory handle and `name` is one component resolved without reparses.
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
            options,
            None,
            0,
        )
    };
    if status == STATUS_OBJECT_NAME_NOT_FOUND {
        return Err(OpenRelativeError::Missing);
    }
    if status.0 < 0 {
        return Err(OpenRelativeError::Io(status_error(status)));
    }
    if handle.is_invalid() {
        return Err(OpenRelativeError::Io(io::Error::other(
            "NtCreateFile returned an invalid handle",
        )));
    }
    // SAFETY: successful NtCreateFile returned unique ownership of a valid kernel handle.
    Ok(unsafe { File::from_raw_handle(handle.0) })
}

fn rename_handle_no_replace(
    source: &File,
    parent: &File,
    destination: &str,
) -> Result<(), InitialBootstrapFoundationError> {
    let mut buffer = rename_buffer(parent, destination, true)?;
    let length = rename_buffer_length(destination)?;
    let mut io_status = IO_STATUS_BLOCK::default();
    // SAFETY: the aligned buffer contains a complete FILE_RENAME_INFORMATION and the verified
    // source and parent handles remain live throughout this synchronous call.
    let status = unsafe {
        NtSetInformationFile(
            HANDLE(source.as_raw_handle()),
            &mut io_status,
            buffer.as_mut_ptr().cast::<c_void>(),
            length,
            FileRenameInformationEx,
        )
    };
    if status.0 >= 0 {
        return Ok(());
    }
    if is_information_class_unsupported(status) {
        let mut fallback = rename_buffer(parent, destination, false)?;
        let mut fallback_status = IO_STATUS_BLOCK::default();
        // SAFETY: the legacy information class consumes the same complete aligned buffer and
        // preserves the no-replace request for the same verified handles.
        let status = unsafe {
            NtSetInformationFile(
                HANDLE(source.as_raw_handle()),
                &mut fallback_status,
                fallback.as_mut_ptr().cast::<c_void>(),
                length,
                FileRenameInformation,
            )
        };
        return status_result(status);
    }
    status_result(status)
}

fn rename_buffer(parent: &File, name: &str, extended: bool) -> Result<Vec<usize>, InitialBootstrapFoundationError> {
    let wide = name.encode_utf16().collect::<Vec<_>>();
    let byte_length = wide
        .len()
        .checked_mul(size_of::<u16>())
        .ok_or_else(|| InitialBootstrapFoundationError::invalid("rename name length overflow"))?;
    let total = size_of::<FILE_RENAME_INFORMATION>()
        .checked_add(byte_length)
        .ok_or_else(|| InitialBootstrapFoundationError::invalid("rename buffer length overflow"))?;
    let mut storage = vec![0_usize; total.div_ceil(size_of::<usize>())];
    let info = storage.as_mut_ptr().cast::<FILE_RENAME_INFORMATION>();
    // SAFETY: Vec<usize> supplies sufficient alignment and capacity for the fixed header and name.
    unsafe {
        if extended {
            (*info).Anonymous.Flags = 0;
        } else {
            (*info).Anonymous.ReplaceIfExists = false;
        }
        (*info).RootDirectory = HANDLE(parent.as_raw_handle());
        (*info).FileNameLength = u32::try_from(byte_length)
            .map_err(|_| InitialBootstrapFoundationError::invalid("rename name length is not representable"))?;
        ptr::copy_nonoverlapping(wide.as_ptr(), (*info).FileName.as_mut_ptr(), wide.len());
    }
    Ok(storage)
}

fn rename_buffer_length(name: &str) -> Result<u32, InitialBootstrapFoundationError> {
    let bytes = name
        .encode_utf16()
        .count()
        .checked_mul(size_of::<u16>())
        .ok_or_else(|| InitialBootstrapFoundationError::invalid("rename name length overflow"))?;
    let total = size_of::<FILE_RENAME_INFORMATION>()
        .checked_add(bytes)
        .ok_or_else(|| InitialBootstrapFoundationError::invalid("rename buffer length overflow"))?;
    u32::try_from(total)
        .map_err(|_| InitialBootstrapFoundationError::invalid("rename buffer length is not representable"))
}

fn status_result(status: NTSTATUS) -> Result<(), InitialBootstrapFoundationError> {
    if status.0 >= 0 {
        Ok(())
    } else {
        Err(InitialBootstrapFoundationError::io(status_error(status)))
    }
}

fn is_information_class_unsupported(status: NTSTATUS) -> bool {
    // SAFETY: RtlNtStatusToDosError accepts every NTSTATUS and has no pointer arguments.
    let code = unsafe { RtlNtStatusToDosError(status) };
    code == ERROR_INVALID_PARAMETER.0 || code == ERROR_NOT_SUPPORTED.0
}

fn complete_exact_prefix(
    file: &File,
    expected: &[u8],
    object: &'static str,
) -> Result<(), InitialBootstrapFoundationError> {
    if require_exact_prefix(file, expected, object)? {
        return Ok(());
    }
    let length = file.metadata().map_err(InitialBootstrapFoundationError::io)?.len();
    let consumed = usize::try_from(length)
        .map_err(|_| InitialBootstrapFoundationError::invalid("artifact length is not representable"))?;
    write_all_at(file, &expected[consumed..], length)
}

fn require_exact_prefix(
    file: &File,
    expected: &[u8],
    object: &'static str,
) -> Result<bool, InitialBootstrapFoundationError> {
    validate_regular(file, object)?;
    let length = usize::try_from(file.metadata().map_err(InitialBootstrapFoundationError::io)?.len())
        .map_err(|_| InitialBootstrapFoundationError::invalid("artifact length is not representable"))?;
    if length > expected.len() {
        return Err(InitialBootstrapFoundationError::invalid(
            "artifact is longer than canonical bytes",
        ));
    }
    if length != 0 {
        let mut prefix = vec![0_u8; length];
        read_exact_at(file, &mut prefix, 0)?;
        if prefix != expected[..length] {
            return Err(InitialBootstrapFoundationError::invalid(
                "artifact prefix differs from canonical bytes",
            ));
        }
    }
    Ok(length == expected.len())
}

fn require_exact_file(
    file: &File,
    expected: &[u8],
    object: &'static str,
) -> Result<(), InitialBootstrapFoundationError> {
    validate_regular(file, object)?;
    let actual_length = file.metadata().map_err(InitialBootstrapFoundationError::io)?.len();
    if actual_length != expected.len() as u64 {
        return Err(InitialBootstrapFoundationError::invalid(
            "artifact length differs from canonical bytes",
        ));
    }
    let mut actual = vec![0_u8; expected.len()];
    read_exact_at(file, &mut actual, 0)?;
    if actual != expected {
        return Err(InitialBootstrapFoundationError::invalid(
            "artifact bytes differ from canonical bytes",
        ));
    }
    Ok(())
}

fn validate_directory(file: &File, object: &'static str) -> Result<(), InitialBootstrapFoundationError> {
    let (attributes, standard) = handle_information(file)?;
    if attributes.FileAttributes & FILE_ATTRIBUTE_REPARSE_POINT.0 != 0 {
        return Err(InitialBootstrapFoundationError::invalid(match object {
            "Store root" => "Store root is a reparse point",
            _ => "lifecycle directory is a reparse point",
        }));
    }
    if !standard.Directory || attributes.FileAttributes & FILE_ATTRIBUTE_DIRECTORY.0 == 0 {
        return Err(InitialBootstrapFoundationError::invalid(match object {
            "Store root" => "Store root is not a directory",
            _ => "lifecycle handle is not a directory",
        }));
    }
    Ok(())
}

fn validate_regular(file: &File, object: &'static str) -> Result<(), InitialBootstrapFoundationError> {
    let (attributes, standard) = handle_information(file)?;
    if attributes.FileAttributes & FILE_ATTRIBUTE_REPARSE_POINT.0 != 0
        || standard.Directory
        || attributes.FileAttributes & FILE_ATTRIBUTE_DIRECTORY.0 != 0
        || standard.NumberOfLinks != 1
    {
        return Err(InitialBootstrapFoundationError::invalid(match object {
            "store.meta" => "store.meta is not a single-link regular file",
            "store.meta bootstrap temporary" => "store.meta temporary is not a single-link regular file",
            "ACKNOWLEDGED.v1" => "ACKNOWLEDGED.v1 is not a single-link regular file",
            "generation-0 log" => "generation-0 log is not a single-link regular file",
            _ => "bootstrap artifact is not a single-link regular file",
        }));
    }
    Ok(())
}

fn handle_information(
    file: &File,
) -> Result<(FILE_ATTRIBUTE_TAG_INFO, FILE_STANDARD_INFO), InitialBootstrapFoundationError> {
    Ok((
        query_information::<FILE_ATTRIBUTE_TAG_INFO>(file, FileAttributeTagInfo)?,
        query_information::<FILE_STANDARD_INFO>(file, FileStandardInfo)?,
    ))
}

fn query_information<T: Copy>(
    file: &File,
    class: windows::Win32::Storage::FileSystem::FILE_INFO_BY_HANDLE_CLASS,
) -> Result<T, InitialBootstrapFoundationError> {
    let mut output = MaybeUninit::<T>::uninit();
    // SAFETY: the retained handle remains borrowed and output is aligned writable storage of the
    // exact information-class size; Windows initializes it completely on success.
    unsafe {
        GetFileInformationByHandleEx(
            HANDLE(file.as_raw_handle()),
            class,
            output.as_mut_ptr().cast(),
            size_of::<T>() as u32,
        )
        .map_err(|error| InitialBootstrapFoundationError::io(windows_error_to_io(error)))?;
        Ok(output.assume_init())
    }
}

fn physical_identity(file: &File) -> Result<PhysicalFileKey, InitialBootstrapFoundationError> {
    physical_file_key(file).map_err(InitialBootstrapFoundationError::io)
}

fn require_identity(
    actual: PhysicalFileKey,
    expected: PhysicalFileKey,
    detail: &'static str,
) -> Result<(), InitialBootstrapFoundationError> {
    if actual == expected {
        Ok(())
    } else {
        Err(InitialBootstrapFoundationError::invalid(detail))
    }
}

fn require_same_file(left: &File, right: &File, detail: &'static str) -> Result<(), InitialBootstrapFoundationError> {
    require_identity(physical_identity(left)?, physical_identity(right)?, detail)
}

fn write_all_at(file: &File, mut bytes: &[u8], mut offset: u64) -> Result<(), InitialBootstrapFoundationError> {
    let mut interrupted = 0;
    while !bytes.is_empty() {
        match file.seek_write(bytes, offset) {
            Ok(0) => {
                return Err(InitialBootstrapFoundationError::io(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "bootstrap positional write returned zero",
                )));
            }
            Ok(written) => {
                interrupted = 0;
                bytes = &bytes[written..];
                offset = offset
                    .checked_add(written as u64)
                    .ok_or_else(|| InitialBootstrapFoundationError::invalid("write offset overflow"))?;
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted && interrupted < MAX_INTERRUPTED_RETRIES => {
                interrupted += 1;
            }
            Err(error) => return Err(InitialBootstrapFoundationError::io(error)),
        }
    }
    Ok(())
}

fn read_exact_at(file: &File, mut bytes: &mut [u8], mut offset: u64) -> Result<(), InitialBootstrapFoundationError> {
    let mut interrupted = 0;
    while !bytes.is_empty() {
        match file.seek_read(bytes, offset) {
            Ok(0) => {
                return Err(InitialBootstrapFoundationError::io(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "bootstrap positional read reached EOF",
                )));
            }
            Ok(read) => {
                interrupted = 0;
                let (_, remaining) = bytes.split_at_mut(read);
                bytes = remaining;
                offset = offset
                    .checked_add(read as u64)
                    .ok_or_else(|| InitialBootstrapFoundationError::invalid("read offset overflow"))?;
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted && interrupted < MAX_INTERRUPTED_RETRIES => {
                interrupted += 1;
            }
            Err(error) => return Err(InitialBootstrapFoundationError::io(error)),
        }
    }
    Ok(())
}

fn is_ntfs_volume(file: &File) -> Result<bool, InitialBootstrapFoundationError> {
    let mut filesystem_name = [0_u16; 32];
    // SAFETY: the retained root handle remains borrowed and the fixed UTF-16 output buffer is live.
    unsafe {
        GetVolumeInformationByHandleW(
            HANDLE(file.as_raw_handle()),
            None,
            None,
            None,
            None,
            Some(&mut filesystem_name),
        )
        .map_err(|error| InitialBootstrapFoundationError::io(windows_error_to_io(error)))?;
    }
    let length = filesystem_name
        .iter()
        .position(|unit| *unit == 0)
        .unwrap_or(filesystem_name.len());
    Ok(String::from_utf16_lossy(&filesystem_name[..length]).eq_ignore_ascii_case("NTFS"))
}

fn windows_error_to_io(error: windows::core::Error) -> io::Error {
    windows::Win32::Foundation::WIN32_ERROR::from_error(&error)
        .map(|code| io::Error::from_raw_os_error(code.0 as i32))
        .unwrap_or_else(|| io::Error::other(error))
}

fn status_error(status: NTSTATUS) -> io::Error {
    // SAFETY: RtlNtStatusToDosError accepts every NTSTATUS value and has no pointer arguments.
    let code = unsafe { RtlNtStatusToDosError(status) };
    io::Error::from_raw_os_error(code as i32)
}
