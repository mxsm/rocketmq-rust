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

use std::ffi::CStr;
use std::fs::File;
use std::io;
use std::os::fd::AsRawFd;
use std::os::fd::FromRawFd;
use std::os::unix::fs::FileExt;
use std::os::unix::fs::MetadataExt;

use super::InitialBootstrapFoundationFailure;
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
use crate::mapped_file::retirement::io::FileLedgerIo;
use crate::mapped_file::retirement::sidecar::decode_store_meta;
use crate::mapped_file::retirement::sidecar::encode_store_meta;
use crate::mapped_file::retirement::sidecar::StoreMeta;
use crate::mapped_file::retirement::sidecar::STORE_META_LENGTH;

const LIFECYCLE_DIRECTORY: &CStr = c".rocketmq-lifecycle";
const STORE_META_FILE: &CStr = c"store.meta";
const STORE_META_TEMP_FILE: &CStr = c"store.meta.bootstrap.tmp";
const ACKNOWLEDGEMENT_FILE: &CStr = c"ACKNOWLEDGED.v1";
const GENERATION_ZERO_LOG: &CStr = c"retirement.log.g00000000000000000000";
const GENERATION_ZERO_SNAPSHOT: &CStr = c"manifest.snapshot.g00000000000000000000";
const GENERATION_ZERO_SNAPSHOT_TEMP: &CStr = c"manifest.snapshot.g00000000000000000000.bootstrap.tmp";
const ENABLED_MARKER: &CStr = c"ENABLED.v1";
const ENABLED_MARKER_TEMP: &CStr = c"ENABLED.v1.bootstrap.tmp";
const ACKNOWLEDGEMENT_FILE_LENGTH: usize = 208;
const MAX_INTERRUPTED_RETRIES: usize = 16;
const STRICT_RESOLVE: u64 =
    libc::RESOLVE_BENEATH | libc::RESOLVE_NO_MAGICLINKS | libc::RESOLVE_NO_SYMLINKS | libc::RESOLVE_NO_XDEV;

pub(super) fn prepare(
    store_root: File,
    expected_meta: &StoreMeta,
) -> Result<PreparedInitialBootstrapFoundation, InitialBootstrapFoundationFailure> {
    let root_metadata = store_root.metadata().map_err(InitialBootstrapFoundationFailure::io)?;
    if !root_metadata.is_dir() {
        return Err(InitialBootstrapFoundationFailure::invalid(
            "Store root is not a directory",
        ));
    }
    let lifecycle = open_or_create_lifecycle_directory(&store_root, root_metadata.dev())?;
    let canonical_meta = encode_store_meta(expected_meta).map_err(InitialBootstrapFoundationFailure::sidecar)?;
    publish_store_meta(&lifecycle, &canonical_meta)?;
    let acknowledgement = ensure_acknowledgement(&lifecycle)?;
    ensure_generation_zero_log(&lifecycle, &acknowledgement)?;

    let decoded_meta = decode_store_meta(&canonical_meta).map_err(InitialBootstrapFoundationFailure::sidecar)?;
    let foundation = BootstrapFoundationEvidence {
        store_meta: CanonicalStoreMetaEvidence {
            meta: decoded_meta,
            canonical_bytes: canonical_meta,
            stored_crc32: u32::from_le_bytes(
                canonical_meta[60..64]
                    .try_into()
                    .map_err(|_| InitialBootstrapFoundationFailure::invalid("store.meta CRC field is unavailable"))?,
            ),
        },
    };
    let ledger =
        FileLedgerIo::open_from_store_root(&store_root, 0).map_err(InitialBootstrapFoundationFailure::ledger)?;
    let artifacts = InitialArtifactStore::new(
        store_root.try_clone().map_err(InitialBootstrapFoundationFailure::io)?,
        lifecycle.try_clone().map_err(InitialBootstrapFoundationFailure::io)?,
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
    device: u64,
    inode: u64,
    snapshot_temporary_synced: bool,
    snapshot_verified: bool,
    marker_temporary_synced: bool,
    marker_directory_synced: bool,
    marker_verified: bool,
}

impl InitialArtifactStore {
    fn new(store_root: File, lifecycle: File) -> Result<Self, InitialBootstrapFoundationFailure> {
        let metadata = lifecycle.metadata().map_err(InitialBootstrapFoundationFailure::io)?;
        if !metadata.is_dir() {
            return Err(InitialBootstrapFoundationFailure::invalid(
                "retained lifecycle handle is not a directory",
            ));
        }
        Ok(Self {
            store_root,
            lifecycle,
            device: metadata.dev(),
            inode: metadata.ino(),
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
    ) -> Result<ImmutableArtifactProgress, InitialBootstrapFoundationFailure> {
        self.verify_lifecycle()?;
        let final_file = open_optional(
            &self.lifecycle,
            GENERATION_ZERO_SNAPSHOT,
            libc::O_RDONLY | libc::O_NONBLOCK,
        )?;
        let temporary = open_optional(
            &self.lifecycle,
            GENERATION_ZERO_SNAPSHOT_TEMP,
            libc::O_RDWR | libc::O_NONBLOCK,
        )?;
        match (final_file, temporary) {
            (Some(final_file), None) => {
                require_exact_file(&final_file, &planned.encoded, "bootstrap snapshot")?;
                Ok(if self.snapshot_verified {
                    ImmutableArtifactProgress::Verified
                } else {
                    ImmutableArtifactProgress::Published
                })
            }
            (Some(_), Some(_)) => Err(InitialBootstrapFoundationFailure::invalid(
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
    ) -> Result<(), InitialBootstrapFoundationFailure> {
        self.verify_lifecycle()?;
        match step {
            ImmutableArtifactStep::WriteTemporary => {
                let temporary = match open_optional(
                    &self.lifecycle,
                    GENERATION_ZERO_SNAPSHOT_TEMP,
                    libc::O_RDWR | libc::O_NONBLOCK,
                )? {
                    Some(file) => file,
                    None => create_exclusive(&self.lifecycle, GENERATION_ZERO_SNAPSHOT_TEMP)?,
                };
                complete_exact_prefix(&temporary, &planned.encoded, "bootstrap snapshot temporary")
            }
            ImmutableArtifactStep::SyncTemporary => {
                let temporary = open_required(
                    &self.lifecycle,
                    GENERATION_ZERO_SNAPSHOT_TEMP,
                    libc::O_RDWR | libc::O_NONBLOCK,
                )?;
                require_exact_file(&temporary, &planned.encoded, "bootstrap snapshot temporary")?;
                temporary.sync_all().map_err(InitialBootstrapFoundationFailure::io)?;
                self.snapshot_temporary_synced = true;
                Ok(())
            }
            ImmutableArtifactStep::PublishFinalNoReplace => {
                if !self.snapshot_temporary_synced {
                    return Err(InitialBootstrapFoundationFailure::invalid(
                        "bootstrap snapshot temporary was not synced in this process",
                    ));
                }
                rename_no_replace(&self.lifecycle, GENERATION_ZERO_SNAPSHOT_TEMP, GENERATION_ZERO_SNAPSHOT)
            }
            ImmutableArtifactStep::ReopenAndVerify => {
                self.lifecycle
                    .sync_all()
                    .map_err(InitialBootstrapFoundationFailure::io)?;
                let final_file = open_required(
                    &self.lifecycle,
                    GENERATION_ZERO_SNAPSHOT,
                    libc::O_RDONLY | libc::O_NONBLOCK,
                )?;
                require_exact_file(&final_file, &planned.encoded, "bootstrap snapshot")?;
                self.snapshot_verified = true;
                Ok(())
            }
        }
    }

    pub(super) fn inspect_initial_marker(
        &self,
        planned: &PlannedInitialMarker,
    ) -> Result<InitialMarkerProgress, InitialBootstrapFoundationFailure> {
        self.verify_lifecycle()?;
        let final_file = open_optional(&self.lifecycle, ENABLED_MARKER, libc::O_RDONLY | libc::O_NONBLOCK)?;
        let temporary = open_optional(&self.lifecycle, ENABLED_MARKER_TEMP, libc::O_RDWR | libc::O_NONBLOCK)?;
        match (final_file, temporary) {
            (Some(final_file), None) => {
                require_exact_file(&final_file, &planned.encoded_file, "ENABLED.v1")?;
                if self.marker_verified {
                    let evidence =
                        InitialMarkerVerificationEvidence::from_reopened_bytes(planned.encoded_file, planned)
                            .ok_or_else(|| {
                                InitialBootstrapFoundationFailure::invalid("marker verification mismatch")
                            })?;
                    Ok(InitialMarkerProgress::Verified(Box::new(evidence)))
                } else if self.marker_directory_synced {
                    Ok(InitialMarkerProgress::DirectorySynced)
                } else {
                    Ok(InitialMarkerProgress::Published)
                }
            }
            (Some(_), Some(_)) => Err(InitialBootstrapFoundationFailure::invalid(
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
    ) -> Result<(), InitialBootstrapFoundationFailure> {
        self.verify_lifecycle()?;
        match step {
            InitialMarkerStep::WriteTemporary => {
                let temporary =
                    match open_optional(&self.lifecycle, ENABLED_MARKER_TEMP, libc::O_RDWR | libc::O_NONBLOCK)? {
                        Some(file) => file,
                        None => create_exclusive(&self.lifecycle, ENABLED_MARKER_TEMP)?,
                    };
                complete_exact_prefix(&temporary, &planned.encoded_file, "ENABLED.v1 temporary")
            }
            InitialMarkerStep::SyncTemporary => {
                let temporary = open_required(&self.lifecycle, ENABLED_MARKER_TEMP, libc::O_RDWR | libc::O_NONBLOCK)?;
                require_exact_file(&temporary, &planned.encoded_file, "ENABLED.v1 temporary")?;
                temporary.sync_all().map_err(InitialBootstrapFoundationFailure::io)?;
                self.marker_temporary_synced = true;
                Ok(())
            }
            InitialMarkerStep::PublishFinalNoReplace => {
                if !self.marker_temporary_synced {
                    return Err(InitialBootstrapFoundationFailure::invalid(
                        "ENABLED.v1 temporary was not synced in this process",
                    ));
                }
                rename_no_replace(&self.lifecycle, ENABLED_MARKER_TEMP, ENABLED_MARKER)
            }
            InitialMarkerStep::SyncLifecycleDirectory => {
                self.lifecycle
                    .sync_all()
                    .map_err(InitialBootstrapFoundationFailure::io)?;
                self.marker_directory_synced = true;
                Ok(())
            }
            InitialMarkerStep::ReopenAndVerifyEntireFile => {
                if !self.marker_directory_synced {
                    return Err(InitialBootstrapFoundationFailure::invalid(
                        "ENABLED.v1 directory entry was not synced in this process",
                    ));
                }
                let final_file = open_required(&self.lifecycle, ENABLED_MARKER, libc::O_RDONLY | libc::O_NONBLOCK)?;
                require_exact_file(&final_file, &planned.encoded_file, "ENABLED.v1")?;
                self.marker_verified = true;
                Ok(())
            }
        }
    }

    fn verify_lifecycle(&self) -> Result<(), InitialBootstrapFoundationFailure> {
        let metadata = self
            .lifecycle
            .metadata()
            .map_err(InitialBootstrapFoundationFailure::io)?;
        let reopened = open_required(
            &self.store_root,
            LIFECYCLE_DIRECTORY,
            libc::O_RDONLY | libc::O_DIRECTORY,
        )?;
        let reopened_metadata = reopened.metadata().map_err(InitialBootstrapFoundationFailure::io)?;
        if metadata.is_dir()
            && metadata.dev() == self.device
            && metadata.ino() == self.inode
            && reopened_metadata.is_dir()
            && reopened_metadata.dev() == self.device
            && reopened_metadata.ino() == self.inode
        {
            Ok(())
        } else {
            Err(InitialBootstrapFoundationFailure::invalid(
                "retained lifecycle directory is no longer bound beneath the retained Store root",
            ))
        }
    }
}

fn open_or_create_lifecycle_directory(
    store_root: &File,
    root_device: u64,
) -> Result<File, InitialBootstrapFoundationFailure> {
    let lifecycle = match open_optional(store_root, LIFECYCLE_DIRECTORY, libc::O_RDONLY | libc::O_DIRECTORY)? {
        Some(directory) => directory,
        None => {
            // SAFETY: the retained root descriptor is live, the name is a single NUL-terminated
            // component, and no pointer escapes this call.
            let result = unsafe { libc::mkdirat(store_root.as_raw_fd(), LIFECYCLE_DIRECTORY.as_ptr(), 0o700) };
            if result != 0 {
                return Err(InitialBootstrapFoundationFailure::io(io::Error::last_os_error()));
            }
            store_root.sync_all().map_err(InitialBootstrapFoundationFailure::io)?;
            open_required(store_root, LIFECYCLE_DIRECTORY, libc::O_RDONLY | libc::O_DIRECTORY)?
        }
    };
    let metadata = lifecycle.metadata().map_err(InitialBootstrapFoundationFailure::io)?;
    if !metadata.is_dir() || metadata.dev() != root_device {
        return Err(InitialBootstrapFoundationFailure::invalid(
            "lifecycle directory is not a contained same-device directory",
        ));
    }
    Ok(lifecycle)
}

fn publish_store_meta(
    lifecycle: &File,
    expected: &[u8; STORE_META_LENGTH],
) -> Result<(), InitialBootstrapFoundationFailure> {
    let final_file = open_optional(lifecycle, STORE_META_FILE, libc::O_RDONLY | libc::O_NONBLOCK)?;
    let temporary = open_optional(lifecycle, STORE_META_TEMP_FILE, libc::O_RDWR | libc::O_NONBLOCK)?;
    match (final_file, temporary) {
        (Some(final_file), None) => {
            require_exact_file(&final_file, expected, "store.meta")?;
            lifecycle.sync_all().map_err(InitialBootstrapFoundationFailure::io)
        }
        (Some(_), Some(_)) => Err(InitialBootstrapFoundationFailure::invalid(
            "store.meta final and bootstrap temporary both exist",
        )),
        (None, temporary) => {
            let temporary = match temporary {
                Some(file) => file,
                None => create_exclusive(lifecycle, STORE_META_TEMP_FILE)?,
            };
            complete_exact_prefix(&temporary, expected, "store.meta bootstrap temporary")?;
            temporary.sync_all().map_err(InitialBootstrapFoundationFailure::io)?;
            require_exact_file(&temporary, expected, "store.meta bootstrap temporary")?;
            rename_no_replace(lifecycle, STORE_META_TEMP_FILE, STORE_META_FILE)?;
            lifecycle.sync_all().map_err(InitialBootstrapFoundationFailure::io)?;
            let final_file = open_required(lifecycle, STORE_META_FILE, libc::O_RDONLY | libc::O_NONBLOCK)?;
            require_same_file(&temporary, &final_file, "store.meta publication changed identity")?;
            require_exact_file(&final_file, expected, "store.meta")
        }
    }
}

fn ensure_acknowledgement(
    lifecycle: &File,
) -> Result<[u8; ACKNOWLEDGEMENT_FILE_LENGTH], InitialBootstrapFoundationFailure> {
    let file = match open_optional(lifecycle, ACKNOWLEDGEMENT_FILE, libc::O_RDWR | libc::O_NONBLOCK)? {
        Some(file) => file,
        None => create_exclusive(lifecycle, ACKNOWLEDGEMENT_FILE)?,
    };
    validate_regular(&file, "ACKNOWLEDGED.v1")?;
    let length = usize::try_from(file.metadata().map_err(InitialBootstrapFoundationFailure::io)?.len())
        .map_err(|_| InitialBootstrapFoundationFailure::invalid("ACKNOWLEDGED.v1 length is not representable"))?;
    if length > ACKNOWLEDGEMENT_FILE_LENGTH {
        return Err(InitialBootstrapFoundationFailure::invalid(
            "ACKNOWLEDGED.v1 is oversized",
        ));
    }
    let mut bytes = [0_u8; ACKNOWLEDGEMENT_FILE_LENGTH];
    if length != 0 {
        read_exact_at(&file, &mut bytes[..length], 0)?;
    }
    if length < ACKNOWLEDGEMENT_FILE_LENGTH {
        if bytes[..length].iter().any(|byte| *byte != 0) {
            return Err(InitialBootstrapFoundationFailure::invalid(
                "partial ACKNOWLEDGED.v1 contains nonzero bytes",
            ));
        }
        write_all_at(&file, &bytes[length..], length as u64)?;
    }
    file.sync_all().map_err(InitialBootstrapFoundationFailure::io)?;
    require_exact_file(&file, &bytes, "ACKNOWLEDGED.v1")?;
    lifecycle.sync_all().map_err(InitialBootstrapFoundationFailure::io)?;
    Ok(bytes)
}

fn ensure_generation_zero_log(
    lifecycle: &File,
    acknowledgement: &[u8; ACKNOWLEDGEMENT_FILE_LENGTH],
) -> Result<(), InitialBootstrapFoundationFailure> {
    if let Some(file) = open_optional(lifecycle, GENERATION_ZERO_LOG, libc::O_RDWR | libc::O_NONBLOCK)? {
        validate_regular(&file, "generation-0 log")?;
        return Ok(());
    }
    if acknowledgement.iter().any(|byte| *byte != 0) {
        return Err(InitialBootstrapFoundationFailure::invalid(
            "acknowledgement bytes exist without generation-0 log",
        ));
    }
    let file = create_exclusive(lifecycle, GENERATION_ZERO_LOG)?;
    file.sync_all().map_err(InitialBootstrapFoundationFailure::io)?;
    lifecycle.sync_all().map_err(InitialBootstrapFoundationFailure::io)
}

fn open_optional(parent: &File, name: &CStr, flags: i32) -> Result<Option<File>, InitialBootstrapFoundationFailure> {
    match openat2(parent, name, flags, 0) {
        Ok(file) => Ok(Some(file)),
        Err(error) if error.raw_os_error() == Some(libc::ENOENT) => Ok(None),
        Err(error) => Err(InitialBootstrapFoundationFailure::io(error)),
    }
}

fn open_required(parent: &File, name: &CStr, flags: i32) -> Result<File, InitialBootstrapFoundationFailure> {
    openat2(parent, name, flags, 0).map_err(InitialBootstrapFoundationFailure::io)
}

fn create_exclusive(parent: &File, name: &CStr) -> Result<File, InitialBootstrapFoundationFailure> {
    openat2(
        parent,
        name,
        libc::O_RDWR | libc::O_NONBLOCK | libc::O_CREAT | libc::O_EXCL,
        0o600,
    )
    .map_err(InitialBootstrapFoundationFailure::io)
}

fn openat2(parent: &File, name: &CStr, flags: i32, mode: u32) -> io::Result<File> {
    // SAFETY: all-zero is a valid version-zero `open_how` value.
    let mut how: libc::open_how = unsafe { std::mem::zeroed() };
    how.flags = u64::try_from(flags | libc::O_CLOEXEC | libc::O_NOFOLLOW)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid open flags"))?;
    how.mode = u64::from(mode);
    how.resolve = STRICT_RESOLVE;
    // SAFETY: `parent` owns a live directory descriptor, `name` is a live single component, and
    // `how` is a version-zero structure whose mode is present only for create calls.
    let result = unsafe {
        libc::syscall(
            libc::SYS_openat2,
            parent.as_raw_fd(),
            name.as_ptr(),
            &how,
            std::mem::size_of::<libc::open_how>(),
        )
    };
    if result < 0 {
        return Err(io::Error::last_os_error());
    }
    let descriptor =
        i32::try_from(result).map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "openat2 descriptor overflow"))?;
    // SAFETY: successful openat2 returned this descriptor uniquely and `File` takes sole ownership.
    Ok(unsafe { File::from_raw_fd(descriptor) })
}

fn rename_no_replace(
    parent: &File,
    source: &CStr,
    destination: &CStr,
) -> Result<(), InitialBootstrapFoundationFailure> {
    // SAFETY: both names are live NUL-terminated components beneath the same retained directory;
    // `RENAME_NOREPLACE` forbids overwriting an existing final artifact.
    let result = unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            parent.as_raw_fd(),
            source.as_ptr(),
            parent.as_raw_fd(),
            destination.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    };
    if result < 0 {
        Err(InitialBootstrapFoundationFailure::io(io::Error::last_os_error()))
    } else {
        Ok(())
    }
}

fn complete_exact_prefix(
    file: &File,
    expected: &[u8],
    object: &'static str,
) -> Result<(), InitialBootstrapFoundationFailure> {
    let complete = require_exact_prefix(file, expected, object)?;
    if complete {
        return Ok(());
    }
    let length = file.metadata().map_err(InitialBootstrapFoundationFailure::io)?.len();
    let consumed = usize::try_from(length)
        .map_err(|_| InitialBootstrapFoundationFailure::invalid("artifact length is not representable"))?;
    write_all_at(file, &expected[consumed..], length)
}

fn require_exact_prefix(
    file: &File,
    expected: &[u8],
    object: &'static str,
) -> Result<bool, InitialBootstrapFoundationFailure> {
    validate_regular(file, object)?;
    let length = usize::try_from(file.metadata().map_err(InitialBootstrapFoundationFailure::io)?.len())
        .map_err(|_| InitialBootstrapFoundationFailure::invalid("artifact length is not representable"))?;
    if length > expected.len() {
        return Err(InitialBootstrapFoundationFailure::invalid(
            "artifact is longer than canonical bytes",
        ));
    }
    if length != 0 {
        let mut prefix = vec![0_u8; length];
        read_exact_at(file, &mut prefix, 0)?;
        if prefix != expected[..length] {
            return Err(InitialBootstrapFoundationFailure::invalid(
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
) -> Result<(), InitialBootstrapFoundationFailure> {
    validate_regular(file, object)?;
    let actual_length = file.metadata().map_err(InitialBootstrapFoundationFailure::io)?.len();
    if actual_length != expected.len() as u64 {
        return Err(InitialBootstrapFoundationFailure::invalid(
            "artifact length differs from canonical bytes",
        ));
    }
    let mut actual = vec![0_u8; expected.len()];
    read_exact_at(file, &mut actual, 0)?;
    if actual != expected {
        return Err(InitialBootstrapFoundationFailure::invalid(
            "artifact bytes differ from canonical bytes",
        ));
    }
    Ok(())
}

fn validate_regular(file: &File, object: &'static str) -> Result<(), InitialBootstrapFoundationFailure> {
    let metadata = file.metadata().map_err(InitialBootstrapFoundationFailure::io)?;
    if !metadata.is_file() || metadata.nlink() != 1 {
        return Err(InitialBootstrapFoundationFailure::invalid(match object {
            "store.meta" => "store.meta is not a single-link regular file",
            "store.meta bootstrap temporary" => "store.meta temporary is not a single-link regular file",
            "ACKNOWLEDGED.v1" => "ACKNOWLEDGED.v1 is not a single-link regular file",
            "generation-0 log" => "generation-0 log is not a single-link regular file",
            _ => "bootstrap artifact is not a single-link regular file",
        }));
    }
    Ok(())
}

fn require_same_file(left: &File, right: &File, detail: &'static str) -> Result<(), InitialBootstrapFoundationFailure> {
    let left = left.metadata().map_err(InitialBootstrapFoundationFailure::io)?;
    let right = right.metadata().map_err(InitialBootstrapFoundationFailure::io)?;
    if left.dev() == right.dev() && left.ino() == right.ino() {
        Ok(())
    } else {
        Err(InitialBootstrapFoundationFailure::invalid(detail))
    }
}

fn write_all_at(file: &File, mut bytes: &[u8], mut offset: u64) -> Result<(), InitialBootstrapFoundationFailure> {
    let mut interrupted = 0;
    while !bytes.is_empty() {
        match file.write_at(bytes, offset) {
            Ok(0) => {
                return Err(InitialBootstrapFoundationFailure::io(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "bootstrap positional write returned zero",
                )));
            }
            Ok(written) => {
                interrupted = 0;
                bytes = &bytes[written..];
                offset = offset
                    .checked_add(written as u64)
                    .ok_or_else(|| InitialBootstrapFoundationFailure::invalid("write offset overflow"))?;
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted && interrupted < MAX_INTERRUPTED_RETRIES => {
                interrupted += 1;
            }
            Err(error) => return Err(InitialBootstrapFoundationFailure::io(error)),
        }
    }
    Ok(())
}

fn read_exact_at(file: &File, mut bytes: &mut [u8], mut offset: u64) -> Result<(), InitialBootstrapFoundationFailure> {
    let mut interrupted = 0;
    while !bytes.is_empty() {
        match file.read_at(bytes, offset) {
            Ok(0) => {
                return Err(InitialBootstrapFoundationFailure::io(io::Error::new(
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
                    .ok_or_else(|| InitialBootstrapFoundationFailure::invalid("read offset overflow"))?;
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted && interrupted < MAX_INTERRUPTED_RETRIES => {
                interrupted += 1;
            }
            Err(error) => return Err(InitialBootstrapFoundationFailure::io(error)),
        }
    }
    Ok(())
}
