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
use std::fs::Metadata;
use std::io;
use std::os::fd::AsRawFd;
use std::os::fd::FromRawFd;
use std::os::unix::fs::FileExt;
use std::os::unix::fs::MetadataExt;

use super::IoOperation;
use super::LedgerIo;
use super::LedgerIoFailure;
use crate::mapped_file::retirement::codec::ACKNOWLEDGEMENT_FILE_LENGTH;
use crate::mapped_file::retirement::codec::ACKNOWLEDGEMENT_SLOT_LENGTH;

const LIFECYCLE_DIRECTORY: &str = ".rocketmq-lifecycle";
const ACKNOWLEDGEMENT_FILE: &str = "ACKNOWLEDGED.v1";
const MAX_INTERRUPTED_RETRIES: usize = 16;
#[cfg(not(target_os = "linux"))]
const MISSING_CONTAINMENT_REASON: &str = "mount escape is not excluded without an openat2-equivalent containment proof";
#[cfg(target_os = "linux")]
const STRICT_RESOLVE: u64 =
    libc::RESOLVE_BENEATH | libc::RESOLVE_NO_MAGICLINKS | libc::RESOLVE_NO_SYMLINKS | libc::RESOLVE_NO_XDEV;

/// Unix ledger backend opened strictly relative to the caller's Store root handle.
pub(in crate::mapped_file::retirement) struct FileLedgerIo {
    store_root: File,
    lifecycle_directory: File,
    lifecycle_identity: UnixFileIdentity,
    log_name: String,
    log: File,
    log_identity: UnixFileIdentity,
    acknowledgement: File,
    acknowledgement_identity: UnixFileIdentity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct UnixFileIdentity {
    device: u64,
    inode: u64,
}

impl FileLedgerIo {
    pub(in crate::mapped_file::retirement) fn open_from_store_root(
        store_root: &File,
        log_generation: u64,
    ) -> Result<Self, LedgerIoFailure> {
        #[cfg(target_os = "linux")]
        {
            Self::open_validated(store_root, log_generation)
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = (store_root, log_generation);
            Err(LedgerIoFailure::UnsupportedPlatform {
                platform: "unix",
                reason: MISSING_CONTAINMENT_REASON,
            })
        }
    }

    fn open_validated(store_root: &File, log_generation: u64) -> Result<Self, LedgerIoFailure> {
        let retained_root = store_root
            .try_clone()
            .map_err(|source| LedgerIoFailure::io(IoOperation::InspectHandle, source))?;
        let root_metadata = metadata(&retained_root, IoOperation::InspectHandle)?;
        if !root_metadata.is_dir() {
            return Err(LedgerIoFailure::NotDirectory { object: "Store root" });
        }
        let lifecycle_directory =
            open_existing_directory(&retained_root, LIFECYCLE_DIRECTORY, IoOperation::OpenLifecycleDirectory)?;
        let lifecycle_metadata = metadata(&lifecycle_directory, IoOperation::InspectHandle)?;
        if root_metadata.dev() != lifecycle_metadata.dev() {
            return Err(LedgerIoFailure::CrossDeviceLifecycleDirectory);
        }

        let log_name = format!("retirement.log.g{log_generation:020}");
        let log = open_existing_file(&lifecycle_directory, &log_name, IoOperation::OpenLog)?;
        let acknowledgement = open_existing_file(
            &lifecycle_directory,
            ACKNOWLEDGEMENT_FILE,
            IoOperation::OpenAcknowledgementFile,
        )?;
        let log_metadata = validate_file(&log, "retirement log", lifecycle_metadata.dev())?;
        let acknowledgement_metadata =
            validate_file(&acknowledgement, "acknowledgement file", lifecycle_metadata.dev())?;
        require_length(
            "acknowledgement file",
            acknowledgement_metadata.len(),
            ACKNOWLEDGEMENT_FILE_LENGTH as u64,
        )?;

        Ok(Self {
            store_root: retained_root,
            lifecycle_directory,
            lifecycle_identity: file_identity(&lifecycle_metadata),
            log_name,
            log,
            log_identity: file_identity(&log_metadata),
            acknowledgement,
            acknowledgement_identity: file_identity(&acknowledgement_metadata),
        })
    }

    fn verify_bindings(&self) -> Result<(), LedgerIoFailure> {
        let root_metadata = metadata(&self.store_root, IoOperation::InspectHandle)?;
        if !root_metadata.is_dir() {
            return Err(LedgerIoFailure::NotDirectory { object: "Store root" });
        }
        let lifecycle_directory = open_existing_directory(
            &self.store_root,
            LIFECYCLE_DIRECTORY,
            IoOperation::OpenLifecycleDirectory,
        )?;
        let lifecycle_metadata = metadata(&lifecycle_directory, IoOperation::InspectHandle)?;
        if root_metadata.dev() != lifecycle_metadata.dev() {
            return Err(LedgerIoFailure::CrossDeviceLifecycleDirectory);
        }
        require_identity(
            "lifecycle directory",
            file_identity(&lifecycle_metadata),
            self.lifecycle_identity,
        )?;
        require_identity(
            "retained lifecycle directory",
            file_identity(&metadata(&self.lifecycle_directory, IoOperation::InspectHandle)?),
            self.lifecycle_identity,
        )?;

        let log = open_existing_file(&lifecycle_directory, &self.log_name, IoOperation::OpenLog)?;
        let log_metadata = validate_file(&log, "retirement log", lifecycle_metadata.dev())?;
        require_identity("retirement log", file_identity(&log_metadata), self.log_identity)?;
        let retained_log_metadata = validate_file(&self.log, "retirement log", lifecycle_metadata.dev())?;
        require_identity(
            "retained retirement log",
            file_identity(&retained_log_metadata),
            self.log_identity,
        )?;

        let acknowledgement = open_existing_file(
            &lifecycle_directory,
            ACKNOWLEDGEMENT_FILE,
            IoOperation::OpenAcknowledgementFile,
        )?;
        let acknowledgement_metadata =
            validate_file(&acknowledgement, "acknowledgement file", lifecycle_metadata.dev())?;
        require_identity(
            "acknowledgement file",
            file_identity(&acknowledgement_metadata),
            self.acknowledgement_identity,
        )?;
        require_length(
            "acknowledgement file",
            acknowledgement_metadata.len(),
            ACKNOWLEDGEMENT_FILE_LENGTH as u64,
        )?;
        let retained_acknowledgement_metadata =
            validate_file(&self.acknowledgement, "acknowledgement file", lifecycle_metadata.dev())?;
        require_identity(
            "retained acknowledgement file",
            file_identity(&retained_acknowledgement_metadata),
            self.acknowledgement_identity,
        )?;
        require_length(
            "retained acknowledgement file",
            retained_acknowledgement_metadata.len(),
            ACKNOWLEDGEMENT_FILE_LENGTH as u64,
        )
    }

    /// Exercises the same bounded handle operations used by the production backend.
    #[cfg(test)]
    fn open_handle_relative_for_test(store_root: &File, log_generation: u64) -> Result<Self, LedgerIoFailure> {
        Self::open_validated(store_root, log_generation)
    }
}

pub(in crate::mapped_file::retirement) const fn managed_lifecycle_writer_supported() -> bool {
    cfg!(target_os = "linux")
}

impl LedgerIo for FileLedgerIo {
    fn append_log(&mut self, expected_offset: u64, bytes: &[u8]) -> Result<(), LedgerIoFailure> {
        self.verify_bindings()?;
        let actual = file_len(&self.log, IoOperation::AppendLog)?;
        if actual != expected_offset {
            return Err(LedgerIoFailure::OffsetMismatch {
                object: "retirement log",
                expected: expected_offset,
                actual,
            });
        }
        let expected_end = expected_offset
            .checked_add(u64::try_from(bytes.len()).map_err(|_| LedgerIoFailure::LengthOverflow {
                object: "retirement log append",
            })?)
            .ok_or(LedgerIoFailure::LengthOverflow {
                object: "retirement log append",
            })?;
        write_all_at(&self.log, bytes, expected_offset, IoOperation::AppendLog)?;
        let actual_end = file_len(&self.log, IoOperation::AppendLog)?;
        if actual_end != expected_end {
            return Err(LedgerIoFailure::OffsetMismatch {
                object: "retirement log EOF after append",
                expected: expected_end,
                actual: actual_end,
            });
        }
        Ok(())
    }

    fn sync_log(&mut self) -> Result<(), LedgerIoFailure> {
        self.verify_bindings()?;
        self.log
            .sync_all()
            .map_err(|source| LedgerIoFailure::io(IoOperation::SyncLog, source))
    }

    fn write_acknowledgement_slot(
        &mut self,
        slot_index: u8,
        bytes: &[u8; ACKNOWLEDGEMENT_SLOT_LENGTH],
    ) -> Result<(), LedgerIoFailure> {
        let offset = acknowledgement_slot_offset(slot_index)?;
        self.verify_bindings()?;
        require_acknowledgement_file_length(&self.acknowledgement)?;
        write_all_at(
            &self.acknowledgement,
            bytes,
            offset,
            IoOperation::WriteAcknowledgementSlot,
        )?;
        require_acknowledgement_file_length(&self.acknowledgement)
    }

    fn sync_acknowledgement_file(&mut self) -> Result<(), LedgerIoFailure> {
        self.verify_bindings()?;
        self.acknowledgement
            .sync_all()
            .map_err(|source| LedgerIoFailure::io(IoOperation::SyncAcknowledgementFile, source))
    }

    fn read_acknowledgement_slot(
        &mut self,
        slot_index: u8,
    ) -> Result<[u8; ACKNOWLEDGEMENT_SLOT_LENGTH], LedgerIoFailure> {
        let offset = acknowledgement_slot_offset(slot_index)?;
        self.verify_bindings()?;
        require_acknowledgement_file_length(&self.acknowledgement)?;
        let mut bytes = [0_u8; ACKNOWLEDGEMENT_SLOT_LENGTH];
        read_exact_at(
            &self.acknowledgement,
            &mut bytes,
            offset,
            IoOperation::ReadAcknowledgementSlot,
        )?;
        Ok(bytes)
    }

    fn read_log_exact(&mut self, offset: u64, output: &mut [u8]) -> Result<(), LedgerIoFailure> {
        self.verify_bindings()?;
        read_exact_at(&self.log, output, offset, IoOperation::ReadLog)
    }

    fn log_len(&mut self) -> Result<u64, LedgerIoFailure> {
        self.verify_bindings()?;
        file_len(&self.log, IoOperation::ReadLogLength)
    }
}

fn open_existing_directory(parent: &File, name: &str, operation: IoOperation) -> Result<File, LedgerIoFailure> {
    let file = open_contained(parent, name, libc::O_RDONLY | libc::O_DIRECTORY, operation)?;
    if !metadata(&file, IoOperation::InspectHandle)?.is_dir() {
        return Err(LedgerIoFailure::NotDirectory {
            object: "lifecycle directory",
        });
    }
    Ok(file)
}

fn open_existing_file(parent: &File, name: &str, operation: IoOperation) -> Result<File, LedgerIoFailure> {
    open_contained(parent, name, libc::O_RDWR | libc::O_NONBLOCK, operation)
}

fn open_contained(parent: &File, name: &str, flags: i32, operation: IoOperation) -> Result<File, LedgerIoFailure> {
    let name = CString::new(name)
        .map_err(|source| LedgerIoFailure::io(operation, io::Error::new(io::ErrorKind::InvalidInput, source)))?;

    #[cfg(target_os = "linux")]
    {
        openat2(parent, &name, flags, operation)
    }
    #[cfg(not(target_os = "linux"))]
    {
        openat(parent, &name, flags, operation)
    }
}

#[cfg(target_os = "linux")]
fn openat2(parent: &File, name: &CString, flags: i32, operation: IoOperation) -> Result<File, LedgerIoFailure> {
    // SAFETY: `open_how` contains only integer fields, so the all-zero bit pattern is valid.
    let mut how: libc::open_how = unsafe { std::mem::zeroed() };
    how.flags =
        u64::try_from(flags | libc::O_CLOEXEC | libc::O_NOFOLLOW).map_err(|_| LedgerIoFailure::LengthOverflow {
            object: "Linux open flags",
        })?;
    how.resolve = STRICT_RESOLVE;
    // SAFETY: `parent` owns a live directory descriptor, `name` is a live NUL-terminated single
    // component, `how` contains a supported version-zero layout, and no creation flag is present.
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
        return Err(LedgerIoFailure::io(operation, io::Error::last_os_error()));
    }
    let descriptor = i32::try_from(result).map_err(|_| LedgerIoFailure::LengthOverflow {
        object: "Linux openat2 descriptor",
    })?;
    // SAFETY: successful openat2 returned this descriptor uniquely and it has not been wrapped or
    // closed. `File` becomes its sole owner.
    Ok(unsafe { File::from_raw_fd(descriptor) })
}

#[cfg(not(target_os = "linux"))]
fn openat(parent: &File, name: &CString, flags: i32, operation: IoOperation) -> Result<File, LedgerIoFailure> {
    // SAFETY: `name` is a live NUL-terminated C string, `parent` owns a valid descriptor, no
    // creation flag is supplied, and a nonnegative returned descriptor is immediately owned.
    let descriptor = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            name.as_ptr(),
            flags | libc::O_CLOEXEC | libc::O_NOFOLLOW,
        )
    };
    if descriptor < 0 {
        return Err(LedgerIoFailure::io(operation, io::Error::last_os_error()));
    }
    // SAFETY: `descriptor` was just returned uniquely by `openat` and has not been wrapped or
    // closed. `File` becomes its sole owner.
    Ok(unsafe { File::from_raw_fd(descriptor) })
}

fn metadata(file: &File, operation: IoOperation) -> Result<Metadata, LedgerIoFailure> {
    file.metadata().map_err(|source| LedgerIoFailure::io(operation, source))
}

fn validate_file(file: &File, object: &'static str, expected_device: u64) -> Result<Metadata, LedgerIoFailure> {
    let metadata = metadata(file, IoOperation::InspectHandle)?;
    if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
        return Err(LedgerIoFailure::NotRegularFile { object });
    }
    if metadata.nlink() != 1 {
        return Err(LedgerIoFailure::UnexpectedLinkCount {
            object,
            actual: metadata.nlink(),
        });
    }
    if metadata.dev() != expected_device {
        return Err(LedgerIoFailure::CrossDeviceObject { object });
    }
    Ok(metadata)
}

fn file_identity(metadata: &Metadata) -> UnixFileIdentity {
    UnixFileIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
    }
}

fn require_identity(
    object: &'static str,
    actual: UnixFileIdentity,
    expected: UnixFileIdentity,
) -> Result<(), LedgerIoFailure> {
    if actual != expected {
        return Err(LedgerIoFailure::BindingChanged { object });
    }
    Ok(())
}

fn acknowledgement_slot_offset(slot_index: u8) -> Result<u64, LedgerIoFailure> {
    if slot_index > 1 {
        return Err(LedgerIoFailure::InvalidAcknowledgementSlotIndex { slot_index });
    }
    Ok(u64::from(slot_index) * ACKNOWLEDGEMENT_SLOT_LENGTH as u64)
}

fn require_acknowledgement_file_length(file: &File) -> Result<(), LedgerIoFailure> {
    require_length(
        "acknowledgement file",
        file_len(file, IoOperation::InspectHandle)?,
        ACKNOWLEDGEMENT_FILE_LENGTH as u64,
    )
}

fn require_length(object: &'static str, actual: u64, expected: u64) -> Result<(), LedgerIoFailure> {
    if actual != expected {
        return Err(LedgerIoFailure::InvalidLength {
            object,
            expected,
            actual,
        });
    }
    Ok(())
}

fn file_len(file: &File, operation: IoOperation) -> Result<u64, LedgerIoFailure> {
    Ok(metadata(file, operation)?.len())
}

fn write_all_at(file: &File, mut bytes: &[u8], mut offset: u64, operation: IoOperation) -> Result<(), LedgerIoFailure> {
    let mut interrupted_retries = 0;
    while !bytes.is_empty() {
        match file.write_at(bytes, offset) {
            Ok(0) => {
                return Err(LedgerIoFailure::io(
                    operation,
                    io::Error::new(io::ErrorKind::WriteZero, "positional write returned zero"),
                ));
            }
            Ok(written) => {
                interrupted_retries = 0;
                bytes = &bytes[written..];
                offset = offset
                    .checked_add(u64::try_from(written).map_err(|_| LedgerIoFailure::LengthOverflow {
                        object: "positional write",
                    })?)
                    .ok_or(LedgerIoFailure::LengthOverflow {
                        object: "positional write",
                    })?;
            }
            Err(source) if source.kind() == io::ErrorKind::Interrupted => {
                interrupted_retries += 1;
                if interrupted_retries > MAX_INTERRUPTED_RETRIES {
                    return Err(LedgerIoFailure::io(operation, source));
                }
            }
            Err(source) => return Err(LedgerIoFailure::io(operation, source)),
        }
    }
    Ok(())
}

fn read_exact_at(
    file: &File,
    mut output: &mut [u8],
    mut offset: u64,
    operation: IoOperation,
) -> Result<(), LedgerIoFailure> {
    let mut interrupted_retries = 0;
    while !output.is_empty() {
        match file.read_at(output, offset) {
            Ok(0) => {
                return Err(LedgerIoFailure::io(
                    operation,
                    io::Error::new(io::ErrorKind::UnexpectedEof, "positional read reached EOF"),
                ));
            }
            Ok(read) => {
                interrupted_retries = 0;
                let (_, remaining) = output.split_at_mut(read);
                output = remaining;
                offset = offset
                    .checked_add(u64::try_from(read).map_err(|_| LedgerIoFailure::LengthOverflow {
                        object: "positional read",
                    })?)
                    .ok_or(LedgerIoFailure::LengthOverflow {
                        object: "positional read",
                    })?;
            }
            Err(source) if source.kind() == io::ErrorKind::Interrupted => {
                interrupted_retries += 1;
                if interrupted_retries > MAX_INTERRUPTED_RETRIES {
                    return Err(LedgerIoFailure::io(operation, source));
                }
            }
            Err(source) => return Err(LedgerIoFailure::io(operation, source)),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::Write;
    use std::os::unix::fs::symlink;

    use super::*;

    #[test]
    fn opens_and_mutates_only_existing_handle_relative_files() {
        let fixture = fixture(208);
        let mut io = FileLedgerIo::open_handle_relative_for_test(&fixture.root, 2).expect("fixture is valid");
        let slot = [0x5a; ACKNOWLEDGEMENT_SLOT_LENGTH];

        io.append_log(0, b"frame").expect("frame appends");
        io.sync_log().expect("log syncs");
        io.write_acknowledgement_slot(1, &slot).expect("slot writes");
        io.sync_acknowledgement_file().expect("acknowledgement syncs");

        assert_eq!(io.read_acknowledgement_slot(1).expect("slot rereads"), slot);
        let mut frame = [0_u8; 5];
        io.read_log_exact(0, &mut frame).expect("frame rereads");
        assert_eq!(&frame, b"frame");
        assert_eq!(io.log_len().expect("length reads"), 5);
        assert_eq!(managed_lifecycle_writer_supported(), cfg!(target_os = "linux"));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_managed_writer_uses_the_strict_production_open() {
        let fixture = fixture(208);

        assert!(managed_lifecycle_writer_supported());
        let mut io = FileLedgerIo::open_from_store_root(&fixture.root, 2)
            .expect("Linux production writer opens an existing contained ledger");
        io.append_log(0, b"frame").expect("production log append");
        io.sync_log().expect("production log sync");
        assert_eq!(io.log_len().expect("production log length"), 5);
    }

    #[cfg(not(target_os = "linux"))]
    #[test]
    fn managed_writer_stays_disabled_without_an_openat2_equivalent_containment_proof() {
        let fixture = fixture(208);

        assert!(!managed_lifecycle_writer_supported());
        assert!(matches!(
            FileLedgerIo::open_from_store_root(&fixture.root, 2),
            Err(LedgerIoFailure::UnsupportedPlatform {
                platform: "unix",
                reason,
            }) if reason.contains("openat2-equivalent")
        ));
    }

    #[test]
    fn refuses_a_symlinked_lifecycle_directory() {
        let store = tempfile::tempdir().expect("store tempdir");
        let outside = tempfile::tempdir().expect("outside tempdir");
        symlink(outside.path(), store.path().join(LIFECYCLE_DIRECTORY)).expect("symlink creates");
        let root = File::open(store.path()).expect("root handle opens");

        assert!(matches!(
            FileLedgerIo::open_handle_relative_for_test(&root, 0),
            Err(LedgerIoFailure::Io {
                operation: IoOperation::OpenLifecycleDirectory,
                ..
            })
        ));
    }

    #[test]
    fn refuses_an_acknowledgement_file_with_the_wrong_fixed_length() {
        let fixture = fixture(207);

        assert!(matches!(
            FileLedgerIo::open_handle_relative_for_test(&fixture.root, 2),
            Err(LedgerIoFailure::InvalidLength {
                object: "acknowledgement file",
                expected: 208,
                actual: 207,
            })
        ));
    }

    #[test]
    fn refuses_to_append_after_the_lifecycle_directory_binding_is_replaced() {
        let fixture = fixture(208);
        let mut io = FileLedgerIo::open_handle_relative_for_test(&fixture.root, 2).expect("fixture is valid");
        let lifecycle = fixture.lifecycle();
        let detached = fixture._store.path().join("detached-lifecycle");
        fs::rename(&lifecycle, &detached).expect("detach opened lifecycle directory");
        fs::create_dir(&lifecycle).expect("create replacement lifecycle directory");
        File::create(lifecycle.join("retirement.log.g00000000000000000002")).expect("replacement log");
        File::create(lifecycle.join(ACKNOWLEDGEMENT_FILE))
            .and_then(|file| file.set_len(208))
            .expect("replacement acknowledgement");

        assert!(io.append_log(0, b"must-not-write").is_err());
        assert_eq!(
            fs::metadata(lifecycle.join("retirement.log.g00000000000000000002"))
                .expect("replacement log metadata")
                .len(),
            0
        );
        assert_eq!(
            fs::metadata(detached.join("retirement.log.g00000000000000000002"))
                .expect("detached log metadata")
                .len(),
            0
        );
    }

    #[test]
    fn refuses_to_append_after_the_log_binding_is_replaced() {
        let fixture = fixture(208);
        let mut io = FileLedgerIo::open_handle_relative_for_test(&fixture.root, 2).expect("fixture is valid");
        let log = fixture.lifecycle().join("retirement.log.g00000000000000000002");
        let detached = fixture.lifecycle().join("detached.log");
        fs::rename(&log, &detached).expect("detach opened log");
        fs::write(&log, b"replacement-must-remain").expect("install replacement log");

        assert!(io.append_log(0, b"must-not-write").is_err());
        assert_eq!(fs::read(&log).unwrap(), b"replacement-must-remain");
        assert_eq!(fs::metadata(&detached).unwrap().len(), 0);
    }

    #[test]
    fn refuses_to_write_after_the_acknowledgement_binding_is_replaced() {
        let fixture = fixture(208);
        let mut io = FileLedgerIo::open_handle_relative_for_test(&fixture.root, 2).expect("fixture is valid");
        let acknowledgement = fixture.lifecycle().join(ACKNOWLEDGEMENT_FILE);
        let detached = fixture.lifecycle().join("detached-acknowledgement");
        fs::rename(&acknowledgement, &detached).expect("detach opened acknowledgement");
        fs::write(&acknowledgement, vec![0x7b; ACKNOWLEDGEMENT_FILE_LENGTH])
            .expect("install replacement acknowledgement");

        assert!(io
            .write_acknowledgement_slot(0, &[0x5a; ACKNOWLEDGEMENT_SLOT_LENGTH])
            .is_err());
        assert_eq!(
            fs::read(&acknowledgement).expect("replacement acknowledgement reads"),
            vec![0x7b; ACKNOWLEDGEMENT_FILE_LENGTH]
        );
        assert_eq!(
            fs::read(&detached).expect("detached acknowledgement reads"),
            vec![0; ACKNOWLEDGEMENT_FILE_LENGTH]
        );
    }

    #[test]
    fn refuses_a_sidecar_with_an_external_hard_link() {
        let fixture = fixture(208);
        let log = fixture.lifecycle().join("retirement.log.g00000000000000000002");
        fs::hard_link(&log, fixture._store.path().join("external-log-link")).expect("hard link creates");

        assert!(matches!(
            FileLedgerIo::open_handle_relative_for_test(&fixture.root, 2),
            Err(LedgerIoFailure::UnexpectedLinkCount {
                object: "retirement log",
                actual: 2,
            })
        ));
    }

    struct Fixture {
        _store: tempfile::TempDir,
        root: File,
    }

    impl Fixture {
        fn lifecycle(&self) -> std::path::PathBuf {
            self._store.path().join(LIFECYCLE_DIRECTORY)
        }
    }

    fn fixture(acknowledgement_length: usize) -> Fixture {
        let store = tempfile::tempdir().expect("store tempdir");
        let lifecycle = store.path().join(LIFECYCLE_DIRECTORY);
        fs::create_dir(&lifecycle).expect("lifecycle directory creates");
        File::create(lifecycle.join("retirement.log.g00000000000000000002")).expect("log creates");
        let mut acknowledgement = File::create(lifecycle.join(ACKNOWLEDGEMENT_FILE)).expect("acknowledgement creates");
        acknowledgement
            .write_all(&vec![0_u8; acknowledgement_length])
            .expect("acknowledgement sizes");
        let root = File::open(store.path()).expect("root handle opens");
        Fixture { _store: store, root }
    }
}
