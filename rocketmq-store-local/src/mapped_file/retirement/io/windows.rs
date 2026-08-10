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
use std::mem::size_of;
use std::mem::MaybeUninit;
use std::os::windows::fs::FileExt;
use std::os::windows::io::AsRawHandle;
use std::os::windows::io::FromRawHandle;
use std::ptr;

use windows::core::PWSTR;
use windows::Wdk::Foundation::OBJECT_ATTRIBUTES;
use windows::Wdk::Storage::FileSystem::NtCreateFile;
use windows::Wdk::Storage::FileSystem::FILE_DIRECTORY_FILE;
use windows::Wdk::Storage::FileSystem::FILE_NON_DIRECTORY_FILE;
use windows::Wdk::Storage::FileSystem::FILE_OPEN;
use windows::Wdk::Storage::FileSystem::FILE_OPEN_REPARSE_POINT;
use windows::Wdk::Storage::FileSystem::FILE_SYNCHRONOUS_IO_NONALERT;
use windows::Wdk::Storage::FileSystem::NTCREATEFILE_CREATE_OPTIONS;
use windows::Win32::Foundation::RtlNtStatusToDosError;
use windows::Win32::Foundation::HANDLE;
use windows::Win32::Foundation::NTSTATUS;
use windows::Win32::Foundation::OBJ_DONT_REPARSE;
use windows::Win32::Foundation::UNICODE_STRING;
use windows::Win32::Storage::FileSystem::FileAttributeTagInfo;
use windows::Win32::Storage::FileSystem::FileStandardInfo;
use windows::Win32::Storage::FileSystem::GetFileInformationByHandleEx;
use windows::Win32::Storage::FileSystem::GetVolumeInformationByHandleW;
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

use super::IoOperation;
use super::LedgerIo;
use super::LedgerIoError;
use crate::mapped_file::retirement::codec::ACKNOWLEDGEMENT_FILE_LENGTH;
use crate::mapped_file::retirement::codec::ACKNOWLEDGEMENT_SLOT_LENGTH;
use crate::mapped_file::retirement::identity::PhysicalFileKey;
use crate::mapped_file::retirement::platform::physical_file_key;

const LIFECYCLE_DIRECTORY: &str = ".rocketmq-lifecycle";
const ACKNOWLEDGEMENT_FILE: &str = "ACKNOWLEDGED.v1";
const MAX_INTERRUPTED_RETRIES: usize = 16;
const NON_NTFS_REASON: &str = "managed lifecycle writer is qualified only for an NTFS Store root";

/// Windows ledger backend opened strictly relative to the caller's retained Store-root handle.
pub(in crate::mapped_file::retirement) struct FileLedgerIo {
    store_root: File,
    lifecycle_directory: File,
    lifecycle_identity: PhysicalFileKey,
    log_name: String,
    log: File,
    log_identity: PhysicalFileKey,
    acknowledgement: File,
    acknowledgement_identity: PhysicalFileKey,
}

impl FileLedgerIo {
    pub(in crate::mapped_file::retirement) fn open_from_store_root(
        store_root: &File,
        log_generation: u64,
    ) -> Result<Self, LedgerIoError> {
        let retained_root = store_root
            .try_clone()
            .map_err(|source| LedgerIoError::io(IoOperation::InspectHandle, source))?;
        validate_directory(&retained_root, "Store root")?;
        if !is_ntfs_volume(&retained_root)? {
            return Err(LedgerIoError::UnsupportedPlatform {
                platform: "windows",
                reason: NON_NTFS_REASON,
            });
        }

        let lifecycle_directory =
            open_existing_directory(&retained_root, LIFECYCLE_DIRECTORY, IoOperation::OpenLifecycleDirectory)?;
        let lifecycle_identity = physical_identity(&lifecycle_directory)?;
        let log_name = format!("retirement.log.g{log_generation:020}");
        let log = open_existing_file(&lifecycle_directory, &log_name, IoOperation::OpenLog)?;
        let acknowledgement = open_existing_file(
            &lifecycle_directory,
            ACKNOWLEDGEMENT_FILE,
            IoOperation::OpenAcknowledgementFile,
        )?;
        let log_identity = validate_file(&log, "retirement log")?;
        let acknowledgement_identity = validate_file(&acknowledgement, "acknowledgement file")?;
        require_length(
            "acknowledgement file",
            file_len(&acknowledgement, IoOperation::InspectHandle)?,
            ACKNOWLEDGEMENT_FILE_LENGTH as u64,
        )?;

        Ok(Self {
            store_root: retained_root,
            lifecycle_directory,
            lifecycle_identity,
            log_name,
            log,
            log_identity,
            acknowledgement,
            acknowledgement_identity,
        })
    }

    fn verify_bindings(&self) -> Result<(), LedgerIoError> {
        validate_directory(&self.store_root, "Store root")?;
        let lifecycle_directory = open_existing_directory(
            &self.store_root,
            LIFECYCLE_DIRECTORY,
            IoOperation::OpenLifecycleDirectory,
        )?;
        require_identity(
            "lifecycle directory",
            physical_identity(&lifecycle_directory)?,
            self.lifecycle_identity,
        )?;
        validate_directory(&self.lifecycle_directory, "retained lifecycle directory")?;
        require_identity(
            "retained lifecycle directory",
            physical_identity(&self.lifecycle_directory)?,
            self.lifecycle_identity,
        )?;

        let log = open_existing_file(&lifecycle_directory, &self.log_name, IoOperation::OpenLog)?;
        require_identity(
            "retirement log",
            validate_file(&log, "retirement log")?,
            self.log_identity,
        )?;
        require_identity(
            "retained retirement log",
            validate_file(&self.log, "retained retirement log")?,
            self.log_identity,
        )?;

        let acknowledgement = open_existing_file(
            &lifecycle_directory,
            ACKNOWLEDGEMENT_FILE,
            IoOperation::OpenAcknowledgementFile,
        )?;
        require_identity(
            "acknowledgement file",
            validate_file(&acknowledgement, "acknowledgement file")?,
            self.acknowledgement_identity,
        )?;
        require_acknowledgement_file_length(&acknowledgement)?;
        require_identity(
            "retained acknowledgement file",
            validate_file(&self.acknowledgement, "retained acknowledgement file")?,
            self.acknowledgement_identity,
        )?;
        require_acknowledgement_file_length(&self.acknowledgement)
    }
}

pub(in crate::mapped_file::retirement) const fn managed_lifecycle_writer_supported() -> bool {
    true
}

impl LedgerIo for FileLedgerIo {
    fn append_log(&mut self, expected_offset: u64, bytes: &[u8]) -> Result<(), LedgerIoError> {
        self.verify_bindings()?;
        let actual = file_len(&self.log, IoOperation::AppendLog)?;
        if actual != expected_offset {
            return Err(LedgerIoError::OffsetMismatch {
                object: "retirement log",
                expected: expected_offset,
                actual,
            });
        }
        let expected_end = expected_offset
            .checked_add(u64::try_from(bytes.len()).map_err(|_| LedgerIoError::LengthOverflow {
                object: "retirement log append",
            })?)
            .ok_or(LedgerIoError::LengthOverflow {
                object: "retirement log append",
            })?;
        write_all_at(&self.log, bytes, expected_offset, IoOperation::AppendLog)?;
        let actual_end = file_len(&self.log, IoOperation::AppendLog)?;
        if actual_end != expected_end {
            return Err(LedgerIoError::OffsetMismatch {
                object: "retirement log EOF after append",
                expected: expected_end,
                actual: actual_end,
            });
        }
        Ok(())
    }

    fn sync_log(&mut self) -> Result<(), LedgerIoError> {
        self.verify_bindings()?;
        self.log
            .sync_all()
            .map_err(|source| LedgerIoError::io(IoOperation::SyncLog, source))
    }

    fn write_acknowledgement_slot(
        &mut self,
        slot_index: u8,
        bytes: &[u8; ACKNOWLEDGEMENT_SLOT_LENGTH],
    ) -> Result<(), LedgerIoError> {
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

    fn sync_acknowledgement_file(&mut self) -> Result<(), LedgerIoError> {
        self.verify_bindings()?;
        self.acknowledgement
            .sync_all()
            .map_err(|source| LedgerIoError::io(IoOperation::SyncAcknowledgementFile, source))
    }

    fn read_acknowledgement_slot(
        &mut self,
        slot_index: u8,
    ) -> Result<[u8; ACKNOWLEDGEMENT_SLOT_LENGTH], LedgerIoError> {
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

    fn read_log_exact(&mut self, offset: u64, output: &mut [u8]) -> Result<(), LedgerIoError> {
        self.verify_bindings()?;
        read_exact_at(&self.log, output, offset, IoOperation::ReadLog)
    }

    fn log_len(&mut self) -> Result<u64, LedgerIoError> {
        self.verify_bindings()?;
        file_len(&self.log, IoOperation::ReadLogLength)
    }
}

fn open_existing_directory(parent: &File, name: &str, operation: IoOperation) -> Result<File, LedgerIoError> {
    let file = open_relative(parent, name, true, operation)?;
    validate_directory(&file, "lifecycle directory")?;
    Ok(file)
}

fn open_existing_file(parent: &File, name: &str, operation: IoOperation) -> Result<File, LedgerIoError> {
    let file = open_relative(parent, name, false, operation)?;
    validate_file(&file, "lifecycle sidecar")?;
    Ok(file)
}

fn open_relative(parent: &File, name: &str, directory: bool, operation: IoOperation) -> Result<File, LedgerIoError> {
    let mut wide = name.encode_utf16().collect::<Vec<_>>();
    let byte_length = wide
        .len()
        .checked_mul(size_of::<u16>())
        .and_then(|value| u16::try_from(value).ok())
        .ok_or(LedgerIoError::LengthOverflow {
            object: "Windows relative name",
        })?;
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
        FILE_ACCESS_RIGHTS(
            FILE_READ_DATA.0 | FILE_WRITE_DATA.0 | FILE_APPEND_DATA.0 | FILE_READ_ATTRIBUTES.0 | SYNCHRONIZE.0,
        )
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
    // SAFETY: pointers refer to live fixed storage for this synchronous call; RootDirectory is a
    // retained verified directory handle and `name` is a validated fixed sidecar component. The
    // call opens only an existing entry and requests no reparse traversal.
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
            options,
            None,
            0,
        )
    };
    if status.0 < 0 {
        return Err(LedgerIoError::io(operation, status_error(status)));
    }
    if handle.is_invalid() {
        return Err(LedgerIoError::io(
            operation,
            io::Error::other("NtCreateFile returned an invalid handle"),
        ));
    }
    // SAFETY: successful NtCreateFile returned unique ownership of a valid kernel handle.
    Ok(unsafe { File::from_raw_handle(handle.0) })
}

fn validate_directory(file: &File, object: &'static str) -> Result<(), LedgerIoError> {
    let (attributes, standard) = handle_information(file)?;
    if attributes.FileAttributes & FILE_ATTRIBUTE_REPARSE_POINT.0 != 0 {
        return Err(LedgerIoError::ReparsePoint { object });
    }
    if !standard.Directory || attributes.FileAttributes & FILE_ATTRIBUTE_DIRECTORY.0 == 0 {
        return Err(LedgerIoError::NotDirectory { object });
    }
    Ok(())
}

fn validate_file(file: &File, object: &'static str) -> Result<PhysicalFileKey, LedgerIoError> {
    let (attributes, standard) = handle_information(file)?;
    if attributes.FileAttributes & FILE_ATTRIBUTE_REPARSE_POINT.0 != 0 {
        return Err(LedgerIoError::ReparsePoint { object });
    }
    if standard.Directory || attributes.FileAttributes & FILE_ATTRIBUTE_DIRECTORY.0 != 0 {
        return Err(LedgerIoError::NotRegularFile { object });
    }
    if standard.NumberOfLinks != 1 {
        return Err(LedgerIoError::UnexpectedLinkCount {
            object,
            actual: u64::from(standard.NumberOfLinks),
        });
    }
    physical_identity(file)
}

fn handle_information(file: &File) -> Result<(FILE_ATTRIBUTE_TAG_INFO, FILE_STANDARD_INFO), LedgerIoError> {
    let attributes = query_information::<FILE_ATTRIBUTE_TAG_INFO>(file, FileAttributeTagInfo)?;
    let standard = query_information::<FILE_STANDARD_INFO>(file, FileStandardInfo)?;
    Ok((attributes, standard))
}

fn query_information<T: Copy + Default>(
    file: &File,
    class: windows::Win32::Storage::FileSystem::FILE_INFO_BY_HANDLE_CLASS,
) -> Result<T, LedgerIoError> {
    let mut output = MaybeUninit::<T>::uninit();
    // SAFETY: the retained handle remains borrowed and output is aligned writable storage of the
    // exact fixed information-class size; Windows initializes it completely on success.
    unsafe {
        GetFileInformationByHandleEx(
            HANDLE(file.as_raw_handle()),
            class,
            output.as_mut_ptr().cast(),
            size_of::<T>() as u32,
        )
        .map_err(|error| LedgerIoError::io(IoOperation::InspectHandle, windows_error_to_io(error)))?;
        Ok(output.assume_init())
    }
}

fn physical_identity(file: &File) -> Result<PhysicalFileKey, LedgerIoError> {
    physical_file_key(file).map_err(|source| LedgerIoError::io(IoOperation::InspectHandle, source))
}

fn require_identity(
    object: &'static str,
    actual: PhysicalFileKey,
    expected: PhysicalFileKey,
) -> Result<(), LedgerIoError> {
    if actual != expected {
        return Err(LedgerIoError::BindingChanged { object });
    }
    Ok(())
}

fn acknowledgement_slot_offset(slot_index: u8) -> Result<u64, LedgerIoError> {
    if slot_index > 1 {
        return Err(LedgerIoError::InvalidAcknowledgementSlotIndex { slot_index });
    }
    Ok(u64::from(slot_index) * ACKNOWLEDGEMENT_SLOT_LENGTH as u64)
}

fn require_acknowledgement_file_length(file: &File) -> Result<(), LedgerIoError> {
    require_length(
        "acknowledgement file",
        file_len(file, IoOperation::InspectHandle)?,
        ACKNOWLEDGEMENT_FILE_LENGTH as u64,
    )
}

fn require_length(object: &'static str, actual: u64, expected: u64) -> Result<(), LedgerIoError> {
    if actual != expected {
        return Err(LedgerIoError::InvalidLength {
            object,
            expected,
            actual,
        });
    }
    Ok(())
}

fn file_len(file: &File, operation: IoOperation) -> Result<u64, LedgerIoError> {
    file.metadata()
        .map(|metadata| metadata.len())
        .map_err(|source| LedgerIoError::io(operation, source))
}

fn write_all_at(file: &File, mut bytes: &[u8], mut offset: u64, operation: IoOperation) -> Result<(), LedgerIoError> {
    let mut interrupted_retries = 0;
    while !bytes.is_empty() {
        match file.seek_write(bytes, offset) {
            Ok(0) => {
                return Err(LedgerIoError::io(
                    operation,
                    io::Error::new(io::ErrorKind::WriteZero, "positional write returned zero"),
                ));
            }
            Ok(written) => {
                interrupted_retries = 0;
                bytes = &bytes[written..];
                offset = offset
                    .checked_add(u64::try_from(written).map_err(|_| LedgerIoError::LengthOverflow {
                        object: "positional write",
                    })?)
                    .ok_or(LedgerIoError::LengthOverflow {
                        object: "positional write",
                    })?;
            }
            Err(source) if source.kind() == io::ErrorKind::Interrupted => {
                interrupted_retries += 1;
                if interrupted_retries > MAX_INTERRUPTED_RETRIES {
                    return Err(LedgerIoError::io(operation, source));
                }
            }
            Err(source) => return Err(LedgerIoError::io(operation, source)),
        }
    }
    Ok(())
}

fn read_exact_at(
    file: &File,
    mut output: &mut [u8],
    mut offset: u64,
    operation: IoOperation,
) -> Result<(), LedgerIoError> {
    let mut interrupted_retries = 0;
    while !output.is_empty() {
        match file.seek_read(output, offset) {
            Ok(0) => {
                return Err(LedgerIoError::io(
                    operation,
                    io::Error::new(io::ErrorKind::UnexpectedEof, "positional read reached EOF"),
                ));
            }
            Ok(read) => {
                interrupted_retries = 0;
                let (_, remaining) = output.split_at_mut(read);
                output = remaining;
                offset = offset
                    .checked_add(u64::try_from(read).map_err(|_| LedgerIoError::LengthOverflow {
                        object: "positional read",
                    })?)
                    .ok_or(LedgerIoError::LengthOverflow {
                        object: "positional read",
                    })?;
            }
            Err(source) if source.kind() == io::ErrorKind::Interrupted => {
                interrupted_retries += 1;
                if interrupted_retries > MAX_INTERRUPTED_RETRIES {
                    return Err(LedgerIoError::io(operation, source));
                }
            }
            Err(source) => return Err(LedgerIoError::io(operation, source)),
        }
    }
    Ok(())
}

fn is_ntfs_volume(file: &File) -> Result<bool, LedgerIoError> {
    let mut filesystem_name = [0_u16; 32];
    // SAFETY: the retained root handle remains borrowed for this synchronous call and the fixed
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
        .map_err(|error| LedgerIoError::io(IoOperation::InspectHandle, windows_error_to_io(error)))?;
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

#[cfg(test)]
mod tests {
    use std::fs;
    use std::fs::OpenOptions;
    use std::os::windows::fs::OpenOptionsExt;

    use windows::Win32::Storage::FileSystem::FILE_FLAG_BACKUP_SEMANTICS;
    use windows::Win32::Storage::FileSystem::FILE_FLAG_OPEN_REPARSE_POINT;

    use super::*;

    #[test]
    fn managed_writer_opens_and_mutates_only_existing_handle_relative_files() {
        let fixture = fixture(ACKNOWLEDGEMENT_FILE_LENGTH as u64);
        let mut io = FileLedgerIo::open_from_store_root(&fixture.root, 2).expect("Windows managed writer opens");
        let slot = [0x5a; ACKNOWLEDGEMENT_SLOT_LENGTH];

        assert!(managed_lifecycle_writer_supported());
        io.append_log(0, b"frame").expect("frame appends");
        io.sync_log().expect("log syncs");
        io.write_acknowledgement_slot(1, &slot).expect("slot writes");
        io.sync_acknowledgement_file().expect("acknowledgement syncs");
        assert_eq!(io.read_acknowledgement_slot(1).expect("slot rereads"), slot);
        let mut frame = [0_u8; 5];
        io.read_log_exact(0, &mut frame).expect("frame rereads");
        assert_eq!(&frame, b"frame");
        assert_eq!(io.log_len().expect("log length reads"), 5);
    }

    #[test]
    fn refuses_an_acknowledgement_file_with_the_wrong_fixed_length() {
        let fixture = fixture((ACKNOWLEDGEMENT_FILE_LENGTH - 1) as u64);
        assert!(matches!(
            FileLedgerIo::open_from_store_root(&fixture.root, 2),
            Err(LedgerIoError::InvalidLength {
                object: "acknowledgement file",
                expected: 208,
                actual: 207,
            })
        ));
    }

    #[test]
    fn refuses_to_append_after_the_log_binding_is_replaced() {
        let fixture = fixture(ACKNOWLEDGEMENT_FILE_LENGTH as u64);
        let mut io = FileLedgerIo::open_from_store_root(&fixture.root, 2).expect("fixture is valid");
        let log = fixture.lifecycle().join("retirement.log.g00000000000000000002");
        let detached = fixture.lifecycle().join("detached.log");
        fs::rename(&log, &detached).expect("detach opened log");
        fs::write(&log, b"replacement-must-remain").expect("install replacement log");

        assert!(io.append_log(0, b"must-not-write").is_err());
        assert_eq!(
            fs::read(&log).expect("replacement log reads"),
            b"replacement-must-remain"
        );
        assert_eq!(fs::metadata(&detached).expect("detached metadata").len(), 0);
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

    fn fixture(acknowledgement_length: u64) -> Fixture {
        let store = tempfile::tempdir().expect("Store tempdir");
        let lifecycle = store.path().join(LIFECYCLE_DIRECTORY);
        fs::create_dir(&lifecycle).expect("lifecycle directory creates");
        File::create(lifecycle.join("retirement.log.g00000000000000000002")).expect("log creates");
        File::create(lifecycle.join(ACKNOWLEDGEMENT_FILE))
            .and_then(|file| file.set_len(acknowledgement_length))
            .expect("acknowledgement sizes");
        let root = OpenOptions::new()
            .read(true)
            .share_mode(FILE_SHARE_READ.0 | FILE_SHARE_WRITE.0 | FILE_SHARE_DELETE.0)
            .custom_flags(FILE_FLAG_BACKUP_SEMANTICS.0 | FILE_FLAG_OPEN_REPARSE_POINT.0)
            .open(store.path())
            .expect("Store root handle opens");
        Fixture { _store: store, root }
    }
}
