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

//! Synced candidate files and platform-specific atomic replacement shared by local stores.

use std::{
    error::Error as StdError,
    fmt,
    fs::{self, OpenOptions},
    io::{self, Write},
    path::Path,
    sync::atomic::{AtomicU64, Ordering},
};

use serde::Serialize;

use super::config_store::ConfigStoreFailure;

static TEMP_FILE_SEQUENCE: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, thiserror::Error)]
#[error("the storage path has no parent")]
struct InvalidStoragePath;

pub(super) fn write_json_atomically<T: Serialize>(path: &Path, value: &T) -> Result<(), ConfigStoreFailure> {
    let parent = path
        .parent()
        .ok_or_else(|| ConfigStoreFailure::validation(InvalidStoragePath))?;
    fs::create_dir_all(parent)
        .map_err(|source| ConfigStoreFailure::io("create directory", parent.to_path_buf(), source))?;
    let sequence = TEMP_FILE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let file_name = path.file_name().and_then(|name| name.to_str()).unwrap_or("config.json");
    let temporary = parent.join(format!(".{file_name}.{}.{}.tmp", std::process::id(), sequence));
    // Keep a synced temporary file on failure. In particular, Windows replacement failures may
    // require both the original/backup and replacement file for operator recovery.
    write_and_replace(&temporary, path, value)
}

fn write_and_replace<T: Serialize>(temporary: &Path, target: &Path, value: &T) -> Result<(), ConfigStoreFailure> {
    let mut bytes = serde_json::to_vec_pretty(value).map_err(ConfigStoreFailure::validation)?;
    bytes.push(b'\n');
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(temporary)
        .map_err(|source| ConfigStoreFailure::io("create temporary file", temporary.to_path_buf(), source))?;
    file.write_all(&bytes)
        .map_err(|source| ConfigStoreFailure::io("write temporary file", temporary.to_path_buf(), source))?;
    file.sync_all()
        .map_err(|source| ConfigStoreFailure::io("sync temporary file", temporary.to_path_buf(), source))?;
    drop(file);
    atomic_replace(temporary, target).map_err(|source| {
        ConfigStoreFailure::io("atomic replacement", target.to_path_buf(), io::Error::other(source))
    })?;
    sync_parent(target)?;
    Ok(())
}

enum AtomicReplaceFailure {
    Io(io::Error),
    #[cfg(windows)]
    Recovery {
        replacement: io::Error,
        restore: io::Error,
    },
}

impl AtomicReplaceFailure {
    #[cfg(all(test, windows))]
    fn raw_os_error(&self) -> Option<i32> {
        match self {
            Self::Io(source) => source.raw_os_error(),
            Self::Recovery { replacement, .. } => replacement.raw_os_error(),
        }
    }
}

impl fmt::Debug for AtomicReplaceFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let restore_kind: Option<io::ErrorKind> = match self {
            #[cfg(windows)]
            Self::Recovery { restore, .. } => Some(restore.kind()),
            Self::Io(_) => None,
        };
        #[cfg(windows)]
        let recovery_failed = matches!(self, Self::Recovery { .. });
        #[cfg(not(windows))]
        let recovery_failed = false;
        formatter
            .debug_struct("AtomicReplaceFailure")
            .field("recovery_failed", &recovery_failed)
            .field("restore_kind", &restore_kind)
            .finish()
    }
}

impl fmt::Display for AtomicReplaceFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Io(_) => "Atomic replacement failed.",
            #[cfg(windows)]
            Self::Recovery { .. } => "Atomic replacement and recovery both failed.",
        })
    }
}

impl StdError for AtomicReplaceFailure {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Io(source) => Some(source),
            #[cfg(windows)]
            Self::Recovery { replacement, .. } => Some(replacement),
        }
    }
}

#[cfg(not(windows))]
fn atomic_replace(temporary: &Path, target: &Path) -> Result<(), AtomicReplaceFailure> {
    fs::rename(temporary, target).map_err(AtomicReplaceFailure::Io)
}

#[cfg(windows)]
fn atomic_replace(temporary: &Path, target: &Path) -> Result<(), AtomicReplaceFailure> {
    atomic_replace_with(temporary, target, &SystemWindowsReplace)
}

#[cfg(windows)]
trait WindowsReplace {
    fn replace(&self, target: &Path, replacement: &Path, backup: &Path) -> io::Result<()>;
}

#[cfg(windows)]
struct SystemWindowsReplace;

#[cfg(windows)]
impl WindowsReplace for SystemWindowsReplace {
    fn replace(&self, target: &Path, replacement: &Path, backup: &Path) -> io::Result<()> {
        use std::os::windows::ffi::OsStrExt as _;

        #[link(name = "Kernel32")]
        unsafe extern "system" {
            fn ReplaceFileW(
                replaced_file_name: *const u16,
                replacement_file_name: *const u16,
                backup_file_name: *const u16,
                replace_flags: u32,
                exclude: *mut std::ffi::c_void,
                reserved: *mut std::ffi::c_void,
            ) -> i32;
        }

        let replaced = target.as_os_str().encode_wide().chain(Some(0)).collect::<Vec<_>>();
        let replacement = replacement.as_os_str().encode_wide().chain(Some(0)).collect::<Vec<_>>();
        let backup = backup.as_os_str().encode_wide().chain(Some(0)).collect::<Vec<_>>();
        // SAFETY: all path buffers are NUL-terminated and live for the call; reserved pointer
        // arguments are null, and ReplaceFileW does not retain any supplied pointer.
        let replaced_ok = unsafe {
            ReplaceFileW(
                replaced.as_ptr(),
                replacement.as_ptr(),
                backup.as_ptr(),
                0x0000_0002,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            )
        };
        if replaced_ok == 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }
}

#[cfg(windows)]
fn atomic_replace_with(temporary: &Path, target: &Path, api: &dyn WindowsReplace) -> Result<(), AtomicReplaceFailure> {
    if !target.exists() {
        return fs::rename(temporary, target).map_err(AtomicReplaceFailure::Io);
    }
    let sequence = TEMP_FILE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let file_name = target
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("config.json");
    let backup = target.with_file_name(format!(
        ".{file_name}.{}.{}.replacement-backup",
        std::process::id(),
        sequence
    ));
    let recovery = target.with_file_name(format!(
        ".{file_name}.{}.{}.pre-replacement-recovery",
        std::process::id(),
        sequence
    ));
    fs::hard_link(target, &recovery).map_err(AtomicReplaceFailure::Io)?;

    match api.replace(target, temporary, &backup) {
        Ok(()) => {
            if backup.exists() {
                let _ = fs::remove_file(&backup);
            }
            let _ = fs::remove_file(&recovery);
            Ok(())
        }
        Err(replace_error) => {
            if !target.exists() {
                let restore_result = if backup.exists() {
                    fs::rename(&backup, target)
                } else {
                    fs::copy(&recovery, target).map(|_| ())
                };
                if let Err(restore_error) = restore_result {
                    return Err(AtomicReplaceFailure::Recovery {
                        replacement: replace_error,
                        restore: restore_error,
                    });
                }
            }
            Err(AtomicReplaceFailure::Io(replace_error))
        }
    }
}

#[cfg(not(windows))]
fn sync_parent(target: &Path) -> Result<(), ConfigStoreFailure> {
    use std::fs::File;

    let parent = target
        .parent()
        .ok_or_else(|| ConfigStoreFailure::validation(InvalidStoragePath))?;
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| ConfigStoreFailure::io("sync directory", parent.to_path_buf(), source))
}

#[cfg(windows)]
fn sync_parent(_target: &Path) -> Result<(), ConfigStoreFailure> {
    // ReplaceFileW completes the same-volume atomic replacement. Windows does not expose
    // directory handles through std::fs::File, while the temporary file itself is synced above.
    Ok(())
}

#[cfg(all(test, windows))]
mod tests {
    use super::*;

    #[cfg(windows)]
    #[test]
    fn windows_existing_target_is_replaced_atomically() {
        let directory = tempfile::tempdir().expect("temp directory");
        let target = directory.path().join("config.json");
        let replacement = directory.path().join("replacement.tmp");
        fs::write(&target, b"old").expect("old target");
        fs::write(&replacement, b"new").expect("new replacement");

        atomic_replace(&replacement, &target).expect("atomic replacement");

        assert_eq!(fs::read(&target).expect("target"), b"new");
        assert!(!replacement.exists());
    }

    #[cfg(windows)]
    #[test]
    fn windows_move_replacement_failure_restores_original_and_preserves_replacement() {
        struct MoveReplacementFailure;

        impl WindowsReplace for MoveReplacementFailure {
            fn replace(&self, target: &Path, _replacement: &Path, backup: &Path) -> io::Result<()> {
                fs::rename(target, backup)?;
                Err(io::Error::from_raw_os_error(1177))
            }
        }

        let directory = tempfile::tempdir().expect("temp directory");
        let target = directory.path().join("config.json");
        let replacement = directory.path().join("replacement.tmp");
        fs::write(&target, b"old").expect("old target");
        fs::write(&replacement, b"new").expect("new replacement");

        let error = atomic_replace_with(&replacement, &target, &MoveReplacementFailure)
            .expect_err("injected replacement failure");

        assert_eq!(error.raw_os_error(), Some(1177));
        assert_eq!(fs::read(&target).expect("restored target"), b"old");
        assert_eq!(fs::read(&replacement).expect("preserved replacement"), b"new");
    }

    #[cfg(windows)]
    #[test]
    fn windows_remove_replaced_failure_retains_both_original_names() {
        struct RemoveReplacedFailure;

        impl WindowsReplace for RemoveReplacedFailure {
            fn replace(&self, _target: &Path, _replacement: &Path, _backup: &Path) -> io::Result<()> {
                Err(io::Error::from_raw_os_error(1175))
            }
        }

        assert_windows_replacement_failure_preserves_files(&RemoveReplacedFailure, 1175);
    }

    #[cfg(windows)]
    #[test]
    fn windows_move_replacement_failure_restores_from_pre_replacement_recovery() {
        struct MoveReplacementFailure;

        impl WindowsReplace for MoveReplacementFailure {
            fn replace(&self, target: &Path, _replacement: &Path, _backup: &Path) -> io::Result<()> {
                fs::remove_file(target)?;
                Err(io::Error::from_raw_os_error(1176))
            }
        }

        assert_windows_replacement_failure_preserves_files(&MoveReplacementFailure, 1176);
    }

    #[cfg(windows)]
    fn assert_windows_replacement_failure_preserves_files(api: &dyn WindowsReplace, code: i32) {
        let directory = tempfile::tempdir().expect("temp directory");
        let target = directory.path().join("config.json");
        let replacement = directory.path().join("replacement.tmp");
        fs::write(&target, b"old").expect("old target");
        fs::write(&replacement, b"new").expect("new replacement");

        let error = atomic_replace_with(&replacement, &target, api).expect_err("injected replacement failure");

        assert_eq!(error.raw_os_error(), Some(code));
        assert_eq!(fs::read(&target).expect("retained/restored target"), b"old");
        assert_eq!(fs::read(&replacement).expect("preserved replacement"), b"new");
    }
}
