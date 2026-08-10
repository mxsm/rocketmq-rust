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
use std::io::Write;
use std::mem::MaybeUninit;
use std::os::fd::AsRawFd;
use std::os::fd::FromRawFd;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::MetadataExt;
use std::path::Component;
use std::path::Path;

use super::FileIdentity;
use super::ABORT_FILE_NAME;
use super::LOCK_FILE_NAME;

const DIRECTORY_FLAGS: i32 = libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW;

pub(super) fn open_root(path: &Path, create: bool) -> io::Result<File> {
    let mut components = path.components();
    let mut current = if path.is_absolute() {
        // SAFETY: the nul-terminated static path is valid for the duration of the call. On
        // success ownership of the returned descriptor is transferred exactly once to File.
        let descriptor = unsafe { libc::open(c"/".as_ptr(), DIRECTORY_FLAGS) };
        file_from_descriptor(descriptor)?
    } else {
        // SAFETY: the nul-terminated static path is valid for the duration of the call. On
        // success ownership of the returned descriptor is transferred exactly once to File.
        let descriptor = unsafe { libc::open(c".".as_ptr(), DIRECTORY_FLAGS) };
        file_from_descriptor(descriptor)?
    };

    for component in components.by_ref() {
        match component {
            Component::RootDir | Component::CurDir => {}
            Component::Normal(name) => {
                let name = c_string(name)?;
                current = match open_directory_at(&current, &name) {
                    Ok(directory) => directory,
                    Err(error) if create && error.kind() == io::ErrorKind::NotFound => {
                        // SAFETY: current is an owned live directory descriptor and name is a
                        // single nul-terminated path component. mkdirat does not follow a final
                        // symlink; an EEXIST race is resolved by the no-follow reopen below.
                        let result = unsafe { libc::mkdirat(current.as_raw_fd(), name.as_ptr(), 0o750) };
                        if result != 0 {
                            let mkdir_error = io::Error::last_os_error();
                            if mkdir_error.kind() != io::ErrorKind::AlreadyExists {
                                return Err(mkdir_error);
                            }
                        }
                        open_directory_at(&current, &name)?
                    }
                    Err(error) => return Err(error),
                };
            }
            Component::ParentDir | Component::Prefix(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Store root must not contain parent or platform-prefix components",
                ));
            }
        }
    }
    Ok(current)
}

pub(super) fn verify_root_directory(file: &File) -> io::Result<()> {
    let metadata = file.metadata()?;
    if !metadata.file_type().is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Store root handle is not a directory",
        ));
    }
    Ok(())
}

pub(super) fn open_lock_file(root: &File, create: bool) -> io::Result<File> {
    let name = CString::new(LOCK_FILE_NAME).expect("static lock name contains no nul byte");
    let mut flags = libc::O_RDWR | libc::O_CLOEXEC | libc::O_NOFOLLOW;
    if create {
        flags |= libc::O_CREAT;
    }
    // SAFETY: root is an owned live directory descriptor and name is a single nul-terminated
    // component. O_NOFOLLOW prevents a final symbolic-link target from being opened.
    let descriptor = unsafe { libc::openat(root.as_raw_fd(), name.as_ptr(), flags, 0o640) };
    file_from_descriptor(descriptor)
}

pub(super) fn verify_lock_file(file: &File) -> io::Result<()> {
    let metadata = file.metadata()?;
    if !metadata.file_type().is_file() || metadata.nlink() != 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Store lock handle must be a singly linked regular file",
        ));
    }
    Ok(())
}

pub(super) fn file_identity(file: &File) -> io::Result<FileIdentity> {
    let metadata = file.metadata()?;
    let mut file_id = [0_u8; 16];
    file_id[..8].copy_from_slice(&metadata.ino().to_le_bytes());
    Ok(FileIdentity {
        volume: metadata.dev(),
        file_id,
    })
}

pub(super) fn abort_marker_present(root: &File) -> io::Result<bool> {
    entry_present(root, ABORT_FILE_NAME)
}

pub(super) fn create_abort_marker(root: &File, contents: &[u8]) -> io::Result<()> {
    // Open before truncating so a non-regular existing entry is rejected without mutation.
    // SAFETY: root is a live retained directory descriptor and the literal is one
    // nul-terminated component. O_NOFOLLOW prevents opening a final symbolic link, and a
    // successful descriptor is transferred exactly once into File below.
    let descriptor = unsafe {
        libc::openat(
            root.as_raw_fd(),
            c"abort".as_ptr(),
            libc::O_WRONLY | libc::O_CREAT | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK,
            0o640,
        )
    };
    let mut marker = file_from_descriptor(descriptor)?;
    let metadata = marker.metadata()?;
    if !metadata.file_type().is_file() || metadata.nlink() != 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Store abort marker must be a singly linked regular file",
        ));
    }
    marker.set_len(0)?;
    marker.write_all(contents)?;
    marker.sync_all()?;
    root.sync_all()
}

fn entry_present(root: &File, entry_name: &str) -> io::Result<bool> {
    let name = CString::new(entry_name).expect("static Store entry name contains no nul byte");
    let mut metadata = MaybeUninit::<libc::stat>::uninit();
    // SAFETY: root is a live directory descriptor, name is a single nul-terminated component,
    // and metadata points to writable storage for one stat value. AT_SYMLINK_NOFOLLOW makes
    // every existing final object, including a dangling symlink, recognizable evidence.
    let result = unsafe {
        libc::fstatat(
            root.as_raw_fd(),
            name.as_ptr(),
            metadata.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    };
    if result == 0 {
        return Ok(true);
    }
    let error = io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::ENOENT) {
        Ok(false)
    } else {
        Err(error)
    }
}

pub(super) fn remove_abort_marker(root: &File) -> io::Result<()> {
    // SAFETY: root is a live retained directory descriptor and the literal is one
    // nul-terminated component. unlinkat removes that directory entry without following it.
    let result = unsafe { libc::unlinkat(root.as_raw_fd(), c"abort".as_ptr(), 0) };
    if result == 0 {
        return Ok(());
    }
    let error = io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::ENOENT) {
        Ok(())
    } else {
        Err(error)
    }
}

pub(super) fn is_unsafe_path_error(error: &io::Error) -> bool {
    error.kind() == io::ErrorKind::InvalidData
        || error.kind() == io::ErrorKind::InvalidInput
        || error.raw_os_error() == Some(libc::ELOOP)
}

fn open_directory_at(parent: &File, name: &CString) -> io::Result<File> {
    // SAFETY: parent is an owned live directory descriptor and name is one nul-terminated path
    // component. O_DIRECTORY and O_NOFOLLOW enforce the directory/no-link invariant.
    let descriptor = unsafe { libc::openat(parent.as_raw_fd(), name.as_ptr(), DIRECTORY_FLAGS) };
    file_from_descriptor(descriptor)
}

fn file_from_descriptor(descriptor: i32) -> io::Result<File> {
    if descriptor < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: descriptor is a newly returned owned descriptor and is transferred exactly once.
    Ok(unsafe { File::from_raw_fd(descriptor) })
}

fn c_string(component: &std::ffi::OsStr) -> io::Result<CString> {
    CString::new(component.as_bytes()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "Store root component contains an embedded nul byte",
        )
    })
}
