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
use std::path::Path;

use super::FileIdentity;

pub(super) fn open_root(_path: &Path, _create: bool) -> io::Result<File> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "verified Store roots are unsupported on this platform",
    ))
}

pub(super) fn verify_root_directory(_file: &File) -> io::Result<()> {
    unreachable!("unsupported platform cannot open a Store root")
}

pub(super) fn open_lock_file(_root: &File, _create: bool) -> io::Result<File> {
    unreachable!("unsupported platform cannot open a Store root")
}

pub(super) fn verify_lock_file(_file: &File) -> io::Result<()> {
    unreachable!("unsupported platform cannot open a Store root")
}

pub(super) fn file_identity(_file: &File) -> io::Result<FileIdentity> {
    unreachable!("unsupported platform cannot open a Store root")
}

pub(super) fn abort_marker_present(_root: &File) -> io::Result<bool> {
    unreachable!("unsupported platform cannot open a Store root")
}

pub(super) fn create_abort_marker(_root: &File, _contents: &[u8]) -> io::Result<()> {
    unreachable!("unsupported platform cannot open a Store root")
}

pub(super) fn remove_abort_marker(_root: &File) -> io::Result<()> {
    unreachable!("unsupported platform cannot open a Store root")
}

pub(super) fn is_unsafe_path_error(_error: &io::Error) -> bool {
    false
}
