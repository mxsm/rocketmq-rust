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

use std::fs;
use std::fs::File;
use std::io;
use std::path::Path;

use rocketmq_store_local::mapped_file::inspect_managed_lifecycle_read_only;
use rocketmq_store_local::mapped_file::ManagedLifecycleReadErrorKind;
use rocketmq_store_local::mapped_file::ManagedLifecycleReadOutcome;

#[test]
fn legacy_probe_is_read_only_and_does_not_create_lifecycle_state() {
    let root = tempfile::tempdir().expect("temporary Store root");
    let handle = open_root(root.path()).expect("open Store root without following aliases");

    assert_eq!(
        inspect_managed_lifecycle_read_only(&handle).expect("legacy probe succeeds"),
        ManagedLifecycleReadOutcome::LegacyAbsent
    );
    assert_eq!(fs::read_dir(root.path()).expect("read Store root").count(), 0);
}

#[test]
fn unknown_lifecycle_evidence_fails_closed_without_mutation() {
    let root = tempfile::tempdir().expect("temporary Store root");
    let lifecycle = root.path().join(".rocketmq-lifecycle");
    fs::create_dir(&lifecycle).expect("create lifecycle directory");
    let unknown = lifecycle.join("unknown.future");
    fs::write(&unknown, b"preserve-for-forensics").expect("write unknown evidence");
    let handle = open_root(root.path()).expect("open Store root without following aliases");

    let error = inspect_managed_lifecycle_read_only(&handle).expect_err("unknown evidence must fail closed");

    assert_eq!(error.kind(), ManagedLifecycleReadErrorKind::Corruption);
    assert_eq!(
        fs::read(&unknown).expect("unknown evidence remains"),
        b"preserve-for-forensics"
    );
    assert_eq!(fs::read_dir(&lifecycle).expect("read lifecycle directory").count(), 1);
}

#[cfg(unix)]
fn open_root(path: &Path) -> io::Result<File> {
    File::open(path)
}

#[cfg(windows)]
fn open_root(path: &Path) -> io::Result<File> {
    use std::fs::OpenOptions;
    use std::os::windows::fs::OpenOptionsExt;

    use windows::Win32::Storage::FileSystem::FILE_FLAG_BACKUP_SEMANTICS;
    use windows::Win32::Storage::FileSystem::FILE_FLAG_OPEN_REPARSE_POINT;

    OpenOptions::new()
        .read(true)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS.0 | FILE_FLAG_OPEN_REPARSE_POINT.0)
        .open(path)
}
