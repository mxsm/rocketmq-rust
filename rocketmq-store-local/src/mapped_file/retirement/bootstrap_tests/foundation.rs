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

#[cfg(any(target_os = "linux", windows))]
use std::fs;
use std::fs::File;
#[cfg(windows)]
use std::fs::OpenOptions;
#[cfg(windows)]
use std::os::windows::fs::OpenOptionsExt;

#[cfg(any(target_os = "linux", windows))]
use crate::mapped_file::retirement::sidecar::encode_store_meta;
use crate::mapped_file::retirement::sidecar::StoreMeta;

#[cfg(any(target_os = "linux", windows))]
use super::execute_prepared_initial_bootstrap;
use super::prepare_initial_bootstrap_foundation;
use super::support::store_uuid;

fn store_meta() -> StoreMeta {
    StoreMeta {
        store_uuid: store_uuid(),
        creation_time_ns: 17,
        bootstrap_id: [0x51; 16],
    }
}

#[cfg(not(windows))]
fn open_root(path: &std::path::Path) -> File {
    File::open(path).expect("open Store root")
}

#[cfg(windows)]
fn open_root(path: &std::path::Path) -> File {
    use windows::Win32::Storage::FileSystem::FILE_FLAG_BACKUP_SEMANTICS;

    OpenOptions::new()
        .read(true)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS.0)
        .open(path)
        .expect("open Store root")
}

#[cfg(any(target_os = "linux", windows))]
#[test]
fn native_foundation_is_handle_relative_and_reopens_exact_initial_artifacts() {
    let root = tempfile::tempdir().expect("temporary Store root");
    let root_file = open_root(root.path());
    let meta = store_meta();

    let prepared = prepare_initial_bootstrap_foundation(root_file, &meta)
        .expect("platform creates the exact bootstrap foundation");

    let lifecycle = root.path().join(".rocketmq-lifecycle");
    assert_eq!(
        fs::read(lifecycle.join("store.meta")).expect("read store.meta"),
        encode_store_meta(&meta).expect("encode store.meta")
    );
    assert_eq!(
        fs::read(lifecycle.join("ACKNOWLEDGED.v1")).expect("read acknowledgement"),
        [0_u8; 208]
    );
    assert_eq!(
        fs::read(lifecycle.join("retirement.log.g00000000000000000000")).expect("read generation-0 log"),
        Vec::<u8>::new()
    );
    assert_eq!(prepared.store_uuid_for_test(), meta.store_uuid);
}

#[cfg(any(target_os = "linux", windows))]
#[test]
fn native_executor_idempotently_replays_the_complete_initial_bootstrap() {
    let root = tempfile::tempdir().expect("temporary Store root");
    fs::create_dir(root.path().join("commitlog")).expect("commitlog directory");
    fs::write(root.path().join("commitlog/00000000000000000000"), [7_u8; 32]).expect("legacy segment");
    let meta = store_meta();

    let execute = || {
        let root_file = open_root(root.path());
        let prepared = prepare_initial_bootstrap_foundation(root_file, &meta).expect("prepare exact foundation");
        execute_prepared_initial_bootstrap(prepared).expect("complete fenced bootstrap")
    };

    let first = execute();
    let second = execute();

    assert_eq!(first.store_uuid(), second.store_uuid());
    assert_eq!(first.witness_sequence(), second.witness_sequence());
    assert_eq!(first.acknowledgement_epoch(), second.acknowledgement_epoch());
    assert_eq!(first.marker_epoch(), second.marker_epoch());
    assert_eq!(first.witness_sequence(), 3);
    assert_eq!(first.acknowledgement_epoch(), 3);
    assert_eq!(first.marker_epoch(), 1);
    let lifecycle = root.path().join(".rocketmq-lifecycle");
    for expected in [
        "store.meta",
        "ACKNOWLEDGED.v1",
        "retirement.log.g00000000000000000000",
        "manifest.snapshot.g00000000000000000000",
        "ENABLED.v1",
    ] {
        assert!(lifecycle.join(expected).is_file(), "missing {expected}");
    }
    assert!(!lifecycle.join("store.meta.bootstrap.tmp").exists());
    assert!(!lifecycle
        .join("manifest.snapshot.g00000000000000000000.bootstrap.tmp")
        .exists());
    assert!(!lifecycle.join("ENABLED.v1.bootstrap.tmp").exists());
}

#[cfg(target_os = "linux")]
#[test]
fn linux_fences_bootstrap_when_the_lifecycle_directory_binding_changes() {
    let root = tempfile::tempdir().expect("temporary Store root");
    fs::create_dir(root.path().join("commitlog")).expect("commitlog directory");
    fs::write(root.path().join("commitlog/00000000000000000000"), [7_u8; 32]).expect("legacy segment");
    let meta = store_meta();
    let root_file = open_root(root.path());
    let prepared = prepare_initial_bootstrap_foundation(root_file, &meta).expect("prepare exact foundation");

    let lifecycle = root.path().join(".rocketmq-lifecycle");
    let displaced = root.path().join(".rocketmq-lifecycle.displaced");
    fs::rename(&lifecycle, &displaced).expect("displace lifecycle directory");
    fs::create_dir(&lifecycle).expect("install replacement lifecycle directory");
    fs::write(lifecycle.join("replacement.sentinel"), b"replacement").expect("replacement sentinel");

    execute_prepared_initial_bootstrap(prepared)
        .expect_err("a replacement lifecycle directory must fence bootstrap before mutation");

    assert_eq!(
        fs::read(lifecycle.join("replacement.sentinel")).expect("replacement sentinel remains"),
        b"replacement"
    );
    assert_eq!(
        fs::read_dir(&lifecycle).expect("replacement directory").count(),
        1,
        "bootstrap must not publish artifacts into the replacement directory"
    );
}

#[cfg(not(any(target_os = "linux", windows)))]
#[test]
fn unsupported_platform_is_rejected_before_any_artifact_is_created() {
    let root = tempfile::tempdir().expect("temporary Store root");
    let root_file = open_root(root.path());

    let error = prepare_initial_bootstrap_foundation(root_file, &store_meta())
        .expect_err("this platform is not Wave-B writer-qualified");

    assert_eq!(error.category_for_test(), "unsupported-platform");
    assert!(!root.path().join(".rocketmq-lifecycle").exists());
}
