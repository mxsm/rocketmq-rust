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

use std::collections::BTreeMap;
use std::fs;
use std::fs::OpenOptions;
use std::path::Path;
use std::path::PathBuf;

use rocketmq_store_inspect::downgrade_preflight::run_preflight;
use rocketmq_store_inspect::downgrade_preflight::DowngradePreflightRequest;
use rocketmq_store_rocksdb::config::RocksDbColumnFamilyConfig;
use rocketmq_store_rocksdb::profile_marker::PopConsumerProfileMarker;
use rocketmq_store_rocksdb::profile_marker::POP_CONSUMER_PROFILE_COLUMN_FAMILY;
use rocketmq_store_rocksdb::profile_marker::POP_CONSUMER_PROFILE_MARKER_KEY;
use rocketmq_store_rocksdb::store::KeyValueStore;
use rocketmq_store_rocksdb::store::RocksDbStore;
use rocketmq_store_rocksdb::RocksDbConfig;

#[test]
fn legacy_absent_store_is_safe_for_pre_one_zero_target() {
    let temp = tempfile::tempdir().expect("tempdir");
    let store = temp.path().join("store");
    fs::create_dir_all(store.join("commitlog")).expect("commitlog");
    let config = write_config(temp.path(), &store, false);

    let report = run_preflight(&DowngradePreflightRequest::new("0.9.0", config)).expect("preflight");

    assert!(report.allowed, "{report:?}");
    assert!(report
        .checks
        .iter()
        .any(|check| check.id == "pop" && check.status == "legacy-absent"));
}

#[test]
fn extended_timer_owner_denies_pre_one_zero_target() {
    let temp = tempfile::tempdir().expect("tempdir");
    let store = temp.path().join("store");
    fs::create_dir_all(store.join("commitlog")).expect("commitlog");
    fs::create_dir_all(store.join("config")).expect("config");
    fs::write(store.join("config/timer-store-owner.meta"), b"extended_timeline:v1:7\n").expect("owner marker");
    let config = write_config(temp.path(), &store, false);

    let report = run_preflight(&DowngradePreflightRequest::new("0.9.0", config)).expect("preflight");

    assert!(!report.allowed);
    assert!(report
        .checks
        .iter()
        .any(|check| check.id == "timer" && check.status == "incompatible"));
    assert!(report.actions.iter().any(|action| action.contains("quiesce")));
}

#[test]
fn declared_pop_profile_without_database_fails_closed() {
    let temp = tempfile::tempdir().expect("tempdir");
    let store = temp.path().join("store");
    fs::create_dir_all(store.join("commitlog")).expect("commitlog");
    fs::create_dir_all(store.join("config")).expect("config");
    fs::write(
        store.join("config/storage-format-inventory.json"),
        br#"{"popConsumerProfile":{"declared":true,"formatVersion":1}}"#,
    )
    .expect("format inventory");
    let config = write_config(temp.path(), &store, false);

    let report = run_preflight(&DowngradePreflightRequest::new("1.0.0", config)).expect("preflight");

    assert!(!report.allowed);
    assert!(report
        .checks
        .iter()
        .any(|check| check.id == "pop" && check.status == "declared-present-invalid"));
}

#[test]
fn multipath_segment_outside_primary_requires_consolidation() {
    let temp = tempfile::tempdir().expect("tempdir");
    let store = temp.path().join("store");
    let primary = temp.path().join("commitlog-a");
    let secondary = temp.path().join("commitlog-b");
    fs::create_dir_all(&store).expect("store");
    fs::create_dir_all(&primary).expect("primary");
    fs::create_dir_all(&secondary).expect("secondary");
    fs::write(primary.join("00000000000000000000"), [0_u8; 8]).expect("primary segment");
    fs::write(secondary.join("00000000000000000008"), [1_u8; 8]).expect("secondary segment");
    let config = temp.path().join("broker.toml");
    fs::write(
        &config,
        format!(
            "[store]\nstorePathRootDir = {:?}\nstorePathCommitLog = {:?}\nmappedFileSizeCommitLog = 8\n",
            store.to_string_lossy(),
            format!("{},{}", primary.display(), secondary.display())
        ),
    )
    .expect("config");

    let report = run_preflight(&DowngradePreflightRequest::new("0.9.0", config)).expect("preflight");

    assert!(!report.allowed);
    assert!(report
        .checks
        .iter()
        .any(|check| check.id == "multipath" && check.status == "incompatible"));
}

#[test]
fn legacy_database_without_profile_cf_is_absent_until_declared() {
    let temp = tempfile::tempdir().expect("tempdir");
    let store = temp.path().join("store");
    fs::create_dir_all(store.join("commitlog")).expect("commitlog");
    create_pop_database(&store.join("kvStore"), false, None);
    let before = snapshot(&store.join("kvStore"));
    let config = write_config(temp.path(), &store, false);

    let legacy = run_preflight(&DowngradePreflightRequest::new("1.0.0", &config)).expect("legacy preflight");
    assert!(legacy.allowed, "{legacy:?}");
    assert!(legacy
        .checks
        .iter()
        .any(|check| check.id == "pop" && check.status == "legacy-absent"));
    assert_eq!(snapshot(&store.join("kvStore")), before);

    fs::create_dir_all(store.join("config")).expect("config");
    fs::write(
        store.join("config/storage-format-inventory.json"),
        br#"{"popConsumerProfile":{"declared":true}}"#,
    )
    .expect("inventory");
    let declared = run_preflight(&DowngradePreflightRequest::new("1.0.0", config)).expect("declared preflight");
    assert!(!declared.allowed);
    assert!(declared
        .checks
        .iter()
        .any(|check| check.id == "pop" && check.status == "declared-present-invalid"));
    assert_eq!(snapshot(&store.join("kvStore")), before);
}

#[test]
fn valid_markers_allow_current_reader_and_fence_pre_one_zero_without_mutation() {
    let temp = tempfile::tempdir().expect("tempdir");
    let store = temp.path().join("store");
    fs::create_dir_all(store.join("commitlog")).expect("commitlog");
    fs::create_dir_all(store.join("config")).expect("config");
    create_pop_database(&store.join("kvStore"), true, Some(PopConsumerProfileMarker::new(7)));
    fs::write(
        store.join("config/storage-format-inventory.json"),
        br#"{"popConsumerProfile":{"declared":true}}"#,
    )
    .expect("inventory");
    fs::write(
        store.join("config/timer-store-owner.meta"),
        b"extended_timeline:v1:11\n",
    )
    .expect("timer marker");
    fs::create_dir_all(store.join("compaction")).expect("compaction");
    fs::write(store.join("compaction/CURRENT"), compaction_current(2)).expect("compaction current");
    fs::write(
        store.join("config/tieredStoreMetadata.json"),
        br#"{"format":"rocketmq-tiered-metadata","version":1}"#,
    )
    .expect("tiered metadata");
    let config = write_config(temp.path(), &store, true);
    let before = snapshot(&store.join("kvStore"));

    let current = run_preflight(&DowngradePreflightRequest::new("1.0.0", &config)).expect("current preflight");
    assert!(current.allowed, "{current:?}");
    for id in ["pop", "timer", "compaction", "tiered"] {
        assert!(current
            .checks
            .iter()
            .any(|check| check.id == id && check.status == "present-valid"));
    }
    assert_eq!(snapshot(&store.join("kvStore")), before);

    let old = run_preflight(&DowngradePreflightRequest::new("0.9.0", config)).expect("old preflight");
    assert!(!old.allowed);
    for id in ["pop", "timer", "compaction", "tiered"] {
        assert!(old
            .checks
            .iter()
            .any(|check| check.id == id && check.status == "incompatible"));
    }
    assert_eq!(snapshot(&store.join("kvStore")), before);
}

#[test]
fn incomplete_compaction_generation_and_active_store_lock_fail_closed() {
    let temp = tempfile::tempdir().expect("tempdir");
    let store = temp.path().join("store");
    fs::create_dir_all(store.join("commitlog")).expect("commitlog");
    fs::create_dir_all(store.join("compaction/generations/gen-1")).expect("half generation");
    fs::write(store.join("compaction/generations/gen-1/GENERATION"), b"partial").expect("partial generation");
    let config = write_config(temp.path(), &store, true);

    let report = run_preflight(&DowngradePreflightRequest::new("1.0.0", &config)).expect("preflight");
    assert!(!report.allowed);
    assert!(report
        .checks
        .iter()
        .any(|check| check.id == "compaction" && check.status == "declared-present-invalid"));

    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(store.join("lock"))
        .expect("lock file");
    fs2::FileExt::try_lock_exclusive(&lock).expect("simulate Broker lock");
    let error =
        run_preflight(&DowngradePreflightRequest::new("1.0.0", config)).expect_err("active Store lock must fail");
    assert!(error.to_string().contains("Broker must be stopped"));
    fs2::FileExt::unlock(&lock).expect("unlock fixture");
}

#[test]
fn legacy_compaction_current_is_accepted_but_a_bad_checksum_is_not() {
    let temp = tempfile::tempdir().expect("tempdir");
    let store = temp.path().join("store");
    fs::create_dir_all(store.join("commitlog")).expect("commitlog");
    fs::create_dir_all(store.join("compaction")).expect("compaction");
    let current_path = store.join("compaction/CURRENT");
    let valid = compaction_current(1);
    fs::write(&current_path, valid).expect("legacy CURRENT");
    let config = write_config(temp.path(), &store, true);

    let legacy = run_preflight(&DowngradePreflightRequest::new("0.9.0", &config)).expect("legacy preflight");
    assert!(legacy.allowed, "{legacy:?}");
    assert!(legacy
        .checks
        .iter()
        .any(|check| check.id == "compaction" && check.status == "present-valid"));

    let mut corrupt = valid;
    corrupt[10] ^= 1;
    fs::write(&current_path, corrupt).expect("corrupt CURRENT");
    let rejected = run_preflight(&DowngradePreflightRequest::new("1.0.0", config)).expect("corrupt preflight");
    assert!(!rejected.allowed);
    assert!(rejected
        .checks
        .iter()
        .any(|check| check.id == "compaction" && check.status == "corrupt"));
    assert_eq!(fs::read(current_path).expect("CURRENT remains unchanged"), corrupt);
}

fn write_config(root: &std::path::Path, store: &std::path::Path, compaction: bool) -> std::path::PathBuf {
    let config = root.join("broker.toml");
    fs::write(
        &config,
        format!(
            "[store]\nstorePathRootDir = {:?}\nstorePathCommitLog = {:?}\nmappedFileSizeCommitLog = 8\nenableCompaction = {compaction}\ntimerStoreMode = \"java_compat\"\n",
            store.to_string_lossy(),
            store.join("commitlog").to_string_lossy()
        ),
    )
    .expect("config");
    config
}

fn create_pop_database(path: &Path, profile_cf: bool, marker: Option<PopConsumerProfileMarker>) {
    let mut config = RocksDbConfig {
        enabled: true,
        path: path.to_path_buf(),
        ..RocksDbConfig::default()
    };
    if profile_cf {
        let mut profile = RocksDbColumnFamilyConfig::consume_queue_default();
        profile.name = POP_CONSUMER_PROFILE_COLUMN_FAMILY.to_owned();
        config.column_families.push(profile);
    }
    let database = RocksDbStore::open(config).expect("create RocksDB fixture");
    if let Some(marker) = marker {
        database
            .put_cf(
                POP_CONSUMER_PROFILE_COLUMN_FAMILY,
                POP_CONSUMER_PROFILE_MARKER_KEY,
                &marker.encode().expect("encode marker"),
            )
            .expect("write marker");
    }
    database.flush().expect("flush fixture");
    database.close();
}

fn compaction_current(version: u16) -> [u8; 40] {
    let mut bytes = [0_u8; 40];
    bytes[0..4].copy_from_slice(&0x4343_5547_u32.to_be_bytes());
    bytes[4..6].copy_from_slice(&version.to_be_bytes());
    bytes[8..16].copy_from_slice(&7_u64.to_be_bytes());
    bytes[16..24].copy_from_slice(&u64::MAX.to_be_bytes());
    bytes[24..32].copy_from_slice(&8_u64.to_be_bytes());
    let checksum = rocketmq_model::utils::crc32_utils::crc32(&bytes[..32]);
    bytes[32..36].copy_from_slice(&checksum.to_be_bytes());
    bytes
}

fn snapshot(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    let mut output = BTreeMap::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(&directory).expect("read fixture directory") {
            let path = entry.expect("fixture entry").path();
            if path.is_dir() {
                pending.push(path);
            } else {
                output.insert(path.strip_prefix(root).unwrap().to_path_buf(), fs::read(path).unwrap());
            }
        }
    }
    output
}
