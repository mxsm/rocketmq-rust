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
use std::fs::OpenOptions;
use std::io;

use rocketmq_store_inspect::multipath_consolidate::consolidate_multipath;
use rocketmq_store_inspect::multipath_consolidate::consolidate_multipath_with_environment;
use rocketmq_store_inspect::multipath_consolidate::consolidate_multipath_with_hook;
use rocketmq_store_inspect::multipath_consolidate::ConsolidationRequest;

const V0_9_COMMIT_LOG: &[u8] =
    include_bytes!("../../../rocketmq-store/tests/fixtures/upgrade/v0.9.0/local-file/commitlog/00000000000000000000");

#[test]
fn consolidates_contiguous_segments_and_leaves_sources_unchanged() {
    let temp = tempfile::tempdir().expect("tempdir");
    let root_a = temp.path().join("a");
    let root_b = temp.path().join("b");
    let target = temp.path().join("target");
    fs::create_dir_all(&root_a).expect("root a");
    fs::create_dir_all(&root_b).expect("root b");
    let segment_size = V0_9_COMMIT_LOG.len() as u64;
    fs::write(root_a.join("00000000000000000000"), V0_9_COMMIT_LOG).expect("segment zero");
    fs::write(
        root_b.join(format!("{segment_size:020}")),
        vec![0_u8; V0_9_COMMIT_LOG.len()],
    )
    .expect("segment one");

    let report = consolidate_multipath(&ConsolidationRequest::new(
        vec![root_a.clone(), root_b.clone()],
        target.clone(),
        segment_size,
    ))
    .expect("consolidation succeeds");

    assert_eq!(report.segment_count, 2);
    assert_eq!(report.copied_bytes, segment_size * 2);
    assert_eq!(report.record_count, 2);
    assert_eq!(fs::read(target.join("00000000000000000000")).unwrap(), V0_9_COMMIT_LOG);
    assert_eq!(
        fs::read(target.join(format!("{segment_size:020}"))).unwrap(),
        vec![0_u8; V0_9_COMMIT_LOG.len()]
    );
    assert_eq!(fs::read(root_a.join("00000000000000000000")).unwrap(), V0_9_COMMIT_LOG);
    assert_eq!(
        fs::read(root_b.join(format!("{segment_size:020}"))).unwrap(),
        vec![0_u8; V0_9_COMMIT_LOG.len()]
    );
}

#[test]
fn rejects_duplicate_owner_and_gap_without_publishing_target() {
    let temp = tempfile::tempdir().expect("tempdir");
    let root_a = temp.path().join("a");
    let root_b = temp.path().join("b");
    fs::create_dir_all(&root_a).expect("root a");
    fs::create_dir_all(&root_b).expect("root b");
    fs::write(root_a.join("00000000000000000000"), [0_u8; 8]).expect("segment zero");
    fs::write(root_b.join("00000000000000000000"), [1_u8; 8]).expect("duplicate");
    let target = temp.path().join("target-duplicate");
    let error = consolidate_multipath(&ConsolidationRequest::new(
        vec![root_a.clone(), root_b.clone()],
        target.clone(),
        8,
    ))
    .expect_err("duplicate owner must fail");
    assert!(error.to_string().contains("duplicate"));
    assert!(!target.exists());

    fs::remove_file(root_b.join("00000000000000000000")).expect("remove duplicate");
    fs::write(root_b.join("00000000000000000016"), [0_u8; 8]).expect("gapped segment");
    let target = temp.path().join("target-gap");
    let error = consolidate_multipath(&ConsolidationRequest::new(vec![root_a, root_b], target.clone(), 8))
        .expect_err("gap must fail");
    assert!(error.to_string().contains("expected offset 8"));
    assert!(!target.exists());
}

#[test]
fn injected_interruption_never_publishes_partial_target() {
    let temp = tempfile::tempdir().expect("tempdir");
    let root = temp.path().join("root");
    let target = temp.path().join("target");
    fs::create_dir_all(&root).expect("root");
    fs::write(root.join("00000000000000000000"), [0_u8; 8]).expect("segment zero");
    fs::write(root.join("00000000000000000008"), [0_u8; 8]).expect("segment eight");

    let error = consolidate_multipath_with_hook(
        &ConsolidationRequest::new(vec![root.clone()], target.clone(), 8),
        |copied| {
            if copied == 1 {
                Err(io::Error::other("injected interruption"))
            } else {
                Ok(())
            }
        },
    )
    .expect_err("interruption must fail");

    assert!(error.to_string().contains("injected interruption"));
    assert!(!target.exists());
    assert_eq!(fs::read(root.join("00000000000000000000")).unwrap(), [0_u8; 8]);
    assert_eq!(fs::read(root.join("00000000000000000008")).unwrap(), [0_u8; 8]);
}

#[test]
fn rejects_running_broker_lock_without_copying() {
    let temp = tempfile::tempdir().expect("tempdir");
    let root = temp.path().join("source");
    let target = temp.path().join("target");
    fs::create_dir_all(&root).expect("source");
    fs::write(root.join("00000000000000000000"), [0_u8; 8]).expect("segment");
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(temp.path().join("lock"))
        .expect("lock file");
    fs2::FileExt::try_lock_exclusive(&lock).expect("simulate Broker lock");

    let error = consolidate_multipath(
        &ConsolidationRequest::new(vec![root], target.clone(), 8).with_store_root(temp.path().to_path_buf()),
    )
    .expect_err("active Broker lock must fail");

    assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
    assert!(!target.exists());
    fs2::FileExt::unlock(&lock).expect("unlock fixture");
}

#[test]
fn rejects_insufficient_target_space_before_staging() {
    let temp = tempfile::tempdir().expect("tempdir");
    let root = temp.path().join("source");
    let target = temp.path().join("target");
    fs::create_dir_all(&root).expect("source");
    fs::write(root.join("00000000000000000000"), [0_u8; 8]).expect("segment");

    let error = consolidate_multipath_with_environment(
        &ConsolidationRequest::new(vec![root], target.clone(), 8),
        |_| Ok(7),
        |_| Ok(()),
    )
    .expect_err("insufficient capacity must fail");

    assert_eq!(error.kind(), io::ErrorKind::StorageFull);
    assert!(!target.exists());
}
