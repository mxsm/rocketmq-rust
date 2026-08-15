// Copyright 2023 The RocketMQ Rust Authors
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
use std::sync::Arc;

use rocketmq_store::test_support::MultipathCommitLogHarness;
use rocketmq_store::test_support::StoreFaultInjector;
use rocketmq_store::test_support::StoreFaultPoint;

const SEGMENT_SIZE: u64 = 16;

#[test]
fn f08_mp_001_segments_are_created_across_real_roots() {
    let temp = tempfile::tempdir().expect("tempdir");
    let first = temp.path().join("a");
    let second = temp.path().join("b");
    let harness =
        MultipathCommitLogHarness::try_new(vec![first, second], vec![], SEGMENT_SIZE, None).expect("multipath harness");

    harness.create_segment(0, &[1; 16]).expect("first segment");
    harness.create_segment(16, &[2; 16]).expect("second segment");
    let owners = harness.segment_owners();

    assert_eq!(owners.len(), 2);
    assert_ne!(owners[0].1, owners[1].1);
}

#[test]
fn f08_mp_002_readonly_root_recovers_but_never_allocates() {
    let temp = tempfile::tempdir().expect("tempdir");
    let writable = temp.path().join("writable");
    let readonly = temp.path().join("readonly");
    fs::create_dir_all(&readonly).expect("readonly root");
    fs::write(readonly.join("00000000000000000000"), [7_u8; 16]).expect("old segment");

    let mut harness =
        MultipathCommitLogHarness::try_new(vec![writable.clone()], vec![readonly.clone()], SEGMENT_SIZE, None)
            .expect("multipath harness");
    assert!(harness.load());
    harness.create_segment(16, &[8; 16]).expect("new segment");

    let owners = harness.segment_owners();
    assert_eq!(owners[0].1, fs::canonicalize(readonly).expect("canonical readonly"));
    assert_eq!(owners[1].1, fs::canonicalize(writable).expect("canonical writable"));
}

#[test]
fn f08_mp_003_restart_rebuilds_global_order_and_truncates_the_real_owner() {
    let temp = tempfile::tempdir().expect("tempdir");
    let first = temp.path().join("a");
    let second = temp.path().join("b");
    {
        let harness =
            MultipathCommitLogHarness::try_new(vec![first.clone(), second.clone()], vec![], SEGMENT_SIZE, None)
                .expect("multipath harness");
        harness.create_segment(0, &[1; 16]).expect("first segment");
        harness.create_segment(16, &[2; 16]).expect("second segment");
    }

    let mut restarted =
        MultipathCommitLogHarness::try_new(vec![first, second], vec![], SEGMENT_SIZE, None).expect("restart");
    assert!(restarted.load());
    assert_eq!(
        restarted.read_range(0, 32).expect("global read"),
        [[1_u8; 16], [2_u8; 16]].concat()
    );
    assert!(restarted.truncate(16));
    assert_eq!(restarted.segment_owners().len(), 2);
    assert!(restarted.read_range(16, 1).is_err());
}

#[test]
fn f08_mp_004_prewrite_failure_moves_root_but_mid_segment_failure_fences() {
    let temp = tempfile::tempdir().expect("tempdir");
    let first = temp.path().join("a");
    let second = temp.path().join("b");
    fs::create_dir_all(&first).expect("first root");
    fs::create_dir_all(&second).expect("second root");
    let injector = Arc::new(StoreFaultInjector::default());
    injector.fail_on(StoreFaultPoint::CreateSegment, &first, 1);
    injector.fail_on(StoreFaultPoint::Append, &second, 1);
    let harness = MultipathCommitLogHarness::try_new(
        vec![first.clone(), second.clone()],
        vec![],
        SEGMENT_SIZE,
        Some(injector),
    )
    .expect("multipath harness");

    assert!(harness.create_segment(0, &[1; 16]).is_err());
    assert!(harness.is_write_fenced());
    let owners = harness.segment_owners();
    assert_eq!(owners.len(), 1);
    assert_eq!(owners[0].1, fs::canonicalize(second).expect("canonical second"));
    assert!(harness.create_segment(16, &[2; 16]).is_err());
}

#[test]
fn f08_mp_005_retired_roots_stay_readable_and_missing_readonly_roots_fail() {
    let temp = tempfile::tempdir().expect("tempdir");
    let first = temp.path().join("a");
    let second = temp.path().join("b");
    let harness = MultipathCommitLogHarness::try_new(vec![first.clone(), second.clone()], vec![], SEGMENT_SIZE, None)
        .expect("multipath harness");
    let first_path = harness.create_segment(0, &[3; 16]).expect("first segment");
    let retired_root = first_path.parent().expect("segment root").to_path_buf();
    harness.retire(&retired_root).expect("retire root");
    let second_path = harness.create_segment(16, &[4; 16]).expect("second segment");

    assert_ne!(second_path.parent().expect("second root"), retired_root);
    assert_eq!(harness.read_range(0, 16).expect("retired read"), [3_u8; 16]);
    assert!(MultipathCommitLogHarness::try_new(
        vec![temp.path().join("live")],
        vec![temp.path().join("missing-history")],
        SEGMENT_SIZE,
        None,
    )
    .is_err());
}

#[test]
fn f08_mp_006_replica_read_crosses_physical_root_boundary() {
    let temp = tempfile::tempdir().expect("tempdir");
    let harness = MultipathCommitLogHarness::try_new(
        vec![temp.path().join("a"), temp.path().join("b")],
        vec![],
        SEGMENT_SIZE,
        None,
    )
    .expect("multipath harness");
    harness.create_segment(0, b"0123456789abcdef").expect("first segment");
    harness.create_segment(16, b"ghijklmnopqrstuv").expect("second segment");

    assert_eq!(
        harness.read_range(8, 16).expect("cross-root transfer"),
        b"89abcdefghijklmn"
    );
}

#[test]
fn all_fault_points_are_deterministic_and_store_local() {
    let temp = tempfile::tempdir().expect("tempdir");
    let root = temp.path().join("root");
    fs::create_dir_all(&root).expect("root");
    let injector = Arc::new(StoreFaultInjector::default());
    injector.fail_on(StoreFaultPoint::Preallocate, &root, 1);
    let harness =
        MultipathCommitLogHarness::try_new(vec![root.clone()], vec![], SEGMENT_SIZE, Some(Arc::clone(&injector)))
            .expect("multipath harness");
    assert!(harness.candidate_roots(StoreFaultPoint::Preallocate).is_err());
    assert_eq!(
        harness
            .candidate_roots(StoreFaultPoint::Preallocate)
            .expect("second preallocation probe")
            .len(),
        1
    );

    harness.create_segment(0, &[5; 16]).expect("segment");
    injector.fail_on(StoreFaultPoint::Read, &root, 1);
    assert!(harness.read_range(0, 1).is_err());

    for (index, point) in [
        StoreFaultPoint::Flush,
        StoreFaultPoint::Truncate,
        StoreFaultPoint::Delete,
    ]
    .into_iter()
    .enumerate()
    {
        let case_root = temp.path().join(format!("case-{index}"));
        fs::create_dir_all(&case_root).expect("case root");
        let case_injector = Arc::new(StoreFaultInjector::default());
        let mut case = MultipathCommitLogHarness::try_new(
            vec![case_root.clone()],
            vec![],
            SEGMENT_SIZE,
            Some(Arc::clone(&case_injector)),
        )
        .expect("fault harness");
        case.create_segment(0, &[6; 16]).expect("case segment");
        case_injector.fail_on(point, &case_root, 1);
        let succeeded = match point {
            StoreFaultPoint::Flush => case.flush(),
            StoreFaultPoint::Truncate => case.truncate(8),
            StoreFaultPoint::Delete => case.destroy(),
            _ => unreachable!("covered above"),
        };
        assert!(!succeeded, "{point:?} should fail deterministically");
        assert!(case.is_write_fenced());
    }
}
