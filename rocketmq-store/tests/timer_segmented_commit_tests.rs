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

use rocketmq_store::test_support::run_segmented_commit_crash_probe;
use rocketmq_store::test_support::run_segmented_snapshot_probe;
use rocketmq_store::test_support::SegmentedCommitTestCrashPoint;

#[test]
fn native_fsync_overlay_and_checkpoint_crash_boundaries_recover_without_loss() {
    for crash in [
        SegmentedCommitTestCrashPoint::BeforeNativeAppend,
        SegmentedCommitTestCrashPoint::AfterNativeFsyncBeforeOverlay,
        SegmentedCommitTestCrashPoint::AfterAtomicOverlayCheckpoint,
        SegmentedCommitTestCrashPoint::AfterPublish,
    ] {
        let probe = run_segmented_commit_crash_probe(crash);
        match crash {
            SegmentedCommitTestCrashPoint::BeforeNativeAppend => {
                assert!(!probe.native_before_replay);
                assert!(!probe.overlay_before_replay);
                assert!(!probe.checkpoint_before_replay);
                assert_eq!(probe.orphan_records_before_replay, 0);
            }
            SegmentedCommitTestCrashPoint::AfterNativeFsyncBeforeOverlay => {
                assert!(probe.native_before_replay);
                assert!(!probe.overlay_before_replay);
                assert!(!probe.checkpoint_before_replay);
                assert_eq!(probe.orphan_records_before_replay, 1);
            }
            SegmentedCommitTestCrashPoint::AfterAtomicOverlayCheckpoint
            | SegmentedCommitTestCrashPoint::AfterPublish => {
                assert!(probe.native_before_replay);
                assert!(probe.overlay_before_replay);
                assert!(probe.checkpoint_before_replay);
                assert_eq!(probe.orphan_records_before_replay, 0);
            }
        }
        assert_eq!(probe.native_records_after_replay, 1, "{crash:?}");
        assert!(probe.overlay_after_replay, "{crash:?}");
        assert!(probe.checkpoint_after_replay, "{crash:?}");
        assert_eq!(probe.orphan_records_after_replay, 0, "{crash:?}");
    }
}

#[test]
fn overlay_checkpoint_is_atomic_with_overlay_state() {
    let probe = run_segmented_commit_crash_probe(SegmentedCommitTestCrashPoint::AfterAtomicOverlayCheckpoint);
    assert_eq!(probe.overlay_before_replay, probe.checkpoint_before_replay);
}

#[test]
fn snapshot_binds_native_manifest_and_rocksdb_sequence() {
    let probe = run_segmented_snapshot_probe();
    assert!(probe.joint_binding_valid);
    assert!(probe.rocks_sequence_nonzero);
    assert!(probe.damaged_native_binding_rejected);
    assert!(probe.damaged_artifact_rejected);
}
