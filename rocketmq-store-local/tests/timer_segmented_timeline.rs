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

use std::fs::OpenOptions;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;

use rocketmq_store_api::TimerEngineId;
use rocketmq_store_api::TimerGeneration;
use rocketmq_store_api::TimerId;
use rocketmq_store_api::TimerPayloadStoreLocator;
use rocketmq_store_api::TimerSourceCqOffset;
use rocketmq_store_local::timer::segmented_timeline::SegmentedTimeline;
use rocketmq_store_local::timer::segmented_timeline::SegmentedTimelineConfig;
use rocketmq_store_local::timer::timeline_manifest::TimelineManifestV1;
use rocketmq_store_local::timer::timeline_segment::inspect_timeline_run;
use rocketmq_store_local::timer::timeline_segment::write_timeline_run;
use rocketmq_store_local::timer::timeline_segment::TimelinePartitionKey;
use rocketmq_store_local::timer::timeline_segment::TimelineRunKind;
use rocketmq_store_local::timer::timeline_segment::TimelineRunReader;
use rocketmq_store_local::timer::timeline_segment::TimelineSegmentKey;
use rocketmq_store_local::timer::timeline_segment::TimelineSegmentRecord;
use tempfile::TempDir;

fn record(sequence: u64, due_time_ms: i64) -> TimelineSegmentRecord {
    let partition = TimelinePartitionKey::from_deadline(due_time_ms, 3).expect("partition");
    TimelineSegmentRecord {
        key: TimelineSegmentKey {
            due_time_ms,
            lane: 3,
            timer_id: TimerId::new(u128::from(sequence)),
            generation: TimerGeneration::new(sequence % 3),
        },
        payload: TimerPayloadStoreLocator::try_new(
            partition.due_day_utc,
            3,
            1,
            sequence.saturating_mul(128),
            100,
            u32::try_from(sequence).unwrap_or(u32::MAX),
        )
        .expect("payload"),
        source_cq_offset: TimerSourceCqOffset::new(sequence as i64),
        source_physical_offset: sequence as i64 * 1_024,
        source_size: 100,
        state_version: 0,
        owner_engine: TimerEngineId::ExtendedTimeline,
        shadow_only: false,
    }
}

fn record_in_lane(sequence: u64, due_time_ms: i64, lane: u16) -> TimelineSegmentRecord {
    let mut record = record(sequence, due_time_ms);
    let partition = TimelinePartitionKey::from_deadline(due_time_ms, lane).expect("partition");
    record.key.lane = lane;
    record.payload = TimerPayloadStoreLocator::try_new(
        partition.due_day_utc,
        lane,
        1,
        sequence.saturating_mul(128),
        100,
        u32::try_from(sequence).unwrap_or(u32::MAX),
    )
    .expect("payload");
    record
}

fn open(root: &TempDir) -> SegmentedTimeline {
    SegmentedTimeline::open(root.path(), SegmentedTimelineConfig::default()).expect("open")
}

#[test]
fn codec_round_trips_sealed_fixed_records() {
    let root = TempDir::new().expect("root");
    let records = vec![record(1, 3_600_010), record(2, 3_600_020)];
    let partition = records[0].key.partition().expect("partition");
    let descriptor = write_timeline_run(
        root.path(),
        "delta-1.run",
        TimelineRunKind::Delta,
        partition,
        1,
        1,
        &records,
    )
    .expect("write");
    assert_eq!(
        inspect_timeline_run(root.path(), "delta-1.run").expect("inspect"),
        descriptor
    );
    let mut reader = TimelineRunReader::open(root.path(), descriptor).expect("reader");
    assert_eq!(reader.read_next().expect("first"), Some(records[0]));
    assert_eq!(reader.read_next().expect("second"), Some(records[1]));
    assert_eq!(reader.read_next().expect("eof"), None);
}

#[test]
fn codec_rejects_unknown_version_bad_crc_and_unsealed_tail() {
    for (name, offset) in [("version", 4u64), ("crc", 140u64)] {
        let root = TempDir::new().expect("root");
        let source = record(1, 3_600_010);
        write_timeline_run(
            root.path(),
            "damage.run",
            TimelineRunKind::Delta,
            source.key.partition().expect("partition"),
            1,
            1,
            &[source],
        )
        .expect("write");
        let mut file = OpenOptions::new()
            .write(true)
            .open(root.path().join("damage.run"))
            .expect("open damage");
        file.seek(SeekFrom::Start(offset)).expect("seek");
        file.write_all(&[0x7f]).expect("damage");
        file.sync_all().expect("sync");
        assert!(inspect_timeline_run(root.path(), "damage.run").is_err(), "{name}");
    }

    let root = TempDir::new().expect("root");
    let source = record(1, 3_600_010);
    write_timeline_run(
        root.path(),
        "unsealed.run",
        TimelineRunKind::Delta,
        source.key.partition().expect("partition"),
        1,
        1,
        &[source],
    )
    .expect("write");
    OpenOptions::new()
        .write(true)
        .open(root.path().join("unsealed.run"))
        .expect("open")
        .set_len(144 + 100)
        .expect("truncate footer");
    assert!(inspect_timeline_run(root.path(), "unsealed.run").is_err());
}

#[test]
fn recovery_falls_back_to_the_previous_manifest_and_reports_orphan_run() {
    let root = TempDir::new().expect("root");
    let timeline = open(&root);
    timeline.append_batch(&[record(1, 3_600_010)]).expect("first");
    timeline.append_batch(&[record(2, 3_600_020)]).expect("second");
    let latest = timeline.manifest();
    assert_eq!(latest.generation, 2);
    drop(timeline);

    let manifest_root = root.path().join("timer-extended/timeline-segments-v1");
    std::fs::write(manifest_root.join("CURRENT.A"), b"truncated").expect("damage newest");
    let recovered = open(&root);
    assert_eq!(recovered.manifest().generation, 1);
    assert_eq!(recovered.orphan_runs().expect("orphans").len(), 1);
}

#[test]
fn range_pages_same_millisecond_hotspot_without_loss() {
    let root = TempDir::new().expect("root");
    let timeline = open(&root);
    let records = (1..=100_000)
        .map(|sequence| record(sequence, 7_200_000))
        .collect::<Vec<_>>();
    timeline.append_batch(&records).expect("append hotspot");
    let mut continuation = None;
    let mut actual = Vec::new();
    loop {
        let page = timeline
            .scan_due(
                None,
                7_200_001,
                997,
                997 * TimelineSegmentRecord::encoded_size(),
                continuation,
            )
            .expect("scan");
        actual.extend(page.records.iter().map(|record| record.key));
        continuation = page.continuation;
        if continuation.is_none() {
            break;
        }
    }
    assert_eq!(actual.len(), records.len());
    assert!(actual.windows(2).all(|pair| pair[0] < pair[1]));
    assert_eq!(actual.first(), records.first().map(|record| &record.key));
    assert_eq!(actual.last(), records.last().map(|record| &record.key));
}

#[test]
fn range_restarts_from_full_key_when_manifest_changes_between_pages() {
    let root = TempDir::new().expect("root");
    let timeline = open(&root);
    timeline
        .append_batch(&[record(1, 3_600_010), record(2, 3_600_020)])
        .expect("first batch");
    let first = timeline.scan_due(None, 4_000_000, 1, 100, None).expect("first page");
    assert_eq!(first.records.len(), 1);
    timeline.append_batch(&[record(3, 3_600_030)]).expect("new delta");
    let second = timeline
        .scan_due(None, 4_000_000, 10, 1_000, first.continuation)
        .expect("resume");
    assert_eq!(
        second
            .records
            .iter()
            .map(|record| record.key.timer_id.get())
            .collect::<Vec<_>>(),
        vec![2, 3]
    );
}

#[test]
fn range_continuation_crosses_lane_partitions_without_filtering_next_lane() {
    let root = TempDir::new().expect("root");
    let timeline = open(&root);
    let records = (0..4u16)
        .flat_map(|lane| {
            (0..3u64).map(move |offset| {
                let sequence = u64::from(lane) * 10 + offset + 1;
                record_in_lane(sequence, 7_200_000 + i64::try_from(offset).expect("offset"), lane)
            })
        })
        .collect::<Vec<_>>();
    timeline.append_batch(&records).expect("append lanes");

    let mut continuation = None;
    let mut actual = Vec::new();
    loop {
        let page = timeline
            .scan_due(
                None,
                7_200_010,
                2,
                2 * TimelineSegmentRecord::encoded_size(),
                continuation,
            )
            .expect("scan lanes");
        actual.extend(page.records.iter().map(|record| record.key));
        continuation = page.continuation;
        if continuation.is_none() {
            break;
        }
    }
    actual.sort_unstable();
    let mut expected = records.iter().map(|record| record.key).collect::<Vec<_>>();
    expected.sort_unstable();
    assert_eq!(actual, expected);
}

#[test]
fn merge_is_bounded_and_snapshot_pin_fences_garbage_collection() {
    let root = TempDir::new().expect("root");
    let timeline = SegmentedTimeline::open(
        root.path(),
        SegmentedTimelineConfig {
            max_open_runs: 8,
            merge_delta_runs: 2,
            merge_max_input_runs: 8,
            merge_max_output_bytes: 1_000,
        },
    )
    .expect("open");
    timeline.append_batch(&[record(1, 3_600_010)]).expect("first");
    timeline.append_batch(&[record(2, 3_600_020)]).expect("second");
    let pin = timeline.pin_snapshot(7).expect("pin");
    let merged = timeline.merge_one(|_| true).expect("merge");
    assert_eq!(merged.merged_runs, 2);
    assert_eq!(merged.output_records, 2);
    assert_eq!(timeline.manifest().garbage_runs.len(), 2);
    assert_eq!(timeline.release_snapshot(pin).expect("release"), 2);
    assert!(timeline.manifest().garbage_runs.is_empty());
}

#[test]
fn merge_yields_to_due_delivery_without_publishing_partial_state() {
    let root = TempDir::new().expect("root");
    let timeline = SegmentedTimeline::open(
        root.path(),
        SegmentedTimelineConfig {
            max_open_runs: 8,
            merge_delta_runs: 2,
            merge_max_input_runs: 8,
            merge_max_output_bytes: 1_000,
        },
    )
    .expect("open");
    timeline.append_batch(&[record(1, 3_600_010)]).expect("first");
    timeline.append_batch(&[record(2, 3_600_020)]).expect("second");
    let generation = timeline.manifest().generation;

    let yielded = timeline.merge_one_prioritized(|_| true, || true).expect("yield merge");
    assert!(yielded.deferred);
    assert_eq!(timeline.manifest().generation, generation);
    assert_eq!(timeline.manifest().active_runs.len(), 2);

    let completed = timeline
        .merge_one_prioritized(|_| true, || false)
        .expect("resume merge");
    assert_eq!(completed.merged_runs, 2);
    assert_eq!(completed.output_records, 2);
}

#[test]
fn gc_deletes_only_unpinned_whole_partition() {
    let root = TempDir::new().expect("root");
    let timeline = open(&root);
    let source = record(1, 3_600_010);
    timeline.append_batch(&[source]).expect("append");
    let partition = source.key.partition().expect("partition");
    let pin = timeline.pin_snapshot(8).expect("pin");
    assert!(timeline.delete_partition(partition).is_err());
    timeline.release_snapshot(pin).expect("release");
    assert_eq!(timeline.delete_partition(partition).expect("delete"), 1);
    assert!(timeline.manifest().active_runs.is_empty());
}

#[test]
fn manifest_rejects_both_damaged_copies() {
    let root = TempDir::new().expect("root");
    let manifest_root = root.path().join("timer-extended/timeline-segments-v1");
    std::fs::create_dir_all(&manifest_root).expect("mkdir");
    std::fs::write(manifest_root.join("CURRENT.A"), b"bad-a").expect("a");
    std::fs::write(manifest_root.join("CURRENT.B"), b"bad-b").expect("b");
    assert!(TimelineManifestV1::load(&manifest_root).is_err());
}
