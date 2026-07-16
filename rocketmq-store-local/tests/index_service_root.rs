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

use rocketmq_store_local::index::dispatch::IndexDispatchRoot;
use rocketmq_store_local::index::service::build_index_key;
use rocketmq_store_local::index::service::build_index_key_into;
use rocketmq_store_local::index::service::build_index_key_with_type;
use rocketmq_store_local::index::service::destroy_index_files;
use rocketmq_store_local::index::service::drive_index_build_keys;
use rocketmq_store_local::index::service::drive_index_service_put;
use rocketmq_store_local::index::service::expired_index_file_count;
use rocketmq_store_local::index::service::has_indexable_keys;
use rocketmq_store_local::index::service::index_build_key_capacity;
use rocketmq_store_local::index::service::index_flush_checkpoint_timestamp;
use rocketmq_store_local::index::service::max_index_dispatch_offset;
use rocketmq_store_local::index::service::plan_index_build;
use rocketmq_store_local::index::service::plan_last_index_file;
use rocketmq_store_local::index::service::query_index_files;
use rocketmq_store_local::index::service::restore_index_safe_offset;
use rocketmq_store_local::index::service::retry_index_file_create;
use rocketmq_store_local::index::service::should_remove_unsafe_index_file;
use rocketmq_store_local::index::service::shutdown_index_files;
use rocketmq_store_local::index::service::total_index_file_size;
use rocketmq_store_local::index::service::IndexBuildKeyKind;
use rocketmq_store_local::index::service::IndexBuildKeysOutcome;
use rocketmq_store_local::index::service::IndexBuildPreflight;
use rocketmq_store_local::index::service::IndexServiceFile;
use rocketmq_store_local::index::service::IndexServiceRoot;

#[derive(Clone, Debug, PartialEq, Eq)]
struct FakeFile {
    id: usize,
    size: usize,
    entries: bool,
    full: bool,
    begin_timestamp: i64,
    end_timestamp: i64,
    end_phy_offset: i64,
    offsets: Vec<i64>,
}

impl IndexServiceFile for FakeFile {
    fn file_size(&self) -> usize {
        self.size
    }

    fn has_entries(&self) -> bool {
        self.entries
    }

    fn is_write_full(&self) -> bool {
        self.full
    }

    fn begin_timestamp(&self) -> i64 {
        self.begin_timestamp
    }

    fn end_timestamp(&self) -> i64 {
        self.end_timestamp
    }

    fn end_phy_offset(&self) -> i64 {
        self.end_phy_offset
    }

    fn is_time_matched(&self, begin: i64, end: i64) -> bool {
        begin <= self.end_timestamp && end >= self.begin_timestamp
    }

    fn select_phy_offsets(&self, offsets: &mut Vec<i64>, _key: &str, max_num: usize, _begin: i64, _end: i64) {
        for offset in &self.offsets {
            if offsets.len() >= max_num {
                break;
            }
            offsets.push(*offset);
        }
    }
}

#[test]
fn service_root_preserves_adapter_identity_and_mutability() {
    let mut root = IndexServiceRoot::new(vec![1]);
    assert_eq!(root.adapter(), &[1]);
    root.adapter_mut().push(2);
    assert_eq!(&*root, &[1, 2]);
    assert_eq!(root.into_adapter(), [1, 2]);
}

#[test]
fn query_driver_walks_newest_first_and_reports_newest_metadata() {
    let files = vec![
        fake_file(1, 10, 20, 100, vec![10]),
        fake_file(2, 20, 30, 200, vec![20, 21]),
    ];

    let result = query_index_files(&files, "Topic#Key", 2, 0, 40);

    assert_eq!(result.get_phy_offsets(), &[20, 21]);
    assert_eq!(result.get_index_last_update_timestamp(), 30);
    assert_eq!(result.get_index_last_update_phyoffset(), 200);
    assert_eq!(total_index_file_size(&files), 200);
    assert_eq!(max_index_dispatch_offset(&files), Some(200));
}

#[test]
fn load_and_expiration_plans_keep_safe_offset_and_newest_file_rules() {
    let mut files = vec![fake_file(1, 10, 20, 100, vec![]), fake_file(2, 20, 30, 200, vec![])];
    assert_eq!(expired_index_file_count(&files, 150), 1);
    assert_eq!(restore_index_safe_offset(&files, 250, false), 250);
    assert_eq!(restore_index_safe_offset(&files, 250, true), 200);
    assert!(should_remove_unsafe_index_file(false, 31, 30));
    assert!(!should_remove_unsafe_index_file(true, 31, 30));

    let mut shutdown_ids = Vec::new();
    shutdown_index_files(&mut files, |file| shutdown_ids.push(file.id));
    assert_eq!(shutdown_ids, [1, 2]);
    assert!(files.is_empty());

    let mut files = vec![fake_file(3, 30, 40, 300, vec![])];
    let mut destroy_ids = Vec::new();
    destroy_index_files(&mut files, |file| destroy_ids.push(file.id));
    assert_eq!(destroy_ids, [3]);
    assert!(files.is_empty());
}

#[test]
fn creation_retry_rollover_and_flush_plans_preserve_legacy_order() {
    let reusable = fake_file(1, 10, 20, 100, vec![]);
    let seed = plan_last_index_file(std::slice::from_ref(&reusable));
    assert_eq!(seed.reusable, Some(reusable.clone()));
    assert_eq!(seed.previous_full, None);

    let mut full = reusable;
    full.full = true;
    let seed = plan_last_index_file(std::slice::from_ref(&full));
    assert_eq!(seed.previous_full, Some(full.clone()));
    assert_eq!(seed.previous_end_phy_offset, 100);
    assert_eq!(seed.previous_end_timestamp, 20);
    assert_eq!(index_flush_checkpoint_timestamp(&full), Some(20));

    let mut attempts = 0;
    let mut waits = Vec::new();
    let created = retry_index_file_create(
        3,
        || {
            attempts += 1;
            (attempts == 3).then_some(7)
        },
        |attempt, maximum| waits.push((attempt, maximum)),
    );
    assert_eq!(created, Some(7));
    assert_eq!(waits, [(1, 3), (2, 3)]);

    let mut put_attempts = Vec::new();
    let written = drive_index_service_put(
        1,
        |file| {
            put_attempts.push(*file);
            *file == 3
        },
        |file| Some(*file + 1),
    );
    assert_eq!(written, Some(3));
    assert_eq!(put_attempts, [1, 2, 3]);
}

#[test]
fn build_preflight_and_key_driver_preserve_skip_and_key_order() {
    assert_eq!(
        plan_index_build(true, true, 100, 20, None),
        IndexBuildPreflight::AdvanceSafeOffset(Some(120))
    );
    assert_eq!(
        plan_index_build(false, false, -1, 20, None),
        IndexBuildPreflight::AdvanceSafeOffset(None)
    );
    assert_eq!(
        plan_index_build(false, true, 99, 20, Some(100)),
        IndexBuildPreflight::SkipOldOffset
    );
    assert_eq!(
        plan_index_build(false, true, 100, 20, Some(100)),
        IndexBuildPreflight::Build
    );
    assert!(has_indexable_keys(None, "k1 k2", None, " "));
    assert!(!has_indexable_keys(None, "  ", None, " "));

    let mut visited = Vec::new();
    let outcome = drive_index_build_keys(
        Some("uniq"),
        "k1  k2",
        Some("tag"),
        " ",
        "U",
        "T",
        |kind, key, index_type| {
            visited.push((kind, key.to_string(), index_type.map(str::to_string)));
            true
        },
    );
    assert_eq!(outcome, IndexBuildKeysOutcome::Completed);
    assert_eq!(
        visited,
        [
            (IndexBuildKeyKind::Unique, "uniq".into(), Some("U".into())),
            (IndexBuildKeyKind::Normal, "k1".into(), None),
            (IndexBuildKeyKind::Normal, "k2".into(), None),
            (IndexBuildKeyKind::Tag, "tag".into(), Some("T".into())),
        ]
    );

    let outcome = drive_index_build_keys(None, "key", Some("tag"), " ", "U", "T", |kind, _, _| {
        kind != IndexBuildKeyKind::Tag
    });
    assert_eq!(outcome, IndexBuildKeysOutcome::TagFailed);
    assert!(outcome.advances_safe_offset());
}

#[test]
fn key_builders_reuse_exact_capacity_and_format() {
    assert_eq!(build_index_key("Topic", "key"), "Topic#key");
    assert_eq!(build_index_key_with_type("Topic", "tag", "T"), "Topic#T#tag");
    let mut buffer = String::new();
    assert_eq!(
        build_index_key_into(&mut buffer, "Topic", "uniq", Some("U")),
        "Topic#U#uniq"
    );
    assert_eq!(
        index_build_key_capacity("Topic", Some("uniq"), "a longer", Some("tag"), " ", "U", "T"),
        12
    );
}

#[test]
fn dispatch_root_gates_build_and_progress_with_one_adapter() {
    let root = IndexDispatchRoot::new(7);
    let mut request = 1;
    assert!(!root.dispatch(false, &mut request, |_, request| *request += 1));
    assert_eq!(request, 1);
    assert!(root.dispatch(true, &mut request, |adapter, request| *request += adapter));
    assert_eq!(request, 8);
    assert_eq!(root.progress(false, |_| Some(99)), None);
    assert_eq!(root.progress(true, |adapter| Some(*adapter as i64)), Some(7));
}

fn fake_file(id: usize, begin_timestamp: i64, end_timestamp: i64, end_phy_offset: i64, offsets: Vec<i64>) -> FakeFile {
    FakeFile {
        id,
        size: 100,
        entries: true,
        full: false,
        begin_timestamp,
        end_timestamp,
        end_phy_offset,
        offsets,
    }
}
