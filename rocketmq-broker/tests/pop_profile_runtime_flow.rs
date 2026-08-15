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

use rocketmq_broker::test_support::PopProfileStoreProbe;
use tempfile::TempDir;

#[test]
fn accepted_profile_becomes_visible_only_after_durable_upsert() {
    let root = TempDir::new().expect("temp dir");
    let store = PopProfileStoreProbe::open(root.path(), 8).expect("open profile store");

    assert!(store.snapshot().is_empty());
    store
        .upsert("group-a", &["topic-a", "%RETRY%group-a"], 2, 100)
        .expect("persist accepted profile");
    let restored = store.restore_compensation();
    assert_eq!(
        restored,
        vec![("group-a".into(), vec!["%RETRY%group-a".into(), "topic-a".into()])]
    );
}

#[test]
fn rejected_profile_never_creates_durable_or_compensated_state() {
    let root = TempDir::new().expect("temp dir");
    let store = PopProfileStoreProbe::open(root.path(), 8).expect("open profile store");

    assert!(store.upsert("", &["topic-a"], 1, 100).is_err());
    assert!(store.upsert("group-a", &[], 1, 100).is_err());
    assert!(store.snapshot().is_empty());
    assert!(store.restore_compensation().is_empty());
    assert_eq!(store.generation(), 0);
}
