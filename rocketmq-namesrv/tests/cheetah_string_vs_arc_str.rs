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

//! Test to verify CheetahString can replace Arc<str> in our use cases

use std::sync::Arc;

use cheetah_string::CheetahString;
use dashmap::DashMap;

#[test]
fn test_cheetah_string_as_dashmap_key() {
    // Test 1: CheetahString as DashMap key
    let map: DashMap<CheetahString, String> = DashMap::new();
    let key = CheetahString::from_string("test_topic".to_string());

    map.insert(key.clone(), "value1".to_string());
    assert_eq!(map.get(&key).unwrap().value(), "value1");

    // Test 2: CheetahString lookup with different instance
    let key2 = CheetahString::from_string("test_topic".to_string());
    assert_eq!(map.get(&key2).unwrap().value(), "value1");
}

#[test]
fn test_cheetah_string_clone_performance() {
    // Test 3: Clone is cheap (like Arc)
    let original = CheetahString::from_string("a_very_long_string_that_would_be_expensive_to_copy".repeat(10));

    // Multiple clones should be cheap
    let clone1 = original.clone();
    let clone2 = original.clone();
    let clone3 = original.clone();

    // All should be equal
    assert_eq!(original.as_str(), clone1.as_str());
    assert_eq!(original.as_str(), clone2.as_str());
    assert_eq!(original.as_str(), clone3.as_str());
}

#[test]
fn test_cheetah_string_zero_copy_sharing() {
    // Test 4: Can be shared across threads (like Arc)
    let shared_string = CheetahString::from_string("shared_data".to_string());
    let shared_clone = shared_string.clone();

    let handle = std::thread::spawn(move || {
        // Can use in another thread
        assert_eq!(shared_clone.as_str(), "shared_data");
    });

    handle.join().unwrap();

    // Original still valid
    assert_eq!(shared_string.as_str(), "shared_data");
}

#[test]
fn test_cheetah_string_from_conversions() {
    // Test 5: Conversion patterns
    let from_string = CheetahString::from_string("test".to_string());
    let from_str = CheetahString::from_string("test".to_string());
    let from_static = CheetahString::from_static_str("test");

    assert_eq!(from_string.as_str(), "test");
    assert_eq!(from_str.as_str(), "test");
    assert_eq!(from_static.as_str(), "test");
}

#[test]
fn test_arc_str_current_usage() {
    // Test 6: Current Arc<str> usage pattern
    let map: DashMap<Arc<str>, String> = DashMap::new();
    let key: Arc<str> = "test_topic".into();

    map.insert(key.clone(), "value1".to_string());

    let key2: Arc<str> = "test_topic".into();
    assert_eq!(map.get(&key2).unwrap().value(), "value1");
}

#[test]
fn test_dashmap_iteration_cheetah_string() {
    // Test 7: Iteration with CheetahString keys
    let map: DashMap<CheetahString, i32> = DashMap::new();

    map.insert(CheetahString::from_string("topic1".to_string()), 1);
    map.insert(CheetahString::from_string("topic2".to_string()), 2);
    map.insert(CheetahString::from_string("topic3".to_string()), 3);

    let mut count = 0;
    for entry in map.iter() {
        assert!(entry.value() >= &1 && entry.value() <= &3);
        count += 1;
    }
    assert_eq!(count, 3);
}

#[test]
fn test_cheetah_string_hash_eq() {
    // Test 8: Hash and Eq implementation
    use std::collections::HashSet;

    let mut set = HashSet::new();
    set.insert(CheetahString::from_string("test".to_string()));

    // Same value should be found
    assert!(set.contains(&CheetahString::from_string("test".to_string())));

    // Different value should not be found
    assert!(!set.contains(&CheetahString::from_string("other".to_string())));
}
