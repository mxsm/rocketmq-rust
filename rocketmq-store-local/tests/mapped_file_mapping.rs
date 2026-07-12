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

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Barrier;

use rocketmq_store_local::mapped_file::mapping::LazyMmapStats;
use rocketmq_store_local::mapped_file::mapping::MappedFileMapping;

#[test]
fn eager_mapping_starts_initialized_without_lazy_statistics() {
    let mapping = MappedFileMapping::new_eager(String::from("eager"));

    assert!(!mapping.is_lazy_enabled());
    assert!(mapping.is_mapped());
    assert_eq!(mapping.get().map(String::as_str), Some("eager"));
    assert_eq!(mapping.stats(), LazyMmapStats::default());
}

#[test]
fn lazy_mapping_starts_eligible_and_uninitialized() {
    let mapping = MappedFileMapping::<String>::new_lazy();

    assert!(mapping.is_lazy_enabled());
    assert!(!mapping.is_mapped());
    assert!(mapping.get().is_none());
    assert_eq!(
        mapping.stats(),
        LazyMmapStats {
            eligible_files: 1,
            mapped_files: 0,
            map_operations: 0,
            map_failures: 0,
            total_millis: 0,
            last_millis: 0,
        }
    );
}

#[test]
fn successful_lazy_initialization_records_one_operation() {
    let mapping = MappedFileMapping::new_lazy();

    let value = mapping
        .get_or_try_init(|| Ok::<_, &'static str>(String::from("mapped")))
        .expect("initializer succeeds");

    assert_eq!(value, "mapped");
    assert!(mapping.is_mapped());
    let stats = mapping.stats();
    assert_eq!(stats.eligible_files, 1);
    assert_eq!(stats.mapped_files, 1);
    assert_eq!(stats.map_operations, 1);
    assert_eq!(stats.map_failures, 0);
    assert_eq!(stats.total_millis, stats.last_millis);
}

#[test]
fn failed_lazy_initialization_is_retryable_and_records_each_failure() {
    let mapping = MappedFileMapping::new_lazy();
    let attempts = AtomicUsize::new(0);

    let first = mapping.get_or_try_init(|| {
        attempts.fetch_add(1, Ordering::SeqCst);
        Err::<usize, _>("first attempt failed")
    });
    assert_eq!(first, Err("first attempt failed"));
    assert!(!mapping.is_mapped());
    assert_eq!(mapping.stats().map_failures, 1);

    let value = mapping
        .get_or_try_init(|| {
            attempts.fetch_add(1, Ordering::SeqCst);
            Ok::<_, &'static str>(42)
        })
        .expect("retry succeeds");
    assert_eq!(*value, 42);
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    assert_eq!(mapping.stats().map_failures, 1);
    assert_eq!(mapping.stats().map_operations, 1);
}

#[test]
fn concurrent_lazy_callers_run_initializer_exactly_once() {
    const CALLERS: usize = 8;
    let mapping = Arc::new(MappedFileMapping::new_lazy());
    let start = Arc::new(Barrier::new(CALLERS + 1));
    let initializations = Arc::new(AtomicUsize::new(0));

    std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(CALLERS);
        for _ in 0..CALLERS {
            let mapping = Arc::clone(&mapping);
            let start = Arc::clone(&start);
            let initializations = Arc::clone(&initializations);
            handles.push(scope.spawn(move || {
                start.wait();
                mapping
                    .get_or_try_init(|| {
                        initializations.fetch_add(1, Ordering::SeqCst);
                        Ok::<_, &'static str>(17usize)
                    })
                    .map(|value| value as *const usize as usize)
            }));
        }

        start.wait();
        let addresses = handles
            .into_iter()
            .map(|handle| handle.join().expect("caller does not panic").expect("init succeeds"))
            .collect::<Vec<_>>();
        assert!(addresses.windows(2).all(|pair| pair[0] == pair[1]));
    });

    assert_eq!(initializations.load(Ordering::SeqCst), 1);
    assert_eq!(mapping.stats().map_operations, 1);
}

#[test]
fn initialized_value_identity_is_stable_across_reads() {
    let mapping = MappedFileMapping::new_lazy();
    let initialized = mapping
        .get_or_try_init(|| Ok::<_, &'static str>(Box::new(23usize)))
        .expect("init succeeds");
    let read = mapping.get().expect("mapping remains initialized");

    assert!(std::ptr::eq(initialized, read));
}

#[test]
fn statistics_are_monotonic_after_failures_and_success() {
    let mapping = MappedFileMapping::new_lazy();
    let initial = mapping.stats();
    let _ = mapping.get_or_try_init(|| Err::<usize, _>("failed"));
    let after_failure = mapping.stats();
    let _ = mapping
        .get_or_try_init(|| Ok::<_, &'static str>(1))
        .expect("retry succeeds");
    let after_success = mapping.stats();

    assert!(after_failure.map_failures >= initial.map_failures);
    assert!(after_success.map_failures >= after_failure.map_failures);
    assert!(after_success.map_operations >= after_failure.map_operations);
    assert!(after_success.total_millis >= after_failure.total_millis);
}
