use rand::Rng;
use rocketmq_client_rust::common::thread_local_index::ThreadLocalIndex;
use std::cell::RefCell;
use std::time::Instant;

// Original implementation for comparison
thread_local! {
    static ORIGINAL_INDEX: RefCell<Option<i32>> = const {RefCell::new(None)};
}

const POSITIVE_MASK: i32 = 0x7FFFFFFF;
const MAX: i32 = i32::MAX;

#[derive(Default, Clone)]
pub struct OriginalThreadLocalIndex;

impl OriginalThreadLocalIndex {
    pub fn increment_and_get(&self) -> i32 {
        ORIGINAL_INDEX.with(|index| {
            let mut index = index.borrow_mut();
            let new_value = match *index {
                Some(val) => val.wrapping_add(1) & POSITIVE_MASK,
                None => rand::rng().random_range(0..=MAX) & POSITIVE_MASK,
            };
            *index = Some(new_value);
            new_value
        })
    }

    pub fn reset(&self) {
        let new_value = rand::rng().random_range(0..=MAX).abs();
        ORIGINAL_INDEX.with(|index| {
            *index.borrow_mut() = Some(new_value);
        });
    }
}

#[test]
fn test_thread_local_index_basic() {
    let idx = ThreadLocalIndex;
    let val1 = idx.increment_and_get();
    let val2 = idx.increment_and_get();

    // Values should be positive
    assert!(val1 >= 0);
    assert!(val2 >= 0);

    // Values should increment
    assert_ne!(val1, val2);
}

#[test]
fn test_thread_local_index_reset() {
    let idx = ThreadLocalIndex;
    let _ = idx.increment_and_get();
    idx.reset();
    let val = idx.increment_and_get();

    // Value after reset should still be positive
    assert!(val >= 0);
}

#[test]
fn test_performance_comparison() {
    println!("\nThreadLocalIndex Performance Comparison");
    println!("{}", "=".repeat(70));

    // Test 1: First call (cold start)
    println!("\nTest 1: First Call (includes RNG initialization)");
    println!("{}", "-".repeat(70));

    let iterations = 1000;

    // Original - first call
    let start = Instant::now();
    for _ in 0..iterations {
        ORIGINAL_INDEX.with(|idx| *idx.borrow_mut() = None);
        let _ = OriginalThreadLocalIndex.increment_and_get();
    }
    let original_first = start.elapsed();

    // Optimized - first call
    let start = Instant::now();
    for _ in 0..iterations {
        let idx = ThreadLocalIndex;
        idx.reset();
        let _ = idx.increment_and_get();
    }
    let optimized_first = start.elapsed();

    println!(
        "Original:  {:>10.2?} ({:.2} ns/iter)",
        original_first,
        original_first.as_nanos() as f64 / iterations as f64
    );
    println!(
        "Optimized: {:>10.2?} ({:.2} ns/iter)",
        optimized_first,
        optimized_first.as_nanos() as f64 / iterations as f64
    );
    println!(
        "Speedup:   {:.2}x faster",
        original_first.as_nanos() as f64 / optimized_first.as_nanos() as f64
    );

    // Test 2: Subsequent calls (hot path)
    println!("\nTest 2: Subsequent Calls (hot path)");
    println!("{}", "-".repeat(70));

    // Initialize
    let _ = OriginalThreadLocalIndex.increment_and_get();
    let _ = ThreadLocalIndex.increment_and_get();

    let iterations = 100_000;

    // Original - hot
    let start = Instant::now();
    for _ in 0..iterations {
        let _ = OriginalThreadLocalIndex.increment_and_get();
    }
    let original_hot = start.elapsed();

    // Optimized - hot
    let start = Instant::now();
    for _ in 0..iterations {
        let _ = ThreadLocalIndex.increment_and_get();
    }
    let optimized_hot = start.elapsed();

    println!(
        "Original:  {:>10.2?} ({:.2} ns/iter)",
        original_hot,
        original_hot.as_nanos() as f64 / iterations as f64
    );
    println!(
        "Optimized: {:>10.2?} ({:.2} ns/iter)",
        optimized_hot,
        optimized_hot.as_nanos() as f64 / iterations as f64
    );
    println!(
        "Speedup:   {:.2}x faster",
        original_hot.as_nanos() as f64 / optimized_hot.as_nanos() as f64
    );

    // Summary
    println!("\n{}", "=".repeat(70));
    println!("PERFORMANCE SUMMARY");
    println!("{}", "=".repeat(70));
    println!(
        "First Call:       {:.2}x faster",
        original_first.as_nanos() as f64 / optimized_first.as_nanos() as f64
    );
    println!(
        "Subsequent Calls: {:.2}x faster",
        original_hot.as_nanos() as f64 / optimized_hot.as_nanos() as f64
    );

    // Note: Performance assertions are removed to avoid flaky tests due to system noise.
    // The actual performance comparison is shown in the output above.
    // For reliable performance testing, use benchmarks in the benches/ directory.
}

#[test]
fn test_thread_safety() {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    let counter = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    for _ in 0..4 {
        let counter_clone = counter.clone();
        let handle = std::thread::spawn(move || {
            let idx = ThreadLocalIndex;
            for _ in 0..1000 {
                let val = idx.increment_and_get();
                assert!(val >= 0);
                counter_clone.fetch_add(1, Ordering::Relaxed);
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    assert_eq!(counter.load(Ordering::Relaxed), 4000);
}
