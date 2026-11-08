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

//! A sum counter that stays cheap when many threads add to it at once.

use std::cell::Cell;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::LazyLock;
use std::sync::OnceLock;

/// Upper bound on cells per counter, so a counter never grows past a few KiB.
const MAX_CELLS: usize = 64;

/// Cells allocated when a counter first sees contention: the CPU count rounded up to a power of
/// two, so a thread's cell index is a mask.
static CELL_COUNT: LazyLock<usize> = LazyLock::new(|| {
    std::thread::available_parallelism()
        .map_or(4, usize::from)
        .next_power_of_two()
        .min(MAX_CELLS)
});

static NEXT_THREAD_SLOT: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    /// Round-robin slot of the current thread, assigned on first use.
    static THREAD_SLOT: Cell<Option<usize>> = const { Cell::new(None) };
}

#[inline]
fn thread_slot() -> usize {
    THREAD_SLOT.with(|slot| match slot.get() {
        Some(index) => index,
        None => {
            let index = NEXT_THREAD_SLOT.fetch_add(1, Ordering::Relaxed);
            slot.set(Some(index));
            index
        }
    })
}

/// One counter word on its own pair of cache lines, so that neighbouring cells and the
/// adjacent-line prefetcher do not make writers on different cores share a line.
#[repr(align(128))]
#[derive(Default)]
struct PaddedCounter(AtomicU64);

/// A wrapping `u64` sum split into per-thread cells under contention.
///
/// This follows Java's `LongAdder`, which RocketMQ's Java `StatsItem` uses: an uncontended
/// writer adds to one base word with a single compare-and-swap. The first failed swap allocates
/// padded cells, and from then on each thread adds to its own cell. Counters that are never
/// written concurrently therefore keep the size of a single word plus one pointer.
///
/// [`Self::sum`] is not an atomic snapshot: additions that race with it may or may not be
/// included, which is the same guarantee the previous pair of relaxed atomics gave.
#[derive(Default)]
pub(crate) struct StripedCounter {
    base: AtomicU64,
    cells: OnceLock<Box<[PaddedCounter]>>,
}

impl StripedCounter {
    /// Adds `delta`, wrapping on overflow.
    #[inline]
    pub(crate) fn add(&self, delta: u64) {
        if delta == 0 {
            return;
        }
        if let Some(cells) = self.cells.get() {
            Self::add_to_cell(cells, delta);
            return;
        }
        let current = self.base.load(Ordering::Relaxed);
        if self
            .base
            .compare_exchange(
                current,
                current.wrapping_add(delta),
                Ordering::Relaxed,
                Ordering::Relaxed,
            )
            .is_err()
        {
            let cells = self
                .cells
                .get_or_init(|| (0..*CELL_COUNT).map(|_| PaddedCounter::default()).collect());
            Self::add_to_cell(cells, delta);
        }
    }

    #[inline]
    fn add_to_cell(cells: &[PaddedCounter], delta: u64) {
        // `cells.len()` is a power of two, so the mask selects a valid index.
        cells[thread_slot() & (cells.len() - 1)]
            .0
            .fetch_add(delta, Ordering::Relaxed);
    }

    /// Returns the current sum of every addition.
    pub(crate) fn sum(&self) -> u64 {
        let base = self.base.load(Ordering::Relaxed);
        self.cells.get().map_or(base, |cells| {
            cells
                .iter()
                .fold(base, |total, cell| total.wrapping_add(cell.0.load(Ordering::Relaxed)))
        })
    }

    #[cfg(test)]
    fn is_striped(&self) -> bool {
        self.cells.get().is_some()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Barrier;

    use super::*;

    #[test]
    fn uncontended_additions_stay_in_the_base_word() {
        let counter = StripedCounter::default();
        counter.add(5);
        counter.add(0);
        counter.add(7);

        assert_eq!(counter.sum(), 12);
        assert!(!counter.is_striped());
    }

    #[test]
    fn concurrent_additions_are_all_counted() {
        const THREADS: usize = 8;
        const ADDS: u64 = 20_000;
        let counter = Arc::new(StripedCounter::default());
        let start = Arc::new(Barrier::new(THREADS));
        let workers = (0..THREADS)
            .map(|_| {
                let counter = Arc::clone(&counter);
                let start = Arc::clone(&start);
                std::thread::spawn(move || {
                    start.wait();
                    for _ in 0..ADDS {
                        counter.add(3);
                    }
                })
            })
            .collect::<Vec<_>>();
        for worker in workers {
            worker.join().expect("counter worker");
        }

        assert_eq!(counter.sum(), THREADS as u64 * ADDS * 3);
    }

    #[test]
    fn striped_sum_includes_the_base_and_every_cell() {
        let counter = StripedCounter::default();
        counter.add(10);
        let cells = counter
            .cells
            .get_or_init(|| (0..*CELL_COUNT).map(|_| PaddedCounter::default()).collect());
        cells[0].0.fetch_add(4, Ordering::Relaxed);
        counter.add(6);

        assert!(counter.is_striped());
        assert_eq!(counter.sum(), 20);
    }

    #[test]
    fn sum_wraps_like_the_previous_atomic() {
        let counter = StripedCounter::default();
        counter.add(u64::MAX);
        counter.add(2);

        assert_eq!(counter.sum(), 1);
    }
}
