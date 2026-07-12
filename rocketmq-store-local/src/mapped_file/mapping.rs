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

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::OnceLock;
use std::time::Instant;

use parking_lot::Mutex;

/// Aggregate statistics for lazy mapped-file initialization.
///
/// Eager mappings report the default value because they are not eligible for lazy initialization.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LazyMmapStats {
    /// Number of files configured for lazy mapping.
    pub eligible_files: u64,
    /// Number of eligible files whose mapping has been initialized.
    pub mapped_files: u64,
    /// Number of successful lazy initialization operations.
    pub map_operations: u64,
    /// Number of failed lazy initialization attempts.
    pub map_failures: u64,
    /// Total elapsed time of successful lazy initialization operations, in milliseconds.
    pub total_millis: u64,
    /// Elapsed time of the most recent successful lazy initialization, in milliseconds.
    pub last_millis: u64,
}

impl LazyMmapStats {
    /// Saturating-adds counters from `other` and retains its latest non-zero latency.
    pub fn saturating_add_assign(&mut self, other: Self) {
        self.eligible_files = self.eligible_files.saturating_add(other.eligible_files);
        self.mapped_files = self.mapped_files.saturating_add(other.mapped_files);
        self.map_operations = self.map_operations.saturating_add(other.map_operations);
        self.map_failures = self.map_failures.saturating_add(other.map_failures);
        self.total_millis = self.total_millis.saturating_add(other.total_millis);
        if other.last_millis != 0 {
            self.last_millis = other.last_millis;
        }
    }
}

/// Owns the deterministic eager/lazy initialization lifecycle for one mapped-file value.
///
/// The mapped value remains generic so callers retain ownership of the concrete mmap and its
/// compatibility wrapper. Lazy initialization is serialized, while already-initialized reads do
/// not acquire the initialization lock.
pub struct MappedFileMapping<M> {
    value: OnceLock<M>,
    init_lock: Mutex<()>,
    lazy_enabled: bool,
    map_operations: AtomicU64,
    map_failures: AtomicU64,
    total_millis: AtomicU64,
    last_millis: AtomicU64,
}

impl<M> MappedFileMapping<M> {
    /// Creates an eagerly initialized mapping.
    ///
    /// Eager mappings are not counted as lazy-eligible or lazily mapped files.
    pub fn new_eager(value: M) -> Self {
        Self {
            value: OnceLock::from(value),
            init_lock: Mutex::new(()),
            lazy_enabled: false,
            map_operations: AtomicU64::new(0),
            map_failures: AtomicU64::new(0),
            total_millis: AtomicU64::new(0),
            last_millis: AtomicU64::new(0),
        }
    }

    /// Creates an uninitialized mapping eligible for lazy initialization.
    pub fn new_lazy() -> Self {
        Self {
            value: OnceLock::new(),
            init_lock: Mutex::new(()),
            lazy_enabled: true,
            map_operations: AtomicU64::new(0),
            map_failures: AtomicU64::new(0),
            total_millis: AtomicU64::new(0),
            last_millis: AtomicU64::new(0),
        }
    }

    /// Returns whether this mapping was configured for lazy initialization.
    #[inline]
    pub fn is_lazy_enabled(&self) -> bool {
        self.lazy_enabled
    }

    /// Returns whether the mapping value has been initialized.
    #[inline]
    pub fn is_mapped(&self) -> bool {
        self.value.get().is_some()
    }

    /// Returns the initialized mapping value, if present.
    #[inline]
    pub fn get(&self) -> Option<&M> {
        self.value.get()
    }

    /// Returns a consistent snapshot of the lazy initialization statistics.
    pub fn stats(&self) -> LazyMmapStats {
        LazyMmapStats {
            eligible_files: u64::from(self.lazy_enabled),
            mapped_files: u64::from(self.lazy_enabled && self.is_mapped()),
            map_operations: self.map_operations.load(Ordering::Acquire),
            map_failures: self.map_failures.load(Ordering::Acquire),
            total_millis: self.total_millis.load(Ordering::Acquire),
            last_millis: self.last_millis.load(Ordering::Acquire),
        }
    }

    /// Returns the existing value or initializes it with `initializer` exactly once.
    ///
    /// Concurrent callers double-check the cell while holding the initialization lock, so only
    /// one successful initializer runs. A failed attempt increments the failure count, leaves the
    /// cell uninitialized, and permits a later caller to retry.
    ///
    /// # Errors
    ///
    /// Returns the initializer error without storing a value when initialization fails.
    pub fn get_or_try_init<E, F>(&self, initializer: F) -> Result<&M, E>
    where
        F: FnOnce() -> Result<M, E>,
    {
        if let Some(value) = self.value.get() {
            return Ok(value);
        }

        let _guard = self.init_lock.lock();
        if let Some(value) = self.value.get() {
            return Ok(value);
        }

        let start = Instant::now();
        match initializer() {
            Ok(value) => {
                let elapsed_millis = start.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
                if self.lazy_enabled {
                    self.map_operations.fetch_add(1, Ordering::AcqRel);
                    self.total_millis.fetch_add(elapsed_millis, Ordering::AcqRel);
                    self.last_millis.store(elapsed_millis, Ordering::Release);
                }
                Ok(self.value.get_or_init(|| value))
            }
            Err(error) => {
                if self.lazy_enabled {
                    self.map_failures.fetch_add(1, Ordering::AcqRel);
                }
                Err(error)
            }
        }
    }
}
