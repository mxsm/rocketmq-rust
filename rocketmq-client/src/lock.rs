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

use std::hint::spin_loop;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicI32;
use std::sync::atomic::Ordering;

/// Java-compatible CAS read/write lock used by producer backpressure paths.
///
/// Java's `ReadWriteCASLock` is a small spin lock backed by one `AtomicBoolean`
/// for writer admission and one `AtomicInteger` for active readers. This Rust
/// implementation intentionally mirrors those semantics instead of wrapping a
/// fair `RwLock`, because Java exposes `getWriteLock` and `getReadLock` as
/// direct state probes.
#[derive(Debug)]
pub struct ReadWriteCASLock {
    write_lock: AtomicBool,
    read_lock: AtomicI32,
}

impl Default for ReadWriteCASLock {
    fn default() -> Self {
        Self::new()
    }
}

impl ReadWriteCASLock {
    pub fn new() -> Self {
        Self {
            write_lock: AtomicBool::new(true),
            read_lock: AtomicI32::new(0),
        }
    }

    pub fn acquire_write_lock(&self) {
        while self
            .write_lock
            .compare_exchange(true, false, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            spin_loop();
        }

        while self.read_lock.load(Ordering::Acquire) != 0 {
            spin_loop();
        }
    }

    pub fn release_write_lock(&self) {
        let _ = self
            .write_lock
            .compare_exchange(false, true, Ordering::Release, Ordering::Relaxed);
    }

    pub fn acquire_read_lock(&self) {
        while !self.write_lock.load(Ordering::Acquire) {
            spin_loop();
        }
        self.read_lock.fetch_add(1, Ordering::AcqRel);
    }

    pub fn release_read_lock(&self) {
        self.read_lock.fetch_sub(1, Ordering::AcqRel);
    }

    pub fn get_write_lock(&self) -> bool {
        self.write_lock.load(Ordering::Acquire) && self.read_lock.load(Ordering::Acquire) == 0
    }

    pub fn get_read_lock(&self) -> bool {
        self.write_lock.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;
    use std::sync::Barrier;
    use std::thread;
    use std::time::Duration;

    use super::*;

    #[test]
    fn read_write_cas_lock_state_matches_java_accessors() {
        let lock = ReadWriteCASLock::new();
        assert!(lock.get_write_lock());
        assert!(lock.get_read_lock());

        lock.acquire_read_lock();
        assert!(!lock.get_write_lock());
        assert!(lock.get_read_lock());

        lock.release_read_lock();
        assert!(lock.get_write_lock());
        assert!(lock.get_read_lock());

        lock.acquire_write_lock();
        assert!(!lock.get_write_lock());
        assert!(!lock.get_read_lock());

        lock.release_write_lock();
        assert!(lock.get_write_lock());
        assert!(lock.get_read_lock());
    }

    #[test]
    fn write_lock_waits_for_active_readers_like_java() {
        let lock = Arc::new(ReadWriteCASLock::new());
        lock.acquire_read_lock();

        let writer_started = Arc::new(Barrier::new(2));
        let writer_acquired = Arc::new(AtomicBool::new(false));
        let writer_lock = Arc::clone(&lock);
        let writer_started_inner = Arc::clone(&writer_started);
        let writer_acquired_inner = Arc::clone(&writer_acquired);

        let handle = thread::spawn(move || {
            writer_started_inner.wait();
            writer_lock.acquire_write_lock();
            writer_acquired_inner.store(true, Ordering::Release);
            writer_lock.release_write_lock();
        });

        writer_started.wait();
        thread::sleep(Duration::from_millis(20));
        assert!(!writer_acquired.load(Ordering::Acquire));

        lock.release_read_lock();
        handle.join().expect("writer thread panicked");
        assert!(writer_acquired.load(Ordering::Acquire));
        assert!(lock.get_write_lock());
    }
}
