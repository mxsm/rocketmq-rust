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

//! Lock-free state of the committed-read gate, shared by production code and the Loom model.
//!
//! One word holds three things: whether committed reads are enabled for the file, whether a rewrite
//! has closed the gate, and how many readers are inside. Every change to the word is a
//! read-modify-write, so the release made by each reader's exit stays in the release sequence that a
//! draining rewrite later acquires.

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

/// Minimal atomic facade so the Loom model checks the same transitions with its own atomics.
pub(crate) trait CommittedReadAtomic: Send + Sync {
    fn new(value: usize) -> Self;

    fn load_acquire(&self) -> usize;

    fn fetch_add_acquire(&self, value: usize) -> usize;

    fn fetch_or_acq_rel(&self, bits: usize) -> usize;

    fn fetch_and_release(&self, bits: usize) -> usize;

    fn fetch_sub_release(&self, value: usize) -> usize;
}

impl CommittedReadAtomic for AtomicUsize {
    #[inline]
    fn new(value: usize) -> Self {
        AtomicUsize::new(value)
    }

    #[inline]
    fn load_acquire(&self) -> usize {
        self.load(Ordering::Acquire)
    }

    #[inline]
    fn fetch_add_acquire(&self, value: usize) -> usize {
        self.fetch_add(value, Ordering::Acquire)
    }

    #[inline]
    fn fetch_or_acq_rel(&self, bits: usize) -> usize {
        self.fetch_or(bits, Ordering::AcqRel)
    }

    #[inline]
    fn fetch_and_release(&self, bits: usize) -> usize {
        self.fetch_and(bits, Ordering::Release)
    }

    #[inline]
    fn fetch_sub_release(&self, value: usize) -> usize {
        self.fetch_sub(value, Ordering::Release)
    }
}

const ENABLED: usize = 1 << (usize::BITS - 1);
const CLOSED: usize = 1 << (usize::BITS - 2);
const READERS: usize = CLOSED - 1;

/// Outcome of one attempt to enter the gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CommittedReadEntry {
    /// The reader is inside; it must call [`CommittedReadState::exit`] exactly once.
    Entered,
    /// Committed reads were never enabled for this file.
    Disabled,
    /// A rewrite has closed the gate.
    Closed,
    /// The reader count is at its maximum.
    Saturated,
}

/// Gate word for one mapped file.
///
/// These rules, kept by the callers, are what make the word sufficient:
///
/// - `enable` runs only under the file's writer lock and the bit never clears. A rewrite that sees
///   the gate disabled while holding that lock knows no reader holds committed bytes or can enter
///   before it releases the lock, so it changes bytes without the gate.
/// - Otherwise the rewrite calls `close`, waits until no reader is inside, then makes its change
///   under the writer lock and calls `reopen`. Rewrites of one file are serialized, so the closed
///   bit has a single owner.
/// - A reader enters before it loads the read position. A rewrite that finished before the entry
///   released its lowered positions through `reopen`, which the entry acquires; a rewrite that
///   closes after the entry waits for the matching `exit`.
#[derive(Debug)]
pub(crate) struct CommittedReadState<A: CommittedReadAtomic = AtomicUsize> {
    word: A,
}

impl<A: CommittedReadAtomic> CommittedReadState<A> {
    #[inline]
    pub(crate) fn new() -> Self {
        Self { word: A::new(0) }
    }

    #[inline]
    pub(crate) fn is_enabled(&self) -> bool {
        self.word.load_acquire() & ENABLED != 0
    }

    /// Enables committed reads. The caller holds the file's writer lock.
    #[inline]
    pub(crate) fn enable(&self) {
        self.word.fetch_or_acq_rel(ENABLED);
    }

    /// Counts the reader in with one read-modify-write, then backs out if the gate turned out to
    /// be disabled or closed, so contended readers never retry.
    ///
    /// A reader that backs out never touched committed bytes; a draining rewrite simply sees its
    /// short-lived count disappear.
    #[inline]
    pub(crate) fn try_enter(&self) -> CommittedReadEntry {
        let previous = self.word.fetch_add_acquire(1);
        let refused = if previous & ENABLED == 0 {
            CommittedReadEntry::Disabled
        } else if previous & CLOSED != 0 {
            CommittedReadEntry::Closed
        } else if previous & READERS >= READERS - 1 {
            CommittedReadEntry::Saturated
        } else {
            return CommittedReadEntry::Entered;
        };
        self.word.fetch_sub_release(1);
        refused
    }

    /// Leaves the gate after the reader's last use of committed bytes.
    ///
    /// Each call must match one [`CommittedReadEntry::Entered`].
    #[inline]
    pub(crate) fn exit(&self) {
        let previous = self.word.fetch_sub_release(1);
        debug_assert!(previous & READERS != 0, "committed-read exit without an entry");
    }

    /// Closes the gate before a rewrite. The caller is the file's only rewrite in progress.
    ///
    /// The caller then waits until [`Self::readers`] is zero before changing committed bytes and
    /// calls [`Self::reopen`] once its new positions are published. Closing a gate that is not yet
    /// enabled also keeps readers out, should one enable it meanwhile.
    #[inline]
    pub(crate) fn close(&self) {
        let previous = self.word.fetch_or_acq_rel(CLOSED);
        debug_assert!(previous & CLOSED == 0, "committed-read gate closed twice");
    }

    /// Returns the number of readers inside, acquiring the release of every exit it counts.
    #[inline]
    pub(crate) fn readers(&self) -> usize {
        self.word.load_acquire() & READERS
    }

    /// Reopens the gate after a rewrite published its positions.
    #[inline]
    pub(crate) fn reopen(&self) {
        self.word.fetch_and_release(!CLOSED);
    }
}

impl<A: CommittedReadAtomic> Default for CommittedReadState<A> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn readers_enter_only_after_enable_and_while_open() {
        let state = CommittedReadState::<AtomicUsize>::new();
        assert_eq!(state.try_enter(), CommittedReadEntry::Disabled);

        state.enable();
        assert_eq!(state.try_enter(), CommittedReadEntry::Entered);
        assert_eq!(state.try_enter(), CommittedReadEntry::Entered);
        assert_eq!(state.readers(), 2);

        state.close();
        assert_eq!(state.try_enter(), CommittedReadEntry::Closed);
        state.exit();
        state.exit();
        assert_eq!(state.readers(), 0);

        state.reopen();
        assert_eq!(state.try_enter(), CommittedReadEntry::Entered);
        assert_eq!(state.readers(), 1);
        assert!(state.is_enabled());
    }
}
