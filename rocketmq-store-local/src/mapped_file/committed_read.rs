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

//! Lets readers borrow committed bytes of a writable mapping without the writer lock.
//!
//! Appends write only at or above the read position, so the bytes below it do not change until a
//! rewrite lowers a position or writes below it: truncation, reset, a positioned write, warm-up.
//! A reader enters the gate, then loads the read position, and may keep slices of the bytes below
//! that position until it leaves.
//!
//! A rewrite first checks, under the writer lock, whether the gate is enabled and the change
//! reaches below the read position; only then does it need the gate. It releases the writer lock,
//! closes the gate, and waits for every reader to leave, then takes the writer lock again for the
//! change itself and reopens the gate once its new positions are published. Waiting without the
//! writer lock matters: a reader that still holds ranges, such as a pull assembling a batch, falls
//! back to the locked copy path while the gate is closed, and must be able to finish and drop its
//! ranges. Rewrites of one file are serialized by the gate, so its closed bit has one owner.
//!
//! The transitions live in [`super::committed_read_model`] so the Loom model checks the same code.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use parking_lot::Mutex;
use parking_lot::MutexGuard;

use super::committed_read_model::CommittedReadEntry;
use super::committed_read_model::CommittedReadState;

/// How long a rewrite waits for readers to drop committed ranges before it gives up unchanged.
///
/// Pull responses and similar readers hold ranges for the time it takes to answer one request, so a
/// range still held after this long belongs to a stalled connection or a leak; the rewrite reports
/// it and its caller retries.
pub(crate) const COMMITTED_READ_DRAIN_TIMEOUT: Duration = Duration::from_secs(3);

/// Committed-read gate of one mapped file.
#[derive(Debug, Default)]
pub(crate) struct CommittedReadGate {
    state: CommittedReadState,
    /// Serializes rewrites. Taken before the writer lock, never while holding it.
    rewrites: Mutex<()>,
}

/// Result of asking to enter the gate.
pub(crate) enum CommittedReadAdmission {
    Entered(CommittedReadGuard),
    /// Committed reads were never enabled for this file.
    Disabled,
    /// A rewrite is in progress, or the reader count is saturated.
    Busy,
}

/// A reader inside the gate. Dropping it leaves the gate.
///
/// It must outlive every slice of committed bytes obtained after entering.
#[must_use = "dropping the guard immediately lets rewrites change the committed bytes"]
pub(crate) struct CommittedReadGuard {
    gate: Arc<CommittedReadGate>,
}

impl Drop for CommittedReadGuard {
    fn drop(&mut self) {
        self.gate.state.exit();
    }
}

impl fmt::Debug for CommittedReadGuard {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CommittedReadGuard")
    }
}

/// A rewrite in progress: no reader holds committed bytes and none can enter until it is dropped.
#[must_use = "dropping the window reopens the gate before the rewrite has run"]
pub(crate) struct RewriteWindow<'a> {
    gate: &'a CommittedReadGate,
    _serial: MutexGuard<'a, ()>,
}

impl Drop for RewriteWindow<'_> {
    fn drop(&mut self) {
        // Reopens before `_serial` is released, so the next rewrite finds the gate open.
        self.gate.state.reopen();
    }
}

/// Readers still held committed ranges when the drain timeout expired.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("{readers} committed-read ranges were still held after {waited:?}")]
pub(crate) struct CommittedReadsBusy {
    pub(crate) readers: usize,
    pub(crate) waited: Duration,
}

impl CommittedReadGate {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Enables committed reads. The caller holds the file's writer lock.
    #[inline]
    pub(crate) fn enable(&self) {
        self.state.enable();
    }

    /// Returns whether committed reads are enabled. Stable while the writer lock is held.
    #[inline]
    pub(crate) fn is_enabled(&self) -> bool {
        self.state.is_enabled()
    }

    #[inline]
    pub(crate) fn try_enter(self: &Arc<Self>) -> CommittedReadAdmission {
        match self.state.try_enter() {
            CommittedReadEntry::Entered => {
                CommittedReadAdmission::Entered(CommittedReadGuard { gate: Arc::clone(self) })
            }
            CommittedReadEntry::Disabled => CommittedReadAdmission::Disabled,
            CommittedReadEntry::Closed | CommittedReadEntry::Saturated => CommittedReadAdmission::Busy,
        }
    }

    /// Opens a rewrite window. The caller must not hold the file's writer lock: readers that hold
    /// ranges may need it to finish. It takes the writer lock inside the window for the change.
    ///
    /// Waits up to `timeout` for readers to leave. The wait sleeps in one-millisecond steps once
    /// spinning and yielding have not drained the readers; rewrites are rare maintenance operations
    /// that already block on file work.
    ///
    /// # Errors
    ///
    /// Returns [`CommittedReadsBusy`] after reopening the gate when readers remain at the deadline.
    pub(crate) fn begin_rewrite(&self, timeout: Duration) -> Result<RewriteWindow<'_>, CommittedReadsBusy> {
        let serial = self.rewrites.lock();
        self.state.close();
        let window = RewriteWindow {
            gate: self,
            _serial: serial,
        };
        let started = Instant::now();
        let mut attempts = 0_u32;
        loop {
            let readers = self.state.readers();
            if readers == 0 {
                return Ok(window);
            }
            let waited = started.elapsed();
            if waited >= timeout {
                return Err(CommittedReadsBusy { readers, waited });
            }
            if attempts < 64 {
                std::hint::spin_loop();
            } else if attempts < 128 {
                std::thread::yield_now();
            } else {
                std::thread::sleep(Duration::from_millis(1));
            }
            attempts = attempts.saturating_add(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entered(gate: &Arc<CommittedReadGate>) -> CommittedReadGuard {
        match gate.try_enter() {
            CommittedReadAdmission::Entered(guard) => guard,
            CommittedReadAdmission::Disabled => panic!("gate is disabled"),
            CommittedReadAdmission::Busy => panic!("gate is busy"),
        }
    }

    #[test]
    fn a_closed_gate_keeps_out_readers_that_enable_it_meanwhile() {
        let gate = Arc::new(CommittedReadGate::new());
        assert!(matches!(gate.try_enter(), CommittedReadAdmission::Disabled));
        let window = gate.begin_rewrite(Duration::ZERO).expect("nothing to drain");
        gate.enable();
        assert!(matches!(gate.try_enter(), CommittedReadAdmission::Busy));
        drop(window);
        drop(entered(&gate));
    }

    #[test]
    fn rewrite_waits_for_held_ranges_and_reopens_when_it_gives_up() {
        let gate = Arc::new(CommittedReadGate::new());
        gate.enable();
        let held = entered(&gate);

        let busy = gate
            .begin_rewrite(Duration::from_millis(5))
            .err()
            .expect("a held range blocks the rewrite");
        assert_eq!(busy.readers, 1);
        let reader = entered(&gate);
        drop(reader);

        drop(held);
        let window = gate.begin_rewrite(Duration::ZERO).expect("no readers remain");
        assert!(
            matches!(gate.try_enter(), CommittedReadAdmission::Busy),
            "readers stay out while the rewrite runs"
        );
        drop(window);
        drop(entered(&gate));
    }

    #[test]
    fn rewrite_completes_once_another_thread_drops_its_range() {
        let gate = Arc::new(CommittedReadGate::new());
        gate.enable();
        let held = entered(&gate);
        let (release, released) = std::sync::mpsc::channel::<()>();
        let reader = std::thread::spawn(move || {
            released.recv().expect("release signal");
            drop(held);
        });
        release.send(()).expect("reader alive");
        let window = gate
            .begin_rewrite(Duration::from_secs(30))
            .expect("the reader drops its range");
        drop(window);
        reader.join().expect("reader thread");
    }
}
