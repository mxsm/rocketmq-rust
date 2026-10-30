// Copyright 2026 The RocketMQ Rust Authors
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

//! Typed recall of the `native` engine.
//!
//! A recall is registered in the cancel set before its tombstone is written, so a batch taken
//! between the two still sees it, and the registration tells whether the timer can still be
//! stopped. The tombstone, appended to the Timer Topic as Java writes it, is what makes the
//! recall durable and reaches the slaves; once indexed it replaces the registration.

use super::NativeRecallVerdict;
use super::NativeRejection;
use super::NativeTimerEngine;
use crate::timer::native::cancel_set::NativeRecall;

impl NativeTimerEngine {
    /// Registers a recall of the timer due at `due_ms` that `key` names for `generation`, ahead
    /// of its tombstone. `key` takes any form a tombstone's key takes; delivery checks it
    /// against the timer's message.
    ///
    /// The timer can still be stopped only when it is due after `now_ms` and after every
    /// deadline a lane may have taken timers up to, which is decided under the lock that taking
    /// timers holds.
    ///
    /// # Errors
    ///
    /// Returns the rejection the engine would give a timer due at `due_ms`: an invalid
    /// deadline, or one past the maximum delay. Nothing is registered then.
    pub(crate) fn register_recall(
        &self,
        key: &[u8],
        generation: u64,
        due_ms: i64,
        now_ms: i64,
    ) -> Result<NativeRecallVerdict, NativeRejection> {
        self.admit(due_ms, now_ms)?;
        let mut state = self.state.lock();
        if due_ms <= now_ms.max(state.taken_until_ms) {
            return Ok(NativeRecallVerdict::TooLate);
        }
        state.cancels.hold(NativeRecall {
            key: key.to_vec(),
            generation,
        });
        Ok(NativeRecallVerdict::Cancelled)
    }

    /// Withdraws a recall registered as [`NativeRecallVerdict::Cancelled`] whose tombstone could
    /// not be written. The recall stays while its tombstone is indexed or another registration
    /// holds it.
    pub(crate) fn withdraw_recall(&self, key: &[u8], generation: u64) {
        self.state.lock().cancels.release(&NativeRecall {
            key: key.to_vec(),
            generation,
        });
    }
}
