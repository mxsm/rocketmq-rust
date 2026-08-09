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

use std::collections::BTreeSet;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CompletionDisposition {
    Advanced { durable_through: u64 },
    BufferedGap { durable_through: u64 },
    Duplicate { durable_through: u64 },
    StaleEpoch,
    GapLimitReached,
}

#[derive(Debug)]
pub(crate) struct OrderedCompletionTracker {
    epoch: u64,
    durable_through: u64,
    observed_through: u64,
    max_gaps: usize,
    completed: BTreeSet<u64>,
}

impl OrderedCompletionTracker {
    pub(crate) fn new(epoch: u64, durable_through: u64, max_gaps: usize) -> Self {
        Self {
            epoch,
            durable_through,
            observed_through: durable_through,
            max_gaps: max_gaps.max(1),
            completed: BTreeSet::new(),
        }
    }

    pub(crate) const fn epoch(&self) -> u64 {
        self.epoch
    }

    pub(crate) const fn durable_through(&self) -> u64 {
        self.durable_through
    }

    pub(crate) const fn observed_through(&self) -> u64 {
        self.observed_through
    }

    pub(crate) fn gap_count(&self) -> usize {
        usize::try_from(self.observed_through.saturating_sub(self.durable_through)).unwrap_or(usize::MAX)
    }

    pub(crate) fn can_accept(&self) -> bool {
        self.gap_count() < self.max_gaps
    }

    pub(crate) fn observe_pending(&mut self, epoch: u64, sequence: u64) -> CompletionDisposition {
        if epoch != self.epoch {
            return CompletionDisposition::StaleEpoch;
        }
        self.observed_through = self.observed_through.max(sequence);
        if self.gap_count() > self.max_gaps {
            CompletionDisposition::GapLimitReached
        } else {
            CompletionDisposition::BufferedGap {
                durable_through: self.durable_through,
            }
        }
    }

    pub(crate) fn commit_prefix(&mut self, epoch: u64, sequence: u64) -> CompletionDisposition {
        if epoch != self.epoch {
            return CompletionDisposition::StaleEpoch;
        }
        self.observed_through = self.observed_through.max(sequence);
        if sequence <= self.durable_through {
            return CompletionDisposition::Duplicate {
                durable_through: self.durable_through,
            };
        }
        // A durable event represents the storage engine's global checkpoint, not one isolated
        // worker result. The caller may therefore jump over observed sequences only after that
        // checkpoint has made the complete prefix through `sequence` crash-safe.
        self.durable_through = sequence;
        self.completed.retain(|completed| *completed > sequence);
        CompletionDisposition::Advanced {
            durable_through: sequence,
        }
    }

    pub(crate) fn observe(&mut self, epoch: u64, sequence: u64) -> CompletionDisposition {
        if epoch != self.epoch {
            return CompletionDisposition::StaleEpoch;
        }
        self.observed_through = self.observed_through.max(sequence);
        if sequence <= self.durable_through || self.completed.contains(&sequence) {
            return CompletionDisposition::Duplicate {
                durable_through: self.durable_through,
            };
        }
        if sequence > self.durable_through.saturating_add(1) && self.completed.len() >= self.max_gaps {
            return CompletionDisposition::GapLimitReached;
        }
        self.completed.insert(sequence);
        let previous = self.durable_through;
        while self.completed.remove(&self.durable_through.saturating_add(1)) {
            self.durable_through = self.durable_through.saturating_add(1);
        }
        if self.durable_through > previous {
            CompletionDisposition::Advanced {
                durable_through: self.durable_through,
            }
        } else {
            CompletionDisposition::BufferedGap {
                durable_through: self.durable_through,
            }
        }
    }

    pub(crate) fn reset_epoch(&mut self, epoch: u64, durable_through: u64) {
        self.epoch = epoch;
        self.durable_through = durable_through;
        self.observed_through = durable_through;
        self.completed.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timer_out_of_order_completion_never_crosses_the_first_gap() {
        let mut tracker = OrderedCompletionTracker::new(3, 9, 8);
        assert_eq!(
            tracker.observe(3, 10),
            CompletionDisposition::Advanced { durable_through: 10 }
        );
        assert_eq!(
            tracker.observe(3, 12),
            CompletionDisposition::BufferedGap { durable_through: 10 }
        );
        assert_eq!(
            tracker.observe(3, 11),
            CompletionDisposition::Advanced { durable_through: 12 }
        );
    }

    #[test]
    fn timer_gap_memory_and_role_epoch_are_bounded() {
        let mut tracker = OrderedCompletionTracker::new(1, 0, 2);
        assert!(matches!(
            tracker.observe(1, 2),
            CompletionDisposition::BufferedGap { .. }
        ));
        assert!(matches!(
            tracker.observe(1, 3),
            CompletionDisposition::BufferedGap { .. }
        ));
        assert_eq!(tracker.observe(1, 4), CompletionDisposition::GapLimitReached);
        assert_eq!(tracker.observe(2, 1), CompletionDisposition::StaleEpoch);
        tracker.reset_epoch(2, 8);
        assert_eq!(tracker.gap_count(), 0);
        assert_eq!(tracker.durable_through(), 8);
    }

    #[test]
    fn timer_later_durable_prefix_resolves_earlier_observed_work() {
        let mut tracker = OrderedCompletionTracker::new(5, 0, 8);
        assert!(matches!(
            tracker.observe_pending(5, 1),
            CompletionDisposition::BufferedGap { .. }
        ));
        assert_eq!(tracker.gap_count(), 1);
        assert_eq!(
            tracker.commit_prefix(5, 2),
            CompletionDisposition::Advanced { durable_through: 2 }
        );
        assert_eq!(tracker.gap_count(), 0);
    }
}
