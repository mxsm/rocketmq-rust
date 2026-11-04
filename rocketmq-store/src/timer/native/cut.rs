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

//! Native timers whose source a replica cut off.
//!
//! A replica that joins a new master cuts off what that master does not hold, and the engine
//! may have indexed timers among it. Their entries lie in bucket files that are not rewritten,
//! so they are kept here until they come due, and leave the wheel then without a delivery. The
//! Timer CQ offsets behind the cut are used again, so an entry is told from those of the new
//! records by the CommitLog offset it came from.

use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;

use parking_lot::RwLock;
use rocketmq_store_local::timer::native_format::NativeDueCursor;
use tracing::warn;

/// File under the store's `config` directory that holds the timers.
const CUT_TIMERS_FILE: &str = "timer-native-cut.meta";

/// The record an index entry came from.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct NativeCutSource {
    pub(crate) cq_offset: i64,
    pub(crate) key_hash: u64,
    pub(crate) commit_log_offset: i64,
}

/// When a timer's entry leaves the wheel.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct NativeCutDeadline {
    pub(crate) lane: u16,
    /// Its entry is due by this, at whatever deadline the engine indexed it.
    pub(crate) due_by_ms: i64,
}

/// A timer the engine indexed from a record that was cut off.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct NativeCutTimer {
    pub(crate) source: NativeCutSource,
    pub(crate) deadline: NativeCutDeadline,
}

/// The cut timers whose entries may still come due.
pub(crate) struct NativeCutTimers {
    path: PathBuf,
    timers: RwLock<HashMap<NativeCutSource, NativeCutDeadline>>,
}

impl NativeCutTimers {
    /// Loads what an earlier run left. Lines that cannot be read are left out with a warning:
    /// the entries they name then stop their lane, which `timerSkipUnknownError` lets go.
    pub(crate) fn open(store_root: impl AsRef<Path>) -> Self {
        let path = store_root.as_ref().join("config").join(CUT_TIMERS_FILE);
        let text = fs::read_to_string(&path).unwrap_or_default();
        let timers = text
            .lines()
            .filter_map(|line| {
                let timer = parse_line(line);
                if timer.is_none() {
                    warn!(line, "skip an unreadable cut native timer");
                }
                timer
            })
            .map(|timer| (timer.source, timer.deadline))
            .collect();
        Self {
            path,
            timers: RwLock::new(timers),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.timers.read().is_empty()
    }

    pub(crate) fn contains(&self, source: &NativeCutSource) -> bool {
        self.timers.read().contains_key(source)
    }

    /// Adds `timers`, durably.
    ///
    /// # Errors
    ///
    /// Returns the I/O error of saving them; the timers on disk stay as they were then.
    pub(crate) fn add(&self, timers: &[NativeCutTimer]) -> std::io::Result<()> {
        if timers.is_empty() {
            return Ok(());
        }
        let mut held = self.timers.write();
        let mut next = held.clone();
        next.extend(timers.iter().map(|timer| (timer.source, timer.deadline)));
        self.save(&next)?;
        *held = next;
        Ok(())
    }

    /// Takes `source` out because a new record has its very place: the entry that came from
    /// the old one reads the new one now, which then needs no entry of its own. Returns
    /// whether `source` was here.
    pub(crate) fn restore(&self, source: &NativeCutSource) -> bool {
        let mut held = self.timers.write();
        let restored = held.remove(source).is_some();
        if restored {
            if let Err(error) = self.save(&held) {
                warn!(%error, "save the cut native timers failed");
            }
        }
        restored
    }

    /// Forgets the timers each lane's durable watermark has passed.
    ///
    /// # Errors
    ///
    /// Returns the I/O error of saving what is left, which only keeps the others longer.
    pub(crate) fn prune(&self, watermarks: &[NativeDueCursor]) -> std::io::Result<()> {
        if self.is_empty() {
            return Ok(());
        }
        let mut held = self.timers.write();
        let before = held.len();
        held.retain(|_, deadline| {
            watermarks
                .get(usize::from(deadline.lane))
                .is_none_or(|watermark| watermark.due_ms <= deadline.due_by_ms)
        });
        if held.len() == before {
            return Ok(());
        }
        self.save(&held)
    }

    fn save(&self, timers: &HashMap<NativeCutSource, NativeCutDeadline>) -> std::io::Result<()> {
        if timers.is_empty() {
            return match fs::remove_file(&self.path) {
                Err(error) if error.kind() != std::io::ErrorKind::NotFound => Err(error),
                _ => Ok(()),
            };
        }
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }
        let temp = self.path.with_extension("meta.tmp");
        let mut file = fs::File::create(&temp)?;
        for (source, deadline) in timers {
            writeln!(
                file,
                "{} {} {} {} {}",
                source.cq_offset, source.key_hash, source.commit_log_offset, deadline.lane, deadline.due_by_ms
            )?;
        }
        file.sync_all()?;
        fs::rename(&temp, &self.path)
    }
}

fn parse_line(line: &str) -> Option<NativeCutTimer> {
    let mut fields = line.split_ascii_whitespace();
    let timer = NativeCutTimer {
        source: NativeCutSource {
            cq_offset: fields.next()?.parse().ok()?,
            key_hash: fields.next()?.parse().ok()?,
            commit_log_offset: fields.next()?.parse().ok()?,
        },
        deadline: NativeCutDeadline {
            lane: fields.next()?.parse().ok()?,
            due_by_ms: fields.next()?.parse().ok()?,
        },
    };
    fields.next().is_none().then_some(timer)
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    fn cut_timer(cq_offset: i64, lane: u16, due_by_ms: i64) -> NativeCutTimer {
        NativeCutTimer {
            source: NativeCutSource {
                cq_offset,
                key_hash: 7,
                commit_log_offset: cq_offset * 100,
            },
            deadline: NativeCutDeadline { lane, due_by_ms },
        }
    }

    fn watermark(due_ms: i64) -> NativeDueCursor {
        NativeDueCursor { due_ms, cq_offset: 0 }
    }

    #[test]
    fn cut_timers_survive_a_restart_until_their_lanes_pass_them() {
        let root = TempDir::new().unwrap();
        let timers = NativeCutTimers::open(root.path());
        let early = cut_timer(1, 0, 1_000);
        let late = cut_timer(2, 1, 2_000);
        timers.add(&[early, late]).unwrap();

        let restarted = NativeCutTimers::open(root.path());
        assert!(restarted.contains(&early.source));
        assert!(restarted.contains(&late.source));

        restarted.prune(&[watermark(1_001), watermark(1_001)]).unwrap();

        let restarted = NativeCutTimers::open(root.path());
        assert!(!restarted.contains(&early.source));
        assert!(restarted.contains(&late.source));

        restarted.prune(&[watermark(1_001), watermark(2_001)]).unwrap();

        assert!(restarted.is_empty());
        assert!(NativeCutTimers::open(root.path()).is_empty());
    }

    #[test]
    fn a_timer_restored_by_a_new_record_is_gone_after_a_restart() {
        let root = TempDir::new().unwrap();
        let timers = NativeCutTimers::open(root.path());
        let timer = cut_timer(1, 0, 1_000);
        timers.add(&[timer]).unwrap();

        assert!(timers.restore(&timer.source));
        assert!(!timers.restore(&timer.source));

        assert!(NativeCutTimers::open(root.path()).is_empty());
    }
}
