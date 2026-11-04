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

//! Which master wrote which part of a CommitLog.
//!
//! Every master term starts at an offset. Two replicas hold the same bytes up to the end of the
//! last term they share, so a replica that joins a new master can find how much of its log is
//! also the master's, however many failovers it missed.

use std::fs;
use std::fs::File;
use std::io;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;

/// A master term and the CommitLog offset its records start at.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EpochStart {
    pub epoch: i32,
    pub start_offset: i64,
}

/// The master terms of a CommitLog, oldest first.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EpochLog {
    entries: Vec<EpochStart>,
}

impl EpochLog {
    /// A log of `entries`, oldest first.
    pub fn new(entries: Vec<EpochStart>) -> Self {
        Self { entries }
    }

    pub fn entries(&self) -> &[EpochStart] {
        &self.entries
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn last_epoch(&self) -> Option<i32> {
        self.entries.last().map(|entry| entry.epoch)
    }

    /// Starts `epoch` at `start_offset`, and says whether it is newer than the last term. Terms
    /// that start past `start_offset` go: they came with a list from a master this log never
    /// caught up with.
    pub fn begin(&mut self, epoch: i32, start_offset: i64) -> bool {
        if self.last_epoch().is_some_and(|last| last >= epoch) {
            return false;
        }
        self.entries.retain(|entry| entry.start_offset <= start_offset);
        self.entries.push(EpochStart { epoch, start_offset });
        true
    }

    /// Where the term at `index` ends in a log that ends at `end`.
    fn end_of(&self, index: usize, end: i64) -> i64 {
        self.entries.get(index + 1).map_or(end, |next| next.start_offset)
    }

    /// The offset up to which this log, ending at `end`, and `other`, ending at `other_end`, hold
    /// the same bytes: the end of the newest term they share. `None` when they share no term.
    pub fn common_end(&self, end: i64, other: &Self, other_end: i64) -> Option<i64> {
        self.entries.iter().enumerate().rev().find_map(|(index, entry)| {
            let other_index = other.entries.iter().position(|candidate| candidate == entry)?;
            Some(
                self.end_of(index, end)
                    .min(other.end_of(other_index, other_end))
                    .min(end),
            )
        })
    }

    /// Where a replica with this log, ending at `end`, joins `master`, whose log ends at
    /// `master_end`. `None` when they went separate ways and no point is known.
    ///
    /// A log without history falls back on the offsets. That is a new replica or one that ran an
    /// older version, and a master of an older version reports its current term at offset zero.
    pub fn join_point(&self, end: i64, master: &Self, master_end: i64) -> Option<i64> {
        if self.is_empty() {
            return Some(end.min(master_end));
        }
        if let Some(common) = self.common_end(end, master, master_end) {
            return Some(common);
        }
        match master.entries() {
            [] | [EpochStart { start_offset: 0, .. }] => Some(end.min(master_end)),
            // The master's history starts after this log's: it upgraded since.
            [first, ..] if self.last_epoch().is_some_and(|last| last < first.epoch) => {
                Some(end.min(first.start_offset))
            }
            _ => None,
        }
    }
}

/// The durable copy of a store's [`EpochLog`].
pub struct EpochFile {
    path: PathBuf,
}

impl EpochFile {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
        }
    }

    /// The saved log, or an empty one when nothing was saved yet.
    ///
    /// # Errors
    ///
    /// Returns the I/O error of reading the file, or `InvalidData` for a file whose count,
    /// checksum or lines do not hold.
    pub fn read(&self) -> io::Result<EpochLog> {
        let text = match fs::read_to_string(&self.path) {
            Ok(text) => text,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(EpochLog::default()),
            Err(error) => return Err(error),
        };
        parse_checkpoint(&text)
    }

    /// Replaces the saved log, durably.
    ///
    /// # Errors
    ///
    /// Returns the I/O error of writing the file; the old log stays then.
    pub fn write(&self, log: &EpochLog) -> io::Result<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }
        let temp = self.path.with_extension("tmp");
        let mut file = File::create(&temp)?;
        file.write_all(checkpoint_text(log).as_bytes())?;
        file.sync_all()?;
        fs::rename(&temp, &self.path)
    }
}

/// The file as Java's `CheckpointFile` writes it: the number of entries, the checksum of the
/// entry lines, then one `epoch-startOffset` line per entry.
fn checkpoint_text(log: &EpochLog) -> String {
    let entries: String = log
        .entries()
        .iter()
        .map(|entry| format!("{}-{}\n", entry.epoch, entry.start_offset))
        .collect();
    format!("{}\n{}\n{entries}", log.entries().len(), java_crc32(entries.as_bytes()))
}

fn parse_checkpoint(text: &str) -> io::Result<EpochLog> {
    let mut lines = text.lines();
    let (Some(count), Some(checksum)) = (lines.next(), lines.next()) else {
        return Err(invalid_line(text));
    };
    let count: usize = count.trim().parse().map_err(|_| invalid_line(count))?;
    let checksum: u32 = checksum.trim().parse().map_err(|_| invalid_line(checksum))?;
    let entries: Vec<&str> = lines.filter(|line| !line.is_empty()).collect();
    // A file written on Windows ends its lines there as Java does.
    let separator = if text.contains("\r\n") { "\r\n" } else { "\n" };
    let content: String = entries.iter().map(|line| format!("{line}{separator}")).collect();
    if entries.len() != count || java_crc32(content.as_bytes()) != checksum {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "the epoch file does not match its count or checksum",
        ));
    }
    entries
        .into_iter()
        .map(|line| {
            let (epoch, start_offset) = line.split_once('-').ok_or_else(|| invalid_line(line))?;
            Ok(EpochStart {
                epoch: epoch.parse().map_err(|_| invalid_line(line))?,
                start_offset: start_offset.parse().map_err(|_| invalid_line(line))?,
            })
        })
        .collect::<io::Result<Vec<_>>>()
        .map(EpochLog::new)
}

/// Java's `UtilAll.crc32`: CRC-32 with the sign bit cleared.
fn java_crc32(bytes: &[u8]) -> u32 {
    crate::mapped_file::retirement::codec::crc32(bytes) & 0x7FFF_FFFF
}

fn invalid_line(line: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, format!("invalid epoch line: {line}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_file_is_the_one_a_java_broker_keeps() {
        let dir = tempfile::tempdir().unwrap();
        let file = EpochFile::new(dir.path().join("epochFileCheckpoint"));
        let terms = log(&[(1, 0), (2, 1024)]);

        file.write(&terms).unwrap();

        assert_eq!(
            fs::read_to_string(dir.path().join("epochFileCheckpoint")).unwrap(),
            "2\n327520601\n1-0\n2-1024\n"
        );
        assert_eq!(file.read().unwrap(), terms);
    }

    #[test]
    fn a_file_a_java_broker_wrote_on_windows_reads() {
        assert_eq!(
            parse_checkpoint("2\r\n805398763\r\n1-0\r\n2-1024\r\n").unwrap(),
            log(&[(1, 0), (2, 1024)])
        );
    }

    #[test]
    fn a_damaged_file_is_refused() {
        for text in [
            "2\n327520601\n1-0\n2-1025\n",
            "3\n327520601\n1-0\n2-1024\n",
            "1-0\n2-1024\n",
        ] {
            assert_eq!(
                parse_checkpoint(text).unwrap_err().kind(),
                io::ErrorKind::InvalidData,
                "{text:?}"
            );
        }
    }

    fn log(entries: &[(i32, i64)]) -> EpochLog {
        EpochLog::new(
            entries
                .iter()
                .map(|&(epoch, start_offset)| EpochStart { epoch, start_offset })
                .collect(),
        )
    }

    #[test]
    fn a_replica_ahead_in_the_same_term_ends_where_the_master_ends() {
        let replica = log(&[(1, 0)]);
        let master = log(&[(1, 0)]);
        assert_eq!(replica.common_end(900, &master, 700), Some(700));
        assert_eq!(replica.common_end(500, &master, 700), Some(500));
    }

    #[test]
    fn a_replica_from_the_old_term_ends_where_the_new_term_started() {
        let replica = log(&[(1, 0), (2, 100)]);
        let master = log(&[(1, 0), (2, 100), (3, 300)]);
        assert_eq!(replica.common_end(450, &master, 900), Some(300));
        assert_eq!(replica.common_end(250, &master, 900), Some(250));
    }

    #[test]
    fn a_term_that_never_reached_the_master_goes() {
        let replica = log(&[(1, 0), (3, 100)]);
        let master = log(&[(1, 0), (2, 50), (4, 120)]);
        assert_eq!(replica.common_end(200, &master, 900), Some(50));
    }

    #[test]
    fn a_term_with_another_start_is_another_term() {
        let replica = log(&[(1, 0), (2, 100)]);
        let master = log(&[(1, 0), (2, 80)]);
        assert_eq!(replica.common_end(200, &master, 900), Some(80));
    }

    #[test]
    fn a_term_past_the_end_of_the_log_does_not_count_as_held() {
        let replica = log(&[(1, 0), (2, 200)]);
        let master = log(&[(1, 0), (3, 150)]);
        assert_eq!(replica.common_end(100, &master, 900), Some(100));
    }

    #[test]
    fn logs_without_a_shared_term_have_no_common_end() {
        assert_eq!(log(&[(1, 0)]).common_end(100, &log(&[(2, 0)]), 100), None);
        assert_eq!(log(&[]).common_end(100, &log(&[(2, 0)]), 100), None);
    }

    #[test]
    fn a_replica_without_history_goes_by_the_offsets() {
        let master = log(&[(1, 0), (2, 100)]);
        assert_eq!(log(&[]).join_point(150, &master, 120), Some(120));
        assert_eq!(log(&[]).join_point(90, &master, 120), Some(90));
    }

    #[test]
    fn a_master_of_an_older_version_goes_by_the_offsets() {
        let replica = log(&[(3, 500)]);
        assert_eq!(replica.join_point(900, &log(&[(4, 0)]), 700), Some(700));
        assert_eq!(replica.join_point(900, &log(&[]), 700), Some(700));
    }

    #[test]
    fn a_master_with_a_younger_history_counts_from_its_first_term() {
        let replica = log(&[(3, 500)]);
        assert_eq!(replica.join_point(900, &log(&[(5, 800)]), 1000), Some(800));
    }

    #[test]
    fn logs_that_went_separate_ways_have_no_join_point() {
        let replica = log(&[(5, 500)]);
        assert_eq!(replica.join_point(900, &log(&[(3, 100), (4, 300)]), 1000), None);
    }

    #[test]
    fn a_shared_term_decides_the_join_point() {
        let replica = log(&[(1, 0), (2, 100)]);
        let master = log(&[(1, 0), (2, 100), (3, 300)]);
        assert_eq!(replica.join_point(450, &master, 900), Some(300));
    }

    #[test]
    fn a_new_term_must_be_newer_and_drops_what_this_log_never_held() {
        let mut epochs = log(&[(1, 0), (2, 100), (3, 300)]);
        assert!(!epochs.begin(3, 400));
        assert!(!epochs.begin(2, 400));
        assert!(epochs.begin(4, 200));
        assert_eq!(epochs, log(&[(1, 0), (2, 100), (4, 200)]));
    }

    #[test]
    fn the_file_keeps_the_log() {
        let dir = tempfile::tempdir().unwrap();
        let file = EpochFile::new(dir.path().join("epochFileCheckpoint"));
        assert_eq!(file.read().unwrap(), EpochLog::default());

        let epochs = log(&[(1, 0), (5, 1234567)]);
        file.write(&epochs).unwrap();
        assert_eq!(file.read().unwrap(), epochs);

        file.write(&log(&[(6, 7)])).unwrap();
        assert_eq!(file.read().unwrap(), log(&[(6, 7)]));
    }
}
