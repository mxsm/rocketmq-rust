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

//! The file audit sink: one append handle that stays open, and rotation by size.

use std::fs::File;
use std::fs::Metadata;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;

use super::AuditRecord;
use super::AuditSink;
use crate::McpError;

/// Writes audit records to `audit.path`, one JSON document on each line.
///
/// The file stays open between records. Every file operation still runs on the blocking
/// executor: the writer moves into the operation and comes back with its result.
pub(super) struct FileAuditSink {
    /// `None` until the first record, and after an operation that could not hand the writer
    /// back. The next operation then starts from the configuration again.
    writer: Option<AuditFileWriter>,
    path: PathBuf,
    max_file_bytes: u64,
    blocking: rocketmq_runtime::BlockingExecutor,
}

impl FileAuditSink {
    pub(super) fn new(path: PathBuf, max_file_bytes: u64, blocking: rocketmq_runtime::BlockingExecutor) -> Self {
        Self {
            writer: None,
            path,
            max_file_bytes,
            blocking,
        }
    }

    async fn with_writer<F>(&mut self, name: &'static str, operation: F) -> crate::McpResult<()>
    where
        F: FnOnce(&mut AuditFileWriter) -> std::io::Result<()> + Send + 'static,
    {
        let mut writer = self
            .writer
            .take()
            .unwrap_or_else(|| AuditFileWriter::new(self.path.clone(), self.max_file_bytes));
        let (writer, result) = self
            .blocking
            .spawn_io(name, move || {
                let result = operation(&mut writer);
                (writer, result)
            })
            .await
            .map_err(McpError::from_source)?;
        self.writer = Some(writer);
        result.map_err(McpError::from_source)
    }
}

impl AuditSink for FileAuditSink {
    async fn write(&mut self, _record: &AuditRecord, encoded: &[u8]) -> crate::McpResult<()> {
        let encoded = encoded.to_vec();
        self.with_writer("rocketmq-mcp-audit-file-write", move |writer| writer.append(&encoded))
            .await
    }

    async fn flush(&mut self) -> crate::McpResult<()> {
        self.with_writer("rocketmq-mcp-audit-file-flush", AuditFileWriter::sync)
            .await
    }
}

/// The audit file as the blocking side sees it. Every method blocks on file I/O.
pub(super) struct AuditFileWriter {
    path: PathBuf,
    /// A file that would grow past this many bytes is sealed first. Zero never seals.
    max_file_bytes: u64,
    open: Option<OpenAuditFile>,
    /// The number of the next sealed file, once the directory was read for it.
    next_sealed: Option<u64>,
    /// Set while sealing fails, so that the failure is reported once and not for every record.
    sealing_fails: bool,
}

struct OpenAuditFile {
    file: File,
    /// The bytes in the file.
    len: u64,
    identity: FileIdentity,
}

impl AuditFileWriter {
    pub(super) fn new(path: PathBuf, max_file_bytes: u64) -> Self {
        Self {
            path,
            max_file_bytes,
            open: None,
            next_sealed: None,
            sealing_fails: false,
        }
    }

    /// Appends one encoded record, sealing the file first when the record would make it too big.
    ///
    /// # Errors
    ///
    /// Fails when the file cannot be opened or written. The handle is given up then, so the
    /// next record opens the file again.
    pub(super) fn append(&mut self, encoded: &[u8]) -> std::io::Result<()> {
        let appended = self.try_append(encoded);
        if appended.is_err() {
            self.open = None;
        }
        appended
    }

    /// Makes the records that were appended durable.
    ///
    /// # Errors
    ///
    /// Fails when the file cannot be synchronized.
    pub(super) fn sync(&mut self) -> std::io::Result<()> {
        match &self.open {
            Some(open) => open.file.sync_all(),
            None => Ok(()),
        }
    }

    fn try_append(&mut self, encoded: &[u8]) -> std::io::Result<()> {
        // The file is whatever the path names now: a log shipper may have moved it away or cut
        // it short since the last record.
        if self.open.as_mut().is_some_and(|open| !open.is_still_at(&self.path)) {
            self.open = None;
        }
        let record_len = encoded.len() as u64;
        if self.open_file()?.len > 0 && self.exceeds_limit(record_len) {
            self.seal();
        }
        let open = self.open_file()?;
        open.file.write_all(encoded)?;
        open.len = open.len.saturating_add(record_len);
        Ok(())
    }

    fn exceeds_limit(&self, record_len: u64) -> bool {
        let len = self.open.as_ref().map_or(0, |open| open.len);
        self.max_file_bytes > 0 && len.saturating_add(record_len) > self.max_file_bytes
    }

    fn open_file(&mut self) -> std::io::Result<&mut OpenAuditFile> {
        match &mut self.open {
            Some(open) => Ok(open),
            closed => Ok(closed.insert(OpenAuditFile::open(&self.path)?)),
        }
    }

    /// Moves the full file to its sealed name, so that the next record starts a new file.
    ///
    /// A record is worth more than a tidy directory: when the file cannot be sealed, it is kept
    /// and grows past the limit until sealing works again.
    fn seal(&mut self) {
        match self.try_seal() {
            Ok(()) => self.sealing_fails = false,
            Err(error) => {
                if !self.sealing_fails {
                    tracing::warn!(
                        error_kind = ?error.kind(),
                        "rocketmq-mcp audit file could not be sealed; it keeps growing"
                    );
                }
                self.sealing_fails = true;
            }
        }
    }

    fn try_seal(&mut self) -> std::io::Result<()> {
        let number = self.free_sealed_number()?;
        if let Some(open) = self.open.take() {
            // The file is complete before it changes its name, so no later step can lose it:
            // after a crash it is under one of the two names, with every record.
            open.file.sync_all()?;
        }
        std::fs::rename(&self.path, sealed_path(&self.path, number))?;
        // The number is used up only now, so a rename that failed leaves no gap.
        self.next_sealed = number.checked_add(1);
        Ok(())
    }

    /// The number the full file is sealed under: the one after the highest in use.
    ///
    /// A sealed file is never replaced. When the number is taken after all, the directory is
    /// read once more before giving up.
    fn free_sealed_number(&mut self) -> std::io::Result<u64> {
        for _ in 0..2 {
            let number = match self.next_sealed.take() {
                Some(number) => number,
                None => highest_sealed_number(&self.path)?.saturating_add(1),
            };
            if !sealed_path(&self.path, number).try_exists()? {
                self.next_sealed = Some(number);
                return Ok(number);
            }
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "no sealed audit file name is free",
        ))
    }
}

impl OpenAuditFile {
    fn open(path: &Path) -> std::io::Result<Self> {
        if let Some(parent) = path.parent().filter(|parent| !parent.as_os_str().is_empty()) {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        let metadata = file.metadata()?;
        Ok(Self {
            len: metadata.len(),
            identity: FileIdentity::of(&metadata),
            file,
        })
    }

    /// Whether `path` still names this file, and how long the file is now.
    ///
    /// A file that was moved away, replaced, or cut short is no longer the one to append to.
    fn is_still_at(&mut self, path: &Path) -> bool {
        match std::fs::metadata(path) {
            Ok(metadata) if FileIdentity::of(&metadata) == self.identity && metadata.len() >= self.len => {
                self.len = metadata.len();
                true
            }
            _ => false,
        }
    }
}

/// What tells one file from another under the same name. Only Unix offers it; elsewhere this
/// is empty, and a replaced file is noticed by its length as soon as it is the shorter one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FileIdentity {
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
}

impl FileIdentity {
    #[cfg(unix)]
    fn of(metadata: &Metadata) -> Self {
        use std::os::unix::fs::MetadataExt;
        Self {
            device: metadata.dev(),
            inode: metadata.ino(),
        }
    }

    #[cfg(not(unix))]
    fn of(_metadata: &Metadata) -> Self {
        Self {}
    }
}

/// `<path>.<six-digit number>`, the naming of the sealed audit files of the control server.
fn sealed_path(path: &Path, number: u64) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(format!(".{number:06}"));
    PathBuf::from(name)
}

/// The highest number a sealed file of `path` has, or zero when there is none.
fn highest_sealed_number(path: &Path) -> std::io::Result<u64> {
    let Some(file_name) = path.file_name() else {
        return Ok(0);
    };
    let directory = match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    };
    let mut highest = 0;
    for entry in std::fs::read_dir(directory)? {
        let entry_name = entry?.file_name();
        let number = entry_name
            .as_encoded_bytes()
            .strip_prefix(file_name.as_encoded_bytes())
            .and_then(|suffix| suffix.strip_prefix(b"."))
            .filter(|digits| !digits.is_empty() && digits.iter().all(u8::is_ascii_digit))
            .and_then(|digits| std::str::from_utf8(digits).ok()?.parse::<u64>().ok());
        highest = highest.max(number.unwrap_or(0));
    }
    Ok(highest)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(name: &str) -> Vec<u8> {
        format!("{{\"request_id\":\"{name}\"}}\n").into_bytes()
    }

    fn lines(path: &Path) -> Vec<String> {
        std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(str::to_string)
            .collect()
    }

    fn request_ids(names: &[&str]) -> Vec<String> {
        names
            .iter()
            .map(|name| format!("{{\"request_id\":\"{name}\"}}"))
            .collect()
    }

    #[test]
    fn the_file_stays_open_between_records() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("audit.jsonl");
        let mut writer = AuditFileWriter::new(path.clone(), 0);
        writer.append(&record("request-1")).unwrap();

        // A file that may no longer be opened for writing still takes the next record, because
        // the writer does not open it again.
        let writable = std::fs::metadata(&path).unwrap().permissions();
        let mut read_only = writable.clone();
        read_only.set_readonly(true);
        std::fs::set_permissions(&path, read_only).unwrap();
        let appended = writer.append(&record("request-2"));
        std::fs::set_permissions(&path, writable).unwrap();

        appended.unwrap();
        writer.sync().unwrap();
        assert_eq!(lines(&path), request_ids(&["request-1", "request-2"]));
    }

    #[test]
    fn a_full_file_is_sealed_under_the_next_number_and_no_sealed_file_is_replaced() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("audit.jsonl");
        // An earlier run left a sealed file, and a file that only looks like one.
        std::fs::write(sealed_path(&path, 3), "earlier\n").unwrap();
        std::fs::write(directory.path().join("audit.jsonl.bak"), "not sealed\n").unwrap();
        let record_len = record("request-1").len() as u64;
        // Two records fit, a third does not.
        let mut writer = AuditFileWriter::new(path.clone(), 2 * record_len);

        for name in ["request-1", "request-2", "request-3", "request-4", "request-5"] {
            writer.append(&record(name)).unwrap();
        }

        assert_eq!(lines(&sealed_path(&path, 3)), ["earlier"]);
        assert_eq!(lines(&sealed_path(&path, 4)), request_ids(&["request-1", "request-2"]));
        assert_eq!(lines(&sealed_path(&path, 5)), request_ids(&["request-3", "request-4"]));
        assert_eq!(lines(&path), request_ids(&["request-5"]));
        assert!(!sealed_path(&path, 1).exists());
    }

    #[test]
    fn a_record_is_never_split_or_lost_to_the_size_limit() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("audit.jsonl");
        // No record fits the limit, so every record gets a file of its own.
        let mut writer = AuditFileWriter::new(path.clone(), 4);
        for name in ["request-1", "request-2"] {
            writer.append(&record(name)).unwrap();
        }
        assert_eq!(lines(&sealed_path(&path, 1)), request_ids(&["request-1"]));
        assert_eq!(lines(&path), request_ids(&["request-2"]));

        // Without a limit the file is never sealed.
        let path = directory.path().join("unlimited.jsonl");
        let mut writer = AuditFileWriter::new(path.clone(), 0);
        for name in ["request-1", "request-2", "request-3"] {
            writer.append(&record(name)).unwrap();
        }
        assert_eq!(lines(&path), request_ids(&["request-1", "request-2", "request-3"]));
        assert!(!sealed_path(&path, 1).exists());
    }

    #[test]
    fn a_file_that_cannot_be_sealed_keeps_its_records_and_is_sealed_later() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("audit.jsonl");
        let record_len = record("request-1").len() as u64;
        let mut writer = AuditFileWriter::new(path.clone(), record_len);
        writer.append(&record("request-1")).unwrap();

        // The numbers are used up: no name is left to seal the file under.
        let last = sealed_path(&path, u64::MAX);
        std::fs::write(&last, "last\n").unwrap();
        writer.append(&record("request-2")).unwrap();
        writer.append(&record("request-3")).unwrap();
        assert_eq!(lines(&path), request_ids(&["request-1", "request-2", "request-3"]));
        assert_eq!(lines(&last), ["last"]);

        // Once a name is free again, the file is sealed with everything it holds.
        std::fs::remove_file(&last).unwrap();
        writer.append(&record("request-4")).unwrap();
        assert_eq!(
            lines(&sealed_path(&path, 1)),
            request_ids(&["request-1", "request-2", "request-3"])
        );
        assert_eq!(lines(&path), request_ids(&["request-4"]));
    }

    #[test]
    fn a_file_that_was_moved_away_or_cut_short_is_not_written_to_again() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("audit.jsonl");
        let mut writer = AuditFileWriter::new(path.clone(), 0);
        writer.append(&record("request-1")).unwrap();

        // A log shipper moves the file away: the next record starts a new file under the name.
        let moved = directory.path().join("audit.jsonl.shipped");
        std::fs::rename(&path, &moved).unwrap();
        writer.append(&record("request-2")).unwrap();
        assert_eq!(lines(&moved), request_ids(&["request-1"]));
        assert_eq!(lines(&path), request_ids(&["request-2"]));

        // A log shipper copies the file and empties it: the next record follows what is left.
        OpenOptions::new().write(true).truncate(true).open(&path).unwrap();
        writer.append(&record("request-3")).unwrap();
        assert_eq!(lines(&path), request_ids(&["request-3"]));
    }

    #[test]
    fn a_failure_keeps_its_io_cause_and_does_not_name_the_path() {
        use std::error::Error;

        // A directory cannot be opened as the audit file.
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().to_path_buf();
        let mut writer = AuditFileWriter::new(path.clone(), 0);

        let error = McpError::from_source(writer.append(b"record").unwrap_err());

        assert!(error.source().unwrap().is::<std::io::Error>());
        assert!(!format!("{error} {error:?}").contains(path.to_str().unwrap()));
        // Nothing was appended, so there is nothing to make durable.
        writer.sync().unwrap();
    }
}
