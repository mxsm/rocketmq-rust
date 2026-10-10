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

//! The durable JSONL audit sink: one active segment file with sealed segments beside it.
//!
//! `capacity` bounds one segment, not the life of the trail. When the active segment is full it
//! is synced, closed and renamed to `<path>.<segment number>`, and a new active segment starts
//! with a header that names the sealed segment's last sequence and digest. Invocations still in
//! flight are carried over as copies of their `started` records, so the active segment alone is
//! enough to recover and to check every terminal record against its start.
//!
//! The sink never deletes a sealed segment; archiving them is an operator task.

use std::collections::BTreeMap;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use sha2::Digest;
use sha2::Sha256;
use tokio::io::AsyncBufRead;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::sync::Mutex;

use super::evidence::hex;
use super::recovery::recover_audit_state;
use super::wire::AuditSegmentHeader;
use super::AuditEvent;
use super::AuditFuture;
use super::AuditInvocationId;
use super::AuditRecord;
use super::AuditSchemaVersion;
use super::PoisonOnDrop;
use super::ReliableAuditSink;
use crate::error::ControlError;

pub(super) const MAX_AUDIT_FILE_BYTES: u64 = 64 * 1024 * 1024;
/// Suffix of the next segment while it is being prepared.
const NEXT_SEGMENT_SUFFIX: &str = "next";

pub struct JsonlAuditSink {
    writer: Arc<dyn DurableAuditWriter>,
    state: Mutex<JsonlState>,
    capacity: usize,
    max_record_bytes: usize,
    max_file_bytes: u64,
    poisoned: AtomicBool,
}

struct JsonlState {
    /// Invocation records of the active segment, carried `started` copies included.
    records: Vec<AuditRecord>,
    /// Lines in the active segment: its header, the carried copies and its own records.
    lines: usize,
    bytes_used: u64,
    /// Running digest of the active segment, which becomes the digest of the sealed file.
    digest: Sha256,
    segment: u64,
    last_sequence: u64,
    /// `started` records of the invocations that this process has not finished.
    open: BTreeMap<AuditInvocationId, AuditRecord>,
}

pub(super) trait DurableAuditWriter: Send + Sync {
    fn append<'a>(&'a self, encoded: &'a [u8]) -> AuditFuture<'a, Result<(), ControlError>>;
    fn flush(&self) -> AuditFuture<'_, Result<(), ControlError>>;
    fn sync(&self) -> AuditFuture<'_, Result<(), ControlError>>;

    /// Seals the active segment as segment `sealed` and starts the next one with `first_lines`.
    ///
    /// The default writer has nowhere to seal a segment to, so a full segment fails closed.
    fn rotate<'a>(&'a self, sealed: u64, first_lines: &'a [u8]) -> AuditFuture<'a, Result<(), ControlError>> {
        let _ = (sealed, first_lines);
        Box::pin(async { Err(ControlError::audit_unavailable()) })
    }
}

struct TokioAuditWriter {
    path: PathBuf,
    /// `None` only after a failed rotation, which leaves the sink poisoned.
    file: Mutex<Option<tokio::fs::File>>,
}

impl DurableAuditWriter for TokioAuditWriter {
    fn append<'a>(&'a self, encoded: &'a [u8]) -> AuditFuture<'a, Result<(), ControlError>> {
        Box::pin(async move {
            let mut file = self.file.lock().await;
            let file = file.as_mut().ok_or_else(ControlError::audit_unavailable)?;
            file.write_all(encoded)
                .await
                .map_err(|_| ControlError::audit_unavailable())
        })
    }

    fn flush(&self) -> AuditFuture<'_, Result<(), ControlError>> {
        Box::pin(async move {
            let mut file = self.file.lock().await;
            let file = file.as_mut().ok_or_else(ControlError::audit_unavailable)?;
            file.flush().await.map_err(|_| ControlError::audit_unavailable())
        })
    }

    fn sync(&self) -> AuditFuture<'_, Result<(), ControlError>> {
        Box::pin(async move {
            let mut file = self.file.lock().await;
            let file = file.as_mut().ok_or_else(ControlError::audit_unavailable)?;
            file.sync_data().await.map_err(|_| ControlError::audit_unavailable())
        })
    }

    fn rotate<'a>(&'a self, sealed: u64, first_lines: &'a [u8]) -> AuditFuture<'a, Result<(), ControlError>> {
        Box::pin(async move {
            let unavailable = |_| ControlError::audit_unavailable();
            let mut active = self.file.lock().await;
            let sealed_path = sealed_segment_path(&self.path, sealed);
            let next_path = next_segment_path(&self.path);
            // An existing file under the sealed name is history that must not be replaced.
            if tokio::fs::try_exists(&sealed_path).await.map_err(unavailable)? {
                return Err(ControlError::audit_unavailable());
            }
            // The next segment is durable before the active one is renamed away, so a crash at
            // any point leaves either the old active segment or a complete new one.
            let mut next = tokio::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&next_path)
                .await
                .map_err(unavailable)?;
            next.write_all(first_lines).await.map_err(unavailable)?;
            next.flush().await.map_err(unavailable)?;
            next.sync_all().await.map_err(unavailable)?;
            drop(next);
            // Both handles are closed before the renames; Windows refuses to rename an open file.
            let mut sealing = active.take().ok_or_else(ControlError::audit_unavailable)?;
            sealing.flush().await.map_err(unavailable)?;
            sealing.sync_all().await.map_err(unavailable)?;
            drop(sealing);
            tokio::fs::rename(&self.path, &sealed_path).await.map_err(unavailable)?;
            tokio::fs::rename(&next_path, &self.path).await.map_err(unavailable)?;
            sync_parent_directory(&self.path).await?;
            *active = Some(open_for_append(&self.path).await?);
            Ok(())
        })
    }
}

/// Makes the renames of a rotation durable. Windows has no directory handle to sync.
#[cfg(unix)]
async fn sync_parent_directory(path: &Path) -> Result<(), ControlError> {
    let parent = match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    };
    tokio::fs::File::open(parent)
        .await
        .map_err(|_| ControlError::audit_unavailable())?
        .sync_all()
        .await
        .map_err(|_| ControlError::audit_unavailable())
}

#[cfg(not(unix))]
async fn sync_parent_directory(_path: &Path) -> Result<(), ControlError> {
    Ok(())
}

async fn open_for_append(path: &Path) -> Result<tokio::fs::File, ControlError> {
    tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await
        .map_err(|_| ControlError::audit_unavailable())
}

fn with_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(".");
    name.push(suffix);
    PathBuf::from(name)
}

/// Returns where segment `segment` is kept once it is sealed: `<path>.<six-digit number>`.
pub(super) fn sealed_segment_path(path: &Path, segment: u64) -> PathBuf {
    with_suffix(path, &format!("{segment:06}"))
}

fn next_segment_path(path: &Path) -> PathBuf {
    with_suffix(path, NEXT_SEGMENT_SUFFIX)
}

/// What an existing active segment holds.
struct ParsedSegment {
    header: Option<AuditSegmentHeader>,
    records: Vec<AuditRecord>,
    lines: usize,
    digest: Sha256,
    last_sequence: u64,
}

impl JsonlAuditSink {
    /// Opens the active segment at `path` and loads its records for recovery and queries.
    ///
    /// A rotation that a crash interrupted after sealing is finished first. Invocations left
    /// without a terminal record by an earlier process stay as they are: that process is gone,
    /// so nothing can finish them, and they are not carried into later segments.
    ///
    /// # Errors
    ///
    /// Returns `audit_unavailable` if the segment cannot be opened or safely loaded, or if a
    /// file already exists under the name this segment will be sealed to.
    pub async fn open(path: impl AsRef<Path>, capacity: usize, max_record_bytes: usize) -> Result<Self, ControlError> {
        let path = path.as_ref();
        let max_file_bytes = audit_file_limit(capacity, max_record_bytes)?;
        let unavailable = |_| ControlError::audit_unavailable();
        let next_path = next_segment_path(path);
        if !tokio::fs::try_exists(path).await.map_err(unavailable)?
            && tokio::fs::try_exists(&next_path).await.map_err(unavailable)?
        {
            tokio::fs::rename(&next_path, path).await.map_err(unavailable)?;
        }
        let metadata = match tokio::fs::metadata(path).await {
            Ok(metadata) => Some(metadata),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(_) => return Err(ControlError::audit_unavailable()),
        };
        let bytes_used = metadata.as_ref().map_or(0, std::fs::Metadata::len);
        if bytes_used > max_file_bytes {
            return Err(ControlError::audit_unavailable());
        }
        let parsed = if metadata.is_some() {
            parse_existing_file(path, capacity, max_record_bytes, max_file_bytes).await?
        } else {
            ParsedSegment {
                header: None,
                records: Vec::new(),
                lines: 0,
                digest: Sha256::new(),
                last_sequence: 0,
            }
        };
        let segment = parsed.header.as_ref().map_or(1, |header| header.segment);
        if tokio::fs::try_exists(sealed_segment_path(path, segment))
            .await
            .map_err(unavailable)?
        {
            tracing::error!(
                segment,
                "a sealed audit segment already has the number of the active segment"
            );
            return Err(ControlError::audit_unavailable());
        }
        let unfinished = unfinished_invocations(&parsed.records);
        if unfinished > 0 {
            tracing::warn!(
                segment,
                unfinished,
                "the active audit segment holds invocations without a terminal record"
            );
        }
        let file = open_for_append(path).await?;
        Ok(Self {
            writer: Arc::new(TokioAuditWriter {
                path: path.to_path_buf(),
                file: Mutex::new(Some(file)),
            }),
            state: Mutex::new(JsonlState {
                records: parsed.records,
                lines: parsed.lines,
                bytes_used,
                digest: parsed.digest,
                segment,
                last_sequence: parsed.last_sequence,
                open: BTreeMap::new(),
            }),
            capacity,
            max_record_bytes,
            max_file_bytes,
            poisoned: AtomicBool::new(false),
        })
    }

    #[cfg(test)]
    pub(super) fn with_writer(
        writer: Arc<dyn DurableAuditWriter>,
        capacity: usize,
        max_record_bytes: usize,
    ) -> Result<Self, ControlError> {
        Ok(Self {
            writer,
            state: Mutex::new(JsonlState {
                records: Vec::new(),
                lines: 0,
                bytes_used: 0,
                digest: Sha256::new(),
                segment: 1,
                last_sequence: 0,
                open: BTreeMap::new(),
            }),
            capacity,
            max_record_bytes,
            max_file_bytes: audit_file_limit(capacity, max_record_bytes)?,
            poisoned: AtomicBool::new(false),
        })
    }

    /// Seals the full active segment and starts the next one.
    ///
    /// The caller holds the poison guard: any failure here leaves the sink unavailable.
    async fn rotate(&self, state: &mut JsonlState) -> Result<(), ControlError> {
        let header = AuditSegmentHeader::new(
            state
                .segment
                .checked_add(1)
                .ok_or_else(ControlError::audit_unavailable)?,
            state.last_sequence,
            hex(state.digest.clone().finalize().as_slice()),
            state.open.keys().copied().collect(),
        );
        let mut first_lines = encode_line(&header, self.max_record_bytes)?;
        for record in state.open.values() {
            first_lines.extend(encode_line(record, self.max_record_bytes)?);
        }
        let carried_lines = state
            .open
            .len()
            .checked_add(1)
            .ok_or_else(ControlError::audit_unavailable)?;
        let first_bytes = u64::try_from(first_lines.len()).map_err(|_| ControlError::audit_unavailable())?;
        // The new segment must have room left for the record that caused the rotation.
        if carried_lines >= self.capacity || first_bytes >= self.max_file_bytes {
            return Err(ControlError::audit_unavailable());
        }
        self.writer.rotate(state.segment, &first_lines).await?;
        tracing::info!(
            segment = state.segment,
            records = state.lines,
            last_sequence = state.last_sequence,
            carried = state.open.len(),
            "audit segment was sealed"
        );
        state.segment = header.segment;
        state.records = state.open.values().cloned().collect();
        state.lines = carried_lines;
        state.bytes_used = first_bytes;
        state.digest = Sha256::new();
        state.digest.update(&first_lines);
        Ok(())
    }
}

impl ReliableAuditSink for JsonlAuditSink {
    fn append<'a>(&'a self, record: &'a AuditRecord) -> AuditFuture<'a, Result<(), ControlError>> {
        Box::pin(async move {
            if record.schema_version != AuditSchemaVersion::V3 || self.poisoned.load(Ordering::Acquire) {
                return Err(ControlError::audit_unavailable());
            }
            let encoded = super::encode_record(record, self.max_record_bytes)?;
            let encoded_len =
                u64::try_from(encoded.len().saturating_add(1)).map_err(|_| ControlError::audit_unavailable())?;
            let mut state = self.state.lock().await;
            if self.poisoned.load(Ordering::Acquire) {
                return Err(ControlError::audit_unavailable());
            }
            let poison = PoisonOnDrop::new(&self.poisoned);
            let full = |state: &JsonlState| {
                state.lines >= self.capacity
                    || state
                        .bytes_used
                        .checked_add(encoded_len)
                        .is_none_or(|next| next > self.max_file_bytes)
            };
            if full(&state) {
                self.rotate(&mut state).await?;
                if full(&state) {
                    return Err(ControlError::audit_unavailable());
                }
            }
            // The record and its line break are made durable in two steps, so a torn write is an
            // unterminated tail that recovery rejects instead of a record it could misread.
            if self.writer.append(&encoded).await.is_err()
                || self.writer.flush().await.is_err()
                || self.writer.sync().await.is_err()
                || self.writer.append(b"\n").await.is_err()
                || self.writer.flush().await.is_err()
                || self.writer.sync().await.is_err()
            {
                return Err(ControlError::audit_unavailable());
            }
            state.bytes_used = state.bytes_used.saturating_add(encoded_len);
            state.lines = state.lines.saturating_add(1);
            state.digest.update(&encoded);
            state.digest.update(b"\n");
            state.last_sequence = record.sequence;
            match record.event {
                AuditEvent::Started => {
                    state.open.insert(record.invocation_id, record.clone());
                }
                AuditEvent::Completed | AuditEvent::Failed => {
                    state.open.remove(&record.invocation_id);
                }
            }
            state.records.push(record.clone());
            poison.disarm();
            Ok(())
        })
    }

    fn records(&self) -> AuditFuture<'_, Result<Vec<AuditRecord>, ControlError>> {
        Box::pin(async move {
            if self.poisoned.load(Ordering::Acquire) {
                return Err(ControlError::audit_unavailable());
            }
            let state = self.state.lock().await;
            if self.poisoned.load(Ordering::Acquire) {
                return Err(ControlError::audit_unavailable());
            }
            Ok(state.records.clone())
        })
    }

    fn last_sequence(&self) -> AuditFuture<'_, Result<u64, ControlError>> {
        Box::pin(async move {
            let state = self.state.lock().await;
            if self.poisoned.load(Ordering::Acquire) {
                return Err(ControlError::audit_unavailable());
            }
            Ok(state.last_sequence)
        })
    }
}

fn encode_line<T: serde::Serialize>(value: &T, max_record_bytes: usize) -> Result<Vec<u8>, ControlError> {
    let mut encoded = serde_json::to_vec(value).map_err(|_| ControlError::audit_unavailable())?;
    if encoded.len() > max_record_bytes {
        return Err(ControlError::audit_unavailable());
    }
    encoded.push(b'\n');
    Ok(encoded)
}

fn unfinished_invocations(records: &[AuditRecord]) -> usize {
    let mut open = std::collections::BTreeSet::new();
    for record in records {
        match record.event {
            AuditEvent::Started => {
                open.insert(record.invocation_id);
            }
            AuditEvent::Completed | AuditEvent::Failed => {
                open.remove(&record.invocation_id);
            }
        }
    }
    open.len()
}

pub(super) fn audit_file_limit(capacity: usize, max_record_bytes: usize) -> Result<u64, ControlError> {
    let per_record = max_record_bytes
        .checked_add(1)
        .ok_or_else(ControlError::audit_unavailable)?;
    let configured = capacity
        .checked_mul(per_record)
        .ok_or_else(ControlError::audit_unavailable)?;
    Ok(u64::try_from(configured)
        .map_err(|_| ControlError::audit_unavailable())?
        .min(MAX_AUDIT_FILE_BYTES))
}

async fn parse_existing_file(
    path: &Path,
    capacity: usize,
    max_record_bytes: usize,
    max_file_bytes: u64,
) -> Result<ParsedSegment, ControlError> {
    let file = tokio::fs::File::open(path)
        .await
        .map_err(|_| ControlError::audit_unavailable())?;
    let mut reader = BufReader::new(file);
    let mut header = None;
    let mut records = Vec::new();
    let mut digest = Sha256::new();
    let mut lines = 0_usize;
    let mut bytes_read = 0_u64;
    loop {
        let Some(mut line) = read_bounded_line(&mut reader, max_record_bytes).await? else {
            break;
        };
        bytes_read = bytes_read
            .checked_add(u64::try_from(line.len()).map_err(|_| ControlError::audit_unavailable())?)
            .ok_or_else(ControlError::audit_unavailable)?;
        if bytes_read > max_file_bytes || line.len() == 1 || lines >= capacity {
            return Err(ControlError::audit_unavailable());
        }
        digest.update(&line);
        line.pop();
        // Only the first line of a segment may be its header.
        if lines == 0 {
            if let Ok(parsed) = serde_json::from_slice::<AuditSegmentHeader>(&line) {
                header = Some(parsed);
                lines = 1;
                continue;
            }
        }
        lines += 1;
        let record: AuditRecord = serde_json::from_slice(&line).map_err(|_| ControlError::audit_unavailable())?;
        records.push(record);
    }
    let mut last_sequence = records.last().map_or(0, |record| record.sequence);
    if let Some(header) = &header {
        let carried = header.validated_open_invocations()?;
        let copies = records
            .get(..carried.len())
            .ok_or_else(ControlError::audit_unavailable)?;
        let copied = copies
            .iter()
            .zip(carried)
            .all(|(record, id)| record.event == AuditEvent::Started && record.invocation_id == *id);
        // Every record of this segment itself comes after the sealed segment's last sequence.
        let continues = records
            .get(carried.len())
            .is_none_or(|record| record.sequence > header.previous_last_sequence);
        if !copied || !continues {
            return Err(ControlError::audit_unavailable());
        }
        last_sequence = last_sequence.max(header.previous_last_sequence);
    }
    recover_audit_state(&records)?;
    Ok(ParsedSegment {
        header,
        records,
        lines,
        digest,
        last_sequence,
    })
}

async fn read_bounded_line<R>(reader: &mut R, max_record_bytes: usize) -> Result<Option<Vec<u8>>, ControlError>
where
    R: AsyncBufRead + Unpin,
{
    let max_line_bytes = max_record_bytes
        .checked_add(1)
        .ok_or_else(ControlError::audit_unavailable)?;
    let mut line = Vec::with_capacity(max_line_bytes.min(4096));
    loop {
        let buffer = reader.fill_buf().await.map_err(|_| ControlError::audit_unavailable())?;
        if buffer.is_empty() {
            return if line.is_empty() {
                Ok(None)
            } else {
                Err(ControlError::audit_unavailable())
            };
        }
        let consumed = buffer
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(buffer.len(), |position| position + 1);
        let next_len = line
            .len()
            .checked_add(consumed)
            .ok_or_else(ControlError::audit_unavailable)?;
        if next_len > max_line_bytes {
            return Err(ControlError::audit_unavailable());
        }
        let complete = buffer[consumed - 1] == b'\n';
        line.extend_from_slice(&buffer[..consumed]);
        reader.consume(consumed);
        if complete {
            return Ok(Some(line));
        }
    }
}
