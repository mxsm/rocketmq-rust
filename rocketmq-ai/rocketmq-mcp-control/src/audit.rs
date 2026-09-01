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

use std::collections::BTreeMap;
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use tokio::io::AsyncBufRead;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::sync::Mutex;

use crate::error::ControlError;
use crate::error::ControlErrorCode;
use crate::model::ClusterName;
use crate::model::ControlOperation;

pub const AUDIT_SCHEMA_VERSION: &str = "rocketmq-mcp-control.audit.v1";
const MAX_AUDIT_FILE_BYTES: u64 = 64 * 1024 * 1024;
const AUDIT_TRANSACTION_TIMEOUT: Duration = Duration::from_secs(2);

pub type AuditFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AuditEvent {
    Started,
    Completed,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize, JsonSchema)]
#[serde(transparent)]
pub struct AuditInvocationId(u64);

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AuditRecord {
    pub schema_version: String,
    pub sequence: u64,
    pub invocation_id: AuditInvocationId,
    pub timestamp_unix_millis: u64,
    pub event: AuditEvent,
    pub operation: ControlOperation,
    pub cluster: ClusterName,
    pub dry_run: bool,
    pub error_code: Option<ControlErrorCode>,
}

#[derive(Debug, Clone)]
pub struct AuditInvocation {
    id: AuditInvocationId,
    operation: ControlOperation,
    cluster: ClusterName,
    dry_run: bool,
    trail_identity: Arc<TrailIdentity>,
}

impl AuditInvocation {
    pub const fn id(&self) -> AuditInvocationId {
        self.id
    }
}

pub trait ReliableAuditSink: Send + Sync {
    fn append<'a>(&'a self, record: &'a AuditRecord) -> AuditFuture<'a, Result<(), ControlError>>;
    fn records(&self) -> AuditFuture<'_, Result<Vec<AuditRecord>, ControlError>>;
}

#[derive(Clone)]
pub struct AuditTrail {
    sink: Arc<dyn ReliableAuditSink>,
    state: Arc<Mutex<AuditTrailState>>,
    poisoned: Arc<AtomicBool>,
    identity: Arc<TrailIdentity>,
}

#[derive(Debug)]
struct TrailIdentity;

struct AuditTrailState {
    sequence: u64,
    invocations: BTreeMap<AuditInvocationId, RecoveredInvocation>,
}

#[derive(Clone)]
struct RecoveredInvocation {
    operation: ControlOperation,
    cluster: ClusterName,
    dry_run: bool,
    terminal: bool,
}

struct PoisonOnDrop<'a> {
    poisoned: &'a AtomicBool,
    armed: bool,
}

impl<'a> PoisonOnDrop<'a> {
    fn new(poisoned: &'a AtomicBool) -> Self {
        Self { poisoned, armed: true }
    }

    fn disarm(mut self) {
        self.armed = false;
    }
}

impl Drop for PoisonOnDrop<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.poisoned.store(true, Ordering::Release);
        }
    }
}

impl AuditTrail {
    #[cfg(test)]
    pub(crate) fn new(sink: Arc<dyn ReliableAuditSink>) -> Self {
        Self {
            sink,
            state: Arc::new(Mutex::new(AuditTrailState {
                sequence: 0,
                invocations: BTreeMap::new(),
            })),
            poisoned: Arc::new(AtomicBool::new(false)),
            identity: Arc::new(TrailIdentity),
        }
    }

    /// Resumes sequence allocation from a previously persisted sink.
    ///
    /// # Errors
    ///
    /// Returns `audit_unavailable` if existing records cannot be queried or are out of order.
    pub async fn resume(sink: Arc<dyn ReliableAuditSink>) -> Result<Self, ControlError> {
        let records = tokio::time::timeout(AUDIT_TRANSACTION_TIMEOUT, sink.records())
            .await
            .map_err(|_| ControlError::audit_unavailable())??;
        let state = recover_audit_state(&records)?;
        Ok(Self {
            sink,
            state: Arc::new(Mutex::new(state)),
            poisoned: Arc::new(AtomicBool::new(false)),
            identity: Arc::new(TrailIdentity),
        })
    }

    pub async fn start(
        &self,
        operation: ControlOperation,
        cluster: &ClusterName,
        dry_run: bool,
    ) -> Result<AuditInvocation, ControlError> {
        self.ensure_available()?;
        let mut state = self.state.lock().await;
        self.ensure_available()?;
        let sequence = state
            .sequence
            .checked_add(1)
            .ok_or_else(ControlError::audit_unavailable)?;
        let invocation = AuditInvocation {
            id: AuditInvocationId(sequence),
            operation,
            cluster: cluster.clone(),
            dry_run,
            trail_identity: self.identity.clone(),
        };
        let record = AuditRecord {
            schema_version: AUDIT_SCHEMA_VERSION.to_string(),
            sequence,
            invocation_id: invocation.id,
            timestamp_unix_millis: timestamp_unix_millis()?,
            event: AuditEvent::Started,
            operation,
            cluster: cluster.clone(),
            dry_run,
            error_code: None,
        };
        self.append_record(&record).await?;
        state.sequence = sequence;
        state.invocations.insert(
            invocation.id,
            RecoveredInvocation {
                operation,
                cluster: cluster.clone(),
                dry_run,
                terminal: false,
            },
        );
        Ok(invocation)
    }

    pub async fn terminal(
        &self,
        invocation: &AuditInvocation,
        error_code: Option<ControlErrorCode>,
    ) -> Result<(), ControlError> {
        self.ensure_available()?;
        if !Arc::ptr_eq(&self.identity, &invocation.trail_identity) {
            return Err(ControlError::audit_unavailable());
        }
        let mut state = self.state.lock().await;
        self.ensure_available()?;
        let recovered = state
            .invocations
            .get(&invocation.id)
            .ok_or_else(ControlError::audit_unavailable)?;
        if recovered.terminal
            || recovered.operation != invocation.operation
            || recovered.cluster != invocation.cluster
            || recovered.dry_run != invocation.dry_run
        {
            return Err(ControlError::audit_unavailable());
        }
        let sequence = state
            .sequence
            .checked_add(1)
            .ok_or_else(ControlError::audit_unavailable)?;
        let record = AuditRecord {
            schema_version: AUDIT_SCHEMA_VERSION.to_string(),
            sequence,
            invocation_id: invocation.id,
            timestamp_unix_millis: timestamp_unix_millis()?,
            event: if error_code.is_some() {
                AuditEvent::Failed
            } else {
                AuditEvent::Completed
            },
            operation: invocation.operation,
            cluster: invocation.cluster.clone(),
            dry_run: invocation.dry_run,
            error_code,
        };
        self.append_record(&record).await?;
        state.sequence = sequence;
        if let Some(recovered) = state.invocations.get_mut(&invocation.id) {
            recovered.terminal = true;
        }
        Ok(())
    }

    pub async fn records(&self) -> Result<Vec<AuditRecord>, ControlError> {
        self.ensure_available()?;
        tokio::time::timeout(AUDIT_TRANSACTION_TIMEOUT, self.sink.records())
            .await
            .map_err(|_| ControlError::audit_unavailable())?
    }

    async fn append_record(&self, record: &AuditRecord) -> Result<(), ControlError> {
        let poison = PoisonOnDrop::new(&self.poisoned);
        match tokio::time::timeout(AUDIT_TRANSACTION_TIMEOUT, self.sink.append(record)).await {
            Ok(Ok(())) => {
                poison.disarm();
                Ok(())
            }
            Ok(Err(error)) => Err(error),
            Err(_) => Err(ControlError::audit_unavailable()),
        }
    }

    fn ensure_available(&self) -> Result<(), ControlError> {
        if self.poisoned.load(Ordering::Acquire) {
            Err(ControlError::audit_unavailable())
        } else {
            Ok(())
        }
    }
}

fn recover_audit_state(records: &[AuditRecord]) -> Result<AuditTrailState, ControlError> {
    let mut sequence = 0;
    let mut invocations = BTreeMap::new();
    for record in records {
        if record.schema_version != AUDIT_SCHEMA_VERSION || record.sequence <= sequence {
            return Err(ControlError::audit_unavailable());
        }
        match record.event {
            AuditEvent::Started => {
                if record.invocation_id.0 != record.sequence
                    || record.error_code.is_some()
                    || invocations
                        .insert(
                            record.invocation_id,
                            RecoveredInvocation {
                                operation: record.operation,
                                cluster: record.cluster.clone(),
                                dry_run: record.dry_run,
                                terminal: false,
                            },
                        )
                        .is_some()
                {
                    return Err(ControlError::audit_unavailable());
                }
            }
            AuditEvent::Completed | AuditEvent::Failed => {
                let recovered = invocations
                    .get_mut(&record.invocation_id)
                    .ok_or_else(ControlError::audit_unavailable)?;
                if recovered.terminal
                    || recovered.operation != record.operation
                    || recovered.cluster != record.cluster
                    || recovered.dry_run != record.dry_run
                    || matches!(record.event, AuditEvent::Completed) != record.error_code.is_none()
                {
                    return Err(ControlError::audit_unavailable());
                }
                recovered.terminal = true;
            }
        }
        sequence = record.sequence;
    }
    Ok(AuditTrailState { sequence, invocations })
}

fn timestamp_unix_millis() -> Result<u64, ControlError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ControlError::audit_unavailable())?
        .as_millis()
        .try_into()
        .map_err(|_| ControlError::audit_unavailable())
}

pub struct MemoryAuditSink {
    state: Mutex<MemoryAuditState>,
    capacity: usize,
    max_record_bytes: usize,
    max_file_bytes: Option<u64>,
    reject_writes: bool,
}

struct MemoryAuditState {
    records: Vec<AuditRecord>,
    bytes_used: u64,
}

impl MemoryAuditSink {
    pub fn new(capacity: usize, max_record_bytes: usize) -> Self {
        Self {
            state: Mutex::new(MemoryAuditState {
                records: Vec::new(),
                bytes_used: 0,
            }),
            capacity,
            max_record_bytes,
            max_file_bytes: audit_file_limit(capacity, max_record_bytes).ok(),
            reject_writes: false,
        }
    }

    pub fn failing(capacity: usize, max_record_bytes: usize) -> Self {
        Self {
            state: Mutex::new(MemoryAuditState {
                records: Vec::new(),
                bytes_used: 0,
            }),
            capacity,
            max_record_bytes,
            max_file_bytes: audit_file_limit(capacity, max_record_bytes).ok(),
            reject_writes: true,
        }
    }
}

impl ReliableAuditSink for MemoryAuditSink {
    fn append<'a>(&'a self, record: &'a AuditRecord) -> AuditFuture<'a, Result<(), ControlError>> {
        Box::pin(async move {
            let encoded = encode_record(record, self.max_record_bytes)?;
            let encoded_len = u64::try_from(encoded.len()).map_err(|_| ControlError::audit_unavailable())?;
            let mut state = self.state.lock().await;
            let next_bytes = state
                .bytes_used
                .checked_add(encoded_len)
                .ok_or_else(ControlError::audit_unavailable)?;
            if self.reject_writes
                || state.records.len() >= self.capacity
                || encoded.is_empty()
                || self.max_file_bytes.is_none_or(|limit| next_bytes > limit)
            {
                return Err(ControlError::audit_unavailable());
            }
            state.records.push(record.clone());
            state.bytes_used = next_bytes;
            Ok(())
        })
    }

    fn records(&self) -> AuditFuture<'_, Result<Vec<AuditRecord>, ControlError>> {
        Box::pin(async move {
            if self.max_file_bytes.is_none() {
                return Err(ControlError::audit_unavailable());
            }
            Ok(self.state.lock().await.records.clone())
        })
    }
}

pub struct JsonlAuditSink {
    writer: Arc<dyn DurableAuditWriter>,
    state: Mutex<JsonlState>,
    capacity: usize,
    max_record_bytes: usize,
    max_file_bytes: u64,
    poisoned: AtomicBool,
}

struct JsonlState {
    records: Vec<AuditRecord>,
    bytes_used: u64,
}

trait DurableAuditWriter: Send + Sync {
    fn append<'a>(&'a self, encoded: &'a [u8]) -> AuditFuture<'a, Result<(), ControlError>>;
    fn flush(&self) -> AuditFuture<'_, Result<(), ControlError>>;
    fn sync(&self) -> AuditFuture<'_, Result<(), ControlError>>;
}

struct TokioAuditWriter {
    file: Mutex<tokio::fs::File>,
}

impl DurableAuditWriter for TokioAuditWriter {
    fn append<'a>(&'a self, encoded: &'a [u8]) -> AuditFuture<'a, Result<(), ControlError>> {
        Box::pin(async move {
            let mut file = self.file.lock().await;
            file.write_all(encoded)
                .await
                .map_err(|_| ControlError::audit_unavailable())
        })
    }

    fn flush(&self) -> AuditFuture<'_, Result<(), ControlError>> {
        Box::pin(async move {
            self.file
                .lock()
                .await
                .flush()
                .await
                .map_err(|_| ControlError::audit_unavailable())
        })
    }

    fn sync(&self) -> AuditFuture<'_, Result<(), ControlError>> {
        Box::pin(async move {
            self.file
                .lock()
                .await
                .sync_data()
                .await
                .map_err(|_| ControlError::audit_unavailable())
        })
    }
}

impl JsonlAuditSink {
    /// Opens a bounded JSONL sink and loads existing valid records for queries.
    ///
    /// # Errors
    ///
    /// Returns `audit_unavailable` if the file cannot be opened or safely loaded.
    pub async fn open(path: impl AsRef<Path>, capacity: usize, max_record_bytes: usize) -> Result<Self, ControlError> {
        let path = path.as_ref();
        let max_file_bytes = audit_file_limit(capacity, max_record_bytes)?;
        let metadata = match tokio::fs::metadata(path).await {
            Ok(metadata) => Some(metadata),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(_) => return Err(ControlError::audit_unavailable()),
        };
        let bytes_used = metadata.as_ref().map_or(0, std::fs::Metadata::len);
        if bytes_used > max_file_bytes {
            return Err(ControlError::audit_unavailable());
        }
        let existing = if metadata.is_some() {
            parse_existing_file(path, capacity, max_record_bytes, max_file_bytes).await?
        } else {
            Vec::new()
        };
        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await
            .map_err(|_| ControlError::audit_unavailable())?;
        Ok(Self {
            writer: Arc::new(TokioAuditWriter { file: Mutex::new(file) }),
            state: Mutex::new(JsonlState {
                records: existing,
                bytes_used,
            }),
            capacity,
            max_record_bytes,
            max_file_bytes,
            poisoned: AtomicBool::new(false),
        })
    }

    #[cfg(test)]
    fn with_writer(
        writer: Arc<dyn DurableAuditWriter>,
        capacity: usize,
        max_record_bytes: usize,
    ) -> Result<Self, ControlError> {
        Ok(Self {
            writer,
            state: Mutex::new(JsonlState {
                records: Vec::new(),
                bytes_used: 0,
            }),
            capacity,
            max_record_bytes,
            max_file_bytes: audit_file_limit(capacity, max_record_bytes)?,
            poisoned: AtomicBool::new(false),
        })
    }
}

impl ReliableAuditSink for JsonlAuditSink {
    fn append<'a>(&'a self, record: &'a AuditRecord) -> AuditFuture<'a, Result<(), ControlError>> {
        Box::pin(async move {
            if self.poisoned.load(Ordering::Acquire) {
                return Err(ControlError::audit_unavailable());
            }
            let encoded = encode_record(record, self.max_record_bytes)?;
            let encoded_len =
                u64::try_from(encoded.len().saturating_add(1)).map_err(|_| ControlError::audit_unavailable())?;
            let mut state = self.state.lock().await;
            let next_bytes = state
                .bytes_used
                .checked_add(encoded_len)
                .ok_or_else(ControlError::audit_unavailable)?;
            if self.poisoned.load(Ordering::Acquire)
                || state.records.len() >= self.capacity
                || next_bytes > self.max_file_bytes
            {
                return Err(ControlError::audit_unavailable());
            }
            let poison = PoisonOnDrop::new(&self.poisoned);
            if self.writer.append(&encoded).await.is_err()
                || self.writer.flush().await.is_err()
                || self.writer.sync().await.is_err()
                || self.writer.append(b"\n").await.is_err()
                || self.writer.flush().await.is_err()
                || self.writer.sync().await.is_err()
            {
                return Err(ControlError::audit_unavailable());
            }
            state.bytes_used = next_bytes;
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
}

fn encode_record(record: &AuditRecord, max_record_bytes: usize) -> Result<Vec<u8>, ControlError> {
    let encoded = serde_json::to_vec(record).map_err(|_| ControlError::audit_unavailable())?;
    if encoded.len() > max_record_bytes {
        return Err(ControlError::audit_unavailable());
    }
    Ok(encoded)
}

fn audit_file_limit(capacity: usize, max_record_bytes: usize) -> Result<u64, ControlError> {
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
) -> Result<Vec<AuditRecord>, ControlError> {
    let file = tokio::fs::File::open(path)
        .await
        .map_err(|_| ControlError::audit_unavailable())?;
    let mut reader = BufReader::new(file);
    let mut records = Vec::new();
    let mut last_sequence = 0;
    let mut invocations = std::collections::BTreeMap::new();
    let mut bytes_read = 0_u64;
    loop {
        let Some(mut line) = read_bounded_line(&mut reader, max_record_bytes).await? else {
            break;
        };
        let read = line.len();
        bytes_read = bytes_read
            .checked_add(u64::try_from(read).map_err(|_| ControlError::audit_unavailable())?)
            .ok_or_else(ControlError::audit_unavailable)?;
        if bytes_read > max_file_bytes || line.len() == 1 || records.len() >= capacity {
            return Err(ControlError::audit_unavailable());
        }
        line.pop();
        let record: AuditRecord = serde_json::from_slice(&line).map_err(|_| ControlError::audit_unavailable())?;
        if record.schema_version != AUDIT_SCHEMA_VERSION {
            return Err(ControlError::audit_unavailable());
        }
        if record.sequence <= last_sequence {
            return Err(ControlError::audit_unavailable());
        }
        match record.event {
            AuditEvent::Started => {
                if record.invocation_id.0 != record.sequence
                    || record.error_code.is_some()
                    || invocations
                        .insert(
                            record.invocation_id,
                            (record.operation, record.cluster.clone(), record.dry_run, false),
                        )
                        .is_some()
                {
                    return Err(ControlError::audit_unavailable());
                }
            }
            AuditEvent::Completed | AuditEvent::Failed => {
                let Some((operation, cluster, dry_run, terminal_seen)) = invocations.get_mut(&record.invocation_id)
                else {
                    return Err(ControlError::audit_unavailable());
                };
                if *terminal_seen
                    || *operation != record.operation
                    || cluster != &record.cluster
                    || *dry_run != record.dry_run
                    || matches!(record.event, AuditEvent::Completed) != record.error_code.is_none()
                {
                    return Err(ControlError::audit_unavailable());
                }
                *terminal_seen = true;
            }
        }
        last_sequence = record.sequence;
        records.push(record);
    }
    Ok(records)
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU8;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering as AtomicOrdering;
    use std::sync::Mutex as StdMutex;

    use futures_util::future::join_all;

    use super::*;

    #[tokio::test]
    async fn jsonl_sink_persists_queryable_ordered_bounded_records() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("control-audit.jsonl");
        let sink = Arc::new(JsonlAuditSink::open(&path, 16, 4096).await.unwrap());
        let audit = AuditTrail::new(sink.clone());
        let cluster = ClusterName::try_new("cluster-a").unwrap();
        let invocation = audit
            .start(ControlOperation::TopicUpsert, &cluster, true)
            .await
            .unwrap();
        audit.terminal(&invocation, None).await.unwrap();

        let records = sink.records().await.unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].sequence, 1);
        assert_eq!(records[1].sequence, 2);
        assert_eq!(records[0].invocation_id, records[1].invocation_id);
        let disk = tokio::fs::read_to_string(&path).await.unwrap();
        assert_eq!(disk.lines().count(), 2);
        for forbidden in [
            "Bearer",
            "access_key",
            "secret_key",
            "127.0.0.1",
            "operator@example.test",
            "request-1234",
            "raw backend",
        ] {
            assert!(!disk.contains(forbidden));
        }
        drop(audit);
        drop(sink);

        let resumed_sink = Arc::new(JsonlAuditSink::open(&path, 16, 4096).await.unwrap());
        let resumed = AuditTrail::resume(resumed_sink.clone()).await.unwrap();
        let invocation = resumed
            .start(ControlOperation::TopicUpsert, &cluster, true)
            .await
            .unwrap();
        resumed
            .terminal(&invocation, Some(ControlErrorCode::Conflict))
            .await
            .unwrap();
        let resumed_records = resumed_sink.records().await.unwrap();
        assert_eq!(resumed_records[2].sequence, 3);
        assert_eq!(resumed_records[3].sequence, 4);
        assert_eq!(resumed_records[2].invocation_id, resumed_records[3].invocation_id);
    }

    #[tokio::test]
    async fn bounded_sinks_fail_instead_of_dropping_records() {
        let sink = MemoryAuditSink::new(1, 4096);
        let cluster = ClusterName::try_new("cluster-a").unwrap();
        let record = AuditRecord {
            schema_version: AUDIT_SCHEMA_VERSION.to_string(),
            sequence: 1,
            invocation_id: AuditInvocationId(1),
            timestamp_unix_millis: 1,
            event: AuditEvent::Started,
            operation: ControlOperation::TopicUpsert,
            cluster,
            dry_run: true,
            error_code: None,
        };
        sink.append(&record).await.unwrap();
        assert_eq!(
            sink.append(&record).await.unwrap_err().code(),
            ControlErrorCode::AuditUnavailable
        );
    }

    #[tokio::test]
    async fn concurrent_invocations_keep_global_order_and_stable_links() {
        let sink = Arc::new(MemoryAuditSink::new(64, 4096));
        let audit = AuditTrail::new(sink.clone());
        let cluster = ClusterName::try_new("cluster-a").unwrap();
        join_all((0..16).map(|_| {
            let audit = audit.clone();
            let cluster = cluster.clone();
            async move {
                let invocation = audit
                    .start(ControlOperation::TopicUpsert, &cluster, true)
                    .await
                    .unwrap();
                audit.terminal(&invocation, None).await.unwrap();
            }
        }))
        .await;
        let records = sink.records().await.unwrap();
        assert_eq!(records.len(), 32);
        assert!(records.windows(2).all(|pair| pair[0].sequence < pair[1].sequence));
        for invocation_id in records
            .iter()
            .map(|record| record.invocation_id)
            .collect::<std::collections::BTreeSet<_>>()
        {
            let linked = records
                .iter()
                .filter(|record| record.invocation_id == invocation_id)
                .collect::<Vec<_>>();
            assert_eq!(linked.len(), 2);
            assert_eq!(linked[0].event, AuditEvent::Started);
            assert_eq!(linked[1].event, AuditEvent::Completed);
        }
    }

    #[tokio::test]
    async fn terminal_state_rejects_duplicate_unknown_and_cross_trail_tokens() {
        let cluster = ClusterName::try_new("cluster-a").unwrap();
        let sink = Arc::new(MemoryAuditSink::new(32, 4096));
        let audit = AuditTrail::new(sink.clone());

        let sequential = audit
            .start(ControlOperation::TopicUpsert, &cluster, true)
            .await
            .unwrap();
        audit.terminal(&sequential, None).await.unwrap();
        assert!(audit.terminal(&sequential, None).await.is_err());

        let concurrent = audit
            .start(ControlOperation::ConsumerGroupUpsert, &cluster, true)
            .await
            .unwrap();
        let (first, second) = tokio::join!(
            audit.terminal(&concurrent, None),
            audit.terminal(&concurrent, Some(ControlErrorCode::Conflict))
        );
        assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);

        let other_sink = Arc::new(MemoryAuditSink::new(8, 4096));
        let other = AuditTrail::new(other_sink);
        let other_invocation = other
            .start(ControlOperation::TopicUpsert, &cluster, true)
            .await
            .unwrap();
        assert!(other.terminal(&concurrent, None).await.is_err());
        other.terminal(&other_invocation, None).await.unwrap();

        let unknown = AuditInvocation {
            id: AuditInvocationId(u64::MAX),
            operation: ControlOperation::TopicUpsert,
            cluster,
            dry_run: true,
            trail_identity: audit.identity.clone(),
        };
        assert!(audit.terminal(&unknown, None).await.is_err());
        assert_eq!(sink.records().await.unwrap().len(), 4);
    }

    #[tokio::test]
    async fn restart_preserves_dangling_start_and_allocates_a_new_invocation() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("dangling.jsonl");
        let cluster = ClusterName::try_new("cluster-a").unwrap();
        let sink = Arc::new(JsonlAuditSink::open(&path, 16, 4096).await.unwrap());
        let audit = AuditTrail::new(sink.clone());
        let completed = audit
            .start(ControlOperation::ConsumerGroupUpsert, &cluster, true)
            .await
            .unwrap();
        audit.terminal(&completed, None).await.unwrap();
        let dangling = audit
            .start(ControlOperation::TopicUpsert, &cluster, true)
            .await
            .unwrap();
        drop(audit);
        drop(sink);

        let resumed_sink = Arc::new(JsonlAuditSink::open(&path, 16, 4096).await.unwrap());
        let resumed = AuditTrail::resume(resumed_sink.clone()).await.unwrap();
        {
            let recovered = resumed.state.lock().await;
            assert!(recovered.invocations[&completed.id()].terminal);
            assert!(!recovered.invocations[&dangling.id()].terminal);
        }
        let next = resumed
            .start(ControlOperation::ConsumerGroupUpsert, &cluster, true)
            .await
            .unwrap();
        resumed.terminal(&next, None).await.unwrap();
        assert!(next.id() > dangling.id());
        let records = resumed_sink.records().await.unwrap();
        assert_eq!(
            records
                .iter()
                .filter(|record| record.invocation_id == dangling.id())
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn metadata_cap_tail_and_corruption_fail_closed() {
        let directory = tempfile::tempdir().unwrap();
        let limit = audit_file_limit(16, 512).unwrap();
        assert_eq!(limit, 16 * 513);

        let sparse = directory.path().join("sparse.jsonl");
        let file = tokio::fs::File::create(&sparse).await.unwrap();
        file.set_len(limit + 1).await.unwrap();
        drop(file);
        assert!(JsonlAuditSink::open(&sparse, 16, 512).await.is_err());

        let cluster = ClusterName::try_new("cluster-a").unwrap();
        let record = AuditRecord {
            schema_version: AUDIT_SCHEMA_VERSION.to_string(),
            sequence: 1,
            invocation_id: AuditInvocationId(1),
            timestamp_unix_millis: 1,
            event: AuditEvent::Started,
            operation: ControlOperation::TopicUpsert,
            cluster,
            dry_run: true,
            error_code: None,
        };
        let tail = directory.path().join("tail.jsonl");
        let encoded = serde_json::to_vec(&record).unwrap();
        tokio::fs::write(&tail, &encoded).await.unwrap();
        assert!(JsonlAuditSink::open(&tail, 16, 4096).await.is_err());

        let exact = directory.path().join("exact.jsonl");
        let mut exact_line = encoded.clone();
        exact_line.push(b'\n');
        tokio::fs::write(&exact, exact_line).await.unwrap();
        assert!(JsonlAuditSink::open(&exact, 16, encoded.len()).await.is_ok());

        let plus_one = directory.path().join("plus-one.jsonl");
        let mut oversized_line = encoded.clone();
        oversized_line.extend_from_slice(b" \n");
        tokio::fs::write(&plus_one, oversized_line).await.unwrap();
        assert!(JsonlAuditSink::open(&plus_one, 16, encoded.len()).await.is_err());

        let corrupt = directory.path().join("corrupt.jsonl");
        tokio::fs::write(&corrupt, b"{not-json}\n").await.unwrap();
        assert!(JsonlAuditSink::open(&corrupt, 16, 4096).await.is_err());
    }

    #[tokio::test]
    async fn file_and_query_budgets_clamp_and_overflow_fail_closed() {
        assert_eq!(audit_file_limit(65_536, 16_384).unwrap(), MAX_AUDIT_FILE_BYTES);
        assert!(audit_file_limit(usize::MAX, 1).is_err());
        assert!(audit_file_limit(1, usize::MAX).is_err());

        let directory = tempfile::tempdir().unwrap();
        let oversized = directory.path().join("oversized.jsonl");
        let file = tokio::fs::File::create(&oversized).await.unwrap();
        file.set_len(MAX_AUDIT_FILE_BYTES + 1).await.unwrap();
        drop(file);
        assert!(JsonlAuditSink::open(&oversized, 65_536, 16_384).await.is_err());

        let cluster = ClusterName::try_new("cluster-a").unwrap();
        let record = AuditRecord {
            schema_version: AUDIT_SCHEMA_VERSION.to_string(),
            sequence: 1,
            invocation_id: AuditInvocationId(1),
            timestamp_unix_millis: 1,
            event: AuditEvent::Started,
            operation: ControlOperation::TopicUpsert,
            cluster,
            dry_run: true,
            error_code: None,
        };
        let encoded_len = u64::try_from(encode_record(&record, 4096).unwrap().len()).unwrap();
        let sink = MemoryAuditSink {
            state: Mutex::new(MemoryAuditState {
                records: Vec::new(),
                bytes_used: 0,
            }),
            capacity: 2,
            max_record_bytes: 4096,
            max_file_bytes: Some(encoded_len),
            reject_writes: false,
        };
        sink.append(&record).await.unwrap();
        assert!(sink.append(&record).await.is_err());
        let state = sink.state.lock().await;
        assert_eq!(state.records.len(), 1);
        assert!(state.bytes_used <= sink.max_file_bytes.unwrap());
    }

    #[derive(Clone, Copy)]
    enum FailureStage {
        Append,
        Flush,
        Sync,
    }

    struct StageFailWriter {
        stage: FailureStage,
        append_calls: AtomicUsize,
        flush_calls: AtomicUsize,
        sync_calls: AtomicUsize,
    }

    struct SwitchableHangWriter {
        stage: AtomicU8,
        buffer: StdMutex<Vec<u8>>,
        entered: AtomicUsize,
    }

    impl SwitchableHangWriter {
        fn new() -> Self {
            Self {
                stage: AtomicU8::new(0),
                buffer: StdMutex::new(Vec::new()),
                entered: AtomicUsize::new(0),
            }
        }

        fn hang_at(&self, stage: FailureStage) {
            self.stage.store(
                match stage {
                    FailureStage::Append => 1,
                    FailureStage::Flush => 2,
                    FailureStage::Sync => 3,
                },
                AtomicOrdering::SeqCst,
            );
        }

        fn stage(&self) -> u8 {
            self.stage.load(AtomicOrdering::SeqCst)
        }
    }

    impl DurableAuditWriter for SwitchableHangWriter {
        fn append<'a>(&'a self, encoded: &'a [u8]) -> AuditFuture<'a, Result<(), ControlError>> {
            Box::pin(async move {
                if self.stage() == 1 {
                    let prefix = encoded.len().min(8);
                    self.buffer.lock().unwrap().extend_from_slice(&encoded[..prefix]);
                    self.entered.fetch_add(1, AtomicOrdering::SeqCst);
                    std::future::pending().await
                } else {
                    self.buffer.lock().unwrap().extend_from_slice(encoded);
                    Ok(())
                }
            })
        }

        fn flush(&self) -> AuditFuture<'_, Result<(), ControlError>> {
            Box::pin(async move {
                if self.stage() == 2 {
                    self.entered.fetch_add(1, AtomicOrdering::SeqCst);
                    std::future::pending().await
                } else {
                    Ok(())
                }
            })
        }

        fn sync(&self) -> AuditFuture<'_, Result<(), ControlError>> {
            Box::pin(async move {
                if self.stage() == 3 {
                    self.entered.fetch_add(1, AtomicOrdering::SeqCst);
                    std::future::pending().await
                } else {
                    Ok(())
                }
            })
        }
    }

    impl DurableAuditWriter for StageFailWriter {
        fn append<'a>(&'a self, _encoded: &'a [u8]) -> AuditFuture<'a, Result<(), ControlError>> {
            Box::pin(async move {
                self.append_calls.fetch_add(1, AtomicOrdering::SeqCst);
                if matches!(self.stage, FailureStage::Append) {
                    Err(ControlError::audit_unavailable())
                } else {
                    Ok(())
                }
            })
        }

        fn flush(&self) -> AuditFuture<'_, Result<(), ControlError>> {
            Box::pin(async move {
                self.flush_calls.fetch_add(1, AtomicOrdering::SeqCst);
                if matches!(self.stage, FailureStage::Flush) {
                    Err(ControlError::audit_unavailable())
                } else {
                    Ok(())
                }
            })
        }

        fn sync(&self) -> AuditFuture<'_, Result<(), ControlError>> {
            Box::pin(async move {
                self.sync_calls.fetch_add(1, AtomicOrdering::SeqCst);
                if matches!(self.stage, FailureStage::Sync) {
                    Err(ControlError::audit_unavailable())
                } else {
                    Ok(())
                }
            })
        }
    }

    #[tokio::test]
    async fn append_flush_and_sync_failures_poison_queries() {
        let cluster = ClusterName::try_new("cluster-a").unwrap();
        for stage in [FailureStage::Append, FailureStage::Flush, FailureStage::Sync] {
            let writer = Arc::new(StageFailWriter {
                stage,
                append_calls: AtomicUsize::new(0),
                flush_calls: AtomicUsize::new(0),
                sync_calls: AtomicUsize::new(0),
            });
            let sink = Arc::new(JsonlAuditSink::with_writer(writer.clone(), 16, 4096).unwrap());
            let audit = AuditTrail::new(sink.clone());
            assert!(audit
                .start(ControlOperation::TopicUpsert, &cluster, true)
                .await
                .is_err());
            assert_eq!(writer.append_calls.load(AtomicOrdering::SeqCst), 1);
            assert_eq!(
                writer.flush_calls.load(AtomicOrdering::SeqCst),
                usize::from(!matches!(stage, FailureStage::Append))
            );
            assert_eq!(
                writer.sync_calls.load(AtomicOrdering::SeqCst),
                usize::from(matches!(stage, FailureStage::Sync))
            );
            assert!(sink.records().await.is_err());
        }
    }

    #[tokio::test(start_paused = true)]
    async fn hanging_terminal_transactions_poison_and_leave_no_recoverable_partial_record() {
        let cluster = ClusterName::try_new("cluster-a").unwrap();
        for stage in [FailureStage::Append, FailureStage::Flush, FailureStage::Sync] {
            let writer = Arc::new(SwitchableHangWriter::new());
            let sink = Arc::new(JsonlAuditSink::with_writer(writer.clone(), 16, 4096).unwrap());
            let audit = AuditTrail::new(sink.clone());
            let invocation = audit
                .start(ControlOperation::TopicUpsert, &cluster, true)
                .await
                .unwrap();
            writer.hang_at(stage);
            assert_eq!(
                audit.terminal(&invocation, None).await.unwrap_err().code(),
                ControlErrorCode::AuditUnavailable
            );
            assert!(!audit.state.lock().await.invocations[&invocation.id()].terminal);
            assert!(audit.records().await.is_err());
            assert!(audit
                .start(ControlOperation::TopicUpsert, &cluster, true)
                .await
                .is_err());

            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("partial.jsonl");
            let bytes = writer.buffer.lock().unwrap().clone();
            tokio::fs::write(&path, bytes).await.unwrap();
            assert!(JsonlAuditSink::open(&path, 16, 4096).await.is_err());
        }
    }

    #[tokio::test]
    async fn dropping_a_hanging_audit_caller_permanently_poisoned_the_transaction() {
        let cluster = ClusterName::try_new("cluster-a").unwrap();
        let writer = Arc::new(SwitchableHangWriter::new());
        let sink = Arc::new(JsonlAuditSink::with_writer(writer.clone(), 16, 4096).unwrap());
        let audit = AuditTrail::new(sink);
        let invocation = audit
            .start(ControlOperation::TopicUpsert, &cluster, true)
            .await
            .unwrap();
        writer.hang_at(FailureStage::Append);
        let task = tokio::spawn({
            let audit = audit.clone();
            async move { audit.terminal(&invocation, None).await }
        });
        while writer.entered.load(AtomicOrdering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert!(audit.records().await.is_err());
        assert!(audit
            .start(ControlOperation::TopicUpsert, &cluster, true)
            .await
            .is_err());
    }
}
