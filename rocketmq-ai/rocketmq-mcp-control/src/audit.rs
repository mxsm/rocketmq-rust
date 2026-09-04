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

use self::recovery::duration_millis;
use self::recovery::recover_audit_state;
use self::recovery::terminal_event;
use self::recovery::timestamp_unix_millis;
use self::recovery::validate_terminal;

pub const AUDIT_SCHEMA_VERSION: &str = "rocketmq-mcp-control.audit.v2";
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, JsonSchema)]
pub enum AuditSchemaVersion {
    V1,
    V2,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AuditMode {
    DryRun,
    Execute,
}

impl AuditMode {
    const fn from_dry_run(dry_run: bool) -> Self {
        if dry_run {
            Self::DryRun
        } else {
            Self::Execute
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AuditResult {
    Started,
    Planned,
    Applied,
    Partial,
    Conflict,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize, JsonSchema)]
#[serde(transparent)]
pub struct AuditInvocationId(u64);

#[derive(Clone, PartialEq, Eq, JsonSchema)]
pub struct AuditRecord {
    pub schema_version: AuditSchemaVersion,
    pub sequence: u64,
    pub invocation_id: AuditInvocationId,
    pub timestamp_unix_millis: u64,
    pub event: AuditEvent,
    pub operation: ControlOperation,
    pub cluster: ClusterName,
    pub operator: Option<String>,
    pub reason: Option<String>,
    pub mode: AuditMode,
    pub result: AuditResult,
    pub error_code: Option<ControlErrorCode>,
    pub duration_millis: Option<u64>,
}

impl std::fmt::Debug for AuditRecord {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AuditRecord")
            .field("schema_version", &self.schema_version)
            .field("sequence", &self.sequence)
            .field("invocation_id", &self.invocation_id)
            .field("timestamp_unix_millis", &self.timestamp_unix_millis)
            .field("event", &self.event)
            .field("operation", &self.operation)
            .field("cluster", &self.cluster)
            .field("identity_recorded", &self.operator.is_some())
            .field("reason_recorded", &self.reason.is_some())
            .field("mode", &self.mode)
            .field("result", &self.result)
            .field("error_code", &self.error_code)
            .field("duration_millis", &self.duration_millis)
            .finish()
    }
}

mod recovery;
mod wire;

#[derive(Clone, PartialEq, Eq)]
pub struct AuditContext {
    operator: String,
    reason: Option<String>,
}

impl AuditContext {
    /// Creates the identity evidence written only to the durable audit sink.
    ///
    /// # Errors
    ///
    /// Returns a closed authorization or argument error when either value is unsafe to persist.
    pub fn try_new(operator: &str, reason: Option<&str>) -> Result<Self, ControlError> {
        if !crate::model::valid_operator(operator) {
            return Err(ControlError::permission_denied());
        }
        if reason.is_some_and(|value| !crate::model::valid_reason(value)) {
            return Err(ControlError::invalid_argument());
        }
        Ok(Self {
            operator: operator.to_owned(),
            reason: reason.map(ToOwned::to_owned),
        })
    }
}

impl std::fmt::Debug for AuditContext {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AuditContext")
            .field("operator_validated", &true)
            .field("reason_recorded", &self.reason.is_some())
            .finish()
    }
}

#[derive(Clone)]
pub struct AuditInvocation {
    id: AuditInvocationId,
    operation: ControlOperation,
    cluster: ClusterName,
    context: AuditContext,
    mode: AuditMode,
    started_at: tokio::time::Instant,
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
    schema_version: AuditSchemaVersion,
    operation: ControlOperation,
    cluster: ClusterName,
    operator: Option<String>,
    reason: Option<String>,
    mode: AuditMode,
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
            .map_err(|_| ControlError::audit_unavailable())?
            .map_err(|_| ControlError::audit_unavailable())?;
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
        context: &AuditContext,
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
            context: context.clone(),
            mode: AuditMode::from_dry_run(dry_run),
            started_at: tokio::time::Instant::now(),
            trail_identity: self.identity.clone(),
        };
        let record = AuditRecord {
            schema_version: AuditSchemaVersion::V2,
            sequence,
            invocation_id: invocation.id,
            timestamp_unix_millis: timestamp_unix_millis()?,
            event: AuditEvent::Started,
            operation,
            cluster: cluster.clone(),
            operator: Some(context.operator.clone()),
            reason: context.reason.clone(),
            mode: invocation.mode,
            result: AuditResult::Started,
            error_code: None,
            duration_millis: None,
        };
        self.append_record(&record).await?;
        state.sequence = sequence;
        state.invocations.insert(
            invocation.id,
            RecoveredInvocation {
                schema_version: AuditSchemaVersion::V2,
                operation,
                cluster: cluster.clone(),
                operator: Some(context.operator.clone()),
                reason: context.reason.clone(),
                mode: invocation.mode,
                terminal: false,
            },
        );
        Ok(invocation)
    }

    pub async fn terminal(
        &self,
        invocation: &AuditInvocation,
        result: AuditResult,
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
            || recovered.schema_version != AuditSchemaVersion::V2
            || recovered.operation != invocation.operation
            || recovered.cluster != invocation.cluster
            || recovered.operator.as_deref() != Some(invocation.context.operator.as_str())
            || recovered.reason != invocation.context.reason
            || recovered.mode != invocation.mode
        {
            return Err(ControlError::audit_unavailable());
        }
        validate_terminal(result, error_code)?;
        let sequence = state
            .sequence
            .checked_add(1)
            .ok_or_else(ControlError::audit_unavailable)?;
        let record = AuditRecord {
            schema_version: AuditSchemaVersion::V2,
            sequence,
            invocation_id: invocation.id,
            timestamp_unix_millis: timestamp_unix_millis()?,
            event: terminal_event(result),
            operation: invocation.operation,
            cluster: invocation.cluster.clone(),
            operator: Some(invocation.context.operator.clone()),
            reason: invocation.context.reason.clone(),
            mode: invocation.mode,
            result,
            error_code,
            duration_millis: Some(duration_millis(invocation.started_at.elapsed())?),
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
            .map_err(|_| ControlError::audit_unavailable())
    }

    async fn append_record(&self, record: &AuditRecord) -> Result<(), ControlError> {
        let poison = PoisonOnDrop::new(&self.poisoned);
        match tokio::time::timeout(AUDIT_TRANSACTION_TIMEOUT, self.sink.append(record)).await {
            Ok(Ok(())) => {
                poison.disarm();
                Ok(())
            }
            Ok(Err(_)) => Err(ControlError::audit_unavailable()),
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
            if record.schema_version != AuditSchemaVersion::V2 {
                return Err(ControlError::audit_unavailable());
            }
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
            if record.schema_version != AuditSchemaVersion::V2 || self.poisoned.load(Ordering::Acquire) {
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
        records.push(record);
    }
    recover_audit_state(&records)?;
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
#[path = "audit/tests.rs"]
mod tests;
