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

use std::collections::VecDeque;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use async_trait::async_trait;
use serde::Serialize;
use tokio::sync::mpsc;
use tokio::sync::Notify;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;

use crate::config::AuditConfig;
use crate::guard::sanitizer::sanitize_text;
use crate::guard::GuardError;
use crate::guard::RiskLevel;

pub const AUDIT_SCHEMA_VERSION: u16 = 1;

const MAX_AUDIT_RECORDS: usize = 1024;
const MAX_REQUEST_ID_BYTES: usize = 128;
const MAX_IDENTITY_BYTES: usize = 256;
const MAX_TOOL_BYTES: usize = 256;
const MAX_ARGUMENTS_HASH_BYTES: usize = 128;
const MAX_ERROR_BYTES: usize = 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditStatus {
    Success,
    Failure,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AuditRecord {
    pub schema_version: u16,
    pub timestamp_unix_ms: u128,
    pub request_id: String,
    pub operator: String,
    pub client: Option<String>,
    pub cluster: Option<String>,
    pub tool: String,
    pub arguments_hash: String,
    pub risk_level: RiskLevel,
    pub status: AuditStatus,
    pub duration_ms: u128,
    pub error: Option<String>,
}

impl AuditRecord {
    #[allow(
        clippy::too_many_arguments,
        reason = "audit records are constructed from a complete decision"
    )]
    pub fn new(
        request_id: String,
        operator: String,
        client: Option<String>,
        cluster: Option<String>,
        tool: String,
        arguments_hash: String,
        risk_level: RiskLevel,
        status: AuditStatus,
        duration_ms: u128,
        error: Option<String>,
    ) -> Self {
        Self {
            schema_version: AUDIT_SCHEMA_VERSION,
            timestamp_unix_ms: now_unix_ms(),
            request_id,
            operator,
            client,
            cluster,
            tool,
            arguments_hash,
            risk_level,
            status,
            duration_ms,
            error,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuditMetrics {
    pub queued: u64,
    pub accepted: u64,
    pub written: u64,
    pub dropped: u64,
    pub oversized: u64,
    pub count_capacity_drops: u64,
    pub byte_capacity_drops: u64,
    pub closed_drops: u64,
    pub sink_failures: u64,
    pub flush_failures: u64,
    pub pending_records: u64,
    pub pending_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditDrainStatus {
    Disabled,
    Drained,
    TimedOut,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuditDrainReport {
    pub status: AuditDrainStatus,
    pub elapsed: Duration,
    pub accepted: u64,
    pub written: u64,
    pub dropped: u64,
    pub oversized: u64,
    pub count_capacity_drops: u64,
    pub byte_capacity_drops: u64,
    pub closed_drops: u64,
    pub sink_failures: u64,
    pub flush_failures: u64,
    pub pending_records: u64,
    pub pending_bytes: u64,
}

impl AuditDrainReport {
    pub fn is_healthy(&self) -> bool {
        matches!(self.status, AuditDrainStatus::Disabled | AuditDrainStatus::Drained)
            && self.pending_records == 0
            && self.pending_bytes == 0
            && self.dropped == 0
            && self.sink_failures == 0
            && self.flush_failures == 0
    }

    pub fn log_if_unhealthy(&self) {
        if !self.is_healthy() {
            tracing::warn!(
                status = ?self.status,
                accepted = self.accepted,
                written = self.written,
                dropped = self.dropped,
                oversized = self.oversized,
                sink_failures = self.sink_failures,
                flush_failures = self.flush_failures,
                pending_records = self.pending_records,
                pending_bytes = self.pending_bytes,
                "rocketmq-mcp audit drain report is unhealthy"
            );
        }
    }
}

#[derive(Debug, Default)]
struct AuditCounters {
    queued: AtomicU64,
    accepted: AtomicU64,
    written: AtomicU64,
    dropped: AtomicU64,
    oversized: AtomicU64,
    count_capacity_drops: AtomicU64,
    byte_capacity_drops: AtomicU64,
    closed_drops: AtomicU64,
    sink_failures: AtomicU64,
    flush_failures: AtomicU64,
    pending_records: AtomicU64,
    pending_bytes: AtomicU64,
}

#[derive(Debug)]
struct WriterCompletion {
    finished: AtomicBool,
    drained: AtomicBool,
    notify: Notify,
}

impl WriterCompletion {
    fn new() -> Self {
        Self {
            finished: AtomicBool::new(false),
            drained: AtomicBool::new(false),
            notify: Notify::new(),
        }
    }

    fn finish(&self, drained: bool) {
        if drained {
            self.drained.store(true, Ordering::Release);
        }
        self.finished.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }
}

struct CompletionGuard(Arc<WriterCompletion>);

impl Drop for CompletionGuard {
    fn drop(&mut self) {
        self.0.finish(false);
    }
}

#[derive(Debug, Default)]
enum AuditLifecycle {
    #[default]
    Unstarted,
    Disabled,
    Memory {
        closed: bool,
    },
    Running {
        sender: Option<mpsc::Sender<AuditEnvelope>>,
        byte_budget: Arc<Semaphore>,
        completion: Arc<WriterCompletion>,
    },
}

#[derive(Debug, Clone, Default)]
pub struct AuditLog {
    records: Arc<Mutex<VecDeque<AuditRecord>>>,
    lifecycle: Arc<Mutex<AuditLifecycle>>,
    counters: Arc<AuditCounters>,
}

impl AuditLog {
    pub fn start(
        &self,
        config: &AuditConfig,
        service_context: &rocketmq_runtime::ServiceContext,
    ) -> Result<(), GuardError> {
        let sink: Box<dyn AuditSink> = match config.sink.as_str() {
            "file" => Box::new(FileAuditSink {
                path: PathBuf::from(&config.path),
                blocking: service_context.blocking().clone(),
            }),
            "tracing" => Box::new(TracingAuditSink),
            "memory" => Box::new(MemoryAuditSink),
            _ => {
                return Err(GuardError::InvalidArgument("unsupported audit sink".to_string()));
            }
        };
        self.start_with_sink(config, service_context, sink)
    }

    pub fn record(&self, config: &AuditConfig, record: AuditRecord) {
        if !config.enabled {
            return;
        }

        let Some((record, encoded)) = self.prepare_record(config, record) else {
            return;
        };

        if config.sink == "memory" {
            let is_closed = self
                .lifecycle
                .lock()
                .map(|lifecycle| matches!(*lifecycle, AuditLifecycle::Memory { closed: true }))
                .unwrap_or(true);
            if is_closed {
                self.drop_closed();
            } else {
                self.accept_memory(record);
            }
            return;
        }

        let queue = self.lifecycle.lock().ok().and_then(|lifecycle| match &*lifecycle {
            AuditLifecycle::Running {
                sender: Some(sender),
                byte_budget,
                ..
            } => Some((sender.clone(), byte_budget.clone())),
            _ => None,
        });
        let Some((sender, byte_budget)) = queue else {
            self.drop_closed();
            return;
        };

        let encoded_len = encoded.len() as u64;
        let permit = match byte_budget.try_acquire_many_owned(encoded.len() as u32) {
            Ok(permit) => permit,
            Err(_) => {
                self.counters.dropped.fetch_add(1, Ordering::Relaxed);
                self.counters.byte_capacity_drops.fetch_add(1, Ordering::Relaxed);
                tracing::warn!("rocketmq-mcp audit byte capacity is full; audit record dropped");
                return;
            }
        };

        self.counters.pending_records.fetch_add(1, Ordering::Relaxed);
        self.counters.pending_bytes.fetch_add(encoded_len, Ordering::Relaxed);
        let envelope = AuditEnvelope {
            record,
            encoded,
            _byte_permit: permit,
            pending: PendingAuditRecord {
                counters: self.counters.clone(),
                encoded_len,
            },
        };

        match sender.try_send(envelope) {
            Ok(()) => {
                self.counters.queued.fetch_add(1, Ordering::Relaxed);
                self.counters.accepted.fetch_add(1, Ordering::Relaxed);
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.counters.dropped.fetch_add(1, Ordering::Relaxed);
                self.counters.count_capacity_drops.fetch_add(1, Ordering::Relaxed);
                tracing::warn!("rocketmq-mcp audit record capacity is full; audit record dropped");
            }
            Err(mpsc::error::TrySendError::Closed(_)) => self.drop_closed(),
        }
    }

    pub fn records(&self) -> Vec<AuditRecord> {
        self.records
            .lock()
            .map(|records| records.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub fn metrics(&self) -> AuditMetrics {
        AuditMetrics {
            queued: self.counters.queued.load(Ordering::Relaxed),
            accepted: self.counters.accepted.load(Ordering::Relaxed),
            written: self.counters.written.load(Ordering::Relaxed),
            dropped: self.counters.dropped.load(Ordering::Relaxed),
            oversized: self.counters.oversized.load(Ordering::Relaxed),
            count_capacity_drops: self.counters.count_capacity_drops.load(Ordering::Relaxed),
            byte_capacity_drops: self.counters.byte_capacity_drops.load(Ordering::Relaxed),
            closed_drops: self.counters.closed_drops.load(Ordering::Relaxed),
            sink_failures: self.counters.sink_failures.load(Ordering::Relaxed),
            flush_failures: self.counters.flush_failures.load(Ordering::Relaxed),
            pending_records: self.counters.pending_records.load(Ordering::Relaxed),
            pending_bytes: self.counters.pending_bytes.load(Ordering::Relaxed),
        }
    }

    pub async fn close_and_drain(&self, deadline: rocketmq_runtime::ShutdownDeadline) -> AuditDrainReport {
        let started_at = Instant::now();
        let completion = {
            let Ok(mut lifecycle) = self.lifecycle.lock() else {
                return self.drain_report(AuditDrainStatus::TimedOut, started_at.elapsed());
            };
            match &mut *lifecycle {
                AuditLifecycle::Running { sender, completion, .. } => {
                    sender.take();
                    Some(completion.clone())
                }
                AuditLifecycle::Memory { closed } => {
                    *closed = true;
                    return self.drain_report(AuditDrainStatus::Drained, started_at.elapsed());
                }
                AuditLifecycle::Unstarted | AuditLifecycle::Disabled => {
                    return self.drain_report(AuditDrainStatus::Disabled, started_at.elapsed());
                }
            }
        };

        let Some(completion) = completion else {
            return self.drain_report(AuditDrainStatus::TimedOut, started_at.elapsed());
        };
        let mut timed_out = false;
        while !completion.finished.load(Ordering::Acquire) {
            let notified = completion.notify.notified();
            if completion.finished.load(Ordering::Acquire) {
                break;
            }
            if deadline.is_expired()
                || tokio::time::timeout_at(tokio::time::Instant::from_std(deadline.instant()), notified)
                    .await
                    .is_err()
            {
                timed_out = true;
                break;
            }
        }

        let metrics = self.metrics();
        let status = if timed_out
            || !completion.drained.load(Ordering::Acquire)
            || metrics.pending_records > 0
            || metrics.pending_bytes > 0
        {
            AuditDrainStatus::TimedOut
        } else {
            AuditDrainStatus::Drained
        };
        self.drain_report(status, started_at.elapsed())
    }

    fn start_with_sink(
        &self,
        config: &AuditConfig,
        service_context: &rocketmq_runtime::ServiceContext,
        sink: Box<dyn AuditSink>,
    ) -> Result<(), GuardError> {
        validate_queue_config(config)?;
        let mut lifecycle = self
            .lifecycle
            .lock()
            .map_err(|_| GuardError::InvalidArgument("audit queue state is unavailable".to_string()))?;
        if !matches!(*lifecycle, AuditLifecycle::Unstarted) {
            return Ok(());
        }
        if !config.enabled {
            *lifecycle = AuditLifecycle::Disabled;
            return Ok(());
        }
        if config.sink == "memory" {
            *lifecycle = AuditLifecycle::Memory { closed: false };
            return Ok(());
        }

        let (sender, receiver) = mpsc::channel(config.queue_capacity);
        let byte_budget = Arc::new(Semaphore::new(config.queue_max_bytes));
        let completion = Arc::new(WriterCompletion::new());
        let counters = self.counters.clone();
        let records = self.records.clone();
        let writer_completion = completion.clone();
        service_context
            .task_group()
            .spawn_service("rocketmq-mcp-audit-writer", async move {
                run_audit_writer(receiver, sink, counters, records, writer_completion).await;
            })
            .map_err(|error| GuardError::InvalidArgument(format!("failed to start audit writer: {error}")))?;
        *lifecycle = AuditLifecycle::Running {
            sender: Some(sender),
            byte_budget,
            completion,
        };
        Ok(())
    }

    fn prepare_record(&self, config: &AuditConfig, record: AuditRecord) -> Option<(AuditRecord, Vec<u8>)> {
        let record = sanitize_record(record);
        let mut encoded = match serde_json::to_vec(&record) {
            Ok(encoded) => encoded,
            Err(_) => {
                self.counters.dropped.fetch_add(1, Ordering::Relaxed);
                self.counters.sink_failures.fetch_add(1, Ordering::Relaxed);
                tracing::warn!("rocketmq-mcp audit record serialization failed");
                return None;
            }
        };
        encoded.push(b'\n');
        if encoded.len() > config.max_record_bytes {
            self.counters.dropped.fetch_add(1, Ordering::Relaxed);
            self.counters.oversized.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(
                record_bytes = encoded.len(),
                max_record_bytes = config.max_record_bytes,
                "rocketmq-mcp oversized audit record dropped"
            );
            return None;
        }
        Some((record, encoded))
    }

    fn accept_memory(&self, record: AuditRecord) {
        push_record(&self.records, record);
        self.counters.accepted.fetch_add(1, Ordering::Relaxed);
        self.counters.written.fetch_add(1, Ordering::Relaxed);
    }

    fn drop_closed(&self) {
        self.counters.dropped.fetch_add(1, Ordering::Relaxed);
        self.counters.closed_drops.fetch_add(1, Ordering::Relaxed);
        tracing::warn!("rocketmq-mcp audit queue is closed; audit record dropped");
    }

    fn drain_report(&self, status: AuditDrainStatus, elapsed: Duration) -> AuditDrainReport {
        let metrics = self.metrics();
        AuditDrainReport {
            status,
            elapsed,
            accepted: metrics.accepted,
            written: metrics.written,
            dropped: metrics.dropped,
            oversized: metrics.oversized,
            count_capacity_drops: metrics.count_capacity_drops,
            byte_capacity_drops: metrics.byte_capacity_drops,
            closed_drops: metrics.closed_drops,
            sink_failures: metrics.sink_failures,
            flush_failures: metrics.flush_failures,
            pending_records: metrics.pending_records,
            pending_bytes: metrics.pending_bytes,
        }
    }
}

struct PendingAuditRecord {
    counters: Arc<AuditCounters>,
    encoded_len: u64,
}

impl Drop for PendingAuditRecord {
    fn drop(&mut self) {
        self.counters.pending_records.fetch_sub(1, Ordering::Relaxed);
        self.counters
            .pending_bytes
            .fetch_sub(self.encoded_len, Ordering::Relaxed);
    }
}

struct AuditEnvelope {
    record: AuditRecord,
    encoded: Vec<u8>,
    _byte_permit: OwnedSemaphorePermit,
    pending: PendingAuditRecord,
}

#[async_trait]
trait AuditSink: Send {
    async fn write(&mut self, record: &AuditRecord, encoded: &[u8]) -> Result<(), ()>;
    async fn flush(&mut self) -> Result<(), ()>;
}

struct FileAuditSink {
    path: PathBuf,
    blocking: rocketmq_runtime::BlockingExecutor,
}

#[async_trait]
impl AuditSink for FileAuditSink {
    async fn write(&mut self, _record: &AuditRecord, encoded: &[u8]) -> Result<(), ()> {
        let path = self.path.clone();
        let encoded = encoded.to_vec();
        self.blocking
            .spawn_io("rocketmq-mcp-audit-file-write", move || {
                write_file_record(path, &encoded)
            })
            .await
            .map_err(|_| ())?
    }

    async fn flush(&mut self) -> Result<(), ()> {
        let path = self.path.clone();
        self.blocking
            .spawn_io("rocketmq-mcp-audit-file-flush", move || flush_audit_file(path))
            .await
            .map_err(|_| ())?
    }
}

struct TracingAuditSink;

#[async_trait]
impl AuditSink for TracingAuditSink {
    async fn write(&mut self, record: &AuditRecord, _encoded: &[u8]) -> Result<(), ()> {
        tracing::info!(
            schema_version = record.schema_version,
            request_id = %record.request_id,
            operator = %record.operator,
            client = ?record.client,
            cluster = ?record.cluster,
            tool = %record.tool,
            risk_level = ?record.risk_level,
            status = ?record.status,
            duration_ms = record.duration_ms,
            "rocketmq-mcp audit record"
        );
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), ()> {
        Ok(())
    }
}

struct MemoryAuditSink;

#[async_trait]
impl AuditSink for MemoryAuditSink {
    async fn write(&mut self, _record: &AuditRecord, _encoded: &[u8]) -> Result<(), ()> {
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), ()> {
        Ok(())
    }
}

async fn run_audit_writer(
    mut receiver: mpsc::Receiver<AuditEnvelope>,
    mut sink: Box<dyn AuditSink>,
    counters: Arc<AuditCounters>,
    records: Arc<Mutex<VecDeque<AuditRecord>>>,
    completion: Arc<WriterCompletion>,
) {
    let completion = CompletionGuard(completion);
    while let Some(envelope) = receiver.recv().await {
        if sink.write(&envelope.record, &envelope.encoded).await.is_ok() {
            push_record(&records, envelope.record);
            counters.written.fetch_add(1, Ordering::Relaxed);
        } else {
            counters.sink_failures.fetch_add(1, Ordering::Relaxed);
            tracing::warn!("rocketmq-mcp asynchronous audit sink write failed");
        }
        drop(envelope.pending);
    }

    if sink.flush().await.is_err() {
        counters.flush_failures.fetch_add(1, Ordering::Relaxed);
        tracing::warn!("rocketmq-mcp audit sink flush failed");
    }
    completion.0.finish(true);
}

fn push_record(records: &Mutex<VecDeque<AuditRecord>>, record: AuditRecord) {
    let Ok(mut records) = records.lock() else {
        return;
    };
    if records.len() == MAX_AUDIT_RECORDS {
        records.pop_front();
    }
    records.push_back(record);
}

fn write_file_record(path: PathBuf, encoded: &[u8]) -> Result<(), ()> {
    if let Some(parent) = path.parent().filter(|parent| !parent.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent).map_err(|_| ())?;
    }
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|_| ())?;
    file.write_all(encoded).map_err(|_| ())
}

fn flush_audit_file(path: PathBuf) -> Result<(), ()> {
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|_| ())?;
    file.sync_all().map_err(|_| ())
}

fn sanitize_record(mut record: AuditRecord) -> AuditRecord {
    record.schema_version = AUDIT_SCHEMA_VERSION;
    record.request_id = sanitize_field(&record.request_id, MAX_REQUEST_ID_BYTES);
    record.operator = sanitize_field(&record.operator, MAX_IDENTITY_BYTES);
    record.client = record
        .client
        .as_deref()
        .map(|value| sanitize_field(value, MAX_IDENTITY_BYTES));
    record.cluster = record
        .cluster
        .as_deref()
        .map(|value| sanitize_field(value, MAX_IDENTITY_BYTES));
    record.tool = sanitize_field(&record.tool, MAX_TOOL_BYTES);
    record.arguments_hash = sanitize_field(&record.arguments_hash, MAX_ARGUMENTS_HASH_BYTES);
    record.error = record
        .error
        .as_deref()
        .map(|value| sanitize_field(value, MAX_ERROR_BYTES));
    record
}

fn sanitize_field(value: &str, max_bytes: usize) -> String {
    let sanitized = sanitize_text(value)
        .chars()
        .map(|character| if character.is_control() { '\u{fffd}' } else { character })
        .collect::<String>();
    truncate_utf8(&sanitized, max_bytes).to_string()
}

fn truncate_utf8(value: &str, max_bytes: usize) -> &str {
    if value.len() <= max_bytes {
        return value;
    }
    let mut end = max_bytes;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    &value[..end]
}

fn validate_queue_config(config: &AuditConfig) -> Result<(), GuardError> {
    if config.queue_capacity == 0 {
        return Err(GuardError::InvalidArgument(
            "audit queue capacity must be greater than zero".to_string(),
        ));
    }
    if config.max_record_bytes == 0 || config.queue_max_bytes < config.max_record_bytes {
        return Err(GuardError::InvalidArgument(
            "audit byte capacity must cover one non-empty record".to_string(),
        ));
    }
    if config.queue_max_bytes > u32::MAX as usize {
        return Err(GuardError::InvalidArgument(
            "audit byte capacity exceeds the supported maximum".to_string(),
        ));
    }
    Ok(())
}

fn now_unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests;
