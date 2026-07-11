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
use std::path::Path;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use serde::Serialize;
use tokio::sync::mpsc;

use crate::config::AuditConfig;
use crate::guard::GuardError;
use crate::guard::RiskLevel;

const MAX_AUDIT_RECORDS: usize = 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditStatus {
    Success,
    Failure,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AuditRecord {
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
    pub dropped: u64,
    pub sink_failures: u64,
}

#[derive(Debug, Default)]
struct AuditCounters {
    queued: AtomicU64,
    dropped: AtomicU64,
    sink_failures: AtomicU64,
}

#[derive(Debug, Clone, Default)]
pub struct AuditLog {
    records: Arc<Mutex<VecDeque<AuditRecord>>>,
    sender: Arc<Mutex<Option<mpsc::Sender<AuditRecord>>>>,
    counters: Arc<AuditCounters>,
}

impl AuditLog {
    pub fn start(
        &self,
        config: &AuditConfig,
        service_context: &rocketmq_runtime::ServiceContext,
    ) -> Result<(), GuardError> {
        if !config.enabled || config.sink == "memory" {
            return Ok(());
        }
        let (sender, receiver) = mpsc::channel(config.queue_capacity);
        let mut current_sender = self
            .sender
            .lock()
            .map_err(|_| GuardError::InvalidArgument("audit queue state is unavailable".to_string()))?;
        if current_sender.is_some() {
            return Ok(());
        }
        *current_sender = Some(sender);
        drop(current_sender);

        let config = config.clone();
        let counters = self.counters.clone();
        let blocking = service_context.blocking().clone();
        service_context
            .task_group()
            .spawn_service("rocketmq-mcp-audit-writer", async move {
                run_audit_writer(receiver, config, blocking, counters).await;
            })
            .map_err(|error| GuardError::InvalidArgument(format!("failed to start audit writer: {error}")))?;
        Ok(())
    }

    pub fn record(&self, config: &AuditConfig, record: AuditRecord) {
        self.push(record.clone());
        if let Some(sender) = self.sender.lock().ok().and_then(|sender| sender.clone()) {
            match sender.try_send(record) {
                Ok(()) => {
                    self.counters.queued.fetch_add(1, Ordering::Relaxed);
                }
                Err(mpsc::error::TrySendError::Full(_)) => {
                    self.counters.dropped.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!("rocketmq-mcp audit queue is full; audit record dropped");
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    self.counters.sink_failures.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!("rocketmq-mcp audit queue is closed; audit record dropped");
                }
            }
            return;
        }

        if let Err(error) = write_sink(config, &record) {
            self.counters.sink_failures.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(?error, "rocketmq-mcp audit sink write failed");
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
            dropped: self.counters.dropped.load(Ordering::Relaxed),
            sink_failures: self.counters.sink_failures.load(Ordering::Relaxed),
        }
    }

    fn push(&self, record: AuditRecord) {
        let Ok(mut records) = self.records.lock() else {
            return;
        };
        if records.len() == MAX_AUDIT_RECORDS {
            records.pop_front();
        }
        records.push_back(record);
    }
}

async fn run_audit_writer(
    mut receiver: mpsc::Receiver<AuditRecord>,
    config: AuditConfig,
    blocking: rocketmq_runtime::BlockingExecutor,
    counters: Arc<AuditCounters>,
) {
    while let Some(record) = receiver.recv().await {
        let config = config.clone();
        let write_result = match config.sink.as_str() {
            "file" => {
                let record = record.clone();
                blocking
                    .spawn_io("rocketmq-mcp-audit-file-write", move || {
                        write_file_record(&config, &record)
                    })
                    .await
                    .map_err(|error| error.to_string())
                    .and_then(|result| result)
            }
            _ => write_sink(&config, &record),
        };
        if let Err(error) = write_result {
            counters.sink_failures.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(?error, "rocketmq-mcp asynchronous audit sink write failed");
        }
    }
}

fn write_sink(config: &AuditConfig, record: &AuditRecord) -> Result<(), String> {
    match config.sink.as_str() {
        "memory" => Ok(()),
        "file" => write_file_record(config, record),
        "tracing" => {
            tracing::info!(
                request_id = %record.request_id,
                operator = %record.operator,
                client = ?record.client,
                tool = %record.tool,
                risk_level = ?record.risk_level,
                status = ?record.status,
                duration_ms = record.duration_ms,
                "rocketmq-mcp audit record"
            );
            Ok(())
        }
        sink => Err(format!("unknown rocketmq-mcp audit sink `{sink}`")),
    }
}

fn write_file_record(config: &AuditConfig, record: &AuditRecord) -> Result<(), String> {
    let path = Path::new(&config.path);
    if let Some(parent) = path.parent().filter(|parent| !parent.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent)
            .map_err(|error| format!("failed to create audit directory `{}`: {error}", parent.display()))?;
    }
    let line = serde_json::to_string(record).map_err(|error| format!("failed to serialize audit record: {error}"))?;
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|error| format!("failed to open audit file `{}`: {error}", config.path))?;
    writeln!(file, "{line}").map_err(|error| format!("failed to write audit file `{}`: {error}", config.path))
}

fn now_unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default()
}
