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
use std::sync::Arc;
use std::sync::Mutex;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use serde::Serialize;

use crate::config::AuditConfig;
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
    #[allow(clippy::too_many_arguments)]
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

#[derive(Debug, Clone, Default)]
pub struct AuditLog {
    records: Arc<Mutex<VecDeque<AuditRecord>>>,
}

impl AuditLog {
    pub fn record(&self, config: &AuditConfig, record: AuditRecord) {
        self.push(record.clone());

        match config.sink.as_str() {
            "memory" => {}
            "file" => write_file_record(config, &record),
            "tracing" => {
                tracing::info!(
                    request_id = %record.request_id,
                    tool = %record.tool,
                    risk_level = ?record.risk_level,
                    status = ?record.status,
                    duration_ms = record.duration_ms,
                    "rocketmq-mcp audit record"
                );
            }
            sink => tracing::warn!(sink, "unknown rocketmq-mcp audit sink"),
        }
    }

    pub fn records(&self) -> Vec<AuditRecord> {
        self.records
            .lock()
            .map(|records| records.iter().cloned().collect())
            .unwrap_or_default()
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

fn write_file_record(config: &AuditConfig, record: &AuditRecord) {
    if config.path.trim().is_empty() {
        tracing::warn!("rocketmq-mcp audit file sink has an empty path");
        return;
    }

    let path = Path::new(&config.path);
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            if let Err(error) = std::fs::create_dir_all(parent) {
                tracing::warn!(?error, path = %config.path, "failed to create rocketmq-mcp audit directory");
                return;
            }
        }
    }

    let line = match serde_json::to_string(record) {
        Ok(line) => line,
        Err(error) => {
            tracing::warn!(?error, "failed to serialize rocketmq-mcp audit record");
            return;
        }
    };

    match OpenOptions::new().create(true).append(true).open(path) {
        Ok(mut file) => {
            if let Err(error) = writeln!(file, "{line}") {
                tracing::warn!(?error, path = %config.path, "failed to write rocketmq-mcp audit record");
            }
        }
        Err(error) => {
            tracing::warn!(?error, path = %config.path, "failed to open rocketmq-mcp audit file");
        }
    }
}

fn now_unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default()
}
