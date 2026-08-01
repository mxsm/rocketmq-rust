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
use std::time::Duration;

use rocketmq_sre_contracts::CorrelationId;
use tokio::sync::Mutex;

use super::ReadAdapterKind;
use crate::ConnectorErrorCode;

const MAX_AUDIT_EVENTS: usize = 256;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReadAuditOutcome {
    Allowed,
    Denied,
    RateLimited,
    TimedOut,
    Cancelled,
    SourceFailed,
}

impl ReadAuditOutcome {
    pub(crate) fn from_error(code: ConnectorErrorCode) -> Self {
        match code {
            ConnectorErrorCode::UnauthorizedScope
            | ConnectorErrorCode::TenantMismatch
            | ConnectorErrorCode::ClusterNotAllowed
            | ConnectorErrorCode::InvalidEvidenceQuery => Self::Denied,
            ConnectorErrorCode::RateLimited => Self::RateLimited,
            ConnectorErrorCode::DeadlineExceeded => Self::TimedOut,
            ConnectorErrorCode::QueryCancelled => Self::Cancelled,
            _ => Self::SourceFailed,
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Allowed => "allowed",
            Self::Denied => "denied",
            Self::RateLimited => "rate_limited",
            Self::TimedOut => "timed_out",
            Self::Cancelled => "cancelled",
            Self::SourceFailed => "source_failed",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ReadAuditEvent {
    pub adapter: ReadAdapterKind,
    pub resource_class: &'static str,
    pub outcome: ReadAuditOutcome,
    pub latency_bucket: &'static str,
    pub correlation_id: CorrelationId,
}

#[derive(Default)]
pub(crate) struct ReadAudit {
    events: Mutex<VecDeque<ReadAuditEvent>>,
}

impl ReadAudit {
    pub(crate) async fn record(
        &self,
        adapter: ReadAdapterKind,
        resource_class: &'static str,
        outcome: ReadAuditOutcome,
        latency: Duration,
        correlation_id: CorrelationId,
    ) {
        let event = ReadAuditEvent {
            adapter,
            resource_class,
            outcome,
            latency_bucket: latency_bucket(latency),
            correlation_id,
        };
        tracing::info!(
            adapter = adapter.as_str(),
            resource_class,
            outcome = outcome.as_str(),
            latency_bucket = event.latency_bucket,
            correlation_id = %correlation_id,
            "read gateway request completed"
        );
        let mut events = self.events.lock().await;
        if events.len() == MAX_AUDIT_EVENTS {
            events.pop_front();
        }
        events.push_back(event);
    }

    #[cfg(test)]
    pub(crate) async fn events(&self) -> Vec<ReadAuditEvent> {
        self.events.lock().await.iter().cloned().collect()
    }
}

fn latency_bucket(latency: Duration) -> &'static str {
    match latency.as_millis() {
        0..=9 => "lt_10ms",
        10..=99 => "lt_100ms",
        100..=999 => "lt_1s",
        _ => "gte_1s",
    }
}
