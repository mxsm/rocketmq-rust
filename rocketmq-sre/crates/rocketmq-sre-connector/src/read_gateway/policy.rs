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
use std::collections::BTreeSet;
use std::collections::VecDeque;
use std::time::Duration;
use std::time::Instant;

use chrono::Utc;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::TenantId;
use tokio::sync::Mutex;
use tokio::sync::Semaphore;
use tokio::sync::SemaphorePermit;

use super::ReadContext;
use crate::ConnectorConfig;
use crate::ConnectorError;
use crate::ConnectorErrorCode;

pub(crate) struct ReadPolicy {
    tenant_id: TenantId,
    cluster_allowlist: BTreeSet<String>,
    cluster_ids: BTreeMap<String, ClusterId>,
    concurrency: Semaphore,
    recent: Mutex<VecDeque<Instant>>,
    max_per_minute: usize,
    pub(crate) max_rows: usize,
    pub(crate) max_bytes: usize,
    max_time_range: Duration,
    max_deadline: Duration,
    pseudonymization_key: Vec<u8>,
}

impl ReadPolicy {
    pub(crate) fn from_config(config: &ConnectorConfig) -> Self {
        Self {
            tenant_id: config.tenant_id,
            cluster_allowlist: config.cluster_allowlist.clone(),
            cluster_ids: config.cluster_ids.clone(),
            concurrency: Semaphore::new(config.source_limits.max_concurrency),
            recent: Mutex::new(VecDeque::with_capacity(
                config.source_limits.max_requests_per_minute.min(1024),
            )),
            max_per_minute: config.source_limits.max_requests_per_minute,
            max_rows: config.source_limits.max_rows,
            max_bytes: config.source_limits.max_bytes,
            max_time_range: config.source_limits.max_time_range,
            max_deadline: config.source_limits.max_deadline,
            pseudonymization_key: config.pseudonymization_key().to_vec(),
        }
    }

    #[cfg(test)]
    pub(crate) fn for_test(
        tenant_id: TenantId,
        cluster_id: ClusterId,
        max_concurrency: usize,
        max_per_minute: usize,
    ) -> Self {
        Self {
            tenant_id,
            cluster_allowlist: BTreeSet::from(["local".to_owned()]),
            cluster_ids: BTreeMap::from([("local".to_owned(), cluster_id)]),
            concurrency: Semaphore::new(max_concurrency),
            recent: Mutex::new(VecDeque::with_capacity(max_per_minute.min(1024))),
            max_per_minute,
            max_rows: 8,
            max_bytes: 4096,
            max_time_range: Duration::from_secs(60),
            max_deadline: Duration::from_secs(5),
            pseudonymization_key: b"read-gateway-test-key".to_vec(),
        }
    }

    pub(crate) fn authorize(&self, context: &ReadContext<'_>) -> Result<(), ConnectorError> {
        if context.tenant_id != self.tenant_id {
            return Err(scoped_error(
                context,
                ConnectorErrorCode::TenantMismatch,
                "read context tenant differs from the connector boundary",
            ));
        }
        if self.cluster_ids.get(context.external_cluster) != Some(&context.cluster_id)
            || !self.cluster_allowlist.contains(context.external_cluster)
        {
            return Err(scoped_error(
                context,
                ConnectorErrorCode::ClusterNotAllowed,
                "read context cluster differs from the connector boundary",
            ));
        }
        if context.subject.trim().is_empty() || context.subject.len() > 256 {
            return Err(scoped_error(
                context,
                ConnectorErrorCode::UnauthorizedScope,
                "read context subject is missing or invalid",
            ));
        }
        if context.time_range_start > context.time_range_end
            || context
                .time_range_end
                .signed_duration_since(context.time_range_start)
                .to_std()
                .map_or(true, |duration| duration > self.max_time_range)
        {
            return Err(scoped_error(
                context,
                ConnectorErrorCode::InvalidEvidenceQuery,
                "read context time range exceeds the configured bound",
            ));
        }
        let now = Utc::now();
        let remaining = context.deadline.signed_duration_since(now).to_std().map_err(|_| {
            scoped_error(
                context,
                ConnectorErrorCode::DeadlineExceeded,
                "read context deadline elapsed",
            )
        })?;
        if remaining.is_zero() {
            return Err(scoped_error(
                context,
                ConnectorErrorCode::DeadlineExceeded,
                "read context deadline elapsed",
            ));
        }
        if remaining > self.max_deadline {
            return Err(scoped_error(
                context,
                ConnectorErrorCode::InvalidEvidenceQuery,
                "read context deadline exceeds the configured bound",
            ));
        }
        if context.cancel.is_cancelled() {
            return Err(scoped_error(
                context,
                ConnectorErrorCode::QueryCancelled,
                "read context was cancelled before admission",
            ));
        }
        Ok(())
    }

    pub(crate) async fn enter(&self, context: &ReadContext<'_>) -> Result<SemaphorePermit<'_>, ConnectorError> {
        let now = Instant::now();
        {
            let mut recent = self.recent.lock().await;
            while recent
                .front()
                .is_some_and(|instant| now.duration_since(*instant) >= Duration::from_secs(60))
            {
                recent.pop_front();
            }
            if recent.len() >= self.max_per_minute {
                return Err(scoped_error(
                    context,
                    ConnectorErrorCode::RateLimited,
                    "read gateway rate budget is exhausted",
                ));
            }
            recent.push_back(now);
        }
        self.concurrency.try_acquire().map_err(|_| {
            scoped_error(
                context,
                ConnectorErrorCode::RateLimited,
                "read gateway concurrency budget is exhausted",
            )
        })
    }

    pub(crate) fn validate_completion(&self, context: &ReadContext<'_>) -> Result<(), ConnectorError> {
        if context.cancel.is_cancelled() {
            return Err(scoped_error(
                context,
                ConnectorErrorCode::QueryCancelled,
                "read context was cancelled before completion",
            ));
        }
        if Utc::now() >= context.deadline {
            return Err(scoped_error(
                context,
                ConnectorErrorCode::DeadlineExceeded,
                "read context deadline elapsed before completion",
            ));
        }
        Ok(())
    }

    pub(crate) fn pseudonymization_key(&self) -> &[u8] {
        &self.pseudonymization_key
    }
}

fn scoped_error(context: &ReadContext<'_>, code: ConnectorErrorCode, detail: &'static str) -> ConnectorError {
    ConnectorError::new(
        code,
        matches!(
            code,
            ConnectorErrorCode::DeadlineExceeded | ConnectorErrorCode::RateLimited
        ),
        detail,
    )
    .with_correlation_id(context.correlation_id)
}
