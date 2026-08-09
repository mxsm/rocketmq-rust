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

use std::path::PathBuf;
use std::sync::Arc;

use rocketmq_model::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_model::common::message::MessageConst;
use rocketmq_model::common::message::MessageTrait;
use rocketmq_store_api::TimerEngineId;
use rocketmq_store_rocksdb::timer::timeline_index::RocksDbTimelineIndex;
use thiserror::Error;

use super::ShadowTimelineMaterializer;
use crate::config::timer_store_config::TimerStoreConfig;
use crate::timer::role::TimerRoleState;

const RECORD_OVERHEAD_RESERVE: u64 = 1_024;

/// Bytes-aware, fail-closed admission view over durable Timeline usage.
pub(crate) struct TimelineAdmissionController {
    config: TimerStoreConfig,
    admission_horizon_days: u16,
    store_root: PathBuf,
    timeline: Arc<RocksDbTimelineIndex>,
    materializer: Arc<ShadowTimelineMaterializer>,
    role: Arc<TimerRoleState>,
}

impl TimelineAdmissionController {
    pub(crate) fn new(
        config: TimerStoreConfig,
        admission_horizon_days: u16,
        store_root: PathBuf,
        timeline: Arc<RocksDbTimelineIndex>,
        materializer: Arc<ShadowTimelineMaterializer>,
        role: Arc<TimerRoleState>,
    ) -> Self {
        Self {
            config,
            admission_horizon_days,
            store_root,
            timeline,
            materializer,
            role,
        }
    }

    /// Validates a canonical Extended Timer before the source CommitLog append is acknowledged.
    pub(crate) fn check(&self, message: &MessageExtBrokerInner, now_ms: i64) -> Result<(), TimelineAdmissionError> {
        if message.property(MessageConst::TIMER_ENGINE_TYPE).as_deref()
            != Some(TimerEngineId::ExtendedTimeline.as_str())
        {
            return Ok(());
        }
        if !self.role.accepts_admission() {
            return Err(TimelineAdmissionError::RoleInactive);
        }
        let due_time_ms = message
            .property(MessageConst::PROPERTY_TIMER_ORIGINAL_DELIVER_MS)
            .or_else(|| message.property(MessageConst::PROPERTY_TIMER_DELIVER_MS))
            .and_then(|value| value.parse::<i64>().ok())
            .ok_or(TimelineAdmissionError::MalformedTimer)?;
        // The storage format supports the full configured horizon, while the admission
        // horizon is the independently controlled canary boundary for new work.
        let horizon_days = self.admission_horizon_days.min(self.config.horizon_days);
        let horizon_ms = i64::from(horizon_days)
            .checked_mul(86_400_000)
            .ok_or(TimelineAdmissionError::HorizonOverflow)?;
        if due_time_ms <= now_ms || due_time_ms.saturating_sub(now_ms) > horizon_ms {
            return Err(TimelineAdmissionError::HorizonExceeded);
        }
        let real_topic = message
            .property(MessageConst::PROPERTY_REAL_TOPIC)
            .filter(|topic| !topic.is_empty())
            .ok_or(TimelineAdmissionError::MalformedTimer)?;
        let encoded_bytes = estimated_payload_bytes(message, &real_topic);
        if encoded_bytes > self.config.payload_record_bytes as u64 {
            return Err(TimelineAdmissionError::RecordTooLarge);
        }

        let metrics = self.materializer.metrics();
        if metrics.materialization_lag > self.config.materialization_lag_reject_messages {
            return Err(TimelineAdmissionError::MaterializationLag);
        }
        // Unmaterialized source sizes are not yet in PayloadStore. Charging every one at the
        // configured record ceiling is intentionally conservative and prevents admission races.
        let unmaterialized_bytes = metrics
            .materialization_lag
            .saturating_mul(self.config.payload_record_bytes as u64);
        let pending_messages = metrics
            .payload_records
            .saturating_add(metrics.materialization_lag)
            .saturating_add(1);
        let pending_bytes = metrics
            .payload_live_bytes
            .saturating_add(unmaterialized_bytes)
            .saturating_add(encoded_bytes);
        if pending_messages > self.config.max_pending_messages || pending_bytes > self.config.max_pending_bytes {
            return Err(TimelineAdmissionError::GlobalCapacity);
        }

        let keys = usage_summary_keys(&real_topic, due_time_ms);
        let topic = self.summary(&keys.topic)?;
        let tenant = self.summary(&keys.tenant)?;
        let bucket = self.summary(&keys.bucket)?;
        if topic
            .1
            .saturating_add(unmaterialized_bytes)
            .saturating_add(encoded_bytes)
            > self.config.max_topic_pending_bytes
        {
            return Err(TimelineAdmissionError::TopicQuota);
        }
        if tenant
            .1
            .saturating_add(unmaterialized_bytes)
            .saturating_add(encoded_bytes)
            > self.config.max_tenant_pending_bytes
        {
            return Err(TimelineAdmissionError::TenantQuota);
        }
        if bucket.0.saturating_add(metrics.materialization_lag).saturating_add(1) > self.config.max_bucket_messages
            || bucket
                .1
                .saturating_add(unmaterialized_bytes)
                .saturating_add(encoded_bytes)
                > self.config.max_bucket_bytes
        {
            return Err(TimelineAdmissionError::HotBucket);
        }

        let free = fs2::available_space(&self.store_root)?;
        let total = fs2::total_space(&self.store_root)?;
        let minimum_ratio_bytes = total
            .saturating_mul(u64::from(self.config.minimum_free_ratio_basis_points))
            .div_ceil(10_000);
        if free
            < self
                .config
                .minimum_free_bytes
                .max(minimum_ratio_bytes)
                .saturating_add(encoded_bytes)
        {
            return Err(TimelineAdmissionError::DiskHeadroom);
        }
        Ok(())
    }

    fn summary(&self, key: &[u8]) -> Result<(u64, u64), TimelineAdmissionError> {
        Ok(self.timeline.bucket_summary(key)?.unwrap_or_default())
    }
}

fn estimated_payload_bytes(message: &MessageExtBrokerInner, real_topic: &str) -> u64 {
    let body = message.get_body().map_or(0usize, |body| body.len());
    u64::try_from(
        body.saturating_add(message.properties_string.len())
            .saturating_add(real_topic.len())
            .saturating_add(RECORD_OVERHEAD_RESERVE as usize),
    )
    .unwrap_or(u64::MAX)
}

pub(crate) struct UsageSummaryKeys {
    pub(crate) global: Vec<u8>,
    pub(crate) topic: Vec<u8>,
    pub(crate) tenant: Vec<u8>,
    pub(crate) bucket: Vec<u8>,
}

pub(crate) fn usage_summary_keys(topic: &str, due_time_ms: i64) -> UsageSummaryKeys {
    let tenant = topic.split_once('%').map_or("__default__", |(namespace, _)| namespace);
    UsageSummaryKeys {
        global: b"usage/global".to_vec(),
        topic: prefixed_key(b"usage/topic/", topic.as_bytes()),
        tenant: prefixed_key(b"usage/tenant/", tenant.as_bytes()),
        bucket: prefixed_key(b"usage/second/", &due_time_ms.div_euclid(1_000).to_be_bytes()),
    }
}

fn prefixed_key(prefix: &[u8], suffix: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(prefix.len().saturating_add(suffix.len()));
    key.extend_from_slice(prefix);
    key.extend_from_slice(suffix);
    key
}

#[derive(Debug, Error)]
pub(crate) enum TimelineAdmissionError {
    #[error("Extended Timer admission lease is inactive")]
    RoleInactive,
    #[error("malformed Extended Timer metadata")]
    MalformedTimer,
    #[error("Extended Timer deadline arithmetic overflow")]
    HorizonOverflow,
    #[error("Extended Timer deadline is outside the configured horizon")]
    HorizonExceeded,
    #[error("Extended Timer payload exceeds the record limit")]
    RecordTooLarge,
    #[error("Extended Timer materialization lag exceeds the admission limit")]
    MaterializationLag,
    #[error("Extended Timer global pending capacity is exhausted")]
    GlobalCapacity,
    #[error("Extended Timer topic quota is exhausted")]
    TopicQuota,
    #[error("Extended Timer tenant quota is exhausted")]
    TenantQuota,
    #[error("Extended Timer due-second bucket is full")]
    HotBucket,
    #[error("Extended Timer filesystem headroom is insufficient")]
    DiskHeadroom,
    #[error(transparent)]
    Timeline(#[from] rocketmq_error::RocketMQError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}
