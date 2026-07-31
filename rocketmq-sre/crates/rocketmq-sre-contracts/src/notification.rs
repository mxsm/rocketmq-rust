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

use chrono::DateTime;
use chrono::Utc;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

use crate::ClusterId;
use crate::IncidentId;
use crate::NotificationDeliveryId;
use crate::NotificationTargetId;
use crate::OnCallOwnerId;
use crate::TenantId;

/// Supported notification transports. Phase 2 implements signed webhook and mocks.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NotificationChannel {
    SignedWebhook,
    Email,
    Pager,
}

/// Tenant-scoped target with a secret reference instead of inline credentials.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct NotificationTarget {
    pub id: NotificationTargetId,
    pub tenant_id: TenantId,
    pub cluster_id: Option<ClusterId>,
    pub name: String,
    pub channel: NotificationChannel,
    pub endpoint: String,
    pub secret_reference: Option<String>,
    pub enabled: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Durable delivery state for the transactional outbox.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NotificationDeliveryStatus {
    Pending,
    Delivering,
    Delivered,
    RetryScheduled,
    Failed,
}

/// One bounded, idempotent notification delivery.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct NotificationDelivery {
    pub id: NotificationDeliveryId,
    pub target_id: NotificationTargetId,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub incident_id: IncidentId,
    pub delivery_key: String,
    pub status: NotificationDeliveryStatus,
    pub sanitized_summary: String,
    pub deep_link: String,
    pub attempt_count: u16,
    pub next_attempt_at: Option<DateTime<Utc>>,
    pub last_error_code: Option<String>,
    pub delivered_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

/// Static or externally synchronized on-call ownership mapping.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct OnCallOwner {
    pub id: OnCallOwnerId,
    pub tenant_id: TenantId,
    pub cluster_id: Option<ClusterId>,
    pub resource_selector: String,
    pub owner: String,
    pub target_ids: Vec<NotificationTargetId>,
    pub source: String,
    pub valid_from: DateTime<Utc>,
    pub valid_until: Option<DateTime<Utc>>,
}
