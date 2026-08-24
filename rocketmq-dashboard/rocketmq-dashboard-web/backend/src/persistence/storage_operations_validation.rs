// Copyright 2023 The RocketMQ Rust Authors
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

use super::BACKUP_FORMAT_VERSION;
use super::COLLECTION_FILES;
use super::MAX_BACKUP_LINE_BYTES;
use super::format::BackupData;
use super::format::BackupEndpoint;
use crate::model::AuditAction;
use crate::model::AuditActorKind;
use crate::model::AuditEvent;
use crate::model::AuditOutcome;
use crate::model::AuditResourceType;
use crate::model::DashboardEnvironment;
use crate::model::SessionTokenHash;
use crate::model::StorageBackend;
use crate::persistence::error::PersistenceError;
use serde_json::Value;
use std::collections::BTreeSet;

pub(super) fn has_exact_fields(value: &Value, fields: &[&str]) -> bool {
    let Some(object) = value.as_object() else {
        return false;
    };
    object.len() == fields.len() && fields.iter().all(|field| object.contains_key(*field))
}

fn validate_endpoint_fields(value: &Value) -> bool {
    has_exact_fields(
        value,
        &[
            "endpointId",
            "endpointType",
            "address",
            "role",
            "isEnabled",
            "isActive",
            "sortOrder",
            "createdAtMs",
            "updatedAtMs",
        ],
    )
}

pub(super) fn validate_environment_record(value: &Value) -> bool {
    has_exact_fields(
        value,
        &[
            "environmentId",
            "name",
            "useVIPChannel",
            "useTLS",
            "revision",
            "createdAtMs",
            "updatedAtMs",
            "endpoints",
        ],
    ) && value
        .get("endpoints")
        .and_then(Value::as_array)
        .is_some_and(|items| items.iter().all(validate_endpoint_fields))
}

pub(super) fn validate_endpoint_record(value: &Value) -> bool {
    has_exact_fields(
        value,
        &[
            "environmentId",
            "endpointId",
            "endpointType",
            "address",
            "role",
            "isEnabled",
            "isActive",
            "sortOrder",
            "createdAtMs",
            "updatedAtMs",
        ],
    )
}

pub(super) fn validate_monitor_record(value: &Value) -> bool {
    has_exact_fields(
        value,
        &[
            "environmentId",
            "consumerGroup",
            "minCount",
            "maxDiffTotal",
            "revision",
            "createdAtMs",
            "updatedAtMs",
        ],
    )
}

pub(super) fn validate_history_record(value: &Value) -> bool {
    has_exact_fields(value, &["environmentId", "metric", "bucketMs", "dimensions", "value"])
        && value
            .get("dimensions")
            .and_then(Value::as_array)
            .is_some_and(|items| items.iter().all(|item| has_exact_fields(item, &["key", "value"])))
}

pub(super) fn validate_session_record(value: &Value) -> bool {
    has_exact_fields(
        value,
        &[
            "sessionId",
            "tokenHash",
            "username",
            "createdAtMs",
            "expiresAtMs",
            "lastSeenAtMs",
            "revokedAtMs",
        ],
    )
}

pub(super) fn validate_audit_record(value: &Value) -> bool {
    has_exact_fields(
        value,
        &[
            "eventId",
            "requestId",
            "actor",
            "action",
            "resourceType",
            "resourceName",
            "environmentId",
            "outcome",
            "detail",
            "createdAtMs",
        ],
    ) && value
        .get("actor")
        .is_some_and(|actor| has_exact_fields(actor, &["kind", "username"]))
}

pub(super) fn attach_endpoints(
    environments: &mut [DashboardEnvironment],
    endpoints: Vec<BackupEndpoint>,
) -> Result<(), PersistenceError> {
    for environment in environments.iter_mut() {
        environment.endpoints.clear();
    }
    for row in endpoints {
        let environment = environments
            .iter_mut()
            .find(|environment| environment.environment_id == row.environment_id)
            .ok_or(PersistenceError::CorruptedData)?;
        environment.endpoints.push(row.endpoint);
    }
    Ok(())
}

pub(super) fn verify_data(data: &BackupData, expected_backend: Option<StorageBackend>) -> Result<(), PersistenceError> {
    if data.manifest.format_version != BACKUP_FORMAT_VERSION
        || expected_backend.is_some_and(|backend| data.manifest.backend != backend)
        || data.manifest.scope.collections
            != COLLECTION_FILES
                .iter()
                .map(|file| file.trim_end_matches(".ndjson").to_string())
                .collect::<Vec<_>>()
    {
        return Err(PersistenceError::UnsupportedLayout);
    }
    let mut normalized = data.clone();
    normalized.refresh_counts()?;
    if normalized.manifest.counts != data.manifest.counts {
        return Err(PersistenceError::CorruptedData);
    }
    let mut environments = BTreeSet::new();
    let mut names = BTreeSet::new();
    for environment in &data.environments {
        environment.validate().map_err(PersistenceError::InvalidConfig)?;
        if environment.revision.0 == 0
            || environment.created_at_ms < 0
            || environment.updated_at_ms < environment.created_at_ms
            || !environments.insert(environment.environment_id.0.clone())
            || !names.insert(environment.name.clone())
        {
            return Err(PersistenceError::CorruptedData);
        }
    }
    let mut monitor_ids = BTreeSet::new();
    for monitor in &data.monitors {
        monitor.validate().map_err(PersistenceError::InvalidConfig)?;
        if monitor.revision.0 == 0
            || monitor.created_at_ms < 0
            || monitor.updated_at_ms < monitor.created_at_ms
            || !environments.contains(&monitor.environment_id.0)
            || !monitor_ids.insert((monitor.environment_id.0.clone(), monitor.consumer_group.clone()))
        {
            return Err(PersistenceError::CorruptedData);
        }
    }
    let mut history_ids = BTreeSet::new();
    for history in &data.history {
        let mut history = history.clone();
        history.normalize().map_err(PersistenceError::InvalidConfig)?;
        let key = (
            history.environment_id.0.clone(),
            history.metric.clone(),
            history.bucket_ms,
            history.dimensions_json().map_err(PersistenceError::InvalidConfig)?,
        );
        if !environments.contains(&history.environment_id.0) || !history_ids.insert(key) {
            return Err(PersistenceError::CorruptedData);
        }
    }
    let mut session_ids = BTreeSet::new();
    let mut session_hashes = BTreeSet::new();
    for session in &data.sessions {
        let hash = parse_token_hash(&session.token_hash)?;
        if uuid::Uuid::parse_str(&session.session_id).is_err()
            || session.username.is_empty()
            || session.username.len() > 128
            || session.created_at_ms < 0
            || session.expires_at_ms <= session.created_at_ms
            || session.last_seen_at_ms < session.created_at_ms
            || session.revoked_at_ms.is_some_and(|value| value < session.created_at_ms)
            || !session_ids.insert(session.session_id.clone())
            || !session_hashes.insert(hash.lower_hex())
        {
            return Err(PersistenceError::CorruptedData);
        }
    }
    let mut audit_ids = BTreeSet::new();
    for audit in &data.audit {
        if uuid::Uuid::parse_str(&audit.event_id).is_err()
            || uuid::Uuid::parse_str(&audit.request_id).is_err()
            || audit.created_at_ms < 0
            || !audit_ids.insert(audit.event_id.clone())
        {
            return Err(PersistenceError::CorruptedData);
        }
        validate_audit(audit)?;
    }
    Ok(())
}

fn validate_audit(audit: &AuditEvent) -> Result<(), PersistenceError> {
    let actor_kind = audit.actor.kind.code();
    if AuditActorKind::parse(actor_kind).is_none()
        || AuditAction::parse(audit.action.code()).is_none()
        || AuditResourceType::parse(audit.resource_type.code()).is_none()
        || AuditOutcome::parse(audit.outcome.code()).is_none()
        || audit.actor.username.as_ref().is_some_and(|name| name.len() > 128)
        || audit.resource_name.as_ref().is_some_and(|name| name.len() > 255)
        || audit
            .detail
            .as_ref()
            .is_some_and(|detail| serde_json::to_vec(detail).map_or(true, |bytes| bytes.len() > MAX_BACKUP_LINE_BYTES))
    {
        return Err(PersistenceError::CorruptedData);
    }
    Ok(())
}

pub(super) fn parse_token_hash(value: &str) -> Result<SessionTokenHash, PersistenceError> {
    if value.len() != 64 || !value.as_bytes().iter().all(u8::is_ascii_hexdigit) {
        return Err(PersistenceError::CorruptedData);
    }
    let mut bytes = [0_u8; 32];
    for (index, chunk) in value.as_bytes().chunks_exact(2).enumerate() {
        let text = std::str::from_utf8(chunk).map_err(|_| PersistenceError::CorruptedData)?;
        bytes[index] = u8::from_str_radix(text, 16).map_err(|_| PersistenceError::CorruptedData)?;
    }
    Ok(SessionTokenHash(bytes))
}
