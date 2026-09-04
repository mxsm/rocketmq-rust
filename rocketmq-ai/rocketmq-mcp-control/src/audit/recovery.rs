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
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use super::AuditEvent;
use super::AuditMode;
use super::AuditRecord;
use super::AuditResult;
use super::AuditSchemaVersion;
use super::AuditTrailState;
use super::RecoveredInvocation;
use crate::error::ControlError;
use crate::error::ControlErrorCode;

pub(super) fn recover_audit_state(records: &[AuditRecord]) -> Result<AuditTrailState, ControlError> {
    let mut sequence = 0;
    let mut invocations = BTreeMap::new();
    for record in records {
        validate_record_shape(record)?;
        if record.sequence <= sequence {
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
                                schema_version: record.schema_version,
                                operation: record.operation,
                                cluster: record.cluster.clone(),
                                operator: record.operator.clone(),
                                reason: record.reason.clone(),
                                mode: record.mode,
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
                    || recovered.schema_version != record.schema_version
                    || recovered.operator != record.operator
                    || recovered.reason != record.reason
                    || recovered.mode != record.mode
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

pub(super) fn validate_record_shape(record: &AuditRecord) -> Result<(), ControlError> {
    match record.schema_version {
        AuditSchemaVersion::V1 => {
            if record.operator.is_some() || record.reason.is_some() || record.duration_millis.is_some() {
                return Err(ControlError::audit_unavailable());
            }
            let expected = match record.event {
                AuditEvent::Started if record.error_code.is_none() => AuditResult::Started,
                AuditEvent::Completed if record.error_code.is_none() && matches!(record.mode, AuditMode::DryRun) => {
                    AuditResult::Planned
                }
                AuditEvent::Completed if record.error_code.is_none() => AuditResult::Applied,
                AuditEvent::Failed if record.error_code == Some(ControlErrorCode::PreconditionConflict) => {
                    AuditResult::Conflict
                }
                AuditEvent::Failed if record.error_code.is_some() => AuditResult::Failed,
                AuditEvent::Started | AuditEvent::Completed | AuditEvent::Failed => {
                    return Err(ControlError::audit_unavailable());
                }
            };
            if record.result != expected {
                return Err(ControlError::audit_unavailable());
            }
        }
        AuditSchemaVersion::V2 => {
            let Some(operator) = record.operator.as_deref() else {
                return Err(ControlError::audit_unavailable());
            };
            if !crate::model::valid_operator(operator)
                || record
                    .reason
                    .as_deref()
                    .is_some_and(|value| !crate::model::valid_reason(value))
            {
                return Err(ControlError::audit_unavailable());
            }
            match record.event {
                AuditEvent::Started => {
                    if record.result != AuditResult::Started
                        || record.error_code.is_some()
                        || record.duration_millis.is_some()
                    {
                        return Err(ControlError::audit_unavailable());
                    }
                }
                AuditEvent::Completed => {
                    if !matches!(record.result, AuditResult::Planned | AuditResult::Applied)
                        || record.error_code.is_some()
                        || record.duration_millis.is_none()
                        || (record.result == AuditResult::Planned) != matches!(record.mode, AuditMode::DryRun)
                    {
                        return Err(ControlError::audit_unavailable());
                    }
                }
                AuditEvent::Failed => {
                    if record.duration_millis.is_none() {
                        return Err(ControlError::audit_unavailable());
                    }
                    validate_terminal(record.result, record.error_code)?;
                }
            }
        }
    }
    Ok(())
}

pub(super) fn validate_terminal(result: AuditResult, error_code: Option<ControlErrorCode>) -> Result<(), ControlError> {
    let valid = match result {
        AuditResult::Planned | AuditResult::Applied => error_code.is_none(),
        AuditResult::Partial => error_code == Some(ControlErrorCode::PartialApply),
        AuditResult::Conflict => error_code == Some(ControlErrorCode::PreconditionConflict),
        AuditResult::Failed => error_code.is_some_and(|code| {
            !matches!(
                code,
                ControlErrorCode::PartialApply | ControlErrorCode::PreconditionConflict
            )
        }),
        AuditResult::Started => false,
    };
    if valid {
        Ok(())
    } else {
        Err(ControlError::audit_unavailable())
    }
}

pub(super) const fn terminal_event(result: AuditResult) -> AuditEvent {
    match result {
        AuditResult::Planned | AuditResult::Applied => AuditEvent::Completed,
        AuditResult::Partial | AuditResult::Conflict | AuditResult::Failed => AuditEvent::Failed,
        AuditResult::Started => AuditEvent::Failed,
    }
}

pub(super) fn duration_millis(duration: Duration) -> Result<u64, ControlError> {
    duration
        .as_millis()
        .try_into()
        .map_err(|_| ControlError::audit_unavailable())
}

pub(super) fn timestamp_unix_millis() -> Result<u64, ControlError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ControlError::audit_unavailable())?
        .as_millis()
        .try_into()
        .map_err(|_| ControlError::audit_unavailable())
}
