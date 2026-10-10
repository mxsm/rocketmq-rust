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

use serde::de::Error as _;
use serde::ser::Error as _;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;

use super::evidence::is_digest;
use super::AuditEvent;
use super::AuditInvocationId;
use super::AuditMode;
use super::AuditRecord;
use super::AuditResult;
use super::AuditSchemaVersion;
use super::AuditTarget;
use super::AuditTargetResults;
use crate::error::ControlError;
use crate::error::ControlErrorCode;
use crate::model::ClusterName;
use crate::model::ControlOperation;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
enum AuditV1SchemaVersion {
    #[serde(rename = "rocketmq-mcp-control.audit.v1")]
    V1,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
enum AuditV2SchemaVersion {
    #[serde(rename = "rocketmq-mcp-control.audit.v2")]
    V2,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
enum AuditV3SchemaVersion {
    #[serde(rename = "rocketmq-mcp-control.audit.v3")]
    V3,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
enum AuditSegmentSchemaVersion {
    #[serde(rename = "rocketmq-mcp-control.audit-segment.v1")]
    V1,
}

/// First line of every audit segment after the first one.
///
/// It links the segment to the one sealed before it and names the invocations that were still
/// in flight, whose `started` records follow as copies.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct AuditSegmentHeader {
    schema_version: AuditSegmentSchemaVersion,
    pub(super) segment: u64,
    pub(super) previous_last_sequence: u64,
    /// SHA-256 of the sealed segment file.
    previous_segment_digest: String,
    open_invocations: Vec<AuditInvocationId>,
}

impl AuditSegmentHeader {
    pub(super) fn new(
        segment: u64,
        previous_last_sequence: u64,
        previous_segment_digest: String,
        open_invocations: Vec<AuditInvocationId>,
    ) -> Self {
        Self {
            schema_version: AuditSegmentSchemaVersion::V1,
            segment,
            previous_last_sequence,
            previous_segment_digest,
            open_invocations,
        }
    }

    /// Returns the carried invocations after checking the header against itself.
    pub(super) fn validated_open_invocations(&self) -> Result<&[AuditInvocationId], ControlError> {
        let ordered = self.open_invocations.windows(2).all(|pair| pair[0] < pair[1]);
        let started_before = self
            .open_invocations
            .last()
            .is_none_or(|last| last.0 <= self.previous_last_sequence);
        if self.segment < 2 || !is_digest(&self.previous_segment_digest) || !ordered || !started_before {
            return Err(ControlError::audit_unavailable());
        }
        Ok(&self.open_invocations)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum LegacyControlErrorCode {
    InvalidConfig,
    RequestRejected,
    Unauthorized,
    PermissionDenied,
    OperationUnavailable,
    InvalidArguments,
    AuditUnavailable,
    Conflict,
    Timeout,
    Cancelled,
    ExecutionFailed,
    ShutdownFailed,
}

impl LegacyControlErrorCode {
    const fn into_current(self) -> ControlErrorCode {
        match self {
            Self::InvalidConfig => ControlErrorCode::InvalidConfig,
            Self::RequestRejected => ControlErrorCode::RequestRejected,
            Self::Unauthorized => ControlErrorCode::Unauthorized,
            Self::PermissionDenied => ControlErrorCode::PermissionDenied,
            Self::OperationUnavailable => ControlErrorCode::OperationUnavailable,
            Self::InvalidArguments => ControlErrorCode::InvalidArgument,
            Self::AuditUnavailable => ControlErrorCode::AuditUnavailable,
            Self::Conflict => ControlErrorCode::PreconditionConflict,
            Self::Timeout => ControlErrorCode::Timeout,
            Self::Cancelled => ControlErrorCode::Cancelled,
            Self::ExecutionFailed => ControlErrorCode::ExecutionFailed,
            Self::ShutdownFailed => ControlErrorCode::ShutdownFailed,
        }
    }

    const fn from_current(value: ControlErrorCode) -> Option<Self> {
        match value {
            ControlErrorCode::InvalidConfig => Some(Self::InvalidConfig),
            ControlErrorCode::RequestRejected => Some(Self::RequestRejected),
            ControlErrorCode::Unauthorized => Some(Self::Unauthorized),
            ControlErrorCode::PermissionDenied => Some(Self::PermissionDenied),
            ControlErrorCode::OperationUnavailable => Some(Self::OperationUnavailable),
            ControlErrorCode::InvalidArgument => Some(Self::InvalidArguments),
            ControlErrorCode::AuditUnavailable => Some(Self::AuditUnavailable),
            ControlErrorCode::PreconditionConflict => Some(Self::Conflict),
            ControlErrorCode::Timeout => Some(Self::Timeout),
            ControlErrorCode::Cancelled => Some(Self::Cancelled),
            ControlErrorCode::ExecutionFailed => Some(Self::ExecutionFailed),
            ControlErrorCode::ShutdownFailed => Some(Self::ShutdownFailed),
            ControlErrorCode::ClusterNotAllowed
            | ControlErrorCode::OperationNotAllowed
            | ControlErrorCode::MutationDisabled
            | ControlErrorCode::ConfirmationRequired
            | ControlErrorCode::RateLimited
            | ControlErrorCode::PartialApply
            | ControlErrorCode::VerificationFailed => None,
        }
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct AuditRecordV1Wire {
    schema_version: AuditV1SchemaVersion,
    sequence: u64,
    invocation_id: AuditInvocationId,
    timestamp_unix_millis: u64,
    event: AuditEvent,
    operation: ControlOperation,
    cluster: ClusterName,
    dry_run: bool,
    error_code: Option<LegacyControlErrorCode>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct AuditRecordV2Wire {
    schema_version: AuditV2SchemaVersion,
    sequence: u64,
    invocation_id: AuditInvocationId,
    timestamp_unix_millis: u64,
    event: AuditEvent,
    operation: ControlOperation,
    cluster: ClusterName,
    operator: String,
    #[serde(deserialize_with = "deserialize_nullable")]
    reason: Option<String>,
    mode: AuditMode,
    result: AuditResult,
    #[serde(deserialize_with = "deserialize_nullable")]
    error_code: Option<ControlErrorCode>,
    #[serde(deserialize_with = "deserialize_nullable")]
    duration_millis: Option<u64>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct AuditRecordV3Wire {
    schema_version: AuditV3SchemaVersion,
    sequence: u64,
    invocation_id: AuditInvocationId,
    timestamp_unix_millis: u64,
    event: AuditEvent,
    operation: ControlOperation,
    cluster: ClusterName,
    operator: String,
    #[serde(deserialize_with = "deserialize_nullable")]
    reason: Option<String>,
    mode: AuditMode,
    result: AuditResult,
    #[serde(deserialize_with = "deserialize_nullable")]
    error_code: Option<ControlErrorCode>,
    #[serde(deserialize_with = "deserialize_nullable")]
    duration_millis: Option<u64>,
    target: AuditTarget,
    requested_digest: String,
    #[serde(deserialize_with = "deserialize_nullable")]
    request_key_digest: Option<String>,
    #[serde(deserialize_with = "deserialize_nullable")]
    before_digest: Option<String>,
    #[serde(deserialize_with = "deserialize_nullable")]
    changed: Option<bool>,
    #[serde(deserialize_with = "deserialize_nullable")]
    target_results: Option<AuditTargetResults>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum AuditRecordWire {
    // Boxed: a version-3 record is several times the size of the older shapes.
    V3(Box<AuditRecordV3Wire>),
    V2(AuditRecordV2Wire),
    V1(AuditRecordV1Wire),
}

fn deserialize_nullable<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

impl Serialize for AuditRecord {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        super::recovery::validate_record_shape(self).map_err(S::Error::custom)?;
        match self.schema_version {
            AuditSchemaVersion::V1 => {
                let error_code = self
                    .error_code
                    .map(|code| {
                        LegacyControlErrorCode::from_current(code)
                            .ok_or_else(|| S::Error::custom("invalid v1 error code"))
                    })
                    .transpose()?;
                AuditRecordV1Wire {
                    schema_version: AuditV1SchemaVersion::V1,
                    sequence: self.sequence,
                    invocation_id: self.invocation_id,
                    timestamp_unix_millis: self.timestamp_unix_millis,
                    event: self.event,
                    operation: self.operation,
                    cluster: self.cluster.clone(),
                    dry_run: matches!(self.mode, AuditMode::DryRun),
                    error_code,
                }
                .serialize(serializer)
            }
            AuditSchemaVersion::V2 => AuditRecordV2Wire {
                schema_version: AuditV2SchemaVersion::V2,
                sequence: self.sequence,
                invocation_id: self.invocation_id,
                timestamp_unix_millis: self.timestamp_unix_millis,
                event: self.event,
                operation: self.operation,
                cluster: self.cluster.clone(),
                operator: self
                    .operator
                    .clone()
                    .ok_or_else(|| S::Error::custom("missing v2 operator"))?,
                reason: self.reason.clone(),
                mode: self.mode,
                result: self.result,
                error_code: self.error_code,
                duration_millis: self.duration_millis,
            }
            .serialize(serializer),
            AuditSchemaVersion::V3 => AuditRecordV3Wire {
                schema_version: AuditV3SchemaVersion::V3,
                sequence: self.sequence,
                invocation_id: self.invocation_id,
                timestamp_unix_millis: self.timestamp_unix_millis,
                event: self.event,
                operation: self.operation,
                cluster: self.cluster.clone(),
                operator: self
                    .operator
                    .clone()
                    .ok_or_else(|| S::Error::custom("missing v3 operator"))?,
                reason: self.reason.clone(),
                mode: self.mode,
                result: self.result,
                error_code: self.error_code,
                duration_millis: self.duration_millis,
                target: self
                    .target
                    .clone()
                    .ok_or_else(|| S::Error::custom("missing v3 target"))?,
                requested_digest: self
                    .requested_digest
                    .clone()
                    .ok_or_else(|| S::Error::custom("missing v3 requested digest"))?,
                request_key_digest: self.request_key_digest.clone(),
                before_digest: self.before_digest.clone(),
                changed: self.changed,
                target_results: self.target_results,
            }
            .serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for AuditRecord {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let record = match AuditRecordWire::deserialize(deserializer)? {
            AuditRecordWire::V1(record) => {
                let error_code = record.error_code.map(LegacyControlErrorCode::into_current);
                let result = match record.event {
                    AuditEvent::Started => AuditResult::Started,
                    AuditEvent::Completed if record.dry_run => AuditResult::Planned,
                    AuditEvent::Completed => AuditResult::Applied,
                    AuditEvent::Failed if error_code == Some(ControlErrorCode::PreconditionConflict) => {
                        AuditResult::Conflict
                    }
                    AuditEvent::Failed => AuditResult::Failed,
                };
                Self {
                    schema_version: AuditSchemaVersion::V1,
                    sequence: record.sequence,
                    invocation_id: record.invocation_id,
                    timestamp_unix_millis: record.timestamp_unix_millis,
                    event: record.event,
                    operation: record.operation,
                    cluster: record.cluster,
                    operator: None,
                    reason: None,
                    mode: AuditMode::from_dry_run(record.dry_run),
                    result,
                    error_code,
                    duration_millis: None,
                    target: None,
                    requested_digest: None,
                    request_key_digest: None,
                    before_digest: None,
                    changed: None,
                    target_results: None,
                }
            }
            AuditRecordWire::V2(record) => Self {
                schema_version: AuditSchemaVersion::V2,
                sequence: record.sequence,
                invocation_id: record.invocation_id,
                timestamp_unix_millis: record.timestamp_unix_millis,
                event: record.event,
                operation: record.operation,
                cluster: record.cluster,
                operator: Some(record.operator),
                reason: record.reason,
                mode: record.mode,
                result: record.result,
                error_code: record.error_code,
                duration_millis: record.duration_millis,
                target: None,
                requested_digest: None,
                request_key_digest: None,
                before_digest: None,
                changed: None,
                target_results: None,
            },
            AuditRecordWire::V3(record) => Self {
                schema_version: AuditSchemaVersion::V3,
                sequence: record.sequence,
                invocation_id: record.invocation_id,
                timestamp_unix_millis: record.timestamp_unix_millis,
                event: record.event,
                operation: record.operation,
                cluster: record.cluster,
                operator: Some(record.operator),
                reason: record.reason,
                mode: record.mode,
                result: record.result,
                error_code: record.error_code,
                duration_millis: record.duration_millis,
                target: Some(record.target),
                requested_digest: Some(record.requested_digest),
                request_key_digest: record.request_key_digest,
                before_digest: record.before_digest,
                changed: record.changed,
                target_results: record.target_results,
            },
        };
        super::recovery::validate_record_shape(&record).map_err(D::Error::custom)?;
        Ok(record)
    }
}
