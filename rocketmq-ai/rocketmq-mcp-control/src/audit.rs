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
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::Mutex;

use crate::error::ControlError;
use crate::error::ControlErrorCode;
use crate::model::ClusterName;
use crate::model::ControlOperation;

use self::jsonl::audit_file_limit;
use self::recovery::duration_millis;
use self::recovery::recover_audit_state;
use self::recovery::terminal_event;
use self::recovery::timestamp_unix_millis;
use self::recovery::validate_terminal;

pub use self::evidence::digest_canonical_json;
pub use self::evidence::sha256_hex;
pub use self::evidence::AuditBrokerSet;
pub use self::evidence::AuditOutcome;
pub use self::evidence::AuditSubject;
pub use self::evidence::AuditTarget;
pub use self::evidence::AuditTargetResults;
pub use self::evidence::MAX_INLINE_BROKER_NAMES_BYTES;
pub use self::jsonl::JsonlAuditSink;

pub const AUDIT_SCHEMA_VERSION: &str = "rocketmq-mcp-control.audit.v3";
/// Smallest record bound the configuration accepts. Every version-3 record fits within it,
/// whatever its operator, reason and target, so a valid configuration cannot lose a record to
/// its size.
pub const MIN_AUDIT_RECORD_BYTES: usize = 4_096;
const AUDIT_TRANSACTION_TIMEOUT: Duration = Duration::from_secs(2);

pub type AuditFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AuditEvent {
    Started,
    Completed,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, JsonSchema)]
pub enum AuditSchemaVersion {
    V1,
    V2,
    V3,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AuditMode {
    DryRun,
    Execute,
}

impl AuditMode {
    const fn from_dry_run(dry_run: bool) -> Self {
        if dry_run {
            Self::DryRun
        } else {
            Self::Execute
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AuditResult {
    Started,
    Planned,
    Applied,
    Partial,
    Conflict,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize, JsonSchema)]
#[serde(transparent)]
pub struct AuditInvocationId(u64);

impl AuditInvocationId {
    /// Returns the sequence number of the invocation's durable `started` record.
    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Clone, PartialEq, Eq, JsonSchema)]
pub struct AuditRecord {
    pub schema_version: AuditSchemaVersion,
    pub sequence: u64,
    pub invocation_id: AuditInvocationId,
    pub timestamp_unix_millis: u64,
    pub event: AuditEvent,
    pub operation: ControlOperation,
    pub cluster: ClusterName,
    pub operator: Option<String>,
    pub reason: Option<String>,
    pub mode: AuditMode,
    pub result: AuditResult,
    pub error_code: Option<ControlErrorCode>,
    pub duration_millis: Option<u64>,
    /// The object of the mutation. Version-3 records always carry it; older ones never do.
    pub target: Option<AuditTarget>,
    /// Digest of what the call asked for, on version-3 records.
    pub requested_digest: Option<String>,
    /// Digest of the request key, when the call carried one.
    pub request_key_digest: Option<String>,
    /// Digest of the state read before the change, on a terminal record that has one.
    pub before_digest: Option<String>,
    /// Whether a target was written, on a terminal record whose attempt reported it.
    pub changed: Option<bool>,
    /// Per-target outcome counts, on a terminal record whose attempt reported them.
    pub target_results: Option<AuditTargetResults>,
}

impl std::fmt::Debug for AuditRecord {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AuditRecord")
            .field("schema_version", &self.schema_version)
            .field("sequence", &self.sequence)
            .field("invocation_id", &self.invocation_id)
            .field("timestamp_unix_millis", &self.timestamp_unix_millis)
            .field("event", &self.event)
            .field("operation", &self.operation)
            .field("cluster", &self.cluster)
            .field("identity_recorded", &self.operator.is_some())
            .field("reason_recorded", &self.reason.is_some())
            .field("mode", &self.mode)
            .field("result", &self.result)
            .field("error_code", &self.error_code)
            .field("duration_millis", &self.duration_millis)
            .field("target", &self.target)
            .field("changed", &self.changed)
            .field("target_results", &self.target_results)
            .finish()
    }
}

mod evidence;
mod jsonl;
mod recovery;
mod wire;

#[derive(Clone, PartialEq, Eq)]
pub struct AuditContext {
    operator: String,
    reason: Option<String>,
}

impl AuditContext {
    /// Creates the identity evidence written only to the durable audit sink.
    ///
    /// # Errors
    ///
    /// Returns a closed authorization or argument error when either value is unsafe to persist.
    pub fn try_new(operator: &str, reason: Option<&str>) -> Result<Self, ControlError> {
        if !crate::model::valid_operator(operator) {
            return Err(ControlError::permission_denied());
        }
        if reason.is_some_and(|value| !crate::model::valid_reason(value)) {
            return Err(ControlError::invalid_argument());
        }
        Ok(Self {
            operator: operator.to_owned(),
            reason: reason.map(ToOwned::to_owned),
        })
    }
}

impl std::fmt::Debug for AuditContext {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AuditContext")
            .field("operator_validated", &true)
            .field("reason_recorded", &self.reason.is_some())
            .finish()
    }
}

#[derive(Clone)]
pub struct AuditInvocation {
    id: AuditInvocationId,
    operation: ControlOperation,
    cluster: ClusterName,
    context: AuditContext,
    subject: AuditSubject,
    mode: AuditMode,
    started_at: tokio::time::Instant,
    trail_identity: Arc<TrailIdentity>,
}

impl AuditInvocation {
    pub const fn id(&self) -> AuditInvocationId {
        self.id
    }
}

pub trait ReliableAuditSink: Send + Sync {
    fn append<'a>(&'a self, record: &'a AuditRecord) -> AuditFuture<'a, Result<(), ControlError>>;

    /// Returns the records the sink still holds. A segmented sink returns its active segment.
    fn records(&self) -> AuditFuture<'_, Result<Vec<AuditRecord>, ControlError>>;

    /// Returns the highest sequence ever stored, including records the sink no longer holds.
    fn last_sequence(&self) -> AuditFuture<'_, Result<u64, ControlError>> {
        Box::pin(async move { Ok(self.records().await?.last().map_or(0, |record| record.sequence)) })
    }
}

#[derive(Clone)]
pub struct AuditTrail {
    sink: Arc<dyn ReliableAuditSink>,
    state: Arc<Mutex<AuditTrailState>>,
    poisoned: Arc<AtomicBool>,
    identity: Arc<TrailIdentity>,
}

#[derive(Debug)]
struct TrailIdentity;

struct AuditTrailState {
    sequence: u64,
    /// Invocations with a `started` record and no terminal record yet.
    invocations: BTreeMap<AuditInvocationId, RecoveredInvocation>,
}

#[derive(Clone)]
struct RecoveredInvocation {
    schema_version: AuditSchemaVersion,
    operation: ControlOperation,
    cluster: ClusterName,
    operator: Option<String>,
    reason: Option<String>,
    mode: AuditMode,
    target: Option<AuditTarget>,
    requested_digest: Option<String>,
    request_key_digest: Option<String>,
    terminal: bool,
}

struct PoisonOnDrop<'a> {
    poisoned: &'a AtomicBool,
    armed: bool,
}

impl<'a> PoisonOnDrop<'a> {
    fn new(poisoned: &'a AtomicBool) -> Self {
        Self { poisoned, armed: true }
    }

    fn disarm(mut self) {
        self.armed = false;
    }
}

impl Drop for PoisonOnDrop<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.poisoned.store(true, Ordering::Release);
        }
    }
}

impl AuditTrail {
    #[cfg(test)]
    pub(crate) fn new(sink: Arc<dyn ReliableAuditSink>) -> Self {
        Self {
            sink,
            state: Arc::new(Mutex::new(AuditTrailState {
                sequence: 0,
                invocations: BTreeMap::new(),
            })),
            poisoned: Arc::new(AtomicBool::new(false)),
            identity: Arc::new(TrailIdentity),
        }
    }

    /// Resumes sequence allocation from a previously persisted sink.
    ///
    /// Invocations that an earlier process left without a terminal record stay unfinished: only
    /// the process that started an invocation holds the token that can finish it.
    ///
    /// # Errors
    ///
    /// Returns `audit_unavailable` if existing records cannot be queried or are out of order.
    pub async fn resume(sink: Arc<dyn ReliableAuditSink>) -> Result<Self, ControlError> {
        let (records, last_sequence) = tokio::time::timeout(AUDIT_TRANSACTION_TIMEOUT, async {
            Ok::<_, ControlError>((sink.records().await?, sink.last_sequence().await?))
        })
        .await
        .map_err(|_| ControlError::audit_unavailable())?
        .map_err(|_| ControlError::audit_unavailable())?;
        let mut state = recover_audit_state(&records)?;
        state.sequence = state.sequence.max(last_sequence);
        state.invocations.clear();
        Ok(Self {
            sink,
            state: Arc::new(Mutex::new(state)),
            poisoned: Arc::new(AtomicBool::new(false)),
            identity: Arc::new(TrailIdentity),
        })
    }

    /// Persists the `started` record of one invocation.
    ///
    /// # Errors
    ///
    /// Returns `audit_unavailable` if `subject` was built for another operation or the record
    /// cannot be made durable. No session may be opened in either case.
    pub async fn start(
        &self,
        context: &AuditContext,
        operation: ControlOperation,
        cluster: &ClusterName,
        dry_run: bool,
        subject: &AuditSubject,
    ) -> Result<AuditInvocation, ControlError> {
        self.ensure_available()?;
        if subject.operation() != operation {
            return Err(ControlError::audit_unavailable());
        }
        let mut state = self.state.lock().await;
        self.ensure_available()?;
        let sequence = state
            .sequence
            .checked_add(1)
            .ok_or_else(ControlError::audit_unavailable)?;
        let invocation = AuditInvocation {
            id: AuditInvocationId(sequence),
            operation,
            cluster: cluster.clone(),
            context: context.clone(),
            subject: subject.clone(),
            mode: AuditMode::from_dry_run(dry_run),
            started_at: tokio::time::Instant::now(),
            trail_identity: self.identity.clone(),
        };
        let record = AuditRecord {
            schema_version: AuditSchemaVersion::V3,
            sequence,
            invocation_id: invocation.id,
            timestamp_unix_millis: timestamp_unix_millis()?,
            event: AuditEvent::Started,
            operation,
            cluster: cluster.clone(),
            operator: Some(context.operator.clone()),
            reason: context.reason.clone(),
            mode: invocation.mode,
            result: AuditResult::Started,
            error_code: None,
            duration_millis: None,
            target: Some(subject.target.clone()),
            requested_digest: Some(subject.requested_digest.clone()),
            request_key_digest: subject.request_key_digest.clone(),
            before_digest: None,
            changed: None,
            target_results: None,
        };
        self.append_record(&record).await?;
        state.sequence = sequence;
        state.invocations.insert(
            invocation.id,
            RecoveredInvocation {
                schema_version: AuditSchemaVersion::V3,
                operation,
                cluster: cluster.clone(),
                operator: Some(context.operator.clone()),
                reason: context.reason.clone(),
                mode: invocation.mode,
                target: record.target,
                requested_digest: record.requested_digest,
                request_key_digest: record.request_key_digest,
                terminal: false,
            },
        );
        Ok(invocation)
    }

    /// Persists the terminal record of one invocation, repeating the object and digests of its
    /// `started` record and adding what the attempt reported.
    ///
    /// # Errors
    ///
    /// Returns `audit_unavailable` if the invocation is unknown, already finished or from
    /// another trail, if the result and code do not belong together, or if the record cannot be
    /// made durable.
    pub async fn terminal(
        &self,
        invocation: &AuditInvocation,
        result: AuditResult,
        error_code: Option<ControlErrorCode>,
        outcome: &AuditOutcome,
    ) -> Result<(), ControlError> {
        self.ensure_available()?;
        if !Arc::ptr_eq(&self.identity, &invocation.trail_identity) || !outcome.is_valid() {
            return Err(ControlError::audit_unavailable());
        }
        let mut state = self.state.lock().await;
        self.ensure_available()?;
        let recovered = state
            .invocations
            .get(&invocation.id)
            .ok_or_else(ControlError::audit_unavailable)?;
        if recovered.terminal
            || recovered.schema_version != AuditSchemaVersion::V3
            || recovered.operation != invocation.operation
            || recovered.cluster != invocation.cluster
            || recovered.operator.as_deref() != Some(invocation.context.operator.as_str())
            || recovered.reason != invocation.context.reason
            || recovered.mode != invocation.mode
            || recovered.target.as_ref() != Some(&invocation.subject.target)
            || recovered.requested_digest.as_deref() != Some(invocation.subject.requested_digest.as_str())
            || recovered.request_key_digest != invocation.subject.request_key_digest
        {
            return Err(ControlError::audit_unavailable());
        }
        validate_terminal(result, error_code)?;
        let sequence = state
            .sequence
            .checked_add(1)
            .ok_or_else(ControlError::audit_unavailable)?;
        let record = AuditRecord {
            schema_version: AuditSchemaVersion::V3,
            sequence,
            invocation_id: invocation.id,
            timestamp_unix_millis: timestamp_unix_millis()?,
            event: terminal_event(result),
            operation: invocation.operation,
            cluster: invocation.cluster.clone(),
            operator: Some(invocation.context.operator.clone()),
            reason: invocation.context.reason.clone(),
            mode: invocation.mode,
            result,
            error_code,
            duration_millis: Some(duration_millis(invocation.started_at.elapsed())?),
            target: Some(invocation.subject.target.clone()),
            requested_digest: Some(invocation.subject.requested_digest.clone()),
            request_key_digest: invocation.subject.request_key_digest.clone(),
            before_digest: outcome.before_digest.clone(),
            changed: outcome.changed,
            target_results: outcome.target_results,
        };
        self.append_record(&record).await?;
        state.sequence = sequence;
        // A finished invocation needs no state: its token cannot write a second terminal record.
        state.invocations.remove(&invocation.id);
        Ok(())
    }

    /// Returns the records the sink still holds, which for the JSONL sink is its active segment.
    pub async fn records(&self) -> Result<Vec<AuditRecord>, ControlError> {
        self.ensure_available()?;
        tokio::time::timeout(AUDIT_TRANSACTION_TIMEOUT, self.sink.records())
            .await
            .map_err(|_| ControlError::audit_unavailable())?
            .map_err(|_| ControlError::audit_unavailable())
    }

    async fn append_record(&self, record: &AuditRecord) -> Result<(), ControlError> {
        let poison = PoisonOnDrop::new(&self.poisoned);
        match tokio::time::timeout(AUDIT_TRANSACTION_TIMEOUT, self.sink.append(record)).await {
            Ok(Ok(())) => {
                poison.disarm();
                Ok(())
            }
            Ok(Err(_)) => Err(ControlError::audit_unavailable()),
            Err(_) => Err(ControlError::audit_unavailable()),
        }
    }

    fn ensure_available(&self) -> Result<(), ControlError> {
        if self.poisoned.load(Ordering::Acquire) {
            Err(ControlError::audit_unavailable())
        } else {
            Ok(())
        }
    }
}

pub struct MemoryAuditSink {
    state: Mutex<MemoryAuditState>,
    capacity: usize,
    max_record_bytes: usize,
    max_file_bytes: Option<u64>,
    reject_writes: bool,
}

struct MemoryAuditState {
    records: Vec<AuditRecord>,
    bytes_used: u64,
}

impl MemoryAuditSink {
    pub fn new(capacity: usize, max_record_bytes: usize) -> Self {
        Self {
            state: Mutex::new(MemoryAuditState {
                records: Vec::new(),
                bytes_used: 0,
            }),
            capacity,
            max_record_bytes,
            max_file_bytes: audit_file_limit(capacity, max_record_bytes).ok(),
            reject_writes: false,
        }
    }

    pub fn failing(capacity: usize, max_record_bytes: usize) -> Self {
        Self {
            state: Mutex::new(MemoryAuditState {
                records: Vec::new(),
                bytes_used: 0,
            }),
            capacity,
            max_record_bytes,
            max_file_bytes: audit_file_limit(capacity, max_record_bytes).ok(),
            reject_writes: true,
        }
    }
}

impl ReliableAuditSink for MemoryAuditSink {
    fn append<'a>(&'a self, record: &'a AuditRecord) -> AuditFuture<'a, Result<(), ControlError>> {
        Box::pin(async move {
            if record.schema_version != AuditSchemaVersion::V3 {
                return Err(ControlError::audit_unavailable());
            }
            let encoded = encode_record(record, self.max_record_bytes)?;
            let encoded_len = u64::try_from(encoded.len()).map_err(|_| ControlError::audit_unavailable())?;
            let mut state = self.state.lock().await;
            let next_bytes = state
                .bytes_used
                .checked_add(encoded_len)
                .ok_or_else(ControlError::audit_unavailable)?;
            if self.reject_writes
                || state.records.len() >= self.capacity
                || encoded.is_empty()
                || self.max_file_bytes.is_none_or(|limit| next_bytes > limit)
            {
                return Err(ControlError::audit_unavailable());
            }
            state.records.push(record.clone());
            state.bytes_used = next_bytes;
            Ok(())
        })
    }

    fn records(&self) -> AuditFuture<'_, Result<Vec<AuditRecord>, ControlError>> {
        Box::pin(async move {
            if self.max_file_bytes.is_none() {
                return Err(ControlError::audit_unavailable());
            }
            Ok(self.state.lock().await.records.clone())
        })
    }
}

fn encode_record(record: &AuditRecord, max_record_bytes: usize) -> Result<Vec<u8>, ControlError> {
    let encoded = serde_json::to_vec(record).map_err(|_| ControlError::audit_unavailable())?;
    if encoded.len() > max_record_bytes {
        return Err(ControlError::audit_unavailable());
    }
    Ok(encoded)
}

#[cfg(test)]
#[path = "audit/tests.rs"]
mod tests;
