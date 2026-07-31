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

use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_runtime::BudgetDimension;
use rocketmq_runtime::BudgetLimit;
use rocketmq_runtime::FullPolicy;
use rocketmq_runtime::RateLimit;
use rocketmq_runtime::ResourceBudget;
use rocketmq_runtime::ResourceBudgetTree;
use rocketmq_runtime::ResourcePermit;

use rocketmq_protocol::protocol::remoting_command::RemotingCommand;

use crate::deadline::RequestDeadline;

static NEXT_TABLE_ID: AtomicU64 = AtomicU64::new(1);

/// Estimates the heap and inline memory retained while a remoting command is
/// in flight.
///
/// The estimate deliberately includes metadata as well as the body so
/// header-heavy requests cannot bypass byte admission with an empty body.
pub(crate) fn materialize_and_estimate_remoting_command_retained_bytes(command: &mut RemotingCommand) -> usize {
    command.materialize_custom_header_to_ext_fields();
    let remark_bytes = command.remark().map_or(0, cheetah_string::CheetahString::len);
    let ext_field_bytes = command.ext_fields().map_or(0, |fields| {
        fields
            .capacity()
            .saturating_mul(std::mem::size_of::<(
                cheetah_string::CheetahString,
                cheetah_string::CheetahString,
            )>())
            .saturating_add(
                fields
                    .iter()
                    .map(|(key, value)| key.len().saturating_add(value.len()))
                    .sum::<usize>(),
            )
    });
    let body_bytes = command.body().map_or(0, bytes::Bytes::len);

    std::mem::size_of::<RemotingCommand>()
        .saturating_add(remark_bytes)
        .saturating_add(ext_field_bytes)
        .saturating_add(body_bytes)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct PendingRequestKey {
    owner_id: u64,
    opaque: i32,
}

#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PendingRequestToken {
    key: PendingRequestKey,
    reservation: u64,
}

/// A single physical connection's request-correlation generation.
///
/// Owners are never reused within a table. Once an owner is retired by close or
/// timeout, callers must create a new connection and owner before registering
/// more requests. This prevents a late response from completing a request on a
/// replacement connection without retaining an unbounded opaque tombstone set.
#[derive(Debug, Clone)]
pub struct PendingRequestOwner {
    table_id: u64,
    id: u64,
    state: Arc<Mutex<PendingOwnerState>>,
}

#[derive(Debug)]
struct PendingOwnerState {
    accepting: bool,
}

impl PendingRequestOwner {
    fn new(table_id: u64, id: u64) -> Self {
        Self {
            table_id,
            id,
            state: Arc::new(Mutex::new(PendingOwnerState { accepting: true })),
        }
    }

    fn retire(&self) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .accepting = false;
    }
}

struct PendingRequest {
    reservation: u64,
    owner: PendingRequestOwner,
    created_at: Instant,
    deadline: RequestDeadline,
    timeout_millis: u64,
    _permit: ResourcePermit,
    completion: Box<dyn PendingCompletion>,
}

trait PendingCompletion: Send + Sync {
    fn complete(&self, result: RocketMQResult<RemotingCommand>);
}

struct OneShotCompletion {
    sender: Mutex<Option<tokio::sync::oneshot::Sender<RocketMQResult<RemotingCommand>>>>,
}

impl PendingCompletion for OneShotCompletion {
    fn complete(&self, result: RocketMQResult<RemotingCommand>) {
        let sender = self
            .sender
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(sender) = sender {
            let _ = sender.send(result);
        }
    }
}

struct PendingRequestTableInner {
    table_id: u64,
    entries: DashMap<PendingRequestKey, PendingRequest>,
    next_owner: AtomicU64,
    next_reservation: AtomicU64,
    default_owner: PendingRequestOwner,
    budget: ResourceBudget,
    max_request_age: Duration,
    rejected_count: AtomicUsize,
    rejected_bytes: AtomicUsize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PendingRequestLimits {
    pub max_count: usize,
    pub max_bytes: usize,
    pub admission_rate_per_second: u64,
    pub max_request_age: Duration,
}

impl Default for PendingRequestLimits {
    fn default() -> Self {
        Self {
            max_count: PendingRequestTable::DEFAULT_CAPACITY,
            max_bytes: PendingRequestTable::DEFAULT_MAX_BYTES,
            admission_rate_per_second: PendingRequestTable::DEFAULT_CAPACITY as u64,
            max_request_age: Duration::from_secs(300),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PendingRequestUsage {
    pub count: usize,
    pub bytes: usize,
    pub rejected_count: usize,
    pub rejected_bytes: usize,
}

/// Concurrent pending request registry with connection-aware, exactly-once completion.
#[derive(Clone)]
pub struct PendingRequestTable {
    inner: Arc<PendingRequestTableInner>,
}

/// RAII ownership of one pending request reservation.
pub struct PendingRequestGuard {
    table: PendingRequestTable,
    token: Option<PendingRequestToken>,
    deadline: RequestDeadline,
}

impl Default for PendingRequestTable {
    fn default() -> Self {
        Self::new()
    }
}

impl PendingRequestTable {
    const DEFAULT_CAPACITY: usize = 65_536;
    const DEFAULT_MAX_BYTES: usize = 256 * 1024 * 1024;

    pub fn new() -> Self {
        Self::with_limits(PendingRequestLimits::default())
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self::with_limits(PendingRequestLimits {
            max_count: capacity,
            ..PendingRequestLimits::default()
        })
    }

    /// Builds a table from explicit count, byte, rate, and age limits.
    ///
    /// # Errors
    ///
    /// Returns a typed configuration error when budget validation fails.
    pub fn try_with_limits(limits: PendingRequestLimits) -> RocketMQResult<Self> {
        let process_budget = ResourceBudgetTree::new(
            "standalone-pending-request-process",
            BudgetLimit::new(limits.max_count.max(1), limits.max_bytes.max(1), FullPolicy::Reject),
        )
        .map_err(|error| RocketMQError::ConfigInvalidValue {
            key: "transport.pendingRequest.standalone",
            value: limits.max_bytes.to_string(),
            reason: error.to_string(),
        })?
        .root();
        Self::try_with_limits_and_budget(limits, &process_budget)
    }

    /// Builds a table below an injected process resource root.
    ///
    /// # Errors
    ///
    /// Returns a typed configuration error when the derived budget is invalid.
    pub fn try_with_limits_and_budget(
        limits: PendingRequestLimits,
        process_budget: &ResourceBudget,
    ) -> RocketMQResult<Self> {
        let process_capacity = process_budget.limit().capacity;
        let max_count = limits.max_count.max(1).min(process_capacity.count);
        let max_bytes = limits.max_bytes.max(1).min(process_capacity.bytes);
        let request_rate = limits.admission_rate_per_second.max(1);
        let max_request_age = limits.max_request_age;
        let table_id = NEXT_TABLE_ID.fetch_add(1, Ordering::Relaxed);
        let budget = process_budget
            .child(
                format!("pending-request-table-{table_id}"),
                BudgetLimit::new(max_count, max_bytes, FullPolicy::Reject)
                    .with_rate(RateLimit::new(request_rate, request_rate))
                    .with_max_age(max_request_age),
            )
            .map_err(|error| RocketMQError::ConfigInvalidValue {
                key: "transport.pendingRequest",
                value: format!(
                    "count={max_count},bytes={max_bytes},rate={request_rate},age_ms={}",
                    max_request_age.as_millis()
                ),
                reason: error.to_string(),
            })?;
        Ok(Self {
            inner: Arc::new(PendingRequestTableInner {
                table_id,
                entries: DashMap::with_capacity(max_count),
                next_owner: AtomicU64::new(2),
                next_reservation: AtomicU64::new(1),
                default_owner: PendingRequestOwner::new(table_id, 1),
                budget,
                max_request_age,
                rejected_count: AtomicUsize::new(0),
                rejected_bytes: AtomicUsize::new(0),
            }),
        })
    }

    /// Compatibility constructor for callers that cannot return startup
    /// configuration errors.
    ///
    /// # Panics
    ///
    /// Panics when process-memory detection or budget validation fails. New
    /// production composition should use [`Self::try_with_limits`].
    pub fn with_limits(limits: PendingRequestLimits) -> Self {
        Self::try_with_limits(limits).unwrap_or_else(|error| panic!("invalid pending request resource limits: {error}"))
    }

    /// Creates a correlation owner for one physical connection.
    pub fn new_owner(&self) -> PendingRequestOwner {
        PendingRequestOwner::new(
            self.inner.table_id,
            self.inner.next_owner.fetch_add(1, Ordering::Relaxed),
        )
    }

    /// Compatibility adapter for callers that use a table as one connection.
    pub fn register(
        &self,
        opaque: i32,
        deadline: RequestDeadline,
        sender: tokio::sync::oneshot::Sender<RocketMQResult<RemotingCommand>>,
    ) -> RocketMQResult<PendingRequestGuard> {
        self.register_with_bytes(opaque, deadline, 0, sender)
    }

    pub fn register_with_bytes(
        &self,
        opaque: i32,
        deadline: RequestDeadline,
        retained_bytes: usize,
        sender: tokio::sync::oneshot::Sender<RocketMQResult<RemotingCommand>>,
    ) -> RocketMQResult<PendingRequestGuard> {
        self.register_for_owner_with_bytes(&self.inner.default_owner, opaque, deadline, retained_bytes, sender)
    }

    pub fn register_for_owner(
        &self,
        owner: &PendingRequestOwner,
        opaque: i32,
        deadline: RequestDeadline,
        sender: tokio::sync::oneshot::Sender<RocketMQResult<RemotingCommand>>,
    ) -> RocketMQResult<PendingRequestGuard> {
        self.register_for_owner_with_bytes(owner, opaque, deadline, 0, sender)
    }

    pub fn register_for_owner_with_bytes(
        &self,
        owner: &PendingRequestOwner,
        opaque: i32,
        deadline: RequestDeadline,
        retained_bytes: usize,
        sender: tokio::sync::oneshot::Sender<RocketMQResult<RemotingCommand>>,
    ) -> RocketMQResult<PendingRequestGuard> {
        let deadline = deadline.capped(self.inner.max_request_age);
        deadline.ensure_before_send("pending_request")?;
        self.validate_owner(owner)?;
        let owner_state = owner.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if !owner_state.accepting {
            return Err(RocketMQError::network_connection_failed(
                "pending_request",
                "connection owner is retired; reconnect before sending another request",
            ));
        }
        let permit = self.inner.budget.try_acquire_data(retained_bytes).map_err(|error| {
            match error.dimension() {
                BudgetDimension::Bytes => {
                    self.inner.rejected_bytes.fetch_add(1, Ordering::Relaxed);
                }
                BudgetDimension::Count | BudgetDimension::Rate => {
                    self.inner.rejected_count.fetch_add(1, Ordering::Relaxed);
                }
            }
            RocketMQError::network_queue_full("pending_request")
        })?;
        let reservation = self.inner.next_reservation.fetch_add(1, Ordering::Relaxed);
        let key = PendingRequestKey {
            owner_id: owner.id,
            opaque,
        };
        let token = PendingRequestToken { key, reservation };
        let created_at = Instant::now();
        let timeout_millis = deadline.budget_millis();
        let pending = PendingRequest {
            reservation,
            owner: owner.clone(),
            created_at,
            deadline,
            timeout_millis,
            _permit: permit,
            completion: Box::new(OneShotCompletion {
                sender: Mutex::new(Some(sender)),
            }),
        };

        let result = match self.inner.entries.entry(key) {
            Entry::Vacant(entry) => {
                entry.insert(pending);
                Ok(PendingRequestGuard {
                    table: self.clone(),
                    token: Some(token),
                    deadline,
                })
            }
            Entry::Occupied(_) => Err(RocketMQError::network_connection_failed(
                "pending_request",
                format!("opaque {opaque} is already reserved on this connection"),
            )),
        };
        drop(owner_state);
        result
    }

    /// Compatibility adapter for the table's default single-connection owner.
    pub fn complete_response(&self, opaque: i32, response: RemotingCommand) -> bool {
        self.complete_response_for_owner(&self.inner.default_owner, opaque, response)
    }

    pub fn complete_response_for_owner(
        &self,
        owner: &PendingRequestOwner,
        opaque: i32,
        response: RemotingCommand,
    ) -> bool {
        if owner.table_id != self.inner.table_id {
            return false;
        }
        let Some(pending) = self.take_key(PendingRequestKey {
            owner_id: owner.id,
            opaque,
        }) else {
            return false;
        };
        pending.completion.complete(Ok(response));
        true
    }

    pub fn len(&self) -> usize {
        self.inner.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.entries.is_empty()
    }

    pub fn usage(&self) -> PendingRequestUsage {
        let snapshot = self.inner.budget.snapshot();
        PendingRequestUsage {
            count: snapshot.current_count,
            bytes: snapshot.current_bytes,
            rejected_count: self.inner.rejected_count.load(Ordering::Relaxed),
            rejected_bytes: self.inner.rejected_bytes.load(Ordering::Relaxed),
        }
    }

    /// Returns the age of the oldest pending request for low-cardinality diagnostics.
    pub fn oldest_age(&self, now: Instant) -> Option<Duration> {
        self.inner.entries.iter().map(|entry| entry.age(now)).max()
    }

    pub fn close_owner<F>(&self, owner: &PendingRequestOwner, mut cause: F) -> usize
    where
        F: FnMut() -> RocketMQError,
    {
        if owner.table_id != self.inner.table_id {
            return 0;
        }
        let mut owner_state = owner.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        owner_state.accepting = false;
        let tokens = self.tokens_for_owner(owner.id);
        let mut completed = 0;
        for token in tokens {
            let Some(pending) = self.take_token(token) else {
                continue;
            };
            pending.completion.complete(Err(cause()));
            completed += 1;
        }
        drop(owner_state);
        completed
    }

    /// Compatibility adapter that retires and completes every active owner.
    pub fn close_all<F>(&self, mut cause: F) -> usize
    where
        F: FnMut() -> RocketMQError,
    {
        let tokens: Vec<PendingRequestToken> = self
            .inner
            .entries
            .iter()
            .map(|entry| PendingRequestToken {
                key: *entry.key(),
                reservation: entry.reservation,
            })
            .collect();
        self.inner.default_owner.retire();
        let mut completed = 0;
        for token in tokens {
            let Some(pending) = self.take_token(token) else {
                continue;
            };
            pending.owner.retire();
            pending.completion.complete(Err(cause()));
            completed += 1;
        }
        completed
    }

    /// Completes every request whose registered absolute response deadline has elapsed.
    pub fn expire_due(&self, now: tokio::time::Instant) -> usize {
        let expired: Vec<(PendingRequestToken, u64, PendingRequestOwner)> = self
            .inner
            .entries
            .iter()
            .filter(|entry| entry.is_expired(now))
            .map(|entry| {
                (
                    PendingRequestToken {
                        key: *entry.key(),
                        reservation: entry.reservation,
                    },
                    entry.timeout_millis,
                    entry.owner.clone(),
                )
            })
            .collect();
        let mut completed = 0;
        for (token, timeout_millis, owner) in expired {
            owner.retire();
            if self.complete_token(
                token,
                Err(RocketMQError::network_response_timeout(
                    "pending_request",
                    timeout_millis,
                )),
            ) {
                completed += 1;
            }
        }
        completed
    }

    #[doc(hidden)]
    pub fn complete_token(&self, token: PendingRequestToken, result: RocketMQResult<RemotingCommand>) -> bool {
        let Some(pending) = self.take_token(token) else {
            return false;
        };
        pending.completion.complete(result);
        true
    }

    fn validate_owner(&self, owner: &PendingRequestOwner) -> RocketMQResult<()> {
        if owner.table_id == self.inner.table_id {
            Ok(())
        } else {
            Err(RocketMQError::network_connection_failed(
                "pending_request",
                "connection owner belongs to a different pending request table",
            ))
        }
    }

    fn tokens_for_owner(&self, owner_id: u64) -> Vec<PendingRequestToken> {
        self.inner
            .entries
            .iter()
            .filter(|entry| entry.key().owner_id == owner_id)
            .map(|entry| PendingRequestToken {
                key: *entry.key(),
                reservation: entry.reservation,
            })
            .collect()
    }

    fn take_token(&self, token: PendingRequestToken) -> Option<PendingRequest> {
        match self.inner.entries.entry(token.key) {
            Entry::Occupied(entry) if entry.get().reservation == token.reservation => Some(entry.remove()),
            Entry::Occupied(_) | Entry::Vacant(_) => None,
        }
    }

    fn take_key(&self, key: PendingRequestKey) -> Option<PendingRequest> {
        match self.inner.entries.entry(key) {
            Entry::Occupied(entry) => Some(entry.remove()),
            Entry::Vacant(_) => None,
        }
    }
}

impl PendingRequestGuard {
    #[doc(hidden)]
    pub fn token(&self) -> PendingRequestToken {
        self.token.expect("active pending request guard must have a token")
    }

    #[doc(hidden)]
    pub fn deadline(&self) -> RequestDeadline {
        self.deadline
    }

    pub fn complete(mut self, result: RocketMQResult<RemotingCommand>) -> bool {
        let token = self
            .token
            .take()
            .expect("active pending request guard must have a token");
        self.table.complete_token(token, result)
    }

    #[doc(hidden)]
    pub fn expire(mut self, addr: impl Into<String>) -> RocketMQError {
        let token = self
            .token
            .take()
            .expect("active pending request guard must have a token");
        let addr = addr.into();
        let timeout_millis = self.deadline.budget_millis();
        let error = RocketMQError::network_response_timeout(addr.clone(), timeout_millis);
        if let Some(pending) = self.table.take_token(token) {
            pending.owner.retire();
            pending
                .completion
                .complete(Err(RocketMQError::network_response_timeout(addr, timeout_millis)));
        }
        error
    }
}

impl Drop for PendingRequestGuard {
    fn drop(&mut self) {
        if let Some(token) = self.token.take() {
            self.table.complete_token(
                token,
                Err(RocketMQError::network_connection_failed(
                    "pending_request",
                    "reservation dropped before completion",
                )),
            );
        }
    }
}

impl PendingRequest {
    fn age(&self, now: Instant) -> Duration {
        now.saturating_duration_since(self.created_at)
    }

    fn is_expired(&self, now: tokio::time::Instant) -> bool {
        now >= self.deadline.instant()
    }
}

#[cfg(test)]
mod owner_epoch_tests {
    use std::collections::HashMap;
    use std::sync::mpsc;

    use cheetah_string::CheetahString;

    use super::*;

    #[test]
    fn retained_byte_estimate_includes_body_and_dynamic_metadata() {
        let mut command = RemotingCommand::create_remoting_command(7)
            .set_remark("resource-budget")
            .set_ext_fields(HashMap::from([(
                CheetahString::from_static_str("tenant"),
                CheetahString::from_static_str("tenant-a"),
            )]))
            .set_body(vec![1_u8; 1024]);

        let estimate = materialize_and_estimate_remoting_command_retained_bytes(&mut command);

        assert!(estimate >= std::mem::size_of::<RemotingCommand>() + 1024);
        assert!(estimate > std::mem::size_of::<RemotingCommand>() + 1024 + "resource-budget".len());
    }

    #[test]
    fn registration_linearizes_behind_the_owner_close_gate() {
        let table = PendingRequestTable::new();
        let owner = table.new_owner();
        let mut closing = owner.state.lock().unwrap();
        let (started_tx, started_rx) = mpsc::channel();
        let (result_tx, result_rx) = mpsc::channel();
        let registering_table = table.clone();
        let registering_owner = owner.clone();
        let registration = std::thread::spawn(move || {
            let (sender, _receiver) = tokio::sync::oneshot::channel();
            started_tx.send(()).unwrap();
            let result = registering_table.register_for_owner(
                &registering_owner,
                7,
                RequestDeadline::from_timeout_millis(30_000),
                sender,
            );
            result_tx.send(result.is_ok()).unwrap();
        });

        started_rx.recv().unwrap();
        assert!(
            result_rx.recv_timeout(Duration::from_millis(20)).is_err(),
            "registration must wait while close owns the epoch gate"
        );
        closing.accepting = false;
        drop(closing);

        assert!(!result_rx.recv_timeout(Duration::from_secs(1)).unwrap());
        registration.join().unwrap();
        assert!(table.is_empty());
    }
}
