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

use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;

use crate::protocol::remoting_command::RemotingCommand;

static NEXT_TABLE_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct PendingRequestKey {
    owner_id: u64,
    opaque: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PendingRequestToken {
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
    accepting: Arc<AtomicBool>,
}

impl PendingRequestOwner {
    fn new(table_id: u64, id: u64) -> Self {
        Self {
            table_id,
            id,
            accepting: Arc::new(AtomicBool::new(true)),
        }
    }

    fn retire(&self) {
        self.accepting.store(false, Ordering::Release);
    }

    fn is_accepting(&self) -> bool {
        self.accepting.load(Ordering::Acquire)
    }
}

struct PendingRequest {
    reservation: u64,
    owner: PendingRequestOwner,
    created_at: Instant,
    deadline: Instant,
    timeout_millis: u64,
    _permit: OwnedSemaphorePermit,
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
    permits: Arc<Semaphore>,
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
    deadline: Instant,
}

impl Default for PendingRequestTable {
    fn default() -> Self {
        Self::new()
    }
}

impl PendingRequestTable {
    const DEFAULT_CAPACITY: usize = 65_536;

    pub fn new() -> Self {
        Self::with_capacity(Self::DEFAULT_CAPACITY)
    }

    pub fn with_capacity(capacity: usize) -> Self {
        let capacity = capacity.max(1);
        let table_id = NEXT_TABLE_ID.fetch_add(1, Ordering::Relaxed);
        Self {
            inner: Arc::new(PendingRequestTableInner {
                table_id,
                entries: DashMap::with_capacity(capacity),
                next_owner: AtomicU64::new(2),
                next_reservation: AtomicU64::new(1),
                default_owner: PendingRequestOwner::new(table_id, 1),
                permits: Arc::new(Semaphore::new(capacity)),
            }),
        }
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
        timeout_millis: u64,
        sender: tokio::sync::oneshot::Sender<RocketMQResult<RemotingCommand>>,
    ) -> RocketMQResult<PendingRequestGuard> {
        self.register_for_owner(&self.inner.default_owner, opaque, timeout_millis, sender)
    }

    pub fn register_for_owner(
        &self,
        owner: &PendingRequestOwner,
        opaque: i32,
        timeout_millis: u64,
        sender: tokio::sync::oneshot::Sender<RocketMQResult<RemotingCommand>>,
    ) -> RocketMQResult<PendingRequestGuard> {
        self.validate_owner(owner)?;
        if !owner.is_accepting() {
            return Err(RocketMQError::network_connection_failed(
                "pending_request",
                "connection owner is retired; reconnect before sending another request",
            ));
        }
        let permit = self.inner.permits.clone().try_acquire_owned().map_err(|_| {
            RocketMQError::network_connection_failed("pending_request", "pending request admission capacity exhausted")
        })?;
        let reservation = self.inner.next_reservation.fetch_add(1, Ordering::Relaxed);
        let key = PendingRequestKey {
            owner_id: owner.id,
            opaque,
        };
        let token = PendingRequestToken { key, reservation };
        let created_at = Instant::now();
        let deadline = created_at + Duration::from_millis(timeout_millis);
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

        match self.inner.entries.entry(key) {
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
        }
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
        owner.retire();
        let tokens = self.tokens_for_owner(owner.id);
        let mut completed = 0;
        for token in tokens {
            let Some(pending) = self.take_token(token) else {
                continue;
            };
            pending.completion.complete(Err(cause()));
            completed += 1;
        }
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
    pub fn expire_due(&self, now: Instant) -> usize {
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
                Err(RocketMQError::Timeout {
                    operation: "pending_request_response",
                    timeout_ms: timeout_millis,
                }),
            ) {
                completed += 1;
            }
        }
        completed
    }

    pub(crate) fn complete_token(&self, token: PendingRequestToken, result: RocketMQResult<RemotingCommand>) -> bool {
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
    pub(crate) fn token(&self) -> PendingRequestToken {
        self.token.expect("active pending request guard must have a token")
    }

    pub(crate) fn deadline(&self) -> Instant {
        self.deadline
    }

    pub fn complete(mut self, result: RocketMQResult<RemotingCommand>) -> bool {
        let token = self
            .token
            .take()
            .expect("active pending request guard must have a token");
        self.table.complete_token(token, result)
    }

    pub(crate) fn expire(mut self, operation: &'static str, timeout_millis: u64) -> RocketMQError {
        let token = self
            .token
            .take()
            .expect("active pending request guard must have a token");
        let error = RocketMQError::Timeout {
            operation,
            timeout_ms: timeout_millis,
        };
        if let Some(pending) = self.table.take_token(token) {
            pending.owner.retire();
            pending.completion.complete(Err(RocketMQError::Timeout {
                operation,
                timeout_ms: timeout_millis,
            }));
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

    fn is_expired(&self, now: Instant) -> bool {
        now >= self.deadline
    }
}
