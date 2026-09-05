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

use std::fmt;
use std::sync::Arc;
use std::sync::Mutex;

use smallvec::SmallVec;
use std::time::Duration;
use tokio::sync::Notify;

use super::clock::MonotonicClock;
use super::clock::SystemMonotonicClock;
use super::limit::BudgetClass;
use super::limit::BudgetDimension;
use super::limit::BudgetLimit;
use super::limit::FullPolicy;
use super::limit::RateLimit;

#[derive(Debug, Clone, PartialEq, Eq)]
/// Represents budget snapshot.
pub struct BudgetSnapshot {
    /// The path value.
    pub path: Arc<str>,
    /// The number of current entries.
    pub current_count: usize,
    /// The current size in bytes.
    pub current_bytes: usize,
    /// The number of admitted entries.
    pub admitted_count: u64,
    /// The number of released entries.
    pub released_count: u64,
    /// The number of rejected entries.
    pub rejected_count: u64,
    /// The number of throttled entries.
    pub throttled_count: u64,
    /// The number of dropped entries.
    pub dropped_count: u64,
    /// The number of coalesced entries.
    pub coalesced_count: u64,
    /// The number of closed slow consumer entries.
    pub closed_slow_consumer_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// A normal resource-budget admission rejection.
pub struct BudgetRejection {
    path: Arc<str>,
    exhausted_path: Arc<str>,
    dimension: BudgetDimension,
    policy: FullPolicy,
}

impl BudgetRejection {
    #[must_use]
    /// Returns the path.
    pub fn path(&self) -> &str {
        &self.path
    }

    #[must_use]
    /// Returns the exhausted path.
    pub fn exhausted_path(&self) -> &str {
        &self.exhausted_path
    }

    #[must_use]
    /// Returns the dimension.
    pub const fn dimension(&self) -> BudgetDimension {
        self.dimension
    }

    #[must_use]
    /// Returns the policy.
    pub const fn policy(&self) -> FullPolicy {
        self.policy
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// The result of an attempted permit rebind.
pub enum PermitRebindOutcome {
    /// The permit now owns the target budget chain.
    Rebound,
    /// The permit was already bound to the requested target.
    Unchanged,
    /// The target rejected the permit and the source permit is unchanged.
    Rejected(BudgetRejection),
}

/// Represents resource budget tree.
pub struct ResourceBudgetTree {
    root: ResourceBudget,
}

impl fmt::Debug for ResourceBudgetTree {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResourceBudgetTree")
            .field("root", &self.root)
            .finish()
    }
}

impl ResourceBudgetTree {
    /// Creates a new `ResourceBudgetTree`.
    ///
    /// # Errors
    ///
    /// Returns a contract violation when the root name is blank or malformed,
    /// or when its budget limit is invalid.
    pub fn new(name: impl Into<String>, limit: BudgetLimit) -> Result<Self, crate::RuntimeContractViolation> {
        Self::with_clock(name, limit, Arc::new(SystemMonotonicClock::new()))
    }

    /// Creates a tree with an injected monotonic clock.
    ///
    /// # Errors
    ///
    /// Returns a contract violation when the root name is blank or malformed,
    /// or when its budget limit is invalid.
    pub fn with_clock(
        name: impl Into<String>,
        limit: BudgetLimit,
        clock: Arc<dyn MonotonicClock>,
    ) -> Result<Self, crate::RuntimeContractViolation> {
        let name = validated_name(name.into())?;
        limit.validate(&name)?;
        let node = Arc::new(BudgetNode::new(Arc::from(name.as_str()), limit, clock));
        let capacity_notify = Arc::new(Notify::new());
        Ok(Self {
            root: ResourceBudget {
                node: Arc::clone(&node),
                chain: Arc::from([node]),
                capacity_notify,
            },
        })
    }

    #[must_use]
    /// Returns the root.
    pub fn root(&self) -> ResourceBudget {
        self.root.clone()
    }
}

#[derive(Clone)]
/// Represents resource budget.
pub struct ResourceBudget {
    node: Arc<BudgetNode>,
    chain: Arc<[Arc<BudgetNode>]>,
    capacity_notify: Arc<Notify>,
}

impl fmt::Debug for ResourceBudget {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResourceBudget")
            .field("path", &self.node.path)
            .field("limit", &self.node.limit)
            .finish()
    }
}

impl ResourceBudget {
    /// Returns the child.
    ///
    /// # Errors
    ///
    /// Returns a contract violation when the child name is blank or malformed,
    /// its limit is invalid, or its limit exceeds its parent.
    pub fn child(&self, name: impl Into<String>, limit: BudgetLimit) -> Result<Self, crate::RuntimeContractViolation> {
        let name = validated_name(name.into())?;
        let path: Arc<str> = Arc::from(format!("{}/{}", self.node.path, name));
        limit.validate_child(self.node.limit, &path)?;
        let node = Arc::new(BudgetNode::new(path, limit, Arc::clone(&self.node.clock)));
        let mut chain = Vec::with_capacity(self.chain.len() + 1);
        chain.extend(self.chain.iter().cloned());
        chain.push(Arc::clone(&node));
        Ok(Self {
            node,
            chain: Arc::from(chain),
            capacity_notify: Arc::clone(&self.capacity_notify),
        })
    }

    /// Attempts to acquire.
    ///
    /// A [`BudgetRejection`] is a normal bounded outcome when the requested
    /// capacity is unavailable; it is not an operational runtime failure.
    pub fn try_acquire(&self, bytes: usize, class: BudgetClass) -> Result<ResourcePermit, BudgetRejection> {
        self.try_acquire_internal(bytes, class, true)
    }

    pub(crate) fn try_acquire_waiting(
        &self,
        bytes: usize,
        class: BudgetClass,
    ) -> Result<ResourcePermit, BudgetRejection> {
        self.try_acquire_internal(bytes, class, false)
    }

    fn try_acquire_internal(
        &self,
        bytes: usize,
        class: BudgetClass,
        record_failure: bool,
    ) -> Result<ResourcePermit, BudgetRejection> {
        let mut reservations = SmallVec::<[NodeReservation; 4]>::with_capacity(self.chain.len());
        for node in self.chain.iter() {
            match node.try_reserve(bytes, class) {
                Ok(reservation) => reservations.push(reservation),
                Err(dimension) => {
                    if record_failure {
                        self.record_node_rejection(node, dimension);
                    }
                    return Err(BudgetRejection {
                        path: Arc::clone(&self.node.path),
                        exhausted_path: Arc::clone(&node.path),
                        dimension,
                        policy: self.node.limit.full_policy,
                    });
                }
            }
        }
        for reservation in &mut reservations {
            reservation.commit();
        }
        Ok(ResourcePermit {
            reservations,
            bytes,
            class,
            capacity_notify: Arc::clone(&self.capacity_notify),
        })
    }

    pub(crate) fn permanent_acquire_rejection(&self, bytes: usize, class: BudgetClass) -> Option<BudgetRejection> {
        self.chain.iter().find_map(|node| {
            let dimension = node.permanent_exhaustion_dimension(bytes, class)?;
            self.record_node_rejection(node, dimension);
            Some(BudgetRejection {
                path: Arc::clone(&self.node.path),
                exhausted_path: Arc::clone(&node.path),
                dimension,
                policy: self.node.limit.full_policy,
            })
        })
    }

    pub(crate) fn record_budget_rejection(&self, error: &BudgetRejection) {
        if let Some(node) = self
            .chain
            .iter()
            .find(|node| node.path.as_ref() == error.exhausted_path())
        {
            self.record_node_rejection(node, error.dimension());
        }
    }

    fn record_node_rejection(&self, exhausted_node: &Arc<BudgetNode>, dimension: BudgetDimension) {
        exhausted_node.record_rejection(dimension);
        if !Arc::ptr_eq(exhausted_node, &self.node) {
            self.node.record_rejection(dimension);
        }
    }

    pub(crate) fn capacity_notify(&self) -> &Notify {
        &self.capacity_notify
    }

    /// Attempts to acquire data.
    ///
    /// A [`BudgetRejection`] is a normal bounded outcome when data capacity is
    /// unavailable; it is not an operational runtime failure.
    pub fn try_acquire_data(&self, bytes: usize) -> Result<ResourcePermit, BudgetRejection> {
        self.try_acquire(bytes, BudgetClass::Data)
    }

    /// Attempts to acquire control.
    ///
    /// A [`BudgetRejection`] is a normal bounded outcome when control capacity
    /// is unavailable; it is not an operational runtime failure.
    pub fn try_acquire_control(&self, bytes: usize) -> Result<ResourcePermit, BudgetRejection> {
        self.try_acquire(bytes, BudgetClass::Control)
    }

    #[must_use]
    /// Returns the snapshot.
    pub fn snapshot(&self) -> BudgetSnapshot {
        self.node.snapshot()
    }

    #[must_use]
    /// Returns the path.
    pub fn path(&self) -> &str {
        &self.node.path
    }

    #[must_use]
    /// Returns the limit.
    pub fn limit(&self) -> BudgetLimit {
        self.node.limit
    }

    /// Returns the current instant from this tree's injected monotonic clock.
    #[must_use]
    pub fn monotonic_now(&self) -> Duration {
        self.node.clock.now()
    }

    /// Records items discarded by a queue or custom retention boundary.
    pub fn record_dropped(&self, count: usize) {
        self.node.record_dropped(count);
    }

    /// Records items folded into an already retained logical item.
    pub fn record_coalesced(&self, count: usize) {
        self.node.record_coalesced(count);
    }

    /// Records a slow consumer closed by this budget's full policy.
    pub fn record_slow_consumer_closed(&self) {
        self.node.record_slow_consumer_closed();
    }
}

fn validated_name(name: String) -> Result<String, crate::RuntimeContractViolation> {
    let name = name.trim();
    if name.is_empty() {
        return Err(crate::RuntimeContractViolation::EmptyBudgetName);
    }
    if name.contains('/') {
        return Err(crate::RuntimeContractViolation::InvalidBudgetName);
    }
    Ok(name.to_owned())
}

/// Represents resource permit.
pub struct ResourcePermit {
    reservations: SmallVec<[NodeReservation; 4]>,
    bytes: usize,
    class: BudgetClass,
    capacity_notify: Arc<Notify>,
}

impl fmt::Debug for ResourcePermit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResourcePermit")
            .field(
                "path",
                &self.reservations.last().map(|reservation| &reservation.node.path),
            )
            .field("bytes", &self.bytes)
            .field("class", &self.class)
            .finish()
    }
}

impl ResourcePermit {
    #[must_use]
    /// Returns the bytes.
    pub const fn bytes(&self) -> usize {
        self.bytes
    }

    #[must_use]
    /// Returns the class.
    pub const fn class(&self) -> BudgetClass {
        self.class
    }

    /// Promotes a data reservation to the control class without releasing capacity.
    ///
    /// The total count and byte ownership remain unchanged at every budget level. Data-only
    /// counters and rate tokens are released before a same-tree rebind can use control reserve.
    pub fn promote_to_control(&mut self) {
        if self.class == BudgetClass::Control {
            return;
        }
        for reservation in &mut self.reservations {
            let mut state = reservation
                .node
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.data_count = state.data_count.saturating_sub(1);
            state.data_bytes = state.data_bytes.saturating_sub(reservation.bytes);
            state.restore_data_rate(reservation.node.limit);
            reservation.class = BudgetClass::Control;
        }
        self.class = BudgetClass::Control;
        self.capacity_notify.notify_waiters();
    }

    /// Moves this reservation to another budget in the same resource tree.
    ///
    /// Reservations for the common ancestor chain remain owned throughout the
    /// transfer. Target-only reservations are acquired before source-only
    /// reservations are released, so the payload is always covered without
    /// charging common ancestors twice.
    ///
    /// Returns a normal rejection when the target is full. Contract failures
    /// and rejections leave this permit unchanged and valid for its source.
    ///
    /// # Errors
    ///
    /// Returns [`crate::RuntimeContractViolation::PermitTargetInDifferentTree`]
    /// when `target` is outside this permit's resource-budget tree.
    pub fn try_rebind(
        &mut self,
        target: &ResourceBudget,
    ) -> Result<PermitRebindOutcome, crate::RuntimeContractViolation> {
        let common_ancestors = self
            .reservations
            .iter()
            .zip(target.chain.iter())
            .take_while(|(reservation, target_node)| Arc::ptr_eq(&reservation.node, target_node))
            .count();
        if common_ancestors == 0 {
            return Err(crate::RuntimeContractViolation::PermitTargetInDifferentTree);
        }
        if common_ancestors == self.reservations.len() && common_ancestors == target.chain.len() {
            return Ok(PermitRebindOutcome::Unchanged);
        }

        let mut target_reservations =
            SmallVec::<[NodeReservation; 4]>::with_capacity(target.chain.len() - common_ancestors);
        for node in target.chain.iter().skip(common_ancestors) {
            match node.try_reserve(self.bytes, self.class) {
                Ok(reservation) => target_reservations.push(reservation),
                Err(dimension) => {
                    target.record_node_rejection(node, dimension);
                    return Ok(PermitRebindOutcome::Rejected(BudgetRejection {
                        path: Arc::clone(&target.node.path),
                        exhausted_path: Arc::clone(&node.path),
                        dimension,
                        policy: target.node.limit.full_policy,
                    }));
                }
            }
        }
        for reservation in &mut target_reservations {
            reservation.commit();
        }

        self.reservations.truncate(common_ancestors);
        self.reservations.extend(target_reservations);
        self.capacity_notify.notify_waiters();
        Ok(PermitRebindOutcome::Rebound)
    }
}

impl Drop for ResourcePermit {
    fn drop(&mut self) {
        self.reservations.clear();
        self.capacity_notify.notify_waiters();
    }
}

struct BudgetNode {
    path: Arc<str>,
    limit: BudgetLimit,
    clock: Arc<dyn MonotonicClock>,
    state: Mutex<BudgetState>,
}

impl BudgetNode {
    fn new(path: Arc<str>, limit: BudgetLimit, clock: Arc<dyn MonotonicClock>) -> Self {
        let now = clock.now();
        Self {
            path,
            limit,
            clock,
            state: Mutex::new(BudgetState::new(limit, now)),
        }
    }

    fn try_reserve(self: &Arc<Self>, bytes: usize, class: BudgetClass) -> Result<NodeReservation, BudgetDimension> {
        let now = self.clock.now();
        let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        state.refill(self.limit, now);

        let dimension = if state.current_count >= self.limit.capacity.count {
            Some(BudgetDimension::Count)
        } else if state
            .current_bytes
            .checked_add(bytes)
            .is_none_or(|total| total > self.limit.capacity.bytes)
        {
            Some(BudgetDimension::Bytes)
        } else if class == BudgetClass::Data
            && state.data_count
                >= self
                    .limit
                    .capacity
                    .count
                    .saturating_sub(self.limit.control_reserve.count)
        {
            Some(BudgetDimension::Count)
        } else if class == BudgetClass::Data
            && state.data_bytes.checked_add(bytes).is_none_or(|total| {
                total
                    > self
                        .limit
                        .capacity
                        .bytes
                        .saturating_sub(self.limit.control_reserve.bytes)
            })
        {
            Some(BudgetDimension::Bytes)
        } else if !state.rate_available(self.limit, class) {
            Some(BudgetDimension::Rate)
        } else {
            None
        };

        if let Some(dimension) = dimension {
            return Err(dimension);
        }

        state.current_count += 1;
        state.current_bytes += bytes;
        if class == BudgetClass::Data {
            state.data_count += 1;
            state.data_bytes += bytes;
        }
        state.consume_rate(self.limit, class);
        drop(state);

        Ok(NodeReservation {
            node: Arc::clone(self),
            bytes,
            class,
            committed: false,
        })
    }

    fn permanent_exhaustion_dimension(&self, bytes: usize, class: BudgetClass) -> Option<BudgetDimension> {
        let (count_capacity, byte_capacity) = match class {
            BudgetClass::Data => (
                self.limit
                    .capacity
                    .count
                    .saturating_sub(self.limit.control_reserve.count),
                self.limit
                    .capacity
                    .bytes
                    .saturating_sub(self.limit.control_reserve.bytes),
            ),
            BudgetClass::Control => (self.limit.capacity.count, self.limit.capacity.bytes),
        };
        if count_capacity == 0 {
            Some(BudgetDimension::Count)
        } else if bytes > byte_capacity {
            Some(BudgetDimension::Bytes)
        } else {
            None
        }
    }

    fn record_rejection(&self, dimension: BudgetDimension) {
        let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        state.rejected_count = state.rejected_count.saturating_add(1);
        if dimension == BudgetDimension::Rate {
            state.throttled_count = state.throttled_count.saturating_add(1);
        }
    }

    fn record_dropped(&self, count: usize) {
        let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        state.dropped_count = state.dropped_count.saturating_add(count as u64);
    }

    fn record_coalesced(&self, count: usize) {
        let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        state.coalesced_count = state.coalesced_count.saturating_add(count as u64);
    }

    fn record_slow_consumer_closed(&self) {
        let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        state.closed_slow_consumer_count = state.closed_slow_consumer_count.saturating_add(1);
    }

    fn snapshot(&self) -> BudgetSnapshot {
        let state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        BudgetSnapshot {
            path: Arc::clone(&self.path),
            current_count: state.current_count,
            current_bytes: state.current_bytes,
            admitted_count: state.admitted_count,
            released_count: state.released_count,
            rejected_count: state.rejected_count,
            throttled_count: state.throttled_count,
            dropped_count: state.dropped_count,
            coalesced_count: state.coalesced_count,
            closed_slow_consumer_count: state.closed_slow_consumer_count,
        }
    }
}

struct NodeReservation {
    node: Arc<BudgetNode>,
    bytes: usize,
    class: BudgetClass,
    committed: bool,
}

impl NodeReservation {
    fn commit(&mut self) {
        let mut state = self
            .node
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.admitted_count = state.admitted_count.saturating_add(1);
        self.committed = true;
    }
}

impl Drop for NodeReservation {
    fn drop(&mut self) {
        let mut state = self
            .node
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.current_count = state.current_count.saturating_sub(1);
        state.current_bytes = state.current_bytes.saturating_sub(self.bytes);
        if self.class == BudgetClass::Data {
            state.data_count = state.data_count.saturating_sub(1);
            state.data_bytes = state.data_bytes.saturating_sub(self.bytes);
        }
        if self.committed {
            state.released_count = state.released_count.saturating_add(1);
        } else {
            state.restore_rate(self.node.limit, self.class);
        }
    }
}

struct BudgetState {
    current_count: usize,
    current_bytes: usize,
    data_count: usize,
    data_bytes: usize,
    total_tokens: Option<TokenBucket>,
    data_tokens: Option<TokenBucket>,
    admitted_count: u64,
    released_count: u64,
    rejected_count: u64,
    throttled_count: u64,
    dropped_count: u64,
    coalesced_count: u64,
    closed_slow_consumer_count: u64,
}

impl BudgetState {
    fn new(limit: BudgetLimit, now: Duration) -> Self {
        let total_tokens = limit.capacity.rate.map(|rate| TokenBucket::new(rate, now));
        let data_tokens = data_rate_limit(limit).map(|rate| TokenBucket::new(rate, now));
        Self {
            current_count: 0,
            current_bytes: 0,
            data_count: 0,
            data_bytes: 0,
            total_tokens,
            data_tokens,
            admitted_count: 0,
            released_count: 0,
            rejected_count: 0,
            throttled_count: 0,
            dropped_count: 0,
            coalesced_count: 0,
            closed_slow_consumer_count: 0,
        }
    }

    fn refill(&mut self, limit: BudgetLimit, now: Duration) {
        if let (Some(bucket), Some(rate)) = (&mut self.total_tokens, limit.capacity.rate) {
            bucket.refill(rate, now);
        }
        if let (Some(bucket), Some(rate)) = (&mut self.data_tokens, data_rate_limit(limit)) {
            bucket.refill(rate, now);
        }
    }

    fn rate_available(&self, limit: BudgetLimit, class: BudgetClass) -> bool {
        let total_available = self.total_tokens.as_ref().is_none_or(TokenBucket::has_token);
        let data_available = class == BudgetClass::Control
            || data_rate_limit(limit).is_none()
            || self.data_tokens.as_ref().is_some_and(TokenBucket::has_token);
        total_available && data_available
    }

    fn consume_rate(&mut self, limit: BudgetLimit, class: BudgetClass) {
        if let Some(bucket) = &mut self.total_tokens {
            bucket.consume();
        }
        if class == BudgetClass::Data && data_rate_limit(limit).is_some() {
            if let Some(bucket) = &mut self.data_tokens {
                bucket.consume();
            }
        }
    }

    fn restore_rate(&mut self, limit: BudgetLimit, class: BudgetClass) {
        if let (Some(bucket), Some(rate)) = (&mut self.total_tokens, limit.capacity.rate) {
            bucket.restore(rate);
        }
        if class == BudgetClass::Data {
            if let (Some(bucket), Some(rate)) = (&mut self.data_tokens, data_rate_limit(limit)) {
                bucket.restore(rate);
            }
        }
    }

    fn restore_data_rate(&mut self, limit: BudgetLimit) {
        if let (Some(bucket), Some(rate)) = (&mut self.data_tokens, data_rate_limit(limit)) {
            bucket.restore(rate);
        }
    }
}

fn data_rate_limit(limit: BudgetLimit) -> Option<RateLimit> {
    let total = limit.capacity.rate?;
    let reserve = limit.control_reserve.rate.unwrap_or(RateLimit::new(0, 0));
    Some(RateLimit::new(
        total.permits_per_second.saturating_sub(reserve.permits_per_second),
        total.burst.saturating_sub(reserve.burst),
    ))
}

struct TokenBucket {
    tokens: f64,
    last_refill: Duration,
}

impl TokenBucket {
    fn new(rate: RateLimit, now: Duration) -> Self {
        Self {
            tokens: rate.burst as f64,
            last_refill: now,
        }
    }

    fn refill(&mut self, rate: RateLimit, now: Duration) {
        let elapsed = now.saturating_sub(self.last_refill).as_secs_f64();
        self.tokens = (self.tokens + elapsed * rate.permits_per_second as f64).min(rate.burst as f64);
        self.last_refill = now;
    }

    fn has_token(&self) -> bool {
        self.tokens >= 1.0
    }

    fn consume(&mut self) {
        self.tokens = (self.tokens - 1.0).max(0.0);
    }

    fn restore(&mut self, rate: RateLimit) {
        self.tokens = (self.tokens + 1.0).min(rate.burst as f64);
    }
}
