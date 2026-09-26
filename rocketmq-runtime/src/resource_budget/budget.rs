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
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;

use smallvec::SmallVec;
use std::time::Duration;
use tokio::sync::Notify;

use super::clock::MonotonicClock;
use super::clock::SystemMonotonicClock;
use super::dynamic::DynamicBudgetKey;
use super::dynamic::DynamicKeyRegistrationFailure;
use super::dynamic::DynamicKeyRegistry;
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
    reason: BudgetRejectionReason,
    policy: FullPolicy,
}

/// Why a resource budget refused new work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BudgetRejectionReason {
    /// Count, byte, or rate capacity is exhausted.
    Capacity(BudgetDimension),
    /// This dynamic generation or one of its ancestors has closed admission.
    Closed,
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
    /// Returns the exhausted capacity dimension, or `None` for closed admission.
    pub const fn dimension(&self) -> Option<BudgetDimension> {
        match self.reason {
            BudgetRejectionReason::Capacity(dimension) => Some(dimension),
            BudgetRejectionReason::Closed => None,
        }
    }

    /// Returns the admission rejection reason without projecting closure onto capacity.
    #[must_use]
    pub const fn reason(&self) -> BudgetRejectionReason {
        self.reason
    }

    /// Returns whether admission has permanently closed for this handle.
    #[must_use]
    pub const fn is_closed(&self) -> bool {
        matches!(self.reason, BudgetRejectionReason::Closed)
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
                keys: DynamicKeyRegistry::new(),
                admission_gates: Arc::from([]),
            },
        })
    }

    /// Creates a tree whose dynamic key registry holds at most `max_entries`
    /// names.
    ///
    /// # Errors
    ///
    /// Returns a contract violation when the root name is blank or malformed,
    /// or when its budget limit is invalid.
    pub fn with_clock_and_key_capacity(
        name: impl Into<String>,
        limit: BudgetLimit,
        clock: Arc<dyn MonotonicClock>,
        max_entries: usize,
    ) -> Result<Self, crate::RuntimeContractViolation> {
        let mut tree = Self::with_clock(name, limit, clock)?;
        tree.root.keys = DynamicKeyRegistry::with_max_entries(max_entries);
        Ok(tree)
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
    keys: DynamicKeyRegistry,
    admission_gates: Arc<[Arc<AdmissionGate>]>,
}

struct AdmissionGate {
    path: Arc<str>,
    closed: Mutex<bool>,
}

// Always acquire gates root-to-leaf, before node or queue state locks. Static
// budgets have no gates. Keeping this guard across a synchronous queue admission
// also prevents closure from triggering destructive capacity/age policies.
pub(super) struct BudgetAdmission<'a> {
    budget: &'a ResourceBudget,
    _guards: SmallVec<[MutexGuard<'a, bool>; 2]>,
}

impl BudgetAdmission<'_> {
    pub(super) fn try_acquire(&self, bytes: usize, class: BudgetClass) -> Result<ResourcePermit, BudgetRejection> {
        self.budget.try_acquire_open(bytes, class, true)
    }

    pub(super) fn try_acquire_waiting(
        &self,
        bytes: usize,
        class: BudgetClass,
    ) -> Result<ResourcePermit, BudgetRejection> {
        self.budget.try_acquire_open(bytes, class, false)
    }
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
    /// Creates a child that inherits every dynamic ancestor's admission state.
    ///
    /// Creating a static child after closure is allowed; the resulting handle
    /// remains closed and cannot acquire or accept rebound permits.
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
            keys: self.keys.clone(),
            admission_gates: self.admission_gates.clone(),
        })
    }

    /// Registers a bounded dynamic child key under this budget.
    ///
    /// The key owns a child budget and holds its name reserved until
    /// [`DynamicBudgetKey::retire_until`] observes that the child has no
    /// reservations left. A name that is already held cannot be registered
    /// again, so a retired generation never runs beside the one that replaced
    /// it. Use [`Self::child`] when the key does not need a retirement
    /// lifecycle.
    ///
    /// # Errors
    ///
    /// Returns [`DynamicKeyRegistrationFailure`] when the child contract is
    /// invalid, a dynamic ancestor has closed, the name is still held, or the
    /// tree holds its maximum number of dynamic keys.
    pub fn register_dynamic_child(
        &self,
        name: impl Into<String>,
        limit: BudgetLimit,
    ) -> Result<DynamicBudgetKey, DynamicKeyRegistrationFailure> {
        let _admission = self.admit().map_err(|_| DynamicKeyRegistrationFailure::Closed)?;
        let name = validated_name(name.into()).map_err(DynamicKeyRegistrationFailure::Invalid)?;
        let mut child = self
            .child(name.as_str(), limit)
            .map_err(DynamicKeyRegistrationFailure::Invalid)?;
        let name: Arc<str> = Arc::clone(&child.node.path);
        let mut gates = child.admission_gates.to_vec();
        gates.push(Arc::new(AdmissionGate {
            path: name.clone(),
            closed: Mutex::new(false),
        }));
        child.admission_gates = Arc::from(gates);
        self.keys.register(child, name)
    }

    /// Returns whether a dynamic ancestor has permanently closed admission.
    /// Existing permits remain valid and can still be released or moved to an open budget.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.admission_gates
            .iter()
            .any(|gate| *gate.closed.lock().unwrap_or_else(std::sync::PoisonError::into_inner))
    }

    pub(super) fn close_dynamic(&self) {
        if let Some(gate) = self.admission_gates.last() {
            *gate.closed.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = true;
            self.capacity_notify.notify_waiters();
        }
    }

    pub(super) fn admit(&self) -> Result<BudgetAdmission<'_>, BudgetRejection> {
        let mut guards = SmallVec::new();
        for gate in self.admission_gates.iter() {
            let guard = gate.closed.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            if *guard {
                let rejection = BudgetRejection {
                    path: self.node.path.clone(),
                    exhausted_path: gate.path.clone(),
                    reason: BudgetRejectionReason::Closed,
                    policy: self.node.limit.full_policy,
                };
                self.record_budget_rejection(&rejection);
                return Err(rejection);
            }
            guards.push(guard);
        }
        Ok(BudgetAdmission {
            budget: self,
            _guards: guards,
        })
    }

    /// Attempts to acquire.
    ///
    /// A [`BudgetRejection`] is a normal bounded outcome when the requested
    /// capacity is unavailable; it is not an operational runtime failure.
    pub fn try_acquire(&self, bytes: usize, class: BudgetClass) -> Result<ResourcePermit, BudgetRejection> {
        self.try_acquire_internal(bytes, class, true)
    }

    fn try_acquire_internal(
        &self,
        bytes: usize,
        class: BudgetClass,
        record_failure: bool,
    ) -> Result<ResourcePermit, BudgetRejection> {
        let _admission = self.admit()?;
        self.try_acquire_open(bytes, class, record_failure)
    }

    fn try_acquire_open(
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
                        self.record_node_rejection(node, BudgetRejectionReason::Capacity(dimension));
                    }
                    return Err(BudgetRejection {
                        path: Arc::clone(&self.node.path),
                        exhausted_path: Arc::clone(&node.path),
                        reason: BudgetRejectionReason::Capacity(dimension),
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
        let _admission = match self.admit() {
            Ok(admission) => admission,
            Err(rejection) => return Some(rejection),
        };
        self.chain.iter().find_map(|node| {
            let dimension = node.permanent_exhaustion_dimension(bytes, class)?;
            self.record_node_rejection(node, BudgetRejectionReason::Capacity(dimension));
            Some(BudgetRejection {
                path: Arc::clone(&self.node.path),
                exhausted_path: Arc::clone(&node.path),
                reason: BudgetRejectionReason::Capacity(dimension),
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
            self.record_node_rejection(node, error.reason());
        }
    }

    fn record_node_rejection(&self, exhausted_node: &Arc<BudgetNode>, reason: BudgetRejectionReason) {
        exhausted_node.record_rejection(reason);
        if !Arc::ptr_eq(exhausted_node, &self.node) {
            self.node.record_rejection(reason);
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
            let node = &reservation.node;
            release_reserved(&node.data_count, 1);
            release_reserved(&node.data_bytes, reservation.bytes);
            if let Some(rate) = &node.rate {
                BudgetNode::lock_rate(rate).restore_data(node.limit);
            }
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
        if !self.belongs_to_tree(target) {
            return Err(crate::RuntimeContractViolation::PermitTargetInDifferentTree);
        }
        let admission = match target.admit() {
            Ok(admission) => admission,
            Err(rejection) => return Ok(PermitRebindOutcome::Rejected(rejection)),
        };
        Ok(self.try_rebind_admitted(&admission))
    }

    pub(super) fn belongs_to_tree(&self, target: &ResourceBudget) -> bool {
        self.reservations
            .first()
            .zip(target.chain.first())
            .is_some_and(|(reservation, node)| Arc::ptr_eq(&reservation.node, node))
    }

    pub(super) fn try_rebind_admitted(&mut self, admission: &BudgetAdmission<'_>) -> PermitRebindOutcome {
        let target = admission.budget;
        let common_ancestors = self
            .reservations
            .iter()
            .zip(target.chain.iter())
            .take_while(|(reservation, target_node)| Arc::ptr_eq(&reservation.node, target_node))
            .count();
        debug_assert!(
            common_ancestors > 0,
            "caller validates the permit tree before admission"
        );
        if common_ancestors == self.reservations.len() && common_ancestors == target.chain.len() {
            return PermitRebindOutcome::Unchanged;
        }

        let mut target_reservations =
            SmallVec::<[NodeReservation; 4]>::with_capacity(target.chain.len() - common_ancestors);
        for node in target.chain.iter().skip(common_ancestors) {
            match node.try_reserve(self.bytes, self.class) {
                Ok(reservation) => target_reservations.push(reservation),
                Err(dimension) => {
                    target.record_node_rejection(node, BudgetRejectionReason::Capacity(dimension));
                    return PermitRebindOutcome::Rejected(BudgetRejection {
                        path: Arc::clone(&target.node.path),
                        exhausted_path: Arc::clone(&node.path),
                        reason: BudgetRejectionReason::Capacity(dimension),
                        policy: target.node.limit.full_policy,
                    });
                }
            }
        }
        for reservation in &mut target_reservations {
            reservation.commit();
        }

        self.reservations.truncate(common_ancestors);
        self.reservations.extend(target_reservations);
        self.capacity_notify.notify_waiters();
        PermitRebindOutcome::Rebound
    }
}

impl Drop for ResourcePermit {
    fn drop(&mut self) {
        self.reservations.clear();
        self.capacity_notify.notify_waiters();
    }
}

/// One budget level.
///
/// Count and byte capacity is reserved with atomic read-modify-writes. A
/// request that fails at a later dimension, or at a later node of its chain,
/// rolls back what it reserved, so no counter ever exceeds its limit. A
/// concurrent request that observes capacity during such a rollback can be
/// rejected even though the capacity is about to return.
///
/// Only a node that configures a rate takes a lock, around its token buckets.
struct BudgetNode {
    path: Arc<str>,
    limit: BudgetLimit,
    clock: Arc<dyn MonotonicClock>,
    current_count: AtomicUsize,
    current_bytes: AtomicUsize,
    data_count: AtomicUsize,
    data_bytes: AtomicUsize,
    counters: BudgetCounters,
    rate: Option<Mutex<RateState>>,
}

#[derive(Default)]
struct BudgetCounters {
    admitted: AtomicU64,
    released: AtomicU64,
    rejected: AtomicU64,
    throttled: AtomicU64,
    dropped: AtomicU64,
    coalesced: AtomicU64,
    closed_slow_consumer: AtomicU64,
}

/// Adds `amount` to `counter` when the sum stays within `limit`.
fn try_reserve_within(counter: &AtomicUsize, amount: usize, limit: usize) -> bool {
    counter
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            current.checked_add(amount).filter(|total| *total <= limit)
        })
        .is_ok()
}

fn release_reserved(counter: &AtomicUsize, amount: usize) {
    let previous = counter.fetch_sub(amount, Ordering::AcqRel);
    debug_assert!(previous >= amount, "budget capacity released more than reserved");
}

impl BudgetNode {
    fn new(path: Arc<str>, limit: BudgetLimit, clock: Arc<dyn MonotonicClock>) -> Self {
        let rate = limit
            .capacity
            .rate
            .map(|_| Mutex::new(RateState::new(limit, clock.now())));
        Self {
            path,
            limit,
            clock,
            current_count: AtomicUsize::new(0),
            current_bytes: AtomicUsize::new(0),
            data_count: AtomicUsize::new(0),
            data_bytes: AtomicUsize::new(0),
            counters: BudgetCounters::default(),
            rate,
        }
    }

    fn lock_rate(rate: &Mutex<RateState>) -> MutexGuard<'_, RateState> {
        rate.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn try_reserve(self: &Arc<Self>, bytes: usize, class: BudgetClass) -> Result<NodeReservation, BudgetDimension> {
        let capacity = self.limit.capacity;
        let control_reserve = self.limit.control_reserve;
        if !try_reserve_within(&self.current_count, 1, capacity.count) {
            return Err(BudgetDimension::Count);
        }
        if !try_reserve_within(&self.current_bytes, bytes, capacity.bytes) {
            release_reserved(&self.current_count, 1);
            return Err(BudgetDimension::Bytes);
        }
        if class == BudgetClass::Data {
            if !try_reserve_within(
                &self.data_count,
                1,
                capacity.count.saturating_sub(control_reserve.count),
            ) {
                self.release_capacity(bytes, BudgetClass::Control);
                return Err(BudgetDimension::Count);
            }
            if !try_reserve_within(
                &self.data_bytes,
                bytes,
                capacity.bytes.saturating_sub(control_reserve.bytes),
            ) {
                release_reserved(&self.data_count, 1);
                self.release_capacity(bytes, BudgetClass::Control);
                return Err(BudgetDimension::Bytes);
            }
        }
        if let Some(rate) = &self.rate {
            let mut rate = Self::lock_rate(rate);
            rate.refill(self.limit, self.clock.now());
            if !rate.available(self.limit, class) {
                drop(rate);
                self.release_capacity(bytes, class);
                return Err(BudgetDimension::Rate);
            }
            rate.consume(self.limit, class);
        }

        Ok(NodeReservation {
            node: Arc::clone(self),
            bytes,
            class,
            committed: false,
        })
    }

    /// Returns reserved count and bytes; data counters too for a data reservation.
    fn release_capacity(&self, bytes: usize, class: BudgetClass) {
        if class == BudgetClass::Data {
            release_reserved(&self.data_count, 1);
            release_reserved(&self.data_bytes, bytes);
        }
        release_reserved(&self.current_bytes, bytes);
        release_reserved(&self.current_count, 1);
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

    fn record_rejection(&self, reason: BudgetRejectionReason) {
        self.counters.rejected.fetch_add(1, Ordering::Relaxed);
        if reason == BudgetRejectionReason::Capacity(BudgetDimension::Rate) {
            self.counters.throttled.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn record_dropped(&self, count: usize) {
        self.counters.dropped.fetch_add(count as u64, Ordering::Relaxed);
    }

    fn record_coalesced(&self, count: usize) {
        self.counters.coalesced.fetch_add(count as u64, Ordering::Relaxed);
    }

    fn record_slow_consumer_closed(&self) {
        self.counters.closed_slow_consumer.fetch_add(1, Ordering::Relaxed);
    }

    /// Reads each counter atomically; the fields are not one consistent cut.
    fn snapshot(&self) -> BudgetSnapshot {
        BudgetSnapshot {
            path: Arc::clone(&self.path),
            current_count: self.current_count.load(Ordering::Acquire),
            current_bytes: self.current_bytes.load(Ordering::Acquire),
            admitted_count: self.counters.admitted.load(Ordering::Relaxed),
            released_count: self.counters.released.load(Ordering::Relaxed),
            rejected_count: self.counters.rejected.load(Ordering::Relaxed),
            throttled_count: self.counters.throttled.load(Ordering::Relaxed),
            dropped_count: self.counters.dropped.load(Ordering::Relaxed),
            coalesced_count: self.counters.coalesced.load(Ordering::Relaxed),
            closed_slow_consumer_count: self.counters.closed_slow_consumer.load(Ordering::Relaxed),
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
        self.node.counters.admitted.fetch_add(1, Ordering::Relaxed);
        self.committed = true;
    }
}

impl Drop for NodeReservation {
    fn drop(&mut self) {
        self.node.release_capacity(self.bytes, self.class);
        if self.committed {
            self.node.counters.released.fetch_add(1, Ordering::Relaxed);
        } else if let Some(rate) = &self.node.rate {
            // A rolled-back reservation returns the rate token it consumed.
            BudgetNode::lock_rate(rate).restore(self.node.limit, self.class);
        }
    }
}

/// Token buckets of a rate-limited node.
struct RateState {
    total_tokens: Option<TokenBucket>,
    data_tokens: Option<TokenBucket>,
}

impl RateState {
    fn new(limit: BudgetLimit, now: Duration) -> Self {
        Self {
            total_tokens: limit.capacity.rate.map(|rate| TokenBucket::new(rate, now)),
            data_tokens: data_rate_limit(limit).map(|rate| TokenBucket::new(rate, now)),
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

    fn available(&self, limit: BudgetLimit, class: BudgetClass) -> bool {
        let total_available = self.total_tokens.as_ref().is_none_or(TokenBucket::has_token);
        let data_available = class == BudgetClass::Control
            || data_rate_limit(limit).is_none()
            || self.data_tokens.as_ref().is_some_and(TokenBucket::has_token);
        total_available && data_available
    }

    fn consume(&mut self, limit: BudgetLimit, class: BudgetClass) {
        if let Some(bucket) = &mut self.total_tokens {
            bucket.consume();
        }
        if class == BudgetClass::Data && data_rate_limit(limit).is_some() {
            if let Some(bucket) = &mut self.data_tokens {
                bucket.consume();
            }
        }
    }

    fn restore(&mut self, limit: BudgetLimit, class: BudgetClass) {
        if let (Some(bucket), Some(rate)) = (&mut self.total_tokens, limit.capacity.rate) {
            bucket.restore(rate);
        }
        if class == BudgetClass::Data {
            self.restore_data(limit);
        }
    }

    fn restore_data(&mut self, limit: BudgetLimit) {
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
