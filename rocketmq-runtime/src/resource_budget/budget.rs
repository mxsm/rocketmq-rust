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
use std::sync::atomic::fence;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;

use smallvec::SmallVec;
use std::time::Duration;
use tokio::sync::futures::Notified;
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

/// Represents budget snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
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

/// A normal resource-budget admission rejection.
#[derive(Debug, Clone, PartialEq, Eq)]
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

/// The result of moving a permit's reservations from its source budget to a
/// target budget in the same tree.
///
/// Reservations held by ancestors shared by both budgets stay held throughout
/// the transfer. A rejection leaves the source permit exactly as it was.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PermitBudgetTransferStatus {
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
        let admission_gates: Arc<[Arc<AdmissionGate>]> = Arc::from([]);
        let capacity_notify = Arc::new(CapacitySignal::default());
        let node = Arc::new(BudgetNode::new(
            Arc::from(name.as_str()),
            limit,
            clock,
            admission_gates.clone(),
            Arc::clone(&capacity_notify),
            true,
        ));
        Ok(Self {
            root: ResourceBudget {
                node: Arc::clone(&node),
                chain: Arc::from([node]),
                capacity_notify,
                keys: DynamicKeyRegistry::new(),
                admission_gates,
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

/// Represents resource budget.
#[derive(Clone)]
pub struct ResourceBudget {
    node: Arc<BudgetNode>,
    chain: Arc<[Arc<BudgetNode>]>,
    capacity_notify: Arc<CapacitySignal>,
    keys: DynamicKeyRegistry,
    admission_gates: Arc<[Arc<AdmissionGate>]>,
}

/// Wakes tasks of one budget tree that wait for capacity or for a dynamic budget to close.
///
/// Every permit release, rebind, shrink, and promotion in the tree signals capacity, and those
/// happen several times per request, so a capacity signal first checks whether anyone waits
/// instead of always taking the `Notify` lock. A waiter enables its [`Notified`] and registers
/// through [`Self::register_waiter`] before its final capacity check; a signal fences after the
/// capacity change and before reading the waiter count. One of the two always observes the
/// other, so a release is never lost. Closure is rare and always notifies.
#[derive(Default)]
pub(crate) struct CapacitySignal {
    notify: Notify,
    waiters: AtomicUsize,
}

impl CapacitySignal {
    /// Returns a future completed by the next capacity or closure signal.
    pub(crate) fn notified(&self) -> Notified<'_> {
        self.notify.notified()
    }

    /// Counts the caller as waiting for capacity until the guard drops.
    ///
    /// Call it after enabling the [`Notified`] from [`Self::notified`] and before the final
    /// capacity check.
    pub(crate) fn register_waiter(&self) -> CapacityWaiter<'_> {
        self.waiters.fetch_add(1, Ordering::SeqCst);
        fence(Ordering::SeqCst);
        CapacityWaiter { signal: self }
    }

    /// Signals a capacity change, after it is visible in the budget counters.
    fn signal(&self) {
        fence(Ordering::SeqCst);
        if self.waiters.load(Ordering::Acquire) != 0 {
            self.notify.notify_waiters();
        }
    }

    /// Signals that a dynamic budget closed, waking every enabled waiter.
    fn signal_closed(&self) {
        self.notify.notify_waiters();
    }
}

/// A waiter counted by [`CapacitySignal::register_waiter`].
pub(crate) struct CapacityWaiter<'a> {
    signal: &'a CapacitySignal,
}

impl Drop for CapacityWaiter<'_> {
    fn drop(&mut self) {
        self.signal.waiters.fetch_sub(1, Ordering::Release);
    }
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
        self.child_with_gates(name, limit, self.admission_gates.clone(), true)
    }

    /// Creates a group child named `name` together with all of its children.
    ///
    /// The group joins its children under one path, `<this>/<name>/<child>`, and enforces
    /// `limit` like any other level. When no rate is configured and the children's capacities,
    /// and their capacities net of the control reserve, add up to no more than the group's,
    /// the group can never run out before one of its children does. It then counts nothing
    /// itself, which saves one shared update per permit taken below it. The group handle is
    /// not returned, so nothing acquires from it directly or adds children to it later.
    ///
    /// # Errors
    ///
    /// Returns a contract violation when a name or limit is invalid, or when a child limit
    /// exceeds the group limit.
    pub fn child_group<const N: usize>(
        &self,
        name: impl Into<String>,
        limit: BudgetLimit,
        children: [(&str, BudgetLimit); N],
    ) -> Result<[Self; N], crate::RuntimeContractViolation> {
        let counted = !children_fit_within(limit, children.iter().map(|(_, child)| *child));
        let group = self.child_with_gates(name, limit, self.admission_gates.clone(), counted)?;
        let mut created = Vec::with_capacity(N);
        for (child_name, child_limit) in children {
            created.push(group.child(child_name, child_limit)?);
        }
        Ok(created
            .try_into()
            .unwrap_or_else(|_: Vec<Self>| unreachable!("one budget is created for each of the N children")))
    }

    fn child_with_gates(
        &self,
        name: impl Into<String>,
        limit: BudgetLimit,
        admission_gates: Arc<[Arc<AdmissionGate>]>,
        counted: bool,
    ) -> Result<Self, crate::RuntimeContractViolation> {
        let name = validated_name(name.into())?;
        let path: Arc<str> = Arc::from(format!("{}/{}", self.node.path, name));
        limit.validate_child(self.node.limit, &path)?;
        let node = Arc::new(BudgetNode::new(
            path,
            limit,
            Arc::clone(&self.node.clock),
            admission_gates.clone(),
            Arc::clone(&self.capacity_notify),
            counted,
        ));
        let mut chain = Vec::with_capacity(self.chain.len() + 1);
        chain.extend(self.chain.iter().cloned());
        chain.push(Arc::clone(&node));
        Ok(Self {
            node,
            chain: Arc::from(chain),
            capacity_notify: Arc::clone(&self.capacity_notify),
            keys: self.keys.clone(),
            admission_gates,
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
        let path: Arc<str> = Arc::from(format!("{}/{}", self.node.path, name));
        let mut gates = self.admission_gates.to_vec();
        gates.push(Arc::new(AdmissionGate {
            path: path.clone(),
            closed: Mutex::new(false),
        }));
        let child = self
            .child_with_gates(name, limit, Arc::from(gates), true)
            .map_err(DynamicKeyRegistrationFailure::Invalid)?;
        self.keys.register(child, path)
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
            self.capacity_notify.signal_closed();
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
        let mut reservations = PendingReservations::new(&self.chain, bytes, class);
        if let Err((node, dimension)) = reservations.reserve_all() {
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
        reservations.commit();
        Ok(ResourcePermit {
            chain: Arc::clone(&self.chain),
            bytes,
            class,
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

    pub(crate) fn capacity_notify(&self) -> &CapacitySignal {
        &self.capacity_notify
    }

    /// Returns whether a dynamic ancestor can close this budget's admission.
    ///
    /// A budget without dynamic ancestors never closes, so nothing needs to wait for that.
    pub(crate) fn can_close(&self) -> bool {
        !self.admission_gates.is_empty()
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

/// Whether a group with `limit` can never run out before one of `children` does.
///
/// Every reservation below the group is held by exactly one child subtree, and each child
/// keeps its own total and data-class usage within its capacity, so the group's usage never
/// exceeds the children's summed capacities. A rate cannot be derived from the children.
fn children_fit_within(limit: BudgetLimit, children: impl Iterator<Item = BudgetLimit>) -> bool {
    if limit.capacity.rate.is_some() {
        return false;
    }
    let mut sums = Some([0usize; 4]);
    for child in children {
        sums = sums.and_then(|[count, bytes, data_count, data_bytes]| {
            let data = data_capacity(child);
            Some([
                count.checked_add(child.capacity.count)?,
                bytes.checked_add(child.capacity.bytes)?,
                data_count.checked_add(data.0)?,
                data_bytes.checked_add(data.1)?,
            ])
        });
    }
    let Some([count, bytes, data_count, data_bytes]) = sums else {
        return false;
    };
    let (group_data_count, group_data_bytes) = data_capacity(limit);
    count <= limit.capacity.count
        && bytes <= limit.capacity.bytes
        && data_count <= group_data_count
        && data_bytes <= group_data_bytes
}

/// The count and bytes the data class may hold: capacity net of the control reserve.
fn data_capacity(limit: BudgetLimit) -> (usize, usize) {
    (
        limit.capacity.count.saturating_sub(limit.control_reserve.count),
        limit.capacity.bytes.saturating_sub(limit.control_reserve.bytes),
    )
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
///
/// The permit holds one count and its `bytes` of its `class` at every node of `chain`, the
/// chain of the budget it was acquired from or last rebound to, so a node needs no
/// reservation record of its own and the permit shares the budget's chain instead of
/// holding each level.
pub struct ResourcePermit {
    chain: Arc<[Arc<BudgetNode>]>,
    bytes: usize,
    class: BudgetClass,
}

impl fmt::Debug for ResourcePermit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResourcePermit")
            .field("path", &self.chain.last().map(|node| &node.path))
            .field("bytes", &self.bytes)
            .field("class", &self.class)
            .finish()
    }
}

impl ResourcePermit {
    /// Changes the byte reservation without acquiring another count or rate token.
    ///
    /// Growth reserves only the difference at each ancestor. A rejection rolls
    /// back every added byte and leaves this permit unchanged. Shrinking is
    /// allowed after budget closure; callers release the removed payload before
    /// shrinking its charge. This method does not block on available capacity.
    ///
    /// # Errors
    ///
    /// Returns a rejection if growth exceeds any ancestor's byte limit or a
    /// dynamic ancestor has closed admission.
    pub fn try_resize(&mut self, bytes: usize) -> Result<(), BudgetRejection> {
        if bytes <= self.bytes {
            self.shrink_to(bytes);
            return Ok(());
        }
        let chain = &self.chain;
        // Every budget chain starts at its tree root, so a permit always has a leaf.
        let Some(leaf) = chain.last() else {
            return Ok(());
        };
        let mut guards = SmallVec::<[MutexGuard<'_, bool>; 2]>::new();
        for gate in leaf.admission_gates.iter() {
            let guard = gate.closed.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            if *guard {
                leaf.record_rejection(BudgetRejectionReason::Closed);
                return Err(BudgetRejection {
                    path: leaf.path.clone(),
                    exhausted_path: gate.path.clone(),
                    reason: BudgetRejectionReason::Closed,
                    policy: leaf.limit.full_policy,
                });
            }
            guards.push(guard);
        }
        let additional = bytes - self.bytes;
        for (index, node) in chain.iter().enumerate() {
            if !node.try_reserve_bytes(additional, self.class) {
                for reserved in &chain[..index] {
                    reserved.release_bytes(additional, self.class);
                }
                let reason = BudgetRejectionReason::Capacity(BudgetDimension::Bytes);
                node.record_rejection(reason);
                if !Arc::ptr_eq(node, leaf) {
                    leaf.record_rejection(reason);
                }
                node.capacity_notify.signal();
                return Err(BudgetRejection {
                    path: leaf.path.clone(),
                    exhausted_path: node.path.clone(),
                    reason,
                    policy: leaf.limit.full_policy,
                });
            }
        }
        drop(guards);
        self.bytes = bytes;
        Ok(())
    }

    pub(crate) fn shrink_to(&mut self, bytes: usize) {
        let released = self.bytes.saturating_sub(bytes);
        if released == 0 {
            return;
        }
        for node in self.chain.iter() {
            node.release_bytes(released, self.class);
        }
        self.bytes = bytes;
        self.signal_capacity();
    }

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
        for node in self.chain.iter() {
            node.release_data_class(self.bytes);
            if let Some(rate) = &node.rate {
                BudgetNode::lock_rate(rate).restore_data(node.limit);
            }
        }
        self.class = BudgetClass::Control;
        self.signal_capacity();
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
    ) -> Result<PermitBudgetTransferStatus, crate::RuntimeContractViolation> {
        if !self.belongs_to_tree(target) {
            return Err(crate::RuntimeContractViolation::PermitTargetInDifferentTree);
        }
        let admission = match target.admit() {
            Ok(admission) => admission,
            Err(rejection) => return Ok(PermitBudgetTransferStatus::Rejected(rejection)),
        };
        Ok(self.try_rebind_admitted(&admission))
    }

    pub(super) fn belongs_to_tree(&self, target: &ResourceBudget) -> bool {
        self.chain
            .first()
            .zip(target.chain.first())
            .is_some_and(|(held, node)| Arc::ptr_eq(held, node))
    }

    pub(super) fn try_rebind_admitted(&mut self, admission: &BudgetAdmission<'_>) -> PermitBudgetTransferStatus {
        let target = admission.budget;
        let common_ancestors = self
            .chain
            .iter()
            .zip(target.chain.iter())
            .take_while(|(held, target_node)| Arc::ptr_eq(held, target_node))
            .count();
        debug_assert!(
            common_ancestors > 0,
            "caller validates the permit tree before admission"
        );
        if common_ancestors == self.chain.len() && common_ancestors == target.chain.len() {
            return PermitBudgetTransferStatus::Unchanged;
        }

        let mut target_reservations =
            PendingReservations::new(&target.chain[common_ancestors..], self.bytes, self.class);
        if let Err((node, dimension)) = target_reservations.reserve_all() {
            target.record_node_rejection(node, BudgetRejectionReason::Capacity(dimension));
            return PermitBudgetTransferStatus::Rejected(BudgetRejection {
                path: Arc::clone(&target.node.path),
                exhausted_path: Arc::clone(&node.path),
                reason: BudgetRejectionReason::Capacity(dimension),
                policy: target.node.limit.full_policy,
            });
        }
        target_reservations.commit();

        for node in &self.chain[common_ancestors..] {
            node.release_held(self.bytes, self.class);
        }
        // The target chain starts with the common ancestors this permit keeps holding.
        self.chain = Arc::clone(&target.chain);
        self.signal_capacity();
        PermitBudgetTransferStatus::Rebound
    }

    fn signal_capacity(&self) {
        if let Some(root) = self.chain.first() {
            root.capacity_notify.signal();
        }
    }
}

impl Drop for ResourcePermit {
    fn drop(&mut self) {
        for node in self.chain.iter() {
            node.release_held(self.bytes, self.class);
        }
        self.signal_capacity();
    }
}

/// One budget level.
///
/// Count and byte capacity is reserved with atomic read-modify-writes on packed
/// [`PackedUsage`] words: one for all reservations and one for the data class.
/// A request that fails at the data word, or at a later node of its chain,
/// rolls back what it reserved, so no counter ever exceeds its limit. A
/// concurrent request that observes capacity during such a rollback can be
/// rejected even though the capacity is about to return.
///
/// A node of a [`ResourceBudget::child_group`] whose children fit within it
/// can never be the first to run out, so it does not count at all.
///
/// Only a node that configures a rate takes a lock, around its token buckets.
struct BudgetNode {
    path: Arc<str>,
    limit: BudgetLimit,
    clock: Arc<dyn MonotonicClock>,
    counted: bool,
    total: PackedUsage,
    data: PackedUsage,
    total_limit: PackedLimit,
    data_limit: PackedLimit,
    counters: BudgetCounters,
    rate: Option<Mutex<RateState>>,
    // Resize uses the leaf node's gates, including after rebind. Keep this
    // shared state out of every ResourcePermit and its downstream containers.
    admission_gates: Arc<[Arc<AdmissionGate>]>,
    // Every node of a tree shares the tree's signal, so a permit reaches it through its chain.
    capacity_notify: Arc<CapacitySignal>,
}

/// Released counts follow from `admitted` minus what is held, so only admissions are counted.
#[derive(Default)]
struct BudgetCounters {
    admitted: AtomicU64,
    rejected: AtomicU64,
    throttled: AtomicU64,
    dropped: AtomicU64,
    coalesced: AtomicU64,
    closed_slow_consumer: AtomicU64,
}

/// Bits of a [`PackedUsage`] word that hold bytes; the count takes the rest.
const PACKED_BYTE_BITS: u32 = 40;
const PACKED_BYTE_MASK: u64 = (1 << PACKED_BYTE_BITS) - 1;
/// Largest count one budget level holds: 16,777,215.
const MAX_PACKED_COUNT: u64 = (1 << (u64::BITS - PACKED_BYTE_BITS)) - 1;
/// Largest byte total one budget level holds: 1 TiB minus one byte.
const MAX_PACKED_BYTES: u64 = PACKED_BYTE_MASK;

/// A count and a byte limit as a [`PackedUsage`] word holds them.
///
/// A configured limit beyond a field's maximum is treated as that maximum; no
/// process holds 16.7 million permits or a terabyte of reservations at once.
#[derive(Clone, Copy)]
struct PackedLimit {
    count: u64,
    bytes: u64,
}

impl PackedLimit {
    fn new(count: usize, bytes: usize) -> Self {
        Self {
            count: packed_field(count).min(MAX_PACKED_COUNT),
            bytes: packed_field(bytes).min(MAX_PACKED_BYTES),
        }
    }
}

fn packed_field(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

/// The count and bytes reserved at one budget level, in one word, so that one atomic
/// read-modify-write reserves or releases both.
///
/// The count is in the high bits and the bytes in the low [`PACKED_BYTE_BITS`]. A reservation
/// never takes either field past its [`PackedLimit`], and a release returns only what a
/// reservation added, so subtracting a packed amount never borrows across the fields.
#[derive(Default)]
struct PackedUsage(AtomicU64);

impl PackedUsage {
    /// Adds `count` and `bytes` when both stay within `limit`; reports the count first.
    fn try_reserve(&self, count: u64, bytes: usize, limit: PackedLimit) -> Result<(), BudgetDimension> {
        let bytes = packed_field(bytes);
        let mut current = self.0.load(Ordering::Acquire);
        loop {
            let held_count = current >> PACKED_BYTE_BITS;
            let held_bytes = current & PACKED_BYTE_MASK;
            let next_count = held_count + count;
            if next_count > limit.count {
                return Err(BudgetDimension::Count);
            }
            let Some(next_bytes) = held_bytes.checked_add(bytes).filter(|total| *total <= limit.bytes) else {
                return Err(BudgetDimension::Bytes);
            };
            match self.0.compare_exchange_weak(
                current,
                (next_count << PACKED_BYTE_BITS) | next_bytes,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(()),
                Err(actual) => current = actual,
            }
        }
    }

    /// Returns `count` and `bytes` that an earlier reservation added.
    fn release(&self, count: u64, bytes: usize) {
        let bytes = packed_field(bytes);
        debug_assert!(
            bytes <= MAX_PACKED_BYTES,
            "a reservation never holds more than the packed maximum"
        );
        let previous = self.0.fetch_sub((count << PACKED_BYTE_BITS) | bytes, Ordering::AcqRel);
        debug_assert!(
            previous >> PACKED_BYTE_BITS >= count && previous & PACKED_BYTE_MASK >= bytes,
            "budget capacity released more than reserved"
        );
    }

    fn load(&self) -> (usize, usize) {
        let current = self.0.load(Ordering::Acquire);
        (
            usize::try_from(current >> PACKED_BYTE_BITS).unwrap_or(usize::MAX),
            usize::try_from(current & PACKED_BYTE_MASK).unwrap_or(usize::MAX),
        )
    }
}

impl BudgetNode {
    fn try_reserve_bytes(&self, bytes: usize, class: BudgetClass) -> bool {
        if !self.counted {
            return true;
        }
        if self.total.try_reserve(0, bytes, self.total_limit).is_err() {
            return false;
        }
        if class == BudgetClass::Data && self.data.try_reserve(0, bytes, self.data_limit).is_err() {
            self.total.release(0, bytes);
            return false;
        }
        true
    }

    fn release_bytes(&self, bytes: usize, class: BudgetClass) {
        if !self.counted {
            return;
        }
        if class == BudgetClass::Data {
            self.data.release(0, bytes);
        }
        self.total.release(0, bytes);
    }

    fn new(
        path: Arc<str>,
        limit: BudgetLimit,
        clock: Arc<dyn MonotonicClock>,
        admission_gates: Arc<[Arc<AdmissionGate>]>,
        capacity_notify: Arc<CapacitySignal>,
        counted: bool,
    ) -> Self {
        let rate = limit
            .capacity
            .rate
            .map(|_| Mutex::new(RateState::new(limit, clock.now())));
        let capacity = limit.capacity;
        let reserve = limit.control_reserve;
        Self {
            path,
            limit,
            clock,
            counted,
            total: PackedUsage::default(),
            data: PackedUsage::default(),
            total_limit: PackedLimit::new(capacity.count, capacity.bytes),
            data_limit: PackedLimit::new(
                capacity.count.saturating_sub(reserve.count),
                capacity.bytes.saturating_sub(reserve.bytes),
            ),
            counters: BudgetCounters::default(),
            rate,
            admission_gates,
            capacity_notify,
        }
    }

    fn lock_rate(rate: &Mutex<RateState>) -> MutexGuard<'_, RateState> {
        rate.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Reserves one count and `bytes` of `class`, and a rate token where the node has a rate.
    /// [`PendingReservations`] owns the reservation until it is committed to a permit.
    fn try_reserve(&self, bytes: usize, class: BudgetClass) -> Result<(), BudgetDimension> {
        if !self.counted {
            return Ok(());
        }
        self.total.try_reserve(1, bytes, self.total_limit)?;
        if class == BudgetClass::Data {
            if let Err(dimension) = self.data.try_reserve(1, bytes, self.data_limit) {
                self.total.release(1, bytes);
                return Err(dimension);
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

        Ok(())
    }

    /// Returns what a permit held at this node.
    fn release_held(&self, bytes: usize, class: BudgetClass) {
        self.release_capacity(bytes, class);
    }

    /// Returns reserved count and bytes; data counters too for a data reservation.
    fn release_capacity(&self, bytes: usize, class: BudgetClass) {
        if !self.counted {
            return;
        }
        if class == BudgetClass::Data {
            self.data.release(1, bytes);
        }
        self.total.release(1, bytes);
    }

    /// Moves a held data reservation to the control class: count and bytes stay held in total.
    fn release_data_class(&self, bytes: usize) {
        if self.counted {
            self.data.release(1, bytes);
        }
    }

    fn permanent_exhaustion_dimension(&self, bytes: usize, class: BudgetClass) -> Option<BudgetDimension> {
        let limit = match class {
            BudgetClass::Data => self.data_limit,
            BudgetClass::Control => self.total_limit,
        };
        if limit.count == 0 {
            Some(BudgetDimension::Count)
        } else if packed_field(bytes) > limit.bytes {
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
    ///
    /// The released count is the admitted count minus the count held now, so it is exact
    /// whenever no acquisition or release is in progress at this node.
    fn snapshot(&self) -> BudgetSnapshot {
        let (current_count, current_bytes) = self.total.load();
        let admitted_count = self.counters.admitted.load(Ordering::Acquire);
        BudgetSnapshot {
            path: Arc::clone(&self.path),
            current_count,
            current_bytes,
            admitted_count,
            released_count: admitted_count.saturating_sub(current_count as u64),
            rejected_count: self.counters.rejected.load(Ordering::Relaxed),
            throttled_count: self.counters.throttled.load(Ordering::Relaxed),
            dropped_count: self.counters.dropped.load(Ordering::Relaxed),
            coalesced_count: self.counters.coalesced.load(Ordering::Relaxed),
            closed_slow_consumer_count: self.counters.closed_slow_consumer.load(Ordering::Relaxed),
        }
    }
}

/// Reservations taken on a run of nodes for one acquisition or rebind, rolled back unless
/// they are committed to a permit.
struct PendingReservations<'a> {
    nodes: &'a [Arc<BudgetNode>],
    reserved: usize,
    bytes: usize,
    class: BudgetClass,
}

impl<'a> PendingReservations<'a> {
    fn new(nodes: &'a [Arc<BudgetNode>], bytes: usize, class: BudgetClass) -> Self {
        Self {
            nodes,
            reserved: 0,
            bytes,
            class,
        }
    }

    /// Reserves at every node in order, and reports the first node that has no capacity.
    fn reserve_all(&mut self) -> Result<(), (&'a Arc<BudgetNode>, BudgetDimension)> {
        let nodes = self.nodes;
        for node in nodes {
            node.try_reserve(self.bytes, self.class)
                .map_err(|dimension| (node, dimension))?;
            self.reserved += 1;
        }
        Ok(())
    }

    /// Counts the admissions; the caller's permit owns the reservations from now on.
    fn commit(mut self) {
        for node in &self.nodes[..self.reserved] {
            if node.counted {
                node.counters.admitted.fetch_add(1, Ordering::Relaxed);
            }
        }
        self.reserved = 0;
    }
}

impl Drop for PendingReservations<'_> {
    fn drop(&mut self) {
        for node in &self.nodes[..self.reserved] {
            node.release_capacity(self.bytes, self.class);
            if let Some(rate) = &node.rate {
                // A rolled-back reservation returns the rate token it consumed.
                BudgetNode::lock_rate(rate).restore(node.limit, self.class);
            }
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
