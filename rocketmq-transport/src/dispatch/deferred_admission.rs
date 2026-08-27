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

//! Retained-byte admission for deferred V2 waits.

use std::alloc::Layout;
use std::error::Error;
use std::fmt;
use std::mem::size_of;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use rocketmq_runtime::BudgetAcquireError;
use rocketmq_runtime::BudgetClass;
use rocketmq_runtime::BudgetConfigError;
use rocketmq_runtime::BudgetDimension;
use rocketmq_runtime::BudgetLimit;
use rocketmq_runtime::FullPolicy;
use rocketmq_runtime::ResourceBudget;
use rocketmq_runtime::ResourcePermit;

use super::DeferredResponder;
use super::ResponseState;
use crate::admission::AdmissionController;

const DEFERRED_BUDGET_NAME: &str = "transport-deferred-wait";

/// Fixed count and retained-byte limits for deferred waits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeferredWaitLimits {
    max_waiters: usize,
    max_retained_bytes: usize,
}

impl DeferredWaitLimits {
    /// Creates explicit deferred-wait limits.
    #[must_use]
    pub const fn new(max_waiters: usize, max_retained_bytes: usize) -> Self {
        Self {
            max_waiters,
            max_retained_bytes,
        }
    }

    /// Returns the maximum simultaneously retained waits.
    #[must_use]
    pub const fn max_waiters(self) -> usize {
        self.max_waiters
    }

    /// Returns the maximum bytes retained by all waits.
    #[must_use]
    pub const fn max_retained_bytes(self) -> usize {
        self.max_retained_bytes
    }
}

/// Caller-declared variable ownership retained by one deferred wait.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeferredRetainedSizeParts {
    resume_bytes: usize,
    filter_bytes: usize,
    secondary_index_bytes: usize,
    metadata_bytes: usize,
}

impl DeferredRetainedSizeParts {
    /// Starts a retained-size declaration with the owned resume payload.
    #[must_use]
    pub const fn new(resume_bytes: usize) -> Self {
        Self {
            resume_bytes,
            filter_bytes: 0,
            secondary_index_bytes: 0,
            metadata_bytes: 0,
        }
    }

    /// Declares retained filter or subscription-builder storage.
    #[must_use]
    pub const fn with_filter_bytes(mut self, filter_bytes: usize) -> Self {
        self.filter_bytes = filter_bytes;
        self
    }

    /// Declares the retained secondary-index lease estimate.
    #[must_use]
    pub const fn with_secondary_index_bytes(mut self, secondary_index_bytes: usize) -> Self {
        self.secondary_index_bytes = secondary_index_bytes;
        self
    }

    /// Declares other retained wait metadata.
    #[must_use]
    pub const fn with_metadata_bytes(mut self, metadata_bytes: usize) -> Self {
        self.metadata_bytes = metadata_bytes;
        self
    }

    /// Returns the owned resume payload estimate.
    #[must_use]
    pub const fn resume_bytes(self) -> usize {
        self.resume_bytes
    }

    /// Returns the filter or subscription-builder estimate.
    #[must_use]
    pub const fn filter_bytes(self) -> usize {
        self.filter_bytes
    }

    /// Returns the secondary-index lease estimate.
    #[must_use]
    pub const fn secondary_index_bytes(self) -> usize {
        self.secondary_index_bytes
    }

    /// Returns the other retained metadata estimate.
    #[must_use]
    pub const fn metadata_bytes(self) -> usize {
        self.metadata_bytes
    }
}

/// Checked total retained by one admitted deferred wait.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeferredRetainedSize(usize);

impl DeferredRetainedSize {
    /// Computes the exact fixed capability/state/permit charge and adds every
    /// caller-declared retained ownership class with checked arithmetic.
    ///
    /// The logical `Arc<ResponseState>` allocation includes two strong/weak
    /// reference counters, the aligned state payload, and final allocation
    /// padding. Allocator-private metadata is deliberately excluded.
    ///
    /// # Errors
    ///
    /// Returns [`DeferredAdmissionAcquireErrorKind::RetainedSizeOverflow`] if
    /// any layout or size addition exceeds `usize`.
    pub fn try_from_parts(parts: DeferredRetainedSizeParts) -> Result<Self, DeferredAdmissionAcquireError> {
        let bytes = fixed_retained_bytes()
            .and_then(|bytes| bytes.checked_add(parts.resume_bytes))
            .and_then(|bytes| bytes.checked_add(parts.filter_bytes))
            .and_then(|bytes| bytes.checked_add(parts.secondary_index_bytes))
            .and_then(|bytes| bytes.checked_add(parts.metadata_bytes))
            .ok_or_else(DeferredAdmissionAcquireError::retained_size_overflow)?;
        Ok(Self(bytes))
    }

    /// Returns the checked total retained bytes.
    #[must_use]
    pub const fn bytes(self) -> usize {
        self.0
    }
}

/// Stable category for deferred admission configuration failures.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum DeferredAdmissionConfigErrorKind {
    /// The waiter count limit is zero.
    ZeroWaiterCapacity,
    /// The retained-byte limit is zero.
    ZeroRetainedByteCapacity,
    /// A deferred child limit exceeds the injected process root.
    ExceedsProcessCapacity,
    /// The controller was already configured with different limits.
    Conflict,
    /// The established budget implementation rejected another invariant.
    Budget,
}

impl DeferredAdmissionConfigErrorKind {
    /// Returns a stable low-cardinality label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ZeroWaiterCapacity => "zero_waiter_capacity",
            Self::ZeroRetainedByteCapacity => "zero_retained_byte_capacity",
            Self::ExceedsProcessCapacity => "exceeds_process_capacity",
            Self::Conflict => "conflict",
            Self::Budget => "budget",
        }
    }
}

enum DeferredAdmissionConfigErrorSource {
    Conflict,
    Budget(BudgetConfigError),
}

/// Typed, redacted deferred admission configuration failure.
pub struct DeferredAdmissionConfigError {
    kind: DeferredAdmissionConfigErrorKind,
    source: DeferredAdmissionConfigErrorSource,
}

impl DeferredAdmissionConfigError {
    fn from_budget(kind: DeferredAdmissionConfigErrorKind, source: BudgetConfigError) -> Self {
        Self {
            kind,
            source: DeferredAdmissionConfigErrorSource::Budget(source),
        }
    }

    pub(crate) const fn conflict() -> Self {
        Self {
            kind: DeferredAdmissionConfigErrorKind::Conflict,
            source: DeferredAdmissionConfigErrorSource::Conflict,
        }
    }

    /// Returns the stable low-cardinality failure category.
    #[must_use]
    pub const fn kind(&self) -> DeferredAdmissionConfigErrorKind {
        self.kind
    }
}

impl fmt::Debug for DeferredAdmissionConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeferredAdmissionConfigError")
            .field("kind", &self.kind.as_str())
            .finish()
    }
}

impl fmt::Display for DeferredAdmissionConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "deferred admission configuration failed: {}",
            self.kind.as_str()
        )
    }
}

impl Error for DeferredAdmissionConfigError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.source {
            DeferredAdmissionConfigErrorSource::Conflict => None,
            DeferredAdmissionConfigErrorSource::Budget(source) => Some(source),
        }
    }
}

/// Stable category for a failed deferred wait reservation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum DeferredAdmissionAcquireErrorKind {
    /// Checked retained-size arithmetic overflowed.
    RetainedSizeOverflow,
    /// This deferred owner exhausted its waiter count.
    WaiterCapacityExhausted,
    /// This deferred owner exhausted its retained bytes.
    RetainedByteCapacityExhausted,
    /// Another child exhausted the shared process/service root.
    ParentCapacityExhausted,
}

impl DeferredAdmissionAcquireErrorKind {
    /// Returns a stable low-cardinality label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RetainedSizeOverflow => "retained_size_overflow",
            Self::WaiterCapacityExhausted => "waiter_capacity_exhausted",
            Self::RetainedByteCapacityExhausted => "retained_byte_capacity_exhausted",
            Self::ParentCapacityExhausted => "parent_capacity_exhausted",
        }
    }
}

enum DeferredAdmissionAcquireErrorSource {
    RetainedSizeOverflow,
    Budget(BudgetAcquireError),
}

/// Typed, redacted failure to reserve one deferred wait.
pub struct DeferredAdmissionAcquireError {
    kind: DeferredAdmissionAcquireErrorKind,
    source: DeferredAdmissionAcquireErrorSource,
}

impl DeferredAdmissionAcquireError {
    const fn retained_size_overflow() -> Self {
        Self {
            kind: DeferredAdmissionAcquireErrorKind::RetainedSizeOverflow,
            source: DeferredAdmissionAcquireErrorSource::RetainedSizeOverflow,
        }
    }

    fn from_budget(kind: DeferredAdmissionAcquireErrorKind, source: BudgetAcquireError) -> Self {
        Self {
            kind,
            source: DeferredAdmissionAcquireErrorSource::Budget(source),
        }
    }

    /// Returns the stable low-cardinality failure category.
    #[must_use]
    pub const fn kind(&self) -> DeferredAdmissionAcquireErrorKind {
        self.kind
    }
}

impl fmt::Debug for DeferredAdmissionAcquireError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeferredAdmissionAcquireError")
            .field("kind", &self.kind.as_str())
            .finish()
    }
}

impl fmt::Display for DeferredAdmissionAcquireError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "deferred wait admission failed: {}", self.kind.as_str())
    }
}

impl Error for DeferredAdmissionAcquireError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.source {
            DeferredAdmissionAcquireErrorSource::RetainedSizeOverflow => None,
            DeferredAdmissionAcquireErrorSource::Budget(source) => Some(source),
        }
    }
}

/// Independent low-cardinality view of retained deferred waits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeferredAdmissionSnapshot {
    waiting_count: usize,
    retained_bytes: usize,
    rejected_count: u64,
}

impl DeferredAdmissionSnapshot {
    /// Returns the number of currently retained waits.
    #[must_use]
    pub const fn waiting_count(self) -> usize {
        self.waiting_count
    }

    /// Returns the bytes currently retained by waits.
    #[must_use]
    pub const fn retained_bytes(self) -> usize {
        self.retained_bytes
    }

    /// Returns failed deferred reservations observed at this owner.
    #[must_use]
    pub const fn rejected_count(self) -> u64 {
        self.rejected_count
    }
}

struct DeferredAdmissionInner {
    budget: ResourceBudget,
    limits: DeferredWaitLimits,
}

/// Cloneable composition owner for independent deferred-wait admission.
#[derive(Clone)]
pub struct DeferredAdmission {
    inner: Arc<DeferredAdmissionInner>,
}

impl DeferredAdmission {
    /// Atomically configures or retrieves the deferred owner attached to one
    /// transport admission controller.
    ///
    /// Concurrent equal configurations return clones of one owner. A different
    /// configuration is rejected without replacing the established owner.
    ///
    /// # Errors
    ///
    /// Returns a typed configuration error for zero/excess capacity, a
    /// different prior configuration, or an established budget invariant.
    pub fn try_configure(
        controller: &AdmissionController,
        limits: DeferredWaitLimits,
    ) -> Result<Self, DeferredAdmissionConfigError> {
        controller.configure_deferred_admission(limits)
    }

    pub(crate) fn try_new(
        process_budget: &ResourceBudget,
        limits: DeferredWaitLimits,
    ) -> Result<Self, DeferredAdmissionConfigError> {
        let path = format!("{}/{DEFERRED_BUDGET_NAME}", process_budget.path());
        validate_limits(limits, process_budget.limit(), &path)?;
        let budget = process_budget
            .child(
                DEFERRED_BUDGET_NAME,
                BudgetLimit::new(limits.max_waiters, limits.max_retained_bytes, FullPolicy::Reject),
            )
            .map_err(|source| DeferredAdmissionConfigError::from_budget(config_kind(&source), source))?;
        Ok(Self {
            inner: Arc::new(DeferredAdmissionInner { budget, limits }),
        })
    }

    /// Returns the immutable limits shared by this owner.
    #[must_use]
    pub fn limits(&self) -> DeferredWaitLimits {
        self.inner.limits
    }

    /// Returns the current deferred wait count/byte observation.
    #[must_use]
    pub fn snapshot(&self) -> DeferredAdmissionSnapshot {
        let snapshot = self.inner.budget.snapshot();
        DeferredAdmissionSnapshot {
            waiting_count: snapshot.current_count,
            retained_bytes: snapshot.current_bytes,
            rejected_count: snapshot.rejected_count,
        }
    }

    /// Attempts to reserve one already-checked retained size without waiting.
    ///
    /// # Errors
    ///
    /// Returns the exact local count, local retained-byte, or shared-parent
    /// exhaustion category. No registry or asynchronous wait is created.
    pub fn try_reserve(
        &self,
        retained: DeferredRetainedSize,
    ) -> Result<DeferredWaitPermit, DeferredAdmissionAcquireError> {
        let permit = self
            .inner
            .budget
            .try_acquire(retained.bytes(), BudgetClass::Data)
            .map_err(|source| {
                let kind = if source.exhausted_path() == self.inner.budget.path() {
                    match source.dimension() {
                        BudgetDimension::Count => DeferredAdmissionAcquireErrorKind::WaiterCapacityExhausted,
                        BudgetDimension::Bytes => DeferredAdmissionAcquireErrorKind::RetainedByteCapacityExhausted,
                        BudgetDimension::Rate => DeferredAdmissionAcquireErrorKind::ParentCapacityExhausted,
                    }
                } else {
                    DeferredAdmissionAcquireErrorKind::ParentCapacityExhausted
                };
                DeferredAdmissionAcquireError::from_budget(kind, source)
            })?;
        Ok(DeferredWaitPermit {
            permit: Some(permit),
            retained_bytes: retained.bytes(),
        })
    }

    #[cfg(test)]
    pub(crate) fn same_owner(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
}

impl fmt::Debug for DeferredAdmission {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeferredAdmission")
            .field("limits", &self.inner.limits)
            .finish_non_exhaustive()
    }
}

/// Affine ownership of one deferred waiter count/byte reservation.
///
/// The permit owns only the established resource-budget permit. It contains no
/// processor execution permit and creates no task or wait queue.
///
/// ```compile_fail
/// use rocketmq_transport::api::v2::DeferredWaitPermit;
///
/// fn permits_are_affine(permit: &DeferredWaitPermit) {
///     let _: DeferredWaitPermit = permit.clone();
/// }
/// ```
#[must_use]
pub struct DeferredWaitPermit {
    permit: Option<ResourcePermit>,
    retained_bytes: usize,
}

impl DeferredWaitPermit {
    /// Returns the exact bytes charged by this reservation.
    #[must_use]
    pub const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    /// Consumes this permit and releases its count/byte ownership immediately.
    pub fn release(mut self) {
        drop(self.permit.take());
    }
}

impl fmt::Debug for DeferredWaitPermit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeferredWaitPermit")
            .field("retained_bytes", &self.retained_bytes)
            .finish_non_exhaustive()
    }
}

fn validate_limits(
    limits: DeferredWaitLimits,
    parent: BudgetLimit,
    path: &str,
) -> Result<(), DeferredAdmissionConfigError> {
    let failure = if limits.max_waiters == 0 {
        Some((
            DeferredAdmissionConfigErrorKind::ZeroWaiterCapacity,
            BudgetConfigError::ZeroCapacity {
                path: path.to_owned(),
                dimension: BudgetDimension::Count,
            },
        ))
    } else if limits.max_retained_bytes == 0 {
        Some((
            DeferredAdmissionConfigErrorKind::ZeroRetainedByteCapacity,
            BudgetConfigError::ZeroCapacity {
                path: path.to_owned(),
                dimension: BudgetDimension::Bytes,
            },
        ))
    } else if limits.max_waiters > parent.capacity.count {
        Some((
            DeferredAdmissionConfigErrorKind::ExceedsProcessCapacity,
            BudgetConfigError::ChildExceedsParent {
                path: path.to_owned(),
                dimension: BudgetDimension::Count,
            },
        ))
    } else if limits.max_retained_bytes > parent.capacity.bytes {
        Some((
            DeferredAdmissionConfigErrorKind::ExceedsProcessCapacity,
            BudgetConfigError::ChildExceedsParent {
                path: path.to_owned(),
                dimension: BudgetDimension::Bytes,
            },
        ))
    } else {
        None
    };
    match failure {
        Some((kind, source)) => Err(DeferredAdmissionConfigError::from_budget(kind, source)),
        None => Ok(()),
    }
}

fn config_kind(source: &BudgetConfigError) -> DeferredAdmissionConfigErrorKind {
    match source {
        BudgetConfigError::ZeroCapacity {
            dimension: BudgetDimension::Count,
            ..
        } => DeferredAdmissionConfigErrorKind::ZeroWaiterCapacity,
        BudgetConfigError::ZeroCapacity {
            dimension: BudgetDimension::Bytes,
            ..
        } => DeferredAdmissionConfigErrorKind::ZeroRetainedByteCapacity,
        BudgetConfigError::ChildExceedsParent { .. } => DeferredAdmissionConfigErrorKind::ExceedsProcessCapacity,
        BudgetConfigError::EmptyName
        | BudgetConfigError::InvalidName
        | BudgetConfigError::ZeroCapacity {
            dimension: BudgetDimension::Rate,
            ..
        }
        | BudgetConfigError::ZeroRate { .. }
        | BudgetConfigError::ZeroMaxAge { .. }
        | BudgetConfigError::ReserveExceedsCapacity { .. }
        | BudgetConfigError::ReserveWithoutCapacity { .. }
        | BudgetConfigError::ChildMaxAgeExceedsParent { .. } => DeferredAdmissionConfigErrorKind::Budget,
    }
}

fn fixed_retained_bytes() -> Option<usize> {
    let header = Layout::array::<AtomicUsize>(2).ok()?;
    let (state_allocation, _) = header.extend(Layout::new::<ResponseState>()).ok()?;
    size_of::<DeferredResponder>()
        .checked_add(state_allocation.pad_to_align().size())?
        .checked_add(size_of::<DeferredWaitPermit>())
}

#[cfg(test)]
fn response_state_allocation_bytes() -> usize {
    let header = Layout::array::<AtomicUsize>(2).expect("two Arc counters have a valid layout");
    let (state_allocation, _) = header
        .extend(Layout::new::<ResponseState>())
        .expect("ResponseState follows the Arc header");
    state_allocation.pad_to_align().size()
}

#[cfg(test)]
#[path = "deferred_admission/tests.rs"]
mod tests;
