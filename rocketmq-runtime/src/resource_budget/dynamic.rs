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

//! Retirement for dynamic budget keys.
//!
//! A dynamic key is a budget child whose name comes from runtime data, such as
//! a cluster ordering key. `ResourceBudget::child` creates the node but nothing
//! owns its lifetime afterwards, so the key becomes unreachable while permits
//! acquired from it may still be live, and a later child with the same name
//! silently creates a second generation beside the first.
//!
//! [`DynamicBudgetKey`] closes both gaps. The registry keeps the name reserved
//! until retirement completes, so a retired generation cannot be reused while
//! its work is still running, and retirement waits for the real reservations on
//! the key's budget instead of assuming that a closed handle means the work
//! stopped. Every release path in the budget tree already notifies the tree's
//! capacity notification, so the wait needs no extra primitive.

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;

use tokio::time::Instant;

use super::budget::BudgetRejection;
use super::budget::ResourceBudget;
use super::budget::ResourcePermit;
use super::limit::BudgetClass;
use crate::RuntimeContractViolation;

/// The default bound on live dynamic keys per budget tree.
///
/// It matches the metadata target registry default so that a process has one
/// recognizable order of magnitude for its bounded registrations.
pub const DEFAULT_MAX_DYNAMIC_BUDGET_KEYS: usize = 4_096;

/// Why a dynamic key could not be registered.
#[derive(Debug)]
pub enum DynamicKeyRegistrationFailure {
    /// The name or limit violated the budget contract.
    Invalid(RuntimeContractViolation),
    /// The name is already held by a live or retiring key.
    ///
    /// Reusing it would let the next generation of work run beside the
    /// generation it replaced, so retirement has to complete first.
    NameInUse {
        /// The generation that currently holds the name.
        generation: u64,
    },
    /// The tree already holds the maximum number of dynamic keys.
    CapacityExhausted {
        /// The configured bound.
        max_entries: usize,
    },
}

/// Why a dynamic key refused an admission.
#[derive(Debug)]
pub enum DynamicKeyAdmissionRejection {
    /// Admission is closed because the key was retired.
    Closed,
    /// The key's budget rejected the admission.
    Budget(BudgetRejection),
}

/// The outcome of retiring a dynamic key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DynamicKeyRetirement {
    /// Whether the registration was released and the name is free again.
    ///
    /// A released key has no outstanding work. When the deadline elapses
    /// first, the registration stays held so the name cannot be reused while
    /// reservations from this generation are still live.
    pub released: bool,
    /// Reservations still held on the key's budget when the caller stopped
    /// waiting.
    pub outstanding_reservations: usize,
    /// Bytes still reserved on the key's budget when the caller stopped
    /// waiting.
    pub outstanding_bytes: usize,
}

/// One registered dynamic key under a budget tree.
///
/// Cloning shares the same registration and generation. Every clone observes
/// the same closed state, and only the last generation that holds a name can
/// release it.
#[derive(Clone)]
pub struct DynamicBudgetKey {
    inner: Arc<DynamicKeyInner>,
}

struct DynamicKeyInner {
    registry: DynamicKeyRegistry,
    budget: ResourceBudget,
    name: Arc<str>,
    generation: u64,
    closed: AtomicBool,
}

impl std::fmt::Debug for DynamicBudgetKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DynamicBudgetKey")
            .field("name", &self.inner.name)
            .field("generation", &self.inner.generation)
            .field("closed", &self.is_closed())
            .finish_non_exhaustive()
    }
}

impl DynamicBudgetKey {
    /// Returns the name this key holds.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    /// Returns the unique generation of this registration.
    #[must_use]
    pub fn generation(&self) -> u64 {
        self.inner.generation
    }

    /// Returns the budget this key owns.
    ///
    /// The key is the admission boundary for a dynamic key: work that has to be
    /// waited for by [`Self::retire_until`] must be admitted through
    /// [`Self::try_acquire`] or against this budget, because retirement waits
    /// for the reservations on this node.
    #[must_use]
    pub fn budget(&self) -> ResourceBudget {
        self.inner.budget.clone()
    }

    /// Returns whether admission is closed for this key.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.inner.closed.load(Ordering::Acquire)
    }

    /// Closes admission for this key without waiting for its work.
    pub fn close(&self) {
        self.inner.closed.store(true, Ordering::Release);
    }

    /// Acquires capacity from this key.
    ///
    /// # Errors
    ///
    /// Returns [`DynamicKeyAdmissionRejection::Closed`] once the key is
    /// retiring, or the budget rejection when the key's budget is full.
    pub fn try_acquire(
        &self,
        bytes: usize,
        class: BudgetClass,
    ) -> Result<ResourcePermit, DynamicKeyAdmissionRejection> {
        if self.is_closed() {
            return Err(DynamicKeyAdmissionRejection::Closed);
        }
        self.inner
            .budget
            .try_acquire(bytes, class)
            .map_err(DynamicKeyAdmissionRejection::Budget)
    }

    /// Closes admission and waits for the reservations on this key to drain.
    ///
    /// Retirement completes only when the node holds no reservations, which is
    /// what makes it safe for the caller to stop accounting for this key. When
    /// `deadline` elapses first, the registration stays held and the returned
    /// [`DynamicKeyRetirement`] reports the real remaining work.
    pub async fn retire_until(&self, deadline: Instant) -> DynamicKeyRetirement {
        self.close();
        loop {
            // The registration is released on this branch only, so a name can
            // never be freed while reservations from this generation are live.
            let snapshot = self.inner.budget.snapshot();
            if snapshot.current_count == 0 {
                let released = self.inner.registry.release(&self.inner.name, self.inner.generation);
                return DynamicKeyRetirement {
                    released,
                    outstanding_reservations: 0,
                    outstanding_bytes: 0,
                };
            }
            if Instant::now() >= deadline {
                return DynamicKeyRetirement {
                    released: false,
                    outstanding_reservations: snapshot.current_count,
                    outstanding_bytes: snapshot.current_bytes,
                };
            }

            let released = self.inner.budget.capacity_notify().notified();
            tokio::pin!(released);
            released.as_mut().enable();
            // Re-check after registering the waiter, so a release that happened
            // in between is not lost.
            let snapshot = self.inner.budget.snapshot();
            if snapshot.current_count == 0 {
                continue;
            }

            let expired = tokio::time::sleep_until(deadline);
            tokio::pin!(expired);
            tokio::select! {
                biased;
                () = &mut expired => {}
                () = &mut released => {}
            }
        }
    }
}

/// The bounded set of dynamic key names held by one budget tree.
#[derive(Clone)]
pub(super) struct DynamicKeyRegistry {
    inner: Arc<DynamicKeyRegistryInner>,
}

struct DynamicKeyRegistryInner {
    entries: Mutex<HashMap<Arc<str>, u64>>,
    next_generation: AtomicU64,
    max_entries: usize,
}

impl std::fmt::Debug for DynamicKeyRegistry {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let entries = self
            .inner
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len();
        formatter
            .debug_struct("DynamicKeyRegistry")
            .field("entries", &entries)
            .field("max_entries", &self.inner.max_entries)
            .finish()
    }
}

impl DynamicKeyRegistry {
    pub(super) fn new() -> Self {
        Self::with_max_entries(DEFAULT_MAX_DYNAMIC_BUDGET_KEYS)
    }

    pub(super) fn with_max_entries(max_entries: usize) -> Self {
        Self {
            inner: Arc::new(DynamicKeyRegistryInner {
                entries: Mutex::new(HashMap::new()),
                next_generation: AtomicU64::new(1),
                max_entries,
            }),
        }
    }

    pub(super) fn register(
        &self,
        budget: ResourceBudget,
        name: Arc<str>,
    ) -> Result<DynamicBudgetKey, DynamicKeyRegistrationFailure> {
        let mut entries = self
            .inner
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(generation) = entries.get(&name) {
            return Err(DynamicKeyRegistrationFailure::NameInUse {
                generation: *generation,
            });
        }
        if entries.len() >= self.inner.max_entries {
            return Err(DynamicKeyRegistrationFailure::CapacityExhausted {
                max_entries: self.inner.max_entries,
            });
        }
        let generation = self.inner.next_generation.fetch_add(1, Ordering::Relaxed);
        entries.insert(Arc::clone(&name), generation);
        drop(entries);

        Ok(DynamicBudgetKey {
            inner: Arc::new(DynamicKeyInner {
                registry: self.clone(),
                budget,
                name,
                generation,
                closed: AtomicBool::new(false),
            }),
        })
    }

    fn release(&self, name: &Arc<str>, generation: u64) -> bool {
        let mut entries = self
            .inner
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // A stale handle from a previous generation must not release the name
        // that a newer registration now owns.
        if entries.get(name) != Some(&generation) {
            return false;
        }
        entries.remove(name);
        true
    }
}
