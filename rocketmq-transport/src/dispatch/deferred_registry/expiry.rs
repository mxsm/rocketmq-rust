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

use std::num::NonZeroUsize;
use std::ops::Bound;
use std::sync::Arc;

use super::internal::remove_entry;
use super::internal::CleanupCause;
use super::internal::DetachedBatch;
use super::internal::Entry;
use super::internal::EntryPhaseTag;
use super::internal::RegistryInner;
use super::internal::RegistryLifecycle;
use super::ClaimStart;
use super::ClaimedDeferred;
use super::DeferredClaimRejection;
use super::DeferredExpiry;
use super::DeferredId;
use super::DeferredWakeReason;
use super::RequestControlView;
use crate::dispatch::deferred_expiry::DeferredExpiryKind;

/// One bounded, caller-driven expiry sweep result.
///
/// Dropping the batch releases any timeout claims it still owns through the
/// ordinary affine claim path. The registry does not run a background scanner;
/// expiry advances only when a caller invokes [`super::DeferredRegistry::sweep_expired`].
#[must_use]
pub struct DeferredExpiryBatch<R>
where
    R: Send + 'static,
{
    pub(super) claims: Vec<ClaimedDeferred<R>>,
    pub(super) stats: DeferredExpiryBatchStats,
}

impl<R> DeferredExpiryBatch<R>
where
    R: Send + 'static,
{
    /// Returns low-cardinality counters for this bounded pass.
    #[must_use]
    pub const fn stats(&self) -> DeferredExpiryBatchStats {
        self.stats
    }

    /// Returns the timeout claims while consuming the batch.
    #[must_use]
    pub fn into_claims(self) -> Vec<ClaimedDeferred<R>> {
        self.claims
    }
}

/// Low-cardinality counters from one bounded expiry pass.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub struct DeferredExpiryBatchStats {
    pub(super) examined: usize,
    pub(super) long_poll_claims: usize,
    pub(super) pending_long_poll: usize,
    pub(super) owner_expired: usize,
    pub(super) invariant_failures: usize,
}

impl DeferredExpiryBatchStats {
    /// Returns the number of ordered index entries examined against the frozen time.
    #[must_use]
    pub const fn examined(self) -> usize {
        self.examined
    }

    /// Returns the number of affine timeout claims produced.
    #[must_use]
    pub const fn long_poll_claims(self) -> usize {
        self.long_poll_claims
    }

    /// Returns the number of provisional long-poll wakes persisted for later publication.
    #[must_use]
    pub const fn pending_long_poll(self) -> usize {
        self.pending_long_poll
    }

    /// Returns the number of entries detached because canonical owner budget won.
    #[must_use]
    pub const fn owner_expired(self) -> usize {
        self.owner_expired
    }

    /// Returns the number of impossible index or response transitions observed.
    #[must_use]
    pub const fn invariant_failures(self) -> usize {
        self.invariant_failures
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct ExpiryKey {
    pub(super) at: tokio::time::Instant,
    pub(super) kind: ExpiryKeyKind,
    pub(super) id: DeferredId,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) enum ExpiryKeyKind {
    OwnerDeadline,
    LongPollTimeout,
}

pub(super) struct EntryExpiry {
    pub(super) policy: Option<DeferredExpiry>,
    pub(super) scheduled: Option<ExpiryKey>,
    pub(super) timeout_pending: bool,
}

impl EntryExpiry {
    pub(super) fn new(id: DeferredId, control: &RequestControlView, policy: Option<DeferredExpiry>) -> Self {
        let scheduled = match policy {
            Some(policy) => Some(ExpiryKey {
                at: policy.next_at(),
                kind: match policy.kind() {
                    DeferredExpiryKind::LongPollTimeout => ExpiryKeyKind::LongPollTimeout,
                    DeferredExpiryKind::OwnerDeadline => ExpiryKeyKind::OwnerDeadline,
                },
                id,
            }),
            None => control.deadline().map(|deadline| ExpiryKey {
                at: deadline.instant(),
                kind: ExpiryKeyKind::OwnerDeadline,
                id,
            }),
        };
        Self {
            policy,
            scheduled,
            timeout_pending: false,
        }
    }

    pub(super) fn owner_cutoff(&self, control: &RequestControlView) -> Option<tokio::time::Instant> {
        self.policy
            .and_then(DeferredExpiry::resume_cutoff)
            .or_else(|| control.deadline().map(|deadline| deadline.instant()))
    }
}

impl<R> RegistryInner<R>
where
    R: Send + 'static,
{
    pub(super) fn sweep_expired(
        self: &Arc<Self>,
        now: tokio::time::Instant,
        limit: NonZeroUsize,
    ) -> DeferredExpiryBatch<R> {
        let mut detached = DetachedBatch::default();
        let mut due_claims = Vec::new();
        let mut claims = Vec::new();
        let mut stats = DeferredExpiryBatchStats::default();
        {
            let mut state = self.state.lock();
            if state.lifecycle != RegistryLifecycle::Open {
                return DeferredExpiryBatch { claims, stats };
            }
            let max = ExpiryKey {
                at: now,
                kind: ExpiryKeyKind::LongPollTimeout,
                id: DeferredId(u64::MAX),
            };
            let mut keys = Vec::new();
            let remaining = limit.get();
            match state.expiry_cursor {
                Some(cursor) => {
                    keys.extend(
                        state
                            .expiry_index
                            .range((Bound::Excluded(cursor), Bound::Included(max)))
                            .take(remaining)
                            .copied(),
                    );
                    let remaining = remaining - keys.len();
                    if remaining > 0 {
                        let wrap_end = cursor.min(max);
                        keys.extend(state.expiry_index.range(..=wrap_end).take(remaining).copied());
                    }
                }
                None => keys.extend(state.expiry_index.range(..=max).take(remaining).copied()),
            }
            if let Some(last) = keys.last().copied() {
                state.expiry_cursor = Some(last);
            }

            for key in keys {
                stats.examined += 1;
                if !state.expiry_index.remove(&key) {
                    stats.invariant_failures += 1;
                    continue;
                }
                let Some(entry) = state.primary.get_mut(&key.id) else {
                    stats.invariant_failures += 1;
                    continue;
                };
                if entry.expiry.scheduled != Some(key) {
                    stats.invariant_failures += 1;
                    continue;
                }
                entry.expiry.scheduled = None;
                if let Some(cause) = cleanup_cause_at(entry, key, now) {
                    let entry = remove_entry(&mut state, key.id)
                        .expect("the expiry entry was observed while the registry lock is held");
                    detached.push_entry(entry, cause);
                    if cause == CleanupCause::OwnerDeadline {
                        stats.owner_expired += 1;
                    }
                    continue;
                }

                entry.first_reason.get_or_insert(DeferredWakeReason::Timeout);
                entry.expiry.timeout_pending = true;
                if entry.phase.tag() == EntryPhaseTag::Active {
                    due_claims.push(key.id);
                } else {
                    let owner_key = entry.expiry.owner_cutoff(&entry.control).map(|at| ExpiryKey {
                        at,
                        kind: ExpiryKeyKind::OwnerDeadline,
                        id: key.id,
                    });
                    if let Some(owner_key) = owner_key {
                        entry.expiry.scheduled = Some(owner_key);
                        state.expiry_index.insert(owner_key);
                    }
                    stats.pending_long_poll += 1;
                }
            }
        }
        let _ = detached.finish();
        #[cfg(test)]
        let checkpoint = self.sweep_claim_checkpoint.lock().take();
        #[cfg(test)]
        if let Some(checkpoint) = checkpoint {
            checkpoint();
        }
        for id in due_claims {
            match self.start_claim(id, DeferredWakeReason::Timeout, None) {
                ClaimStart::Claimed(claimed) => {
                    claims.push(claimed);
                    stats.long_poll_claims += 1;
                }
                ClaimStart::Rejected(rejection) => {
                    if matches!(rejection, DeferredClaimRejection::Operational(_)) {
                        stats.invariant_failures += 1;
                    }
                }
                ClaimStart::Wait(_) => stats.invariant_failures += 1,
            }
        }
        DeferredExpiryBatch { claims, stats }
    }
}

fn cleanup_cause_at<R>(entry: &Entry<R>, key: ExpiryKey, now: tokio::time::Instant) -> Option<CleanupCause> {
    if entry.control.parent_is_cancelled() {
        Some(CleanupCause::ParentCancelled)
    } else if entry.control.session_is_closed() {
        Some(CleanupCause::SessionClosed)
    } else if entry
        .expiry
        .owner_cutoff(&entry.control)
        .is_some_and(|cutoff| now >= cutoff)
        || key.kind == ExpiryKeyKind::OwnerDeadline
    {
        Some(CleanupCause::OwnerDeadline)
    } else {
        None
    }
}
