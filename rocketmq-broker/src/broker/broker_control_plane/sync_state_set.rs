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

//! Master-side reporting of sync-state-set changes to the Controller.
//!
//! Mirrors Java `ReplicasManager#checkSyncStateSetAndDoReport` and the HA listener behind
//! `ReplicasManager#doReportSyncStateSetChanged`: the master proposes the membership its HA
//! service computed, the Controller leader commits it under a new sync-state-set epoch, and the
//! Broker installs the committed set in `ReplicasManager` and the Store.

use std::sync::atomic::Ordering;
use std::time::Duration;

use rocketmq_store::BrokerReplicationStore;
use tokio::sync::watch;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use super::BrokerControllerRuntime;

/// Delay before the first periodic check, like Java `ReplicasManager#schedulingCheckSyncStateSet`.
const FIRST_CHECK_DELAY: Duration = Duration::from_secs(3);

/// Pause after a failed report, so that replica acknowledgements cannot turn retries into a
/// request storm against the Controller.
const REPORT_RETRY_BACKOFF: Duration = Duration::from_secs(1);

/// Outcome of one sync-state-set check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SyncStateSetCheck {
    /// This broker is not the Controller-assigned master, or the Store or Controller view is not
    /// ready yet.
    Skipped,
    /// The HA service proposes the membership the Controller already committed.
    Unchanged,
    /// The Controller committed the proposal and the Broker installed it.
    Applied,
    /// The Controller committed the proposal, but a newer Controller view had already replaced
    /// the one the report was based on.
    Superseded,
}

impl<MS: BrokerReplicationStore> BrokerControllerRuntime<MS> {
    /// Reports the HA service's sync-state-set proposal to the Controller leader when it differs
    /// from the committed set, then installs the set the Controller committed.
    ///
    /// # Errors
    ///
    /// Returns the AlterSyncStateSet failure, for example a fenced epoch or an unreachable
    /// leader, and rejects an answer whose epoch or membership is invalid.
    pub(crate) async fn check_sync_state_set_and_report(&self) -> crate::broker_error::BrokerResult<SyncStateSetCheck> {
        if self.shutdown.load(Ordering::Acquire) {
            return Ok(SyncStateSetCheck::Skipped);
        }
        let Some(target) = self
            .replicas_snapshot()
            .and_then(|replicas_manager| replicas_manager.sync_state_set_report_target())
        else {
            return Ok(SyncStateSetCheck::Skipped);
        };
        let Ok(Some(mut proposal)) = self.store.sync_state_set_proposal() else {
            return Ok(SyncStateSetCheck::Skipped);
        };
        proposal.insert(target.master_broker_id);
        if proposal == target.sync_state_set {
            return Ok(SyncStateSetCheck::Unchanged);
        }

        let broker_name = self.config.broker_snapshot().broker_identity.broker_name.clone();
        let committed = self
            .broker_outer_api
            .alter_sync_state_set(
                &target.controller_leader_address,
                broker_name,
                target.master_broker_id,
                target.master_epoch,
                proposal,
                target.sync_state_set_epoch,
            )
            .await?;
        let sync_state_set = committed.get_sync_state_set().cloned().unwrap_or_default();
        let sync_state_set_epoch = committed.get_sync_state_set_epoch();

        // Install the answer under the operation gate, so that it cannot interleave with a
        // role change that updates `ReplicasManager` and the Store in the same order.
        let _operation_guard = self.controller.lock_operation().await;
        let applied = self
            .controller
            .with_replicas_mut(|replicas_manager| {
                replicas_manager.apply_reported_sync_state_set(
                    target.master_epoch,
                    sync_state_set.clone(),
                    sync_state_set_epoch,
                )
            })
            .ok_or_else(|| {
                crate::broker_error::invalid_argument(
                    "replicas manager missing while applying an alter sync state set result",
                )
            })??;
        if !applied {
            return Ok(SyncStateSetCheck::Superseded);
        }
        let _ = self
            .store
            .sync_controller_sync_state_set(target.master_broker_id, &sync_state_set);
        Ok(SyncStateSetCheck::Applied)
    }

    /// Reports sync-state-set changes until `cancellation` fires.
    ///
    /// A check runs whenever the HA service publishes a new proposal, which lets a caught-up
    /// replica join promptly, and every `checkSyncStateSetPeriod`, which drops replicas that
    /// stopped catching up and retries rejected proposals. Only the Controller-assigned master
    /// sends reports.
    pub(crate) async fn run_sync_state_set_reporter(&self, cancellation: CancellationToken) {
        let period = Duration::from_millis(self.config.broker_snapshot().check_sync_state_set_period.max(1));
        let mut ticker = tokio::time::interval_at(tokio::time::Instant::now() + FIRST_CHECK_DELAY, period);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut changes = self.store.subscribe_sync_state_set_changes();
        loop {
            tokio::select! {
                biased;
                () = cancellation.cancelled() => return,
                changed = next_sync_state_set_change(&mut changes) => {
                    if !changed {
                        // The HA service that published proposals is gone; resubscribe on a tick.
                        changes = None;
                        continue;
                    }
                }
                _ = ticker.tick() => {
                    if changes.is_none() {
                        changes = self.store.subscribe_sync_state_set_changes();
                    }
                }
            }

            let checked = tokio::select! {
                biased;
                () = cancellation.cancelled() => return,
                checked = self.check_sync_state_set_and_report() => checked,
            };
            if let Err(error) = checked {
                warn!(%error, "Failed to report a sync state set change to the controller");
                tokio::select! {
                    biased;
                    () = cancellation.cancelled() => return,
                    () = tokio::time::sleep(REPORT_RETRY_BACKOFF) => {}
                }
            }
        }
    }
}

/// Waits for the next published proposal; returns `false` once the publisher is gone.
async fn next_sync_state_set_change(changes: &mut Option<watch::Receiver<u64>>) -> bool {
    match changes {
        Some(changes) => changes.changed().await.is_ok(),
        None => std::future::pending().await,
    }
}
