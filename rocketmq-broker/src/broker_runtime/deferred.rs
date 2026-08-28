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

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use rocketmq_runtime::ResourceBudget;
use rocketmq_runtime::TaskGroup;
use rocketmq_transport::api::v1::AdmissionController;
use rocketmq_transport::api::v2::DeferredAdmission;
use rocketmq_transport::api::v2::DeferredExpiryMargins;
use rocketmq_transport::api::v2::DeferredRegistryShutdownOutcome;
use rocketmq_transport::api::v2::DeferredWaitLimits;

use super::*;
use crate::broker_runtime::deferred_producer::BrokerDeferredProducer;
use crate::deferred_generation_handoff::DeferredGenerationHandoff;
use crate::long_polling::notification_deferred::index::NotificationCriteriaLimits;
use crate::long_polling::notification_deferred::service::NotificationDeferredService;
use crate::long_polling::pop_deferred::index::PopCriteriaLimits;
use crate::long_polling::pop_deferred::service::PopDeferredService;
use crate::long_polling::pop_lite_deferred::data::PopLiteDeferredPolicy;
use crate::long_polling::pop_lite_deferred::service::PopLiteDeferredService;
use crate::long_polling::pull_deferred::PullCriteriaLimits;
use crate::long_polling::pull_deferred::PullDeferredService;

const DEFERRED_RECOVERY_MARGIN: Duration = Duration::from_millis(2);
const DEFERRED_WRITE_MARGIN: Duration = Duration::from_millis(2);

pub(super) struct BrokerDeferredLifecycle {
    pub(super) admission_controller: Arc<AdmissionController>,
    pub(super) admission: DeferredAdmission,
    pub(super) handoff: Arc<DeferredGenerationHandoff>,
    pub(super) pop: Arc<PopDeferredService>,
    pub(super) pull: Arc<PullDeferredService>,
    pub(super) notification: Arc<NotificationDeferredService>,
    pub(super) pop_lite: Arc<PopLiteDeferredService>,
    pub(super) producer: Option<Arc<BrokerDeferredProducer<BrokerMessageStore>>>,
    producer_task_group: Option<TaskGroup>,
}

impl BrokerDeferredLifecycle {
    fn try_new(
        config: &BrokerConfig,
        admission_budget: &ResourceBudget,
        retained_bytes: usize,
        lite_event_dispatcher: LiteEventDispatcher,
    ) -> Result<Self, BrokerStartupError> {
        let admission_controller = Arc::new(
            AdmissionController::try_new_with_budget(Default::default(), admission_budget).map_err(|error| {
                BrokerStartupError::Initialization {
                    component: "authorized_dispatcher",
                    detail: format!("failed to create shared Broker admission boundary: {error}"),
                }
            })?,
        );
        let max_entries = usize::try_from(config.max_pop_polling_size).unwrap_or(usize::MAX);
        let max_entries = NonZeroUsize::new(max_entries).ok_or_else(|| BrokerStartupError::Initialization {
            component: "deferred_services",
            detail: "maxPopPollingSize must be greater than zero".to_owned(),
        })?;
        let legacy_per_key = config.pop_polling_size.saturating_add(1);
        let per_key = NonZeroUsize::new(legacy_per_key).ok_or_else(|| BrokerStartupError::Initialization {
            component: "deferred_services",
            detail: "popPollingSize must be greater than zero".to_owned(),
        })?;
        let continuation_count =
            NonZeroUsize::new(config.pop_polling_map_size).ok_or_else(|| BrokerStartupError::Initialization {
                component: "deferred_services",
                detail: "popPollingMapSize must be greater than zero".to_owned(),
            })?;
        let admission = DeferredAdmission::try_configure(
            admission_controller.as_ref(),
            DeferredWaitLimits::new(max_entries.get(), retained_bytes),
        )
        .map_err(|error| BrokerStartupError::Initialization {
            component: "deferred_admission",
            detail: error.to_string(),
        })?;
        let expiry_margins = DeferredExpiryMargins::new(DEFERRED_RECOVERY_MARGIN, DEFERRED_WRITE_MARGIN);
        let pop_lite_policy =
            PopLiteDeferredPolicy::from_config(config).ok_or_else(|| BrokerStartupError::Initialization {
                component: "pop_lite_deferred",
                detail: "PopLite deferred limits must be greater than zero".to_owned(),
            })?;

        let pop = Arc::new(PopDeferredService::new(
            admission.clone(),
            PopCriteriaLimits::new(max_entries, per_key),
            expiry_margins,
            per_key,
        ));
        let pull = Arc::new(PullDeferredService::new(
            admission.clone(),
            PullCriteriaLimits::new(max_entries, per_key),
            expiry_margins,
            per_key,
            per_key,
        ));
        let notification = Arc::new(NotificationDeferredService::new(
            admission.clone(),
            NotificationCriteriaLimits::new(max_entries, config.pop_polling_size, config.pop_polling_map_size),
            expiry_margins,
            per_key,
            per_key,
            continuation_count,
            NonZeroUsize::new(retained_bytes).unwrap_or(NonZeroUsize::MIN),
        ));
        let pop_lite = Arc::new(PopLiteDeferredService::new(
            admission.clone(),
            pop_lite_policy.index_limits,
            lite_event_dispatcher,
            expiry_margins,
            pop_lite_policy.max_age,
            per_key,
        ));

        Ok(Self {
            admission_controller,
            admission,
            handoff: Arc::new(DeferredGenerationHandoff::new()),
            pop,
            pull,
            notification,
            pop_lite,
            producer: None,
            producer_task_group: None,
        })
    }

    pub(super) fn install_producer(
        &mut self,
        producer: Arc<BrokerDeferredProducer<BrokerMessageStore>>,
        task_group: TaskGroup,
    ) -> Result<(), BrokerStartupError> {
        if self.producer.is_some() || self.producer_task_group.is_some() {
            return Err(BrokerStartupError::Initialization {
                component: "deferred_producers",
                detail: "Broker deferred producers were already installed".to_owned(),
            });
        }
        self.producer = Some(producer);
        self.producer_task_group = Some(task_group);
        Ok(())
    }

    pub(super) fn producer_task_group(&self) -> Option<TaskGroup> {
        self.producer_task_group.clone()
    }

    pub(super) fn producer_is_installed(&self) -> bool {
        self.producer.is_some() && self.producer_task_group.is_some()
    }

    pub(super) fn unbind_producer_store(&self) -> bool {
        self.producer
            .as_ref()
            .is_some_and(|producer| producer.unbind_message_store())
    }

    pub(super) fn seal(&self) {
        let _ = self.handoff.seal();
        self.pop.seal();
        self.pull.seal();
        self.notification.seal();
        self.pop_lite.seal();
    }

    pub(super) fn shutdown(&self) -> BrokerDeferredRegistryShutdownOutcomes {
        BrokerDeferredRegistryShutdownOutcomes {
            pop: self.pop.shutdown(),
            pull: self.pull.shutdown(),
            notification: self.notification.shutdown(),
            pop_lite: self.pop_lite.shutdown(),
        }
    }

    pub(super) fn resource_snapshot(&self) -> BrokerDeferredResourceSnapshot {
        let admission = self.admission.snapshot();
        let pop = self.pop.resource_snapshot();
        let pull = self.pull.resource_snapshot();
        let notification = self.notification.snapshot();
        let pop_lite = self.pop_lite.resource_snapshot();
        let handoff = self.handoff.snapshot();
        let handoff_zero = handoff.is_zero();
        let shared_admission = self.admission_controller.snapshot();
        let producer_task_count = self.producer_task_group.as_ref().map_or(0, TaskGroup::task_count);
        BrokerDeferredResourceSnapshot {
            waiting_count: admission.waiting_count(),
            retained_bytes: admission.retained_bytes(),
            pop_live: pop.index.live(),
            pop_reserved: pop.index.reserved(),
            pop_buckets: pop.index.buckets(),
            pop_candidates: pop.index.candidates(),
            pop_resume_executions: pop.resume_executions,
            pop_resume_execution_bytes: pop.resume_execution_bytes,
            pop_active_continuations: pop.active_continuations,
            pop_continuation_bytes: pop.continuation_bytes,
            pop_continuation_rejected: pop.continuation_rejected,
            pop_pending_replays: pop.pending_arrivals,
            pop_pending_replay_bytes: pop.pending_arrival_bytes,
            pop_pending_replay_rejected: pop.pending_arrival_rejected,
            pop_pending_replay_invariant_failures: pop.pending_offset_invariant_failures,
            pull_live: pull.index.live(),
            pull_reserved: pull.index.reserved(),
            pull_candidates: pull.index.candidates(),
            pull_buckets: pull.index.buckets(),
            pull_resume_executions: pull.resume_executions,
            pull_resume_execution_bytes: pull.resume_execution_bytes,
            pull_active_continuations: pull.active_continuations,
            pull_continuation_bytes: pull.continuation_bytes,
            pull_continuation_rejected: pull.continuation_rejected,
            pull_pending_replays: pull.pending_arrivals,
            pull_pending_replay_bytes: pull.pending_arrival_bytes,
            pull_pending_replay_rejected: pull.pending_arrival_rejected,
            pull_pending_replay_invariant_failures: pull.pending_offset_invariant_failures,
            notification_live: notification.index().live(),
            notification_reserved: notification.index().reserved(),
            notification_candidates: notification.index().candidates(),
            notification_keys: notification.index().keys(),
            notification_oldest_waiter_age_millis: notification.index().oldest_waiter_age_millis(),
            notification_prepared: notification.prepared(),
            notification_pending_claims: notification.pending_claims(),
            notification_resume_executions: notification.resume_executions(),
            notification_resume_execution_bytes: notification.resume_execution_bytes(),
            notification_active_continuations: notification.active_continuations(),
            notification_continuation_bytes: notification.continuation_bytes(),
            notification_continuation_rejected: notification.continuation_rejected(),
            notification_pending_replays: notification.pending_arrivals(),
            notification_pending_replay_bytes: notification.pending_arrival_bytes(),
            notification_pending_replay_rejected: notification.pending_arrival_rejected(),
            notification_pending_replay_invariant_failures: notification.pending_offset_invariant_failures(),
            pop_lite_live: pop_lite.index.live,
            pop_lite_reserved: pop_lite.index.reserved,
            pop_lite_candidates: pop_lite.index.candidates,
            pop_lite_clients: pop_lite.index.clients,
            pop_lite_oldest_waiter_age_millis: pop_lite
                .index
                .oldest_waiter_age
                .map(|age| u64::try_from(age.as_millis()).unwrap_or(u64::MAX)),
            pop_lite_event_batches: pop_lite.event_reservations.batches,
            pop_lite_event_count: pop_lite.event_reservations.events,
            pop_lite_event_permits: pop_lite.event_reservations.permits,
            pop_lite_event_bytes: pop_lite.event_reservations.retained_bytes,
            pop_lite_active_client_gates: pop_lite.active_client_gates,
            pop_lite_active_event_producers: pop_lite.active_event_producers,
            pop_lite_prepared: pop_lite.prepared_registrations,
            pop_lite_pending_claims: pop_lite.pending_claims,
            pop_lite_accepted_resumes: pop_lite.accepted_resumes,
            pop_lite_resume_executions: pop_lite.resume_execution_count,
            pop_lite_resume_execution_bytes: pop_lite.resume_execution_bytes,
            pop_lite_pending_replays: pop_lite.pending_replays,
            handoff_tracked_targets: handoff.tracked_targets,
            handoff_occupancy: handoff.occupancy,
            handoff_active_wakes: handoff.active_wakes,
            handoff_candidates: handoff.candidates,
            handoff_wake_gates: handoff.wake_gates,
            handoff_continuations: handoff.continuations,
            handoff_replay_tokens: handoff.replay_tokens,
            handoff_abandoned_replays: handoff.abandoned_replays,
            handoff_zero,
            producer_task_count,
            shared_admission_current_count: shared_admission
                .connections
                .current_count
                .saturating_add(shared_admission.handshakes.current_count)
                .saturating_add(shared_admission.partial_frames.current_count)
                .saturating_add(shared_admission.inflight.current_count)
                .saturating_add(shared_admission.queued.current_count)
                .saturating_add(shared_admission.processors.current_count),
            shared_admission_current_bytes: shared_admission
                .connections
                .current_bytes
                .saturating_add(shared_admission.handshakes.current_bytes)
                .saturating_add(shared_admission.partial_frames.current_bytes)
                .saturating_add(shared_admission.inflight.current_bytes)
                .saturating_add(shared_admission.queued.current_bytes)
                .saturating_add(shared_admission.processors.current_bytes),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct BrokerDeferredResourceSnapshot {
    pub(crate) waiting_count: usize,
    pub(crate) retained_bytes: usize,
    pub(crate) pop_live: usize,
    pub(crate) pop_reserved: usize,
    pub(crate) pop_buckets: usize,
    pub(crate) pop_candidates: usize,
    pub(crate) pop_resume_executions: usize,
    pub(crate) pop_resume_execution_bytes: usize,
    pub(crate) pop_active_continuations: usize,
    pub(crate) pop_continuation_bytes: usize,
    pub(crate) pop_continuation_rejected: usize,
    pub(crate) pop_pending_replays: usize,
    pub(crate) pop_pending_replay_bytes: usize,
    pub(crate) pop_pending_replay_rejected: usize,
    pub(crate) pop_pending_replay_invariant_failures: usize,
    pub(crate) pull_live: usize,
    pub(crate) pull_reserved: usize,
    pub(crate) pull_candidates: usize,
    pub(crate) pull_buckets: usize,
    pub(crate) pull_resume_executions: usize,
    pub(crate) pull_resume_execution_bytes: usize,
    pub(crate) pull_active_continuations: usize,
    pub(crate) pull_continuation_bytes: usize,
    pub(crate) pull_continuation_rejected: usize,
    pub(crate) pull_pending_replays: usize,
    pub(crate) pull_pending_replay_bytes: usize,
    pub(crate) pull_pending_replay_rejected: usize,
    pub(crate) pull_pending_replay_invariant_failures: usize,
    pub(crate) notification_live: usize,
    pub(crate) notification_reserved: usize,
    pub(crate) notification_candidates: usize,
    pub(crate) notification_keys: usize,
    pub(crate) notification_oldest_waiter_age_millis: Option<u64>,
    pub(crate) notification_prepared: usize,
    pub(crate) notification_pending_claims: usize,
    pub(crate) notification_resume_executions: usize,
    pub(crate) notification_resume_execution_bytes: usize,
    pub(crate) notification_active_continuations: usize,
    pub(crate) notification_continuation_bytes: usize,
    pub(crate) notification_continuation_rejected: usize,
    pub(crate) notification_pending_replays: usize,
    pub(crate) notification_pending_replay_bytes: usize,
    pub(crate) notification_pending_replay_rejected: usize,
    pub(crate) notification_pending_replay_invariant_failures: usize,
    pub(crate) pop_lite_live: usize,
    pub(crate) pop_lite_reserved: usize,
    pub(crate) pop_lite_candidates: usize,
    pub(crate) pop_lite_clients: usize,
    pub(crate) pop_lite_oldest_waiter_age_millis: Option<u64>,
    pub(crate) pop_lite_event_batches: usize,
    pub(crate) pop_lite_event_count: usize,
    pub(crate) pop_lite_event_permits: usize,
    pub(crate) pop_lite_event_bytes: usize,
    pub(crate) pop_lite_active_client_gates: usize,
    pub(crate) pop_lite_active_event_producers: usize,
    pub(crate) pop_lite_prepared: usize,
    pub(crate) pop_lite_pending_claims: usize,
    pub(crate) pop_lite_accepted_resumes: usize,
    pub(crate) pop_lite_resume_executions: usize,
    pub(crate) pop_lite_resume_execution_bytes: usize,
    pub(crate) pop_lite_pending_replays: usize,
    pub(crate) handoff_tracked_targets: usize,
    pub(crate) handoff_occupancy: usize,
    pub(crate) handoff_active_wakes: usize,
    pub(crate) handoff_candidates: usize,
    pub(crate) handoff_wake_gates: usize,
    pub(crate) handoff_continuations: usize,
    pub(crate) handoff_replay_tokens: usize,
    pub(crate) handoff_abandoned_replays: usize,
    pub(crate) handoff_zero: bool,
    pub(crate) producer_task_count: usize,
    pub(crate) shared_admission_current_count: usize,
    pub(crate) shared_admission_current_bytes: usize,
}

impl BrokerDeferredResourceSnapshot {
    pub(crate) fn is_zero(self) -> bool {
        if !self.handoff_zero {
            return false;
        }
        let without_handoff_zero = Self {
            handoff_zero: false,
            pop_continuation_rejected: 0,
            pop_pending_replay_rejected: 0,
            pull_continuation_rejected: 0,
            pull_pending_replay_rejected: 0,
            notification_continuation_rejected: 0,
            notification_pending_replay_rejected: 0,
            ..self
        };
        without_handoff_zero == Self::default()
    }
}

#[cfg(test)]
mod resource_snapshot_tests {
    use super::BrokerDeferredResourceSnapshot;

    #[test]
    fn terminal_zero_check_includes_index_occupancy_but_ignores_diagnostic_rejections() {
        let diagnostics_only = BrokerDeferredResourceSnapshot {
            handoff_zero: true,
            pop_continuation_rejected: 1,
            pop_pending_replay_rejected: 2,
            pull_continuation_rejected: 3,
            pull_pending_replay_rejected: 4,
            notification_continuation_rejected: 5,
            notification_pending_replay_rejected: 6,
            ..BrokerDeferredResourceSnapshot::default()
        };
        assert!(diagnostics_only.is_zero());

        let occupancy_snapshots = [
            BrokerDeferredResourceSnapshot {
                pop_buckets: 1,
                ..diagnostics_only
            },
            BrokerDeferredResourceSnapshot {
                pop_candidates: 1,
                ..diagnostics_only
            },
            BrokerDeferredResourceSnapshot {
                pull_buckets: 1,
                ..diagnostics_only
            },
            BrokerDeferredResourceSnapshot {
                notification_keys: 1,
                ..diagnostics_only
            },
            BrokerDeferredResourceSnapshot {
                notification_oldest_waiter_age_millis: Some(0),
                ..diagnostics_only
            },
            BrokerDeferredResourceSnapshot {
                pop_lite_clients: 1,
                ..diagnostics_only
            },
            BrokerDeferredResourceSnapshot {
                pop_lite_oldest_waiter_age_millis: Some(0),
                ..diagnostics_only
            },
            BrokerDeferredResourceSnapshot {
                pop_lite_accepted_resumes: 1,
                ..diagnostics_only
            },
        ];
        assert!(occupancy_snapshots.into_iter().all(|snapshot| !snapshot.is_zero()));
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct BrokerDeferredRegistryShutdownOutcomes {
    pub(crate) pop: DeferredRegistryShutdownOutcome,
    pub(crate) pull: DeferredRegistryShutdownOutcome,
    pub(crate) notification: DeferredRegistryShutdownOutcome,
    pub(crate) pop_lite: DeferredRegistryShutdownOutcome,
}

impl BrokerDeferredRegistryShutdownOutcomes {
    pub(crate) fn is_healthy(self) -> bool {
        [self.pop, self.pull, self.notification, self.pop_lite]
            .into_iter()
            .all(deferred_registry_shutdown_outcome_is_healthy)
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct BrokerDeferredRegistryShutdownReport {
    pub(crate) initial: BrokerDeferredRegistryShutdownOutcomes,
    pub(crate) terminal: BrokerDeferredRegistryShutdownOutcomes,
}

impl BrokerDeferredRegistryShutdownReport {
    pub(crate) const fn new(
        initial: BrokerDeferredRegistryShutdownOutcomes,
        terminal: BrokerDeferredRegistryShutdownOutcomes,
    ) -> Self {
        Self { initial, terminal }
    }

    pub(crate) fn is_healthy(self) -> bool {
        self.initial.is_healthy() && self.terminal.is_healthy()
    }
}

fn deferred_registry_shutdown_outcome_is_healthy(outcome: DeferredRegistryShutdownOutcome) -> bool {
    match outcome {
        DeferredRegistryShutdownOutcome::Completed(stats) => stats.invariant_failures() == 0,
        DeferredRegistryShutdownOutcome::AlreadyClosed => true,
        DeferredRegistryShutdownOutcome::InProgress => false,
        _ => false,
    }
}

impl BrokerComposition {
    pub(super) fn deferred_generation_handoff(&self) -> Option<Arc<DeferredGenerationHandoff>> {
        self.data_plane
            .deferred
            .as_ref()
            .map(|deferred| Arc::clone(&deferred.handoff))
    }
}

impl BrokerRuntime {
    pub(super) fn initialize_deferred_lifecycle(&mut self) -> Result<Arc<AdmissionController>, BrokerStartupError> {
        if let Some(deferred) = self.composition.data_plane.deferred.as_ref() {
            let admission_controller = Arc::clone(&deferred.admission_controller);
            self.composition
                .request_pipeline
                .install_admission_controller(Arc::clone(&admission_controller))?;
            return Ok(admission_controller);
        }
        let config = self.composition.state.broker_config();
        let admission_budget = self
            .composition
            .state
            .service_context
            .as_ref()
            .ok_or_else(|| BrokerStartupError::Initialization {
                component: "deferred_admission",
                detail: "Broker deferred admission requires an injected service context".to_owned(),
            })?
            .process_budget();
        let retained_bytes = usize::try_from(
            self.composition
                .state
                .config_state
                .snapshot()
                .validated()
                .sections()
                .resources()
                .managed_memory_bytes(),
        )
        .unwrap_or(usize::MAX);
        let deferred = BrokerDeferredLifecycle::try_new(
            config.as_ref(),
            &admission_budget,
            retained_bytes,
            self.composition.state.lite_event_dispatcher().clone(),
        )?;
        let admission_controller = Arc::clone(&deferred.admission_controller);
        self.composition
            .request_pipeline
            .install_admission_controller(Arc::clone(&admission_controller))?;
        self.composition.data_plane.deferred = Some(deferred);
        Ok(admission_controller)
    }
}
