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

use std::collections::VecDeque;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use rocketmq_model::common::mix_all;
use rocketmq_runtime::task::service_task::ServiceTask;
use rocketmq_runtime::task::service_task::ServiceTaskContext;
use rocketmq_runtime::task::ServiceManager;
use rocketmq_runtime::BudgetLimit;
use rocketmq_runtime::BudgetedQueue;
use rocketmq_runtime::FullPolicy;
use rocketmq_runtime::ProcessMemoryLimit;
use rocketmq_runtime::ResourceBudget;
use rocketmq_runtime::ResourceBudgetTree;
use rocketmq_runtime::ResourcePermit;
use rocketmq_runtime::ShutdownDeadline;
use rocketmq_runtime::TaskGroup;
use rocketmq_store_api::AckPolicy;
use rocketmq_store_api::ReplicationDecision;
use rocketmq_store_api::WriteAuthority;
use tracing::error;
use tracing::warn;

use crate::base::message_status_enum::PutMessageStatus;
use crate::ha::ack_frontier::AckFrontier;
use crate::ha::general_ha_service::GeneralHAServiceReference;
use crate::ha::ha_service::HAService;
use crate::ha::HAError;
use crate::log_file::group_commit_request::GroupCommitRequest;
pub(crate) use rocketmq_store_local::ha::replication::GroupTransferRuntimeInfo;

const PROGRESS_SAFETY_RECHECK_INTERVAL: Duration = Duration::from_millis(100);
const IDLE_WAIT_INTERVAL: Duration = Duration::from_secs(3600);

pub struct GroupTransferService {
    inner: Arc<GroupTransferServiceInner>,
    service_manager: ServiceManager<GroupTransferServiceInner>,
}

impl GroupTransferService {
    /// Builds the HA acknowledgement queue from the process memory limit and
    /// runs the transfer loop under `parent_task_group`.
    ///
    /// # Errors
    ///
    /// Returns an error when the process memory limit cannot be detected.
    pub fn try_new(ha_service: GeneralHAServiceReference, parent_task_group: TaskGroup) -> Result<Self, HAError> {
        let inner = Arc::new(GroupTransferServiceInner::try_new(ha_service)?);
        Ok(GroupTransferService {
            inner: inner.clone(),
            service_manager: ServiceManager::new_arc_with_task_group(inner, parent_task_group),
        })
    }

    pub async fn start(&self) -> Result<(), HAError> {
        self.service_manager.start().await.map_err(|source| {
            error!(source_present = true, "Failed to start GroupTransferService");
            HAError::operation("start group transfer service", source)
        })
    }

    pub async fn shutdown(&self) {
        let _ = self.service_manager.shutdown().await;
    }

    /// Stops the transfer loop, aborting it if it is still running at `deadline`.
    pub async fn shutdown_until(&self, deadline: ShutdownDeadline) {
        let _ = self.service_manager.shutdown_until(deadline).await;
    }

    pub async fn put_request(&self, request: GroupCommitRequest) {
        self.inner.put_request(request).await;
        self.service_manager.wakeup();
    }

    pub fn notify_transfer_some(&self) {
        self.inner.record_ack_notify();
        self.service_manager.wakeup();
    }

    pub fn notify_transfer_progress(&self) {
        self.service_manager.wakeup();
    }

    pub(crate) fn runtime_info(&self) -> GroupTransferRuntimeInfo {
        self.inner.runtime_info()
    }
}

/// Budget for pending replica-ack waits: bounded by count, bytes and age, not by admission rate.
///
/// Every append that needs replica acks registers exactly one wait after it was appended, so an
/// admission-rate limit would refuse appended writes above that rate with `FlushSlaveTimeout`
/// while the queue still has room.
fn group_transfer_queue_budget(queue_count: usize, queue_bytes: usize) -> Result<ResourceBudget, HAError> {
    let tree = ResourceBudgetTree::new("store", BudgetLimit::new(queue_count, queue_bytes, FullPolicy::Reject))
        .map_err(HAError::budget)?;
    tree.root()
        .child(
            "ha-group-transfer",
            BudgetLimit::new(queue_count, queue_bytes, FullPolicy::Reject).with_max_age(Duration::from_secs(30)),
        )
        .map_err(HAError::budget)
}

struct GroupTransferServiceInner {
    ha_service: GeneralHAServiceReference,
    ack_notify_count: AtomicU64,
    pending_requests: BudgetedQueue<PendingGroupTransfer>,
}

impl GroupTransferServiceInner {
    fn try_new(ha_service: GeneralHAServiceReference) -> Result<Self, HAError> {
        let process_limit = ProcessMemoryLimit::detect()
            .map_err(|error| HAError::operation("detect HA process memory limit", error))?;
        let managed_bytes = process_limit.fraction(1, 16).map_err(HAError::budget)?;
        let queue_bytes = usize::try_from((managed_bytes / 2).max(1)).unwrap_or(usize::MAX);
        let request_bytes = std::mem::size_of::<PendingGroupTransfer>().max(1);
        let queue_count = (queue_bytes / request_bytes).clamp(1, 65_536);
        Ok(GroupTransferServiceInner {
            ha_service,
            ack_notify_count: AtomicU64::new(0),
            pending_requests: BudgetedQueue::new(group_transfer_queue_budget(queue_count, queue_bytes)?),
        })
    }

    #[inline]
    fn record_ack_notify(&self) {
        self.ack_notify_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    fn runtime_info(&self) -> GroupTransferRuntimeInfo {
        let snapshot = self.pending_requests.snapshot();

        GroupTransferRuntimeInfo {
            pending_request_count: snapshot.reserved_count as u64,
            pending_request_oldest_wait_millis: snapshot
                .oldest_age
                .and_then(|age| u64::try_from(age.as_millis()).ok())
                .unwrap_or(0),
            ack_notify_count: self.ack_notify_count.load(std::sync::atomic::Ordering::Relaxed),
        }
    }

    #[inline]
    async fn put_request(&self, request: GroupCommitRequest) {
        let retained_bytes = std::mem::size_of::<PendingGroupTransfer>();
        if matches!(
            self.pending_requests
                .try_push_data(PendingGroupTransfer::new(request), retained_bytes),
            rocketmq_runtime::QueueEnqueueStatus::Rejected { .. }
        ) {
            warn!("HA group-transfer queue rejected a request");
        }
    }

    fn drain_new_requests(&self, active: &mut ActiveTransfers) {
        while let Some(budgeted) = self.pending_requests.try_pop_budgeted() {
            let (mut pending, permit, _) = budgeted.into_parts();
            let policy = match AckPolicy::try_from_legacy(
                pending.request_mut().get_ack_nums(),
                mix_all::ALL_ACK_IN_SYNC_STATE_SET,
            ) {
                Ok(policy) => policy,
                Err(_error) => {
                    warn!("HA group-transfer request has an invalid ACK policy");
                    pending
                        .request_mut()
                        .wakeup_customer(PutMessageStatus::FlushSlaveTimeout);
                    pending.complete();
                    continue;
                }
            };
            active.admit(ActiveGroupTransfer::admitted(pending, policy, permit));
        }
    }

    async fn evaluate_pending(&self, active: &mut ActiveTransfers) -> Option<Instant> {
        self.drain_new_requests(active);
        if active.is_empty() {
            return None;
        }

        let frontier = match self.ha_service.upgrade() {
            Some(ha_service) => {
                let snapshots = ha_service.snapshot_acked_replicas().await;
                AckFrontier::from_snapshots(
                    ha_service.write_authority(),
                    ha_service.sync_state_set(),
                    ha_service.local_durable_watermark(),
                    &snapshots,
                    ha_service.is_auto_switch_enabled(),
                )
            }
            None => AckFrontier::from_snapshots(None, None, 0, &[], false),
        };
        active.resolve(&frontier, Instant::now());
        active.earliest_deadline()
    }
}

/// Admitted waits, by the authority they were appended under and their ACK policy.
///
/// For one authority and policy, [`AckFrontier::decide`] is monotone in the offset: it rejects
/// only for reasons that do not depend on the offset (or for a negative offset), and when it
/// acknowledges an offset it acknowledges every smaller one. Each group therefore keeps its
/// waits in offset order and settles them with a constant number of decisions per evaluation,
/// instead of deciding every waiting append each time an ACK or a new append wakes the service.
#[derive(Default)]
struct ActiveTransfers {
    groups: Vec<TransferGroup>,
}

impl ActiveTransfers {
    fn admit(&mut self, transfer: ActiveGroupTransfer) {
        let authority = transfer.requested_authority();
        let policy = transfer.policy;
        match self
            .groups
            .iter_mut()
            .find(|group| group.authority == authority && group.policy == policy)
        {
            Some(group) => group.admit(transfer),
            None => self.groups.push(TransferGroup::new(authority, policy, transfer)),
        }
    }

    fn is_empty(&self) -> bool {
        self.groups.is_empty()
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.groups.iter().map(|group| group.waits.len()).sum()
    }

    #[cfg(test)]
    fn offsets(&self) -> Vec<i64> {
        self.groups
            .iter()
            .flat_map(|group| group.waits.iter().map(ActiveGroupTransfer::next_offset))
            .collect()
    }

    fn resolve(&mut self, frontier: &AckFrontier, now: Instant) {
        for group in &mut self.groups {
            group.resolve(frontier, now);
        }
        self.groups.retain(|group| !group.waits.is_empty());
    }

    /// No admitted wait expires before the returned instant.
    fn earliest_deadline(&self) -> Option<Instant> {
        self.groups.iter().map(|group| group.earliest_deadline).min()
    }
}

/// Waits of one authority and ACK policy, in offset order.
struct TransferGroup {
    authority: Option<WriteAuthority>,
    policy: AckPolicy,
    waits: VecDeque<ActiveGroupTransfer>,
    /// A lower bound of the waits' deadlines. Settled waits leave it low until the next expiry
    /// scan, which then only comes early.
    earliest_deadline: Instant,
}

impl TransferGroup {
    fn new(authority: Option<WriteAuthority>, policy: AckPolicy, first: ActiveGroupTransfer) -> Self {
        let earliest_deadline = first.deadline();
        Self {
            authority,
            policy,
            waits: VecDeque::from([first]),
            earliest_deadline,
        }
    }

    fn admit(&mut self, transfer: ActiveGroupTransfer) {
        self.earliest_deadline = self.earliest_deadline.min(transfer.deadline());
        let offset = transfer.next_offset();
        // Appends usually wait in offset order; concurrent appends may arrive slightly out of it.
        let index = if self.waits.back().is_none_or(|last| last.next_offset() <= offset) {
            self.waits.len()
        } else {
            self.waits.partition_point(|wait| wait.next_offset() <= offset)
        };
        self.waits.insert(index, transfer);
    }

    fn resolve(&mut self, frontier: &AckFrontier, now: Instant) {
        if now >= self.earliest_deadline {
            self.expire(now);
        }
        // A negative offset is rejected on its own and decides nothing for the other waits.
        while self.waits.front().is_some_and(|wait| wait.next_offset() < 0) {
            if let Some(mut wait) = self.waits.pop_front() {
                warn!("HA group-transfer request was rejected by the canonical decision");
                wait.complete(PutMessageStatus::FlushSlaveTimeout);
            }
        }
        let Some(front) = self.waits.front() else {
            return;
        };
        match self.decide(frontier, front.next_offset()) {
            // Every later wait of the group waits too.
            ReplicationDecision::Wait { .. } => return,
            ReplicationDecision::Reject(_reason) => {
                // The rejection does not depend on the offset: it holds for the whole group.
                warn!(
                    requests = self.waits.len(),
                    "HA group-transfer requests were rejected by the canonical decision"
                );
                for mut wait in self.waits.drain(..) {
                    wait.complete(PutMessageStatus::FlushSlaveTimeout);
                }
                return;
            }
            ReplicationDecision::Acknowledge(_) => {}
        }
        // Acknowledge up to the policy frontier: confirming the last of these acknowledges the
        // earlier ones too.
        let frontier_offset = frontier.policy_frontier(self.policy);
        let mut acknowledged = self
            .waits
            .partition_point(|wait| wait.next_offset() <= frontier_offset)
            .max(1);
        let last_acknowledged = self
            .waits
            .get(acknowledged - 1)
            .map(|wait| self.decide(frontier, wait.next_offset()));
        if acknowledged > 1 && !matches!(last_acknowledged, Some(ReplicationDecision::Acknowledge(_))) {
            // The frontier and the canonical decision disagree: settle the waits one by one.
            acknowledged = 1 + self
                .waits
                .iter()
                .take(acknowledged)
                .skip(1)
                .take_while(|wait| {
                    matches!(
                        self.decide(frontier, wait.next_offset()),
                        ReplicationDecision::Acknowledge(_)
                    )
                })
                .count();
        }
        for mut wait in self.waits.drain(..acknowledged) {
            wait.complete(PutMessageStatus::PutOk);
        }
    }

    fn decide(&self, frontier: &AckFrontier, offset: i64) -> ReplicationDecision {
        frontier.decide(self.authority, self.policy, offset)
    }

    /// Times out the expired waits and resets the deadline bound to the earliest remaining one.
    fn expire(&mut self, now: Instant) {
        let mut earliest: Option<Instant> = None;
        self.waits.retain_mut(|wait| {
            if now >= wait.deadline() {
                wait.complete(PutMessageStatus::FlushSlaveTimeout);
                warn!("HA group-transfer request timed out");
                return false;
            }
            earliest = Some(earliest.map_or(wait.deadline(), |earliest| earliest.min(wait.deadline())));
            true
        });
        self.earliest_deadline = earliest.unwrap_or(now);
    }
}

struct ActiveGroupTransfer {
    pending: PendingGroupTransfer,
    policy: AckPolicy,
    _permit: Option<ResourcePermit>,
}

impl ActiveGroupTransfer {
    fn admitted(pending: PendingGroupTransfer, policy: AckPolicy, permit: ResourcePermit) -> Self {
        Self {
            pending,
            policy,
            _permit: Some(permit),
        }
    }

    #[cfg(test)]
    fn unadmitted(pending: PendingGroupTransfer, policy: AckPolicy) -> Self {
        Self {
            pending,
            policy,
            _permit: None,
        }
    }

    fn deadline(&self) -> Instant {
        self.pending.request.get_deadline()
    }

    fn next_offset(&self) -> i64 {
        self.pending.request.get_next_offset()
    }

    fn ack_nums(&self) -> i32 {
        self.pending.request.get_ack_nums()
    }

    fn requested_authority(&self) -> Option<WriteAuthority> {
        self.pending.request.requested_authority()
    }

    fn complete(&mut self, status: PutMessageStatus) {
        self.pending.request_mut().wakeup_customer(status);
        self.pending.complete();
    }
}

struct PendingGroupTransfer {
    request: GroupCommitRequest,
    completed: bool,
}

impl PendingGroupTransfer {
    fn new(request: GroupCommitRequest) -> Self {
        Self {
            request,
            completed: false,
        }
    }

    fn request_mut(&mut self) -> &mut GroupCommitRequest {
        &mut self.request
    }

    fn complete(&mut self) {
        self.completed = true;
    }
}

impl Drop for PendingGroupTransfer {
    fn drop(&mut self) {
        if !self.completed {
            self.request.wakeup_customer(PutMessageStatus::FlushSlaveTimeout);
        }
    }
}

impl ServiceTask for GroupTransferServiceInner {
    fn get_service_name(&self) -> String {
        "GroupTransferService".to_string()
    }

    async fn run(&self, context: &ServiceTaskContext) {
        let mut active = ActiveTransfers::default();
        while !context.is_stopped() {
            let next_deadline = self.evaluate_pending(&mut active).await;
            if context.is_stopped() {
                break;
            }
            let wait = next_deadline
                .map(|deadline| deadline.saturating_duration_since(Instant::now()))
                .map(|until_deadline| until_deadline.min(PROGRESS_SAFETY_RECHECK_INTERVAL))
                .unwrap_or(IDLE_WAIT_INTERVAL);
            context.wait_for_running(wait).await;
        }
        self.drain_new_requests(&mut active);
    }

    async fn on_wait_end(&self) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ha::default_ha_service::DefaultHAService;
    use crate::ha::general_ha_service::GeneralHAService;
    use crate::ha::test_support::new_test_message_store;

    fn new_test_service(reference: GeneralHAServiceReference) -> GroupTransferService {
        let scope = crate::runtime::test_scope("group-transfer-service-test");
        GroupTransferService::try_new(
            reference,
            crate::runtime::task_group(&scope, "rocketmq-store.ha.group-transfer"),
        )
        .expect("build group transfer service")
    }

    fn new_test_ha_service() -> GeneralHAService {
        let temp_root = tempfile::tempdir().expect("create temp root dir");
        let store = new_test_message_store(temp_root.path(), false);
        let mut service = GeneralHAService::new_with_default_ha_service(DefaultHAService::new(
            store.ha_replica_store_handle(),
            crate::runtime::test_scope("group-transfer-ha-service-test"),
        ));
        service.init().expect("init default ha service");
        service
    }

    #[tokio::test]
    async fn runtime_info_reports_pending_requests_and_ack_notifications() {
        let ha_service = new_test_ha_service();
        let reference = GeneralHAServiceReference::new();
        reference.bind(&ha_service).expect("bind general ha service");
        let service = new_test_service(reference);
        let (request, _response) = GroupCommitRequest::with_ack_nums(128, 5_000, 2);

        service.put_request(request).await;
        service.notify_transfer_some();
        service.notify_transfer_some();

        let runtime_info = service.runtime_info();
        assert_eq!(runtime_info.pending_request_count, 1);
        assert!(runtime_info.pending_request_oldest_wait_millis < 5_000);
        assert_eq!(runtime_info.ack_notify_count, 2);
    }

    #[tokio::test]
    async fn new_request_wakes_the_event_driven_service() {
        let ha_service = new_test_ha_service();
        let reference = GeneralHAServiceReference::new();
        reference.bind(&ha_service).expect("bind general ha service");
        let service = new_test_service(reference);
        service.start().await.expect("start group transfer service");
        let (request, response) = GroupCommitRequest::with_ack_nums(0, 5_000, 1);

        service.put_request(request).await;

        assert_eq!(
            tokio::time::timeout(Duration::from_millis(250), response.wait_for_result())
                .await
                .expect("event-driven response")
                .expect("group transfer status"),
            PutMessageStatus::PutOk
        );
        service.shutdown().await;
    }

    #[tokio::test]
    async fn shutdown_completes_active_and_queued_waiters() {
        let ha_service = new_test_ha_service();
        let reference = GeneralHAServiceReference::new();
        reference.bind(&ha_service).expect("bind general ha service");
        let service = new_test_service(reference);
        service.start().await.expect("start group transfer service");
        let (request, response) = GroupCommitRequest::with_ack_nums(128, 5_000, 2);
        service.put_request(request).await;

        service.shutdown().await;

        assert_eq!(
            tokio::time::timeout(Duration::from_millis(250), response.wait_for_result())
                .await
                .expect("shutdown response")
                .expect("group transfer status"),
            PutMessageStatus::FlushSlaveTimeout
        );
    }

    #[tokio::test]
    async fn overload_rejects_excess_group_transfers_without_growing_the_queue() {
        let ha_service = new_test_ha_service();
        let reference = GeneralHAServiceReference::new();
        reference.bind(&ha_service).expect("bind general ha service");
        let retained_bytes = std::mem::size_of::<PendingGroupTransfer>().max(1);
        let inner = GroupTransferServiceInner {
            ha_service: reference,
            ack_notify_count: AtomicU64::new(0),
            pending_requests: BudgetedQueue::new(
                group_transfer_queue_budget(2, retained_bytes * 2).expect("queue budget"),
            ),
        };
        let mut responses = Vec::new();

        for offset in 0..4 {
            let (request, response) = GroupCommitRequest::with_ack_nums(offset, 5_000, 2);
            inner.put_request(request).await;
            responses.push(response);
        }

        assert_eq!(inner.runtime_info().pending_request_count, 2);
        for response in responses.iter_mut().skip(2) {
            assert_eq!(
                response.wait_for_result_with_timeout().await.expect("rejection status"),
                PutMessageStatus::FlushSlaveTimeout
            );
        }
        assert_eq!(inner.pending_requests.snapshot().rejected_count, 2);
    }

    #[tokio::test]
    async fn admission_rate_is_not_limited_while_the_queue_has_room() {
        let ha_service = new_test_ha_service();
        let reference = GeneralHAServiceReference::new();
        reference.bind(&ha_service).expect("bind general ha service");
        let retained_bytes = std::mem::size_of::<PendingGroupTransfer>().max(1);
        let inner = GroupTransferServiceInner {
            ha_service: reference,
            ack_notify_count: AtomicU64::new(0),
            pending_requests: BudgetedQueue::new(
                group_transfer_queue_budget(2, retained_bytes * 2).expect("queue budget"),
            ),
        };

        // Three times the queue capacity within well under a second: a queue that is drained
        // between bursts must admit every wait.
        for round in 0..3 {
            for offset in 0..2 {
                let (request, _response) = GroupCommitRequest::with_ack_nums(round * 2 + offset, 5_000, 2);
                inner.put_request(request).await;
            }
            assert_eq!(inner.runtime_info().pending_request_count, 2, "round {round}");
            let mut active = ActiveTransfers::default();
            inner.drain_new_requests(&mut active);
            assert_eq!(active.len(), 2);
            drop(active);
        }
        assert_eq!(inner.pending_requests.snapshot().rejected_count, 0);
    }

    #[tokio::test]
    async fn already_expired_group_transfer_completes_without_deadline_subtraction_panic() {
        let ha_service = new_test_ha_service();
        let reference = GeneralHAServiceReference::new();
        reference.bind(&ha_service).expect("bind general ha service");
        let inner = GroupTransferServiceInner::try_new(reference).expect("group transfer service");
        let (request, mut response) = GroupCommitRequest::with_ack_nums(128, 0, 2);

        inner.put_request(request).await;
        let mut active = ActiveTransfers::default();
        inner.evaluate_pending(&mut active).await;

        assert_eq!(
            response.wait_for_result_with_timeout().await.expect("timeout status"),
            PutMessageStatus::FlushSlaveTimeout
        );
    }

    #[tokio::test]
    async fn invalid_ack_policy_fails_closed_and_drains_the_request() {
        let ha_service = new_test_ha_service();
        let reference = GeneralHAServiceReference::new();
        reference.bind(&ha_service).expect("bind general ha service");
        let inner = GroupTransferServiceInner::try_new(reference).expect("group transfer service");
        let (request, mut response) = GroupCommitRequest::with_ack_nums(0, 5_000, 0);

        inner.put_request(request).await;
        let mut active = ActiveTransfers::default();
        inner.evaluate_pending(&mut active).await;

        assert_eq!(
            response.wait_for_result_with_timeout().await.expect("rejected status"),
            PutMessageStatus::FlushSlaveTimeout
        );
        assert_eq!(inner.runtime_info().pending_request_count, 0);
    }

    fn authority(epoch: i32) -> rocketmq_store_api::WriteAuthority {
        rocketmq_store_api::WriteAuthority::try_new(
            0,
            rocketmq_store_api::MasterEpoch::try_from(epoch).expect("positive epoch"),
        )
        .expect("valid authority")
    }

    fn two_replicas() -> AckPolicy {
        AckPolicy::ReplicaCount(rocketmq_store_api::ReplicaCount::try_new(2).expect("two replicas"))
    }

    /// A frontier of master 0 with its replica 1 acknowledged up to `replica_ack`.
    fn frontier_with_replica_ack(epoch: i32, replica_ack: i64) -> AckFrontier {
        AckFrontier::from_snapshots(
            Some(authority(epoch)),
            Some(std::collections::HashSet::from([0, 1])),
            1_000,
            &[crate::ha::ha_service::HAAckedReplicaSnapshot {
                slave_broker_id: Some(1),
                slave_ack_offset: replica_ack,
            }],
            true,
        )
    }

    fn admit_waits(
        active: &mut ActiveTransfers,
        offsets: &[i64],
        timeout_millis: u64,
        requested_authority: rocketmq_store_api::WriteAuthority,
    ) -> Vec<(i64, crate::log_file::group_commit_request::GroupCommitResponse)> {
        offsets
            .iter()
            .map(|offset| {
                let (request, response) =
                    GroupCommitRequest::with_ack_nums_and_authority(*offset, timeout_millis, 2, requested_authority);
                active.admit(ActiveGroupTransfer::unadmitted(
                    PendingGroupTransfer::new(request),
                    two_replicas(),
                ));
                (*offset, response)
            })
            .collect()
    }

    #[tokio::test]
    async fn one_evaluation_acknowledges_every_wait_up_to_the_frontier() {
        let mut active = ActiveTransfers::default();
        // Concurrent appends may wait slightly out of offset order.
        let responses = admit_waits(&mut active, &[300, 100, 500, 200, 400], 5_000, authority(1));

        active.resolve(&frontier_with_replica_ack(1, 300), Instant::now());

        assert_eq!(active.offsets(), vec![400, 500]);
        for (offset, mut response) in responses.into_iter().filter(|(offset, _)| *offset <= 300) {
            assert_eq!(
                response.wait_for_result_with_timeout().await.expect("status"),
                PutMessageStatus::PutOk,
                "offset {offset}"
            );
        }
    }

    #[tokio::test]
    async fn stale_authority_rejects_every_wait_of_its_group() {
        let mut active = ActiveTransfers::default();
        let stale = admit_waits(&mut active, &[100, 900], 5_000, authority(1));
        let current = admit_waits(&mut active, &[200], 5_000, authority(2));

        active.resolve(&frontier_with_replica_ack(2, 300), Instant::now());

        assert!(active.is_empty());
        for (offset, mut response) in stale {
            assert_eq!(
                response.wait_for_result_with_timeout().await.expect("status"),
                PutMessageStatus::FlushSlaveTimeout,
                "offset {offset}"
            );
        }
        for (_, mut response) in current {
            assert_eq!(
                response.wait_for_result_with_timeout().await.expect("status"),
                PutMessageStatus::PutOk
            );
        }
    }

    #[tokio::test]
    async fn negative_offset_is_rejected_without_deciding_for_later_waits() {
        let mut active = ActiveTransfers::default();
        let mut responses = admit_waits(&mut active, &[-1, 100], 5_000, authority(1));

        active.resolve(&frontier_with_replica_ack(1, 300), Instant::now());

        assert!(active.is_empty());
        let (_, mut acknowledged) = responses.pop().expect("offset 100");
        let (_, mut rejected) = responses.pop().expect("offset -1");
        assert_eq!(
            rejected.wait_for_result_with_timeout().await.expect("status"),
            PutMessageStatus::FlushSlaveTimeout
        );
        assert_eq!(
            acknowledged.wait_for_result_with_timeout().await.expect("status"),
            PutMessageStatus::PutOk
        );
    }

    #[tokio::test]
    async fn expired_wait_times_out_while_an_earlier_offset_keeps_waiting() {
        let mut active = ActiveTransfers::default();
        admit_waits(&mut active, &[400], 60_000, authority(1));
        let mut expired = admit_waits(&mut active, &[500], 50, authority(1));
        let frontier = frontier_with_replica_ack(1, 300);

        active.resolve(&frontier, Instant::now());
        assert_eq!(active.offsets(), vec![400, 500]);
        active.resolve(&frontier, Instant::now() + Duration::from_secs(1));

        assert_eq!(active.offsets(), vec![400]);
        let (_, mut expired) = expired.pop().expect("offset 500");
        assert_eq!(
            expired.wait_for_result_with_timeout().await.expect("status"),
            PutMessageStatus::FlushSlaveTimeout
        );
        let earliest = active.earliest_deadline().expect("a wait remains");
        assert!(
            earliest > Instant::now() + Duration::from_secs(30),
            "the bound follows the remaining wait"
        );
    }

    #[tokio::test]
    async fn satisfied_low_offset_is_not_blocked_by_an_unsatisfied_high_offset() {
        use rocketmq_store_api::MasterEpoch;
        use rocketmq_store_api::ReplicaCount;
        use rocketmq_store_api::WriteAuthority;

        let authority =
            WriteAuthority::try_new(0, MasterEpoch::try_from(1).expect("positive epoch")).expect("valid authority");
        let frontier = AckFrontier::from_snapshots(
            Some(authority),
            Some(std::collections::HashSet::from([0, 1])),
            200,
            &[crate::ha::ha_service::HAAckedReplicaSnapshot {
                slave_broker_id: Some(1),
                slave_ack_offset: 120,
            }],
            true,
        );
        let policy = AckPolicy::ReplicaCount(ReplicaCount::try_new(2).expect("two replicas"));
        let (high_request, _high_response) = GroupCommitRequest::with_ack_nums_and_authority(180, 5_000, 2, authority);
        let (low_request, mut low_response) = GroupCommitRequest::with_ack_nums_and_authority(120, 5_000, 2, authority);
        let mut active = ActiveTransfers::default();
        active.admit(ActiveGroupTransfer::unadmitted(
            PendingGroupTransfer::new(high_request),
            policy,
        ));
        active.admit(ActiveGroupTransfer::unadmitted(
            PendingGroupTransfer::new(low_request),
            policy,
        ));

        active.resolve(&frontier, Instant::now());

        assert_eq!(active.offsets(), vec![180]);
        assert_eq!(
            low_response
                .wait_for_result_with_timeout()
                .await
                .expect("low offset status"),
            PutMessageStatus::PutOk
        );
    }

    #[tokio::test]
    async fn async_flush_counts_the_master_copy_before_the_background_flush() {
        use rocketmq_store_api::MasterEpoch;
        use rocketmq_store_api::ReplicaCount;
        use rocketmq_store_api::WriteAuthority;

        use crate::base::message_store_engine::MessageStoreEngine;
        use crate::config::flush_disk_type::FlushDiskType;
        use crate::ha::test_support::new_test_message_store_with_flush;

        let authority =
            WriteAuthority::try_new(0, MasterEpoch::try_from(1).expect("positive epoch")).expect("valid authority");
        let policy = AckPolicy::ReplicaCount(ReplicaCount::try_new(2).expect("two replicas"));
        for (flush_disk_type, acknowledged) in [(FlushDiskType::AsyncFlush, true), (FlushDiskType::SyncFlush, false)] {
            let temp_root = tempfile::tempdir().expect("create temp root dir");
            let mut store = new_test_message_store_with_flush(temp_root.path(), false, flush_disk_type);
            store.init().await.expect("init message store");
            store
                .get_commit_log_mut()
                .append_data(0, &[1, 2, 3, 4], 0, 4)
                .await
                .expect("append data");
            assert!(
                store.get_flushed_where() < 4,
                "the appended bytes must still be unflushed"
            );
            let mut ha_service = GeneralHAService::new_with_default_ha_service(DefaultHAService::new(
                store.ha_replica_store_handle(),
                crate::runtime::test_scope("group-transfer-flush-policy-test"),
            ));
            ha_service.init().expect("init default ha service");

            let frontier = AckFrontier::from_snapshots(
                Some(authority),
                Some(std::collections::HashSet::from([0, 1])),
                ha_service.local_durable_watermark(),
                &[crate::ha::ha_service::HAAckedReplicaSnapshot {
                    slave_broker_id: Some(1),
                    slave_ack_offset: 4,
                }],
                true,
            );

            assert_eq!(
                matches!(
                    frontier.decide(Some(authority), policy, 4),
                    ReplicationDecision::Acknowledge(_)
                ),
                acknowledged,
                "{flush_disk_type:?}"
            );
        }
    }

    #[test]
    fn general_service_reference_does_not_retain_root() {
        let ha_service = new_test_ha_service();
        let reference = GeneralHAServiceReference::new();
        reference.bind(&ha_service).expect("bind general ha service");

        assert!(reference.upgrade().is_some());
        drop(ha_service);
        assert!(reference.upgrade().is_none());
    }
}
