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
use rocketmq_runtime::RateLimit;
use rocketmq_runtime::ResourceBudgetTree;
use rocketmq_runtime::ResourcePermit;
use rocketmq_store_api::AckPolicy;
use rocketmq_store_api::ReplicationDecision;
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
    /// Builds the HA acknowledgement queue from the process memory limit.
    ///
    /// # Panics
    ///
    /// Panics when the process memory limit cannot be detected. Production
    /// composition should prefer [`Self::try_new`].
    pub fn new(ha_service: GeneralHAServiceReference) -> Self {
        Self::try_new(ha_service)
            .unwrap_or_else(|error| panic!("failed to build GroupTransferService resource budget: {error}"))
    }

    pub fn try_new(ha_service: GeneralHAServiceReference) -> Result<Self, HAError> {
        let inner = Arc::new(GroupTransferServiceInner::try_new(ha_service)?);
        Ok(GroupTransferService {
            inner: inner.clone(),
            service_manager: ServiceManager::new_arc_legacy_compatibility(inner),
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
        let tree = ResourceBudgetTree::new("store", BudgetLimit::new(queue_count, queue_bytes, FullPolicy::Reject))
            .map_err(HAError::budget)?;
        let queue_budget = tree
            .root()
            .child(
                "ha-group-transfer",
                BudgetLimit::new(queue_count, queue_bytes, FullPolicy::Reject)
                    .with_rate(RateLimit::new(queue_count as u64, queue_count as u64))
                    .with_max_age(Duration::from_secs(30)),
            )
            .map_err(HAError::budget)?;
        Ok(GroupTransferServiceInner {
            ha_service,
            ack_notify_count: AtomicU64::new(0),
            pending_requests: BudgetedQueue::new(queue_budget),
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
        if let Err(_error) = self
            .pending_requests
            .try_push_data(PendingGroupTransfer::new(request), retained_bytes)
        {
            warn!("HA group-transfer queue rejected a request");
        }
    }

    fn drain_new_requests(&self, active: &mut Vec<ActiveGroupTransfer>) {
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
            active.push(ActiveGroupTransfer::admitted(pending, policy, permit));
        }
    }

    async fn evaluate_pending(&self, active: &mut Vec<ActiveGroupTransfer>) -> Option<Instant> {
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
        Self::resolve_pending(active, &frontier, Instant::now());
        active.iter().map(ActiveGroupTransfer::deadline).min()
    }

    fn resolve_pending(active: &mut Vec<ActiveGroupTransfer>, frontier: &AckFrontier, now: Instant) {
        active.retain_mut(|pending| {
            if now >= pending.deadline() {
                pending.complete(PutMessageStatus::FlushSlaveTimeout);
                warn!("HA group-transfer request timed out");
                return false;
            }
            match frontier.decide(pending.requested_authority(), pending.policy, pending.next_offset()) {
                ReplicationDecision::Acknowledge(_) => {
                    pending.complete(PutMessageStatus::PutOk);
                    false
                }
                ReplicationDecision::Wait { .. } => true,
                ReplicationDecision::Reject(_reason) => {
                    warn!("HA group-transfer request was rejected by the canonical decision");
                    pending.complete(PutMessageStatus::FlushSlaveTimeout);
                    false
                }
            }
        });
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

    fn requested_authority(&self) -> Option<rocketmq_store_api::WriteAuthority> {
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
        let mut active = Vec::new();
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
        let service = GroupTransferService::new(reference);
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
        let service = GroupTransferService::new(reference);
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
        let service = GroupTransferService::new(reference);
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
        let tree = ResourceBudgetTree::new(
            "store-overload-test",
            BudgetLimit::new(2, retained_bytes * 2, FullPolicy::Reject),
        )
        .expect("root budget");
        let queue_budget = tree
            .root()
            .child(
                "ha-group-transfer",
                BudgetLimit::new(2, retained_bytes * 2, FullPolicy::Reject)
                    .with_rate(RateLimit::new(4, 4))
                    .with_max_age(Duration::from_secs(30)),
            )
            .expect("queue budget");
        let inner = GroupTransferServiceInner {
            ha_service: reference,
            ack_notify_count: AtomicU64::new(0),
            pending_requests: BudgetedQueue::new(queue_budget),
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
    async fn already_expired_group_transfer_completes_without_deadline_subtraction_panic() {
        let ha_service = new_test_ha_service();
        let reference = GeneralHAServiceReference::new();
        reference.bind(&ha_service).expect("bind general ha service");
        let inner = GroupTransferServiceInner::try_new(reference).expect("group transfer service");
        let (request, mut response) = GroupCommitRequest::with_ack_nums(128, 0, 2);

        inner.put_request(request).await;
        let mut active = Vec::new();
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
        let mut active = Vec::new();
        inner.evaluate_pending(&mut active).await;

        assert_eq!(
            response.wait_for_result_with_timeout().await.expect("rejected status"),
            PutMessageStatus::FlushSlaveTimeout
        );
        assert_eq!(inner.runtime_info().pending_request_count, 0);
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
        let mut active = vec![
            ActiveGroupTransfer::unadmitted(PendingGroupTransfer::new(high_request), policy),
            ActiveGroupTransfer::unadmitted(PendingGroupTransfer::new(low_request), policy),
        ];

        GroupTransferServiceInner::resolve_pending(&mut active, &frontier, Instant::now());

        assert_eq!(active.len(), 1);
        assert_eq!(active[0].next_offset(), 180);
        assert_eq!(
            low_response
                .wait_for_result_with_timeout()
                .await
                .expect("low offset status"),
            PutMessageStatus::PutOk
        );
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
