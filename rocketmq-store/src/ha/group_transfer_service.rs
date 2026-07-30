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

use std::sync::atomic::AtomicBool;
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
use tokio::sync::Notify;
use tokio::time::timeout;
use tracing::error;
use tracing::warn;

use crate::base::message_status_enum::PutMessageStatus;
use crate::ha::general_ha_service::GeneralHAService;
use crate::ha::general_ha_service::GeneralHAServiceReference;
use crate::ha::ha_service::HAAckedReplicaSnapshot;
use crate::ha::ha_service::HAService;
use crate::log_file::group_commit_request::GroupCommitRequest;
use crate::store_error::HAError;
use crate::store_error::HAResult;
use rocketmq_store_local::ha::replication::has_required_acks;
use rocketmq_store_local::ha::replication::has_required_sync_state_set_acks;
pub(crate) use rocketmq_store_local::ha::replication::GroupTransferRuntimeInfo;

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

    pub fn try_new(ha_service: GeneralHAServiceReference) -> HAResult<Self> {
        let inner = Arc::new(GroupTransferServiceInner::try_new(ha_service)?);
        Ok(GroupTransferService {
            inner: inner.clone(),
            service_manager: ServiceManager::new_arc_legacy_compatibility(inner),
        })
    }

    pub async fn start(&self) -> HAResult<()> {
        self.service_manager.start().await.map_err(|e| {
            error!("Failed to start GroupTransferService: {:?}", e);
            HAError::operation("start group transfer service", e)
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
        if self
            .inner
            .notified
            .1
            .compare_exchange(
                false,
                true,
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
            )
            .is_ok()
        {
            self.inner.notified.0.notify_one();
        }
    }

    pub(crate) fn runtime_info(&self) -> GroupTransferRuntimeInfo {
        self.inner.runtime_info()
    }
}

struct GroupTransferServiceInner {
    ha_service: GeneralHAServiceReference,
    notified: (Arc<Notify>, AtomicBool),
    ack_notify_count: AtomicU64,
    pending_requests: BudgetedQueue<PendingGroupTransfer>,
}

impl GroupTransferServiceInner {
    fn try_new(ha_service: GeneralHAServiceReference) -> HAResult<Self> {
        let process_limit = ProcessMemoryLimit::detect()
            .map_err(|error| HAError::operation("detect HA process memory limit", error))?;
        let managed_bytes = process_limit
            .fraction(1, 16)
            .map_err(|error| HAError::operation("derive HA memory budget", error))?;
        let queue_bytes = usize::try_from((managed_bytes / 2).max(1)).unwrap_or(usize::MAX);
        let request_bytes = std::mem::size_of::<PendingGroupTransfer>().max(1);
        let queue_count = (queue_bytes / request_bytes).clamp(1, 65_536);
        let tree = ResourceBudgetTree::new("store", BudgetLimit::new(queue_count, queue_bytes, FullPolicy::Reject))
            .map_err(|error| HAError::operation("build Store resource budget", error))?;
        let queue_budget = tree
            .root()
            .child(
                "ha-group-transfer",
                BudgetLimit::new(queue_count, queue_bytes, FullPolicy::Reject)
                    .with_rate(RateLimit::new(queue_count as u64, queue_count as u64))
                    .with_max_age(Duration::from_secs(30)),
            )
            .map_err(|error| HAError::operation("build HA group-transfer budget", error))?;
        Ok(GroupTransferServiceInner {
            ha_service,
            notified: (Arc::new(Notify::new()), AtomicBool::new(false)),
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
        if let Err(error) = self
            .pending_requests
            .try_push_data(PendingGroupTransfer::new(request), retained_bytes)
        {
            warn!(error = %error, "HA group-transfer queue rejected a request");
        }
    }

    async fn load_acked_replicas(ha_service: &GeneralHAService) -> Vec<HAAckedReplicaSnapshot> {
        ha_service.snapshot_acked_replicas().await
    }

    async fn do_wait_transfer(&self) {
        let ha_service = self.ha_service.upgrade();

        while let Some(pending) = self.pending_requests.try_pop_budgeted() {
            let (mut pending, _permit, _) = pending.into_parts();
            let request = pending.request_mut();
            let mut transfer_ok = false;
            let deadline = request.get_deadline();
            let all_ack_in_sync_state_set = request.get_ack_nums() == mix_all::ALL_ACK_IN_SYNC_STATE_SET;
            let mut index = 0;
            while !transfer_ok && Instant::now() < deadline {
                if index > 0
                    && timeout(Duration::from_millis(1), self.notified.0.notified())
                        .await
                        .is_ok()
                {
                    let _ = self.notified.1.compare_exchange(
                        true,
                        false,
                        std::sync::atomic::Ordering::SeqCst,
                        std::sync::atomic::Ordering::SeqCst,
                    );
                }
                index += 1;
                let Some(ha_service) = ha_service.as_ref() else {
                    break;
                };
                //handle only one slave ack, ackNums <= 2 means master + 1 slave
                if !all_ack_in_sync_state_set && request.get_ack_nums() <= 2 {
                    transfer_ok = ha_service.get_push_to_slave_max_offset() >= request.get_next_offset();
                    continue;
                }
                if all_ack_in_sync_state_set && ha_service.is_auto_switch_enabled() {
                    if let Some(sync_state_set) = ha_service.sync_state_set() {
                        let acked_replicas = Self::load_acked_replicas(ha_service).await;
                        transfer_ok = has_required_sync_state_set_acks(
                            &sync_state_set,
                            &acked_replicas,
                            request.get_next_offset(),
                        );
                        continue;
                    }
                    transfer_ok = ha_service.in_sync_replicas_nums(request.get_next_offset()) >= request.get_ack_nums();
                } else {
                    let acked_replicas = Self::load_acked_replicas(ha_service).await;
                    transfer_ok = has_required_acks(request.get_ack_nums(), &acked_replicas, request.get_next_offset());
                }
            }
            if !transfer_ok {
                warn!(
                    "transfer message to slave timeout, offset : {}, request acks: {}",
                    request.get_next_offset(),
                    request.get_ack_nums()
                );
            }
            request.wakeup_customer(if transfer_ok {
                PutMessageStatus::PutOk
            } else {
                PutMessageStatus::FlushSlaveTimeout
            });
            pending.complete();
        }
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
        while !context.is_stopped() {
            context.wait_for_running(std::time::Duration::from_millis(10)).await;
            self.on_wait_end().await;
            self.do_wait_transfer().await;
        }
    }

    async fn on_wait_end(&self) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ha::default_ha_service::DefaultHAService;
    use crate::ha::test_support::new_test_message_store;
    use std::collections::HashSet;

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

    #[test]
    fn sync_state_set_ack_requires_all_members() {
        let sync_state_set = HashSet::from([7_i64, 9_i64, 10_i64]);
        let acked_replicas = vec![
            HAAckedReplicaSnapshot {
                slave_broker_id: Some(9),
                slave_ack_offset: 128,
            },
            HAAckedReplicaSnapshot {
                slave_broker_id: Some(10),
                slave_ack_offset: 64,
            },
        ];

        assert!(!has_required_sync_state_set_acks(&sync_state_set, &acked_replicas, 96));
        assert!(has_required_sync_state_set_acks(&sync_state_set, &acked_replicas, 64));
    }

    #[test]
    fn all_ack_in_sync_state_set_requires_controller_members() {
        let request_ack_nums = mix_all::ALL_ACK_IN_SYNC_STATE_SET;
        let sync_state_set = HashSet::from([7_i64, 9_i64, 10_i64]);
        let mut acked_replicas = vec![
            HAAckedReplicaSnapshot {
                slave_broker_id: Some(9),
                slave_ack_offset: 128,
            },
            HAAckedReplicaSnapshot {
                slave_broker_id: Some(10),
                slave_ack_offset: 127,
            },
        ];

        assert!(request_ack_nums < 0);
        assert!(!has_required_sync_state_set_acks(&sync_state_set, &acked_replicas, 128));

        acked_replicas[1].slave_ack_offset = 128;

        assert!(has_required_sync_state_set_acks(&sync_state_set, &acked_replicas, 128));
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
            notified: (Arc::new(Notify::new()), AtomicBool::new(false)),
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
        inner.do_wait_transfer().await;

        assert_eq!(
            response.wait_for_result_with_timeout().await.expect("timeout status"),
            PutMessageStatus::FlushSlaveTimeout
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

    #[test]
    fn sync_state_set_ack_ignores_non_members() {
        let sync_state_set = HashSet::from([7_i64, 9_i64]);
        let acked_replicas = vec![
            HAAckedReplicaSnapshot {
                slave_broker_id: Some(11),
                slave_ack_offset: 256,
            },
            HAAckedReplicaSnapshot {
                slave_broker_id: Some(9),
                slave_ack_offset: 256,
            },
        ];

        assert!(has_required_sync_state_set_acks(&sync_state_set, &acked_replicas, 128));
    }

    #[test]
    fn required_acks_count_master_and_acked_slaves() {
        let acked_replicas = vec![
            HAAckedReplicaSnapshot {
                slave_broker_id: Some(9),
                slave_ack_offset: 32,
            },
            HAAckedReplicaSnapshot {
                slave_broker_id: Some(10),
                slave_ack_offset: 96,
            },
        ];

        assert!(!has_required_acks(3, &acked_replicas, 64));
        assert!(has_required_acks(2, &acked_replicas, 64));
        assert!(has_required_acks(3, &acked_replicas, 32));
    }
}
