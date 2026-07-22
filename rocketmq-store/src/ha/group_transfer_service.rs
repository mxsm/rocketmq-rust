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

use std::collections::LinkedList;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use rocketmq_common::common::mix_all;
use rocketmq_runtime::task::service_task::ServiceContext;
use rocketmq_runtime::task::service_task::ServiceTask;
use rocketmq_runtime::task::ServiceManager;
use tokio::sync::Mutex;
use tokio::sync::Notify;
use tokio::time::timeout;
use tracing::error;
use tracing::warn;

use crate::base::message_status_enum::PutMessageStatus;
use crate::ha::general_ha_service::GeneralHAService;
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
    pub fn new(ha_service: GeneralHAService) -> Self {
        let inner = Arc::new(GroupTransferServiceInner::new(ha_service));
        GroupTransferService {
            inner: inner.clone(),
            service_manager: ServiceManager::new_arc(inner),
        }
    }

    pub async fn start(&self) -> HAResult<()> {
        self.service_manager.start().await.map_err(|e| {
            error!("Failed to start GroupTransferService: {:?}", e);
            HAError::Service(e.to_string())
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
    ha_service: GeneralHAService,
    notified: (Arc<Notify>, AtomicBool),
    ack_notify_count: AtomicU64,
    requests_write: Arc<Mutex<LinkedList<GroupCommitRequest>>>,
    requests_read: Arc<Mutex<LinkedList<GroupCommitRequest>>>,
}

impl GroupTransferServiceInner {
    fn new(ha_service: GeneralHAService) -> Self {
        GroupTransferServiceInner {
            ha_service,
            notified: (Arc::new(Notify::new()), AtomicBool::new(false)),
            ack_notify_count: AtomicU64::new(0),
            requests_write: Arc::new(Mutex::new(LinkedList::new())),
            requests_read: Arc::new(Mutex::new(LinkedList::new())),
        }
    }

    #[inline]
    fn record_ack_notify(&self) {
        self.ack_notify_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    fn runtime_info(&self) -> GroupTransferRuntimeInfo {
        let now = Instant::now();
        let (write_count, write_oldest_wait_millis) = pending_request_snapshot(&self.requests_write, now);
        let (read_count, read_oldest_wait_millis) = pending_request_snapshot(&self.requests_read, now);

        GroupTransferRuntimeInfo {
            pending_request_count: write_count.saturating_add(read_count),
            pending_request_oldest_wait_millis: write_oldest_wait_millis.max(read_oldest_wait_millis),
            ack_notify_count: self.ack_notify_count.load(std::sync::atomic::Ordering::Relaxed),
        }
    }

    #[inline]
    async fn put_request(&self, request: GroupCommitRequest) {
        let mut write_requests = self.requests_write.lock().await;
        write_requests.push_back(request);
    }

    #[inline]
    async fn swap_requests(&self) {
        let mut write_requests = self.requests_write.lock().await;
        let mut read_requests = self.requests_read.lock().await;
        std::mem::swap(&mut *read_requests, &mut *write_requests);
    }

    async fn load_acked_replicas(&self) -> Vec<HAAckedReplicaSnapshot> {
        self.ha_service.snapshot_acked_replicas().await
    }

    async fn do_wait_transfer(&self) {
        let mut read_requests = {
            let mut pending_requests = self.requests_read.lock().await;
            if pending_requests.is_empty() {
                return;
            }
            std::mem::take(&mut *pending_requests)
        };

        if read_requests.is_empty() {
            return;
        }

        for request in read_requests.iter_mut() {
            let mut transfer_ok = false;
            let deadline = request.get_deadline();
            let all_ack_in_sync_state_set = request.get_ack_nums() == mix_all::ALL_ACK_IN_SYNC_STATE_SET;
            let mut index = 0;
            while !transfer_ok && deadline - Instant::now() > Duration::ZERO {
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
                //handle only one slave ack, ackNums <= 2 means master + 1 slave
                if !all_ack_in_sync_state_set && request.get_ack_nums() <= 2 {
                    transfer_ok = self.ha_service.get_push_to_slave_max_offset() >= request.get_next_offset();
                    continue;
                }
                if all_ack_in_sync_state_set && self.ha_service.is_auto_switch_enabled() {
                    if let Some(sync_state_set) = self.ha_service.sync_state_set() {
                        let acked_replicas = self.load_acked_replicas().await;
                        transfer_ok = has_required_sync_state_set_acks(
                            &sync_state_set,
                            &acked_replicas,
                            request.get_next_offset(),
                        );
                        continue;
                    }
                    transfer_ok =
                        self.ha_service.in_sync_replicas_nums(request.get_next_offset()) >= request.get_ack_nums();
                } else {
                    let acked_replicas = self.load_acked_replicas().await;
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
        }
    }
}

fn pending_request_snapshot(requests: &Arc<Mutex<LinkedList<GroupCommitRequest>>>, now: Instant) -> (u64, u64) {
    let Ok(requests) = requests.try_lock() else {
        return (0, 0);
    };
    let count = requests.len() as u64;
    let oldest_wait_millis = requests
        .iter()
        .map(|request| now.saturating_duration_since(request.created_at()).as_millis())
        .max()
        .and_then(|millis| u64::try_from(millis).ok())
        .unwrap_or(0);
    (count, oldest_wait_millis)
}

impl ServiceTask for GroupTransferServiceInner {
    fn get_service_name(&self) -> String {
        "GroupTransferService".to_string()
    }

    async fn run(&self, context: &ServiceContext) {
        while !context.is_stopped() {
            context.wait_for_running(std::time::Duration::from_millis(10)).await;
            self.on_wait_end().await;
            self.do_wait_transfer().await;
        }
    }

    async fn on_wait_end(&self) {
        self.swap_requests().await;
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use cheetah_string::CheetahString;
    use dashmap::DashMap;
    use rocketmq_common::common::broker::broker_config::BrokerConfig;
    use rocketmq_common::common::config::TopicConfig;
    use rocketmq_rust::ArcMut;

    use super::*;
    use crate::config::message_store_config::MessageStoreConfig;
    use crate::ha::default_ha_service::DefaultHAService;
    use crate::message_store::local_file_message_store::LocalFileMessageStore;

    fn new_test_ha_service() -> GeneralHAService {
        let temp_root = tempfile::tempdir().expect("create temp root dir");
        let mut store = ArcMut::new(LocalFileMessageStore::new(
            Arc::new(MessageStoreConfig {
                store_path_root_dir: temp_root.path().to_string_lossy().into_owned().into(),
                ..MessageStoreConfig::default()
            }),
            Arc::new(BrokerConfig::default()),
            Arc::new(DashMap::<CheetahString, Arc<TopicConfig>>::new()),
            None,
            false,
        ));
        let store_clone = store.clone();
        store.set_message_store_arc(store_clone);
        GeneralHAService::new_with_default_ha_service(ArcMut::new(DefaultHAService::new(
            store.ha_replica_store_handle(),
        )))
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
        let service = GroupTransferService::new(new_test_ha_service());
        let (request, _response) = GroupCommitRequest::with_ack_nums(128, 5_000, 2);

        service.put_request(request).await;
        service.notify_transfer_some();
        service.notify_transfer_some();

        let runtime_info = service.runtime_info();
        assert_eq!(runtime_info.pending_request_count, 1);
        assert!(runtime_info.pending_request_oldest_wait_millis < 5_000);
        assert_eq!(runtime_info.ack_notify_count, 2);
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
