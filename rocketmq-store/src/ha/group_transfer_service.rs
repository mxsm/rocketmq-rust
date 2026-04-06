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
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use rocketmq_common::common::mix_all;
use rocketmq_rust::task::service_task::ServiceContext;
use rocketmq_rust::task::service_task::ServiceTask;
use rocketmq_rust::task::ServiceManager;
use tokio::sync::Mutex;
use tokio::sync::Notify;
use tokio::time::timeout;
use tracing::error;
use tracing::warn;

use crate::base::message_status_enum::PutMessageStatus;
use crate::ha::general_ha_service::GeneralHAService;
use crate::ha::general_ha_service::HAAckedReplicaSnapshot;
use crate::ha::ha_connection::HAConnection;
use crate::ha::ha_service::HAService;
use crate::log_file::group_commit_request::GroupCommitRequest;
use crate::store_error::HAError;
use crate::store_error::HAResult;

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

    pub async fn start(&mut self) -> HAResult<()> {
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
}

struct GroupTransferServiceInner {
    ha_service: GeneralHAService,
    notified: (Arc<Notify>, AtomicBool),
    requests_write: Arc<Mutex<LinkedList<GroupCommitRequest>>>,
    requests_read: Arc<Mutex<LinkedList<GroupCommitRequest>>>,
}

impl GroupTransferServiceInner {
    fn new(ha_service: GeneralHAService) -> Self {
        GroupTransferServiceInner {
            ha_service,
            notified: (Arc::new(Notify::new()), AtomicBool::new(false)),
            requests_write: Arc::new(Mutex::new(LinkedList::new())),
            requests_read: Arc::new(Mutex::new(LinkedList::new())),
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
        if let Some(acked_replicas) = self.ha_service.try_snapshot_acked_replicas() {
            return acked_replicas;
        }

        self.ha_service
            .get_connection_list()
            .await
            .into_iter()
            .map(|connection| HAAckedReplicaSnapshot {
                slave_broker_id: connection.slave_broker_id(),
                slave_ack_offset: connection.get_slave_ack_offset(),
            })
            .collect()
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

fn has_required_sync_state_set_acks(
    sync_state_set: &std::collections::HashSet<i64>,
    acked_replicas: &[HAAckedReplicaSnapshot],
    next_offset: i64,
) -> bool {
    if sync_state_set.len() <= 1 {
        return true;
    }

    let mut ack_nums = 1;
    for replica in acked_replicas {
        if replica
            .slave_broker_id
            .is_some_and(|slave_broker_id| sync_state_set.contains(&slave_broker_id))
            && replica.slave_ack_offset >= next_offset
        {
            ack_nums += 1;
        }
        if ack_nums >= sync_state_set.len() {
            return true;
        }
    }

    false
}

fn has_required_acks(required_acks: i32, acked_replicas: &[HAAckedReplicaSnapshot], next_offset: i64) -> bool {
    if required_acks <= 1 {
        return true;
    }

    let mut ack_nums = 1;
    for replica in acked_replicas {
        if replica.slave_ack_offset >= next_offset {
            ack_nums += 1;
        }
        if ack_nums >= required_acks {
            return true;
        }
    }

    false
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

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
