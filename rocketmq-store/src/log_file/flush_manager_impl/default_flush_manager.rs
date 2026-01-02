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

use std::sync::Arc;

use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_rust::ArcMut;
use rocketmq_rust::WeakArcMut;
use tokio::sync::Notify;
use tokio::time;

use crate::base::flush_manager::FlushManager;
use crate::base::message_result::AppendMessageResult;
use crate::base::message_status_enum::PutMessageStatus;
use crate::base::store_checkpoint::StoreCheckpoint;
use crate::config::flush_disk_type::FlushDiskType;
use crate::config::message_store_config::MessageStoreConfig;
use crate::consume_queue::mapped_file_queue::MappedFileQueue;
use crate::log_file::flush_manager_impl::group_commit_request::GroupCommitRequest;

pub struct DefaultFlushManager {
    group_commit_service: Option<GroupCommitService>,
    flush_real_time_service: Option<FlushRealTimeService>,
    commit_real_time_service: Option<CommitRealTimeService>,
    message_store_config: Arc<MessageStoreConfig>,
    mapped_file_queue: Option<ArcMut<MappedFileQueue>>,
}

impl DefaultFlushManager {
    pub fn new(
        message_store_config: Arc<MessageStoreConfig>,
        mapped_file_queue: ArcMut<MappedFileQueue>,
        store_checkpoint: Arc<StoreCheckpoint>,
    ) -> Self {
        let (group_commit_service, flush_real_time_service) = match message_store_config.flush_disk_type {
            FlushDiskType::SyncFlush => (
                Some(GroupCommitService {
                    store_checkpoint: store_checkpoint.clone(),
                    rx_out: None,
                    tx_in: None,
                }),
                None,
            ),
            FlushDiskType::AsyncFlush => (
                None,
                Some(FlushRealTimeService {
                    message_store_config: message_store_config.clone(),
                    store_checkpoint: store_checkpoint.clone(),
                    notified: Arc::new(Notify::new()),
                }),
            ),
        };

        let commit_real_time_service = if message_store_config.transient_store_pool_enable {
            Some(CommitRealTimeService {
                message_store_config: message_store_config.clone(),
                store_checkpoint,
                notified: Arc::new(Default::default()),
                flush_manager: None,
            })
        } else {
            None
        };

        DefaultFlushManager {
            group_commit_service,
            flush_real_time_service,
            message_store_config,
            commit_real_time_service,
            mapped_file_queue: Some(mapped_file_queue),
        }
    }
}

impl DefaultFlushManager {
    pub(crate) fn commit_real_time_service(&self) -> Option<&CommitRealTimeService> {
        self.commit_real_time_service.as_ref()
    }

    pub(crate) fn commit_real_time_service_mut(&mut self) -> Option<&mut CommitRealTimeService> {
        self.commit_real_time_service.as_mut()
    }
}

impl FlushManager for DefaultFlushManager {
    fn start(&mut self) {
        if let Some(ref mut group_commit_service) = self.group_commit_service {
            group_commit_service.start(self.mapped_file_queue.clone().unwrap());
        }
        if let Some(ref mut flush_real_time_service) = self.flush_real_time_service {
            flush_real_time_service.start(self.mapped_file_queue.clone().unwrap());
        }

        if self.message_store_config.transient_store_pool_enable {
            if let Some(ref mut commit_real_time_service) = self.commit_real_time_service {
                commit_real_time_service.start(self.mapped_file_queue.clone().unwrap());
            }
        }
    }

    fn shutdown(&mut self) {
        if let Some(ref mut group_commit_service) = self.group_commit_service {
            group_commit_service.shutdown();
        }
        if let Some(ref mut flush_real_time_service) = self.flush_real_time_service {
            flush_real_time_service.shutdown();
        }
        if let Some(ref mut commit_real_time_service) = self.commit_real_time_service {
            commit_real_time_service.shutdown();
        }
    }

    fn wake_up_flush(&self) {
        if let Some(ref group_commit_service) = self.group_commit_service {
            group_commit_service.wakeup();
        }
        if let Some(ref flush_real_time_service) = self.flush_real_time_service {
            flush_real_time_service.wakeup();
        }
    }

    fn wake_up_commit(&self) {
        if let Some(ref commit_real_time_service) = self.commit_real_time_service {
            commit_real_time_service.wakeup();
        }
    }

    async fn handle_disk_flush(
        &mut self,
        result: &AppendMessageResult,
        message_ext: &MessageExtBrokerInner,
    ) -> PutMessageStatus {
        match self.message_store_config.flush_disk_type {
            FlushDiskType::SyncFlush => {
                if message_ext.is_wait_store_msg_ok() {
                    let commit_request = GroupCommitRequest::new(
                        result.wrote_offset + result.wrote_bytes as i64,
                        self.message_store_config.sync_flush_timeout,
                    );

                    time::timeout(
                        time::Duration::from_millis(self.message_store_config.sync_flush_timeout),
                        self.group_commit_service.as_mut().unwrap().put_request(commit_request),
                    )
                    .await
                    .map_or(PutMessageStatus::FlushDiskTimeout, |request| {
                        request.map_or(PutMessageStatus::FlushDiskTimeout, |request| {
                            request.flush_ok.unwrap_or(PutMessageStatus::FlushDiskTimeout)
                        })
                    })
                } else {
                    self.group_commit_service.as_ref().unwrap().wakeup();
                    PutMessageStatus::PutOk
                }
            }
            FlushDiskType::AsyncFlush => {
                if self.message_store_config.transient_store_pool_enable {
                    self.commit_real_time_service.as_ref().unwrap().wakeup();
                } else {
                    self.flush_real_time_service.as_ref().unwrap().wakeup();
                }
                PutMessageStatus::PutOk
            }
        }
    }
}

struct GroupCommitService {
    store_checkpoint: Arc<StoreCheckpoint>,
    rx_out: Option<tokio::sync::mpsc::Receiver<GroupCommitRequest>>,
    tx_in: Option<tokio::sync::mpsc::Sender<GroupCommitRequest>>,
}

impl GroupCommitService {
    pub async fn put_request(&mut self, request: GroupCommitRequest) -> Option<GroupCommitRequest> {
        if let Some(ref tx_in) = self.tx_in {
            let _ = tx_in.send(request).await;
        }
        match self.rx_out {
            None => None,
            Some(ref mut rx_out) => rx_out.recv().await,
        }
    }

    fn start(&mut self, mapped_file_queue: ArcMut<MappedFileQueue>) {
        let (tx_in, mut rx_in) = tokio::sync::mpsc::channel::<GroupCommitRequest>(1024);
        self.tx_in = Some(tx_in);
        let (tx_out, rx_out) = tokio::sync::mpsc::channel::<GroupCommitRequest>(1024);
        self.rx_out = Some(rx_out);
        tokio::spawn(async move {
            loop {
                match rx_in.recv().await {
                    None => {}
                    Some(mut request) => {
                        let mut flush_ok = mapped_file_queue.get_flushed_where() >= request.next_offset;
                        for i in 0..1000 {
                            if flush_ok {
                                break;
                            }
                            mapped_file_queue.flush(0);
                            flush_ok = mapped_file_queue.get_flushed_where() >= request.next_offset;
                            if flush_ok {
                                break;
                            }
                            time::sleep(time::Duration::from_millis(1)).await;
                        }
                        request.flush_ok = Some(if flush_ok {
                            PutMessageStatus::PutOk
                        } else {
                            PutMessageStatus::FlushDiskTimeout
                        });
                        let _ = tx_out.send(request).await;
                    }
                }
            }
        });
    }

    pub fn wakeup(&self) {}

    pub fn shutdown(&mut self) {}
}

struct FlushRealTimeService {
    message_store_config: Arc<MessageStoreConfig>,
    store_checkpoint: Arc<StoreCheckpoint>,
    notified: Arc<Notify>,
}

impl FlushRealTimeService {
    fn start(&mut self, mapped_file_queue: ArcMut<MappedFileQueue>) {
        let message_store_config = self.message_store_config.clone();
        let store_checkpoint = self.store_checkpoint.clone();
        let notified = self.notified.clone();
        tokio::spawn(async move {
            let mut last_flush_timestamp = 0;
            loop {
                let flush_commit_log_timed = message_store_config.flush_commit_log_timed;
                let interval = message_store_config.flush_interval_commit_log;
                let mut flush_physic_queue_least_pages = message_store_config.flush_commit_log_least_pages;
                let flush_physic_queue_thorough_interval = message_store_config.flush_commit_log_thorough_interval;
                //let mut print_flush_progress = false;

                let current_time_millis = get_current_millis();
                if current_time_millis >= last_flush_timestamp + flush_physic_queue_thorough_interval as u64 {
                    last_flush_timestamp = current_time_millis;
                    flush_physic_queue_least_pages = 0;
                }
                if flush_commit_log_timed {
                    time::sleep(time::Duration::from_millis(interval as u64)).await;
                } else {
                    tokio::select! {
                        _ = notified.notified() => {}
                        _ = tokio::time::sleep(std::time::Duration::from_millis(interval as u64)) => {}
                    }
                }

                mapped_file_queue.flush(flush_physic_queue_least_pages);
                let store_timestamp = mapped_file_queue.get_store_timestamp();
                if store_timestamp > 0 {
                    store_checkpoint.set_physic_msg_timestamp(store_timestamp);
                }
            }
        });
    }

    pub fn wakeup(&self) {
        if !self.message_store_config.flush_commit_log_timed {
            let notified = self.notified.clone();
            tokio::spawn(async move {
                notified.notify_one();
            });
        }
    }

    pub fn shutdown(&mut self) {}
}

pub(crate) struct CommitRealTimeService {
    message_store_config: Arc<MessageStoreConfig>,
    store_checkpoint: Arc<StoreCheckpoint>,
    notified: Arc<Notify>,
    flush_manager: Option<WeakArcMut<DefaultFlushManager>>,
}

impl CommitRealTimeService {
    pub fn wakeup(&self) {
        let notified = self.notified.clone();
        tokio::spawn(async move {
            notified.notify_one();
        });
    }

    fn start(&mut self, mapped_file_queue: ArcMut<MappedFileQueue>) {
        let message_store_config = self.message_store_config.clone();
        let store_checkpoint = self.store_checkpoint.clone();
        let notified = self.notified.clone();
        let flush_manager = self.flush_manager.clone();
        tokio::spawn(async move {
            let mut last_commit_timestamp = 0;
            loop {
                let interval = message_store_config.commit_interval_commit_log;
                let mut commit_data_least_pages = message_store_config.commit_commit_log_least_pages;
                let commit_data_thorough_interval = message_store_config.commit_commit_log_thorough_interval;
                //let mut print_flush_progress = false;

                let begin = get_current_millis();
                if begin >= last_commit_timestamp + commit_data_thorough_interval {
                    last_commit_timestamp = begin;
                    commit_data_least_pages = 0;
                }

                let result = mapped_file_queue.commit(commit_data_least_pages);
                if !result {
                    last_commit_timestamp = get_current_millis();
                    if let Some(flush_manager) = flush_manager.as_ref().unwrap().upgrade() {
                        flush_manager.wake_up_flush();
                    }
                }

                tokio::select! {
                    _ = notified.notified() => {}
                    _ = tokio::time::sleep(std::time::Duration::from_millis(interval)) => {}
                }
            }
        });
    }

    pub fn shutdown(&mut self) {}

    pub fn set_flush_manager(&mut self, flush_manager: WeakArcMut<DefaultFlushManager>) {
        self.flush_manager = Some(flush_manager);
    }
}
