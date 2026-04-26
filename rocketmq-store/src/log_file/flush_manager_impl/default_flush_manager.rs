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
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_rust::ArcMut;
use rocketmq_rust::WeakArcMut;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time;
use tokio_util::sync::CancellationToken;

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
                    tx_in: None,
                    shutdown_token: CancellationToken::new(),
                    worker_handle: None,
                }),
                None,
            ),
            FlushDiskType::AsyncFlush => (
                None,
                Some(FlushRealTimeService {
                    message_store_config: message_store_config.clone(),
                    store_checkpoint: store_checkpoint.clone(),
                    notified: Arc::new(Notify::new()),
                    shutdown_token: CancellationToken::new(),
                    worker_handle: None,
                }),
            ),
        };

        let commit_real_time_service = if message_store_config.transient_store_pool_enable {
            Some(CommitRealTimeService {
                message_store_config: message_store_config.clone(),
                store_checkpoint,
                notified: Arc::new(Default::default()),
                flush_manager: None,
                shutdown_token: CancellationToken::new(),
                worker_handle: None,
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
        if let Some(mapped_file_queue) = self.mapped_file_queue.as_ref() {
            if self.message_store_config.transient_store_pool_enable {
                mapped_file_queue.commit(0);
            }
            mapped_file_queue.flush(0);
        }

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
                    let (commit_request, flush_ok_receiver) = GroupCommitRequest::new(
                        result.wrote_offset + result.wrote_bytes as i64,
                        self.message_store_config.sync_flush_timeout,
                    );

                    time::timeout(
                        time::Duration::from_millis(self.message_store_config.sync_flush_timeout),
                        async {
                            let Some(group_commit_service) = self.group_commit_service.as_mut() else {
                                return PutMessageStatus::FlushDiskTimeout;
                            };
                            if !group_commit_service.put_request(commit_request).await {
                                return PutMessageStatus::FlushDiskTimeout;
                            }
                            flush_ok_receiver.await.unwrap_or(PutMessageStatus::FlushDiskTimeout)
                        },
                    )
                    .await
                    .unwrap_or(PutMessageStatus::FlushDiskTimeout)
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
    tx_in: Option<tokio::sync::mpsc::Sender<GroupCommitRequest>>,
    shutdown_token: CancellationToken,
    worker_handle: Option<JoinHandle<()>>,
}

impl GroupCommitService {
    pub async fn put_request(&mut self, request: GroupCommitRequest) -> bool {
        if self.shutdown_token.is_cancelled() {
            return false;
        }

        let Some(tx_in) = self.tx_in.as_ref() else {
            return false;
        };
        tokio::select! {
            result = tx_in.send(request) => result.is_ok(),
            _ = self.shutdown_token.cancelled() => false,
        }
    }

    fn start(&mut self, mapped_file_queue: ArcMut<MappedFileQueue>) {
        if self.worker_handle.is_some() {
            return;
        }

        self.shutdown_token = CancellationToken::new();
        let (tx_in, mut rx_in) = tokio::sync::mpsc::channel::<GroupCommitRequest>(1024);
        self.tx_in = Some(tx_in);
        let shutdown_token = self.shutdown_token.clone();
        let store_checkpoint = self.store_checkpoint.clone();
        self.worker_handle = Some(tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown_token.cancelled() => {
                        let mut remaining = Vec::new();
                        while let Ok(request) = rx_in.try_recv() {
                            remaining.push(request);
                        }
                        if !remaining.is_empty() {
                            mapped_file_queue.flush(0);
                            complete_group_commit_batch(remaining, mapped_file_queue.get_flushed_where());
                        }
                        break;
                    }
                    maybe_request = rx_in.recv() => match maybe_request {
                    None => break,
                    Some(first_request) => {
                        let mut requests = vec![first_request];
                        while let Ok(request) = rx_in.try_recv() {
                            requests.push(request);
                        }

                        let target_offset = requests.iter().map(|request| request.next_offset).max().unwrap_or(0);
                        let mut flush_ok = mapped_file_queue.get_flushed_where() >= target_offset;
                        for _ in 0..1000 {
                            if flush_ok {
                                break;
                            }
                            mapped_file_queue.flush(0);
                            flush_ok = mapped_file_queue.get_flushed_where() >= target_offset;
                            if flush_ok {
                                break;
                            }
                            time::sleep(time::Duration::from_millis(1)).await;
                        }

                        let flushed_where = mapped_file_queue.get_flushed_where();
                        let store_timestamp = mapped_file_queue.get_store_timestamp();
                        if store_timestamp > 0 {
                            store_checkpoint.set_physic_msg_timestamp(store_timestamp);
                        }
                        complete_group_commit_batch(requests, flushed_where);
                    }
                    }
                }
            }
        }));
    }

    pub fn wakeup(&self) {}

    pub fn shutdown(&mut self) {
        self.shutdown_token.cancel();
        self.tx_in.take();
        if let Some(handle) = self.worker_handle.take() {
            handle.abort();
        }
    }
}

fn complete_group_commit_batch(requests: Vec<GroupCommitRequest>, flushed_where: i64) {
    for request in requests {
        let status = if flushed_where >= request.next_offset {
            PutMessageStatus::PutOk
        } else {
            PutMessageStatus::FlushDiskTimeout
        };
        request.complete(status);
    }
}

struct FlushRealTimeService {
    message_store_config: Arc<MessageStoreConfig>,
    store_checkpoint: Arc<StoreCheckpoint>,
    notified: Arc<Notify>,
    shutdown_token: CancellationToken,
    worker_handle: Option<JoinHandle<()>>,
}

impl FlushRealTimeService {
    fn start(&mut self, mapped_file_queue: ArcMut<MappedFileQueue>) {
        if self.worker_handle.is_some() {
            return;
        }

        self.shutdown_token = CancellationToken::new();
        let message_store_config = self.message_store_config.clone();
        let store_checkpoint = self.store_checkpoint.clone();
        let notified = self.notified.clone();
        let shutdown_token = self.shutdown_token.clone();
        self.worker_handle = Some(tokio::spawn(async move {
            let mut last_flush_timestamp = 0;
            loop {
                if shutdown_token.is_cancelled() {
                    break;
                }

                let flush_commit_log_timed = message_store_config.flush_commit_log_timed;
                let interval = message_store_config.flush_interval_commit_log;
                let mut flush_physic_queue_least_pages = message_store_config.flush_commit_log_least_pages;
                let flush_physic_queue_thorough_interval = message_store_config.flush_commit_log_thorough_interval;
                //let mut print_flush_progress = false;

                let current_time_millis = current_millis();
                if current_time_millis >= last_flush_timestamp + flush_physic_queue_thorough_interval as u64 {
                    last_flush_timestamp = current_time_millis;
                    flush_physic_queue_least_pages = 0;
                }
                if flush_commit_log_timed {
                    tokio::select! {
                        _ = shutdown_token.cancelled() => break,
                        _ = time::sleep(time::Duration::from_millis(interval as u64)) => {}
                    }
                } else {
                    tokio::select! {
                        _ = shutdown_token.cancelled() => break,
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
            mapped_file_queue.flush(0);
            let store_timestamp = mapped_file_queue.get_store_timestamp();
            if store_timestamp > 0 {
                store_checkpoint.set_physic_msg_timestamp(store_timestamp);
            }
        }));
    }

    pub fn wakeup(&self) {
        if !self.message_store_config.flush_commit_log_timed {
            self.notified.notify_one();
        }
    }

    pub fn shutdown(&mut self) {
        self.shutdown_token.cancel();
        self.notified.notify_waiters();
        if let Some(handle) = self.worker_handle.take() {
            handle.abort();
        }
    }
}

pub(crate) struct CommitRealTimeService {
    message_store_config: Arc<MessageStoreConfig>,
    store_checkpoint: Arc<StoreCheckpoint>,
    notified: Arc<Notify>,
    flush_manager: Option<WeakArcMut<DefaultFlushManager>>,
    shutdown_token: CancellationToken,
    worker_handle: Option<JoinHandle<()>>,
}

impl CommitRealTimeService {
    pub fn wakeup(&self) {
        self.notified.notify_one();
    }

    fn start(&mut self, mapped_file_queue: ArcMut<MappedFileQueue>) {
        if self.worker_handle.is_some() {
            return;
        }

        self.shutdown_token = CancellationToken::new();
        let message_store_config = self.message_store_config.clone();
        let store_checkpoint = self.store_checkpoint.clone();
        let notified = self.notified.clone();
        let flush_manager = self.flush_manager.clone();
        let shutdown_token = self.shutdown_token.clone();
        self.worker_handle = Some(tokio::spawn(async move {
            let mut last_commit_timestamp = 0;
            loop {
                if shutdown_token.is_cancelled() {
                    break;
                }

                let interval = message_store_config.commit_interval_commit_log;
                let mut commit_data_least_pages = message_store_config.commit_commit_log_least_pages;
                let commit_data_thorough_interval = message_store_config.commit_commit_log_thorough_interval;
                //let mut print_flush_progress = false;

                let begin = current_millis();
                if begin >= last_commit_timestamp + commit_data_thorough_interval {
                    last_commit_timestamp = begin;
                    commit_data_least_pages = 0;
                }

                let result = mapped_file_queue.commit(commit_data_least_pages);
                if !result {
                    last_commit_timestamp = current_millis();
                    if let Some(flush_manager) = flush_manager.as_ref().unwrap().upgrade() {
                        flush_manager.wake_up_flush();
                    }
                }

                tokio::select! {
                    _ = shutdown_token.cancelled() => break,
                    _ = notified.notified() => {}
                    _ = tokio::time::sleep(std::time::Duration::from_millis(interval)) => {}
                }
            }
            let result = mapped_file_queue.commit(0);
            if !result {
                let store_timestamp = mapped_file_queue.get_store_timestamp();
                if store_timestamp > 0 {
                    store_checkpoint.set_physic_msg_timestamp(store_timestamp);
                }
            }
        }));
    }

    pub fn shutdown(&mut self) {
        self.shutdown_token.cancel();
        self.notified.notify_waiters();
        if let Some(handle) = self.worker_handle.take() {
            handle.abort();
        }
    }

    pub fn set_flush_manager(&mut self, flush_manager: WeakArcMut<DefaultFlushManager>) {
        self.flush_manager = Some(flush_manager);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn complete_group_commit_batch_marks_each_request_by_final_flushed_offset() {
        let (request_64, mut response_64) = GroupCommitRequest::new(64, 5_000);
        let (request_96, mut response_96) = GroupCommitRequest::new(96, 5_000);
        let requests = vec![request_64, request_96];

        complete_group_commit_batch(requests, 80);

        assert_eq!(response_64.try_recv(), Ok(PutMessageStatus::PutOk));
        assert_eq!(response_96.try_recv(), Ok(PutMessageStatus::FlushDiskTimeout));
    }
}
