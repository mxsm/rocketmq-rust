/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
use std::sync::Arc;

use rocketmq_common::common::message::message_single::MessageExtBrokerInner;
use tokio::time;

use crate::{
    base::{
        flush_manager::FlushManager, message_result::AppendMessageResult,
        message_status_enum::PutMessageStatus, store_checkpoint::StoreCheckpoint,
    },
    config::{flush_disk_type::FlushDiskType, message_store_config::MessageStoreConfig},
    consume_queue::mapped_file_queue::MappedFileQueue,
    log_file::flush_manager_impl::group_commit_request::GroupCommitRequest,
};

pub struct DefaultFlushManager {
    group_commit_service: Option<GroupCommitService>,
    flush_real_time_service: Option<FlushRealTimeService>,
    commit_real_time_service: Option<CommitRealTimeService>,
    message_store_config: Arc<MessageStoreConfig>,
    mapped_file_queue: Option<MappedFileQueue>,
}

impl DefaultFlushManager {
    pub fn new(
        message_store_config: Arc<MessageStoreConfig>,
        mapped_file_queue: MappedFileQueue,
        store_checkpoint: Arc<StoreCheckpoint>,
    ) -> Self {
        let (group_commit_service, flush_real_time_service) =
            match message_store_config.flush_disk_type {
                FlushDiskType::SyncFlush => (None, None),
                FlushDiskType::AsyncFlush => (None, None),
            };

        let commit_real_time_service = if message_store_config.transient_store_pool_enable {
            Some(CommitRealTimeService {})
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

impl FlushManager for DefaultFlushManager {
    fn start(&mut self) {
        if let Some(ref mut group_commit_service) = self.group_commit_service {
            group_commit_service.start(self.mapped_file_queue.take().unwrap());
        }
    }

    fn shutdown(&mut self) {}

    fn wake_up_flush(&mut self) {
        unimplemented!()
    }

    fn wake_up_commit(&mut self) {
        unimplemented!()
    }

    async fn handle_disk_flush(
        &mut self,
        result: &AppendMessageResult,
        message_ext: &MessageExtBrokerInner,
    ) -> PutMessageStatus {
        match self.message_store_config.flush_disk_type {
            FlushDiskType::SyncFlush => {
                let commit_request = GroupCommitRequest::new(
                    result.wrote_offset + result.wrote_bytes as i64,
                    self.message_store_config.sync_flush_timeout,
                );
                self.group_commit_service
                    .as_mut()
                    .unwrap()
                    .put_request(commit_request)
                    .await
                    .map_or(PutMessageStatus::FlushDiskTimeout, |request| {
                        request
                            .flush_ok
                            .unwrap_or(PutMessageStatus::FlushDiskTimeout)
                    })
            }
            FlushDiskType::AsyncFlush => PutMessageStatus::PutOk,
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

    fn start(&mut self, mapped_file_queue: MappedFileQueue) {
        let (tx_in, mut rx_in) = tokio::sync::mpsc::channel::<GroupCommitRequest>(1024);
        self.tx_in = Some(tx_in);
        let (tx_out, rx_out) = tokio::sync::mpsc::channel::<GroupCommitRequest>(1024);
        self.rx_out = Some(rx_out);
        tokio::spawn(async move {
            loop {
                match rx_in.recv().await {
                    None => {}
                    Some(mut request) => {
                        let mut flush_ok =
                            mapped_file_queue.get_flushed_where() >= request.next_offset;
                        for i in 0..1000 {
                            if flush_ok {
                                break;
                            }
                            mapped_file_queue.flush(0);
                            flush_ok = mapped_file_queue.get_flushed_where() >= request.next_offset;
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
}

struct FlushRealTimeService {}

struct CommitRealTimeService {}
