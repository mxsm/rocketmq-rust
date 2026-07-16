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
use std::time::Duration;

use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_runtime::TaskGroup;
use rocketmq_rust::ArcMut;
use rocketmq_rust::WeakArcMut;
use rocketmq_store_local::flush::group_commit::run_group_commit_worker;
use rocketmq_store_local::flush::group_commit::GroupCommitStatus;
use rocketmq_store_local::flush::group_commit::GroupCommitWorkerConfig;
use rocketmq_store_local::flush::group_commit::GroupCommitWorkerPorts;
use rocketmq_store_local::flush::group_commit::SyncFlushStats;
use rocketmq_store_local::flush::group_commit::GROUP_COMMIT_CHANNEL_CAPACITY;
use tokio::sync::Notify;
use tokio::time;
use tokio_util::sync::CancellationToken;
use tracing::debug;
use tracing::warn;

use crate::base::flush_manager::FlushManager;
use crate::base::flush_manager::SyncFlushRuntimeInfo;
use crate::base::message_result::AppendMessageResult;
use crate::base::message_status_enum::PutMessageStatus;
use crate::base::message_store::StoreHealthRecorder;
use crate::base::store_checkpoint::StoreCheckpoint;
use crate::config::flush_disk_type::FlushDiskType;
use crate::config::message_store_config::MessageStoreConfig;
use crate::consume_queue::mapped_file_queue::FlushProgress;
use crate::consume_queue::mapped_file_queue::MappedFileQueue;
use crate::log_file::flush_manager_impl::group_commit_request::GroupCommitRequest;
use crate::store_error::StoreError;

pub struct DefaultFlushManager {
    group_commit_service: Option<GroupCommitService>,
    flush_real_time_service: Option<FlushRealTimeService>,
    commit_real_time_service: Option<CommitRealTimeService>,
    message_store_config: Arc<MessageStoreConfig>,
    mapped_file_queue: Option<ArcMut<MappedFileQueue>>,
    sync_flush_stats: SyncFlushStats,
    store_health_recorder: StoreHealthRecorder,
}

impl DefaultFlushManager {
    pub fn new(
        message_store_config: Arc<MessageStoreConfig>,
        mapped_file_queue: ArcMut<MappedFileQueue>,
        store_checkpoint: Arc<StoreCheckpoint>,
    ) -> Self {
        let store_health_recorder =
            StoreHealthRecorder::new(Arc::new(crate::store::running_flags::RunningFlags::new()));
        let sync_flush_stats = SyncFlushStats::default();
        let (group_commit_service, flush_real_time_service) = match message_store_config.flush_disk_type {
            FlushDiskType::SyncFlush => (
                Some(GroupCommitService {
                    store_checkpoint: store_checkpoint.clone(),
                    notified: Arc::new(Notify::new()),
                    tx_in: None,
                    shutdown_token: CancellationToken::new(),
                    worker_group: None,
                    sync_flush_stats: sync_flush_stats.clone(),
                    store_health_recorder: store_health_recorder.clone(),
                    forced_flush_error: None,
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
                    worker_group: None,
                    store_health_recorder: store_health_recorder.clone(),
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
                worker_group: None,
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
            sync_flush_stats,
            store_health_recorder,
        }
    }

    pub(crate) fn set_store_health_recorder(&mut self, store_health_recorder: StoreHealthRecorder) {
        if let Some(group_commit_service) = self.group_commit_service.as_mut() {
            group_commit_service.store_health_recorder = store_health_recorder.clone();
        }
        if let Some(flush_real_time_service) = self.flush_real_time_service.as_mut() {
            flush_real_time_service.store_health_recorder = store_health_recorder.clone();
        }
        self.store_health_recorder = store_health_recorder;
    }
}

impl DefaultFlushManager {
    pub fn sync_flush_runtime_info(&self) -> SyncFlushRuntimeInfo {
        self.sync_flush_stats.snapshot()
    }

    pub(crate) fn commit_real_time_service(&self) -> Option<&CommitRealTimeService> {
        self.commit_real_time_service.as_ref()
    }

    pub(crate) fn commit_real_time_service_mut(&mut self) -> Option<&mut CommitRealTimeService> {
        self.commit_real_time_service.as_mut()
    }

    pub(crate) async fn shutdown_gracefully(&mut self) -> Result<FlushProgress, StoreError> {
        if let Some(ref mut group_commit_service) = self.group_commit_service {
            group_commit_service.shutdown_gracefully().await;
        }
        if let Some(ref mut flush_real_time_service) = self.flush_real_time_service {
            flush_real_time_service.shutdown_gracefully().await;
        }
        if let Some(ref mut commit_real_time_service) = self.commit_real_time_service {
            commit_real_time_service.shutdown_gracefully().await;
        }

        self.try_flush_before_shutdown()
    }

    fn try_flush_before_shutdown(&self) -> Result<FlushProgress, StoreError> {
        let Some(mapped_file_queue) = self.mapped_file_queue.as_ref() else {
            let error = StoreError::InvalidState(String::from("flush manager mapped file queue is not initialized"));
            self.store_health_recorder.record_flush_failure(&error);
            return Err(error);
        };
        if self.message_store_config.transient_store_pool_enable {
            mapped_file_queue.commit(0);
        }
        mapped_file_queue.try_flush(0).map_err(|error| {
            let error = StoreError::mapped_file(error);
            self.store_health_recorder.record_flush_failure(&error);
            error
        })
    }
}

impl FlushManager for DefaultFlushManager {
    fn start(&mut self) {
        let Some(mapped_file_queue) = self.mapped_file_queue.clone() else {
            warn!("DefaultFlushManager cannot start because mapped file queue is not initialized");
            return;
        };

        if let Some(ref mut group_commit_service) = self.group_commit_service {
            group_commit_service.start(mapped_file_queue.clone());
        }
        if let Some(ref mut flush_real_time_service) = self.flush_real_time_service {
            flush_real_time_service.start(mapped_file_queue.clone());
        }

        if self.message_store_config.transient_store_pool_enable {
            if let Some(ref mut commit_real_time_service) = self.commit_real_time_service {
                commit_real_time_service.start(mapped_file_queue);
            }
        }
    }

    fn shutdown(&mut self) {
        if let Err(error) = self.try_flush_before_shutdown() {
            warn!(error = %error, "commitlog flush failed during legacy shutdown");
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
                            match flush_ok_receiver.await {
                                Ok(Ok(GroupCommitStatus::Flushed)) => PutMessageStatus::PutOk,
                                Ok(Ok(GroupCommitStatus::TimedOut)) => PutMessageStatus::FlushDiskTimeout,
                                Ok(Err(error)) => {
                                    warn!(error = %error, "sync flush failed");
                                    PutMessageStatus::FlushDiskTimeout
                                }
                                Err(_) => PutMessageStatus::FlushDiskTimeout,
                            }
                        },
                    )
                    .await
                    .unwrap_or(PutMessageStatus::FlushDiskTimeout)
                } else {
                    let Some(group_commit_service) = self.group_commit_service.as_ref() else {
                        warn!("Sync flush requested but GroupCommitService is not initialized");
                        return PutMessageStatus::FlushDiskTimeout;
                    };
                    group_commit_service.wakeup();
                    PutMessageStatus::PutOk
                }
            }
            FlushDiskType::AsyncFlush => {
                if self.message_store_config.transient_store_pool_enable {
                    let Some(commit_real_time_service) = self.commit_real_time_service.as_ref() else {
                        warn!("Async flush requested but CommitRealTimeService is not initialized");
                        return PutMessageStatus::FlushDiskTimeout;
                    };
                    commit_real_time_service.wakeup();
                } else {
                    let Some(flush_real_time_service) = self.flush_real_time_service.as_ref() else {
                        warn!("Async flush requested but FlushRealTimeService is not initialized");
                        return PutMessageStatus::FlushDiskTimeout;
                    };
                    flush_real_time_service.wakeup();
                }
                PutMessageStatus::PutOk
            }
        }
    }
}

struct GroupCommitService {
    store_checkpoint: Arc<StoreCheckpoint>,
    notified: Arc<Notify>,
    tx_in: Option<tokio::sync::mpsc::Sender<GroupCommitRequest>>,
    shutdown_token: CancellationToken,
    worker_group: Option<TaskGroup>,
    sync_flush_stats: SyncFlushStats,
    store_health_recorder: StoreHealthRecorder,
    forced_flush_error: Option<Arc<StoreError>>,
}

impl GroupCommitService {
    fn configured_flush_error(&self) -> Option<Arc<StoreError>> {
        self.forced_flush_error.clone()
    }

    pub async fn put_request(&mut self, request: GroupCommitRequest) -> bool {
        if self.shutdown_token.is_cancelled() {
            return false;
        }

        let Some(tx_in) = self.tx_in.as_ref() else {
            return false;
        };
        let enqueue_time_millis = request.enqueue_time_millis();
        tokio::select! {
            result = tx_in.send(request) => {
                let sent = result.is_ok();
                if sent {
                    self.sync_flush_stats.record_enqueue(enqueue_time_millis);
                }
                sent
            },
            _ = self.shutdown_token.cancelled() => false,
        }
    }

    fn start(&mut self, mapped_file_queue: ArcMut<MappedFileQueue>) {
        if self.worker_group.is_some() {
            return;
        }

        let worker_group = match crate::runtime::task_group("rocketmq-store.commit-log.group-commit") {
            Ok(worker_group) => worker_group,
            Err(error) => {
                warn!("GroupCommitService cannot start because task group creation failed: {error}");
                return;
            }
        };
        self.shutdown_token = CancellationToken::new();
        let (tx_in, rx_in) = tokio::sync::mpsc::channel::<GroupCommitRequest>(GROUP_COMMIT_CHANNEL_CAPACITY);
        self.tx_in = Some(tx_in);
        let shutdown_token = self.shutdown_token.clone();
        let store_checkpoint = self.store_checkpoint.clone();
        let notified = Arc::clone(&self.notified);
        let sync_flush_stats = self.sync_flush_stats.clone();
        let store_health_recorder = self.store_health_recorder.clone();
        let forced_flush_error = self.configured_flush_error();
        if let Err(error) = worker_group.spawn_service("commit-log-group-commit", async move {
            let flush_queue = mapped_file_queue.clone();
            let flushed_queue = mapped_file_queue.clone();
            let timestamp_queue = mapped_file_queue;
            let ports = GroupCommitWorkerPorts::new(
                move || flush_mapped_file_queue(flush_queue.clone(), 0),
                move || flushed_queue.get_flushed_where(),
                move || timestamp_queue.get_store_timestamp(),
                move |timestamp| store_checkpoint.set_physic_msg_timestamp(timestamp),
                move |error: Arc<StoreError>| store_health_recorder.record_flush_failure(error.as_ref()),
                time::sleep,
            );
            run_group_commit_worker(
                rx_in,
                notified,
                shutdown_token,
                sync_flush_stats,
                forced_flush_error,
                GroupCommitWorkerConfig::legacy(),
                ports,
            )
            .await;
        }) {
            warn!("GroupCommitService cannot start because task spawn failed: {error}");
            self.tx_in.take();
            return;
        }
        self.worker_group = Some(worker_group);
    }

    pub fn wakeup(&self) {
        self.notified.notify_one();
    }

    pub fn shutdown(&mut self) {
        self.shutdown_token.cancel();
        self.tx_in.take();
        shutdown_worker_now("GroupCommitService", &mut self.worker_group);
    }

    pub async fn shutdown_gracefully(&mut self) {
        self.shutdown_token.cancel();
        self.tx_in.take();
        shutdown_worker_gracefully("GroupCommitService", &mut self.worker_group).await;
    }
}

struct FlushRealTimeService {
    message_store_config: Arc<MessageStoreConfig>,
    store_checkpoint: Arc<StoreCheckpoint>,
    notified: Arc<Notify>,
    shutdown_token: CancellationToken,
    worker_group: Option<TaskGroup>,
    store_health_recorder: StoreHealthRecorder,
}

impl FlushRealTimeService {
    fn start(&mut self, mapped_file_queue: ArcMut<MappedFileQueue>) {
        if self.worker_group.is_some() {
            return;
        }

        let worker_group = match crate::runtime::task_group("rocketmq-store.commit-log.flush-real-time") {
            Ok(worker_group) => worker_group,
            Err(error) => {
                warn!("FlushRealTimeService cannot start because task group creation failed: {error}");
                return;
            }
        };
        self.shutdown_token = CancellationToken::new();
        let message_store_config = self.message_store_config.clone();
        let store_checkpoint = self.store_checkpoint.clone();
        let notified = self.notified.clone();
        let shutdown_token = self.shutdown_token.clone();
        let store_health_recorder = self.store_health_recorder.clone();
        if let Err(error) = worker_group.spawn_service("commit-log-flush-real-time", async move {
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

                let flush_result =
                    match flush_mapped_file_queue(mapped_file_queue.clone(), flush_physic_queue_least_pages).await {
                        Ok(result) => result,
                        Err(error) => {
                            store_health_recorder.record_flush_failure(error.as_ref());
                            warn!(error = %error, "asynchronous commitlog flush failed");
                            break;
                        }
                    };
                let store_timestamp = flush_result.store_timestamp;
                if store_timestamp > 0 {
                    store_checkpoint.set_physic_msg_timestamp(store_timestamp);
                }
            }
            match flush_mapped_file_queue(mapped_file_queue, 0).await {
                Ok(flush_result) => {
                    let store_timestamp = flush_result.store_timestamp;
                    if store_timestamp > 0 {
                        store_checkpoint.set_physic_msg_timestamp(store_timestamp);
                    }
                }
                Err(error) => {
                    store_health_recorder.record_flush_failure(error.as_ref());
                    warn!(error = %error, "final asynchronous commitlog flush failed");
                }
            }
        }) {
            warn!("FlushRealTimeService cannot start because task spawn failed: {error}");
            return;
        }
        self.worker_group = Some(worker_group);
    }

    pub fn wakeup(&self) {
        if !self.message_store_config.flush_commit_log_timed {
            self.notified.notify_one();
        }
    }

    pub fn shutdown(&mut self) {
        self.shutdown_token.cancel();
        self.notified.notify_waiters();
        shutdown_worker_now("FlushRealTimeService", &mut self.worker_group);
    }

    pub async fn shutdown_gracefully(&mut self) {
        self.shutdown_token.cancel();
        self.notified.notify_waiters();
        shutdown_worker_gracefully("FlushRealTimeService", &mut self.worker_group).await;
    }
}

pub(crate) struct CommitRealTimeService {
    message_store_config: Arc<MessageStoreConfig>,
    store_checkpoint: Arc<StoreCheckpoint>,
    notified: Arc<Notify>,
    flush_manager: Option<WeakArcMut<DefaultFlushManager>>,
    shutdown_token: CancellationToken,
    worker_group: Option<TaskGroup>,
}

impl CommitRealTimeService {
    pub fn wakeup(&self) {
        self.notified.notify_one();
    }

    fn start(&mut self, mapped_file_queue: ArcMut<MappedFileQueue>) {
        if self.worker_group.is_some() {
            return;
        }

        let worker_group = match crate::runtime::task_group("rocketmq-store.commit-log.commit-real-time") {
            Ok(worker_group) => worker_group,
            Err(error) => {
                warn!("CommitRealTimeService cannot start because task group creation failed: {error}");
                return;
            }
        };
        self.shutdown_token = CancellationToken::new();
        let message_store_config = self.message_store_config.clone();
        let store_checkpoint = self.store_checkpoint.clone();
        let notified = self.notified.clone();
        let flush_manager = self.flush_manager.clone();
        let shutdown_token = self.shutdown_token.clone();
        if let Err(error) = worker_group.spawn_service("commit-log-commit-real-time", async move {
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

                let Some(commit_result) =
                    commit_mapped_file_queue(mapped_file_queue.clone(), commit_data_least_pages).await
                else {
                    break;
                };
                if !commit_result.commit_ok {
                    last_commit_timestamp = current_millis();
                    if let Some(flush_manager) =
                        flush_manager.as_ref().and_then(|flush_manager| flush_manager.upgrade())
                    {
                        flush_manager.wake_up_flush();
                    } else {
                        warn!("CommitRealTimeService cannot wake flush because flush manager is not initialized");
                    }
                }

                tokio::select! {
                    _ = shutdown_token.cancelled() => break,
                    _ = notified.notified() => {}
                    _ = tokio::time::sleep(std::time::Duration::from_millis(interval)) => {}
                }
            }
            if let Some(commit_result) = commit_mapped_file_queue(mapped_file_queue, 0).await {
                if !commit_result.commit_ok {
                    let store_timestamp = commit_result.store_timestamp;
                    if store_timestamp > 0 {
                        store_checkpoint.set_physic_msg_timestamp(store_timestamp);
                    }
                }
            }
        }) {
            warn!("CommitRealTimeService cannot start because task spawn failed: {error}");
            return;
        }
        self.worker_group = Some(worker_group);
    }

    pub fn shutdown(&mut self) {
        self.shutdown_token.cancel();
        self.notified.notify_waiters();
        shutdown_worker_now("CommitRealTimeService", &mut self.worker_group);
    }

    pub async fn shutdown_gracefully(&mut self) {
        self.shutdown_token.cancel();
        self.notified.notify_waiters();
        shutdown_worker_gracefully("CommitRealTimeService", &mut self.worker_group).await;
    }

    pub fn set_flush_manager(&mut self, flush_manager: WeakArcMut<DefaultFlushManager>) {
        self.flush_manager = Some(flush_manager);
    }
}

fn shutdown_worker_now(service_name: &'static str, worker_group: &mut Option<TaskGroup>) {
    if let Some(worker_group) = worker_group.take() {
        worker_group.cancel();
        debug!("{service_name} task group cancellation requested without waiting");
    }
}

async fn shutdown_worker_gracefully(service_name: &'static str, worker_group: &mut Option<TaskGroup>) {
    if let Some(worker_group) = worker_group.take() {
        let report = worker_group.shutdown(Duration::from_secs(5)).await;
        if let Err(error) = crate::runtime::shutdown_report_result(service_name, report) {
            warn!("{service_name} task group failed during graceful shutdown: {error}");
        }
    }
}

pub(crate) async fn flush_mapped_file_queue(
    mapped_file_queue: ArcMut<MappedFileQueue>,
    flush_least_pages: i32,
) -> Result<FlushProgress, Arc<StoreError>> {
    match crate::runtime::spawn_io("commitlog-flush", move || {
        mapped_file_queue.try_flush(flush_least_pages)
    })
    .await
    {
        Ok(Ok(result)) => Ok(result),
        Ok(Err(error)) => Err(Arc::new(StoreError::mapped_file(error))),
        Err(error) => Err(Arc::new(StoreError::InvalidState(format!(
            "commitlog flush task failed: {error}"
        )))),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct CommitMappedFileQueueResult {
    commit_ok: bool,
    store_timestamp: u64,
}

async fn commit_mapped_file_queue(
    mapped_file_queue: ArcMut<MappedFileQueue>,
    commit_least_pages: i32,
) -> Option<CommitMappedFileQueueResult> {
    match crate::runtime::spawn_io("commitlog-commit", move || {
        let commit_ok = mapped_file_queue.commit(commit_least_pages);
        CommitMappedFileQueueResult {
            commit_ok,
            store_timestamp: mapped_file_queue.get_store_timestamp(),
        }
    })
    .await
    {
        Ok(result) => Some(result),
        Err(error) => {
            tracing::error!("commitlog commit task failed: {error}");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use rocketmq_store_local::flush::group_commit::complete_group_commit_batch;
    use rocketmq_store_local::flush::group_commit::complete_group_commit_batch_error;
    use tempfile::tempdir;

    use crate::log_file::mapped_file::MappedFileError;
    use crate::store::running_flags::RunningFlags;

    fn health_recorder() -> StoreHealthRecorder {
        StoreHealthRecorder::new(Arc::new(RunningFlags::new()))
    }

    #[test]
    fn complete_group_commit_batch_marks_each_request_by_final_flushed_offset() {
        let sync_flush_stats = SyncFlushStats::default();
        let (request_64, mut response_64) = GroupCommitRequest::new(64, 5_000);
        let (request_96, mut response_96) = GroupCommitRequest::new(96, 5_000);
        sync_flush_stats.record_enqueue(request_64.enqueue_time_millis());
        sync_flush_stats.record_enqueue(request_96.enqueue_time_millis());
        let requests = vec![request_64, request_96];

        complete_group_commit_batch(requests, 80, &sync_flush_stats);

        assert!(matches!(response_64.try_recv(), Ok(Ok(GroupCommitStatus::Flushed))));
        assert!(matches!(response_96.try_recv(), Ok(Ok(GroupCommitStatus::TimedOut))));

        let runtime_info = sync_flush_stats.snapshot();
        assert_eq!(runtime_info.queue_depth, 0);
        assert_eq!(runtime_info.enqueue_total, 2);
        assert_eq!(runtime_info.completed_total, 2);
        assert_eq!(runtime_info.timeout_total, 1);
    }

    #[test]
    fn group_commit_batch_propagates_one_typed_flush_error_to_every_waiter() {
        let sync_flush_stats = SyncFlushStats::default();
        let store_health_recorder = health_recorder();
        let (first, mut first_response) = GroupCommitRequest::new(64, 5_000);
        let (second, mut second_response) = GroupCommitRequest::new(96, 5_000);
        let error = Arc::new(StoreError::InvalidState("injected flush failure".to_string()));

        store_health_recorder.record_flush_failure(error.as_ref());
        complete_group_commit_batch_error(vec![first, second], error.clone(), &sync_flush_stats);

        let first_error = first_response.try_recv().unwrap().unwrap_err();
        let second_error = second_response.try_recv().unwrap().unwrap_err();
        assert!(Arc::ptr_eq(&first_error, &error));
        assert!(Arc::ptr_eq(&second_error, &error));
        assert!(!store_health_recorder.writeable());
        assert_eq!(
            store_health_recorder
                .last_flush_error()
                .as_ref()
                .map(|error| error.kind),
            Some(crate::store_error::StoreErrorKind::InvalidState)
        );
    }

    #[test]
    fn sync_flush_runtime_info_reports_pending_oldest_wait() {
        let sync_flush_stats = SyncFlushStats::default();
        let (request, _response) = GroupCommitRequest::new(64, 5_000);

        sync_flush_stats.record_enqueue(request.enqueue_time_millis());

        let runtime_info = sync_flush_stats.snapshot();
        assert_eq!(runtime_info.queue_depth, 1);
        assert_eq!(runtime_info.enqueue_total, 1);
        assert_eq!(runtime_info.completed_total, 0);
        assert_eq!(runtime_info.timeout_total, 0);
        assert!(runtime_info.oldest_wait_millis <= current_millis().saturating_sub(request.enqueue_time_millis()));
    }

    #[tokio::test]
    async fn flush_and_commit_helpers_run_empty_queue_on_blocking_pool() {
        let mapped_file_queue = ArcMut::new(MappedFileQueue::default());

        assert!(matches!(
            flush_mapped_file_queue(mapped_file_queue.clone(), 0).await,
            Ok(FlushProgress {
                appended: 0,
                durable_before: 0,
                durable: 0,
                store_timestamp: 0,
            })
        ));
        assert_eq!(
            commit_mapped_file_queue(mapped_file_queue, 0).await,
            Some(CommitMappedFileQueueResult {
                commit_ok: true,
                store_timestamp: 0,
            })
        );
    }

    #[test]
    fn start_without_mapped_file_queue_returns_without_panicking() {
        let temp_dir = tempdir().unwrap();
        let store_checkpoint = Arc::new(StoreCheckpoint::new(temp_dir.path().join("checkpoint")).unwrap());
        let mapped_file_queue = ArcMut::new(MappedFileQueue::default());
        let mut manager = DefaultFlushManager::new(
            Arc::new(MessageStoreConfig::default()),
            mapped_file_queue,
            store_checkpoint,
        );
        manager.mapped_file_queue = None;

        manager.start();
    }

    #[tokio::test]
    async fn handle_disk_flush_returns_timeout_when_sync_service_missing() {
        let temp_dir = tempdir().unwrap();
        let store_checkpoint = Arc::new(StoreCheckpoint::new(temp_dir.path().join("checkpoint")).unwrap());
        let mapped_file_queue = ArcMut::new(MappedFileQueue::default());
        let mut manager = DefaultFlushManager::new(
            Arc::new(MessageStoreConfig {
                flush_disk_type: FlushDiskType::SyncFlush,
                ..MessageStoreConfig::default()
            }),
            mapped_file_queue,
            store_checkpoint,
        );
        manager.group_commit_service = None;

        let status = manager
            .handle_disk_flush(&AppendMessageResult::default(), &MessageExtBrokerInner::default())
            .await;

        assert_eq!(status, PutMessageStatus::FlushDiskTimeout);
    }

    #[tokio::test]
    async fn handle_disk_flush_returns_timeout_when_async_service_missing() {
        let temp_dir = tempdir().unwrap();
        let store_checkpoint = Arc::new(StoreCheckpoint::new(temp_dir.path().join("checkpoint")).unwrap());
        let mapped_file_queue = ArcMut::new(MappedFileQueue::default());
        let mut manager = DefaultFlushManager::new(
            Arc::new(MessageStoreConfig {
                flush_disk_type: FlushDiskType::AsyncFlush,
                ..MessageStoreConfig::default()
            }),
            mapped_file_queue,
            store_checkpoint,
        );
        manager.flush_real_time_service = None;

        let status = manager
            .handle_disk_flush(&AppendMessageResult::default(), &MessageExtBrokerInner::default())
            .await;

        assert_eq!(status, PutMessageStatus::FlushDiskTimeout);
    }

    #[tokio::test]
    async fn group_commit_shutdown_completes_pending_request() {
        let temp_dir = tempdir().unwrap();
        let store_checkpoint = Arc::new(StoreCheckpoint::new(temp_dir.path().join("checkpoint")).unwrap());
        let mapped_file_queue = ArcMut::new(MappedFileQueue::default());
        let mut service = GroupCommitService {
            store_checkpoint,
            notified: Arc::new(Notify::new()),
            tx_in: None,
            shutdown_token: CancellationToken::new(),
            worker_group: None,
            sync_flush_stats: SyncFlushStats::default(),
            store_health_recorder: health_recorder(),
            forced_flush_error: Some(Arc::new(StoreError::mapped_file(MappedFileError::ReferenceUnavailable))),
        };
        let store_health_recorder = service.store_health_recorder.clone();
        let forced_error = service.forced_flush_error.as_ref().unwrap().clone();
        service.start(mapped_file_queue);

        let (failed_request, failed_response) = GroupCommitRequest::new(1, 5_000);
        assert!(service.put_request(failed_request).await);
        let error = time::timeout(time::Duration::from_secs(1), failed_response)
            .await
            .expect("group commit worker should complete the failed waiter")
            .expect("group commit worker should send a result")
            .expect_err("unavailable mapped file must return a typed flush failure");

        assert!(Arc::ptr_eq(&error, &forced_error));
        assert!(!store_health_recorder.writeable());
        assert_eq!(
            store_health_recorder
                .last_flush_error()
                .as_ref()
                .map(|error| error.kind),
            Some(crate::store_error::StoreErrorKind::MappedFile)
        );

        let (pending_request, pending_response) = GroupCommitRequest::new(2, 0);
        assert!(service.put_request(pending_request).await);
        service.shutdown_gracefully().await;
        assert!(service.worker_group.is_none());
        let _completion = time::timeout(time::Duration::from_secs(1), pending_response)
            .await
            .expect("shutdown should complete pending group commit request")
            .expect("group commit worker should send a completion result");
    }

    #[tokio::test]
    async fn flush_real_time_shutdown_gracefully_waits_for_worker() {
        let temp_dir = tempdir().unwrap();
        let store_checkpoint = Arc::new(StoreCheckpoint::new(temp_dir.path().join("checkpoint")).unwrap());
        let mapped_file_queue = ArcMut::new(MappedFileQueue::default());
        let mut service = FlushRealTimeService {
            message_store_config: Arc::new(MessageStoreConfig {
                flush_interval_commit_log: 60_000,
                ..MessageStoreConfig::default()
            }),
            store_checkpoint,
            notified: Arc::new(Notify::new()),
            shutdown_token: CancellationToken::new(),
            worker_group: None,
            store_health_recorder: health_recorder(),
        };

        service.start(mapped_file_queue);
        assert!(service.worker_group.is_some());

        service.shutdown_gracefully().await;

        assert!(service.worker_group.is_none());
    }
}
