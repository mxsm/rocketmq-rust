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

use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Weak;
use std::time::Duration;

use cheetah_string::CheetahString;
use parking_lot::Mutex;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_runtime::TaskGroup;
use rocketmq_store::base::message_store::MessageStore;
use rocketmq_store::consume_queue::cq_ext_unit::CqExtUnit;
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::Notify;
use tokio::time::Instant;
use tracing::info;
use tracing::warn;

use crate::long_polling::many_pull_request::ManyPullRequest;
use crate::long_polling::pull_request::PullRequest;
use crate::processor::pull_message_processor::PullMessageProcessor;

const TOPIC_QUEUE_ID_SEPARATOR: &str = "@";
const NO_PENDING_DEADLINE: u64 = u64::MAX;
const LONG_POLLING_FALLBACK_SCAN_MILLIS: u64 = 5_000;

pub(crate) trait PullRequestProcessor: Send + Sync {
    fn long_polling_scan_config(&self) -> (bool, u64);

    fn max_offset_in_queue(&self, topic: &CheetahString, queue_id: i32) -> Option<i64>;

    fn execute_request_when_wakeup(
        self: Arc<Self>,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request: RemotingCommand,
    );
}

pub struct PullRequestHoldService<MS: MessageStore, RP = PullMessageProcessor<MS>> {
    pull_request_table: Arc<parking_lot::RwLock<HashMap<String, ManyPullRequest>>>,
    pull_message_processor: Weak<RP>,
    schedule_signal: Arc<Notify>,
    running: AtomicBool,
    accepting_requests: AtomicBool,
    next_deadline_millis: AtomicU64,
    lifecycle: AsyncMutex<()>,
    task_group: Mutex<Option<TaskGroup>>,
    marker: PhantomData<fn() -> MS>,
}

impl<MS, RP> PullRequestHoldService<MS, RP>
where
    MS: MessageStore + Send + Sync,
    RP: PullRequestProcessor + 'static,
{
    pub fn new(pull_message_processor: Weak<RP>) -> Self {
        PullRequestHoldService {
            pull_request_table: Arc::new(parking_lot::RwLock::new(HashMap::new())),
            pull_message_processor,
            schedule_signal: Arc::new(Default::default()),
            running: AtomicBool::new(false),
            accepting_requests: AtomicBool::new(false),
            next_deadline_millis: AtomicU64::new(NO_PENDING_DEADLINE),
            lifecycle: AsyncMutex::new(()),
            task_group: Mutex::new(None),
            marker: PhantomData,
        }
    }
}

#[allow(unused_variables)]
impl<MS, RP> PullRequestHoldService<MS, RP>
where
    MS: MessageStore + Send + Sync,
    RP: PullRequestProcessor + 'static,
{
    pub async fn start(this: &Arc<Self>, task_group: TaskGroup) {
        let _lifecycle = this.lifecycle.lock().await;
        if this
            .running
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        let Some(_processor_guard) = this.pull_message_processor.upgrade() else {
            this.running.store(false, Ordering::Release);
            return;
        };

        let cancellation_token = task_group.cancellation_token();
        let service = Arc::downgrade(this);
        *this.task_group.lock() = Some(task_group.clone());

        if let Err(error) = task_group.spawn_service("broker.long-polling.pull-request-hold.scan", async move {
            loop {
                let Some(current) = service.upgrade() else {
                    break;
                };
                let Some(delay) = current.next_scan_delay() else {
                    current.accepting_requests.store(false, Ordering::Release);
                    current.running.store(false, Ordering::Release);
                    break;
                };
                let schedule_signal = Arc::clone(&current.schedule_signal);
                drop(current);
                let handle_future = tokio::time::sleep(delay);
                tokio::select! {
                    _ = cancellation_token.cancelled() => {
                        info!("PullRequestHoldService: shutdown..........");
                        break;
                    }
                    _ = handle_future => {}
                    _ = schedule_signal.notified() => {
                        continue;
                    }
                }
                let Some(current) = service.upgrade() else {
                    break;
                };
                let instant = Instant::now();
                current.check_hold_request();
                let elapsed = instant.elapsed().as_millis();
                if elapsed > 5000 {
                    warn!("PullRequestHoldService: check hold pull request cost {}ms", elapsed);
                }
            }
            if let Some(current) = service.upgrade() {
                current.accepting_requests.store(false, Ordering::Release);
                current.running.store(false, Ordering::Release);
            }
        }) {
            this.task_group.lock().take();
            this.accepting_requests.store(false, Ordering::Release);
            this.running.store(false, Ordering::Release);
            warn!(?error, "failed to spawn PullRequestHoldService scan task");
        } else {
            this.accepting_requests.store(true, Ordering::Release);
        }
    }

    pub async fn shutdown(&self) {
        let _lifecycle = self.lifecycle.lock().await;
        self.accepting_requests.store(false, Ordering::Release);
        self.running.store(false, Ordering::Release);
        let task_group = self.task_group.lock().take();
        if let Some(task_group) = task_group {
            let report = task_group.shutdown(Duration::from_secs(5)).await;
            if !report.is_healthy() {
                warn!(
                    report = %report.to_json(),
                    "PullRequestHoldService shutdown report is unhealthy"
                );
            }
        }
        self.pull_request_table.write().clear();
        self.next_deadline_millis.store(NO_PENDING_DEADLINE, Ordering::Release);
    }

    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }

    pub fn suspend_pull_request(&self, topic: &str, queue_id: i32, mut pull_request: PullRequest) -> bool {
        let key = build_key(topic, queue_id);
        let mut table = self.pull_request_table.write();
        if !self.can_accept_request() {
            return false;
        }
        let mpr = table.entry(key).or_insert_with(ManyPullRequest::new);
        pull_request.request_command_mut().set_suspended_ref(true);
        self.note_request_deadline(pull_request.deadline_millis());
        mpr.add_pull_request(pull_request);
        true
    }

    fn can_accept_request(&self) -> bool {
        self.running.load(Ordering::Acquire)
            && self.accepting_requests.load(Ordering::Acquire)
            && self.pull_message_processor.upgrade().is_some()
    }

    fn note_request_deadline(&self, deadline_millis: u64) {
        let mut current = self.next_deadline_millis.load(Ordering::Acquire);
        while deadline_millis < current {
            match self.next_deadline_millis.compare_exchange(
                current,
                deadline_millis,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.schedule_signal.notify_one();
                    break;
                }
                Err(actual) => current = actual,
            }
        }
    }

    fn rebuild_next_deadline(&self) {
        let table = self.pull_request_table.read();
        let next_deadline = table
            .values()
            .filter_map(ManyPullRequest::min_deadline_millis)
            .min()
            .unwrap_or(NO_PENDING_DEADLINE);
        self.next_deadline_millis.store(next_deadline, Ordering::Release);
    }

    fn next_scan_delay(&self) -> Option<Duration> {
        let processor = self.pull_message_processor.upgrade()?;
        let (long_polling_enable, short_polling_time_mills) = processor.long_polling_scan_config();
        let delay_millis = next_hold_scan_delay_millis(
            self.next_deadline_millis.load(Ordering::Acquire),
            current_millis(),
            long_polling_enable,
            short_polling_time_mills,
        );
        Some(Duration::from_millis(delay_millis))
    }

    fn check_hold_request(&self) {
        let binding = self.pull_request_table.read();
        let keys = binding.keys().cloned().collect::<Vec<String>>();
        drop(binding);
        for key in keys {
            let key_parts: Vec<&str> = key.split(TOPIC_QUEUE_ID_SEPARATOR).collect();
            if key_parts.len() != 2 {
                continue;
            }
            let topic = CheetahString::from(key_parts[0]);
            let queue_id = key_parts[1].parse::<i32>().unwrap();
            let Some(processor) = self.pull_message_processor.upgrade() else {
                return;
            };
            let Some(max_offset) = processor.max_offset_in_queue(&topic, queue_id) else {
                return;
            };
            self.notify_message_arriving(&topic, queue_id, max_offset);
        }
        self.rebuild_next_deadline();
    }

    pub fn notify_message_arriving(&self, topic: &CheetahString, queue_id: i32, max_offset: i64) {
        self.notify_message_arriving_ext(topic, queue_id, max_offset, None, 0, None, None);
    }

    pub fn notify_message_arriving_ext(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        max_offset: i64,
        tags_code: Option<i64>,
        msg_store_time: i64,
        filter_bit_map: Option<Vec<u8>>,
        properties: Option<&HashMap<CheetahString, CheetahString>>,
    ) {
        let key = build_key(topic, queue_id);
        let mut table = self.pull_request_table.write();
        let mut deadline_changed = false;
        if let Some(mpr) = table.get_mut(&key) {
            if let Some(request_list) = mpr.clone_list_and_clear() {
                if request_list.is_empty() {
                    return;
                }
                drop(table);
                let mut replay_list = Vec::new();

                for request in request_list {
                    let mut newest_offset = max_offset;
                    if newest_offset <= request.pull_from_this_offset() {
                        let Some(processor) = self.pull_message_processor.upgrade() else {
                            return;
                        };
                        let Some(current_max_offset) = processor.max_offset_in_queue(topic, queue_id) else {
                            return;
                        };
                        newest_offset = current_max_offset;
                    }

                    if newest_offset > request.pull_from_this_offset() {
                        let match_by_consume_queue = request.message_filter().is_matched_by_consume_queue(
                            tags_code,
                            Some(&CqExtUnit::new(
                                tags_code.unwrap_or(0),
                                msg_store_time,
                                filter_bit_map.clone(),
                            )),
                        );
                        let mut match_by_commit_log = match_by_consume_queue;
                        if match_by_consume_queue && properties.is_some() {
                            match_by_commit_log = request.message_filter().is_matched_by_commit_log(None, properties);
                        }

                        if match_by_commit_log {
                            if let Some(processor) = self.pull_message_processor.upgrade() {
                                processor.execute_request_when_wakeup(
                                    request.client_channel().clone(),
                                    request.connection_handler_context().clone(),
                                    request.request_command().clone(),
                                );
                            }
                            continue;
                        }
                    }

                    if current_millis() >= (request.suspend_timestamp() + request.timeout_millis()) {
                        if let Some(processor) = self.pull_message_processor.upgrade() {
                            processor.execute_request_when_wakeup(
                                request.client_channel().clone(),
                                request.connection_handler_context().clone(),
                                request.request_command().clone(),
                            );
                        }
                        continue;
                    }

                    replay_list.push(request);
                }

                if !replay_list.is_empty() {
                    if let Some(deadline) = replay_list.iter().map(PullRequest::deadline_millis).min() {
                        self.note_request_deadline(deadline);
                    }
                    let mut table = self.pull_request_table.write();
                    let mpr = table.entry(key).or_insert_with(ManyPullRequest::new);
                    mpr.add_pull_requests(replay_list);
                }
                deadline_changed = true;
            }
        }
        if deadline_changed {
            self.rebuild_next_deadline();
        }
    }

    pub fn notify_master_online(&self) {
        let requests = self
            .pull_request_table
            .read()
            .values()
            .filter_map(ManyPullRequest::clone_list_and_clear)
            .flatten()
            .collect::<Vec<_>>();
        for request in requests {
            info!("notify master online, wakeup {}", request.request_command());
            if let Some(processor) = self.pull_message_processor.upgrade() {
                processor.execute_request_when_wakeup(
                    request.client_channel().clone(),
                    request.connection_handler_context().clone(),
                    request.request_command().clone(),
                );
            }
        }
        self.rebuild_next_deadline();
    }
}

fn build_key(topic: &str, queue_id: i32) -> String {
    format!("{topic}{TOPIC_QUEUE_ID_SEPARATOR}{queue_id}")
}

fn next_hold_scan_delay_millis(
    next_deadline_millis: u64,
    now_millis: u64,
    long_polling_enable: bool,
    short_polling_time_mills: u64,
) -> u64 {
    if !long_polling_enable {
        return short_polling_time_mills;
    }
    if next_deadline_millis == NO_PENDING_DEADLINE {
        return LONG_POLLING_FALLBACK_SCAN_MILLIS;
    }
    next_deadline_millis.saturating_sub(now_millis)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use rocketmq_runtime::RuntimeHandle;
    use rocketmq_store::message_store::GenericMessageStore;

    use super::*;

    struct TestPullProcessor;

    impl PullRequestProcessor for TestPullProcessor {
        fn long_polling_scan_config(&self) -> (bool, u64) {
            (true, 10)
        }

        fn max_offset_in_queue(&self, _topic: &CheetahString, _queue_id: i32) -> Option<i64> {
            Some(0)
        }

        fn execute_request_when_wakeup(
            self: Arc<Self>,
            _channel: Channel,
            _ctx: ConnectionHandlerContext,
            _request: RemotingCommand,
        ) {
        }
    }

    fn task_group(name: &'static str) -> TaskGroup {
        TaskGroup::root(name, RuntimeHandle::new(tokio::runtime::Handle::current()))
    }

    #[tokio::test]
    async fn start_shutdown_and_restart_are_serialized() {
        let processor = Arc::new(TestPullProcessor);
        let service = Arc::new(PullRequestHoldService::<GenericMessageStore, _>::new(Arc::downgrade(
            &processor,
        )));

        let first_group = task_group("pull-request-hold-first");
        PullRequestHoldService::start(&service, first_group.clone()).await;
        PullRequestHoldService::start(&service, first_group).await;
        assert!(service.is_running());

        service.shutdown().await;
        assert!(!service.is_running());
        assert!(!service.can_accept_request());

        PullRequestHoldService::start(&service, task_group("pull-request-hold-second")).await;
        assert!(service.is_running());
        assert!(service.can_accept_request());

        service.shutdown().await;
        assert!(!service.is_running());
    }

    #[test]
    fn service_uses_weak_processor_back_reference() {
        let processor = Arc::new(TestPullProcessor);
        let processor_weak = Arc::downgrade(&processor);
        let service = Arc::new(PullRequestHoldService::<GenericMessageStore, _>::new(
            processor_weak.clone(),
        ));

        drop(processor);

        assert!(processor_weak.upgrade().is_none());
        assert!(!service.can_accept_request());
    }

    #[tokio::test]
    async fn active_scan_does_not_keep_service_owner_alive() {
        let processor = Arc::new(TestPullProcessor);
        let service = Arc::new(PullRequestHoldService::<GenericMessageStore, _>::new(Arc::downgrade(
            &processor,
        )));
        let service_weak = Arc::downgrade(&service);
        let group = task_group("pull-request-hold-drop");

        PullRequestHoldService::start(&service, group.clone()).await;
        drop(service);

        tokio::time::timeout(Duration::from_secs(1), async {
            while service_weak.upgrade().is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("scan task must not keep PullRequestHoldService alive");

        let report = group.shutdown(Duration::from_secs(1)).await;
        assert!(report.is_healthy(), "{}", report.to_json());
    }

    #[tokio::test]
    async fn start_rolls_back_when_task_group_is_closed() {
        let processor = Arc::new(TestPullProcessor);
        let service = Arc::new(PullRequestHoldService::<GenericMessageStore, _>::new(Arc::downgrade(
            &processor,
        )));
        let group = task_group("pull-request-hold-closed");
        let report = group.clone().shutdown(Duration::from_secs(1)).await;
        assert!(report.is_healthy(), "{}", report.to_json());

        PullRequestHoldService::start(&service, group).await;

        assert!(!service.is_running());
        assert!(service.task_group.lock().is_none());
    }

    #[test]
    fn next_hold_scan_delay_uses_deadline_when_long_polling_is_enabled() {
        assert_eq!(
            next_hold_scan_delay_millis(NO_PENDING_DEADLINE, 1_000, true, 123),
            5_000
        );
        assert_eq!(next_hold_scan_delay_millis(1_250, 1_000, true, 123), 250);
        assert_eq!(next_hold_scan_delay_millis(900, 1_000, true, 123), 0);
        assert_eq!(next_hold_scan_delay_millis(1_250, 1_000, false, 123), 123);
    }
}
