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
use std::sync::OnceLock;
use std::sync::Weak;
use std::time::Duration;

use cheetah_string::CheetahString;
use parking_lot::Mutex;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_runtime::common::time_utils::current_millis;
use rocketmq_runtime::ShutdownDeadline;
use rocketmq_runtime::ShutdownReport;
use rocketmq_runtime::TaskGroup;
use rocketmq_store::BrokerReadStore;
use rocketmq_store::CqExtUnit;
use rocketmq_transport::api::v1::Channel;
use rocketmq_transport::api::v1::ConnectionHandlerContext;
use rocketmq_transport::api::v1::LegacySessionCleanupInstallError;
use rocketmq_transport::api::v1::LegacySessionExecutionEnrollment;
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::Notify;
use tokio::time::Instant;
use tracing::info;
use tracing::warn;

use super::LegacyServiceFinalization;
use super::LegacyServiceResourceSnapshot;
use super::LegacyServiceShutdownReport;
use crate::deferred_generation_handoff::DeferredGenerationHandoff;
use crate::deferred_generation_handoff::DeferredGenerationTarget;
use crate::deferred_generation_handoff::LegacyContinuation;
use crate::deferred_generation_handoff::LegacyWaitHandoff;
use crate::deferred_generation_handoff::LegacyWakeLease;
use crate::deferred_generation_handoff::RoutePermit;
use crate::long_polling::many_pull_request::ManyPullRequest;
use crate::long_polling::pull_request::PullRequest;
use crate::processor::pull_message_processor::PullMessageProcessor;

const TOPIC_QUEUE_ID_SEPARATOR: &str = "@";
const NO_PENDING_DEADLINE: u64 = u64::MAX;
const LONG_POLLING_FALLBACK_SCAN_MILLIS: u64 = 5_000;

fn remove_session_pull_waiter(
    table: &parking_lot::RwLock<HashMap<String, ManyPullRequest>>,
    key: &str,
    identity: u64,
    handoff: &Weak<LegacyWaitHandoff>,
) {
    if let Some(handoff) = handoff.upgrade() {
        handoff.mark_session_closed();
    }
    let removed = table
        .write()
        .get(key)
        .and_then(|requests| requests.remove_legacy_identity(identity));
    if let Some(request) = removed {
        request.release_legacy_wait();
    }
}

struct PullWakeupClaim {
    wake: Option<LegacyWakeLease>,
    execution: Option<LegacySessionExecutionEnrollment>,
}

pub(crate) trait PullRequestProcessor: Send + Sync {
    fn long_polling_scan_config(&self) -> (bool, u64);

    fn max_offset_in_queue(&self, topic: &CheetahString, queue_id: i32) -> Option<i64>;

    fn execute_request_when_wakeup(
        self: Arc<Self>,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request: RemotingCommand,
        continuation: Option<LegacyContinuation>,
        execution: Option<LegacySessionExecutionEnrollment>,
    ) -> bool;

    fn wakeup_task_group(&self) -> Option<TaskGroup>;
}

pub struct PullRequestHoldService<MS: BrokerReadStore, RP = PullMessageProcessor<MS>> {
    pull_request_table: Arc<parking_lot::RwLock<HashMap<String, ManyPullRequest>>>,
    pull_message_processor: Weak<RP>,
    schedule_signal: Arc<Notify>,
    running: AtomicBool,
    accepting_requests: AtomicBool,
    accepting_wakes: AtomicBool,
    next_deadline_millis: AtomicU64,
    lifecycle: AsyncMutex<()>,
    wake_admission: Mutex<()>,
    task_group: Mutex<Option<TaskGroup>>,
    handoff: OnceLock<Arc<DeferredGenerationHandoff>>,
    master_online_producer: Mutex<Option<Arc<dyn Fn() + Send + Sync + 'static>>>,
    shutdown_wake_failures: AtomicU64,
    marker: PhantomData<fn() -> MS>,
}

impl<MS, RP> PullRequestHoldService<MS, RP>
where
    MS: BrokerReadStore + Send + Sync,
    RP: PullRequestProcessor + 'static,
{
    pub fn new(pull_message_processor: Weak<RP>) -> Self {
        PullRequestHoldService {
            pull_request_table: Arc::new(parking_lot::RwLock::new(HashMap::new())),
            pull_message_processor,
            schedule_signal: Arc::new(Default::default()),
            running: AtomicBool::new(false),
            accepting_requests: AtomicBool::new(false),
            accepting_wakes: AtomicBool::new(false),
            next_deadline_millis: AtomicU64::new(NO_PENDING_DEADLINE),
            lifecycle: AsyncMutex::new(()),
            wake_admission: Mutex::new(()),
            task_group: Mutex::new(None),
            handoff: OnceLock::new(),
            master_online_producer: Mutex::new(None),
            shutdown_wake_failures: AtomicU64::new(0),
            marker: PhantomData,
        }
    }
}

#[allow(unused_variables)]
impl<MS, RP> PullRequestHoldService<MS, RP>
where
    MS: BrokerReadStore + Send + Sync,
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
            this.accepting_wakes.store(true, Ordering::Release);
            this.shutdown_wake_failures.store(0, Ordering::Release);
        }
    }

    pub(crate) async fn stop_producer_until(&self, deadline: ShutdownDeadline) -> Option<ShutdownReport> {
        let _lifecycle = self.lifecycle.lock().await;
        self.accepting_requests.store(false, Ordering::Release);
        {
            let _wake_admission = self.wake_admission.lock();
            self.accepting_wakes.store(false, Ordering::Release);
        }
        self.running.store(false, Ordering::Release);
        let task_group = self.task_group.lock().take();
        match task_group {
            Some(task_group) => Some(task_group.shutdown_until(deadline).await),
            None => None,
        }
    }

    pub(crate) async fn drain_executions_until(&self, deadline: ShutdownDeadline) -> Option<ShutdownReport> {
        let _lifecycle = self.lifecycle.lock().await;
        let task_group = self
            .pull_message_processor
            .upgrade()
            .and_then(|processor| processor.wakeup_task_group());
        match task_group {
            Some(task_group) => Some(task_group.shutdown_until(deadline).await),
            None => None,
        }
    }

    pub(crate) async fn finalize_shutdown(&self) -> LegacyServiceFinalization {
        let _lifecycle = self.lifecycle.lock().await;
        let observed_after_session_drain = self.legacy_resource_snapshot();
        let retired = {
            let mut table = self.pull_request_table.write();
            std::mem::take(&mut *table)
        };
        // PullRequest drops may release handoff leases. Drop only after
        // releasing the table lock so shutdown cannot invert gate -> table.
        drop(retired);
        self.next_deadline_millis.store(NO_PENDING_DEADLINE, Ordering::Release);
        LegacyServiceFinalization {
            observed_after_session_drain,
            terminal: self.legacy_resource_snapshot(),
        }
    }

    pub async fn shutdown(&self) -> LegacyServiceShutdownReport {
        let deadline = ShutdownDeadline::after(Duration::from_secs(5));
        let producer = self.stop_producer_until(deadline).await;
        let executions = self.drain_executions_until(deadline).await;
        let finalization = self.finalize_shutdown().await;
        LegacyServiceShutdownReport {
            name: "pull_request_hold",
            producer,
            executions,
            observed_after_session_drain: finalization.observed_after_session_drain,
            resources: finalization.terminal,
        }
    }

    pub(crate) fn legacy_resource_snapshot(&self) -> LegacyServiceResourceSnapshot {
        let table_entries = self.pull_request_table.read().values().map(ManyPullRequest::len).sum();
        let wake_task_count = self
            .pull_message_processor
            .upgrade()
            .and_then(|processor| processor.wakeup_task_group())
            .map_or(0, |group| group.task_count());
        LegacyServiceResourceSnapshot {
            table_entries,
            tracked_waiters: u64::try_from(table_entries).unwrap_or(u64::MAX),
            task_count: self.task_group.lock().as_ref().map_or(0, TaskGroup::task_count),
            wake_task_count,
            active_executions: u64::try_from(wake_task_count).unwrap_or(u64::MAX),
            shutdown_wake_failures: self.shutdown_wake_failures.load(Ordering::Acquire),
            ..Default::default()
        }
    }

    pub(crate) fn legacy_target_occupied(&self, target: &DeferredGenerationTarget) -> bool {
        let DeferredGenerationTarget::Pull { topic, queue_id } = target else {
            return false;
        };
        let key = build_key(topic.as_str(), *queue_id);
        self.pull_request_table
            .read()
            .get(&key)
            .is_some_and(|requests| !requests.is_empty())
    }

    pub(crate) fn install_handoff(
        &self,
        handoff: Arc<DeferredGenerationHandoff>,
    ) -> Result<(), Arc<DeferredGenerationHandoff>> {
        let table = self.pull_request_table.write();
        if let Some(installed) = self.handoff.get() {
            return if Arc::ptr_eq(installed, &handoff) {
                Ok(())
            } else {
                Err(handoff)
            };
        }
        if table.values().any(|requests| !requests.is_empty()) {
            return Err(handoff);
        }
        self.handoff.set(handoff)
    }

    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }

    pub fn suspend_pull_request(&self, topic: &str, queue_id: i32, mut pull_request: PullRequest) -> bool {
        let key = build_key(topic, queue_id);
        if !self.can_accept_request() {
            return false;
        }
        if let Some(handoff) = self.handoff.get() {
            let target = DeferredGenerationTarget::pull(CheetahString::from(topic), queue_id);
            let rollback_table = Arc::clone(&self.pull_request_table);
            let rollback_key = key.clone();
            let rollback_identity = pull_request.legacy_handoff_identity();
            return handoff
                .arrival_adapter()
                .install_legacy_wait(
                    target.clone(),
                    |lease| {
                        pull_request
                            .install_legacy_handoff(&target, lease)
                            .map_err(|lease| ((), lease))?;
                        let cleanup_table = Arc::clone(&self.pull_request_table);
                        let cleanup_key = key.clone();
                        let cleanup_identity = pull_request.legacy_handoff_identity();
                        let cleanup_handoff = pull_request.legacy_handoff_weak();
                        let cleanup_context = pull_request.connection_handler_context().clone();
                        let mut pending_request = Some(pull_request);
                        match cleanup_context.install_legacy_session_execution(
                            move || {
                                remove_session_pull_waiter(
                                    &cleanup_table,
                                    &cleanup_key,
                                    cleanup_identity,
                                    &cleanup_handoff,
                                );
                            },
                            |cleanup| {
                                let Some(mut request) = pending_request.take() else {
                                    return Err(((), cleanup));
                                };
                                if let Err(cleanup) = request.install_legacy_session_cleanup(cleanup) {
                                    pending_request = Some(request);
                                    return Err(((), cleanup));
                                }
                                request.request_command_mut().set_suspended_ref(true);
                                self.note_request_deadline(request.deadline_millis());
                                let mut table = self.pull_request_table.write();
                                let mpr = table.entry(key.clone()).or_insert_with(ManyPullRequest::new);
                                mpr.add_pull_request(request);
                                Ok(())
                            },
                        ) {
                            Ok(()) => Ok(()),
                            Err(LegacySessionCleanupInstallError::Unavailable) => {
                                #[cfg(test)]
                                {
                                    let mut request = pending_request
                                        .take()
                                        .expect("unavailable cleanup leaves the fresh Pull request owned");
                                    request.request_command_mut().set_suspended_ref(true);
                                    self.note_request_deadline(request.deadline_millis());
                                    let mut table = self.pull_request_table.write();
                                    let mpr = table.entry(key.clone()).or_insert_with(ManyPullRequest::new);
                                    mpr.add_pull_request(request);
                                    Ok(())
                                }
                                #[cfg(not(test))]
                                {
                                    let lease = pending_request
                                        .as_ref()
                                        .expect("unavailable cleanup retains the fresh Pull request")
                                        .take_legacy_wait()
                                        .expect("unavailable cleanup retains the fresh wait lease");
                                    Err(((), lease))
                                }
                            }
                            Err(_) => {
                                let lease = pending_request
                                    .as_ref()
                                    .expect("failed cleanup installation retains the fresh Pull request")
                                    .take_legacy_wait()
                                    .expect("failed cleanup installation retains the fresh wait lease");
                                Err(((), lease))
                            }
                        }
                    },
                    move || {
                        if let Some(requests) = rollback_table.read().get(&rollback_key) {
                            drop(requests.remove_legacy_identity(rollback_identity));
                        }
                    },
                )
                .is_ok();
        }
        pull_request.request_command_mut().set_suspended_ref(true);
        self.note_request_deadline(pull_request.deadline_millis());
        let mut table = self.pull_request_table.write();
        let mpr = table.entry(key).or_insert_with(ManyPullRequest::new);
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
        let _wake_admission = self.wake_admission.lock();
        if !self.accepting_wakes.load(Ordering::Acquire) {
            return;
        }
        let key = build_key(topic, queue_id);
        let mut deadline_changed = false;
        let request_list = self.claim_legacy_requests(&key, topic, queue_id);
        if !request_list.is_empty() {
            let mut replay_list = Vec::new();

            for (request, route) in request_list {
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
                        if let Some(wake) = self.begin_wake(&request, route) {
                            self.submit_wake(&request, wake);
                        } else {
                            replay_list.push(request);
                        }
                        continue;
                    }
                }

                if current_millis() >= (request.suspend_timestamp() + request.timeout_millis()) {
                    if let Some(wake) = self.begin_wake(&request, route) {
                        self.submit_wake(&request, wake);
                    } else {
                        replay_list.push(request);
                    }
                    continue;
                }

                replay_list.push(request);
            }

            if !replay_list.is_empty() {
                self.requeue_legacy_requests(key, replay_list);
            }
            deadline_changed = true;
        }
        if deadline_changed {
            self.rebuild_next_deadline();
        }
    }

    fn claim_legacy_requests(
        &self,
        key: &str,
        topic: &CheetahString,
        queue_id: i32,
    ) -> Vec<(PullRequest, Option<RoutePermit>)> {
        if !self.accepting_wakes.load(Ordering::Acquire) {
            return Vec::new();
        }
        let Some(handoff) = self.handoff.get() else {
            return self
                .pull_request_table
                .read()
                .get(key)
                .map(|requests| requests.drain_with_claim(|_| Some(None)))
                .unwrap_or_default();
        };
        let target = DeferredGenerationTarget::pull(topic.clone(), queue_id);
        handoff
            .arrival_adapter()
            .claim_legacy_table(
                target.clone(),
                |claimed| {
                    if let Some(requests) = self.pull_request_table.read().get(key) {
                        claimed.extend(
                            requests
                                .drain_with_claim(|request| {
                                    (request.legacy_handoff_target().as_ref() == Some(&target)).then_some(())
                                })
                                .into_iter()
                                .map(|(request, ())| request),
                        );
                    }
                },
                |requests| {
                    self.pull_request_table
                        .write()
                        .entry(key.to_string())
                        .or_insert_with(ManyPullRequest::new)
                        .add_pull_requests(requests);
                },
            )
            .map(|claimed| {
                claimed
                    .into_iter()
                    .map(|(request, route)| (request, Some(route)))
                    .collect()
            })
            .unwrap_or_default()
    }

    fn requeue_legacy_requests(&self, key: String, requests: Vec<PullRequest>) {
        if let Some(deadline) = requests.iter().map(PullRequest::deadline_millis).min() {
            self.note_request_deadline(deadline);
        }
        let terminal_requests = {
            let mut table = self.pull_request_table.write();
            let requests_for_key = table.entry(key.clone()).or_insert_with(ManyPullRequest::new);
            // Session cleanup publishes terminal before attempting table
            // removal. Keep both the outer table publication lock and the node
            // lock through add plus reread so neither cleanup nor another
            // notifier can claim the just-published node in between.
            requests_for_key
                .add_and_drain_with_claim(requests, |request| request.legacy_session_closed().then_some(()))
                .into_iter()
                .map(|(request, ())| request)
                .collect::<Vec<_>>()
        };
        for request in terminal_requests {
            request.release_legacy_wait();
        }
    }

    fn begin_wake(&self, request: &PullRequest, route: Option<RoutePermit>) -> Option<PullWakeupClaim> {
        let Some(route) = route else {
            return Some(PullWakeupClaim {
                wake: None,
                execution: None,
            });
        };
        let wait = request.take_legacy_wait()?;
        match wait.begin_wake(route) {
            Ok(wake) => {
                let execution = request.take_legacy_session_execution();
                #[cfg(not(test))]
                execution.as_ref()?;
                Some(PullWakeupClaim {
                    wake: Some(wake),
                    execution,
                })
            }
            Err(error) => {
                let (wait, _route) = error.into_wait_and_route();
                if let Err(wait) = request.restore_legacy_wait(wait) {
                    drop(wait);
                }
                None
            }
        }
    }

    fn submit_wake(&self, request: &PullRequest, wake_claim: PullWakeupClaim) {
        let PullWakeupClaim { wake, execution } = wake_claim;
        if let Some(processor) = self.pull_message_processor.upgrade() {
            if processor.execute_request_when_wakeup(
                request.client_channel().clone(),
                request.connection_handler_context().clone(),
                request.request_command().clone(),
                wake.map(LegacyWakeLease::into_continuation),
                execution,
            ) {
                return;
            }
        }
        self.shutdown_wake_failures.fetch_add(1, Ordering::AcqRel);
    }

    pub(crate) fn install_master_online_producer(
        &self,
        producer: Arc<dyn Fn() + Send + Sync + 'static>,
    ) -> Result<(), Arc<dyn Fn() + Send + Sync + 'static>> {
        let mut installed = self.master_online_producer.lock();
        if installed.is_some() {
            return Err(producer);
        }
        *installed = Some(producer);
        Ok(())
    }

    pub(crate) fn uninstall_master_online_producer(&self, producer: &Arc<dyn Fn() + Send + Sync + 'static>) -> bool {
        let mut installed = self.master_online_producer.lock();
        if installed.as_ref().is_some_and(|current| Arc::ptr_eq(current, producer)) {
            installed.take();
            true
        } else {
            false
        }
    }

    pub(crate) fn has_master_online_producer(&self) -> bool {
        self.master_online_producer.lock().is_some()
    }

    pub fn notify_master_online(&self) {
        let producer = self.master_online_producer.lock().clone();
        if let Some(producer) = producer {
            producer();
            return;
        }
        self.notify_master_online_legacy();
    }

    pub(crate) fn notify_master_online_legacy(&self) {
        let _wake_admission = self.wake_admission.lock();
        if !self.accepting_wakes.load(Ordering::Acquire) {
            return;
        }
        let keys = self.pull_request_table.read().keys().cloned().collect::<Vec<_>>();
        let mut requests = Vec::new();
        for key in keys {
            let mut parts = key.split(TOPIC_QUEUE_ID_SEPARATOR);
            let (Some(topic), Some(queue_id), None) = (parts.next(), parts.next(), parts.next()) else {
                continue;
            };
            let Ok(queue_id) = queue_id.parse::<i32>() else {
                continue;
            };
            requests.extend(
                self.claim_legacy_requests(&key, &CheetahString::from(topic), queue_id)
                    .into_iter()
                    .map(|(request, route)| (key.clone(), request, route)),
            );
        }
        for (key, request, route) in requests {
            info!("notify master online, wakeup {}", request.request_command());
            if let Some(wake) = self.begin_wake(&request, route) {
                self.submit_wake(&request, wake);
            } else {
                self.requeue_legacy_requests(key, vec![request]);
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
    use std::io::Read;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::time::Duration;

    use rocketmq_protocol::code::request_code::RequestCode;
    use rocketmq_runtime::RuntimeContext;
    use rocketmq_store::MessageFilter;
    use rocketmq_store::StorePorts;
    use rocketmq_transport::api::v1::ConnectionHandlerContextWrapper;
    use rocketmq_transport::test_support::Connection;
    use rocketmq_transport::test_support::LegacySessionExecutionHarness;
    use rocketmq_transport::test_support::TestChannelBuilder;

    use super::*;
    use crate::deferred_generation_handoff::DeferredGenerationV2Publisher;

    struct TestPullProcessor;

    struct SessionOwnedPullProcessor {
        calls: AtomicUsize,
        write_response: bool,
    }

    struct RejectAllFilter;

    impl MessageFilter for RejectAllFilter {
        fn is_matched_by_consume_queue(&self, _tags_code: Option<i64>, _cq_ext_unit: Option<&CqExtUnit>) -> bool {
            false
        }

        fn is_matched_by_commit_log(
            &self,
            _msg_buffer: Option<&[u8]>,
            _properties: Option<&HashMap<CheetahString, CheetahString>>,
        ) -> bool {
            false
        }
    }

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
            _continuation: Option<LegacyContinuation>,
            _execution: Option<LegacySessionExecutionEnrollment>,
        ) -> bool {
            true
        }

        fn wakeup_task_group(&self) -> Option<TaskGroup> {
            None
        }
    }

    impl PullRequestProcessor for SessionOwnedPullProcessor {
        fn long_polling_scan_config(&self) -> (bool, u64) {
            (true, 10)
        }

        fn max_offset_in_queue(&self, _topic: &CheetahString, _queue_id: i32) -> Option<i64> {
            Some(0)
        }

        fn execute_request_when_wakeup(
            self: Arc<Self>,
            _channel: Channel,
            ctx: ConnectionHandlerContext,
            _request: RemotingCommand,
            continuation: Option<LegacyContinuation>,
            execution: Option<LegacySessionExecutionEnrollment>,
        ) -> bool {
            let task = async move {
                let _continuation = continuation;
                self.calls.fetch_add(1, Ordering::AcqRel);
                if self.write_response {
                    let response = RemotingCommand::create_remoting_command(0).mark_response_type();
                    let _ = ctx.try_write_response(response).await;
                }
            };
            execution.is_some_and(|execution| execution.try_execute(task).is_ok())
        }

        fn wakeup_task_group(&self) -> Option<TaskGroup> {
            None
        }
    }

    fn task_group(name: &'static str) -> TaskGroup {
        RuntimeContext::from_current(name).root_group().clone()
    }

    async fn test_channel_context() -> (Channel, ConnectionHandlerContext) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind Pull test listener");
        let local_addr = listener.local_addr().expect("Pull test listener address");
        let stream = std::net::TcpStream::connect(local_addr).expect("connect Pull test listener");
        stream.set_nonblocking(true).expect("set Pull stream nonblocking");
        let stream = tokio::net::TcpStream::from_std(stream).expect("convert Pull test stream");
        let channel = TestChannelBuilder::new(Connection::new(stream), crate::test_task_group("pull-handoff-channel"))
            .addresses(local_addr, local_addr)
            .build()
            .expect("build Pull test channel");
        let context = Arc::new(ConnectionHandlerContextWrapper::new(channel.clone()));
        (channel, context)
    }

    async fn cleanup_test_channel_context(
        owner_id: u64,
    ) -> (
        LegacySessionExecutionHarness,
        TaskGroup,
        Channel,
        ConnectionHandlerContext,
    ) {
        let (channel, _base) = test_channel_context().await;
        let session_group = crate::test_task_group(format!("pull-session-execution-{owner_id}"));
        let harness = LegacySessionExecutionHarness::new(owner_id, &session_group);
        let context = harness.context(channel.clone(), 4 * 1024, RequestCode::PullMessage.to_i32());
        (harness, session_group, channel, context)
    }

    async fn session_execution_test_channel_context(
        owner_id: u64,
        writer_barrier: Option<(Arc<Notify>, Arc<Notify>)>,
    ) -> (
        LegacySessionExecutionHarness,
        TaskGroup,
        Channel,
        ConnectionHandlerContext,
        std::net::TcpStream,
    ) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind Pull session listener");
        let local_addr = listener.local_addr().expect("Pull session listener address");
        let stream = std::net::TcpStream::connect(local_addr).expect("connect Pull session peer");
        let (peer, _) = listener.accept().expect("accept Pull session peer");
        peer.set_nonblocking(true).expect("set Pull session peer nonblocking");
        stream
            .set_nonblocking(true)
            .expect("set Pull session stream nonblocking");
        let stream = tokio::net::TcpStream::from_std(stream).expect("convert Pull session stream");
        let connection = Connection::new(stream);
        let mut builder = TestChannelBuilder::new(
            connection,
            crate::test_task_group(format!("pull-session-channel-{owner_id}")),
        )
        .addresses(local_addr, local_addr);
        if let Some((entered, release)) = writer_barrier {
            builder = builder.write_preflight_barrier(entered, release);
        }
        let channel = builder.build().expect("build Pull session channel");
        let session_group = crate::test_task_group(format!("pull-session-owner-{owner_id}"));
        let session = LegacySessionExecutionHarness::new(owner_id, &session_group);
        let context = session.context(channel.clone(), 4 * 1024, RequestCode::PullMessage.to_i32());
        (session, session_group, channel, context, peer)
    }

    fn register_session_waiter<RP>(
        service: &PullRequestHoldService<StorePorts, RP>,
        channel: Channel,
        context: ConnectionHandlerContext,
        topic: &CheetahString,
    ) where
        RP: PullRequestProcessor + 'static,
    {
        let request = PullRequest::new(
            RemotingCommand::create_remoting_command(RequestCode::PullMessage),
            channel,
            context,
            60_000,
            current_millis(),
            0,
            rocketmq_protocol::protocol::heartbeat::subscription_data::SubscriptionData::default(),
            Arc::new(RejectAllFilter),
        );
        assert!(service.suspend_pull_request(topic.as_str(), 0, request));
    }

    fn claim_session_waiter<RP>(
        service: &PullRequestHoldService<StorePorts, RP>,
        topic: &CheetahString,
    ) -> (PullRequest, Option<RoutePermit>)
    where
        RP: PullRequestProcessor + 'static,
    {
        let key = build_key(topic.as_str(), 0);
        let mut claimed = service.claim_legacy_requests(&key, topic, 0);
        assert_eq!(claimed.len(), 1);
        claimed.pop().expect("claim Pull session waiter")
    }

    fn assert_peer_received_no_bytes(peer: &mut std::net::TcpStream) {
        let mut byte = [0_u8; 1];
        let error = peer
            .read(&mut byte)
            .expect_err("cancelled Pull session wrote no response bytes");
        assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);
    }

    #[tokio::test]
    async fn pull_close_between_requeue_publication_and_terminal_reread_releases_once() {
        let processor = Arc::new(TestPullProcessor);
        let service = Arc::new(PullRequestHoldService::<StorePorts, _>::new(Arc::downgrade(&processor)));
        let handoff = Arc::new(DeferredGenerationHandoff::new());
        service
            .install_handoff(Arc::clone(&handoff))
            .expect("install Pull requeue coordinator");
        PullRequestHoldService::start(&service, task_group("pull-terminal-requeue-service")).await;
        let topic = CheetahString::from_static_str("pull-terminal-requeue");
        let key = build_key(topic.as_str(), 0);
        let (session, session_group, channel, context, _peer) =
            session_execution_test_channel_context(8_504, None).await;
        let session = Arc::new(session);
        register_session_waiter(&service, channel, context, &topic);
        let (request, route) = claim_session_waiter(&service, &topic);
        let terminal_view = request.clone();

        let (terminal_requests, closing) = {
            let mut table = service.pull_request_table.write();
            let requests = table.entry(key.clone()).or_insert_with(ManyPullRequest::new);
            requests.add_pull_request(request);
            let closing_session = Arc::clone(&session);
            let closing = std::thread::spawn(move || closing_session.close());
            while !terminal_view.legacy_session_closed() {
                std::thread::yield_now();
            }
            let terminal_requests = requests
                .drain_with_claim(|request| request.legacy_session_closed().then_some(()))
                .into_iter()
                .map(|(request, ())| request)
                .collect::<Vec<_>>();
            (terminal_requests, closing)
        };
        closing.join().expect("Pull close completes after table publication");
        assert_eq!(terminal_requests.len(), 1);
        assert!(service.pull_request_table.read()[&key].is_empty());
        for request in terminal_requests {
            request.release_legacy_wait();
        }
        drop(route);
        assert!(handoff.zero_report().is_zero());

        let report = session_group
            .shutdown_until(ShutdownDeadline::after(Duration::from_secs(1)))
            .await;
        assert!(report.is_healthy(), "{}", report.to_json());
        service.shutdown().await;
    }

    #[tokio::test]
    async fn pull_session_close_after_claim_runs_no_handler_or_writer() {
        let processor = Arc::new(SessionOwnedPullProcessor {
            calls: AtomicUsize::new(0),
            write_response: true,
        });
        let service = Arc::new(PullRequestHoldService::<StorePorts, _>::new(Arc::downgrade(&processor)));
        let handoff = Arc::new(DeferredGenerationHandoff::new());
        service
            .install_handoff(Arc::clone(&handoff))
            .expect("install Pull session coordinator");
        PullRequestHoldService::start(&service, task_group("pull-session-claimed-service")).await;
        let topic = CheetahString::from_static_str("pull-session-claimed");
        let (session, session_group, channel, context, mut peer) =
            session_execution_test_channel_context(8_501, None).await;
        register_session_waiter(&service, channel, context, &topic);
        let (request, route) = claim_session_waiter(&service, &topic);

        session.close();
        let claim = service
            .begin_wake(&request, route)
            .expect("closed Pull claim retains its affine execution enrollment");
        service.submit_wake(&request, claim);
        assert_eq!(processor.calls.load(Ordering::Acquire), 0);
        assert_peer_received_no_bytes(&mut peer);
        assert!(handoff.zero_report().is_zero());

        let report = session_group
            .shutdown_until(ShutdownDeadline::after(Duration::from_secs(1)))
            .await;
        assert!(report.is_healthy(), "{}", report.to_json());
        service.shutdown().await;
    }

    #[tokio::test]
    async fn pull_session_close_before_first_handler_poll_runs_no_handler_or_writer() {
        let processor = Arc::new(SessionOwnedPullProcessor {
            calls: AtomicUsize::new(0),
            write_response: true,
        });
        let service = Arc::new(PullRequestHoldService::<StorePorts, _>::new(Arc::downgrade(&processor)));
        let handoff = Arc::new(DeferredGenerationHandoff::new());
        service
            .install_handoff(Arc::clone(&handoff))
            .expect("install Pull first-poll coordinator");
        PullRequestHoldService::start(&service, task_group("pull-session-first-poll-service")).await;
        let topic = CheetahString::from_static_str("pull-session-first-poll");
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let (session, session_group, channel, context, mut peer) =
            session_execution_test_channel_context(8_502, None).await;
        session.set_first_poll_gate(Arc::clone(&entered), Arc::clone(&release));
        register_session_waiter(&service, channel, context, &topic);
        let (request, route) = claim_session_waiter(&service, &topic);
        let claim = service.begin_wake(&request, route).expect("begin Pull session wake");
        service.submit_wake(&request, claim);

        tokio::time::timeout(Duration::from_secs(1), entered.notified())
            .await
            .expect("session executor accepted Pull wake before handler poll");
        session.close();
        release.notify_one();
        let report = session_group
            .shutdown_until(ShutdownDeadline::after(Duration::from_secs(1)))
            .await;
        assert!(report.is_healthy(), "{}", report.to_json());
        assert_eq!(processor.calls.load(Ordering::Acquire), 0);
        assert_peer_received_no_bytes(&mut peer);
        assert!(handoff.zero_report().is_zero());
        service.shutdown().await;
    }

    #[tokio::test]
    async fn pull_session_close_at_writer_preflight_writes_no_bytes() {
        let processor = Arc::new(SessionOwnedPullProcessor {
            calls: AtomicUsize::new(0),
            write_response: true,
        });
        let service = Arc::new(PullRequestHoldService::<StorePorts, _>::new(Arc::downgrade(&processor)));
        let handoff = Arc::new(DeferredGenerationHandoff::new());
        service
            .install_handoff(Arc::clone(&handoff))
            .expect("install Pull writer coordinator");
        PullRequestHoldService::start(&service, task_group("pull-session-writer-service")).await;
        let topic = CheetahString::from_static_str("pull-session-writer");
        let writer_entered = Arc::new(Notify::new());
        let writer_release = Arc::new(Notify::new());
        let (session, session_group, channel, context, mut peer) = session_execution_test_channel_context(
            8_503,
            Some((Arc::clone(&writer_entered), Arc::clone(&writer_release))),
        )
        .await;
        register_session_waiter(&service, channel, context, &topic);
        let (request, route) = claim_session_waiter(&service, &topic);
        let claim = service.begin_wake(&request, route).expect("begin Pull writer wake");
        service.submit_wake(&request, claim);

        tokio::time::timeout(Duration::from_secs(1), writer_entered.notified())
            .await
            .expect("Pull response reached canonical writer preflight");
        assert_eq!(processor.calls.load(Ordering::Acquire), 1);
        session.close();
        let report = session_group
            .shutdown_until(ShutdownDeadline::after(Duration::from_secs(1)))
            .await;
        assert!(report.is_healthy(), "{}", report.to_json());
        writer_release.notify_one();
        assert_peer_received_no_bytes(&mut peer);
        assert!(handoff.zero_report().is_zero());
        service.shutdown().await;
    }

    #[tokio::test]
    async fn session_close_removes_only_its_exact_pull_waiter() {
        let processor = Arc::new(TestPullProcessor);
        let service = Arc::new(PullRequestHoldService::<StorePorts, _>::new(Arc::downgrade(&processor)));
        let handoff = Arc::new(DeferredGenerationHandoff::new());
        service
            .install_handoff(Arc::clone(&handoff))
            .expect("install the Broker coordinator");
        PullRequestHoldService::start(&service, task_group("pull-session-cleanup")).await;
        let topic = CheetahString::from_static_str("cleanup-pull-topic");
        let (first_session, _first_group, first_channel, first_context) = cleanup_test_channel_context(8_301).await;
        let (second_session, _second_group, second_channel, second_context) = cleanup_test_channel_context(8_302).await;
        for (channel, context) in [(first_channel, first_context), (second_channel, second_context)] {
            let request = PullRequest::new(
                RemotingCommand::create_remoting_command(0),
                channel,
                context,
                60_000,
                current_millis(),
                0,
                rocketmq_protocol::protocol::heartbeat::subscription_data::SubscriptionData::default(),
                Arc::new(RejectAllFilter),
            );
            assert!(service.suspend_pull_request(topic.as_str(), 0, request));
        }
        let key = build_key(topic.as_str(), 0);
        let target = DeferredGenerationTarget::pull(topic.clone(), 0);
        assert_eq!(service.pull_request_table.read()[&key].len(), 2);
        assert_eq!(handoff.snapshot().occupancy, 2);

        first_session.close();
        assert_eq!(service.pull_request_table.read()[&key].len(), 1);
        assert_eq!(handoff.snapshot().occupancy, 1);
        assert!(service.legacy_target_occupied(&target));
        assert!(!service.legacy_target_occupied(&DeferredGenerationTarget::pull(
            CheetahString::from_static_str("unrelated-pull-topic"),
            0,
        )));

        second_session.close();
        assert!(service.pull_request_table.read()[&key].is_empty());
        assert!(handoff.zero_report().is_zero());
        service.shutdown().await;
    }

    #[tokio::test]
    async fn registration_and_transition_share_gate_then_table_order() {
        let processor = Arc::new(TestPullProcessor);
        let service = Arc::new(PullRequestHoldService::<StorePorts, _>::new(Arc::downgrade(&processor)));
        let handoff = Arc::new(DeferredGenerationHandoff::new());
        service
            .install_handoff(Arc::clone(&handoff))
            .expect("install the Broker coordinator");
        PullRequestHoldService::start(&service, task_group("pull-register-transition")).await;
        let topic = CheetahString::from_static_str("pull-register-transition-topic");
        let target = DeferredGenerationTarget::pull(topic.clone(), 0);
        let key = build_key(topic.as_str(), 0);
        let (session, _session_group, channel, context) = cleanup_test_channel_context(8_303).await;
        let session = Arc::new(session);
        let checkpoint = Arc::new(std::sync::Barrier::new(2));
        let release = Arc::new(std::sync::Barrier::new(2));
        let insert_checkpoint = Arc::clone(&checkpoint);
        let insert_release = Arc::clone(&release);
        session.set_insert_checkpoint(move |state_locked| {
            assert!(state_locked, "Pull publication must hold the cleanup enrollment gate");
            insert_checkpoint.wait();
            insert_release.wait();
        });
        let request = PullRequest::new(
            RemotingCommand::create_remoting_command(0),
            channel,
            context,
            60_000,
            current_millis(),
            0,
            rocketmq_protocol::protocol::heartbeat::subscription_data::SubscriptionData::default(),
            Arc::new(RejectAllFilter),
        );
        let registering_service = Arc::clone(&service);
        let registering_topic = topic.clone();
        let registering = std::thread::spawn(move || {
            registering_service.suspend_pull_request(registering_topic.as_str(), 0, request)
        });
        checkpoint.wait();

        let (cutover_started_tx, cutover_started_rx) = std::sync::mpsc::channel();
        let (cutover_done_tx, cutover_done_rx) = std::sync::mpsc::channel();
        let cutover_handoff = Arc::clone(&handoff);
        let cutover = std::thread::spawn(move || {
            cutover_started_tx.send(()).expect("signal cutover attempt");
            let mut transaction = cutover_handoff.cutover_transaction().expect("cutover transaction");
            transaction.seal_legacy_acceptance().expect("seal legacy acceptance");
            transaction
                .publish_v2_aggregate(DeferredGenerationV2Publisher::nonblocking_atomic(|| Ok::<_, ()>(())))
                .expect("publish V2 aggregate");
            transaction.publish_default_new().expect("publish New default");
            drop(transaction);
            cutover_done_tx.send(()).expect("signal cutover completion");
        });
        cutover_started_rx.recv().expect("cutover thread started");
        assert!(
            cutover_done_rx.try_recv().is_err(),
            "cutover must wait for Pull node publication under the coordinator gate"
        );
        release.wait();
        assert!(registering.join().expect("Pull registration thread"));
        cutover.join().expect("cutover thread");
        assert_eq!(service.pull_request_table.read()[&key].len(), 1);
        assert_eq!(handoff.snapshot().occupancy, 1);

        // Leave an intentionally inconsistent table-only sentinel to prove
        // MIG-05's real table probe is serialized by the same outer gate and
        // never trusts an unlocked accounting snapshot.
        let wait = service.pull_request_table.read()[&key]
            .take_first_legacy_wait()
            .expect("published Pull node owns its affine wait lease");
        drop(wait);
        assert_eq!(handoff.snapshot().occupancy, 0);
        assert!(matches!(
            handoff.try_transition_target_to_new(target.clone(), |_| {
                service
                    .pull_request_table
                    .read()
                    .get(&key)
                    .is_some_and(|requests| !requests.is_empty())
            }),
            Err(crate::deferred_generation_handoff::DeferredGenerationTargetTransitionError::LegacyTableOccupied)
        ));

        session.close();
        assert!(service.pull_request_table.read()[&key].is_empty());
        assert!(!service.legacy_target_occupied(&target));
        let replay = handoff
            .try_transition_target_to_new(target, |_| false)
            .expect("empty Pull table may transition exactly once");
        replay.complete_after_replay_accepted();
        assert!(handoff.zero_report().is_zero());
        service.shutdown().await;
    }

    #[tokio::test]
    async fn start_shutdown_and_restart_are_serialized() {
        let processor = Arc::new(TestPullProcessor);
        let service = Arc::new(PullRequestHoldService::<StorePorts, _>::new(Arc::downgrade(&processor)));

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

    #[tokio::test]
    async fn installed_handoff_keeps_pull_filter_miss_registered() {
        let processor = Arc::new(TestPullProcessor);
        let service = Arc::new(PullRequestHoldService::<StorePorts, _>::new(Arc::downgrade(&processor)));
        let handoff = Arc::new(DeferredGenerationHandoff::new());
        service
            .install_handoff(Arc::clone(&handoff))
            .expect("install the Broker coordinator");
        PullRequestHoldService::start(&service, task_group("pull-handoff-filter-miss")).await;
        let topic = CheetahString::from_static_str("pull-filter-topic");
        let (channel, context) = test_channel_context().await;
        let request = PullRequest::new(
            RemotingCommand::create_remoting_command(0),
            channel,
            context,
            60_000,
            current_millis(),
            0,
            rocketmq_protocol::protocol::heartbeat::subscription_data::SubscriptionData::default(),
            Arc::new(RejectAllFilter),
        );
        assert!(service.suspend_pull_request(topic.as_str(), 0, request));
        assert_eq!(handoff.snapshot().occupancy, 1);

        service.notify_message_arriving_ext(&topic, 0, 1, Some(7), 0, None, None);
        let snapshot = handoff.snapshot();
        assert_eq!(snapshot.occupancy, 1);
        assert_eq!(snapshot.candidates, 0);
        assert_eq!(snapshot.active_wakes, 0);

        service.shutdown().await;
        assert!(handoff.zero_report().is_zero());
    }

    #[test]
    fn service_uses_weak_processor_back_reference() {
        let processor = Arc::new(TestPullProcessor);
        let processor_weak = Arc::downgrade(&processor);
        let service = Arc::new(PullRequestHoldService::<StorePorts, _>::new(processor_weak.clone()));

        drop(processor);

        assert!(processor_weak.upgrade().is_none());
        assert!(!service.can_accept_request());
    }

    #[test]
    fn master_online_delegates_exclusively_to_deferred_pull_producer() {
        let processor = Arc::new(TestPullProcessor);
        let service = PullRequestHoldService::<StorePorts, _>::new(Arc::downgrade(&processor));
        let produced = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&produced);
        let installed = service.install_master_online_producer(Arc::new(move || {
            observed.fetch_add(1, Ordering::Relaxed);
        }));
        assert!(installed.is_ok(), "install the Pull master-online producer once");

        service.notify_master_online();

        assert_eq!(produced.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn active_scan_does_not_keep_service_owner_alive() {
        let processor = Arc::new(TestPullProcessor);
        let service = Arc::new(PullRequestHoldService::<StorePorts, _>::new(Arc::downgrade(&processor)));
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
        let service = Arc::new(PullRequestHoldService::<StorePorts, _>::new(Arc::downgrade(&processor)));
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
