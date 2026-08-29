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
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::Weak;
use std::time::Duration;

use crate::config::broker_config::BrokerConfig;
use cheetah_string::CheetahString;
use crossbeam_skiplist::SkipSet;
use dashmap::DashMap;
use parking_lot::Mutex;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_runtime::common::time_utils::current_millis;
use rocketmq_runtime::BudgetConfigError;
use rocketmq_runtime::BudgetLimit;
use rocketmq_runtime::BudgetSnapshot;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_runtime::FullPolicy;
use rocketmq_runtime::RateLimit;
use rocketmq_runtime::ResourceBudget;
use rocketmq_runtime::ShutdownDeadline;
use rocketmq_runtime::ShutdownReport;
use rocketmq_runtime::TaskGroup;
#[cfg(test)]
use rocketmq_runtime::TaskKind;
use rocketmq_transport::api::v1::ConnectionHandlerContext;
use rocketmq_transport::api::v1::LegacySessionCleanupInstallError;
use rocketmq_transport::api::v1::LegacySessionExecutionEnrollment;
use tokio::select;
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::Notify;
use tracing::error;
use tracing::warn;

use super::LegacyExecutionTracker;
use super::LegacyServiceFinalization;
use super::LegacyServiceResourceSnapshot;
use super::LegacyServiceShutdownReport;
use crate::broker_runtime::broker_task_group_or_current;
use crate::deferred_generation_handoff::DeferredGeneration;
use crate::deferred_generation_handoff::DeferredGenerationHandoff;
use crate::deferred_generation_handoff::DeferredGenerationLegacyEnrollmentError;
use crate::deferred_generation_handoff::DeferredGenerationTarget;
use crate::deferred_generation_handoff::LegacyWakeLease;
use crate::deferred_generation_handoff::RoutePermit;
use crate::lite::lite_event_dispatcher::LiteEventDispatcher;
use crate::long_polling::polling_result::PollingResult;
use crate::long_polling::pop_request::PopRequest;

fn prune_empty_polling_queues(polling_map: &DashMap<CheetahString, SkipSet<Arc<PopRequest>>>) {
    polling_map.retain(|_, queue| !queue.is_empty());
}

fn remove_session_pop_lite_waiter(
    polling_map: &DashMap<CheetahString, SkipSet<Arc<PopRequest>>>,
    total_polling_num: &AtomicU64,
    client_id: &CheetahString,
    request: &Weak<PopRequest>,
) {
    let Some(request) = request.upgrade() else {
        return;
    };
    request.mark_legacy_session_closed();
    let removed = polling_map
        .get(client_id)
        .is_some_and(|queue| queue.remove(&request).is_some());
    if removed {
        release_published_polling_count(total_polling_num);
        request.release_resource_permit();
        request.release_legacy_wait();
    }
}

fn reserve_published_polling_count(total_polling_num: &AtomicU64) -> bool {
    total_polling_num
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| count.checked_add(1))
        .is_ok()
}

fn release_published_polling_count(total_polling_num: &AtomicU64) {
    let released = total_polling_num
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| count.checked_sub(1))
        .is_ok();
    assert!(released, "a published PopLite waiter owns one total-polling count");
}

fn restore_published_polling_count(total_polling_num: &AtomicU64) {
    assert!(
        reserve_published_polling_count(total_polling_num),
        "a requeued PopLite waiter must restore its released count"
    );
}

#[trait_variant::make(PopLiteLongPollingRequestProcessor: Send)]
pub(crate) trait LocalPopLiteLongPollingRequestProcessor {
    async fn process_request_when_wakeup(
        &self,
        channel: rocketmq_transport::api::v1::Channel,
        ctx: ConnectionHandlerContext,
        request: RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>>;
}

#[derive(Clone)]
pub(crate) struct PopLiteLongPollingPolicy {
    pop_polling_map_size: usize,
    max_pop_polling_size: u64,
    pop_polling_size: usize,
}

impl PopLiteLongPollingPolicy {
    pub(crate) fn from_config(broker_config: &BrokerConfig) -> Self {
        Self {
            pop_polling_map_size: broker_config.pop_polling_map_size,
            max_pop_polling_size: broker_config.max_pop_polling_size,
            pop_polling_size: broker_config.pop_polling_size,
        }
    }
}

#[derive(Clone)]
pub(crate) struct PopLiteLongPollingServiceContext {
    policy: PopLiteLongPollingPolicy,
    lite_event_dispatcher: LiteEventDispatcher,
    service_context: Option<ChildServiceContext>,
    request_budget: ResourceBudget,
}

impl PopLiteLongPollingServiceContext {
    pub(crate) fn try_with_resource_budget(
        policy: PopLiteLongPollingPolicy,
        lite_event_dispatcher: LiteEventDispatcher,
        service_context: Option<ChildServiceContext>,
        parent_budget: &ResourceBudget,
    ) -> Result<Self, BudgetConfigError> {
        let parent_capacity = parent_budget.limit().capacity;
        let request_count = usize::try_from(policy.max_pop_polling_size)
            .unwrap_or(usize::MAX)
            .min(parent_capacity.count)
            .max(1);
        let request_bytes = (parent_capacity.bytes / 4).max(1);
        let request_rate = u64::try_from(request_count).unwrap_or(u64::MAX).max(1);
        let request_budget = parent_budget.child(
            "lite-long-poll-requests",
            BudgetLimit::new(request_count, request_bytes, FullPolicy::Reject)
                .with_rate(RateLimit::new(request_rate, request_rate))
                .with_max_age(Duration::from_secs(30)),
        )?;
        Ok(Self {
            policy,
            lite_event_dispatcher,
            service_context,
            request_budget,
        })
    }
}

pub(crate) struct PopLiteLongPollingService<RP> {
    context: PopLiteLongPollingServiceContext,
    polling_map: Arc<DashMap<CheetahString, SkipSet<Arc<PopRequest>>>>,
    total_polling_num: Arc<AtomicU64>,
    processor: Weak<RP>,
    running: AtomicBool,
    lifecycle: AsyncMutex<()>,
    polling_admission: Mutex<()>,
    waking_clients: Arc<DashMap<CheetahString, ()>>,
    handoff: OnceLock<Arc<DeferredGenerationHandoff>>,
    producer_task_group: Mutex<Option<TaskGroup>>,
    task_group: Mutex<Option<TaskGroup>>,
    execution_tracker: Arc<LegacyExecutionTracker>,
    shutdown_wake_failures: AtomicU64,
}

#[derive(Debug, Clone)]
pub(crate) struct PopLiteLongPollingResourceSnapshot {
    pub(crate) requests: BudgetSnapshot,
    pub(crate) oldest_request_age: Option<Duration>,
    pub(crate) waking_client_count: usize,
}

struct ClientWakeupClaim {
    client_id: CheetahString,
    waking_clients: Arc<DashMap<CheetahString, ()>>,
}

struct PopLiteWakeupClaim {
    wake: Option<LegacyWakeLease>,
    execution: Option<LegacySessionExecutionEnrollment>,
}

impl Drop for ClientWakeupClaim {
    fn drop(&mut self) {
        self.waking_clients.remove(&self.client_id);
    }
}

impl<RP: PopLiteLongPollingRequestProcessor + Sync + 'static> PopLiteLongPollingService<RP> {
    pub(crate) fn new(context: PopLiteLongPollingServiceContext, processor: Weak<RP>) -> Self {
        Self {
            polling_map: Arc::new(DashMap::with_capacity(context.policy.pop_polling_map_size)),
            context,
            total_polling_num: Arc::new(AtomicU64::new(0)),
            processor,
            running: AtomicBool::new(false),
            lifecycle: AsyncMutex::new(()),
            polling_admission: Mutex::new(()),
            waking_clients: Arc::new(DashMap::new()),
            handoff: OnceLock::new(),
            producer_task_group: Mutex::new(None),
            task_group: Mutex::new(None),
            execution_tracker: Arc::new(LegacyExecutionTracker::default()),
            shutdown_wake_failures: AtomicU64::new(0),
        }
    }

    pub(crate) fn install_handoff(
        &self,
        handoff: Arc<DeferredGenerationHandoff>,
    ) -> Result<(), Arc<DeferredGenerationHandoff>> {
        let _admission = self.polling_admission.lock();
        if let Some(installed) = self.handoff.get() {
            return if Arc::ptr_eq(installed, &handoff) {
                Ok(())
            } else {
                Err(handoff)
            };
        }
        if self.total_polling_num.load(Ordering::Acquire) != 0 || !self.waking_clients.is_empty() {
            return Err(handoff);
        }
        self.handoff.set(handoff)
    }

    pub(crate) async fn start(this: &Arc<Self>) {
        let _lifecycle = this.lifecycle.lock().await;
        if this
            .running
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }

        let Some(producer_task_group) = broker_task_group_or_current(
            this.context.service_context.as_ref(),
            "rocketmq-broker.long-polling.pop-lite.producer",
            "failed to start PopLiteLongPollingService outside Tokio runtime",
        ) else {
            this.running.store(false, Ordering::Release);
            return;
        };
        let Some(execution_task_group) = broker_task_group_or_current(
            this.context.service_context.as_ref(),
            "rocketmq-broker.long-polling.pop-lite.executions",
            "failed to start PopLiteLongPollingService execution owner outside Tokio runtime",
        ) else {
            this.running.store(false, Ordering::Release);
            return;
        };
        let cancellation_token = producer_task_group.cancellation_token();
        let service = Arc::downgrade(this);
        let wakeup_notify = Arc::new(Notify::new());
        let task_wakeup_notify = wakeup_notify.clone();
        *this.producer_task_group.lock() = Some(producer_task_group.clone());
        *this.task_group.lock() = Some(execution_task_group);
        this.shutdown_wake_failures.store(0, Ordering::Release);

        let spawn_result = producer_task_group.spawn_service("broker.long-polling.pop-lite.scan", async move {
            loop {
                select! {
                    _ = cancellation_token.cancelled() => { break; }
                    _ = task_wakeup_notify.notified() => {}
                    _ = tokio::time::sleep(tokio::time::Duration::from_millis(20)) => {}
                }

                let Some(service) = service.upgrade() else {
                    break;
                };

                for client_id in service.context.lite_event_dispatcher.pending_client_ids() {
                    service.wake_up_client(&client_id);
                }

                if service.polling_map.is_empty() {
                    continue;
                }
                let client_ids = service
                    .polling_map
                    .iter()
                    .map(|entry| entry.key().clone())
                    .collect::<Vec<_>>();
                for client_id in client_ids {
                    service.wake_up_expired_requests(&client_id);
                }
                prune_empty_polling_queues(&service.polling_map);
            }

            if let Some(service) = service.upgrade() {
                service.running.store(false, Ordering::Release);
            }
        });

        if let Err(error) = spawn_result {
            this.producer_task_group.lock().take();
            this.task_group.lock().take();
            this.running.store(false, Ordering::Release);
            warn!(?error, "failed to spawn PopLiteLongPollingService scan task");
            return;
        }

        this.context.lite_event_dispatcher.set_wakeup_notify(wakeup_notify);
    }

    pub(crate) async fn stop_producer_until(&self, deadline: ShutdownDeadline) -> Option<ShutdownReport> {
        let _lifecycle = self.lifecycle.lock().await;
        {
            let _admission = self.polling_admission.lock();
            self.running.store(false, Ordering::Release);
        }
        self.context.lite_event_dispatcher.clear_wakeup_notify();
        let task_group = self.producer_task_group.lock().take();
        match task_group {
            Some(task_group) => Some(task_group.shutdown_until(deadline).await),
            None => None,
        }
    }

    pub(crate) async fn drain_executions_until(&self, deadline: ShutdownDeadline) -> Option<ShutdownReport> {
        let _lifecycle = self.lifecycle.lock().await;
        let task_group = self.task_group.lock().take();
        match task_group {
            Some(task_group) => Some(task_group.shutdown_until(deadline).await),
            None => None,
        }
    }

    pub(crate) async fn finalize_shutdown(&self) -> LegacyServiceFinalization {
        let _lifecycle = self.lifecycle.lock().await;
        let observed_after_session_drain = self.legacy_resource_snapshot();
        let keys = self
            .polling_map
            .iter()
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>();
        let retired = keys
            .into_iter()
            .filter_map(|key| self.polling_map.remove(&key).map(|(_, queue)| queue))
            .collect::<Vec<_>>();
        // Drop request cleanup enrollments only after every DashMap shard
        // guard has been released. Registration publishes cleanup -> table,
        // so fallback retirement must not hold table -> cleanup.
        for queue in retired {
            self.drain_polling_queue(&queue);
        }
        self.context.lite_event_dispatcher.clear_wakeup_notify();
        self.waking_clients.clear();
        self.running.store(false, Ordering::Release);
        LegacyServiceFinalization {
            observed_after_session_drain,
            terminal: self.legacy_resource_snapshot(),
        }
    }

    pub(crate) async fn shutdown(&self) -> LegacyServiceShutdownReport {
        let deadline = ShutdownDeadline::after(Duration::from_secs(5));
        let producer = self.stop_producer_until(deadline).await;
        let executions = self.drain_executions_until(deadline).await;
        let finalization = self.finalize_shutdown().await;
        LegacyServiceShutdownReport {
            name: "pop_lite_long_polling",
            producer,
            executions,
            observed_after_session_drain: finalization.observed_after_session_drain,
            resources: finalization.terminal,
        }
    }

    pub(crate) fn legacy_resource_snapshot(&self) -> LegacyServiceResourceSnapshot {
        let budget = self.context.request_budget.snapshot();
        LegacyServiceResourceSnapshot {
            table_entries: self.polling_map.iter().map(|queue| queue.value().len()).sum(),
            tracked_waiters: self.total_polling_num.load(Ordering::Acquire),
            request_budget_count: budget.current_count,
            request_budget_bytes: budget.current_bytes,
            waking_clients: self.waking_clients.len(),
            active_executions: self.execution_tracker.active(),
            task_count: self
                .producer_task_group
                .lock()
                .as_ref()
                .map_or(0, TaskGroup::task_count),
            wake_task_count: self.task_group.lock().as_ref().map_or(0, TaskGroup::task_count),
            shutdown_wake_failures: self.shutdown_wake_failures.load(Ordering::Acquire),
        }
    }

    pub(crate) fn polling(
        &self,
        ctx: ConnectionHandlerContext,
        remoting_command: &mut RemotingCommand,
        client_id: &CheetahString,
        born_time: i64,
        poll_time: i64,
    ) -> PollingResult {
        if poll_time <= 0 {
            return PollingResult::NotPolling;
        }
        if !self.running.load(Ordering::Acquire) {
            return PollingResult::PollingTimeout;
        }

        let requested_expiry = born_time.saturating_add(poll_time);
        let max_age_millis = self
            .context
            .request_budget
            .limit()
            .max_age
            .and_then(|age| u64::try_from(age.as_millis()).ok())
            .unwrap_or(30_000);
        let expired = u64::try_from(requested_expiry)
            .unwrap_or_default()
            .min(current_millis().saturating_add(max_age_millis));
        let retained_bytes = PopRequest::estimated_retained_bytes(remoting_command);
        let permit = match self.context.request_budget.try_acquire_data(retained_bytes) {
            Ok(permit) => permit,
            Err(_) => return PollingResult::PollingFull,
        };
        let request = Arc::new(PopRequest::new_with_resource_permit(
            remoting_command.clone(),
            ctx,
            expired,
            None,
            None,
            permit,
        ));
        let _admission = self.polling_admission.lock();
        if !self.running.load(Ordering::Acquire) {
            return PollingResult::PollingTimeout;
        }

        if self.total_polling_num.load(Ordering::SeqCst) >= self.context.policy.max_pop_polling_size {
            return PollingResult::PollingFull;
        }

        if request.is_timeout() {
            return PollingResult::PollingTimeout;
        }

        prune_empty_polling_queues(&self.polling_map);
        if !self.polling_map.contains_key(client_id)
            && self.polling_map.len() >= self.context.policy.pop_polling_map_size
        {
            return PollingResult::PollingFull;
        }
        if let Some(handoff) = self.handoff.get() {
            let target = DeferredGenerationTarget::pop_lite(client_id.clone());
            let rollback_map = Arc::clone(&self.polling_map);
            let rollback_total = Arc::clone(&self.total_polling_num);
            let rollback_client_id = client_id.clone();
            let rollback_request = Arc::downgrade(&request);
            let enrollment = handoff.arrival_adapter().install_legacy_wait(
                target.clone(),
                |lease| {
                    if !self.polling_map.contains_key(client_id)
                        && self.polling_map.len() >= self.context.policy.pop_polling_map_size
                    {
                        return Err((PollingResult::PollingFull, lease));
                    }
                    let queue = self.polling_map.entry(client_id.clone()).or_default();
                    if queue.len() >= self.context.policy.pop_polling_size
                        || self.total_polling_num.load(Ordering::SeqCst) >= self.context.policy.max_pop_polling_size
                    {
                        return Err((PollingResult::PollingFull, lease));
                    }
                    request
                        .install_legacy_handoff(&target, lease)
                        .map_err(|lease| (PollingResult::PollingTimeout, lease))?;
                    let cleanup_map = Arc::clone(&self.polling_map);
                    let cleanup_total = Arc::clone(&self.total_polling_num);
                    let cleanup_client_id = client_id.clone();
                    let cleanup_request = Arc::downgrade(&request);
                    match request.get_ctx().install_legacy_session_execution(
                        move || {
                            remove_session_pop_lite_waiter(
                                &cleanup_map,
                                &cleanup_total,
                                &cleanup_client_id,
                                &cleanup_request,
                            );
                        },
                        |cleanup| {
                            if !reserve_published_polling_count(&self.total_polling_num) {
                                return Err((PollingResult::PollingFull, cleanup));
                            }
                            if let Err(cleanup) = request.install_legacy_session_cleanup(cleanup) {
                                release_published_polling_count(&self.total_polling_num);
                                return Err((PollingResult::PollingTimeout, cleanup));
                            }
                            remoting_command.set_suspended_ref(true);
                            queue.insert(Arc::clone(&request));
                            Ok(())
                        },
                    ) {
                        Ok(()) => Ok(()),
                        Err(LegacySessionCleanupInstallError::Unavailable) => {
                            #[cfg(test)]
                            {
                                if !reserve_published_polling_count(&self.total_polling_num) {
                                    let lease = request
                                        .take_legacy_wait()
                                        .expect("unavailable cleanup retains the fresh PopLite wait lease");
                                    return Err((PollingResult::PollingFull, lease));
                                }
                                remoting_command.set_suspended_ref(true);
                                queue.insert(Arc::clone(&request));
                                Ok(())
                            }
                            #[cfg(not(test))]
                            {
                                let lease = request
                                    .take_legacy_wait()
                                    .expect("unavailable cleanup retains the fresh PopLite wait lease");
                                Err((PollingResult::PollingTimeout, lease))
                            }
                        }
                        Err(_) => {
                            let lease = request
                                .take_legacy_wait()
                                .expect("failed cleanup installation retains the fresh wait lease");
                            Err((PollingResult::PollingTimeout, lease))
                        }
                    }
                },
                move || {
                    remove_session_pop_lite_waiter(
                        &rollback_map,
                        &rollback_total,
                        &rollback_client_id,
                        &rollback_request,
                    );
                },
            );
            match enrollment {
                Ok(()) => {}
                Err(DeferredGenerationLegacyEnrollmentError::Enrollment(result)) => return result,
                Err(_) => return PollingResult::PollingTimeout,
            }
        } else {
            let queue = self.polling_map.entry(client_id.clone()).or_default();
            if queue.len() >= self.context.policy.pop_polling_size {
                return PollingResult::PollingFull;
            }
            if !reserve_published_polling_count(&self.total_polling_num) {
                return PollingResult::PollingFull;
            }
            remoting_command.set_suspended_ref(true);
            queue.insert(request);
        }
        PollingResult::PollingSuc
    }

    pub(crate) fn wake_up_client(&self, client_id: &CheetahString) -> bool {
        let _admission = self.polling_admission.lock();
        if !self.running.load(Ordering::Acquire) {
            return false;
        }
        if self.waking_clients.insert(client_id.clone(), ()).is_some() {
            return false;
        }
        let claim = ClientWakeupClaim {
            client_id: client_id.clone(),
            waking_clients: self.waking_clients.clone(),
        };
        let Some((pop_request, route)) = self.claim_request(client_id, None) else {
            return false;
        };
        let Some(wake_claim) = self.begin_wake(&pop_request, route) else {
            self.requeue_claimed_request(client_id, pop_request);
            return false;
        };
        self.wake_up_with_claim(pop_request, Some(claim), wake_claim)
    }

    fn requeue_claimed_request(&self, client_id: &CheetahString, request: Arc<PopRequest>) {
        // Restore accounting before publishing the node, matching fresh
        // registration. A concurrent cleanup can then remove the node without
        // observing an uncounted waiter.
        restore_published_polling_count(&self.total_polling_num);
        self.polling_map
            .entry(client_id.clone())
            .or_default()
            .insert(Arc::clone(&request));
        let _ = self.retract_terminal_requeue(client_id, &request);
    }

    fn retract_terminal_requeue(&self, client_id: &CheetahString, request: &Arc<PopRequest>) -> bool {
        if !request.legacy_session_closed() {
            return false;
        }
        let removed = self
            .polling_map
            .get(client_id)
            .is_some_and(|queue| queue.remove(request).is_some());
        // Cleanup and terminal reread race to remove the exact Arc. Only the
        // winner owns count, retained-budget, and handoff release.
        if removed {
            release_published_polling_count(&self.total_polling_num);
            request.release_resource_permit();
            request.release_legacy_wait();
        }
        removed
    }

    fn wake_up(&self, pop_request: Arc<PopRequest>) -> bool {
        let _admission = self.polling_admission.lock();
        if !self.running.load(Ordering::Acquire) {
            return false;
        }
        let Some(route) = self.acquire_route_for_request(&pop_request) else {
            return false;
        };
        let Some(wake_claim) = self.begin_wake(&pop_request, route) else {
            return false;
        };
        self.wake_up_with_claim(pop_request, None, wake_claim)
    }

    fn wake_up_with_claim(
        &self,
        pop_request: Arc<PopRequest>,
        client_wakeup_claim: Option<ClientWakeupClaim>,
        wake_claim: PopLiteWakeupClaim,
    ) -> bool {
        let PopLiteWakeupClaim { wake, execution } = wake_claim;
        pop_request.release_resource_permit();
        if !pop_request.complete() {
            return false;
        }
        match self.processor.upgrade() {
            None => {
                self.shutdown_wake_failures.fetch_add(1, Ordering::AcqRel);
                false
            }
            Some(processor) => {
                let continuation = wake.map(LegacyWakeLease::into_continuation);
                let execution_guard = self.execution_tracker.enter();
                let task = async move {
                    let _execution_guard = execution_guard;
                    let _continuation = continuation;
                    let _client_wakeup_claim = client_wakeup_claim;
                    let channel = pop_request.get_channel().clone();
                    let ctx = pop_request.get_ctx().clone();
                    let opaque = pop_request.get_remoting_command().opaque();
                    let response = processor
                        .process_request_when_wakeup(channel, ctx, pop_request.get_remoting_command().clone())
                        .await;
                    match response {
                        Ok(result) => {
                            if let Some(mut response) = result {
                                let channel = pop_request.get_channel();
                                response.set_opaque_mut(opaque);
                                let _ = channel.channel_inner().send_oneway(response, 1000).await;
                            }
                        }
                        Err(error) => {
                            error!("Execute pop-lite request when wakeup run {}", error);
                        }
                    }
                };

                if let Some(execution) = execution {
                    if let Err(error) = execution.try_execute(task) {
                        self.shutdown_wake_failures.fetch_add(1, Ordering::AcqRel);
                        warn!(?error, "canonical session rejected PopLiteLongPollingService wake-up");
                        return false;
                    }
                    return true;
                }

                #[cfg(test)]
                {
                    let task_group = self.task_group.lock().as_ref().cloned();
                    let Some(task_group) = task_group else {
                        self.shutdown_wake_failures.fetch_add(1, Ordering::AcqRel);
                        warn!("PopLiteLongPollingService test wake-up owner is not running");
                        return false;
                    };
                    if let Err(error) =
                        task_group.spawn("broker.long-polling.pop-lite.test-wake-up", TaskKind::Worker, task)
                    {
                        self.shutdown_wake_failures.fetch_add(1, Ordering::AcqRel);
                        warn!(?error, "failed to spawn PopLiteLongPollingService test wake-up task");
                        return false;
                    }
                    true
                }
                #[cfg(not(test))]
                {
                    self.shutdown_wake_failures.fetch_add(1, Ordering::AcqRel);
                    warn!("PopLiteLongPollingService wake-up has no canonical session owner");
                    false
                }
            }
        }
    }

    fn acquire_route(&self, client_id: &CheetahString) -> Option<Option<RoutePermit>> {
        let Some(handoff) = self.handoff.get() else {
            return Some(None);
        };
        let route = handoff
            .acquire_route(DeferredGenerationTarget::pop_lite(client_id.clone()))
            .ok()?;
        if route.generation() != DeferredGeneration::Legacy {
            return None;
        }
        Some(Some(route))
    }

    fn acquire_route_for_request(&self, request: &PopRequest) -> Option<Option<RoutePermit>> {
        let Some(handoff) = self.handoff.get() else {
            return Some(None);
        };
        let target = request.legacy_handoff_target()?;
        let route = handoff.acquire_route(target).ok()?;
        if route.generation() != DeferredGeneration::Legacy {
            return None;
        }
        Some(Some(route))
    }

    fn begin_wake(&self, request: &PopRequest, route: Option<RoutePermit>) -> Option<PopLiteWakeupClaim> {
        let Some(route) = route else {
            return Some(PopLiteWakeupClaim {
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
                Some(PopLiteWakeupClaim {
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

    fn poll_request(&self, remoting_commands: &SkipSet<Arc<PopRequest>>) -> Option<Arc<PopRequest>> {
        if remoting_commands.is_empty() {
            return None;
        }

        loop {
            let pop_request = remoting_commands.pop_front().map(|entry| entry.value().clone())?;
            release_published_polling_count(&self.total_polling_num);
            if !pop_request.get_channel().connection_ref().is_healthy() {
                pop_request.release_resource_permit();
                pop_request.release_legacy_wait();
                continue;
            }
            return Some(pop_request);
        }
    }

    fn claim_request(
        &self,
        client_id: &CheetahString,
        expected: Option<&Arc<PopRequest>>,
    ) -> Option<(Arc<PopRequest>, Option<RoutePermit>)> {
        loop {
            let candidate = match expected {
                Some(request) => Arc::clone(request),
                None => self
                    .polling_map
                    .get(client_id)
                    .and_then(|queue| queue.front().map(|entry| entry.value().clone()))?,
            };
            let (request, route) = if let Some(handoff) = self.handoff.get() {
                let target = DeferredGenerationTarget::pop_lite(client_id.clone());
                let mut claimed = handoff
                    .arrival_adapter()
                    .claim_legacy_table(
                        target,
                        |claimed| {
                            let Some(queue) = self.polling_map.get(client_id) else {
                                return;
                            };
                            if queue.remove(&candidate).is_some() {
                                claimed.push(Arc::clone(&candidate));
                            }
                        },
                        |requests| {
                            let queue = self.polling_map.entry(client_id.clone()).or_default();
                            for request in requests {
                                queue.insert(request);
                            }
                        },
                    )
                    .ok()?;
                let (request, route) = claimed.pop()?;
                (request, Some(route))
            } else {
                let queue = self.polling_map.get(client_id)?;
                let request = if expected.is_some() {
                    queue.remove(&candidate).map(|entry| entry.value().clone())?
                } else {
                    queue.pop_front().map(|entry| entry.value().clone())?
                };
                (request, None)
            };
            release_published_polling_count(&self.total_polling_num);
            if !request.get_channel().connection_ref().is_healthy() {
                request.release_resource_permit();
                request.release_legacy_wait();
                if expected.is_some() {
                    return None;
                }
                continue;
            }
            return Some((request, route));
        }
    }

    fn wake_up_expired_requests(&self, client_id: &CheetahString) {
        let _admission = self.polling_admission.lock();
        if !self.running.load(Ordering::Acquire) {
            return;
        }
        loop {
            let Some(candidate) = self
                .polling_map
                .get(client_id)
                .and_then(|queue| queue.front().map(|entry| entry.value().clone()))
            else {
                break;
            };
            if !candidate.is_timeout() {
                break;
            }
            let Some((first, route)) = self.claim_request(client_id, Some(&candidate)) else {
                continue;
            };
            if let Some(wake) = self.begin_wake(&first, route) {
                self.wake_up_with_claim(first, None, wake);
            } else {
                self.requeue_claimed_request(client_id, first);
            }
        }
    }

    fn drain_polling_queue(&self, queue: &SkipSet<Arc<PopRequest>>) {
        while let Some(first) = queue.pop_front() {
            release_published_polling_count(&self.total_polling_num);
            first.value().release_resource_permit();
            first.value().release_legacy_session_cleanup();
            first.value().release_legacy_wait();
        }
    }

    #[inline]
    pub(crate) fn get_polling_num(&self, key: &str) -> i32 {
        self.polling_map.get(key).map(|queue| queue.len() as i32).unwrap_or(0)
    }

    pub(crate) fn legacy_target_occupied(&self, target: &DeferredGenerationTarget) -> bool {
        let DeferredGenerationTarget::PopLite { client_id } = target else {
            return false;
        };
        let _admission = self.polling_admission.lock();
        self.polling_map.get(client_id).is_some_and(|queue| !queue.is_empty())
            || self.waking_clients.contains_key(client_id)
    }

    pub(crate) fn resource_snapshot(&self) -> PopLiteLongPollingResourceSnapshot {
        let oldest_request_age = self.polling_map.iter().fold(None::<Duration>, |oldest, queue| {
            queue.value().iter().fold(oldest, |oldest, request| {
                let age = request.value().age();
                Some(oldest.map_or(age, |current| current.max(age)))
            })
        });
        PopLiteLongPollingResourceSnapshot {
            requests: self.context.request_budget.snapshot(),
            oldest_request_age,
            waking_client_count: self.waking_clients.len(),
        }
    }

    #[cfg(test)]
    pub(crate) fn is_running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(crate) fn task_group_for_test(&self) -> Option<TaskGroup> {
        self.task_group.lock().as_ref().cloned()
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read;
    use std::sync::atomic::AtomicUsize;

    use rocketmq_protocol::code::request_code::RequestCode;
    use rocketmq_runtime::MonotonicClock;
    use rocketmq_runtime::ResourceBudgetTree;
    use rocketmq_store::MessageStoreConfig;
    use rocketmq_transport::api::v1::Channel;
    use rocketmq_transport::api::v1::ConnectionHandlerContextWrapper;
    use rocketmq_transport::test_support::Connection;
    use rocketmq_transport::test_support::LegacySessionExecutionHarness;
    use rocketmq_transport::test_support::TestChannelBuilder;

    use super::*;
    use crate::broker_runtime::BrokerRuntime;
    use crate::deferred_generation_handoff::DeferredGenerationV2Publisher;

    #[derive(Default)]
    struct ManualClock {
        millis: AtomicU64,
    }

    impl ManualClock {
        fn advance(&self, duration: Duration) {
            self.millis.fetch_add(
                duration.as_millis().try_into().expect("test duration fits u64"),
                Ordering::AcqRel,
            );
        }
    }

    impl MonotonicClock for ManualClock {
        fn now(&self) -> Duration {
            Duration::from_millis(self.millis.load(Ordering::Acquire))
        }
    }

    struct TestProcessor;

    struct BlockingProcessor {
        calls: AtomicUsize,
        started: Notify,
        release: Notify,
    }

    struct ImmediateResponseProcessor {
        calls: AtomicUsize,
    }

    impl PopLiteLongPollingRequestProcessor for TestProcessor {
        async fn process_request_when_wakeup(
            &self,
            _channel: Channel,
            _ctx: ConnectionHandlerContext,
            _request: RemotingCommand,
        ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
            Ok(None)
        }
    }

    impl PopLiteLongPollingRequestProcessor for BlockingProcessor {
        async fn process_request_when_wakeup(
            &self,
            _channel: Channel,
            _ctx: ConnectionHandlerContext,
            _request: RemotingCommand,
        ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
            self.calls.fetch_add(1, Ordering::AcqRel);
            self.started.notify_one();
            self.release.notified().await;
            Ok(None)
        }
    }

    impl PopLiteLongPollingRequestProcessor for ImmediateResponseProcessor {
        async fn process_request_when_wakeup(
            &self,
            _channel: Channel,
            _ctx: ConnectionHandlerContext,
            _request: RemotingCommand,
        ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
            self.calls.fetch_add(1, Ordering::AcqRel);
            Ok(Some(RemotingCommand::create_remoting_command(0).mark_response_type()))
        }
    }

    fn budgeted_service() -> (PopLiteLongPollingService<TestProcessor>, Arc<ManualClock>) {
        let clock = Arc::new(ManualClock::default());
        let tree = ResourceBudgetTree::with_clock(
            "pop-lite-pinned-node",
            BudgetLimit::new(1, 64 * 1024, FullPolicy::Reject),
            clock.clone(),
        )
        .expect("root budget");
        let context = PopLiteLongPollingServiceContext::try_with_resource_budget(
            PopLiteLongPollingPolicy {
                pop_polling_map_size: 1,
                max_pop_polling_size: 1,
                pop_polling_size: 1,
            },
            LiteEventDispatcher::default(),
            None,
            &tree.root(),
        )
        .expect("PopLite request budget");
        let service = PopLiteLongPollingService::new(context, Weak::new());
        service.running.store(true, Ordering::Release);
        (service, clock)
    }

    async fn test_context() -> ConnectionHandlerContext {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind local test listener");
        let local_addr = listener.local_addr().expect("local listener address");
        let stream = std::net::TcpStream::connect(local_addr).expect("connect local test listener");
        stream.set_nonblocking(true).expect("set nonblocking");
        let stream = tokio::net::TcpStream::from_std(stream).expect("convert TCP stream");
        let connection = Connection::new(stream);
        let channel = rocketmq_transport::test_support::TestChannelBuilder::new(
            connection,
            crate::test_task_group("pop-lite-pinned-node-channel"),
        )
        .addresses(local_addr, local_addr)
        .build()
        .expect("build test channel");
        Arc::new(ConnectionHandlerContextWrapper::new(channel))
    }

    async fn session_execution_test_context(
        owner_id: u64,
        writer_barrier: Option<(Arc<Notify>, Arc<Notify>)>,
    ) -> (
        LegacySessionExecutionHarness,
        TaskGroup,
        ConnectionHandlerContext,
        std::net::TcpStream,
    ) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind PopLite session listener");
        let local_addr = listener.local_addr().expect("PopLite session listener address");
        let stream = std::net::TcpStream::connect(local_addr).expect("connect PopLite session peer");
        let (peer, _) = listener.accept().expect("accept PopLite session peer");
        peer.set_nonblocking(true)
            .expect("set PopLite session peer nonblocking");
        stream
            .set_nonblocking(true)
            .expect("set PopLite session stream nonblocking");
        let stream = tokio::net::TcpStream::from_std(stream).expect("convert PopLite session stream");
        let connection = Connection::new(stream);
        let mut builder = TestChannelBuilder::new(
            connection,
            crate::test_task_group(format!("pop-lite-session-channel-{owner_id}")),
        )
        .addresses(local_addr, local_addr);
        if let Some((entered, release)) = writer_barrier {
            builder = builder.write_preflight_barrier(entered, release);
        }
        let channel = builder.build().expect("build PopLite session channel");
        let session_group = crate::test_task_group(format!("pop-lite-session-execution-{owner_id}"));
        let session = LegacySessionExecutionHarness::new(owner_id, &session_group);
        let context = session.context(channel, 4 * 1024, RequestCode::PopLiteMessage.to_i32());
        (session, session_group, context, peer)
    }

    fn session_test_service<RP>(processor: &Arc<RP>) -> Arc<PopLiteLongPollingService<RP>>
    where
        RP: PopLiteLongPollingRequestProcessor + Sync + 'static,
    {
        let tree = ResourceBudgetTree::new(
            "pop-lite-session-execution",
            BudgetLimit::new(8, 512 * 1024, FullPolicy::Reject),
        )
        .expect("PopLite session root budget");
        let context = PopLiteLongPollingServiceContext::try_with_resource_budget(
            PopLiteLongPollingPolicy {
                pop_polling_map_size: 4,
                max_pop_polling_size: 8,
                pop_polling_size: 8,
            },
            LiteEventDispatcher::default(),
            None,
            &tree.root(),
        )
        .expect("PopLite session request budget");
        let service = Arc::new(PopLiteLongPollingService::new(context, Arc::downgrade(processor)));
        service.running.store(true, Ordering::Release);
        service
    }

    fn register_session_waiter<RP>(
        service: &PopLiteLongPollingService<RP>,
        context: ConnectionHandlerContext,
        client_id: &CheetahString,
    ) -> Arc<PopRequest>
    where
        RP: PopLiteLongPollingRequestProcessor + Sync + 'static,
    {
        let mut command = RemotingCommand::create_remoting_command(RequestCode::PopLiteMessage);
        assert_eq!(
            service.polling(
                context,
                &mut command,
                client_id,
                i64::try_from(current_millis()).expect("test clock fits i64"),
                30_000,
            ),
            PollingResult::PollingSuc
        );
        service
            .polling_map
            .get(client_id)
            .and_then(|queue| queue.front().map(|entry| Arc::clone(entry.value())))
            .expect("registered PopLite session waiter")
    }

    fn assert_peer_received_no_bytes(peer: &mut std::net::TcpStream) {
        let mut byte = [0_u8; 1];
        let error = peer
            .read(&mut byte)
            .expect_err("cancelled PopLite session wrote no response bytes");
        assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);
    }

    #[tokio::test]
    async fn pop_lite_terminal_requeue_races_have_one_release_owner() {
        let processor = Arc::new(ImmediateResponseProcessor {
            calls: AtomicUsize::new(0),
        });
        let service = session_test_service(&processor);
        let handoff = Arc::new(DeferredGenerationHandoff::new());
        service
            .install_handoff(Arc::clone(&handoff))
            .expect("install PopLite terminal requeue coordinator");
        let client_id = CheetahString::from_static_str("pop-lite-terminal-requeue");

        let (missed_session, missed_group, missed_context, _missed_peer) =
            session_execution_test_context(8_411, None).await;
        let registered = register_session_waiter(&service, missed_context, &client_id);
        let (request, route) = service
            .claim_request(&client_id, Some(&registered))
            .expect("claim PopLite waiter before terminal cleanup misses the table");
        missed_session.close();
        service.requeue_claimed_request(&client_id, request);
        drop(route);
        assert_eq!(service.get_polling_num(&client_id), 0);
        assert!(handoff.zero_report().is_zero());
        let report = missed_group
            .shutdown_until(ShutdownDeadline::after(Duration::from_secs(1)))
            .await;
        assert!(report.is_healthy(), "{}", report.to_json());

        let (winning_session, winning_group, winning_context, _winning_peer) =
            session_execution_test_context(8_412, None).await;
        let registered = register_session_waiter(&service, winning_context, &client_id);
        let (request, route) = service
            .claim_request(&client_id, Some(&registered))
            .expect("claim PopLite waiter before terminal publication races the reread");
        restore_published_polling_count(&service.total_polling_num);
        service
            .polling_map
            .entry(client_id.clone())
            .or_default()
            .insert(Arc::clone(&request));
        winning_session.close();
        assert!(
            !service.retract_terminal_requeue(&client_id, &request),
            "session cleanup already owns the exact published PopLite waiter"
        );
        drop(route);
        assert_eq!(service.get_polling_num(&client_id), 0);
        assert!(handoff.zero_report().is_zero());
        let report = winning_group
            .shutdown_until(ShutdownDeadline::after(Duration::from_secs(1)))
            .await;
        assert!(report.is_healthy(), "{}", report.to_json());

        service.shutdown().await;
    }

    #[tokio::test]
    async fn pop_lite_session_close_after_claim_runs_no_handler_or_writer() {
        let processor = Arc::new(ImmediateResponseProcessor {
            calls: AtomicUsize::new(0),
        });
        let service = session_test_service(&processor);
        let handoff = Arc::new(DeferredGenerationHandoff::new());
        service
            .install_handoff(Arc::clone(&handoff))
            .expect("install PopLite session coordinator");
        let client_id = CheetahString::from_static_str("pop-lite-session-claimed");
        let (session, session_group, context, mut peer) = session_execution_test_context(8_401, None).await;
        let registered = register_session_waiter(&service, context, &client_id);
        let (request, route) = service
            .claim_request(&client_id, Some(&registered))
            .expect("claim exact PopLite session waiter");
        assert_eq!(service.get_polling_num(&client_id), 0);

        session.close();
        let claim = service
            .begin_wake(&request, route)
            .expect("closed PopLite claim retains its affine execution enrollment");
        assert!(!service.wake_up_with_claim(request, None, claim));
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
    async fn pop_lite_session_close_before_first_handler_poll_runs_no_handler_or_writer() {
        let processor = Arc::new(ImmediateResponseProcessor {
            calls: AtomicUsize::new(0),
        });
        let service = session_test_service(&processor);
        let handoff = Arc::new(DeferredGenerationHandoff::new());
        service
            .install_handoff(Arc::clone(&handoff))
            .expect("install PopLite first-poll coordinator");
        let client_id = CheetahString::from_static_str("pop-lite-session-first-poll");
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let (session, session_group, context, mut peer) = session_execution_test_context(8_402, None).await;
        session.set_first_poll_gate(Arc::clone(&entered), Arc::clone(&release));
        register_session_waiter(&service, context, &client_id);

        assert!(service.wake_up_client(&client_id));
        tokio::time::timeout(Duration::from_secs(1), entered.notified())
            .await
            .expect("session executor accepted PopLite wake before handler poll");
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
    async fn pop_lite_session_close_at_writer_preflight_writes_no_bytes() {
        let processor = Arc::new(ImmediateResponseProcessor {
            calls: AtomicUsize::new(0),
        });
        let service = session_test_service(&processor);
        let handoff = Arc::new(DeferredGenerationHandoff::new());
        service
            .install_handoff(Arc::clone(&handoff))
            .expect("install PopLite writer coordinator");
        let client_id = CheetahString::from_static_str("pop-lite-session-writer");
        let writer_entered = Arc::new(Notify::new());
        let writer_release = Arc::new(Notify::new());
        let (session, session_group, context, mut peer) =
            session_execution_test_context(8_403, Some((Arc::clone(&writer_entered), Arc::clone(&writer_release))))
                .await;
        register_session_waiter(&service, context, &client_id);

        assert!(service.wake_up_client(&client_id));
        tokio::time::timeout(Duration::from_secs(1), writer_entered.notified())
            .await
            .expect("PopLite response reached canonical writer preflight");
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
    async fn pop_lite_registration_and_transition_use_gate_then_table_order() {
        let processor = Arc::new(TestProcessor);
        let tree = ResourceBudgetTree::new(
            "pop-lite-gate-order",
            BudgetLimit::new(4, 256 * 1024, FullPolicy::Reject),
        )
        .expect("root budget");
        let context = PopLiteLongPollingServiceContext::try_with_resource_budget(
            PopLiteLongPollingPolicy {
                pop_polling_map_size: 2,
                max_pop_polling_size: 4,
                pop_polling_size: 4,
            },
            LiteEventDispatcher::default(),
            None,
            &tree.root(),
        )
        .expect("PopLite request budget");
        let service = Arc::new(PopLiteLongPollingService::new(context, Arc::downgrade(&processor)));
        let handoff = Arc::new(DeferredGenerationHandoff::new());
        service
            .install_handoff(Arc::clone(&handoff))
            .expect("install the Broker coordinator");
        service.running.store(true, Ordering::Release);
        let client_id = CheetahString::from_static_str("gate-order-pop-lite-client");
        let execution_group = crate::test_task_group("gate-order-pop-lite-session");
        let session = LegacySessionExecutionHarness::new(8_250, &execution_group);
        let base_context = test_context().await;
        let context = session.context(base_context.channel().clone(), 0, 0);
        let entered = Arc::new(std::sync::Barrier::new(2));
        let release = Arc::new(std::sync::Barrier::new(2));
        let checkpoint_entered = Arc::clone(&entered);
        let checkpoint_release = Arc::clone(&release);
        session.set_insert_checkpoint(move |state_locked| {
            assert!(state_locked);
            checkpoint_entered.wait();
            checkpoint_release.wait();
        });

        let registering_service = Arc::clone(&service);
        let registering_client = client_id.clone();
        let registering = std::thread::spawn(move || {
            let mut command = RemotingCommand::create_remoting_command(0);
            registering_service.polling(
                context,
                &mut command,
                &registering_client,
                i64::try_from(current_millis()).expect("test clock fits i64"),
                30_000,
            )
        });
        entered.wait();

        let cutover_handoff = Arc::clone(&handoff);
        let (cutover_started_tx, cutover_started_rx) = std::sync::mpsc::channel();
        let cutover = std::thread::spawn(move || {
            cutover_started_tx.send(()).expect("signal cutover attempt");
            let mut transaction = cutover_handoff.cutover_transaction().expect("cutover transaction");
            transaction.seal_legacy_acceptance().expect("seal legacy acceptance");
            transaction
                .publish_v2_aggregate(DeferredGenerationV2Publisher::nonblocking_atomic(|| Ok::<_, ()>(())))
                .expect("publish aggregate");
            transaction.publish_default_new().expect("publish New default");
        });
        cutover_started_rx.recv().expect("cutover started");
        assert!(!cutover.is_finished(), "cutover must wait behind registration's gate");
        release.wait();
        assert_eq!(
            registering.join().expect("registration thread"),
            PollingResult::PollingSuc
        );
        cutover.join().expect("cutover thread");

        let target = DeferredGenerationTarget::pop_lite(client_id.clone());
        assert!(service.legacy_target_occupied(&target));
        assert!(
            !service.legacy_target_occupied(&DeferredGenerationTarget::pop_lite(CheetahString::from_static_str(
                "unrelated-pop-lite-client"
            ),))
        );
        assert!(matches!(
            handoff.try_transition_target_to_new(target.clone(), |_| {
                service
                    .polling_map
                    .get(&client_id)
                    .is_some_and(|queue| !queue.is_empty())
            }),
            Err(crate::deferred_generation_handoff::DeferredGenerationTargetTransitionError::Draining(_))
                | Err(crate::deferred_generation_handoff::DeferredGenerationTargetTransitionError::LegacyTableOccupied)
        ));
        session.close();
        assert!(!service.legacy_target_occupied(&target));
        let replay = handoff
            .try_transition_target_to_new(target, |_| {
                service
                    .polling_map
                    .get(&client_id)
                    .is_some_and(|queue| !queue.is_empty())
            })
            .expect("closed session leaves target drained");
        replay.complete_after_replay_accepted();
        assert!(handoff.zero_report().is_zero());
        service.running.store(false, Ordering::Release);
    }

    #[tokio::test]
    async fn shutdown_drains_accepted_pop_lite_execution_after_handler_terminal() {
        let processor = Arc::new(BlockingProcessor {
            calls: AtomicUsize::new(0),
            started: Notify::new(),
            release: Notify::new(),
        });
        let mut runtime = BrokerRuntime::new(
            Arc::new(BrokerConfig::default()),
            Arc::new(MessageStoreConfig::default()),
        );
        let state = runtime.runtime_state_mut();
        let context = PopLiteLongPollingServiceContext::try_with_resource_budget(
            PopLiteLongPollingPolicy {
                pop_polling_map_size: 1,
                max_pop_polling_size: 2,
                pop_polling_size: 2,
            },
            LiteEventDispatcher::default(),
            state.broker_service_context(),
            state.resource_budget(),
        )
        .expect("PopLite request budget");
        let service = Arc::new(PopLiteLongPollingService::new(context, Arc::downgrade(&processor)));
        PopLiteLongPollingService::start(&service).await;
        let request = Arc::new(PopRequest::new(
            RemotingCommand::create_remoting_command(0),
            test_context().await,
            current_millis() + 60_000,
            None,
            None,
        ));

        assert!(service.wake_up(request));
        tokio::time::timeout(Duration::from_secs(1), processor.started.notified())
            .await
            .expect("accepted PopLite handler must start");

        let deadline = ShutdownDeadline::after(Duration::from_secs(1));
        let producer_report = service
            .stop_producer_until(deadline)
            .await
            .expect("started service owns its producer group");
        assert!(producer_report.is_healthy(), "{}", producer_report.to_json());
        let mut execution_drain = Box::pin(service.drain_executions_until(deadline));
        tokio::select! {
            biased;
            _ = &mut execution_drain => panic!("PopLite execution drained before the accepted handler barrier"),
            _ = tokio::task::yield_now() => {}
        }
        assert_eq!(service.legacy_resource_snapshot().active_executions, 1);

        processor.release.notify_one();
        let execution_report = execution_drain.await.expect("started service owns its execution group");
        assert!(execution_report.is_healthy(), "{}", execution_report.to_json());
        assert!(service.finalize_shutdown().await.terminal.is_zero());
    }

    #[tokio::test]
    async fn session_close_removes_only_its_exact_pop_lite_waiter() {
        let processor = Arc::new(TestProcessor);
        let tree = ResourceBudgetTree::new(
            "pop-lite-session-cleanup",
            BudgetLimit::new(4, 256 * 1024, FullPolicy::Reject),
        )
        .expect("root budget");
        let context = PopLiteLongPollingServiceContext::try_with_resource_budget(
            PopLiteLongPollingPolicy {
                pop_polling_map_size: 2,
                max_pop_polling_size: 4,
                pop_polling_size: 4,
            },
            LiteEventDispatcher::default(),
            None,
            &tree.root(),
        )
        .expect("PopLite request budget");
        let service = Arc::new(PopLiteLongPollingService::new(context, Arc::downgrade(&processor)));
        let handoff = Arc::new(DeferredGenerationHandoff::new());
        service
            .install_handoff(Arc::clone(&handoff))
            .expect("install the Broker coordinator");
        service.running.store(true, Ordering::Release);
        let client_id = CheetahString::from_static_str("cleanup-pop-lite-client");
        let (first_session, _first_group, first_context, _first_peer) =
            session_execution_test_context(8_201, None).await;
        let (second_session, _second_group, second_context, _second_peer) =
            session_execution_test_context(8_202, None).await;
        for context in [first_context, second_context] {
            let mut command = RemotingCommand::create_remoting_command(0);
            assert_eq!(
                service.polling(
                    context,
                    &mut command,
                    &client_id,
                    i64::try_from(current_millis()).expect("test clock fits i64"),
                    30_000,
                ),
                PollingResult::PollingSuc
            );
        }
        assert_eq!(service.get_polling_num(&client_id), 2);
        assert_eq!(handoff.snapshot().occupancy, 2);

        first_session.close();
        assert_eq!(service.get_polling_num(&client_id), 1);
        assert_eq!(handoff.snapshot().occupancy, 1);

        second_session.close();
        assert_eq!(service.get_polling_num(&client_id), 0);
        assert!(handoff.zero_report().is_zero());
        assert_eq!(service.resource_snapshot().requests.current_count, 0);
        service.shutdown().await;
    }

    #[tokio::test]
    async fn session_close_cannot_observe_a_half_published_pop_lite_waiter() {
        let processor = Arc::new(TestProcessor);
        let tree = ResourceBudgetTree::new(
            "pop-lite-atomic-session-cleanup",
            BudgetLimit::new(2, 128 * 1024, FullPolicy::Reject),
        )
        .expect("root budget");
        let context = PopLiteLongPollingServiceContext::try_with_resource_budget(
            PopLiteLongPollingPolicy {
                pop_polling_map_size: 1,
                max_pop_polling_size: 2,
                pop_polling_size: 2,
            },
            LiteEventDispatcher::default(),
            None,
            &tree.root(),
        )
        .expect("PopLite request budget");
        let service = Arc::new(PopLiteLongPollingService::new(context, Arc::downgrade(&processor)));
        let handoff = Arc::new(DeferredGenerationHandoff::new());
        service
            .install_handoff(Arc::clone(&handoff))
            .expect("install the Broker coordinator");
        service.running.store(true, Ordering::Release);
        let client_id = CheetahString::from_static_str("atomic-cleanup-pop-lite-client");
        let (session, _session_group, connection_context, _peer) = session_execution_test_context(8_203, None).await;
        let session = Arc::new(session);
        let checkpoint = Arc::new(std::sync::Barrier::new(2));
        let release = Arc::new(std::sync::Barrier::new(2));
        let insert_checkpoint = Arc::clone(&checkpoint);
        let insert_release = Arc::clone(&release);
        session.set_insert_checkpoint(move |state_locked| {
            assert!(state_locked, "cleanup enrollment must hold its publication gate");
            insert_checkpoint.wait();
            insert_release.wait();
        });

        let polling_service = Arc::clone(&service);
        let polling_client_id = client_id.clone();
        let polling = std::thread::spawn(move || {
            let mut command = RemotingCommand::create_remoting_command(0);
            let result = polling_service.polling(
                connection_context,
                &mut command,
                &polling_client_id,
                i64::try_from(current_millis()).expect("test clock fits i64"),
                30_000,
            );
            (result, command.suspended())
        });
        checkpoint.wait();

        let (close_started_tx, close_started_rx) = std::sync::mpsc::channel();
        let (close_done_tx, close_done_rx) = std::sync::mpsc::channel();
        let closing_session = Arc::clone(&session);
        let closing = std::thread::spawn(move || {
            close_started_tx.send(()).expect("signal PopLite close attempt");
            closing_session.close();
            close_done_tx.send(()).expect("signal PopLite close completion");
        });
        close_started_rx.recv().expect("PopLite close thread started");
        assert!(
            close_done_rx.try_recv().is_err(),
            "close cannot pass an in-progress PopLite publication"
        );
        release.wait();

        let (result, suspended) = polling.join().expect("PopLite polling thread");
        closing.join().expect("PopLite close thread");
        assert_eq!(result, PollingResult::PollingSuc);
        assert!(suspended, "accepted PopLite waiter must publish suspended=true");
        assert_eq!(service.get_polling_num(&client_id), 0);
        assert_eq!(service.total_polling_num.load(Ordering::Acquire), 0);
        assert!(handoff.zero_report().is_zero());
        let resources = service.resource_snapshot().requests;
        assert_eq!(resources.current_count, 0);
        assert_eq!(resources.current_bytes, 0);
        service.shutdown().await;
    }

    async fn insert_budgeted_request(
        service: &PopLiteLongPollingService<TestProcessor>,
        client_id: &CheetahString,
        expired: u64,
    ) -> usize {
        let command = RemotingCommand::create_remoting_command(0);
        let retained_bytes = PopRequest::estimated_retained_bytes(&command);
        let permit = service
            .context
            .request_budget
            .try_acquire_data(retained_bytes)
            .expect("first suspended request should fit");
        let request = Arc::new(PopRequest::new_with_resource_permit(
            command,
            test_context().await,
            expired,
            None,
            None,
            permit,
        ));
        service
            .polling_map
            .entry(client_id.clone())
            .or_default()
            .insert(request);
        service.total_polling_num.fetch_add(1, Ordering::AcqRel);
        retained_bytes
    }

    #[tokio::test]
    async fn installed_handoff_preserves_pop_lite_client_single_flight_until_terminal() {
        let processor = Arc::new(BlockingProcessor {
            calls: AtomicUsize::new(0),
            started: Notify::new(),
            release: Notify::new(),
        });
        let mut runtime = BrokerRuntime::new(
            Arc::new(BrokerConfig::default()),
            Arc::new(MessageStoreConfig::default()),
        );
        let state = runtime.runtime_state_mut();
        let context = PopLiteLongPollingServiceContext::try_with_resource_budget(
            PopLiteLongPollingPolicy {
                pop_polling_map_size: 2,
                max_pop_polling_size: 4,
                pop_polling_size: 4,
            },
            LiteEventDispatcher::default(),
            state.broker_service_context(),
            state.resource_budget(),
        )
        .expect("PopLite request budget");
        let service = Arc::new(PopLiteLongPollingService::new(context, Arc::downgrade(&processor)));
        let handoff = Arc::new(DeferredGenerationHandoff::new());
        service
            .install_handoff(Arc::clone(&handoff))
            .expect("install the Broker coordinator");
        PopLiteLongPollingService::start(&service).await;
        assert!(service.is_running(), "Broker-owned PopLite service must start");
        let client_id = CheetahString::from_static_str("single-flight-client");
        let (first_session, first_session_group, first_context, _first_peer) =
            session_execution_test_context(8_404, None).await;
        let (second_session, second_session_group, second_context, _second_peer) =
            session_execution_test_context(8_405, None).await;
        for context in [first_context, second_context] {
            let mut command = RemotingCommand::create_remoting_command(0);
            assert_eq!(
                service.polling(
                    context,
                    &mut command,
                    &client_id,
                    i64::try_from(current_millis()).expect("test clock fits i64"),
                    30_000,
                ),
                PollingResult::PollingSuc
            );
        }
        assert_eq!(handoff.snapshot().occupancy, 2);

        assert!(service.wake_up_client(&client_id));
        tokio::time::timeout(Duration::from_secs(1), processor.started.notified())
            .await
            .expect("first PopLite wake must start");
        assert!(!service.wake_up_client(&client_id));
        let first_active = handoff.snapshot();
        assert_eq!(first_active.occupancy, 1);
        assert_eq!(first_active.continuations, 1);

        processor.release.notify_one();
        tokio::time::timeout(Duration::from_secs(1), async {
            while handoff.snapshot().continuations != 0
                || service.legacy_resource_snapshot().waking_clients != 0
                || service.legacy_resource_snapshot().active_executions != 0
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("first PopLite terminal releases client wake gate");

        processor.release.notify_one();
        assert!(service.wake_up_client(&client_id));
        tokio::time::timeout(Duration::from_secs(1), async {
            while !handoff.zero_report().is_zero()
                || service.legacy_resource_snapshot().waking_clients != 0
                || service.legacy_resource_snapshot().active_executions != 0
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("second PopLite terminal releases final ownership");
        assert_eq!(processor.calls.load(Ordering::Acquire), 2);
        first_session.close();
        second_session.close();
        service.shutdown().await;
        for session_group in [first_session_group, second_session_group] {
            let report = session_group
                .shutdown_until(ShutdownDeadline::after(Duration::from_secs(1)))
                .await;
            assert!(report.is_healthy(), "{}", report.to_json());
        }
        assert!(service.legacy_resource_snapshot().is_zero());
        assert!(handoff.zero_report().is_zero());
    }

    fn assert_budget_released_and_readmits(
        service: &PopLiteLongPollingService<TestProcessor>,
        clock: &ManualClock,
        retained_bytes: usize,
    ) {
        let terminal = service.resource_snapshot().requests;
        assert_eq!(terminal.current_count, 0);
        assert_eq!(terminal.current_bytes, 0);

        clock.advance(Duration::from_secs(1));
        let readmitted = service
            .context
            .request_budget
            .try_acquire_data(retained_bytes)
            .expect("terminal request must immediately return its capacity");
        let admitted = service.resource_snapshot().requests;
        assert_eq!(admitted.current_count, 1);
        assert_eq!(admitted.current_bytes, retained_bytes);
        drop(readmitted);
        assert_eq!(service.resource_snapshot().requests.current_count, 0);
        assert_eq!(service.resource_snapshot().requests.current_bytes, 0);
    }

    #[tokio::test]
    async fn arrival_terminal_releases_budget_while_removed_skipset_node_stays_pinned() {
        let (service, clock) = budgeted_service();
        let client_id = CheetahString::from_static_str("arrival-client");
        let mut command = RemotingCommand::create_remoting_command(0);
        let retained_bytes = PopRequest::estimated_retained_bytes(&command);

        assert_eq!(
            service.polling(
                test_context().await,
                &mut command,
                &client_id,
                i64::try_from(current_millis()).expect("test clock fits i64"),
                30_000,
            ),
            PollingResult::PollingSuc
        );
        let queue = service.polling_map.get(&client_id).expect("client queue");
        let pinned_node = queue.value().front().expect("suspended request node");
        assert_eq!(service.resource_snapshot().requests.current_count, 1);
        assert_eq!(service.resource_snapshot().requests.current_bytes, retained_bytes);

        let request = service.poll_request(queue.value()).expect("arrival claims request");
        let duplicate = Arc::clone(&request);
        assert!(!service.wake_up(request));
        assert!(!service.wake_up(duplicate));

        assert_budget_released_and_readmits(&service, clock.as_ref(), retained_bytes);
        assert!(pinned_node.is_removed(), "guard must still pin the removed node");
    }

    #[tokio::test]
    async fn timeout_terminal_releases_budget_while_removed_skipset_node_stays_pinned() {
        let (service, clock) = budgeted_service();
        let client_id = CheetahString::from_static_str("timeout-client");
        let retained_bytes = insert_budgeted_request(&service, &client_id, 0).await;
        let queue = service.polling_map.get(&client_id).expect("client queue");
        let pinned_node = queue.value().front().expect("suspended request node");

        service.wake_up_expired_requests(&client_id);

        assert_eq!(service.total_polling_num.load(Ordering::Acquire), 0);
        assert_budget_released_and_readmits(&service, clock.as_ref(), retained_bytes);
        assert!(pinned_node.is_removed(), "guard must still pin the removed node");
    }

    #[tokio::test]
    async fn cancellation_drain_releases_budget_while_removed_skipset_node_stays_pinned() {
        let (service, clock) = budgeted_service();
        let client_id = CheetahString::from_static_str("cancel-client");
        let retained_bytes =
            insert_budgeted_request(&service, &client_id, current_millis().saturating_add(30_000)).await;
        let queue = service.polling_map.get(&client_id).expect("client queue");
        let pinned_node = queue.value().front().expect("suspended request node");
        service.running.store(false, Ordering::Release);

        service.drain_polling_queue(queue.value());

        assert_eq!(service.total_polling_num.load(Ordering::Acquire), 0);
        assert_budget_released_and_readmits(&service, clock.as_ref(), retained_bytes);
        assert!(pinned_node.is_removed(), "guard must still pin the removed node");
    }

    #[test]
    fn overload_rejects_excess_long_poll_requests_and_releases_permits() {
        let tree = rocketmq_runtime::ResourceBudgetTree::new(
            "broker-long-poll-overload",
            BudgetLimit::new(4, 4096, FullPolicy::Reject),
        )
        .expect("root budget");
        let policy = PopLiteLongPollingPolicy {
            pop_polling_map_size: 2,
            max_pop_polling_size: 2,
            pop_polling_size: 2,
        };
        let context = PopLiteLongPollingServiceContext::try_with_resource_budget(
            policy,
            LiteEventDispatcher::default(),
            None,
            &tree.root(),
        )
        .expect("long-poll budgets");

        let first = context.request_budget.try_acquire_data(1).expect("first request");
        let second = context.request_budget.try_acquire_data(1).expect("second request");
        assert!(context.request_budget.try_acquire_data(1).is_err());
        assert!(context.request_budget.try_acquire_data(1).is_err());
        assert_eq!(context.request_budget.snapshot().current_count, 2);
        assert_eq!(context.request_budget.snapshot().rejected_count, 2);

        drop((first, second));
        assert_eq!(context.request_budget.snapshot().current_count, 0);
    }

    #[test]
    fn empty_client_polling_queues_are_pruned() {
        let polling_map = DashMap::new();
        polling_map.insert(
            CheetahString::from_static_str("empty-client"),
            SkipSet::<Arc<PopRequest>>::new(),
        );

        prune_empty_polling_queues(&polling_map);

        assert!(polling_map.is_empty());
    }

    #[test]
    fn pop_lite_long_polling_policy_captures_only_required_startup_values() {
        let broker_config = BrokerConfig {
            pop_polling_map_size: 11,
            max_pop_polling_size: 22,
            pop_polling_size: 33,
            ..Default::default()
        };

        let policy = PopLiteLongPollingPolicy::from_config(&broker_config);

        assert_eq!(policy.pop_polling_map_size, 11);
        assert_eq!(policy.max_pop_polling_size, 22);
        assert_eq!(policy.pop_polling_size, 33);
    }

    #[test]
    fn pop_lite_long_polling_source_uses_only_explicit_capabilities() {
        let source = include_str!("pop_lite_long_polling_service.rs");

        assert!(!source.contains(concat!("rocketmq_rust::", "ArcMut")));
        assert!(!source.contains(concat!("BrokerRuntime", "Inner")));
        assert!(!source.contains(concat!("Message", "Store")));
        assert!(source.contains("PopLiteLongPollingServiceContext"));
        assert!(source.contains("lite_event_dispatcher: LiteEventDispatcher"));
        assert!(source.contains("service_context: Option<ChildServiceContext>"));
        assert!(source.contains("request_budget: ResourceBudget"));
        assert!(source.contains("waking_clients: Arc<DashMap<CheetahString, ()>>"));
    }
}
