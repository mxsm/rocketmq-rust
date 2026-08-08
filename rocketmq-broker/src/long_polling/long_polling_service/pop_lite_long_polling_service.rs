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
use rocketmq_runtime::TaskGroup;
use rocketmq_runtime::TaskKind;
use rocketmq_transport::api::v1::ConnectionHandlerContext;
use tokio::select;
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::Notify;
use tracing::error;
use tracing::warn;

use crate::broker_runtime::broker_task_group_or_current;
use crate::lite::lite_event_dispatcher::LiteEventDispatcher;
use crate::long_polling::polling_result::PollingResult;
use crate::long_polling::pop_request::PopRequest;

fn prune_empty_polling_queues(polling_map: &DashMap<CheetahString, SkipSet<Arc<PopRequest>>>) {
    polling_map.retain(|_, queue| !queue.is_empty());
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
    polling_map: DashMap<CheetahString, SkipSet<Arc<PopRequest>>>,
    total_polling_num: AtomicU64,
    processor: Weak<RP>,
    running: AtomicBool,
    lifecycle: AsyncMutex<()>,
    polling_admission: Mutex<()>,
    waking_clients: Arc<DashMap<CheetahString, ()>>,
    task_group: Mutex<Option<TaskGroup>>,
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

impl Drop for ClientWakeupClaim {
    fn drop(&mut self) {
        self.waking_clients.remove(&self.client_id);
    }
}

impl<RP: PopLiteLongPollingRequestProcessor + Sync + 'static> PopLiteLongPollingService<RP> {
    pub(crate) fn new(context: PopLiteLongPollingServiceContext, processor: Weak<RP>) -> Self {
        Self {
            polling_map: DashMap::with_capacity(context.policy.pop_polling_map_size),
            context,
            total_polling_num: AtomicU64::new(0),
            processor,
            running: AtomicBool::new(false),
            lifecycle: AsyncMutex::new(()),
            polling_admission: Mutex::new(()),
            waking_clients: Arc::new(DashMap::new()),
            task_group: Mutex::new(None),
        }
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

        let Some(task_group) = broker_task_group_or_current(
            this.context.service_context.as_ref(),
            "rocketmq-broker.long-polling.pop-lite",
            "failed to start PopLiteLongPollingService outside Tokio runtime",
        ) else {
            this.running.store(false, Ordering::Release);
            return;
        };
        let cancellation_token = task_group.cancellation_token();
        let service = Arc::downgrade(this);
        let wakeup_notify = Arc::new(Notify::new());
        let task_wakeup_notify = wakeup_notify.clone();
        *this.task_group.lock() = Some(task_group.clone());

        let spawn_result = task_group.spawn_service("broker.long-polling.pop-lite.scan", async move {
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
                for entry in service.polling_map.iter() {
                    let queue = entry.value();
                    if queue.is_empty() {
                        continue;
                    }
                    loop {
                        let Some(first) = queue.pop_front() else {
                            break;
                        };
                        let first = first.value().clone();
                        if !first.is_timeout() {
                            queue.insert(first);
                            break;
                        }
                        service.total_polling_num.fetch_sub(1, Ordering::AcqRel);
                        service.wake_up(first);
                    }
                }
                prune_empty_polling_queues(&service.polling_map);
            }

            if let Some(service) = service.upgrade() {
                for entry in service.polling_map.iter() {
                    let queue = entry.value();
                    while let Some(first) = queue.pop_front() {
                        service.total_polling_num.fetch_sub(1, Ordering::AcqRel);
                        service.wake_up(first.value().clone());
                    }
                }
                service.polling_map.clear();
                service.running.store(false, Ordering::Release);
            }
        });

        if let Err(error) = spawn_result {
            this.task_group.lock().take();
            this.running.store(false, Ordering::Release);
            warn!(?error, "failed to spawn PopLiteLongPollingService scan task");
            return;
        }

        this.context.lite_event_dispatcher.set_wakeup_notify(wakeup_notify);
    }

    pub(crate) async fn shutdown(&self) {
        let _lifecycle = self.lifecycle.lock().await;
        self.running.store(false, Ordering::Release);
        let task_group = self.task_group.lock().take();
        if let Some(task_group) = task_group {
            let report = task_group.shutdown(Duration::from_secs(5)).await;
            if !report.is_healthy() {
                warn!(
                    report = %report.to_json(),
                    "PopLiteLongPollingService shutdown report is unhealthy"
                );
            }
        }
        self.context.lite_event_dispatcher.clear_wakeup_notify();
        self.waking_clients.clear();
        self.running.store(false, Ordering::Release);
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
        let queue = self.polling_map.entry(client_id.clone()).or_default();
        if queue.len() >= self.context.policy.pop_polling_size {
            return PollingResult::PollingFull;
        }

        queue.insert(request);
        remoting_command.set_suspended_ref(true);
        self.total_polling_num.fetch_add(1, Ordering::SeqCst);
        PollingResult::PollingSuc
    }

    pub(crate) fn wake_up_client(&self, client_id: &CheetahString) -> bool {
        if self.waking_clients.insert(client_id.clone(), ()).is_some() {
            return false;
        }
        let claim = ClientWakeupClaim {
            client_id: client_id.clone(),
            waking_clients: self.waking_clients.clone(),
        };
        let Some(remoting_commands) = self.polling_map.get(client_id) else {
            return false;
        };
        let Some(pop_request) = self.poll_request(remoting_commands.value()) else {
            return false;
        };
        self.wake_up_with_claim(pop_request, Some(claim))
    }

    fn wake_up(&self, pop_request: Arc<PopRequest>) -> bool {
        self.wake_up_with_claim(pop_request, None)
    }

    fn wake_up_with_claim(&self, pop_request: Arc<PopRequest>, client_wakeup_claim: Option<ClientWakeupClaim>) -> bool {
        if !pop_request.complete() {
            return false;
        }
        match self.processor.upgrade() {
            None => false,
            Some(processor) => {
                let task_group = self.task_group.lock().as_ref().cloned();
                let Some(task_group) = task_group else {
                    warn!("PopLiteLongPollingService wake-up skipped because task group is not running");
                    return false;
                };

                let spawn_result =
                    task_group.spawn("broker.long-polling.pop-lite.wake-up", TaskKind::Worker, async move {
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
                    });
                if let Err(error) = spawn_result {
                    warn!(?error, "failed to spawn PopLiteLongPollingService wake-up task");
                    return false;
                }
                true
            }
        }
    }

    fn poll_request(&self, remoting_commands: &SkipSet<Arc<PopRequest>>) -> Option<Arc<PopRequest>> {
        if remoting_commands.is_empty() {
            return None;
        }

        loop {
            let pop_request = remoting_commands.pop_front().map(|entry| entry.value().clone())?;
            self.total_polling_num.fetch_sub(1, Ordering::AcqRel);
            if !pop_request.get_channel().connection_ref().is_healthy() {
                continue;
            }
            return Some(pop_request);
        }
    }

    #[inline]
    pub(crate) fn get_polling_num(&self, key: &str) -> i32 {
        self.polling_map.get(key).map(|queue| queue.len() as i32).unwrap_or(0)
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
    use super::*;

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
