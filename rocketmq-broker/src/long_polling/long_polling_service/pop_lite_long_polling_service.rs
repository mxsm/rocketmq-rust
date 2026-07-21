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

use cheetah_string::CheetahString;
use crossbeam_skiplist::SkipSet;
use dashmap::DashMap;
use parking_lot::Mutex;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_runtime::TaskGroup;
use rocketmq_runtime::TaskKind;
use tokio::select;
use tokio::sync::mpsc::unbounded_channel;
use tokio::sync::Mutex as AsyncMutex;
use tracing::error;
use tracing::warn;

use crate::broker_runtime::broker_task_group_or_current;
use crate::lite::lite_event_dispatcher::LiteEventDispatcher;
use crate::long_polling::polling_result::PollingResult;
use crate::long_polling::pop_request::PopRequest;

#[trait_variant::make(PopLiteLongPollingRequestProcessor: Send)]
pub(crate) trait LocalPopLiteLongPollingRequestProcessor {
    async fn process_request_when_wakeup(
        &self,
        channel: rocketmq_remoting::net::channel::Channel,
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
    parent_task_group: Option<TaskGroup>,
}

impl PopLiteLongPollingServiceContext {
    pub(crate) fn new(
        policy: PopLiteLongPollingPolicy,
        lite_event_dispatcher: LiteEventDispatcher,
        parent_task_group: Option<TaskGroup>,
    ) -> Self {
        Self {
            policy,
            lite_event_dispatcher,
            parent_task_group,
        }
    }
}

pub(crate) struct PopLiteLongPollingService<RP> {
    context: PopLiteLongPollingServiceContext,
    polling_map: DashMap<CheetahString, SkipSet<Arc<PopRequest>>>,
    total_polling_num: AtomicU64,
    processor: Weak<RP>,
    running: AtomicBool,
    lifecycle: AsyncMutex<()>,
    task_group: Mutex<Option<TaskGroup>>,
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
            this.context.parent_task_group.as_ref(),
            "rocketmq-broker.long-polling.pop-lite",
            "failed to start PopLiteLongPollingService outside Tokio runtime",
        ) else {
            this.running.store(false, Ordering::Release);
            return;
        };
        let cancellation_token = task_group.cancellation_token();
        let service = Arc::downgrade(this);
        let (wakeup_tx, mut wakeup_rx) = unbounded_channel();
        *this.task_group.lock() = Some(task_group.clone());

        let spawn_result = task_group.spawn_service("broker.long-polling.pop-lite.scan", async move {
            loop {
                let client_id = select! {
                    _ = cancellation_token.cancelled() => { break; }
                    wakeup = wakeup_rx.recv() => {
                        match wakeup {
                            Some(client_id) => Some(client_id),
                            None => break,
                        }
                    }
                    _ = tokio::time::sleep(tokio::time::Duration::from_millis(20)) => None,
                };

                let Some(service) = service.upgrade() else {
                    break;
                };

                if let Some(client_id) = client_id {
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
            }

            if let Some(service) = service.upgrade() {
                for entry in service.polling_map.iter() {
                    let queue = entry.value();
                    while let Some(first) = queue.pop_front() {
                        service.total_polling_num.fetch_sub(1, Ordering::AcqRel);
                        service.wake_up(first.value().clone());
                    }
                }
                service.running.store(false, Ordering::Release);
            }
        });

        if let Err(error) = spawn_result {
            this.task_group.lock().take();
            this.running.store(false, Ordering::Release);
            warn!(?error, "failed to spawn PopLiteLongPollingService scan task");
            return;
        }

        this.context.lite_event_dispatcher.set_wakeup_sender(wakeup_tx);
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

        let expired = born_time.saturating_add(poll_time);
        let request = Arc::new(PopRequest::new(
            remoting_command.clone(),
            ctx,
            expired as u64,
            None,
            None,
        ));

        if self.total_polling_num.load(Ordering::SeqCst) >= self.context.policy.max_pop_polling_size {
            return PollingResult::PollingFull;
        }

        if request.is_timeout() {
            return PollingResult::PollingTimeout;
        }

        let queue = self.polling_map.entry(client_id.clone()).or_default();
        if queue.len() > self.context.policy.pop_polling_size {
            return PollingResult::PollingFull;
        }

        queue.insert(request);
        remoting_command.set_suspended_ref(true);
        self.total_polling_num.fetch_add(1, Ordering::SeqCst);
        PollingResult::PollingSuc
    }

    pub(crate) fn wake_up_client(&self, client_id: &CheetahString) -> bool {
        let Some(remoting_commands) = self.polling_map.get(client_id) else {
            return false;
        };
        let Some(pop_request) = self.poll_request(remoting_commands.value()) else {
            return false;
        };
        self.wake_up(pop_request)
    }

    fn wake_up(&self, pop_request: Arc<PopRequest>) -> bool {
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
        assert!(source.contains("parent_task_group: Option<TaskGroup>"));
    }
}
