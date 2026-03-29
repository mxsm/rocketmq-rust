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

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use cheetah_string::CheetahString;
use crossbeam_skiplist::SkipSet;
use dashmap::DashMap;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_remoting::runtime::processor::RequestProcessor;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_store::MessageStore;
use tokio::select;
use tokio::sync::mpsc::unbounded_channel;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::Notify;
use tracing::error;

use crate::broker_runtime::BrokerRuntimeInner;
use crate::long_polling::polling_result::PollingResult;
use crate::long_polling::pop_request::PopRequest;

pub(crate) struct PopLiteLongPollingService<MS: MessageStore, RP> {
    broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
    polling_map: DashMap<CheetahString, SkipSet<Arc<PopRequest>>>,
    total_polling_num: AtomicU64,
    processor: Option<ArcMut<RP>>,
    notify: Notify,
    wakeup_tx: UnboundedSender<CheetahString>,
    wakeup_rx: Option<UnboundedReceiver<CheetahString>>,
}

impl<MS: MessageStore, RP: RequestProcessor + Sync + 'static> PopLiteLongPollingService<MS, RP> {
    pub(crate) fn new(broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>) -> Self {
        let (wakeup_tx, wakeup_rx) = unbounded_channel();
        Self {
            broker_runtime_inner: broker_runtime_inner.clone(),
            polling_map: DashMap::with_capacity(broker_runtime_inner.broker_config().pop_polling_map_size),
            total_polling_num: AtomicU64::new(0),
            processor: None,
            notify: Notify::new(),
            wakeup_tx,
            wakeup_rx: Some(wakeup_rx),
        }
    }

    pub(crate) fn start(this: ArcMut<Self>) {
        let mut wakeup_rx = this
            .mut_from_ref()
            .wakeup_rx
            .take()
            .expect("pop-lite long polling receiver should only be taken once");
        tokio::spawn(async move {
            loop {
                select! {
                    _ = this.notify.notified() => { break; }
                    wakeup = wakeup_rx.recv() => {
                        match wakeup {
                            Some(client_id) => {
                                this.wake_up_client(&client_id);
                            }
                            None => break,
                        }
                    }
                    _ = tokio::time::sleep(tokio::time::Duration::from_millis(20)) => {}
                }

                if this.polling_map.is_empty() {
                    continue;
                }
                for entry in this.polling_map.iter() {
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
                        this.total_polling_num.fetch_sub(1, Ordering::AcqRel);
                        this.wake_up(first);
                    }
                }
            }

            for entry in this.polling_map.iter() {
                let queue = entry.value();
                while let Some(first) = queue.pop_front() {
                    this.total_polling_num.fetch_sub(1, Ordering::AcqRel);
                    this.wake_up(first.value().clone());
                }
            }
        });
    }

    pub(crate) fn shutdown(&mut self) {
        self.notify.notify_waiters();
    }

    pub(crate) fn wakeup_sender(&self) -> UnboundedSender<CheetahString> {
        self.wakeup_tx.clone()
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

        let expired = born_time.saturating_add(poll_time);
        let request = Arc::new(PopRequest::new(
            remoting_command.clone(),
            ctx,
            expired as u64,
            None,
            None,
        ));

        if self.total_polling_num.load(Ordering::SeqCst)
            >= self.broker_runtime_inner.broker_config().max_pop_polling_size
        {
            return PollingResult::PollingFull;
        }

        if request.is_timeout() {
            return PollingResult::PollingTimeout;
        }

        let queue = self.polling_map.entry(client_id.clone()).or_default();
        if queue.len() > self.broker_runtime_inner.broker_config().pop_polling_size {
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
        match self.processor.clone() {
            None => false,
            Some(mut processor) => {
                tokio::spawn(async move {
                    let channel = pop_request.get_channel().clone();
                    let ctx = pop_request.get_ctx().clone();
                    let opaque = pop_request.get_remoting_command().opaque();
                    let response = processor
                        .process_request(channel, ctx, &mut pop_request.get_remoting_command().clone())
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

    pub(crate) fn set_processor(&mut self, processor: ArcMut<RP>) {
        self.processor = Some(processor);
    }
}
