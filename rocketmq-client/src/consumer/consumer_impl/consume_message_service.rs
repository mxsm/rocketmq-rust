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

use cheetah_string::CheetahString;
use rocketmq_common::common::message::message_client_ext::MessageClientExt;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_remoting::protocol::body::consume_message_directly_result::ConsumeMessageDirectlyResult;
use rocketmq_rust::ArcMut;
use rocketmq_rust::WeakArcMut;

use crate::consumer::consumer_impl::pop_process_queue::PopProcessQueue;
use crate::consumer::consumer_impl::process_queue::ProcessQueue;
use rocketmq_remoting::protocol::body::cm_result::CMResult;

pub struct ConsumeMessageServiceGeneral<T, K> {
    consume_message_concurrently_service: Option<ArcMut<T>>,
    consume_message_orderly_service: Option<ArcMut<K>>,
}
impl<T, K> ConsumeMessageServiceGeneral<T, K> {
    pub fn new(
        consume_message_concurrently_service: Option<ArcMut<T>>,
        consume_message_orderly_service: Option<ArcMut<K>>,
    ) -> Self {
        Self {
            consume_message_concurrently_service,
            consume_message_orderly_service,
        }
    }

    pub fn get_consume_message_concurrently_service_weak(&self) -> Option<WeakArcMut<T>> {
        self.consume_message_concurrently_service
            .as_ref()
            .map(ArcMut::downgrade)
    }
}

impl<T, K> ConsumeMessageServiceGeneral<T, K>
where
    T: ConsumeMessageServiceTrait,
    K: ConsumeMessageServiceTrait,
{
    pub fn start(&mut self) {
        if let Some(consume_message_concurrently_service) = &mut self.consume_message_concurrently_service {
            let this = consume_message_concurrently_service.clone();
            consume_message_concurrently_service.start(this);
        }

        if let Some(consume_message_orderly_service) = &mut self.consume_message_orderly_service {
            let this = consume_message_orderly_service.clone();
            consume_message_orderly_service.start(this);
        }
    }

    pub async fn shutdown(&mut self, await_terminate_millis: u64) {
        if let Some(consume_message_concurrently_service) = &mut self.consume_message_concurrently_service {
            consume_message_concurrently_service
                .shutdown(await_terminate_millis)
                .await;
        }

        if let Some(consume_message_orderly_service) = &mut self.consume_message_orderly_service {
            consume_message_orderly_service.shutdown(await_terminate_millis).await;
        }
    }

    pub fn update_core_pool_size(&self, core_pool_size: usize) {
        if let Some(consume_message_concurrently_service) = &self.consume_message_concurrently_service {
            consume_message_concurrently_service.update_core_pool_size(core_pool_size);
        }

        if let Some(consume_message_orderly_service) = &self.consume_message_orderly_service {
            consume_message_orderly_service.update_core_pool_size(core_pool_size);
        }
    }

    pub fn inc_core_pool_size(&self) {
        if let Some(consume_message_concurrently_service) = &self.consume_message_concurrently_service {
            consume_message_concurrently_service.inc_core_pool_size();
        }

        if let Some(consume_message_orderly_service) = &self.consume_message_orderly_service {
            consume_message_orderly_service.inc_core_pool_size();
        }
    }

    pub fn dec_core_pool_size(&self) {
        if let Some(consume_message_concurrently_service) = &self.consume_message_concurrently_service {
            consume_message_concurrently_service.dec_core_pool_size();
        }

        if let Some(consume_message_orderly_service) = &self.consume_message_orderly_service {
            consume_message_orderly_service.dec_core_pool_size();
        }
    }

    pub fn get_core_pool_size(&self) -> usize {
        if let Some(consume_message_concurrently_service) = &self.consume_message_concurrently_service {
            return consume_message_concurrently_service.get_core_pool_size();
        }

        if let Some(consume_message_orderly_service) = &self.consume_message_orderly_service {
            return consume_message_orderly_service.get_core_pool_size();
        }

        num_cpus::get()
    }

    pub async fn consume_message_directly(
        &self,
        msg: MessageExt,
        broker_name: Option<CheetahString>,
    ) -> ConsumeMessageDirectlyResult {
        if let Some(consume_message_concurrently_service) = &self.consume_message_concurrently_service {
            let this = consume_message_concurrently_service.clone();
            consume_message_concurrently_service
                .consume_message_directly(msg, broker_name)
                .await
        } else if let Some(consume_message_orderly_service) = &self.consume_message_orderly_service {
            let this = consume_message_orderly_service.clone();
            consume_message_orderly_service
                .consume_message_directly(msg, broker_name)
                .await
        } else {
            ConsumeMessageDirectlyResult::new(
                false,
                true,
                CMResult::CRThrowException,
                CheetahString::from_static_str("ConsumeMessageServiceGeneral has no direct-consume service"),
                0,
            )
        }
    }

    pub async fn submit_consume_request(
        &self,
        msgs: Vec<ArcMut<MessageExt>>,
        process_queue: Arc<ProcessQueue>,
        message_queue: MessageQueue,
        dispatch_to_consume: bool,
    ) {
        if let Some(consume_message_concurrently_service) = &self.consume_message_concurrently_service {
            let this = consume_message_concurrently_service.clone();
            consume_message_concurrently_service
                .submit_consume_request(this, msgs, process_queue, message_queue, dispatch_to_consume)
                .await;
        } else if let Some(consume_message_orderly_service) = &self.consume_message_orderly_service {
            let this = consume_message_orderly_service.clone();
            consume_message_orderly_service
                .submit_consume_request(this, msgs, process_queue, message_queue, dispatch_to_consume)
                .await;
        }
    }

    pub async fn submit_pop_consume_request(
        &self,
        msgs: Vec<MessageExt>,
        process_queue: &PopProcessQueue,
        message_queue: &MessageQueue,
    ) {
        let _ = (msgs, process_queue, message_queue);
    }

    pub fn get_consume_message_concurrently_service(&self) -> Option<ArcMut<T>> {
        self.consume_message_concurrently_service.as_ref().cloned()
    }
}

pub struct ConsumeMessagePopServiceGeneral<T, K> {
    consume_message_pop_concurrently_service: Option<ArcMut<T>>,
    consume_message_pop_orderly_service: Option<ArcMut<K>>,
}

impl<T, K> ConsumeMessagePopServiceGeneral<T, K> {
    pub fn new(
        consume_message_pop_concurrently_service: Option<ArcMut<T>>,
        consume_message_pop_orderly_service: Option<ArcMut<K>>,
    ) -> Self {
        Self {
            consume_message_pop_concurrently_service,
            consume_message_pop_orderly_service,
        }
    }
}

impl<T, K> ConsumeMessagePopServiceGeneral<T, K>
where
    T: ConsumeMessageServiceTrait,
    K: ConsumeMessageServiceTrait,
{
    pub fn start(&mut self) {
        if let Some(consume_message_pop_concurrently_service) = &mut self.consume_message_pop_concurrently_service {
            let this = consume_message_pop_concurrently_service.clone();
            consume_message_pop_concurrently_service.start(this);
        }

        if let Some(consume_message_pop_orderly_service) = &mut self.consume_message_pop_orderly_service {
            let this = consume_message_pop_orderly_service.clone();
            consume_message_pop_orderly_service.start(this);
        }
    }

    pub async fn shutdown(&mut self, await_terminate_millis: u64) {
        if let Some(consume_message_pop_concurrently_service) = &mut self.consume_message_pop_concurrently_service {
            consume_message_pop_concurrently_service
                .shutdown(await_terminate_millis)
                .await;
        }

        if let Some(consume_message_pop_orderly_service) = &mut self.consume_message_pop_orderly_service {
            consume_message_pop_orderly_service
                .shutdown(await_terminate_millis)
                .await;
        }
    }

    pub fn update_core_pool_size(&self, core_pool_size: usize) {
        if let Some(consume_message_pop_concurrently_service) = &self.consume_message_pop_concurrently_service {
            consume_message_pop_concurrently_service.update_core_pool_size(core_pool_size);
        }

        if let Some(consume_message_pop_orderly_service) = &self.consume_message_pop_orderly_service {
            consume_message_pop_orderly_service.update_core_pool_size(core_pool_size);
        }
    }

    pub fn inc_core_pool_size(&self) {
        if let Some(consume_message_pop_concurrently_service) = &self.consume_message_pop_concurrently_service {
            consume_message_pop_concurrently_service.inc_core_pool_size();
        }

        if let Some(consume_message_pop_orderly_service) = &self.consume_message_pop_orderly_service {
            consume_message_pop_orderly_service.inc_core_pool_size();
        }
    }

    pub fn dec_core_pool_size(&self) {
        if let Some(consume_message_pop_concurrently_service) = &self.consume_message_pop_concurrently_service {
            consume_message_pop_concurrently_service.dec_core_pool_size();
        }

        if let Some(consume_message_pop_orderly_service) = &self.consume_message_pop_orderly_service {
            consume_message_pop_orderly_service.dec_core_pool_size();
        }
    }

    pub fn get_core_pool_size(&self) -> usize {
        if let Some(consume_message_pop_concurrently_service) = &self.consume_message_pop_concurrently_service {
            return consume_message_pop_concurrently_service.get_core_pool_size();
        }

        if let Some(consume_message_pop_orderly_service) = &self.consume_message_pop_orderly_service {
            return consume_message_pop_orderly_service.get_core_pool_size();
        }

        num_cpus::get()
    }

    pub(crate) async fn consume_message_directly(
        &self,
        msg: MessageExt,
        broker_name: Option<CheetahString>,
    ) -> ConsumeMessageDirectlyResult {
        if let Some(consume_message_pop_concurrently_service) = &self.consume_message_pop_concurrently_service {
            let this = consume_message_pop_concurrently_service.clone();
            consume_message_pop_concurrently_service
                .consume_message_directly(msg, broker_name)
                .await
        } else if let Some(consume_message_pop_orderly_service) = &self.consume_message_pop_orderly_service {
            let this = consume_message_pop_orderly_service.clone();
            consume_message_pop_orderly_service
                .consume_message_directly(msg, broker_name)
                .await
        } else {
            ConsumeMessageDirectlyResult::new(
                false,
                true,
                CMResult::CRThrowException,
                CheetahString::from_static_str("ConsumeMessagePopServiceGeneral has no direct-consume service"),
                0,
            )
        }
    }

    pub async fn submit_consume_request(
        &self,
        msgs: Vec<ArcMut<MessageClientExt>>,
        process_queue: Arc<ProcessQueue>,
        message_queue: MessageQueue,
        dispatch_to_consume: bool,
    ) {
        let _ = (msgs, process_queue, message_queue, dispatch_to_consume);
    }

    pub async fn submit_pop_consume_request(
        &self,
        msgs: Vec<MessageExt>,
        process_queue: &PopProcessQueue,
        message_queue: &MessageQueue,
    ) {
        if let Some(consume_message_pop_concurrently_service) = &self.consume_message_pop_concurrently_service {
            let this = consume_message_pop_concurrently_service.clone();
            consume_message_pop_concurrently_service
                .submit_pop_consume_request(this, msgs, process_queue, message_queue)
                .await;
        } else if let Some(consume_message_pop_orderly_service) = &self.consume_message_pop_orderly_service {
            let this = consume_message_pop_orderly_service.clone();
            consume_message_pop_orderly_service
                .submit_pop_consume_request(this, msgs, process_queue, message_queue)
                .await;
        }
    }
}

/// Trait defining the behavior of a message consumption service.
#[allow(async_fn_in_trait)]
pub trait ConsumeMessageServiceTrait {
    /// Starts the message consumption service.
    ///
    /// # Arguments
    ///
    /// * `this` - An `ArcMut` reference to the service instance.
    fn start(&mut self, this: ArcMut<Self>);

    /// Shuts down the message consumption service.
    ///
    /// # Arguments
    ///
    /// * `await_terminate_millis` - The number of milliseconds to wait for termination.
    async fn shutdown(&mut self, await_terminate_millis: u64);

    /// Updates the core pool size of the service.
    ///
    /// # Arguments
    ///
    /// * `core_pool_size` - The new core pool size.
    fn update_core_pool_size(&self, core_pool_size: usize) {}

    /// Increases the core pool size of the service by one.
    fn inc_core_pool_size(&self) {}

    /// Decreases the core pool size of the service by one.
    fn dec_core_pool_size(&self) {}

    /// Gets the current core pool size of the service.
    ///
    /// # Returns
    ///
    /// The current core pool size.
    fn get_core_pool_size(&self) -> usize {
        num_cpus::get()
    }

    /// Consumes a message directly.
    ///
    /// # Arguments
    ///
    /// * `msg` - The message to be consumed.
    /// * `broker_name` - An optional broker name.
    ///
    /// # Returns
    ///
    /// A `ConsumeMessageDirectlyResult` indicating the result of the consumption.
    async fn consume_message_directly(
        &self,
        msg: MessageExt,
        broker_name: Option<CheetahString>,
    ) -> ConsumeMessageDirectlyResult;

    /// Submits a consume request.
    ///
    /// # Arguments
    ///
    /// * `this` - An `ArcMut` reference to the service instance.
    /// * `msgs` - A vector of messages to be consumed.
    /// * `process_queue` - The process queue.
    /// * `message_queue` - The message queue.
    /// * `dispatch_to_consume` - A boolean indicating whether to dispatch to consume.
    async fn submit_consume_request(
        &self,
        this: ArcMut<Self>,
        msgs: Vec<ArcMut<MessageExt>>,
        process_queue: Arc<ProcessQueue>,
        message_queue: MessageQueue,
        dispatch_to_consume: bool,
    );

    /// Submits a pop consume request.
    ///
    /// # Arguments
    ///
    /// * `msgs` - A vector of messages to be consumed.
    /// * `process_queue` - The pop process queue.
    /// * `message_queue` - The message queue.
    async fn submit_pop_consume_request(
        &self,
        this: ArcMut<Self>,
        msgs: Vec<MessageExt>,
        process_queue: &PopProcessQueue,
        message_queue: &MessageQueue,
    );
}

/// Java-compatible public name for message consumption services.
///
/// Java exposes this contract as `ConsumeMessageService`. The original Rust
/// implementation used `ConsumeMessageServiceTrait`; this blanket trait keeps
/// existing implementors working while exposing the Java-equivalent API name.
pub trait ConsumeMessageService: ConsumeMessageServiceTrait {}

impl<T> ConsumeMessageService for T where T: ConsumeMessageServiceTrait + ?Sized {}

#[cfg(test)]
mod tests {
    use super::*;

    struct NoopConsumeMessageService;

    impl ConsumeMessageServiceTrait for NoopConsumeMessageService {
        fn start(&mut self, _this: ArcMut<Self>) {}

        async fn shutdown(&mut self, _await_terminate_millis: u64) {}

        async fn consume_message_directly(
            &self,
            _msg: MessageExt,
            _broker_name: Option<CheetahString>,
        ) -> ConsumeMessageDirectlyResult {
            ConsumeMessageDirectlyResult::default()
        }

        async fn submit_consume_request(
            &self,
            _this: ArcMut<Self>,
            _msgs: Vec<ArcMut<MessageExt>>,
            _process_queue: Arc<ProcessQueue>,
            _message_queue: MessageQueue,
            _dispatch_to_consume: bool,
        ) {
        }

        async fn submit_pop_consume_request(
            &self,
            _this: ArcMut<Self>,
            _msgs: Vec<MessageExt>,
            _process_queue: &PopProcessQueue,
            _message_queue: &MessageQueue,
        ) {
        }
    }

    fn assert_consume_message_service<T: ConsumeMessageService>() {}

    #[test]
    fn consume_message_service_java_name_is_blanket_implemented() {
        assert_consume_message_service::<NoopConsumeMessageService>();
    }
}
