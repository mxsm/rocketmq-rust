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

use rocketmq_common::common::message::message_client_ext::MessageClientExt;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::ArcRefCellWrapper;
use rocketmq_common::WeakCellWrapper;
use rocketmq_remoting::protocol::body::consume_message_directly_result::ConsumeMessageDirectlyResult;

use crate::consumer::consumer_impl::pop_process_queue::PopProcessQueue;
use crate::consumer::consumer_impl::process_queue::ProcessQueue;

pub struct ConsumeMessageServiceGeneral<T, K> {
    consume_message_concurrently_service: Option<ArcRefCellWrapper<T>>,
    consume_message_orderly_service: Option<ArcRefCellWrapper<K>>,
}
impl<T, K> ConsumeMessageServiceGeneral<T, K> {
    pub fn new(
        consume_message_concurrently_service: Option<ArcRefCellWrapper<T>>,
        consume_message_orderly_service: Option<ArcRefCellWrapper<K>>,
    ) -> Self {
        Self {
            consume_message_concurrently_service,
            consume_message_orderly_service,
        }
    }

    pub fn get_consume_message_concurrently_service_weak(&self) -> WeakCellWrapper<T> {
        ArcRefCellWrapper::downgrade(self.consume_message_concurrently_service.as_ref().unwrap())
    }
}

impl<T, K> ConsumeMessageServiceGeneral<T, K>
where
    T: ConsumeMessageServiceTrait,
    K: ConsumeMessageServiceTrait,
{
    pub fn start(&mut self) {
        if let Some(consume_message_concurrently_service) =
            &mut self.consume_message_concurrently_service
        {
            let consume_message_concurrently_service_weak =
                ArcRefCellWrapper::downgrade(consume_message_concurrently_service);
            consume_message_concurrently_service.start(consume_message_concurrently_service_weak);
        }

        if let Some(consume_message_orderly_service) = &mut self.consume_message_orderly_service {
            let consume_message_pop_concurrently_service_weak =
                ArcRefCellWrapper::downgrade(consume_message_orderly_service);
            consume_message_orderly_service.start(consume_message_pop_concurrently_service_weak);
        }
    }

    pub async fn shutdown(&mut self, await_terminate_millis: u64) {
        todo!()
    }

    pub fn update_core_pool_size(&self, core_pool_size: usize) {
        todo!()
    }

    pub fn inc_core_pool_size(&self) {
        todo!()
    }

    pub fn dec_core_pool_size(&self) {
        todo!()
    }

    pub fn get_core_pool_size(&self) -> usize {
        todo!()
    }

    pub async fn consume_message_directly(
        &self,
        msg: &MessageExt,
        broker_name: &str,
    ) -> ConsumeMessageDirectlyResult {
        todo!()
    }

    pub async fn submit_consume_request(
        &self,
        msgs: Vec<ArcRefCellWrapper<MessageClientExt>>,
        process_queue: Arc<ProcessQueue>,
        message_queue: MessageQueue,
        dispatch_to_consume: bool,
    ) {
        if let Some(consume_message_concurrently_service) =
            &self.consume_message_concurrently_service
        {
            consume_message_concurrently_service
                .submit_consume_request(
                    ArcRefCellWrapper::downgrade(consume_message_concurrently_service),
                    msgs,
                    process_queue,
                    message_queue,
                    dispatch_to_consume,
                )
                .await;
        } else if let Some(consume_message_orderly_service) = &self.consume_message_orderly_service
        {
            consume_message_orderly_service
                .submit_consume_request(
                    ArcRefCellWrapper::downgrade(consume_message_orderly_service),
                    msgs,
                    process_queue,
                    message_queue,
                    dispatch_to_consume,
                )
                .await;
        }
    }

    pub async fn submit_pop_consume_request(
        &self,
        msgs: Vec<MessageExt>,
        process_queue: &PopProcessQueue,
        message_queue: &MessageQueue,
    ) {
        unimplemented!("ConsumeMessageServiceGeneral not support submit_pop_consume_request")
    }

    pub fn get_consume_message_concurrently_service(&self) -> ArcRefCellWrapper<T> {
        self.consume_message_concurrently_service
            .as_ref()
            .unwrap()
            .clone()
    }
}

pub struct ConsumeMessagePopServiceGeneral<T, K> {
    consume_message_pop_concurrently_service: Option<ArcRefCellWrapper<T>>,
    consume_message_pop_orderly_service: Option<ArcRefCellWrapper<K>>,
}

impl<T, K> ConsumeMessagePopServiceGeneral<T, K> {
    pub fn new(
        consume_message_pop_concurrently_service: Option<ArcRefCellWrapper<T>>,
        consume_message_pop_orderly_service: Option<ArcRefCellWrapper<K>>,
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
        if let Some(consume_message_pop_concurrently_service) =
            &mut self.consume_message_pop_concurrently_service
        {
            let consume_message_pop_concurrently_service_weak =
                ArcRefCellWrapper::downgrade(consume_message_pop_concurrently_service);
            consume_message_pop_concurrently_service
                .start(consume_message_pop_concurrently_service_weak);
        }

        if let Some(consume_message_pop_orderly_service) =
            &mut self.consume_message_pop_orderly_service
        {
            let consume_message_pop_orderly_service_weak =
                ArcRefCellWrapper::downgrade(consume_message_pop_orderly_service);
            consume_message_pop_orderly_service.start(consume_message_pop_orderly_service_weak);
        }
    }

    pub async fn shutdown(&mut self, await_terminate_millis: u64) {
        todo!()
    }

    fn update_core_pool_size(&self, core_pool_size: usize) {
        todo!()
    }

    fn inc_core_pool_size(&self) {
        todo!()
    }

    fn dec_core_pool_size(&self) {
        todo!()
    }

    fn get_core_pool_size(&self) -> usize {
        todo!()
    }

    async fn consume_message_directly(
        &self,
        msg: &MessageExt,
        broker_name: &str,
    ) -> ConsumeMessageDirectlyResult {
        todo!()
    }

    pub async fn submit_consume_request(
        &self,
        msgs: Vec<ArcRefCellWrapper<MessageClientExt>>,
        process_queue: Arc<ProcessQueue>,
        message_queue: MessageQueue,
        dispatch_to_consume: bool,
    ) {
        unimplemented!("submit_consume_request")
    }

    async fn submit_pop_consume_request(
        &self,
        msgs: Vec<MessageExt>,
        process_queue: &PopProcessQueue,
        message_queue: &MessageQueue,
    ) {
        todo!()
    }
}

pub trait ConsumeMessageServiceTrait {
    fn start(&mut self, this: WeakCellWrapper<Self>);

    async fn shutdown(&mut self, await_terminate_millis: u64);

    fn update_core_pool_size(&self, core_pool_size: usize);

    fn inc_core_pool_size(&self);

    fn dec_core_pool_size(&self);

    fn get_core_pool_size(&self) -> usize;

    async fn consume_message_directly(
        &self,
        msg: &MessageExt,
        broker_name: &str,
    ) -> ConsumeMessageDirectlyResult;

    async fn submit_consume_request(
        &self,
        this: WeakCellWrapper<Self>,
        msgs: Vec<ArcRefCellWrapper<MessageClientExt>>,
        process_queue: Arc<ProcessQueue>,
        message_queue: MessageQueue,
        dispatch_to_consume: bool,
    );

    async fn submit_pop_consume_request(
        &self,
        msgs: Vec<MessageExt>,
        process_queue: &PopProcessQueue,
        message_queue: &MessageQueue,
    );
}
