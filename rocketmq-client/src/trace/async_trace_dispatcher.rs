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

use std::any::Any;
use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_remoting::runtime::RPCHook;
use rocketmq_rust::ArcMut;

use crate::base::access_channel::AccessChannel;
use crate::consumer::consumer_impl::default_mq_push_consumer_impl::DefaultMQPushConsumerImpl;
use crate::producer::producer_impl::default_mq_producer_impl::DefaultMQProducerImpl;
use crate::trace::trace_dispatcher::TraceDispatcher;
use crate::trace::trace_dispatcher::Type;

pub struct AsyncTraceDispatcher;

impl AsyncTraceDispatcher {
    pub fn new(group: &str, type_: Type, trace_topic_name: &str, rpc_hook: Option<Arc<dyn RPCHook>>) -> Self {
        AsyncTraceDispatcher
    }
}

impl TraceDispatcher for AsyncTraceDispatcher {
    fn start(&self, name_srv_addr: &str, access_channel: AccessChannel) -> rocketmq_error::RocketMQResult<()> {
        todo!()
    }

    fn append(&self, ctx: &dyn Any) -> bool {
        todo!()
    }

    fn flush(&self) -> rocketmq_error::RocketMQResult<()> {
        todo!()
    }

    fn shutdown(&self) {
        todo!()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn Any {
        self
    }
}

impl AsyncTraceDispatcher {
    pub fn set_host_producer(&mut self, host_producer: ArcMut<DefaultMQProducerImpl>) {}
    pub fn set_host_consumer(&mut self, host_producer: ArcMut<DefaultMQPushConsumerImpl>) {}

    pub fn set_namespace_v2(&mut self, namespace_v2: Option<CheetahString>) {}
}
