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
use std::collections::HashMap;
use std::collections::HashSet;

use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::body::request::lock_batch_request_body::LockBatchRequestBody;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::protocol::RemotingDeserializable;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;

use crate::processor::admin_broker_processor::Inner;

#[derive(Clone)]
pub(super) struct BatchMqHandler {
    inner: Inner,
}

impl BatchMqHandler {
    pub(super) fn new(inner: Inner) -> Self {
        Self { inner }
    }

    pub async fn lock_natch_mq(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        request: RemotingCommand,
    ) -> Option<RemotingCommand> {
        let mut request_body = LockBatchRequestBody::decode(request.get_body().unwrap()).unwrap();
        let mut lock_ok_mqset = HashSet::new();
        let self_lock_okmqset = self.inner.rebalance_lock_manager.try_lock_batch(
            request_body.consumer_group.as_ref().unwrap(),
            &request_body.mq_set,
            request_body.client_id.as_ref().unwrap(),
        );
        if request_body.only_this_broker || !self.inner.broker_config.lock_in_strict_mode {
            lock_ok_mqset = self_lock_okmqset;
        } else {
            request_body.only_this_broker = true;
            let replica_size = self.inner.message_store_config.total_replicas;
            let quorum = replica_size / 2 + 1;
            if quorum <= 1 {
                lock_ok_mqset = self_lock_okmqset;
            } else {
                let mut mq_lock_map = HashMap::with_capacity(32);
                for mq in self_lock_okmqset.iter() {
                    if !mq_lock_map.contains_key(mq) {
                        mq_lock_map.insert(mq.clone(), 0);
                    }
                    mq_lock_map.insert(mq.clone(), mq_lock_map.get(mq).unwrap() + 1);
                }
                let mut addr_map = HashMap::with_capacity(8);
                addr_map.extend(self.inner.broker_member_group.broker_addrs.clone());
                addr_map.remove(&self.inner.broker_config.broker_identity.broker_id);

                for (mq, broker_addr) in addr_map {

                }
            }
        }
        unimplemented!("lock_natch_mq")
    }

    pub async fn unlock_batch_mq(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        request: RemotingCommand,
    ) -> Option<RemotingCommand> {
        unimplemented!("unlockBatchMQ")
    }
}
