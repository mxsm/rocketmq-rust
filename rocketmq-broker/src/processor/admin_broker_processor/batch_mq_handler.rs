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
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::body::request::lock_batch_request_body::LockBatchRequestBody;
use rocketmq_remoting::protocol::body::response::lock_batch_response_body::LockBatchResponseBody;
use rocketmq_remoting::protocol::body::unlock_batch_request_body::UnlockBatchRequestBody;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::protocol::RemotingDeserializable;
use rocketmq_remoting::protocol::RemotingSerializable;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_rust::ArcMut;
use rocketmq_rust::CountDownLatch;
use rocketmq_store::base::message_store::MessageStore;
use tokio::sync::Mutex;
use tracing::warn;

use crate::broker_runtime::BrokerRuntimeInner;

#[derive(Clone)]
pub(super) struct BatchMqHandler<MS: MessageStore> {
    broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
}

impl<MS: MessageStore> BatchMqHandler<MS> {
    pub(super) fn new(broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>) -> Self {
        Self { broker_runtime_inner }
    }

    pub async fn lock_natch_mq(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let mut request_body = LockBatchRequestBody::decode(request.get_body().unwrap()).unwrap();
        let mut lock_ok_mqset = HashSet::new();
        let self_lock_okmqset = self.broker_runtime_inner.rebalance_lock_manager().try_lock_batch(
            request_body.consumer_group.as_ref().unwrap(),
            &request_body.mq_set,
            request_body.client_id.as_ref().unwrap(),
        );
        if request_body.only_this_broker || !self.broker_runtime_inner.broker_config().lock_in_strict_mode {
            lock_ok_mqset = self_lock_okmqset;
        } else {
            request_body.only_this_broker = true;
            let replica_size = self.broker_runtime_inner.message_store_config().total_replicas;
            let quorum = replica_size / 2 + 1;
            if quorum <= 1 {
                lock_ok_mqset = self_lock_okmqset;
            } else {
                let mut mq_lock_map = HashMap::with_capacity(32);
                for mq in self_lock_okmqset.iter() {
                    *mq_lock_map.entry(mq.clone()).or_insert(0) += 1;
                }
                let mut addr_map = HashMap::with_capacity(8);
                addr_map.extend(self.broker_runtime_inner.broker_member_group().broker_addrs.clone());
                addr_map.remove(&self.broker_runtime_inner.broker_config().broker_identity.broker_id);

                let count_down_latch = CountDownLatch::new(addr_map.len() as u32);
                let request_body = Bytes::from(request_body.encode().expect("lockBatchMQ encode error"));
                let mq_lock_map_arc = Arc::new(Mutex::new(mq_lock_map.clone()));
                for (_, broker_addr) in addr_map {
                    let count_down_latch = count_down_latch.clone();
                    let broker_outer_api_ = self.broker_runtime_inner.clone();
                    let mq_lock_map = mq_lock_map_arc.clone();
                    let request_body_cloned = request_body.clone();
                    tokio::spawn(async move {
                        let result = broker_outer_api_
                            .broker_outer_api()
                            .lock_batch_mq_async(&broker_addr, request_body_cloned, 1000)
                            .await;
                        match result {
                            Ok(lock_ok_mqs) => {
                                let mut mq_lock_map = mq_lock_map.lock().await;
                                for mq in lock_ok_mqs {
                                    *mq_lock_map.entry(mq).or_insert(0) += 1;
                                }
                            }
                            Err(e) => {
                                warn!("lockBatchMQAsync failed: {:?}", e);
                            }
                        }
                        count_down_latch.count_down().await;
                    });
                }
                count_down_latch.wait_timeout(Duration::from_secs(2)).await;

                let mq_lock_map = mq_lock_map_arc.lock().await;
                for (mq, count) in &*mq_lock_map {
                    if *count >= quorum {
                        lock_ok_mqset.insert(mq.clone());
                    }
                }
            }
        }
        let response_body = LockBatchResponseBody {
            lock_ok_mq_set: lock_ok_mqset,
        };
        Ok(Some(
            RemotingCommand::create_response_command()
                .set_body(response_body.encode().expect("lockBatchMQ encode error")),
        ))
    }

    pub async fn unlock_batch_mq(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let mut request_body = UnlockBatchRequestBody::decode(request.get_body().unwrap()).unwrap();
        if request_body.only_this_broker || !self.broker_runtime_inner.broker_config().lock_in_strict_mode {
            self.broker_runtime_inner.rebalance_lock_manager().unlock_batch(
                request_body.consumer_group.as_ref().unwrap(),
                &request_body.mq_set,
                request_body.client_id.as_ref().unwrap(),
            );
        } else {
            request_body.only_this_broker = true;
            let request_body = Bytes::from(request_body.encode().expect("unlockBatchMQ encode error"));
            for broker_addr in self.broker_runtime_inner.broker_member_group().broker_addrs.values() {
                match self
                    .broker_runtime_inner
                    .broker_outer_api()
                    .unlock_batch_mq_async(broker_addr, request_body.clone(), 1000)
                    .await
                {
                    Ok(_) => {}
                    Err(e) => {
                        warn!("unlockBatchMQ exception on {}, {}", broker_addr, e);
                    }
                }
            }
        }
        Ok(Some(RemotingCommand::create_response_command()))
    }
}
