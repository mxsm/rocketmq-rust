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
use rocketmq_common::common::message::message_queue::MessageQueue;

use crate::rpc::rpc_request::RpcRequest;
use crate::rpc::rpc_response::RpcResponse;
use crate::Result;

#[trait_variant::make(RpcClient:Send)]
pub trait RpcClientLocal {
    async fn invoke(&self, request: RpcRequest, timeout_millis: u64) -> Result<RpcResponse>;

    async fn invoke_mq(
        &self,
        mq: MessageQueue,
        request: RpcRequest,
        timeout_millis: u64,
    ) -> Result<RpcResponse>;
}
