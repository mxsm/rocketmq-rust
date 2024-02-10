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

use crate::{protocol::remoting_command::RemotingCommand, runtime::RPCHook};

pub trait RemotingService {
    fn start(&mut self) -> impl std::future::Future<Output = ()> + Send;

    fn shutdown(&mut self);

    fn register_rpc_hook(&mut self, hook: impl RPCHook);

    fn clear_rpc_hook(&mut self);
}

pub struct ResponseFuture;

pub trait InvokeCallback {
    fn operation_complete(&self, response_future: ResponseFuture);
    fn operation_succeed(&self, _response: RemotingCommand) {}
    fn operation_fail(&self, _throwable: Box<dyn std::error::Error>) {}
}
