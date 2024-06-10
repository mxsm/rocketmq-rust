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
use std::sync::Arc;

use crate::protocol::remoting_command::RemotingCommand;
use crate::runtime::processor::RequestProcessor;

pub mod config;
pub mod processor;
pub mod server;

pub type BoxedRequestProcessor = Arc<Box<dyn RequestProcessor + Send + Sync + 'static>>;

pub type RequestProcessorTable = HashMap<i32, BoxedRequestProcessor>;

pub trait RPCHook: Send + Sync + 'static {
    fn do_before_request(&self, remote_addr: &str, request: &RemotingCommand);

    fn do_after_response(
        &self,
        remote_addr: &str,
        request: &RemotingCommand,
        response: &RemotingCommand,
    );
}

/*pub struct ServiceBridge {
    //Limiting the maximum number of one-way requests.
    pub(crate) semaphore_oneway: tokio::sync::Semaphore,
    //Limiting the maximum number of asynchronous requests.
    pub(crate) semaphore_async: tokio::sync::Semaphore,
    //Cache mapping between request unique code(opaque-request header) and ResponseFuture.
    pub(crate) response_table: HashMap<i32, ResponseFuture>,

    pub(crate) processor_table: Option<RequestProcessorTable>,
    pub(crate) default_request_processor_pair: Option<BoxedRequestProcessor>,

    pub(crate) rpc_hooks: Vec<Box<dyn RPCHook>>,
}*/
