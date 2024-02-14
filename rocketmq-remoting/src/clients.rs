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

use std::{
    collections::HashMap,
    fmt::{Debug, Formatter},
    sync::Arc,
    time::Duration,
};

pub use blocking_client::BlockingClient;
pub use client::Client;
use rocketmq_common::TokioExecutorService;

use crate::net::ResponseFuture;
use crate::{
    protocol::remoting_command::RemotingCommand,
    remoting::{InvokeCallback, RemotingService},
    runtime::processor::RequestProcessor,
};

mod async_client;
mod blocking_client;

mod client;

#[derive(Default)]
pub struct RemoteClient {
    inner: HashMap<String, BlockingClient>,
}

impl Clone for RemoteClient {
    fn clone(&self) -> Self {
        Self {
            inner: HashMap::new(),
        }
    }
}

impl Debug for RemoteClient {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("RemoteClient")
    }
}

impl RemoteClient {
    /// Create a new `RemoteClient` instance.
    pub fn new() -> Self {
        Self {
            inner: HashMap::new(),
        }
    }

    pub fn invoke_oneway(
        &mut self,
        addr: String,
        request: RemotingCommand,
        timeout: Duration,
    ) -> anyhow::Result<()> {
        self.inner
            .entry(addr.clone())
            .or_insert_with(|| BlockingClient::connect(addr).unwrap())
            .invoke_oneway(request, timeout)
    }
}

trait RemotingClient: RemotingService {
    fn update_name_server_address_list(&mut self, addrs: Vec<String>);

    fn get_name_server_address_list(&self) -> Vec<String>;

    fn get_available_name_srv_list(&self) -> Vec<String>;

    fn invoke_sync(
        &mut self,
        addr: String,
        request: RemotingCommand,
        timeout_millis: u64,
    ) -> Result<RemotingCommand, Box<dyn std::error::Error>>;
    fn invoke_async(
        &mut self,
        addr: String,
        request: RemotingCommand,
        timeout_millis: u64,
        invoke_callback: Arc<dyn InvokeCallback>,
    ) -> Result<(), Box<dyn std::error::Error>>;
    fn invoke_oneway(
        &self,
        addr: String,
        request: RemotingCommand,
        timeout_millis: u64,
    ) -> Result<(), Box<dyn std::error::Error>>;

    fn invoke(
        &mut self,
        addr: String,
        request: RemotingCommand,
        timeout_millis: u64,
    ) -> Result<RemotingCommand, Box<dyn std::error::Error>> {
        let future = Arc::new(DefaultInvokeCallback {});
        match self.invoke_async(addr, request, timeout_millis, future) {
            Ok(_) => Ok(RemotingCommand::default()),
            Err(e) => Err(e),
        }
    }

    fn register_processor(
        &mut self,
        request_code: i32,
        processor: impl RequestProcessor + Send + Sync + 'static,
        executor: Arc<TokioExecutorService>,
    );

    fn set_callback_executor(&mut self, executor: Arc<TokioExecutorService>);

    fn is_address_reachable(&mut self, addr: String);
}

struct DefaultInvokeCallback;

impl InvokeCallback for DefaultInvokeCallback {
    fn operation_complete(&self, _response_future: ResponseFuture) {}

    fn operation_succeed(&self, _response: RemotingCommand) {}

    fn operation_fail(&self, _throwable: Box<dyn std::error::Error>) {}
}
