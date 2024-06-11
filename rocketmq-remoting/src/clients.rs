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
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;
use std::time::Duration;

pub use blocking_client::BlockingClient;
pub use client::Client;
use rocketmq_common::TokioExecutorService;

use crate::error::RemotingError;
use crate::net::ResponseFuture;
use crate::protocol::remoting_command::RemotingCommand;
use crate::remoting::InvokeCallback;
use crate::remoting::RemotingService;
use crate::runtime::processor::RequestProcessor;

mod async_client;
mod blocking_client;

mod client;
pub mod rocketmq_default_impl;

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

#[allow(async_fn_in_trait)]
pub trait RemotingClient: RemotingService {
    fn update_name_server_address_list(&self, addrs: Vec<String>);

    fn get_name_server_address_list(&self) -> Vec<String>;

    fn get_available_name_srv_list(&self) -> Vec<String>;

    async fn invoke_async(
        &self,
        addr: String,
        request: RemotingCommand,
        timeout_millis: u64,
    ) -> Result<RemotingCommand, RemotingError>;

    async fn invoke_oneway(&self, addr: String, request: RemotingCommand, timeout_millis: u64);

    fn is_address_reachable(&mut self, addr: String);

    fn close_clients(&mut self, addrs: Vec<String>);
}

impl<T> InvokeCallback for T
where
    T: Fn(Option<RemotingCommand>, Option<Box<dyn std::error::Error>>, Option<ResponseFuture>)
        + Send
        + Sync,
{
    fn operation_complete(&self, response_future: ResponseFuture) {
        self(None, None, Some(response_future))
    }

    fn operation_succeed(&self, response: RemotingCommand) {
        self(Some(response), None, None)
    }

    fn operation_fail(&self, throwable: Box<dyn std::error::Error>) {
        self(None, Some(throwable), None)
    }
}
