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
use rocketmq_common::{common::future::CompletableFuture, TokioExecutorService};

use crate::{
    net::ResponseFuture,
    protocol::remoting_command::RemotingCommand,
    remoting::{InvokeCallback, RemotingService},
    runtime::processor::RequestProcessor,
};

mod async_client;
mod blocking_client;

mod client;
mod rocketmq_default_impl;

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
    fn update_name_server_address_list(&mut self, addrs: Vec<String>);

    fn get_name_server_address_list(&self) -> Vec<String>;

    fn get_available_name_srv_list(&self) -> Vec<String>;

    fn invoke_sync(
        &mut self,
        addr: String,
        request: RemotingCommand,
        timeout_millis: u64,
    ) -> Result<RemotingCommand, Box<dyn std::error::Error>>;

    async fn invoke_async(
        &mut self,
        addr: String,
        request: RemotingCommand,
        timeout_millis: u64,
        invoke_callback: impl InvokeCallback,
    ) -> Result<(), Box<dyn std::error::Error>>;

    fn invoke_oneway(
        &self,
        addr: String,
        request: RemotingCommand,
        timeout_millis: u64,
    ) -> Result<(), Box<dyn std::error::Error>>;

    async fn invoke(
        &mut self,
        addr: String,
        request: RemotingCommand,
        timeout_millis: u64,
    ) -> Result<CompletableFuture<RemotingCommand>, Box<dyn std::error::Error>> {
        let completable_future = CompletableFuture::new();
        let sender = completable_future.get_sender();
        match self
            .invoke_async(
                addr,
                request,
                timeout_millis,
                |response: Option<RemotingCommand>,
                 error: Option<Box<dyn std::error::Error>>,
                 _response_future: Option<ResponseFuture>| {
                    if let Some(response) = response {
                        let _ = sender.blocking_send(response);
                    } else if let Some(_error) = error {
                    }
                },
            )
            .await
        {
            Ok(_) => Ok(completable_future),
            Err(err) => Err(err),
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
