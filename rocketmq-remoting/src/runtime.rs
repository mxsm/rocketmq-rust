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

use std::{collections::HashMap, sync::Arc, time::Duration};

use rocketmq_common::{common::Pair, TokioExecutorService};
use tokio::{time, time::timeout};

use crate::{
    clients::Client,
    net::ResponseFuture,
    protocol::{remoting_command::RemotingCommand, RemotingCommandType},
    remoting::InvokeCallback,
    runtime::{processor::RequestProcessor, server::ConnectionHandlerContext},
};

pub mod config;
pub mod processor;
pub mod server;

pub type ArcDefaultRequestProcessor =
    Arc<tokio::sync::RwLock<Box<dyn RequestProcessor + Send + Sync + 'static>>>;

pub type ArcProcessorTable =
    Arc<tokio::sync::RwLock<HashMap<i32, Box<dyn RequestProcessor + Sync + Send + 'static>>>>;

pub trait RPCHook: Send + Sync + 'static {
    fn do_before_request(&self, remote_addr: &str, request: &RemotingCommand);

    fn do_after_response(
        &self,
        remote_addr: &str,
        request: &RemotingCommand,
        response: &RemotingCommand,
    );
}

pub struct ServiceBridge {
    //Limiting the maximum number of one-way requests.
    pub(crate) semaphore_oneway: tokio::sync::Semaphore,
    //Limiting the maximum number of asynchronous requests.
    pub(crate) semaphore_async: tokio::sync::Semaphore,
    //Cache mapping between request unique code(opaque-request header) and ResponseFuture.
    pub(crate) response_table: HashMap<i32, ResponseFuture>,

    pub(crate) processor_table: Option<ArcProcessorTable>,
    pub(crate) default_request_processor_pair: Option<ArcDefaultRequestProcessor>,

    pub(crate) processor_table1: HashMap<
        i32, /* request code */
        Pair<Box<dyn RequestProcessor + Send + Sync>, TokioExecutorService>,
    >,
    pub(crate) default_request_processor_pair1:
        Option<Pair<Box<dyn RequestProcessor + Send + Sync>, TokioExecutorService>>,

    pub(crate) rpc_hooks: Vec<Box<dyn RPCHook>>,
}

impl ServiceBridge {
    pub fn new() -> Self {
        Self {
            semaphore_oneway: tokio::sync::Semaphore::new(1000),
            semaphore_async: tokio::sync::Semaphore::new(1000),
            response_table: HashMap::new(),
            processor_table: Some(Arc::new(tokio::sync::RwLock::new(HashMap::new()))),
            default_request_processor_pair: None,
            processor_table1: Default::default(),
            default_request_processor_pair1: None,
            rpc_hooks: Vec::new(),
        }
    }
}

impl Default for ServiceBridge {
    fn default() -> Self {
        Self::new()
    }
}

impl ServiceBridge {
    pub fn process_message_received(
        &mut self,
        ctx: ConnectionHandlerContext,
        msg: RemotingCommand,
    ) {
        match msg.get_type() {
            RemotingCommandType::REQUEST => self.process_request_command(ctx, msg),
            RemotingCommandType::RESPONSE => self.process_response_command(ctx, msg),
        }
    }

    pub fn process_request_command(
        &mut self,
        _ctx: ConnectionHandlerContext,
        _msg: RemotingCommand,
    ) {
    }

    pub fn process_response_command(
        &mut self,
        _ctx: ConnectionHandlerContext,
        _msg: RemotingCommand,
    ) {
    }

    pub async fn invoke_async(
        client: &mut Client,
        //client: Arc<Mutex<Client>>,
        request: RemotingCommand,
        timeout_millis: u64,
        invoke_callback: impl InvokeCallback,
    ) {
        if let Ok(resp) = time::timeout(Duration::from_millis(timeout_millis), async {
            client.invoke(request).await.unwrap()
        })
        .await
        {
            invoke_callback.operation_succeed(resp)
        }
    }

    pub async fn invoke_sync(
        client: &mut Client,
        request: RemotingCommand,
        timeout_millis: u64,
    ) -> Option<RemotingCommand> {
        let result = timeout(Duration::from_millis(timeout_millis), async {
            // client.lock().await.invoke(request).await.unwrap()
            client.invoke(request).await.unwrap()
        })
        .await;
        Some(result.unwrap())
    }
}
