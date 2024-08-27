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
use std::sync::Arc;

use crate::base::response_future::ResponseFuture;
use crate::protocol::remoting_command::RemotingCommand;
use crate::runtime::RPCHook;

/// `RemotingService` trait defines the core functionalities for a remoting service.
///
/// This trait outlines the essential operations for starting, shutting down, and managing RPC hooks
/// within a remoting service. Implementors of this trait are expected to provide concrete
/// implementations for these operations, facilitating the management of remote procedure calls.
///
/// # Requirements
/// Implementors must be `Send` to ensure thread safety, allowing instances to be transferred
/// across thread boundaries.
#[allow(async_fn_in_trait)]
pub trait RemotingService: Send {
    /// Asynchronously starts the remoting service.
    ///
    /// This function should initialize and start the service, making it ready to handle incoming
    /// or outgoing remote procedure calls. The exact implementation details, such as opening
    /// network connections or preparing internal state, are left to the implementor.
    async fn start(&self);

    /// Shuts down the remoting service.
    ///
    /// This function is responsible for gracefully shutting down the service. It should ensure
    /// that all resources are released, and any ongoing operations are completed or aborted
    /// appropriately before the service stops.
    fn shutdown(&mut self);

    /// Registers an RPC hook.
    ///
    /// This function allows for the registration of an RPC hook, which can be used to intercept
    /// and modify the behavior of remote procedure calls. Hooks can be used for logging,
    /// monitoring, or modifying the requests or responses of RPCs.
    ///
    /// # Arguments
    /// * `hook` - An implementation of the `RPCHook` trait that will be registered.
    fn register_rpc_hook(&mut self, hook: Arc<Box<dyn RPCHook>>);

    /// Clears all registered RPC hooks.
    ///
    /// This function removes all previously registered RPC hooks, returning the service to its
    /// default state without any hooks. This can be useful for cleanup or when changing the
    /// configuration of the service.
    fn clear_rpc_hook(&mut self);
}
pub trait InvokeCallback {
    fn operation_complete(&self, response_future: ResponseFuture);
    fn operation_succeed(&self, response: RemotingCommand);
    fn operation_fail(&self, throwable: Box<dyn std::error::Error>);
}
