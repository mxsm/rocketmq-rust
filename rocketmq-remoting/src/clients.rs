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
pub use blocking_client::BlockingClient;
pub use client::Client;

use crate::base::response_future::ResponseFuture;
use crate::protocol::remoting_command::RemotingCommand;
use crate::remoting::InvokeCallback;
use crate::remoting::RemotingService;
use crate::runtime::processor::RequestProcessor;
use crate::Result;

mod async_client;
mod blocking_client;

mod client;
pub mod rocketmq_default_impl;

/// `RemotingClient` trait extends `RemotingService` to provide client-specific remote interaction
/// functionalities.
///
/// This trait defines methods for managing name remoting_server addresses, invoking commands
/// asynchronously or without expecting a response, checking if an address is reachable, and closing
/// clients connected to specific addresses.
#[allow(async_fn_in_trait)]
pub trait RemotingClient: RemotingService {
    /// Updates the list of name remoting_server addresses.
    ///
    /// # Arguments
    /// * `addrs` - A list of name remoting_server addresses to update.
    async fn update_name_server_address_list(&self, addrs: Vec<String>);

    /// Retrieves the current list of name remoting_server addresses.
    ///
    /// # Returns
    /// A vector containing the current list of name remoting_server addresses.
    fn get_name_server_address_list(&self) -> &[String];

    /// Retrieves a list of available name remoting_server addresses.
    ///
    /// # Returns
    /// A vector containing the list of available name remoting_server addresses.
    fn get_available_name_srv_list(&self) -> Vec<String>;

    /// Asynchronously invokes a command on a specified address.
    ///
    /// # Arguments
    /// * `addr` - The address to invoke the command on.
    /// * `request` - The `RemotingCommand` to be sent.
    /// * `timeout_millis` - The timeout for the operation in milliseconds.
    ///
    /// # Returns
    /// A `Result` containing either the response `RemotingCommand` or an `Error`.
    async fn invoke_async(
        &self,
        addr: Option<String>,
        request: RemotingCommand,
        timeout_millis: u64,
    ) -> Result<RemotingCommand>;

    /// Invokes a command on a specified address without waiting for a response.
    ///
    /// # Arguments
    /// * `addr` - The address to invoke the command on.
    /// * `request` - The `RemotingCommand` to be sent.
    /// * `timeout_millis` - The timeout for the operation in milliseconds.
    async fn invoke_oneway(&self, addr: String, request: RemotingCommand, timeout_millis: u64);

    /// Checks if a specified address is reachable.
    ///
    /// # Arguments
    /// * `addr` - The address to check for reachability.
    fn is_address_reachable(&mut self, addr: String);

    /// Closes clients connected to the specified addresses.
    ///
    /// # Arguments
    /// * `addrs` - A list of addresses whose clients should be closed.
    fn close_clients(&mut self, addrs: Vec<String>);

    fn register_processor(&mut self, processor: impl RequestProcessor + Sync);
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
