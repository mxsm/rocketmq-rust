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

use std::net::SocketAddr;

use crate::protocol::remoting_command::RemotingCommand;

pub mod config;
pub mod connection_handler_context;
pub mod processor;
pub mod processor_v2;

/// Trait defining hooks for RPC (Remote Procedure Call) interactions.
///
/// This trait provides hooks that can be implemented to execute custom logic
/// before and after an RPC request is processed. It is designed for scenarios
/// where additional operations such as logging, authentication, or metrics collection
/// are required around RPC requests. Implementors of this trait must be `Send`, `Sync`,
/// and have a `'static` lifetime, ensuring they can be safely shared across threads
/// and have a lifetime covering the entire execution of the program.
pub trait RPCHook: Send + Sync + 'static {
    /// Executes custom logic before an RPC request is processed.
    ///
    /// This method allows for actions such as authentication checks, logging, or
    /// initialization tasks to be performed prior to processing the request. It takes
    /// a mutable reference to the `RemotingCommand` allowing for the request to be
    /// modified before processing.
    ///
    /// # Arguments
    /// * `remote_addr` - The socket address of the remote caller.
    /// * `request` - A mutable reference to the `RemotingCommand` representing the incoming
    ///   request.
    ///
    /// # Returns
    /// A `Result` indicating the outcome of the pre-processing step. Returning an `Err`
    /// value can be used to halt the processing of the request.
    fn do_before_request(
        &self,
        remote_addr: SocketAddr,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<()>;

    /// Executes custom logic after an RPC response is processed.
    ///
    /// This method allows for actions such as logging, metrics collection, or
    /// post-processing tasks to be performed after the response is generated.
    /// It takes references to the remote caller's address, the original request,
    /// and a mutable reference to the response, enabling modifications to the response
    /// before it is sent back to the caller.
    ///
    /// # Arguments
    /// * `remote_addr` - The socket address of the remote caller.
    /// * `request` - A reference to the `RemotingCommand` representing the original request.
    /// * `response` - A mutable reference to the `RemotingCommand` representing the response to be
    ///   sent back to the caller.
    ///
    /// # Returns
    /// A `Result` indicating the outcome of the post-processing step. Returning an `Err`
    /// value can be used to signal a failure in the post-processing logic.
    fn do_after_response(
        &self,
        remote_addr: SocketAddr,
        request: &RemotingCommand,
        response: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<()>;
}
