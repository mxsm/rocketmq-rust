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

//! Runtime components for remoting operations.
//!
//! This module provides the core abstractions and configuration for handling
//! RPC requests, including hooks, processors, and connection management.

use std::net::SocketAddr;
use std::sync::Arc;

use crate::protocol::remoting_command::RemotingCommand;

pub mod config;
pub mod connection_handler_context;
pub mod processor;
pub mod processor_v2;

/// Defines hooks for intercepting RPC requests and responses.
///
/// Implementors can execute custom logic before requests are processed and after
/// responses are generated. This enables authentication, logging, metrics collection,
/// and other cross-cutting concerns.
///
/// Implementations must be thread-safe and have a static lifetime.
pub trait RPCHook: Send + Sync + 'static {
    /// Invoked before processing an RPC request.
    ///
    /// The request may be modified before processing. The remote address identifies
    /// the caller.
    ///
    /// # Errors
    ///
    /// Returns an error if the request should not be processed.
    fn do_before_request(
        &self,
        remote_addr: SocketAddr,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<()>;

    /// Invoked after generating an RPC response.
    ///
    /// The response may be modified before being sent to the caller. Both the
    /// original request and the remote address are provided for context.
    ///
    /// # Errors
    ///
    /// Returns an error if post-processing fails.
    fn do_after_response(
        &self,
        remote_addr: SocketAddr,
        request: &RemotingCommand,
        response: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<()>;
}

/// Thread-safe, reference-counted RPC hook.
pub type RPCHookArc = Arc<dyn RPCHook>;
