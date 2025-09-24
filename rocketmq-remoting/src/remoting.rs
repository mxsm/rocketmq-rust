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

use rocketmq_rust::WeakArcMut;

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
    async fn start(&self, this: WeakArcMut<Self>);

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
    fn register_rpc_hook(&mut self, hook: Box<dyn RPCHook>);

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

#[allow(unused_variables)]
pub(crate) mod inner {
    use std::collections::HashMap;
    use std::sync::Arc;

    use tracing::warn;

    use crate::base::response_future::ResponseFuture;
    use crate::net::channel::Channel;
    use crate::protocol::remoting_command::RemotingCommand;
    use crate::protocol::RemotingCommandType;
    use crate::remoting_server::rocketmq_tokio_server::Shutdown;
    use crate::runtime::connection_handler_context::ConnectionHandlerContext;
    use crate::runtime::processor::RequestProcessor;
    use crate::runtime::RPCHook;

    pub(crate) struct RemotingGeneralHandler<RP> {
        pub(crate) request_processor: RP,
        //pub(crate) shutdown: Shutdown,
        pub(crate) rpc_hooks: Vec<Box<dyn RPCHook>>,
        pub(crate) response_table: HashMap<i32, ResponseFuture>,
    }

    impl<RP> RemotingGeneralHandler<RP>
    where
        RP: RequestProcessor + Sync + 'static,
    {
        pub async fn process_message_received(
            &mut self,
            ctx: &ConnectionHandlerContext,
            cmd: RemotingCommand,
        ) {
            match cmd.get_type() {
                RemotingCommandType::REQUEST => {
                    self.process_request_command(ctx, cmd);
                }
                RemotingCommandType::RESPONSE => {
                    self.process_response_command(ctx, cmd);
                }
            }
        }

        fn process_request_command(
            &mut self,
            ctx: &ConnectionHandlerContext,
            cmd: RemotingCommand,
        ) {
            /*let opaque = cmd.opaque();
            let reject_request = self.request_processor.reject_request(cmd.code());
            const REJECT_REQUEST_MSG: &str =
                "[REJECT REQUEST]system busy, start flow control for a while";
            if reject_request.0 {
                let response = if let Some(response) = reject_request.1 {
                    response
                } else {
                    RemotingCommand::create_response_command_with_code_remark(
                        ResponseCode::SystemBusy,
                        REJECT_REQUEST_MSG,
                    )
                };
                self.connection_handler_context
                    .channel
                    .connection
                    .send_command(response.set_opaque(opaque))
                    .await?;
                continue;
            }
            let oneway_rpc = cmd.is_oneway_rpc();
            //before handle request hooks

            let exception = self
                .do_before_rpc_hooks(&(self.channel_inner.1), Some(&mut cmd))
                .err();
            //handle error if return have
            match self.handle_error(oneway_rpc, opaque, exception).await {
                crate::remoting_server::server::HandleErrorResult::Continue => continue,
                crate::remoting_server::server::HandleErrorResult::ReturnMethod => return Ok(()),
                crate::remoting_server::server::HandleErrorResult::GoHead => {}
            }

            let mut response = {
                let channel = self.channel_inner.1.clone();
                let ctx = self.connection_handler_context.clone();
                let result = self
                    .request_processor
                    .process_request(channel, ctx, &mut cmd)
                    .await
                    .unwrap_or_else(|_err| {
                        Some(RemotingCommand::create_response_command_with_code(
                            ResponseCode::SystemError,
                        ))
                    });
                result
            };

            let exception = self
                .do_after_rpc_hooks(&self.channel_inner.1, &cmd, response.as_mut())
                .err();

            match self.handle_error(oneway_rpc, opaque, exception).await {
                crate::remoting_server::server::HandleErrorResult::Continue => continue,
                crate::remoting_server::server::HandleErrorResult::ReturnMethod => return Ok(()),
                crate::remoting_server::server::HandleErrorResult::GoHead => {}
            }
            if response.is_none() || oneway_rpc {
                continue;
            }
            let response = response.unwrap();
            let result = self
                .channel_inner
                .0
                .connection
                .send_command(response.set_opaque(opaque))
                .await;
            match result {
                Ok(_) => {}
                Err(err) => match err {
                    RocketmqError::Io(io_error) => {
                        error!("connection disconnect: {}", io_error);
                        return Ok(());
                    }
                    _ => {
                        error!("send response failed: {}", err);
                    }
                },
            };*/
        }

        fn process_response_command(
            &mut self,
            ctx: &ConnectionHandlerContext,
            cmd: RemotingCommand,
        ) {
            if let Some(future) = self.response_table.remove(&cmd.opaque()) {
                let _ = future.tx.send(Ok(cmd));
            } else {
                warn!(
                    "receive response, cmd={}, but not matched any request, address={}, \
                     channelId={}",
                    cmd,
                    ctx.channel().remote_address(),
                    ctx.channel().channel_id(),
                );
            }
        }

        fn do_after_rpc_hooks(
            &self,
            channel: &Channel,
            request: &RemotingCommand,
            response: Option<&mut RemotingCommand>,
        ) -> rocketmq_error::RocketMQResult<()> {
            unimplemented!("do_after_rpc_hooks unimplemented")
        }

        pub fn do_before_rpc_hooks(
            &self,
            channel: &Channel,
            request: Option<&mut RemotingCommand>,
        ) -> rocketmq_error::RocketMQResult<()> {
            unimplemented!("do_before_rpc_hooks unimplemented")
        }

        pub fn register_rpc_hook(&mut self, hook: Box<dyn RPCHook>) {
            self.rpc_hooks.push(hook);
        }
    }
}
