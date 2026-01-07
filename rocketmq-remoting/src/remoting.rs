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
    fn register_rpc_hook(&mut self, hook: Arc<dyn RPCHook>);

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

    use rocketmq_error::RocketMQError;
    use rocketmq_error::RocketMQResult;
    use rocketmq_rust::ArcMut;
    use tracing::error;
    use tracing::warn;

    use crate::base::response_future::ResponseFuture;
    use crate::code::response_code::ResponseCode;
    use crate::net::channel::Channel;
    use crate::protocol::remoting_command::RemotingCommand;
    use crate::protocol::RemotingCommandType;
    use crate::runtime::connection_handler_context::ConnectionHandlerContext;
    use crate::runtime::processor::RequestProcessor;
    use crate::runtime::RPCHook;

    pub(crate) struct RemotingGeneralHandler<RP> {
        pub(crate) request_processor: RP,
        pub(crate) rpc_hooks: Vec<Arc<dyn RPCHook>>,
        pub(crate) response_table: ArcMut<HashMap<i32, ResponseFuture>>,
    }

    impl<RP> RemotingGeneralHandler<RP>
    where
        RP: RequestProcessor + Sync + 'static,
    {
        pub async fn process_message_received(&mut self, ctx: &mut ConnectionHandlerContext, cmd: RemotingCommand) {
            match cmd.get_type() {
                RemotingCommandType::REQUEST => match self.process_request_command(ctx, cmd).await {
                    Ok(_) => {}
                    Err(e) => {
                        error!("process request command failed: {}", e);
                    }
                },
                RemotingCommandType::RESPONSE => {
                    self.process_response_command(ctx, cmd);
                }
            }
        }

        async fn process_request_command(
            &mut self,
            ctx: &mut ConnectionHandlerContext,
            mut cmd: RemotingCommand,
        ) -> RocketMQResult<()> {
            let opaque = cmd.opaque();
            let reject_request = self.request_processor.reject_request(cmd.code());
            const REJECT_REQUEST_MSG: &str = "[REJECT REQUEST]system busy, start flow control for a while";
            if reject_request.0 {
                let response = if let Some(response) = reject_request.1 {
                    response
                } else {
                    RemotingCommand::create_response_command_with_code_remark(
                        ResponseCode::SystemBusy,
                        REJECT_REQUEST_MSG,
                    )
                };
                ctx.channel
                    .connection_mut()
                    .send_command(response.set_opaque(opaque))
                    .await?;
                return Ok(());
            }
            let oneway_rpc = cmd.is_oneway_rpc();
            //before handle request hooks
            let exception = self.do_before_rpc_hooks(ctx.channel(), Some(&mut cmd)).err();
            //handle error if return have
            match handle_error(ctx, oneway_rpc, opaque, exception).await {
                HandleErrorResult::ReturnMethod => return Ok(()),
                HandleErrorResult::GoHead => {}
            }

            let mut response = {
                let channel = ctx.channel.clone();
                let ctx = ctx.clone();
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

            let exception = self.do_after_rpc_hooks(ctx.channel(), &cmd, response.as_mut()).err();

            match handle_error(ctx, oneway_rpc, opaque, exception).await {
                HandleErrorResult::ReturnMethod => return Ok(()),
                HandleErrorResult::GoHead => {}
            }
            if response.is_none() || oneway_rpc {
                return Ok(());
            }
            let response = response.unwrap();
            let result = ctx
                .channel_mut()
                .connection_mut()
                .send_command(response.set_opaque(opaque))
                .await;
            match result {
                Ok(_) => {}
                Err(err) => match err {
                    RocketMQError::IO(io_error) => {
                        error!("connection disconnect: {}", io_error);
                        return Ok(());
                    }
                    _ => {
                        error!("send response failed: {}", err);
                    }
                },
            };
            Ok(())
        }

        fn process_response_command(&mut self, ctx: &mut ConnectionHandlerContext, cmd: RemotingCommand) {
            if let Some(future) = self.response_table.remove(&cmd.opaque()) {
                match future.tx.send(Ok(cmd)) {
                    Ok(_) => {}
                    Err(e) => {
                        warn!("send response to future failed, maybe timeout");
                    }
                }
            } else {
                warn!(
                    "receive response, cmd={}, but not matched any request, address={}, channelId={}",
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
            if let Some(response) = response {
                for hook in self.rpc_hooks.iter() {
                    hook.do_after_response(channel.remote_address(), request, response)?;
                }
            }
            Ok(())
        }

        pub fn do_before_rpc_hooks(
            &self,
            channel: &Channel,
            request: Option<&mut RemotingCommand>,
        ) -> rocketmq_error::RocketMQResult<()> {
            if let Some(request) = request {
                for hook in self.rpc_hooks.iter() {
                    hook.do_before_request(channel.remote_address(), request)?;
                }
            }
            Ok(())
        }

        pub fn register_rpc_hook(&mut self, hook: Arc<dyn RPCHook>) {
            self.rpc_hooks.push(hook);
        }
    }
    async fn handle_error(
        ctx: &mut ConnectionHandlerContext,
        oneway_rpc: bool,
        opaque: i32,
        exception: Option<RocketMQError>,
    ) -> HandleErrorResult {
        if let Some(exception_inner) = exception {
            match exception_inner {
                RocketMQError::Internal(message) if message.starts_with("Abort") => {
                    let code = ResponseCode::SystemError;
                    if oneway_rpc {
                        return HandleErrorResult::ReturnMethod;
                    }
                    let response = RemotingCommand::create_response_command_with_code_remark(code, message);
                    tokio::select! {
                        result =ctx.connection_mut().send_command(response.set_opaque(opaque)) => match result{
                            Ok(_) =>{},
                            Err(err) => {
                                match err {
                                    RocketMQError::IO(io_error) => {
                                        error!("send response failed: {}", io_error);
                                        return HandleErrorResult::ReturnMethod;
                                    }
                                    _ => { error!("send response failed: {}", err);}
                                }
                            },
                        },
                    }
                }
                _ => {
                    if !oneway_rpc {
                        let response = RemotingCommand::create_response_command_with_code_remark(
                            ResponseCode::SystemError,
                            exception_inner.to_string(),
                        );
                        tokio::select! {
                            result =ctx.connection_mut().send_command(response.set_opaque(opaque)) => match result{
                                Ok(_) =>{},
                                Err(err) => {
                                    match err {
                                        RocketMQError::IO(io_error) => {
                                            error!("send response failed: {}", io_error);
                                            return HandleErrorResult::ReturnMethod;
                                        }
                                        _ => { error!("send response failed: {}", err);}
                                    }
                                },
                            },
                        }
                    }
                }
            }
            HandleErrorResult::ReturnMethod
        } else {
            HandleErrorResult::GoHead
        }
    }
    enum HandleErrorResult {
        ReturnMethod,
        GoHead,
    }
}
