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
use std::sync::Weak;

use crate::base::response_future::ResponseFuture;
use crate::runtime::RPCHook;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;

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
    async fn start(&self, this: Weak<Self>);

    /// Shuts down the remoting service.
    ///
    /// This function is responsible for gracefully shutting down the service. It should ensure
    /// that all resources are released, and any ongoing operations are completed or aborted
    /// appropriately before the service stops.
    fn shutdown(&self);

    /// Registers an RPC hook.
    ///
    /// This function allows for the registration of an RPC hook, which can be used to intercept
    /// and modify the behavior of remote procedure calls. Hooks can be used for logging,
    /// monitoring, or modifying the requests or responses of RPCs.
    ///
    /// # Arguments
    /// * `hook` - An implementation of the `RPCHook` trait that will be registered.
    fn register_rpc_hook(&self, hook: Arc<dyn RPCHook>);

    /// Clears all registered RPC hooks.
    ///
    /// This function removes all previously registered RPC hooks, returning the service to its
    /// default state without any hooks. This can be useful for cleanup or when changing the
    /// configuration of the service.
    fn clear_rpc_hook(&self);
}
pub trait InvokeCallback {
    fn operation_complete(&self, response_future: ResponseFuture);
    fn operation_succeed(&self, response: RemotingCommand);
    fn operation_fail(&self, throwable: rocketmq_error::RocketMQError);
}

#[allow(unused_variables)]
pub(crate) mod inner {
    use std::net::SocketAddr;
    use std::sync::Arc;

    use rocketmq_error::RocketMQError;
    use rocketmq_error::RocketMQResult;
    use tracing::error;
    use tracing::warn;
    use tracing::Instrument;

    use crate::base::pending_request_table::PendingRequestTable;
    use crate::hook_registry::HookRegistry;
    use crate::hook_registry::HookSnapshot;
    use crate::runtime::connection_handler_context::ConnectionHandlerContext;
    use crate::runtime::processor::RequestProcessor;
    use crate::runtime::RPCHook;
    use crate::telemetry::TransportTelemetry;
    use rocketmq_protocol::code::request_code::RequestCode;
    use rocketmq_protocol::code::response_code::ResponseCode;
    use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
    use rocketmq_protocol::protocol::RemotingCommandType;

    pub(crate) struct RemotingGeneralHandler<RP> {
        pub(crate) request_processor: RP,
        rpc_hooks: HookRegistry,
        pub(crate) response_table: PendingRequestTable,
        telemetry: TransportTelemetry,
    }

    impl<RP> RemotingGeneralHandler<RP>
    where
        RP: RequestProcessor + Sync + Clone + 'static,
    {
        #[cfg(test)]
        pub(crate) fn new(
            request_processor: RP,
            rpc_hooks: Vec<Arc<dyn RPCHook>>,
            response_table: PendingRequestTable,
        ) -> Self {
            Self::new_with_telemetry(request_processor, rpc_hooks, response_table, TransportTelemetry::noop())
        }

        pub(crate) fn new_with_telemetry(
            request_processor: RP,
            rpc_hooks: Vec<Arc<dyn RPCHook>>,
            response_table: PendingRequestTable,
            telemetry: TransportTelemetry,
        ) -> Self {
            Self {
                request_processor,
                rpc_hooks: HookRegistry::new(rpc_hooks),
                response_table,
                telemetry,
            }
        }

        pub async fn process_message_received(&self, ctx: &ConnectionHandlerContext, cmd: RemotingCommand) {
            match cmd.get_type() {
                RemotingCommandType::REQUEST => {
                    let span = self.telemetry.request_span(cmd.code(), cmd.opaque());
                    match self.process_request_command(ctx, cmd).instrument(span).await {
                        Ok(_) => {}
                        Err(e) => {
                            error!("process request command failed: {}", e);
                        }
                    }
                }
                RemotingCommandType::RESPONSE => {
                    self.process_response_command(ctx, cmd);
                }
            }
        }

        async fn process_request_command(
            &self,
            ctx: &ConnectionHandlerContext,
            mut cmd: RemotingCommand,
        ) -> RocketMQResult<()> {
            let opaque = cmd.opaque();
            let mut metrics_guard = self.telemetry.request_guard(
                cmd.code(),
                cmd.body().map_or(0, |body| body.len() as u64),
                is_long_polling_request(cmd.code()),
            );
            let mut request_processor = self.request_processor.clone();
            let reject_request = request_processor.reject_request(cmd.code());
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
                let response_code = response.code();
                let result = ctx.channel().send_command(response.set_opaque(opaque)).await;
                match result {
                    Ok(_) => {
                        metrics_guard.complete_response(response_code);
                    }
                    Err(error) => {
                        metrics_guard.complete_write_channel_failed(response_code);
                        return Err(error);
                    }
                }
                return Ok(());
            }
            let oneway_rpc = cmd.is_oneway_rpc();
            let hook_snapshot = self.rpc_hooks.snapshot();
            //before handle request hooks
            let exception = self
                .do_before_rpc_hooks_with_snapshot(
                    hook_snapshot.as_deref(),
                    ctx.channel().remote_address(),
                    Some(&mut cmd),
                )
                .err();
            //handle error if return have
            match handle_error(ctx, oneway_rpc, opaque, exception).await {
                HandleErrorResult::ReturnMethod => {
                    metrics_guard.complete_process_request_failed(ResponseCode::SystemError.to_i32());
                    return Ok(());
                }
                HandleErrorResult::GoHead => {}
            }

            let mut response = {
                let channel = ctx.channel().clone();
                let ctx = ctx.clone();
                match request_processor.process_request(channel, ctx, &mut cmd).await {
                    Ok(result) => result,
                    Err(_err) => {
                        metrics_guard.complete_process_request_failed(ResponseCode::SystemError.to_i32());
                        Some(RemotingCommand::create_response_command_with_code(
                            ResponseCode::SystemError,
                        ))
                    }
                }
            };

            let exception = self
                .do_after_rpc_hooks_with_snapshot(
                    hook_snapshot.as_deref(),
                    ctx.channel().remote_address(),
                    &cmd,
                    response.as_mut(),
                )
                .err();

            match handle_error(ctx, oneway_rpc, opaque, exception).await {
                HandleErrorResult::ReturnMethod => {
                    metrics_guard.complete_process_request_failed(ResponseCode::SystemError.to_i32());
                    return Ok(());
                }
                HandleErrorResult::GoHead => {}
            }
            if oneway_rpc {
                metrics_guard.complete_oneway();
                return Ok(());
            }
            let Some(response) = response else {
                metrics_guard.complete_cancelled();
                return Ok(());
            };
            let response_code = response.code();
            let result = ctx.channel().send_command(response.set_opaque(opaque)).await;
            match result {
                Ok(_) => {
                    metrics_guard.complete_response(response_code);
                }
                Err(err) => match err {
                    RocketMQError::IO(io_error) => {
                        metrics_guard.complete_write_channel_failed(response_code);
                        error!("connection disconnect: {}", io_error);
                        return Ok(());
                    }
                    _ => {
                        metrics_guard.complete_write_channel_failed(response_code);
                        error!("send response failed: {}", err);
                    }
                },
            };
            Ok(())
        }

        fn process_response_command(&self, ctx: &ConnectionHandlerContext, cmd: RemotingCommand) {
            let opaque = cmd.opaque();
            let code = cmd.code();
            let completed = match ctx.channel().pending_request_owner() {
                Some(owner) => self.response_table.complete_response_for_owner(owner, opaque, cmd),
                None => self.response_table.complete_response(opaque, cmd),
            };
            if !completed {
                warn!(
                    opaque,
                    code,
                    address = %ctx.channel().remote_address(),
                    channel_id = %ctx.channel().channel_id(),
                    "received response without a matching pending request",
                );
            }
        }

        pub(crate) fn hook_snapshot(&self) -> Option<Arc<HookSnapshot>> {
            self.rpc_hooks.snapshot()
        }

        pub(crate) fn do_before_rpc_hooks_with_snapshot(
            &self,
            snapshot: Option<&HookSnapshot>,
            remote_address: SocketAddr,
            request: Option<&mut RemotingCommand>,
        ) -> rocketmq_error::RocketMQResult<()> {
            if let (Some(snapshot), Some(request)) = (snapshot, request) {
                for hook in snapshot.hooks() {
                    hook.do_before_request(remote_address, request)?;
                }
            }
            Ok(())
        }

        pub(crate) fn do_after_rpc_hooks_with_snapshot(
            &self,
            snapshot: Option<&HookSnapshot>,
            remote_address: SocketAddr,
            request: &RemotingCommand,
            response: Option<&mut RemotingCommand>,
        ) -> rocketmq_error::RocketMQResult<()> {
            if let (Some(snapshot), Some(response)) = (snapshot, response) {
                for hook in snapshot.hooks() {
                    hook.do_after_response(remote_address, request, response)?;
                }
            }
            Ok(())
        }

        pub fn register_rpc_hook(&self, hook: Arc<dyn RPCHook>) {
            self.rpc_hooks.register(hook);
        }

        pub fn clear_rpc_hook(&self) {
            self.rpc_hooks.clear();
        }
    }

    #[inline]
    fn is_long_polling_request(request_code: i32) -> bool {
        matches!(
            RequestCode::from(request_code),
            RequestCode::PullMessage
                | RequestCode::PopMessage
                | RequestCode::PopLiteMessage
                | RequestCode::LitePullMessage
                | RequestCode::Notification
                | RequestCode::PollingInfo
        )
    }

    async fn handle_error(
        ctx: &ConnectionHandlerContext,
        oneway_rpc: bool,
        opaque: i32,
        exception: Option<RocketMQError>,
    ) -> HandleErrorResult {
        if let Some(exception_inner) = exception {
            if !oneway_rpc {
                let response = crate::error_response::command_from_error(&exception_inner);
                tokio::select! {
                    result =ctx.channel().send_command(response.set_opaque(opaque)) => match result{
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
            HandleErrorResult::ReturnMethod
        } else {
            HandleErrorResult::GoHead
        }
    }
    enum HandleErrorResult {
        ReturnMethod,
        GoHead,
    }

    #[cfg(test)]
    mod tests {
        use rocketmq_protocol::code::request_code::RequestCode;

        use super::is_long_polling_request;

        #[test]
        fn long_polling_request_codes_match_broker_poll_paths() {
            assert!(is_long_polling_request(RequestCode::PullMessage.to_i32()));
            assert!(is_long_polling_request(RequestCode::PopMessage.to_i32()));
            assert!(is_long_polling_request(RequestCode::Notification.to_i32()));
            assert!(!is_long_polling_request(RequestCode::SendMessage.to_i32()));
        }
    }
}
