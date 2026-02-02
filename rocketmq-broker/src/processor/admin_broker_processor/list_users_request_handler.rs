use crate::broker_runtime::BrokerRuntimeInner;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_store::MessageStore;

#[derive(Clone)]
pub struct ListUsersRequestHandler<MS: MessageStore> {
    broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
}

impl<MS: MessageStore> ListUsersRequestHandler<MS> {
    pub fn new(broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>) -> Self {
        Self { broker_runtime_inner }
    }

    pub async fn list_users_request(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        _request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        // let request_header = request.decode_command_custom_header::<ListUsersRequestHeader>()?;
        let response = RemotingCommand::default();

        // TODO get authentication metadata manager and do operations
        Ok(Some(response.set_code(ResponseCode::SystemError).set_remark(
            "ListUsers not implemented: authentication metadata manager not configured",
        )))
    }
}
