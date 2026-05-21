use std::sync::Arc;

use rocketmq_error::RocketMQError;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::header::delete_acl_request_header::DeleteAclRequestHeader;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_store::base::message_store::MessageStore;

use crate::auth::auth_admin_service::AuthAdminService;
use crate::broker_runtime::BrokerRuntimeInner;

#[derive(Clone)]
pub struct DeleteAclRequestHandler<MS: MessageStore> {
    _broker_runtime_inner: rocketmq_rust::ArcMut<BrokerRuntimeInner<MS>>,
    auth_admin_service: Arc<AuthAdminService>,
}

impl<MS: MessageStore> DeleteAclRequestHandler<MS> {
    pub fn new(
        broker_runtime_inner: rocketmq_rust::ArcMut<BrokerRuntimeInner<MS>>,
        auth_admin_service: Arc<AuthAdminService>,
    ) -> Self {
        Self {
            _broker_runtime_inner: broker_runtime_inner,
            auth_admin_service,
        }
    }

    pub async fn delete_acl(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let request_header = request.decode_command_custom_header::<DeleteAclRequestHeader>()?;
        let response = RemotingCommand::create_response_command();

        if request_header.subject.is_empty() {
            return Ok(Some(
                response
                    .set_code(ResponseCode::InvalidParameter)
                    .set_remark("The subject is blank"),
            ));
        }

        match self
            .auth_admin_service
            .delete_acl(
                request_header.subject.as_str(),
                request_header
                    .policy_type
                    .as_ref()
                    .map(|policy_type| policy_type.as_str()),
                request_header.resource.as_ref().map(|resource| resource.as_str()),
            )
            .await
        {
            Ok(()) => Ok(Some(response.set_code(ResponseCode::Success))),
            Err(error) => Ok(Some(map_error_response(response, error))),
        }
    }
}

fn map_error_response(response: RemotingCommand, error: RocketMQError) -> RemotingCommand {
    super::map_auth_admin_error_response(response, error)
}
