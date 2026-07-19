use std::sync::Arc;

use rocketmq_error::RocketMQError;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::header::list_acl_request_header::ListAclRequestHeader;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::protocol::RemotingSerializable;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;

use crate::auth::auth_admin_service::AuthAdminService;

#[derive(Clone)]
pub struct ListAclRequestHandler {
    auth_admin_service: Arc<AuthAdminService>,
}

impl ListAclRequestHandler {
    pub fn new(auth_admin_service: Arc<AuthAdminService>) -> Self {
        Self { auth_admin_service }
    }

    pub async fn list_acl(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let request_header = request.decode_command_custom_header::<ListAclRequestHeader>()?;
        let mut response = RemotingCommand::create_response_command();

        match self
            .auth_admin_service
            .list_acls(
                non_empty(request_header.subject_filter.as_str()),
                non_empty(request_header.resource_filter.as_str()),
            )
            .await
        {
            Ok(acls) => {
                if !acls.is_empty() {
                    response.set_body_mut_ref(acls.encode()?);
                }
                Ok(Some(response.set_code(ResponseCode::Success)))
            }
            Err(error) => Ok(Some(map_error_response(response, error))),
        }
    }
}

fn non_empty(value: &str) -> Option<&str> {
    if value.trim().is_empty() {
        None
    } else {
        Some(value)
    }
}

fn map_error_response(response: RemotingCommand, error: RocketMQError) -> RemotingCommand {
    super::map_auth_admin_error_response(response, error)
}
