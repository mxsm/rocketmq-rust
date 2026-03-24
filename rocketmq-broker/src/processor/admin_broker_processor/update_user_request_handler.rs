use crate::auth::auth_admin_service::AuthAdminService;
use crate::auth::user_converter::UserConverter;
use crate::broker_runtime::BrokerRuntimeInner;
use cheetah_string::CheetahString;
use rocketmq_auth::authentication::enums::user_type::UserType;
use rocketmq_error::RocketMQError;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::body::user_info::UserInfo;
use rocketmq_remoting::protocol::header::update_user_request_header::UpdateUserRequestHeader;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::protocol::RemotingDeserializable;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_store::MessageStore;
use std::sync::Arc;

#[derive(Clone)]
pub struct UpdateUserRequestHandler<MS: MessageStore> {
    _broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
    auth_admin_service: Arc<AuthAdminService>,
}

impl<MS: MessageStore> UpdateUserRequestHandler<MS> {
    pub fn new(
        broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
        auth_admin_service: Arc<AuthAdminService>,
    ) -> Self {
        Self {
            _broker_runtime_inner: broker_runtime_inner,
            auth_admin_service,
        }
    }

    pub async fn update_user(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let request_header = request.decode_command_custom_header::<UpdateUserRequestHeader>()?;

        let response = RemotingCommand::create_response_command();

        if request_header.username.is_empty() {
            return Ok(Some(
                response
                    .set_code(ResponseCode::InvalidParameter)
                    .set_remark("The username is blank"),
            ));
        }

        let body = match request.get_body() {
            Some(body) => body,
            None => {
                return Ok(Some(
                    response
                        .set_code(ResponseCode::InvalidParameter)
                        .set_remark("Request body is empty"),
                ));
            }
        };
        let mut user_info: UserInfo = UserInfo::decode(body)?;

        user_info.username = Option::from(request_header.username);
        let user = UserConverter::convert_user(&user_info);

        if user.user_type() == Option::from(UserType::Super) && self.is_not_super_user_login(request).await {
            return Ok(Some(
                response
                    .set_code(ResponseCode::NoPermission)
                    .set_remark("The super user can only be update by super user"),
            ));
        }

        match self.auth_admin_service.update_user(user).await {
            Ok(()) => Ok(Some(response.set_code(ResponseCode::Success))),
            Err(error) => Ok(Some(map_error_response(response, error))),
        }
    }

    async fn is_not_super_user_login(&self, _request: &RemotingCommand) -> bool {
        let Some(access_key) = _request
            .ext_fields()
            .and_then(|fields| fields.get(&CheetahString::from_static_str("AccessKey")))
        else {
            return false;
        };

        !self
            .auth_admin_service
            .is_super_user(access_key.as_str())
            .await
            .unwrap_or(false)
    }
}

fn map_error_response(response: RemotingCommand, error: RocketMQError) -> RemotingCommand {
    match error {
        RocketMQError::IllegalArgument(message) => {
            response.set_code(ResponseCode::InvalidParameter).set_remark(message)
        }
        RocketMQError::BrokerPermissionDenied { operation } => {
            response.set_code(ResponseCode::NoPermission).set_remark(operation)
        }
        other => response
            .set_code(ResponseCode::SystemError)
            .set_remark(other.to_string()),
    }
}
