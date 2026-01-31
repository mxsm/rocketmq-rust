use crate::auth::user_converter::UserConverter;
use crate::broker_runtime::BrokerRuntimeInner;
use rocketmq_auth::authentication::enums::user_type::UserType;
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

#[derive(Clone)]
pub struct UpdateUserRequestHandler<MS: MessageStore> {
    broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
}

impl<MS: MessageStore> UpdateUserRequestHandler<MS> {
    pub fn new(broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>) -> Self {
        Self { broker_runtime_inner }
    }

    pub async fn update_user(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let request_header = request.decode_command_custom_header::<UpdateUserRequestHeader>()?;

        let mut response = RemotingCommand::default();

        if (request_header.username.is_empty()) {
            return Ok(Some(
                response
                    .set_code(ResponseCode::InvalidParameter)
                    .set_remark("The username is blank"),
            ));
        }

        let mut user_info: UserInfo = UserInfo::decode(request.get_body().unwrap())?;

        user_info.username = Option::from(request_header.username);

        let user = UserConverter::convert_user(&user_info);

        if user.user_type() == Option::from(UserType::Super) && self.is_not_super_user_login(request).await {
            return Ok(Some(
                response
                    .set_code(ResponseCode::SystemError)
                    .set_remark("The super user can only be update by super user"),
            ));
        }

        //TODO get authentication metadata manager and do operations
        response.set_code_ref(ResponseCode::Success);
        Ok(Some(response))
    }

    async fn is_not_super_user_login(&self, request: &RemotingCommand) -> bool {
        // Get "AccessKey" from ext_fields
        let access_key = request.ext_fields().unwrap().get("AccessKey");

        // If access key is missing or empty, authentication may not be enabled
        let access_key = match access_key {
            Some(k) if !k.is_empty() => k,
            _ => return false,
        };

        // Check superuser (await the async function)
        // TODO get the authentication metadata manager and check if the user is superuser
        let is_super = false;

        !is_super
    }
}
