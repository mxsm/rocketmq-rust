use cheetah_string::CheetahString;
use rocketmq_common::RocketMQVersion;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::protocol::header::empty_header::EmptyHeader;
use rocketmq_remoting::protocol::header::get_consumer_running_info_request_header::GetConsumerRunningInfoRequestHeader;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_store::MessageStore;

use crate::broker_runtime::BrokerRuntimeInner;

#[derive(Clone)]
pub(super) struct GetConsumerRunningInfoHandler<MS: MessageStore> {
    broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
}
impl<MS: MessageStore> GetConsumerRunningInfoHandler<MS> {
    pub fn new(broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>) -> Self {
        Self {
            broker_runtime_inner,
        }
    }
    pub async fn get_consumer_running_info(
        &self,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let request_header =
            request.decode_command_custom_header_fast::<GetConsumerRunningInfoRequestHeader>()?;

        self.call_consumer(
            RequestCode::GetConsumerRunningInfo.to_i32(),
            request,
            request_header.consumer_group,
            request_header.client_id,
        )
        .await
    }
    async fn call_consumer(
        &self,
        request_code: i32,
        request: &mut RemotingCommand,
        consumer_group: CheetahString,
        client_id: CheetahString,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let mut response = RemotingCommand::create_response_command();
        let mut client_channel_info = self
            .broker_runtime_inner
            .consumer_manager()
            .find_channel_by_client_id(consumer_group.as_ref(), client_id.as_ref());

        if let Some(client_channel_info) = &mut client_channel_info {
            if (client_channel_info.version() as u32) < RocketMQVersion::V3_1_8_SNAPSHOT.ordinal() {
                response = response
                    .set_code(ResponseCode::SystemError)
                    .set_remark(format!(
                        "The Consumer <{}> Version <{}> too low to finish, please upgrade it to \
                         V3_1_8_SNAPSHOT",
                        client_id,
                        client_channel_info.version()
                    ));
                return Ok(Some(response));
            }

            let mut new_request =
                RemotingCommand::create_request_command(request_code, EmptyHeader {});
            if let Some(ext_fields) = request.ext_fields() {
                new_request = new_request.set_ext_fields(ext_fields.clone());
            }
            if let Some(body) = request.body() {
                new_request = new_request.set_body(body.clone());
            }
            if let Some(ms) = self
                .broker_runtime_inner
                .transactional_message_check_listener()
            {
                let remoting_cmd = ms
                    .broker_to_client()
                    .call_client(client_channel_info.channel_mut(), new_request, 3000)
                    .await?;
                return Ok(Some(remoting_cmd));
            }
        } else {
            response = response
                .set_code(ResponseCode::SystemError)
                .set_remark(format!(
                    "The Consumer <{}> <{}> not online",
                    consumer_group, client_id
                ));
            return Ok(Some(response));
        }

        Err(rocketmq_error::RocketMQError::Internal(
            "GetConsumerRunningInfoHandler::call_consumer err".to_string(),
        ))
    }
}
