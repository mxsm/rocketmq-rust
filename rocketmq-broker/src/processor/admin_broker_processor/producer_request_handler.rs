use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::protocol::body::connection::Connection;
use rocketmq_remoting::protocol::body::producer_connection::ProducerConnection;
use rocketmq_remoting::protocol::header::get_producer_connection_list_request_header::GetProducerConnectionListRequestHeader;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::protocol::RemotingSerializable;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_store::MessageStore;

use crate::broker_runtime::BrokerRuntimeInner;

#[derive(Clone)]
pub(super) struct ProducerRequestHandler<MS: MessageStore> {
    broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
}
impl<MS: MessageStore> ProducerRequestHandler<MS> {
    pub fn new(broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>) -> Self {
        Self { broker_runtime_inner }
    }
    pub async fn get_producer_connection_list(
        &self,
        _ctx: ConnectionHandlerContext,
        request: &RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let mut response = RemotingCommand::create_response_command();
        let request_header = request.decode_command_custom_header_fast::<GetProducerConnectionListRequestHeader>()?;
        let mut producer_connection = ProducerConnection::new();

        if let Some(channel_info_hashmap) = self
            .broker_runtime_inner
            .producer_manager()
            .get_producer_table()
            .data()
            .get(request_header.producer_group().as_str())
        {
            for i in channel_info_hashmap {
                let mut connection = Connection::new();
                connection.set_client_id(i.client_id().into());
                connection.set_language(i.language());
                connection.set_version(i.version());
                connection.set_client_addr(i.remote_ip().into());

                producer_connection.connection_set_mut().insert(connection);
            }
            let body = producer_connection.encode()?;
            response = response.set_body(body).set_code(ResponseCode::Success);
            return Ok(Some(response));
        }

        response = response.set_code(ResponseCode::SystemError).set_remark(format!(
            "the producer group[{}] not exist",
            request_header.producer_group()
        ));
        Ok(Some(response))
    }
}
