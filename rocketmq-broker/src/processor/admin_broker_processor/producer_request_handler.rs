use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::protocol::body::connection::Connection;
use rocketmq_remoting::protocol::body::producer_connection::ProducerConnection;
use rocketmq_remoting::protocol::header::get_producer_connection_list_request_header::GetProducerConnectionListRequestHeader;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::protocol::RemotingSerializable;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;

use crate::client::manager::producer_manager::ProducerChannelRegistry;

#[derive(Clone)]
pub(super) struct ProducerRequestHandler {
    producer_registry: ProducerChannelRegistry,
}
impl ProducerRequestHandler {
    pub fn new(producer_registry: ProducerChannelRegistry) -> Self {
        Self { producer_registry }
    }
    pub async fn get_producer_connection_list(
        &self,
        _ctx: ConnectionHandlerContext,
        request: &RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let mut response = RemotingCommand::create_response_command();
        let request_header = request.decode_command_custom_header_fast::<GetProducerConnectionListRequestHeader>()?;
        let mut producer_connection = ProducerConnection::new();

        let producer_table = self.producer_registry.producer_table();
        if let Some(channel_info_hashmap) = producer_table.data().get(request_header.producer_group().as_str()) {
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

    pub async fn get_all_producer_info(
        &self,
        _ctx: ConnectionHandlerContext,
        _request: &RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let mut response = RemotingCommand::create_response_command();
        let producer_table_info = self.producer_registry.producer_table();
        let body = producer_table_info.encode()?;
        response = response.set_body(body).set_code(ResponseCode::Success);
        Ok(Some(response))
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn producer_request_handler_has_no_broker_runtime_owner() {
        let source = include_str!("producer_request_handler.rs");
        let production_source = source.split("#[cfg(test)]").next().expect("production source");
        assert!(production_source.contains("ProducerChannelRegistry"));
        assert!(!production_source.contains(concat!("BrokerRuntime", "Inner")));
        assert!(!production_source.contains(concat!("Arc", "Mut")));
        assert!(!production_source.contains(concat!("Message", "Store")));
    }
}
