// Copyright 2026 The RocketMQ Rust Authors
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

use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::body::connection::Connection;
use rocketmq_protocol::protocol::body::producer_connection::ProducerConnection;
use rocketmq_protocol::protocol::header::get_producer_connection_list_request_header::GetProducerConnectionListRequestHeader;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_protocol::protocol::RemotingSerializable;

use crate::client::manager::producer_manager::ProducerSessionRegistry;

#[derive(Clone)]
pub(super) struct ProducerRequestHandler {
    producer_registry: ProducerSessionRegistry,
}
impl ProducerRequestHandler {
    pub fn new(producer_registry: ProducerSessionRegistry) -> Self {
        Self { producer_registry }
    }
    pub async fn get_producer_connection_list(
        &self,
        request: &RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let response = RemotingCommand::create_java_default_error_response_command();
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
            return Ok(Some(RemotingCommand::create_success_response_command().set_body(body)));
        }

        Ok(Some(response.set_code(ResponseCode::SystemError).set_remark(format!(
            "the producer group[{}] not exist",
            request_header.producer_group()
        ))))
    }

    pub async fn get_all_producer_info(
        &self,
        _request: &RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let producer_table_info = self.producer_registry.producer_table();
        let body = producer_table_info.encode()?;
        Ok(Some(RemotingCommand::create_success_response_command().set_body(body)))
    }
}
