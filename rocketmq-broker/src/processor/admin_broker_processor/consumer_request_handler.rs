/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::body::connection::Connection;
use rocketmq_remoting::protocol::body::consumer_connection::ConsumerConnection;
use rocketmq_remoting::protocol::header::get_consumer_connection_list_request_header::GetConsumerConnectionListRequestHeader;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::protocol::RemotingSerializable;
use rocketmq_remoting::runtime::server::ConnectionHandlerContext;

use crate::processor::admin_broker_processor::Inner;

#[derive(Clone)]
pub(super) struct ConsumerRequestHandler {
    inner: Inner,
}

impl ConsumerRequestHandler {
    pub fn new(inner: Inner) -> Self {
        Self { inner }
    }
}

impl ConsumerRequestHandler {
    pub async fn get_consumer_connection_list(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        request: RemotingCommand,
    ) -> Option<RemotingCommand> {
        let mut response = RemotingCommand::create_response_command();
        let request_header = request
            .decode_command_custom_header::<GetConsumerConnectionListRequestHeader>()
            .unwrap();
        let consumer_group_info = self
            .inner
            .consume_manager
            .get_consumer_group_info(request_header.get_consumer_group());
        match consumer_group_info {
            Some(consumer_group_info) => {
                let mut body_data = ConsumerConnection::new();
                body_data.set_consume_from_where(consumer_group_info.get_consume_from_where());
                body_data.set_consume_type(consumer_group_info.get_consume_type());
                body_data.set_message_model(consumer_group_info.get_message_model());
                let subscription_table =
                    consumer_group_info.get_subscription_table().read().clone();
                body_data
                    .get_subscription_table()
                    .extend(subscription_table);

                for (channel, info) in consumer_group_info.get_channel_info_table().read().iter() {
                    let mut connection = Connection::new();
                    connection.set_client_id(info.client_id().clone());
                    connection.set_language(info.language());
                    connection.set_version(info.version());
                    connection.set_client_addr(channel.remote_address().to_string());
                    body_data.get_connection_set().insert(connection);
                }
                let body = body_data.encode();
                response.set_body_mut_ref(Some(body));
                Some(response)
            }
            None => Some(
                response
                    .set_code(ResponseCode::ConsumerNotOnline)
                    .set_remark(Some(format!(
                        "the consumer group[{}] not online",
                        request_header.get_consumer_group()
                    ))),
            ),
        }
    }
}
