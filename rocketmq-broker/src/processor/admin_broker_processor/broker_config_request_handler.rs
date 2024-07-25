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

use std::collections::HashMap;

use bytes::Bytes;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::runtime::server::ConnectionHandlerContext;

use crate::processor::admin_broker_processor::Inner;

#[derive(Clone)]
pub(super) struct BrokerConfigRequestHandler {
    inner: Inner,
}

impl BrokerConfigRequestHandler {
    pub fn new(inner: Inner) -> Self {
        BrokerConfigRequestHandler { inner }
    }
}
impl BrokerConfigRequestHandler {
    pub async fn update_broker_config(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        _request: RemotingCommand,
    ) -> Option<RemotingCommand> {
        todo!()
    }

    pub async fn get_broker_config(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        _request: RemotingCommand,
    ) -> Option<RemotingCommand> {
        let mut response = RemotingCommand::create_response_command();
        // broker config => broker config
        // default message store config => message store config
        let broker_config = self.inner.broker_config.clone();
        let message_store_config = self
            .inner
            .default_message_store
            .message_store_config()
            .clone();
        let broker_config_properties = broker_config.get_properties();
        let message_store_config_properties = message_store_config.get_properties();
        let combine_map = broker_config_properties
            .iter()
            .chain(message_store_config_properties.iter())
            .collect::<HashMap<_, _>>();
        let mut body = String::new();
        for (key, value) in combine_map {
            body.push_str(&format!("{}:{}\n", key, value));
        }
        if !body.is_empty() {
            response.set_body_mut_ref(Some(Bytes::from(body)));
        }
        Some(response)
    }
}
